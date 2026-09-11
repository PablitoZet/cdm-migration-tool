"""Consistent read-only PostgreSQL extraction for Content Server inventory."""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

from .inventory import discovery_summary, source_signature

logger = logging.getLogger("CDM.SourceDB")


SUBTYPE_NAMES = {
    0: "Folder", 1: "Shortcut", 131: "Category", 136: "Compound Document",
    140: "URL", 144: "Document", 154: "Revision", 202: "Project",
    298: "Collection", 751: "Compound/Email", 848: "Business Workspace",
    849: "Business Workspace Subtype", 899: "Business Workspace Template",
}

SCHEMA_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SourceDB:
    def __init__(self, db_config: Any):
        get = db_config.get
        self.config = db_config
        schema = str(get("db_schema", "public") or "public").strip()
        if not SCHEMA_NAME_PATTERN.match(schema):
            raise RuntimeError(
                f"Invalid db_schema {schema!r}: must be a plain PostgreSQL identifier"
            )
        self._schema = schema
        self.connection_args = {
            "host": get("db_host"), "port": int(get("db_port", 5432)),
            "dbname": get("db_name", "cs"), "user": get("db_user"),
            "password": get("db_password", ""), "sslmode": get("sslmode", "require"),
            "connect_timeout": int(get("db_connect_timeout", 10)),
            "application_name": "cdm-migration-readonly-v2",
        }
        self._psycopg2 = None

    def _driver(self):
        if self._psycopg2 is None:
            try:
                import psycopg2
                import psycopg2.extras
            except ImportError as exc:
                raise RuntimeError("psycopg2-binary is required for PostgreSQL extraction") from exc
            self._psycopg2 = psycopg2
        return self._psycopg2

    @contextmanager
    def snapshot(self) -> Iterator[Any]:
        driver = self._driver()
        conn = driver.connect(**self.connection_args)
        try:
            conn.set_session(readonly=True, autocommit=False, isolation_level="REPEATABLE READ")
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = %s", (int(self.config.get("db_statement_timeout_ms", 300000)),))
                cur.execute("SELECT txid_current_snapshot()")
                snapshot_id = cur.fetchone()[0]
            yield conn, snapshot_id
            conn.rollback()  # Explicitly close the read-only snapshot.
        finally:
            conn.close()

    def test_connection(self) -> dict[str, Any]:
        try:
            with self.snapshot() as (conn, snapshot_id):
                with conn.cursor() as cur:
                    cur.execute("SELECT version(), current_database(), current_user")
                    version, database, user = cur.fetchone()
                    cur.execute(f"SELECT COUNT(*) FROM {self._schema}.DTree")
                    count = cur.fetchone()[0]
                return {
                    "status": "connected", "version": version, "database": database,
                    "user": user, "dtree_objects": count, "snapshot": snapshot_id,
                    "read_only": True,
                }
        except Exception as exc:
            return {"status": "error", "error": str(exc)}

    def extract_all(self, root_node_id: int) -> dict[str, Any]:
        with self.snapshot() as (conn, snapshot_id):
            nodes = self._extract_nodes(conn, root_node_id)
            if not nodes:
                return {"nodes": [], "versions": [], "categories": [], "owners": [], "snapshot": snapshot_id}
            node_ids = [row["source_id"] for row in nodes]
            versions = self._extract_versions(conn, node_ids)
            categories = self._extract_categories(conn, node_ids)
            owner_ids = sorted({
                int(row["owner_id"]) for row in nodes if row.get("owner_id") is not None
            })
            owners = self._extract_owners(conn, owner_ids)
        return {
            "nodes": nodes, "versions": versions, "categories": categories, "owners": owners,
            "snapshot": snapshot_id, "extracted_at": datetime.now(UTC).isoformat(),
            "source_signature": source_signature(nodes, versions, categories, owners),
        }

    def inspect_scope(self, root_node_id: int) -> dict[str, Any]:
        """Read-only preview used before committing a manifest extraction."""
        extracted = self.extract_all(root_node_id)
        summary = discovery_summary(
            extracted["nodes"], extracted["versions"], extracted["categories"], extracted.get("owners", [])
        )
        summary["snapshot"] = extracted["snapshot"]
        return summary

    def scope_signature(self, root_node_id: int) -> str:
        return str(self.inspect_scope(root_node_id)["signature"])

    def _cursor(self, conn):
        return conn.cursor(cursor_factory=self._driver().extras.RealDictCursor)

    def _extract_nodes(self, conn, root_node_id: int) -> list[dict[str, Any]]:
        reference_column = self._first_existing_column(conn, "dtree", ("originalid", "linkid"))
        url_column = self._first_existing_column(conn, "dtree", ("url",))
        description_column = self._first_existing_column(conn, "dtree", ("comment", "description"))
        reserved_column = self._first_existing_column(conn, "dtree", ("reservedby", "reserved"))
        root_reference = f"d.{reference_column}" if reference_column else "NULL::integer"
        child_reference = f"c.{reference_column}" if reference_column else "NULL::integer"
        root_url = f"d.{url_column}" if url_column else "NULL::text"
        child_url = f"c.{url_column}" if url_column else "NULL::text"
        root_description = f"d.{description_column}" if description_column else "''::text"
        child_description = f"c.{description_column}" if description_column else "''::text"
        root_reserved = f"d.{reserved_column}" if reserved_column else "NULL::integer"
        child_reserved = f"c.{reserved_column}" if reserved_column else "NULL::integer"
        query = f"""
            WITH RECURSIVE workspace_tree AS (
                SELECT d.DataID,d.ParentID,d.Name,d.SubType,d.CreateDate,d.ModifyDate,
                       d.OwnerID,d.PermID,{root_reference} reference_source_id,{root_url} url_value,
                       {root_description} description_value,{root_reserved} reserved_by,
                       0 AS depth,ARRAY[d.DataID] AS path_ids,d.Name::text AS full_path
                  FROM {self._schema}.DTree d WHERE d.DataID=%s AND COALESCE(d.Deleted,0)=0
                UNION ALL
                SELECT c.DataID,c.ParentID,c.Name,c.SubType,c.CreateDate,c.ModifyDate,
                       c.OwnerID,c.PermID,{child_reference},{child_url},
                       {child_description},{child_reserved},
                       w.depth+1,w.path_ids||c.DataID,(w.full_path||'/'||c.Name)::text
                  FROM {self._schema}.DTree c JOIN workspace_tree w ON c.ParentID=w.DataID
                 WHERE COALESCE(c.Deleted,0)=0 AND NOT c.DataID=ANY(w.path_ids)
            )
            SELECT DataID source_id,ParentID parent_source_id,Name name,SubType subtype,
                   depth,full_path path,COALESCE(description_value,'')::text description,
                   CreateDate create_date,ModifyDate modify_date,
                   OwnerID owner_id,NULL::integer group_id,PermID permissions_id,
                   reference_source_id,url_value,reserved_by
              FROM workspace_tree ORDER BY depth,source_id
        """
        with self._cursor(conn) as cur:
            cur.execute(query, (root_node_id,))
            rows = [dict(row) for row in cur.fetchall()]
        for row in rows:
            row["type_name"] = SUBTYPE_NAMES.get(row["subtype"], f"Type_{row['subtype']}")
            row["extra"] = {
                key: value for key, value in {
                    "reference_source_id": row.pop("reference_source_id", None),
                    "url": row.pop("url_value", None),
                    "reserved_by": row.pop("reserved_by", None),
                }.items() if value is not None
            }
        return rows

    def _extract_versions(self, conn, node_ids: list[int]) -> list[dict[str, Any]]:
        if self._column_exists(conn, "dversdata", "providerdata"):
            provider_data_expr = "d.ProviderData"
            provider_type_expr = "NULL::text"
            provider_data_join = ""
        elif (
            self._column_exists(conn, "providerdata", "providerid")
            and self._column_exists(conn, "providerdata", "providerdata")
        ):
            provider_data_expr = "p.ProviderData"
            provider_type_expr = (
                "p.ProviderType"
                if self._column_exists(conn, "providerdata", "providertype")
                else "NULL::text"
            )
            provider_data_join = f"LEFT JOIN {self._schema}.ProviderData p ON p.ProviderID=d.ProviderID"
        else:
            provider_data_expr = "NULL::text"
            provider_type_expr = "NULL::text"
            provider_data_join = ""
        comment_column = self._first_existing_column(conn, "dversdata", ("vercomment", "comment", "description"))
        comment_expr = f"d.{comment_column}" if comment_column else "NULL::text"
        version_id_expr = (
            "d.VersionID" if self._column_exists(conn, "dversdata", "versionid") else "NULL::bigint"
        )
        primary_filter = (
            "AND (d.VerType IS NULL OR d.VerType='')"
            if self._column_exists(conn, "dversdata", "vertype") else ""
        )
        transient_filter = (
            "AND COALESCE(d.Transient,0)=0"
            if self._column_exists(conn, "dversdata", "transient") else ""
        )
        query = f"""
            SELECT d.DocID doc_source_id,d.Version version_num,d.FileName file_name,d.MimeType mime_type,
                   d.DataSize data_size,d.ProviderID provider_id,{provider_type_expr} provider_type,
                   {provider_data_expr} provider_data,
                   d.VerCDate ver_create_date,d.VerMDate ver_modify_date,d.FileMDate ver_file_date,
                   {version_id_expr} version_id,
                   {comment_expr} version_comment
              FROM {self._schema}.DVersData d
              {provider_data_join}
             WHERE d.DocID=ANY(%s)
                   {primary_filter}
                   {transient_filter}
             ORDER BY d.DocID,d.Version
        """
        with self._cursor(conn) as cur:
            cur.execute(query, (node_ids,))
            rows = [dict(row) for row in cur.fetchall()]
        template = self.config.get("azure_blob_locator_template")
        seen: set[tuple[int, int]] = set()
        for row in rows:
            identity = (int(row["doc_source_id"]), int(row["version_num"]))
            if identity in seen:
                raise RuntimeError(
                    "Source contains duplicate primary DVersData rows for one document version; "
                    "a lossless manifest cannot be created"
                )
            seen.add(identity)
            provider_data = row.get("provider_data")
            if isinstance(provider_data, str) and provider_data.startswith(("https://", "azure://", "file://")):
                row["blob_locator"] = provider_data
            elif self._is_archive_center_descriptor(row.get("provider_type"), provider_data):
                row["blob_locator"] = None
            elif template and provider_data not in (None, ""):
                row["blob_locator"] = str(template).format(**row)
            else:
                row["blob_locator"] = None
        return rows

    @staticmethod
    def _is_archive_center_descriptor(provider_type: Any, provider_data: Any) -> bool:
        provider = str(provider_type or "").lower()
        descriptor = str(provider_data or "").lower()
        return provider.startswith("ac") or "'providerinfo'='ixos://" in descriptor

    def _extract_categories(self, conn, node_ids: list[int]) -> list[dict[str, Any]]:
        # This extracts primitive LLAttrData values. Complex sets/multi-row
        # fidelity is made explicit through AttrID + row_num and must be mapped
        # in pre-flight against the target category definitions.
        row_expr = "COALESCE(a.EntryNum,0)" if self._column_exists(conn, "llattrdata", "entrynum") else "0"
        real_expr = "a.ValReal" if self._column_exists(conn, "llattrdata", "valreal") else "NULL::double precision"
        int_expr = "a.ValInt" if self._column_exists(conn, "llattrdata", "valint") else "NULL::integer"
        query = f"""
            SELECT a.ID source_id,a.DefID def_id,d.Name cat_name,a.AttrID attr_id,
                   {row_expr} row_num,a.ValStr val_str,a.ValLong val_long,a.ValDate val_date,
                   {real_expr} val_real,{int_expr} val_int
              FROM {self._schema}.LLAttrData a LEFT JOIN {self._schema}.DTree d ON a.DefID=d.DataID
             WHERE a.ID=ANY(%s) ORDER BY a.ID,a.DefID,a.AttrID,{row_expr}
        """
        with self._cursor(conn) as cur:
            cur.execute(query, (node_ids,))
            return [dict(row) for row in cur.fetchall()]

    def _extract_owners(self, conn, owner_ids: list[int]) -> list[dict[str, Any]]:
        """Read each referenced KUAF identity once without assuming its schema."""
        if not owner_ids:
            return []
        result = {
            owner_id: {
                "source_owner_id": owner_id,
                "login": None,
                "email": None,
                "display_name": None,
                "active": None,
                "status_value": None,
                "status_source": None,
                "identity_status": "UNKNOWN",
            }
            for owner_id in owner_ids
        }
        id_column = self._first_existing_column(conn, "kuaf", ("id", "userid", "user_id"))
        if not id_column:
            return list(result.values())
        login_column = self._first_existing_column(
            conn, "kuaf", ("username", "loginname", "login", "user_name", "name")
        )
        email_column = self._first_existing_column(
            conn, "kuaf", ("mailaddress", "email", "emailaddress", "mail")
        )
        display_column = self._first_existing_column(
            conn, "kuaf", ("displayname", "fullname", "full_name", "nameformatted")
        )
        first_name_column = self._first_existing_column(conn, "kuaf", ("firstname", "first_name"))
        last_name_column = self._first_existing_column(conn, "kuaf", ("lastname", "last_name"))
        deleted_column = self._first_existing_column(conn, "kuaf", ("deleted",))
        disabled_column = self._first_existing_column(conn, "kuaf", ("disabled", "isdisabled"))
        active_column = self._first_existing_column(conn, "kuaf", ("active", "isactive", "enabled"))
        status_column = self._first_existing_column(
            conn, "kuaf", ("userstatus", "status", "accountstatus")
        )
        type_column = self._first_existing_column(conn, "kuaf", ("type", "user_type"))

        def expression(column: str | None) -> str:
            return f"k.{column}" if column else "NULL::text"

        display_expression = expression(display_column)
        if not display_column and (first_name_column or last_name_column):
            display_expression = (
                "NULLIF(BTRIM(CONCAT_WS(' ', "
                f"{expression(first_name_column)}, {expression(last_name_column)})), '')"
            )

        lookup_ids = sorted({self._kuaf_lookup_id(owner_id) for owner_id in owner_ids})
        query = f"""
            SELECT k.{id_column} kuaf_id,
                   {expression(login_column)} login_value,
                   {expression(email_column)} email_value,
                   {display_expression} display_value,
                   {expression(deleted_column)} deleted_value,
                   {expression(disabled_column)} disabled_value,
                   {expression(active_column)} active_value,
                   {expression(status_column)} status_value,
                   {expression(type_column)} type_value
              FROM {self._schema}.KUAF k
             WHERE k.{id_column}=ANY(%s)
             ORDER BY k.{id_column}
        """
        with self._cursor(conn) as cur:
            cur.execute(query, (lookup_ids,))
            rows = [dict(row) for row in cur.fetchall()]
        rows_by_kuaf_id: dict[int, list[dict[str, Any]]] = {}
        for row in rows:
            rows_by_kuaf_id.setdefault(int(row["kuaf_id"]), []).append(row)
        for owner_id in owner_ids:
            matches = rows_by_kuaf_id.get(self._kuaf_lookup_id(owner_id), [])
            if not matches:
               continue
            if len(matches) > 1:
               result[owner_id].update({
                   "identity_status": "AMBIGUOUS",
                   "active": None,
                   "status_value": None,
                   "status_source": None,
               })
               continue
            row = matches[0]
            active, status_source = self._owner_active_state(row)
            identity_status = "KNOWN"
            if active is None:
                identity_status = "STATUS_UNKNOWN"
            elif not row.get("login_value") and not row.get("email_value"):
                identity_status = "UNRESOLVED"
            result[owner_id] = {
                "source_owner_id": owner_id,
                "login": self._clean_text(row.get("login_value")),
                "email": self._clean_text(row.get("email_value")),
                "display_name": self._clean_text(row.get("display_value")),
                "active": active,
                "status_value": self._clean_text(row.get("status_value")),
                "status_source": status_source,
                "source_type": self._clean_text(row.get("type_value")),
                "identity_status": identity_status,
            }
        return [result[owner_id] for owner_id in owner_ids]

    @staticmethod
    def _kuaf_lookup_id(owner_id: int) -> int:
        # Content Server encodes user owners as negative DTree IDs while KUAF IDs are positive.
        return -owner_id if owner_id < 0 else owner_id

    @staticmethod
    def _clean_text(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _owner_active_state(row: dict[str, Any]) -> tuple[bool | None, str | None]:
        deleted = row.get("deleted_value")
        if deleted is not None:
            parsed = _parse_bool(deleted)
            return (not parsed if parsed is not None else None), "deleted"
        disabled = row.get("disabled_value")
        if disabled is not None:
            parsed = _parse_bool(disabled)
            return (not parsed if parsed is not None else None), "disabled"
        active = row.get("active_value")
        if active is not None:
            return _parse_bool(active), "active"
        status = row.get("status_value")
        if status is not None:
            text = str(status).strip().casefold()
            if text in {"active", "enabled", "1", "true", "open"}:
                return True, "status"
            if text in {"inactive", "disabled", "deleted", "0", "false", "closed"}:
                return False, "status"
        return None, "status"

    def _column_exists(self, conn, table: str, column: str) -> bool:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT 1 FROM information_schema.columns
                   WHERE table_schema=%s AND lower(table_name)=%s AND lower(column_name)=%s""",
                (self._schema, table.lower(), column.lower()),
            )
            return cur.fetchone() is not None

    def _first_existing_column(self, conn, table: str, candidates: tuple[str, ...]) -> str | None:
        for candidate in candidates:
            if self._column_exists(conn, table, candidate):
                return candidate
        return None

    # Compatibility methods for scripts that still call the old interface.
    def extract_workspace_nodes(self, root_node_id: int) -> list[dict[str, Any]]:
        return self.extract_all(root_node_id)["nodes"]

    def extract_node_versions(self, node_ids: list[int]) -> list[dict[str, Any]]:
        with self.snapshot() as (conn, _):
            return self._extract_versions(conn, node_ids)

    def extract_node_categories(self, node_ids: list[int]) -> list[dict[str, Any]]:
        with self.snapshot() as (conn, _):
            return self._extract_categories(conn, node_ids)


def _parse_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if value == 1:
            return True
        if value == 0:
            return False
    text = str(value).strip().casefold()
    if text in {"1", "true", "t", "yes", "y", "active", "enabled", "open"}:
        return True
    if text in {"0", "false", "f", "no", "n", "inactive", "disabled", "deleted", "closed"}:
        return False
    return None
