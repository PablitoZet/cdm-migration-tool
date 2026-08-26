"""Consistent read-only PostgreSQL extraction for Content Server inventory."""

from __future__ import annotations

import logging
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


class SourceDB:
    def __init__(self, db_config: Any):
        get = db_config.get
        self.config = db_config
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
                    cur.execute("SELECT COUNT(*) FROM public.DTree")
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
                return {"nodes": [], "versions": [], "categories": [], "snapshot": snapshot_id}
            node_ids = [row["source_id"] for row in nodes]
            versions = self._extract_versions(conn, node_ids)
            categories = self._extract_categories(conn, node_ids)
        return {
            "nodes": nodes, "versions": versions, "categories": categories,
            "snapshot": snapshot_id, "extracted_at": datetime.now(UTC).isoformat(),
            "source_signature": source_signature(nodes, versions, categories),
        }

    def inspect_scope(self, root_node_id: int) -> dict[str, Any]:
        """Read-only preview used before committing a manifest extraction."""
        extracted = self.extract_all(root_node_id)
        summary = discovery_summary(
            extracted["nodes"], extracted["versions"], extracted["categories"]
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
                  FROM public.DTree d WHERE d.DataID=%s AND COALESCE(d.Deleted,0)=0
                UNION ALL
                SELECT c.DataID,c.ParentID,c.Name,c.SubType,c.CreateDate,c.ModifyDate,
                       c.OwnerID,c.PermID,{child_reference},{child_url},
                       {child_description},{child_reserved},
                       w.depth+1,w.path_ids||c.DataID,(w.full_path||'/'||c.Name)::text
                  FROM public.DTree c JOIN workspace_tree w ON c.ParentID=w.DataID
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
        provider_data_expr = "ProviderData" if self._column_exists(conn, "dversdata", "providerdata") else "NULL::text"
        comment_column = self._first_existing_column(conn, "dversdata", ("vercomment", "comment", "description"))
        comment_expr = comment_column if comment_column else "NULL::text"
        query = f"""
            SELECT DocID doc_source_id,Version version_num,FileName file_name,MimeType mime_type,
                   DataSize data_size,ProviderID provider_id,{provider_data_expr} provider_data,
                   VerCDate ver_create_date,VerMDate ver_modify_date,FileMDate ver_file_date,
                   {comment_expr} version_comment
              FROM public.DVersData WHERE DocID=ANY(%s) ORDER BY DocID,Version
        """
        with self._cursor(conn) as cur:
            cur.execute(query, (node_ids,))
            rows = [dict(row) for row in cur.fetchall()]
        template = self.config.get("azure_blob_locator_template")
        for row in rows:
            provider_data = row.get("provider_data")
            if template:
                row["blob_locator"] = str(template).format(**row)
            elif isinstance(provider_data, str) and provider_data.startswith(("https://", "azure://", "file://")):
                row["blob_locator"] = provider_data
            else:
                row["blob_locator"] = None
        return rows

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
              FROM public.LLAttrData a LEFT JOIN public.DTree d ON a.DefID=d.DataID
             WHERE a.ID=ANY(%s) ORDER BY a.ID,a.DefID,a.AttrID,{row_expr}
        """
        with self._cursor(conn) as cur:
            cur.execute(query, (node_ids,))
            return [dict(row) for row in cur.fetchall()]

    @staticmethod
    def _column_exists(conn, table: str, column: str) -> bool:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT 1 FROM information_schema.columns
                   WHERE table_schema='public' AND lower(table_name)=%s AND lower(column_name)=%s""",
                (table.lower(), column.lower()),
            )
            return cur.fetchone() is not None

    @classmethod
    def _first_existing_column(cls, conn, table: str, candidates: tuple[str, ...]) -> str | None:
        for candidate in candidates:
            if cls._column_exists(conn, table, candidate):
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
