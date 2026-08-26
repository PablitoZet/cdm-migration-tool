"""Versioned SQLite manifest and run-scoped migration state machine."""

from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager, suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .instance_lock import InstanceLock
from .inventory import source_signature
from .models import ItemState, RunMode, RunStatus, SourceNode, SourceVersion

SCHEMA_VERSION = 3


def utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


class StateConflict(RuntimeError):
    pass


class ManifestStore:
    """Owns inventory, execution state, checkpoints and audit attempts.

    Inventory is environment-local and durable. Execution state is keyed by a
    UUID run_id, therefore simulation never mutates live migration state.
    """

    def __init__(self, db_path: str = "migration_state_v2.db"):
        self.db_path = str(Path(db_path))
        self._instance_lock = InstanceLock(self.db_path)
        self._init_db()

    def close(self) -> None:
        self._instance_lock.close()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        """Yield a read connection and always release its file descriptor."""
        conn = self._get_conn()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def transaction(self, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        conn = self._get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self.transaction(immediate=True) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS schema_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS manifest_nodes (
                    source_id INTEGER PRIMARY KEY,
                    parent_source_id INTEGER,
                    name TEXT NOT NULL,
                    subtype INTEGER NOT NULL,
                    type_name TEXT NOT NULL,
                    depth INTEGER NOT NULL,
                    path TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    source_created_at TEXT,
                    source_modified_at TEXT,
                    owner_id INTEGER,
                    group_id INTEGER,
                    permissions_id INTEGER,
                    extra_json TEXT NOT NULL DEFAULT '{}',
                    extracted_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS manifest_versions (
                    doc_source_id INTEGER NOT NULL,
                    version_num INTEGER NOT NULL,
                    file_name TEXT NOT NULL,
                    mime_type TEXT NOT NULL,
                    data_size INTEGER NOT NULL CHECK(data_size >= 0),
                    provider_id INTEGER,
                    provider_data TEXT,
                    blob_locator TEXT,
                    source_sha256 TEXT,
                    source_created_at TEXT,
                    source_modified_at TEXT,
                    source_comment TEXT,
                    PRIMARY KEY(doc_source_id, version_num),
                    FOREIGN KEY(doc_source_id) REFERENCES manifest_nodes(source_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS manifest_categories (
                    source_id INTEGER NOT NULL,
                    def_id INTEGER NOT NULL,
                    cat_name TEXT,
                    attr_key TEXT NOT NULL,
                    row_num INTEGER NOT NULL DEFAULT 0,
                    value_json TEXT NOT NULL,
                    target_category_id INTEGER,
                    target_attr_key TEXT,
                    PRIMARY KEY(source_id, def_id, attr_key, row_num),
                    FOREIGN KEY(source_id) REFERENCES manifest_nodes(source_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS migration_runs (
                    run_id TEXT PRIMARY KEY,
                    environment TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    root_node_id INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    config_fingerprint TEXT,
                    source_snapshot TEXT,
                    started_at TEXT,
                    completed_at TEXT,
                    created_at TEXT NOT NULL,
                    stop_requested INTEGER NOT NULL DEFAULT 0,
                    notes TEXT
                );

                CREATE TABLE IF NOT EXISTS run_items (
                    run_id TEXT NOT NULL,
                    source_id INTEGER NOT NULL,
                    phase TEXT NOT NULL,
                    state TEXT NOT NULL,
                    target_id INTEGER,
                    target_parent_id INTEGER,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    lease_owner TEXT,
                    lease_expires_at TEXT,
                    next_attempt_at TEXT,
                    last_error_code TEXT,
                    last_error TEXT,
                    bytes_transferred INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(run_id, source_id),
                    FOREIGN KEY(run_id) REFERENCES migration_runs(run_id) ON DELETE CASCADE,
                    FOREIGN KEY(source_id) REFERENCES manifest_nodes(source_id)
                );

                CREATE TABLE IF NOT EXISTS version_transfers (
                    run_id TEXT NOT NULL,
                    source_id INTEGER NOT NULL,
                    version_num INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    target_version_num INTEGER,
                    source_sha256 TEXT,
                    target_sha256 TEXT,
                    upload_key TEXT,
                    next_part INTEGER NOT NULL DEFAULT 1,
                    part_size INTEGER,
                    bytes_transferred INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(run_id, source_id, version_num),
                    FOREIGN KEY(run_id, source_id) REFERENCES run_items(run_id, source_id) ON DELETE CASCADE,
                    FOREIGN KEY(source_id, version_num) REFERENCES manifest_versions(doc_source_id, version_num)
                );

                CREATE TABLE IF NOT EXISTS id_mapping (
                    source_id INTEGER PRIMARY KEY,
                    target_id INTEGER NOT NULL,
                    run_id TEXT NOT NULL,
                    subtype INTEGER NOT NULL,
                    migration_id TEXT NOT NULL UNIQUE,
                    verified INTEGER NOT NULL DEFAULT 0,
                    mapped_at TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES migration_runs(run_id)
                );

                CREATE TABLE IF NOT EXISTS attempt_log (
                    attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    source_id INTEGER,
                    version_num INTEGER,
                    operation TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    http_status INTEGER,
                    correlation_id TEXT,
                    detail TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES migration_runs(run_id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_nodes_parent_depth ON manifest_nodes(parent_source_id, depth);
                CREATE INDEX IF NOT EXISTS idx_run_items_claim ON run_items(run_id, phase, state, next_attempt_at, lease_expires_at);
                CREATE INDEX IF NOT EXISTS idx_run_items_target ON run_items(target_id);
                CREATE INDEX IF NOT EXISTS idx_attempt_run_source ON attempt_log(run_id, source_id);
                """
            )
            conn.execute(
                "INSERT INTO schema_meta(key,value) VALUES('schema_version',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(SCHEMA_VERSION),),
            )
            columns = {
                str(row["name"]).lower()
                for row in conn.execute("PRAGMA table_info(manifest_versions)")
            }
            if "source_comment" not in columns:
                conn.execute("ALTER TABLE manifest_versions ADD COLUMN source_comment TEXT")
        with suppress(OSError):
            Path(self.db_path).chmod(0o600)

    def clear_inventory(self) -> None:
        with self.transaction(immediate=True) as conn:
            active = conn.execute(
                "SELECT COUNT(*) FROM migration_runs WHERE status IN (?,?,?,?)",
                (RunStatus.CREATED, RunStatus.RUNNING, RunStatus.PAUSED, RunStatus.STOPPING),
            ).fetchone()[0]
            if active:
                raise StateConflict("Cannot replace inventory while a run is active")
            conn.execute("DELETE FROM id_mapping")
            conn.execute("DELETE FROM migration_runs")
            conn.execute("DELETE FROM manifest_nodes")
            conn.execute("DELETE FROM schema_meta WHERE key<>'schema_version'")

    def import_extracted_data(
        self,
        nodes: Sequence[dict[str, Any]],
        versions: Sequence[dict[str, Any]],
        categories: Sequence[dict[str, Any]],
        *,
        snapshot: str | None = None,
        extracted_at: str | None = None,
        signature: str | None = None,
        source_root_id: int | None = None,
        source_profile_id: str | None = None,
    ) -> None:
        now = extracted_at or utcnow()
        signature = signature or source_signature(nodes, versions, categories)
        with self.transaction(immediate=True) as conn:
            for node in nodes:
                conn.execute(
                    """
                    INSERT INTO manifest_nodes(
                        source_id,parent_source_id,name,subtype,type_name,depth,path,description,
                        source_created_at,source_modified_at,owner_id,group_id,permissions_id,extra_json,extracted_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(source_id) DO UPDATE SET
                        parent_source_id=excluded.parent_source_id,name=excluded.name,subtype=excluded.subtype,
                        type_name=excluded.type_name,depth=excluded.depth,path=excluded.path,
                        description=excluded.description,source_created_at=excluded.source_created_at,
                        source_modified_at=excluded.source_modified_at,owner_id=excluded.owner_id,
                        group_id=excluded.group_id,permissions_id=excluded.permissions_id,
                        extra_json=excluded.extra_json,extracted_at=excluded.extracted_at
                    """,
                    (
                        node["source_id"], node.get("parent_source_id"), node["name"], node["subtype"],
                        node.get("type_name", f"Type_{node['subtype']}"), node.get("depth", 0),
                        node.get("path", node["name"]), node.get("description", ""),
                        _iso(node.get("create_date", node.get("source_created_at"))),
                        _iso(node.get("modify_date", node.get("source_modified_at"))), node.get("owner_id"),
                        node.get("group_id"), node.get("permissions_id"),
                        json.dumps(node.get("extra", {}), default=str, sort_keys=True), now,
                    ),
                )
            for version in versions:
                conn.execute(
                    """
                    INSERT INTO manifest_versions(
                        doc_source_id,version_num,file_name,mime_type,data_size,provider_id,provider_data,
                        blob_locator,source_sha256,source_created_at,source_modified_at,source_comment
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(doc_source_id,version_num) DO UPDATE SET
                        file_name=excluded.file_name,mime_type=excluded.mime_type,data_size=excluded.data_size,
                        provider_id=excluded.provider_id,provider_data=excluded.provider_data,
                        blob_locator=excluded.blob_locator,source_created_at=excluded.source_created_at,
                        source_modified_at=excluded.source_modified_at,source_comment=excluded.source_comment
                    """,
                    (
                        version["doc_source_id"], version["version_num"], version.get("file_name") or "content.bin",
                        version.get("mime_type") or "application/octet-stream", int(version.get("data_size") or 0),
                        version.get("provider_id"), version.get("provider_data"), version.get("blob_locator"),
                        version.get("source_sha256"),
                        _iso(version.get("ver_create_date", version.get("source_created_at"))),
                        _iso(version.get("ver_modify_date", version.get("source_modified_at"))),
                        version.get("version_comment", version.get("comment")),
                    ),
                )
            for category in categories:
                value = _category_value(category)
                attr_key = str(category.get("attr_key") or category.get("attr_id"))
                conn.execute(
                    """
                    INSERT INTO manifest_categories(
                        source_id,def_id,cat_name,attr_key,row_num,value_json,target_category_id,target_attr_key
                    ) VALUES(?,?,?,?,?,?,?,?)
                    ON CONFLICT(source_id,def_id,attr_key,row_num) DO UPDATE SET
                        cat_name=excluded.cat_name,value_json=excluded.value_json,
                        target_category_id=excluded.target_category_id,target_attr_key=excluded.target_attr_key
                    """,
                    (
                        category.get("source_id", category.get("doc_source_id")), category["def_id"],
                        category.get("cat_name"), attr_key, int(category.get("row_num") or 0),
                        json.dumps(value, default=str), category.get("target_category_id"),
                        category.get("target_attr_key"),
                    ),
                )
            metadata = {
                "inventory_signature": signature,
                "inventory_snapshot": snapshot or "",
                "inventory_extracted_at": now,
            }
            if source_root_id is not None:
                metadata["inventory_source_root_id"] = str(source_root_id)
            if source_profile_id is not None:
                metadata["inventory_source_profile_id"] = source_profile_id
            for key, value in metadata.items():
                conn.execute(
                    "INSERT INTO schema_meta(key,value) VALUES(?,?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (key, str(value)),
                )
            conn.execute("DELETE FROM schema_meta WHERE key LIKE 'freeze_%'")

    def metadata(self) -> dict[str, str]:
        with self.connection() as conn:
            return {
                str(row["key"]): str(row["value"])
                for row in conn.execute("SELECT key,value FROM schema_meta")
            }

    def confirm_source_freeze(self, observed_signature: str, operator: str, note: str = "") -> dict[str, Any]:
        operator = operator.strip()
        if not operator:
            raise StateConflict("Freeze confirmation requires an operator name")
        metadata = self.metadata()
        expected = metadata.get("inventory_signature")
        if not expected:
            raise StateConflict("Manifest has no source signature")
        if observed_signature != expected:
            raise StateConflict(
                f"Source changed since extraction: manifest={expected[:12]} observed={observed_signature[:12]}"
            )
        confirmed_at = utcnow()
        values = {
            "freeze_confirmed_signature": observed_signature,
            "freeze_confirmed_at": confirmed_at,
            "freeze_operator": operator,
            "freeze_note": note[:1000],
        }
        with self.transaction(immediate=True) as conn:
            for key, value in values.items():
                conn.execute(
                    "INSERT INTO schema_meta(key,value) VALUES(?,?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (key, value),
                )
        return self.freeze_status()

    def freeze_status(self) -> dict[str, Any]:
        metadata = self.metadata()
        inventory = metadata.get("inventory_signature")
        confirmed = metadata.get("freeze_confirmed_signature")
        return {
            "confirmed": bool(inventory and confirmed and inventory == confirmed),
            "inventory_signature": inventory,
            "confirmed_signature": confirmed,
            "confirmed_at": metadata.get("freeze_confirmed_at"),
            "operator": metadata.get("freeze_operator"),
            "note": metadata.get("freeze_note"),
        }

    def parity_report(self, environment: Any, *, include_qualification: bool = True) -> dict[str, Any]:
        """Describe whether the extracted scope can be recreated functionally 1:1."""
        with self.connection() as conn:
            total_nodes = int(conn.execute("SELECT COUNT(*) FROM manifest_nodes").fetchone()[0])
            unsupported = conn.execute(
                "SELECT subtype,COUNT(*) n FROM manifest_nodes "
                "WHERE subtype NOT IN (0,1,136,140,144,154,202,298,751,848) GROUP BY subtype"
            ).fetchall()
            unmapped_categories = conn.execute(
                "SELECT COUNT(*) FROM manifest_categories "
                "WHERE target_category_id IS NULL OR target_attr_key IS NULL OR target_attr_key=''"
            ).fetchone()[0]
            duplicate_target_keys = conn.execute(
                """SELECT COUNT(*) FROM (
                     SELECT source_id,target_category_id,target_attr_key,COUNT(*) n
                     FROM manifest_categories
                     WHERE target_category_id IS NOT NULL AND target_attr_key IS NOT NULL
                     GROUP BY source_id,target_category_id,target_attr_key HAVING COUNT(*)>1
                   )"""
            ).fetchone()[0]
            missing_references = conn.execute(
                """SELECT COUNT(*) FROM manifest_nodes
                   WHERE (subtype=1 AND json_extract(extra_json,'$.reference_source_id') IS NULL)
                      OR (subtype=140 AND COALESCE(json_extract(extra_json,'$.url'),'')='')"""
            ).fetchone()[0]
            external_references = conn.execute(
                """SELECT COUNT(*) FROM manifest_nodes ref
                   WHERE ref.subtype=1
                     AND NOT EXISTS(
                       SELECT 1 FROM manifest_nodes target
                       WHERE target.source_id=CAST(json_extract(ref.extra_json,'$.reference_source_id') AS INTEGER)
                     )"""
            ).fetchone()[0]
            active_reservations = conn.execute(
                """SELECT COUNT(*) FROM manifest_nodes
                   WHERE json_extract(extra_json,'$.reserved_by') IS NOT NULL
                     AND CAST(json_extract(extra_json,'$.reserved_by') AS TEXT) NOT IN ('0','false','')"""
            ).fetchone()[0]
            version_comments = conn.execute(
                "SELECT COUNT(*) FROM manifest_versions WHERE COALESCE(source_comment,'')<>''"
            ).fetchone()[0]
            permission_ids = {
                str(row[0]) for row in conn.execute(
                    "SELECT DISTINCT permissions_id FROM manifest_nodes WHERE permissions_id IS NOT NULL"
                )
            }
            owner_ids = {
                str(row[0]) for row in conn.execute(
                    "SELECT DISTINCT owner_id FROM manifest_nodes WHERE owner_id IS NOT NULL"
                )
            }
            workspaces = [dict(row) for row in conn.execute(
                "SELECT source_id,type_name,depth FROM manifest_nodes WHERE subtype=848"
            )]
        checks: list[dict[str, Any]] = []

        def add(check_id: str, passed: bool, detail: str, *, qualification: bool = False) -> None:
            status = "PASS" if passed else "DEFERRED" if qualification and not include_qualification else "FAIL"
            checks.append({"id": check_id, "status": status, "detail": detail})

        add("SUPPORTED_OBJECT_TYPES", not unsupported, f"unsupported={[(r['subtype'], r['n']) for r in unsupported]}")
        add("CATEGORY_SCHEMA_MAPPING", unmapped_categories == 0, f"unmapped_attributes={unmapped_categories}")
        add(
            "CATEGORY_ROW_FIDELITY", duplicate_target_keys == 0,
            f"colliding_multirow_target_keys={duplicate_target_keys}; use a target key template containing {{row}}",
        )
        add("REFERENCE_FIDELITY", missing_references == 0, f"references_without_target_or_url={missing_references}")
        add(
            "REFERENCE_SCOPE", external_references == 0,
            f"shortcuts_pointing_outside_migration_scope={external_references}",
        )
        add("ACTIVE_RESERVATIONS", active_reservations == 0, f"reserved_or_checked_out_nodes={active_reservations}")
        add(
            "VERSION_COMMENT_PARITY", version_comments == 0,
            f"version_comments_requiring_tenant-qualified migration={version_comments}",
        )

        permission_strategy = environment.get("permission_strategy")
        permission_mappings = environment.get("permission_mappings", {}) or {}
        permissions_ok = (
            permission_strategy == "mapped_acl" and permission_ids <= set(permission_mappings)
        ) or (
            permission_strategy == "inherit_target" and bool(environment.get("target_acl_approved"))
        )
        add(
            "PERMISSION_PARITY", permissions_ok,
            f"strategy={permission_strategy!r}, source_permids={len(permission_ids)}, "
            f"target_acl_approved={bool(environment.get('target_acl_approved'))}",
        )

        system_strategy = environment.get("system_attribute_strategy")
        owner_mappings = environment.get("owner_mappings", {}) or {}
        system_ok = system_strategy == "preserve" and owner_ids <= set(owner_mappings)
        add(
            "SYSTEM_ATTRIBUTE_PARITY", system_ok,
            f"strategy={system_strategy!r}, unmapped_owners={len(owner_ids - set(owner_mappings))}",
        )

        routes = environment.get("workspace_routes", {}) or {}
        missing_workspaces = [
            row["source_id"] for row in workspaces
            if not (row["depth"] == 0 and environment.get("source_root_maps_to_target", False))
            and not (routes.get(str(row["source_id"])) or routes.get(row["type_name"]))
        ]
        add("WORKSPACE_TYPE_PARITY", not missing_workspaces, f"unmapped_workspaces={missing_workspaces[:20]}")
        add(
            "WORKSPACE_ROLE_PARITY", bool(environment.get("workspace_roles_qualified")),
            "workspace role membership must be qualified in GX39 TEST",
            qualification=True,
        )
        add(
            "LIFECYCLE_PARITY", bool(environment.get("lifecycle_operations_qualified")),
            "check-out, metadata edit and new-version operations must pass GX39 TEST",
            qualification=True,
        )
        add(
            "SEARCH_PARITY", bool(environment.get("search_and_facets_qualified")),
            "category indexing, metadata search and facets must pass GX39 TEST",
            qualification=True,
        )
        add(
            "ACTIVE_WORKFLOW_SCOPE", bool(environment.get("active_workflows_confirmed_zero")),
            "scope must have zero active workflows or an explicit workflow migration plan",
        )
        add(
            "LEGACY_LINK_CONTINUITY", bool(environment.get("legacy_links_qualified")),
            "legacy on-prem URLs/bookmarks must resolve to the migrated Cloud NodeID",
            qualification=True,
        )
        add(
            "HISTORICAL_AUDIT_SCOPE", bool(environment.get("historical_audit_out_of_scope_approved")),
            "historical Content Server audit events are not recreated by this tool",
        )
        add(
            "PERSONAL_STATE_SCOPE", bool(environment.get("personal_state_out_of_scope_approved")),
            "personal favorites, recent items and notification subscriptions are outside workspace migration scope",
        )
        if total_nodes == 0:
            for check in checks:
                check["status"] = "NOT_CHECKED"
                check["detail"] = "Scan the source workspace before evaluating this item."
            return {
                "status": "NOT_CHECKED",
                "definition": "Migration completeness and operational equivalence",
                "qualification_included": include_qualification,
                "failure_count": 0,
                "checks": checks,
            }
        failures = [check for check in checks if check["status"] == "FAIL"]
        return {
            "status": "PASS" if not failures else "FAIL",
            "definition": "Migration completeness and operational equivalence",
            "qualification_included": include_qualification,
            "failure_count": len(failures),
            "checks": checks,
        }

    def apply_category_mappings(self, mappings: dict[str, Any], *, reset: bool = False) -> int:
        """Apply explicit environment-specific DefID/AttrID mappings."""
        updated = 0
        with self.transaction(immediate=True) as conn:
            if reset:
                conn.execute(
                    "UPDATE manifest_categories SET target_category_id=NULL,target_attr_key=NULL"
                )
            for source_def_id, definition in mappings.items():
                target_category_id = definition.get("target_category_id")
                for source_attr, target_attr in (definition.get("attributes") or {}).items():
                    cur = conn.execute(
                        """UPDATE manifest_categories SET target_category_id=?,target_attr_key=?
                           WHERE def_id=? AND attr_key=?""",
                        (target_category_id, str(target_attr), int(source_def_id), str(source_attr)),
                    )
                    updated += cur.rowcount
        return updated

    def create_run(
        self,
        environment: str,
        mode: RunMode | str,
        root_node_id: int,
        *,
        max_documents: int | None = None,
        config_fingerprint: str | None = None,
        source_snapshot: str | None = None,
    ) -> str:
        mode = RunMode(mode)
        run_id = str(uuid.uuid4())
        now = utcnow()
        with self.transaction(immediate=True) as conn:
            active = conn.execute(
                "SELECT run_id FROM migration_runs WHERE status IN (?,?,?,?) LIMIT 1",
                (RunStatus.CREATED, RunStatus.RUNNING, RunStatus.PAUSED, RunStatus.STOPPING),
            ).fetchone()
            if active:
                raise StateConflict(f"Another run is active: {active['run_id']}")
            total = conn.execute("SELECT COUNT(*) FROM manifest_nodes").fetchone()[0]
            if total == 0:
                raise StateConflict("Manifest is empty")
            conn.execute(
                """INSERT INTO migration_runs(
                    run_id,environment,mode,root_node_id,status,config_fingerprint,source_snapshot,created_at
                ) VALUES(?,?,?,?,?,?,?,?)""",
                (run_id, environment, mode, root_node_id, RunStatus.CREATED, config_fingerprint, source_snapshot, now),
            )
            selected_nodes: set[int] | None = None
            if max_documents:
                selected_docs = self._representative_pilot_documents(conn, max_documents)
                selected_nodes = set(selected_docs)
                selected_nodes.update(
                    int(row[0]) for row in conn.execute(
                        "SELECT source_id FROM manifest_nodes WHERE subtype IN (1,140) "
                        "ORDER BY subtype,source_id LIMIT 20"
                    )
                )
                for row in conn.execute(
                    "SELECT source_id,extra_json FROM manifest_nodes WHERE source_id IN "
                    "(SELECT source_id FROM manifest_nodes WHERE subtype=1 ORDER BY source_id LIMIT 20)"
                ):
                    referenced = json.loads(row["extra_json"]).get("reference_source_id")
                    if referenced is not None:
                        selected_nodes.add(int(referenced))
                parents = {
                    parent_row["source_id"]: parent_row["parent_source_id"]
                    for parent_row in conn.execute("SELECT source_id,parent_source_id FROM manifest_nodes")
                }
                for source_id in tuple(selected_nodes):
                    parent_id = parents.get(source_id)
                    while parent_id is not None and parent_id in parents and parent_id not in selected_nodes:
                        selected_nodes.add(parent_id)
                        parent_id = parents.get(parent_id)
            for row in conn.execute("SELECT source_id,subtype FROM manifest_nodes ORDER BY depth,source_id"):
                phase = _phase_for_subtype(row["subtype"])
                if selected_nodes is not None and row["source_id"] not in selected_nodes:
                    continue
                existing = conn.execute("SELECT target_id,verified FROM id_mapping WHERE source_id=?", (row["source_id"],)).fetchone()
                state = ItemState.VERIFIED if existing and existing["verified"] else ItemState.READY
                target_id = existing["target_id"] if existing else None
                conn.execute(
                    "INSERT INTO run_items(run_id,source_id,phase,state,target_id,updated_at) VALUES(?,?,?,?,?,?)",
                    (run_id, row["source_id"], phase, state, target_id, now),
                )
                if phase == "DOCUMENT":
                    for version in conn.execute(
                        "SELECT version_num,source_sha256 FROM manifest_versions WHERE doc_source_id=? ORDER BY version_num",
                        (row["source_id"],),
                    ):
                        conn.execute(
                            """INSERT INTO version_transfers(
                                run_id,source_id,version_num,state,source_sha256,updated_at
                            ) VALUES(?,?,?,?,?,?)""",
                            (run_id, row["source_id"], version["version_num"], state, version["source_sha256"], now),
                        )
        return run_id

    @staticmethod
    def _representative_pilot_documents(conn: sqlite3.Connection, limit: int) -> set[int]:
        """Choose a deterministic risk-stratified document pilot."""
        rows = [
            dict(row) for row in conn.execute(
                """
                SELECT n.source_id,n.subtype,n.depth,n.path,n.name,
                       COUNT(v.version_num) version_count,
                       COALESCE(MAX(v.data_size),0) max_size,
                       COALESCE(SUM(v.data_size),0) total_size,
                       COUNT(DISTINCT c.def_id) category_count,
                       MIN(v.provider_id) provider_id,
                       EXISTS(
                         SELECT 1 FROM manifest_nodes sibling
                         WHERE sibling.parent_source_id=n.parent_source_id
                           AND sibling.name=n.name AND sibling.source_id<>n.source_id
                       ) duplicate_name
                  FROM manifest_nodes n
                  LEFT JOIN manifest_versions v ON v.doc_source_id=n.source_id
                  LEFT JOIN manifest_categories c ON c.source_id=n.source_id
                 WHERE n.subtype IN (136,144,154,751)
                 GROUP BY n.source_id
                """
            )
        ]
        if not rows or limit <= 0:
            return set()
        by_id = {int(row["source_id"]): row for row in rows}
        ordered_candidates: list[int] = []

        def add(rows_to_add: Iterable[dict[str, Any]], count: int = 1) -> None:
            for row in list(rows_to_add)[:count]:
                source_id = int(row["source_id"])
                if source_id not in ordered_candidates:
                    ordered_candidates.append(source_id)

        add(sorted(rows, key=lambda row: (-int(row["max_size"]), int(row["source_id"]))), 3)
        add(sorted(rows, key=lambda row: (-int(row["version_count"]), int(row["source_id"]))), 3)
        add(sorted(rows, key=lambda row: (-int(row["depth"]), int(row["source_id"]))), 2)
        add(sorted(rows, key=lambda row: (-len(str(row["path"])), int(row["source_id"]))), 2)
        add((row for row in rows if bool(row["duplicate_name"])), 2)
        add((row for row in rows if not str(row["name"]).isascii()), 2)

        for subtype in sorted({int(row["subtype"]) for row in rows}):
            add(row for row in rows if int(row["subtype"]) == subtype)
        for provider in sorted({str(row["provider_id"]) for row in rows}):
            add(row for row in rows if str(row["provider_id"]) == provider)
        category_documents = conn.execute(
            """SELECT c.def_id,MIN(c.source_id) source_id FROM manifest_categories c
               JOIN manifest_nodes n ON n.source_id=c.source_id
               WHERE n.subtype IN (136,144,154,751) GROUP BY c.def_id ORDER BY c.def_id"""
        ).fetchall()
        for row in category_documents:
            source_id = int(row["source_id"])
            if source_id in by_id and source_id not in ordered_candidates:
                ordered_candidates.append(source_id)

        risk_order = sorted(
            rows,
            key=lambda row: (
                -int(row["category_count"] > 0),
                -int(row["version_count"]),
                -int(row["max_size"]),
                -int(row["depth"]),
                int(row["source_id"]),
            ),
        )
        add(risk_order, len(risk_order))
        return set(ordered_candidates[:limit])

    def start_run(self, run_id: str) -> None:
        self._set_run_status(run_id, RunStatus.RUNNING, allowed=(RunStatus.CREATED, RunStatus.PAUSED), started=True)

    def pause_run(self, run_id: str) -> None:
        self._set_run_status(run_id, RunStatus.PAUSED, allowed=(RunStatus.RUNNING,))

    def request_stop(self, run_id: str) -> None:
        with self.transaction(immediate=True) as conn:
            cur = conn.execute(
                "UPDATE migration_runs SET stop_requested=1,status=? WHERE run_id=? AND status IN (?,?)",
                (RunStatus.STOPPING, run_id, RunStatus.RUNNING, RunStatus.PAUSED),
            )
            if cur.rowcount != 1:
                raise StateConflict("Run is not stoppable")

    def should_stop(self, run_id: str) -> bool:
        with self.connection() as conn:
            row = conn.execute("SELECT stop_requested FROM migration_runs WHERE run_id=?", (run_id,)).fetchone()
            return bool(row and row[0])

    def run_status(self, run_id: str) -> dict[str, Any]:
        with self.connection() as conn:
            row = conn.execute("SELECT * FROM migration_runs WHERE run_id=?", (run_id,)).fetchone()
            if not row:
                raise KeyError(run_id)
            result = dict(row)
            result.update(self.run_summary(run_id, conn=conn))
            return result

    def latest_run(self) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute("SELECT run_id FROM migration_runs ORDER BY created_at DESC LIMIT 1").fetchone()
        return self.run_status(row[0]) if row else None

    def recover_run(self, run_id: str) -> None:
        """Explicit operator recovery after process loss or a controlled stop."""
        now = utcnow()
        with self.transaction(immediate=True) as conn:
            row = conn.execute("SELECT status FROM migration_runs WHERE run_id=?", (run_id,)).fetchone()
            if not row:
                raise KeyError(run_id)
            if row["status"] == RunStatus.COMPLETED:
                raise StateConflict("A completed run cannot be recovered")
            conn.execute(
                """UPDATE run_items SET state=?,lease_owner=NULL,lease_expires_at=NULL,
                   next_attempt_at=?,updated_at=? WHERE run_id=? AND state IN (?,?,?,?)""",
                (
                    ItemState.RETRY_WAIT, now, now, run_id,
                    ItemState.CLAIMED, ItemState.UPLOADING,
                    ItemState.REMOTE_COMMITTED, ItemState.METADATA_APPLIED,
                ),
            )
            conn.execute(
                "UPDATE migration_runs SET status=?,stop_requested=0,completed_at=NULL WHERE run_id=?",
                (RunStatus.CREATED, run_id),
            )

    def claim_next(
        self, run_id: str, phase: str, worker_id: str, *, lease_seconds: int = 300,
    ) -> dict[str, Any] | None:
        now = utcnow()
        expires = (datetime.now(UTC) + timedelta(seconds=lease_seconds)).isoformat(timespec="milliseconds")
        with self.transaction(immediate=True) as conn:
            row = conn.execute(
                """
                SELECT i.*,n.* FROM run_items i JOIN manifest_nodes n USING(source_id)
                WHERE i.run_id=? AND i.phase=? AND (
                    i.state=? OR
                    (i.state=? AND i.next_attempt_at<=?) OR
                    (i.state=? AND i.lease_expires_at<?)
                )
                AND (
                    NOT EXISTS(SELECT 1 FROM manifest_nodes parent WHERE parent.source_id=n.parent_source_id)
                    OR EXISTS(SELECT 1 FROM id_mapping mapping WHERE mapping.source_id=n.parent_source_id)
                    OR EXISTS(
                        SELECT 1 FROM run_items parent_item
                        WHERE parent_item.run_id=i.run_id AND parent_item.source_id=n.parent_source_id
                        AND parent_item.state=?
                    )
                )
                ORDER BY n.depth,n.source_id LIMIT 1
                """,
                (
                    run_id, phase, ItemState.READY, ItemState.RETRY_WAIT, now,
                    ItemState.CLAIMED, now, ItemState.SIMULATED,
                ),
            ).fetchone()
            if not row:
                return None
            conn.execute(
                """UPDATE run_items SET state=?,lease_owner=?,lease_expires_at=?,attempt_count=attempt_count+1,
                   updated_at=? WHERE run_id=? AND source_id=?""",
                (ItemState.CLAIMED, worker_id, expires, now, run_id, row["source_id"]),
            )
            result = dict(row)
            result.update({"state": str(ItemState.CLAIMED), "lease_owner": worker_id, "lease_expires_at": expires})
            return result

    def block_children_of_failed(self, run_id: str, phase: str) -> int:
        """Propagate a terminal parent failure without flattening descendants."""
        with self.transaction(immediate=True) as conn:
            cur = conn.execute(
                """UPDATE run_items AS child SET state=?,last_error_code='PARENT_FAILED',
                   last_error='Parent object failed terminally',updated_at=?
                   WHERE child.run_id=? AND child.phase=? AND child.state IN (?,?)
                   AND EXISTS(
                       SELECT 1 FROM manifest_nodes node
                       JOIN run_items parent ON parent.run_id=child.run_id
                           AND parent.source_id=node.parent_source_id
                       WHERE node.source_id=child.source_id AND parent.state=?
                   )""",
                (
                    ItemState.FAILED_TERMINAL, utcnow(), run_id, phase,
                    ItemState.READY, ItemState.RETRY_WAIT, ItemState.FAILED_TERMINAL,
                ),
            )
            return cur.rowcount

    def mark_state(
        self,
        run_id: str,
        source_id: int,
        state: ItemState,
        *,
        worker_id: str | None = None,
        target_id: int | None = None,
        target_parent_id: int | None = None,
        bytes_transferred: int | None = None,
    ) -> None:
        now = utcnow()
        with self.transaction(immediate=True) as conn:
            row = conn.execute(
                "SELECT lease_owner FROM run_items WHERE run_id=? AND source_id=?", (run_id, source_id)
            ).fetchone()
            if not row:
                raise KeyError((run_id, source_id))
            if worker_id and row["lease_owner"] not in (None, worker_id):
                raise StateConflict(f"Lease owned by {row['lease_owner']}")
            conn.execute(
                """UPDATE run_items SET state=?,target_id=COALESCE(?,target_id),
                   target_parent_id=COALESCE(?,target_parent_id),
                   bytes_transferred=COALESCE(?,bytes_transferred),lease_owner=NULL,lease_expires_at=NULL,
                   next_attempt_at=NULL,last_error_code=NULL,last_error=NULL,updated_at=?
                   WHERE run_id=? AND source_id=?""",
                (state, target_id, target_parent_id, bytes_transferred, now, run_id, source_id),
            )

    def commit_mapping(
        self, run_id: str, source_id: int, target_id: int, target_parent_id: int,
        migration_id: str, *, worker_id: str,
    ) -> None:
        now = utcnow()
        with self.transaction(immediate=True) as conn:
            row = conn.execute(
                "SELECT subtype,lease_owner FROM run_items JOIN manifest_nodes USING(source_id) "
                "WHERE run_id=? AND source_id=?", (run_id, source_id),
            ).fetchone()
            if not row or row["lease_owner"] != worker_id:
                raise StateConflict("Cannot commit without owning the item lease")
            conn.execute(
                """INSERT INTO id_mapping(source_id,target_id,run_id,subtype,migration_id,mapped_at)
                   VALUES(?,?,?,?,?,?) ON CONFLICT(source_id) DO UPDATE SET
                   target_id=excluded.target_id,run_id=excluded.run_id,migration_id=excluded.migration_id,
                   subtype=excluded.subtype,mapped_at=excluded.mapped_at""",
                (source_id, target_id, run_id, row["subtype"], migration_id, now),
            )
            conn.execute(
                """UPDATE run_items SET state=?,target_id=?,target_parent_id=?,updated_at=?
                   WHERE run_id=? AND source_id=?""",
                (ItemState.REMOTE_COMMITTED, target_id, target_parent_id, now, run_id, source_id),
            )

    def record_failure(
        self, run_id: str, source_id: int, error: Exception | str, *, max_attempts: int,
        error_code: str = "MIGRATION_ERROR", retryable: bool = True,
    ) -> ItemState:
        now_dt = datetime.now(UTC)
        with self.transaction(immediate=True) as conn:
            row = conn.execute(
                "SELECT attempt_count FROM run_items WHERE run_id=? AND source_id=?", (run_id, source_id)
            ).fetchone()
            if not row:
                raise KeyError((run_id, source_id))
            terminal = not retryable or row["attempt_count"] >= max_attempts
            state = ItemState.FAILED_TERMINAL if terminal else ItemState.RETRY_WAIT
            retry_at = None if terminal else (
                now_dt + timedelta(seconds=min(300, 2 ** max(1, row["attempt_count"])))
            ).isoformat(timespec="milliseconds")
            conn.execute(
                """UPDATE run_items SET state=?,next_attempt_at=?,last_error_code=?,last_error=?,
                   lease_owner=NULL,lease_expires_at=NULL,updated_at=? WHERE run_id=? AND source_id=?""",
                (state, retry_at, error_code, str(error)[:4000], now_dt.isoformat(timespec="milliseconds"), run_id, source_id),
            )
            return state

    def resolve_parent(self, source_parent_id: int | None, default_root: int) -> int:
        if source_parent_id is None:
            return default_root
        with self.connection() as conn:
            in_scope = conn.execute("SELECT 1 FROM manifest_nodes WHERE source_id=?", (source_parent_id,)).fetchone()
            mapping = conn.execute("SELECT target_id FROM id_mapping WHERE source_id=?", (source_parent_id,)).fetchone()
            if mapping:
                return int(mapping[0])
            if in_scope:
                raise StateConflict(f"Parent {source_parent_id} is in scope but not committed")
            return default_root

    def source_node(self, source_id: int) -> SourceNode:
        with self.connection() as conn:
            row = conn.execute("SELECT * FROM manifest_nodes WHERE source_id=?", (source_id,)).fetchone()
            if not row:
                raise KeyError(source_id)
            return SourceNode(
                source_id=row["source_id"], parent_source_id=row["parent_source_id"], name=row["name"],
                subtype=row["subtype"], type_name=row["type_name"], depth=row["depth"], path=row["path"],
                description=row["description"], created_at=row["source_created_at"],
                modified_at=row["source_modified_at"], owner_id=row["owner_id"], group_id=row["group_id"],
                permissions_id=row["permissions_id"], extra=json.loads(row["extra_json"]),
            )

    def source_versions(self, source_id: int) -> list[SourceVersion]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM manifest_versions WHERE doc_source_id=? ORDER BY version_num", (source_id,)
            ).fetchall()
        return [
            SourceVersion(
                source_id=row["doc_source_id"], version_num=row["version_num"], file_name=row["file_name"],
                mime_type=row["mime_type"], size=row["data_size"], provider_id=row["provider_id"],
                provider_data=row["provider_data"], blob_locator=row["blob_locator"],
                source_sha256=row["source_sha256"], created_at=row["source_created_at"],
                modified_at=row["source_modified_at"], comment=row["source_comment"],
            ) for row in rows
        ]

    def categories(self, source_id: int) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM manifest_categories WHERE source_id=? ORDER BY def_id,attr_key,row_num", (source_id,)
            ).fetchall()
        return [{**dict(row), "value": json.loads(row["value_json"])} for row in rows]

    def update_version_transfer(self, run_id: str, source_id: int, version_num: int, **values: Any) -> None:
        allowed = {
            "state", "target_version_num", "source_sha256", "target_sha256", "upload_key",
            "next_part", "part_size", "bytes_transferred", "last_error",
        }
        unknown = set(values) - allowed
        if unknown:
            raise ValueError(f"Unsupported version transfer fields: {sorted(unknown)}")
        if not values:
            return
        assignments = ",".join(f"{key}=?" for key in values)
        with self.transaction(immediate=True) as conn:
            conn.execute(
                f"UPDATE version_transfers SET {assignments},updated_at=? WHERE run_id=? AND source_id=? AND version_num=?",
                (*values.values(), utcnow(), run_id, source_id, version_num),
            )

    def version_transfer(self, run_id: str, source_id: int, version_num: int) -> dict[str, Any]:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM version_transfers WHERE run_id=? AND source_id=? AND version_num=?",
                (run_id, source_id, version_num),
            ).fetchone()
            if not row:
                raise KeyError((run_id, source_id, version_num))
            return dict(row)

    def mark_mapping_verified(self, source_id: int) -> None:
        with self.transaction(immediate=True) as conn:
            conn.execute("UPDATE id_mapping SET verified=1 WHERE source_id=?", (source_id,))

    def lookup_mapping(self, source_id: int) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                """SELECT mapping.*,node.name
                   FROM id_mapping mapping
                   LEFT JOIN manifest_nodes node ON node.source_id=mapping.source_id
                   WHERE mapping.source_id=?""",
                (source_id,),
            ).fetchone()
            return dict(row) if row else None

    def append_attempt(
        self, run_id: str, operation: str, outcome: str, *, source_id: int | None = None,
        version_num: int | None = None, http_status: int | None = None,
        correlation_id: str | None = None, detail: str | None = None,
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO attempt_log(run_id,source_id,version_num,operation,outcome,http_status,
                   correlation_id,detail,created_at) VALUES(?,?,?,?,?,?,?,?,?)""",
                (run_id, source_id, version_num, operation, outcome, http_status, correlation_id,
                detail[:4000] if detail else None, utcnow()),
            )

    def attempt_metrics(self, run_id: str) -> dict[str, Any]:
        with self.connection() as conn:
            row = conn.execute(
                """SELECT COUNT(*) request_count,
                          COALESCE(SUM(http_status=429),0) throttled_count,
                          COALESCE(SUM(http_status BETWEEN 500 AND 599),0) server_error_count,
                          COALESCE(SUM(outcome='NETWORK_ERROR'),0) network_error_count
                     FROM attempt_log WHERE run_id=?""",
                (run_id,),
            ).fetchone()
            last = conn.execute(
                """SELECT operation,outcome,http_status,correlation_id,created_at
                     FROM attempt_log WHERE run_id=? ORDER BY attempt_id DESC LIMIT 1""",
                (run_id,),
            ).fetchone()
        return {**dict(row), "last_request": dict(last) if last else None}

    def run_history(self, limit: int = 50) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM migration_runs ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                item.update(self.run_summary(item["run_id"], conn=conn))
                result.append(item)
            return result

    def run_summary(self, run_id: str, *, conn: sqlite3.Connection | None = None) -> dict[str, Any]:
        owns = conn is None
        conn = conn or self._get_conn()
        try:
            rows = conn.execute(
                "SELECT state,COUNT(*) n,COALESCE(SUM(bytes_transferred),0) b FROM run_items WHERE run_id=? GROUP BY state",
                (run_id,),
            ).fetchall()
            counts = {row["state"]: row["n"] for row in rows}
            total = sum(counts.values())
            terminal = sum(counts.get(str(s), 0) for s in (
                ItemState.VERIFIED, ItemState.SIMULATED, ItemState.SKIPPED, ItemState.FAILED_TERMINAL
            ))
            return {
                "total_nodes": total,
                "state_counts": counts,
                "completed_nodes": terminal,
                "failed_nodes": counts.get(ItemState.FAILED_TERMINAL, 0),
                "verified_nodes": counts.get(ItemState.VERIFIED, 0),
                "simulated_nodes": counts.get(ItemState.SIMULATED, 0),
                "transferred_bytes": sum(row["b"] for row in rows),
                "progress_percent": round(100 * terminal / total, 2) if total else 0.0,
            }
        finally:
            if owns:
                conn.close()

    def phase_counts(self, run_id: str, phase: str) -> dict[str, int]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT state,COUNT(*) n FROM run_items WHERE run_id=? AND phase=? GROUP BY state",
                (run_id, phase),
            ).fetchall()
            return {row["state"]: row["n"] for row in rows}

    def inventory_summary(self) -> dict[str, Any]:
        with self.connection() as conn:
            row = conn.execute(
                """SELECT COUNT(*) total_nodes,
                   COALESCE(SUM(subtype IN (0,202,298,848,899)),0) total_containers,
                   COALESCE(SUM(subtype IN (136,144,154,751)),0) total_docs FROM manifest_nodes"""
            ).fetchone()
            versions = conn.execute(
                "SELECT COUNT(*) total_versions,COALESCE(SUM(data_size),0) total_bytes FROM manifest_versions"
            ).fetchone()
            return {**dict(row), **dict(versions)}

    def list_run_items(self, run_id: str, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                """SELECT i.*,n.name,n.path,n.subtype,n.type_name,n.depth FROM run_items i
                   JOIN manifest_nodes n USING(source_id) WHERE i.run_id=?
                   ORDER BY n.depth,n.source_id LIMIT ? OFFSET ?""", (run_id, limit, offset),
            ).fetchall()
            return [dict(row) for row in rows]

    def verification_items(self, run_id: str) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                """SELECT i.*,n.name,n.path,n.subtype,n.depth,n.parent_source_id,n.description,
                   n.source_created_at,n.source_modified_at,n.owner_id,m.target_id mapped_target_id,
                   pm.target_id mapped_parent_id FROM run_items i
                   JOIN manifest_nodes n USING(source_id)
                   LEFT JOIN id_mapping m ON m.source_id=i.source_id
                   LEFT JOIN id_mapping pm ON pm.source_id=n.parent_source_id
                   WHERE i.run_id=? ORDER BY n.depth,n.source_id""", (run_id,),
            ).fetchall()
            return [dict(row) for row in rows]

    def verification_versions(self, run_id: str) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                """SELECT t.*,v.file_name,v.mime_type,v.data_size,i.target_id
                   FROM version_transfers t
                   JOIN manifest_versions v ON v.doc_source_id=t.source_id AND v.version_num=t.version_num
                   JOIN run_items i ON i.run_id=t.run_id AND i.source_id=t.source_id
                   WHERE t.run_id=? ORDER BY t.source_id,t.version_num""", (run_id,),
            ).fetchall()
            return [dict(row) for row in rows]

    def finish_run(self, run_id: str, status: RunStatus | None = None) -> RunStatus:
        summary = self.run_summary(run_id)
        if status is None:
            if self.should_stop(run_id):
                status = RunStatus.STOPPED
            elif summary["failed_nodes"]:
                status = RunStatus.COMPLETED_WITH_ERRORS
            else:
                status = RunStatus.COMPLETED
        with self.transaction(immediate=True) as conn:
            conn.execute(
                "UPDATE migration_runs SET status=?,completed_at=? WHERE run_id=?",
                (status, utcnow(), run_id),
            )
        return status

    def backup(self, target_path: str) -> None:
        source = self._get_conn()
        target = sqlite3.connect(target_path)
        try:
            source.backup(target)
        finally:
            target.close()
            source.close()
        with suppress(OSError):
            Path(target_path).chmod(0o600)

    def _set_run_status(
        self, run_id: str, status: RunStatus, *, allowed: Iterable[RunStatus], started: bool = False,
    ) -> None:
        with self.transaction(immediate=True) as conn:
            placeholders = ",".join("?" for _ in allowed)
            args: list[Any] = [status]
            started_sql = ",started_at=COALESCE(started_at,?)" if started else ""
            if started:
                args.append(utcnow())
            args.extend([run_id, *allowed])
            cur = conn.execute(
                f"UPDATE migration_runs SET status=?{started_sql} WHERE run_id=? AND status IN ({placeholders})", args,
            )
            if cur.rowcount != 1:
                raise StateConflict(f"Invalid run transition to {status}")


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _category_value(row: dict[str, Any]) -> Any:
    for key in ("value", "val_str", "val_long", "val_int", "val_real", "val_date"):
        if row.get(key) is not None:
            return _iso(row[key])
    return None


def _phase_for_subtype(subtype: int) -> str:
    if subtype in (0, 202, 298, 848, 899):
        return "CONTAINER"
    if subtype in (136, 144, 154, 751):
        return "DOCUMENT"
    return "REFERENCE"
