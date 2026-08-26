"""Offline and corporate-network qualification checks."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from .client import OpenTextCloudClient
from .db import SourceDB
from .manifest import ManifestStore
from .models import SourceVersion
from .source import build_binary_source


@dataclass(frozen=True)
class Check:
    id: str
    status: str
    detail: str


class PreflightAuditor:
    def __init__(self, env_cfg: Any, manifest: ManifestStore, settings: dict[str, Any]):
        self.env = env_cfg
        self.manifest = manifest
        self.settings = settings

    def run(
        self, *, online: bool = False, sample_blobs: int = 0, for_mode: str | None = None,
    ) -> dict[str, Any]:
        checks: list[Check] = []
        self._offline_checks(checks, for_mode)
        if online:
            self._online_checks(checks, sample_blobs)
        failures = sum(check.status == "FAIL" for check in checks)
        warnings = sum(check.status == "WARN" for check in checks)
        return {
            "status": "FAIL" if failures else "PASS_WITH_WARNINGS" if warnings else "PASS",
            "online": online,
            "failure_count": failures,
            "warning_count": warnings,
            "checks": [asdict(check) for check in checks],
        }

    def _offline_checks(self, checks: list[Check], for_mode: str | None) -> None:
        summary = self.manifest.inventory_summary()
        metadata = self.manifest.metadata()
        checks.append(Check(
            "MANIFEST_NOT_EMPTY", "PASS" if summary["total_nodes"] else "FAIL",
            f"nodes={summary['total_nodes']}, versions={summary['total_versions']}, bytes={summary['total_bytes']}",
        ))
        expected_root = str(self.env.get("source_workspace_nodeid") or "")
        bound_root = metadata.get("inventory_source_root_id", "")
        bound_profile = metadata.get("inventory_source_profile_id", "")
        checks.append(Check(
            "MANIFEST_SOURCE_BINDING",
            "PASS" if bound_root == expected_root and bound_profile == getattr(self.env, "key", "") else "FAIL",
            f"manifest_profile={bound_profile!r}, active_profile={getattr(self.env, 'key', '')!r}, "
            f"manifest_root={bound_root!r}, configured_root={expected_root!r}; re-extract after changing scope.",
        ))
        checks.append(Check(
            "TLS_VERIFICATION", "PASS" if self.env.get("verify_ssl", True) else "FAIL",
            "TLS certificate verification must be enabled for every live API connection.",
        ))
        target_root = int(self.env.get("target_workspace_nodeid") or 0)
        checks.append(Check(
            "TARGET_ROOT_CONFIGURED", "PASS" if target_root > 0 else "FAIL",
            f"configured_target_root={target_root}; zero and negative IDs are forbidden.",
        ))
        permission_strategy = self.env.get("permission_strategy")
        checks.append(Check(
            "PERMISSION_STRATEGY",
            "PASS" if permission_strategy in ("inherit_target", "mapped_acl") else "FAIL",
            f"configured={permission_strategy!r}; choose inherit_target or mapped_acl and approve it.",
        ))
        system_strategy = self.env.get("system_attribute_strategy")
        checks.append(Check(
            "SYSTEM_ATTRIBUTE_STRATEGY",
            "PASS" if system_strategy in ("preserve", "accept_target_generated") else "FAIL",
            f"configured={system_strategy!r}; timestamp/owner behavior requires explicit approval.",
        ))
        marker_ready = bool(self.env.get("migration_category_id") and self.env.get("migration_attribute_key"))
        marker_required = for_mode in ("pilot", "full")
        checks.append(Check(
            "IDEMPOTENCY_MARKER",
            "PASS" if marker_ready else "FAIL" if marker_required else "WARN",
            "A target indexed text attribute is required before Pilot or Full Cutover to prevent duplicate creates.",
        ))
        parity = self.manifest.parity_report(
            self.env, include_qualification=for_mode not in ("pilot",)
        )
        parity_failures = [item["id"] for item in parity["checks"] if item["status"] != "PASS"]
        checks.append(Check(
            "FUNCTIONAL_PARITY",
            "PASS" if parity["status"] == "PASS" else "FAIL",
            f"migration_readiness_failures={parity_failures}",
        ))
        classification = str(self.env.get("environment_class", "")).lower()
        is_production = classification == "production" or (
            not classification and getattr(self.env, "key", "") == "prod"
        )
        if is_production:
            freeze = self.manifest.freeze_status()
            checks.append(Check(
                "SOURCE_READ_ONLY_FREEZE",
                "PASS" if freeze["confirmed"] else "FAIL",
                f"confirmed={freeze['confirmed']}, at={freeze.get('confirmed_at')}, operator={freeze.get('operator')}",
            ))
        with self.manifest.connection() as conn:
            missing_locator = conn.execute(
                "SELECT COUNT(*) FROM manifest_versions WHERE blob_locator IS NULL OR blob_locator=''"
            ).fetchone()[0]
            max_size = conn.execute("SELECT COALESCE(MAX(data_size),0) FROM manifest_versions").fetchone()[0]
            heavy = conn.execute(
                "SELECT COUNT(*) FROM manifest_versions WHERE data_size>=?",
                (int(self.settings.get("multipart_threshold_bytes", 50 * 1024 * 1024)),),
            ).fetchone()[0]
            missing_category_map = conn.execute(
                """SELECT COUNT(*) FROM manifest_categories
                   WHERE target_category_id IS NULL OR target_attr_key IS NULL OR target_attr_key=''"""
            ).fetchone()[0]
            unsupported = conn.execute(
                "SELECT COUNT(*) FROM manifest_nodes WHERE subtype NOT IN (0,1,136,140,144,154,202,298,751,848)"
            ).fetchone()[0]
            workspaces = [dict(row) for row in conn.execute(
                "SELECT source_id,type_name,depth FROM manifest_nodes WHERE subtype=848"
            )]
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
        checks.append(Check(
            "BLOB_LOCATORS", "PASS" if missing_locator == 0 else "FAIL",
            f"missing={missing_locator}; every version needs a deterministic Azure/file locator.",
        ))
        checks.append(Check(
            "MULTIPART_SCOPE", "WARN" if heavy else "PASS",
            f"heavy_versions={heavy}, maximum_bytes={max_size}; online tenant check is mandatory when heavy_versions>0.",
        ))
        checks.append(Check(
            "CATEGORY_MAPPING", "PASS" if missing_category_map == 0 else "FAIL",
            f"unmapped_attributes={missing_category_map}",
        ))
        checks.append(Check(
            "SUPPORTED_SUBTYPES", "PASS" if unsupported == 0 else "FAIL",
            f"unsupported_nodes={unsupported}; unsupported objects are never silently converted.",
        ))
        routes = self.env.get("workspace_routes", {}) or {}
        unmapped_workspaces = [
            row["source_id"] for row in workspaces
            if not (row["depth"] == 0 and self.env.get("source_root_maps_to_target", False))
            and not (routes.get(str(row["source_id"])) or routes.get(row["type_name"]))
        ]
        checks.append(Check(
            "WORKSPACE_ROUTES", "PASS" if not unmapped_workspaces else "FAIL",
            f"unmapped_business_workspaces={unmapped_workspaces[:20]}",
        ))
        if permission_strategy == "mapped_acl":
            permission_mappings = self.env.get("permission_mappings", {}) or {}
            missing_permissions = sorted(permission_ids - set(permission_mappings))
            checks.append(Check(
                "ACL_MAPPING_COVERAGE", "PASS" if not missing_permissions else "FAIL",
                f"unmapped_source_permission_ids={missing_permissions[:20]}",
            ))
        else:
            checks.append(Check(
                "ACL_MAPPING_COVERAGE", "WARN",
                "Target inheritance selected; the target root ACL must be independently approved.",
            ))
        if system_strategy == "preserve":
            owner_mappings = self.env.get("owner_mappings", {}) or {}
            missing_owners = sorted(owner_ids - set(owner_mappings))
            checks.append(Check(
                "OWNER_MAPPING_COVERAGE", "PASS" if not missing_owners else "FAIL",
                f"unmapped_source_owner_ids={missing_owners[:20]}",
            ))

    def _online_checks(self, checks: list[Check], sample_blobs: int) -> None:
        db_status = SourceDB(self.env).test_connection()
        checks.append(Check(
            "SOURCE_DB_ONLINE", "PASS" if db_status.get("status") == "connected" else "FAIL",
            _safe_detail(db_status),
        ))
        try:
            target = OpenTextCloudClient(self.env, int(self.settings.get("max_retries", 5)))
            target_status = target.test_connection()
            checks.append(Check(
                "TARGET_AUTH", "PASS" if target_status.get("status") == "connected" else "FAIL",
                _safe_detail(target_status),
            ))
            root_id = int(self.env.get("target_workspace_nodeid"))
            props = target.get_node(root_id)
            checks.append(Check(
                "TARGET_ROOT", "PASS" if int(props.get("id", -1)) == root_id else "FAIL",
                f"id={props.get('id')}, name={props.get('name')}, type={props.get('type')}",
            ))
            with self.manifest.connection() as conn:
                heavy = conn.execute(
                    "SELECT COUNT(*) FROM manifest_versions WHERE data_size>=?",
                    (int(self.settings.get("multipart_threshold_bytes", 50 * 1024 * 1024)),),
                ).fetchone()[0]
            if heavy:
                multipart = target.get_multipart_settings()
                checks.append(Check(
                    "TARGET_MULTIPART", "PASS" if multipart.get("is_enabled") else "FAIL",
                    f"settings={multipart}",
                ))
        except Exception as exc:
            checks.append(Check("TARGET_API", "FAIL", f"{type(exc).__name__}: {exc}"))
        if sample_blobs:
            try:
                source = build_binary_source(self.env)
                with self.manifest.connection() as conn:
                    rows = conn.execute(
                        "SELECT * FROM manifest_versions ORDER BY data_size DESC LIMIT ?", (sample_blobs,)
                    ).fetchall()
                for row in rows:
                    version = SourceVersion(
                        source_id=row["doc_source_id"], version_num=row["version_num"],
                        file_name=row["file_name"], mime_type=row["mime_type"], size=row["data_size"],
                        provider_id=row["provider_id"], provider_data=row["provider_data"],
                        blob_locator=row["blob_locator"], source_sha256=row["source_sha256"],
                    )
                    source.validate(version)
                checks.append(Check("SOURCE_BLOB_SAMPLES", "PASS", f"validated={len(rows)}"))
            except Exception as exc:
                checks.append(Check("SOURCE_BLOB_SAMPLES", "FAIL", f"{type(exc).__name__}: {exc}"))


def _safe_detail(value: dict[str, Any]) -> str:
    sanitized = {key: val for key, val in value.items() if key.lower() not in {"ticket", "password", "token"}}
    return str(sanitized)[:1000]
