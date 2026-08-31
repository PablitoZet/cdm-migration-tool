"""Offline and corporate-network qualification checks."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from .client import OpenTextCloudClient
from .db import SourceDB
from .manifest import ManifestStore
from .models import SourceVersion
from .provenance import (
    NODE_PROVENANCE_FIELDS,
    VERSION_PROVENANCE_FIELDS,
    OwnerResolution,
    exception_set_digest,
)
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
        max_documents: int | None = None,
        owner_resolutions: list[OwnerResolution] | None = None,
        fallback_approval: dict[str, Any] | None = None,
        service_identity: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        checks: list[Check] = []
        self._offline_checks(
            checks, for_mode, max_documents, owner_resolutions, fallback_approval, service_identity,
        )
        if online:
            self._online_checks(
                checks, sample_blobs, max_documents, owner_resolutions, fallback_approval, service_identity,
            )
        failures = sum(check.status == "FAIL" for check in checks)
        warnings = sum(check.status == "WARN" for check in checks)
        return {
            "status": "FAIL" if failures else "PASS_WITH_WARNINGS" if warnings else "PASS",
            "online": online,
            "failure_count": failures,
            "warning_count": warnings,
            "checks": [asdict(check) for check in checks],
        }

    def _offline_checks(
        self, checks: list[Check], for_mode: str | None,
        max_documents: int | None,
        owner_resolutions: list[OwnerResolution] | None,
        fallback_approval: dict[str, Any] | None,
        service_identity: dict[str, Any] | None,
    ) -> None:
        summary = self.manifest.inventory_summary(max_documents)
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
        marker_ready = bool(self.env.get("migration_category_id") and self.env.get("migration_attribute_key"))
        marker_required = for_mode in ("pilot", "full")
        checks.append(Check(
            "IDEMPOTENCY_MARKER",
            "PASS" if marker_ready else "FAIL" if marker_required else "WARN",
            "A target indexed text attribute is required before Pilot or Full Cutover to prevent duplicate creates.",
        ))
        parity = self.manifest.parity_report(
            self.env,
            include_qualification=for_mode not in ("pilot", "dry_run"),
            max_documents=max_documents,
        )
        parity_failures = [item["id"] for item in parity["checks"] if item["status"] == "FAIL"]
        if for_mode == "dry_run":
            dry_run_parity_checks = {
                "SUPPORTED_OBJECT_TYPES",
                "CATEGORY_SCHEMA_MAPPING",
                "CATEGORY_ROW_FIDELITY",
                "REFERENCE_FIDELITY",
                "REFERENCE_SCOPE",
                "ACTIVE_RESERVATIONS",
                "VERSION_COMMENT_PARITY",
                "WORKSPACE_TYPE_PARITY",
            }
            parity_failures = [
                check_id for check_id in parity_failures
                if check_id in dry_run_parity_checks
            ]
        checks.append(Check(
            "FUNCTIONAL_PARITY",
            "PASS" if not parity_failures else "FAIL",
            f"migration_readiness_failures={parity_failures}",
        ))
        classification = str(self.env.get("environment_class", "")).lower()
        is_production = classification == "production" or (
            not classification and getattr(self.env, "key", "") == "prod"
        )
        if is_production and for_mode == "full":
            freeze = self.manifest.freeze_status()
            checks.append(Check(
                "SOURCE_READ_ONLY_FREEZE",
                "PASS" if freeze["confirmed"] else "FAIL",
                f"confirmed={freeze['confirmed']}, at={freeze.get('confirmed_at')}, operator={freeze.get('operator')}",
            ))
        with self.manifest.connection() as conn:
            scope_ids = self.manifest.source_ids_for_scope(max_documents)
            node_scope, node_params = self.manifest._scope_clause(scope_ids, "source_id")
            version_scope, version_params = self.manifest._scope_clause(scope_ids, "doc_source_id")
            category_scope, category_params = self.manifest._scope_clause(scope_ids, "source_id")
            missing_locator = conn.execute(
                "SELECT COUNT(*) FROM manifest_versions "
                f"WHERE (blob_locator IS NULL OR blob_locator=''){version_scope}",
                version_params,
            ).fetchone()[0]
            max_size = conn.execute(
                f"SELECT COALESCE(MAX(data_size),0) FROM manifest_versions WHERE 1=1{version_scope}",
                version_params,
            ).fetchone()[0]
            heavy = conn.execute(
                f"SELECT COUNT(*) FROM manifest_versions WHERE data_size>=?{version_scope}",
                (int(self.settings.get("multipart_threshold_bytes", 50 * 1024 * 1024)),) + version_params,
            ).fetchone()[0]
            missing_category_map = conn.execute(
                """SELECT COUNT(*) FROM manifest_categories
                   WHERE (target_category_id IS NULL OR target_attr_key IS NULL OR target_attr_key='')"""
                + category_scope,
                category_params,
            ).fetchone()[0]
            unsupported = conn.execute(
                "SELECT COUNT(*) FROM manifest_nodes "
                f"WHERE subtype NOT IN (0,1,136,140,144,154,202,298,751,848){node_scope}",
                node_params,
            ).fetchone()[0]
            workspaces = [dict(row) for row in conn.execute(
                f"SELECT source_id,type_name,depth FROM manifest_nodes WHERE subtype=848{node_scope}",
                node_params,
            )]
            permission_ids = {
                str(row[0]) for row in conn.execute(
                    f"SELECT DISTINCT permissions_id FROM manifest_nodes "
                    f"WHERE permissions_id IS NOT NULL{node_scope}", node_params
                )
            }
            owner_ids = {
                str(row[0]) for row in conn.execute(
                    f"SELECT DISTINCT owner_id FROM manifest_nodes "
                    f"WHERE owner_id IS NOT NULL{node_scope}", node_params
                )
            }
            missing_owner_nodes = conn.execute(
                f"SELECT COUNT(*) FROM manifest_nodes "
                f"WHERE owner_id IS NULL{node_scope}", node_params
            ).fetchone()[0]
        adapter = str(self.env.get("binary_source_adapter", "azure")).lower()
        if adapter == "content_server":
            rest_missing = [
                key for key in ("source_cs_url", "source_cs_user", "source_cs_password")
                if not self.env.get(key)
            ]
            checks.append(Check(
                "BLOB_LOCATORS", "PASS" if not rest_missing else "FAIL",
                "Source Content Server REST resolves binaries by DataID/version; "
                f"missing_configuration={rest_missing}",
            ))
        else:
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
        checks.append(Check(
            "OWNER_RESOLUTION_POLICY",
            "PASS",
            "Owned By is resolved once per distinct source owner from exact KUAF identity evidence.",
        ))
        manifest_owners = {
            str(owner.source_owner_id): owner for owner in self.manifest.owner_identities()
        }
        missing_identity_rows = sorted(owner_ids - set(manifest_owners))
        incomplete_identities = sorted(
            source_id for source_id, owner in manifest_owners.items()
            if source_id in owner_ids and (
                owner.identity_status in {"UNKNOWN", "STATUS_UNKNOWN", "AMBIGUOUS", "UNRESOLVED"}
                or owner.active is None
            )
        )
        identity_failures = missing_identity_rows + incomplete_identities
        checks.append(Check(
            "OWNER_IDENTITY_COVERAGE",
            "PASS" if not identity_failures and not missing_owner_nodes else "FAIL",
            f"missing_or_incomplete_source_owner_ids={identity_failures[:20]}; "
            f"nodes_without_owner_id={missing_owner_nodes}; "
            "KUAF identity and activation status must be captured read-only.",
        ))
        provenance_category = self.env.get("provenance_category_id")
        provenance_keys = self.env.get("provenance_attribute_keys", {}) or {}
        version_keys = self.env.get("provenance_version_attribute_keys", {}) or {}
        missing_provenance = [
            field for field in NODE_PROVENANCE_FIELDS if not provenance_keys.get(field)
        ]
        missing_version_provenance = [
            field for field in VERSION_PROVENANCE_FIELDS if not version_keys.get(field)
        ]
        provenance_configured = bool(
            provenance_category and not missing_provenance and (
                not summary["total_versions"] or not missing_version_provenance
            )
        )
        provenance_status = "PASS" if provenance_configured else (
            "WARN" if for_mode == "dry_run" else "FAIL"
        )
        checks.append(Check(
            "PROVENANCE_CONFIGURATION", provenance_status,
            f"category_id={'configured' if provenance_category else 'missing'}, "
            f"missing_node_fields={missing_provenance}, missing_version_fields={missing_version_provenance}",
        ))
        if for_mode in ("pilot", "full"):
            expected_resolution_ids = {int(owner_id) for owner_id in owner_ids}
            resolved_ids = {
                resolution.source_owner_id for resolution in owner_resolutions or ()
            }
            resolution_failures = sorted(expected_resolution_ids - resolved_ids)
            if owner_resolutions is not None:
                resolution_failures += [
                    resolution.source_owner_id for resolution in owner_resolutions
                    if resolution.resolution_status not in {"EXACT", "APPROVED_FALLBACK"}
                    or resolution.target_member_id is None
                ]
            checks.append(Check(
                "OWNER_RESOLUTION",
                "PASS" if not resolution_failures else "FAIL",
                f"missing_or_invalid_source_owner_ids={sorted(set(resolution_failures))[:20]}",
            ))
            exception_rows = [
                resolution for resolution in owner_resolutions or ()
                if resolution.resolution_status == "APPROVED_FALLBACK"
            ]
            approval_ok = bool(
                not exception_rows or (
                    fallback_approval
                    and fallback_approval.get("exception_set_digest") == exception_set_digest(exception_rows)
                    and fallback_approval.get("operator")
                    and fallback_approval.get("change_record")
                )
            )
            checks.append(Check(
                "OWNER_FALLBACK_APPROVAL",
                "PASS" if approval_ok else "FAIL",
                f"exceptions={len(exception_rows)}, approval={'present' if fallback_approval else 'missing'}",
            ))
            checks.append(Check(
                "SERVICE_ACCOUNT_IDENTITY",
                "PASS" if service_identity and service_identity.get("id") else "FAIL",
                "Created By must read back to the resolved migration service account.",
            ))

    def _online_checks(
        self, checks: list[Check], sample_blobs: int,
        max_documents: int | None,
        owner_resolutions: list[OwnerResolution] | None,
        fallback_approval: dict[str, Any] | None,
        service_identity: dict[str, Any] | None,
    ) -> None:
        db_status = SourceDB(self.env).test_connection()
        checks.append(Check(
            "SOURCE_DB_ONLINE", "PASS" if db_status.get("status") == "connected" else "FAIL",
            _safe_detail(db_status),
        ))
        target = None
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
            owner_route_ready = bool(self.env.get("owner_assignment_qualified"))
            checks.append(Check(
                "OWNER_ASSIGNMENT_QUALIFICATION",
                "PASS" if owner_route_ready else "FAIL",
                "Ordinary folders/documents and every Business Workspace route require a qualified owner-write path.",
            ))
            provenance_route_ready = bool(self.env.get("provenance_category_qualified"))
            checks.append(Check(
                "PROVENANCE_APPLICABILITY",
                "PASS" if provenance_route_ready else "FAIL",
                "CDM Migration Provenance must be applicable and writable on every migrated object type.",
            ))
            checks.append(Check(
                "PROVENANCE_READBACK_QUALIFICATION",
                "PASS" if bool(self.env.get("provenance_readback_qualified")) else "FAIL",
                "Provenance values and ordered version rows require qualified target read-back.",
            ))
            checks.append(Check(
                "CREATOR_READBACK_QUALIFICATION",
                "PASS" if bool(self.env.get("creator_readback_qualified")) else "FAIL",
                "Created By must be readable and identify the migration service account.",
            ))
            if owner_resolutions is not None:
                checks.append(Check(
                    "OWNER_RESOLUTION_ONLINE",
                    "PASS" if all(
                        row.target_member_id is not None
                        and row.resolution_status in {"EXACT", "APPROVED_FALLBACK"}
                        for row in owner_resolutions
                    ) else "FAIL",
                    f"resolved_distinct_source_owners={len(owner_resolutions)}",
                ))
            if fallback_approval:
                checks.append(Check(
                    "FALLBACK_APPROVAL_BOUND",
                    "PASS" if bool(fallback_approval.get("exception_set_digest")) else "FAIL",
                    "Fallback approval must be bound to the current exception digest.",
                ))
            if service_identity:
                checks.append(Check(
                    "SERVICE_ACCOUNT_READBACK",
                    "PASS" if service_identity.get("id") else "FAIL",
                    f"service_member_id={service_identity.get('id')}",
                ))
            with self.manifest.connection() as conn:
                scope_ids = self.manifest.source_ids_for_scope(max_documents)
                _, version_params = self.manifest._scope_clause(scope_ids, "doc_source_id")
                version_scope, _ = self.manifest._scope_clause(scope_ids, "doc_source_id")
                heavy = conn.execute(
                    f"SELECT COUNT(*) FROM manifest_versions WHERE data_size>=?{version_scope}",
                    (int(self.settings.get("multipart_threshold_bytes", 50 * 1024 * 1024)),) + version_params,
                ).fetchone()[0]
            if heavy:
                multipart = target.get_multipart_settings()
                checks.append(Check(
                    "TARGET_MULTIPART", "PASS" if multipart.get("is_enabled") else "FAIL",
                    f"settings={multipart}",
                ))
        except Exception as exc:
            checks.append(Check("TARGET_API", "FAIL", f"{type(exc).__name__}: {exc}"))
        finally:
            if target is not None:
                target.close()
        if sample_blobs:
            source = None
            try:
                source = build_binary_source(self.env)
                with self.manifest.connection() as conn:
                    scope_ids = self.manifest.source_ids_for_scope(max_documents)
                    version_scope, version_params = self.manifest._scope_clause(
                        scope_ids, "doc_source_id"
                    )
                    rows = conn.execute(
                        f"SELECT * FROM manifest_versions "
                        f"WHERE 1=1{version_scope} ORDER BY data_size DESC LIMIT ?",
                        version_params + (sample_blobs,),
                    ).fetchall()
                for row in rows:
                    version = SourceVersion(
                        source_id=row["doc_source_id"], version_num=row["version_num"],
                        file_name=row["file_name"], mime_type=row["mime_type"], size=row["data_size"],
                        provider_id=row["provider_id"], provider_data=row["provider_data"],
                        blob_locator=row["blob_locator"], source_sha256=row["source_sha256"],
                        created_at=row["source_created_at"], modified_at=row["source_modified_at"],
                        file_date=row["source_file_date"],
                    )
                    source.validate(version)
                checks.append(Check("SOURCE_BLOB_SAMPLES", "PASS", f"validated={len(rows)}"))
            except Exception as exc:
                checks.append(Check("SOURCE_BLOB_SAMPLES", "FAIL", f"{type(exc).__name__}: {exc}"))
            finally:
                if source is not None and hasattr(source, "close"):
                    try:
                        source.close()
                    except Exception as exc:
                        checks.append(Check(
                            "SOURCE_BINARY_SESSION_CLEANUP", "FAIL",
                            f"{type(exc).__name__}: {exc}",
                        ))


def _safe_detail(value: dict[str, Any]) -> str:
    sanitized = {key: val for key, val in value.items() if key.lower() not in {"ticket", "password", "token"}}
    return str(sanitized)[:1000]
