"""Run-scoped, restartable migration orchestration."""

from __future__ import annotations

import hashlib
import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import UTC, datetime
from typing import Any, BinaryIO

from .client import OpenTextCloudClient
from .config import EnvironmentConfig
from .db import SourceDB
from .manifest import ManifestStore, StateConflict
from .models import (
    AmbiguousRemoteCommit,
    ItemState,
    RetryableMigrationError,
    RunMode,
    RunStatus,
    SourceNode,
    SourceVersion,
    TerminalMigrationError,
)
from .provenance import (
    METADATA_CONTRACT_VERSION,
    OwnerResolution,
    exception_set_digest,
    fallback_resolve,
    member_identity,
    normalized_identity,
    provenance_values,
    resolve_fallback_principal,
    resolve_manual_owner_mapping,
    resolve_owner_identity,
    resolve_target_login,
    sanitized_owner_summary,
    utc_iso,
    version_provenance_values,
)
from .source import build_binary_source

logger = logging.getLogger("CDM.Pipeline")


class HashingReader:
    def __init__(self, source: BinaryIO, digest=None):
        self.source = source
        self.digest = digest or hashlib.sha256()
        self.bytes_read = 0

    def read(self, size: int = -1) -> bytes:
        data = self.source.read(size)
        if data:
            self.digest.update(data)
            self.bytes_read += len(data)
        return data

    def hexdigest(self) -> str:
        return self.digest.hexdigest()


class MigrationPipeline:
    def __init__(
        self, config: Any, state_db_path: str = "migration_state_v2.db", *,
        source_db: SourceDB | None = None, target=None, binary_source=None,
    ):
        self.config = config
        if hasattr(config, "environment"):
            self.env_name = config.default_environment
            self.env_cfg = config.environment()
            self.settings = config.migration_settings
        else:
            self.env_name = config.get("default_environment", "dev")
            self.env_cfg = config.get("environments", {}).get(self.env_name, {})
            self.settings = config.get("migration_settings", {})
        self.manifest = ManifestStore(state_db_path)
        # Profile edits are allowed after extraction. Rebuild the derived target
        # mapping columns so removed or changed mappings cannot remain stale.
        self.manifest.apply_category_mappings(
            self.env_cfg.get("category_mappings", {}) or {}, reset=True,
        )
        self.source_db = source_db or SourceDB(self.env_cfg)
        self._target = target
        self._binary_source = binary_source
        self._run_thread: threading.Thread | None = None
        self._active_run_id: str | None = None
        self._control_lock = threading.RLock()
        self._pause_event = threading.Event()
        self._pause_event.set()
        self._large_upload_slots = threading.Semaphore(int(self.settings.get("large_file_workers", 1)))
        self._started_monotonic = 0.0
        self._bytes = 0
        self._processed = 0
        self._active_workers = 0
        self._metrics_lock = threading.Lock()
        self.recent_logs: list[dict[str, str]] = []

    @property
    def target(self):
        if self._target is None:
            self._target = OpenTextCloudClient(self.env_cfg, int(self.settings.get("max_retries", 5)))
        return self._target

    @property
    def binary_source(self):
        if self._binary_source is None:
            self._binary_source = build_binary_source(self.env_cfg)
        return self._binary_source

    @property
    def is_running(self) -> bool:
        return bool(self._run_thread and self._run_thread.is_alive())

    @property
    def is_paused(self) -> bool:
        return self.is_running and not self._pause_event.is_set()

    @property
    def is_production(self) -> bool:
        configured = str(self.env_cfg.get("environment_class", "")).lower()
        return configured == "production" or (not configured and self.env_name == "prod")

    def close(self) -> None:
        if self.is_running:
            raise StateConflict("Cannot close a pipeline while a run is active")
        try:
            if self._target is not None and hasattr(self._target, "close"):
                self._target.close()
        finally:
            try:
                if self._binary_source is not None and hasattr(self._binary_source, "close"):
                    self._binary_source.close()
            finally:
                self.manifest.close()

    def log(self, message: str, level: str = "INFO") -> None:
        entry = {
            "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
            "level": level,
            "message": message,
        }
        with self._metrics_lock:
            self.recent_logs.append(entry)
            del self.recent_logs[:-500]
        getattr(logger, level.lower(), logger.info)(message)

    def run_extraction(self, root_node_id: int | None = None) -> dict[str, Any]:
        with self._control_lock:
            if self.is_running:
                raise StateConflict("Cannot extract while migration is active")
            root = int(root_node_id or self.env_cfg.get("source_workspace_nodeid"))
            self.log(f"Starting consistent source snapshot for NodeID {root}")
            extracted = self.source_db.extract_all(root)
            if not extracted["nodes"]:
                return {"status": "empty", "nodes_count": 0, "snapshot": extracted["snapshot"]}
            self.manifest.clear_inventory()
            self.manifest.import_extracted_data(
                extracted["nodes"], extracted["versions"], extracted["categories"],
                snapshot=extracted.get("snapshot"),
                extracted_at=extracted.get("extracted_at"),
                signature=extracted.get("source_signature"),
                source_root_id=root,
                source_profile_id=self.env_name,
                owners=extracted.get("owners", []),
            )
            mapped = self.manifest.apply_category_mappings(
                self.env_cfg.get("category_mappings", {}) or {}, reset=True,
            )
            summary = self.manifest.inventory_summary()
            self.log(
                f"Manifest imported from snapshot {extracted['snapshot']}: "
                f"{summary['total_nodes']} nodes, {mapped} category attributes mapped"
            )
            return {"status": "success", "snapshot": extracted["snapshot"], "mapped_attributes": mapped, **summary}

    def inspect_source_scope(self, root_node_id: int | None = None) -> dict[str, Any]:
        root = int(root_node_id or self.env_cfg.get("source_workspace_nodeid"))
        return self.source_db.inspect_scope(root)

    def inspect_target_root(self, target_node_id: int | None = None) -> dict[str, Any]:
        target = int(target_node_id or self.env_cfg.get("target_workspace_nodeid"))
        properties = self.target.get_node(target)
        return {"status": "found", "requested_id": target, "properties": properties}

    def inspect_target_contract(self, target_node_id: int) -> dict[str, Any]:
        """Read target update capabilities without mutating the existing object."""
        target = int(target_node_id)
        return {
            "status": "inspected",
            "node_id": target,
            "properties": self.target.get_node(target),
            "system_attribute_capabilities": self.target.system_attribute_capabilities(target),
        }

    def confirm_source_freeze(self, operator: str, note: str = "") -> dict[str, Any]:
        root = int(self.env_cfg.get("source_workspace_nodeid"))
        observed = self.source_db.scope_signature(root)
        return self.manifest.confirm_source_freeze(observed, operator, note)

    def start_migration(
        self, max_items: int | None = None, dry_run: bool = False, threads: int | None = None,
        *, mode: str | None = None, owner_exception_approval: dict[str, Any] | None = None,
    ) -> str:
        with self._control_lock:
            if self.is_running:
                raise StateConflict(f"Run {self._active_run_id} is already active")
            worker_count = int(threads or self.settings.get("worker_threads", 8))
            maximum = int(self.settings.get("max_worker_threads", 16))
            if not 1 <= worker_count <= maximum:
                raise ValueError(f"threads must be between 1 and {maximum}")
            run_mode = RunMode(mode or (RunMode.DRY_RUN if dry_run else RunMode.PILOT if max_items else RunMode.FULL))
            if run_mode == RunMode.FULL and self.is_production:
                freeze = self.manifest.freeze_status()
                if not freeze["confirmed"]:
                    raise TerminalMigrationError(
                        "Production FULL run requires a confirmed source read-only freeze with an unchanged signature"
                    )
            owner_resolutions: list[OwnerResolution] = []
            fallback_approval: dict[str, Any] | None = None
            service_identity: dict[str, Any] | None = None
            if run_mode != RunMode.DRY_RUN and self._owner_contract_available():
                owner_resolutions, fallback_approval, service_identity = self._resolve_run_identities(
                    owner_exception_approval, max_items
                )
            if run_mode == RunMode.DRY_RUN or self.env_cfg.get("ot_cloud_url"):
                from .preflight import PreflightAuditor

                preflight_env = (
                    self.env_cfg
                    if isinstance(self.env_cfg, EnvironmentConfig)
                    else EnvironmentConfig(self.env_name, self.env_cfg)
                )
                report = PreflightAuditor(
                    preflight_env, self.manifest, self.settings
                ).run(
                    online=run_mode != RunMode.DRY_RUN,
                    for_mode=str(run_mode),
                    max_documents=max_items,
                    owner_resolutions=owner_resolutions,
                    fallback_approval=fallback_approval,
                    service_identity=service_identity,
                )
                if report["status"] == "FAIL":
                    failed = [check["id"] for check in report["checks"] if check["status"] == "FAIL"]
                    raise TerminalMigrationError(f"Offline pre-flight failed: {', '.join(failed)}")
            root = int(self.env_cfg.get("target_workspace_nodeid"))
            metadata = self.manifest.metadata()
            run_id = self.manifest.create_run(
                self.env_name, run_mode, root,
                max_documents=max_items,
                config_fingerprint=self._config_fingerprint(),
                source_snapshot=metadata.get("inventory_snapshot"),
                metadata_contract_version=(
                    METADATA_CONTRACT_VERSION
                    if self._owner_contract_available()
                    else "legacy-system-attributes"
                ),
                owner_resolutions=owner_resolutions,
                fallback_approval=fallback_approval,
                service_identity=service_identity,
            )
            self._active_run_id = run_id
            self._pause_event.set()
            self._started_monotonic = time.monotonic()
            self._bytes = 0
            self._processed = 0
            self._run_thread = threading.Thread(
                target=self._execute, args=(run_id, run_mode, worker_count),
                name=f"cdm-run-{run_id[:8]}", daemon=False,
            )
            self._run_thread.start()
            self.log(f"Started {run_mode} run {run_id} with {worker_count} document workers")
            return run_id

    def _owner_contract_available(self) -> bool:
        target = self._target
        if target is None:
            return bool(self.env_cfg.get("ot_cloud_url"))
        return all(
            callable(getattr(target, name, None))
            for name in (
                "lookup_members", "assign_owner", "apply_provenance",
                "read_owner", "read_creator", "read_provenance", "get_member",
            )
        )

    def _manifest_owner_ids(self, max_documents: int | None = None) -> list[int]:
        return sorted(self.manifest.owner_ids_for_scope(max_documents))

    def owner_readiness(
        self, *, online: bool = True, max_documents: int | None = None,
    ) -> dict[str, Any]:
        """Return GET-only owner resolution evidence for the operator UI."""
        owner_ids = self._manifest_owner_ids(max_documents)
        owners = {owner.source_owner_id: owner for owner in self.manifest.owner_identities()}
        missing = sorted(set(owner_ids) - set(owners))
        result: dict[str, Any] = {
            "status": "BLOCKED" if missing else "NOT_CHECKED",
            "online": online,
            "max_documents": max_documents,
            "owner_count": len(owner_ids),
            "owners": [
                sanitized_owner_summary(owners[owner_id])
                for owner_id in owner_ids if owner_id in owners
            ],
            "missing_owner_ids": missing,
            "resolutions": [],
            "exceptions": [],
            "exception_set_digest": None,
            "fallback": None,
            "service_account": None,
        }
        if missing:
            return result
        if not online:
            result["status"] = "ONLINE_CHECK_REQUIRED"
            return result
        target = self.target
        lookup = getattr(target, "lookup_members", None) or getattr(target, "find_members", None)
        if not callable(lookup):
            result["status"] = "BLOCKED"
            result["error"] = "GX39 client does not expose GET-only member lookup"
            return result

        mappings = self.env_cfg.get("owner_mappings", {}) or {}
        resolutions = [
            resolve_manual_owner_mapping(owners[owner_id], mappings, lookup)
            or resolve_owner_identity(owners[owner_id], lookup)
            for owner_id in owner_ids
        ]
        result["resolutions"] = [
            {
                "source_owner_id": resolution.source_owner_id,
                "status": resolution.resolution_status,
                "reason": resolution.reason,
                "target_member_id": resolution.target_member_id,
                "target_login": resolution.target_login,
                "target_email": resolution.target_email,
                "target_display_name": resolution.target_display_name,
                "target_active": resolution.target_active,
            }
            for resolution in resolutions
        ]
        exceptions = [resolution for resolution in resolutions if resolution.is_exception]
        service_login = str(
            self.env_cfg.get("service_account_login")
            or self.env_cfg.get("ot_cloud_user")
            or getattr(target, "username", "")
        ).strip()
        service_member = resolve_target_login(service_login, lookup) if service_login else None
        result["exceptions"] = [
            {
                "source_owner_id": resolution.source_owner_id,
                "reason": resolution.reason,
                "summary": sanitized_owner_summary(owners[resolution.source_owner_id]),
            }
            for resolution in exceptions
        ]
        if exceptions:
            result["exception_set_digest"] = exception_set_digest(resolutions)
            fallback_config = (
                self.env_cfg.get("owner_fallback")
                or self.env_cfg.get("legacy_owner_fallback")
                or (
                    {"member_id": service_member["id"], "login": service_member.get("login")}
                    if service_member is not None else {}
                )
            )
            if fallback_config:
                try:
                    fallback = resolve_fallback_principal(fallback_config, lookup)
                except TerminalMigrationError as exc:
                    result["fallback_error"] = str(exc)
                else:
                    result["fallback"] = {
                        "id": fallback.get("id"),
                        "login": fallback.get("login"),
                        "email": fallback.get("email"),
                        "display_name": fallback.get("display_name"),
                        "active": fallback.get("active"),
                    }
            if result.get("fallback") is None:
                result["status"] = "BLOCKED"
            else:
                result["status"] = "APPROVAL_REQUIRED"
        else:
            result["status"] = "PASS"

        service_email = str(self.env_cfg.get("service_account_email") or "").strip()
        if service_login or service_email:
            result["service_account"] = {
                "status": "EXACT" if service_member else "UNRESOLVED",
                "reason": None if service_member else "UNRESOLVED",
                "id": service_member.get("id") if service_member else None,
                "login": service_member.get("login") if service_member else None,
                "email": service_member.get("email") if service_member else None,
                "active": service_member.get("active") if service_member else None,
            }
            if (
                service_member is None
                or service_member.get("id") is None
            ):
                result["status"] = "BLOCKED"
        else:
            result["service_account"] = {"status": "MISSING"}
            result["status"] = "BLOCKED"
        return result

    def _resolve_run_identities(
        self, approval: dict[str, Any] | None, max_documents: int | None = None,
    ) -> tuple[list[OwnerResolution], dict[str, Any] | None, dict[str, Any]]:
        owners = self.manifest.owner_identities()
        owner_ids = self.manifest.owner_ids_for_scope(max_documents)
        by_id = {owner.source_owner_id: owner for owner in owners}
        missing = sorted(owner_ids - set(by_id))
        if missing:
            raise TerminalMigrationError(
                f"OWNER_IDENTITY_COVERAGE: missing KUAF identity rows for source owners {missing[:20]}"
            )
        target = self.target
        lookup = getattr(target, "lookup_members", None) or getattr(target, "find_members", None)
        if not callable(lookup):
            raise TerminalMigrationError("GX39 client does not expose GET-only member lookup")
        mappings = self.env_cfg.get("owner_mappings", {}) or {}
        resolutions = [
            resolve_manual_owner_mapping(by_id[owner_id], mappings, lookup)
            or resolve_owner_identity(by_id[owner_id], lookup)
            for owner_id in sorted(owner_ids)
        ]
        exceptions = [resolution for resolution in resolutions if resolution.is_exception]
        service_login = str(
            self.env_cfg.get("service_account_login")
            or self.env_cfg.get("ot_cloud_user")
            or getattr(target, "username", "")
        ).strip()
        service_member = resolve_target_login(service_login, lookup) if service_login else None
        fallback_approval: dict[str, Any] | None = None
        if exceptions:
            fallback_config = (
                self.env_cfg.get("owner_fallback")
                or self.env_cfg.get("legacy_owner_fallback")
                or (
                    {"member_id": service_member["id"], "login": service_member.get("login")}
                    if service_member is not None else {}
                )
            )
            if not isinstance(fallback_config, dict) or not fallback_config:
                raise TerminalMigrationError(
                    "OWNER_FALLBACK_CONFIGURATION: unresolved or deactivated source owners require "
                    "an explicitly configured active fallback"
                )
            fallback_member = resolve_fallback_principal(fallback_config, lookup)
            resolutions = [
                fallback_resolve(
                    by_id[resolution.source_owner_id],
                    fallback_member,
                    reason=str(resolution.reason or "UNRESOLVED"),
                    resolved_at=resolution.resolved_at,
                )
                if resolution.is_exception else resolution
                for resolution in resolutions
            ]
            digest = exception_set_digest(resolutions)
            if not approval or approval.get("approved") is not True:
                raise TerminalMigrationError(
                    f"OWNER_EXCEPTION_APPROVAL: explicit approval is required for exception digest {digest}"
                )
            supplied_digest = str(
                approval.get("exception_set_digest") or approval.get("digest") or ""
            )
            operator = str(approval.get("operator") or "").strip()
            change_record = str(approval.get("change_record") or "").strip()
            if supplied_digest != digest or not operator or not change_record:
                raise TerminalMigrationError(
                    "OWNER_EXCEPTION_APPROVAL: digest, operator and change record must match the current exceptions"
                )
            if approval.get("fallback_member_id") is not None and int(approval["fallback_member_id"]) != int(
                fallback_member["id"]
            ):
                raise TerminalMigrationError("OWNER_EXCEPTION_APPROVAL: fallback principal changed")
            fallback_approval = {
                "fallback_member_id": int(fallback_member["id"]),
                "fallback_login": fallback_member.get("login"),
                "fallback_email": fallback_member.get("email"),
                "fallback_display_name": fallback_member.get("display_name"),
                "exception_set_digest": digest,
                "exception_count": len(exceptions),
                "operator": operator,
                "change_record": change_record,
            }
        service_email = str(self.env_cfg.get("service_account_email") or "").strip()
        if not service_login and not service_email:
            raise TerminalMigrationError(
                "SERVICE_ACCOUNT_IDENTITY: configured GX39 migration account is missing"
            )
        if service_member is None:
            raise TerminalMigrationError(
                "SERVICE_ACCOUNT_IDENTITY: migration account is not one unique active GX39 user"
            )
        service_identity = {
            "id": service_member["id"],
            "login": service_member.get("login"),
            "email": service_member.get("email"),
            "display_name": service_member.get("display_name"),
            "active": service_member.get("active"),
        }
        return resolutions, fallback_approval, service_identity

    def pause(self) -> None:
        with self._control_lock:
            if not self._active_run_id or not self.is_running:
                raise StateConflict("No active run")
            self._pause_event.clear()
            self.manifest.pause_run(self._active_run_id)
            self.log(f"Paused run {self._active_run_id}; in-flight request may finish")

    def recover_run(self, run_id: str, threads: int | None = None) -> None:
        with self._control_lock:
            if self.is_running:
                raise StateConflict("Another run is already active")
            worker_count = int(threads or self.settings.get("worker_threads", 8))
            maximum = int(self.settings.get("max_worker_threads", 16))
            if not 1 <= worker_count <= maximum:
                raise ValueError(f"threads must be between 1 and {maximum}")
            run = self.manifest.run_status(run_id)
            if run.get("config_fingerprint") and run["config_fingerprint"] != self._config_fingerprint():
                raise TerminalMigrationError(
                    "Profile configuration changed since this run started; start a new idempotent run instead of recovery"
                )
            if (
                RunMode(run["mode"]) == RunMode.FULL and self.is_production
                and not self.manifest.freeze_status()["confirmed"]
            ):
                raise TerminalMigrationError("Production recovery requires a valid source freeze confirmation")
            run_mode = RunMode(run["mode"])
            stored_resolutions = self.manifest.run_owner_resolutions(run_id)
            stored_approval = self.manifest.fallback_approval(run_id)
            stored_service = {
                "id": run.get("service_member_id"),
                "login": run.get("service_login"),
                "email": run.get("service_email"),
            } if run.get("service_member_id") is not None else None
            if run_mode != RunMode.DRY_RUN and self._owner_contract_available():
                self._validate_recovery_identities(
                    run_id, stored_resolutions, stored_approval, stored_service,
                )
            if run_mode != RunMode.DRY_RUN and self.env_cfg.get("ot_cloud_url"):
                from .preflight import PreflightAuditor

                preflight_env = (
                    self.env_cfg
                    if isinstance(self.env_cfg, EnvironmentConfig)
                    else EnvironmentConfig(self.env_name, self.env_cfg)
                )
                report = PreflightAuditor(
                    preflight_env, self.manifest, self.settings
                ).run(
                    online=True,
                    for_mode=str(run_mode),
                    owner_resolutions=stored_resolutions,
                    fallback_approval=stored_approval,
                    service_identity=stored_service,
                )
                if report["status"] == "FAIL":
                    failed = [check["id"] for check in report["checks"] if check["status"] == "FAIL"]
                    raise TerminalMigrationError(
                        f"Recovery pre-flight failed: {', '.join(failed)}"
                    )
            self.manifest.recover_run(run_id)
            run = self.manifest.run_status(run_id)
            self._active_run_id = run_id
            self._pause_event.set()
            self._started_monotonic = time.monotonic()
            self._run_thread = threading.Thread(
                target=self._execute,
                args=(run_id, RunMode(run["mode"]), worker_count),
                name=f"cdm-recover-{run_id[:8]}", daemon=False,
            )
            self._run_thread.start()

    def _validate_recovery_identities(
        self, run_id: str, resolutions: list[OwnerResolution],
        approval: dict[str, Any] | None, service_identity: dict[str, Any] | None,
    ) -> None:
        target = self.target
        for resolution in resolutions:
            owner = self.manifest.owner_identity(resolution.source_owner_id)
            if owner.fingerprint != resolution.source_identity_fingerprint:
                raise TerminalMigrationError(
                    f"Recovery source owner identity changed for {resolution.source_owner_id}"
                )
            if resolution.target_member_id is None:
                raise TerminalMigrationError(
                    f"Recovery owner resolution is missing target member {resolution.source_owner_id}"
                )
            member = member_identity(target.get_member(int(resolution.target_member_id)))
            if member["active"] is not True:
                raise TerminalMigrationError(
                    f"Recovery target owner {resolution.target_member_id} is not active"
                )
            if (
                resolution.target_login
                and normalized_identity(member.get("login"))
                != normalized_identity(resolution.target_login)
            ):
                raise TerminalMigrationError(
                    f"Recovery target owner login changed for {resolution.source_owner_id}"
                )
            if (
                resolution.target_email
                and normalized_identity(member.get("email"))
                != normalized_identity(resolution.target_email)
            ):
                raise TerminalMigrationError(
                    f"Recovery target owner email changed for {resolution.source_owner_id}"
                )
        if (
            any(row.resolution_status == "APPROVED_FALLBACK" for row in resolutions)
            and (not approval or approval.get("exception_set_digest") != exception_set_digest(resolutions))
        ):
            raise TerminalMigrationError("Recovery fallback approval digest is no longer valid")
        if not service_identity or service_identity.get("id") is None:
            raise TerminalMigrationError("Recovery service-account identity is missing")
        service = member_identity(target.get_member(int(service_identity["id"])))
        if service["active"] is not True:
            raise TerminalMigrationError("Recovery migration service account is not active")
        if (
            service_identity.get("login")
            and normalized_identity(service.get("login"))
            != normalized_identity(service_identity["login"])
        ):
            raise TerminalMigrationError("Recovery migration service-account login changed")
        if (
            service_identity.get("email")
            and normalized_identity(service.get("email"))
            != normalized_identity(service_identity["email"])
        ):
            raise TerminalMigrationError("Recovery migration service-account email changed")

    def resume(self) -> None:
        with self._control_lock:
            if not self._active_run_id or not self.is_running:
                raise StateConflict("No active run")
            self.manifest.start_run(self._active_run_id)
            self._pause_event.set()
            self.log(f"Resumed run {self._active_run_id}")

    def stop(self) -> None:
        with self._control_lock:
            if not self._active_run_id or not self.is_running:
                raise StateConflict("No active run")
            self.manifest.request_stop(self._active_run_id)
            self._pause_event.set()
            self.log(f"Stop requested for {self._active_run_id}; no new items will be claimed")

    def wait(self, timeout: float | None = None) -> bool:
        thread = self._run_thread
        if thread:
            thread.join(timeout)
        return not self.is_running

    def _execute(self, run_id: str, mode: RunMode, workers: int) -> None:
        try:
            self.manifest.start_run(run_id)
            if mode != RunMode.DRY_RUN:
                self.target.event_sink = lambda event: self.manifest.append_attempt(
                    run_id,
                    event.get("operation", "HTTP"),
                    event.get("outcome", "UNKNOWN"),
                    http_status=event.get("http_status"),
                    correlation_id=event.get("correlation_id"),
                    detail=event.get("detail"),
                )
            self._run_phase(run_id, mode, "CONTAINER", 1, self._process_container)
            if not self.manifest.should_stop(run_id):
                self._run_phase(run_id, mode, "DOCUMENT", workers, self._process_document)
            if not self.manifest.should_stop(run_id):
                self._run_phase(run_id, mode, "REFERENCE", min(2, workers), self._process_reference)
            # Owner reassignment runs as a final, dedicated phase after every
            # container/document/reference in the run has been created. GX39
            # inherits ACLs from a parent at child-creation time; reassigning a
            # container's owner before all of its descendants (at any depth)
            # exist would strip the migration account's ACL entry on the
            # parent and cause every not-yet-created child to inherit an ACL
            # without that account, breaking subsequent metadata writes with
            # "Insufficient permissions". Deferring owner assignment until the
            # whole tree is committed avoids that race entirely.
            if not self.manifest.should_stop(run_id) and mode != RunMode.DRY_RUN:
                self._run_phase(run_id, mode, "OWNER", workers, self._process_owner_assignment)
            final = self.manifest.finish_run(run_id)
            self.log(f"Run {run_id} finished with {final}")
        except Exception as exc:
            logger.exception("Fatal migration run failure")
            self.log(f"Fatal run error: {exc}", "ERROR")
            self.manifest.finish_run(run_id, RunStatus.FAILED)
        finally:
            with self._control_lock:
                self._pause_event.set()

    def _run_phase(
        self, run_id: str, mode: RunMode, phase: str, concurrency: int,
        processor: Callable[[str, str, dict[str, Any], RunMode], None],
    ) -> None:
        self.log(f"Starting phase {phase} with concurrency {concurrency}")
        while not self.manifest.should_stop(run_id):
            self._pause_event.wait()
            if self.manifest.should_stop(run_id):
                break
            with ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix=f"cdm-{phase.lower()}") as pool:
                futures = [pool.submit(self._worker_loop, run_id, mode, phase, processor) for _ in range(concurrency)]
                wait(futures)
                for future in futures:
                    future.result()
            if phase == "OWNER":
                counts = self.manifest.phase_counts(run_id, "OWNER")
                if self.manifest.block_children_of_failed(run_id, "OWNER"):
                    counts = self.manifest.phase_counts(run_id, "OWNER")
                pending = counts.get(ItemState.OWNER_PENDING, 0)
                retrying = counts.get(ItemState.RETRY_WAIT, 0)
                claimed = counts.get(ItemState.CLAIMED, 0)
                if pending or claimed:
                    if retrying:
                        time.sleep(1.0)
                    continue
                if retrying:
                    time.sleep(1.0)
                    continue
                break
            counts = self.manifest.phase_counts(run_id, phase)
            if self.manifest.block_children_of_failed(run_id, phase):
                counts = self.manifest.phase_counts(run_id, phase)
            retrying = counts.get(ItemState.RETRY_WAIT, 0)
            ready = counts.get(ItemState.READY, 0)
            claimed = counts.get(ItemState.CLAIMED, 0)
            if ready or claimed:
                if retrying:
                    time.sleep(1.0)
                continue
            if retrying:
                time.sleep(1.0)
                continue
            break

    def _worker_loop(
        self, run_id: str, mode: RunMode, phase: str,
        processor: Callable[[str, str, dict[str, Any], RunMode], None],
    ) -> None:
        worker_id = f"{threading.current_thread().name}-{threading.get_ident()}"
        while not self.manifest.should_stop(run_id):
            self._pause_event.wait()
            if self.manifest.should_stop(run_id):
                return
            if phase == "OWNER":
                item = self.manifest.claim_next_owner(
                    run_id, worker_id,
                    lease_seconds=int(self.settings.get("item_lease_seconds", 1800)),
                )
            else:
                item = self.manifest.claim_next(
                    run_id, phase, worker_id,
                    lease_seconds=int(self.settings.get("item_lease_seconds", 1800)),
                )
            if not item:
                return
            with self._metrics_lock:
                self._active_workers += 1
            try:
                processor(run_id, worker_id, item, mode)
                with self._metrics_lock:
                    self._processed += 1
            except TerminalMigrationError as exc:
                self.manifest.record_failure(
                    run_id, item["source_id"], exc,
                    max_attempts=int(self.settings.get("max_item_attempts", 5)), retryable=False,
                    error_code=type(exc).__name__,
                )
                self.log(f"Terminal failure for {item['source_id']}: {exc}", "ERROR")
            except (RetryableMigrationError, AmbiguousRemoteCommit) as exc:
                state = self.manifest.record_failure(
                    run_id, item["source_id"], exc,
                    max_attempts=int(self.settings.get("max_item_attempts", 5)), retryable=True,
                    error_code=type(exc).__name__,
                )
                self.log(f"Retryable failure for {item['source_id']} -> {state}: {exc}", "WARNING")
            except StateConflict as exc:
                self.manifest.record_failure(
                    run_id, item["source_id"], exc,
                    max_attempts=1, retryable=False, error_code="DEPENDENCY_BLOCKED",
                )
                self.log(f"Blocked item {item['source_id']}: {exc}", "ERROR")
            except Exception as exc:
                logger.exception("Unexpected item failure")
                self.manifest.record_failure(
                    run_id, item["source_id"], exc,
                    max_attempts=int(self.settings.get("max_item_attempts", 5)), retryable=True,
                    error_code="UNEXPECTED",
                )
            finally:
                with self._metrics_lock:
                    self._active_workers = max(0, self._active_workers - 1)

    def _process_container(self, run_id: str, worker_id: str, item: dict[str, Any], mode: RunMode) -> None:
        node = self.manifest.source_node(item["source_id"])
        if mode == RunMode.DRY_RUN:
            self._validate_node(node)
            self.manifest.mark_state(run_id, node.source_id, ItemState.SIMULATED, worker_id=worker_id)
            return
        target_root = int(self.env_cfg.get("target_workspace_nodeid"))
        parent = self.manifest.resolve_parent(node.parent_source_id, target_root)
        migration_id = self._migration_id(node.source_id)
        if node.depth == 0 and bool(self.env_cfg.get("source_root_maps_to_target", False)):
            properties = self.target.get_node(target_root)
            if int(properties.get("id", -1)) != target_root:
                raise TerminalMigrationError("Configured target root could not be read")
            self.manifest.commit_mapping(
                run_id, node.source_id, target_root, int(properties.get("parent_id", 0)),
                migration_id, worker_id=worker_id,
            )
            self.manifest.mark_state(run_id, node.source_id, ItemState.VERIFIED, worker_id=worker_id)
            self.manifest.mark_mapping_verified(node.source_id)
            return
        try:
            target_id = self.target.create_container(node, parent, migration_id)
        except AmbiguousRemoteCommit:
            target_id = self.target.find_by_migration_id(parent, migration_id)
            if not target_id:
                raise
        self.manifest.commit_mapping(run_id, node.source_id, target_id, parent, migration_id, worker_id=worker_id)
        categories = self.manifest.categories(node.source_id)
        if categories:
            self.target.apply_categories(target_id, categories)
        self._apply_node_policies(run_id, target_id, node)
        self._verify_metadata(run_id, target_id, node)
        properties = self.target.get_node(target_id)
        if int(properties.get("parent_id", -1)) != parent or properties.get("name") != node.name:
            raise TerminalMigrationError(f"Container read-after-write mismatch for {node.source_id}")
        self._finish_item(run_id, node.source_id, worker_id=worker_id)

    def _process_document(self, run_id: str, worker_id: str, item: dict[str, Any], mode: RunMode) -> None:
        node = self.manifest.source_node(item["source_id"])
        versions = self.manifest.source_versions(node.source_id)
        if not versions:
            raise TerminalMigrationError(f"Document {node.source_id} has no version records")
        if mode == RunMode.DRY_RUN:
            self._validate_node(node)
            for version in versions:
                self._validate_version_manifest(version)
            self.manifest.mark_state(run_id, node.source_id, ItemState.SIMULATED, worker_id=worker_id)
            return
        parent = self.manifest.resolve_parent(node.parent_source_id, int(self.env_cfg.get("target_workspace_nodeid")))
        migration_id = self._migration_id(node.source_id)
        mapping = self.manifest.lookup_mapping(node.source_id)
        marker_target = self.target.find_by_migration_id(parent, migration_id)
        if mapping and marker_target and int(mapping["target_id"]) != marker_target:
            raise TerminalMigrationError("Durable mapping conflicts with the migration marker")
        target_id = marker_target or (int(mapping["target_id"]) if mapping else None)
        if mapping and not marker_target:
            self.target.apply_migration_marker(target_id, migration_id)
        elif marker_target and not mapping:
            self.manifest.commit_mapping(
                run_id, node.source_id, marker_target, parent, migration_id, worker_id=worker_id,
            )
        preexisting_target = target_id is not None
        for index, version in enumerate(versions):
            transfer = self.manifest.version_transfer(run_id, node.source_id, version.version_num)
            if transfer["state"] == ItemState.VERIFIED:
                target_id = target_id or item.get("target_id")
                continue
            self.binary_source.validate(version)
            if target_id is not None and (
                preexisting_target or index == 0 or transfer["state"] != ItemState.READY
            ):
                reconciled = self._try_reconcile_version(run_id, node, version, target_id)
                if reconciled:
                    continue
            creating_document = index == 0 and target_id is None
            self.manifest.update_version_transfer(
                run_id, node.source_id, version.version_num, state=ItemState.UPLOADING,
            )
            if version.size >= self.target.multipart_threshold:
                with self._large_upload_slots:
                    result, source_hash = self._multipart_upload(
                        run_id, node, version, parent, migration_id, target_id
                    )
            else:
                with self.binary_source.open(version) as raw:
                    hashing = HashingReader(raw)
                    if index == 0 and target_id is None:
                        try:
                            result = self.target.upload_first_version(node, version, parent, hashing, migration_id)
                        except AmbiguousRemoteCommit:
                            recovered = self.target.find_by_migration_id(parent, migration_id)
                            if not recovered:
                                raise
                            from .models import UploadResult
                            result = UploadResult(recovered, version.version_num)
                    else:
                        if target_id is None:
                            raise TerminalMigrationError("Cannot add a version before document creation")
                        result = self.target.upload_next_version(target_id, version, hashing)
                    source_hash = hashing.hexdigest()
            self._assert_declared_hash(version, source_hash)
            target_id = result.target_id
            self.manifest.update_version_transfer(
                run_id, node.source_id, version.version_num,
                state=ItemState.REMOTE_COMMITTED, target_version_num=result.version_number or version.version_num,
                source_sha256=source_hash, bytes_transferred=version.size,
            )
            if creating_document:
                self.manifest.commit_mapping(
                    run_id, node.source_id, target_id, parent, migration_id, worker_id=worker_id,
                )
                self.target.apply_migration_marker(target_id, migration_id)
            with self._metrics_lock:
                self._bytes += version.size
        if target_id is None:
            raise TerminalMigrationError("Document did not resolve to a target ID")
        self.manifest.commit_mapping(run_id, node.source_id, target_id, parent, migration_id, worker_id=worker_id)
        categories = self.manifest.categories(node.source_id)
        if categories:
            self.target.apply_categories(target_id, categories)
        self._apply_node_policies(run_id, target_id, node, versions)
        self._verify_metadata(run_id, target_id, node, versions)
        self.manifest.mark_state(
            run_id, node.source_id, ItemState.METADATA_APPLIED,
            worker_id=worker_id, target_id=target_id, target_parent_id=parent,
            bytes_transferred=sum(v.size for v in versions),
        )
        if bool(self.settings.get("verify_sha256", True)):
            self._verify_document(run_id, node, target_id, versions)
        self._finish_item(run_id, node.source_id, target_id=target_id)

    def _finish_item(
        self, run_id: str, source_id: int, *, worker_id: str | None = None, target_id: int | None = None,
    ) -> None:
        """Mark an item's content/provenance work done and queue its final owner assignment.

        Owner reassignment is deferred to a dedicated run-wide OWNER phase
        (see MigrationPipeline._execute) so that no container loses the
        migration account's ACL entry before all of its descendants exist.
        """
        self.manifest.mark_state(
            run_id, source_id, ItemState.OWNER_PENDING, worker_id=worker_id, target_id=target_id,
        )
        self.manifest.mark_mapping_verified(source_id)

    def _apply_node_policies(
        self, run_id: str, target_id: int, node: SourceNode,
        versions: list[SourceVersion] | None = None,
    ) -> None:
        modern_methods = (
            "assign_owner", "apply_provenance",
            "read_owner", "read_creator", "read_provenance",
        )
        modern_contract = all(
            callable(getattr(self.target, name, None))
            for name in modern_methods
        )
        partial_modern_contract = any(
            callable(getattr(self.target, name, None)) for name in modern_methods
        )
        if partial_modern_contract and not modern_contract:
            raise TerminalMigrationError(
                "GX39 owner/provenance contract is incomplete; qualified read-back methods are required"
            )
        if modern_contract:
            if node.owner_id is None:
                raise TerminalMigrationError(
                    f"Source node {node.source_id} has no owner identity for the owner/provenance contract"
                )
            owner = self.manifest.owner_identity(int(node.owner_id))
            resolution = self.manifest.run_owner_resolution(run_id, int(node.owner_id))
            if resolution.target_member_id is None:
                raise TerminalMigrationError(
                    f"Source owner {node.owner_id} has no resolved target member"
                )
            node_values = provenance_values(
                {
                    "source_id": node.source_id,
                    "source_created_at": node.created_at,
                    "source_modified_at": node.modified_at,
                },
                owner,
                resolution,
            )
            version_values = (
                [
                    version_provenance_values(
                        {
                            "version_num": version.version_num,
                            "source_created_at": version.created_at,
                            "source_modified_at": version.modified_at,
                            "source_file_date": version.file_date,
                        }
                    )
                    for version in versions
                ]
                if versions is not None
                else None
            )
            # Provenance is written by the migration service account. Owner
            # reassignment happens later, in a dedicated run-wide final phase
            # (see MigrationPipeline._execute / _process_owner_assignment),
            # so that no container loses the migration account's ACL entry
            # before all of its descendants (at any depth) have been created.
            self.target.apply_provenance(target_id, node_values, version_values)
        elif self.env_cfg.get("system_attribute_strategy") == "preserve":
            # Compatibility for isolated pre-contract test doubles only. The
            # real client exposes the modern contract and never uses this path.
            apply_system = getattr(self.target, "apply_system_attributes", None)
            if callable(apply_system):
                apply_system(target_id, node)
        if self.env_cfg.get("permission_strategy") == "mapped_acl":
            policies = self.env_cfg.get("permission_mappings", {}) or {}
            policy = policies.get(str(node.permissions_id))
            if policy is None:
                raise TerminalMigrationError(
                    f"No target ACL policy for source PermID {node.permissions_id}"
                )
            self.target.apply_permission_policy(target_id, policy)

    def _verify_metadata(
        self, run_id: str, target_id: int, node: SourceNode,
        versions: list[SourceVersion] | None = None,
    ) -> None:
        modern_methods = ("assign_owner", "apply_provenance", "read_owner", "read_creator", "read_provenance")
        modern_contract = all(callable(getattr(self.target, name, None)) for name in modern_methods)
        partial_modern_contract = any(
            callable(getattr(self.target, name, None)) for name in modern_methods
        )
        if partial_modern_contract and not modern_contract:
            raise TerminalMigrationError(
                "GX39 owner/provenance contract is incomplete; qualified read-back methods are required"
            )
        if not modern_contract:
            return
        if node.owner_id is None:
            raise TerminalMigrationError(f"Missing source owner for metadata verification {node.source_id}")
        resolution = self.manifest.run_owner_resolution(run_id, int(node.owner_id))
        if resolution.target_member_id is None:
            raise TerminalMigrationError(f"Missing target owner resolution for {node.source_id}")
        # Owner read-back is intentionally NOT verified here: owner
        # reassignment is deferred to a dedicated run-wide final phase (see
        # MigrationPipeline._process_owner_assignment) so a container never
        # loses the migration account's ACL entry before all descendants
        # exist. Owner read-back is verified there, once assign_owner runs.
        run = self.manifest.run_status(run_id)
        creator = self.target.read_creator(target_id)
        creator_id = _member_id_from_value(creator)
        expected_creator = run.get("service_member_id")
        if expected_creator is None or creator_id != int(expected_creator):
            raise TerminalMigrationError(
                f"Target creator read-back mismatch for {node.source_id}: "
                f"expected {expected_creator}, got {creator_id}"
            )
        node_values = provenance_values(
            {
                "source_id": node.source_id,
                "source_created_at": node.created_at,
                "source_modified_at": node.modified_at,
            },
            self.manifest.owner_identity(int(node.owner_id)),
            resolution,
        )
        version_values = (
            [
                version_provenance_values(
                    {
                        "version_num": version.version_num,
                        "source_created_at": version.created_at,
                        "source_modified_at": version.modified_at,
                        "source_file_date": version.file_date,
                    }
                )
                for version in versions
            ]
            if versions is not None
            else None
        )
        actual = self.target.read_provenance(target_id)
        self._assert_provenance_readback(node_values, version_values, actual)

    def _assert_provenance_readback(
        self, node_values: dict[str, Any], version_values: list[dict[str, Any]] | None,
        actual: dict[str, Any],
    ) -> None:
        attribute_keys = getattr(self.target, "provenance_attribute_keys", {}) or {}
        for field, expected in node_values.items():
            key = str(attribute_keys.get(field) or field)
            found, value = _find_mapping_value(actual, key)
            if not found or _metadata_value(value) != _metadata_value(expected):
                raise TerminalMigrationError(f"Provenance read-back mismatch for {field}")
        if version_values is None:
            return
        version_keys = getattr(self.target, "provenance_version_attribute_keys", {}) or {}
        row_key = str(getattr(self.target, "provenance_versions_field", "version_rows"))
        found, rows = _find_mapping_value(actual, row_key)
        if not found or not isinstance(rows, list) or len(rows) != len(version_values):
            raise TerminalMigrationError("Version provenance read-back row count mismatch")
        for expected_row, actual_row in zip(version_values, rows, strict=True):
            if not isinstance(actual_row, dict):
                raise TerminalMigrationError("Version provenance read-back row is not an object")
            for field, expected in expected_row.items():
                key = str(version_keys.get(field) or field)
                present, value = _find_mapping_value(actual_row, key)
                if not present or _metadata_value(value) != _metadata_value(expected):
                    raise TerminalMigrationError(
                        f"Version provenance read-back mismatch for {field}"
                    )

    def _process_owner_assignment(self, run_id: str, worker_id: str, item: dict[str, Any], mode: RunMode) -> None:
        """Final run-wide phase: reassign the target owner once the whole tree exists.

        Deferring this from _apply_node_policies avoids GX39 ACL inheritance
        breaking not-yet-created descendants (see finding on container owner
        reassignment racing with child creation).
        """
        node = self.manifest.source_node(item["source_id"])
        target_id = item.get("target_id")
        if target_id is None:
            mapping = self.manifest.lookup_mapping(node.source_id)
            target_id = mapping["target_id"] if mapping else None
        if target_id is None:
            raise TerminalMigrationError(f"No target mapping available for owner assignment on {node.source_id}")
        target_id = int(target_id)
        modern_methods = ("assign_owner", "read_owner")
        modern_contract = all(callable(getattr(self.target, name, None)) for name in modern_methods)
        if modern_contract:
            if node.owner_id is None:
                raise TerminalMigrationError(
                    f"Source node {node.source_id} has no owner identity for the owner/provenance contract"
                )
            resolution = self.manifest.run_owner_resolution(run_id, int(node.owner_id))
            if resolution.target_member_id is None:
                raise TerminalMigrationError(f"Source owner {node.owner_id} has no resolved target member")
            self.target.assign_owner(target_id, int(resolution.target_member_id))
            owner = self.target.read_owner(target_id)
            owner_id = _member_id_from_value(owner)
            if owner_id != int(resolution.target_member_id):
                raise TerminalMigrationError(
                    f"Target owner read-back mismatch for {node.source_id}: "
                    f"expected {resolution.target_member_id}, got {owner_id}"
                )
        self.manifest.mark_state(run_id, node.source_id, ItemState.VERIFIED, worker_id=worker_id, target_id=target_id)

    def _multipart_upload(
        self, run_id: str, node: SourceNode, version: SourceVersion, parent: int,
        migration_id: str, target_id: int | None,
    ):
        transfer = self.manifest.version_transfer(run_id, node.source_id, version.version_num)
        upload_key = transfer.get("upload_key")
        part_size = int(transfer.get("part_size") or self.target.multipart_part_size)
        next_part = int(transfer.get("next_part") or 1)
        if not upload_key:
            upload_key = self.target.start_multipart(version)
            next_part = 1
            self.manifest.update_version_transfer(
                run_id, node.source_id, version.version_num,
                state=ItemState.UPLOADING, upload_key=upload_key, next_part=1, part_size=part_size,
            )
        offset = (next_part - 1) * part_size
        if offset > version.size:
            raise TerminalMigrationError("Multipart checkpoint exceeds the declared source size")
        digest = hashlib.sha256()
        with self.binary_source.open(version) as stream:
            remaining = offset
            while remaining:
                chunk = stream.read(min(8 * 1024 * 1024, remaining))
                if not chunk:
                    raise TerminalMigrationError("Cannot reconstruct source hash before multipart checkpoint")
                digest.update(chunk)
                remaining -= len(chunk)
            part = next_part
            transferred = offset
            while transferred < version.size:
                chunk = stream.read(min(part_size, version.size - transferred))
                if not chunk:
                    raise TerminalMigrationError("Source stream ended before declared file size")
                digest.update(chunk)
                self.target.upload_multipart_part(upload_key, part, chunk, version.file_name)
                transferred += len(chunk)
                part += 1
                self.manifest.update_version_transfer(
                    run_id, node.source_id, version.version_num,
                    state=ItemState.UPLOADING, next_part=part, bytes_transferred=transferred,
                )
                if self.manifest.should_stop(run_id):
                    raise RetryableMigrationError("Stop requested at multipart checkpoint")
                self._pause_event.wait()
        result = self.target.complete_multipart(
            upload_key, node, version, parent, migration_id, existing_target_id=target_id
        )
        return result, digest.hexdigest()

    def _verify_document(
        self, run_id: str, node: SourceNode, target_id: int, versions: list[SourceVersion],
    ) -> None:
        for version in versions:
            transfer = self.manifest.version_transfer(run_id, node.source_id, version.version_num)
            expected = transfer.get("source_sha256")
            if not expected:
                raise TerminalMigrationError(f"Missing source hash for {node.source_id} v{version.version_num}")
            digest = hashlib.sha256()
            target_version = transfer.get("target_version_num") or version.version_num
            for chunk in self.target.iter_content(target_id, int(target_version)):
                if chunk:
                    digest.update(chunk)
            actual = digest.hexdigest()
            if actual != expected:
                raise TerminalMigrationError(
                    f"SHA-256 mismatch for {node.source_id} v{version.version_num}: {expected[:12]} != {actual[:12]}"
                )
            self.manifest.update_version_transfer(
                run_id, node.source_id, version.version_num,
                state=ItemState.VERIFIED, target_sha256=actual,
            )

    def _process_reference(self, run_id: str, worker_id: str, item: dict[str, Any], mode: RunMode) -> None:
        node = self.manifest.source_node(item["source_id"])
        if mode == RunMode.DRY_RUN:
            self._validate_node(node)
            self.manifest.mark_state(run_id, node.source_id, ItemState.SIMULATED, worker_id=worker_id)
            return
        parent = self.manifest.resolve_parent(node.parent_source_id, int(self.env_cfg.get("target_workspace_nodeid")))
        referenced_target = None
        reference_source = node.extra.get("reference_source_id")
        if reference_source:
            mapping = self.manifest.lookup_mapping(int(reference_source))
            if not mapping:
                raise TerminalMigrationError(f"Shortcut target {reference_source} has not been mapped")
            referenced_target = int(mapping["target_id"])
        migration_id = self._migration_id(node.source_id)
        try:
            target_id = self.target.create_reference(
                node, parent, migration_id, referenced_target_id=referenced_target
            )
        except AmbiguousRemoteCommit:
            target_id = self.target.find_by_migration_id(parent, migration_id)
            if not target_id:
                raise
        self.manifest.commit_mapping(run_id, node.source_id, target_id, parent, migration_id, worker_id=worker_id)
        categories = self.manifest.categories(node.source_id)
        if categories:
            self.target.apply_categories(target_id, categories)
        self._apply_node_policies(run_id, target_id, node)
        self._verify_metadata(run_id, target_id, node)
        properties = self.target.get_node(target_id)
        if int(properties.get("parent_id", -1)) != parent or properties.get("name") != node.name:
            raise TerminalMigrationError(f"Reference read-after-write mismatch for {node.source_id}")
        self._finish_item(run_id, node.source_id, worker_id=worker_id)

    def _try_reconcile_version(
        self, run_id: str, node: SourceNode, version: SourceVersion, target_id: int,
    ) -> bool:
        """Resolve an ambiguous prior commit without creating another version."""
        with self.binary_source.open(version) as raw:
            source_digest = hashlib.sha256()
            while True:
                chunk = raw.read(8 * 1024 * 1024)
                if not chunk:
                    break
                source_digest.update(chunk)
        source_hash = source_digest.hexdigest()
        self._assert_declared_hash(version, source_hash)
        transfer = self.manifest.version_transfer(run_id, node.source_id, version.version_num)
        target_version = int(transfer.get("target_version_num") or version.version_num)
        available_versions = {
            int(item["version_number"])
            for item in self.target.list_versions(target_id)
            if item.get("version_number") is not None
        }
        if target_version not in available_versions:
            return False
        target_digest = hashlib.sha256()
        for chunk in self.target.iter_content(target_id, target_version):
            if chunk:
                target_digest.update(chunk)
        target_hash = target_digest.hexdigest()
        if target_hash != source_hash:
            raise TerminalMigrationError(
                f"Existing migration marker for {node.source_id} points to non-matching content"
            )
        self.manifest.update_version_transfer(
            run_id, node.source_id, version.version_num,
            state=ItemState.VERIFIED, target_version_num=target_version,
            source_sha256=source_hash, target_sha256=target_hash,
            bytes_transferred=version.size,
        )
        return True

    @staticmethod
    def _validate_node(node: SourceNode) -> None:
        if not node.name or "\x00" in node.name:
            raise TerminalMigrationError(f"Invalid node name for {node.source_id}")
        if len(node.path) > 4096:
            raise TerminalMigrationError(f"Source path exceeds safety limit for {node.source_id}")

    def _validate_version_manifest(self, version: SourceVersion) -> None:
        if version.size < 0:
            raise TerminalMigrationError("Negative version size")
        adapter = str(self.env_cfg.get("binary_source_adapter", "azure")).lower()
        if adapter == "content_server":
            missing = [
                key for key in ("source_cs_url", "source_cs_user", "source_cs_password")
                if not self.env_cfg.get(key)
            ]
            if missing:
                raise TerminalMigrationError(
                    f"Missing source Content Server REST configuration: {', '.join(missing)}"
                )
        elif bool(self.settings.get("dry_run_require_blob_locator", True)) and not version.blob_locator:
            raise TerminalMigrationError(
                f"Missing blob locator for {version.source_id} v{version.version_num}"
            )

    @staticmethod
    def _assert_declared_hash(version: SourceVersion, actual: str) -> None:
        if version.source_sha256 and version.source_sha256.lower() != actual.lower():
            raise TerminalMigrationError(
                f"Source hash changed for {version.source_id} v{version.version_num}"
            )

    def _migration_id(self, source_id: int) -> str:
        namespace = str(self.env_cfg.get("migration_namespace", self.env_name)).strip()
        return f"CDM:{namespace}:{source_id}"

    def _config_fingerprint(self) -> str:
        safe = {
            "environment": self.env_name,
            "environment_class": self.env_cfg.get("environment_class"),
            "db_host": self.env_cfg.get("db_host"),
            "db_port": self.env_cfg.get("db_port"),
            "db_name": self.env_cfg.get("db_name"),
            "db_user": self.env_cfg.get("db_user"),
            "db_sslmode": self.env_cfg.get("sslmode"),
            "source_root": self.env_cfg.get("source_workspace_nodeid"),
            "binary_source_adapter": self.env_cfg.get("binary_source_adapter"),
            "source_cs_url": self.env_cfg.get("source_cs_url"),
            "azure_storage_account_url": self.env_cfg.get("azure_storage_account_url"),
            "azure_blob_locator_template": self.env_cfg.get("azure_blob_locator_template"),
            "target_root": self.env_cfg.get("target_workspace_nodeid"),
            "cloud_url": self.env_cfg.get("ot_cloud_url"),
            "migration_namespace": self.env_cfg.get("migration_namespace"),
            "migration_category_id": self.env_cfg.get("migration_category_id"),
            "migration_attribute_key": self.env_cfg.get("migration_attribute_key"),
            "provenance_category_id": self.env_cfg.get("provenance_category_id"),
            "provenance_attribute_keys": self.env_cfg.get("provenance_attribute_keys", {}),
            "provenance_version_attribute_keys": self.env_cfg.get(
                "provenance_version_attribute_keys", {}
            ),
            "owner_fallback": self.env_cfg.get("owner_fallback", {}),
            "owner_assignment_endpoint": self.env_cfg.get("owner_assignment_endpoint"),
            "member_lookup_endpoint": self.env_cfg.get("member_lookup_endpoint"),
            "service_account_login": self.env_cfg.get("service_account_login"),
            "service_account_email": self.env_cfg.get("service_account_email"),
            "category_mappings": self.env_cfg.get("category_mappings", {}),
            "workspace_routes": self.env_cfg.get("workspace_routes", {}),
            "permission_strategy": self.env_cfg.get("permission_strategy"),
            "permission_mappings": self.env_cfg.get("permission_mappings", {}),
            "settings": self.settings,
        }
        import json
        return hashlib.sha256(json.dumps(safe, sort_keys=True, default=str).encode()).hexdigest()

    def get_telemetry(self) -> dict[str, Any]:
        run = self.manifest.latest_run()
        inventory = self.manifest.inventory_summary()
        elapsed = max(0.001, time.monotonic() - self._started_monotonic) if self._started_monotonic else 0.0
        with self._metrics_lock:
            speed_mb = self._bytes / 1024 / 1024 / elapsed if elapsed else 0.0
            speed_files = self._processed / elapsed if elapsed else 0.0
            logs = list(self.recent_logs[-100:])
            active_workers = self._active_workers
        mode_label = "IDLE"
        if run:
            mode_label = {
                "dry_run": "DRY-RUN SIMULATION",
                "pilot": "REPRESENTATIVE PILOT",
                "full": "FULL CUTOVER",
                "verify_only": "VERIFY ONLY",
            }.get(run["mode"], run["mode"].upper())
        request_metrics = self.manifest.attempt_metrics(run["run_id"]) if run else {
            "request_count": 0, "throttled_count": 0, "server_error_count": 0,
            "network_error_count": 0, "last_request": None,
        }
        return {
            "status": run["status"] if run else "IDLE",
            "run_id": run["run_id"] if run else None,
            "execution_mode": mode_label,
            "dry_run": bool(run and run["mode"] == "dry_run"),
            "environment": self.env_name,
            **inventory,
            "total_folders": inventory.get("total_containers", 0),
            **({
                "success_nodes": run["verified_nodes"] + run["simulated_nodes"],
                "failed_nodes": run["failed_nodes"],
                "remote_committed_nodes": run.get("remote_committed_nodes", 0),
                "metadata_pending_nodes": run.get("metadata_pending_nodes", 0),
                "progress_percent": run["progress_percent"],
                "state_counts": run["state_counts"],
            } if run else {"success_nodes": 0, "failed_nodes": 0, "progress_percent": 0.0, "state_counts": {}}),
            "pending_nodes": max(
                0,
                inventory.get("total_nodes", 0)
                - ((run["verified_nodes"] + run["simulated_nodes"] + run["failed_nodes"]) if run else 0),
            ),
            "transferred_bytes": self._bytes,
            "configured_threads": int(self.settings.get("worker_threads", 8)),
            "active_workers": active_workers,
            "speed_mb_per_sec": round(speed_mb, 2),
            "speed_files_per_sec": round(speed_files, 2),
            "http": request_metrics,
            "rate_limit_rps": (
                round(float(self._target.rate_limiter.rate), 2)
                if self._target is not None and hasattr(self._target, "rate_limiter") else None
            ),
            "freeze": self.manifest.freeze_status(),
            "logs": logs,
        }


def _member_id_from_value(value: Any) -> int | None:
    if isinstance(value, dict):
        for key in ("id", "member_id", "memberid", "user_id", "userid", "owner_id"):
            if value.get(key) is not None:
                try:
                    return int(value[key])
                except (TypeError, ValueError):
                    return None
        for nested in value.values():
            found = _member_id_from_value(nested)
            if found is not None:
                return found
    return None


def _find_mapping_value(value: Any, key: str) -> tuple[bool, Any]:
    if isinstance(value, dict):
        if key in value:
            return True, value[key]
        for nested in value.values():
            found, result = _find_mapping_value(nested, key)
            if found:
                return True, result
    return False, None


def _metadata_value(value: Any) -> Any:
    if isinstance(value, str):
        try:
            normalized = utc_iso(value)
        except TerminalMigrationError:
            normalized = None
        return normalized or value.strip()
    if isinstance(value, (dict, list)):
        import json
        return json.dumps(value, sort_keys=True, default=str)
    return value
