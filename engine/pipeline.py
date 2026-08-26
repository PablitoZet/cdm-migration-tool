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
        if self._target is not None and hasattr(self._target, "close"):
            self._target.close()
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

    def confirm_source_freeze(self, operator: str, note: str = "") -> dict[str, Any]:
        root = int(self.env_cfg.get("source_workspace_nodeid"))
        observed = self.source_db.scope_signature(root)
        return self.manifest.confirm_source_freeze(observed, operator, note)

    def start_migration(
        self, max_items: int | None = None, dry_run: bool = False, threads: int | None = None,
        *, mode: str | None = None,
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
            if run_mode != RunMode.DRY_RUN and self.env_cfg.get("ot_cloud_url"):
                from .preflight import PreflightAuditor

                report = PreflightAuditor(
                    self.env_cfg, self.manifest, self.settings
                ).run(online=False, for_mode=str(run_mode))
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
        self._apply_node_policies(target_id, node)
        properties = self.target.get_node(target_id)
        if int(properties.get("parent_id", -1)) != parent or properties.get("name") != node.name:
            raise TerminalMigrationError(f"Container read-after-write mismatch for {node.source_id}")
        self.manifest.mark_state(run_id, node.source_id, ItemState.VERIFIED, worker_id=worker_id)
        self.manifest.mark_mapping_verified(node.source_id)

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
        target_id = self.target.find_by_migration_id(parent, migration_id)
        for index, version in enumerate(versions):
            transfer = self.manifest.version_transfer(run_id, node.source_id, version.version_num)
            if transfer["state"] == ItemState.VERIFIED:
                target_id = target_id or item.get("target_id")
                continue
            self.binary_source.validate(version)
            if target_id is not None:
                reconciled = self._try_reconcile_version(run_id, node, version, target_id)
                if reconciled:
                    continue
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
            with self._metrics_lock:
                self._bytes += version.size
        if target_id is None:
            raise TerminalMigrationError("Document did not resolve to a target ID")
        self.manifest.commit_mapping(run_id, node.source_id, target_id, parent, migration_id, worker_id=worker_id)
        self._apply_version_policies(run_id, target_id, node, versions)
        categories = self.manifest.categories(node.source_id)
        if categories:
            self.target.apply_categories(target_id, categories)
        self._apply_node_policies(target_id, node)
        self.manifest.mark_state(
            run_id, node.source_id, ItemState.METADATA_APPLIED,
            worker_id=worker_id, target_id=target_id, target_parent_id=parent,
            bytes_transferred=sum(v.size for v in versions),
        )
        if bool(self.settings.get("verify_sha256", True)):
            self._verify_document(run_id, node, target_id, versions)
        self.manifest.mark_state(run_id, node.source_id, ItemState.VERIFIED, target_id=target_id)
        self.manifest.mark_mapping_verified(node.source_id)

    def _apply_node_policies(self, target_id: int, node: SourceNode) -> None:
        if self.env_cfg.get("system_attribute_strategy") == "preserve":
            self.target.apply_system_attributes(target_id, node)
        if self.env_cfg.get("permission_strategy") == "mapped_acl":
            policies = self.env_cfg.get("permission_mappings", {}) or {}
            policy = policies.get(str(node.permissions_id))
            if policy is None:
                raise TerminalMigrationError(
                    f"No target ACL policy for source PermID {node.permissions_id}"
                )
            self.target.apply_permission_policy(target_id, policy)

    def _apply_version_policies(
        self, run_id: str, target_id: int, node: SourceNode, versions: list[SourceVersion],
    ) -> None:
        if self.env_cfg.get("system_attribute_strategy") != "preserve":
            return
        for version in versions:
            transfer = self.manifest.version_transfer(run_id, node.source_id, version.version_num)
            target_version = int(transfer.get("target_version_num") or version.version_num)
            self.target.apply_system_attributes(target_id, version, version_num=target_version)

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
        digest = hashlib.sha256()
        if offset:
            with self.binary_source.open(version) as prefix:
                remaining = offset
                while remaining:
                    chunk = prefix.read(min(8 * 1024 * 1024, remaining))
                    if not chunk:
                        raise TerminalMigrationError("Cannot reconstruct source hash before multipart checkpoint")
                    digest.update(chunk)
                    remaining -= len(chunk)
        with self.binary_source.open(version, offset=offset) as stream:
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
        self._apply_node_policies(target_id, node)
        properties = self.target.get_node(target_id)
        if int(properties.get("parent_id", -1)) != parent or properties.get("name") != node.name:
            raise TerminalMigrationError(f"Reference read-after-write mismatch for {node.source_id}")
        self.manifest.mark_state(run_id, node.source_id, ItemState.VERIFIED, worker_id=worker_id)
        self.manifest.mark_mapping_verified(node.source_id)

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
        target_digest = hashlib.sha256()
        try:
            for chunk in self.target.iter_content(target_id, version.version_num):
                if chunk:
                    target_digest.update(chunk)
        except TerminalMigrationError as exc:
            if "HTTP 404" in str(exc):
                return False
            raise
        target_hash = target_digest.hexdigest()
        if target_hash != source_hash:
            raise TerminalMigrationError(
                f"Existing migration marker for {node.source_id} points to non-matching content"
            )
        self.manifest.update_version_transfer(
            run_id, node.source_id, version.version_num,
            state=ItemState.VERIFIED, target_version_num=version.version_num,
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
        if bool(self.settings.get("dry_run_require_blob_locator", True)) and not version.blob_locator:
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
            "source_root": self.env_cfg.get("source_workspace_nodeid"),
            "target_root": self.env_cfg.get("target_workspace_nodeid"),
            "cloud_url": self.env_cfg.get("ot_cloud_url"),
            "migration_namespace": self.env_cfg.get("migration_namespace"),
            "migration_category_id": self.env_cfg.get("migration_category_id"),
            "migration_attribute_key": self.env_cfg.get("migration_attribute_key"),
            "category_mappings": self.env_cfg.get("category_mappings", {}),
            "workspace_routes": self.env_cfg.get("workspace_routes", {}),
            "permission_strategy": self.env_cfg.get("permission_strategy"),
            "permission_mappings": self.env_cfg.get("permission_mappings", {}),
            "system_attribute_strategy": self.env_cfg.get("system_attribute_strategy"),
            "owner_mappings": self.env_cfg.get("owner_mappings", {}),
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
