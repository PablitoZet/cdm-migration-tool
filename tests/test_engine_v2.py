from __future__ import annotations

import io
import os
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from engine.client import MultipartStream, OpenTextCloudClient, _TokenManager
from engine.config import (
    ConfigurationError,
    EnvironmentConfig,
    load_config,
    normalize_profile_values,
    save_config,
)
from engine.db import SourceDB
from engine.instance_lock import InstanceAlreadyRunning
from engine.manifest import ManifestStore, StateConflict
from engine.models import (
    ItemState,
    RetryableMigrationError,
    RunMode,
    RunStatus,
    SourceVersion,
    TerminalMigrationError,
    UploadResult,
)
from engine.pipeline import MigrationPipeline
from engine.preflight import PreflightAuditor
from engine.reconciler import AutomatedVerifier
from engine.source import AzureBlobBinarySource, ContentServerBinarySource, LocalBinarySource


class FakeTarget:
    multipart_threshold = 1024 * 1024
    multipart_part_size = 128 * 1024

    def __init__(self):
        self.next_id = 10000
        self.nodes = {9000: {"id": 9000, "name": "target-root", "parent_id": 2000, "type": 0}}
        self.contents = {}
        self.markers = {}
        self.system_attribute_calls = []
        self.permission_policy_calls = []
        self.first_version_uploads = 0
        self.next_version_uploads = 0

    def create_container(self, node, parent_id, migration_id):
        self.next_id += 1
        self.nodes[self.next_id] = {
            "id": self.next_id, "name": node.name, "parent_id": parent_id, "type": node.subtype,
        }
        self.markers[(parent_id, migration_id)] = self.next_id
        return self.next_id

    def find_by_migration_id(self, parent_id, migration_id):
        return self.markers.get((parent_id, migration_id))

    def apply_migration_marker(self, target_id, migration_id):
        node = self.nodes[target_id]
        self.markers[(node["parent_id"], migration_id)] = target_id

    def get_node(self, target_id):
        return self.nodes[target_id]

    def list_versions(self, target_id):
        return [
            {"version_number": version_num}
            for item_target, version_num in self.contents
            if item_target == target_id
        ]

    def apply_categories(self, target_id, categories):
        return None

    def apply_system_attributes(self, target_id, source, *, version_num=None):
        self.system_attribute_calls.append((target_id, source.source_id, version_num))

    def apply_permission_policy(self, target_id, policy):
        self.permission_policy_calls.append((target_id, policy))

    def upload_first_version(self, node, version, parent_id, stream, migration_id):
        self.first_version_uploads += 1
        self.next_id += 1
        data = _read_all(stream)
        self.nodes[self.next_id] = {
            "id": self.next_id, "name": node.name, "parent_id": parent_id, "type": 144,
        }
        self.contents[(self.next_id, version.version_num)] = data
        self.markers[(parent_id, migration_id)] = self.next_id
        return UploadResult(self.next_id, version.version_num)

    def upload_next_version(self, target_id, version, stream):
        self.next_version_uploads += 1
        self.contents[(target_id, version.version_num)] = _read_all(stream)
        return UploadResult(target_id, version.version_num)

    def iter_content(self, target_id, version_num=None):
        data = self.contents[(target_id, version_num or 1)]
        for start in range(0, len(data), 7):
            yield data[start:start + 7]

    def create_reference(self, node, parent_id, migration_id, referenced_target_id=None):
        self.next_id += 1
        self.nodes[self.next_id] = {
            "id": self.next_id, "name": node.name, "parent_id": parent_id, "type": node.subtype,
        }
        return self.next_id


class AmbiguousOnceTarget(FakeTarget):
    def __init__(self):
        super().__init__()
        self.raised = False

    def upload_first_version(self, node, version, parent_id, stream, migration_id):
        result = super().upload_first_version(node, version, parent_id, stream, migration_id)
        if not self.raised:
            self.raised = True
            from engine.models import AmbiguousRemoteCommit
            raise AmbiguousRemoteCommit("response lost after remote commit")
        return result


def _read_all(stream):
    chunks = []
    while True:
        part = stream.read(13)
        if not part:
            return b"".join(chunks)
        chunks.append(part)


def _inventory(binary_path: Path):
    nodes = [
        {"source_id": 1, "parent_source_id": 999, "name": "source-root", "subtype": 0,
         "type_name": "Folder", "depth": 0, "path": "source-root"},
        {"source_id": 2, "parent_source_id": 1, "name": "child", "subtype": 0,
         "type_name": "Folder", "depth": 1, "path": "source-root/child"},
        {"source_id": 3, "parent_source_id": 2, "name": "document.txt", "subtype": 144,
         "type_name": "Document", "depth": 2, "path": "source-root/child/document.txt"},
    ]
    versions = [
        {"doc_source_id": 3, "version_num": 1, "file_name": "document.txt", "mime_type": "text/plain",
         "data_size": binary_path.stat().st_size, "provider_id": 1, "blob_locator": str(binary_path)},
    ]
    return nodes, versions


def _config():
    return {
        "default_environment": "dev",
        "environments": {"dev": {
            "target_workspace_nodeid": 9000,
            "binary_source_adapter": "local",
            "source_root_maps_to_target": True,
            "migration_namespace": "test",
            "migration_category_id": 1,
            "migration_attribute_key": "1_2",
            "permission_strategy": "inherit_target",
            "system_attribute_strategy": "accept_target_generated",
        }},
        "migration_settings": {
            "worker_threads": 2, "max_worker_threads": 4, "verify_sha256": True,
            "dry_run_require_blob_locator": True, "max_item_attempts": 2,
        },
    }


class _VersionCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, query, params):
        if "information_schema.columns" in query:
            self.rows = [(1,)] if tuple(params) in self.connection.columns else []
        else:
            self.connection.version_query = query
            self.rows = self.connection.version_rows

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class _VersionConnection:
    columns = {
        ("providerdata", "providerid"),
        ("providerdata", "providerdata"),
        ("providerdata", "providertype"),
        ("dversdata", "vercomment"),
        ("dversdata", "versionid"),
        ("dversdata", "vertype"),
        ("dversdata", "transient"),
    }

    def __init__(self, version_rows):
        self.version_rows = version_rows
        self.version_query = ""

    def cursor(self, **_kwargs):
        return _VersionCursor(self)


class _TestSourceDB(SourceDB):
    def _cursor(self, conn):
        return conn.cursor()


def _version_row(provider_data="blob/content.bin", provider_type="azureblob"):
    return {
        "doc_source_id": 3,
        "version_num": 1,
        "file_name": "content.bin",
        "mime_type": "application/octet-stream",
        "data_size": 10,
        "provider_id": 42,
        "provider_type": provider_type,
        "provider_data": provider_data,
        "version_id": 7,
        "ver_create_date": None,
        "ver_modify_date": None,
        "ver_file_date": None,
        "version_comment": None,
    }


class SourceExtractionTests(unittest.TestCase):
    def test_primary_versions_join_provider_data_and_exclude_renditions(self):
        connection = _VersionConnection([_version_row()])
        rows = _TestSourceDB({
            "azure_blob_locator_template": "azure://content/{provider_data}",
        })._extract_versions(connection, [3])

        self.assertIn("LEFT JOIN public.ProviderData p ON p.ProviderID=d.ProviderID", connection.version_query)
        self.assertIn("(d.VerType IS NULL OR d.VerType='')", connection.version_query)
        self.assertIn("COALESCE(d.Transient,0)=0", connection.version_query)
        self.assertEqual(rows[0]["blob_locator"], "azure://content/blob/content.bin")

    def test_missing_provider_data_does_not_create_a_false_locator(self):
        rows = _TestSourceDB({
            "azure_blob_locator_template": "azure://content/{provider_data}",
        })._extract_versions(_VersionConnection([_version_row(None)]), [3])
        self.assertIsNone(rows[0]["blob_locator"])

    def test_archive_center_descriptor_requires_a_qualified_binary_adapter(self):
        descriptor = "A<1,?,'providerInfo'='ixos://archive@host','storageProviderName'='store','subProviderName'='primary'>"
        rows = _TestSourceDB({
            "azure_blob_locator_template": "azure://content/{provider_data}",
        })._extract_versions(_VersionConnection([_version_row(descriptor, "acprimary")]), [3])
        self.assertIsNone(rows[0]["blob_locator"])

    def test_duplicate_primary_version_identity_fails_closed(self):
        connection = _VersionConnection([_version_row(), {**_version_row(), "file_name": "other.bin"}])
        with self.assertRaisesRegex(RuntimeError, "duplicate primary DVersData"):
            _TestSourceDB({})._extract_versions(connection, [3])


class ManifestTests(unittest.TestCase):
    def test_empty_manifest_readiness_is_not_reported_as_passed(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ManifestStore(str(Path(tmp) / "state.db"))
            report = store.parity_report(EnvironmentConfig("dev", {}))
            self.assertEqual(report["status"], "NOT_CHECKED")
            self.assertEqual(report["failure_count"], 0)
            self.assertTrue(report["checks"])
            self.assertTrue(all(check["status"] == "NOT_CHECKED" for check in report["checks"]))
            store.close()

    def test_single_instance_lock_and_freeze_signature(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "state.db")
            store = ManifestStore(path)
            with self.assertRaises(InstanceAlreadyRunning):
                ManifestStore(path)
            content = Path(tmp) / "content.bin"
            content.write_bytes(b"freeze")
            nodes, versions = _inventory(content)
            store.import_extracted_data(nodes, versions, [])
            signature = store.metadata()["inventory_signature"]
            with self.assertRaises(StateConflict):
                store.confirm_source_freeze("0" * 64, "operator")
            freeze = store.confirm_source_freeze(signature, "operator", "CHG-123")
            self.assertTrue(freeze["confirmed"])
            store.close()
            reopened = ManifestStore(path)
            reopened.close()

    def test_parity_contract_requires_explicit_operational_qualification(self):
        with tempfile.TemporaryDirectory() as tmp:
            content = Path(tmp) / "content.bin"
            content.write_bytes(b"parity")
            store = ManifestStore(str(Path(tmp) / "state.db"))
            nodes, versions = _inventory(content)
            store.import_extracted_data(nodes, versions, [])
            incomplete = _config()["environments"]["dev"]
            incomplete_report = store.parity_report(incomplete)
            self.assertEqual(incomplete_report["status"], "FAIL")
            owner_check = next(
                check for check in incomplete_report["checks"]
                if check["id"] == "SYSTEM_ATTRIBUTE_PARITY"
            )
            self.assertIn("unmapped_source_owner_ids=", owner_check["detail"])
            qualified = {
                **incomplete,
                "permission_strategy": "inherit_target",
                "target_acl_approved": True,
                "system_attribute_strategy": "preserve",
                "owner_mappings": {},
                "workspace_roles_qualified": True,
                "lifecycle_operations_qualified": True,
                "search_and_facets_qualified": True,
                "active_workflows_confirmed_zero": True,
                "legacy_links_qualified": True,
                "historical_audit_out_of_scope_approved": True,
                "personal_state_out_of_scope_approved": True,
            }
            self.assertEqual(store.parity_report(qualified)["status"], "PASS")
            store.close()

    def test_pilot_contains_only_selected_documents_and_ancestors(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ManifestStore(str(Path(tmp) / "state.db"))
            nodes = [
                {"source_id": 1, "parent_source_id": 999, "name": "root", "subtype": 0,
                 "type_name": "Folder", "depth": 0, "path": "root"},
                {"source_id": 2, "parent_source_id": 1, "name": "a", "subtype": 0,
                 "type_name": "Folder", "depth": 1, "path": "root/a"},
                {"source_id": 3, "parent_source_id": 1, "name": "unused", "subtype": 0,
                 "type_name": "Folder", "depth": 1, "path": "root/unused"},
                {"source_id": 4, "parent_source_id": 2, "name": "doc", "subtype": 144,
                 "type_name": "Document", "depth": 2, "path": "root/a/doc"},
                {"source_id": 5, "parent_source_id": 3, "name": "doc2", "subtype": 144,
                 "type_name": "Document", "depth": 2, "path": "root/unused/doc2"},
            ]
            versions = [
                {"doc_source_id": 4, "version_num": 1, "file_name": "a", "mime_type": "x", "data_size": 1},
                {"doc_source_id": 5, "version_num": 1, "file_name": "b", "mime_type": "x", "data_size": 1},
            ]
            store.import_extracted_data(nodes, versions, [])
            run_id = store.create_run("dev", RunMode.PILOT, 9000, max_documents=1)
            self.assertEqual(store.run_summary(run_id)["total_nodes"], 3)
            store.close()

    def test_retry_limit_and_parent_dependency(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ManifestStore(str(Path(tmp) / "state.db"))
            nodes = [
                {"source_id": 1, "parent_source_id": 999, "name": "root", "subtype": 0,
                 "type_name": "Folder", "depth": 0, "path": "root"},
                {"source_id": 2, "parent_source_id": 1, "name": "child", "subtype": 0,
                 "type_name": "Folder", "depth": 1, "path": "root/child"},
            ]
            store.import_extracted_data(nodes, [], [])
            run_id = store.create_run("dev", RunMode.FULL, 9000)
            store.start_run(run_id)
            item = store.claim_next(run_id, "CONTAINER", "worker")
            self.assertEqual(item["source_id"], 1)
            self.assertEqual(store.record_failure(run_id, 1, "temporary", max_attempts=2), ItemState.RETRY_WAIT)
            with self.assertRaises(StateConflict):
                store.resolve_parent(1, 9000)
            store.close()

    def test_checkpoint_survives_recovery(self):
        with tempfile.TemporaryDirectory() as tmp:
            content = Path(tmp) / "content.bin"
            content.write_bytes(b"abc")
            store = ManifestStore(str(Path(tmp) / "state.db"))
            nodes, versions = _inventory(content)
            store.import_extracted_data(nodes, versions, [])
            run_id = store.create_run("dev", RunMode.FULL, 9000)
            store.start_run(run_id)
            store.update_version_transfer(
                run_id, 3, 1, state=ItemState.UPLOADING, upload_key="key-1", next_part=7, part_size=16
            )
            store.finish_run(run_id, RunStatus.STOPPED)
            store.recover_run(run_id)
            checkpoint = store.version_transfer(run_id, 3, 1)
            self.assertEqual(checkpoint["upload_key"], "key-1")
            self.assertEqual(checkpoint["next_part"], 7)
            store.close()

    def test_recovery_requeues_only_retryable_terminal_failures(self):
        with (
            tempfile.TemporaryDirectory() as tmp,
            closing(ManifestStore(str(Path(tmp) / "state.db"))) as store,
        ):
            nodes = [
                {"source_id": 1, "parent_source_id": 999, "name": "one", "subtype": 0,
                 "type_name": "Folder", "depth": 0, "path": "one"},
                {"source_id": 2, "parent_source_id": 999, "name": "two", "subtype": 0,
                 "type_name": "Folder", "depth": 0, "path": "two"},
            ]
            store.import_extracted_data(nodes, [], [])
            run_id = store.create_run("dev", RunMode.FULL, 9000)
            store.start_run(run_id)
            retryable = store.claim_next(run_id, "CONTAINER", "worker")
            self.assertIsNotNone(retryable)
            store.record_failure(
                run_id, retryable["source_id"], "ambiguous",
                max_attempts=1, retryable=True, error_code="AmbiguousRemoteCommit",
            )
            terminal = store.claim_next(run_id, "CONTAINER", "worker")
            self.assertIsNotNone(terminal)
            store.record_failure(
                run_id, terminal["source_id"], "invalid",
                max_attempts=1, retryable=False, error_code="TerminalMigrationError",
            )
            store.finish_run(run_id, RunStatus.COMPLETED_WITH_ERRORS)

            store.recover_run(run_id)

            with store.connection() as conn:
                states = {
                    row["source_id"]: (row["state"], row["attempt_count"])
                    for row in conn.execute(
                        "SELECT source_id,state,attempt_count FROM run_items WHERE run_id=?",
                        (run_id,),
                    )
                }
            self.assertEqual(states[retryable["source_id"]], (ItemState.RETRY_WAIT, 0))
            self.assertEqual(states[terminal["source_id"]], (ItemState.FAILED_TERMINAL, 1))


class PipelineTests(unittest.TestCase):
    def test_recovery_online_preflight_failure_does_not_mutate_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = _config()
            config["environments"]["dev"]["ot_cloud_url"] = "https://target.example.invalid/cs/cs"
            with closing(MigrationPipeline(
                config, str(Path(tmp) / "state.db"),
                target=FakeTarget(), binary_source=LocalBinarySource(),
            )) as pipeline:
                nodes = [{
                    "source_id": 1, "parent_source_id": 999, "name": "root",
                    "subtype": 0, "type_name": "Folder", "depth": 0, "path": "root",
                }]
                pipeline.manifest.import_extracted_data(nodes, [], [])
                run_id = pipeline.manifest.create_run(
                    "dev", RunMode.PILOT, 9000,
                    config_fingerprint=pipeline._config_fingerprint(),
                )
                pipeline.manifest.finish_run(run_id, RunStatus.STOPPED)
                with (
                    patch(
                        "engine.preflight.PreflightAuditor.run",
                        return_value={
                            "status": "FAIL",
                            "checks": [{
                                "id": "TARGET_SYSTEM_ATTRIBUTE_PRESERVATION",
                                "status": "FAIL",
                            }],
                        },
                    ),
                    self.assertRaisesRegex(
                        TerminalMigrationError,
                        "TARGET_SYSTEM_ATTRIBUTE_PRESERVATION",
                    ),
                ):
                    pipeline.recover_run(run_id, 1)
                self.assertEqual(
                    pipeline.manifest.run_status(run_id)["status"],
                    RunStatus.STOPPED,
                )

    def test_replacement_run_reconciles_ready_versions_without_duplicate_upload(self):
        with tempfile.TemporaryDirectory() as tmp:
            content = Path(tmp) / "document.txt"
            content.write_bytes(b"existing target content")
            target = FakeTarget()
            with closing(MigrationPipeline(
                _config(), str(Path(tmp) / "state.db"),
                target=target, binary_source=LocalBinarySource(),
            )) as pipeline:
                nodes, versions = _inventory(content)
                pipeline.manifest.import_extracted_data(nodes, versions, [])
                first_run = pipeline.start_migration(threads=1, mode="full")
                self.assertTrue(pipeline.wait(5))
                self.assertEqual(
                    pipeline.manifest.run_status(first_run)["status"],
                    RunStatus.COMPLETED,
                )
                with pipeline.manifest.transaction(immediate=True) as conn:
                    conn.execute("UPDATE id_mapping SET verified=0 WHERE source_id=3")
                upload_count = target.first_version_uploads

                replacement = pipeline.start_migration(threads=1, mode="full")
                self.assertTrue(pipeline.wait(5))

                self.assertEqual(
                    pipeline.manifest.run_status(replacement)["status"],
                    RunStatus.COMPLETED,
                )
                self.assertEqual(target.first_version_uploads, upload_count)

    def test_document_mapping_and_marker_are_durable_before_later_versions(self):
        class FailingNextVersionTarget(FakeTarget):
            def upload_first_version(self, node, version, parent_id, stream, migration_id):
                result = super().upload_first_version(node, version, parent_id, stream, migration_id)
                self.markers.pop((parent_id, migration_id), None)
                return result

            def upload_next_version(self, target_id, version, stream):
                raise TerminalMigrationError("later version unavailable")

        with tempfile.TemporaryDirectory() as tmp:
            first = Path(tmp) / "v1.txt"
            second = Path(tmp) / "v2.txt"
            first.write_bytes(b"one")
            second.write_bytes(b"two")
            config = _config()
            target = FailingNextVersionTarget()
            with closing(MigrationPipeline(
                config, str(Path(tmp) / "state.db"),
                target=target, binary_source=LocalBinarySource(),
            )) as pipeline:
                nodes, versions = _inventory(first)
                versions.append({
                    **versions[0], "version_num": 2, "blob_locator": str(second),
                    "data_size": second.stat().st_size,
                })
                pipeline.manifest.import_extracted_data(nodes, versions, [])
                pipeline.start_migration(threads=1, mode="full")
                self.assertTrue(pipeline.wait(5))
                mapping = pipeline.manifest.lookup_mapping(3)
                parent_mapping = pipeline.manifest.lookup_mapping(2)
                self.assertIsNotNone(mapping)
                self.assertIsNotNone(parent_mapping)
                self.assertEqual(
                    target.find_by_migration_id(parent_mapping["target_id"], "CDM:test:3"),
                    mapping["target_id"],
                )

    def test_pipeline_close_releases_manifest_when_source_close_fails(self):
        class Source:
            def close(self):
                raise OSError("logout failed")

        with tempfile.TemporaryDirectory() as tmp:
            state = str(Path(tmp) / "state.db")
            pipeline = MigrationPipeline(
                _config(), state, target=FakeTarget(), binary_source=Source(),
            )
            with self.assertRaisesRegex(OSError, "logout failed"):
                pipeline.close()
            reopened = ManifestStore(state)
            reopened.close()

    def test_recovery_fingerprint_binds_non_secret_binary_source_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = _config()
            base["environments"]["dev"].update({
                "db_host": "db-a.example.invalid",
                "db_port": 5432,
                "db_name": "cs",
                "db_user": "reader",
                "sslmode": "require",
                "binary_source_adapter": "content_server",
                "source_cs_url": "https://source-a.example.invalid/cs/cs",
            })
            changed = {
                **base,
                "environments": {
                    "dev": {
                        **base["environments"]["dev"],
                        "source_cs_url": "https://source-b.example.invalid/cs/cs",
                    }
                },
            }
            with closing(MigrationPipeline(
                base, str(Path(tmp) / "one.db"),
                target=FakeTarget(), binary_source=LocalBinarySource(),
            )) as first, closing(MigrationPipeline(
                changed, str(Path(tmp) / "two.db"),
                target=FakeTarget(), binary_source=LocalBinarySource(),
            )) as second:
                self.assertNotEqual(first._config_fingerprint(), second._config_fingerprint())

    def test_dry_run_rejects_missing_owner_mapping_before_creating_a_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            content = Path(tmp) / "document.txt"
            content.write_bytes(b"owner mapping")
            config = _config()
            config["environments"]["dev"].update({
                "source_workspace_nodeid": 1,
                "system_attribute_strategy": "preserve",
                "owner_mappings": {},
            })
            with closing(MigrationPipeline(
                config, str(Path(tmp) / "state.db"),
                target=FakeTarget(), binary_source=LocalBinarySource(),
            )) as pipeline:
                nodes, versions = _inventory(content)
                for node in nodes:
                    node["owner_id"] = 42
                pipeline.manifest.import_extracted_data(
                    nodes, versions, [], source_root_id=1, source_profile_id="dev",
                )
                with self.assertRaisesRegex(TerminalMigrationError, "OWNER_MAPPING_COVERAGE"):
                    pipeline.start_migration(dry_run=True, threads=1, mode="dry_run")
                self.assertIsNone(pipeline.manifest.latest_run())

    def test_multipart_recovery_replays_prefix_without_requiring_range(self):
        class Source:
            def __init__(self, content):
                self.content = content
                self.offsets = []

            def open(self, _version, *, offset=0):
                self.offsets.append(offset)
                if offset:
                    raise AssertionError("Pipeline must not require source Range support")
                return io.BytesIO(self.content)

            def validate(self, _version):
                pass

        class Target(FakeTarget):
            multipart_threshold = 1
            multipart_part_size = 4

            def __init__(self):
                super().__init__()
                self.parts = []

            def upload_multipart_part(self, upload_key, part_number, data, file_name):
                self.parts.append((upload_key, part_number, data, file_name))

            def complete_multipart(
                self, upload_key, node, version, parent_id, migration_id, *, existing_target_id=None,
            ):
                return UploadResult(existing_target_id or 12345, version.version_num)

        with tempfile.TemporaryDirectory() as tmp:
            content = b"0123456789"
            path = Path(tmp) / "document.bin"
            path.write_bytes(content)
            source = Source(content)
            target = Target()
            with closing(MigrationPipeline(
                _config(), str(Path(tmp) / "state.db"), target=target, binary_source=source,
            )) as pipeline:
                nodes, versions = _inventory(path)
                versions[0]["data_size"] = len(content)
                pipeline.manifest.import_extracted_data(nodes, versions, [])
                run_id = pipeline.manifest.create_run("dev", RunMode.FULL, 9000)
                pipeline.manifest.update_version_transfer(
                    run_id, 3, 1, state=ItemState.UPLOADING,
                    upload_key="upload-1", next_part=2, part_size=4,
                )
                node = pipeline.manifest.source_node(3)
                version = pipeline.manifest.source_versions(3)[0]
                _result, source_hash = pipeline._multipart_upload(
                    run_id, node, version, 9000, "CDM:test:3", None,
                )
                self.assertEqual(source.offsets, [0])
                self.assertEqual(
                    [(part_number, data) for _, part_number, data, _ in target.parts],
                    [(2, b"4567"), (3, b"89")],
                )
                self.assertEqual(source_hash, __import__("hashlib").sha256(content).hexdigest())

    def test_production_full_run_requires_freeze_confirmation(self):
        with tempfile.TemporaryDirectory() as tmp:
            content = Path(tmp) / "document.txt"
            content.write_bytes(b"production")
            config = _config()
            config["environments"]["dev"]["environment_class"] = "production"
            with closing(MigrationPipeline(
                config, str(Path(tmp) / "state.db"),
                target=FakeTarget(), binary_source=LocalBinarySource(),
            )) as pipeline:
                nodes, versions = _inventory(content)
                pipeline.manifest.import_extracted_data(nodes, versions, [])
                with self.assertRaisesRegex(Exception, "source read-only freeze"):
                    pipeline.start_migration(threads=1, mode="full")

    def test_preserve_strategy_applies_node_and_version_system_attributes(self):
        with tempfile.TemporaryDirectory() as tmp:
            content = Path(tmp) / "document.txt"
            content.write_bytes(b"dated version")
            config = _config()
            config["environments"]["dev"]["system_attribute_strategy"] = "preserve"
            config["environments"]["dev"]["owner_mappings"] = {"42": 4200}
            target = FakeTarget()
            with closing(MigrationPipeline(
                config, str(Path(tmp) / "state.db"),
                target=target, binary_source=LocalBinarySource(),
            )) as pipeline:
                nodes, versions = _inventory(content)
                for node in nodes:
                    node.update({"source_created_at": "2024-01-01T00:00:00Z", "owner_id": 42})
                versions[0]["source_created_at"] = "2024-01-02T00:00:00Z"
                pipeline.manifest.import_extracted_data(nodes, versions, [])

                run_id = pipeline.start_migration(threads=2, mode="full")
                self.assertTrue(pipeline.wait(5))
                self.assertEqual(pipeline.manifest.run_status(run_id)["status"], RunStatus.COMPLETED)
                version_calls = [call for call in target.system_attribute_calls if call[2] is not None]
                self.assertEqual(len(version_calls), 1)
                self.assertEqual(version_calls[0][2], 1)

    def test_dry_run_isolated_then_full_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            content = Path(tmp) / "document.txt"
            content.write_bytes(b"production-like bytes\x00with binary")
            config = _config()
            config["environments"]["dev"]["source_workspace_nodeid"] = 1
            with closing(MigrationPipeline(
                config, str(Path(tmp) / "state.db"),
                target=FakeTarget(), binary_source=LocalBinarySource(),
            )) as pipeline:
                nodes, versions = _inventory(content)
                pipeline.manifest.import_extracted_data(
                    nodes, versions, [], source_root_id=1, source_profile_id="dev",
                )

                dry_id = pipeline.start_migration(dry_run=True, threads=2, mode="dry_run")
                self.assertTrue(pipeline.wait(5))
                dry = pipeline.manifest.run_status(dry_id)
                self.assertEqual(dry["simulated_nodes"], 3)
                self.assertIsNone(pipeline.manifest.lookup_mapping(1))

                full_id = pipeline.start_migration(threads=2, mode="full")
                self.assertTrue(pipeline.wait(5))
                full = pipeline.manifest.run_status(full_id)
                self.assertEqual(full["status"], RunStatus.COMPLETED)
                self.assertEqual(full["verified_nodes"], 3)
                self.assertEqual(pipeline.manifest.lookup_mapping(3)["name"], "document.txt")
                transfer = pipeline.manifest.version_transfer(full_id, 3, 1)
                self.assertEqual(transfer["state"], ItemState.VERIFIED)
                self.assertEqual(transfer["source_sha256"], transfer["target_sha256"])

    def test_ambiguous_create_is_reconciled_by_marker(self):
        with tempfile.TemporaryDirectory() as tmp:
            content = Path(tmp) / "document.txt"
            content.write_bytes(b"remote commit then response loss")
            target = AmbiguousOnceTarget()
            with closing(MigrationPipeline(
                _config(), str(Path(tmp) / "state.db"),
                target=target, binary_source=LocalBinarySource(),
            )) as pipeline:
                nodes, versions = _inventory(content)
                pipeline.manifest.import_extracted_data(nodes, versions, [])
                run_id = pipeline.start_migration(threads=2, mode="full")
                self.assertTrue(pipeline.wait(5))
                self.assertEqual(pipeline.manifest.run_status(run_id)["status"], RunStatus.COMPLETED)
                document_nodes = [node for node in target.nodes.values() if node["name"] == "document.txt"]
                self.assertEqual(len(document_nodes), 1)


class VerificationTests(unittest.TestCase):
    def test_verifier_fails_closed_for_simulation_and_unmapped_category(self):
        with tempfile.TemporaryDirectory() as tmp:
            content = Path(tmp) / "document.txt"
            content.write_bytes(b"abc")
            store = ManifestStore(str(Path(tmp) / "state.db"))
            nodes, versions = _inventory(content)
            categories = [{
                "source_id": 3, "def_id": 50, "cat_name": "DocInfo", "attr_id": 2, "val_str": "X"
            }]
            store.import_extracted_data(nodes, versions, categories)
            run_id = store.create_run("dev", RunMode.DRY_RUN, 9000)
            store.start_run(run_id)
            for phase in ("CONTAINER", "DOCUMENT"):
                while item := store.claim_next(run_id, phase, "test"):
                    store.mark_state(run_id, item["source_id"], ItemState.SIMULATED, worker_id="test")
            store.finish_run(run_id)
            report = AutomatedVerifier(store).run_all_tests(run_id, live=False)
            self.assertEqual(report["overall_status"], "FAIL")
            category_test = next(test for test in report["tests"] if test["id"] == "TEST_03_CATEGORY_VALUES")
            self.assertEqual(category_test["status"], "FAIL")
            store.close()


class TransportTests(unittest.TestCase):
    def test_system_attribute_capabilities_reject_external_dates_as_system_fidelity(self):
        class Response:
            def json(self):
                return {
                    "forms": [{
                        "schema": {
                            "properties": {
                                "external_create_date": {"readonly": False},
                                "external_modify_date": {"readonly": False},
                            }
                        },
                        "options": {"fields": {}},
                    }]
                }

        client = object.__new__(OpenTextCloudClient)
        client._request = lambda *_args, **_kwargs: Response()
        capabilities = client.system_attribute_capabilities(999)
        self.assertFalse(capabilities["preserves_system_dates_and_owner"])
        self.assertTrue(capabilities["node_create_forms"]["0"]["external_create_date"])
        self.assertFalse(capabilities["node_create_forms"]["144"]["owner"])
        self.assertFalse(capabilities["node_update_qualified"])
        self.assertFalse(capabilities["version_update_qualified"])

    def test_marker_readback_handles_nested_gx39_category_shape(self):
        class Response:
            def json(self):
                return {
                    "results": {
                        "data": {
                            "categories": {"108324_2": "CDM:test:123"}
                        }
                    }
                }

        client = object.__new__(OpenTextCloudClient)
        client.migration_category_id = 108324
        client.migration_attribute_key = "108324_2"
        client._request = lambda *_args, **_kwargs: Response()
        self.assertTrue(client._migration_id_matches(999, "CDM:test:123"))

    def test_version_listing_unwraps_gx39_nested_version_shape(self):
        class Response:
            def json(self):
                return {
                    "results": [{
                        "data": {"versions": {"version_number": 2, "file_size": 10}}
                    }]
                }

        client = object.__new__(OpenTextCloudClient)
        client._request = lambda *_args, **_kwargs: Response()
        self.assertEqual(
            client.list_versions(123),
            [{"version_number": 2, "file_size": 10}],
        )

    def test_source_content_server_streams_the_requested_version(self):
        class Response:
            def __init__(self, status, *, payload=None, content=b""):
                self.status_code = status
                self._payload = payload or {}
                self.headers = {"Content-Length": str(len(content))} if content else {}
                self.raw = io.BytesIO(content)
                self.raw.decode_content = False

            def json(self):
                return self._payload

            def close(self):
                pass

        class Session:
            def __init__(self):
                self.verify = None
                self.get_calls = []

            def post(self, *_args, **_kwargs):
                return Response(200, payload={"ticket": "fixture-ticket"})

            def head(self, *_args, **_kwargs):
                return Response(200)

            def get(self, url, **kwargs):
                self.get_calls.append((url, kwargs))
                return Response(200, content=b"source-version")

            def delete(self, *_args, **_kwargs):
                return Response(200)

            def close(self):
                pass

        session = Session()
        source = ContentServerBinarySource(
            "https://source.example.invalid/cs/cs", "fixture-user", "fixture-password",
            session=session,
        )
        version = SourceVersion(123, 2, "sample.bin", "application/octet-stream", 14)
        source.validate(version)
        with source.open(version) as stream:
            self.assertEqual(stream.read(), b"source-version")
        self.assertTrue(all(
            call[0].endswith("/api/v2/nodes/123/versions/2/content")
            for call in session.get_calls
        ))
        self.assertTrue(all(call[1]["stream"] for call in session.get_calls))
        self.assertTrue(all(
            call[1]["headers"]["Accept-Encoding"] == "identity"
            for call in session.get_calls
        ))
        source.close()

    def test_source_content_server_ignores_encoded_transport_length(self):
        class Response:
            status_code = 200
            headers = {"Content-Length": "31", "Content-Encoding": "gzip"}
            raw = io.BytesIO(b"source-version")
            raw.decode_content = False

            def json(self):
                return {"ticket": "fixture-ticket"}

            def close(self):
                pass

        class Session:
            verify = None

            def post(self, *_args, **_kwargs):
                return Response()

            def get(self, *_args, **_kwargs):
                return Response()

            def close(self):
                pass

        source = ContentServerBinarySource(
            "https://source.example.invalid/cs/cs", "fixture-user", "fixture-password",
            session=Session(),
        )
        version = SourceVersion(123, 1, "sample.bin", "application/octet-stream", 14)
        with source.open(version) as stream:
            self.assertEqual(stream.read(), b"source-version")

    def test_source_content_server_rejects_decoded_bytes_beyond_manifest_size(self):
        class Response:
            status_code = 200
            headers = {"Content-Length": "31", "Content-Encoding": "gzip"}
            raw = io.BytesIO(b"source-version-extra")
            raw.decode_content = False

            def json(self):
                return {"ticket": "fixture-ticket"}

            def close(self):
                pass

        class Session:
            verify = None

            def post(self, *_args, **_kwargs):
                return Response()

            def get(self, *_args, **_kwargs):
                return Response()

            def close(self):
                pass

        source = ContentServerBinarySource(
            "https://source.example.invalid/cs/cs", "fixture-user", "fixture-password",
            session=Session(),
        )
        version = SourceVersion(123, 1, "sample.bin", "application/octet-stream", 14)
        with (
            self.assertRaisesRegex(OSError, "exceeds the declared version size"),
            source.open(version) as stream,
        ):
            stream.read(14)

    def test_source_content_server_retries_transient_authentication(self):
        class Response:
            headers = {}

            def __init__(self, status):
                self.status_code = status

            def json(self):
                return {"ticket": "fixture-ticket"}

            def close(self):
                pass

        class Session:
            verify = None

            def __init__(self):
                self.statuses = iter((503, 200))
                self.calls = 0

            def post(self, *_args, **_kwargs):
                self.calls += 1
                return Response(next(self.statuses))

            def close(self):
                pass

        session = Session()
        source = ContentServerBinarySource(
            "https://source.example.invalid/cs/cs", "fixture-user", "fixture-password",
            max_retries=2, session=session,
        )
        with patch("engine.source.time.sleep"):
            self.assertEqual(source._authenticate(), "fixture-ticket")
        self.assertEqual(session.calls, 2)

    def test_source_content_server_exhausted_download_is_retryable(self):
        class Response:
            headers = {}

            def __init__(self, status):
                self.status_code = status

            def json(self):
                return {"ticket": "fixture-ticket"}

            def close(self):
                pass

        class Session:
            verify = None

            def post(self, *_args, **_kwargs):
                return Response(200)

            def get(self, *_args, **_kwargs):
                return Response(503)

            def close(self):
                pass

        source = ContentServerBinarySource(
            "https://source.example.invalid/cs/cs", "fixture-user", "fixture-password",
            max_retries=1, session=Session(),
        )
        version = SourceVersion(123, 1, "sample.bin", "application/octet-stream", 14)
        with self.assertRaisesRegex(RetryableMigrationError, "temporarily failed"):
            source.open(version)

    def test_source_content_server_logout_failure_is_best_effort(self):
        class Response:
            status_code = 200
            headers = {}

            def json(self):
                return {"ticket": "fixture-ticket"}

            def close(self):
                pass

        class Session:
            verify = None

            def __init__(self):
                self.closed = False

            def post(self, *_args, **_kwargs):
                return Response()

            def delete(self, *_args, **_kwargs):
                raise OSError("logout failed")

            def close(self):
                self.closed = True

        session = Session()
        source = ContentServerBinarySource(
            "https://source.example.invalid/cs/cs", "fixture-user", "fixture-password",
            session=session,
        )
        source._token_manager.get()
        with self.assertLogs("CDM.Source", level="WARNING"):
            source.close()
        self.assertTrue(session.closed)

    def test_http_clients_enable_native_certificate_store(self):
        with patch("engine.client.configure_native_trust_store") as configure_target:
            OpenTextCloudClient({"ot_cloud_url": "https://example.invalid"}, max_retries=1)
            configure_target.assert_called_once_with()

        source = AzureBlobBinarySource("https://storage.example.invalid")
        version = SourceVersion(
            1, 1, "sample.bin", "application/octet-stream", 1,
            blob_locator="azure://content/sample.bin",
        )
        with patch("engine.source.configure_native_trust_store") as configure_source:
            source._client(version)
            configure_source.assert_called_once_with()

    def test_windows_drive_path_is_not_treated_as_a_url_scheme(self):
        locator = r"C:\content\sample.bin"
        version = SourceVersion(1, 1, "sample.bin", "application/octet-stream", 1, blob_locator=locator)
        self.assertEqual(LocalBinarySource._path(version), Path(locator))

    def test_stale_ticket_is_verified_before_reauthentication(self):
        authenticated: list[str] = []
        verified: list[str] = []

        def authenticate():
            ticket = f"ticket-{len(authenticated) + 1}"
            authenticated.append(ticket)
            return ticket

        def verify(ticket):
            verified.append(ticket)
            return True

        manager = _TokenManager(authenticate, verify, ttl_seconds=0)
        self.assertEqual(manager.get(), "ticket-1")
        self.assertEqual(manager.get(), "ticket-1")
        self.assertEqual(authenticated, ["ticket-1"])
        self.assertEqual(verified, ["ticket-1"])
        self.assertEqual(manager.peek(), "ticket-1")

    def test_streaming_multipart_body_has_exact_length(self):
        import requests

        data = b"0123456789" * 100
        body = MultipartStream(
            {"body": "{}"}, "file", "sample.bin", "application/octet-stream", io.BytesIO(data), len(data)
        )
        prepared = requests.Request(
            "POST", "https://example.invalid/api/v2/nodes", data=body,
            headers={"Content-Type": body.content_type, "Content-Length": str(len(body))},
        ).prepare()
        self.assertEqual(prepared.headers["Content-Length"], str(len(body)))
        self.assertNotIn("Transfer-Encoding", prepared.headers)
        encoded = _read_all(body)
        self.assertEqual(len(encoded), len(body))
        self.assertIn(data, encoded)
        self.assertTrue(encoded.endswith(b"--\r\n"))


class ConfigTests(unittest.TestCase):
    def test_fresh_example_is_secret_free_and_ready_for_ui_configuration(self):
        path = Path(__file__).parents[1] / "config.example.json"
        config = load_config(path)
        self.assertEqual(config.default_environment, "test")
        self.assertEqual(set(config.environments), {"test", "production"})
        self.assertFalse(config.environment("test").is_production)
        self.assertTrue(config.environment("production").is_production)
        for environment in config.environments.values():
            self.assertFalse(environment.get("db_password"))
            self.assertFalse(environment.get("ot_cloud_password"))
            self.assertFalse(environment.get("azure_storage_sas_url"))
            self.assertIsNone(environment.get("migration_attribute_key"))

    def test_container_sas_url_derives_all_azure_runtime_fields(self):
        values = normalize_profile_values("prod", {
            "azure_storage_sas_url": (
                "https://storage.example.invalid/content?sv=placeholder&sp=rl&sig=placeholder"
            ),
            "migration_attribute_key": "45678_2",
        })
        self.assertEqual(values["azure_storage_account_url"], "https://storage.example.invalid")
        self.assertEqual(values["azure_storage_sas_token"], "sv=placeholder&sp=rl&sig=placeholder")
        self.assertEqual(values["azure_blob_locator_template"], "azure://content/{provider_data}")
        self.assertEqual(values["migration_category_id"], 45678)
        self.assertEqual(values["migration_namespace"], "cdm-prod")

    def test_source_content_server_url_selects_the_rest_binary_adapter(self):
        values = normalize_profile_values("test", {
            "source_cs_url": "https://source.example.invalid/cs/cs/",
            "azure_storage_sas_url": (
                "https://storage.example.invalid/content?sv=placeholder&sp=rl&sig=placeholder"
            ),
        })
        self.assertEqual(values["source_cs_url"], "https://source.example.invalid/cs/cs")
        self.assertEqual(values["binary_source_adapter"], "content_server")

    def test_migration_policy_uses_fixed_business_defaults(self):
        values = normalize_profile_values("dev", {
            "source_root_maps_to_target": True,
            "system_attribute_strategy": "accept_target_generated",
            "permission_strategy": "mapped_acl",
        })
        self.assertFalse(values["source_root_maps_to_target"])
        self.assertEqual(values["system_attribute_strategy"], "preserve")
        self.assertEqual(values["permission_strategy"], "inherit_target")

    def test_blob_specific_sas_url_is_rejected(self):
        with self.assertRaisesRegex(ConfigurationError, "container-level"):
            normalize_profile_values("prod", {
                "azure_storage_sas_url": (
                    "https://storage.example.invalid/content/one-file.bin?sv=x&sig=placeholder"
                ),
            })

    def test_local_production_credentials_can_be_persisted(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                '{"default_environment":"prod","environments":{"prod":'
                '{"db_password":"fixture-db-value","ot_cloud_password":"fixture-cloud-value",'
                '"azure_storage_sas_token":"fixture-sas-value",'
                '"verify_ssl":true}},"migration_settings":{}}', encoding="utf-8"
            )
            config = load_config(path)
            self.assertEqual(config.environment().get("db_password"), "fixture-db-value")
            save_config(config, path)
            persisted = path.read_text(encoding="utf-8")
            self.assertIn("fixture-db-value", persisted)
            if os.name != "nt":
                self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_arbitrary_production_profile_uses_the_same_local_secret_model(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                '{"default_environment":"example_prod","environments":{"example_prod":'
                '{"environment_class":"production","db_password":"fixture-db-value",'
                '"verify_ssl":true}},"migration_settings":{}}', encoding="utf-8",
            )
            self.assertEqual(load_config(path).environment().get("db_password"), "fixture-db-value")

    def test_secret_environment_variable_names_are_editable_but_values_are_masked(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                '{"default_environment":"qa","environments":{"qa":'
                '{"environment_class":"test","db_password_env":"CDM_QA_DB",'
                '"verify_ssl":true}},"migration_settings":{}}', encoding="utf-8",
            )
            profile = load_config(path).environment().public_view()
            self.assertEqual(profile["db_password_env"], "CDM_QA_DB")
            self.assertEqual(profile["db_password"], "<missing>")


class PreflightTests(unittest.TestCase):
    def test_online_source_sample_always_closes_binary_source(self):
        class Source:
            def __init__(self):
                self.closed = False

            def validate(self, _version):
                pass

            def close(self):
                self.closed = True

        class Target:
            def test_connection(self):
                return {"status": "connected"}

            def get_node(self, target_id):
                return {"id": target_id, "name": "target", "type": 0}

            def system_attribute_capabilities(self, _target_id):
                return {
                    "preserves_system_dates_and_owner": False,
                    "node_types": {},
                }

            def close(self):
                pass

        with tempfile.TemporaryDirectory() as tmp:
            content = Path(tmp) / "content.bin"
            content.write_bytes(b"sample")
            store = ManifestStore(str(Path(tmp) / "state.db"))
            nodes, versions = _inventory(content)
            store.import_extracted_data(
                nodes, versions, [], source_root_id=1, source_profile_id="qa",
            )
            values = {
                **_config()["environments"]["dev"],
                "source_workspace_nodeid": 1,
            }
            source = Source()
            with (
                patch("engine.preflight.SourceDB.test_connection", return_value={"status": "connected"}),
                patch("engine.preflight.OpenTextCloudClient", return_value=Target()),
                patch("engine.preflight.build_binary_source", return_value=source),
            ):
                PreflightAuditor(EnvironmentConfig("qa", values), store, {}).run(
                    online=True, sample_blobs=1,
                )
            self.assertTrue(source.closed)
            store.close()

    def test_production_freeze_is_required_only_for_full_cutover(self):
        with tempfile.TemporaryDirectory() as tmp:
            content = Path(tmp) / "content.bin"
            content.write_bytes(b"freeze boundary")
            store = ManifestStore(str(Path(tmp) / "state.db"))
            nodes, versions = _inventory(content)
            store.import_extracted_data(
                nodes, versions, [], source_root_id=1, source_profile_id="prod",
            )
            values = {
                **_config()["environments"]["dev"],
                "environment_class": "production",
                "source_workspace_nodeid": 1,
            }
            auditor = PreflightAuditor(EnvironmentConfig("prod", values), store, {})
            dry = auditor.run(for_mode="dry_run")
            full = auditor.run(for_mode="full")
            self.assertNotIn("SOURCE_READ_ONLY_FREEZE", {check["id"] for check in dry["checks"]})
            freeze = next(check for check in full["checks"] if check["id"] == "SOURCE_READ_ONLY_FREEZE")
            self.assertEqual(freeze["status"], "FAIL")
            store.close()
    def test_content_server_rest_replaces_direct_blob_locator_requirement(self):
        with tempfile.TemporaryDirectory() as tmp:
            content = Path(tmp) / "content.bin"
            content.write_bytes(b"rest")
            store = ManifestStore(str(Path(tmp) / "state.db"))
            nodes, versions = _inventory(content)
            versions[0]["blob_locator"] = None
            store.import_extracted_data(
                nodes, versions, [], source_root_id=1, source_profile_id="qa",
            )
            values = {
                **_config()["environments"]["dev"],
                "binary_source_adapter": "content_server",
                "source_workspace_nodeid": 1,
                "source_cs_url": "https://source.example.invalid/cs/cs",
                "source_cs_user": "fixture-user",
                "source_cs_password": "fixture-password",
            }
            report = PreflightAuditor(EnvironmentConfig("qa", values), store, {}).run()
            locator = next(check for check in report["checks"] if check["id"] == "BLOB_LOCATORS")
            self.assertEqual(locator["status"], "PASS")
            self.assertIn("DataID/version", locator["detail"])
            store.close()

    def test_pilot_defers_only_gx39_qualification_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            content = Path(tmp) / "content.bin"
            content.write_bytes(b"pilot")
            store = ManifestStore(str(Path(tmp) / "state.db"))
            nodes, versions = _inventory(content)
            store.import_extracted_data(
                nodes, versions, [], source_root_id=1, source_profile_id="qa",
            )
            values = {
                **_config()["environments"]["dev"],
                "source_workspace_nodeid": 1,
                "target_acl_approved": True,
                "system_attribute_strategy": "preserve",
                "active_workflows_confirmed_zero": True,
                "historical_audit_out_of_scope_approved": True,
                "personal_state_out_of_scope_approved": True,
                "workspace_roles_qualified": False,
                "lifecycle_operations_qualified": False,
                "search_and_facets_qualified": False,
                "legacy_links_qualified": False,
            }
            auditor = PreflightAuditor(EnvironmentConfig("qa", values), store, {})
            pilot = auditor.run(for_mode="pilot")
            full = auditor.run(for_mode="full")
            pilot_parity = next(check for check in pilot["checks"] if check["id"] == "FUNCTIONAL_PARITY")
            full_parity = next(check for check in full["checks"] if check["id"] == "FUNCTIONAL_PARITY")
            self.assertEqual(pilot_parity["status"], "PASS")
            self.assertEqual(full_parity["status"], "FAIL")
            store.close()


if __name__ == "__main__":
    unittest.main()
