from __future__ import annotations

import io
import tempfile
import unittest
from pathlib import Path

from engine.client import MultipartStream, _TokenManager
from engine.config import (
    ConfigurationError,
    EnvironmentConfig,
    load_config,
    normalize_profile_values,
    save_config,
)
from engine.instance_lock import InstanceAlreadyRunning
from engine.manifest import ManifestStore, StateConflict
from engine.models import ItemState, RunMode, RunStatus, UploadResult
from engine.pipeline import MigrationPipeline
from engine.preflight import PreflightAuditor
from engine.reconciler import AutomatedVerifier
from engine.source import LocalBinarySource


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

    def create_container(self, node, parent_id, migration_id):
        self.next_id += 1
        self.nodes[self.next_id] = {
            "id": self.next_id, "name": node.name, "parent_id": parent_id, "type": node.subtype,
        }
        self.markers[(parent_id, migration_id)] = self.next_id
        return self.next_id

    def find_by_migration_id(self, parent_id, migration_id):
        return self.markers.get((parent_id, migration_id))

    def get_node(self, target_id):
        return self.nodes[target_id]

    def apply_categories(self, target_id, categories):
        return None

    def apply_system_attributes(self, target_id, source, *, version_num=None):
        self.system_attribute_calls.append((target_id, source.source_id, version_num))

    def apply_permission_policy(self, target_id, policy):
        self.permission_policy_calls.append((target_id, policy))

    def upload_first_version(self, node, version, parent_id, stream, migration_id):
        self.next_id += 1
        data = _read_all(stream)
        self.nodes[self.next_id] = {
            "id": self.next_id, "name": node.name, "parent_id": parent_id, "type": 144,
        }
        self.contents[(self.next_id, version.version_num)] = data
        self.markers[(parent_id, migration_id)] = self.next_id
        return UploadResult(self.next_id, version.version_num)

    def upload_next_version(self, target_id, version, stream):
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
            self.assertEqual(store.parity_report(incomplete)["status"], "FAIL")
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


class PipelineTests(unittest.TestCase):
    def test_production_full_run_requires_freeze_confirmation(self):
        with tempfile.TemporaryDirectory() as tmp:
            content = Path(tmp) / "document.txt"
            content.write_bytes(b"production")
            config = _config()
            config["environments"]["dev"]["environment_class"] = "production"
            pipeline = MigrationPipeline(
                config, str(Path(tmp) / "state.db"),
                target=FakeTarget(), binary_source=LocalBinarySource(),
            )
            nodes, versions = _inventory(content)
            pipeline.manifest.import_extracted_data(nodes, versions, [])
            with self.assertRaisesRegex(Exception, "source read-only freeze"):
                pipeline.start_migration(threads=1, mode="full")
            pipeline.close()

    def test_preserve_strategy_applies_node_and_version_system_attributes(self):
        with tempfile.TemporaryDirectory() as tmp:
            content = Path(tmp) / "document.txt"
            content.write_bytes(b"dated version")
            config = _config()
            config["environments"]["dev"]["system_attribute_strategy"] = "preserve"
            config["environments"]["dev"]["owner_mappings"] = {"42": 4200}
            target = FakeTarget()
            pipeline = MigrationPipeline(
                config, str(Path(tmp) / "state.db"),
                target=target, binary_source=LocalBinarySource(),
            )
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
            pipeline = MigrationPipeline(
                _config(), str(Path(tmp) / "state.db"),
                target=FakeTarget(), binary_source=LocalBinarySource(),
            )
            nodes, versions = _inventory(content)
            pipeline.manifest.import_extracted_data(nodes, versions, [])

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
            pipeline = MigrationPipeline(
                _config(), str(Path(tmp) / "state.db"),
                target=target, binary_source=LocalBinarySource(),
            )
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


class TransportTests(unittest.TestCase):
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
        data = b"0123456789" * 100
        body = MultipartStream(
            {"body": "{}"}, "file", "sample.bin", "application/octet-stream", io.BytesIO(data), len(data)
        )
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
                "https://storage.example.invalid/content?sv=2025-01-05&sp=rl&sig=secret"
            ),
            "migration_attribute_key": "45678_2",
        })
        self.assertEqual(values["azure_storage_account_url"], "https://storage.example.invalid")
        self.assertEqual(values["azure_storage_sas_token"], "sv=2025-01-05&sp=rl&sig=secret")
        self.assertEqual(values["azure_blob_locator_template"], "azure://content/{provider_data}")
        self.assertEqual(values["migration_category_id"], 45678)
        self.assertEqual(values["migration_namespace"], "cdm-prod")

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
                    "https://storage.example.invalid/content/one-file.bin?sv=x&sig=y"
                ),
            })

    def test_local_production_credentials_can_be_persisted(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                '{"default_environment":"prod","environments":{"prod":'
                '{"db_password":"saved-locally","ot_cloud_password":"cloud-secret",'
                '"azure_storage_sas_token":"sas-secret",'
                '"verify_ssl":true}},"migration_settings":{}}', encoding="utf-8"
            )
            config = load_config(path)
            self.assertEqual(config.environment().get("db_password"), "saved-locally")
            save_config(config, path)
            persisted = path.read_text(encoding="utf-8")
            self.assertIn("saved-locally", persisted)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_arbitrary_production_profile_uses_the_same_local_secret_model(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                '{"default_environment":"example_prod","environments":{"example_prod":'
                '{"environment_class":"production","db_password":"saved-locally",'
                '"verify_ssl":true}},"migration_settings":{}}', encoding="utf-8",
            )
            self.assertEqual(load_config(path).environment().get("db_password"), "saved-locally")

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
