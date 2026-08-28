from __future__ import annotations

import json
import unittest
from pathlib import Path

from engine.version import VERSION


class RepositoryContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = Path(__file__).parents[1]

    def test_canonical_handoff_documents_exist(self):
        for name in (
            "README.md",
            "AGENTS.md",
            "CORPORATE_HANDOFF.md",
            "ARCHITECTURE.md",
            "DEPLOYMENT_AND_QUALIFICATION.md",
            "MIGRATION_PLAN.md",
        ):
            self.assertTrue((self.root / name).is_file(), name)

    def test_removed_legacy_documents_do_not_return(self):
        for name in ("ARCHITECTURE_V2.md", "SYSTEM_ARCHITECTURE_AUDIT_DOSSIER.md"):
            self.assertFalse((self.root / name).exists(), name)

    def test_example_configuration_contains_no_saved_secrets(self):
        payload = json.loads((self.root / "config.example.json").read_text(encoding="utf-8"))
        for profile in payload["environments"].values():
            for key in (
                "db_password",
                "ot_cloud_password",
                "azure_storage_sas_url",
                "azure_storage_sas_token",
            ):
                self.assertFalse(profile.get(key), key)
            self.assertFalse(profile.get("db_host"), "db_host")
            self.assertFalse(profile.get("ot_cloud_url"), "ot_cloud_url")
            self.assertIsNone(profile.get("source_workspace_nodeid"), "source_workspace_nodeid")
            self.assertIsNone(profile.get("target_workspace_nodeid"), "target_workspace_nodeid")

    def test_project_version_matches_runtime_version(self):
        import tomllib

        payload = tomllib.loads((self.root / "pyproject.toml").read_text(encoding="utf-8"))
        self.assertEqual(payload["project"]["version"], VERSION)


if __name__ == "__main__":
    unittest.main()
