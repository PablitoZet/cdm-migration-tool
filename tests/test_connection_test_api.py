from __future__ import annotations

import importlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


class ConnectionTestApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()
        root = Path(cls.temp_dir.name)
        config = json.loads(
            (Path(__file__).parents[1] / "config.example.json").read_text(encoding="utf-8")
        )
        config["environments"]["test"].update({
            "db_host": "saved-db-host",
            "db_user": "saved-db-user",
            "db_password": "saved-db-password",
            "ot_cloud_url": "https://saved.example/cs/cs",
            "ot_cloud_user": "saved-cloud-user",
            "ot_cloud_password": "saved-cloud-password",
        })
        config_path = root / "config.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        cls.previous_config_path = os.environ.get("CDM_CONFIG_PATH")
        cls.previous_state_dir = os.environ.get("CDM_STATE_DIR")
        os.environ["CDM_CONFIG_PATH"] = str(config_path)
        os.environ["CDM_STATE_DIR"] = str(root / "state")
        cls.app_module = importlib.import_module("app")

    @classmethod
    def tearDownClass(cls):
        cls.app_module._pipeline.close()
        if cls.previous_config_path is None:
            os.environ.pop("CDM_CONFIG_PATH", None)
        else:
            os.environ["CDM_CONFIG_PATH"] = cls.previous_config_path
        if cls.previous_state_dir is None:
            os.environ.pop("CDM_STATE_DIR", None)
        else:
            os.environ["CDM_STATE_DIR"] = cls.previous_state_dir
        cls.temp_dir.cleanup()

    def test_draft_connection_test_uses_form_values_without_persisting_them(self):
        app = self.app_module
        before = dict(app._config.environment("test").values)
        source_result = {"status": "connected", "read_only": True}
        target_result = {"status": "connected", "tls_verification": True}
        request = app.ConnectionTestRequest(values={
            "db_host": "draft-db-host",
            "db_password": "draft-db-password",
            "ot_cloud_url": "https://draft.example/cs/cs",
            "ot_cloud_password": "draft-cloud-password",
        })

        with (
            patch.object(app, "SourceDB") as source_factory,
            patch.object(app, "OpenTextCloudClient") as target_factory,
            patch.object(app, "save_config") as save_config,
        ):
            source_factory.return_value.test_connection.return_value = source_result
            target_factory.return_value.test_connection.return_value = target_result

            result = app.test_profile_connections("test", request)

        self.assertTrue(result["tested_without_saving"])
        self.assertEqual(result["source_db"], source_result)
        self.assertEqual(result["target_cloud"], target_result)
        self.assertEqual(app._config.environment("test").values, before)
        save_config.assert_not_called()
        draft = source_factory.call_args.args[0]
        self.assertEqual(draft.get("db_host"), "draft-db-host")
        self.assertEqual(draft.get("db_password"), "draft-db-password")
        target_factory.return_value.close.assert_called_once_with()

    def test_new_profile_can_test_connections_before_first_save(self):
        app = self.app_module
        request = app.ConnectionTestRequest(values={
            "name": "Draft profile",
            "db_host": "draft-db-host",
            "db_password": "draft-db-password",
            "ot_cloud_url": "https://draft.example/cs/cs",
            "ot_cloud_password": "draft-cloud-password",
        })

        with (
            patch.object(app, "SourceDB") as source_factory,
            patch.object(app, "OpenTextCloudClient") as target_factory,
        ):
            source_factory.return_value.test_connection.return_value = {
                "status": "connected", "read_only": True,
            }
            target_factory.return_value.test_connection.return_value = {
                "status": "connected", "tls_verification": True,
            }

            result = app.test_profile_connections("draft", request)

        self.assertTrue(result["tested_without_saving"])
        self.assertNotIn("draft", app._config.environments)
        self.assertEqual(source_factory.call_args.args[0].get("db_host"), "draft-db-host")
        target_factory.return_value.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
