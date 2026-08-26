"""Fail-closed, target-backed post-migration verification gates."""

from __future__ import annotations

import hashlib
import json
import time
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from .manifest import ManifestStore
from .models import ItemState


class AutomatedVerifier:
    def __init__(self, manifest: ManifestStore, client=None):
        self.manifest = manifest
        self.client = client

    def run_all_tests(self, run_id: str, *, live: bool = True, redownload: bool = False) -> dict[str, Any]:
        started = time.monotonic()
        tests = [
            self._inventory(run_id, live),
            self._hashes(run_id, live and redownload),
            self._categories(run_id, live),
            self._versions(run_id, live),
            self._system_attributes(run_id, live),
            self._permissions(run_id, live),
        ]
        passed = all(test["status"] == "PASS" for test in tests)
        return {
            "overall_status": "PASS" if passed else "FAIL",
            "run_id": run_id,
            "execution_time_seconds": round(time.monotonic() - started, 3),
            "tests_run": len(tests),
            "tests_passed": sum(t["status"] == "PASS" for t in tests),
            "tests_failed": sum(t["status"] == "FAIL" for t in tests),
            "tests": tests,
        }

    def _inventory(self, run_id: str, live: bool) -> dict[str, Any]:
        rows = self.manifest.verification_items(run_id)
        failures: list[str] = []
        if not rows:
            failures.append("run has no items")
        for row in rows:
            if row["state"] != ItemState.VERIFIED:
                failures.append(f"{row['source_id']}: state={row['state']}")
                continue
            target_id = row.get("mapped_target_id") or row.get("target_id")
            if not target_id:
                failures.append(f"{row['source_id']}: no durable mapping")
                continue
            if live:
                if not self.client:
                    failures.append("live inventory requested without target client")
                    break
                try:
                    props = self.client.get_node(int(target_id))
                    mapped_root = (
                        bool(getattr(self.client, "source_root_maps_to_target", False))
                        and int(row.get("depth") or 0) == 0
                    )
                    if not mapped_root and props.get("name") != row["name"]:
                        failures.append(f"{row['source_id']}: target name mismatch")
                    if not mapped_root and _normal(props.get("description")) != _normal(row.get("description")):
                        failures.append(f"{row['source_id']}: target description mismatch")
                    expected_parent = row.get("mapped_parent_id") or row.get("target_parent_id")
                    if expected_parent and int(props.get("parent_id", -1)) != int(expected_parent):
                        failures.append(f"{row['source_id']}: target parent mismatch")
                except Exception as exc:
                    failures.append(f"{row['source_id']}: {type(exc).__name__}: {exc}")
        return _result(
            "TEST_01_TARGET_INVENTORY", "Target-backed inventory and hierarchy",
            failures, f"Checked {len(rows)} run items; every item must be VERIFIED and readable on target.",
        )

    def _hashes(self, run_id: str, redownload: bool) -> dict[str, Any]:
        rows = self.manifest.verification_versions(run_id)
        failures: list[str] = []
        if not rows:
            failures.append("run has no document versions")
        for row in rows:
            source_hash = row.get("source_sha256")
            target_hash = row.get("target_sha256")
            if row["state"] != ItemState.VERIFIED or not source_hash or not target_hash:
                failures.append(f"{row['source_id']} v{row['version_num']}: hashes not verified")
                continue
            if source_hash != target_hash:
                failures.append(f"{row['source_id']} v{row['version_num']}: stored hash mismatch")
                continue
            if redownload:
                if not self.client:
                    failures.append("redownload requested without target client")
                    break
                try:
                    digest = hashlib.sha256()
                    target_version = row.get("target_version_num") or row["version_num"]
                    for chunk in self.client.iter_content(int(row["target_id"]), int(target_version)):
                        if chunk:
                            digest.update(chunk)
                    if digest.hexdigest() != source_hash:
                        failures.append(f"{row['source_id']} v{row['version_num']}: independent hash mismatch")
                except Exception as exc:
                    failures.append(f"{row['source_id']} v{row['version_num']}: {exc}")
        return _result(
            "TEST_02_VERSION_SHA256", "Per-version SHA-256 integrity",
            failures, f"Checked {len(rows)} version transfer records; exceptions are failures.",
        )

    def _categories(self, run_id: str, live: bool) -> dict[str, Any]:
        items = self.manifest.verification_items(run_id)
        failures: list[str] = []
        attributes_checked = 0
        if live and not self.client:
            failures.append("live category validation requested without target client")
        for item in items:
            attributes = self.manifest.categories(item["source_id"])
            if not attributes:
                continue
            grouped: dict[int, dict[str, Any]] = defaultdict(dict)
            for attr in attributes:
                cat_id = attr.get("target_category_id")
                attr_template = attr.get("target_attr_key")
                if not cat_id or not attr_template:
                    failures.append(
                        f"{item['source_id']}: unmapped DefID={attr.get('def_id')} Attr={attr.get('attr_key')}"
                    )
                    continue
                attr_key = str(attr_template).format(row=int(attr.get("row_num") or 0))
                if attr_key in grouped[int(cat_id)]:
                    failures.append(
                        f"{item['source_id']}: duplicate target category key {cat_id}/{attr_key}"
                    )
                    continue
                grouped[int(cat_id)][attr_key] = attr["value"]
            if live and self.client:
                for cat_id, expected in grouped.items():
                    try:
                        target_id = item.get("mapped_target_id") or item.get("target_id")
                        if not target_id:
                            raise ValueError("missing target mapping")
                        actual = self.client.get_category(int(target_id), cat_id)
                        for key, value in expected.items():
                            attributes_checked += 1
                            if _normal(actual.get(key)) != _normal(value):
                                failures.append(f"{item['source_id']}: category {cat_id} attribute {key} mismatch")
                    except Exception as exc:
                        failures.append(f"{item['source_id']}: category {cat_id}: {exc}")
        return _result(
            "TEST_03_CATEGORY_VALUES", "Target category value parity",
            failures, f"Checked {attributes_checked} mapped target attribute values.",
        )

    def _versions(self, run_id: str, live: bool) -> dict[str, Any]:
        rows = self.manifest.verification_versions(run_id)
        expected: dict[int, list[int]] = defaultdict(list)
        targets: dict[int, int] = {}
        failures: list[str] = []
        for row in rows:
            expected[row["source_id"]].append(int(row["version_num"]))
            if row.get("target_id"):
                targets[row["source_id"]] = int(row["target_id"])
        for source_id, sequence in expected.items():
            if sequence != sorted(set(sequence)):
                failures.append(f"{source_id}: source version sequence is duplicated or unordered")
            if live:
                if not self.client:
                    failures.append("live version validation requested without target client")
                    break
                try:
                    actual_rows = self.client.list_versions(targets[source_id])
                    actual = []
                    for row in actual_rows:
                        for key in ("version_number", "version", "version_num"):
                            if row.get(key) is not None:
                                actual.append(int(row[key]))
                                break
                    if len(actual) != len(sequence):
                        failures.append(f"{source_id}: expected {len(sequence)} versions, target has {len(actual)}")
                except Exception as exc:
                    failures.append(f"{source_id}: {exc}")
        return _result(
            "TEST_04_VERSION_CHAIN", "Target version-chain continuity",
            failures, f"Checked {len(expected)} document version chains.",
        )

    def _system_attributes(self, run_id: str, live: bool) -> dict[str, Any]:
        failures: list[str] = []
        checked = 0
        if live and not self.client:
            failures.append("live system-attribute validation requested without target client")
        if live and self.client:
            if getattr(self.client, "system_attribute_strategy", None) != "preserve":
                failures.append("system_attribute_strategy is not preserve")
            for row in self.manifest.verification_items(run_id):
                target_id = row.get("mapped_target_id") or row.get("target_id")
                if not target_id:
                    continue
                if (
                    bool(getattr(self.client, "source_root_maps_to_target", False))
                    and int(row.get("depth") or 0) == 0
                ):
                    continue
                try:
                    props = self.client.get_node(int(target_id))
                    for source_key, target_key in (
                        ("source_created_at", "create_date"),
                        ("source_modified_at", "modify_date"),
                    ):
                        expected = row.get(source_key)
                        if expected is not None and _normal_time(props.get(target_key)) != _normal_time(expected):
                            failures.append(f"{row['source_id']}: {target_key} mismatch")
                    source_owner = row.get("owner_id")
                    if source_owner is not None:
                        expected_owner = self.client.owner_mappings.get(str(source_owner))
                        if expected_owner is None or str(props.get("owner_id")) != str(expected_owner):
                            failures.append(f"{row['source_id']}: owner mismatch")
                    checked += 1
                except Exception as exc:
                    failures.append(f"{row['source_id']}: {exc}")
        return _result(
            "TEST_05_SYSTEM_ATTRIBUTES", "Dates, owner and system-attribute parity",
            failures, f"Checked {checked} target nodes.",
        )

    def _permissions(self, run_id: str, live: bool) -> dict[str, Any]:
        failures: list[str] = []
        checked = 0
        if live and not self.client:
            failures.append("live permission validation requested without target client")
        if live and self.client:
            for row in self.manifest.verification_items(run_id):
                target_id = row.get("mapped_target_id") or row.get("target_id")
                if not target_id:
                    continue
                try:
                    permissions = self.client.list_permissions(int(target_id))
                    if not permissions:
                        failures.append(f"{row['source_id']}: target returned no permission entries")
                    checked += 1
                except Exception as exc:
                    failures.append(f"{row['source_id']}: permission read failed: {exc}")
        return _result(
            "TEST_06_PERMISSION_READBACK", "Target ACL visibility and read-back",
            failures, f"Read target permissions for {checked} nodes; policy semantics require GX39 qualification.",
        )


def _normal(value: Any) -> str:
    if value is None:
        return "<NULL>"
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, default=str)
    return str(value).strip()


def _normal_time(value: Any) -> str:
    if value is None:
        return "<NULL>"
    text = str(value).strip().replace(" ", "T")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC).isoformat(timespec="seconds")
    except ValueError:
        return text


def _result(test_id: str, name: str, failures: list[str], details: str) -> dict[str, Any]:
    return {
        "id": test_id,
        "name": name,
        "status": "FAIL" if failures else "PASS",
        "details": details,
        "failure_count": len(failures),
        "failures": failures[:100],
        "truncated": len(failures) > 100,
    }
