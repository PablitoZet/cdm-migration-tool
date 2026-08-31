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
from .provenance import (
    METADATA_CONTRACT_VERSION,
    exception_set_digest,
    provenance_values,
    version_provenance_values,
)


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
            self._owner_provenance(run_id, live),
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

    def _owner_provenance(self, run_id: str, live: bool) -> dict[str, Any]:
        failures: list[str] = []
        checked = 0
        run = self.manifest.run_status(run_id)
        if run.get("metadata_contract_version") != METADATA_CONTRACT_VERSION:
            failures.append(
                f"run metadata contract is {run.get('metadata_contract_version')!r}, "
                f"expected {METADATA_CONTRACT_VERSION!r}"
            )
        resolutions = {
            resolution.source_owner_id: resolution
            for resolution in self.manifest.run_owner_resolutions(run_id)
        }
        exceptions = [
            resolution for resolution in resolutions.values()
            if resolution.resolution_status == "APPROVED_FALLBACK"
        ]
        approval = self.manifest.fallback_approval(run_id)
        if exceptions and (
            not approval or approval.get("exception_set_digest") != exception_set_digest(exceptions)
        ):
            failures.append("fallback approval evidence is missing or has a different exception digest")
        if live and not self.client:
            failures.append("live owner/provenance validation requested without target client")
        for row in self.manifest.verification_items(run_id):
            target_id = row.get("mapped_target_id") or row.get("target_id")
            if not target_id:
                failures.append(f"{row['source_id']}: no durable target mapping")
                continue
            if row.get("metadata_contract_version") != METADATA_CONTRACT_VERSION:
                failures.append(f"{row['source_id']}: item metadata contract is not current")
            source_owner = row.get("owner_id")
            if source_owner is None:
                failures.append(f"{row['source_id']}: immutable owner resolution is missing")
                continue
            source_owner_id = int(str(source_owner))
            resolution = resolutions.get(source_owner_id)
            if resolution is None or resolution.target_member_id is None:
                failures.append(f"{row['source_id']}: immutable owner resolution is missing")
                continue
            try:
                owner = self.manifest.owner_identity(source_owner_id)
                versions = (
                    self.manifest.source_versions(int(row["source_id"]))
                    if int(row["subtype"]) in (136, 144, 154, 751) else []
                )
                expected_node = provenance_values(
                    {
                        "source_id": row["source_id"],
                        "source_created_at": row.get("source_created_at"),
                        "source_modified_at": row.get("source_modified_at"),
                    },
                    owner,
                    resolution,
                )
                expected_versions = [
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
                if live:
                    if not all(
                        callable(getattr(self.client, name, None))
                        for name in ("read_owner", "read_creator", "read_provenance")
                    ):
                        failures.append(f"{row['source_id']}: modern metadata read-back is unavailable")
                        continue
                    actual_owner = self.client.read_owner(int(target_id))
                    if _member_id(actual_owner) != int(resolution.target_member_id):
                        failures.append(f"{row['source_id']}: owner read-back mismatch")
                    creator = self.client.read_creator(int(target_id))
                    expected_creator = run.get("service_member_id")
                    if expected_creator is None or _member_id(creator) != int(expected_creator):
                        failures.append(f"{row['source_id']}: creator read-back mismatch")
                    actual_provenance = self.client.read_provenance(int(target_id))
                    failures.extend(
                        f"{row['source_id']}: {failure}"
                        for failure in _provenance_failures(
                            actual_provenance, expected_node, expected_versions,
                            getattr(self.client, "provenance_attribute_keys", {}) or {},
                            getattr(self.client, "provenance_version_attribute_keys", {}) or {},
                            str(getattr(self.client, "provenance_versions_field", "version_rows")),
                        )
                    )
                checked += 1
            except Exception as exc:
                failures.append(f"{row['source_id']}: {type(exc).__name__}: {exc}")
        return _result(
            "TEST_05_OWNER_PROVENANCE", "Owner, creator and migration provenance read-back",
            failures, f"Checked {checked} target nodes.",
        )

    # Compatibility alias for callers using the old test method name.
    _system_attributes = _owner_provenance

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


def _member_id(value: Any) -> int | None:
    if isinstance(value, dict):
        for key in ("id", "member_id", "memberid", "user_id", "userid", "owner_id"):
            if value.get(key) is not None:
                try:
                    return int(value[key])
                except (TypeError, ValueError):
                    return None
        for nested in value.values():
            member_id = _member_id(nested)
            if member_id is not None:
                return member_id
    return None


def _find_value(value: Any, key: str) -> tuple[bool, Any]:
    if isinstance(value, dict):
        if key in value:
            return True, value[key]
        for nested in value.values():
            found, result = _find_value(nested, key)
            if found:
                return True, result
    return False, None


def _normal_metadata(value: Any) -> Any:
    if isinstance(value, str):
        return _normal_time(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, default=str)
    return value


def _provenance_failures(
    actual: dict[str, Any],
    expected_node: dict[str, Any],
    expected_versions: list[dict[str, Any]],
    node_keys: dict[str, Any],
    version_keys: dict[str, Any],
    versions_field: str,
) -> list[str]:
    failures: list[str] = []
    for field, expected in expected_node.items():
        key = str(node_keys.get(field) or field)
        present, value = _find_value(actual, key)
        if not present or _normal_metadata(value) != _normal_metadata(expected):
            failures.append(f"provenance field {field} mismatch")
    if expected_versions:
        present, rows = _find_value(actual, versions_field)
        if not present or not isinstance(rows, list) or len(rows) != len(expected_versions):
            failures.append("version provenance row count mismatch")
        else:
            for index, (expected, actual_row) in enumerate(zip(expected_versions, rows, strict=True), 1):
                if not isinstance(actual_row, dict):
                    failures.append(f"version provenance row {index} is not an object")
                    continue
                for field, expected_value in expected.items():
                    key = str(version_keys.get(field) or field)
                    found, value = _find_value(actual_row, key)
                    if not found or _normal_metadata(value) != _normal_metadata(expected_value):
                        failures.append(f"version provenance row {index} field {field} mismatch")
    return failures


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
