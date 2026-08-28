#!/usr/bin/env python3
"""Build a secret-free, checksummed transfer archive."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tomllib
import zipfile
from datetime import UTC, datetime
from pathlib import Path

from engine.version import VERSION

ROOT = Path(__file__).resolve().parent
EXCLUDED_NAMES = {
    "config.json", ".env", ".coverage", ".DS_Store",
}
EXCLUDED_PARTS = {
    ".git", "__pycache__", ".venv", ".pytest_cache", ".ruff_cache", ".mypy_cache", "dist",
}
SENSITIVE_SUFFIXES = {".key", ".pem", ".p12", ".pfx", ".jks"}


def include(path: Path) -> bool:
    relative = path.relative_to(ROOT)
    if path.name in EXCLUDED_NAMES or any(part in EXCLUDED_PARTS for part in relative.parts):
        return False
    if path.name.startswith("migration_state") or path.suffix in {".pyc", ".log"}:
        return False
    if path.suffix.lower() in SENSITIVE_SUFFIXES:
        return False
    return path.is_file()


def validate_release_inputs(files: list[Path]) -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    if project["project"]["version"] != VERSION:
        raise RuntimeError("pyproject.toml version does not match engine.version.VERSION")
    example = json.loads((ROOT / "config.example.json").read_text(encoding="utf-8"))
    for profile_id, profile in example.get("environments", {}).items():
        for key in ("db_password", "ot_cloud_password", "azure_storage_sas_url", "azure_storage_sas_token"):
            if profile.get(key):
                raise RuntimeError(f"config.example.json contains a value for {profile_id}.{key}")
        for key in ("db_host", "ot_cloud_url", "source_workspace_nodeid", "target_workspace_nodeid"):
            if profile.get(key):
                raise RuntimeError(f"config.example.json contains an environment identifier for {profile_id}.{key}")
    forbidden = {"config.json", ".env"}
    for path in files:
        if path.name in forbidden or path.name.startswith("migration_state"):
            raise RuntimeError(f"Sensitive runtime file selected for release: {path}")


def main() -> int:
    compile_result = subprocess.run(
        [sys.executable, "-m", "compileall", "-q", str(ROOT)],
        cwd=ROOT, check=False,
    )
    if compile_result.returncode:
        return compile_result.returncode
    result = subprocess.run(
        [
            sys.executable, "-W", "error::ResourceWarning", "-m", "unittest",
            "discover", "-s", str(ROOT / "tests"), "-v",
        ],
        cwd=ROOT, check=False,
    )
    if result.returncode:
        return result.returncode
    dist = ROOT / "dist"
    dist.mkdir(exist_ok=True)
    version = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    release_version = VERSION.replace(".", "_")
    archive = dist / f"cdm_migration_tool_v{release_version}_{version}.zip"
    files = sorted(path for path in ROOT.rglob("*") if include(path))
    validate_release_inputs(files)
    manifest = {
        str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in files
    }
    manifest["_metadata"] = {
        "application_version": VERSION,
        "created_at": datetime.now(UTC).isoformat(),
        "secrets_included": False,
        "state_databases_included": False,
    }
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as bundle:
        for path in files:
            bundle.write(path, Path("cdm_migration_tool") / path.relative_to(ROOT))
        bundle.writestr(
            "cdm_migration_tool/RELEASE_SHA256.json",
            json.dumps(manifest, indent=2, sort_keys=True),
        )
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    archive.with_suffix(".zip.sha256").write_text(f"{digest}  {archive.name}\n", encoding="ascii")
    print(archive)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
