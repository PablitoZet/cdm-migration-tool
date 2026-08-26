"""Hardened local control API for the CDM migration engine v2."""

from __future__ import annotations

import csv
import io
import os
from contextlib import suppress
from dataclasses import replace
from pathlib import Path
from threading import RLock
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Response
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from engine.config import (
    SECRET_KEYS,
    AppConfig,
    ConfigurationError,
    EnvironmentConfig,
    load_config,
    normalize_profile_values,
    save_config,
    validate_profile_id,
)
from engine.manifest import StateConflict
from engine.models import TerminalMigrationError
from engine.pipeline import MigrationPipeline
from engine.preflight import PreflightAuditor
from engine.reconciler import AutomatedVerifier
from engine.version import VERSION

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = Path(os.getenv("CDM_CONFIG_PATH", BASE_DIR / "config.json"))
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="CDM Migration Tool", version=VERSION)
_runtime_lock = RLock()
_config: AppConfig
_pipeline: MigrationPipeline
_verifier: AutomatedVerifier


def _state_path(environment: str) -> str:
    state_dir = Path(os.getenv("CDM_STATE_DIR", BASE_DIR))
    state_dir.mkdir(parents=True, exist_ok=True)
    with suppress(OSError):
        state_dir.chmod(0o700)
    return str(state_dir / f"migration_state_v2_{environment}.db")


def _load_runtime_config() -> AppConfig:
    return load_config(CONFIG_PATH)


def _install_runtime(config: AppConfig) -> None:
    global _config, _pipeline, _verifier
    previous = globals().get("_pipeline")
    if previous is not None:
        previous.close()
    _config = config
    _pipeline = MigrationPipeline(config, _state_path(config.default_environment))
    _verifier = AutomatedVerifier(_pipeline.manifest)


_install_runtime(_load_runtime_config())


def require_api_key() -> None:
    """Compatibility dependency; localhost operation requires no login."""


class EnvSwitchRequest(BaseModel):
    environment: str
    confirmation: str | None = None


class ExtractRequest(BaseModel):
    node_id: int | None = None
    force: bool = False


class MigrateRequest(BaseModel):
    mode: str = Field(pattern="^(dry_run|pilot|full)$")
    threads: int = Field(default=8, ge=1, le=32)
    max_documents: int | None = Field(default=None, ge=1)
    confirmation: str | None = None


class RecoverRequest(BaseModel):
    run_id: str
    threads: int = Field(default=8, ge=1, le=32)
    confirmation: str | None = None


class VerifyRequest(BaseModel):
    run_id: str | None = None
    live: bool = True
    redownload: bool = False


class ProfileRequest(BaseModel):
    values: dict[str, object]
    make_active: bool = False


class CredentialRequest(BaseModel):
    db_password: str | None = None
    ot_cloud_password: str | None = None
    azure_storage_sas_token: str | None = None
    azure_storage_sas_url: str | None = None
    clear: bool = False


class DiscoveryRequest(BaseModel):
    node_id: int | None = Field(default=None, gt=0)


class FreezeRequest(BaseModel):
    operator: str = Field(min_length=2, max_length=200)
    note: str = Field(default="", max_length=1000)
    source_is_read_only: bool
    confirmation: str | None = None


PROFILE_EDITABLE_KEYS = {
    "name", "workspace_title", "environment_class",
    "source_workspace_nodeid", "target_workspace_nodeid", "source_root_maps_to_target",
    "db_host", "db_port", "db_name", "db_user", "db_password_env", "sslmode",
    "db_connect_timeout", "db_statement_timeout_ms",
    "binary_source_adapter", "azure_storage_account_url", "azure_storage_sas_token_env",
    "azure_storage_sas_url_env",
    "azure_blob_locator_template",
    "ot_cloud_url", "otds_url", "ot_cloud_user", "ot_cloud_password_env", "verify_ssl",
    "migration_namespace", "migration_category_id", "migration_attribute_key",
    "permission_strategy", "permission_mappings", "target_acl_approved",
    "system_attribute_strategy", "system_attribute_field_map", "owner_mappings",
    "workspace_routes", "category_mappings", "multipart_version_target_field",
    "workspace_roles_qualified", "lifecycle_operations_qualified",
    "search_and_facets_qualified", "active_workflows_confirmed_zero",
    "legacy_links_qualified", "historical_audit_out_of_scope_approved",
    "personal_state_out_of_scope_approved",
    "requests_per_second", "connect_timeout_seconds", "read_timeout_seconds",
    "ticket_keepalive_seconds",
} | set(SECRET_KEYS)


def _production_profile(environment: EnvironmentConfig | None = None) -> bool:
    return (environment or _config.environment()).is_production


def _require_production_confirmation(value: str | None) -> None:
    if value != "confirmed":
        raise HTTPException(403, "Confirm the production operation in the application")


@app.get("/api/status", dependencies=[Depends(require_api_key)])
def status(deep: bool = False):
    env = _config.environment()
    inventory = _pipeline.manifest.inventory_summary()
    freeze = _pipeline.manifest.freeze_status()
    parity = _pipeline.manifest.parity_report(env)
    pilot_preflight = (
        PreflightAuditor(env, _pipeline.manifest, _config.migration_settings).run(for_mode="pilot")
        if inventory["total_nodes"] else None
    )
    full_preflight = (
        PreflightAuditor(env, _pipeline.manifest, _config.migration_settings).run(for_mode="full")
        if inventory["total_nodes"] else None
    )
    history = _pipeline.manifest.run_history(50)
    successful_modes = sorted({
        str(run["mode"])
        for run in history
        if str(run["status"]) == "COMPLETED" and int(run.get("failed_nodes") or 0) == 0
    })
    verification_run = next((
        run for run in history
        if str(run["mode"]) in {"pilot", "full"}
        and str(run["status"]) == "COMPLETED"
        and int(run.get("failed_nodes") or 0) == 0
    ), None)
    active_run = next((
        run for run in history
        if str(run["status"]) in {"CREATED", "RUNNING", "PAUSED", "STOPPING"}
    ), None)
    source = {"status": "not_checked"}
    target = {"status": "not_checked"}
    if deep:
        source = _pipeline.source_db.test_connection()
        try:
            target = _pipeline.target.test_connection()
        except Exception as exc:
            target = {"status": "error", "error": str(exc)}
    return {
        "active_environment": _config.default_environment,
        "environment_class": env.environment_class,
        "environment_label": env.get("name", _config.default_environment.upper()),
        "workspace_title": env.get("workspace_title", "Workspace"),
        "source_workspace_nodeid": env.get("source_workspace_nodeid"),
        "target_workspace_nodeid": env.get("target_workspace_nodeid"),
        "state_db_file": Path(_state_path(_config.default_environment)).name,
        "source_db": source,
        "target_cloud": target,
        "manifest_stats": inventory,
        "freeze": freeze,
        "parity": parity,
        "workflow": {
            "manifest_ready": bool(inventory["total_nodes"]),
            "marker_ready": bool(env.get("migration_category_id") and env.get("migration_attribute_key")),
            "pilot_ready": bool(pilot_preflight and pilot_preflight["status"] != "FAIL"),
            "pilot_blockers": [
                check["id"] for check in (pilot_preflight or {}).get("checks", [])
                if check["status"] == "FAIL"
            ],
            "full_ready": bool(full_preflight and full_preflight["status"] != "FAIL"),
            "full_blockers": [
                check["id"] for check in (full_preflight or {}).get("checks", [])
                if check["status"] == "FAIL"
            ],
            "successful_modes": successful_modes,
            "active_run": ({
                "run_id": active_run["run_id"],
                "mode": active_run["mode"],
                "status": active_run["status"],
            } if active_run else None),
            "verification_available": bool(verification_run),
            "verification_run_id": verification_run["run_id"] if verification_run else None,
        },
        "settings": _config.migration_settings,
    }


@app.get("/api/profiles", dependencies=[Depends(require_api_key)])
def profiles():
    result = []
    for key, environment in _config.environments.items():
        view = environment.public_view()
        view["profile_id"] = key
        view["environment_class"] = environment.environment_class
        view["active"] = key == _config.default_environment
        view["credentials_configured"] = {
            secret: bool(environment.get(secret)) for secret in SECRET_KEYS
        }
        result.append(view)
    return {"active_profile": _config.default_environment, "profiles": result}


@app.put("/api/profiles/{profile_id}", dependencies=[Depends(require_api_key)])
def upsert_profile(profile_id: str, request: ProfileRequest):
    try:
        profile_id = validate_profile_id(profile_id)
    except ConfigurationError as exc:
        raise HTTPException(422, str(exc)) from exc
    unknown = set(request.values) - PROFILE_EDITABLE_KEYS
    if unknown:
        raise HTTPException(422, f"Unsupported profile fields: {sorted(unknown)}")
    with _runtime_lock:
        if _pipeline.is_running:
            raise HTTPException(409, "Cannot edit profiles while a run is active")
        environments = dict(_config.environments)
        current = environments.get(profile_id)
        values = dict(current.values) if current else {}
        values.update({
            key: value for key, value in request.values.items()
            if key not in SECRET_KEYS or value not in (None, "")
        })
        try:
            values = normalize_profile_values(profile_id, values)
        except ConfigurationError as exc:
            raise HTTPException(422, str(exc)) from exc
        environment = EnvironmentConfig(profile_id, values)
        if environment.environment_class not in {"sandbox", "test", "production"}:
            raise HTTPException(422, "Invalid environment_class")
        environments[profile_id] = environment
        default = profile_id if request.make_active else _config.default_environment
        updated = replace(_config, environments=environments, default_environment=default)
        save_config(updated, CONFIG_PATH)
        _install_runtime(_load_runtime_config())
    return {"status": "saved", "profile_id": profile_id, "active": default == profile_id}


@app.delete("/api/profiles/{profile_id}", dependencies=[Depends(require_api_key)])
def delete_profile(profile_id: str):
    profile_id = profile_id.lower()
    with _runtime_lock:
        if _pipeline.is_running:
            raise HTTPException(409, "Cannot delete profiles while a run is active")
        if profile_id == _config.default_environment:
            raise HTTPException(409, "Activate another profile before deleting this profile")
        if profile_id not in _config.environments:
            raise HTTPException(404, "Profile not found")
        environments = dict(_config.environments)
        del environments[profile_id]
        updated = replace(_config, environments=environments)
        save_config(updated, CONFIG_PATH)
        _install_runtime(_load_runtime_config())
    return {"status": "deleted", "profile_id": profile_id}


@app.put("/api/profiles/{profile_id}/credentials", dependencies=[Depends(require_api_key)])
def set_profile_credentials(profile_id: str, request: CredentialRequest):
    profile_id = profile_id.lower()
    if profile_id not in _config.environments:
        raise HTTPException(404, "Profile not found")
    with _runtime_lock:
        if _pipeline.is_running:
            raise HTTPException(409, "Cannot change credentials while a run is active")
        environments = dict(_config.environments)
        values = dict(environments[profile_id].values)
        if request.clear:
            for key in SECRET_KEYS:
                values[key] = ""
        else:
            values.update({
                key: value for key, value in request.model_dump().items()
                if key in SECRET_KEYS and value
            })
        environments[profile_id] = EnvironmentConfig(profile_id, values)
        save_config(replace(_config, environments=environments), CONFIG_PATH)
        _install_runtime(_load_runtime_config())
    return {
        "status": "cleared" if request.clear else "saved",
        "profile_id": profile_id,
        "persisted": True,
    }


@app.post("/api/environment", dependencies=[Depends(require_api_key)])
def switch_environment(request: EnvSwitchRequest):
    environment = request.environment.lower()
    if environment not in _config.environments:
        raise HTTPException(400, "Unknown environment")
    with _runtime_lock:
        if _pipeline.is_running:
            raise HTTPException(409, "Cannot switch environment while a run is active")
        if _config.environment(environment).is_production:
            _require_production_confirmation(request.confirmation)
        _install_runtime(replace(_config, default_environment=environment))
    return {"status": "success", "active_environment": environment}


@app.post("/api/extract", dependencies=[Depends(require_api_key)])
def extract(request: ExtractRequest):
    if _pipeline.is_running:
        raise HTTPException(409, "Cannot extract during an active run")
    existing = _pipeline.manifest.inventory_summary()["total_nodes"]
    if existing and not request.force:
        return {
            "status": "confirmation_required",
            "message": f"Manifest contains {existing} nodes. Set force=true after taking a state backup.",
        }
    try:
        return _pipeline.run_extraction(request.node_id)
    except Exception as exc:
        raise HTTPException(500, str(exc)) from exc


@app.post("/api/discovery/source", dependencies=[Depends(require_api_key)])
def discover_source(request: DiscoveryRequest):
    if _pipeline.is_running:
        raise HTTPException(409, "Cannot inspect source during an active run")
    try:
        return _pipeline.inspect_source_scope(request.node_id)
    except Exception as exc:
        raise HTTPException(502, f"Source discovery failed: {type(exc).__name__}: {exc}") from exc


@app.post("/api/discovery/target", dependencies=[Depends(require_api_key)])
def discover_target(request: DiscoveryRequest):
    if _pipeline.is_running:
        raise HTTPException(409, "Cannot inspect target during an active run")
    try:
        return _pipeline.inspect_target_root(request.node_id)
    except Exception as exc:
        raise HTTPException(502, f"Target discovery failed: {type(exc).__name__}: {exc}") from exc


@app.get("/api/compatibility", dependencies=[Depends(require_api_key)])
def compatibility():
    return _pipeline.manifest.parity_report(_config.environment())


@app.get("/api/freeze", dependencies=[Depends(require_api_key)])
def freeze_status():
    return _pipeline.manifest.freeze_status()


@app.post("/api/freeze/confirm", dependencies=[Depends(require_api_key)])
def confirm_freeze(request: FreezeRequest):
    if not request.source_is_read_only:
        raise HTTPException(409, "Source read-only acknowledgement is mandatory")
    if _production_profile():
        _require_production_confirmation(request.confirmation)
    if _pipeline.is_running:
        raise HTTPException(409, "Cannot establish a freeze while a run is active")
    try:
        return _pipeline.confirm_source_freeze(request.operator, request.note)
    except (StateConflict, TerminalMigrationError, ValueError) as exc:
        raise HTTPException(409, str(exc)) from exc
    except Exception as exc:
        raise HTTPException(502, f"Freeze verification failed: {type(exc).__name__}: {exc}") from exc


@app.post("/api/preflight", dependencies=[Depends(require_api_key)])
def preflight(online: bool = False, sample_blobs: int = Query(0, ge=0, le=100)):
    return PreflightAuditor(
        _config.environment(), _pipeline.manifest, _config.migration_settings
    ).run(online=online, sample_blobs=sample_blobs)


@app.post("/api/migrate/start", dependencies=[Depends(require_api_key)])
def start_migration(request: MigrateRequest):
    if _production_profile():
        _require_production_confirmation(request.confirmation)
    max_docs = request.max_documents
    if request.mode == "pilot":
        max_docs = max_docs or 100
    try:
        run_id = _pipeline.start_migration(
            max_items=max_docs,
            dry_run=request.mode == "dry_run",
            threads=request.threads,
            mode=request.mode,
        )
        return {"status": "started", "run_id": run_id, "mode": request.mode}
    except (StateConflict, ConfigurationError, TerminalMigrationError, ValueError) as exc:
        raise HTTPException(409, str(exc)) from exc


@app.post("/api/migrate/recover", dependencies=[Depends(require_api_key)])
def recover(request: RecoverRequest):
    if _production_profile():
        _require_production_confirmation(request.confirmation)
    try:
        _pipeline.recover_run(request.run_id, request.threads)
        return {"status": "recovering", "run_id": request.run_id}
    except (StateConflict, KeyError, TerminalMigrationError, ValueError) as exc:
        raise HTTPException(409, str(exc)) from exc


@app.post("/api/migrate/pause", dependencies=[Depends(require_api_key)])
def pause():
    try:
        _pipeline.pause()
        return {"status": "paused"}
    except StateConflict as exc:
        raise HTTPException(409, str(exc)) from exc


@app.post("/api/migrate/resume", dependencies=[Depends(require_api_key)])
def resume():
    try:
        _pipeline.resume()
        return {"status": "resumed"}
    except StateConflict as exc:
        raise HTTPException(409, str(exc)) from exc


@app.post("/api/migrate/stop", dependencies=[Depends(require_api_key)])
def stop():
    try:
        _pipeline.stop()
        return {"status": "stopping"}
    except StateConflict as exc:
        raise HTTPException(409, str(exc)) from exc


@app.get("/api/telemetry", dependencies=[Depends(require_api_key)])
def telemetry():
    return _pipeline.get_telemetry()


@app.get("/api/lookup", dependencies=[Depends(require_api_key)])
def lookup(source_id: int):
    mapping = _pipeline.manifest.lookup_mapping(source_id)
    if not mapping:
        return {
            "source_id": source_id,
            "found": False,
            "message": "This object has not been migrated or is not present in the active manifest.",
        }
    base = str(_config.environment().get("ot_cloud_url")).rstrip("/")
    return {
        "source_id": source_id, "found": True, "target_id": mapping["target_id"],
        "name": mapping.get("name") or f"Source object {source_id}",
        "verified": bool(mapping["verified"]),
        "smartview_url": f"{base}/app/nodes/{mapping['target_id']}",
        "direct_download_url": f"{base}?func=doc.fetchcsui&nodeid={mapping['target_id']}&action=download",
    }


@app.post("/api/verify", dependencies=[Depends(require_api_key)])
def verify(request: VerifyRequest):
    run: dict[str, Any] | None
    if request.run_id:
        run = _pipeline.manifest.run_status(request.run_id)
    else:
        run = next((
            candidate for candidate in _pipeline.manifest.run_history(50)
            if str(candidate["mode"]) in {"pilot", "full"}
            and str(candidate["status"]) == "COMPLETED"
            and int(candidate.get("failed_nodes") or 0) == 0
        ), None)
    if not run:
        raise HTTPException(404, "No completed Pilot or Full Cutover run is available for verification")
    if str(run["status"]) != "COMPLETED":
        raise HTTPException(409, "Complete the selected migration run before starting verification")
    client = None
    if request.live:
        try:
            client = _pipeline.target
        except Exception as exc:
            raise HTTPException(503, str(exc)) from exc
    verifier = AutomatedVerifier(_pipeline.manifest, client)
    return verifier.run_all_tests(run["run_id"], live=request.live, redownload=request.redownload)


@app.get("/api/verify", dependencies=[Depends(require_api_key)])
def verify_compatibility():
    """Compatibility route; intentionally performs only local fail-closed checks."""
    run = _pipeline.manifest.latest_run()
    if not run:
        raise HTTPException(404, "No migration run found")
    return AutomatedVerifier(_pipeline.manifest).run_all_tests(run["run_id"], live=False)


@app.get("/api/history", dependencies=[Depends(require_api_key)])
def history():
    return {"runs": _pipeline.manifest.run_history(50)}


@app.get("/api/nodes", dependencies=[Depends(require_api_key)])
def nodes(page: int = Query(1, ge=1), limit: int = Query(25, ge=5, le=100)):
    run = _pipeline.manifest.latest_run()
    if not run:
        return {"items": [], "page": page, "limit": limit, "total_count": 0, "total_pages": 1}
    total = run["total_nodes"]
    items = _pipeline.manifest.list_run_items(run["run_id"], limit, (page - 1) * limit)
    for item in items:
        item["status"] = item["state"]
        item["error_msg"] = item.get("last_error")
        item["dry_run"] = run["mode"] == "dry_run"
        item["checksum_status"] = (
            "SIMULATED_DRYRUN" if item["state"] == "SIMULATED"
            else "MATCH_VERIFIED" if item["state"] == "VERIFIED" and item["subtype"] in (136, 144, 154, 751)
            else "NOT_APPLICABLE" if item["subtype"] not in (136, 144, 154, 751)
            else "UPLOADED_PENDING_AUDIT"
        )
    return {
        "items": items, "page": page, "limit": limit, "total_count": total,
        "total_pages": max(1, (total + limit - 1) // limit),
    }


def _audit_rows():
    run = _pipeline.manifest.latest_run()
    return _pipeline.manifest.verification_items(run["run_id"]) if run else []


@app.get("/api/export/csv", dependencies=[Depends(require_api_key)])
def export_csv():
    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")
    writer.writerow(["Run_ID", "Source_NodeID", "Name", "Type", "Path", "Target_NodeID", "State", "Error"])
    for row in _audit_rows():
        writer.writerow([
            row["run_id"], row["source_id"], row["name"], row["type_name"], row["path"],
            row.get("mapped_target_id") or "", row["state"], row.get("last_error") or "",
        ])
    return Response(
        output.getvalue().encode("utf-8-sig"), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=CDM_Reconciliation_v2.csv"},
    )


@app.get("/api/export/xlsx", dependencies=[Depends(require_api_key)])
def export_xlsx():
    try:
        import openpyxl
    except ImportError as exc:
        raise HTTPException(501, "openpyxl is not installed") from exc
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "Reconciliation"
    sheet.append(["Run ID", "Source NodeID", "Name", "Type", "Path", "Target NodeID", "State", "Error"])
    for row in _audit_rows():
        sheet.append([
            row["run_id"], row["source_id"], row["name"], row["type_name"], row["path"],
            row.get("mapped_target_id") or "", row["state"], row.get("last_error") or "",
        ])
    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = sheet.dimensions
    stream = io.BytesIO()
    workbook.save(stream)
    return Response(
        stream.getvalue(), media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=CDM_Reconciliation_v2.xlsx"},
    )


if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
def index():
    path = STATIC_DIR / "index.html"
    return FileResponse(path) if path.exists() else HTMLResponse(f"<h1>CDM Migration Tool v{VERSION}</h1>")


if __name__ == "__main__":
    import uvicorn
    # Passing an import string would import this module a second time after it
    # has already created the state-store lock under __main__.
    uvicorn.run(app, host="127.0.0.1", port=8110, reload=False)
