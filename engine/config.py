"""Configuration loading and atomic local persistence."""

from __future__ import annotations

import json
import os
import re
import tempfile
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

SECRET_KEYS = {
    "db_password": "CDM_DB_PASSWORD",
    "ot_cloud_password": "CDM_OT_PASSWORD",
    "azure_storage_sas_token": "CDM_AZURE_SAS_TOKEN",
    "azure_storage_sas_url": "CDM_AZURE_SAS_URL",
}
PROFILE_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_-]{1,47}$")
ENVIRONMENT_CLASSES = frozenset({"sandbox", "test", "production"})


class ConfigurationError(ValueError):
    pass


def normalize_profile_values(profile_id: str, raw: dict[str, Any]) -> dict[str, Any]:
    """Derive technical settings from the small operator-facing profile contract."""
    values = dict(raw)
    # Fixed policy for the selected source scope and GX39 destination.
    values["source_root_maps_to_target"] = False
    values["system_attribute_strategy"] = "preserve"
    values["permission_strategy"] = "inherit_target"
    values["migration_namespace"] = str(
        values.get("migration_namespace") or f"cdm-{profile_id}"
    )

    marker_key = str(values.get("migration_attribute_key") or "").strip()
    if marker_key:
        match = re.match(r"^(\d+)_", marker_key)
        if not match:
            raise ConfigurationError(
                "Migration marker attribute key must use the OpenText format '<category_id>_<attribute_id>'"
            )
        derived_category = int(match.group(1))
        configured_category = int(values.get("migration_category_id") or derived_category)
        if configured_category != derived_category:
            raise ConfigurationError(
                "Migration marker category ID does not match the attribute key prefix"
            )
        values["migration_category_id"] = derived_category
        values["migration_attribute_key"] = marker_key
    else:
        values["migration_category_id"] = None
        values["migration_attribute_key"] = None

    sas_url = str(values.get("azure_storage_sas_url") or "").strip()
    if sas_url:
        parsed = urlparse(sas_url)
        if parsed.scheme.lower() != "https" or not parsed.netloc:
            raise ConfigurationError("Azure Container SAS URL must be a complete HTTPS URL")
        path_parts = [unquote(part) for part in parsed.path.split("/") if part]
        if not path_parts:
            raise ConfigurationError(
                "Azure SAS URL must identify the content container, not only the storage account"
            )
        if len(path_parts) > 1:
            raise ConfigurationError(
                "Azure SAS URL appears to identify one blob. Generate a container-level SAS URL instead"
            )
        token = parsed.query or str(values.get("azure_storage_sas_token") or "").lstrip("?")
        if not token:
            raise ConfigurationError(
                "Azure Container SAS URL must include its SAS query token"
            )
        values["binary_source_adapter"] = "azure"
        values["azure_storage_account_url"] = f"{parsed.scheme}://{parsed.netloc}"
        values["azure_storage_sas_token"] = token
        values["azure_blob_locator_template"] = f"azure://{path_parts[0]}/{{provider_data}}"
    return values


def _resolve_secret(env_name: str, key: str, raw: dict[str, Any]) -> str:
    configured_var = raw.get(f"{key}_env")
    candidates = [configured_var] if configured_var else []
    candidates += [f"{SECRET_KEYS[key]}_{env_name.upper()}", SECRET_KEYS[key]]
    for variable in candidates:
        if variable and os.getenv(variable):
            return os.environ[variable]
    # This is a localhost-only internal operator tool. Persisted credentials are
    # allowed by design; the config file is restricted to the current OS user.
    value = raw.get(key, "")
    return str(value or "")


@dataclass(frozen=True)
class EnvironmentConfig:
    key: str
    values: dict[str, Any]

    def get(self, key: str, default: Any = None) -> Any:
        return self.values.get(key, default)

    def require(self, *keys: str) -> None:
        missing = [key for key in keys if self.values.get(key) in (None, "")]
        if missing:
            raise ConfigurationError(f"Missing configuration for {self.key}: {', '.join(missing)}")

    def public_view(self) -> dict[str, Any]:
        return {
            key: ("<configured>" if key in SECRET_KEYS and value else "<missing>" if key in SECRET_KEYS else value)
            for key, value in self.values.items()
        }

    @property
    def environment_class(self) -> str:
        configured = str(self.values.get("environment_class", "")).lower()
        if configured in ENVIRONMENT_CLASSES:
            return configured
        return "production" if self.key == "prod" else "test" if self.key == "staging" else "sandbox"

    @property
    def is_production(self) -> bool:
        return self.environment_class == "production"


@dataclass(frozen=True)
class AppConfig:
    default_environment: str
    environments: dict[str, EnvironmentConfig]
    migration_settings: dict[str, Any]
    security: dict[str, Any]

    def environment(self, name: str | None = None) -> EnvironmentConfig:
        key = (name or self.default_environment).lower()
        try:
            return self.environments[key]
        except KeyError as exc:
            raise ConfigurationError(f"Unknown environment: {key}") from exc


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path)
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    with suppress(OSError):
        config_path.chmod(0o600)
    environments: dict[str, EnvironmentConfig] = {}
    for raw_name, raw_values in payload.get("environments", {}).items():
        name = validate_profile_id(raw_name)
        if name in environments:
            raise ConfigurationError(f"Duplicate profile ID after normalization: {name}")
        values = dict(raw_values)
        classification = str(values.get("environment_class", "")).lower()
        if classification and classification not in ENVIRONMENT_CLASSES:
            raise ConfigurationError(f"Invalid environment_class for {name}: {classification}")
        is_production = classification == "production" or (not classification and name == "prod")
        for key in SECRET_KEYS:
            values[key] = _resolve_secret(name, key, raw_values)
        values = normalize_profile_values(name, values)
        values["verify_ssl"] = bool(values.get("verify_ssl", True))
        if is_production and not values["verify_ssl"]:
            raise ConfigurationError("TLS verification cannot be disabled in PROD")
        environments[name] = EnvironmentConfig(name, values)
    if not environments:
        raise ConfigurationError("No environments configured")
    default_environment = str(payload.get("default_environment", next(iter(environments)))).lower()
    if default_environment not in environments:
        raise ConfigurationError(f"Unknown default_environment: {default_environment}")
    return AppConfig(
        default_environment=default_environment,
        environments=environments,
        migration_settings=dict(payload.get("migration_settings", {})),
        security=dict(payload.get("security", {})),
    )


def validate_profile_id(profile_id: str) -> str:
    profile_id = profile_id.strip().lower()
    if not PROFILE_ID_PATTERN.fullmatch(profile_id):
        raise ConfigurationError(
            "Profile ID must start with a letter and contain 2-48 lowercase letters, digits, '-' or '_'"
        )
    return profile_id


def serializable_config(config: AppConfig) -> dict[str, Any]:
    environments: dict[str, dict[str, Any]] = {}
    for key, environment in config.environments.items():
        values = dict(environment.values)
        values["environment_class"] = environment.environment_class
        environments[key] = values
    return {
        "default_environment": config.default_environment,
        "environments": environments,
        "migration_settings": config.migration_settings,
        "security": config.security,
    }


def save_config(config: AppConfig, path: str | Path) -> None:
    """Atomically persist the local operator configuration with mode 0600."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(serializable_config(config), indent=2, sort_keys=True) + "\n"
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=target.parent, prefix=f".{target.name}.", delete=False,
        ) as handle:
            temporary = Path(handle.name)
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        assert temporary is not None
        temporary.chmod(0o600)
        os.replace(temporary, target)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def validate_worker_count(value: int, maximum: int = 16) -> int:
    if not 1 <= value <= maximum:
        raise ConfigurationError(f"worker_threads must be between 1 and {maximum}")
    return value
