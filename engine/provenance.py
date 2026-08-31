"""Owner identity and provenance contracts shared by the migration pipeline."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .models import TerminalMigrationError

METADATA_CONTRACT_VERSION = "owner-provenance-v1"
PROVENANCE_CATEGORY_NAME = "CDM Migration Provenance"
SOURCE_SYSTEM_NAME = "OpenText Content Server"

NODE_PROVENANCE_FIELDS = (
    "source_data_id",
    "source_created_at",
    "source_modified_at",
    "source_owner_id",
    "source_owner_login",
    "source_owner_email",
    "source_owner_display_name",
    "source_owner_resolution_status",
    "source_system",
)
VERSION_PROVENANCE_FIELDS = (
    "version_number",
    "version_created_at",
    "version_modified_at",
    "version_file_date",
)

OWNER_EXCEPTION_REASONS = frozenset({
    "UNRESOLVED",
    "DEACTIVATED",
    "AMBIGUOUS",
    "CONFLICTING_IDENTIFIERS",
    "STATUS_UNKNOWN",
})


@dataclass(frozen=True)
class OwnerIdentity:
    """Source KUAF identity retained in the local manifest."""

    source_owner_id: int
    login: str | None = None
    email: str | None = None
    display_name: str | None = None
    active: bool | None = None
    status_value: str | None = None
    status_source: str | None = None
    identity_status: str = "KNOWN"

    @property
    def fingerprint(self) -> str:
        return owner_identity_fingerprint(self)


@dataclass(frozen=True)
class OwnerResolution:
    """Immutable target identity decision for one source owner."""

    source_owner_id: int
    source_identity_fingerprint: str
    target_member_id: int | None
    resolution_status: str
    reason: str | None = None
    target_login: str | None = None
    target_email: str | None = None
    target_display_name: str | None = None
    target_active: bool | None = None
    resolved_at: str | None = None

    @property
    def is_exception(self) -> bool:
        return self.resolution_status != "EXACT"


def normalized_identity(value: Any) -> str | None:
    """Normalize an identity key without changing the original evidence."""
    if value is None:
        return None
    text = str(value).strip().casefold()
    return text or None


def normalized_optional(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def tri_state_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if value == 1:
            return True
        if value == 0:
            return False
    text = str(value).strip().casefold()
    if text in {"1", "true", "t", "yes", "y", "active", "enabled", "open"}:
        return True
    if text in {"0", "false", "f", "no", "n", "inactive", "disabled", "deleted", "closed"}:
        return False
    return None


def owner_identity_from_row(row: Mapping[str, Any]) -> OwnerIdentity:
    source_owner_id = row.get("source_owner_id", row.get("owner_id"))
    if source_owner_id is None:
        raise ValueError("Source owner identity has no OwnerID")
    active = tri_state_bool(row.get("active"))
    identity_status = str(row.get("identity_status") or "").strip().upper() or "KNOWN"
    if active is None and identity_status == "KNOWN":
        identity_status = "STATUS_UNKNOWN"
    return OwnerIdentity(
        source_owner_id=int(source_owner_id),
        login=normalized_optional(row.get("login")),
        email=normalized_optional(row.get("email")),
        display_name=normalized_optional(row.get("display_name")),
        active=active,
        status_value=normalized_optional(row.get("status_value")),
        status_source=normalized_optional(row.get("status_source")),
        identity_status=identity_status,
    )


def owner_identity_fingerprint(owner: OwnerIdentity | Mapping[str, Any]) -> str:
    if not isinstance(owner, OwnerIdentity):
        owner = owner_identity_from_row(owner)
    value = (
        owner.source_owner_id,
        normalized_identity(owner.login),
        normalized_identity(owner.email),
        normalized_identity(owner.display_name),
        owner.active,
        owner.status_value,
        owner.status_source,
        owner.identity_status,
    )
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def source_owner_exception_reason(owner: OwnerIdentity) -> str | None:
    if owner.active is False:
        return "DEACTIVATED"
    if owner.active is None or owner.identity_status in {"STATUS_UNKNOWN", "UNKNOWN"}:
        return "STATUS_UNKNOWN"
    if owner.identity_status in OWNER_EXCEPTION_REASONS:
        return owner.identity_status
    if not owner.login and not owner.email:
        return "UNRESOLVED"
    return None


def exception_set_digest(resolutions: Sequence[OwnerResolution | Mapping[str, Any]]) -> str:
    entries = []
    for resolution in resolutions:
        if not isinstance(resolution, OwnerResolution):
            resolution = owner_resolution_from_row(resolution)
        if resolution.resolution_status == "EXACT":
            continue
        entries.append((
            resolution.source_owner_id,
            resolution.source_identity_fingerprint,
            resolution.reason,
        ))
    entries.sort()
    return hashlib.sha256(
        json.dumps(entries, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()


def owner_resolution_from_row(row: Mapping[str, Any]) -> OwnerResolution:
    return OwnerResolution(
        source_owner_id=int(row["source_owner_id"]),
        source_identity_fingerprint=str(row["source_identity_fingerprint"]),
        target_member_id=int(row["target_member_id"]) if row.get("target_member_id") is not None else None,
        resolution_status=str(row["resolution_status"]),
        reason=str(row["reason"]) if row.get("reason") else None,
        target_login=normalized_optional(row.get("target_login")),
        target_email=normalized_optional(row.get("target_email")),
        target_display_name=normalized_optional(row.get("target_display_name")),
        target_active=tri_state_bool(row.get("target_active")),
        resolved_at=normalized_optional(row.get("resolved_at")),
    )


def _member_id(member: Mapping[str, Any]) -> int | None:
    value = _member_value(member, "id", "member_id", "memberid", "user_id", "userid")
    if value is not None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    return None


def _member_value(member: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if member.get(key) is not None:
            return member[key]
    for nested_key in ("properties", "member", "user", "attributes"):
        nested = member.get(nested_key)
        if isinstance(nested, Mapping):
            value = _member_value(nested, *keys)
            if value is not None:
                return value
    return None


def member_identity(member: Mapping[str, Any]) -> dict[str, Any]:
    """Extract a stable, sanitized identity from a target member response."""
    member_id = _member_id(member)
    return {
        "id": member_id,
        "login": normalized_optional(_member_value(member, "login", "username", "user_name")),
        "email": normalized_optional(_member_value(member, "email", "mail", "mail_address")),
        "display_name": normalized_optional(_member_value(member, "display_name", "displayName", "full_name", "name")),
        "active": tri_state_bool(_member_value(member, "active", "enabled", "is_active", "status")),
    }


def _unique_active_member(
    candidates: Sequence[Mapping[str, Any]],
    *,
    identity_key: str,
    identity_value: str,
) -> tuple[dict[str, Any] | None, str | None]:
    exact: list[dict[str, Any]] = []
    unknown_status = False
    for candidate in candidates:
        member = member_identity(candidate)
        if member["id"] is None:
            continue
        if normalized_identity(member.get(identity_key)) != normalized_identity(identity_value):
            continue
        if member["active"] is None:
            unknown_status = True
            continue
        if member["active"] is not True:
            continue
        exact.append(member)
    unique_ids = {member["id"] for member in exact}
    if len(unique_ids) > 1:
        return None, "AMBIGUOUS"
    if unknown_status:
        return None, "STATUS_UNKNOWN"
    if not exact:
        return None, "UNRESOLVED"
    return exact[0], None


def resolve_owner_identity(
    owner: OwnerIdentity,
    lookup: Callable[[str, str], Sequence[Mapping[str, Any]]],
) -> OwnerResolution:
    """Resolve one source owner using exact, active target identity matches."""
    now = datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    reason = source_owner_exception_reason(owner)
    if reason:
        return OwnerResolution(
            owner.source_owner_id, owner.fingerprint, None, "UNRESOLVED",
            reason=reason, resolved_at=now,
        )

    identifiers = [
        ("login", owner.login),
        ("email", owner.email),
    ]
    matched: list[dict[str, Any]] = []
    for key, value in identifiers:
        if not value:
            continue
        candidates = lookup(key, value)
        member, failure = _unique_active_member(candidates, identity_key=key, identity_value=value)
        if failure:
            return OwnerResolution(
                owner.source_owner_id, owner.fingerprint, None, "UNRESOLVED",
                reason=(
                    "AMBIGUOUS"
                    if failure == "AMBIGUOUS"
                    else "STATUS_UNKNOWN"
                    if failure == "STATUS_UNKNOWN"
                    else "UNRESOLVED"
                ),
                resolved_at=now,
            )
        assert member is not None
        matched.append(member)
    if not matched:
        return OwnerResolution(
            owner.source_owner_id, owner.fingerprint, None, "UNRESOLVED",
            reason="UNRESOLVED", resolved_at=now,
        )
    member_ids = {member["id"] for member in matched}
    if len(member_ids) != 1:
        return OwnerResolution(
            owner.source_owner_id, owner.fingerprint, None, "UNRESOLVED",
            reason="CONFLICTING_IDENTIFIERS", resolved_at=now,
        )
    member = matched[0]
    # When both identifiers are present, both lookups must describe the same
    # exact target identity, not merely the same numeric member ID.
    if owner.login and normalized_identity(member.get("login")) != normalized_identity(owner.login):
        return OwnerResolution(
            owner.source_owner_id, owner.fingerprint, None, "UNRESOLVED",
            reason="CONFLICTING_IDENTIFIERS", resolved_at=now,
        )
    if owner.email and normalized_identity(member.get("email")) != normalized_identity(owner.email):
        return OwnerResolution(
            owner.source_owner_id, owner.fingerprint, None, "UNRESOLVED",
            reason="CONFLICTING_IDENTIFIERS", resolved_at=now,
        )
    return OwnerResolution(
        owner.source_owner_id, owner.fingerprint, int(member["id"]), "EXACT",
        target_login=member.get("login"), target_email=member.get("email"),
        target_display_name=member.get("display_name"), target_active=member.get("active"),
        resolved_at=now,
    )


def resolve_fallback_principal(
    fallback: Mapping[str, Any],
    lookup: Callable[[str, str], Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    """Resolve and validate the configured active fallback principal."""
    member_id = _member_id(fallback)
    login = normalized_optional(fallback.get("login") or fallback.get("username"))
    email = normalized_optional(fallback.get("email") or fallback.get("mail"))
    if member_id is None or (not login and not email):
        raise TerminalMigrationError(
            "Fallback principal requires a member_id and an exact login or email"
        )
    matches: list[dict[str, Any]] = []
    for key, value in (("login", login), ("email", email)):
        if value:
            member, failure = _unique_active_member(
                lookup(key, value), identity_key=key, identity_value=value
            )
            if failure or member is None:
                raise TerminalMigrationError(
                    f"Fallback principal {key} is not a unique active GX39 user"
                )
            matches.append(member)
    if any(int(member["id"]) != member_id for member in matches):
        raise TerminalMigrationError("Fallback principal identifiers resolve to different GX39 users")
    member = matches[0]
    if login and normalized_identity(member.get("login")) != normalized_identity(login):
        raise TerminalMigrationError("Fallback principal login read-back does not match")
    if email and normalized_identity(member.get("email")) != normalized_identity(email):
        raise TerminalMigrationError("Fallback principal email read-back does not match")
    return member


def fallback_resolve(
    owner: OwnerIdentity,
    fallback_member: Mapping[str, Any],
    *,
    reason: str,
    resolved_at: str | None = None,
) -> OwnerResolution:
    member = member_identity(fallback_member)
    if member["id"] is None or member["active"] is not True:
        raise TerminalMigrationError("Fallback target must be an active GX39 user")
    return OwnerResolution(
        owner.source_owner_id,
        owner.fingerprint,
        int(member["id"]),
        "APPROVED_FALLBACK",
        reason=reason,
        target_login=member.get("login"),
        target_email=member.get("email"),
        target_display_name=member.get("display_name"),
        target_active=member.get("active"),
        resolved_at=resolved_at or datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
    )


def provenance_values(
    node: Mapping[str, Any],
    owner: OwnerIdentity,
    resolution: OwnerResolution,
) -> dict[str, Any]:
    """Build deterministic semantic provenance values before target mapping."""
    return {
        "source_data_id": str(node["source_id"]),
        "source_created_at": utc_iso(node.get("source_created_at", node.get("create_date"))),
        "source_modified_at": utc_iso(node.get("source_modified_at", node.get("modify_date"))),
        "source_owner_id": str(owner.source_owner_id),
        "source_owner_login": owner.login,
        "source_owner_email": owner.email,
        "source_owner_display_name": owner.display_name,
        "source_owner_resolution_status": resolution.resolution_status,
        "source_system": SOURCE_SYSTEM_NAME,
    }


def version_provenance_values(version: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "version_number": int(version["version_num"]),
        "version_created_at": utc_iso(version.get("source_created_at", version.get("ver_create_date"))),
        "version_modified_at": utc_iso(version.get("source_modified_at", version.get("ver_modify_date"))),
        "version_file_date": utc_iso(version.get("source_file_date", version.get("ver_file_date"))),
    }


def utc_iso(value: Any) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip().replace(" ", "T")
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise TerminalMigrationError(f"Invalid source date for provenance: {value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def mapped_provenance_payload(
    semantic_values: Mapping[str, Any],
    attribute_keys: Mapping[str, Any],
) -> dict[str, Any]:
    """Map semantic fields to qualified target keys without guessing wire shape."""
    payload: dict[str, Any] = {}
    for field, value in semantic_values.items():
        key = attribute_keys.get(field)
        if not key:
            raise TerminalMigrationError(
                f"Missing qualified provenance attribute mapping for {field}"
            )
        payload[str(key)] = value
    return payload


def sanitized_owner_summary(owner: OwnerIdentity) -> dict[str, Any]:
    return {
        "source_owner_id": owner.source_owner_id,
        "has_login": bool(owner.login),
        "has_email": bool(owner.email),
        "has_display_name": bool(owner.display_name),
        "active": owner.active,
        "identity_status": owner.identity_status,
    }
