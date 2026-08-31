"""Deterministic source inventory signatures used by the freeze gate."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from typing import Any


def source_signature(
    nodes: Sequence[dict[str, Any]],
    versions: Sequence[dict[str, Any]],
    categories: Sequence[dict[str, Any]],
    owners: Sequence[dict[str, Any]] | None = None,
) -> str:
    digest = hashlib.sha256()

    def feed(kind: str, values: tuple[Any, ...]) -> None:
        digest.update(kind.encode("ascii"))
        digest.update(json.dumps(values, default=str, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
        digest.update(b"\n")

    for row in sorted(nodes, key=lambda item: int(item["source_id"])):
        feed("N", (
            row["source_id"], row.get("parent_source_id"), row.get("name"), row.get("subtype"),
            row.get("create_date", row.get("source_created_at")),
            row.get("modify_date", row.get("source_modified_at")),
            row.get("owner_id"), row.get("permissions_id"), row.get("extra", {}),
        ))
    for row in sorted(versions, key=lambda item: (int(item["doc_source_id"]), int(item["version_num"]))):
        feed("V", (
            row["doc_source_id"], row["version_num"], row.get("file_name"), row.get("mime_type"),
            int(row.get("data_size") or 0), row.get("provider_id"), row.get("provider_type"),
            row.get("provider_data"),
            row.get("ver_create_date", row.get("source_created_at")),
            row.get("ver_modify_date", row.get("source_modified_at")),
            row.get("ver_file_date", row.get("source_file_date")),
            row.get("version_comment", row.get("comment")),
        ))
    for row in sorted(categories, key=lambda item: (
        int(item.get("source_id", item.get("doc_source_id", 0))), int(item["def_id"]),
        str(item.get("attr_key", item.get("attr_id", ""))), int(item.get("row_num") or 0),
    )):
        feed("C", (
            row.get("source_id", row.get("doc_source_id")), row["def_id"],
            row.get("attr_key", row.get("attr_id")), int(row.get("row_num") or 0),
            row.get("value", row.get("val_str")), row.get("val_long"), row.get("val_int"),
            row.get("val_real"), row.get("val_date"),
        ))
    for row in sorted(owners or (), key=lambda item: int(item.get("source_owner_id", item.get("owner_id", 0)))):
        feed("O", (
            row.get("source_owner_id", row.get("owner_id")),
            row.get("login"),
            row.get("email"),
            row.get("display_name"),
            row.get("active"),
            row.get("status_value"),
            row.get("status_source"),
            row.get("identity_status"),
        ))
    return digest.hexdigest()


def discovery_summary(
    nodes: Sequence[dict[str, Any]],
    versions: Sequence[dict[str, Any]],
    categories: Sequence[dict[str, Any]],
    owners: Sequence[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    subtype_counts: dict[str, int] = {}
    for node in nodes:
        key = f"{node.get('subtype')}:{node.get('type_name', 'Unknown')}"
        subtype_counts[key] = subtype_counts.get(key, 0) + 1
    provider_counts: dict[str, int] = {}
    for version in versions:
        key = str(version.get("provider_type") or "unknown")
        provider_counts[key] = provider_counts.get(key, 0) + 1
    return {
        "root": nodes[0] if nodes else None,
        "nodes": len(nodes),
        "documents": sum(node.get("subtype") in (136, 144, 154, 751) for node in nodes),
        "containers": sum(node.get("subtype") in (0, 202, 298, 848, 899) for node in nodes),
        "references": sum(node.get("subtype") in (1, 140) for node in nodes),
        "active_reservations": sum(
            (node.get("extra") or {}).get("reserved_by") not in (None, 0, "0", False, "")
            for node in nodes
        ),
        "versions": len(versions),
        "version_comments": sum(bool(version.get("version_comment")) for version in versions),
        "bytes": sum(int(version.get("data_size") or 0) for version in versions),
        "max_file_bytes": max((int(version.get("data_size") or 0) for version in versions), default=0),
        "max_depth": max((int(node.get("depth") or 0) for node in nodes), default=0),
        "subtypes": subtype_counts,
        "category_definitions": sorted({int(row["def_id"]) for row in categories}),
        "permission_ids": len({node.get("permissions_id") for node in nodes if node.get("permissions_id") is not None}),
        "owner_ids": len({node.get("owner_id") for node in nodes if node.get("owner_id") is not None}),
        "owner_identities": len(owners or ()),
        "owner_identity_statuses": {
            str(status): sum(1 for owner in owners or () if str(owner.get("identity_status") or status) == status)
            for status in sorted({str(owner.get("identity_status") or "UNKNOWN") for owner in owners or ()})
        },
        "providers": provider_counts,
        "signature": source_signature(nodes, versions, categories, owners),
    }
