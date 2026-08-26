"""Shared domain types for the migration engine.

This module deliberately has no third-party dependencies so state and pipeline
tests can run on an isolated workstation.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, BinaryIO, Protocol


class RunMode(StrEnum):
    DRY_RUN = "dry_run"
    PILOT = "pilot"
    FULL = "full"
    VERIFY_ONLY = "verify_only"


class RunStatus(StrEnum):
    CREATED = "CREATED"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    STOPPING = "STOPPING"
    COMPLETED = "COMPLETED"
    COMPLETED_WITH_ERRORS = "COMPLETED_WITH_ERRORS"
    FAILED = "FAILED"
    STOPPED = "STOPPED"


class ItemState(StrEnum):
    READY = "READY"
    CLAIMED = "CLAIMED"
    UPLOADING = "UPLOADING"
    REMOTE_COMMITTED = "REMOTE_COMMITTED"
    METADATA_APPLIED = "METADATA_APPLIED"
    VERIFIED = "VERIFIED"
    RETRY_WAIT = "RETRY_WAIT"
    BLOCKED = "BLOCKED"
    FAILED_TERMINAL = "FAILED_TERMINAL"
    SIMULATED = "SIMULATED"
    SKIPPED = "SKIPPED"


TERMINAL_STATES = {
    ItemState.VERIFIED,
    ItemState.FAILED_TERMINAL,
    ItemState.SIMULATED,
    ItemState.SKIPPED,
}


CONTAINER_TYPES = frozenset({0, 202, 298, 848, 899})
DOCUMENT_TYPES = frozenset({136, 144, 154, 751})
REFERENCE_TYPES = frozenset({1, 140})


@dataclass(frozen=True)
class SourceVersion:
    source_id: int
    version_num: int
    file_name: str
    mime_type: str
    size: int
    provider_id: int | None = None
    provider_data: str | None = None
    blob_locator: str | None = None
    source_sha256: str | None = None
    created_at: str | None = None
    modified_at: str | None = None
    comment: str | None = None


@dataclass(frozen=True)
class SourceNode:
    source_id: int
    parent_source_id: int | None
    name: str
    subtype: int
    type_name: str
    depth: int
    path: str
    description: str = ""
    created_at: str | None = None
    modified_at: str | None = None
    owner_id: int | None = None
    group_id: int | None = None
    permissions_id: int | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class UploadResult:
    target_id: int
    version_number: int | None = None
    remote_fingerprint: str | None = None


@dataclass(frozen=True)
class MultipartCheckpoint:
    upload_key: str
    next_part: int
    part_size: int


class BinarySource(Protocol):
    def open(self, version: SourceVersion, *, offset: int = 0) -> BinaryIO: ...
    def validate(self, version: SourceVersion) -> None: ...


class TargetRepository(Protocol):
    def test_connection(self) -> dict[str, Any]: ...
    def get_multipart_settings(self) -> dict[str, Any]: ...
    def find_by_migration_id(self, parent_id: int, migration_id: str) -> int | None: ...
    def create_container(self, node: SourceNode, parent_id: int, migration_id: str) -> int: ...
    def upload_first_version(
        self, node: SourceNode, version: SourceVersion, parent_id: int,
        stream: BinaryIO, migration_id: str,
    ) -> UploadResult: ...
    def upload_next_version(
        self, target_id: int, version: SourceVersion, stream: BinaryIO,
    ) -> UploadResult: ...
    def apply_categories(self, target_id: int, categories: list[dict[str, Any]]) -> None: ...
    def apply_system_attributes(
        self, target_id: int, source: SourceNode | SourceVersion, *, version_num: int | None = None,
    ) -> None: ...
    def apply_permission_policy(self, target_id: int, policy: list[dict[str, Any]]) -> None: ...
    def iter_content(self, target_id: int, version_num: int | None = None) -> Any: ...


class RetryableMigrationError(RuntimeError):
    """A transient failure that may be scheduled for another attempt."""


class TerminalMigrationError(RuntimeError):
    """A deterministic failure which requires operator/configuration action."""


class AmbiguousRemoteCommit(RetryableMigrationError):
    """The remote side may have committed but the response was not received."""
