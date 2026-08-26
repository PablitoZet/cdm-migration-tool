"""Resilient, bounded-memory OpenText Content Server REST client."""

from __future__ import annotations

import email.utils
import io
import json
import logging
import random
import threading
import time
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, BinaryIO

from .models import (
    AmbiguousRemoteCommit,
    RetryableMigrationError,
    SourceNode,
    SourceVersion,
    TerminalMigrationError,
    UploadResult,
)

logger = logging.getLogger("CDM.OpenText")


class _TokenManager:
    def __init__(self, authenticate: Callable[[], str], verify: Callable[[str], bool], ttl_seconds: int = 900):
        self._authenticate = authenticate
        self._verify = verify
        self._ttl_seconds = ttl_seconds
        self._ticket: str | None = None
        self._acquired = 0.0
        self._lock = threading.Condition()
        self._refreshing = False

    def get(self, force: bool = False) -> str:
        with self._lock:
            while self._refreshing:
                self._lock.wait()
            stale = time.monotonic() - self._acquired >= self._ttl_seconds
            if self._ticket and not force and not stale:
                return self._ticket
            previous = self._ticket
            self._refreshing = True
        try:
            valid = False
            if previous and not force:
                try:
                    valid = self._verify(previous)
                except Exception:
                    logger.warning("Ticket keep-alive probe failed; authenticating again", exc_info=True)
            ticket = previous if valid else self._authenticate()
            assert ticket is not None
        finally:
            with self._lock:
                if "ticket" in locals():
                    self._ticket = ticket
                    self._acquired = time.monotonic()
                self._refreshing = False
                self._lock.notify_all()
        return ticket

    def peek(self) -> str | None:
        """Return the cached ticket without authenticating or performing I/O."""
        with self._lock:
            return self._ticket

    def invalidate(self, ticket: str) -> None:
        with self._lock:
            if self._ticket == ticket:
                self._ticket = None


class AdaptiveRateLimiter:
    """Small global limiter; 429 responses reduce the allowed request rate."""

    def __init__(self, requests_per_second: float = 8.0, minimum: float = 0.5):
        self.rate = max(minimum, requests_per_second)
        self.minimum = minimum
        self._next = 0.0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            delay = max(0.0, self._next - now)
            self._next = max(now, self._next) + 1.0 / self.rate
        if delay:
            time.sleep(delay)

    def throttled(self) -> None:
        with self._lock:
            self.rate = max(self.minimum, self.rate * 0.7)

    def successful(self) -> None:
        with self._lock:
            self.rate = min(self.rate + 0.02, 100.0)


class MultipartStream(io.RawIOBase):
    """Streaming multipart/form-data body with a stable Content-Length."""

    def __init__(
        self, fields: dict[str, str | int], file_field: str, file_name: str,
        mime_type: str, stream: BinaryIO, size: int,
    ):
        boundary = f"cdm-{uuid.uuid4().hex}"
        parts: list[bytes] = []
        for key, value in fields.items():
            parts.append(
                f"--{boundary}\r\nContent-Disposition: form-data; name=\"{key}\"\r\n\r\n{value}\r\n".encode()
            )
        parts.append(
            (
                f"--{boundary}\r\nContent-Disposition: form-data; name=\"{file_field}\"; "
                f"filename=\"{file_name.replace(chr(34), '_')}\"\r\nContent-Type: {mime_type}\r\n\r\n"
            ).encode()
        )
        self.prefix = b"".join(parts)
        self.suffix = f"\r\n--{boundary}--\r\n".encode()
        self.content_type = f"multipart/form-data; boundary={boundary}"
        self.stream = stream
        self.file_size = size
        self.length = len(self.prefix) + size + len(self.suffix)
        self._prefix_pos = 0
        self._file_read = 0
        self._suffix_pos = 0

    def __len__(self) -> int:
        return self.length

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            size = self.length
        out = bytearray()
        if self._prefix_pos < len(self.prefix) and size:
            count = min(size, len(self.prefix) - self._prefix_pos)
            out += self.prefix[self._prefix_pos:self._prefix_pos + count]
            self._prefix_pos += count
            size -= count
        if self._file_read < self.file_size and size:
            count = min(size, self.file_size - self._file_read)
            chunk = self.stream.read(count)
            if not chunk:
                raise OSError(f"Source stream ended {self.file_size - self._file_read} bytes early")
            out += chunk
            self._file_read += len(chunk)
            size -= len(chunk)
        if self._file_read == self.file_size and self._suffix_pos < len(self.suffix) and size:
            count = min(size, len(self.suffix) - self._suffix_pos)
            out += self.suffix[self._suffix_pos:self._suffix_pos + count]
            self._suffix_pos += count
        return bytes(out)


@dataclass(frozen=True)
class ResponseInfo:
    status: int
    correlation_id: str | None


class OpenTextCloudClient:
    def __init__(self, cloud_config: Any, max_retries: int = 5):
        try:
            import requests
        except ImportError as exc:
            raise RuntimeError("requests is required for OpenText connectivity") from exc
        self._requests = requests
        getter = cloud_config.get
        self.base_url = str(getter("ot_cloud_url", "")).rstrip("/")
        self.username = str(getter("ot_cloud_user", ""))
        self.password = str(getter("ot_cloud_password", ""))
        self.verify_ssl = bool(getter("verify_ssl", True))
        self.max_retries = int(getter("max_retries", max_retries))
        self.connect_timeout = float(getter("connect_timeout_seconds", 15))
        self.read_timeout = float(getter("read_timeout_seconds", 180))
        self.multipart_threshold = int(getter("multipart_threshold_bytes", 50 * 1024 * 1024))
        self.multipart_part_size = int(getter("multipart_part_size_bytes", 16 * 1024 * 1024))
        self.workspace_routes = getter("workspace_routes", {}) or {}
        self.multipart_version_target_field = str(getter("multipart_version_target_field", "node_id"))
        self.migration_category_id = getter("migration_category_id")
        self.migration_attribute_key = getter("migration_attribute_key")
        self.owner_mappings = getter("owner_mappings", {}) or {}
        self.system_attribute_strategy = getter("system_attribute_strategy")
        self.permission_strategy = getter("permission_strategy")
        self.source_root_maps_to_target = bool(getter("source_root_maps_to_target", False))
        self.target_root_id = int(getter("target_workspace_nodeid", 0) or 0)
        self.system_attribute_field_map = getter("system_attribute_field_map", {}) or {
            "created_at": "create_date",
            "modified_at": "modify_date",
            "owner_id": "owner_id",
        }
        self._thread_local = threading.local()
        self.event_sink: Callable[[dict[str, Any]], None] | None = None
        self.rate_limiter = AdaptiveRateLimiter(float(getter("requests_per_second", 8.0)))
        self._token_manager = _TokenManager(
            self._authenticate_raw,
            self._verify_ticket,
            ttl_seconds=max(60, int(getter("ticket_keepalive_seconds", 900))),
        )

    def _session(self):
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = self._requests.Session()
            session.verify = self.verify_ssl
            adapter = self._requests.adapters.HTTPAdapter(
                pool_connections=4, pool_maxsize=4, max_retries=0, pool_block=True
            )
            session.mount("https://", adapter)
            self._thread_local.session = session
        return session

    def _authenticate_raw(self) -> str:
        if not self.base_url or not self.username or not self.password:
            raise TerminalMigrationError("OpenText URL or credentials are missing")
        response = self._session().post(
            f"{self.base_url}/api/v1/auth",
            data={"username": self.username, "password": self.password},
            timeout=(self.connect_timeout, self.read_timeout),
        )
        if response.status_code != 200:
            raise TerminalMigrationError(f"OpenText authentication failed: HTTP {response.status_code}")
        ticket = response.json().get("ticket")
        if not ticket:
            raise TerminalMigrationError("OpenText authentication response did not contain a ticket")
        return ticket

    def _verify_ticket(self, ticket: str) -> bool:
        response = self._session().head(
            f"{self.base_url}/api/v1/auth", headers={"OTCSTICKET": ticket},
            timeout=(self.connect_timeout, self.read_timeout),
        )
        return response.status_code == 200

    def _request(
        self, method: str, endpoint: str, *, retry_safe: bool = True,
        expected: Iterable[int] = (200,), stream: bool = False, **kwargs: Any,
    ):
        url = f"{self.base_url}{endpoint}"
        expected_set = set(expected)
        last_error: Exception | None = None
        base_headers = dict(kwargs.pop("headers", {}))
        timeout = kwargs.pop("timeout", (self.connect_timeout, self.read_timeout))
        for attempt in range(1, self.max_retries + 1):
            ticket = self._token_manager.get()
            headers = dict(base_headers)
            headers["OTCSTICKET"] = ticket
            self.rate_limiter.acquire()
            if retry_safe:
                _rewind_files(kwargs.get("files"))
            try:
                response = self._session().request(
                    method, url, headers=headers, stream=stream,
                    timeout=timeout, **kwargs,
                )
            except (self._requests.ConnectionError, self._requests.Timeout) as exc:
                self._emit({
                    "operation": f"{method} {endpoint}", "outcome": "NETWORK_ERROR",
                    "detail": f"{type(exc).__name__}: {exc}",
                })
                if not retry_safe:
                    raise AmbiguousRemoteCommit(f"Ambiguous {method} outcome for {endpoint}: {exc}") from exc
                last_error = exc
                if attempt == self.max_retries:
                    break
                time.sleep(self._backoff(attempt, None))
                continue
            self._emit({
                "operation": f"{method} {endpoint}",
                "outcome": "SUCCESS" if response.status_code in expected_set else "HTTP_ERROR",
                "http_status": response.status_code,
                "correlation_id": self._correlation_id(response.headers),
            })
            if response.status_code in expected_set:
                self.rate_limiter.successful()
                return response
            if response.status_code == 401 and attempt < self.max_retries:
                self._token_manager.invalidate(ticket)
                if not retry_safe:
                    raise AmbiguousRemoteCommit(f"Authentication expired during non-idempotent {method}")
                continue
            if response.status_code == 429:
                self.rate_limiter.throttled()
            if response.status_code in (408, 429, 500, 502, 503, 504) and retry_safe and attempt < self.max_retries:
                time.sleep(self._backoff(attempt, response.headers.get("Retry-After")))
                continue
            detail = response.text[:1000]
            if response.status_code in (400, 403, 404, 409, 413, 422):
                raise TerminalMigrationError(f"OpenText HTTP {response.status_code} for {endpoint}: {detail}")
            raise RetryableMigrationError(f"OpenText HTTP {response.status_code} for {endpoint}: {detail}")
        raise RetryableMigrationError(f"Request failed after {self.max_retries} attempts: {last_error}")

    def _emit(self, event: dict[str, Any]) -> None:
        if self.event_sink:
            try:
                self.event_sink(event)
            except Exception:
                logger.warning("HTTP audit event sink failed", exc_info=True)

    @staticmethod
    def _correlation_id(headers: Any) -> str | None:
        for key in ("X-Correlation-ID", "X-Request-ID", "OTCSTransactionID", "Traceparent"):
            if headers.get(key):
                return str(headers[key])[:500]
        return None

    @staticmethod
    def _backoff(attempt: int, retry_after: str | None) -> float:
        if retry_after:
            try:
                return min(300.0, max(0.0, float(retry_after)))
            except ValueError:
                try:
                    target = email.utils.parsedate_to_datetime(retry_after)
                    return min(300.0, max(0.0, (target - datetime.now(UTC)).total_seconds()))
                except (TypeError, ValueError):
                    pass
        return random.uniform(0.0, min(60.0, 2 ** attempt))

    def test_connection(self) -> dict[str, Any]:
        try:
            ticket = self._token_manager.get()
            return {
                "status": "connected" if self._verify_ticket(ticket) else "error",
                "base_url": self.base_url,
                "user": self.username,
                "tls_verification": self.verify_ssl,
            }
        except Exception as exc:
            return {"status": "error", "error": str(exc), "base_url": self.base_url}

    def get_multipart_settings(self) -> dict[str, Any]:
        response = self._request("GET", "/api/v2/multipart/settings")
        return response.json().get("results", {}).get("data", {})

    def get_node(self, target_id: int) -> dict[str, Any]:
        response = self._request(
            "GET", f"/api/v2/nodes/{target_id}"
            "?fields=properties{id,name,type,parent_id,size,description,create_date,modify_date,owner_id}"
        )
        return response.json().get("results", {}).get("data", {}).get("properties", {})

    def list_permissions(self, target_id: int) -> list[dict[str, Any]]:
        response = self._request("GET", f"/api/v2/nodes/{target_id}/permissions")
        payload = response.json().get("results", [])
        if isinstance(payload, dict):
            payload = payload.get("data", payload.get("permissions", []))
        return [item.get("data", item) for item in payload] if isinstance(payload, list) else []

    def get_category(self, target_id: int, category_id: int) -> dict[str, Any]:
        response = self._request("GET", f"/api/v2/nodes/{target_id}/categories/{category_id}")
        payload = response.json().get("results", {})
        return payload.get("data", payload)

    def list_versions(self, target_id: int) -> list[dict[str, Any]]:
        response = self._request("GET", f"/api/v2/nodes/{target_id}/versions")
        payload = response.json().get("results", [])
        if isinstance(payload, dict):
            payload = payload.get("data", payload.get("versions", []))
        result = []
        for item in payload if isinstance(payload, list) else []:
            result.append(item.get("data", item) if isinstance(item, dict) else {"version_number": item})
        return result

    def find_by_migration_id(self, parent_id: int, migration_id: str) -> int | None:
        if not self.migration_category_id or not self.migration_attribute_key:
            return None
        # Category-backed search is tenant/index dependent. Use a narrow paged
        # parent scan as a deterministic recovery fallback and confirm metadata.
        page = 1
        while True:
            response = self._request(
                "GET", f"/api/v2/nodes/{parent_id}/nodes",
                params={"page": page, "limit": 100, "fields": "properties{id,name,type,parent_id}"},
            )
            payload = response.json()
            for item in payload.get("results", []):
                props = item.get("data", {}).get("properties", {})
                target_id = props.get("id")
                if target_id and self._migration_id_matches(int(target_id), migration_id):
                    return int(target_id)
            collection = payload.get("collection", {})
            if page >= int(collection.get("paging", {}).get("page_total", page)):
                return None
            page += 1

    def _migration_id_matches(self, target_id: int, migration_id: str) -> bool:
        response = self._request(
            "GET", f"/api/v2/nodes/{target_id}/categories/{self.migration_category_id}"
        )
        data = response.json().get("results", {}).get("data", {})
        return str(data.get(str(self.migration_attribute_key), "")) == migration_id

    def create_container(self, node: SourceNode, parent_id: int, migration_id: str) -> int:
        existing = self.find_by_migration_id(parent_id, migration_id)
        if existing:
            return existing
        if node.subtype == 848:
            route = self.workspace_routes.get(str(node.source_id)) or self.workspace_routes.get(node.type_name)
            if not route:
                raise TerminalMigrationError(
                    f"Business Workspace {node.source_id} requires an explicit workspace_routes mapping"
                )
            body = self._with_migration_category({
                "name": node.name,
                "description": node.description,
                "parent_id": parent_id,
                "wksp_type_id": route["workspace_type_id"],
                "template_id": route["template_id"],
            }, migration_id)
            response = self._request(
                "POST", "/api/v2/businessworkspaces/", retry_safe=False,
                expected=(200, 201), data={"body": json.dumps(body)},
            )
        else:
            if node.subtype not in (0, 202, 298):
                raise TerminalMigrationError(f"Unsupported container subtype {node.subtype}")
            body = {"type": node.subtype, "parent_id": parent_id, "name": node.name, "description": node.description}
            response = self._request(
                "POST", "/api/v2/nodes", retry_safe=False, expected=(200, 201),
                data={"body": json.dumps(self._with_migration_category(body, migration_id))},
            )
        target_id = _extract_target_id(response.json())
        if not target_id:
            raise RetryableMigrationError("Create response did not contain a target node ID")
        return target_id

    def upload_first_version(
        self, node: SourceNode, version: SourceVersion, parent_id: int,
        stream: BinaryIO, migration_id: str,
    ) -> UploadResult:
        if version.size >= self.multipart_threshold:
            raise TerminalMigrationError("Large upload must use the checkpointed multipart methods")
        node_body = self._with_migration_category(
            {
                "type": 144,
                "parent_id": parent_id,
                "name": node.name,
                "description": node.description,
            },
            migration_id,
        )
        fields: dict[str, str | int] = {"body": json.dumps(node_body)}
        body = MultipartStream(fields, "file", version.file_name, version.mime_type, stream, version.size)
        response = self._request(
            "POST", "/api/v2/nodes", retry_safe=False, expected=(200, 201), data=body,
            headers={"Content-Type": body.content_type, "Content-Length": str(len(body))},
        )
        target_id = _extract_target_id(response.json())
        if not target_id:
            raise RetryableMigrationError("Upload response did not contain a target node ID")
        return UploadResult(target_id=target_id, version_number=version.version_num)

    def upload_next_version(self, target_id: int, version: SourceVersion, stream: BinaryIO) -> UploadResult:
        if version.size >= self.multipart_threshold:
            raise TerminalMigrationError("Large subsequent versions require tenant-qualified multipart semantics")
        body = MultipartStream({}, "file", version.file_name, version.mime_type, stream, version.size)
        response = self._request(
            "POST", f"/api/v1/nodes/{target_id}/versions", retry_safe=False,
            expected=(200, 201), data=body,
            headers={"Content-Type": body.content_type, "Content-Length": str(len(body))},
        )
        version_number = _extract_version_number(response.json())
        return UploadResult(target_id=target_id, version_number=version_number)

    def start_multipart(self, version: SourceVersion) -> str:
        settings = self.get_multipart_settings()
        if not settings.get("is_enabled"):
            raise TerminalMigrationError("OpenText multipart upload is disabled on this tenant")
        response = self._request(
            "POST", "/api/v2/multipart", retry_safe=False, expected=(200, 201),
            data={"file_name": version.file_name, "file_size": version.size},
        )
        upload_key = _find_json_key(response.json(), "upload_key")
        if not upload_key:
            raise RetryableMigrationError("Multipart start response did not contain upload_key")
        return str(upload_key)

    def upload_multipart_part(self, upload_key: str, part_number: int, data: bytes, file_name: str) -> None:
        # A single bounded part is intentionally materialized. The requests
        # encoder may copy it, but memory remains O(part_size), never O(file).
        self._request(
            "POST", f"/api/v2/multipart/{upload_key}/{part_number}", retry_safe=True,
            expected=(200, 201, 204), files={"file": (file_name, io.BytesIO(data), "application/octet-stream")},
        )

    def complete_multipart(
        self, upload_key: str, node: SourceNode, version: SourceVersion,
        parent_id: int, migration_id: str, *, existing_target_id: int | None = None,
    ) -> UploadResult:
        body: dict[str, Any] = {"file_name": version.file_name}
        if existing_target_id is None:
            body.update({
                "type": 144,
                "parent_id": parent_id,
                "name": node.name,
                "description": node.description,
            })
            body = self._with_migration_category(body, migration_id)
        else:
            # GX39 contract qualification must confirm whether the tenant uses
            # node_id or id for a multipart version completion. The field is
            # configurable to keep this tenant-specific wire detail isolated.
            body[self.multipart_version_target_field] = existing_target_id
            body["add_version"] = True
        response = self._request(
            "POST", f"/api/v2/multipart/{upload_key}", retry_safe=False,
            expected=(200, 201), data={"body": json.dumps(body)},
        )
        target_id = _extract_target_id(response.json())
        if not target_id:
            raise RetryableMigrationError("Multipart completion did not contain a target node ID")
        return UploadResult(target_id=target_id, version_number=version.version_num)

    def create_reference(
        self, node: SourceNode, parent_id: int, migration_id: str,
        *, referenced_target_id: int | None = None,
    ) -> int:
        existing = self.find_by_migration_id(parent_id, migration_id)
        if existing:
            return existing
        if node.subtype == 1:
            if not referenced_target_id:
                raise TerminalMigrationError(f"Shortcut {node.source_id} has no resolved target")
            body = {
                "type": 1,
                "parent_id": parent_id,
                "name": node.name,
                "description": node.description,
                "original_id": referenced_target_id,
            }
        elif node.subtype == 140:
            url = node.extra.get("url")
            if not url:
                raise TerminalMigrationError(f"URL node {node.source_id} has no extracted URL")
            body = {
                "type": 140,
                "parent_id": parent_id,
                "name": node.name,
                "description": node.description,
                "url": url,
            }
        else:
            raise TerminalMigrationError(f"Unsupported reference subtype {node.subtype}")
        response = self._request(
            "POST", "/api/v2/nodes", retry_safe=False, expected=(200, 201),
            data={"body": json.dumps(self._with_migration_category(body, migration_id))},
        )
        target_id = _extract_target_id(response.json())
        if not target_id:
            raise RetryableMigrationError("Reference create response did not contain a node ID")
        return target_id

    def cancel_multipart(self, upload_key: str) -> None:
        self._request("DELETE", f"/api/v2/multipart/{upload_key}", expected=(200, 204, 404))

    def apply_categories(self, target_id: int, categories: list[dict[str, Any]]) -> None:
        grouped: dict[int, dict[str, Any]] = {}
        for attr in categories:
            cat_id = attr.get("target_category_id")
            key_template = attr.get("target_attr_key")
            if not cat_id or not key_template:
                raise TerminalMigrationError(
                    f"Missing target category mapping for source DefID {attr.get('def_id')} attr {attr.get('attr_key')}"
                )
            key = str(key_template).format(row=int(attr.get("row_num") or 0))
            values = grouped.setdefault(int(cat_id), {})
            if key in values:
                raise TerminalMigrationError(
                    f"Category mapping collision for target category {cat_id} attribute {key}"
                )
            values[key] = attr["value"]
        for cat_id, values in grouped.items():
            body = {"category_id": cat_id, **values}
            self._request(
                "PUT", f"/api/v2/nodes/{target_id}/categories/{cat_id}",
                expected=(200, 201, 204), data={"body": json.dumps(body)},
            )

    def apply_system_attributes(
        self, target_id: int, source: SourceNode | SourceVersion, *, version_num: int | None = None,
    ) -> None:
        body: dict[str, Any] = {}
        if source.created_at:
            body[self.system_attribute_field_map["created_at"]] = source.created_at
        if source.modified_at:
            body[self.system_attribute_field_map["modified_at"]] = source.modified_at
        owner_id = getattr(source, "owner_id", None)
        if owner_id is not None:
            mapped_owner = self.owner_mappings.get(str(owner_id))
            if mapped_owner is None:
                raise TerminalMigrationError(f"No target owner mapping for source owner {owner_id}")
            body[self.system_attribute_field_map["owner_id"]] = mapped_owner
        if not body:
            return
        endpoint = (
            f"/api/v2/nodes/{target_id}/versions/{version_num}"
            if version_num is not None else f"/api/v2/nodes/{target_id}/systemattributes"
        )
        self._request(
            "PUT", endpoint, expected=(200, 201, 204), data={"body": json.dumps(body)}
        )

    def apply_permission_policy(self, target_id: int, policy: list[dict[str, Any]]) -> None:
        for operation in policy:
            method = str(operation.get("method", "PUT")).upper()
            suffix = str(operation.get("endpoint", ""))
            if method not in {"POST", "PUT", "DELETE"} or not suffix.startswith("/permissions/"):
                raise TerminalMigrationError("Invalid configured permission policy operation")
            kwargs: dict[str, Any] = {}
            if operation.get("body") is not None:
                kwargs["data"] = {"body": json.dumps(operation["body"])}
            self._request(
                method, f"/api/v2/nodes/{target_id}{suffix}",
                expected=(200, 201, 204), **kwargs,
            )

    def _with_migration_category(self, body: dict[str, Any], migration_id: str) -> dict[str, Any]:
        if not self.migration_category_id or not self.migration_attribute_key:
            raise TerminalMigrationError(
                "migration_category_id and migration_attribute_key are mandatory for idempotent creation"
            )
        body["roles"] = {"categories": {str(self.migration_attribute_key): migration_id}}
        return body

    def _apply_migration_marker(self, target_id: int, migration_id: str) -> None:
        if not self.migration_category_id or not self.migration_attribute_key:
            raise TerminalMigrationError("Migration marker category is required for idempotent uploads")
        body = {"category_id": self.migration_category_id, str(self.migration_attribute_key): migration_id}
        self._request(
            "PUT", f"/api/v2/nodes/{target_id}/categories/{self.migration_category_id}",
            expected=(200, 201, 204), data={"body": json.dumps(body)},
        )

    def iter_content(self, target_id: int, version_num: int | None = None):
        endpoint = (
            f"/api/v1/nodes/{target_id}/versions/{version_num}/content"
            if version_num is not None else f"/api/v2/nodes/{target_id}/content"
        )
        response = self._request("GET", endpoint, stream=True)
        yield from response.iter_content(chunk_size=8 * 1024 * 1024)

    def close(self) -> None:
        session = getattr(self._thread_local, "session", None)
        ticket = self._token_manager.peek()
        try:
            if ticket and session is not None:
                session.delete(
                f"{self.base_url}/api/v1/auth", headers={"OTCSTICKET": ticket},
                timeout=(self.connect_timeout, self.read_timeout),
            )
        except Exception:
            logger.warning("Could not close OpenText session", exc_info=True)
        finally:
            if ticket:
                self._token_manager.invalidate(ticket)
            if session is not None:
                session.close()


def _find_json_key(value: Any, key: str) -> Any:
    if isinstance(value, dict):
        if key in value:
            return value[key]
        for nested in value.values():
            found = _find_json_key(nested, key)
            if found is not None:
                return found
    elif isinstance(value, list):
        for nested in value:
            found = _find_json_key(nested, key)
            if found is not None:
                return found
    return None


def _extract_target_id(payload: Any) -> int | None:
    value = _find_json_key(payload, "id")
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _extract_version_number(payload: Any) -> int | None:
    for key in ("version_number", "version", "version_num"):
        value = _find_json_key(payload, key)
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                pass
    return None


def _rewind_files(files: Any) -> None:
    if not isinstance(files, dict):
        return
    for value in files.values():
        candidate = value[1] if isinstance(value, tuple) and len(value) > 1 else value
        if hasattr(candidate, "seek"):
            candidate.seek(0)
