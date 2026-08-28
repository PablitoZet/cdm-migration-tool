"""Binary source adapters with bounded-memory reads."""

from __future__ import annotations

import io
import logging
import time
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from .client import _TokenManager
from .models import BinarySource, RetryableMigrationError, SourceVersion, TerminalMigrationError
from .tls import configure_native_trust_store

logger = logging.getLogger("CDM.Source")


class LocalBinarySource(BinarySource):
    """Offline/test adapter for file paths and file:// locators."""

    def validate(self, version: SourceVersion) -> None:
        path = self._path(version)
        if not path.is_file():
            raise TerminalMigrationError(f"Source file does not exist: {path}")
        actual = path.stat().st_size
        if actual != version.size:
            raise TerminalMigrationError(
                f"Source size mismatch for {path}: manifest={version.size}, actual={actual}"
            )

    def open(self, version: SourceVersion, *, offset: int = 0):
        self.validate(version)
        handle = self._path(version).open("rb")
        handle.seek(offset)
        return handle

    @staticmethod
    def _path(version: SourceVersion) -> Path:
        if not version.blob_locator:
            raise TerminalMigrationError(
                f"Missing blob_locator for {version.source_id} v{version.version_num}"
            )
        locator = version.blob_locator
        if len(locator) >= 3 and locator[0].isalpha() and locator[1] == ":" and locator[2] in "\\/":
            return Path(locator)
        parsed = urlparse(locator)
        if parsed.scheme == "file":
            return Path(unquote(parsed.path))
        if parsed.scheme:
            raise TerminalMigrationError(f"Unsupported local locator scheme: {parsed.scheme}")
        return Path(locator)


class _ChunkIteratorReader(io.RawIOBase):
    def __init__(self, chunks, expected_remaining: int):
        self._chunks = iter(chunks)
        self._buffer = memoryview(b"")
        self._remaining = expected_remaining
        self._closed = False

    def readable(self) -> bool:
        return True

    def readinto(self, target) -> int:
        if self._closed or self._remaining <= 0:
            return 0
        view = memoryview(target)
        written = 0
        while written < len(view) and self._remaining > 0:
            if len(self._buffer) == 0:
                try:
                    self._buffer = memoryview(next(self._chunks))
                except StopIteration:
                    break
            count = min(len(view) - written, len(self._buffer), self._remaining)
            view[written:written + count] = self._buffer[:count]
            self._buffer = self._buffer[count:]
            self._remaining -= count
            written += count
        return written

    def close(self) -> None:
        self._closed = True
        super().close()


class AzureBlobBinarySource(BinarySource):
    """Azure Blob adapter loaded lazily so offline tests need no Azure SDK."""

    def __init__(self, account_url: str, credential: str | None = None, *, read_chunk_size: int = 8 * 1024 * 1024):
        self.account_url = account_url.rstrip("/")
        self.credential = credential or None
        self.read_chunk_size = read_chunk_size

    def _client(self, version: SourceVersion):
        configure_native_trust_store()
        try:
            from azure.storage.blob import BlobClient
        except ImportError as exc:
            raise RuntimeError("azure-storage-blob is required for Azure source access") from exc
        locator = version.blob_locator
        if not locator:
            raise TerminalMigrationError(
                f"Missing blob_locator for {version.source_id} v{version.version_num}; "
                "ProviderID alone is not sufficient"
            )
        if locator.startswith("https://"):
            return BlobClient.from_blob_url(locator, credential=self.credential)
        if locator.startswith("azure://"):
            parsed = urlparse(locator)
            return BlobClient(
                account_url=self.account_url,
                container_name=parsed.netloc,
                blob_name=parsed.path.lstrip("/"),
                credential=self.credential,
            )
        raise TerminalMigrationError(f"Unsupported Azure blob locator: {locator[:80]}")

    def validate(self, version: SourceVersion) -> None:
        properties = self._client(version).get_blob_properties()
        actual = int(properties.size)
        if actual != version.size:
            raise TerminalMigrationError(
                f"Azure size mismatch for {version.source_id} v{version.version_num}: "
                f"manifest={version.size}, actual={actual}"
            )

    def open(self, version: SourceVersion, *, offset: int = 0):
        if offset < 0 or offset > version.size:
            raise ValueError("Invalid source offset")
        downloader = self._client(version).download_blob(offset=offset, max_concurrency=1)
        raw = _ChunkIteratorReader(downloader.chunks(), version.size - offset)
        return io.BufferedReader(raw, buffer_size=self.read_chunk_size)


class _ResponseReader(io.RawIOBase):
    def __init__(self, response: Any, expected_remaining: int):
        self._response = response
        self._raw = response.raw
        self._raw.decode_content = True
        self._remaining = expected_remaining
        self._eof_checked = False

    def readable(self) -> bool:
        return True

    def readinto(self, target) -> int:
        if self.closed:
            return 0
        if self._remaining <= 0:
            self._check_no_excess()
            return 0
        view = memoryview(target)
        data = self._raw.read(min(len(view), self._remaining))
        if not data:
            raise OSError("Source Content Server stream ended before the declared version size")
        view[:len(data)] = data
        self._remaining -= len(data)
        if self._remaining == 0:
            self._check_no_excess()
        return len(data)

    def _check_no_excess(self) -> None:
        if self._remaining or self._eof_checked:
            return
        self._eof_checked = True
        if self._raw.read(1):
            raise OSError("Source Content Server stream exceeds the declared version size")

    def close(self) -> None:
        try:
            if not self.closed:
                self._check_no_excess()
        finally:
            self._response.close()
            super().close()


class ContentServerBinarySource(BinarySource):
    """Read version content through the source Content Server REST API."""

    def __init__(
        self, base_url: str, username: str, password: str, *,
        read_chunk_size: int = 8 * 1024 * 1024,
        connect_timeout: float = 15,
        read_timeout: float = 300,
        max_retries: int = 5,
        ticket_keepalive_seconds: int = 900,
        session: Any = None,
    ):
        configure_native_trust_store()
        try:
            import requests
        except ImportError as exc:
            raise RuntimeError("requests is required for source Content Server access") from exc
        self._requests = requests
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.read_chunk_size = read_chunk_size
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.max_retries = max_retries
        self._session = session or requests.Session()
        self._session.verify = True
        self._token_manager = _TokenManager(
            self._authenticate,
            self._verify_ticket,
            ttl_seconds=max(60, ticket_keepalive_seconds),
        )

    def _authenticate(self) -> str:
        if not self.base_url or not self.username or not self.password:
            raise TerminalMigrationError("Source Content Server REST URL or credentials are missing")
        transient = (408, 429, 500, 502, 503, 504)
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self._session.post(
                    f"{self.base_url}/api/v1/auth",
                    data={"username": self.username, "password": self.password},
                    timeout=(self.connect_timeout, self.read_timeout),
                )
            except self._requests.RequestException as exc:
                if attempt == self.max_retries:
                    raise RetryableMigrationError(
                        "Source Content Server authentication request failed after retries"
                    ) from exc
                time.sleep(min(30.0, 2 ** (attempt - 1)))
                continue
            if response.status_code in transient:
                status = response.status_code
                retry_after = response.headers.get("Retry-After")
                response.close()
                if attempt == self.max_retries:
                    raise RetryableMigrationError(
                        f"Source Content Server authentication temporarily failed: HTTP {status}"
                    )
                try:
                    delay = float(retry_after) if retry_after is not None else 2 ** (attempt - 1)
                except ValueError:
                    delay = 2 ** (attempt - 1)
                time.sleep(min(60.0, max(0.0, delay)))
                continue
            break
        else:
            raise RetryableMigrationError("Source Content Server authentication failed after retries")
        if response.status_code != 200:
            response.close()
            raise TerminalMigrationError(
                f"Source Content Server authentication failed: HTTP {response.status_code}"
            )
        try:
            ticket = response.json().get("ticket")
        finally:
            response.close()
        if not ticket:
            raise TerminalMigrationError(
                "Source Content Server authentication response did not contain a ticket"
            )
        return str(ticket)

    def _verify_ticket(self, ticket: str) -> bool:
        response = self._session.head(
            f"{self.base_url}/api/v1/auth",
            headers={"OTCSTICKET": ticket},
            timeout=(self.connect_timeout, self.read_timeout),
        )
        try:
            return response.status_code == 200
        finally:
            response.close()

    def _open_response(self, version: SourceVersion, offset: int = 0):
        if offset < 0 or offset > version.size:
            raise ValueError("Invalid source offset")
        endpoint = (
            f"{self.base_url}/api/v2/nodes/{version.source_id}"
            f"/versions/{version.version_num}/content"
        )
        expected_status = 206 if offset else 200
        expected_remaining = version.size - offset
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            ticket = self._token_manager.get()
            headers = {"OTCSTICKET": ticket, "Accept-Encoding": "identity"}
            if offset:
                headers["Range"] = f"bytes={offset}-"
            try:
                response = self._session.get(
                    endpoint,
                    headers=headers,
                    stream=True,
                    timeout=(self.connect_timeout, self.read_timeout),
                )
            except self._requests.RequestException as exc:
                last_error = exc
                if attempt == self.max_retries:
                    raise RetryableMigrationError(
                        "Source Content Server version download failed after retries"
                    ) from exc
                time.sleep(min(30.0, 2 ** (attempt - 1)))
                continue
            if response.status_code == 401 and attempt < self.max_retries:
                response.close()
                self._token_manager.invalidate(ticket)
                continue
            if response.status_code in (408, 429, 500, 502, 503, 504):
                status = response.status_code
                retry_after = response.headers.get("Retry-After")
                response.close()
                if attempt == self.max_retries:
                    raise RetryableMigrationError(
                        f"Source Content Server version download temporarily failed: HTTP {status}"
                    )
                try:
                    delay = float(retry_after) if retry_after is not None else 2 ** (attempt - 1)
                except ValueError:
                    delay = 2 ** (attempt - 1)
                time.sleep(min(60.0, max(0.0, delay)))
                continue
            if response.status_code != expected_status:
                status = response.status_code
                response.close()
                if offset and status == 200:
                    raise TerminalMigrationError(
                        "Source Content Server ignored the Range request; multipart resume is not safe"
                    )
                raise TerminalMigrationError(
                    f"Source Content Server version download failed: HTTP {status}"
                )
            content_length = response.headers.get("Content-Length")
            content_encoding = str(response.headers.get("Content-Encoding") or "").lower()
            if not content_encoding and content_length is not None and int(content_length) != expected_remaining:
                response.close()
                raise TerminalMigrationError(
                    "Source Content Server version size differs from the source manifest"
                )
            return response
        raise RetryableMigrationError(
            f"Source Content Server version download failed after retries: {type(last_error).__name__}"
        ) from last_error

    def validate(self, version: SourceVersion) -> None:
        response = self._open_response(version)
        response.close()

    def open(self, version: SourceVersion, *, offset: int = 0):
        response = self._open_response(version, offset)
        raw = _ResponseReader(response, version.size - offset)
        return io.BufferedReader(raw, buffer_size=self.read_chunk_size)

    def close(self) -> None:
        ticket = self._token_manager.peek()
        try:
            if ticket:
                response = self._session.delete(
                    f"{self.base_url}/api/v1/auth",
                    headers={"OTCSTICKET": ticket},
                    timeout=(self.connect_timeout, self.read_timeout),
                )
                response.close()
        except Exception:
            logger.warning("Could not close source Content Server session", exc_info=True)
        finally:
            if ticket:
                self._token_manager.invalidate(ticket)
            try:
                self._session.close()
            except Exception:
                logger.warning("Could not close source HTTP session", exc_info=True)


def build_binary_source(config) -> BinarySource:
    adapter = str(config.get("binary_source_adapter", "azure")).lower()
    if adapter == "local":
        return LocalBinarySource()
    if adapter == "azure":
        account_url = config.get("azure_storage_account_url")
        if not account_url:
            account = config.get("azure_storage_account")
            account_url = f"https://{account}.blob.core.windows.net" if account else ""
        if not account_url:
            raise TerminalMigrationError("Azure storage account URL is not configured")
        return AzureBlobBinarySource(account_url, config.get("azure_storage_sas_token"))
    if adapter == "content_server":
        return ContentServerBinarySource(
            str(config.get("source_cs_url") or ""),
            str(config.get("source_cs_user") or ""),
            str(config.get("source_cs_password") or ""),
            connect_timeout=float(config.get("connect_timeout_seconds", 15)),
            read_timeout=float(config.get("source_read_timeout_seconds", 300)),
            max_retries=int(config.get("max_retries", 5)),
            ticket_keepalive_seconds=int(config.get("ticket_keepalive_seconds", 900)),
        )
    raise TerminalMigrationError(f"Unknown binary_source_adapter: {adapter}")
