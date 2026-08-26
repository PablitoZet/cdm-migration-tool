"""Binary source adapters with bounded-memory reads."""

from __future__ import annotations

import io
from pathlib import Path
from urllib.parse import unquote, urlparse

from .models import BinarySource, SourceVersion, TerminalMigrationError


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
    raise TerminalMigrationError(f"Unknown binary_source_adapter: {adapter}")
