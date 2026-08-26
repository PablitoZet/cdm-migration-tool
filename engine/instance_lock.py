"""Cross-process guard for a profile state database."""

from __future__ import annotations

import os
from pathlib import Path
from typing import BinaryIO


class InstanceAlreadyRunning(RuntimeError):
    pass


class InstanceLock:
    def __init__(self, state_db_path: str):
        self.path = Path(f"{state_db_path}.instance.lock")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle: BinaryIO | None = self.path.open("a+b")
        try:
            self._acquire()
            self._handle.seek(0)
            self._handle.truncate()
            self._handle.write(f"pid={os.getpid()}\n".encode("ascii"))
            self._handle.flush()
        except Exception:
            self.close()
            raise

    def _acquire(self) -> None:
        assert self._handle is not None
        try:
            import fcntl

            fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except ImportError:
            import msvcrt

            try:
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_NBLCK, 1)  # type: ignore[attr-defined]
            except OSError as exc:
                raise InstanceAlreadyRunning(f"State database is already owned: {self.path}") from exc
        except BlockingIOError as exc:
            raise InstanceAlreadyRunning(f"State database is already owned: {self.path}") from exc

    def close(self) -> None:
        if self._handle is None:
            return
        try:
            try:
                import fcntl

                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
            except (ImportError, OSError):
                pass
        finally:
            self._handle.close()
            self._handle = None

    def __del__(self) -> None:
        self.close()
