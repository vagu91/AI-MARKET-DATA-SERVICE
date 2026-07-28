from __future__ import annotations

import os
import threading
import time
from pathlib import Path
from typing import BinaryIO


_PROCESS_LOCKS: dict[str, threading.Lock] = {}
_PROCESS_LOCKS_GUARD = threading.Lock()


class ForceGenerationLease:
    """Cross-thread/process single-flight lease without canonical DB writes."""

    def __init__(self, database_path: Path) -> None:
        resolved = Path(database_path).resolve()
        self.lock_path = resolved.with_name(
            f".{resolved.stem}.force-generation.lock"
        )
        key = str(resolved)
        with _PROCESS_LOCKS_GUARD:
            self.process_lock = _PROCESS_LOCKS.setdefault(
                key,
                threading.Lock(),
            )
        self.handle: BinaryIO | None = None

    def acquire(self) -> None:
        self.process_lock.acquire()
        try:
            self.lock_path.parent.mkdir(parents=True, exist_ok=True)
            self.handle = self.lock_path.open("a+b")
            self.handle.seek(0, os.SEEK_END)
            if self.handle.tell() == 0:
                self.handle.write(b"\0")
                self.handle.flush()
            self.handle.seek(0)
            _lock_file(self.handle)
        except Exception:
            if self.handle is not None:
                self.handle.close()
                self.handle = None
            self.process_lock.release()
            raise

    def release(self) -> None:
        try:
            if self.handle is not None:
                _unlock_file(self.handle)
                self.handle.close()
                self.handle = None
        finally:
            self.process_lock.release()


def _lock_file(handle: BinaryIO) -> None:
    if os.name == "nt":
        import msvcrt

        while True:
            try:
                msvcrt.locking(
                    handle.fileno(),
                    msvcrt.LK_NBLCK,
                    1,
                )
                return
            except OSError:
                time.sleep(0.05)
    else:
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)


def _unlock_file(handle: BinaryIO) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
