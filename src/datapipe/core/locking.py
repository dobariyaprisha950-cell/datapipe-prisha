"""Cross-process advisory locks for a pipeline's output transaction directory."""

from __future__ import annotations

import os
import time
from importlib import import_module
from pathlib import Path
from typing import Any, BinaryIO, Self, cast


class OutputLockTimeout(TimeoutError):
    """Raised when another process holds the output directory lock too long."""


class OutputDirectoryLock:
    """An OS-released advisory lock; the lock file itself is intentionally persistent."""

    def __init__(self, output_root: Path, timeout: float = 30.0) -> None:
        if timeout < 0:
            raise ValueError("lock timeout must not be negative")
        self.path = output_root.resolve() / ".datapipe" / "writer.lock"
        self.timeout = timeout
        self._stream: BinaryIO | None = None

    def acquire(self) -> None:
        deadline = time.monotonic() + self.timeout
        while not self.try_acquire():
            if time.monotonic() >= deadline:
                raise OutputLockTimeout(
                    f"timed out after {self.timeout:g}s waiting for output lock {self.path}"
                )
            time.sleep(min(0.05, max(0.001, deadline - time.monotonic())))

    def try_acquire(self) -> bool:
        """Attempt one non-blocking acquisition, suitable for async polling."""
        if self._stream is not None:
            raise RuntimeError("output lock is already acquired")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        stream = self.path.open("a+b")
        if os.name == "nt":
            msvcrt = cast(Any, import_module("msvcrt"))

            stream.seek(0, os.SEEK_END)
            if stream.tell() == 0:
                stream.write(b"\0")
                stream.flush()
        try:
            if os.name == "nt":
                msvcrt = cast(Any, import_module("msvcrt"))

                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                fcntl = cast(Any, import_module("fcntl"))
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, BlockingIOError):
            stream.close()
            return False
        self._stream = stream
        return True

    def release(self) -> None:
        stream, self._stream = self._stream, None
        if stream is None:
            return
        try:
            if os.name == "nt":
                msvcrt = cast(Any, import_module("msvcrt"))

                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl = cast(Any, import_module("fcntl"))
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        finally:
            stream.close()

    def __enter__(self) -> Self:
        self.acquire()
        return self

    def __exit__(self, *_: object) -> None:
        self.release()
