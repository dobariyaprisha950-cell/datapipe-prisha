"""Atomic JSON checkpoint persistence."""

import json
import os
import tempfile
from pathlib import Path
from typing import Any


class Checkpoint:
    def __init__(self, path: str | Path, signature: str | None = None) -> None:
        self.path = Path(path)
        self.signature = signature

    def load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        value = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise TypeError("checkpoint root must be an object")
        if self.signature is not None:
            if value.get("version") != 1 or value.get("signature") != self.signature:
                raise ValueError("checkpoint format or pipeline configuration does not match")
            cursors = value.get("cursors")
            if not isinstance(cursors, dict):
                raise TypeError("checkpoint cursors must be an object")
            return cursors
        return value

    def save(self, state: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(prefix=".checkpoint-", dir=self.path.parent, text=True)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                payload = state
                if self.signature is not None:
                    payload = {"version": 1, "signature": self.signature, "cursors": state}
                json.dump(payload, stream)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(name, self.path)
        finally:
            if os.path.exists(name):
                os.unlink(name)
