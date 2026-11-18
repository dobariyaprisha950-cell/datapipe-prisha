"""Atomic, date-partitioned JSONL output."""

import hashlib
import json
import os
import re
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


class JSONLSink:
    def __init__(self, root: str, partition_by: str = "date") -> None:
        self.root = Path(root).resolve()
        self.partition_by = partition_by

    def write(
        self, records: Sequence[Mapping[str, Any]], batch_id: str | None = None
    ) -> list[Path]:
        groups: dict[str, list[Mapping[str, Any]]] = {}
        for record in records:
            key = str(record.get(self.partition_by, "unknown"))[:10]
            if not re.fullmatch(r"[A-Za-z0-9_-]+", key):
                raise ValueError("unsafe partition value")
            groups.setdefault(key, []).append(record)
        paths = []
        for key, rows in groups.items():
            directory = (self.root / f"{self.partition_by}={key}").resolve()
            if self.root not in directory.parents:
                raise ValueError("output path escaped configured root")
            directory.mkdir(parents=True, exist_ok=True)
            payload = "".join(
                json.dumps(dict(row), default=str, separators=(",", ":")) + "\n" for row in rows
            )
            digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
            identity = hashlib.sha256(f"{batch_id or ''}:{digest}".encode()).hexdigest()
            target = directory / f"part-{identity}.jsonl"
            fd, name = tempfile.mkstemp(prefix=".datapipe-", dir=directory, text=True)
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(name, target)
            finally:
                if os.path.exists(name):
                    os.unlink(name)
            paths.append(target)
        return paths
