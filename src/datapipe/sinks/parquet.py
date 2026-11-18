"""Atomic partitioned Parquet output."""

import hashlib
import json
import os
import re
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


class ParquetSink:
    def __init__(self, root: str, partition_by: str = "date") -> None:
        self.root = Path(root).resolve()
        self.partition_by = partition_by

    def write(
        self, records: Sequence[Mapping[str, Any]], batch_id: str | None = None
    ) -> list[Path]:
        groups: dict[str, list[Mapping[str, Any]]] = {}
        for row in records:
            key = str(row.get(self.partition_by, "unknown"))[:10]
            if not re.fullmatch(r"[A-Za-z0-9_-]+", key):
                raise ValueError("unsafe partition value")
            groups.setdefault(key, []).append(row)
        output = []
        for key, rows in groups.items():
            folder = (self.root / f"{self.partition_by}={key}").resolve()
            if self.root not in folder.parents:
                raise ValueError("output path escaped configured root")
            folder.mkdir(parents=True, exist_ok=True)
            encoded = json.dumps(
                [dict(row) for row in rows], default=str, separators=(",", ":"), sort_keys=True
            ).encode("utf-8")
            digest = hashlib.sha256(encoded).hexdigest()
            identity = hashlib.sha256(f"{batch_id or ''}:{digest}".encode()).hexdigest()
            target = folder / f"part-{identity}.parquet"
            fd, name = tempfile.mkstemp(dir=folder, prefix=".datapipe-")
            os.close(fd)
            try:
                pq.write_table(pa.Table.from_pylist([dict(r) for r in rows]), name)
                os.replace(name, target)
            finally:
                if os.path.exists(name):
                    os.unlink(name)
            output.append(target)
        return output
