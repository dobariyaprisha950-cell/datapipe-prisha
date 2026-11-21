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


def _arrow_table(rows: Sequence[Mapping[str, Any]]) -> pa.Table:
    """Build a table while encoding dynamic nested values as Arrow JSON columns.

    Parquet cannot encode an empty struct. JSON extension columns preserve empty
    objects and heterogeneous nested values without inventing struct fields.
    """
    values = [dict(row) for row in rows]
    columns = {name for row in values for name, value in row.items() if isinstance(value, (Mapping, list))}
    json_values: dict[str, list[str | None]] = {}
    for name in columns:
        encoded = [
            None
            if row.get(name) is None
            else json.dumps(row[name], default=str, sort_keys=True, separators=(",", ":"))
            for row in values
        ]
        json_values[name] = encoded
        for row, item in zip(values, encoded, strict=True):
            row[name] = item
    table = pa.Table.from_pylist(values)
    for name, encoded in json_values.items():
        position = table.schema.get_field_index(name)
        table = table.set_column(
            position,
            pa.field(name, pa.json_()),
            pa.array(encoded, type=pa.json_()),
        )
    return table


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
                table = _arrow_table(rows)
                with pa.OSFile(name, "wb") as parquet_file:
                    pq.write_table(table, parquet_file)
                with open(name, "rb+") as parquet_file:
                    parquet_file.flush()
                    os.fsync(parquet_file.fileno())
                os.replace(name, target)
            finally:
                if os.path.exists(name):
                    os.unlink(name)
            output.append(target)
        return output
