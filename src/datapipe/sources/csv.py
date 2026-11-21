"""Streaming CSV source."""

from __future__ import annotations

import csv
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, cast


class _TrackedLines:
    """Remember the source text consumed for one parsed CSV row."""

    def __init__(self, stream: Any) -> None:
        self.stream = stream
        self.lines: list[str] = []

    def __iter__(self) -> _TrackedLines:
        return self

    def __next__(self) -> str:
        line = cast(str, next(self.stream))
        self.lines.append(line)
        return line

    def take(self) -> str:
        value = "".join(self.lines)
        self.lines.clear()
        return value


def _next_row(reader: csv.DictReader[str], tracked: _TrackedLines) -> dict[str, Any] | None:
    try:
        row = next(reader, None)
    except csv.Error as exc:
        line = reader.line_num
        raw = tracked.take()
        return {
            "__datapipe_error__": {
                "line": line,
                "input": raw,
                "reason": f"malformed CSV: {exc}",
            }
        }
    tracked.take()
    return cast(dict[str, Any] | None, row)


class CSVSource:
    def __init__(
        self,
        name: str,
        path: str,
        delimiter: str = ",",
        encoding: str = "utf-8",
        start_row: int = 0,
    ) -> None:
        self.name, self.path, self.delimiter, self.encoding, self.cursor = (
            name,
            Path(path),
            delimiter,
            encoding,
            start_row,
        )

    async def read(self) -> AsyncIterator[dict[str, Any]]:
        # Read bounded batches; never materialize the complete input dataset.
        stream = self.path.open(newline="", encoding=self.encoding)
        try:
            tracked = _TrackedLines(stream)
            reader = csv.DictReader(tracked, delimiter=self.delimiter, strict=True)
            _ = reader.fieldnames
            tracked.take()
            start = self.cursor
            for _ in range(start):
                if _next_row(reader, tracked) is None:
                    break
            while True:
                chunk: list[tuple[dict[str, Any], int]] = []
                for _ in range(500):
                    row = _next_row(reader, tracked)
                    if row is None:
                        break
                    chunk.append((row, reader.line_num))
                for row, line_number in chunk:
                    self.cursor += 1
                    if None in row or any(value is None for value in row.values()):
                        yield {
                            "__datapipe_error__": {
                                "line": line_number,
                                "input": row,
                                "reason": "CSV row has a different number of fields than the header",
                            }
                        }
                    else:
                        yield row
                if len(chunk) < 500:
                    break
        finally:
            stream.close()
