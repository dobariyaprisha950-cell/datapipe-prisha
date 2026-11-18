"""Streaming CSV source."""

import csv
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any


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
            reader = csv.DictReader(stream, delimiter=self.delimiter)
            start = self.cursor
            for _ in range(start):
                if next(reader, None) is None:
                    break
            while True:
                chunk: list[tuple[dict[str, Any], int]] = []
                for _ in range(500):
                    row = next(reader, None)
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
