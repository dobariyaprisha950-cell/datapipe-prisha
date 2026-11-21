import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from datapipe.sinks.jsonl import JSONLSink
from datapipe.sinks.parquet import ParquetSink


def test_jsonl_sink_same_batch_is_idempotent(tmp_path: Path) -> None:
    sink = JSONLSink(str(tmp_path))
    batch = [{"id": 1, "date": "2026-01-01"}]
    first = sink.write(batch, batch_id="source:0:1")
    second = sink.write(batch, batch_id="source:0:1")
    assert first == second
    assert len(list(tmp_path.glob("date=*/part-*.jsonl"))) == 1


def test_jsonl_sink_rejects_path_traversal(tmp_path: Path) -> None:
    sink = JSONLSink(str(tmp_path))
    with pytest.raises(ValueError, match="unsafe partition"):
        sink.write([{"date": "../../escape"}])


def test_failed_replace_leaves_no_final_or_temporary_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sink = JSONLSink(str(tmp_path))

    def fail_replace(_source: str, _target: Path) -> None:
        raise OSError("simulated commit failure")

    monkeypatch.setattr("datapipe.sinks.jsonl.os.replace", fail_replace)
    with pytest.raises(OSError, match="simulated commit failure"):
        sink.write([{"id": 1, "date": "2026-01-01"}])
    partition = tmp_path / "date=2026-01-01"
    assert list(partition.glob("part-*.jsonl")) == []
    assert list(partition.glob(".datapipe-*")) == []


def test_parquet_sink_partitions_and_round_trips_records(tmp_path: Path) -> None:
    sink = ParquetSink(str(tmp_path))
    records = [
        {"id": "one", "date": "2026-01-01", "value": 1, "data": {}},
        {"id": "two", "date": "2026-01-02", "value": 2, "data": {"nested": [1, 2]}},
    ]
    files = sink.write(records, batch_id="batch-1")
    assert {path.parent.name for path in files} == {"date=2026-01-01", "date=2026-01-02"}
    round_trip = [pq.ParquetFile(path).read().to_pylist()[0] for path in files]
    assert [
        (row["id"], row["date"], row["value"], json.loads(row["data"]))
        for row in sorted(round_trip, key=lambda row: row["id"])
    ] == [(row["id"], row["date"], row["value"], row["data"]) for row in records]


def test_parquet_sink_failed_replace_leaves_no_partial_final_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sink = ParquetSink(str(tmp_path))

    def fail_replace(_source: str, _target: Path) -> None:
        raise OSError("simulated parquet promotion failure")

    monkeypatch.setattr("datapipe.sinks.parquet.os.replace", fail_replace)
    with pytest.raises(OSError, match="simulated parquet promotion failure"):
        sink.write([{"id": "one", "date": "2026-01-01"}], batch_id="batch-1")
    partition = tmp_path / "date=2026-01-01"
    assert list(partition.glob("part-*.parquet")) == []
    assert list(partition.glob(".datapipe-*")) == []
