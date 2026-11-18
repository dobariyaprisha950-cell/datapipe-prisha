from pathlib import Path

import pytest

from datapipe.sinks.jsonl import JSONLSink


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
