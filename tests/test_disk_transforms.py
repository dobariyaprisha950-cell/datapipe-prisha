import json
import os
import tempfile
import threading
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from datapipe.config.schema import PipelineConfig, TransformConfig
from datapipe.core.checkpoint import Checkpoint
from datapipe.core.commit import OutputCommitter
from datapipe.core.engine import _config_signature, run_pipeline
from datapipe.core.exceptions import TransformStateError
from datapipe.sinks.jsonl import JSONLSink
from datapipe.transforms.aggregation import aggregate_daily
from datapipe.transforms.deduplication import deduplicate
from datapipe.transforms.state import DiskTransformProcessor


def _state(root: Path, transforms: list[dict[str, Any]]) -> DiskTransformProcessor:
    return DiskTransformProcessor(
        root, [TransformConfig.model_validate(transform) for transform in transforms]
    )


def test_disk_dedup_preserves_first_seen_and_python_key_equality(tmp_path: Path) -> None:
    rows: list[dict[str, Any]] = [
        {"id": 1, "value": "integer"},
        {"id": True, "value": "boolean"},
        {"id": 1.0, "value": "float"},
        {"id": None, "value": "null-first"},
        {"id": None, "value": "null-second"},
        {"value": "missing-first"},
        {"value": "missing-second"},
    ]
    state = _state(tmp_path, [{"type": "deduplication", "settings": {"key": "id"}}])
    try:
        actual = [row for source in rows if (row := state.process(dict(source))) is not None]
        assert actual == deduplicate(rows, "id")
        assert state.duplicates == 3
        assert len(state.pending_updates()) == 2
    finally:
        state.close()


@given(
    st.lists(
        st.tuples(
            st.sampled_from(["2026-01-01", "2026-01-02", "bad", None]),
            st.one_of(st.integers(-100, 100), st.floats(-100, 100, allow_nan=False), st.none()),
        ),
        max_size=100,
    )
)
def test_disk_aggregation_matches_reference(rows: list[tuple[str | None, int | float | None]]) -> None:
    records = [{"date": day, "value": value} for day, value in rows]
    with tempfile.TemporaryDirectory() as directory:
        state = _state(
            Path(directory),
            [
                {
                    "type": "aggregation",
                    "settings": {
                        "date_field": "date",
                        "metrics": {
                            "value": "sum",
                            "n": "count",
                            "lo": "min",
                            "hi": "max",
                        },
                    },
                }
            ],
        )
        try:
            for record in records:
                assert state.process(record) is None
            actual = list(state.aggregate_rows(0, "date", None))
            expected = aggregate_daily(
                records,
                "date",
                {"value": "sum", "n": "count", "lo": "min", "hi": "max"},
            )
            assert actual == expected
        finally:
            state.close()


def _pipeline(source: Path, output: Path, checkpoint: Path, transforms: list[dict[str, Any]], batch: int = 2) -> PipelineConfig:
    return PipelineConfig.model_validate(
        {
            "name": "disk-state-integration",
            "sources": [{"name": "input", "type": "jsonl", "path": str(source)}],
            "transforms": transforms,
            "sink": {"type": "jsonl", "output_dir": str(output), "batch_size": batch},
            "checkpoint": str(checkpoint),
            "queue_size": 8,
        }
    )


def _all_output(output: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for path in output.glob("*=*/part-*.jsonl")
        for line in path.read_text(encoding="utf-8").splitlines()
    ]


def _ordered_output(output: Path) -> list[dict[str, Any]]:
    manifest = json.loads((output / ".datapipe" / "manifest.json").read_text(encoding="utf-8"))
    rows: list[dict[str, Any]] = []
    batches = sorted(manifest["batches"].values(), key=lambda batch: batch["sequence"])
    for batch in batches:
        for file in batch["files"]:
            target = file["target"]
            if "transform-state" in Path(target).parts or "dead-letter-batches" in Path(target).parts:
                continue
            path = output / target
            if path.suffix == ".jsonl":
                rows.extend(json.loads(line) for line in path.read_text(encoding="utf-8").splitlines())
    return rows


@pytest.mark.asyncio
async def test_dedupe_reconstructs_state_and_resumes_appended_jsonl(tmp_path: Path) -> None:
    source = tmp_path / "rows.jsonl"
    source.write_text(
        '{"id":"a","date":"2026-01-01"}\n'
        '{"id":"b","date":"2026-01-01"}\n'
        '{"id":"a","date":"2026-01-01"}\n',
        encoding="utf-8",
    )
    output, checkpoint = tmp_path / "out", tmp_path / "checkpoint.json"
    config = _pipeline(
        source,
        output,
        checkpoint,
        [{"type": "deduplication", "settings": {"key": "id"}}],
        batch=1,
    )
    first = await run_pipeline(config)
    assert (first["records_written"], first["duplicates_removed"]) == (2, 1)
    with source.open("a", encoding="utf-8") as stream:
        stream.write('{"id":"b","date":"2026-01-01"}\n')
        stream.write('{"id":"c","date":"2026-01-01"}\n')
    second = await run_pipeline(config)
    assert (second["records_written"], second["duplicates_removed"]) == (1, 1)
    assert sorted(row["id"] for row in _all_output(output)) == ["a", "b", "c"]
    assert not list((output / ".datapipe").glob(".transform-state-*.sqlite*"))


@pytest.mark.asyncio
async def test_aggregation_is_incremental_and_only_rewrites_dirty_days(tmp_path: Path) -> None:
    source = tmp_path / "rows.jsonl"
    source.write_text(
        '{"id":"a","date":"2026-01-01","value":2}\n'
        '{"id":"b","date":"2026-01-01","value":3}\n'
        '{"id":"c","date":"2026-01-02","value":4}\n',
        encoding="utf-8",
    )
    output, checkpoint = tmp_path / "out", tmp_path / "checkpoint.json"
    transforms = [
        {"type": "aggregation", "settings": {"date_field": "date", "metrics": {"value": "sum", "id": "count"}}}
    ]
    config = _pipeline(source, output, checkpoint, transforms, batch=2)
    first = await run_pipeline(config)
    assert first["records_written"] == 2
    with source.open("a", encoding="utf-8") as stream:
        stream.write('{"id":"d","date":"2026-01-01","value":5}\n')
    second = await run_pipeline(config)
    assert second["records_written"] == 1
    all_rows = _ordered_output(output)
    newest_day = [row for row in all_rows if row["date"] == "2026-01-01"][-1]
    assert newest_day == {"date": "2026-01-01", "value_sum": 10.0, "id_count": 3}
    assert len([row for row in all_rows if row["date"] == "2026-01-01"]) == 2
    third = await run_pipeline(config)
    assert third["records_written"] == 0


@pytest.mark.asyncio
async def test_dedupe_before_aggregation_uses_only_first_seen_records(tmp_path: Path) -> None:
    source = tmp_path / "combined.jsonl"
    source.write_text(
        '{"id":"a","date":"2026-01-01","value":2}\n'
        '{"id":"a","date":"2026-01-01","value":200}\n'
        '{"id":"b","date":"2026-01-01","value":3}\n',
        encoding="utf-8",
    )
    config = _pipeline(
        source,
        tmp_path / "out",
        tmp_path / "checkpoint.json",
        [
            {"type": "deduplication", "settings": {"key": "id"}},
            {
                "type": "aggregation",
                "settings": {"date_field": "date", "metrics": {"value": "sum", "id": "count"}},
            },
        ],
        batch=1,
    )
    report = await run_pipeline(config)
    assert report["duplicates_removed"] == 1
    assert _all_output(tmp_path / "out") == [
        {"date": "2026-01-01", "value_sum": 5.0, "id_count": 2}
    ]


@pytest.mark.asyncio
async def test_committed_dedupe_state_recovers_when_checkpoint_save_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "rows.jsonl"
    source.write_text('{"id":"a","date":"2026-01-01"}\n', encoding="utf-8")
    output, checkpoint = tmp_path / "out", tmp_path / "checkpoint.json"
    config = _pipeline(
        source,
        output,
        checkpoint,
        [{"type": "deduplication", "settings": {"key": "id"}}],
        batch=1,
    )
    original = Checkpoint.save

    def fail_first_save(self: Checkpoint, value: dict[str, Any]) -> None:
        monkeypatch.setattr(Checkpoint, "save", original)
        raise OSError("injected checkpoint failure")

    monkeypatch.setattr(Checkpoint, "save", fail_first_save)
    with pytest.raises(ExceptionGroup):
        await run_pipeline(config)
    state_files = list((output / ".datapipe" / "transform-state").glob("*.jsonl"))
    assert len(state_files) == 1
    with source.open("a", encoding="utf-8") as stream:
        stream.write('{"id":"a","date":"2026-01-01"}\n')
        stream.write('{"id":"b","date":"2026-01-01"}\n')
    recovered = await run_pipeline(config)
    assert recovered["duplicates_removed"] == 1
    assert recovered["records_written"] == 1
    assert sorted(row["id"] for row in _all_output(output)) == ["a", "b"]


@pytest.mark.asyncio
async def test_uncommitted_dedupe_delta_replays_after_sink_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "rows.jsonl"
    source.write_text(
        '{"id":"a","date":"2026-01-01"}\n'
        '{"id":"b","date":"2026-01-01"}\n',
        encoding="utf-8",
    )
    output, checkpoint = tmp_path / "out", tmp_path / "checkpoint.json"
    config = _pipeline(
        source,
        output,
        checkpoint,
        [{"type": "deduplication", "settings": {"key": "id"}}],
        batch=1,
    )
    original = JSONLSink.write
    calls = 0

    def fail_second_batch(self: JSONLSink, rows: Any, batch_id: str | None = None) -> list[Path]:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected sink failure")
        return original(self, rows, batch_id)

    monkeypatch.setattr(JSONLSink, "write", fail_second_batch)
    with pytest.raises(ExceptionGroup):
        await run_pipeline(config)
    cursors = json.loads(checkpoint.read_text(encoding="utf-8"))["cursors"]
    assert cursors["input"] == 1
    monkeypatch.setattr(JSONLSink, "write", original)
    resumed = await run_pipeline(config)
    assert resumed["records_written"] == 1
    assert sorted(row["id"] for row in _all_output(output)) == ["a", "b"]


@pytest.mark.asyncio
async def test_streaming_high_cardinality_dedup_does_not_keep_source_rows(
    tmp_path: Path,
) -> None:
    import psutil

    count = int(os.environ.get("DATAPIPE_DEDUPE_RECORDS", "100000"))
    source = tmp_path / "many.jsonl"
    with source.open("w", encoding="utf-8") as stream:
        for index in range(count):
            stream.write(json.dumps({"id": index, "date": "2026-01-01"}) + "\n")
    process = psutil.Process()
    baseline = process.memory_info().rss
    high_water = [baseline]
    stop = threading.Event()

    def sample_rss() -> None:
        while not stop.wait(0.01):
            high_water[0] = max(high_water[0], process.memory_info().rss)

    sampler = threading.Thread(target=sample_rss, daemon=True)
    sampler.start()
    config = _pipeline(
        source,
        tmp_path / "out",
        tmp_path / "checkpoint.json",
        [{"type": "deduplication", "settings": {"key": "id"}}],
        batch=20_000,
    )
    try:
        result = await run_pipeline(config)
    finally:
        stop.set()
        sampler.join()
    high_water[0] = max(high_water[0], process.memory_info().rss)
    growth = high_water[0] - baseline
    state_disk = sum(
        path.stat().st_size
        for path in (tmp_path / "out" / ".datapipe").rglob("*")
        if path.is_file()
    )
    output_disk = sum(
        path.stat().st_size
        for path in (tmp_path / "out").rglob("*")
        if path.is_file()
    )
    print(
        f"{count} high-cardinality dedupe peak RSS growth={growth} bytes; "
        f"state disk={state_disk} bytes; total output disk={output_disk} bytes"
    )
    assert result["records_written"] == count
    assert result["duplicates_removed"] == 0
    assert growth < 96 * 1024 * 1024


@pytest.mark.asyncio
async def test_legacy_stateful_journal_is_rejected_instead_of_silently_losing_state(
    tmp_path: Path,
) -> None:
    source = tmp_path / "rows.jsonl"
    source.write_text('{"id":"a","date":"2026-01-01"}\n', encoding="utf-8")
    output = tmp_path / "out"
    config = _pipeline(
        source,
        output,
        tmp_path / "checkpoint.json",
        [{"type": "deduplication", "settings": {"key": "id"}}],
    )
    old = OutputCommitter(output, _config_signature(config), None, {})
    old.manifest["batches"]["batch-000000000001"] = {
        "sequence": 1,
        "status": "committed",
        "cursors": {"input": 1},
        "files": [],
    }
    old.manifest["next_sequence"] = 2
    old._persist()
    with pytest.raises(ValueError, match="legacy output journal"):
        await run_pipeline(config)


@pytest.mark.asyncio
async def test_high_cardinality_aggregation_spills_group_state_to_sqlite(tmp_path: Path) -> None:
    from datetime import date, timedelta

    count = 30_000
    source = tmp_path / "many-days.jsonl"
    first_day = date(2020, 1, 1)
    with source.open("w", encoding="utf-8") as stream:
        for index in range(count):
            day = (first_day + timedelta(days=index)).isoformat()
            stream.write(json.dumps({"id": index, "date": day, "value": index}) + "\n")
    config = _pipeline(
        source,
        tmp_path / "out",
        tmp_path / "checkpoint.json",
        [
            {
                "type": "aggregation",
                "settings": {"date_field": "date", "metrics": {"value": "sum"}},
            }
        ],
        batch=10_000,
    )
    config.sink.partition_by = "kind"
    result = await run_pipeline(config)
    rows = _ordered_output(tmp_path / "out")
    assert result["records_written"] == count
    assert len(rows) == count
    assert rows[0] == {"date": "2020-01-01", "value_sum": 0.0}
    assert rows[-1] == {
        "date": (first_day + timedelta(days=count - 1)).isoformat(),
        "value_sum": float(count - 1),
    }


@pytest.mark.asyncio
async def test_streaming_large_aggregation_keeps_only_group_state_in_memory(tmp_path: Path) -> None:
    import psutil

    count = int(os.environ.get("DATAPIPE_AGGREGATION_RECORDS", "100000"))
    source = tmp_path / "many-rows.jsonl"
    with source.open("w", encoding="utf-8") as stream:
        for index in range(count):
            stream.write(json.dumps({"id": index, "date": "2026-01-01", "value": index}) + "\n")
    process = psutil.Process()
    baseline = process.memory_info().rss
    high_water = [baseline]
    stop = threading.Event()

    def sample_rss() -> None:
        while not stop.wait(0.01):
            high_water[0] = max(high_water[0], process.memory_info().rss)

    sampler = threading.Thread(target=sample_rss, daemon=True)
    sampler.start()
    config = _pipeline(
        source,
        tmp_path / "out",
        tmp_path / "checkpoint.json",
        [
            {
                "type": "aggregation",
                "settings": {
                    "date_field": "date",
                    "metrics": {"value": "sum", "id": "count"},
                },
            }
        ],
        batch=20_000,
    )
    try:
        result = await run_pipeline(config)
    finally:
        stop.set()
        sampler.join()
    high_water[0] = max(high_water[0], process.memory_info().rss)
    growth = high_water[0] - baseline
    rows = _all_output(tmp_path / "out")
    print(f"{count} row aggregation peak RSS growth={growth} bytes")
    assert result["records_written"] == 1
    assert rows == [
        {"date": "2026-01-01", "value_sum": float(count * (count - 1) // 2), "id_count": count}
    ]
    assert growth < 96 * 1024 * 1024


def test_temporary_state_database_cleanup_removes_stale_artifacts(tmp_path: Path) -> None:
    metadata = tmp_path / ".datapipe"
    metadata.mkdir()
    stale = metadata / ".transform-state-stale.sqlite-journal"
    stale.write_text("stale", encoding="utf-8")
    state = _state(tmp_path, [{"type": "deduplication", "settings": {"key": "id"}}])
    try:
        assert not stale.exists()
    finally:
        state.close()


def test_sqlite_failure_is_reported_as_transform_state_error(tmp_path: Path) -> None:
    state = _state(tmp_path, [{"type": "deduplication", "settings": {"key": "id"}}])
    try:
        state.connection.close()
        with pytest.raises(TransformStateError, match="deduplication failed"):
            state.accept(0, {"id": "x"}, "id")
    finally:
        state.close()
