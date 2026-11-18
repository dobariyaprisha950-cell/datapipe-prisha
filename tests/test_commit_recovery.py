import json
from pathlib import Path
from typing import Any

import pytest
from aiohttp import web

from datapipe.config.schema import PipelineConfig
from datapipe.core.checkpoint import Checkpoint
from datapipe.core.engine import run_pipeline
from datapipe.sinks.jsonl import JSONLSink


def _config(tmp_path: Path, *, rows: int = 3, batch_size: int = 1) -> PipelineConfig:
    tmp_path.mkdir(parents=True, exist_ok=True)
    source = tmp_path / "input.jsonl"
    source.write_text(
        "".join(
            json.dumps({"id": f"r{index}", "date": f"2026-01-0{index + 1}"}) + "\n"
            for index in range(rows)
        ),
        encoding="utf-8",
    )
    return PipelineConfig.model_validate(
        {
            "name": "commit-recovery",
            "sources": [{"name": "local", "type": "jsonl", "path": str(source)}],
            "sink": {
                "type": "jsonl",
                "output_dir": str(tmp_path / "output"),
                "batch_size": batch_size,
            },
            "checkpoint": str(tmp_path / "checkpoint.json"),
        }
    )


def _records(output: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for file in output.glob("date=*/part-*.jsonl")
        for line in file.read_text(encoding="utf-8").splitlines()
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_after_staging", [False, True])
async def test_write_failure_leaves_cursor_behind_and_replays(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fail_after_staging: bool
) -> None:
    config = _config(tmp_path)
    original = JSONLSink.write
    first = True

    def failing_write(
        self: JSONLSink, records: Any, batch_id: str | None = None
    ) -> list[Path]:
        nonlocal first
        if first:
            first = False
            if fail_after_staging:
                original(self, records, batch_id)
            raise OSError("injected sink failure")
        return original(self, records, batch_id)

    monkeypatch.setattr(JSONLSink, "write", failing_write)
    with pytest.raises((ExceptionGroup, OSError)):
        await run_pipeline(config)
    checkpoint_path = tmp_path / "checkpoint.json"
    if checkpoint_path.exists():
        checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        assert checkpoint["cursors"].get("local", 0) == 0
    assert _records(tmp_path / "output") == []
    assert list((tmp_path / "output" / ".datapipe" / "staging").iterdir()) == []

    monkeypatch.setattr(JSONLSink, "write", original)
    report = await run_pipeline(config)
    assert report["records_written"] == 3
    assert sorted(record["id"] for record in _records(tmp_path / "output")) == ["r0", "r1", "r2"]
    manifest = json.loads((tmp_path / "output" / ".datapipe" / "manifest.json").read_text())
    assert [run["status"] for run in manifest["runs"]] == ["interrupted", "completed"]


@pytest.mark.asyncio
async def test_crash_after_output_commit_before_checkpoint_recovers_without_duplicate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path, rows=1)

    def fail_checkpoint(_self: Checkpoint, _state: dict[str, Any]) -> None:
        raise OSError("injected checkpoint failure")

    monkeypatch.setattr(Checkpoint, "save", fail_checkpoint)
    with pytest.raises((ExceptionGroup, OSError)):
        await run_pipeline(config)
    assert len(_records(tmp_path / "output")) == 1
    assert list((tmp_path / "output" / ".datapipe" / "staging").iterdir())

    monkeypatch.undo()
    resumed = await run_pipeline(config)
    assert resumed["total_fetched"] == 0
    assert len(_records(tmp_path / "output")) == 1
    raw = json.loads((tmp_path / "checkpoint.json").read_text(encoding="utf-8"))
    assert raw["cursors"]["local"] == 1


@pytest.mark.asyncio
async def test_partial_multi_partition_promotion_is_completed_on_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path, rows=2, batch_size=10)
    original_replace = __import__("os").replace
    promotions = 0

    def fail_second_promotion(source: Any, target: Any) -> None:
        nonlocal promotions
        source_path = Path(source)
        target_path = Path(target)
        if (
            "staging" in source_path.parts
            and "staging" not in target_path.parts
            and target_path.parent.name.startswith("date=")
            and target_path.name.startswith("part-")
        ):
            promotions += 1
            if promotions == 2:
                raise OSError("injected interruption during multi-file promotion")
        original_replace(source, target)

    monkeypatch.setattr("datapipe.core.commit.os.replace", fail_second_promotion)
    with pytest.raises((ExceptionGroup, OSError)):
        await run_pipeline(config)
    assert len(_records(tmp_path / "output")) == 1
    assert list((tmp_path / "output" / ".datapipe" / "staging").iterdir())

    monkeypatch.undo()
    resumed = await run_pipeline(config)
    assert resumed["total_fetched"] == 0
    assert sorted(record["id"] for record in _records(tmp_path / "output")) == ["r0", "r1"]


@pytest.mark.asyncio
async def test_empty_and_final_partial_batch_commit_source_cursor(tmp_path: Path) -> None:
    empty = _config(tmp_path / "empty", rows=0, batch_size=5)
    empty_report = await run_pipeline(empty)
    assert empty_report["records_written"] == 0
    empty_state = json.loads((tmp_path / "empty" / "checkpoint.json").read_text())
    assert empty_state["cursors"]["local"] == 0

    partial = _config(tmp_path / "partial", rows=2, batch_size=5)
    report = await run_pipeline(partial)
    assert report["records_written"] == 2
    assert len(_records(tmp_path / "partial" / "output")) == 2


@pytest.mark.asyncio
async def test_multiple_sources_progress_is_recorded_per_source(tmp_path: Path) -> None:
    first = tmp_path / "first.jsonl"
    second = tmp_path / "second.jsonl"
    first.write_text('{"id":"a","date":"2026-01-01"}\n', encoding="utf-8")
    second.write_text('{"id":"b","date":"2026-01-02"}\n', encoding="utf-8")
    config = PipelineConfig.model_validate(
        {
            "name": "multi-source-commit",
            "sources": [
                {"name": "first", "type": "jsonl", "path": str(first)},
                {"name": "second", "type": "jsonl", "path": str(second)},
            ],
            "sink": {
                "type": "jsonl",
                "output_dir": str(tmp_path / "output"),
                "batch_size": 1,
            },
            "checkpoint": str(tmp_path / "checkpoint.json"),
        }
    )
    report = await run_pipeline(config)
    assert report["records_written"] == 2
    cursors = json.loads((tmp_path / "checkpoint.json").read_text())["cursors"]
    assert cursors == {"first": 1, "second": 1}
    assert len(_records(tmp_path / "output")) == 2


@pytest.mark.asyncio
async def test_http_page_item_offset_resumes_inside_partially_committed_page(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    requests: list[int] = []

    async def handler(request: web.Request) -> web.Response:
        page = int(request.query["page"])
        requests.append(page)
        rows = (
            [
                {"id": "http-a", "date": "2026-01-01"},
                {"id": "http-b", "date": "2026-01-01"},
            ]
            if page == 1
            else []
        )
        return web.json_response(rows)

    app = web.Application()
    app.router.add_get("/items", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    assert site._server is not None
    port = site._server.sockets[0].getsockname()[1]
    config = PipelineConfig.model_validate(
        {
            "name": "http-item-checkpoint",
            "sources": [
                {
                    "name": "api",
                    "type": "http",
                    "url": f"http://127.0.0.1:{port}/items",
                    "page_param": "page",
                }
            ],
            "sink": {"type": "jsonl", "output_dir": str(tmp_path / "output"), "batch_size": 1},
            "checkpoint": str(tmp_path / "checkpoint.json"),
        }
    )
    original = JSONLSink.write
    writes = 0

    def fail_second_batch(
        self: JSONLSink, records: Any, batch_id: str | None = None
    ) -> list[Path]:
        nonlocal writes
        writes += 1
        if writes == 2:
            raise OSError("injected failure after first HTTP item commit")
        return original(self, records, batch_id)

    monkeypatch.setattr(JSONLSink, "write", fail_second_batch)
    try:
        with pytest.raises(ExceptionGroup):
            await run_pipeline(config)
        state = json.loads((tmp_path / "checkpoint.json").read_text(encoding="utf-8"))
        assert state["cursors"]["api"] == {"page": 1, "offset": 1}

        monkeypatch.setattr(JSONLSink, "write", original)
        resumed = await run_pipeline(config)
    finally:
        await runner.cleanup()
    assert resumed["records_written"] == 1
    assert requests == [1, 2, 1, 2]
    assert sorted(record["id"] for record in _records(tmp_path / "output")) == [
        "http-a",
        "http-b",
    ]
