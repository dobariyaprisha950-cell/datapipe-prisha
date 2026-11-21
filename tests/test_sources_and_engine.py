import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
from aiohttp import web

from datapipe.config.schema import PipelineConfig, SourceConfig
from datapipe.core.engine import _blocking_to_completion, _source, _transform_record, run_pipeline
from datapipe.plugins.discovery import discover
from datapipe.plugins.registry import SourceRegistry
from datapipe.sinks.jsonl import JSONLSink
from datapipe.sources.jsonl import JSONLSource


async def with_http_server(
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
    check: Callable[[str], Awaitable[None]],
) -> None:
    app = web.Application()
    app.router.add_get("/data", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    assert site._server is not None
    port = site._server.sockets[0].getsockname()[1]
    try:
        await check(f"http://127.0.0.1:{port}/data")
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_blocking_commit_finishes_before_cancellation_propagates() -> None:
    import threading

    started = threading.Event()
    release = threading.Event()

    def commit() -> str:
        started.set()
        assert release.wait(3)
        return "committed"

    task = asyncio.create_task(_blocking_to_completion(commit))
    assert await asyncio.to_thread(started.wait, 3)
    task.cancel()
    await asyncio.sleep(0.02)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task


def test_plugin_source_receives_optional_common_path_and_url() -> None:
    class GenericPlugin:
        def __init__(self, **settings: Any) -> None:
            self.settings = settings

    SourceRegistry.register("test-generic-settings", GenericPlugin)
    config = SourceConfig.model_validate(
        {
            "name": "plugin",
            "type": "test-generic-settings",
            "path": "plugin-data.csv",
            "url": "https://example.invalid/data",
        }
    )
    source = _source(config, {})
    assert source.settings["path"] == "plugin-data.csv"
    assert source.settings["url"] == "https://example.invalid/data"


def test_fingerprint_transform_preserves_non_numeric_values() -> None:
    config = PipelineConfig.model_validate(
        {
            "name": "fingerprint-nonnumeric",
            "sources": [{"name": "input", "type": "jsonl", "path": "unused.jsonl"}],
            "transforms": [
                {"type": "cpu_fingerprint", "settings": {"field": "value", "rounds": 5}}
            ],
            "sink": {"type": "jsonl", "output_dir": "unused-output"},
        }
    )
    row = {"id": "record", "value": "opaque"}
    assert _transform_record(row, config) == row


@pytest.mark.asyncio
async def test_jsonl_malformed_is_emitted_as_rejection(tmp_path: Path) -> None:
    path = tmp_path / "in.jsonl"
    path.write_text('{"id":"ok","date":"2026-01-01"}\n{bad json}\n', encoding="utf-8")
    source = JSONLSource("local", str(path))
    rows = [row async for row in source.read()]
    assert rows[0]["id"] == "ok"
    assert rows[1]["__datapipe_error__"]["input"] == "{bad json}"
    assert source.cursor == 2


@pytest.mark.asyncio
async def test_csv_parse_and_shape_errors_are_dead_lettered_without_losing_good_rows(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "mixed.csv"
    source_path.write_text(
        "id,date,amount\n"
        "ok,2026-01-01,5\n"
        "short,2026-01-01\n"
        'broken,2026-01-02,"unterminated\n',
        encoding="utf-8",
    )
    output = tmp_path / "out"
    config = PipelineConfig.model_validate(
        {
            "name": "csv-dead-letter",
            "sources": [{"name": "csv", "type": "csv", "path": str(source_path)}],
            "sink": {"type": "jsonl", "output_dir": str(output)},
        }
    )
    report = await run_pipeline(config)
    dead = [
        json.loads(line)
        for line in (output / "dead-letter.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert report["total_fetched"] == 3
    assert report["invalid_records"] == 2
    assert report["records_written"] == 1
    assert [item["source"] for item in dead] == ["csv", "csv"]
    assert "broken,2026-01-02" in dead[1]["record"]


@pytest.mark.asyncio
async def test_user_record_with_reserved_marker_key_does_not_crash_pipeline(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "reserved-key.jsonl"
    source_path.write_text(
        json.dumps(
            {"id": "kept", "date": "2026-01-01", "__datapipe_error__": "user-value"}
        )
        + "\n",
        encoding="utf-8",
    )
    output = tmp_path / "out"
    config = PipelineConfig.model_validate(
        {
            "name": "reserved-key",
            "sources": [{"name": "jsonl", "type": "jsonl", "path": str(source_path)}],
            "sink": {"type": "jsonl", "output_dir": str(output)},
        }
    )
    report = await run_pipeline(config)
    assert report["records_written"] == 1
    written = json.loads(next(output.glob("date=*/part-*.jsonl")).read_text(encoding="utf-8"))
    assert written["__datapipe_error__"] == "user-value"


@pytest.mark.asyncio
async def test_http_malformed_json_and_invalid_items_reach_central_dead_letter(
    tmp_path: Path,
) -> None:
    async def handler(request: web.Request) -> web.Response:
        page = int(request.query["page"])
        if page == 1:
            return web.json_response([{"id": "ok", "date": "2026-01-01"}, "bad item"])
        if page == 2:
            return web.Response(text="not json", content_type="application/json")
        return web.json_response([])

    async def check(url: str) -> None:
        output = tmp_path / "out"
        config = PipelineConfig.model_validate(
            {
                "name": "http-dead-letter",
                "sources": [
                    {"name": "api", "type": "http", "url": url, "page_param": "page"}
                ],
                "sink": {"type": "jsonl", "output_dir": str(output)},
            }
        )
        report = await run_pipeline(config)
        dead = [
            json.loads(line)
            for line in (output / "dead-letter.jsonl").read_text(encoding="utf-8").splitlines()
        ]
        assert report["records_written"] == 1
        assert report["invalid_records"] == 2
        assert {item["record"] for item in dead} == {"bad item", "not json"}
        assert all(item["source"] == "api" for item in dead)

    await with_http_server(handler, check)


@pytest.mark.asyncio
async def test_pipeline_commits_partitioned_parquet_output(tmp_path: Path) -> None:
    from pyarrow import parquet

    source = tmp_path / "parquet.jsonl"
    source.write_text(
        '{"id":"one","date":"2026-01-01","value":1}\n'
        '{"id":"two","date":"2026-01-02","value":2}\n',
        encoding="utf-8",
    )
    output = tmp_path / "out"
    config = PipelineConfig.model_validate(
        {
            "name": "parquet-integration",
            "sources": [{"name": "input", "type": "jsonl", "path": str(source)}],
            "sink": {
                "type": "parquet",
                "output_dir": str(output),
                "batch_size": 1,
            },
        }
    )
    report = await run_pipeline(config)
    files = sorted(output.glob("date=*/part-*.parquet"))
    assert report["records_written"] == 2
    assert [parquet.ParquetFile(path).read().to_pylist()[0]["id"] for path in files] == [
        "one",
        "two",
    ]


@pytest.mark.asyncio
async def test_http_sources_share_client_and_obey_per_host_connection_limit(
    tmp_path: Path,
) -> None:
    active = 0
    high_water = 0

    async def handler(request: web.Request) -> web.StreamResponse:
        nonlocal active, high_water
        active += 1
        high_water = max(high_water, active)
        try:
            await asyncio.sleep(0.02)
            page = int(request.query["page"])
            name = request.query.get("source", "record")
            return web.json_response(
                [{"id": name, "date": "2026-01-01"}] if page == 1 else []
            )
        finally:
            active -= 1

    async def check(url: str) -> None:
        output = tmp_path / "out"
        sources = [
            {
                "name": f"api-{index}",
                "type": "http",
                "url": url,
                "page_param": "page",
            }
            for index in range(3)
        ]
        config = PipelineConfig.model_validate(
            {
                "name": "shared-http-client",
                "sources": sources,
                "concurrency": 2,
                "sink": {"type": "jsonl", "output_dir": str(output)},
            }
        )
        report = await run_pipeline(config)
        assert report["records_written"] == 3

    await with_http_server(handler, check)
    assert high_water <= 2


@pytest.mark.asyncio
async def test_bounded_queue_applies_backpressure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from datapipe.core import engine

    full_observations = 0
    base_queue = asyncio.Queue

    class ObservedQueue(base_queue):
        async def put(self, item: Any) -> None:
            nonlocal full_observations
            if self.full():
                full_observations += 1
            await super().put(item)

    monkeypatch.setattr(engine.asyncio, "Queue", ObservedQueue)
    source = tmp_path / "quick.jsonl"
    source.write_text(
        "".join(json.dumps({"id": index, "date": "2026-01-01"}) + "\n" for index in range(12)),
        encoding="utf-8",
    )
    config = PipelineConfig.model_validate(
        {
            "name": "backpressure",
            "sources": [{"name": "input", "type": "jsonl", "path": str(source)}],
            "queue_size": 1,
            "sink": {"type": "jsonl", "output_dir": str(tmp_path / "out"), "batch_size": 1},
        }
    )
    report = await run_pipeline(config)
    assert report["records_written"] == 12
    assert full_observations > 0


class SampleIntegrationSource:
    def __init__(self, name: str, records: list[dict[str, object]]) -> None:
        self.name, self.records = name, records
        self.cursor = len(records)

    async def read(self) -> AsyncIterator[dict[str, object]]:
        for record in self.records:
            yield record


class SlowCancellationSource:
    closed = False

    def __init__(self, name: str) -> None:
        self.name = name

    async def read(self) -> AsyncIterator[dict[str, object]]:
        try:
            while True:
                await asyncio.sleep(0.01)
                yield {"id": "slow", "date": "2026-01-01"}
        finally:
            type(self).closed = True


class ResumableIntegrationSource:
    def __init__(
        self, name: str, records: list[dict[str, object]], cursor: int = 0
    ) -> None:
        self.name, self.records, self.cursor = name, records, cursor

    async def read(self) -> AsyncIterator[dict[str, object]]:
        for index in range(self.cursor, len(self.records)):
            self.cursor = index + 1
            yield self.records[index]


@pytest.mark.asyncio
async def test_installed_source_registry_plugin_runs_and_invalid_record_dead_letters(
    tmp_path: Path,
) -> None:
    plugin_name = "test-sample-plugin"
    SourceRegistry.register(plugin_name, SampleIntegrationSource)
    config = PipelineConfig.model_validate(
        {
            "name": "plugin-check",
            "sources": [
                {
                    "name": "plug",
                    "type": plugin_name,
                    "settings": {
                        "records": [
                            {"id": "valid", "date": "2026-01-01"},
                            {"id": "invalid", "date": False},
                        ]
                    },
                }
            ],
            "sink": {"type": "jsonl", "output_dir": str(tmp_path / "out")},
            "checkpoint": str(tmp_path / "checkpoint.json"),
        }
    )
    result = await run_pipeline(config)
    assert result["records_written"] == 1
    assert result["invalid_records"] == 1
    dead = (tmp_path / "out" / "dead-letter.jsonl").read_text(encoding="utf-8")
    row = json.loads(dead)
    assert row["source"] == "plug" and row["record"]["id"] == "invalid"
    checkpoint = json.loads((tmp_path / "checkpoint.json").read_text(encoding="utf-8"))
    assert checkpoint["cursors"]["plug"] == 2


@pytest.mark.asyncio
async def test_file_checkpoint_resume_skips_committed_jsonl_rows(tmp_path: Path) -> None:
    source_file = tmp_path / "records.jsonl"
    source_file.write_text(
        '{"id":"one","date":"2026-01-01"}\n{"id":"two","date":"2026-01-02"}\n', encoding="utf-8"
    )
    checkpoint = tmp_path / "cursor.json"
    config = PipelineConfig.model_validate(
        {
            "name": "file-resume",
            "sources": [{"name": "file", "type": "jsonl", "path": str(source_file)}],
            "sink": {"type": "jsonl", "output_dir": str(tmp_path / "out")},
            "checkpoint": str(checkpoint),
        }
    )
    first = await run_pipeline(config)
    second = await run_pipeline(config)
    assert first["total_fetched"] == 2
    assert second["total_fetched"] == 0
    assert json.loads(checkpoint.read_text(encoding="utf-8"))["cursors"]["file"] == 2


@pytest.mark.asyncio
async def test_csv_checkpoint_resume_skips_committed_rows(tmp_path: Path) -> None:
    source_file = tmp_path / "records.csv"
    source_file.write_text(
        "id,date,value\none,2026-01-01,1\ntwo,2026-01-02,2\n", encoding="utf-8"
    )
    checkpoint = tmp_path / "csv-checkpoint.json"
    config = PipelineConfig.model_validate(
        {
            "name": "csv-resume",
            "sources": [{"name": "csv", "type": "csv", "path": str(source_file)}],
            "sink": {
                "type": "jsonl",
                "output_dir": str(tmp_path / "out"),
                "batch_size": 1,
            },
            "checkpoint": str(checkpoint),
        }
    )
    first = await run_pipeline(config)
    second = await run_pipeline(config)
    assert first["total_fetched"] == 2 and second["total_fetched"] == 0
    assert json.loads(checkpoint.read_text(encoding="utf-8"))["cursors"]["csv"] == 2
    assert len(list((tmp_path / "out").glob("date=*/part-*.jsonl"))) == 2


@pytest.mark.asyncio
async def test_http_page_checkpoint_resumes_after_successful_output(tmp_path: Path) -> None:
    requests: list[int] = []

    async def handler(request: web.Request) -> web.Response:
        page = int(request.query["page"])
        requests.append(page)
        rows = [{"id": "api-1", "date": "2026-01-01"}] if page == 1 else []
        return web.json_response(rows)

    app = web.Application()
    app.router.add_get("/items", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    assert site._server is not None
    port = site._server.sockets[0].getsockname()[1]
    checkpoint = tmp_path / "http-checkpoint.json"
    config = PipelineConfig.model_validate(
        {
            "name": "http-resume",
            "sources": [
                {
                    "name": "api",
                    "type": "http",
                    "url": f"http://127.0.0.1:{port}/items",
                    "page_param": "page",
                }
            ],
            "sink": {"type": "jsonl", "output_dir": str(tmp_path / "out")},
            "checkpoint": str(checkpoint),
        }
    )
    try:
        first = await run_pipeline(config)
        second = await run_pipeline(config)
    finally:
        await runner.cleanup()
    assert first["records_written"] == 1 and second["total_fetched"] == 0
    assert requests == [1, 2, 2]
    assert json.loads(checkpoint.read_text(encoding="utf-8"))["cursors"]["api"] == {
        "page": 2,
        "offset": 0,
    }


@pytest.mark.asyncio
async def test_failed_source_isolated_when_not_fail_fast(tmp_path: Path) -> None:
    config = PipelineConfig.model_validate(
        {
            "name": "isolation",
            "sources": [
                {"name": "missing", "type": "jsonl", "path": str(tmp_path / "missing")},
                {
                    "name": "good",
                    "type": "sample-isolation",
                    "settings": {"records": [{"id": "ok", "date": "2026-01-01"}]},
                },
            ],
            "sink": {"type": "jsonl", "output_dir": str(tmp_path / "out")},
        }
    )
    SourceRegistry.register("sample-isolation", SampleIntegrationSource)
    result = await run_pipeline(config)
    assert result["records_written"] == 1
    assert len(result["source_errors"]) == 1


@pytest.mark.asyncio
async def test_fail_fast_propagates_source_failure(tmp_path: Path) -> None:
    config = PipelineConfig.model_validate(
        {
            "name": "fail-fast",
            "sources": [{"name": "missing", "type": "jsonl", "path": str(tmp_path / "missing")}],
            "sink": {"type": "jsonl", "output_dir": str(tmp_path / "out")},
        }
    )
    with pytest.raises(ExceptionGroup):
        await run_pipeline(config, fail_fast=True)


@pytest.mark.asyncio
async def test_cancellation_closes_source_and_http_session(tmp_path: Path) -> None:
    SlowCancellationSource.closed = False
    SourceRegistry.register("slow-cancel", SlowCancellationSource)
    config = PipelineConfig.model_validate(
        {
            "name": "cancel",
            "sources": [{"name": "slow", "type": "slow-cancel", "settings": {}}],
            "sink": {"type": "jsonl", "output_dir": str(tmp_path / "out")},
        }
    )
    task = asyncio.create_task(run_pipeline(config))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert SlowCancellationSource.closed


@pytest.mark.asyncio
async def test_discovered_entrypoint_plugin_executes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class ExampleEntryPoint:
        name = "sample"

        @staticmethod
        def load() -> type[SampleIntegrationSource]:
            return SampleIntegrationSource

    monkeypatch.setattr(
        "datapipe.plugins.discovery.entry_points", lambda **kwargs: [ExampleEntryPoint()]
    )
    assert "sample" in discover()
    config = PipelineConfig.model_validate(
        {
            "name": "installed-plugin",
            "sources": [
                {
                    "name": "external",
                    "type": "sample",
                    "settings": {
                        "records": [{"id": "plugin-1", "date": "2026-01-01"}]
                    },
                }
            ],
            "sink": {"type": "jsonl", "output_dir": str(tmp_path / "out")},
        }
    )
    result = await run_pipeline(config)
    assert result["records_written"] == 1
    part = next((tmp_path / "out").glob("date=*/part-*.jsonl"))
    assert json.loads(part.read_text(encoding="utf-8"))["id"] == "plugin-1"


@pytest.mark.asyncio
async def test_resumable_plugin_receives_committed_cursor_on_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    SourceRegistry.register("resumable-test", ResumableIntegrationSource)
    config = PipelineConfig.model_validate(
        {
            "name": "resumable-plugin",
            "sources": [
                {
                    "name": "plugin",
                    "type": "resumable-test",
                    "settings": {
                        "records": [
                            {"id": "plugin-a", "date": "2026-01-01"},
                            {"id": "plugin-b", "date": "2026-01-02"},
                        ]
                    },
                }
            ],
            "sink": {"type": "jsonl", "output_dir": str(tmp_path / "out"), "batch_size": 1},
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
            raise OSError("injected plugin batch failure")
        return original(self, records, batch_id)

    monkeypatch.setattr(JSONLSink, "write", fail_second_batch)
    with pytest.raises(ExceptionGroup):
        await run_pipeline(config)
    checkpoint = json.loads((tmp_path / "checkpoint.json").read_text(encoding="utf-8"))
    assert checkpoint["cursors"]["plugin"] == 1

    monkeypatch.setattr(JSONLSink, "write", original)
    report = await run_pipeline(config)
    assert report["records_written"] == 1
    output = list((tmp_path / "out").glob("date=*/part-*.jsonl"))
    ids = [json.loads(part.read_text(encoding="utf-8"))["id"] for part in output]
    assert sorted(ids) == ["plugin-a", "plugin-b"]


@pytest.mark.asyncio
async def test_dry_run_does_not_create_output(tmp_path: Path) -> None:
    source_file = tmp_path / "in.jsonl"
    source_file.write_text('{"id":"one","date":"2026-01-01"}\n', encoding="utf-8")
    output = tmp_path / "out"
    config = PipelineConfig.model_validate(
        {
            "name": "dry",
            "sources": [{"name": "file", "type": "jsonl", "path": str(source_file)}],
            "sink": {"type": "jsonl", "output_dir": str(output)},
        }
    )
    result = await run_pipeline(config, dry_run=True)
    assert result["dry_run"] is True
    assert not output.exists()


@pytest.mark.asyncio
async def test_all_configured_transforms_run_in_order(tmp_path: Path) -> None:
    source_file = tmp_path / "in.jsonl"
    source_file.write_text(
        '{"id":"a","date":"2026-01-01","amount":2}\n'
        '{"id":"a","date":"2026-01-01","amount":8}\n'
        '{"id":"b","date":"2026-01-01","amount":3}\n',
        encoding="utf-8",
    )
    config = PipelineConfig.model_validate(
        {
            "name": "transforms",
            "sources": [{"name": "file", "type": "jsonl", "path": str(source_file)}],
            "transforms": [
                {"type": "mapping", "settings": {"fields": {"amount": "value"}}},
                {"type": "deduplication", "settings": {"key": "id"}},
                {"type": "filtering", "settings": {"field": "value", "op": "gt", "value": 2}},
                {
                    "type": "aggregation",
                    "settings": {"date_field": "date", "metrics": {"value": "sum"}},
                },
            ],
            "sink": {"type": "jsonl", "output_dir": str(tmp_path / "out")},
        }
    )
    result = await run_pipeline(config)
    assert result["duplicates_removed"] == 1
    assert result["records_written"] == 1
    part = next((tmp_path / "out").glob("date=*/part-*.jsonl"))
    assert json.loads(part.read_text(encoding="utf-8"))["value_sum"] == 3.0


@pytest.mark.asyncio
async def test_record_local_mapping_and_filtering_use_batched_stream_path(
    tmp_path: Path,
) -> None:
    source_file = tmp_path / "record-local.jsonl"
    source_file.write_text(
        '{"id":"keep","date":"2026-01-01","amount":5}\n'
        '{"id":"drop","date":"2026-01-01","amount":1}\n',
        encoding="utf-8",
    )
    output = tmp_path / "out"
    config = PipelineConfig.model_validate(
        {
            "name": "record-local",
            "sources": [{"name": "input", "type": "jsonl", "path": str(source_file)}],
            "transforms": [
                {"type": "mapping", "settings": {"fields": {"amount": "value"}}},
                {"type": "filtering", "settings": {"field": "value", "op": "gt", "value": 2}},
            ],
            "sink": {"type": "jsonl", "output_dir": str(output), "batch_size": 1},
        }
    )
    report = await run_pipeline(config)
    assert report["valid_records"] == 2
    assert report["records_written"] == 1
    part = next(output.glob("date=*/part-*.jsonl"))
    assert json.loads(part.read_text(encoding="utf-8")) == {
        "id": "keep",
        "date": "2026-01-01",
        "value": 5,
        "data": {},
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("execution", ["auto", "thread", "process"])
async def test_fingerprint_transform_uses_batched_stream_path(
    tmp_path: Path, execution: str
) -> None:
    from datapipe.transforms.parallel import _fingerprint

    source = tmp_path / "fingerprint.jsonl"
    source.write_text(
        '{"id":"one","date":"2026-01-01","value":42}\n'
        '{"id":"two","date":"2026-01-02","value":-7}\n',
        encoding="utf-8",
    )
    output = tmp_path / "out"
    config = PipelineConfig.model_validate(
        {
            "name": "fingerprint-stream",
            "sources": [{"name": "input", "type": "jsonl", "path": str(source)}],
            "transforms": [
                {
                    "type": "cpu_fingerprint",
                    "settings": {
                        "field": "value",
                        "output_field": "fp",
                        "rounds": 5000,
                        "execution": execution,
                        "workers": 2,
                    },
                }
            ],
            "sink": {"type": "jsonl", "output_dir": str(output), "batch_size": 1},
        }
    )
    result = await run_pipeline(config)
    assert result["records_written"] == 2
    rows = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted(output.glob("date=*/part-*.jsonl"))
    ]
    assert [row["fp"] for row in rows] == [_fingerprint(42, 5000), _fingerprint(-7, 5000)]


@pytest.mark.asyncio
async def test_two_million_jsonl_rows_stream_with_bounded_process_rss(tmp_path: Path) -> None:
    import threading

    import psutil

    record_count = 2_000_000
    source_file = tmp_path / "two-million.jsonl"
    with source_file.open("w", encoding="utf-8", newline="\n") as stream:
        for index in range(record_count):
            stream.write(json.dumps({"id": index, "date": "2026-01-01", "value": index}))
            stream.write("\n")

    process = psutil.Process()
    baseline = process.memory_info().rss
    high_water = [baseline]
    stop = threading.Event()

    def sample_rss() -> None:
        while not stop.wait(0.01):
            high_water[0] = max(high_water[0], process.memory_info().rss)

    sampler = threading.Thread(target=sample_rss, daemon=True)
    sampler.start()
    checkpoint = tmp_path / "checkpoint.json"
    output = tmp_path / "out"
    config = PipelineConfig.model_validate(
        {
            "name": "two-million-stream",
            "sources": [{"name": "large", "type": "jsonl", "path": str(source_file)}],
            "transforms": [
                {"type": "mapping", "settings": {"fields": {"value": "amount"}}},
                {"type": "filtering", "settings": {"field": "amount", "op": "ge", "value": 0}},
            ],
            "sink": {
                "type": "jsonl",
                "output_dir": str(output),
                "batch_size": 20_000,
            },
            "queue_size": 256,
            "checkpoint": str(checkpoint),
        }
    )
    try:
        result = await run_pipeline(config)
    finally:
        stop.set()
        sampler.join()
    high_water[0] = max(high_water[0], process.memory_info().rss)

    assert result["records_written"] == record_count
    assert json.loads(checkpoint.read_text(encoding="utf-8"))["cursors"]["large"] == record_count
    output_files = list(output.glob("date=*/part-*.jsonl"))
    assert len(output_files) == record_count // 20_000
    written_count = sum(sum(1 for _ in part.open(encoding="utf-8")) for part in output_files)
    assert written_count == record_count
    print(
        f"2m records RSS baseline={baseline} bytes, peak={high_water[0]} bytes, "
        f"delta={high_water[0] - baseline} bytes"
    )
    assert high_water[0] - baseline < 128 * 1024 * 1024
