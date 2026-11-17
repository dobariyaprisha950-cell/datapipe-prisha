import json
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from datapipe.config.schema import PipelineConfig
from datapipe.core.engine import run_pipeline
from datapipe.plugins.registry import SourceRegistry
from datapipe.sources.jsonl import JSONLSource


@pytest.mark.asyncio
async def test_jsonl_malformed_is_emitted_as_rejection(tmp_path: Path) -> None:
    path = tmp_path / "in.jsonl"
    path.write_text('{"id":"ok","date":"2026-01-01"}\n{bad json}\n', encoding="utf-8")
    source = JSONLSource("local", str(path))
    rows = [row async for row in source.read()]
    assert rows[0]["id"] == "ok"
    assert rows[1]["__datapipe_error__"]["input"] == "{bad json}"
    assert source.cursor == 2


class SampleIntegrationSource:
    def __init__(self, name: str, records: list[dict[str, object]]) -> None:
        self.name, self.records = name, records
        self.cursor = len(records)

    async def read(self) -> AsyncIterator[dict[str, object]]:
        for record in self.records:
            yield record


@pytest.mark.asyncio
async def test_installed_source_registry_plugin_runs_and_invalid_record_dead_letters(
    tmp_path: Path,
) -> None:
    plugin_name = "test-sample-plugin"
    SourceRegistry.register(plugin_name, SampleIntegrationSource)
    config = PipelineConfig.model_validate({
        "name": "plugin-check",
        "sources": [{"name": "plug", "type": plugin_name,
                     "settings": {"records": [
                         {"id": "valid", "date": "2026-01-01"},
                         {"id": "invalid", "date": False},
                     ]}}],
        "sink": {"type": "jsonl", "output_dir": str(tmp_path / "out")},
        "checkpoint": str(tmp_path / "checkpoint.json"),
    })
    result = await run_pipeline(config)
    assert result["records_written"] == 1
    assert result["invalid_records"] == 1
    dead = (tmp_path / "out" / "dead-letter.jsonl").read_text(encoding="utf-8")
    row = json.loads(dead)
    assert row["source"] == "plug" and row["record"]["id"] == "invalid"
    checkpoint = json.loads((tmp_path / "checkpoint.json").read_text(encoding="utf-8"))
    assert checkpoint["plug"] == 2


@pytest.mark.asyncio
async def test_file_checkpoint_resume_skips_committed_jsonl_rows(tmp_path: Path) -> None:
    source_file = tmp_path / "records.jsonl"
    source_file.write_text(
        '{"id":"one","date":"2026-01-01"}\n'
        '{"id":"two","date":"2026-01-02"}\n', encoding="utf-8")
    checkpoint = tmp_path / "cursor.json"
    config = PipelineConfig.model_validate({
        "name": "file-resume", "sources": [{"name": "file", "type": "jsonl",
                                             "path": str(source_file)}],
        "sink": {"type": "jsonl", "output_dir": str(tmp_path / "out")},
        "checkpoint": str(checkpoint),
    })
    first = await run_pipeline(config)
    second = await run_pipeline(config)
    assert first["total_fetched"] == 2
    assert second["total_fetched"] == 0
    assert json.loads(checkpoint.read_text(encoding="utf-8"))["file"] == 2


@pytest.mark.asyncio
async def test_failed_source_isolated_when_not_fail_fast(tmp_path: Path) -> None:
    config = PipelineConfig.model_validate({
        "name": "isolation",
        "sources": [
            {"name": "missing", "type": "jsonl", "path": str(tmp_path / "missing")},
            {"name": "good", "type": "sample-isolation",
             "settings": {"records": [{"id": "ok", "date": "2026-01-01"}]}},
        ],
        "sink": {"type": "jsonl", "output_dir": str(tmp_path / "out")},
    })
    SourceRegistry.register("sample-isolation", SampleIntegrationSource)
    result = await run_pipeline(config)
    assert result["records_written"] == 1
    assert len(result["source_errors"]) == 1


@pytest.mark.asyncio
async def test_fail_fast_propagates_source_failure(tmp_path: Path) -> None:
    config = PipelineConfig.model_validate({
        "name": "fail-fast", "sources": [{"name": "missing", "type": "jsonl",
                                               "path": str(tmp_path / "missing")}],
        "sink": {"type": "jsonl", "output_dir": str(tmp_path / "out")},
    })
    with pytest.raises(ExceptionGroup):
        await run_pipeline(config, fail_fast=True)
