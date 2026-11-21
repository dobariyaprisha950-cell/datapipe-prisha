import io
import json
import logging
from pathlib import Path
from typing import Any, Self

import pytest

from datapipe.config.schema import PipelineConfig
from datapipe.core.engine import run_pipeline
from datapipe.observability.logging import ContextFilter, JsonLogFormatter, log_context
from datapipe.observability.redaction import RedactionFilter


def test_json_logs_include_context_and_redact_formatted_secrets() -> None:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.addFilter(RedactionFilter())
    handler.addFilter(ContextFilter())
    handler.setFormatter(JsonLogFormatter())
    logger = logging.getLogger("datapipe-observability-test")
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    try:
        with log_context(run_id="run-123", source="api", stage="source_read"):
            logger.info("Authorization: Bearer %s", "top-secret")
    finally:
        logger.handlers.clear()
    rendered = stream.getvalue().strip()
    record = json.loads(rendered)
    assert record["run_id"] == "run-123"
    assert record["source"] == "api"
    assert record["stage"] == "source_read"
    assert "top-secret" not in rendered
    assert "[REDACTED]" in record["message"]


class RecordingSpan:
    def __init__(self, name: str, attributes: dict[str, Any]) -> None:
        self.name = name
        self.attributes = dict(attributes)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def set_attribute(self, name: str, value: Any) -> None:
        self.attributes[name] = value

    def record_exception(self, _exception: BaseException) -> None:
        return None

    def set_status(self, _status: Any) -> None:
        return None


class RecordingTracer:
    def __init__(self) -> None:
        self.spans: list[RecordingSpan] = []

    def start_as_current_span(
        self, name: str, attributes: dict[str, Any] | None = None
    ) -> RecordingSpan:
        span = RecordingSpan(name, attributes or {})
        self.spans.append(span)
        return span


@pytest.mark.asyncio
async def test_pipeline_creates_pipeline_source_transform_and_sink_spans(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from datapipe.core import engine

    tracer = RecordingTracer()
    monkeypatch.setattr(engine, "tracer", tracer)
    source = tmp_path / "input.jsonl"
    source.write_text('{"id":"one","date":"2026-01-01"}\n', encoding="utf-8")
    config = PipelineConfig.model_validate(
        {
            "name": "span-test",
            "sources": [{"name": "source-a", "type": "jsonl", "path": str(source)}],
            "sink": {"type": "jsonl", "output_dir": str(tmp_path / "out")},
        }
    )
    await run_pipeline(config)
    names = {span.name for span in tracer.spans}
    assert names == {
        "datapipe.pipeline",
        "datapipe.source.read",
        "datapipe.record_validation_transform",
        "datapipe.sink.commit",
    }
    pipeline = next(span for span in tracer.spans if span.name == "datapipe.pipeline")
    assert pipeline.attributes["datapipe.valid_records"] == 1
