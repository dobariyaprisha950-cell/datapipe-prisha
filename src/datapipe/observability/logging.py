"""Structured logging with asynchronous run, source, and stage context."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token
from datetime import UTC, datetime
from typing import Any

from datapipe.observability.redaction import redact

run_id_context: ContextVar[str] = ContextVar("datapipe_run_id", default="-")
source_context: ContextVar[str] = ContextVar("datapipe_source", default="-")
stage_context: ContextVar[str] = ContextVar("datapipe_stage", default="-")


@contextmanager
def log_context(
    *, run_id: str | None = None, source: str | None = None, stage: str | None = None
) -> Iterator[None]:
    """Temporarily bind log fields; contextvars propagate to asyncio tasks/threads."""
    variables: list[tuple[ContextVar[str], Token[str]]] = []
    for variable, value in (
        (run_id_context, run_id),
        (source_context, source),
        (stage_context, stage),
    ):
        if value is not None:
            variables.append((variable, variable.set(value)))
    try:
        yield
    finally:
        for variable, token in reversed(variables):
            variable.reset(token)


class ContextFilter(logging.Filter):
    """Add contextual fields to each structured log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = run_id_context.get()
        record.source = source_context.get()
        record.stage = stage_context.get()
        return True


class JsonLogFormatter(logging.Formatter):
    """Format one valid JSON object per log record."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "time": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "run_id": getattr(record, "run_id", "-"),
            "source": getattr(record, "source", "-"),
            "stage": getattr(record, "stage", "-"),
            "message": redact(record.getMessage()),
        }
        if record.exc_info:
            payload["exception"] = redact(self.formatException(record.exc_info))
        elif record.exc_text:
            payload["exception"] = redact(record.exc_text)
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
