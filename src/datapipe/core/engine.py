"""Concurrent source orchestration with bounded handoff and commit-after-output checkpoints."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
import uuid
from collections import Counter
from collections.abc import AsyncIterator, Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import aiohttp
from opentelemetry.trace import Status, StatusCode
from pydantic import ValidationError

from datapipe.config.schema import PipelineConfig, SourceConfig
from datapipe.core.checkpoint import Checkpoint
from datapipe.core.commit import OutputCommitter
from datapipe.core.locking import OutputDirectoryLock, OutputLockTimeout
from datapipe.models.records import Record
from datapipe.observability.logging import log_context
from datapipe.observability.tracing import tracer
from datapipe.plugins.discovery import discover
from datapipe.plugins.registry import SourceRegistry
from datapipe.sinks.jsonl import JSONLSink
from datapipe.sinks.parquet import ParquetSink
from datapipe.sinks.report import write_report
from datapipe.sources.csv import CSVSource
from datapipe.sources.http import HTTPSource
from datapipe.sources.jsonl import JSONLSource
from datapipe.transforms.aggregation import aggregate_daily
from datapipe.transforms.deduplication import deduplicate
from datapipe.transforms.filtering import filter_records
from datapipe.transforms.mapping import map_record
from datapipe.transforms.parallel import (
    ExecutionMode,
    FingerprintExecutor,
    _fingerprint_fast,
    fingerprint_records_parallel,
)
from datapipe.transforms.state import DiskTransformProcessor

log = logging.getLogger("datapipe")
async def _blocking_to_completion[R](function: Callable[..., R], *args: Any) -> R:
    """Run blocking commit work without releasing the writer lock mid-thread."""
    worker = asyncio.create_task(asyncio.to_thread(function, *args))
    try:
        return await asyncio.shield(worker)
    except asyncio.CancelledError:
        # Cancelling an await does not stop the worker thread. Let atomic file
        # promotion and checkpoint reconciliation finish before propagating.
        await asyncio.shield(worker)
        raise


def _config_signature(config: PipelineConfig) -> str:
    value = config.model_dump(mode="json", exclude={"checkpoint"})
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _source(config: SourceConfig, saved: dict[str, Any]) -> Any:
    """Build a built-in source or an installed entry-point plugin."""
    if config.type == "csv":
        return CSVSource(
            config.name,
            config.path or "",
            config.delimiter,
            config.encoding,
            int(saved.get(config.name, 0)),
        )
    if config.type == "jsonl":
        return JSONLSource(
            config.name, config.path or "", start_line=int(saved.get(config.name, 0))
        )
    if config.type == "http":
        cursor = saved.get(config.name, config.start_page)
        if isinstance(cursor, dict):
            start_page = int(cursor.get("page", config.start_page))
            start_offset = int(cursor.get("offset", 0))
        else:
            # Version-one checkpoints stored only the next page integer.
            start_page = int(cursor)
            start_offset = 0
        return HTTPSource(
            config.name,
            config.url or "",
            config.timeout,
            config.retries,
            config.auth_env,
            config.page_param,
            start_page,
            config.items_key,
            start_offset,
        )
    discover()
    settings = {**config.settings, "name": config.name}
    if config.name in saved:
        settings.setdefault("cursor", saved[config.name])
    if config.path is not None:
        settings.setdefault("path", config.path)
    if config.url is not None:
        settings.setdefault("url", config.url)
    return SourceRegistry.create(config.type, **settings)


def _transform_record(
    row: dict[str, Any],
    config: PipelineConfig,
    fingerprint_executors: Mapping[int, FingerprintExecutor] | None = None,
) -> dict[str, Any] | None:
    """Apply the order-preserving, record-local transformation subset."""
    result = row
    for stage, transform in enumerate(config.transforms):
        settings = transform.settings
        if transform.type == "mapping":
            result = map_record(result, settings.get("fields", {}))
        elif transform.type == "filtering":
            selected = next(
                iter(
                    filter_records(
                        [result],
                        str(settings["field"]),
                        str(settings["op"]),
                        settings.get("value"),
                    )
                ),
                None,
            )
            if selected is None:
                return None
            result = dict(selected)
        elif transform.type == "cpu_fingerprint":
            value = result.get(str(settings["field"]))
            if isinstance(value, (int, float)):
                rounds = int(settings.get("rounds", 1000))
                executor = (fingerprint_executors or {}).get(stage)
                result[str(settings.get("output_field", "fingerprint"))] = (
                    executor.apply(int(value), rounds)
                    if executor is not None
                    else _fingerprint_fast(int(value), rounds)
                )
    return result


async def _read(source: Any, session: aiohttp.ClientSession) -> AsyncIterator[Any]:
    """Support plugins with read() and HTTP implementations accepting shared sessions."""
    if isinstance(source, HTTPSource):
        async for item in source.read(session):
            yield item
    else:
        async for item in source.read():
            yield item


async def _execute_pipeline_with_run_id(
    config: PipelineConfig,
    fail_fast: bool,
    dry_run: bool,
    transform_state: DiskTransformProcessor | None,
    fingerprint_executors: Mapping[int, FingerprintExecutor],
    run_id: str,
) -> dict[str, Any]:
    """Run all sources concurrently; checkpoints advance only after sinks commit."""
    started = datetime.now(UTC)
    clock = time.perf_counter()
    queue: asyncio.Queue[tuple[str, Any, Any] | None] = asyncio.Queue(config.queue_size)
    counts: Counter[str] = Counter()
    valid: list[dict[str, Any]] = []
    valid_count = 0
    streamed_record_count = 0
    dead: list[dict[str, Any]] = []
    dead_count = 0
    errors: list[dict[str, str]] = []
    source_configs = {item.name: item for item in config.sources}
    state = Checkpoint(config.checkpoint, _config_signature(config)) if config.checkpoint else None
    cursors = state.load() if state else {}
    streaming = all(
        transform.type
        in {"mapping", "filtering", "cpu_fingerprint", "deduplication", "aggregation"}
        for transform in config.transforms
    )
    output_root = Path(config.sink.output_dir).resolve()
    committer = (
        OutputCommitter(output_root, _config_signature(config), state, cursors)
        if not dry_run
        else None
    )
    if committer is not None:
        cursors = committer.cursors
        if (
            transform_state is not None
            and committer.manifest["batches"]
            and not committer.has_committed_transform_state()
        ):
            raise ValueError(
                "cannot safely resume this stateful pipeline from a legacy output journal; "
                "the previous deduplication/aggregation state was not persisted"
            )
        committer.begin_run(run_id)
        if transform_state is not None:
            transform_state.restore(committer.committed_transform_updates())
    instances = {name: _source(item, cursors) for name, item in source_configs.items()}
    completed: set[str] = set()
    sink = (
        JSONLSink(str(output_root), config.sink.partition_by)
        if config.sink.type == "jsonl"
        else ParquetSink(str(output_root), config.sink.partition_by)
    )
    written: list[Path] = []
    stream_batch: list[dict[str, Any]] = []
    stream_dead: list[dict[str, Any]] = []
    stream_batch_position: dict[str, Any] = {}
    stream_batch_raw_count = 0

    async def flush_stream_batch() -> None:
        nonlocal stream_batch_raw_count
        state_updates = transform_state.pending_updates() if transform_state is not None else []
        if (
            transform_state is not None
            and not state_updates
            and (stream_batch or stream_dead or stream_batch_position or stream_batch_raw_count)
        ):
            state_updates = [{"op": "version", "version": 1}]
        if not stream_batch and not stream_dead and not stream_batch_position and not state_updates:
            return
        if committer is not None:
            with (
                log_context(stage="sink_commit"),
                tracer.start_as_current_span(
                    "datapipe.sink.commit",
                    attributes={
                        "datapipe.records.output": len(stream_batch),
                        "datapipe.records.dead_letter": len(stream_dead),
                        "datapipe.sources.checkpointed": len(stream_batch_position),
                    },
                ) as commit_span,
            ):
                written.extend(
                    await _blocking_to_completion(
                        committer.commit_batch,
                        stream_batch,
                        stream_dead,
                        stream_batch_position,
                        sink,
                        state_updates,
                    )
                )
                commit_span.set_attribute("datapipe.output.files", len(written))
            if transform_state is not None:
                transform_state.clear_pending()
        stream_batch.clear()
        stream_dead.clear()
        stream_batch_position.clear()
        stream_batch_raw_count = 0

    session_timeout = aiohttp.ClientTimeout(total=max(s.timeout for s in config.sources))
    connector = aiohttp.TCPConnector(limit=config.concurrency, limit_per_host=config.concurrency)

    async def producer(name: str, source: Any) -> None:
        with (
            log_context(source=name, stage="source_read"),
            tracer.start_as_current_span(
                "datapipe.source.read",
                attributes={"datapipe.source": name, "datapipe.source_type": source_configs[name].type},
            ) as source_span,
        ):
            cancelled = False
            should_send_end = True
            try:
                async for raw in _read(source, session):
                    await queue.put((name, raw, getattr(source, "cursor", None)))
                completed.add(name)
            except asyncio.CancelledError:
                cancelled = True
                raise
            except Exception as exc:
                source_span.record_exception(exc)
                source_span.set_status(Status(StatusCode.ERROR, str(exc)))
                errors.append({"source": name, "error": f"{type(exc).__name__}: {exc}"})
                log.exception("source failed: %s", name)
                if fail_fast:
                    should_send_end = False
                    raise
            finally:
                if cancelled:
                    try:
                        queue.put_nowait(None)
                    except asyncio.QueueFull:
                        pass
                elif should_send_end:
                    await queue.put(None)

    async def _consume_records() -> None:
        nonlocal valid_count, dead_count, streamed_record_count, stream_batch_raw_count
        finished = 0
        while finished < len(instances):
            entry = await queue.get()
            try:
                if entry is None:
                    finished += 1
                    continue
                name, raw, source_cursor = entry
                counts[name] += 1
                if streaming:
                    stream_batch_raw_count += 1
                    if source_cursor is not None:
                        stream_batch_position[name] = source_cursor
                marker = (
                    raw.get("__datapipe_error__")
                    if isinstance(raw, dict) and set(raw) == {"__datapipe_error__"}
                    else None
                )
                reason = marker if isinstance(marker, Mapping) else None
                if reason is not None:
                    rejected = (
                        {
                            "source": name,
                            "record": reason.get("input", raw),
                            "timestamp": datetime.now(UTC).isoformat(),
                            "error": reason.get("reason", "malformed input"),
                            "details": reason,
                        }
                    )
                    if streaming:
                        stream_dead.append(rejected)
                    else:
                        dead.append(rejected)
                    dead_count += 1
                else:
                    try:
                        record = Record.model_validate(raw).model_dump(
                            mode="json", exclude_none=True
                        )
                        valid_count += 1
                        if streaming:
                            transformed = (
                                transform_state.process(record, fingerprint_executors=fingerprint_executors)
                                if transform_state is not None
                                else _transform_record(record, config, fingerprint_executors)
                            )
                            if transformed is not None:
                                stream_batch.append(transformed)
                                streamed_record_count += 1
                        else:
                            valid.append(record)
                    except (ValidationError, TypeError, ValueError) as exc:
                        details = (
                            exc.errors(include_url=False)
                            if isinstance(exc, ValidationError)
                            else str(exc)
                        )
                        rejected = {
                            "source": name,
                            "record": raw,
                            "timestamp": datetime.now(UTC).isoformat(),
                            "error": str(exc),
                            "details": details,
                        }
                        if streaming:
                            stream_dead.append(rejected)
                        else:
                            dead.append(rejected)
                        dead_count += 1
                if streaming and stream_batch_raw_count >= config.sink.batch_size:
                    await flush_stream_batch()
            finally:
                queue.task_done()

    async def consumer() -> None:
        with (
            log_context(stage="record_validation_transform"),
            tracer.start_as_current_span("datapipe.record_validation_transform") as span,
        ):
            await _consume_records()
            span.set_attribute("datapipe.records.fetched", sum(counts.values()))
            span.set_attribute("datapipe.records.valid", valid_count)
            span.set_attribute("datapipe.records.invalid", dead_count)

    async with (
        aiohttp.ClientSession(timeout=session_timeout, connector=connector) as session,
        asyncio.TaskGroup() as group,
    ):
        group.create_task(consumer(), name="datapipe-consumer")
        for name, source in instances.items():
            group.create_task(producer(name, source), name=f"datapipe-source-{name}")

    if streaming and not dry_run:
        for name in completed:
            cursor_value = getattr(instances[name], "cursor", None)
            if cursor_value is not None:
                stream_batch_position[name] = cursor_value
        await flush_stream_batch()

    if transform_state is not None and committer is not None:
        for stage, transform in enumerate(config.transforms):
            if transform.type != "aggregation":
                continue
            last_day, done = transform_state.progress(stage)
            if done:
                continue
            date_field = str(transform.settings.get("date_field", "date"))
            processed = 0
            for aggregate in transform_state.aggregate_rows(stage, date_field, last_day):
                transformed = transform_state.process(
                    aggregate, stage + 1, fingerprint_executors
                )
                if transformed is not None:
                    stream_batch.append(transformed)
                    streamed_record_count += 1
                transform_state.set_progress(stage, str(aggregate[date_field]))
                processed += 1
                if processed >= config.sink.batch_size:
                    await flush_stream_batch()
                    processed = 0
            final_day, _ = transform_state.progress(stage)
            transform_state.set_progress(stage, final_day, True)
            await flush_stream_batch()

    records: list[dict[str, Any]] = valid
    duplicates = transform_state.duplicates if transform_state is not None else 0
    for transform in (() if streaming else config.transforms):
        settings = transform.settings
        if transform.type == "mapping":
            records = [map_record(row, settings.get("fields", {})) for row in records]
        elif transform.type == "filtering":
            records = [
                dict(row)
                for row in filter_records(
                    records, str(settings["field"]), str(settings["op"]), settings.get("value")
                )
            ]
        elif transform.type == "deduplication":
            before = len(records)
            records = [dict(row) for row in deduplicate(records, str(settings["key"]))]
            duplicates += before - len(records)
        elif transform.type == "aggregation":
            records = aggregate_daily(
                records, str(settings.get("date_field", "date")), settings.get("metrics", {})
            )
        elif transform.type == "cpu_fingerprint":
            records = fingerprint_records_parallel(
                records,
                str(settings["field"]),
                str(settings.get("output_field", "fingerprint")),
                int(settings.get("rounds", 1000)),
                int(settings.get("workers", config.concurrency)),
            )

    if dry_run:
        return {
            "pipeline": config.name,
            "sources": list(source_configs),
            "dry_run": True,
            "transforms": len(config.transforms),
        }

    if not streaming and committer is not None:
        final_cursors = {
            name: instances[name].cursor
            for name in completed
            if getattr(instances[name], "cursor", None) is not None
        }
        written.extend(
            await _blocking_to_completion(
                committer.commit_batch, records, dead, final_cursors, sink
            )
        )
    if committer is not None:
        await _blocking_to_completion(committer.materialize_dead_letters)

    duration = time.perf_counter() - clock
    report: dict[str, Any] = {
        "run_id": run_id,
        "pipeline": config.name,
        "started_at": started,
        "finished_at": datetime.now(UTC),
        "total_fetched": sum(counts.values()),
        "valid_records": valid_count,
        "invalid_records": dead_count,
        "duplicates_removed": duplicates,
        "records_written": streamed_record_count if streaming else len(records),
        "records_per_source": {name: counts[name] for name in source_configs},
        "source_errors": errors,
        "duration_seconds": duration,
        "processing_stats": {
            "records_per_second": valid_count / duration if duration else 0,
            "output_files": len(written),
        },
    }
    write_report(output_root / "summary.json", report)
    if committer is not None:
        committer.complete_run(run_id)
    return report


async def _execute_pipeline(
    config: PipelineConfig,
    fail_fast: bool,
    dry_run: bool,
    transform_state: DiskTransformProcessor | None,
    fingerprint_executors: Mapping[int, FingerprintExecutor],
) -> dict[str, Any]:
    run_id = str(uuid.uuid4())
    with (
        log_context(run_id=run_id, stage="pipeline"),
        tracer.start_as_current_span(
            "datapipe.pipeline",
            attributes={"datapipe.pipeline": config.name, "datapipe.run_id": run_id},
        ) as pipeline_span,
    ):
        report = await _execute_pipeline_with_run_id(
            config, fail_fast, dry_run, transform_state, fingerprint_executors, run_id
        )
        for metric in ("total_fetched", "valid_records", "invalid_records", "records_written"):
            value = report.get(metric)
            if isinstance(value, int):
                pipeline_span.set_attribute(f"datapipe.{metric}", value)
        return report


async def _run_pipeline_unlocked(
    config: PipelineConfig, fail_fast: bool = False, dry_run: bool = False
) -> dict[str, Any]:
    """Create and always clean disposable on-disk transformation state."""
    stateful = any(
        transform.type in {"deduplication", "aggregation"} for transform in config.transforms
    )
    transform_state = (
        DiskTransformProcessor(Path(config.sink.output_dir), config.transforms)
        if stateful and not dry_run
        else None
    )
    fingerprint_executors: dict[int, FingerprintExecutor] = {}
    try:
        if not dry_run:
            for stage, transform in enumerate(config.transforms):
                if transform.type != "cpu_fingerprint":
                    continue
                expected = transform.settings.get("expected_records")
                fingerprint_executors[stage] = FingerprintExecutor(
                    cast(ExecutionMode, str(transform.settings.get("execution", "auto"))),
                    int(transform.settings.get("workers", config.concurrency)),
                    int(expected) if expected is not None else None,
                    int(transform.settings.get("rounds", 1000)),
                )
        return await _execute_pipeline(
            config, fail_fast, dry_run, transform_state, fingerprint_executors
        )
    finally:
        if transform_state is not None:
            transform_state.close()
        for executor in fingerprint_executors.values():
            executor.close()


async def run_pipeline(
    config: PipelineConfig, fail_fast: bool = False, dry_run: bool = False
) -> dict[str, Any]:
    """Run one pipeline while excluding other writers to its output directory."""
    if dry_run:
        return await _run_pipeline_unlocked(config, fail_fast, True)
    root = Path(config.sink.output_dir).resolve()
    lock = OutputDirectoryLock(root)
    deadline = time.monotonic() + lock.timeout
    while not lock.try_acquire():
        if time.monotonic() >= deadline:
            raise OutputLockTimeout(
                f"timed out after {lock.timeout:g}s waiting for output lock {lock.path}"
            )
        await asyncio.sleep(min(0.05, max(0.001, deadline - time.monotonic())))
    try:
        return await _run_pipeline_unlocked(config, fail_fast, False)
    finally:
        lock.release()
