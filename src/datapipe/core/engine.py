"""Concurrent source orchestration with bounded handoff and commit-after-output checkpoints."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
import uuid
from collections import Counter
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiohttp
from pydantic import ValidationError

from datapipe.config.schema import PipelineConfig, SourceConfig
from datapipe.core.checkpoint import Checkpoint
from datapipe.core.commit import OutputCommitter
from datapipe.models.records import Record
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
from datapipe.transforms.parallel import fingerprint_records_parallel

log = logging.getLogger("datapipe")


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


def _transform_record(row: dict[str, Any], config: PipelineConfig) -> dict[str, Any] | None:
    """Apply the order-preserving, record-local transformation subset."""
    result = row
    for transform in config.transforms:
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
    return result


async def _read(source: Any, session: aiohttp.ClientSession) -> AsyncIterator[Any]:
    """Support plugins with read() and HTTP implementations accepting shared sessions."""
    if isinstance(source, HTTPSource):
        async for item in source.read(session):
            yield item
    else:
        async for item in source.read():
            yield item


async def run_pipeline(
    config: PipelineConfig, fail_fast: bool = False, dry_run: bool = False
) -> dict[str, Any]:
    """Run all sources concurrently; checkpoints advance only after sinks commit."""
    started = datetime.now(UTC)
    clock = time.perf_counter()
    run_id = str(uuid.uuid4())
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
    streaming = all(transform.type in {"mapping", "filtering"} for transform in config.transforms)
    output_root = Path(config.sink.output_dir).resolve()
    committer = (
        OutputCommitter(output_root, _config_signature(config), state, cursors)
        if not dry_run
        else None
    )
    if committer is not None:
        cursors = committer.cursors
        committer.begin_run(run_id)
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
        if not stream_batch and not stream_dead and not stream_batch_position:
            return
        if committer is not None:
            written.extend(
                await asyncio.to_thread(
                    committer.commit_batch, stream_batch, stream_dead, stream_batch_position, sink
                )
            )
        stream_batch.clear()
        stream_dead.clear()
        stream_batch_position.clear()
        stream_batch_raw_count = 0

    session_timeout = aiohttp.ClientTimeout(total=max(s.timeout for s in config.sources))
    connector = aiohttp.TCPConnector(limit=config.concurrency, limit_per_host=config.concurrency)

    async def producer(name: str, source: Any) -> None:
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

    async def consumer() -> None:
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
                reason = raw.get("__datapipe_error__") if isinstance(raw, dict) else None
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
                            transformed = _transform_record(record, config)
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

    records: list[dict[str, Any]] = valid
    duplicates = 0
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
            await asyncio.to_thread(committer.commit_batch, records, dead, final_cursors, sink)
        )
    if committer is not None:
        await asyncio.to_thread(committer.materialize_dead_letters)

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
