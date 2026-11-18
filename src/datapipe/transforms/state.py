"""Bounded-memory, journal-replayable state for deduplication and aggregation."""

from __future__ import annotations

import json
import math
import os
import sqlite3
import tempfile
import uuid
from collections.abc import Iterable, Iterator, Mapping, Sequence
from datetime import date
from fractions import Fraction
from pathlib import Path
from typing import Any

from datapipe.config.schema import TransformConfig
from datapipe.core.exceptions import TransformStateError
from datapipe.transforms.filtering import filter_records
from datapipe.transforms.mapping import map_record
from datapipe.transforms.parallel import FingerprintExecutor, _fingerprint_fast


class DiskTransformProcessor:
    """Run stateful transforms against a temporary SQLite index.

    Committed state is reconstructed from output-journal delta files. The SQLite
    database is a disposable working index, never an independent checkpoint.
    """

    def __init__(self, root: Path, transforms: Sequence[TransformConfig]) -> None:
        self.transforms = transforms
        self._progress_reopened: set[int] = set()
        for transform in transforms:
            if transform.type == "aggregation":
                metrics = transform.settings.get("metrics", {})
                if not isinstance(metrics, Mapping) or any(
                    operation not in {"sum", "count", "min", "max"}
                    for operation in metrics.values()
                ):
                    raise ValueError("aggregation metrics must use sum, count, min, or max")
        metadata = root.resolve() / ".datapipe"
        metadata.mkdir(parents=True, exist_ok=True)
        self._remove_stale_databases(metadata)
        fd, filename = tempfile.mkstemp(prefix=".transform-state-", suffix=".sqlite", dir=metadata)
        os.close(fd)
        self.path = Path(filename)
        self.duplicates = 0
        try:
            self.connection = sqlite3.connect(self.path)
            self.connection.execute("PRAGMA journal_mode=DELETE")
            self.connection.execute("PRAGMA synchronous=FULL")
            self.connection.execute("PRAGMA temp_store=FILE")
            self.connection.execute("PRAGMA cache_size=-8192")
            self.connection.executescript(
                """
                CREATE TABLE seen(stage INTEGER NOT NULL, token TEXT NOT NULL,
                                  PRIMARY KEY(stage, token));
                CREATE TABLE pending_seen(stage INTEGER NOT NULL, token TEXT NOT NULL,
                                          PRIMARY KEY(stage, token));
                CREATE TABLE aggregates(stage INTEGER NOT NULL, day TEXT NOT NULL,
                                        state TEXT NOT NULL, PRIMARY KEY(stage, day));
                CREATE TABLE pending_aggregates(stage INTEGER NOT NULL, day TEXT NOT NULL,
                                                state TEXT NOT NULL,
                                                PRIMARY KEY(stage, day));
                CREATE TABLE dirty_aggregates(stage INTEGER NOT NULL, day TEXT NOT NULL,
                                              PRIMARY KEY(stage, day));
                CREATE TABLE progress(stage INTEGER PRIMARY KEY, last_day TEXT, done INTEGER NOT NULL);
                CREATE TABLE pending_progress(stage INTEGER PRIMARY KEY, last_day TEXT,
                                              done INTEGER NOT NULL);
                """
            )
            self.connection.commit()
        except sqlite3.Error as exc:
            self.close()
            raise TransformStateError(f"could not initialize disk-backed transform state: {exc}") from exc

    @staticmethod
    def _remove_stale_databases(metadata: Path) -> None:
        """Remove only this application's abandoned SQLite work files.

        The output-root lock must be held by the caller, so no cooperating
        pipeline can have a live temporary state database in this directory.
        """
        try:
            for candidate in metadata.glob(".transform-state-*.sqlite*"):
                if candidate.is_file():
                    candidate.unlink()
        except OSError as exc:
            raise TransformStateError(f"could not clean stale transformation state: {exc}") from exc

    def restore(self, events: Iterable[Mapping[str, Any]]) -> None:
        """Rebuild the working index from committed state events, one at a time."""
        try:
            with self.connection:
                for event in events:
                    operation = event.get("op")
                    if operation == "version":
                        if event.get("version") != 1:
                            raise TransformStateError("unsupported transform-state journal version")
                        continue
                    stage = int(event["stage"])
                    if stage < 0 or stage >= len(self.transforms):
                        raise TransformStateError(f"invalid transform-state stage: {stage}")
                    if operation == "dedupe":
                        self.connection.execute(
                            "INSERT OR IGNORE INTO seen(stage, token) VALUES (?, ?)",
                            (stage, str(event["token"])),
                        )
                    elif operation == "aggregate":
                        self.connection.execute(
                            "INSERT INTO aggregates(stage, day, state) VALUES (?, ?, ?) "
                            "ON CONFLICT(stage, day) DO UPDATE SET state=excluded.state",
                            (stage, str(event["day"]), json.dumps(event["state"], allow_nan=True)),
                        )
                        self.connection.execute(
                            "INSERT OR IGNORE INTO dirty_aggregates(stage, day) VALUES (?, ?)",
                            (stage, str(event["day"])),
                        )
                    elif operation == "progress":
                        self.connection.execute(
                            "INSERT INTO progress(stage, last_day, done) VALUES (?, ?, ?) "
                            "ON CONFLICT(stage) DO UPDATE SET last_day=excluded.last_day, "
                            "done=excluded.done",
                            (stage, event.get("last_day"), int(bool(event.get("done")))),
                        )
                        if event.get("done"):
                            self.connection.execute(
                                "DELETE FROM dirty_aggregates WHERE stage=?", (stage,)
                            )
                    else:
                        raise TransformStateError(f"unknown transform-state event: {operation!r}")
        except sqlite3.Error as exc:
            raise TransformStateError(f"could not restore disk-backed transform state: {exc}") from exc

    def process(
        self,
        row: dict[str, Any],
        start: int = 0,
        fingerprint_executors: Mapping[int, FingerprintExecutor] | None = None,
    ) -> dict[str, Any] | None:
        """Apply transforms from ``start``, stopping at a stateful aggregate."""
        result = row
        for stage in range(start, len(self.transforms)):
            transform = self.transforms[stage]
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
            elif transform.type == "deduplication":
                if not self.accept(stage, result, str(settings["key"])):
                    return None
            elif transform.type == "aggregation":
                self.add_to_aggregate(
                    stage,
                    result,
                    str(settings.get("date_field", "date")),
                    settings.get("metrics", {}),
                )
                return None
        return result

    @staticmethod
    def _key_token(value: Any) -> str:
        try:
            hash(value)
        except TypeError as exc:
            raise ValueError("deduplication key must be hashable") from exc
        if isinstance(value, (bool, int)):
            numerator, denominator = int(value), 1
            tag = "number"
        elif isinstance(value, float):
            if math.isnan(value):
                # NaN is unequal to itself in Python set semantics.
                return json.dumps(["nan", uuid.uuid4().hex], separators=(",", ":"))
            if math.isinf(value):
                return json.dumps(["infinity", 1 if value > 0 else -1], separators=(",", ":"))
            numerator, denominator = value.as_integer_ratio()
            tag = "number"
        elif value is None:
            return '["none"]'
        elif isinstance(value, str):
            return json.dumps(["string", value], ensure_ascii=False, separators=(",", ":"))
        else:
            # Record values cross JSON serialization at the validation boundary;
            # unsupported hashable plugin values still need a stable, safe token.
            return json.dumps(
                [type(value).__qualname__, str(value)], ensure_ascii=False, separators=(",", ":")
            )
        return json.dumps([tag, numerator, denominator], separators=(",", ":"))

    def accept(self, stage: int, row: Mapping[str, Any], key: str) -> bool:
        """Return true for the first record with a key, preserving missing-key behavior."""
        if key not in row:
            return True
        token = self._key_token(row[key])
        try:
            cursor = self.connection.execute(
                "INSERT OR IGNORE INTO seen(stage, token) VALUES (?, ?)", (stage, token)
            )
            if cursor.rowcount == 0:
                self.duplicates += 1
                return False
            self.connection.execute(
                "INSERT OR IGNORE INTO pending_seen(stage, token) VALUES (?, ?)", (stage, token)
            )
            return True
        except sqlite3.Error as exc:
            raise TransformStateError(f"disk-backed deduplication failed: {exc}") from exc

    def _aggregate_state(
        self, stage: int, day: str, row_count: int, row: Mapping[str, Any], metrics: Mapping[str, str]
    ) -> dict[str, Any]:
        saved = self.connection.execute(
            "SELECT state FROM aggregates WHERE stage=? AND day=?", (stage, day)
        ).fetchone()
        state: dict[str, Any] = (
            json.loads(saved[0]) if saved else {"rows": 0, "metrics": {}}
        )
        state["rows"] = row_count
        metric_state: dict[str, Any] = state["metrics"]
        for field, operation in metrics.items():
            if operation not in {"sum", "count", "min", "max"}:
                raise ValueError(f"unsupported aggregate: {operation}")
            if operation == "count":
                continue
            value = row.get(field)
            if not isinstance(value, (int, float)):
                continue
            numeric = float(value)
            prior = metric_state.get(field)
            if operation == "sum":
                accumulator = prior or {"units": 0, "nan": False, "positive_inf": False, "negative_inf": False}
                if math.isnan(numeric):
                    accumulator["nan"] = True
                elif math.isinf(numeric):
                    accumulator["positive_inf" if numeric > 0 else "negative_inf"] = True
                else:
                    numerator, denominator = numeric.as_integer_ratio()
                    exponent = denominator.bit_length() - 1
                    accumulator["units"] += numerator << (1074 - exponent)
                metric_state[field] = accumulator
            elif operation == "min":
                metric_state[field] = numeric if prior is None else min(prior, numeric)
            elif operation == "max":
                metric_state[field] = numeric if prior is None else max(prior, numeric)
        return state

    def add_to_aggregate(
        self, stage: int, row: Mapping[str, Any], date_field: str, metrics: Mapping[str, str]
    ) -> None:
        value = row.get(date_field)
        if value is None:
            return
        day = str(value)[:10]
        try:
            date.fromisoformat(day)
        except ValueError:
            return
        try:
            current = self.connection.execute(
                "SELECT state FROM aggregates WHERE stage=? AND day=?", (stage, day)
            ).fetchone()
            previous: dict[str, Any] = (
                json.loads(current[0]) if current else {"rows": 0, "metrics": {}}
            )
            state = self._aggregate_state(stage, day, int(previous["rows"]) + 1, row, metrics)
            encoded = json.dumps(state, allow_nan=True, separators=(",", ":"))
            self.connection.execute(
                "INSERT INTO aggregates(stage, day, state) VALUES (?, ?, ?) "
                "ON CONFLICT(stage, day) DO UPDATE SET state=excluded.state",
                (stage, day, encoded),
            )
            self.connection.execute(
                "INSERT INTO pending_aggregates(stage, day, state) VALUES (?, ?, ?) "
                "ON CONFLICT(stage, day) DO UPDATE SET state=excluded.state",
                (stage, day, encoded),
            )
            self.connection.execute(
                "INSERT OR IGNORE INTO dirty_aggregates(stage, day) VALUES (?, ?)",
                (stage, day),
            )
            if stage not in self._progress_reopened:
                self._progress_reopened.add(stage)
                progress = self.connection.execute(
                    "SELECT last_day, done FROM progress WHERE stage=?", (stage,)
                ).fetchone()
                if progress is None or progress[0] is not None or bool(progress[1]):
                    self.set_progress(stage, None, False)
        except sqlite3.Error as exc:
            raise TransformStateError(f"disk-backed aggregation failed: {exc}") from exc

    def aggregate_rows(self, stage: int, date_field: str, after: str | None) -> Iterator[dict[str, Any]]:
        try:
            if after is None:
                cursor = self.connection.execute(
                    "SELECT a.day, a.state FROM aggregates a JOIN dirty_aggregates d "
                    "ON a.stage=d.stage AND a.day=d.day WHERE a.stage=? ORDER BY a.day",
                    (stage,),
                )
            else:
                cursor = self.connection.execute(
                    "SELECT a.day, a.state FROM aggregates a JOIN dirty_aggregates d "
                    "ON a.stage=d.stage AND a.day=d.day WHERE a.stage=? AND a.day>? ORDER BY a.day",
                    (stage, after),
                )
            for day, encoded in cursor:
                state = json.loads(encoded)
                result: dict[str, Any] = {date_field: day}
                for field, operation in self.transforms[stage].settings.get("metrics", {}).items():
                    if operation == "count":
                        result[f"{field}_count"] = state["rows"]
                    elif operation == "sum":
                        accumulator = state["metrics"].get(field)
                        if accumulator is None:
                            result[f"{field}_sum"] = 0
                        elif accumulator["nan"] or (
                            accumulator["positive_inf"] and accumulator["negative_inf"]
                        ):
                            result[f"{field}_sum"] = float("nan")
                        elif accumulator["positive_inf"]:
                            result[f"{field}_sum"] = float("inf")
                        elif accumulator["negative_inf"]:
                            result[f"{field}_sum"] = float("-inf")
                        else:
                            exact = Fraction(accumulator["units"], 1 << 1074)
                            try:
                                result[f"{field}_sum"] = float(exact)
                            except OverflowError:
                                sign = -1.0 if accumulator["units"] < 0 else 1.0
                                result[f"{field}_sum"] = math.copysign(float("inf"), sign)
                    elif operation in {"min", "max"} and field in state["metrics"]:
                        result[f"{field}_{operation}"] = state["metrics"][field]
                yield result
        except sqlite3.Error as exc:
            raise TransformStateError(f"could not read disk-backed aggregates: {exc}") from exc

    def progress(self, stage: int) -> tuple[str | None, bool]:
        try:
            row = self.connection.execute(
                "SELECT last_day, done FROM progress WHERE stage=?", (stage,)
            ).fetchone()
            return (row[0], bool(row[1])) if row else (None, False)
        except sqlite3.Error as exc:
            raise TransformStateError(f"could not read aggregation progress: {exc}") from exc

    def set_progress(self, stage: int, last_day: str | None, done: bool = False) -> None:
        try:
            values = (stage, last_day, int(done))
            self.connection.execute(
                "INSERT INTO progress(stage, last_day, done) VALUES (?, ?, ?) "
                "ON CONFLICT(stage) DO UPDATE SET last_day=excluded.last_day, done=excluded.done",
                values,
            )
            self.connection.execute(
                "INSERT INTO pending_progress(stage, last_day, done) VALUES (?, ?, ?) "
                "ON CONFLICT(stage) DO UPDATE SET last_day=excluded.last_day, done=excluded.done",
                values,
            )
            if done:
                self.connection.execute("DELETE FROM dirty_aggregates WHERE stage=?", (stage,))
        except sqlite3.Error as exc:
            raise TransformStateError(f"could not save aggregation progress: {exc}") from exc

    def pending_updates(self) -> list[dict[str, Any]]:
        """Return only current-batch deltas; their count is bounded by the batch size."""
        try:
            updates: list[dict[str, Any]] = []
            for stage, token in self.connection.execute(
                "SELECT stage, token FROM pending_seen ORDER BY stage, token"
            ):
                updates.append({"op": "dedupe", "stage": stage, "token": token})
            for stage, day, encoded in self.connection.execute(
                "SELECT stage, day, state FROM pending_aggregates ORDER BY stage, day"
            ):
                updates.append(
                    {"op": "aggregate", "stage": stage, "day": day, "state": json.loads(encoded)}
                )
            for stage, last_day, done in self.connection.execute(
                "SELECT stage, last_day, done FROM pending_progress ORDER BY stage"
            ):
                updates.append(
                    {"op": "progress", "stage": stage, "last_day": last_day, "done": bool(done)}
                )
            return updates
        except sqlite3.Error as exc:
            raise TransformStateError(f"could not read pending transformation state: {exc}") from exc

    def clear_pending(self) -> None:
        try:
            with self.connection:
                self.connection.execute("DELETE FROM pending_seen")
                self.connection.execute("DELETE FROM pending_aggregates")
                self.connection.execute("DELETE FROM pending_progress")
        except sqlite3.Error as exc:
            raise TransformStateError(f"could not clear committed transform deltas: {exc}") from exc

    def close(self) -> None:
        connection = getattr(self, "connection", None)
        if connection is not None:
            connection.close()
            del self.connection
        for suffix in ("", "-journal", "-wal", "-shm"):
            try:
                Path(f"{self.path}{suffix}").unlink(missing_ok=True)
            except OSError as exc:
                raise TransformStateError(f"could not remove temporary state file: {exc}") from exc
