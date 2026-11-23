"""Deterministic per-day numeric aggregation."""

import math
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import date
from fractions import Fraction
from typing import Any

_MAX_EXACT_INTEGER = 1 << 53
_DEFAULT_BATCH_SIZE = 50_000
_MIN_NATIVE_RECORDS = 5_000


@dataclass(slots=True)
class _DailyArrowSummary:
    """Native daily aggregate retaining integer sums for exact state merging."""

    day: str
    row_count: int = 0
    metrics: dict[str, Any] = dataclass_field(default_factory=dict)
    integer_sums: dict[str, int] = dataclass_field(default_factory=dict)


def _group_records(
    records: Iterable[Mapping[str, Any]], date_field: str
) -> dict[str, list[Mapping[str, Any]]]:
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in records:
        value = row.get(date_field)
        if value is None:
            continue
        day = str(value)[:10]
        try:
            date.fromisoformat(day)
        except ValueError:
            continue
        groups[day].append(row)
    return groups


def _aggregate_grouped_python(
    groups: Mapping[str, list[Mapping[str, Any]]],
    date_field: str,
    metrics: Mapping[str, str],
) -> list[dict[str, Any]]:
    """Exact reference implementation retained for unsafe inputs and benchmarks."""
    result = []
    for day, rows in sorted(groups.items()):
        out: dict[str, Any] = {date_field: day}
        for field, op in metrics.items():
            values = [
                float(row[field])
                for row in rows
                if field in row and isinstance(row[field], (int, float))
            ]
            if op == "count":
                out[f"{field}_{op}"] = len(rows)
            elif op == "sum":
                positive_inf = any(value == math.inf for value in values)
                negative_inf = any(value == -math.inf for value in values)
                if any(math.isnan(value) for value in values) or (
                    positive_inf and negative_inf
                ):
                    total = float("nan")
                elif positive_inf:
                    total = math.inf
                elif negative_inf:
                    total = -math.inf
                else:
                    exact = sum((Fraction.from_float(value) for value in values), Fraction())
                    try:
                        total = float(exact)
                    except OverflowError:
                        if exact.numerator > 0:
                            total = math.inf
                        elif exact.numerator < 0:
                            total = -math.inf
                        else:
                            total = 0.0
                out[f"{field}_{op}"] = total
            elif op == "min" and values:
                out[f"{field}_{op}"] = min(values)
            elif op == "max" and values:
                out[f"{field}_{op}"] = max(values)
            else:
                if op not in {"count", "sum", "min", "max"}:
                    raise ValueError(f"unsupported aggregate: {op}")
        result.append(out)
    return result


def _run_arrow_group_by(table: Any, specs: list[tuple[str, str]]) -> Any:
    """Run the native multithread-capable Arrow grouping kernel."""
    return table.group_by("_datapipe_day", use_threads=True).aggregate(specs)


def _aggregate_grouped_arrow(
    records: Sequence[Mapping[str, Any]],
    date_field: str,
    metrics: Mapping[str, str],
    batch_size: int,
) -> list[_DailyArrowSummary] | None:
    """Aggregate safe integer rows with bounded Arrow tables.

    Return ``None`` as soon as any batch cannot be proven safe; the caller then
    computes the complete result with the reference Python implementation.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    if not metrics:
        return None
    metric_items = list(metrics.items())
    if any(
        not isinstance(field, str)
        or operation not in {"sum", "count", "min", "max"}
        for field, operation in metric_items
    ):
        return None
    # Alias user fields so the grouping key and aggregate names never collide.
    day_column = "_datapipe_day"
    value_columns = {
        index: f"_datapipe_value_{index}"
        for index, (_, operation) in enumerate(metric_items)
        if operation != "count"
    }
    aggregation_specs = [(day_column, "count")]
    aggregation_specs.extend(
        (column, metric_items[index][1]) for index, column in value_columns.items()
    )

    results: dict[str, _DailyArrowSummary] = {}
    absolute_sums = {field: 0 for field, op in metric_items if op == "sum"}
    batch: list[dict[str, Any]] = []

    def flush_batch() -> bool:
        if not batch:
            return True
        try:
            source_table = pa.Table.from_pylist(batch)
            source_dates = source_table.column(date_field)
            if not pa.types.is_string(source_dates.type):
                return False
            date_matches = pc.match_substring_regex(
                source_dates, r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
            )
            parsed_dates = pc.strptime(
                source_dates, format="%Y-%m-%d", unit="s", error_is_null=True
            )
            if not pc.all(date_matches).as_py() or parsed_dates.null_count:
                return False
            columns: dict[str, Any] = {day_column: source_table.column(date_field)}
            for index, (field, operation) in enumerate(metric_items):
                if operation == "count":
                    continue
                if field in source_table.column_names:
                    columns[value_columns[index]] = source_table.column(field)
                else:
                    columns[value_columns[index]] = pa.nulls(len(batch))
            table = pa.table(columns)
        except (KeyError, pa.ArrowException, OverflowError, TypeError, ValueError):
            return False

        for index, (field, operation) in enumerate(metric_items):
            if operation == "count":
                continue
            column = table.column(value_columns[index])
            if not (pa.types.is_integer(column.type) or pa.types.is_null(column.type)):
                return False
            if operation != "sum":
                # Integer min/max is monotonic under Python's final int-to-float
                # conversion, so no exact-float bound is needed for those ops.
                continue
            extrema = pc.min_max(column).as_py()
            minimum, maximum = extrema["min"], extrema["max"]
            if minimum is None or maximum is None:
                continue
            if abs(minimum) > _MAX_EXACT_INTEGER or abs(maximum) > _MAX_EXACT_INTEGER:
                return False
            bound = max(abs(minimum), abs(maximum)) * len(batch)
            if bound > _MAX_EXACT_INTEGER:
                return False
            # Sum a conservative max-absolute-value bound across batches.
            # This proves every possible absolute partial sum stays exactly
            # representable, without an additional native abs/sum pass.
            absolute_sums[field] += bound
            if absolute_sums[field] > _MAX_EXACT_INTEGER:
                return False

        grouped = _run_arrow_group_by(table, aggregation_specs)
        for partial in grouped.to_pylist():
            day = partial[day_column]
            summary = results.setdefault(day, _DailyArrowSummary(day))
            summary.row_count += partial[f"{day_column}_count"]
            for index, (field, operation) in enumerate(metric_items):
                output_key = f"{field}_{operation}"
                if operation == "count":
                    summary.metrics[output_key] = summary.row_count
                    continue
                value = partial[f"{value_columns[index]}_{operation}"]
                if operation == "sum":
                    total = summary.integer_sums.get(field, 0) + (value or 0)
                    summary.integer_sums[field] = total
                    summary.metrics[output_key] = total
                elif value is not None:
                    previous = summary.metrics.get(output_key)
                    summary.metrics[output_key] = (
                        value
                        if previous is None
                        else min(previous, value)
                        if operation == "min"
                        else max(previous, value)
                    )
        batch.clear()
        return True

    for offset in range(0, len(records), batch_size):
        batch.extend(records[offset : offset + batch_size])  # type: ignore[arg-type]
        # Arrow conversion failures preserve custom Mapping implementations
        # through the exact fallback.
        if not flush_batch():
            return None
    if not flush_batch():
        return None

    # The exact Python implementation returns floats for numeric min/max.
    for summary in results.values():
        for field, operation in metric_items:
            output_key = f"{field}_{operation}"
            if operation in {"min", "max"} and output_key in summary.metrics:
                summary.metrics[output_key] = float(summary.metrics[output_key])
    return [results[day] for day in sorted(results)]


def _aggregate_daily_python(
    records: Iterable[Mapping[str, Any]], date_field: str, metrics: Mapping[str, str]
) -> list[dict[str, Any]]:
    """Run the exact pure-Python baseline, including its date grouping rules."""
    return _aggregate_grouped_python(_group_records(records, date_field), date_field, metrics)


def aggregate_daily(
    records: Iterable[Mapping[str, Any]],
    date_field: str,
    metrics: Mapping[str, str],
    *,
    batch_size: int = _DEFAULT_BATCH_SIZE,
) -> list[dict[str, Any]]:
    """Aggregate by ISO day; use bounded Arrow batches only for proven-safe data.

    The Python row groups retain the established in-memory API semantics. Arrow
    table materialization is separately bounded by ``batch_size``. Unsafe numeric
    inputs use the original exact Fraction implementation unchanged.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    record_list = list(records)
    if len(record_list) < _MIN_NATIVE_RECORDS:
        groups = _group_records(record_list, date_field)
        return _aggregate_grouped_python(groups, date_field, metrics)
    result = _aggregate_grouped_arrow(record_list, date_field, metrics, batch_size)
    if result is None:
        groups = _group_records(record_list, date_field)
        return _aggregate_grouped_python(groups, date_field, metrics)
    return [
        {
            date_field: summary.day,
            **{
                f"{field}_{operation}": (
                    float(summary.metrics.get(f"{field}_{operation}", 0))
                    if operation == "sum"
                    else summary.metrics.get(f"{field}_{operation}")
                )
                for field, operation in metrics.items()
                if operation == "sum"
                or f"{field}_{operation}" in summary.metrics
            },
        }
        for summary in result
    ]
