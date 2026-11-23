import json
import math
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from datapipe.transforms import aggregation


def _records(values: list[Any], dates: list[str] | None = None) -> list[dict[str, Any]]:
    days = dates or ["2026-01-01"] * len(values)
    return [{"date": day, "value": value} for day, value in zip(days, values, strict=True)]


def _normalized(rows: list[dict[str, Any]]) -> str:
    return json.dumps(rows, sort_keys=True, separators=(",", ":"), allow_nan=True)


def test_safe_integer_input_uses_arrow_and_matches_exact_python(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [
        {
            "date": f"2026-01-{index % 28 + 1:02d}",
            "value": index % 97,
            "low": index % 11,
            "high": index % 101,
        }
        for index in range(5_000)
    ]
    calls: list[int] = []
    original = aggregation._run_arrow_group_by

    def spy(table, specs):
        calls.append(table.num_rows)
        return original(table, specs)

    monkeypatch.setattr(aggregation, "_run_arrow_group_by", spy)
    metrics = {"value": "sum", "rows": "count", "low": "min", "high": "max"}
    expected = aggregation._aggregate_daily_python(rows, "date", metrics)
    actual = aggregation.aggregate_daily(rows, "date", metrics, batch_size=1_024)
    assert actual == expected
    assert calls == [1_024, 1_024, 1_024, 1_024, 904]


@pytest.mark.parametrize(
    "values",
    [
        [1.5, 2.0],
        [1e16, 1.0, -1e16],
        [0.1] * 1_001,
        [float("nan"), 1.0],
        [float("inf"), 1.0],
        [float("-inf"), 1.0],
        [-(0.0), 1.0],
        [10**30, 1],
        [True, 1],
    ],
)
def test_unsafe_numbers_use_exact_python_fallback(
    monkeypatch: pytest.MonkeyPatch, values: list[Any]
) -> None:
    repeats = max(1, (5_000 + len(values) - 1) // len(values))
    rows = _records(values * repeats)

    def fail_if_arrow(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("unsafe input must not execute the native Arrow kernel")

    monkeypatch.setattr(aggregation, "_run_arrow_group_by", fail_if_arrow)
    assert _normalized(aggregation.aggregate_daily(rows, "date", {"value": "sum"})) == (
        _normalized(aggregation._aggregate_daily_python(rows, "date", {"value": "sum"}))
    )


def test_exact_fraction_cancellation_has_literal_expected_result() -> None:
    rows = _records([1e16, 1.0, -1e16])

    assert aggregation.aggregate_daily(rows, "date", {"value": "sum"}) == [
        {"date": "2026-01-01", "value_sum": 1.0}
    ]


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ([1e308, 1e308], float("inf")),
        ([-1e308, -1e308], float("-inf")),
    ],
)
def test_exact_fraction_overflow_returns_signed_infinity(
    values: list[float], expected: float
) -> None:
    rows = _records(values)

    assert aggregation.aggregate_daily(rows, "date", {"value": "sum"}) == [
        {"date": "2026-01-01", "value_sum": expected}
    ]


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ([float("inf"), 1.0], float("inf")),
        ([float("-inf"), 1.0], float("-inf")),
        ([float("inf"), float("-inf")], float("nan")),
        ([float("nan"), 1.0], float("nan")),
    ],
)
def test_exact_sum_handles_nan_and_infinities_literally(
    values: list[float], expected: float
) -> None:
    result = aggregation.aggregate_daily(_records(values), "date", {"value": "sum"})[0][
        "value_sum"
    ]
    if math.isnan(expected):
        assert math.isnan(result)
    else:
        assert result == expected


def test_exact_sum_preserves_subnormal_value_and_positive_zero() -> None:
    smallest_subnormal = float.fromhex("0x0.0000000000001p-1022")
    subnormal_result = aggregation.aggregate_daily(
        _records([smallest_subnormal, smallest_subnormal, -smallest_subnormal]),
        "date",
        {"value": "sum"},
    )[0]["value_sum"]
    zero_result = aggregation.aggregate_daily(
        _records([-0.0]), "date", {"value": "sum"}
    )[0]["value_sum"]

    assert subnormal_result == smallest_subnormal
    assert math.copysign(1.0, zero_result) == 1.0


def test_missing_metrics_dates_and_empty_min_max_preserve_semantics() -> None:
    rows = [
        {"date": "2026-01-01", "value": 1},
        {"date": "2026-01-01"},
        {"date": "bad", "value": 2},
        {"value": 3},
    ] * 1_250
    metrics = {"value": "sum", "missing": "count", "other": "min", "max_value": "max"}
    actual = aggregation.aggregate_daily(rows, "date", metrics, batch_size=511)
    expected = aggregation._aggregate_daily_python(rows, "date", metrics)
    assert actual == expected
    assert actual == [{"date": "2026-01-01", "value_sum": 1_250.0, "missing_count": 2_500}]


@pytest.mark.parametrize(
    "rows",
    [
        [{"value": index} for index in range(5_000)],
        [{"date": "not-a-date", "value": index} for index in range(5_000)],
    ],
)
def test_all_missing_or_invalid_dates_keep_empty_result(rows: list[dict[str, Any]]) -> None:
    assert aggregation.aggregate_daily(rows, "date", {"value": "sum"}) == []


def test_mixed_safe_and_unsafe_groups_fall_back_without_changing_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [
        {"date": f"2026-01-{index % 2 + 1:02d}", "value": index % 100}
        for index in range(5_000)
    ]
    rows[-1]["value"] = 0.25

    def fail_if_arrow(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("one unsafe group must route the complete call to Python")

    monkeypatch.setattr(aggregation, "_run_arrow_group_by", fail_if_arrow)
    metrics = {"value": "sum"}
    assert aggregation.aggregate_daily(rows, "date", metrics) == aggregation._aggregate_daily_python(
        rows, "date", metrics
    )


@pytest.mark.parametrize("batch_size", [1, 7, 31, 128])
def test_arrow_batch_boundaries_preserve_order_and_exact_results(
    batch_size: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(aggregation, "_MIN_NATIVE_RECORDS", 1)
    rows = [
        {"date": f"2026-02-{index % 13 + 1:02d}", "value": index % 31, "low": index % 7}
        for index in range(60)
    ]
    metrics = {"value": "sum", "rows": "count", "low": "min"}
    actual = aggregation.aggregate_daily(rows, "date", metrics, batch_size=batch_size)
    expected = aggregation._aggregate_daily_python(rows, "date", metrics)
    assert actual == expected
    assert [row["date"] for row in actual] == sorted(row["date"] for row in actual)


@given(st.lists(st.tuples(st.integers(-100, 100), st.integers(1, 3)), max_size=60))
@settings(max_examples=25)
def test_safe_integer_aggregation_matches_python_for_generated_batches(
    values: list[tuple[int, int]],
) -> None:
    original_minimum = aggregation._MIN_NATIVE_RECORDS
    aggregation._MIN_NATIVE_RECORDS = 1
    try:
        rows = [
            {"date": f"2026-03-{day:02d}", "value": value}
            for value, day in values
        ]
        metrics = {"value": "sum", "rows": "count", "value_min": "min"}
        assert aggregation.aggregate_daily(rows, "date", metrics, batch_size=32) == (
            aggregation._aggregate_daily_python(rows, "date", metrics)
        )
    finally:
        aggregation._MIN_NATIVE_RECORDS = original_minimum


def test_empty_records_and_empty_metrics_keep_reference_behavior() -> None:
    assert aggregation.aggregate_daily([], "date", {"value": "unsupported"}) == []
    rows = [{"date": "2026-01-02"}, {"date": "2026-01-01"}]
    assert aggregation.aggregate_daily(rows, "date", {}) == [
        {"date": "2026-01-01"},
        {"date": "2026-01-02"},
    ]


def test_non_positive_batch_size_is_rejected() -> None:
    with pytest.raises(ValueError, match="batch_size must be positive"):
        aggregation.aggregate_daily([], "date", {}, batch_size=0)
