import pytest
from hypothesis import given
from hypothesis import strategies as st

from datapipe.transforms.aggregation import aggregate_daily
from datapipe.transforms.deduplication import deduplicate
from datapipe.transforms.filtering import filter_records
from datapipe.transforms.mapping import map_record
from datapipe.transforms.parallel import (
    FingerprintExecutor,
    _fingerprint,
    _fingerprint_fast,
    choose_fingerprint_execution,
    fingerprint_records,
    fingerprint_records_parallel,
)


@given(st.sampled_from([-999, 0, 1, 2**31, 2**40]), st.sampled_from([1, 2, 40, 1000, 5000]))
def test_fast_fingerprint_is_exactly_compatible(value, rounds):
    assert _fingerprint_fast(value, rounds) == _fingerprint(value, rounds)


def test_mapping():
    assert map_record({"a": 1, "b": 2}, {"a": "x"}) == {"x": 1, "b": 2}


def test_filtering():
    assert len(filter_records([{"x": 1}, {"x": 2}], "x", "gt", 1)) == 1


def test_dedup_first_seen():
    assert deduplicate([{"id": 1, "v": "first"}, {"id": 1, "v": "last"}], "id") == [
        {"id": 1, "v": "first"}
    ]


def test_aggregation():
    assert aggregate_daily(
        [{"date": "2026-01-01", "x": 2}, {"date": "2026-01-01", "x": 3}], "date", {"x": "sum"}
    ) == [{"date": "2026-01-01", "x_sum": 5.0}]


def test_parallel_cpu_fingerprint_matches_python_baseline():
    records = [{"id": index, "value": index * 3} for index in range(24)]
    pure = fingerprint_records(records, "value", "fingerprint", rounds=40)
    parallel = fingerprint_records_parallel(records, "value", "fingerprint", rounds=40, workers=3)
    assert parallel == pure


@pytest.mark.parametrize("execution", ["sequential", "thread", "process"])
def test_fingerprint_execution_override_is_deterministic(execution):
    records = [{"id": index, "value": index * 17} for index in range(16)]
    expected = fingerprint_records(records, "value", "fp", rounds=5000)
    actual = fingerprint_records_parallel(
        records, "value", "fp", rounds=5000, workers=2, execution=execution
    )
    assert actual == expected


def test_automatic_fingerprint_strategy_avoids_measured_dispatch_overhead():
    assert choose_fingerprint_execution(20_000, 5_000, workers=4) == "sequential"


def test_streaming_fingerprint_executor_closes_and_matches_baseline():
    executor = FingerprintExecutor("thread", workers=2)
    try:
        assert executor.apply(123, 5000) == _fingerprint(123, 5000)
    finally:
        executor.close()


@given(st.lists(st.integers(), max_size=100))
def test_dedupe_idempotent(values):
    rows = [{"id": v} for v in values]
    once = deduplicate(rows, "id")
    assert deduplicate(once, "id") == once
    assert len(once) <= len(rows)


@given(st.lists(st.integers(), max_size=100))
def test_filter_never_increases(values):
    assert len(filter_records([{"x": v} for v in values], "x", "gt", 0)) <= len(values)
