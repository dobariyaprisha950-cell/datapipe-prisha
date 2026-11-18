"""CPU transform using a process pool for deterministic numeric normalization."""

from collections.abc import Iterable, Mapping
from concurrent.futures import ProcessPoolExecutor
from functools import lru_cache
from typing import Any


def _normalize(chunk: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    values = [float(r[field]) for r in chunk if isinstance(r.get(field), (int, float))]
    if not values:
        return chunk
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    scale = variance**0.5 or 1.0
    return [
        {
            **row,
            field: (float(row[field]) - mean) / scale
            if isinstance(row.get(field), (int, float))
            else row.get(field),
        }
        for row in chunk
    ]


def normalize(records: Iterable[Mapping[str, Any]], field: str) -> list[dict[str, Any]]:
    return _normalize([dict(r) for r in records], field)


def normalize_parallel(
    records: Iterable[Mapping[str, Any]], field: str, workers: int = 2
) -> list[dict[str, Any]]:
    items = [dict(r) for r in records]
    if len(items) < 2 or workers < 2:
        return _normalize(items, field)
    # Global mean/variance keeps baseline and parallel output identical.
    values = [float(r[field]) for r in items if isinstance(r.get(field), (int, float))]
    if not values:
        return items
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    scale = variance**0.5 or 1.0
    chunk_size = (len(items) + workers - 1) // workers
    chunks = [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]
    with ProcessPoolExecutor(max_workers=workers) as pool:
        parts = list(pool.map(_normalize_global, [(chunk, field, mean, scale) for chunk in chunks]))
    return [row for part in parts for row in part]


def _normalize_global(task: tuple[list[dict[str, Any]], str, float, float]) -> list[dict[str, Any]]:
    chunk, field, mean, scale = task
    return [
        {
            **r,
            field: (float(r[field]) - mean) / scale
            if isinstance(r.get(field), (int, float))
            else r.get(field),
        }
        for r in chunk
    ]


def _fingerprint_chunk(task: tuple[list[int], int]) -> list[int]:
    values, rounds = task
    return [_fingerprint_fast(value, rounds) for value in values]


def _fingerprint(value: int, rounds: int) -> int:
    state = value & 0xFFFFFFFF
    for counter in range(rounds):
        state = (state * 1_664_525 + 1_013_904_223 + counter) & 0xFFFFFFFF
    return state


def _compose(
    first: tuple[int, int, int, int], second: tuple[int, int, int, int]
) -> tuple[int, int, int, int]:
    """Compose affine counter segments modulo 2**32: second(first(state))."""
    a, c, d, length = first
    e, f, g, second_length = second
    mask = 0xFFFFFFFF
    return (
        (e * a) & mask,
        (e * c + f + g * length) & mask,
        (e * d + g) & mask,
        length + second_length,
    )


@lru_cache(maxsize=128)
def _fingerprint_coefficients(rounds: int) -> tuple[int, int]:
    """Return exact affine coefficients for the legacy iterative fingerprint."""
    if rounds < 1:
        raise ValueError("rounds must be positive")
    result = (1, 0, 0, 0)
    segment = (1_664_525, 1_013_904_223, 1, 1)
    remaining = rounds
    while remaining:
        if remaining & 1:
            result = _compose(result, segment)
        segment = _compose(segment, segment)
        remaining >>= 1
    return result[0], result[1]


def _fingerprint_fast(value: int, rounds: int) -> int:
    multiplier, offset = _fingerprint_coefficients(rounds)
    return (multiplier * (value & 0xFFFFFFFF) + offset) & 0xFFFFFFFF


def fingerprint_records(
    records: Iterable[Mapping[str, Any]], field: str, output_field: str, rounds: int = 1000
) -> list[dict[str, Any]]:
    """Pure-Python CPU-heavy deterministic numeric fingerprint transform."""
    if rounds < 1:
        raise ValueError("rounds must be positive")
    result = []
    for source in records:
        row = dict(source)
        value = row.get(field)
        if isinstance(value, (int, float)):
            row[output_field] = _fingerprint_fast(int(value), rounds)
        result.append(row)
    return result


def fingerprint_records_parallel(
    records: Iterable[Mapping[str, Any]],
    field: str,
    output_field: str,
    rounds: int = 1000,
    workers: int = 2,
) -> list[dict[str, Any]]:
    """Parallel equivalent using process-safe integer chunks and stable ordering."""
    if rounds < 1:
        raise ValueError("rounds must be positive")
    items = [dict(record) for record in records]
    positions_and_values = [
        (index, int(row[field]))
        for index, row in enumerate(items)
        if isinstance(row.get(field), (int, float))
    ]
    if len(positions_and_values) < 2 or workers < 2:
        return fingerprint_records(items, field, output_field, rounds)
    chunk_size = (len(positions_and_values) + workers - 1) // workers
    chunks = [
        positions_and_values[index : index + chunk_size]
        for index in range(0, len(positions_and_values), chunk_size)
    ]
    tasks = [([value for _, value in chunk], rounds) for chunk in chunks]
    with ProcessPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(_fingerprint_chunk, tasks))
    for chunk, fingerprints in zip(chunks, results, strict=True):
        for (index, _), fingerprint in zip(chunk, fingerprints, strict=True):
            items[index][output_field] = fingerprint
    return items
