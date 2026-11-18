"""Stable first-seen deduplication."""

from collections.abc import Iterable, Mapping
from typing import Any


def deduplicate(records: Iterable[Mapping[str, Any]], key: str) -> list[Mapping[str, Any]]:
    seen: set[Any] = set()
    result = []
    for row in records:
        if key not in row:
            result.append(row)
            continue
        value = row[key]
        try:
            duplicate = value in seen
        except TypeError as exc:
            raise ValueError(f"deduplication key {key!r} must be hashable") from exc
        if not duplicate:
            seen.add(value)
            result.append(row)
    return result
