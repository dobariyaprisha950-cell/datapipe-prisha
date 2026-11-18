"""Core source, transform and sink interfaces."""

from collections.abc import AsyncIterator, Iterable, Mapping
from typing import Any, Protocol, TypeVar

T = TypeVar("T")
Record = Mapping[str, Any]


class Source(Protocol):
    name: str

    def read(self) -> AsyncIterator[Record]: ...


class Transform(Protocol[T]):
    def apply(self, records: Iterable[T]) -> list[T]: ...


class Sink(Protocol):
    async def write(self, records: list[Record]) -> None: ...
