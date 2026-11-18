"""Plugin registry."""

from typing import Any, ClassVar


class SourceRegistry:
    _sources: ClassVar[dict[str, type[Any]]] = {}

    @classmethod
    def register(cls, name: str, source: type[Any]) -> None:
        if name in cls._sources:
            raise ValueError(f"source plugin already registered: {name}")
        cls._sources[name] = source

    @classmethod
    def create(cls, source_type: str, **settings: Any) -> Any:
        try:
            return cls._sources[source_type](**settings)
        except KeyError as exc:
            raise KeyError(f"unknown source type: {source_type}") from exc

    @classmethod
    def names(cls) -> list[str]:
        return sorted(cls._sources)
