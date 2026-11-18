"""Plugin registry."""

import inspect
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
            source_class = cls._sources[source_type]
        except KeyError as exc:
            raise KeyError(f"unknown source type: {source_type}") from exc
        if "cursor" in settings:
            try:
                parameters = inspect.signature(source_class).parameters
            except (TypeError, ValueError):
                settings.pop("cursor", None)
            else:
                accepts_kwargs = any(
                    item.kind is inspect.Parameter.VAR_KEYWORD for item in parameters.values()
                )
                cursor_parameter = parameters.get("cursor")
                accepts_cursor = cursor_parameter is not None and cursor_parameter.kind in {
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    inspect.Parameter.KEYWORD_ONLY,
                }
                if not (accepts_cursor or accepts_kwargs):
                    settings.pop("cursor", None)
        return source_class(**settings)

    @classmethod
    def names(cls) -> list[str]:
        return sorted(cls._sources)
