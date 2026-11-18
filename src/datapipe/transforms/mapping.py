"""Field renaming and projection."""

from collections.abc import Mapping
from typing import Any


def map_record(record: Mapping[str, Any], fields: Mapping[str, str]) -> dict[str, Any]:
    return {target: record[source] for source, target in fields.items() if source in record} | {
        k: v for k, v in record.items() if k not in fields
    }
