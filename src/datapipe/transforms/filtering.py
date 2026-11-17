"""Safe declarative field filtering."""
from collections.abc import Iterable, Mapping
from typing import Any
OPS = {"eq": lambda a,b:a==b, "ne":lambda a,b:a!=b, "gt":lambda a,b:a>b, "ge":lambda a,b:a>=b, "lt":lambda a,b:a<b, "le":lambda a,b:a<=b, "in":lambda a,b:a in b, "contains":lambda a,b:b in a}
def filter_records(records: Iterable[Mapping[str, Any]], field: str, op: str, value: Any) -> list[Mapping[str, Any]]:
    if op not in OPS: raise ValueError(f"unsupported filter operator: {op}")
    try: return [row for row in records if field in row and OPS[op](row[field], value)]
    except TypeError as exc: raise ValueError(f"incompatible values for {field} {op}") from exc
