"""Deterministic per-day numeric aggregation."""
from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import Any
def aggregate_daily(records: Iterable[Mapping[str, Any]], date_field: str, metrics: Mapping[str, str]) -> list[dict[str, Any]]:
    groups: dict[str,list[Mapping[str,Any]]] = defaultdict(list)
    for row in records:
        value = row.get(date_field)
        if value is None: continue
        day = str(value)[:10]
        try: from datetime import date; date.fromisoformat(day)
        except ValueError: continue
        groups[day].append(row)
    result=[]
    for day, rows in sorted(groups.items()):
        out: dict[str,Any] = {date_field:day}
        for field, op in metrics.items():
            values=[float(r[field]) for r in rows if field in r and isinstance(r[field],(int,float))]
            if op == "count": out[f"{field}_{op}"]=len(rows)
            elif op == "sum": out[f"{field}_{op}"]=sum(values)
            elif op == "min" and values: out[f"{field}_{op}"]=min(values)
            elif op == "max" and values: out[f"{field}_{op}"]=max(values)
            else:
                if op not in {"count","sum","min","max"}: raise ValueError(f"unsupported aggregate: {op}")
        result.append(out)
    return result
