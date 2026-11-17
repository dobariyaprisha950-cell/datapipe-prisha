"""CPU transform using a process pool for deterministic numeric normalization."""
from concurrent.futures import ProcessPoolExecutor
from collections.abc import Iterable,Mapping
from typing import Any
def _normalize(chunk:list[dict[str,Any]],field:str)->list[dict[str,Any]]:
    values=[float(r[field]) for r in chunk if isinstance(r.get(field),(int,float))]
    if not values:return chunk
    mean=sum(values)/len(values); variance=sum((v-mean)**2 for v in values)/len(values); scale=variance**0.5 or 1.0
    return [{**row,field:(float(row[field])-mean)/scale if isinstance(row.get(field),(int,float)) else row.get(field)} for row in chunk]
def normalize(records:Iterable[Mapping[str,Any]],field:str)->list[dict[str,Any]]:
    return _normalize([dict(r) for r in records],field)
def normalize_parallel(records:Iterable[Mapping[str,Any]],field:str,workers:int=2)->list[dict[str,Any]]:
    items=[dict(r) for r in records]
    if len(items)<2 or workers<2:return _normalize(items,field)
    # Global mean/variance keeps baseline and parallel output identical.
    values=[float(r[field]) for r in items if isinstance(r.get(field),(int,float))]
    if not values:return items
    mean=sum(values)/len(values); variance=sum((v-mean)**2 for v in values)/len(values); scale=variance**0.5 or 1.0
    chunk_size=(len(items)+workers-1)//workers
    chunks=[items[i:i+chunk_size] for i in range(0,len(items),chunk_size)]
    with ProcessPoolExecutor(max_workers=workers) as pool: parts=list(pool.map(_normalize_global,[(chunk,field,mean,scale) for chunk in chunks]))
    return [row for part in parts for row in part]
def _normalize_global(task:tuple[list[dict[str,Any]],str,float,float])->list[dict[str,Any]]:
    chunk,field,mean,scale=task
    return [{**r,field:(float(r[field])-mean)/scale if isinstance(r.get(field),(int,float)) else r.get(field)} for r in chunk]
