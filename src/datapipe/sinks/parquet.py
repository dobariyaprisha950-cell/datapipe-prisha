"""Atomic partitioned Parquet output."""
import os,tempfile,re,uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any
import pyarrow as pa
import pyarrow.parquet as pq
class ParquetSink:
    def __init__(self,root:str,partition_by:str="date")->None:self.root=Path(root).resolve();self.partition_by=partition_by
    def write(self,records:list[Mapping[str,Any]])->list[Path]:
        groups:dict[str,list[Mapping[str,Any]]]={}
        for row in records:
            key=str(row.get(self.partition_by,"unknown"))[:10]
            if not re.fullmatch(r"[A-Za-z0-9_-]+",key):raise ValueError("unsafe partition value")
            groups.setdefault(key,[]).append(row)
        output=[]
        for key,rows in groups.items():
            folder=(self.root/f"{self.partition_by}={key}").resolve()
            if self.root not in folder.parents:raise ValueError("output path escaped configured root")
            folder.mkdir(parents=True,exist_ok=True);target=folder/f"part-{uuid.uuid4().hex}.parquet";fd,name=tempfile.mkstemp(dir=folder,prefix=".datapipe-");os.close(fd)
            try:pq.write_table(pa.Table.from_pylist([dict(r) for r in rows]),name);os.replace(name,target)
            finally:
                if os.path.exists(name):os.unlink(name)
            output.append(target)
        return output
