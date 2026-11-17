"""Atomic, date-partitioned JSONL output."""
import json,os,re,tempfile,uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any
class JSONLSink:
    def __init__(self,root:str,partition_by:str="date")->None:self.root=Path(root).resolve();self.partition_by=partition_by
    def write(self,records:list[Mapping[str,Any]])->list[Path]:
        groups:dict[str,list[Mapping[str,Any]]]={}
        for record in records:
            key=str(record.get(self.partition_by,"unknown"))[:10]
            if not re.fullmatch(r"[A-Za-z0-9_-]+",key):raise ValueError("unsafe partition value")
            groups.setdefault(key,[]).append(record)
        paths=[]
        for key,rows in groups.items():
            directory=(self.root/f"{self.partition_by}={key}").resolve()
            if self.root not in directory.parents:raise ValueError("output path escaped configured root")
            directory.mkdir(parents=True,exist_ok=True);target=directory/f"part-{uuid.uuid4().hex}.jsonl"
            fd,name=tempfile.mkstemp(prefix=".datapipe-",dir=directory,text=True)
            try:
                with os.fdopen(fd,"w",encoding="utf-8") as stream:
                    for row in rows:stream.write(json.dumps(dict(row),default=str,separators=(",",":"))+"\n")
                    stream.flush();os.fsync(stream.fileno())
                os.replace(name,target)
            finally:
                if os.path.exists(name):os.unlink(name)
            paths.append(target)
        return paths
