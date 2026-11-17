"""Atomic JSON checkpoint persistence."""
import json,os,tempfile
from pathlib import Path
from typing import Any
class Checkpoint:
    def __init__(self,path:str|Path)->None:self.path=Path(path)
    def load(self)->dict[str,Any]:
        if not self.path.exists():return {}
        value=json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(value,dict):raise ValueError("checkpoint root must be an object")
        return value
    def save(self,state:dict[str,Any])->None:
        self.path.parent.mkdir(parents=True,exist_ok=True)
        fd,name=tempfile.mkstemp(prefix=".checkpoint-",dir=self.path.parent,text=True)
        try:
            with os.fdopen(fd,"w",encoding="utf-8") as stream:json.dump(state,stream);stream.flush();os.fsync(stream.fileno())
            os.replace(name,self.path)
        finally:
            if os.path.exists(name):os.unlink(name)
