"""Atomic summary report writer."""
import json,os,tempfile
from pathlib import Path
from typing import Any
def write_report(path:Path,report:dict[str,Any])->None:
    path.parent.mkdir(parents=True,exist_ok=True);fd,name=tempfile.mkstemp(dir=path.parent,prefix=".report-",text=True)
    try:
        with os.fdopen(fd,"w",encoding="utf-8") as f:json.dump(report,f,indent=2,default=str);f.flush();os.fsync(f.fileno())
        os.replace(name,path)
    finally:
        if os.path.exists(name):os.unlink(name)
