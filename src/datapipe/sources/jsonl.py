"""Streaming JSON-lines source with malformed row isolation."""
import asyncio,json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any,Callable
class JSONLSource:
    def __init__(self,name:str,path:str,on_error:Callable[[int,str],None]|None=None,start_line:int=0)->None:
        self.name,self.path,self.on_error,self.cursor=name,Path(path),on_error,start_line
    async def read(self)->AsyncIterator[dict[str,Any]]:
        stream=self.path.open(encoding="utf-8")
        try:
            for number,line in enumerate(stream,1):
                if number<=self.cursor: continue
                if not line.strip(): continue
                try:
                    value=await asyncio.to_thread(json.loads,line)
                    if not isinstance(value,dict): raise ValueError("line must contain a JSON object")
                    yield value
                except (json.JSONDecodeError,ValueError) as exc:
                    rejected={"__datapipe_error__":{"line":number,"input":line.rstrip("\r\n"),"reason":str(exc)}}
                    if self.on_error: self.on_error(number,str(exc))
                    yield rejected
                self.cursor=number
        finally: stream.close()
