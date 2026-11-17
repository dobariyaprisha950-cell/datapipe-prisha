"""Example independently installable source plugin."""
from collections.abc import AsyncIterator
class SampleSource:
    def __init__(self, name:str="sample")->None:self.name=name
    async def read(self)->AsyncIterator[dict[str,str]]:
        yield {"id":"plugin-1","date":"2026-01-01","kind":"example"}
