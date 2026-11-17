"""Typed record boundary."""
from datetime import date, datetime
from typing import Any
from pydantic import BaseModel, ConfigDict, Field

class Record(BaseModel):
    model_config = ConfigDict(extra="allow")
    id: str | int | None = None
    date: date | datetime | None = None
    data: dict[str, Any] = Field(default_factory=dict)
