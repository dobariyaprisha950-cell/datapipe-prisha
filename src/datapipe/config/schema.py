"""Pydantic configuration models."""
from typing import Any, Literal
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
class SourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    type: str
    path: str | None = None
    url: str | None = None
    delimiter: str = ","
    encoding: str = "utf-8"
    auth_env: str | None = None
    timeout: float = Field(default=20, gt=0)
    page_param: str | None = None
    start_page: int = 1
    items_key: str | None = None
    retries: int = Field(default=3, ge=0, le=10)
    settings: dict[str, Any] = Field(default_factory=dict)
    cursor: str | None = None

    @field_validator("delimiter")
    @classmethod
    def one_character_delimiter(cls, value: str) -> str:
        if len(value) != 1:
            raise ValueError("delimiter must be exactly one character")
        return value

    @model_validator(mode="after")
    def require_builtin_location(self) -> "SourceConfig":
        if self.type in {"csv", "jsonl"} and not self.path:
            raise ValueError(f"{self.type} source requires path")
        if self.type == "http" and not self.url:
            raise ValueError("http source requires url")
        if self.start_page < 1:
            raise ValueError("start_page must be >= 1")
        return self
class TransformConfig(BaseModel):
    type: Literal["mapping", "filtering", "deduplication", "aggregation"]
    settings: dict[str, Any] = Field(default_factory=dict)
class SinkConfig(BaseModel):
    type: Literal["jsonl", "parquet"] = "jsonl"
    output_dir: str
    partition_by: str = "date"
    batch_size: int = Field(default=1000, gt=0)
class PipelineConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    sources: list[SourceConfig] = Field(min_length=1)
    transforms: list[TransformConfig] = Field(default_factory=list)
    sink: SinkConfig
    concurrency: int = Field(default=4, gt=0, le=128)
    queue_size: int = Field(default=1000, gt=0)
    checkpoint: str | None = None
    log_level: str = "INFO"

    @model_validator(mode="after")
    def unique_source_names(self) -> "PipelineConfig":
        names = [source.name for source in self.sources]
        if len(names) != len(set(names)):
            raise ValueError("source names must be unique")
        return self
