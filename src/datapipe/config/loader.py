"""TOML/YAML configuration loading."""

import tomllib
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from datapipe.config.schema import PipelineConfig
from datapipe.core.exceptions import ConfigurationError


def load_config(path: Path) -> PipelineConfig:
    try:
        raw: Any
        with path.open("rb") as stream:
            raw = (
                yaml.safe_load(stream)
                if path.suffix.lower() in {".yaml", ".yml"}
                else tomllib.load(stream)
            )
        return PipelineConfig.model_validate(raw)
    except (OSError, tomllib.TOMLDecodeError, yaml.YAMLError, ValidationError) as exc:
        raise ConfigurationError(f"{path}: {exc}") from exc
