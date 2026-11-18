from pathlib import Path

import pytest
from pydantic import ValidationError

from datapipe.config.loader import load_config
from datapipe.config.schema import TransformConfig


@pytest.mark.parametrize(
    "transform",
    [
        {"type": "deduplication", "settings": {}},
        {"type": "filtering", "settings": {"field": "v", "op": "approx", "value": 2}},
        {"type": "aggregation", "settings": {"metrics": {"v": "median"}}},
        {"type": "cpu_fingerprint", "settings": {"field": "v", "execution": "eval"}},
    ],
)
def test_transform_strategy_configuration_is_validated(transform):
    with pytest.raises(ValidationError):
        TransformConfig.model_validate(transform)


def test_load_example():
    assert load_config(Path("pipeline.toml")).name == "sample-sales"
