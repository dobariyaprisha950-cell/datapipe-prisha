from pathlib import Path
from datapipe.config.loader import load_config
def test_load_example():assert load_config(Path("pipeline.toml")).name=="sample-sales"
