# Five-minute demonstration

1. `uv sync --group dev` (installation).
2. `uv run datapipe validate-config pipeline.toml` (configuration).
3. `uv run datapipe run pipeline.toml --json` (CSV plus JSONL ingestion, mapping, deduplication, partitioned output and report).
4. Inspect `examples/output/date=*/part-*.jsonl`, `examples/output/dead-letter.jsonl`, and `examples/output/summary.json`.
5. `uv pip install -e examples/plugin` then `uv run datapipe list-plugins` (plugin discovery). Configure `type = "sample"` with `settings = {}` to run the plugin source.
6. `uv run python benchmarks/benchmark.py --records 200000 --workers 4` (measured performance output).

No demo video is included. The benchmark records measurements for the current machine; do not report a speed target as passed unless the measured output supports it.
