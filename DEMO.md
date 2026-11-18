# Five-minute DATAPIPE demonstration

Run these commands from the repository root. Runtime output is placed under ignored `work/` paths.

1. Install the locked development environment: `uv sync --locked --group dev`.
2. Validate the two-source YAML pipeline: `uv run datapipe validate-config examples/demo-pipeline.yaml`.
3. Run it with machine-readable output: `uv run datapipe run examples/demo-pipeline.yaml --json`.
4. Inspect `work/demo-output/date=*/part-*.jsonl`, `work/demo-output/dead-letter.jsonl`, and `work/demo-output/summary.json`. The sample data includes valid rows, an invalid date, an invalid record, and malformed JSONL.
5. Run the same pipeline again. The committed manifest/checkpoint should resume at EOF and report zero newly fetched rows; existing partition files remain.
6. Install the independent example plugin: `uv pip install -e examples/plugin`.
7. Discover it: `uv run datapipe list-plugins --json` should include `sample`.
8. Validate and run the plugin pipeline: `uv run datapipe validate-config examples/plugin/pipeline.toml`, then `uv run datapipe run examples/plugin/pipeline.toml --json`. Inspect `work/plugin-demo-output/summary.json` and the date partition.
9. Run focused automated checks: `uv run pytest tests/test_commit_recovery.py tests/test_sources_and_engine.py -k 'not two_million'`.
10. Run a short reproducible comparison: `uv run python benchmarks/benchmark.py --records 5000 --workers 4 --rounds 1000 --runs 1`. The command prints measured results and normalized output equality. The recorded benchmark in `BENCHMARKS.md` compares sequential, thread, and process execution, confirms exact output equality, and reports the measured speedups below 1x.

No demo video is included. The OS-level SIGINT/SIGTERM subprocess tests run on POSIX and are skipped on Windows because Windows cannot deliver catchable POSIX signals in the same way; Ctrl+C in an attached Windows console is handled by the CLI.
