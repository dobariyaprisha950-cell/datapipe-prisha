# datapipe

An asynchronous Python CLI for ingesting HTTP JSON, CSV and JSONL, validating records, transforming data, and writing date-partitioned outputs.

## Requirements and install

Python 3.12+. Install with `uv sync --group dev`; run the CLI with `uv run datapipe --help`. Package name: `datapipe-prisha`; import and command: `datapipe`.

On Windows, install the Python install manager and Python 3.12, plus uv, from PowerShell:

```powershell
winget install 9NQ7512CXL7T -e --accept-package-agreements --disable-interactivity
py install 3.12
winget install --id=astral-sh.uv -e
# Open a new terminal, then check:
py -3.12 --version
uv --version
```

Then, from this repository, run `uv lock` followed by `uv sync --locked --group dev`. If the `py` command still resolves to a legacy launcher, remove/disable that launcher and reopen the terminal.

## Quick start

From the repository root run `uv run datapipe validate-config pipeline.toml`, then `uv run datapipe run pipeline.toml`. The included example reads CSV and JSONL, maps and deduplicates, then writes date partitions, dead letters and a summary under `examples/output/`. Use `--dry-run`, `--json`, or `--fail-fast`. `--version`, `validate-config`, and `list-plugins` are available.

## Configuration

See `pipeline.toml` and `pipeline.yaml`. Sources have `name` and `type` (`http`, `csv`, or `jsonl`), plus source-specific options. HTTP auth references an environment variable via `auth_env`; token values are never part of config. Transforms are `mapping` (`settings.fields`), `filtering` (`field`, `op`, `value`), `deduplication` (`key`), or `aggregation` (`date_field`, `metrics`). Sinks are JSONL or Parquet and partition by `date` by default. YAML uses safe loading.

## Plugins

Install a package with a `datapipe.sources` entry point, e.g. `[project.entry-points."datapipe.sources"]` with `weather = "package:WeatherSource"`; the class accepts keyword settings and provides `name` and async `read()`. Configure `type = "weather"` with plugin constructor arguments under `settings`. `datapipe list-plugins` discovers installed entry points. See `examples/plugin`.

## Operation and security

Records cross a Pydantic validation boundary; invalid data and malformed JSONL/non-object HTTP rows are written to `dead-letter.jsonl`. Outputs, dead letters, summaries and checkpoint JSON use temporary files plus same-directory atomic replacement. Partition labels reject path characters. A shared HTTP session and bounded connector are used. Page-number HTTP and line/row-based file cursors are saved after normal source completion and successful sink writes. The engine still retains validated records in memory until transforms and sink writes finish, so memory is not bounded by queue size; cursors are source-level, not per output batch. Multiple partition files are not one atomic transaction: a failure may leave individually complete output files, although the checkpoint does not advance and reruns can create duplicate files. OpenTelemetry spans and external secret managers are not implemented. Configure HTTP credentials with environment variables. Logs pass through a redaction filter.

## Development

Run `uv lock` then `uv sync --locked --group dev`, followed by `uv run pytest`, `uv run pytest --cov=datapipe --cov-branch`, `uv run ruff check .`, `uv run mypy src/`, `uv run pip-audit`, and `uv build`. Benchmark: `uv run python benchmarks/benchmark.py`. Results must be measured on your own host; no fabricated measurements are included. See `ARCHITECTURE.md`, `BENCHMARKS.md`, and `DEMO.md`.
