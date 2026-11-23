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

### Verify the published TestPyPI release in a fresh environment

The `0.1.0` TestPyPI release was smoke-tested from a new virtual environment on Windows with Python 3.14.7. To ensure the application wheel comes from TestPyPI while dependencies come from the trusted production index, install runtime dependencies from PyPI first and then install the release wheel without dependency resolution:

```powershell
py -3.12 -m venv .venv-testpypi
$python = ".\.venv-testpypi\Scripts\python.exe"
$cli = ".\.venv-testpypi\Scripts\datapipe.exe"
& $python -m pip install --upgrade pip
& $python -m pip install --index-url https://pypi.org/simple/ 'aiohttp>=3.11,<4' 'opentelemetry-api>=1.29,<2' 'pyarrow>=18,<24' 'pydantic>=2.10,<3' 'PyYAML>=6,<7' 'typer>=0.15,<1'
& $python -m pip install --no-deps --index-url https://test.pypi.org/simple/ datapipe-prisha==0.1.0
& $python -c "import datapipe, importlib.metadata as m; print(m.version('datapipe-prisha')); print(datapipe.__file__)"
& $cli --version
& $cli --help
& $cli validate-config examples/demo-pipeline.yaml
& $cli list-plugins
& $cli run examples/demo-pipeline.yaml --json
```

The pip output should show `datapipe_prisha-0.1.0` downloaded from `test-files.pythonhosted.org`; the import path should point into `.venv-testpypi\Lib\site-packages\datapipe`, not `src`. The included sample uses repository sample data and writes under `work/demo-output`. For an isolated run, copy `examples/demo-pipeline.yaml` and `examples/data/` to a temporary directory, change the configured output and checkpoint paths to that directory, then run the copied config while using the same installed CLI. The checked sample run fetched 7 records from two sources, accepted 4, dead-lettered 3, removed 1 duplicate, and wrote 3 partitioned records plus `summary.json`.

To verify the separately installable example plugin after installing the DATAPIPE release, install `examples/plugin` into that same environment (`& $python -m pip install .\examples\plugin`), then run `& $cli list-plugins`. The plugin is a separate distribution and is not bundled into `datapipe-prisha`.

## Configuration

See `pipeline.toml` and `pipeline.yaml`. Sources have `name` and `type` (`http`, `csv`, or `jsonl`), plus source-specific options. HTTP auth references an environment variable via `auth_env`; token values are never part of config. Transforms include `mapping` (`settings.fields`), `filtering` (`field`, `op`, `value`), `deduplication` (`key`), `aggregation` (`date_field`, `metrics`), and `cpu_fingerprint` (`field`, `rounds`, optional `execution`, `workers`, and `expected_records`). Sinks are JSONL or Parquet and partition by `date` by default. YAML uses safe loading.

## Plugins

Install a package with a `datapipe.sources` entry point, e.g. `[project.entry-points."datapipe.sources"]` with `weather = "package:WeatherSource"`; the class accepts keyword settings and provides `name` and async `read()`. Configure `type = "weather"` with plugin constructor arguments under `settings`. For resume support, accept a `cursor` constructor keyword and expose a watermark that advances as records are yielded. Plugins without cursor support still run but restart from their own default position. `datapipe list-plugins` discovers installed entry points. See `examples/plugin`.

## Operation and security

Records cross a Pydantic validation boundary; invalid values, malformed CSV rows, malformed JSONL lines, malformed HTTP JSON, and non-object HTTP items are written to the central `dead-letter.jsonl` with source, input, timestamp, and error details. Outputs are written through same-filesystem staging and atomic replacement. Partition labels reject path characters. HTTP uses one shared client and bounded connection pool.

Parquet preserves nested list/object values as JSON-encoded Arrow JSON columns; scalar columns retain their native Arrow types. Parquet outputs are partitioned by the configured date field and committed in batches.

### Checkpoints and output recovery

Each non-dry-run persists source cursors in the output manifest; when a separate checkpoint path is configured, that file is reconciled too. Every sink batch is staged below `<output>/.datapipe/staging`, described in the atomic `manifest.json` journal, promoted to its partition paths, and marked committed before source cursors advance. HTTP cursors include page and item offset; CSV and JSONL use data-row and line offsets. Existing version-one cursor checkpoints remain readable. A prepared batch is completed during the next run before source readers resume. If output promotion finished but checkpoint persistence did not, recovery reconciles the checkpoint from the committed manifest. If failure happened before the manifest was prepared, only staged files are discarded and that input is replayed.

This is at-least-once recovery, not exactly-once delivery. A batch is journaled as a transaction, but files in a multi-partition batch are promoted individually; a reader that scans partition directories without consulting the manifest may see part of a prepared batch during recovery. The top-level summary is written atomically after the run finishes; `.datapipe/manifest.json` marks the run completed only after that summary commit. A power loss may still lose directory-entry durability on filesystems that do not honor directory fsync. DATAPIPE serializes cooperating writers with an OS advisory lock at `<output>/.datapipe/writer.lock` and times out after 30 seconds. The lock file persists; OS lock ownership is released after process termination. Advisory locking may vary on network filesystems and does not coordinate with programs that ignore the lock.

No-transform, mapping, filtering, fingerprinting, deduplication, and aggregation pipelines use bounded queue and sink batches. Deduplication and aggregation keep indexed working state in a temporary SQLite database below `.datapipe`; committed state deltas are journaled in the same batch transaction as output and source cursors, and the working database is reconstructed on resume. Deduplication preserves first-seen records. Aggregation stores group state on disk and emits immutable snapshots for changed dates; a later snapshot supersedes an earlier one for that date. Aggregation batches of at least 1,000 valid rows can use a bounded PyArrow group-by path for canonical ISO dates and safe integer metrics; exact integer subtotals merge into the journaled Fraction-unit state. Unsafe values use the existing per-record exact accumulator. Arrow table batches are capped at 50,000 rows by default; this reduces SQLite updates but does not remove the disk state or output transaction boundaries. This bounds memory by queue, batch, and current group rather than input record count, while disk use grows with unique dedupe keys and aggregate groups. Fingerprinting uses logarithmic work in the configured rounds. `execution = "auto"` selects a strategy from the estimate and currently favors sequential work on the measured host; thread and process modes are available explicitly. Temporary working databases are cleaned after success or failure and stale copies are removed under the output lock at startup. SIGINT and SIGTERM are translated into cancellation and exit codes where Python can install catchable handlers. Windows supports Ctrl+C/SIGINT handling in an attached console; real POSIX signal subprocess tests are included in CI but skipped locally on Windows.

Stateful checkpoints written before transform-state journaling was added cannot be migrated reliably. DATAPIPE detects a committed legacy run with dedupe or aggregation and stops with an actionable error rather than silently resuming with incomplete state. Back up its output and start a new output directory for that migration.

HTTP credentials are read from environment variables. Logs are valid JSON with run, source, and stage context, and the redaction filter removes credential values after message formatting. OpenTelemetry spans cover pipeline runs, source reads, record validation/transforms, and sink commits. The project depends on the OpenTelemetry API; configure an SDK and exporter in the host application to export spans. External secret-manager integration is not bundled.

## GitHub and TestPyPI release setup

Release workflow setup and manual repository steps are documented in [RELEASE.md](RELEASE.md). The workflow validates the project on Ubuntu, runs the real POSIX signal tests, builds and checks both distributions, smoke-installs the wheel, and publishes only to TestPyPI using OIDC. It stores no PyPI token. This checkout has no Git remote; no repository creation or package upload has been performed.

## Development

Run `uv lock` then `uv sync --locked --group dev`, followed by `uv run pytest`, `uv run pytest --cov=datapipe --cov-branch`, `uv run ruff check .`, `uv run mypy src/`, `uv run pip-audit`, and `uv build`. Benchmark the isolated transform with `uv run python benchmarks/benchmark.py`; benchmark full pipeline runs with `uv run python benchmarks/benchmark_pipeline.py`; profile a full pipeline with `uv run python benchmarks/profile_pipeline.py`. Results must be measured on your own host; no fabricated measurements are included. See `ARCHITECTURE.md`, `BENCHMARKS.md`, and `DEMO.md`.
