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

Install a package with a `datapipe.sources` entry point, e.g. `[project.entry-points."datapipe.sources"]` with `weather = "package:WeatherSource"`; the class accepts keyword settings and provides `name` and async `read()`. Configure `type = "weather"` with plugin constructor arguments under `settings`. For resume support, accept a `cursor` constructor keyword and expose a watermark that advances as records are yielded. Plugins without cursor support still run but restart from their own default position. `datapipe list-plugins` discovers installed entry points. See `examples/plugin`.

## Operation and security

Records cross a Pydantic validation boundary; invalid data and malformed JSONL/non-object HTTP rows are written to `dead-letter.jsonl`. Outputs are written through same-filesystem staging and atomic replacement. Partition labels reject path characters. HTTP uses one shared client and bounded connection pool.

### Checkpoints and output recovery

Each non-dry-run persists source cursors in the output manifest; when a separate checkpoint path is configured, that file is reconciled too. Every sink batch is staged below `<output>/.datapipe/staging`, described in the atomic `manifest.json` journal, promoted to its partition paths, and marked committed before source cursors advance. HTTP cursors include page and item offset; CSV and JSONL use data-row and line offsets. Existing version-one cursor checkpoints remain readable. A prepared batch is completed during the next run before source readers resume. If output promotion finished but checkpoint persistence did not, recovery reconciles the checkpoint from the committed manifest. If failure happened before the manifest was prepared, only staged files are discarded and that input is replayed.

This is at-least-once recovery, not exactly-once delivery. A batch is journaled as a transaction, but files in a multi-partition batch are promoted individually; a reader that scans partition directories without consulting the manifest may see part of a prepared batch during recovery. The top-level summary is written atomically after the run finishes; `.datapipe/manifest.json` marks the run completed only after that summary commit. A power loss may still lose directory-entry durability on filesystems that do not honor directory fsync. DATAPIPE serializes cooperating writers with an OS advisory lock at `<output>/.datapipe/writer.lock` and times out after 30 seconds. The lock file persists; OS lock ownership is released after process termination. Advisory locking may vary on network filesystems and does not coordinate with programs that ignore the lock.

With no transforms, or only mapping, filtering, and CPU fingerprinting, records flow through bounded queues and sink batches. These record-local transforms need only current-record state. Fingerprinting is mathematically equivalent to the legacy per-round recurrence and uses logarithmic work in the rounds setting. Deduplication and aggregation still materialize the valid dataset, so memory grows with input size; bounded-memory behavior is not claimed for those transforms. Temporary staged files are removed after commit, failure recovery, or the next startup. SIGINT and SIGTERM are translated into cancellation and exit codes on platforms where Python can install catchable handlers. Windows supports Ctrl+C/SIGINT handling in an attached console; sending SIGTERM to a Windows process may terminate it without Python cleanup, so restart recovery remains necessary. Real POSIX signal subprocess tests are included but skipped on Windows and could not be run locally because no Linux distribution is installed.

HTTP credentials are read from environment variables and logs pass through a redaction filter. OpenTelemetry stage spans and external secret-manager integration are not implemented.

## TestPyPI release preparation

The workflow at `.github/workflows/release-testpypi.yml` publishes a GitHub Release to TestPyPI using OIDC after lock, audit, tests, and build checks. It requests `id-token: write` and `contents: read`, and uses the GitHub environment named `testpypi`. It stores no PyPI token.

Before releasing, configure a TestPyPI trusted publisher with the GitHub owner, repository, workflow filename `.github/workflows/release-testpypi.yml`, and environment `testpypi`. Configure the `testpypi` environment in GitHub and apply any required reviewer protection. The distribution metadata is `datapipe-prisha` version `0.1.0`. This workspace currently has no Git remote and no external publisher configuration was verified; repository creation, trusted-publisher setup, and publication remain manual. Do not publish until those settings are in place and a release is authorized.

## Development

Run `uv lock` then `uv sync --locked --group dev`, followed by `uv run pytest`, `uv run pytest --cov=datapipe --cov-branch`, `uv run ruff check .`, `uv run mypy src/`, `uv run pip-audit`, and `uv build`. Benchmark: `uv run python benchmarks/benchmark.py`. Results must be measured on your own host; no fabricated measurements are included. See `ARCHITECTURE.md`, `BENCHMARKS.md`, and `DEMO.md`.
