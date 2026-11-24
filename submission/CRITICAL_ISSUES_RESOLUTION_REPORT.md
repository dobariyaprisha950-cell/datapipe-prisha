# DATAPIPE Critical Issues Resolution Report

Date: 2026-11-24
Project version: `datapipe-prisha` 0.1.0

## Update after authorized commit, push, and Ubuntu CI

**Implementation commit:** `237c3def3502c8658333dd790cb75dc1613cb69a` — `feat: verify source distributions and add project presentation`. It was pushed to the existing `origin/master` branch without force-pushing. The remote `master` reference was independently checked and points to this exact SHA.

- Repository: [dobariyaprisha950-cell/datapipe-prisha](https://github.com/dobariyaprisha950-cell/datapipe-prisha)
- Commit: [237c3def3502c8658333dd790cb75dc1613cb69a](https://github.com/dobariyaprisha950-cell/datapipe-prisha/commit/237c3def3502c8658333dd790cb75dc1613cb69a)
- Ubuntu CI run: [run 45431931687](https://github.com/dobariyaprisha950-cell/datapipe-prisha/actions/runs/45431931687) — completed successfully for Python 3.12 and 3.13.
- Python 3.13 job: [checks (3.13)](https://github.com/dobariyaprisha950-cell/datapipe-prisha/actions/runs/45431931687/job/138579340935) — success.
- Python 3.12 job: [checks (3.12)](https://github.com/dobariyaprisha950-cell/datapipe-prisha/actions/runs/45431931687/job/138579341432) — success.

The full pytest and core branch-coverage steps passed on both Ubuntu jobs. The explicit `POSIX SIGINT and SIGTERM integration tests` step passed on both versions. That parametrized test sends real SIGINT and SIGTERM after a batch is committed, checks checkpoint/staging state, resumes the same pipeline, and asserts a final cursor and exactly 250,000 output records. Both sdist inspection and clean sdist install/sample smoke steps also passed on both versions. The commit is therefore backed by actual Linux signal, checkpoint, and resume test execution—not just workflow configuration.

GitHub's contents API confirmed all five approved presentation files exist at the implementation commit. For example: [PowerPoint](https://github.com/dobariyaprisha950-cell/datapipe-prisha/blob/237c3def3502c8658333dd790cb75dc1613cb69a/presentation/DATAPIPE_5_Minute_Presentation.pptx), [DOCX script](https://github.com/dobariyaprisha950-cell/datapipe-prisha/blob/237c3def3502c8658333dd790cb75dc1613cb69a/presentation/DATAPIPE_5_Minute_Speaking_Script.docx), [TXT script](https://github.com/dobariyaprisha950-cell/datapipe-prisha/blob/237c3def3502c8658333dd790cb75dc1613cb69a/presentation/DATAPIPE_5_Minute_Speaking_Script.txt), [recording guide](https://github.com/dobariyaprisha950-cell/datapipe-prisha/blob/237c3def3502c8658333dd790cb75dc1613cb69a/presentation/DATAPIPE_Demo_Recording_Guide.md), and [validation report](https://github.com/dobariyaprisha950-cell/datapipe-prisha/blob/237c3def3502c8658333dd790cb75dc1613cb69a/presentation/DATAPIPE_Presentation_Validation.md).

The latest `BENCHMARKS.md` evidence remains the measured pooled **3.33x** exact-Python versus PyArrow result for the 50,000-row, 50,000-row-batch in-memory workload, with equal normalized outputs. No benchmark methods or benchmark results were changed for this task. The presentation script's 3.09x figure and its RSS values refer to earlier measured runs; they are historical values, while `BENCHMARKS.md` records the later 3.33x result. The separate `submission/demo-video/` directory was excluded from the commit as requested and remains untracked.

**Current verdict: READY.** GitHub commit/file verification and real Ubuntu SIGINT/SIGTERM checkpoint-resume tests have passed.

## Executive result

**READY — the measured in-memory benchmark target, source-distribution checks, GitHub publication of the approved change set, and real Ubuntu SIGINT/SIGTERM checkpoint-resume tests all passed.** Details and direct GitHub links are in the update above.

## 1. Performance below 3x

**Previous issue:** a recent run reported 2.864x against the 3x assignment target.

**Investigation:** the pure-Python reference spends most of its time converting every float to `Fraction` and adding exact fractions. Warmed cProfile on 50,000 rows attributed 1.158 seconds cumulative to `_aggregate_grouped_python`; `Fraction.from_float` used 0.382 seconds and Fraction addition 0.195 seconds. The Arrow path took 0.120 seconds in `_aggregate_grouped_arrow`; conversion/validation in `flush_batch` took 0.120 seconds inclusive, Arrow compute wrappers 0.064 seconds, and the group-by kernel 0.008 seconds. Profiling numbers are diagnostic and were not used as benchmark timings.

**Change:** the Arrow path no longer copies a batch slice into a second Python list before `Table.from_pylist`. It uses the existing sequence directly for a single batch and one bounded slice per additional batch. The exact Fraction fallback and safety checks are unchanged.

**Paired implementation A/B:** using the 50,000-row workload, 30 alternating calls per version, warm-up, and exact normalized-output checks, the committed pre-change public `aggregate_daily()` median was 0.196845 seconds and the updated median was 0.180306 seconds: 1.092x. The outputs were equal.

**Assignment benchmark:** command, run three times:

```text
.venv\Scripts\python.exe benchmarks\benchmark_aggregation.py --records 50000 --batch-size 50000 --runs 15
```

The benchmark checks exact normalized output equality, alternates timing order, and samples memory only in separate worker processes after timing. Results:

| Run | Exact Python median | PyArrow median | Speed-up | Output equality |
|---:|---:|---:|---:|---|
| 1 | 0.433413 s | 0.137608 s | 3.15x | Equal |
| 2 | 0.669717 s | 0.172688 s | 3.88x | Equal |
| 3 | 0.439245 s | 0.129903 s | 3.38x | Equal |
| Pooled 45 timings | 0.458573 s | 0.137651 s | 3.33x | Equal |

Environment: Python 3.14.7, PyArrow 23.0.1, Windows 10 build 19045, Intel64 Family 6 Model 69 Stepping 1, four logical CPUs, 50,000 records, 50,000-row Arrow batch, 28 dates, sum/count/min/max metrics. Memory was sampled separately in fresh processes: exact Python RSS 49,324,032–49,397,760 bytes and USS 32,145,408–32,219,136 bytes; Arrow RSS 62,214,144–62,697,472 bytes and USS 39,374,848–39,825,408 bytes.

**Acceptance:** PASS for the specified 50,000-row in-memory workload with a 50,000-row bounded Arrow batch. This is not a general pipeline speed-up: the 5,000-row batches and disk-backed end-to-end results documented in `BENCHMARKS.md` remain below 3x. The second benchmark session was slower for both implementations; it is included rather than discarded.

## 2. Source distribution contamination

**Root cause:** the project had no Hatch sdist allowlist, so the default source selection captured local development and presentation material. `.venv-demo` and `.hypothesis` were also absent from `.gitignore`.

**Changes:** `pyproject.toml` now restricts sdist inputs to `src/datapipe`, `README.md`, `LICENSE`, and `pyproject.toml`. `.gitignore` now excludes `.venv-demo`, `.hypothesis`, coverage variants, and root `outputs/`. A standard-library-only archive checker was added at `scripts/verify_sdist.py`; CI and the TestPyPI release validation now check the archive and install/run the built sdist in a clean environment.

**Before evidence:** the prior archive contained 2,557 entries, including 2,356 `.venv-demo` entries, 123 `.hypothesis` entries, and five presentation entries.

**Fresh build:**

```text
work\uv-toolenv-final-audit\Scripts\uv.exe build --out-dir work\critical-build-20261124
```

Both wheel and sdist built successfully. The new sdist was 36,417 bytes and contained 34 entries. `scripts/verify_sdist.py` passed; full archive listing inspection found zero forbidden paths (`.venv-demo`, `.hypothesis`, virtual environments, caches, `work`, `outputs`, or `presentation`). Required package sources, README, license, and metadata were present. The wheel metadata was `Name: datapipe-prisha`, `Version: 0.1.0`.

The old contaminated archive was also passed to the new checker; it was rejected with the forbidden `.hypothesis`, `.venv-demo`, and `presentation` paths listed.

**Clean install and sample:** created `work\critical-sdist-venv` with Python 3.14.7 and installed the new local sdist plus runtime dependencies. Version/import checks passed: version `0.1.0`, import under that environment's `Lib\site-packages\datapipe`, not the local `src` tree. Installed CLI `--version`, `--help`, configuration validation, and a two-source sample run passed. Actual sample summary: 7 fetched, 4 valid, 3 invalid/dead-lettered, 1 duplicate removed, 3 written; output partitions, dead-letter file, manifest, and summary were present.

**Acceptance:** PASS.

## 3. Linux SIGTERM and resume

**Ubuntu Actions evidence:** workflow run [45431931687](https://github.com/dobariyaprisha950-cell/datapipe-prisha/actions/runs/45431931687) completed successfully at implementation commit `237c3def3502c8658333dd790cb75dc1613cb69a` on both `ubuntu-latest` Python 3.12 and Python 3.13.

For both matrix jobs, the complete pytest step, the explicit `POSIX SIGINT and SIGTERM integration tests` step, the core branch-coverage enforcement step, source-distribution verification, and isolated sdist install/sample smoke step all completed with success. The POSIX test parameterizes real SIGINT and SIGTERM subprocess interruption. Each scenario waits for an output batch to be committed, signals the running process, verifies the checkpoint and empty staging directory, resumes the pipeline, then asserts cursor 250,000 and exactly 250,000 records across final partitions. The test thus covers signal delivery, checkpoint persistence, resume, and no record loss/duplication on Ubuntu.

- [Python 3.13 job and step results](https://github.com/dobariyaprisha950-cell/datapipe-prisha/actions/runs/45431931687/job/138579340935)
- [Python 3.12 job and step results](https://github.com/dobariyaprisha950-cell/datapipe-prisha/actions/runs/45431931687/job/138579341432)

**Acceptance:** PASS. This is actual Linux Actions execution; earlier Windows skips are not used as POSIX evidence.

## 4. GitHub presentation files

The approved five deliverables are committed at `237c3def3502c8658333dd790cb75dc1613cb69a` and each was confirmed present by GitHub's contents API:

- [PowerPoint](https://github.com/dobariyaprisha950-cell/datapipe-prisha/blob/237c3def3502c8658333dd790cb75dc1613cb69a/presentation/DATAPIPE_5_Minute_Presentation.pptx)
- [DOCX speaking script](https://github.com/dobariyaprisha950-cell/datapipe-prisha/blob/237c3def3502c8658333dd790cb75dc1613cb69a/presentation/DATAPIPE_5_Minute_Speaking_Script.docx)
- [TXT speaking script](https://github.com/dobariyaprisha950-cell/datapipe-prisha/blob/237c3def3502c8658333dd790cb75dc1613cb69a/presentation/DATAPIPE_5_Minute_Speaking_Script.txt)
- [Demo recording guide](https://github.com/dobariyaprisha950-cell/datapipe-prisha/blob/237c3def3502c8658333dd790cb75dc1613cb69a/presentation/DATAPIPE_Demo_Recording_Guide.md)
- [Presentation validation](https://github.com/dobariyaprisha950-cell/datapipe-prisha/blob/237c3def3502c8658333dd790cb75dc1613cb69a/presentation/DATAPIPE_Presentation_Validation.md)

Before staging, the five files passed package-integrity and text/security inspection. The PowerPoint contains 8 slides and 8 note pages; DOCX/PPTX ZIP integrity passed; no credential patterns or email addresses were found in the scanned content. The presentation's 3.09x and RSS numbers are from earlier actual measurements; the latest verified 3.33x comparison remains in `BENCHMARKS.md` and is not replaced by those historical figures.

**Git state:** branch `master`; at verification time, remote `master` matched implementation commit `237c3def3502c8658333dd790cb75dc1613cb69a`. The report-only follow-up does not change implementation or benchmark files. The implementation commit contains the six approved tracked project files, `scripts/verify_sdist.py`, and the five presentation files. This report records the later CI evidence in a documentation-only follow-up commit. The separate untracked `submission/demo-video/` directory was deliberately excluded. `uv.lock` was not changed; no package was published.

**Acceptance:** PASS.

## Validation commands and results

- `uv lock --check` — passed; resolved 67 locked packages without changing `uv.lock`.
- `.venv\Scripts\python.exe -m pytest --basetemp work\pytest-critical-resolution-full --cov=datapipe.core --cov-branch --cov-report=term-missing --cov-report=json:work/core-coverage-critical-resolution.json --cov-fail-under=85` — **147 passed, 2 skipped**, 210.11 seconds; skips are POSIX signal tests on Windows; non-fatal pytest cache warning (`WinError 183`).
- `scripts/check_branch_coverage.py work\core-coverage-critical-resolution.json --minimum 85` — **238/276 = 86.23% core branch coverage**, passed.
- `.venv\Scripts\ruff.exe check .` — passed.
- `.venv\Scripts\mypy.exe src/` — passed, no issues in 28 source files.
- `git diff --check` — passed before staging.
- Both GitHub workflow YAML files parsed successfully.
- Fresh sdist build, archive inspection, clean isolated install, CLI, config validation and sample pipeline — passed as detailed above.

## Files changed

- `.github/workflows/ci.yml`
- `.github/workflows/release-testpypi.yml`
- `.gitignore`
- `BENCHMARKS.md`
- `pyproject.toml`
- `src/datapipe/transforms/aggregation.py`
- `scripts/verify_sdist.py`
- The five presentation files listed above
- `submission/CRITICAL_ISSUES_RESOLUTION_REPORT.md` (this report)

No dependency metadata or `uv.lock` changes were made. No package was published. Commit `237c3def3502c8658333dd790cb75dc1613cb69a` was pushed to `origin/master`; the Ubuntu CI run passed.

## Acceptance summary

| Critical issue | Status | Evidence |
|---|---|---|
| Performance | PASS | Three 15-run interleaved sessions and pooled 3.33x for the specific 50k-row/50k-batch in-memory workload; normalized outputs equal. Other workloads remain below 3x. |
| Source distribution | PASS | Fresh 34-entry archive excludes development artifacts; Ubuntu CI inspection, clean sdist installation, and sample smoke passed on Python 3.12 and 3.13. |
| Linux SIGTERM/checkpoint/resume | PASS | Ubuntu CI run 45431931687 passed on both Python versions. Both real signal cases resumed and asserted cursor/output count 250,000. |
| GitHub presentation files | PASS | All five files are linked above and verified present at implementation commit `237c3def...`, which is in the remote `master` history. |

## Remaining actions

No remaining action is required for the approved DATAPIPE change set, GitHub verification, or Linux SIGTERM/checkpoint/resume acceptance criteria. The separate video directory was intentionally excluded from this task and remains outside the commit.

Final verdict: **READY — GitHub verification and real Linux signal/checkpoint/resume tests passed.**
