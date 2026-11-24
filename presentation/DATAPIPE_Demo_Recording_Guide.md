# DATAPIPE Demo Recording Guide

## Before recording

Use PowerShell from the repository root. The verified local interpreter is Python 3.14.7 at `.venv\Scripts\python.exe`; the Windows Python launcher (`py`) is not reliable in this environment. Do not use an existing `work/demo-output-fresh` or plugin output directory for a new run. The commands below generate unique temporary paths under `work/`.

Check the interpreter and the project CLI:

```powershell
.\.venv\Scripts\python.exe --version
.\.venv-demo\Scripts\datapipe.exe --version
.\.venv-demo\Scripts\datapipe.exe --help
```

The packaged CLI was verified as version `0.1.0`. In the video, show existing verified installation output if you do not want to spend time installing the package live.

## Optional fresh TestPyPI installation rehearsal

This installs a clean virtual environment under ignored `work/`, resolves runtime dependencies from PyPI, then installs only the DATAPIPE distribution from TestPyPI. It requires internet access.

```powershell
$runId = [guid]::NewGuid().ToString('N')
$envDir = "work\demo-env-$runId"
$basePython = (Resolve-Path '.\.venv\Scripts\python.exe').Path
& $basePython -m venv $envDir
$py = (Resolve-Path "$envDir\Scripts\python.exe").Path
& $py -m pip install --upgrade pip
& $py -m pip install --index-url https://pypi.org/simple/ 'aiohttp>=3.11,<4' 'opentelemetry-api>=1.29,<2' 'pyarrow>=18,<24' 'pydantic>=2.10,<3' 'PyYAML>=6,<7' 'typer>=0.15,<1'
& $py -m pip install --no-deps --index-url https://test.pypi.org/simple/ datapipe-prisha==0.1.0
& $py -c "import datapipe, importlib.metadata as m; print(m.version('datapipe-prisha')); print(datapipe.__file__)"
$cliPath = "$envDir\Scripts\datapipe.exe"
& $cliPath --version
& $cliPath --help
& $cliPath validate-config examples\demo-pipeline-fresh.yaml
& $cliPath list-plugins
```

Expected: version `0.1.0`, an import path under that environment's `Lib\site-packages\datapipe`, a successful help screen, valid configuration, and the built-in/installed plugin list. The separate plugin is not part of the DATAPIPE wheel.

## Five-minute recording sequence

| Time | Slide | Show and do |
|---|---:|---|
| 0:00–0:30 | 1 | Introduce DATAPIPE and its role. |
| 0:30–1:08 | 2 | Trace sources, bounded async queue, validation, transforms and sinks. |
| 1:08–1:42 | 3 | Show the TestPyPI version and fresh environment commands. Avoid typing the full install live; use verified output. |
| 1:42–2:20 | 4 | In PowerShell show `--version`, `--help`, config validation and plugin listing. In VS Code briefly show `examples/demo-pipeline-fresh.yaml`. |
| 2:20–3:03 | 5 | Run a fresh copy of the sample configuration, then show its output folder. |
| 3:03–3:35 | 6 | Show the separate plugin package, entry-point metadata and successful CLI discovery/run. |
| 3:35–4:30 | 7 | Show benchmark evidence and current test/streaming memory results. State the workload limits. |
| 4:30–5:00 | 8 | Summarize the implementation and close. |

### Run the two-source sample without replacing existing demo output

Use the installed package CLI. The following makes a run-specific YAML copy and substitutes both its output and checkpoint paths; source data paths remain the repository examples.

```powershell
$runId = [guid]::NewGuid().ToString('N')
$outputDir = "work\recording-output-$runId"
$checkpoint = "work\recording-checkpoint-$runId.json"
$config = "work\recording-pipeline-$runId.yaml"
$text = Get-Content -Raw examples\demo-pipeline-fresh.yaml
$text = $text.Replace('work/demo-output-fresh', $outputDir.Replace('\', '/'))
$text = $text.Replace('work/demo-checkpoint-fresh.json', $checkpoint.Replace('\', '/'))
[System.IO.File]::WriteAllText($config, $text, [System.Text.UTF8Encoding]::new($false))
.\.venv-demo\Scripts\datapipe.exe validate-config $config
.\.venv-demo\Scripts\datapipe.exe run $config --json
Get-Content "$outputDir\summary.json"
Get-ChildItem $outputDir -Recurse
```

Expected sample counts from the verified fresh run: 7 fetched, 4 valid, 3 invalid, 1 duplicate removed, 3 written. Output includes date-partitioned JSONL, `dead-letter.jsonl`, and `summary.json`. Counts depend on the checked-in example data and project version.

### Install and run the third-party sample plugin safely

The plugin lives at `examples/plugin` as a separately buildable distribution. Install it in the same environment used for the CLI (or the existing `.venv-demo` environment):

```powershell
.\.venv-demo\Scripts\python.exe -m pip install .\examples\plugin
.\.venv-demo\Scripts\datapipe.exe list-plugins
```

For an actual plugin run, copy its TOML config to a unique temporary name and change only the output directory:

```powershell
$runId = [guid]::NewGuid().ToString('N')
$pluginOutput = "work\recording-plugin-output-$runId"
$pluginConfig = "work\recording-plugin-$runId.toml"
$text = (Get-Content -Raw examples\plugin\pipeline.toml).Replace('work/plugin-demo-output', $pluginOutput.Replace('\', '/'))
[System.IO.File]::WriteAllText($pluginConfig, $text, [System.Text.UTF8Encoding]::new($false))
.\.venv-demo\Scripts\datapipe.exe validate-config $pluginConfig
.\.venv-demo\Scripts\datapipe.exe run $pluginConfig --json
Get-ChildItem $pluginOutput -Recurse
```

Expected: plugin name `sample` appears in discovery and one plugin record is written. This sequence was tested previously with the separately installed sample plugin; the current demo run should be treated as fresh evidence only if you execute it and inspect its output.

### Show the automated test result

A current local run used Python 3.14.7 on Windows. Run from the repository root if you need to refresh the evidence before recording:

```powershell
$env:COVERAGE_FILE = 'work/.coverage-presentation-20261124'
.\.venv\Scripts\python.exe -m pytest --basetemp work\pytest-presentation-20261124 --cov=datapipe.core --cov-branch --cov-report=term-missing --cov-fail-under=85
```

Verified result: 147 passed, 2 skipped, 86.23% core branch coverage (238/276 branches); pytest's combined statement-and-branch coverage was 91.41%. The two skipped tests require POSIX signals and were skipped because this run was on Windows. Full tests may take several minutes; for a strict five-minute recording, show the saved successful terminal result instead of waiting for a fresh run.

## Benchmark to show

The measured 3.09× result applies to the in-memory daily aggregation benchmark: 50,000 rows, one 50,000-row Arrow batch, 15 interleaved runs, median exact-Python time 0.417812 s and PyArrow time 0.135114 s, with normalized output equality. Other batch sizes and the disk-backed pipeline did not reach 3×. The 2M/5M streaming memory figures are measured RSS growth and do not imply constant total memory: disk-backed state grows with unique keys/groups. See `BENCHMARKS.md` for the complete method and limitations.

To reproduce the isolated benchmark in a development checkout with uv installed:

```powershell
uv run python benchmarks\benchmark_aggregation.py --records 50000 --batch-size 50000 --runs 15
```

That is the measured command recorded in `BENCHMARKS.md`. The isolated run takes longer than the recorded video segment, so it is optional rehearsal preparation. The GUI/recording itself does not require rerunning the benchmark.

## Screen and recording tips

- Keep PowerShell at the repository root and use a readable font size.
- Use VS Code to show configuration and the standalone plugin package; do not show secret-bearing environment values.
- Expand the unique `work/recording-output-*` directory and show the partition, dead-letter file and summary.
- Avoid exposing user paths, access tokens, or unrelated notifications in the capture.
- Rehearse once with the DOCX script and keep its screen cues visible on a second display if available.
- The PPTX speaker notes carry the same slide-specific narration and time cues as the separate script.
