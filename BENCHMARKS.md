# Benchmark and memory results

## Fingerprint execution modes (2026-11-18)

Measured using Python 3.14.7 on Windows 10 build 19045-SP0, Intel64 Family 6 Model 69 Stepping 1, 4 logical CPUs. Each mode processed 20,000 identical records with 5,000 configured rounds; four workers were requested for the process mode. Three repetitions ran and normalized outputs were compared exactly.

Command:

```powershell
.venv\Scripts\python.exe benchmarks/benchmark.py --records 20000 --workers 4 --rounds 5000 --runs 3
```

| Mode | Run times (s) | Median (s) | Throughput | Peak RSS including children |
|---|---:|---:|---:|---:|
| Sequential | 0.0335290, 0.0321185, 0.0396633 | 0.0335290 | 596,498.6 records/s | 39,870,464 bytes |
| Thread | 0.0498723, 0.0420663, 0.0695136 | 0.0498723 | 401,024.2 records/s | 39,870,464 bytes |
| Process | 0.9230904, 0.8969377, 0.8910895 | 0.8969377 | 22,298 records/s | 136,511,488 bytes |

Speed relative to sequential was 0.6723x for threads and 0.03738x for processes. All outputs matched. The requested 3x parallel speedup was not reached. The transform now uses an exact affine recurrence with logarithmic work in the configured rounds, so scheduling, process startup, serialization, and result reconstruction cost much more than the remaining calculation on this host. Auto mode favors sequential work; thread and process overrides remain available. The previous implementation's sequential median was 52.6781 seconds on this host, compared with 0.0335290 seconds after optimization, a cross-run improvement of about 1,571x. This is not a parallel speedup result.

## cProfile evidence

Command: `.venv\Scripts\python.exe benchmarks/profile_normalizer.py --records 5000 --rounds 5000`.

The profile contained 20,021 calls in 0.024 seconds. `_fingerprint_fast` used 0.005 seconds across 5,000 records; coefficient construction took less than 0.001 seconds. Profiler timings include instrumentation overhead.

## Disk-backed transform tests at two and five million rows

Both regression checks ran against the actual pipeline engine with bounded sink batches and output/checkpoint commits. RSS growth is measured by a background sampler while the process runs; disk use is measured from generated state and output files.

| Workload | Result | Elapsed | Peak RSS growth | State/output disk |
|---|---|---:|---:|---:|
| 2,000,000 unique-key deduplication | Passed | 202.25 s | 36,372,480 bytes | SQLite state 116,942,845 bytes; output 207,832,305 bytes |
| 5,000,000 unique-key deduplication | Passed | 587.68 s | 36,438,016 bytes | SQLite state 294,023,095 bytes; output 522,912,554 bytes |
| 2,000,000 rows, one aggregation group | Passed | 328.58 s | 2,191,360 bytes | The test reported bounded RSS; disk size was not separately captured |

The dedupe runs used a 20,000-record sink batch. The five-million measurement used `DATAPIPE_DEDUPE_RECORDS=5000000`; the test duration includes generated input creation, pipeline execution, memory sampling, disk measurement, and output checks. The aggregation case used one group, so its in-memory group cardinality remained one while input count increased. A separate 30,000-group aggregation case passed and checks many on-disk groups. These are observations on this Windows/Python host, not universal memory guarantees. Working memory grows with configured queue/batch sizes and aggregate group cardinality; SQLite disk use grows with distinct keys, groups, and committed journal deltas.

Paired remeasurement on 2026-11-23 used the same test and instrumentation for 2 million and 5 million unique-key records. In PowerShell, reproduce the two-million run with:

```powershell
$env:DATAPIPE_DEDUPE_RECORDS='2000000'
.venv\Scripts\python.exe -m pytest tests\test_disk_transforms.py::test_streaming_high_cardinality_dedup_does_not_keep_source_rows -s -q --basetemp work\pytest-2m-final
```

For five million records, use:

```powershell
$env:DATAPIPE_DEDUPE_RECORDS='5000000'
.venv\Scripts\python.exe -m pytest tests\test_disk_transforms.py::test_streaming_high_cardinality_dedup_does_not_keep_source_rows -s -q --basetemp work\pytest-5m-final
```

The test generates JSONL incrementally, then measures RSS growth around the pipeline run (so source generation and the source file itself are outside the RSS-growth interval); it checks all records were written and the test's 96 MiB growth limit was met. RSS growth was 36,372,480 bytes at 2 million and 36,438,016 bytes at 5 million, while SQLite state plus output disk grew from about 310 MiB to 779 MiB. This shows bounded process memory for this deduplication workload as the input grows 2.5x; it does not imply bounded disk usage, nor does it guarantee the same RSS on other hosts. Both measurements were on Python 3.14.7 / Windows 10 build 19045. The sampler is periodic and can miss very brief RSS peaks.

## Reproduce

Run `uv run python benchmarks/benchmark.py --records 20000 --workers 4 --rounds 5000 --runs 3` and `uv run python benchmarks/profile_normalizer.py --records 5000 --rounds 5000`. The disk-backed two-million-row checks are `tests/test_disk_transforms.py::test_streaming_high_cardinality_dedup_does_not_keep_source_rows` and `tests/test_disk_transforms.py::test_streaming_large_aggregation_keeps_only_group_state_in_memory`; set `DATAPIPE_DEDUPE_RECORDS=2000000` or `5000000` for the deduplication measurements, and `DATAPIPE_AGGREGATION_RECORDS=2000000` for aggregation. Results depend on host, filesystem, queue, and batch configuration.

## Fingerprint batching follow-up (2026-11-21)

The pre-change profile showed that the streaming engine called `FingerprintExecutor.apply()` once per row. On the 5,000-row process-pipeline profile, this made 5,000 synchronous submissions/results; the caller waited on process-pool IPC for each one. The affine fingerprint itself is already exact and O(log rounds) to derive its coefficients, then O(1) per value. Consequently, process startup, serialization, and synchronization now cost more than the work being parallelized. Threads also run Python bytecode under the GIL.

The pipeline now accumulates validated records up to its bounded sink batch, transforms a whole stage batch, and submits at most one contiguous integer chunk per worker to a reusable executor. The same batching is used around disk-backed deduplication/aggregation, including fingerprint stages after aggregation. Record ordering, first-seen dedupe semantics, per-record transform error routing, and checkpoint commit boundaries are retained. The standalone `fingerprint_records_parallel()` API still constructs a pool per call, so its process startup cost remains visible.

### Isolated API before/after

Measured on Windows 10 build 19045, Python 3.14.7, Intel64 Family 6 Model 69 Stepping 1, four logical CPUs. Both runs used 20,000 identical records, 5,000 rounds, four requested workers, three repetitions, exact normalized-output equality, and the same benchmark command. “Before” used the source at the then-current base commit; “after” used batched executor code.

| Mode | Before median (s) | Before speed vs sequential | After median (s) | After speed vs sequential |
|---|---:|---:|---:|---:|
| Sequential | 0.032962 | 1.00x | 0.041458 | 1.00x |
| Thread | 0.049553 | 0.67x | 0.088752 | 0.47x |
| Process | 1.250154 | 0.026x | 1.091476 | 0.038x |

Both outputs matched exactly. These noisy isolated timings show no material parallel improvement; the public standalone function already sent a small number of large chunks and includes process-pool startup on every call. The after-run process pool used about 143.9 MB peak RSS including children, versus 138.3 MB before. Sequential remains the default.

### Complete pipeline current measurement

Command: `uv run python benchmarks/benchmark_pipeline.py --records 5000 --workers 4 --rounds 5000 --batch-size 500 --runs 3`. This measures a generated JSONL source through validation, fingerprint transformation, output commits, and summary reporting. The three modes use the same input, each writes 5,000 records, and normalized outputs matched. Mode order is deterministically shuffled per repetition. Timed runs do not start an RSS sampler; memory is collected in separate subsequent pipeline runs. Host: Python 3.14.7, Windows 10 build 19045, Intel64 Family 6 Model 69 Stepping 1, four logical CPUs.

| Mode | Run times (s) | Median (s) | Throughput | Peak RSS incl. children (separate memory pass) |
|---|---:|---:|---:|---:|
| Sequential | 0.829298, 0.615053, 0.605960 | 0.615053 | 8,129 records/s | 74,526,720 bytes |
| Thread | 0.647316, 0.597999, 0.615463 | 0.615463 | 8,124 records/s | 74,653,696 bytes |
| Process | 4.703394, 4.817708, 4.822483 | 4.817708 | 1,038 records/s | 320,729,088 bytes |

Relative to sequential, thread mode measured 0.999x and process mode 0.128x. Process mode remains substantially slower and used over four times the sequential peak RSS in its separate memory pass. The memory sampler runs after timing; measurements are absolute process-tree RSS from the same parent process, so retained allocator memory can affect comparisons. These results replace the earlier pipeline table whose memory sampler ran concurrently with timing. Sequential remains the default.

### After-change cProfile evidence

Command: `uv run python benchmarks/profile_pipeline.py --records 5000 --workers 4 --rounds 5000 --batch-size 500 --execution process`. The parent profile observed 10 `_transform_records` batch calls and 50 worker-map result chunks, rather than 5,000 per-record submissions. It attributed about 4.0 seconds to batch transform calls/waiting and 5.3 seconds to process-pool shutdown under profiling. cProfile does not instrument child-process execution and adds overhead; use the unprofiled benchmark table for elapsed-time comparisons. The main remaining process cost is worker startup/IPC and pool teardown, not individual fingerprint arithmetic.

Reproduce the current isolated benchmark with `uv run python benchmarks/benchmark.py --records 20000 --workers 4 --rounds 5000 --runs 3`. Reproduce end-to-end with `uv run python benchmarks/benchmark_pipeline.py --records 5000 --workers 4 --rounds 5000 --batch-size 500 --runs 3`. The 3x target was **not achieved**. Do not opt into process execution for this workload; keep `execution = "auto"` or `"sequential"` unless a different real workload and host benchmark demonstrates a win.

## In-memory daily aggregation PyArrow fast path (2026-11-22)

`benchmarks/benchmark_aggregation.py` compares `_aggregate_daily_python`, the exact `Fraction` reference/fallback implementation, with the public `aggregate_daily` API. Both receive the same deterministic input and metrics, and include input conversion, native validation/grouping, conversion back, and sorted output. Timed repetitions are interleaved with no RSS sampler running. Memory is measured in fresh worker processes; the parent samples the worker process tree and reports peak RSS and USS. The machine used Windows 10 build 19045, Python 3.14.7, PyArrow 23.0.1, Intel64 Family 6 Model 69 Stepping 1, four logical CPUs. Input had 28 dates and sum/count/min/max metrics.

| Rows | Arrow batch size | Runs | Python exact median | PyArrow median | Speed-up | Exact RSS / USS | Arrow RSS / USS | Normalized outputs |
|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 50,000 | 50,000 (default) | 15 | 0.462349 s | 0.135278 s | 3.42x | 49,340,416 / 32,169,984 bytes | 65,220,608 / 43,261,952 bytes | Equal |
| 50,000 | 5,000 | 11 | 0.509039 s | 0.228154 s | 2.23x | 49,225,728 / 32,055,296 bytes | 63,504,384 / 43,098,112 bytes | Equal |
| 200,000 | 5,000 | 3 | 1.879496 s | 0.760609 s | 2.47x | 91,148,288 / 73,977,856 bytes | 104,296,448 / 83,918,848 bytes | Equal |

Fresh remeasurement on 2026-11-23 with the same deterministic 50,000-row workload, one 50,000-row Arrow batch, and 15 interleaved runs produced 0.417812 s exact-Python median versus 0.135114 s PyArrow median: **3.09x**. The outputs were normalized and compared exactly. Throughput was 119,671 records/s versus 370,058 records/s. Fresh subprocess peak RSS/USS was 49,418,240 / 32,243,712 bytes for Python and 64,049,152 / 43,679,744 bytes for Arrow. Environment: Python 3.14.7, PyArrow 23.0.1, Windows 10 build 19045, Intel64 Family 6 Model 69 Stepping 1, four logical CPUs. Reproduce with `uv run python benchmarks/benchmark_aggregation.py --records 50000 --batch-size 50000 --runs 15`. This narrowly exceeds 3x only for this in-memory workload and this batch/host configuration; the 5,000-row-batch and end-to-end measurements below remain under 3x.

For bottleneck evidence, cProfile was run separately on the same 50,000-row workload, with PyArrow warmed before profiling. In the exact Python profile, `_aggregate_grouped_python` took 2.865 s cumulative under instrumentation; `Fraction.from_float` ran 50,000 times and Fraction addition ran 50,000 times. In the warmed Arrow profile, `_aggregate_grouped_arrow` took 0.170 s cumulative; its two `flush_batch` calls accounted for 0.166 s, Arrow compute wrappers for 0.091 s, and the group-by kernel for 0.011 s. Profiling adds instrumentation overhead and these numbers are diagnostic only; benchmark medians above are unprofiled.

The fresh 50,000-row/default-batch run achieved 3.42x (15 interleaved runs), while fresh-process measurements with 5,000-row Arrow batches were 2.23x for 50,000 rows and 2.47x for 200,000 rows. Thus the in-memory 3x objective is demonstrated for the 50,000-row dataset only when the whole dataset fits in one bounded Arrow table; it is not met with a 5,000-row batch or by the disk-backed end-to-end pipeline. Peak USS grew by about 39.8 MiB for Python and 38.9 MiB for Arrow when input grew from 50,000 to 200,000 rows; this is expected because the public in-memory API receives and retains the complete caller-owned list. For 50,000 rows with the 5,000-row Arrow batch, Arrow used about 10.5 MiB more USS than exact Python, including imports, input construction, and native buffers. The batch limits Arrow table materialization, not the API's input or output lists. RSS/USS are independently sampled and can miss a very brief peak; RSS sums process-tree resident pages while USS estimates unique pages. Reproduce with `uv run python benchmarks/benchmark_aggregation.py --records 50000 --batch-size 50000 --runs 15` and scale `--records` or vary `--batch-size` to inspect growth. Results depend on Python/PyArrow builds, CPU, and background load.

### Exact-sum compatibility note

The exact Python sum path is intentionally a semantic change from the implementation in commit `aaf8d99`, which used built-in `sum()` over floats and therefore rounded after each addition. The current implementation sums each finite float as an exact `Fraction` and rounds once when converting the final result back to float. For example, `[1e16, 1.0, -1e16]` produces `0.0` with the old built-in sum and `1.0` with the current exact sum. The output schema is unchanged, but some aggregate values can differ for cancellation-sensitive input. Exact accumulation aligns the in-memory path with disk-backed aggregation and is retained to satisfy the pipeline's exact-sum requirement; callers depending on historical order-dependent floating results should account for this behavior change.

The native path is limited to bounded batches and exact integer/null Arrow columns. Sum fields additionally require every integer and a conservative cumulative absolute-sum bound to be at most 2^53, proving exact conversion and every possible partial sum. Fractional floats, booleans, non-finite values, unsafe sum bounds, unsupported Arrow values, or noncanonical/invalid dates route the full call to the exact Python path. Integer min/max is allowed without the sum bound because integer ordering is monotonic under the final Python int-to-float conversion. The API retains its existing in-memory record collection; only Arrow table materialization is batch-bounded.

### 2026-11-24 remeasurement after removing a redundant batch-reference copy

The Arrow path previously copied each bounded record slice into a second list before calling `Table.from_pylist`. It now passes the original record sequence for a single batch and one slice per batch otherwise. This removes a duplicate Python reference array without changing the input rows, aggregation, exact-safety checks, or output conversion. A separate paired A/B measurement of the public in-memory API used the 50,000-row benchmark input, the same 50,000-row batch and metrics, 30 alternating calls per implementation, warm-up before timing, and normalized-output checks on every call. The committed pre-change implementation had a 0.196845 s median; the updated implementation had a 0.180306 s median (1.092x). Outputs matched. This A/B result measures the allocation change, not speed relative to the exact-Python baseline.

The required exact-Python-versus-PyArrow benchmark was then run three independent times with the existing command and 15 interleaved repetitions per run. Each run included exact normalized-output comparisons; timing did not overlap memory sampling. All three session medians exceeded 3x. Pooling the 45 raw timings per implementation gives 0.458573 s for exact Python and 0.137651 s for PyArrow, or 3.33x. The session-by-session results were:

| Session | Exact Python median | PyArrow median | Speed-up | Outputs equal |
|---:|---:|---:|---:|---|
| 1 | 0.433413 s | 0.137608 s | 3.15x | Yes |
| 2 | 0.669717 s | 0.172688 s | 3.88x | Yes |
| 3 | 0.439245 s | 0.129903 s | 3.38x | Yes |
| Pooled 45 timings | 0.458573 s | 0.137651 s | 3.33x | Yes |

Environment and workload match the table above: Windows 10 build 19045, Python 3.14.7, PyArrow 23.0.1, Intel64 Family 6 Model 69 Stepping 1, four logical CPUs, 50,000 rows, 50,000-row Arrow batch, 28 dates and sum/count/min/max metrics. Separate fresh-process peak memory samples across the sessions were 49,324,032–49,397,760 bytes RSS / 32,145,408–32,219,136 bytes USS for exact Python and 62,214,144–62,697,472 bytes RSS / 39,374,848–39,825,408 bytes USS for PyArrow. The second session ran under substantially slower host timing for both modes; it is retained in the report, not discarded. The >=3x target is reproduced for this one in-memory workload and configuration. It is not a general pipeline or disk-backed speedup: the 5,000-row-batch and complete-pipeline measurements above remain below 3x. Reproduce with `uv run python benchmarks/benchmark_aggregation.py --records 50000 --batch-size 50000 --runs 15`.

A warmed cProfile run on this same data (profiling is diagnostic, not part of the timing benchmark) attributed 1.158 s cumulative to `_aggregate_grouped_python`; `Fraction.from_float` accounted for 0.382 s and Fraction addition for 0.195 s. The Arrow implementation took 0.120 s cumulative in `_aggregate_grouped_arrow`; batch conversion/validation in `flush_batch` accounted for 0.120 s, Arrow compute wrappers 0.064 s, and the group-by kernel 0.008 s. This confirms that exact Fraction arithmetic dominates the reference path, while conversion and validation dominate the accelerated path; the group-by kernel itself is not the main remaining optimization opportunity.

### Streaming pipeline comparison

`benchmarks/benchmark_aggregation_pipeline.py` compares the exact per-record `DiskTransformProcessor` path (Arrow summary helper disabled) with the integrated Arrow batch path through JSONL reading, Pydantic validation, SQLite state, output commits, checkpoint handling, and final output. It uses independent fresh worker processes for memory sampling. It generates one input file in the parent, then each worker runs against that same file. Memory includes pipeline imports, SQLite, output/state buffers, and the Python process tree. Normalized final outputs are compared.

| Rows | Sink batch | Runs | Mode | Median elapsed | Throughput | Peak tree RSS / USS |
|---:|---:|---:|---|---:|---:|---:|
| 20,000 | 5,000 | 3 (previous timing) | Exact per-record disk state | 3.067 s | 6,522 records/s | Previous same-process sample; not comparable |
| 20,000 | 5,000 | 3 (previous timing) | Arrow batch summaries | 1.486 s | 13,457 records/s | Previous same-process sample; not comparable |
| 20,000 | 1,000 | 3 (previous timing) | Exact per-record disk state | 3.692 s | 5,417 records/s | Previous same-process sample; not comparable |
| 20,000 | 1,000 | 3 (previous timing) | Arrow batch summaries | 2.505 s | 7,985 records/s | Previous same-process sample; not comparable |
| 20,000 | 5,000 | 2 | Exact per-record disk state | 3.373 s | 5,929 records/s | 80,404,480 / 53,239,808 bytes |
| 20,000 | 5,000 | 2 | Arrow batch summaries | 1.476 s | 13,546 records/s | 87,576,576 / 55,414,784 bytes |
| 5,000 | 1,000 | 2 | Exact per-record disk state | 1.246 s | 4,014 records/s | 71,356,416 / 44,191,744 bytes |
| 5,000 | 1,000 | 2 | Arrow batch summaries | 1.011 s | 4,946 records/s | 82,268,160 / 53,780,480 bytes |

The complete pipeline measured 2.28x in the fresh two-run 20,000-row/5,000-batch comparison and 1.23x for the 5,000-row/1,000-batch comparison; both output pairs were equal and both remain below 3x. Fresh-process USS was approximately 2.2 MiB higher for Arrow at 20,000 rows/5,000 batch and 9.1 MiB higher for Arrow at 5,000 rows/1,000 batch. Fixed interpreter/import costs and SQLite/Arrow buffers are significant at small sizes. Two timing repetitions are exploratory and noisier than the earlier three-run measurements. Reproduce with `uv run python benchmarks/benchmark_aggregation_pipeline.py --records 20000 --batch-size 5000 --runs 3` or `--records 5000 --batch-size 1000 --runs 3`. The disk fast path applies to batches of at least 1,000 valid records with safe Arrow-convertible fields. Smaller batches and unsafe records retain the exact per-record path.
