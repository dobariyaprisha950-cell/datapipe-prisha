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
| 2,000,000 unique-key deduplication | Passed | 252.77 s | 30,597,120 bytes | SQLite state 116,942,845 bytes; output 207,832,305 bytes |
| 5,000,000 unique-key deduplication | Passed | 545.67 s | 29,528,064 bytes | SQLite state 294,023,095 bytes; output 522,912,554 bytes |
| 2,000,000 rows, one aggregation group | Passed | 328.58 s | 2,191,360 bytes | The test reported bounded RSS; disk size was not separately captured |

The dedupe runs used a 20,000-record sink batch. The five-million measurement used `DATAPIPE_DEDUPE_RECORDS=5000000`; the test duration includes generated input creation, pipeline execution, memory sampling, disk measurement, and output checks. The aggregation case used one group, so its in-memory group cardinality remained one while input count increased. A separate 30,000-group aggregation case passed and checks many on-disk groups. These are observations on this Windows/Python host, not universal memory guarantees. Working memory grows with configured queue/batch sizes and aggregate group cardinality; SQLite disk use grows with distinct keys, groups, and committed journal deltas.

## Reproduce

Run `uv run python benchmarks/benchmark.py --records 20000 --workers 4 --rounds 5000 --runs 3` and `uv run python benchmarks/profile_normalizer.py --records 5000 --rounds 5000`. The disk-backed two-million-row checks are `tests/test_disk_transforms.py::test_streaming_high_cardinality_dedup_does_not_keep_source_rows` and `tests/test_disk_transforms.py::test_streaming_large_aggregation_keeps_only_group_state_in_memory`; set `DATAPIPE_DEDUPE_RECORDS=2000000` or `5000000` for the deduplication measurements, and `DATAPIPE_AGGREGATION_RECORDS=2000000` for aggregation. Results depend on host, filesystem, queue, and batch configuration.
