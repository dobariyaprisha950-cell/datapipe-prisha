# Benchmark results

## Exact fingerprint optimization (2026-11-18)

Measured in the current workspace using Python 3.14.7 on Windows 10 build 19045-SP0, Intel64 Family 6 Model 69 Stepping 1, 4 logical CPUs. Each mode processed 20,000 identical records with 5,000 configured rounds and four workers requested for the process-pool mode. Three repetitions were run. Outputs were compared for exact equality.

Command:

```powershell
.venv\Scripts\python.exe benchmarks/benchmark.py --records 20000 --workers 4 --rounds 5000 --runs 3
```

| Mode | Run times (s) | Median (s) | Median throughput | Peak RSS including children |
|---|---:|---:|---:|---:|
| Optimized Python | 0.03208, 0.03409, 0.02925 | 0.03208 | 623,410 records/s | 39,374,848 bytes |
| Process pool | 0.96142, 1.00387, 0.97043 | 0.97043 | 20,609 records/s | 141,565,952 bytes |

Measured process-pool speedup: **0.0331x** (about 30.2 times slower), with exact output equality. The process implementation retains process startup, input serialization, result transfer, and reconstruction overhead. Once the per-record recurrence was reduced from 5,000 Python iterations to logarithmic coefficient composition, the computation became too small to amortize a process pool. The engine therefore applies fingerprint transforms in its bounded streaming path and does not dispatch each pipeline run through the process pool.

The prior implementation measured 52.6781 seconds sequential and 28.9839 seconds in the process pool (1.8175x) for the same 20,000 records and 5,000 rounds, across two repetitions on this host. Relative to that previous sequential median, the current sequential median is approximately 1,642x faster. This is a cross-run before/after comparison, not a new parallel speedup claim. The requested 3x process-pool speedup is **not met**; the exact algorithmic optimization eliminates the old hot loop, making process overhead the bottleneck. Output correctness was checked against the original iterative recurrence with signed and large integer values and against the parallel API's output.

## Profile evidence

Command:

```powershell
.venv\Scripts\python.exe benchmarks/profile_normalizer.py --records 5000 --rounds 5000
```

cProfile reported 20,021 calls in 0.024 seconds. `fingerprint_records` took 0.024 seconds cumulative, `_fingerprint_fast` accounted for 0.005 seconds over 5,000 records, and coefficient construction took less than 0.001 seconds. The earlier profile attributed 2.156 seconds to the iterative function over 5,000 records at 1,000 rounds. Profile timings include profiler overhead and are not compared directly with benchmark timing.

## Memory characteristics and limitations

The two-million-row streaming memory regression passed with less than 128 MiB RSS growth while queue size is 256 and sink batches are 20,000 records. The final instrumented run measured baseline RSS 111,620,096 bytes, peak RSS 124,432,384 bytes, and growth of **12,812,288 bytes**. This is process RSS on this Windows host; it is not a cross-platform bound and the test covers the no-transform/map/filter streaming path.

No bounded-memory claim applies to deduplication or aggregation today: those transforms still materialize valid records for whole-run semantics. The fingerprint-only path is now bounded by the queue and sink batch. Disk-backed transform state would need to participate in the output journal before safely advancing batch checkpoints and is not yet implemented.

## Reproduce

Install the dev group and run the benchmark command above. The script prints environment information, every run time, median throughput, peak process-tree RSS, CPU sampling, and output equality. Benchmark results are host-specific and should be reproduced on the target deployment hardware. No 3x speedup should be claimed unless a fresh, apples-to-apples measurement reaches it.
