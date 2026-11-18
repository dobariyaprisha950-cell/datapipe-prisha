# Benchmark results

## Recorded final run

Measured 2026-11-18 on the current host. The requested 3.0x target is **not met**; the process-pool implementation measured 1.8175x faster in this run. Sequential and parallel outputs matched by SHA-256 digest.

| Setting | Value |
|---|---:|
| OS | Windows 10 build 10.0.19045-SP0 |
| Python | 3.14.7 |
| CPU | Intel64 Family 6 Model 69 Stepping 1, GenuineIntel; 4 logical CPUs |
| Records | 20,000 |
| Fingerprint rounds per record | 5,000 |
| Process workers | 4 |
| Repetitions | 2 |

Command:

```powershell
python benchmarks/benchmark.py --records 20000 --workers 4 --rounds 5000 --runs 2
```

| Mode | Run times (s) | Median (s) | Median throughput | Peak RSS including children |
|---|---:|---:|---:|---:|
| Pure Python | 55.9031, 49.4530 | 52.6781 | 379.66 records/s | 39,825,408 bytes |
| Process pool | 28.8588, 29.1090 | 28.9839 | 690.04 records/s | 143,757,312 bytes |

Measured median speedup: **1.8175x**. The process pool used about 104 MB more peak RSS. This is a single-host measurement, not a general performance guarantee.

The benchmark's process-tree CPU utilization sampler reported 21.37% of logical capacity for the baseline and 1.28% for the parallel run. Worker lifetime and sampling make the parallel CPU figure unreliable; it must not be read as evidence that the workers were idle. The measurements show that process startup/communication, Python object preparation, and output reconstruction leave substantial overhead for this implementation, but this run alone does not isolate their individual costs. More repetitions, host CPU quota details, and a worker-count sweep are needed to determine why four workers reach less than 3x on this host. The requested speed target remains unmet.

## Profile evidence

Command:

```powershell
python benchmarks/profile_normalizer.py --records 5000 --rounds 1000
```

The final cProfile run reported 20,002 calls in 2.195 seconds. `fingerprint_records` used 2.195 seconds cumulative; `_fingerprint` accounted for 2.156 seconds over 5,000 calls. This confirms the iterative Python fingerprint loop as the hot path in the sequential implementation. cProfile adds overhead, so these absolute timings are not comparable with the benchmark table.

## Reproduce

Install the dev group and run the commands above. The benchmark prints per-run time, median throughput, peak process-tree RSS, sampled CPU utilization, and an output-equality check. Vary record counts and worker counts, and retain the full environment output. No speed target should be considered met unless the measured result reaches that threshold.
