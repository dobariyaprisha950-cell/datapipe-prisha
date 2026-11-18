# Benchmark results

## Recorded run

Measurements below were captured from the existing project run on 2026-11-17. The speed target is **not met**: the parallel implementation was about 2.01x faster for this workload, below the requested 3x target.

| Setting | Value |
|---|---:|
| OS | Windows 10 build 10.0.19045-SP0 |
| Python | 3.14.7 |
| CPU | Intel64 Family 6 Model 69 Stepping 1; 4 logical CPUs |
| Records | 20,000 |
| Fingerprint rounds per record | 5,000 |
| Process workers | 4 |
| Repetitions | 2 |

Command:

```powershell
python benchmarks/benchmark.py --records 20000 --workers 4 --rounds 5000 --runs 2
```

| Mode | Run times (s) | Median (s) | Median throughput | Peak RSS |
|---|---:|---:|---:|---:|
| Pure Python | 56.4644, 52.5312 | 54.4978 | 366.99 records/s | 38,465,536 bytes |
| Process pool | 27.5772, 26.6543 | 27.1157 | 737.58 records/s | 142,503,936 bytes |

Measured speedup: **2.0098x**. Normalized output equality: **true** (SHA-256 digests matched). The parallel path used roughly 104 MB more peak RSS, so it trades memory for throughput. These results are a single-machine sample and are not a portable performance guarantee.

The benchmark's process-tree CPU utilization sample was 22.78% for the baseline and 0.75% for the process pool. The latter is unreliable because short-lived worker processes can exit between samples; it should not be used to infer CPU saturation.

## Profile evidence

Command:

```powershell
python benchmarks/profile_normalizer.py --records 5000 --rounds 1000
```

Recorded total profile time was 2.106 s; `fingerprint_records` accounted for 2.098 s and `_fingerprint` for 2.071 s over 5,000 calls. This supports that fingerprint calculation is the hot path in that profile. `cProfile` adds overhead, so its absolute timings are not comparable to the benchmark above.

## Reproduce

Install the dev group, then run the commands above. The script prints measured timings, throughput, peak RSS, CPU sample, and an output-equality check for the current host. Run more repetitions and vary worker count for a decision-quality evaluation. The requested 3x result remains unmet by the recorded run.
