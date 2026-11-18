"""Reproducible normalizer benchmark; results are printed, never prefilled."""

import argparse
import hashlib
import json
import platform
import statistics
import threading
import time

import psutil

from datapipe.transforms.parallel import fingerprint_records, fingerprint_records_parallel


def _sample_process_tree(
    process: psutil.Process, stats: dict[str, float], stop: threading.Event
) -> None:
    final_sample = False
    while True:
        total = 0
        cpu_total = 0.0
        for child in [process, *process.children(recursive=True)]:
            try:
                memory = child.memory_info().rss
                cpu = child.cpu_times()
                total += memory
                cpu_total += cpu.user + cpu.system
            except psutil.Error:
                pass
        stats["peak_rss"] = max(stats["peak_rss"], total)
        if "cpu_start" not in stats:
            stats["cpu_start"] = cpu_total
        stats["cpu_seconds"] = max(0.0, cpu_total - stats["cpu_start"])
        if final_sample:
            return
        final_sample = stop.wait(0.25)


def _digest(rows: list[dict[str, object]]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(json.dumps(row, sort_keys=True, separators=(",", ":")).encode())
        digest.update(b"\n")
    return digest.hexdigest()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--records", type=int, default=100_000)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--rounds", type=int, default=1000)
    a = p.parse_args()
    if a.records <= 0 or a.workers <= 0 or a.runs <= 0 or a.rounds <= 0:
        p.error("records, workers, runs and rounds must be positive")
    rows = [{"value": float(i % 997)} for i in range(a.records)]
    modes = ("sequential", "thread", "process")
    timings: dict[str, list[float]] = {name: [] for name in modes}
    peak_rss: dict[str, list[float]] = {name: [] for name in modes}
    cpu_utilization: dict[str, list[float]] = {name: [] for name in modes}
    equality = True
    process = psutil.Process()
    for _ in range(a.runs):
        baseline = fingerprint_records(rows, "value", "fingerprint", a.rounds)
        baseline_digest = _digest(baseline)
        del baseline
        fns = {
            "sequential": lambda: fingerprint_records(rows, "value", "fingerprint", a.rounds),
            "thread": lambda: fingerprint_records_parallel(
                rows, "value", "fingerprint", a.rounds, a.workers, "thread"
            ),
            "process": lambda: fingerprint_records_parallel(
                rows, "value", "fingerprint", a.rounds, a.workers, "process"
            ),
        }
        for name, fn in fns.items():
            sample: dict[str, float] = {"peak_rss": 0.0, "cpu_seconds": 0.0}
            stop = threading.Event()
            sampler = threading.Thread(
                target=_sample_process_tree, args=(process, sample, stop), daemon=True
            )
            sampler.start()
            start = time.perf_counter()
            output = fn()
            elapsed = time.perf_counter() - start
            equality = equality and baseline_digest == _digest(output)
            stop.set()
            sampler.join()
            del output
            timings[name].append(elapsed)
            peak_rss[name].append(sample["peak_rss"])
            core_count = psutil.cpu_count(logical=True) or 1
            cpu_utilization[name].append(sample["cpu_seconds"] / elapsed / core_count * 100)
    if not equality:
        raise RuntimeError("an execution strategy differed from sequential output")
    medians = {name: statistics.median(values) for name, values in timings.items()}
    results = {
        name: {
            "runs_seconds": timings[name],
            "median_seconds": medians[name],
            "median_records_per_second": a.records / medians[name],
            "peak_rss_bytes_including_children": max(peak_rss[name]),
            "median_cpu_utilization_percent_of_logical_capacity": statistics.median(
                cpu_utilization[name]
            ),
        }
        for name in timings
    }
    results["speedup_vs_sequential"] = {
        mode: medians["sequential"] / medians[mode] for mode in modes
    }
    results["output_equal"] = equality
    results["environment"] = {
        "python": platform.python_version(),
        "os": platform.platform(),
        "cpu": platform.processor(),
        "logical_cpus": psutil.cpu_count(logical=True),
        "workers": a.workers,
        "records": a.records,
        "runs": a.runs,
        "rounds_per_record": a.rounds,
    }
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
