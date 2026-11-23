"""Benchmark exact Python daily aggregation against its bounded Arrow fast path."""

from __future__ import annotations

import argparse
import gc
import json
import platform
import statistics
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import psutil
import pyarrow

from datapipe.transforms.aggregation import _aggregate_daily_python, aggregate_daily


def _normalize(rows: list[dict[str, Any]]) -> str:
    return json.dumps(rows, sort_keys=True, separators=(",", ":"), allow_nan=True)


def _measure(
    operation: Callable[[], list[dict[str, Any]]]
) -> tuple[float, list[dict[str, Any]]]:
    gc.collect()
    started = time.perf_counter()
    result = operation()
    return time.perf_counter() - started, result


def _measure_peak_rss(
    mode: str, records: int, batch_size: int
) -> tuple[int, int]:
    """Measure RSS in a fresh subprocess, isolated from allocator retention."""
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--records",
        str(records),
        "--batch-size",
        str(batch_size),
        "--_memory-mode",
        mode,
    ]
    child = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    root = psutil.Process(child.pid)
    tracked: dict[int, psutil.Process] = {root.pid: root}
    rss_samples: list[int] = []
    uss_samples: list[int] = []
    while True:
        for process in tuple(tracked.values()):
            try:
                tracked.update({item.pid: item for item in process.children(recursive=True)})
            except psutil.Error:
                continue
        tree_rss = 0
        tree_uss = 0
        for process in tracked.values():
            try:
                if process.is_running():
                    memory = process.memory_full_info()
                    tree_rss += memory.rss
                    tree_uss += memory.uss
            except psutil.Error:
                continue
        rss_samples.append(tree_rss)
        uss_samples.append(tree_uss)
        child_done = child.poll() is not None
        descendants_alive = False
        for process in tracked.values():
            if process.pid == root.pid:
                continue
            try:
                descendants_alive = process.is_running()
            except psutil.Error:
                continue
            if descendants_alive:
                break
        if child_done and not descendants_alive:
            break
        time.sleep(0.002)
    return_code = child.wait()
    if return_code != 0:
        raise RuntimeError(f"memory worker {mode} exited with code {child.returncode}")
    return max(rss_samples, default=0), max(uss_samples, default=0)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=int, default=50_000)
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument(
        "--_memory-mode",
        choices=("python_exact", "pyarrow_batched"),
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args()
    if min(args.records, args.batch_size, args.runs) < 1:
        parser.error("records, batch-size, and runs must be positive")

    records = [
        {
            "date": f"2026-01-{index % 28 + 1:02d}",
            "amount": index % 97,
            "low": index % 11,
            "high": index % 101,
        }
        for index in range(args.records)
    ]
    metrics = {"amount": "sum", "rows": "count", "low": "min", "high": "max"}
    if args._memory_mode is not None:
        if args._memory_mode == "python_exact":
            _aggregate_daily_python(records, "date", metrics)
        else:
            aggregate_daily(records, "date", metrics, batch_size=args.batch_size)
        return
    baseline = lambda: _aggregate_daily_python(records, "date", metrics)
    accelerated = lambda: aggregate_daily(
        records, "date", metrics, batch_size=args.batch_size
    )
    expected = baseline()
    if _normalize(accelerated()) != _normalize(expected):
        raise RuntimeError("Arrow and pure-Python aggregation outputs differ")

    operations: dict[str, Callable[[], list[dict[str, Any]]]] = {
        "python_exact": baseline,
        "pyarrow_batched": accelerated,
    }
    for operation in operations.values():
        operation()  # warm each implementation outside the timed samples

    timings: dict[str, list[float]] = {name: [] for name in operations}
    for run in range(args.runs):
        names = list(operations)
        if run % 2:
            names.reverse()
        for name in names:
            elapsed, output = _measure(operations[name])
            if _normalize(output) != _normalize(expected):
                raise RuntimeError(f"{name} output changed during benchmark")
            timings[name].append(elapsed)

    medians = {name: statistics.median(samples) for name, samples in timings.items()}
    peak_rss = {
        name: _measure_peak_rss(name, args.records, args.batch_size) for name in operations
    }
    print(
        json.dumps(
            {
                name: {
                    "runs_seconds": timings[name],
                    "median_seconds": medians[name],
                    "median_records_per_second": args.records / medians[name],
                    "peak_tree_rss_bytes": peak_rss[name][0],
                    "peak_tree_uss_bytes": peak_rss[name][1],
                }
                for name in operations
            }
            | {
                "speedup": medians["python_exact"] / medians["pyarrow_batched"],
                "normalized_outputs_equal": True,
                "environment": {
                    "python": platform.python_version(),
                    "pyarrow": pyarrow.__version__,
                    "os": platform.platform(),
                    "cpu": platform.processor(),
                    "logical_cpus": psutil.cpu_count(logical=True),
                    "records": args.records,
                    "batch_size": args.batch_size,
                    "runs": args.runs,
                    "memory_measurement": "fresh subprocess tree peak RSS/USS; includes imports and input creation",
                    "dates": 28,
                    "metrics": metrics,
                },
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
