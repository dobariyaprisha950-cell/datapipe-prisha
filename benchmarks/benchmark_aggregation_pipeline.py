"""Compare exact SQLite row updates with batched Arrow state updates end to end."""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any
from unittest.mock import patch

import psutil
import pyarrow

from datapipe.config.schema import PipelineConfig
from datapipe.core.engine import run_pipeline
from datapipe.transforms import state as transform_state


def _write_input(path: Path, count: int) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        for index in range(count):
            row = {
                "id": index,
                "date": f"2026-01-{index % 28 + 1:02d}",
                "value": index % 97,
            }
            stream.write(json.dumps(row, separators=(",", ":")) + "\n")


def _build_config(source: Path, root: Path, batch_size: int) -> PipelineConfig:
    return PipelineConfig.model_validate(
        {
            "name": "aggregation-performance-comparison",
            "sources": [{"name": "input", "type": "jsonl", "path": str(source)}],
            "transforms": [
                {
                    "type": "aggregation",
                    "settings": {
                        "date_field": "date",
                        "metrics": {"value": "sum", "id": "count"},
                    },
                }
            ],
            "sink": {
                "type": "jsonl",
                "output_dir": str(root / "output"),
                "batch_size": batch_size,
            },
            "checkpoint": str(root / "checkpoint.json"),
            "queue_size": max(2, batch_size),
        }
    )


def _read_output(root: Path) -> list[dict[str, Any]]:
    rows = [
        json.loads(line)
        for path in (root / "output").glob("*=/part-*.jsonl")
        for line in path.read_text(encoding="utf-8").splitlines()
    ]
    return sorted(rows, key=lambda row: row["date"])


def _normalize(rows: list[dict[str, Any]]) -> str:
    return json.dumps(rows, sort_keys=True, separators=(",", ":"), allow_nan=True)


def _run_once(
    source: Path, run_root: Path, batch_size: int, accelerated: bool
) -> tuple[float, list[dict[str, Any]]]:
    config = _build_config(source, run_root, batch_size)
    started = time.perf_counter()
    if accelerated:
        asyncio.run(run_pipeline(config))
    else:
        # Reproduce the prior exact per-record DiskTransformProcessor path.
        with patch.object(transform_state, "_aggregate_grouped_arrow", return_value=None):
            asyncio.run(run_pipeline(config))
    return time.perf_counter() - started, _read_output(run_root)


def _peak_rss(
    mode: str, source: Path, run_root: Path, batch_size: int
) -> tuple[int, int]:
    """Measure absolute RSS in a fresh worker process for each implementation."""
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--batch-size",
        str(batch_size),
        "--_memory-mode",
        mode,
        "--_memory-source",
        str(source),
        "--_memory-root",
        str(run_root),
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
    parser.add_argument("--records", type=int, default=20_000)
    parser.add_argument("--batch-size", type=int, default=5_000)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--_memory-mode", choices=("python_exact", "pyarrow_state_batch"), help=argparse.SUPPRESS)
    parser.add_argument("--_memory-source", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--_memory-root", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if min(args.records, args.batch_size, args.runs) < 1:
        parser.error("records, batch-size, and runs must be positive")

    if args._memory_mode is not None:
        if args._memory_source is None or args._memory_root is None:
            parser.error("memory worker requires source and root")
        _run_once(
            args._memory_source,
            args._memory_root,
            args.batch_size,
            args._memory_mode == "pyarrow_state_batch",
        )
        return

    with tempfile.TemporaryDirectory(prefix="datapipe-aggregation-benchmark-") as temporary:
        root = Path(temporary)
        source = root / "input.jsonl"
        _write_input(source, args.records)
        implementations = {"python_exact": False, "pyarrow_state_batch": True}
        timings: dict[str, list[float]] = {name: [] for name in implementations}
        expected: str | None = None
        for run in range(args.runs):
            names = list(implementations)
            if run % 2:
                names.reverse()
            for name in names:
                accelerated = implementations[name]
                run_root = root / f"timed-{run}-{name}"
                elapsed, output = _run_once(source, run_root, args.batch_size, accelerated)
                normalized = _normalize(output)
                if expected is None:
                    expected = normalized
                elif normalized != expected:
                    raise RuntimeError(f"{name} output differs from exact pipeline output")
                timings[name].append(elapsed)

        medians = {name: statistics.median(values) for name, values in timings.items()}
        peaks = {
            name: _peak_rss(
                name,
                source,
                root / f"memory-{name}",
                args.batch_size,
            )
            for name in implementations
        }
        print(
            json.dumps(
                {
                    name: {
                        "runs_seconds": timings[name],
                        "median_seconds": medians[name],
                        "records_per_second": args.records / medians[name],
                        "peak_tree_rss_bytes": peaks[name][0],
                        "peak_tree_uss_bytes": peaks[name][1],
                    }
                    for name in implementations
                }
                | {
                    "speedup": medians["python_exact"] / medians["pyarrow_state_batch"],
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
                        "memory_measurement": "fresh subprocess tree peak RSS/USS; source file prepared by parent",
                    },
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
