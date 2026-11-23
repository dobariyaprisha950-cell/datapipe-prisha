"""Measure the fingerprint transform through the complete streaming pipeline."""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import random
import statistics
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

import psutil

from datapipe.config.schema import PipelineConfig
from datapipe.core.engine import run_pipeline


def _sample_rss(process: psutil.Process, peak: list[int], stop: threading.Event) -> None:
    while not stop.is_set():
        total = 0
        for child in [process, *process.children(recursive=True)]:
            try:
                total += child.memory_info().rss
            except psutil.Error:
                continue
        peak[0] = max(peak[0], total)
        stop.wait(0.01)


def _read_normalized_rows(output: Path) -> list[dict[str, Any]]:
    rows = [
        json.loads(line)
        for part in output.glob("date=*/*.jsonl")
        for line in part.read_text(encoding="utf-8").splitlines()
    ]
    return sorted(rows, key=lambda row: int(row["id"]))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=int, default=5_000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--rounds", type=int, default=5_000)
    parser.add_argument("--batch-size", type=int, default=1_000)
    parser.add_argument("--runs", type=int, default=3)
    args = parser.parse_args()
    if min(args.records, args.workers, args.rounds, args.batch_size, args.runs) < 1:
        parser.error("records, workers, rounds, batch-size, and runs must be positive")

    work_root = Path("work")
    work_root.mkdir(exist_ok=True)
    modes = ("sequential", "thread", "process")
    timings: dict[str, list[float]] = {mode: [] for mode in modes}
    peak_rss: dict[str, list[int]] = {mode: [] for mode in modes}
    expected: list[dict[str, Any]] | None = None
    process = psutil.Process()

    with tempfile.TemporaryDirectory(prefix="datapipe-fingerprint-e2e-", dir=work_root) as temp:
        root = Path(temp)
        source = root / "input.jsonl"
        with source.open("w", encoding="utf-8", newline="\n") as stream:
            for index in range(args.records):
                stream.write(
                    json.dumps(
                        {"id": index, "date": "2026-01-01", "value": index % 997},
                        separators=(",", ":"),
                    )
                    + "\n"
                )

        def build_config(mode: str, output: Path) -> PipelineConfig:
            return PipelineConfig.model_validate(
                {
                    "name": "fingerprint-e2e-benchmark",
                    "sources": [{"name": "input", "type": "jsonl", "path": str(source)}],
                    "transforms": [
                        {
                            "type": "cpu_fingerprint",
                            "settings": {
                                "field": "value",
                                "output_field": "fingerprint",
                                "rounds": args.rounds,
                                "execution": mode,
                                "workers": args.workers,
                            },
                        }
                    ],
                    "sink": {
                        "type": "jsonl",
                        "output_dir": str(output),
                        "batch_size": args.batch_size,
                    },
                }
            )

        order_rng = random.Random(0xDA7A)
        for run in range(args.runs):
            run_modes = list(modes)
            order_rng.shuffle(run_modes)
            for mode in run_modes:
                output = root / f"{mode}-{run}"
                config = build_config(mode, output)
                started = time.perf_counter()
                report = asyncio.run(run_pipeline(config))
                elapsed = time.perf_counter() - started
                rows = _read_normalized_rows(output)
                if report["records_written"] != args.records or len(rows) != args.records:
                    raise RuntimeError(f"{mode} pipeline wrote an unexpected record count")
                if expected is None:
                    expected = rows
                elif rows != expected:
                    raise RuntimeError(f"{mode} output differed from the normalized baseline")
                timings[mode].append(elapsed)

        # Keep psutil sampling out of the timed measurements. These second passes
        # are memory-only observations and must not be used for timing claims.
        for mode in modes:
            output = root / f"memory-{mode}"
            config = build_config(mode, output)
            peak = [0]
            stop = threading.Event()
            sampler = threading.Thread(
                target=_sample_rss, args=(process, peak, stop), daemon=True
            )
            sampler.start()
            try:
                report = asyncio.run(run_pipeline(config))
            finally:
                stop.set()
                sampler.join()
            rows = _read_normalized_rows(output)
            if report["records_written"] != args.records or len(rows) != args.records:
                raise RuntimeError(f"{mode} memory pass wrote an unexpected record count")
            if expected is not None and rows != expected:
                raise RuntimeError(f"{mode} memory pass output differed from timed output")
            peak_rss[mode].append(peak[0])

    medians = {mode: statistics.median(samples) for mode, samples in timings.items()}
    print(
        json.dumps(
            {
                mode: {
                    "runs_seconds": timings[mode],
                    "median_seconds": medians[mode],
                    "median_records_per_second": args.records / medians[mode],
                    "peak_rss_bytes_including_children": max(peak_rss[mode]),
                }
                for mode in modes
            }
            | {
                "speedup_vs_sequential": {
                    mode: medians["sequential"] / medians[mode] for mode in modes
                },
                "output_equal": True,
                "environment": {
                    "python": platform.python_version(),
                    "os": platform.platform(),
                    "cpu": platform.processor(),
                    "logical_cpus": psutil.cpu_count(logical=True),
                    "workers": args.workers,
                    "records": args.records,
                    "runs": args.runs,
                    "rounds_per_record": args.rounds,
                },
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
