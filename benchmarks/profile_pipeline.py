"""Profile one end-to-end CPU-fingerprint pipeline run with cProfile."""

from __future__ import annotations

import argparse
import asyncio
import cProfile
import json
import pstats
import tempfile
import time
from pathlib import Path

from datapipe.config.schema import PipelineConfig
from datapipe.core.engine import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=int, default=5_000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--rounds", type=int, default=5_000)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument(
        "--execution", choices=("sequential", "thread", "process"), default="process"
    )
    args = parser.parse_args()
    if min(args.records, args.workers, args.rounds, args.batch_size) < 1:
        parser.error("records, workers, rounds, and batch-size must be positive")

    work_root = Path("work")
    work_root.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="datapipe-fingerprint-profile-", dir=work_root) as temp:
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
        config = PipelineConfig.model_validate(
            {
                "name": "fingerprint-profile",
                "sources": [{"name": "input", "type": "jsonl", "path": str(source)}],
                "transforms": [
                    {
                        "type": "cpu_fingerprint",
                        "settings": {
                            "field": "value",
                            "output_field": "fingerprint",
                            "rounds": args.rounds,
                            "execution": args.execution,
                            "workers": args.workers,
                        },
                    }
                ],
                "sink": {
                    "type": "jsonl",
                    "output_dir": str(root / "output"),
                    "batch_size": args.batch_size,
                },
            }
        )
        profiler = cProfile.Profile()
        started = time.perf_counter()
        profiler.enable()
        report = asyncio.run(run_pipeline(config))
        profiler.disable()
        print(
            json.dumps(
                {
                    "execution": args.execution,
                    "records_written": report["records_written"],
                    "elapsed_seconds_with_profiler": time.perf_counter() - started,
                    "profile_scope": "parent process; process-pool worker CPU is not instrumented",
                },
                indent=2,
            )
        )
        pstats.Stats(profiler).strip_dirs().sort_stats("cumulative").print_stats(35)


if __name__ == "__main__":
    main()
