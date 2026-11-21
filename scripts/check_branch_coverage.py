"""Fail CI when core/ branch coverage is below the configured percentage."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def core_branch_percentage(report_path: Path) -> tuple[int, int, float]:
    """Return covered branch arcs, total arcs, and percentage for datapipe.core."""
    report: dict[str, Any] = json.loads(report_path.read_text(encoding="utf-8"))
    files = report.get("files")
    if not isinstance(files, dict):
        raise TypeError("coverage JSON must contain a files object")
    selected = [
        details
        for name, details in files.items()
        if str(name).replace("\\", "/").startswith("src/datapipe/core/")
    ]
    total = sum(int(item["summary"]["num_branches"]) for item in selected)
    missing = sum(int(item["summary"]["missing_branches"]) for item in selected)
    if total == 0:
        raise ValueError("coverage JSON contains no branch data for src/datapipe/core")
    covered = total - missing
    return covered, total, covered / total * 100


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--minimum", type=float, default=85.0)
    args = parser.parse_args()
    try:
        covered, total, percentage = core_branch_percentage(args.report)
    except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        print(f"coverage gate could not read report: {exc}", file=sys.stderr)
        return 2
    print(f"core branch coverage: {covered}/{total} = {percentage:.2f}%")
    if percentage < args.minimum:
        print(f"required: at least {args.minimum:.2f}%", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
