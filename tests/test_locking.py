import asyncio
import subprocess
import sys
import time
from pathlib import Path

import pytest

from datapipe.config.schema import PipelineConfig
from datapipe.core.engine import run_pipeline
from datapipe.core.locking import OutputDirectoryLock, OutputLockTimeout


def test_lock_rejects_invalid_timeout_and_duplicate_acquisition(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="must not be negative"):
        OutputDirectoryLock(tmp_path / "output", timeout=-1)
    lock = OutputDirectoryLock(tmp_path / "output")
    lock.acquire()
    with pytest.raises(RuntimeError, match="already acquired"):
        lock.acquire()
    lock.release()
    lock.release()


def test_lock_timeout_and_independent_roots(tmp_path: Path) -> None:
    first = OutputDirectoryLock(tmp_path / "shared")
    first.acquire()
    try:
        with pytest.raises(OutputLockTimeout, match="timed out"):
            OutputDirectoryLock(tmp_path / "shared", timeout=0.08).acquire()
        with OutputDirectoryLock(tmp_path / "independent", timeout=0.08):
            pass
    finally:
        first.release()


def test_separate_process_cannot_acquire_a_held_output_lock(tmp_path: Path) -> None:
    root = tmp_path / "shared-output"
    held = OutputDirectoryLock(root)
    held.acquire()
    code = f"""
from pathlib import Path
from datapipe.core.locking import OutputDirectoryLock, OutputLockTimeout
lock = OutputDirectoryLock(Path({str(root)!r}), timeout=0.15)
try:
    lock.acquire()
except OutputLockTimeout:
    raise SystemExit(7)
raise SystemExit(0)
"""
    try:
        result = subprocess.run(
            [sys.executable, "-c", code], check=False, capture_output=True, timeout=5
        )
        assert result.returncode == 7, result.stderr.decode(errors="replace")
    finally:
        held.release()


def test_process_termination_releases_kernel_lock(tmp_path: Path) -> None:
    root = tmp_path / "output"
    ready = tmp_path / "ready"
    code = (
        "import time; from pathlib import Path; "
        "from datapipe.core.locking import OutputDirectoryLock; "
        f"lock=OutputDirectoryLock(Path({str(root)!r})); lock.acquire(); "
        f"Path({str(ready)!r}).write_text('ready'); time.sleep(30)"
    )
    process = subprocess.Popen([sys.executable, "-c", code])
    try:
        deadline = time.monotonic() + 10
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert ready.exists(), "child process did not acquire lock"
        process.kill()
        process.wait(timeout=5)
        with OutputDirectoryLock(root, timeout=1):
            pass
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)


@pytest.mark.asyncio
async def test_pipeline_cancellation_while_waiting_for_lock_is_prompt(tmp_path: Path) -> None:
    source = tmp_path / "input.jsonl"
    source.write_text('{"id":"one","date":"2026-01-01"}\n', encoding="utf-8")
    output = tmp_path / "output"
    config = PipelineConfig.model_validate(
        {
            "name": "lock-cancel",
            "sources": [{"name": "file", "type": "jsonl", "path": str(source)}],
            "sink": {"type": "jsonl", "output_dir": str(output)},
        }
    )
    held = OutputDirectoryLock(output)
    held.acquire()
    task = asyncio.create_task(run_pipeline(config))
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1)
    held.release()
    result = await run_pipeline(config)
    assert result["records_written"] == 1
