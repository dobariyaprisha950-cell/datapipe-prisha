import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest


@pytest.mark.skipif(os.name == "nt", reason="Windows cannot deliver catchable POSIX SIGTERM/SIGINT")
@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM])
def test_cli_handles_real_termination_signal_and_keeps_committed_batches(
    tmp_path: Path, signum: signal.Signals
) -> None:
    source = tmp_path / "large.jsonl"
    with source.open("w", encoding="utf-8") as stream:
        for index in range(250_000):
            stream.write(json.dumps({"id": index, "date": "2026-01-01"}) + "\n")
    output = tmp_path / "out"
    config = tmp_path / "pipeline.toml"
    config.write_text(
        f'name="signal-test"\n'
        f'checkpoint="{(tmp_path / "checkpoint.json").as_posix()}"\n'
        f'[[sources]]\nname="input"\ntype="jsonl"\npath="{source.as_posix()}"\n'
        f'[sink]\ntype="jsonl"\noutput_dir="{output.as_posix()}"\n'
        "batch_size=25\n",
        encoding="utf-8",
    )
    executable = Path(sys.executable).with_name("datapipe")
    process = subprocess.Popen(
        [str(executable), "run", str(config)],
        cwd=Path(__file__).parents[1],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    manifest = output / ".datapipe" / "manifest.json"
    deadline = time.monotonic() + 20
    committed = False
    while time.monotonic() < deadline and process.poll() is None:
        if manifest.exists():
            value = json.loads(manifest.read_text(encoding="utf-8"))
            committed = any(batch["status"] == "committed" for batch in value["batches"].values())
            if committed:
                break
        time.sleep(0.02)
    assert committed, "CLI did not commit a batch before the signal timeout"
    process.send_signal(signum)
    stdout, stderr = process.communicate(timeout=30)
    expected = 128 + int(signum)
    assert process.returncode == expected, (stdout, stderr)
    checkpoint = json.loads((tmp_path / "checkpoint.json").read_text(encoding="utf-8"))
    assert checkpoint["cursors"]["input"] > 0
    assert list((output / ".datapipe" / "staging").iterdir()) == []
