"""Crash-recoverable output batches and checkpoint coordination.

The journal is authoritative for prepared and committed batches. Output files are
staged on the destination filesystem, then individually promoted with ``replace``.
Recovery completes a prepared batch before exposing its cursor in the checkpoint.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from datapipe.core.checkpoint import Checkpoint


class OutputCommitter:
    """Commit immutable output batches and recover interrupted promotions."""

    def __init__(
        self,
        output_root: Path,
        signature: str,
        checkpoint: Checkpoint | None,
        cursors: Mapping[str, Any],
    ) -> None:
        self.root = output_root.resolve()
        self.metadata = self.root / ".datapipe"
        self.staging = self.metadata / "staging"
        self.manifest_path = self.metadata / "manifest.json"
        self.signature = signature
        self.checkpoint = checkpoint
        self.cursors = dict(cursors)
        self.root.mkdir(parents=True, exist_ok=True)
        self.metadata.mkdir(parents=True, exist_ok=True)
        if self.manifest_path.exists():
            self.manifest = self._read_manifest()
        else:
            self.manifest = {
                "version": 1,
                "signature": signature,
                "next_sequence": 1,
                "batches": {},
                "runs": [],
            }
            legacy = self.root / "dead-letter.jsonl"
            if legacy.exists():
                shutil.copyfile(legacy, self.metadata / "dead-letter-legacy.jsonl")
            self._persist()
        self.recover()

    def _read_manifest(self) -> dict[str, Any]:
        try:
            value = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"invalid output manifest {self.manifest_path}: {exc}") from exc
        if (
            not isinstance(value, dict)
            or value.get("version") != 1
            or value.get("signature") != self.signature
            or not isinstance(value.get("batches"), dict)
            or not isinstance(value.get("next_sequence"), int)
        ):
            raise ValueError("output manifest format or pipeline configuration does not match")
        value.setdefault("runs", [])
        if not isinstance(value["runs"], list):
            raise TypeError("output manifest runs must be a list")
        return value

    def _persist(self) -> None:
        self.metadata.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=".manifest-", dir=self.metadata, text=True)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(self.manifest, stream, sort_keys=True, separators=(",", ":"))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.manifest_path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def _target(self, relative: str) -> Path:
        target = (self.root / relative).resolve()
        if target != self.root and self.root not in target.parents:
            raise ValueError("manifest output path escaped configured output directory")
        return target

    def _publish(self, batch_id: str, batch: dict[str, Any]) -> None:
        stage_root = self.staging / batch_id
        for item in batch["files"]:
            staged = self._target(item["staged"])
            target = self._target(item["target"])
            if target.exists():
                if staged.exists():
                    staged.unlink()
                continue
            if not staged.exists():
                raise ValueError(f"cannot recover batch {batch_id}: staged file is missing")
            target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(staged, target)
        batch["status"] = "committed"
        batch["stage"] = str(stage_root.relative_to(self.root))
        self._persist()
        self._advance_checkpoint(batch["cursors"])
        shutil.rmtree(stage_root, ignore_errors=True)

    def _advance_checkpoint(self, updates: Mapping[str, Any]) -> None:
        self.cursors.update(updates)
        if self.checkpoint is not None:
            self.checkpoint.save(self.cursors)

    def recover(self) -> None:
        """Finish prepared transactions and reconcile checkpoint state."""
        batches = self.manifest["batches"]
        for batch_id, batch in sorted(
            batches.items(), key=lambda item: item[1].get("sequence", 0)
        ):
            if batch.get("status") == "prepared":
                self._publish(batch_id, batch)
            elif batch.get("status") == "committed":
                self._advance_checkpoint(batch.get("cursors", {}))
            else:
                raise ValueError(f"invalid batch state for {batch_id}")
        self.staging.mkdir(parents=True, exist_ok=True)
        for orphan in self.staging.iterdir():
            if orphan.is_dir():
                shutil.rmtree(orphan, ignore_errors=True)
            else:
                orphan.unlink(missing_ok=True)
        interrupted = False
        for run in self.manifest["runs"]:
            if run.get("status") == "running":
                run["status"] = "interrupted"
                run["finished_at"] = datetime.now(UTC).isoformat()
                interrupted = True
        if interrupted:
            self._persist()
        self.materialize_dead_letters()

    def begin_run(self, run_id: str) -> None:
        self.manifest["runs"].append(
            {"run_id": run_id, "status": "running", "started_at": datetime.now(UTC).isoformat()}
        )
        self._persist()

    def complete_run(self, run_id: str) -> None:
        for run in reversed(self.manifest["runs"]):
            if run.get("run_id") == run_id:
                run["status"] = "completed"
                run["finished_at"] = datetime.now(UTC).isoformat()
                self._persist()
                return
        raise ValueError(f"run {run_id} is missing from the output manifest")

    def commit_batch(
        self,
        records: Sequence[Mapping[str, Any]],
        dead_letters: Sequence[Mapping[str, Any]],
        cursors: Mapping[str, Any],
        sink: Any,
        state_updates: Sequence[Mapping[str, Any]] = (),
    ) -> list[Path]:
        """Stage output, journal intent, promote files, then advance cursors."""
        sequence = self.manifest["next_sequence"]
        batch_id = f"batch-{sequence:012d}"
        stage_root = self.staging / batch_id
        shutil.rmtree(stage_root, ignore_errors=True)
        stage_root.mkdir(parents=True, exist_ok=True)
        try:
            staged_paths = sink.__class__(str(stage_root), sink.partition_by).write(
                records, batch_id=batch_id
            )
            files = [
                {
                    "staged": str(path.relative_to(self.root)),
                    "target": str(
                        (self.root / path.relative_to(stage_root)).relative_to(self.root)
                    ),
                }
                for path in staged_paths
            ]
            if dead_letters:
                staged_dead = stage_root / "dead-letter.jsonl"
                with staged_dead.open("w", encoding="utf-8", newline="\n") as stream:
                    for row in dead_letters:
                        stream.write(
                            json.dumps(dict(row), default=str, separators=(",", ":")) + "\n"
                        )
                    stream.flush()
                    os.fsync(stream.fileno())
                files.append(
                    {
                        "staged": str(staged_dead.relative_to(self.root)),
                        "target": str(
                            (self.metadata / "dead-letter-batches" / f"{batch_id}.jsonl")
                            .relative_to(self.root)
                        ),
                    }
                )
            if state_updates:
                staged_state = stage_root / "transform-state.jsonl"
                with staged_state.open("w", encoding="utf-8", newline="\n") as stream:
                    for update in state_updates:
                        stream.write(
                            json.dumps(dict(update), allow_nan=True, separators=(",", ":")) + "\n"
                        )
                    stream.flush()
                    os.fsync(stream.fileno())
                files.append(
                    {
                        "staged": str(staged_state.relative_to(self.root)),
                        "target": str(
                            (self.metadata / "transform-state" / f"{batch_id}.jsonl")
                            .relative_to(self.root)
                        ),
                    }
                )

            batch = {
                "sequence": sequence,
                "status": "prepared",
                "cursors": dict(cursors),
                "files": files,
            }
            self.manifest["batches"][batch_id] = batch
            self.manifest["next_sequence"] = sequence + 1
            self._persist()
        except BaseException:
            # Keep files only if a prepared journal entry actually reached disk.
            journaled = False
            try:
                saved = json.loads(self.manifest_path.read_text(encoding="utf-8"))
                journaled = saved.get("batches", {}).get(batch_id, {}).get("status") == "prepared"
            except (OSError, json.JSONDecodeError, AttributeError):
                pass
            if not journaled:
                shutil.rmtree(stage_root, ignore_errors=True)
            raise
        self._publish(batch_id, batch)
        return [
            self._target(item["target"])
            for item in files
            if "dead-letter-batches" not in item["target"]
            and "transform-state" not in item["target"]
        ]

    def committed_transform_updates(self) -> Iterable[dict[str, Any]]:
        """Stream journal-committed transform state updates in batch order."""
        batches = sorted(
            self.manifest["batches"].items(), key=lambda item: item[1].get("sequence", 0)
        )
        for batch_id, batch in batches:
            if batch.get("status") != "committed":
                continue
            state_file = self.metadata / "transform-state" / f"{batch_id}.jsonl"
            if not state_file.exists():
                continue
            with state_file.open(encoding="utf-8") as stream:
                for line_number, line in enumerate(stream, start=1):
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError as exc:
                        raise ValueError(
                            f"invalid transform state in {state_file}:{line_number}: {exc}"
                        ) from exc
                    if not isinstance(event, dict):
                        raise TypeError(
                            f"invalid transform state in {state_file}:{line_number}: expected object"
                        )
                    yield event

    def has_committed_transform_state(self) -> bool:
        """Whether this journal has a committed transform-state generation."""
        for batch_id, batch in self.manifest["batches"].items():
            if batch.get("status") != "committed":
                continue
            if (self.metadata / "transform-state" / f"{batch_id}.jsonl").is_file():
                return True
        return False

    def materialize_dead_letters(self) -> None:
        """Rebuild the public aggregate from committed immutable batch files."""
        parts: list[Path] = []
        legacy = self.metadata / "dead-letter-legacy.jsonl"
        if legacy.exists():
            parts.append(legacy)
        for batch_id, batch in sorted(
            self.manifest["batches"].items(), key=lambda item: item[1].get("sequence", 0)
        ):
            if batch.get("status") != "committed":
                continue
            target = self.metadata / "dead-letter-batches" / f"{batch_id}.jsonl"
            if target.exists():
                parts.append(target)
        public = self.root / "dead-letter.jsonl"
        if not parts:
            return
        fd, temporary = tempfile.mkstemp(prefix=".dead-letter-", dir=self.root, text=True)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as output:
                for part in parts:
                    with part.open(encoding="utf-8") as source:
                        shutil.copyfileobj(source, output)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, public)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
