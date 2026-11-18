import json
from pathlib import Path
from typing import Any

import pytest

from datapipe.core.commit import OutputCommitter


def _committer(root: Path) -> OutputCommitter:
    return OutputCommitter(root, "edge-test", None, {})


def test_legacy_dead_letter_is_preserved_when_journal_is_created(tmp_path: Path) -> None:
    legacy = tmp_path / "dead-letter.jsonl"
    legacy.write_text('{"error":"old"}\n', encoding="utf-8")
    _committer(tmp_path)
    preserved = tmp_path / ".datapipe" / "dead-letter-legacy.jsonl"
    assert preserved.read_text(encoding="utf-8") == '{"error":"old"}\n'


@pytest.mark.parametrize(
    "manifest",
    [
        {"version": 1, "signature": "other", "next_sequence": 1, "batches": {}},
        {"version": 1, "signature": "edge-test", "next_sequence": 1, "batches": {}, "runs": {}},
    ],
)
def test_malformed_or_mismatched_manifest_is_rejected(tmp_path: Path, manifest: dict[str, Any]) -> None:
    metadata = tmp_path / ".datapipe"
    metadata.mkdir()
    (metadata / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises((TypeError, ValueError), match="manifest|runs"):
        _committer(tmp_path)


def test_manifest_target_rejects_path_escape(tmp_path: Path) -> None:
    committer = _committer(tmp_path)
    with pytest.raises(ValueError, match="escaped"):
        committer._target("../outside.txt")


def test_publish_is_idempotent_when_destination_was_already_promoted(tmp_path: Path) -> None:
    committer = _committer(tmp_path)
    batch_id = "batch-test"
    stage_root = committer.staging / batch_id
    stage_root.mkdir(parents=True)
    staged = stage_root / "part.jsonl"
    staged.write_text("staged\n", encoding="utf-8")
    target = tmp_path / "partition" / "part.jsonl"
    target.parent.mkdir()
    target.write_text("committed\n", encoding="utf-8")
    batch: dict[str, Any] = {
        "status": "prepared",
        "cursors": {},
        "files": [{"staged": str(staged.relative_to(tmp_path)), "target": "partition/part.jsonl"}],
    }
    committer.manifest["batches"][batch_id] = batch
    committer._publish(batch_id, batch)
    assert batch["status"] == "committed"
    assert target.read_text(encoding="utf-8") == "committed\n"
    assert not stage_root.exists()


def test_publish_refuses_missing_prepared_stage(tmp_path: Path) -> None:
    committer = _committer(tmp_path)
    batch: dict[str, Any] = {
        "status": "prepared",
        "cursors": {},
        "files": [{"staged": ".datapipe/staging/lost/file", "target": "date=x/file"}],
    }
    with pytest.raises(ValueError, match="staged file is missing"):
        committer._publish("lost", batch)


def test_recovery_rejects_unknown_state_and_cleans_orphan_stage_paths(tmp_path: Path) -> None:
    committer = _committer(tmp_path)
    stage = committer.staging / "orphan-dir"
    stage.mkdir()
    (stage / "partial").write_text("unfinished", encoding="utf-8")
    (committer.staging / "orphan-file").write_text("unfinished", encoding="utf-8")
    committer.recover()
    assert list(committer.staging.iterdir()) == []
    committer.manifest["batches"]["bad"] = {"status": "unknown", "sequence": 1}
    with pytest.raises(ValueError, match="invalid batch state"):
        committer.recover()


def test_complete_missing_run_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="missing from the output manifest"):
        _committer(tmp_path).complete_run("not-present")


def test_transform_state_replay_skips_uncommitted_and_missing_deltas(tmp_path: Path) -> None:
    committer = _committer(tmp_path)
    state_dir = committer.metadata / "transform-state"
    state_dir.mkdir()
    (state_dir / "committed.jsonl").write_text('{"op":"version","version":1}\n')
    committer.manifest["batches"] = {
        "prepared": {"status": "prepared", "sequence": 1},
        "committed-without-state": {"status": "committed", "sequence": 2},
        "committed": {"status": "committed", "sequence": 3},
    }
    assert list(committer.committed_transform_updates()) == [{"op": "version", "version": 1}]
    assert committer.has_committed_transform_state()


def test_transform_state_replay_rejects_non_object_event(tmp_path: Path) -> None:
    committer = _committer(tmp_path)
    state_dir = committer.metadata / "transform-state"
    state_dir.mkdir()
    (state_dir / "bad.jsonl").write_text('[1, 2]\n')
    committer.manifest["batches"] = {
        "bad": {"status": "committed", "sequence": 1},
    }
    with pytest.raises(TypeError, match="expected object"):
        list(committer.committed_transform_updates())
