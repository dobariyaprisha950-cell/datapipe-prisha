import json
from pathlib import Path

import pytest

from datapipe.core.checkpoint import Checkpoint


def test_missing_checkpoint_is_empty(tmp_path: Path) -> None:
    assert Checkpoint(tmp_path / "state.json").load() == {}


def test_checkpoint_save_and_reload_is_atomic(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    checkpoint = Checkpoint(path)
    checkpoint.save({"csv": 42, "api": "next-page"})
    assert checkpoint.load() == {"csv": 42, "api": "next-page"}
    assert list(tmp_path.glob(".checkpoint-*")) == []


@pytest.mark.parametrize("contents", ["{broken", "[]", "null"])
def test_corrupt_checkpoint_rejected(tmp_path: Path, contents: str) -> None:
    path = tmp_path / "state.json"
    path.write_text(contents, encoding="utf-8")
    with pytest.raises((json.JSONDecodeError, TypeError)):
        Checkpoint(path).load()


def test_checkpoint_rejects_changed_pipeline_configuration(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    Checkpoint(path, "config-a").save({"source": 10})
    assert Checkpoint(path, "config-a").load() == {"source": 10}
    with pytest.raises(ValueError, match="configuration does not match"):
        Checkpoint(path, "config-b").load()


def test_signed_checkpoint_requires_cursor_object(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    path.write_text(json.dumps({"version": 1, "signature": "expected", "cursors": []}))
    with pytest.raises(TypeError, match="cursors must be an object"):
        Checkpoint(path, "expected").load()


def test_failed_checkpoint_replace_removes_temporary_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import datapipe.core.checkpoint as checkpoint_module

    def fail_replace(_source: str, _target: Path) -> None:
        raise OSError("injected replace failure")

    monkeypatch.setattr(checkpoint_module.os, "replace", fail_replace)
    with pytest.raises(OSError, match="injected replace failure"):
        Checkpoint(tmp_path / "state.json").save({"cursor": 1})
    assert list(tmp_path.glob(".checkpoint-*")) == []
