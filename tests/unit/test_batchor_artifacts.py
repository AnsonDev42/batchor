from __future__ import annotations

from pathlib import Path

import pytest

from batchor import LocalArtifactStore


def test_local_artifact_store_write_stage_export_and_delete(tmp_path: Path) -> None:
    store = LocalArtifactStore(tmp_path / "artifacts")
    store.write_text("run_1/requests/file.jsonl", '{"hello":"world"}\n')

    with store.stage_local_copy("run_1/requests/file.jsonl") as staged:
        assert staged.exists()
        assert staged.read_text(encoding="utf-8") == '{"hello":"world"}\n'

    export_root = tmp_path / "exported"
    exported = store.export_to_directory("run_1/requests/file.jsonl", export_root)
    assert exported == export_root / "run_1/requests/file.jsonl"
    assert exported.read_text(encoding="utf-8") == '{"hello":"world"}\n'
    assert store.read_text("run_1/requests/file.jsonl") == '{"hello":"world"}\n'

    assert store.delete("run_1/requests/file.jsonl") is True
    assert store.delete("run_1/requests/file.jsonl") is False
    assert (tmp_path / "artifacts" / "run_1" / "requests").exists() is False


@pytest.mark.parametrize(
    "key",
    [
        "/absolute/path.jsonl",
        "../escape.jsonl",
        "",
    ],
)
def test_local_artifact_store_rejects_invalid_keys(tmp_path: Path, key: str) -> None:
    store = LocalArtifactStore(tmp_path / "artifacts")
    with pytest.raises(ValueError):
        store.write_text(key, "x")
