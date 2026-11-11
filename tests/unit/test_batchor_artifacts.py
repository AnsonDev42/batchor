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


def test_local_artifact_store_restricts_permissions(tmp_path: Path) -> None:
    store = LocalArtifactStore(tmp_path / "artifacts")
    store.write_text("run_1/requests/file.jsonl", '{"hello":"world"}\n')

    root_mode = store.root.stat().st_mode & 0o777
    file_mode = (store.root / "run_1" / "requests" / "file.jsonl").stat().st_mode & 0o777

    assert root_mode == 0o700
    assert file_mode == 0o600


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


@pytest.mark.parametrize("operation", ["read", "write", "delete", "stage", "export"])
def test_artifact_store_rejects_symlink_escape(tmp_path, operation):
    store = LocalArtifactStore(tmp_path / "artifacts")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "file").write_text("original", encoding="utf-8")
    (store.root / "link").symlink_to(outside, target_is_directory=True)
    actions = {
        "read": lambda: store.read_text("link/file"),
        "write": lambda: store.write_text("link/file", "changed"),
        "delete": lambda: store.delete("link/file"),
        "stage": lambda: store.stage_local_copy("link/file"),
        "export": lambda: store.export_to_directory("link/file", tmp_path / "export"),
    }
    with pytest.raises(ValueError, match="escape"):
        actions[operation]()
    assert (outside / "file").read_text(encoding="utf-8") == "original"


def test_artifact_write_failure_preserves_previous_file(tmp_path):
    store = LocalArtifactStore(tmp_path / "artifacts")
    store.write_text("run/requests/data.jsonl", "original")
    with pytest.raises(UnicodeEncodeError):
        store.write_text("run/requests/data.jsonl", "not ASCII: 中", encoding="ascii")
    assert store.read_text("run/requests/data.jsonl") == "original"
    assert [path.name for path in (store.root / "run/requests").iterdir()] == ["data.jsonl"]
    assert (store.root / "run").stat().st_mode & 0o777 == 0o700


def test_artifact_export_rejects_destination_symlink_escape(tmp_path):
    store = LocalArtifactStore(tmp_path / "artifacts")
    store.write_text("run/file", "data")
    destination = tmp_path / "export"
    destination.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (destination / "run").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="escape"):
        store.export_to_directory("run/file", destination)
    assert not list(outside.iterdir())
