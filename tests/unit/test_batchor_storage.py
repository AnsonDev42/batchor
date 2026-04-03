from __future__ import annotations

from pathlib import Path

from batchor.storage.sqlite import SQLiteStorage


def test_sqlite_storage_default_path_resolution(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("batchor.storage.sqlite.Path.home", lambda: tmp_path)
    storage = SQLiteStorage(name="demo")
    assert storage.path == tmp_path / ".batchor" / "demo.sqlite3"


def test_sqlite_storage_explicit_path_overrides_name(tmp_path: Path) -> None:
    path = tmp_path / "custom.sqlite3"
    storage = SQLiteStorage(name="ignored", path=path)
    assert storage.path == path


def test_sqlite_storage_exposes_schema_version(tmp_path: Path) -> None:
    storage = SQLiteStorage(path=tmp_path / "versioned.sqlite3")
    assert storage.schema_version == 1
