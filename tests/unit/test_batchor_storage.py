from __future__ import annotations

import sqlite3
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
    assert storage.schema_version == 4


def test_sqlite_storage_migrates_missing_control_reason_column(tmp_path: Path) -> None:
    path = tmp_path / "legacy.sqlite3"
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE runs (
                run_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                control_state TEXT NOT NULL,
                created_at TEXT NOT NULL,
                provider_config_json TEXT NOT NULL,
                chunk_policy_json TEXT NOT NULL,
                retry_policy_json TEXT NOT NULL,
                batch_metadata_json TEXT NOT NULL,
                artifact_policy_json TEXT NOT NULL,
                schema_name TEXT,
                structured_output_module TEXT,
                structured_output_qualname TEXT,
                artifacts_exported_at TEXT,
                artifact_export_root TEXT
            )
            """
        )

    storage = SQLiteStorage(path=path)

    with storage.engine.begin() as conn:
        columns = {str(row[1]) for row in conn.exec_driver_sql("PRAGMA table_info(runs)").fetchall()}
    assert "control_reason" in columns
    storage.close()
