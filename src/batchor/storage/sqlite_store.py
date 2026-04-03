from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from batchor.core.enums import ItemStatus
from batchor.providers.registry import ProviderRegistry, build_default_provider_registry
from batchor.storage.sqlite_lifecycle import SQLiteLifecycleMixin
from batchor.storage.sqlite_queries import SQLiteQueryMixin
from batchor.storage.sqlite_results import SQLiteResultsMixin
from batchor.storage.sqlite_schema import METADATA, SQLITE_SCHEMA_VERSION, STORAGE_METADATA_TABLE
from batchor.storage.state import StateStore


class SQLiteStorage(SQLiteResultsMixin, SQLiteLifecycleMixin, SQLiteQueryMixin, StateStore):
    TERMINAL_BATCH_STATUSES = {"completed", "failed", "cancelled", "expired"}
    TERMINAL_ITEM_STATUSES = {ItemStatus.COMPLETED, ItemStatus.FAILED_PERMANENT}
    TERMINAL_ITEM_STATUS_COMPLETED = ItemStatus.COMPLETED
    ACTIVE_ITEM_STATUS_PENDING = ItemStatus.PENDING
    ACTIVE_ITEM_STATUS_SUBMITTED = ItemStatus.SUBMITTED

    def __init__(
        self,
        *,
        name: str = "default",
        path: str | Path | None = None,
        now: Callable[[], datetime] | None = None,
        engine: Engine | None = None,
        provider_registry: ProviderRegistry | None = None,
    ) -> None:
        self.path = Path(path) if path is not None else self.default_path(name)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._now = now or (lambda: datetime.now(timezone.utc))
        self.provider_registry = provider_registry or build_default_provider_registry()
        self.engine = engine or create_engine(
            f"sqlite+pysqlite:///{self.path}",
            future=True,
        )
        METADATA.create_all(self.engine)
        self._ensure_schema()

    @staticmethod
    def default_path(name: str) -> Path:
        normalized = name.strip() or "default"
        return Path.home() / ".batchor" / f"{normalized}.sqlite3"

    def close(self) -> None:
        self.engine.dispose()

    @property
    def artifact_root(self) -> Path:
        return self.path.parent / f"{self.path.stem}_artifacts"

    @property
    def schema_version(self) -> int:
        with self.engine.begin() as conn:
            row = conn.execute(
                STORAGE_METADATA_TABLE.select().where(STORAGE_METADATA_TABLE.c.key == "schema_version")
            ).mappings().first()
            if row is None:
                return SQLITE_SCHEMA_VERSION
            return int(row["value"])

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:  # noqa: BLE001
            pass
