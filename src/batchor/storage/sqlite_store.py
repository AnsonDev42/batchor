"""SQLite-backed :class:`~batchor.StateStore` implementation.

:class:`SQLiteStorage` is the default durable state store.  It uses a
single SQLite file (``~/.batchor/<name>.sqlite3`` by default) with WAL
journal mode and a companion ``<name>_artifacts/`` directory for raw
provider output files.

The implementation is composed via multiple mixin classes:

* :class:`~batchor.storage.sqlite_lifecycle.SQLiteLifecycleMixin` — run
  creation, item ingestion, and lifecycle transitions.
* :class:`~batchor.storage.sqlite_queries.SQLiteQueryMixin` — item
  claiming, status queries, and backoff state.
* :class:`~batchor.storage.sqlite_results.SQLiteResultsMixin` — terminal
  result pagination and artifact inventory.
"""

from __future__ import annotations

from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine

from batchor.core.enums import ItemStatus
from batchor.providers.registry import ProviderRegistry, build_default_provider_registry
from batchor.storage.sqlite_lifecycle import SQLiteLifecycleMixin
from batchor.storage.sqlite_queries import SQLiteQueryMixin
from batchor.storage.sqlite_results import SQLiteResultsMixin
from batchor.storage.sqlite_schema import METADATA, SQLITE_SCHEMA_VERSION, STORAGE_METADATA_TABLE
from batchor.storage.state import StateStore


class SQLiteStorage(SQLiteResultsMixin, SQLiteLifecycleMixin, SQLiteQueryMixin, StateStore):
    """SQLite-backed :class:`~batchor.StateStore`.

    The default state store for batchor runs.  Uses a single SQLite database
    file with WAL journal mode for concurrent reads and a companion artifact
    directory for JSONL files.

    Attributes:
        path: Absolute path to the SQLite database file.
        engine: SQLAlchemy engine used for all database operations.
        provider_registry: Registry used to deserialise provider configs on
            run resume.
    """

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
        """Initialise or open a SQLite storage backend.

        Args:
            name: Logical name used to derive the default file path
                (``~/.batchor/<name>.sqlite3``).  Ignored when *path* is
                provided.
            path: Explicit path to the database file.  ``~`` is expanded.
            now: Optional clock override for testing.  Defaults to
                ``datetime.now(timezone.utc)``.
            engine: Optional pre-built SQLAlchemy engine.  When provided,
                WAL and busy-timeout pragmas are not configured automatically.
            provider_registry: Registry for deserialising provider configs on
                run resume.  Defaults to the built-in registry.
        """
        self.path = Path(path) if path is not None else self.default_path(name)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._now = now or (lambda: datetime.now(UTC))
        self.provider_registry = provider_registry or build_default_provider_registry()
        self.engine = engine or create_engine(
            f"sqlite+pysqlite:///{self.path}",
            future=True,
            connect_args={"timeout": 5.0},
        )
        if engine is None:
            event.listen(self.engine, "connect", self._configure_connection)
        METADATA.create_all(self.engine)
        self._ensure_schema()

    @staticmethod
    def default_path(name: str) -> Path:
        """Return the default database file path for a given store name.

        Args:
            name: Logical store name.  Whitespace-only names fall back to
                ``"default"``.

        Returns:
            ``~/.batchor/<name>.sqlite3``
        """
        normalized = name.strip() or "default"
        return Path.home() / ".batchor" / f"{normalized}.sqlite3"

    @staticmethod
    def _configure_connection(dbapi_connection, _connection_record) -> None:  # noqa: ANN001
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA busy_timeout=5000")
        finally:
            cursor.close()

    def close(self) -> None:
        """Dispose the SQLAlchemy connection pool and release resources."""
        self.engine.dispose()

    @property
    def artifact_root(self) -> Path:
        return self.path.parent / f"{self.path.stem}_artifacts"

    @property
    def schema_version(self) -> int:
        with self.engine.begin() as conn:
            row = (
                conn.execute(STORAGE_METADATA_TABLE.select().where(STORAGE_METADATA_TABLE.c.key == "schema_version"))
                .mappings()
                .first()
            )
            if row is None:
                return SQLITE_SCHEMA_VERSION
            return int(row["value"])

    def __del__(self) -> None:
        with suppress(Exception):
            self.close()
