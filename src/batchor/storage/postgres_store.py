from __future__ import annotations

import os
import re
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable

from sqlalchemy import and_, create_engine, select, text, update
from sqlalchemy.engine import Engine

from batchor.core.enums import ItemStatus
from batchor.providers.registry import ProviderRegistry, build_default_provider_registry
from batchor.storage.sqlite_codec import _decode_object, _nullable_int, _nullable_str
from batchor.storage.sqlite_lifecycle import SQLiteLifecycleMixin
from batchor.storage.sqlite_queries import SQLiteQueryMixin
from batchor.storage.sqlite_results import SQLiteResultsMixin
from batchor.storage.sqlite_schema import (
    ITEMS_TABLE,
    METADATA,
    SQLITE_SCHEMA_VERSION,
    STORAGE_METADATA_TABLE,
)
from batchor.storage.state import ClaimedItem, StateStore


class PostgresStorage(SQLiteResultsMixin, SQLiteLifecycleMixin, SQLiteQueryMixin, StateStore):
    TERMINAL_BATCH_STATUSES = {"completed", "failed", "cancelled", "expired"}
    TERMINAL_ITEM_STATUSES = {ItemStatus.COMPLETED, ItemStatus.FAILED_PERMANENT}
    TERMINAL_ITEM_STATUS_COMPLETED = ItemStatus.COMPLETED
    ACTIVE_ITEM_STATUS_PENDING = ItemStatus.PENDING
    ACTIVE_ITEM_STATUS_SUBMITTED = ItemStatus.SUBMITTED

    def __init__(
        self,
        *,
        dsn: str,
        schema: str = "batchor",
        now: Callable[[], datetime] | None = None,
        engine: Engine | None = None,
        provider_registry: ProviderRegistry | None = None,
    ) -> None:
        if not dsn.strip():
            raise ValueError("PostgresStorage requires a non-empty dsn")
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", schema):
            raise ValueError(f"invalid postgres schema name: {schema}")
        self.dsn = dsn
        self.schema = schema
        self.path = Path(f"postgres/{schema}")
        self._now = now or (lambda: datetime.now(UTC))
        self.provider_registry = provider_registry or build_default_provider_registry()
        self._base_engine = engine or create_engine(dsn, future=True)
        with self._base_engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        self.engine = self._base_engine.execution_options(schema_translate_map={None: schema})
        METADATA.create_all(self.engine)
        self._ensure_schema()

    @classmethod
    def from_env(
        cls,
        *,
        provider_registry: ProviderRegistry | None = None,
    ) -> PostgresStorage:
        dsn = os.getenv("BATCHOR_POSTGRES_DSN", "")
        if not dsn:
            raise ValueError("BATCHOR_POSTGRES_DSN is required to create the default postgres storage backend")
        return cls(
            dsn=dsn,
            schema=os.getenv("BATCHOR_POSTGRES_SCHEMA", "batchor"),
            provider_registry=provider_registry,
        )

    def _ensure_schema(self) -> None:
        with self.engine.begin() as conn:
            existing_schema_row = conn.execute(
                select(STORAGE_METADATA_TABLE.c.value).where(STORAGE_METADATA_TABLE.c.key == "schema_version")
            ).first()
            if existing_schema_row is None:
                conn.execute(
                    STORAGE_METADATA_TABLE.insert(),
                    [{"key": "schema_version", "value": str(SQLITE_SCHEMA_VERSION)}],
                )
            else:
                conn.execute(
                    STORAGE_METADATA_TABLE.update()
                    .where(STORAGE_METADATA_TABLE.c.key == "schema_version")
                    .values(value=str(SQLITE_SCHEMA_VERSION))
                )

    def claim_items_for_submission(
        self,
        *,
        run_id: str,
        max_attempts: int,
        limit: int | None = None,
    ) -> list[ClaimedItem]:
        with self.engine.begin() as conn:
            query = (
                select(ITEMS_TABLE)
                .where(
                    and_(
                        ITEMS_TABLE.c.run_id == run_id,
                        ITEMS_TABLE.c.status.in_((ItemStatus.PENDING, ItemStatus.FAILED_RETRYABLE)),
                        ITEMS_TABLE.c.attempt_count < max_attempts,
                    )
                )
                .order_by(ITEMS_TABLE.c.item_index)
                .with_for_update(skip_locked=True)
            )
            if limit is not None:
                query = query.limit(limit)
            rows = list(conn.execute(query).mappings())
            if not rows:
                return []
            item_ids = [str(row["item_id"]) for row in rows]
            conn.execute(
                update(ITEMS_TABLE)
                .where(
                    and_(
                        ITEMS_TABLE.c.run_id == run_id,
                        ITEMS_TABLE.c.item_id.in_(item_ids),
                    )
                )
                .values(status=ItemStatus.QUEUED_LOCAL)
            )
            return [
                ClaimedItem(
                    item_id=str(row["item_id"]),
                    metadata=_decode_object(row["metadata_json"]),
                    prompt=str(row["prompt"]),
                    system_prompt=_nullable_str(row["system_prompt"]),
                    attempt_count=int(row["attempt_count"]),
                    request_artifact_path=_nullable_str(row["request_artifact_path"]),
                    request_artifact_line=_nullable_int(row["request_artifact_line"]),
                    request_sha256=_nullable_str(row["request_sha256"]),
                )
                for row in rows
            ]

    def close(self) -> None:
        self._base_engine.dispose()

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
