from __future__ import annotations

from datetime import datetime
from typing import Callable, Protocol

from sqlalchemy.engine import Connection, Engine, RowMapping

from batchor.core.enums import ItemStatus
from batchor.core.models import ItemFailure, RunSummary
from batchor.providers.registry import ProviderRegistry
from batchor.storage.state import MaterializedItem, PersistedItemRecord, PersistedRunConfig, RetryBackoffState


class SQLiteStorageProtocol(Protocol):
    engine: Engine
    provider_registry: ProviderRegistry
    TERMINAL_BATCH_STATUSES: set[str]
    TERMINAL_ITEM_STATUSES: set[ItemStatus]
    TERMINAL_ITEM_STATUS_COMPLETED: ItemStatus
    ACTIVE_ITEM_STATUS_PENDING: ItemStatus
    ACTIVE_ITEM_STATUS_SUBMITTED: ItemStatus

    _now: Callable[[], datetime]

    def _item_rows(
        self,
        *,
        run_id: str,
        items: list[MaterializedItem],
    ) -> list[dict[str, object | None]]: ...

    def _fetch_run_row(self, conn: Connection, run_id: str) -> RowMapping: ...

    def _run_config_from_row(self, row: RowMapping) -> PersistedRunConfig: ...

    def _refresh_run_status(self, conn: Connection, run_id: str) -> None: ...

    def _submitted_custom_ids_for_batch(
        self,
        conn: Connection,
        *,
        run_id: str,
        provider_batch_id: str,
    ) -> list[str]: ...

    def _fetch_retry_state(self, conn: Connection, run_id: str) -> RetryBackoffState: ...

    def _backoff_remaining(self, conn: Connection, run_id: str) -> float: ...

    def _summary_for_run(
        self,
        conn: Connection,
        run_id: str,
        *,
        persist: bool,
    ) -> RunSummary: ...

    def _item_records_for_run(self, conn: Connection, run_id: str) -> list[PersistedItemRecord]: ...
