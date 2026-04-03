from __future__ import annotations

from datetime import datetime
from typing import Callable, Protocol

from sqlalchemy.engine import Connection, Engine, RowMapping

from batchor.core.enums import ItemStatus, RunControlState
from batchor.core.models import RunSummary
from batchor.core.types import JSONValue
from batchor.providers.registry import ProviderRegistry
from batchor.storage.state import (
    BatchArtifactPointer,
    IngestCheckpoint,
    MaterializedItem,
    PersistedItemRecord,
    PersistedRunConfig,
    RetryBackoffState,
    RunArtifactInventory,
)


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

    def has_run(self, *, run_id: str) -> bool: ...

    def set_ingest_checkpoint(
        self,
        *,
        run_id: str,
        checkpoint: IngestCheckpoint,
    ) -> None: ...

    def get_ingest_checkpoint(self, *, run_id: str) -> IngestCheckpoint | None: ...

    def update_ingest_checkpoint(
        self,
        *,
        run_id: str,
        next_item_index: int,
        checkpoint_payload: JSONValue | None = None,
        ingestion_complete: bool,
    ) -> None: ...

    def get_run_control_state(self, *, run_id: str) -> RunControlState: ...

    def set_run_control_state(
        self,
        *,
        run_id: str,
        control_state: RunControlState,
    ) -> None: ...

    def get_request_artifact_paths(self, *, run_id: str) -> list[str]: ...

    def clear_request_artifact_pointers(
        self,
        *,
        run_id: str,
        artifact_paths: list[str],
    ) -> int: ...

    def requeue_local_items(self, *, run_id: str) -> int: ...

    def record_batch_artifacts(
        self,
        *,
        run_id: str,
        pointers: list[BatchArtifactPointer],
    ) -> None: ...

    def get_artifact_inventory(self, *, run_id: str) -> RunArtifactInventory: ...

    def clear_batch_artifact_pointers(
        self,
        *,
        run_id: str,
        artifact_paths: list[str],
    ) -> int: ...

    def mark_artifacts_exported(
        self,
        *,
        run_id: str,
        export_root: str,
    ) -> None: ...
