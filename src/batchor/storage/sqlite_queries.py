from __future__ import annotations

from sqlalchemy import func, select, update
from sqlalchemy.engine import Connection, RowMapping

from batchor.core.enums import ItemStatus, RunLifecycleStatus
from batchor.core.models import ChunkPolicy, RetryPolicy, RunSummary
from batchor.storage.sqlite_codec import (
    _decode_datetime,
    _decode_dict,
    _decode_json,
    _decode_item_failure,
    _decode_object,
    _decode_optional_object,
    _decode_string_list,
    _encode_json,
    _nullable_int,
    _nullable_str,
    _require_float,
    _require_int,
)
from batchor.storage.sqlite_protocol import SQLiteStorageProtocol
from batchor.storage.sqlite_schema import BATCHES_TABLE, ITEMS_TABLE, RUN_RETRY_STATE_TABLE, RUNS_TABLE
from batchor.storage.state import MaterializedItem, PersistedItemRecord, PersistedRunConfig, RetryBackoffState


class SQLiteQueryMixin(SQLiteStorageProtocol):
    def _fetch_run_row(self, conn: Connection, run_id: str) -> RowMapping:
        return conn.execute(
            select(RUNS_TABLE).where(RUNS_TABLE.c.run_id == run_id)
        ).mappings().one()

    def _item_rows(
        self,
        *,
        run_id: str,
        items: list[MaterializedItem],
    ) -> list[dict[str, object | None]]:
        rows: list[dict[str, object | None]] = []
        seen_ids: set[str] = set()
        for item in sorted(items, key=lambda entry: entry.item_index):
            if item.item_id in seen_ids:
                raise ValueError(f"duplicate item_id: {item.item_id}")
            seen_ids.add(item.item_id)
            rows.append(
                {
                    "run_id": run_id,
                    "item_id": item.item_id,
                    "item_index": item.item_index,
                    "payload_json": _encode_json(item.payload),
                    "metadata_json": _encode_json(item.metadata),
                    "prompt": item.prompt,
                    "system_prompt": item.system_prompt,
                    "request_artifact_path": None,
                    "request_artifact_line": None,
                    "request_sha256": None,
                    "status": ItemStatus.PENDING,
                    "attempt_count": 0,
                    "active_batch_id": None,
                    "active_custom_id": None,
                    "active_submission_tokens": 0,
                    "output_text": None,
                    "output_json": None,
                    "raw_response_json": None,
                    "error_json": None,
                }
            )
        return rows

    def _run_config_from_row(self, row: RowMapping) -> PersistedRunConfig:
        chunk_data = _decode_dict(row["chunk_policy_json"])
        retry_data = _decode_dict(row["retry_policy_json"])
        metadata = {
            key: str(value)
            for key, value in _decode_dict(row["batch_metadata_json"]).items()
        }
        return PersistedRunConfig(
            provider_config=self.provider_registry.load_config(
                _decode_object(row["provider_config_json"])
            ),
            chunk_policy=ChunkPolicy(
                max_requests=_require_int(chunk_data, "max_requests"),
                max_file_bytes=_require_int(chunk_data, "max_file_bytes"),
                chars_per_token=_require_int(chunk_data, "chars_per_token"),
            ),
            retry_policy=RetryPolicy(
                max_attempts=_require_int(retry_data, "max_attempts"),
                base_backoff_sec=_require_float(retry_data, "base_backoff_sec"),
                max_backoff_sec=_require_float(retry_data, "max_backoff_sec"),
            ),
            batch_metadata=metadata,
            schema_name=_nullable_str(row["schema_name"]),
            structured_output_module=_nullable_str(row["structured_output_module"]),
            structured_output_qualname=_nullable_str(row["structured_output_qualname"]),
        )

    def _ensure_schema(self) -> None:
        with self.engine.begin() as conn:
            columns = {
                str(row[1]) for row in conn.exec_driver_sql("PRAGMA table_info(items)").fetchall()
            }
            if "request_artifact_path" not in columns:
                conn.exec_driver_sql(
                    "ALTER TABLE items ADD COLUMN request_artifact_path TEXT"
                )
            if "request_artifact_line" not in columns:
                conn.exec_driver_sql(
                    "ALTER TABLE items ADD COLUMN request_artifact_line INTEGER"
                )
            if "request_sha256" not in columns:
                conn.exec_driver_sql(
                    "ALTER TABLE items ADD COLUMN request_sha256 TEXT"
                )

    def _fetch_retry_state(self, conn: Connection, run_id: str) -> RetryBackoffState:
        row = conn.execute(
            select(RUN_RETRY_STATE_TABLE).where(RUN_RETRY_STATE_TABLE.c.run_id == run_id)
        ).mappings().one()
        return RetryBackoffState(
            consecutive_failures=int(row["consecutive_failures"]),
            total_failures=int(row["total_failures"]),
            backoff_sec=float(row["backoff_sec"]),
            next_retry_at=_decode_datetime(row["next_retry_at"]),
            last_error_class=_nullable_str(row["last_error_class"]),
        )

    def _backoff_remaining(self, conn: Connection, run_id: str) -> float:
        state = self._fetch_retry_state(conn, run_id)
        if state.next_retry_at is None:
            return 0.0
        remaining = (state.next_retry_at - self._now()).total_seconds()
        return remaining if remaining > 0 else 0.0

    def _summary_for_run(
        self,
        conn: Connection,
        run_id: str,
        *,
        persist: bool,
    ) -> RunSummary:
        counts_rows = conn.execute(
            select(ITEMS_TABLE.c.status, func.count())
            .where(ITEMS_TABLE.c.run_id == run_id)
            .group_by(ITEMS_TABLE.c.status)
        )
        status_counts = {ItemStatus(str(status)): int(count) for status, count in counts_rows}
        total_items = sum(status_counts.values())
        terminal_items = sum(
            count
            for status, count in status_counts.items()
            if status in self.TERMINAL_ITEM_STATUSES
        )
        active_batches = int(
            conn.execute(
                select(func.count())
                .select_from(BATCHES_TABLE)
                .where(
                    (BATCHES_TABLE.c.run_id == run_id)
                    & BATCHES_TABLE.c.status.not_in(tuple(self.TERMINAL_BATCH_STATUSES))
                )
            ).scalar_one()
        )
        backoff_remaining_sec = self._backoff_remaining(conn, run_id)
        status: RunLifecycleStatus = (
            RunLifecycleStatus.COMPLETED
            if terminal_items == total_items and active_batches == 0 and backoff_remaining_sec <= 0
            else RunLifecycleStatus.RUNNING
        )
        if persist:
            conn.execute(
                update(RUNS_TABLE)
                .where(RUNS_TABLE.c.run_id == run_id)
                .values(status=status)
            )
        return RunSummary(
            run_id=run_id,
            status=status,
            total_items=total_items,
            completed_items=status_counts.get(ItemStatus.COMPLETED, 0),
            failed_items=status_counts.get(ItemStatus.FAILED_PERMANENT, 0),
            status_counts=status_counts,
            active_batches=active_batches,
            backoff_remaining_sec=backoff_remaining_sec,
        )

    def _refresh_run_status(self, conn: Connection, run_id: str) -> None:
        self._summary_for_run(conn, run_id, persist=True)

    def _submitted_custom_ids_for_batch(
        self,
        conn: Connection,
        *,
        run_id: str,
        provider_batch_id: str,
    ) -> list[str]:
        batch_row = conn.execute(
            select(BATCHES_TABLE.c.custom_ids_json).where(
                (BATCHES_TABLE.c.run_id == run_id)
                & (BATCHES_TABLE.c.provider_batch_id == provider_batch_id)
            )
        ).mappings().one()
        custom_ids = _decode_string_list(batch_row["custom_ids_json"])
        active_rows = conn.execute(
            select(ITEMS_TABLE.c.active_custom_id).where(
                (ITEMS_TABLE.c.run_id == run_id)
                & (ITEMS_TABLE.c.status == ItemStatus.SUBMITTED)
                & (ITEMS_TABLE.c.active_batch_id == provider_batch_id)
            )
        )
        active = {str(value) for value in active_rows.scalars() if value is not None}
        return [custom_id for custom_id in custom_ids if custom_id in active]

    def _item_records_for_run(self, conn: Connection, run_id: str) -> list[PersistedItemRecord]:
        rows = conn.execute(
            select(ITEMS_TABLE)
            .where(ITEMS_TABLE.c.run_id == run_id)
            .order_by(ITEMS_TABLE.c.item_index)
        ).mappings()
        return [
            PersistedItemRecord(
                item_id=str(row["item_id"]),
                item_index=int(row["item_index"]),
                status=ItemStatus(str(row["status"])),
                attempt_count=int(row["attempt_count"]),
                metadata=_decode_object(row["metadata_json"]),
                output_text=_nullable_str(row["output_text"]),
                output_json=_decode_json(row["output_json"]),
                raw_response=_decode_optional_object(row["raw_response_json"]),
                error=_decode_item_failure(row["error_json"]),
            )
            for row in rows
        ]
