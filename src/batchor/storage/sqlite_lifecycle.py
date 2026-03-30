from __future__ import annotations

from dataclasses import asdict

from sqlalchemy import and_, bindparam, select, update

from batchor.core.enums import ItemStatus, RunLifecycleStatus
from batchor.storage.sqlite_codec import (
    _decode_object,
    _encode_datetime,
    _encode_json,
    _nullable_int,
    _nullable_str,
)
from batchor.storage.sqlite_protocol import SQLiteStorageProtocol
from batchor.storage.sqlite_schema import BATCHES_TABLE, ITEMS_TABLE, RUN_RETRY_STATE_TABLE, RUNS_TABLE
from batchor.storage.state import (
    ActiveBatchRecord,
    ClaimedItem,
    MaterializedItem,
    PersistedRunConfig,
    PreparedSubmission,
    RequestArtifactPointer,
)


class SQLiteLifecycleMixin(SQLiteStorageProtocol):
    def create_run(
        self,
        *,
        run_id: str,
        config: PersistedRunConfig,
        items: list[MaterializedItem],
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                RUNS_TABLE.insert(),
                [
                    {
                        "run_id": run_id,
                        "status": RunLifecycleStatus.RUNNING,
                        "created_at": _encode_datetime(self._now()),
                        "provider_config_json": _encode_json(
                            self.provider_registry.dump_config(config.provider_config)
                        ),
                        "chunk_policy_json": _encode_json(asdict(config.chunk_policy)),
                        "retry_policy_json": _encode_json(asdict(config.retry_policy)),
                        "batch_metadata_json": _encode_json(config.batch_metadata),
                        "schema_name": config.schema_name,
                        "structured_output_module": config.structured_output_module,
                        "structured_output_qualname": config.structured_output_qualname,
                    }
                ],
            )
            conn.execute(
                RUN_RETRY_STATE_TABLE.insert(),
                [
                    {
                        "run_id": run_id,
                        "consecutive_failures": 0,
                        "total_failures": 0,
                        "backoff_sec": 0.0,
                        "next_retry_at": None,
                        "last_error_class": None,
                    }
                ],
            )
            if items:
                conn.execute(ITEMS_TABLE.insert(), self._item_rows(run_id=run_id, items=items))
            self._refresh_run_status(conn, run_id)

    def append_items(
        self,
        *,
        run_id: str,
        items: list[MaterializedItem],
    ) -> None:
        if not items:
            return
        with self.engine.begin() as conn:
            conn.execute(ITEMS_TABLE.insert(), self._item_rows(run_id=run_id, items=items))
            self._refresh_run_status(conn, run_id)

    def get_run_config(self, *, run_id: str) -> PersistedRunConfig:
        with self.engine.begin() as conn:
            row = self._fetch_run_row(conn, run_id)
            return self._run_config_from_row(row)

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
                        ITEMS_TABLE.c.status.in_(
                            (ItemStatus.PENDING, ItemStatus.FAILED_RETRYABLE)
                        ),
                        ITEMS_TABLE.c.attempt_count < max_attempts,
                    )
                )
                .order_by(ITEMS_TABLE.c.item_index)
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
            self._refresh_run_status(conn, run_id)
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

    def release_items_to_pending(self, *, run_id: str, item_ids: list[str]) -> None:
        if not item_ids:
            return
        with self.engine.begin() as conn:
            conn.execute(
                update(ITEMS_TABLE)
                .where(
                    and_(
                        ITEMS_TABLE.c.run_id == run_id,
                        ITEMS_TABLE.c.item_id.in_(item_ids),
                        ITEMS_TABLE.c.status == ItemStatus.QUEUED_LOCAL,
                    )
                )
                .values(status=ItemStatus.PENDING)
            )
            self._refresh_run_status(conn, run_id)

    def register_batch(
        self,
        *,
        run_id: str,
        local_batch_id: str,
        provider_batch_id: str,
        status: str,
        custom_ids: list[str],
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                BATCHES_TABLE.insert(),
                [
                    {
                        "run_id": run_id,
                        "provider_batch_id": provider_batch_id,
                        "local_batch_id": local_batch_id,
                        "status": status,
                        "custom_ids_json": _encode_json(custom_ids),
                        "output_file_id": None,
                        "error_file_id": None,
                    }
                ],
            )
            self._refresh_run_status(conn, run_id)

    def mark_items_submitted(
        self,
        *,
        run_id: str,
        provider_batch_id: str,
        submissions: list[PreparedSubmission],
    ) -> None:
        if not submissions:
            return
        statement = (
            update(ITEMS_TABLE)
            .where(
                and_(
                    ITEMS_TABLE.c.run_id == bindparam("b_run_id"),
                    ITEMS_TABLE.c.item_id == bindparam("b_item_id"),
                )
            )
            .values(
                status=ItemStatus.SUBMITTED,
                active_batch_id=bindparam("b_provider_batch_id"),
                active_custom_id=bindparam("b_custom_id"),
                active_submission_tokens=bindparam("b_submission_tokens"),
                error_json=None,
            )
        )
        with self.engine.begin() as conn:
            conn.execute(
                statement,
                [
                    {
                        "b_run_id": run_id,
                        "b_item_id": submission.item_id,
                        "b_provider_batch_id": provider_batch_id,
                        "b_custom_id": submission.custom_id,
                        "b_submission_tokens": submission.submission_tokens,
                    }
                    for submission in submissions
                ],
            )
            self._refresh_run_status(conn, run_id)

    def update_batch_status(
        self,
        *,
        run_id: str,
        provider_batch_id: str,
        status: str,
        output_file_id: str | None = None,
        error_file_id: str | None = None,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                update(BATCHES_TABLE)
                .where(
                    and_(
                        BATCHES_TABLE.c.run_id == run_id,
                        BATCHES_TABLE.c.provider_batch_id == provider_batch_id,
                    )
                )
                .values(
                    status=status,
                    output_file_id=output_file_id,
                    error_file_id=error_file_id,
                )
            )
            self._refresh_run_status(conn, run_id)

    def record_request_artifacts(
        self,
        *,
        run_id: str,
        pointers: list[RequestArtifactPointer],
    ) -> None:
        if not pointers:
            return
        statement = (
            update(ITEMS_TABLE)
            .where(
                and_(
                    ITEMS_TABLE.c.run_id == bindparam("b_run_id"),
                    ITEMS_TABLE.c.item_id == bindparam("b_item_id"),
                )
            )
            .values(
                request_artifact_path=bindparam("b_request_artifact_path"),
                request_artifact_line=bindparam("b_request_artifact_line"),
                request_sha256=bindparam("b_request_sha256"),
                payload_json="null",
                prompt="",
                system_prompt=None,
            )
        )
        with self.engine.begin() as conn:
            conn.execute(
                statement,
                [
                    {
                        "b_run_id": run_id,
                        "b_item_id": pointer.item_id,
                        "b_request_artifact_path": pointer.artifact_path,
                        "b_request_artifact_line": pointer.line_number,
                        "b_request_sha256": pointer.request_sha256,
                    }
                    for pointer in pointers
                ],
            )
            self._refresh_run_status(conn, run_id)

    def get_active_batches(self, *, run_id: str) -> list[ActiveBatchRecord]:
        with self.engine.begin() as conn:
            rows = conn.execute(
                select(BATCHES_TABLE)
                .where(
                    and_(
                        BATCHES_TABLE.c.run_id == run_id,
                        BATCHES_TABLE.c.status.not_in(tuple(self.TERMINAL_BATCH_STATUSES)),
                    )
                )
                .order_by(BATCHES_TABLE.c.local_batch_id)
            ).mappings()
            return [
                ActiveBatchRecord(
                    provider_batch_id=str(row["provider_batch_id"]),
                    status=str(row["status"]),
                    output_file_id=_nullable_str(row["output_file_id"]),
                    error_file_id=_nullable_str(row["error_file_id"]),
                )
                for row in rows
            ]

    def get_submitted_custom_ids_for_batch(
        self,
        *,
        run_id: str,
        provider_batch_id: str,
    ) -> list[str]:
        with self.engine.begin() as conn:
            return self._submitted_custom_ids_for_batch(
                conn,
                run_id=run_id,
                provider_batch_id=provider_batch_id,
            )
