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
from batchor.storage.sqlite_schema import (
    BATCHES_TABLE,
    ITEMS_TABLE,
    RUN_INGEST_STATE_TABLE,
    RUN_RETRY_STATE_TABLE,
    RUNS_TABLE,
)
from batchor.storage.state import (
    ActiveBatchRecord,
    BatchArtifactPointer,
    ClaimedItem,
    IngestCheckpoint,
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
                            self.provider_registry.dump_config(
                                config.provider_config,
                                include_secrets=False,
                            )
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

    def set_ingest_checkpoint(
        self,
        *,
        run_id: str,
        checkpoint: IngestCheckpoint,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                RUN_INGEST_STATE_TABLE.insert(),
                [
                    {
                        "run_id": run_id,
                        "source_kind": checkpoint.source_kind,
                        "source_ref": checkpoint.source_ref,
                        "source_fingerprint": checkpoint.source_fingerprint,
                        "next_item_index": checkpoint.next_item_index,
                        "ingestion_complete": 1 if checkpoint.ingestion_complete else 0,
                    }
                ],
            )

    def update_ingest_checkpoint(
        self,
        *,
        run_id: str,
        next_item_index: int,
        ingestion_complete: bool,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                update(RUN_INGEST_STATE_TABLE)
                .where(RUN_INGEST_STATE_TABLE.c.run_id == run_id)
                .values(
                    next_item_index=next_item_index,
                    ingestion_complete=1 if ingestion_complete else 0,
                )
            )

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

    def requeue_local_items(self, *, run_id: str) -> int:
        with self.engine.begin() as conn:
            row_count = len(
                list(
                    conn.execute(
                        select(ITEMS_TABLE.c.item_id).where(
                            and_(
                                ITEMS_TABLE.c.run_id == run_id,
                                ITEMS_TABLE.c.status == ItemStatus.QUEUED_LOCAL,
                            )
                        )
                    ).scalars()
                )
            )
            if row_count == 0:
                return 0
            conn.execute(
                update(ITEMS_TABLE)
                .where(
                    and_(
                        ITEMS_TABLE.c.run_id == run_id,
                        ITEMS_TABLE.c.status == ItemStatus.QUEUED_LOCAL,
                    )
                )
                .values(
                    status=ItemStatus.PENDING,
                    active_batch_id=None,
                    active_custom_id=None,
                    active_submission_tokens=0,
                )
            )
            return row_count

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
            current = conn.execute(
                select(
                    BATCHES_TABLE.c.status,
                    BATCHES_TABLE.c.output_file_id,
                    BATCHES_TABLE.c.error_file_id,
                ).where(
                    and_(
                        BATCHES_TABLE.c.run_id == run_id,
                        BATCHES_TABLE.c.provider_batch_id == provider_batch_id,
                    )
                )
            ).mappings().one()
            if (
                str(current["status"]) == status
                and _nullable_str(current["output_file_id"]) == output_file_id
                and _nullable_str(current["error_file_id"]) == error_file_id
            ):
                return
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

    def clear_request_artifact_pointers(
        self,
        *,
        run_id: str,
        artifact_paths: list[str],
    ) -> int:
        if not artifact_paths:
            return 0
        with self.engine.begin() as conn:
            target_paths = sorted(set(artifact_paths))
            row_count = len(
                list(
                    conn.execute(
                        select(ITEMS_TABLE.c.item_id).where(
                            and_(
                                ITEMS_TABLE.c.run_id == run_id,
                                ITEMS_TABLE.c.request_artifact_path.in_(target_paths),
                            )
                        )
                    ).scalars()
                )
            )
            if row_count == 0:
                return 0
            conn.execute(
                update(ITEMS_TABLE)
                .where(
                    and_(
                        ITEMS_TABLE.c.run_id == run_id,
                        ITEMS_TABLE.c.request_artifact_path.in_(target_paths),
                    )
                )
                .values(
                    request_artifact_path=None,
                    request_artifact_line=None,
                    request_sha256=None,
                )
            )
            return row_count

    def record_batch_artifacts(
        self,
        *,
        run_id: str,
        pointers: list[BatchArtifactPointer],
    ) -> None:
        if not pointers:
            return
        statement = (
            update(BATCHES_TABLE)
            .where(
                and_(
                    BATCHES_TABLE.c.run_id == bindparam("b_run_id"),
                    BATCHES_TABLE.c.provider_batch_id == bindparam("b_provider_batch_id"),
                )
            )
            .values(
                output_artifact_path=bindparam("b_output_artifact_path"),
                error_artifact_path=bindparam("b_error_artifact_path"),
            )
        )
        with self.engine.begin() as conn:
            conn.execute(
                statement,
                [
                    {
                        "b_run_id": run_id,
                        "b_provider_batch_id": pointer.provider_batch_id,
                        "b_output_artifact_path": pointer.output_artifact_path,
                        "b_error_artifact_path": pointer.error_artifact_path,
                    }
                    for pointer in pointers
                ],
            )

    def clear_batch_artifact_pointers(
        self,
        *,
        run_id: str,
        artifact_paths: list[str],
    ) -> int:
        if not artifact_paths:
            return 0
        target_paths = sorted(set(artifact_paths))
        with self.engine.begin() as conn:
            row_count = len(
                list(
                    conn.execute(
                        select(BATCHES_TABLE.c.provider_batch_id).where(
                            and_(
                                BATCHES_TABLE.c.run_id == run_id,
                                (
                                    BATCHES_TABLE.c.output_artifact_path.in_(target_paths)
                                    | BATCHES_TABLE.c.error_artifact_path.in_(target_paths)
                                ),
                            )
                        )
                    ).scalars()
                )
            )
            if row_count == 0:
                return 0
            conn.execute(
                update(BATCHES_TABLE)
                .where(
                    and_(
                        BATCHES_TABLE.c.run_id == run_id,
                        (
                            BATCHES_TABLE.c.output_artifact_path.in_(target_paths)
                            | BATCHES_TABLE.c.error_artifact_path.in_(target_paths)
                        ),
                    )
                )
                .values(
                    output_artifact_path=None,
                    error_artifact_path=None,
                )
            )
            return row_count

    def mark_artifacts_exported(
        self,
        *,
        run_id: str,
        export_root: str,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                update(RUNS_TABLE)
                .where(RUNS_TABLE.c.run_id == run_id)
                .values(
                    artifacts_exported_at=_encode_datetime(self._now()),
                    artifact_export_root=export_root,
                )
            )

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
