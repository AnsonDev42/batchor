from __future__ import annotations

from dataclasses import asdict

from sqlalchemy import and_, bindparam, exists, select, update
from sqlalchemy.engine import RowMapping
from sqlalchemy.exc import IntegrityError

from batchor.core.enums import ItemStatus, RunControlState, RunLifecycleStatus
from batchor.core.models import OpenAIProviderConfig, openai_enqueue_quota_scope
from batchor.core.types import JSONValue
from batchor.storage.sqlite_codec import (
    _decode_json,
    _decode_object,
    _encode_datetime,
    _encode_json,
    _nullable_int,
    _nullable_str,
)
from batchor.storage.sqlite_protocol import SQLiteStorageProtocol
from batchor.storage.sqlite_schema import (
    BATCHES_TABLE,
    CAPACITY_SCOPES_TABLE,
    ITEMS_TABLE,
    RUN_INGEST_STATE_TABLE,
    RUN_RETRY_STATE_TABLE,
    RUNS_TABLE,
    SUBMISSION_INTENTS_TABLE,
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
                        "control_state": RunControlState.RUNNING,
                        "control_reason": None,
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
                        "artifact_policy_json": _encode_json(config.artifact_policy.to_payload()),
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
            # Every run starts life with an incomplete ingest marker.  It is
            # deliberately generic: arbitrary iterables cannot be resumed in
            # a fresh process, but they must never make an empty run appear
            # terminal while their first chunk is still being materialized.
            conn.execute(
                RUN_INGEST_STATE_TABLE.insert(),
                [
                    {
                        "run_id": run_id,
                        "source_kind": "unattached",
                        "source_ref": "",
                        "source_fingerprint": "",
                        "next_item_index": 0,
                        "checkpoint_payload_json": None,
                        # Initial rows are already fully materialized; runner
                        # creation deliberately passes an empty list and remains
                        # incomplete until its source finishes.
                        "ingestion_complete": 1 if items else 0,
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

    def append_items_with_ingest_checkpoint(
        self,
        *,
        run_id: str,
        items: list[MaterializedItem],
        next_item_index: int,
        checkpoint_payload: JSONValue | None = None,
        ingestion_complete: bool,
    ) -> None:
        rows = self._item_rows(run_id=run_id, items=items)
        with self.engine.begin() as conn:
            checkpoint_row = conn.execute(
                select(RUN_INGEST_STATE_TABLE.c.next_item_index).where(RUN_INGEST_STATE_TABLE.c.run_id == run_id)
            ).first()
            if checkpoint_row is None:
                raise ValueError(f"run has no ingest checkpoint: {run_id}")
            checkpoint_next_item_index = int(checkpoint_row[0])
            if rows:
                item_ids = [str(row["item_id"]) for row in rows]
                existing_by_id = {
                    str(row["item_id"]): row
                    for row in conn.execute(
                        select(
                            ITEMS_TABLE.c.item_id,
                            ITEMS_TABLE.c.item_index,
                            ITEMS_TABLE.c.payload_json,
                            ITEMS_TABLE.c.metadata_json,
                            ITEMS_TABLE.c.prompt,
                            ITEMS_TABLE.c.system_prompt,
                        ).where(
                            and_(
                                ITEMS_TABLE.c.run_id == run_id,
                                ITEMS_TABLE.c.item_id.in_(item_ids),
                            )
                        )
                    )
                    .mappings()
                    .all()
                }
                rows_to_insert = []
                for row in rows:
                    item_id = str(row["item_id"])
                    existing_row = existing_by_id.get(item_id)
                    if existing_row is None:
                        rows_to_insert.append(row)
                        continue
                    if not _materialized_row_matches_existing(
                        row,
                        existing_row,
                        min_replay_item_index=checkpoint_next_item_index,
                    ):
                        raise ValueError(f"duplicate item_id: {item_id}")
                if rows_to_insert:
                    conn.execute(ITEMS_TABLE.insert(), rows_to_insert)
            conn.execute(
                update(RUN_INGEST_STATE_TABLE)
                .where(RUN_INGEST_STATE_TABLE.c.run_id == run_id)
                .values(
                    next_item_index=next_item_index,
                    checkpoint_payload_json=_encode_json(checkpoint_payload)
                    if checkpoint_payload is not None
                    else None,
                    ingestion_complete=1 if ingestion_complete else 0,
                )
            )

    def set_ingest_checkpoint(
        self,
        *,
        run_id: str,
        checkpoint: IngestCheckpoint,
    ) -> None:
        with self.engine.begin() as conn:
            values = {
                "run_id": run_id,
                "source_kind": checkpoint.source_kind,
                "source_ref": checkpoint.source_ref,
                "source_fingerprint": checkpoint.source_fingerprint,
                "next_item_index": checkpoint.next_item_index,
                "checkpoint_payload_json": _encode_json(checkpoint.checkpoint_payload)
                if checkpoint.checkpoint_payload is not None
                else None,
                "ingestion_complete": 1 if checkpoint.ingestion_complete else 0,
            }
            updated = conn.execute(
                update(RUN_INGEST_STATE_TABLE).where(RUN_INGEST_STATE_TABLE.c.run_id == run_id).values(**values)
            )
            if updated.rowcount == 0:
                conn.execute(RUN_INGEST_STATE_TABLE.insert(), [values])

    def update_ingest_checkpoint(
        self,
        *,
        run_id: str,
        next_item_index: int,
        checkpoint_payload: JSONValue | None = None,
        ingestion_complete: bool,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                update(RUN_INGEST_STATE_TABLE)
                .where(RUN_INGEST_STATE_TABLE.c.run_id == run_id)
                .values(
                    next_item_index=next_item_index,
                    checkpoint_payload_json=_encode_json(checkpoint_payload)
                    if checkpoint_payload is not None
                    else None,
                    ingestion_complete=1 if ingestion_complete else 0,
                )
            )

    def get_run_config(self, *, run_id: str) -> PersistedRunConfig:
        with self.engine.begin() as conn:
            row = self._fetch_run_row(conn, run_id)
            return self._run_config_from_row(row)

    def set_run_control_state(
        self,
        *,
        run_id: str,
        control_state: RunControlState,
        control_reason: str | None = None,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                update(RUNS_TABLE)
                .where(RUNS_TABLE.c.run_id == run_id)
                .values(
                    control_state=control_state,
                    control_reason=control_reason,
                )
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

    def begin_submission_intent(
        self,
        *,
        run_id: str,
        intent_id: str,
        submissions: list[PreparedSubmission],
        quota_scope: str | None,
        submission_tokens: int,
        capacity_limit: int | None,
    ) -> bool:
        with self.engine.begin() as conn:
            if quota_scope is not None and capacity_limit is not None:
                legacy_reserved = self._legacy_active_submitted_tokens(conn, quota_scope=quota_scope)
                try:
                    # A savepoint keeps a Postgres transaction usable after a
                    # concurrent unique-key winner creates the scope first.
                    with conn.begin_nested():
                        conn.execute(
                            CAPACITY_SCOPES_TABLE.insert(),
                            [{"quota_scope": quota_scope, "reserved_tokens": 0}],
                        )
                except IntegrityError:
                    # A competing transaction created the counter first.
                    pass
                reserved = conn.execute(
                    update(CAPACITY_SCOPES_TABLE)
                    .where(
                        (CAPACITY_SCOPES_TABLE.c.quota_scope == quota_scope)
                        & (
                            CAPACITY_SCOPES_TABLE.c.reserved_tokens
                            <= capacity_limit - submission_tokens - legacy_reserved
                        )
                    )
                    .values(reserved_tokens=CAPACITY_SCOPES_TABLE.c.reserved_tokens + submission_tokens)
                )
                if reserved.rowcount != 1:
                    return False
            conn.execute(
                SUBMISSION_INTENTS_TABLE.insert(),
                [
                    {
                        "intent_id": intent_id,
                        "run_id": run_id,
                        "status": "creating",
                        "provider_batch_id": None,
                        "custom_ids_json": _encode_json([submission.custom_id for submission in submissions]),
                        "item_ids_json": _encode_json([submission.item_id for submission in submissions]),
                        "submissions_json": _encode_json(
                            [
                                {
                                    "item_id": submission.item_id,
                                    "custom_id": submission.custom_id,
                                    "submission_tokens": submission.submission_tokens,
                                }
                                for submission in submissions
                            ]
                        ),
                        "quota_scope": quota_scope,
                        "submission_tokens": submission_tokens,
                        "capacity_released": 0,
                        "created_at": _encode_datetime(self._now()),
                    }
                ],
            )
            return True

    def finalize_submission_intent(
        self,
        *,
        run_id: str,
        intent_id: str,
        local_batch_id: str,
        provider_batch_id: str,
        status: str,
        custom_ids: list[str],
        submissions: list[PreparedSubmission],
    ) -> None:
        with self.engine.begin() as conn:
            intent = conn.execute(
                select(SUBMISSION_INTENTS_TABLE.c.status).where(
                    (SUBMISSION_INTENTS_TABLE.c.intent_id == intent_id) & (SUBMISSION_INTENTS_TABLE.c.run_id == run_id)
                )
            ).first()
            if intent is None or str(intent[0]) != "creating":
                raise ValueError(f"submission intent is not creating: {intent_id}")
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
            if submissions:
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
                conn.execute(
                    statement,
                    [
                        {
                            "b_run_id": run_id,
                            "b_item_id": value.item_id,
                            "b_provider_batch_id": provider_batch_id,
                            "b_custom_id": value.custom_id,
                            "b_submission_tokens": value.submission_tokens,
                        }
                        for value in submissions
                    ],
                )
            conn.execute(
                update(SUBMISSION_INTENTS_TABLE)
                .where(SUBMISSION_INTENTS_TABLE.c.intent_id == intent_id)
                .values(status="active", provider_batch_id=provider_batch_id)
            )

    def abandon_submission_intent(self, *, intent_id: str) -> None:
        with self.engine.begin() as conn:
            self._release_submission_intents(conn, intent_ids=[intent_id], status="abandoned")

    def has_indeterminate_submission_intents(self, *, run_id: str) -> bool:
        with self.engine.begin() as conn:
            return (
                conn.execute(
                    select(SUBMISSION_INTENTS_TABLE.c.intent_id)
                    .where(
                        (SUBMISSION_INTENTS_TABLE.c.run_id == run_id)
                        & (SUBMISSION_INTENTS_TABLE.c.status == "creating")
                    )
                    .limit(1)
                ).first()
                is not None
            )

    def release_submission_capacity_for_batch(self, *, run_id: str, provider_batch_id: str) -> None:
        with self.engine.begin() as conn:
            intent_ids = list(
                conn.execute(
                    select(SUBMISSION_INTENTS_TABLE.c.intent_id).where(
                        (SUBMISSION_INTENTS_TABLE.c.run_id == run_id)
                        & (SUBMISSION_INTENTS_TABLE.c.provider_batch_id == provider_batch_id)
                        & (SUBMISSION_INTENTS_TABLE.c.status == "active")
                    )
                ).scalars()
            )
            self._release_submission_intents(conn, intent_ids=[str(value) for value in intent_ids], status=None)

    def abandon_indeterminate_submission_intents(self, *, run_id: str) -> None:
        with self.engine.begin() as conn:
            intent_rows = list(
                conn.execute(
                    select(
                        SUBMISSION_INTENTS_TABLE.c.intent_id,
                        SUBMISSION_INTENTS_TABLE.c.item_ids_json,
                    ).where(
                        (SUBMISSION_INTENTS_TABLE.c.run_id == run_id)
                        & (SUBMISSION_INTENTS_TABLE.c.status == "creating")
                    )
                ).mappings()
            )
            self._release_submission_intents(
                conn,
                intent_ids=[str(row["intent_id"]) for row in intent_rows],
                status="operator_abandoned",
            )
            item_ids = {str(item_id) for row in intent_rows for item_id in _decode_json(row["item_ids_json"])}
            if item_ids:
                conn.execute(
                    update(ITEMS_TABLE)
                    .where(
                        (ITEMS_TABLE.c.run_id == run_id)
                        & (ITEMS_TABLE.c.item_id.in_(item_ids))
                        & (ITEMS_TABLE.c.status == ItemStatus.QUEUED_LOCAL)
                    )
                    .values(
                        status=ItemStatus.PENDING,
                        active_batch_id=None,
                        active_custom_id=None,
                        active_submission_tokens=0,
                    )
                )

    def _legacy_active_submitted_tokens(self, conn, *, quota_scope: str) -> int:  # noqa: ANN001
        """Count active submitted tokens not represented by a v6 intent."""
        rows = conn.execute(
            select(
                RUNS_TABLE.c.provider_config_json,
                ITEMS_TABLE.c.active_submission_tokens,
            )
            .select_from(ITEMS_TABLE.join(RUNS_TABLE, ITEMS_TABLE.c.run_id == RUNS_TABLE.c.run_id))
            .where(
                (ITEMS_TABLE.c.status == ItemStatus.SUBMITTED)
                & ~exists(
                    select(SUBMISSION_INTENTS_TABLE.c.intent_id).where(
                        (SUBMISSION_INTENTS_TABLE.c.run_id == ITEMS_TABLE.c.run_id)
                        & (SUBMISSION_INTENTS_TABLE.c.provider_batch_id == ITEMS_TABLE.c.active_batch_id)
                    )
                )
            )
        ).mappings()
        reserved = 0
        for row in rows:
            provider_config = self.provider_registry.load_config(_decode_object(row["provider_config_json"]))
            if not isinstance(provider_config, OpenAIProviderConfig):
                continue
            if openai_enqueue_quota_scope(provider_config) == quota_scope:
                reserved += int(row["active_submission_tokens"])
        return reserved

    def finalize_indeterminate_submission_as_created(self, *, run_id: str, provider_batch_id: str, status: str) -> None:
        with self.engine.begin() as conn:
            row = (
                conn.execute(
                    select(SUBMISSION_INTENTS_TABLE).where(
                        (SUBMISSION_INTENTS_TABLE.c.run_id == run_id)
                        & (SUBMISSION_INTENTS_TABLE.c.status == "creating")
                    )
                )
                .mappings()
                .all()
            )
            if len(row) != 1:
                raise ValueError("expected exactly one indeterminate submission intent")
            intent = row[0]
            submissions_payload = _decode_json(intent["submissions_json"])
            if not isinstance(submissions_payload, list):
                raise ValueError("invalid persisted submission intent")
            submissions = [
                PreparedSubmission(
                    item_id=str(value["item_id"]),
                    custom_id=str(value["custom_id"]),
                    submission_tokens=int(value["submission_tokens"]),
                )
                for value in submissions_payload
                if isinstance(value, dict)
            ]
            if len(submissions) != len(submissions_payload):
                raise ValueError("invalid persisted submission intent")
            conn.execute(
                BATCHES_TABLE.insert(),
                [
                    {
                        "run_id": run_id,
                        "provider_batch_id": provider_batch_id,
                        "local_batch_id": f"recovered-{str(intent['intent_id'])[-8:]}",
                        "status": status,
                        "custom_ids_json": intent["custom_ids_json"],
                        "output_file_id": None,
                        "error_file_id": None,
                    }
                ],
            )
            statement = (
                update(ITEMS_TABLE)
                .where(
                    and_(ITEMS_TABLE.c.run_id == bindparam("b_run_id"), ITEMS_TABLE.c.item_id == bindparam("b_item_id"))
                )
                .values(
                    status=ItemStatus.SUBMITTED,
                    active_batch_id=bindparam("b_provider_batch_id"),
                    active_custom_id=bindparam("b_custom_id"),
                    active_submission_tokens=bindparam("b_submission_tokens"),
                    error_json=None,
                )
            )
            conn.execute(
                statement,
                [
                    {
                        "b_run_id": run_id,
                        "b_item_id": value.item_id,
                        "b_provider_batch_id": provider_batch_id,
                        "b_custom_id": value.custom_id,
                        "b_submission_tokens": value.submission_tokens,
                    }
                    for value in submissions
                ],
            )
            conn.execute(
                update(SUBMISSION_INTENTS_TABLE)
                .where(SUBMISSION_INTENTS_TABLE.c.intent_id == intent["intent_id"])
                .values(status="active", provider_batch_id=provider_batch_id)
            )

    @staticmethod
    def _release_submission_intents(conn, *, intent_ids: list[str], status: str | None) -> None:  # noqa: ANN001
        for intent_id in intent_ids:
            row = (
                conn.execute(
                    select(
                        SUBMISSION_INTENTS_TABLE.c.quota_scope,
                        SUBMISSION_INTENTS_TABLE.c.submission_tokens,
                        SUBMISSION_INTENTS_TABLE.c.capacity_released,
                    ).where(SUBMISSION_INTENTS_TABLE.c.intent_id == intent_id)
                )
                .mappings()
                .first()
            )
            if row is None or int(row["capacity_released"]) != 0:
                continue
            values: dict[str, object] = {"capacity_released": 1}
            if status is not None:
                values["status"] = status
            updated = conn.execute(
                update(SUBMISSION_INTENTS_TABLE)
                .where(
                    (SUBMISSION_INTENTS_TABLE.c.intent_id == intent_id)
                    & (SUBMISSION_INTENTS_TABLE.c.capacity_released == 0)
                )
                .values(**values)
            )
            if updated.rowcount == 1 and row["quota_scope"] is not None:
                conn.execute(
                    update(CAPACITY_SCOPES_TABLE)
                    .where(CAPACITY_SCOPES_TABLE.c.quota_scope == str(row["quota_scope"]))
                    .values(reserved_tokens=CAPACITY_SCOPES_TABLE.c.reserved_tokens - int(row["submission_tokens"]))
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
            current = (
                conn.execute(
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
                )
                .mappings()
                .one()
            )
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
                        (
                            BATCHES_TABLE.c.status.not_in(tuple(self.TERMINAL_BATCH_STATUSES))
                            | exists(
                                select(ITEMS_TABLE.c.item_id).where(
                                    and_(
                                        ITEMS_TABLE.c.run_id == run_id,
                                        ITEMS_TABLE.c.active_batch_id == BATCHES_TABLE.c.provider_batch_id,
                                        ITEMS_TABLE.c.status == self.ACTIVE_ITEM_STATUS_SUBMITTED,
                                    )
                                )
                            )
                            | exists(
                                select(SUBMISSION_INTENTS_TABLE.c.intent_id).where(
                                    and_(
                                        SUBMISSION_INTENTS_TABLE.c.run_id == run_id,
                                        SUBMISSION_INTENTS_TABLE.c.provider_batch_id
                                        == BATCHES_TABLE.c.provider_batch_id,
                                        SUBMISSION_INTENTS_TABLE.c.capacity_released == 0,
                                    )
                                )
                            )
                        ),
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


def _materialized_row_matches_existing(
    materialized: dict[str, object | None],
    existing: RowMapping,
    *,
    min_replay_item_index: int,
) -> bool:
    materialized_index = materialized["item_index"]
    if not isinstance(materialized_index, int):
        return False
    if materialized_index < min_replay_item_index:
        return False
    return (
        int(existing["item_index"]) == materialized_index
        and _decode_json(existing["payload_json"]) == _decode_json(materialized["payload_json"])
        and _decode_json(existing["metadata_json"]) == _decode_json(materialized["metadata_json"])
        and str(existing["prompt"]) == materialized["prompt"]
        and _nullable_str(existing["system_prompt"]) == materialized["system_prompt"]
    )
