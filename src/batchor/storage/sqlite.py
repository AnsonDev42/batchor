from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Any, Callable, cast

from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    and_,
    bindparam,
    create_engine,
    func,
    select,
    update,
)
from sqlalchemy.engine import Connection, Engine, RowMapping

from batchor.core.enums import ItemStatus, RunLifecycleStatus
from batchor.core.models import (
    ChunkPolicy,
    ItemFailure,
    RetryPolicy,
    RunSummary,
)
from batchor.core.types import JSONObject, JSONValue
from batchor.providers.registry import ProviderRegistry, build_default_provider_registry
from batchor.runtime.retry import compute_backoff_delay
from batchor.storage.state import (
    ActiveBatchRecord,
    ClaimedItem,
    CompletedItemRecord,
    ItemFailureRecord,
    MaterializedItem,
    PersistedItemRecord,
    PersistedRunConfig,
    PreparedSubmission,
    QueuedItemFailureRecord,
    RetryBackoffState,
    StateStore,
    serialize_item_failure,
)


METADATA = MetaData()

RUNS_TABLE = Table(
    "runs",
    METADATA,
    Column("run_id", String, primary_key=True),
    Column("status", String, nullable=False),
    Column("created_at", String, nullable=False),
    Column("provider_config_json", Text, nullable=False),
    Column("chunk_policy_json", Text, nullable=False),
    Column("retry_policy_json", Text, nullable=False),
    Column("batch_metadata_json", Text, nullable=False),
    Column("schema_name", String, nullable=True),
    Column("structured_output_module", String, nullable=True),
    Column("structured_output_qualname", String, nullable=True),
)

ITEMS_TABLE = Table(
    "items",
    METADATA,
    Column("run_id", String, primary_key=True),
    Column("item_id", String, primary_key=True),
    Column("item_index", Integer, nullable=False),
    Column("payload_json", Text, nullable=False),
    Column("metadata_json", Text, nullable=False),
    Column("prompt", Text, nullable=False),
    Column("system_prompt", Text, nullable=True),
    Column("status", String, nullable=False),
    Column("attempt_count", Integer, nullable=False),
    Column("active_batch_id", String, nullable=True),
    Column("active_custom_id", String, nullable=True),
    Column("active_submission_tokens", Integer, nullable=False),
    Column("output_text", Text, nullable=True),
    Column("output_json", Text, nullable=True),
    Column("raw_response_json", Text, nullable=True),
    Column("error_json", Text, nullable=True),
)

BATCHES_TABLE = Table(
    "batches",
    METADATA,
    Column("run_id", String, primary_key=True),
    Column("provider_batch_id", String, primary_key=True),
    Column("local_batch_id", String, nullable=False),
    Column("status", String, nullable=False),
    Column("custom_ids_json", Text, nullable=False),
    Column("output_file_id", String, nullable=True),
    Column("error_file_id", String, nullable=True),
)

RUN_RETRY_STATE_TABLE = Table(
    "run_retry_state",
    METADATA,
    Column("run_id", String, primary_key=True),
    Column("consecutive_failures", Integer, nullable=False),
    Column("total_failures", Integer, nullable=False),
    Column("backoff_sec", Float, nullable=False),
    Column("next_retry_at", String, nullable=True),
    Column("last_error_class", String, nullable=True),
)


class SQLiteStorage(StateStore):
    TERMINAL_BATCH_STATUSES = {"completed", "failed", "cancelled", "expired"}
    TERMINAL_ITEM_STATUSES = {ItemStatus.COMPLETED, ItemStatus.FAILED_PERMANENT}

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

    @staticmethod
    def default_path(name: str) -> Path:
        normalized = name.strip() or "default"
        return Path.home() / ".batchor" / f"{normalized}.sqlite3"

    def close(self) -> None:
        self.engine.dispose()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:  # noqa: BLE001
            pass

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

    def mark_items_completed(
        self,
        *,
        run_id: str,
        completions: list[CompletedItemRecord],
    ) -> None:
        if not completions:
            return
        statement = (
            update(ITEMS_TABLE)
            .where(
                and_(
                    ITEMS_TABLE.c.run_id == bindparam("b_run_id"),
                    ITEMS_TABLE.c.active_custom_id == bindparam("b_custom_id"),
                )
            )
            .values(
                status=ItemStatus.COMPLETED,
                output_text=bindparam("b_output_text"),
                output_json=bindparam("b_output_json"),
                raw_response_json=bindparam("b_raw_response_json"),
                error_json=None,
                active_batch_id=None,
                active_custom_id=None,
                active_submission_tokens=0,
            )
        )
        with self.engine.begin() as conn:
            conn.execute(
                statement,
                [
                    {
                        "b_run_id": run_id,
                        "b_custom_id": completion.custom_id,
                        "b_output_text": completion.output_text,
                        "b_output_json": _encode_json(completion.output_json)
                        if completion.output_json is not None
                        else None,
                        "b_raw_response_json": _encode_json(completion.raw_response),
                    }
                    for completion in completions
                ],
            )
            self._refresh_run_status(conn, run_id)

    def mark_items_failed(
        self,
        *,
        run_id: str,
        failures: list[ItemFailureRecord],
        max_attempts: int,
    ) -> None:
        if not failures:
            return
        with self.engine.begin() as conn:
            custom_ids = [failure.custom_id for failure in failures]
            rows = conn.execute(
                select(
                    ITEMS_TABLE.c.item_id,
                    ITEMS_TABLE.c.active_custom_id,
                    ITEMS_TABLE.c.attempt_count,
                ).where(
                    and_(
                        ITEMS_TABLE.c.run_id == run_id,
                        ITEMS_TABLE.c.active_custom_id.in_(custom_ids),
                    )
                )
            ).mappings()
            attempts_by_custom_id = {
                str(row["active_custom_id"]): {
                    "item_id": str(row["item_id"]),
                    "attempt_count": int(row["attempt_count"]),
                }
                for row in rows
            }
            payloads: list[dict[str, object | None]] = []
            for failure in failures:
                current = attempts_by_custom_id[failure.custom_id]
                attempt_count = int(current["attempt_count"])
                if failure.count_attempt:
                    attempt_count += 1
                payloads.append(
                    {
                        "b_run_id": run_id,
                        "b_item_id": str(current["item_id"]),
                        "b_attempt_count": attempt_count,
                        "b_status": _failed_status(
                            attempt_count=attempt_count,
                            error=failure.error,
                            max_attempts=max_attempts,
                            count_attempt=failure.count_attempt,
                        ),
                        "b_error_json": _encode_json(serialize_item_failure(failure.error)),
                    }
                )
            statement = (
                update(ITEMS_TABLE)
                .where(
                    and_(
                        ITEMS_TABLE.c.run_id == bindparam("b_run_id"),
                        ITEMS_TABLE.c.item_id == bindparam("b_item_id"),
                    )
                )
                .values(
                    attempt_count=bindparam("b_attempt_count"),
                    status=bindparam("b_status"),
                    error_json=bindparam("b_error_json"),
                    active_batch_id=None,
                    active_custom_id=None,
                    active_submission_tokens=0,
                )
            )
            conn.execute(statement, payloads)
            self._refresh_run_status(conn, run_id)

    def mark_queued_items_failed(
        self,
        *,
        run_id: str,
        failures: list[QueuedItemFailureRecord],
        max_attempts: int,
    ) -> None:
        if not failures:
            return
        with self.engine.begin() as conn:
            item_ids = [failure.item_id for failure in failures]
            rows = conn.execute(
                select(
                    ITEMS_TABLE.c.item_id,
                    ITEMS_TABLE.c.attempt_count,
                ).where(
                    and_(
                        ITEMS_TABLE.c.run_id == run_id,
                        ITEMS_TABLE.c.item_id.in_(item_ids),
                    )
                )
            ).mappings()
            attempts_by_item_id = {
                str(row["item_id"]): int(row["attempt_count"])
                for row in rows
            }
            payloads: list[dict[str, object | None]] = []
            for failure in failures:
                attempt_count = attempts_by_item_id[failure.item_id]
                if failure.count_attempt:
                    attempt_count += 1
                payloads.append(
                    {
                        "b_run_id": run_id,
                        "b_item_id": failure.item_id,
                        "b_attempt_count": attempt_count,
                        "b_status": _failed_status(
                            attempt_count=attempt_count,
                            error=failure.error,
                            max_attempts=max_attempts,
                            count_attempt=failure.count_attempt,
                        ),
                        "b_error_json": _encode_json(serialize_item_failure(failure.error)),
                    }
                )
            statement = (
                update(ITEMS_TABLE)
                .where(
                    and_(
                        ITEMS_TABLE.c.run_id == bindparam("b_run_id"),
                        ITEMS_TABLE.c.item_id == bindparam("b_item_id"),
                    )
                )
                .values(
                    attempt_count=bindparam("b_attempt_count"),
                    status=bindparam("b_status"),
                    error_json=bindparam("b_error_json"),
                    active_batch_id=None,
                    active_custom_id=None,
                    active_submission_tokens=0,
                )
            )
            conn.execute(statement, payloads)
            self._refresh_run_status(conn, run_id)

    def reset_batch_items_to_pending(
        self,
        *,
        run_id: str,
        provider_batch_id: str,
        error: ItemFailure,
    ) -> None:
        with self.engine.begin() as conn:
            custom_ids = self._submitted_custom_ids_for_batch(
                conn,
                run_id=run_id,
                provider_batch_id=provider_batch_id,
            )
            if not custom_ids:
                self._refresh_run_status(conn, run_id)
                return
            conn.execute(
                update(ITEMS_TABLE)
                .where(
                    and_(
                        ITEMS_TABLE.c.run_id == run_id,
                        ITEMS_TABLE.c.active_batch_id == provider_batch_id,
                        ITEMS_TABLE.c.active_custom_id.in_(custom_ids),
                        ITEMS_TABLE.c.status == ItemStatus.SUBMITTED,
                    )
                )
                .values(
                    status=ItemStatus.PENDING,
                    error_json=_encode_json(serialize_item_failure(error)),
                    active_batch_id=None,
                    active_custom_id=None,
                    active_submission_tokens=0,
                )
            )
            self._refresh_run_status(conn, run_id)

    def get_active_submitted_token_estimate(self, *, run_id: str) -> int:
        with self.engine.begin() as conn:
            value = conn.execute(
                select(func.coalesce(func.sum(ITEMS_TABLE.c.active_submission_tokens), 0)).where(
                    and_(
                        ITEMS_TABLE.c.run_id == run_id,
                        ITEMS_TABLE.c.status == ItemStatus.SUBMITTED,
                    )
                )
            ).scalar_one()
            return int(value)

    def record_batch_retry_failure(
        self,
        *,
        run_id: str,
        error_class: str,
        base_delay_sec: float,
        max_delay_sec: float,
    ) -> RetryBackoffState:
        with self.engine.begin() as conn:
            current = self._fetch_retry_state(conn, run_id)
            consecutive = current.consecutive_failures + 1
            total = current.total_failures + 1
            backoff_sec = compute_backoff_delay(
                consecutive_failures=consecutive,
                base_delay_sec=base_delay_sec,
                max_delay_sec=max_delay_sec,
            )
            resolved_retry_at = (
                self._now() + timedelta(seconds=backoff_sec)
                if backoff_sec > 0
                else None
            )
            state = RetryBackoffState(
                consecutive_failures=consecutive,
                total_failures=total,
                backoff_sec=backoff_sec,
                next_retry_at=resolved_retry_at,
                last_error_class=error_class,
            )
            conn.execute(
                update(RUN_RETRY_STATE_TABLE)
                .where(RUN_RETRY_STATE_TABLE.c.run_id == run_id)
                .values(
                    consecutive_failures=state.consecutive_failures,
                    total_failures=state.total_failures,
                    backoff_sec=state.backoff_sec,
                    next_retry_at=_encode_datetime(state.next_retry_at),
                    last_error_class=state.last_error_class,
                )
            )
            self._refresh_run_status(conn, run_id)
            return state

    def clear_batch_retry_backoff(self, *, run_id: str) -> None:
        with self.engine.begin() as conn:
            current = self._fetch_retry_state(conn, run_id)
            conn.execute(
                update(RUN_RETRY_STATE_TABLE)
                .where(RUN_RETRY_STATE_TABLE.c.run_id == run_id)
                .values(
                    consecutive_failures=0,
                    total_failures=current.total_failures,
                    backoff_sec=0.0,
                    next_retry_at=None,
                    last_error_class=None,
                )
            )
            self._refresh_run_status(conn, run_id)

    def get_batch_retry_backoff_remaining_sec(self, *, run_id: str) -> float:
        with self.engine.begin() as conn:
            return self._backoff_remaining(conn, run_id)

    def get_run_summary(self, *, run_id: str) -> RunSummary:
        with self.engine.begin() as conn:
            return self._summary_for_run(conn, run_id, persist=True)

    def get_item_records(self, *, run_id: str) -> list[PersistedItemRecord]:
        with self.engine.begin() as conn:
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

    def _fetch_run_row(self, conn: Connection, run_id: str) -> RowMapping:
        return conn.execute(
            select(RUNS_TABLE).where(RUNS_TABLE.c.run_id == run_id)
        ).mappings().one()

    @staticmethod
    def _item_rows(
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
                    and_(
                        BATCHES_TABLE.c.run_id == run_id,
                        BATCHES_TABLE.c.status.not_in(tuple(self.TERMINAL_BATCH_STATUSES)),
                    )
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
                and_(
                    BATCHES_TABLE.c.run_id == run_id,
                    BATCHES_TABLE.c.provider_batch_id == provider_batch_id,
                )
            )
        ).mappings().one()
        custom_ids = _decode_string_list(batch_row["custom_ids_json"])
        active_rows = conn.execute(
            select(ITEMS_TABLE.c.active_custom_id).where(
                and_(
                    ITEMS_TABLE.c.run_id == run_id,
                    ITEMS_TABLE.c.status == ItemStatus.SUBMITTED,
                    ITEMS_TABLE.c.active_batch_id == provider_batch_id,
                )
            )
        )
        active = {str(value) for value in active_rows.scalars() if value is not None}
        return [custom_id for custom_id in custom_ids if custom_id in active]


def _encode_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _decode_json(value: object) -> Any:
    if value is None:
        return None
    return json.loads(str(value))


def _encode_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat()


def _decode_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(str(value))


def _decode_object(value: object) -> JSONObject:
    decoded = _decode_json(value)
    if not isinstance(decoded, dict):
        raise TypeError("expected JSON object")
    return cast(JSONObject, decoded)


def _decode_optional_object(value: object) -> JSONObject | None:
    if value is None:
        return None
    return _decode_object(value)


def _decode_string_list(value: object) -> list[str]:
    decoded = _decode_json(value)
    if not isinstance(decoded, list):
        raise TypeError("expected JSON list")
    return [str(entry) for entry in decoded]


def _nullable_str(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _decode_dict(value: object) -> dict[str, object]:
    decoded = _decode_json(value)
    if not isinstance(decoded, dict):
        raise TypeError("expected JSON object")
    return {str(key): cast(object, entry) for key, entry in decoded.items()}


def _require_str(values: dict[str, object], key: str) -> str:
    value = values[key]
    if not isinstance(value, str):
        raise TypeError(f"expected {key} to be a string")
    return value


def _optional_str(values: dict[str, object], key: str) -> str | None:
    value = values.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"expected {key} to be a string")
    return value


def _require_int(values: dict[str, object], key: str) -> int:
    value = values[key]
    if not isinstance(value, int):
        raise TypeError(f"expected {key} to be an int")
    return value


def _require_float(values: dict[str, object], key: str) -> float:
    value = values[key]
    if isinstance(value, int | float):
        return float(value)
    raise TypeError(f"expected {key} to be numeric")


def _decode_item_failure(value: object) -> ItemFailure | None:
    decoded = _decode_json(value)
    if decoded is None:
        return None
    if not isinstance(decoded, dict):
        raise TypeError("expected JSON object for item failure")
    return ItemFailure(
        error_class=str(decoded["error_class"]),
        message=str(decoded["message"]),
        retryable=bool(decoded["retryable"]),
        raw_error=cast(JSONValue | None, decoded.get("raw_error")),
    )


def _failed_status(
    *,
    attempt_count: int,
    error: ItemFailure,
    max_attempts: int,
    count_attempt: bool,
) -> ItemStatus:
    if not error.retryable:
        return ItemStatus.FAILED_PERMANENT
    if count_attempt and attempt_count >= max_attempts:
        return ItemStatus.FAILED_PERMANENT
    return ItemStatus.FAILED_RETRYABLE
