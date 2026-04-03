from __future__ import annotations

from datetime import timedelta

from sqlalchemy import and_, bindparam, func, select, update

from batchor.core.models import ItemFailure, RunSummary
from batchor.runtime.retry import compute_backoff_delay
from batchor.storage.sqlite_codec import (
    _encode_datetime,
    _encode_json,
    _failed_status,
)
from batchor.storage.sqlite_protocol import SQLiteStorageProtocol
from batchor.storage.sqlite_schema import ITEMS_TABLE, RUN_RETRY_STATE_TABLE
from batchor.storage.state import (
    CompletedItemRecord,
    ItemFailureRecord,
    PersistedItemRecord,
    QueuedItemFailureRecord,
    RetryBackoffState,
    serialize_item_failure,
)


class SQLiteResultsMixin(SQLiteStorageProtocol):
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
                status=self.TERMINAL_ITEM_STATUS_COMPLETED,
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
                return
            conn.execute(
                update(ITEMS_TABLE)
                .where(
                    and_(
                        ITEMS_TABLE.c.run_id == run_id,
                        ITEMS_TABLE.c.active_batch_id == provider_batch_id,
                        ITEMS_TABLE.c.active_custom_id.in_(custom_ids),
                        ITEMS_TABLE.c.status == self.ACTIVE_ITEM_STATUS_SUBMITTED,
                    )
                )
                .values(
                    status=self.ACTIVE_ITEM_STATUS_PENDING,
                    error_json=_encode_json(serialize_item_failure(error)),
                    active_batch_id=None,
                    active_custom_id=None,
                    active_submission_tokens=0,
                )
            )

    def get_active_submitted_token_estimate(self, *, run_id: str) -> int:
        with self.engine.begin() as conn:
            value = conn.execute(
                select(func.coalesce(func.sum(ITEMS_TABLE.c.active_submission_tokens), 0)).where(
                    and_(
                        ITEMS_TABLE.c.run_id == run_id,
                        ITEMS_TABLE.c.status == self.ACTIVE_ITEM_STATUS_SUBMITTED,
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

    def get_batch_retry_backoff_remaining_sec(self, *, run_id: str) -> float:
        with self.engine.begin() as conn:
            return self._backoff_remaining(conn, run_id)

    def get_run_summary(self, *, run_id: str) -> RunSummary:
        with self.engine.begin() as conn:
            return self._summary_for_run(conn, run_id, persist=True)

    def get_item_records(self, *, run_id: str) -> list[PersistedItemRecord]:
        with self.engine.begin() as conn:
            return self._item_records_for_run(conn, run_id)
