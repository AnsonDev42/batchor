from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Protocol

from pydantic import BaseModel

from batchor.types import JSONObject

from batchor.models import (
    BatchItem,
    ItemFailure,
    ItemStatus,
    RunLifecycleStatus,
    RunStatus,
)
from batchor.retry import compute_backoff_delay


@dataclass(frozen=True)
class RetryBackoffState:
    consecutive_failures: int = 0
    total_failures: int = 0
    backoff_sec: float = 0.0
    next_retry_at: datetime | None = None
    last_error_class: str | None = None


@dataclass(frozen=True)
class ClaimedItem:
    item_id: str
    payload: Any
    metadata: JSONObject
    attempt_count: int


@dataclass(frozen=True)
class PreparedSubmission:
    item_id: str
    custom_id: str
    submission_tokens: int


@dataclass(frozen=True)
class CompletedItemRecord:
    custom_id: str
    output_text: str
    raw_response: JSONObject
    output_model: BaseModel | None = None


@dataclass(frozen=True)
class ItemFailureRecord:
    custom_id: str
    error: ItemFailure
    count_attempt: bool


@dataclass(frozen=True)
class ActiveBatchRecord:
    provider_batch_id: str
    status: str
    output_file_id: str | None = None
    error_file_id: str | None = None


class StateStore(Protocol):
    def create_run(
        self,
        *,
        run_id: str,
        items: list[BatchItem[Any]],
        structured: bool,
    ) -> None: ...

    def claim_items_for_submission(
        self,
        *,
        run_id: str,
        max_attempts: int,
        limit: int | None = None,
    ) -> list[ClaimedItem]: ...

    def release_items_to_pending(self, *, run_id: str, item_ids: list[str]) -> None: ...

    def register_batch(
        self,
        *,
        run_id: str,
        local_batch_id: str,
        provider_batch_id: str,
        status: str,
        custom_ids: list[str],
    ) -> None: ...

    def mark_items_submitted(
        self,
        *,
        run_id: str,
        provider_batch_id: str,
        submissions: list[PreparedSubmission],
    ) -> None: ...

    def update_batch_status(
        self,
        *,
        run_id: str,
        provider_batch_id: str,
        status: str,
        output_file_id: str | None = None,
        error_file_id: str | None = None,
    ) -> None: ...

    def get_active_batches(self, *, run_id: str) -> list[ActiveBatchRecord]: ...

    def get_submitted_custom_ids_for_batch(
        self,
        *,
        run_id: str,
        provider_batch_id: str,
    ) -> list[str]: ...

    def mark_items_completed(
        self,
        *,
        run_id: str,
        completions: list[CompletedItemRecord],
    ) -> None: ...

    def mark_items_failed(
        self,
        *,
        run_id: str,
        failures: list[ItemFailureRecord],
        max_attempts: int,
    ) -> None: ...

    def reset_batch_items_to_pending(
        self,
        *,
        run_id: str,
        provider_batch_id: str,
        error: ItemFailure,
    ) -> None: ...

    def get_active_submitted_token_estimate(self, *, run_id: str) -> int: ...

    def record_batch_retry_failure(
        self,
        *,
        run_id: str,
        error_class: str,
        base_delay_sec: float,
        max_delay_sec: float,
    ) -> RetryBackoffState: ...

    def clear_batch_retry_backoff(self, *, run_id: str) -> None: ...

    def get_batch_retry_backoff_remaining_sec(self, *, run_id: str) -> float: ...

    def get_run_status(self, *, run_id: str) -> RunStatus: ...

    def get_item_snapshot(self, *, run_id: str) -> list[dict[str, Any]]: ...

    def is_structured_run(self, *, run_id: str) -> bool: ...


@dataclass
class _StoredItem:
    item_id: str
    payload: Any
    metadata: JSONObject
    status: ItemStatus = "pending"
    attempt_count: int = 0
    active_batch_id: str | None = None
    active_custom_id: str | None = None
    active_submission_tokens: int = 0
    output_text: str | None = None
    output_model: BaseModel | None = None
    raw_response: JSONObject | None = None
    error: ItemFailure | None = None


@dataclass
class _StoredBatch:
    local_batch_id: str
    provider_batch_id: str
    status: str
    custom_ids: list[str]
    output_file_id: str | None = None
    error_file_id: str | None = None


@dataclass
class _StoredRun:
    run_id: str
    structured: bool
    status: RunLifecycleStatus = "awaiting_completion"
    item_ids: list[str] = field(default_factory=list)
    items: dict[str, _StoredItem] = field(default_factory=dict)
    batches: dict[str, _StoredBatch] = field(default_factory=dict)
    backoff: RetryBackoffState = field(default_factory=RetryBackoffState)


class MemoryStateStore:
    TERMINAL_BATCH_STATUSES = {"completed", "failed", "cancelled", "expired"}
    TERMINAL_ITEM_STATUSES = {"completed", "failed_permanent"}

    def __init__(
        self,
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._runs: dict[str, _StoredRun] = {}
        self._now = now or (lambda: datetime.now(timezone.utc))

    def create_run(
        self,
        *,
        run_id: str,
        items: list[BatchItem[Any]],
        structured: bool,
    ) -> None:
        if run_id in self._runs:
            raise ValueError(f"run already exists: {run_id}")
        seen_ids: set[str] = set()
        run = _StoredRun(run_id=run_id, structured=structured)
        for item in items:
            if item.item_id in seen_ids:
                raise ValueError(f"duplicate item_id: {item.item_id}")
            seen_ids.add(item.item_id)
            run.item_ids.append(item.item_id)
            run.items[item.item_id] = _StoredItem(
                item_id=item.item_id,
                payload=item.payload,
                metadata=dict(item.metadata),
            )
        self._runs[run_id] = run
        self._refresh_run_status(run)

    def claim_items_for_submission(
        self,
        *,
        run_id: str,
        max_attempts: int,
        limit: int | None = None,
    ) -> list[ClaimedItem]:
        run = self._get_run(run_id)
        claimed: list[ClaimedItem] = []
        for item_id in run.item_ids:
            item = run.items[item_id]
            if item.status not in {"pending", "failed_retryable"}:
                continue
            if item.attempt_count >= max_attempts:
                continue
            item.status = "queued_local"
            claimed.append(
                ClaimedItem(
                    item_id=item.item_id,
                    payload=item.payload,
                    metadata=dict(item.metadata),
                    attempt_count=item.attempt_count,
                )
            )
            if limit is not None and len(claimed) >= limit:
                break
        self._refresh_run_status(run)
        return claimed

    def release_items_to_pending(self, *, run_id: str, item_ids: list[str]) -> None:
        run = self._get_run(run_id)
        for item_id in item_ids:
            item = run.items[item_id]
            if item.status == "queued_local":
                item.status = "pending"
        self._refresh_run_status(run)

    def register_batch(
        self,
        *,
        run_id: str,
        local_batch_id: str,
        provider_batch_id: str,
        status: str,
        custom_ids: list[str],
    ) -> None:
        run = self._get_run(run_id)
        run.batches[provider_batch_id] = _StoredBatch(
            local_batch_id=local_batch_id,
            provider_batch_id=provider_batch_id,
            status=status,
            custom_ids=list(custom_ids),
        )
        self._refresh_run_status(run)

    def mark_items_submitted(
        self,
        *,
        run_id: str,
        provider_batch_id: str,
        submissions: list[PreparedSubmission],
    ) -> None:
        run = self._get_run(run_id)
        for submission in submissions:
            item = run.items[submission.item_id]
            item.status = "submitted"
            item.active_batch_id = provider_batch_id
            item.active_custom_id = submission.custom_id
            item.active_submission_tokens = submission.submission_tokens
            item.error = None
        self._refresh_run_status(run)

    def update_batch_status(
        self,
        *,
        run_id: str,
        provider_batch_id: str,
        status: str,
        output_file_id: str | None = None,
        error_file_id: str | None = None,
    ) -> None:
        run = self._get_run(run_id)
        batch = run.batches[provider_batch_id]
        batch.status = status
        batch.output_file_id = output_file_id
        batch.error_file_id = error_file_id
        self._refresh_run_status(run)

    def get_active_batches(self, *, run_id: str) -> list[ActiveBatchRecord]:
        run = self._get_run(run_id)
        return [
            ActiveBatchRecord(
                provider_batch_id=batch.provider_batch_id,
                status=batch.status,
                output_file_id=batch.output_file_id,
                error_file_id=batch.error_file_id,
            )
            for batch in run.batches.values()
            if batch.status not in self.TERMINAL_BATCH_STATUSES
        ]

    def get_submitted_custom_ids_for_batch(
        self,
        *,
        run_id: str,
        provider_batch_id: str,
    ) -> list[str]:
        run = self._get_run(run_id)
        batch = run.batches[provider_batch_id]
        submitted: list[str] = []
        for custom_id in batch.custom_ids:
            if any(
                item.active_custom_id == custom_id and item.status == "submitted"
                for item in run.items.values()
            ):
                submitted.append(custom_id)
        return submitted

    def mark_items_completed(
        self,
        *,
        run_id: str,
        completions: list[CompletedItemRecord],
    ) -> None:
        run = self._get_run(run_id)
        for completion in completions:
            item = self._item_for_custom_id(run, completion.custom_id)
            item.status = "completed"
            item.output_text = completion.output_text
            item.output_model = completion.output_model
            item.raw_response = completion.raw_response
            item.error = None
            item.active_batch_id = None
            item.active_custom_id = None
            item.active_submission_tokens = 0
        self._refresh_run_status(run)

    def mark_items_failed(
        self,
        *,
        run_id: str,
        failures: list[ItemFailureRecord],
        max_attempts: int,
    ) -> None:
        run = self._get_run(run_id)
        for failure in failures:
            item = self._item_for_custom_id(run, failure.custom_id)
            if failure.count_attempt:
                item.attempt_count += 1
            item.status = (
                "failed_permanent"
                if failure.count_attempt and item.attempt_count >= max_attempts
                else "failed_retryable"
            )
            item.error = failure.error
            item.active_batch_id = None
            item.active_custom_id = None
            item.active_submission_tokens = 0
        self._refresh_run_status(run)

    def reset_batch_items_to_pending(
        self,
        *,
        run_id: str,
        provider_batch_id: str,
        error: ItemFailure,
    ) -> None:
        run = self._get_run(run_id)
        batch = run.batches[provider_batch_id]
        for custom_id in batch.custom_ids:
            item = self._item_for_custom_id(run, custom_id)
            if item.status != "submitted":
                continue
            item.status = "pending"
            item.error = error
            item.active_batch_id = None
            item.active_custom_id = None
            item.active_submission_tokens = 0
        self._refresh_run_status(run)

    def get_active_submitted_token_estimate(self, *, run_id: str) -> int:
        run = self._get_run(run_id)
        return sum(
            item.active_submission_tokens
            for item in run.items.values()
            if item.status == "submitted"
        )

    def record_batch_retry_failure(
        self,
        *,
        run_id: str,
        error_class: str,
        base_delay_sec: float,
        max_delay_sec: float,
    ) -> RetryBackoffState:
        run = self._get_run(run_id)
        consecutive = run.backoff.consecutive_failures + 1
        total = run.backoff.total_failures + 1
        backoff_sec = compute_backoff_delay(
            consecutive_failures=consecutive,
            base_delay_sec=base_delay_sec,
            max_delay_sec=max_delay_sec,
        )
        next_retry_at = (
            self._now() + timedelta(seconds=backoff_sec)
            if backoff_sec > 0
            else None
        )
        run.backoff = RetryBackoffState(
            consecutive_failures=consecutive,
            total_failures=total,
            backoff_sec=backoff_sec,
            next_retry_at=next_retry_at,
            last_error_class=error_class,
        )
        self._refresh_run_status(run)
        return run.backoff

    def clear_batch_retry_backoff(self, *, run_id: str) -> None:
        run = self._get_run(run_id)
        run.backoff = RetryBackoffState(total_failures=run.backoff.total_failures)
        self._refresh_run_status(run)

    def get_batch_retry_backoff_remaining_sec(self, *, run_id: str) -> float:
        run = self._get_run(run_id)
        if run.backoff.next_retry_at is None:
            return 0.0
        remaining = (run.backoff.next_retry_at - self._now()).total_seconds()
        return remaining if remaining > 0 else 0.0

    def get_run_status(self, *, run_id: str) -> RunStatus:
        run = self._get_run(run_id)
        status_counts: dict[str, int] = {}
        for item in run.items.values():
            status_counts[item.status] = status_counts.get(item.status, 0) + 1
        return RunStatus(
            run_id=run_id,
            status=run.status,
            total_items=len(run.item_ids),
            status_counts=status_counts,
            active_batches=len(self.get_active_batches(run_id=run_id)),
            backoff_remaining_sec=self.get_batch_retry_backoff_remaining_sec(run_id=run_id),
        )

    def get_item_snapshot(self, *, run_id: str) -> list[dict[str, Any]]:
        run = self._get_run(run_id)
        snapshot: list[dict[str, Any]] = []
        for item_id in run.item_ids:
            item = run.items[item_id]
            snapshot.append(
                {
                    "item_id": item.item_id,
                    "status": item.status,
                    "attempt_count": item.attempt_count,
                    "output_text": item.output_text,
                    "output_model": item.output_model,
                    "raw_response": item.raw_response,
                    "error": item.error,
                    "metadata": dict(item.metadata),
                }
            )
        return snapshot

    def is_structured_run(self, *, run_id: str) -> bool:
        return self._get_run(run_id).structured

    def _get_run(self, run_id: str) -> _StoredRun:
        if run_id not in self._runs:
            raise KeyError(f"unknown run_id: {run_id}")
        return self._runs[run_id]

    @staticmethod
    def _item_for_custom_id(run: _StoredRun, custom_id: str) -> _StoredItem:
        for item in run.items.values():
            if item.active_custom_id == custom_id:
                return item
        raise KeyError(f"unknown custom_id: {custom_id}")

    def _refresh_run_status(self, run: _StoredRun) -> None:
        all_terminal = all(
            run.items[item_id].status in self.TERMINAL_ITEM_STATUSES
            for item_id in run.item_ids
        )
        active_batches = any(
            batch.status not in self.TERMINAL_BATCH_STATUSES
            for batch in run.batches.values()
        )
        backoff_remaining = self.get_batch_retry_backoff_remaining_sec(run_id=run.run_id)
        run.status = "completed" if all_terminal and not active_batches and backoff_remaining <= 0 else "awaiting_completion"
