from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable

from batchor.core.enums import ItemStatus, RunLifecycleStatus
from batchor.core.models import ItemFailure, RunSummary
from batchor.core.types import JSONObject, JSONValue
from batchor.runtime.retry import compute_backoff_delay
from batchor.storage.state_models import (
    ActiveBatchRecord,
    BatchArtifactPointer,
    ClaimedItem,
    CompletedItemRecord,
    IngestCheckpoint,
    ItemFailureRecord,
    MaterializedItem,
    PersistedItemRecord,
    PersistedRunConfig,
    PreparedSubmission,
    QueuedItemFailureRecord,
    RequestArtifactPointer,
    RetryBackoffState,
    RunArtifactInventory,
    StateStore,
)


@dataclass
class _StoredItem:
    item_id: str
    item_index: int
    payload: JSONValue
    metadata: JSONObject
    prompt: str
    system_prompt: str | None = None
    request_artifact_path: str | None = None
    request_artifact_line: int | None = None
    request_sha256: str | None = None
    status: ItemStatus = ItemStatus.PENDING
    attempt_count: int = 0
    active_batch_id: str | None = None
    active_custom_id: str | None = None
    active_submission_tokens: int = 0
    output_text: str | None = None
    output_json: JSONValue | None = None
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
    output_artifact_path: str | None = None
    error_artifact_path: str | None = None


@dataclass
class _StoredRun:
    run_id: str
    config: PersistedRunConfig
    status: RunLifecycleStatus = RunLifecycleStatus.RUNNING
    item_ids: list[str] = field(default_factory=list)
    items: dict[str, _StoredItem] = field(default_factory=dict)
    batches: dict[str, _StoredBatch] = field(default_factory=dict)
    backoff: RetryBackoffState = field(default_factory=RetryBackoffState)
    ingest_checkpoint: IngestCheckpoint | None = None
    artifacts_exported_at: datetime | None = None
    artifact_export_root: str | None = None


class MemoryStateStore(StateStore):
    TERMINAL_BATCH_STATUSES = {"completed", "failed", "cancelled", "expired"}
    TERMINAL_ITEM_STATUSES = {ItemStatus.COMPLETED, ItemStatus.FAILED_PERMANENT}

    def __init__(
        self,
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._runs: dict[str, _StoredRun] = {}
        self._now = now or (lambda: datetime.now(timezone.utc))

    def has_run(self, *, run_id: str) -> bool:
        return run_id in self._runs

    def create_run(
        self,
        *,
        run_id: str,
        config: PersistedRunConfig,
        items: list[MaterializedItem],
    ) -> None:
        if run_id in self._runs:
            raise ValueError(f"run already exists: {run_id}")
        run = _StoredRun(run_id=run_id, config=config)
        self._runs[run_id] = run
        self.append_items(run_id=run_id, items=items)
        self._refresh_run_status(run)

    def append_items(
        self,
        *,
        run_id: str,
        items: list[MaterializedItem],
    ) -> None:
        if not items:
            return
        run = self._get_run(run_id)
        for item in sorted(items, key=lambda entry: entry.item_index):
            if item.item_id in run.items:
                raise ValueError(f"duplicate item_id: {item.item_id}")
            run.item_ids.append(item.item_id)
            run.items[item.item_id] = _StoredItem(
                item_id=item.item_id,
                item_index=item.item_index,
                payload=item.payload,
                metadata=dict(item.metadata),
                prompt=item.prompt,
                system_prompt=item.system_prompt,
            )
        self._refresh_run_status(run)

    def set_ingest_checkpoint(
        self,
        *,
        run_id: str,
        checkpoint: IngestCheckpoint,
    ) -> None:
        run = self._get_run(run_id)
        run.ingest_checkpoint = checkpoint

    def get_ingest_checkpoint(self, *, run_id: str) -> IngestCheckpoint | None:
        return self._get_run(run_id).ingest_checkpoint

    def update_ingest_checkpoint(
        self,
        *,
        run_id: str,
        next_item_index: int,
        ingestion_complete: bool,
    ) -> None:
        run = self._get_run(run_id)
        checkpoint = run.ingest_checkpoint
        if checkpoint is None:
            raise ValueError(f"run has no ingest checkpoint: {run_id}")
        run.ingest_checkpoint = IngestCheckpoint(
            source_kind=checkpoint.source_kind,
            source_ref=checkpoint.source_ref,
            source_fingerprint=checkpoint.source_fingerprint,
            next_item_index=next_item_index,
            ingestion_complete=ingestion_complete,
        )

    def get_run_config(self, *, run_id: str) -> PersistedRunConfig:
        return self._get_run(run_id).config

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
            if item.status not in {ItemStatus.PENDING, ItemStatus.FAILED_RETRYABLE}:
                continue
            if item.attempt_count >= max_attempts:
                continue
            item.status = ItemStatus.QUEUED_LOCAL
            claimed.append(
                ClaimedItem(
                    item_id=item.item_id,
                    metadata=dict(item.metadata),
                    prompt=item.prompt,
                    system_prompt=item.system_prompt,
                    attempt_count=item.attempt_count,
                    request_artifact_path=item.request_artifact_path,
                    request_artifact_line=item.request_artifact_line,
                    request_sha256=item.request_sha256,
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
            if item.status == ItemStatus.QUEUED_LOCAL:
                item.status = ItemStatus.PENDING
        self._refresh_run_status(run)

    def record_request_artifacts(
        self,
        *,
        run_id: str,
        pointers: list[RequestArtifactPointer],
    ) -> None:
        if not pointers:
            return
        run = self._get_run(run_id)
        for pointer in pointers:
            item = run.items[pointer.item_id]
            item.request_artifact_path = pointer.artifact_path
            item.request_artifact_line = pointer.line_number
            item.request_sha256 = pointer.request_sha256
            item.payload = None
            item.prompt = ""
            item.system_prompt = None
        self._refresh_run_status(run)

    def get_request_artifact_paths(self, *, run_id: str) -> list[str]:
        run = self._get_run(run_id)
        artifact_paths = {
            item.request_artifact_path
            for item in run.items.values()
            if item.request_artifact_path is not None
        }
        return sorted(artifact_path for artifact_path in artifact_paths if artifact_path is not None)

    def clear_request_artifact_pointers(
        self,
        *,
        run_id: str,
        artifact_paths: list[str],
    ) -> int:
        if not artifact_paths:
            return 0
        run = self._get_run(run_id)
        target_paths = set(artifact_paths)
        cleared = 0
        for item in run.items.values():
            if item.request_artifact_path not in target_paths:
                continue
            item.request_artifact_path = None
            item.request_artifact_line = None
            item.request_sha256 = None
            cleared += 1
        self._refresh_run_status(run)
        return cleared

    def record_batch_artifacts(
        self,
        *,
        run_id: str,
        pointers: list[BatchArtifactPointer],
    ) -> None:
        if not pointers:
            return
        run = self._get_run(run_id)
        for pointer in pointers:
            batch = run.batches[pointer.provider_batch_id]
            batch.output_artifact_path = pointer.output_artifact_path
            batch.error_artifact_path = pointer.error_artifact_path

    def get_artifact_inventory(self, *, run_id: str) -> RunArtifactInventory:
        run = self._get_run(run_id)
        request_paths = sorted(
            {
                item.request_artifact_path
                for item in run.items.values()
                if item.request_artifact_path is not None
            }
        )
        output_paths = sorted(
            {
                batch.output_artifact_path
                for batch in run.batches.values()
                if batch.output_artifact_path is not None
            }
        )
        error_paths = sorted(
            {
                batch.error_artifact_path
                for batch in run.batches.values()
                if batch.error_artifact_path is not None
            }
        )
        return RunArtifactInventory(
            request_artifact_paths=[path for path in request_paths if path is not None],
            output_artifact_paths=[path for path in output_paths if path is not None],
            error_artifact_paths=[path for path in error_paths if path is not None],
            exported_at=run.artifacts_exported_at,
            export_root=run.artifact_export_root,
        )

    def clear_batch_artifact_pointers(
        self,
        *,
        run_id: str,
        artifact_paths: list[str],
    ) -> int:
        if not artifact_paths:
            return 0
        run = self._get_run(run_id)
        target_paths = set(artifact_paths)
        cleared = 0
        for batch in run.batches.values():
            changed = False
            if batch.output_artifact_path in target_paths:
                batch.output_artifact_path = None
                changed = True
            if batch.error_artifact_path in target_paths:
                batch.error_artifact_path = None
                changed = True
            if changed:
                cleared += 1
        return cleared

    def mark_artifacts_exported(
        self,
        *,
        run_id: str,
        export_root: str,
    ) -> None:
        run = self._get_run(run_id)
        run.artifacts_exported_at = self._now()
        run.artifact_export_root = export_root

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
            item.status = ItemStatus.SUBMITTED
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
                item.active_custom_id == custom_id and item.status == ItemStatus.SUBMITTED
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
            item.status = ItemStatus.COMPLETED
            item.output_text = completion.output_text
            item.output_json = completion.output_json
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
            item.status = self._failed_status(
                attempt_count=item.attempt_count,
                error=failure.error,
                max_attempts=max_attempts,
                count_attempt=failure.count_attempt,
            )
            item.error = failure.error
            item.active_batch_id = None
            item.active_custom_id = None
            item.active_submission_tokens = 0
        self._refresh_run_status(run)

    def mark_queued_items_failed(
        self,
        *,
        run_id: str,
        failures: list[QueuedItemFailureRecord],
        max_attempts: int,
    ) -> None:
        run = self._get_run(run_id)
        for failure in failures:
            item = run.items[failure.item_id]
            if failure.count_attempt:
                item.attempt_count += 1
            item.status = self._failed_status(
                attempt_count=item.attempt_count,
                error=failure.error,
                max_attempts=max_attempts,
                count_attempt=failure.count_attempt,
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
            if item.status != ItemStatus.SUBMITTED:
                continue
            item.status = ItemStatus.PENDING
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
            if item.status == ItemStatus.SUBMITTED
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

    def get_run_summary(self, *, run_id: str) -> RunSummary:
        run = self._get_run(run_id)
        self._refresh_run_status(run)
        status_counts: dict[ItemStatus, int] = {}
        for item in run.items.values():
            status_counts[item.status] = status_counts.get(item.status, 0) + 1
        return RunSummary(
            run_id=run_id,
            status=run.status,
            total_items=len(run.item_ids),
            completed_items=status_counts.get(ItemStatus.COMPLETED, 0),
            failed_items=status_counts.get(ItemStatus.FAILED_PERMANENT, 0),
            status_counts=status_counts,
            active_batches=len(self.get_active_batches(run_id=run_id)),
            backoff_remaining_sec=self.get_batch_retry_backoff_remaining_sec(run_id=run_id),
        )

    def get_item_records(self, *, run_id: str) -> list[PersistedItemRecord]:
        run = self._get_run(run_id)
        return [
            PersistedItemRecord(
                item_id=item.item_id,
                item_index=item.item_index,
                status=item.status,
                attempt_count=item.attempt_count,
                metadata=dict(item.metadata),
                output_text=item.output_text,
                output_json=item.output_json,
                raw_response=item.raw_response,
                error=item.error,
            )
            for item in sorted(run.items.values(), key=lambda entry: entry.item_index)
        ]

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
        run.status = (
            RunLifecycleStatus.COMPLETED
            if all_terminal and not active_batches and backoff_remaining <= 0
            else RunLifecycleStatus.RUNNING
        )

    @staticmethod
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


def serialize_item_failure(error: ItemFailure) -> JSONObject:
    return asdict(error)
