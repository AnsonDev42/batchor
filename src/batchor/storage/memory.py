"""In-memory :class:`~batchor.StateStore` implementation.

:class:`MemoryStateStore` stores all run state in Python dictionaries with no
persistence.  It is useful for:

* Unit tests that need a fast, zero-setup state store.
* Single-process jobs where durability is not required.

State is lost when the process exits.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from threading import RLock
from typing import Callable

from batchor.core.enums import ItemStatus, RunControlState, RunLifecycleStatus
from batchor.core.models import ItemFailure, OpenAIProviderConfig, RunSummary, openai_enqueue_quota_scope
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
    terminal_result_sequence: int | None = None
    terminalized_at: datetime | None = None
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
class _StoredSubmissionIntent:
    intent_id: str
    run_id: str
    custom_ids: list[str]
    item_ids: list[str]
    submissions: list[PreparedSubmission]
    quota_scope: str | None
    submission_tokens: int
    status: str = "creating"
    provider_batch_id: str | None = None
    capacity_released: bool = False


@dataclass
class _StoredRun:
    run_id: str
    config: PersistedRunConfig
    status: RunLifecycleStatus = RunLifecycleStatus.RUNNING
    control_state: RunControlState = RunControlState.RUNNING
    control_reason: str | None = None
    item_ids: list[str] = field(default_factory=list)
    items: dict[str, _StoredItem] = field(default_factory=dict)
    batches: dict[str, _StoredBatch] = field(default_factory=dict)
    backoff: RetryBackoffState = field(default_factory=RetryBackoffState)
    ingest_checkpoint: IngestCheckpoint | None = None
    artifacts_exported_at: datetime | None = None
    artifact_export_root: str | None = None


class MemoryStateStore(StateStore):
    """Ephemeral in-memory implementation of :class:`~batchor.StateStore`.

    All state is held in plain Python dictionaries.  No persistence occurs;
    data is lost when the store is garbage-collected or the process exits.

    Thread safety: individual method calls are not locked.  Do not share a
    single instance across threads without external synchronisation.
    """

    TERMINAL_BATCH_STATUSES = {"completed", "failed", "cancelled", "expired"}
    TERMINAL_ITEM_STATUSES = {ItemStatus.COMPLETED, ItemStatus.FAILED_PERMANENT}

    def __init__(
        self,
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._runs: dict[str, _StoredRun] = {}
        self._submission_intents: dict[str, _StoredSubmissionIntent] = {}
        self._submission_intent_lock = RLock()
        self._now = now or (lambda: datetime.now(UTC))

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
        run = _StoredRun(
            run_id=run_id,
            config=config,
            ingest_checkpoint=IngestCheckpoint(
                source_kind="unattached",
                source_ref="",
                source_fingerprint="",
                ingestion_complete=bool(items),
            ),
        )
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

    def append_items_with_ingest_checkpoint(
        self,
        *,
        run_id: str,
        items: list[MaterializedItem],
        next_item_index: int,
        checkpoint_payload: JSONValue | None = None,
        ingestion_complete: bool,
    ) -> None:
        run = self._get_run(run_id)
        checkpoint = run.ingest_checkpoint
        if checkpoint is None:
            raise ValueError(f"run has no ingest checkpoint: {run_id}")
        items_to_append: list[MaterializedItem] = []
        seen_ids: set[str] = set()
        for item in sorted(items, key=lambda entry: entry.item_index):
            if item.item_id in seen_ids:
                raise ValueError(f"duplicate item_id: {item.item_id}")
            seen_ids.add(item.item_id)
            stored = run.items.get(item.item_id)
            if stored is None:
                items_to_append.append(item)
                continue
            if not _stored_item_matches_materialized(
                stored,
                item,
                min_replay_item_index=checkpoint.next_item_index,
            ):
                raise ValueError(f"duplicate item_id: {item.item_id}")
        self.append_items(run_id=run_id, items=items_to_append)
        self.update_ingest_checkpoint(
            run_id=run_id,
            next_item_index=next_item_index,
            checkpoint_payload=checkpoint_payload,
            ingestion_complete=ingestion_complete,
        )

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
        checkpoint_payload: JSONValue | None = None,
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
            checkpoint_payload=checkpoint_payload,
            ingestion_complete=ingestion_complete,
        )

    def get_run_config(self, *, run_id: str) -> PersistedRunConfig:
        return self._get_run(run_id).config

    def get_run_control_state(self, *, run_id: str) -> RunControlState:
        return self._get_run(run_id).control_state

    def set_run_control_state(
        self,
        *,
        run_id: str,
        control_state: RunControlState,
        control_reason: str | None = None,
    ) -> None:
        run = self._get_run(run_id)
        run.control_state = control_state
        run.control_reason = control_reason
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

    def requeue_local_items(self, *, run_id: str) -> int:
        run = self._get_run(run_id)
        requeued = 0
        for item in run.items.values():
            if item.status != ItemStatus.QUEUED_LOCAL:
                continue
            item.status = ItemStatus.PENDING
            item.active_batch_id = None
            item.active_custom_id = None
            item.active_submission_tokens = 0
            requeued += 1
        self._refresh_run_status(run)
        return requeued

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
            item.request_artifact_path for item in run.items.values() if item.request_artifact_path is not None
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
            {item.request_artifact_path for item in run.items.values() if item.request_artifact_path is not None}
        )
        output_paths = sorted(
            {batch.output_artifact_path for batch in run.batches.values() if batch.output_artifact_path is not None}
        )
        error_paths = sorted(
            {batch.error_artifact_path for batch in run.batches.values() if batch.error_artifact_path is not None}
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
        with self._submission_intent_lock:
            if quota_scope is not None and capacity_limit is not None:
                reserved = self._legacy_active_submitted_tokens(quota_scope=quota_scope) + sum(
                    intent.submission_tokens
                    for intent in self._submission_intents.values()
                    if intent.quota_scope == quota_scope and not intent.capacity_released
                )
                if reserved + submission_tokens > capacity_limit:
                    return False
            self._submission_intents[intent_id] = _StoredSubmissionIntent(
                intent_id=intent_id,
                run_id=run_id,
                custom_ids=[submission.custom_id for submission in submissions],
                item_ids=[submission.item_id for submission in submissions],
                submissions=list(submissions),
                quota_scope=quota_scope,
                submission_tokens=submission_tokens,
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
        with self._submission_intent_lock:
            intent = self._submission_intents.get(intent_id)
            if intent is None or intent.run_id != run_id or intent.status != "creating":
                raise ValueError(f"submission intent is not creating: {intent_id}")
            run = self._get_run(run_id)
            run.batches[provider_batch_id] = _StoredBatch(
                local_batch_id=local_batch_id,
                provider_batch_id=provider_batch_id,
                status=status,
                custom_ids=list(custom_ids),
            )
            for submission in submissions:
                item = run.items[submission.item_id]
                item.status = ItemStatus.SUBMITTED
                item.active_batch_id = provider_batch_id
                item.active_custom_id = submission.custom_id
                item.active_submission_tokens = submission.submission_tokens
                item.error = None
            intent.status = "active"
            intent.provider_batch_id = provider_batch_id
            self._refresh_run_status(run)

    def abandon_submission_intent(self, *, intent_id: str) -> None:
        with self._submission_intent_lock:
            intent = self._submission_intents.get(intent_id)
            if intent is not None:
                intent.status = "abandoned"
                intent.capacity_released = True

    def has_indeterminate_submission_intents(self, *, run_id: str) -> bool:
        with self._submission_intent_lock:
            return any(
                intent.run_id == run_id and intent.status == "creating" for intent in self._submission_intents.values()
            )

    def release_submission_capacity_for_batch(self, *, run_id: str, provider_batch_id: str) -> None:
        with self._submission_intent_lock:
            for intent in self._submission_intents.values():
                if (
                    intent.run_id == run_id
                    and intent.provider_batch_id == provider_batch_id
                    and intent.status == "active"
                ):
                    intent.capacity_released = True

    def abandon_indeterminate_submission_intents(self, *, run_id: str) -> None:
        with self._submission_intent_lock:
            abandoned_item_ids: set[str] = set()
            for intent in self._submission_intents.values():
                if intent.run_id == run_id and intent.status == "creating":
                    abandoned_item_ids.update(intent.item_ids)
                    intent.status = "operator_abandoned"
                    intent.capacity_released = True
            run = self._get_run(run_id)
            for item_id in abandoned_item_ids:
                item = run.items[item_id]
                if item.status is ItemStatus.QUEUED_LOCAL:
                    item.status = ItemStatus.PENDING
                    item.active_batch_id = None
                    item.active_custom_id = None
                    item.active_submission_tokens = 0
            self._refresh_run_status(run)

    def _legacy_active_submitted_tokens(self, *, quota_scope: str) -> int:
        """Count submitted rows that predate durable submission intents."""
        reserved = 0
        intent_batch_ids = {
            (intent.run_id, intent.provider_batch_id)
            for intent in self._submission_intents.values()
            if intent.provider_batch_id is not None
        }
        for run in self._runs.values():
            provider_config = run.config.provider_config
            if not isinstance(provider_config, OpenAIProviderConfig):
                continue
            if openai_enqueue_quota_scope(provider_config) != quota_scope:
                continue
            reserved += sum(
                item.active_submission_tokens
                for item in run.items.values()
                if item.status is ItemStatus.SUBMITTED and (run.run_id, item.active_batch_id) not in intent_batch_ids
            )
        return reserved

    def finalize_indeterminate_submission_as_created(self, *, run_id: str, provider_batch_id: str, status: str) -> None:
        with self._submission_intent_lock:
            intents = [
                intent
                for intent in self._submission_intents.values()
                if intent.run_id == run_id and intent.status == "creating"
            ]
            if len(intents) != 1:
                raise ValueError("expected exactly one indeterminate submission intent")
            intent = intents[0]
            self.finalize_submission_intent(
                run_id=run_id,
                intent_id=intent.intent_id,
                local_batch_id=f"recovered-{intent.intent_id[-8:]}",
                provider_batch_id=provider_batch_id,
                status=status,
                custom_ids=intent.custom_ids,
                submissions=intent.submissions,
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
            # A provider-terminal batch remains active locally until every
            # submitted item has been consumed.  This makes result download
            # and item-state mutations safely replayable after a crash.
            if batch.status not in self.TERMINAL_BATCH_STATUSES
            or any(
                item.status is ItemStatus.SUBMITTED and item.active_batch_id == batch.provider_batch_id
                for item in run.items.values()
            )
            or any(
                intent.run_id == run_id
                and intent.provider_batch_id == batch.provider_batch_id
                and not intent.capacity_released
                for intent in self._submission_intents.values()
            )
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
            if item.status is not ItemStatus.SUBMITTED:
                continue
            item.attempt_count += 1
            item.status = ItemStatus.COMPLETED
            item.terminal_result_sequence = self._next_terminal_sequence(run)
            item.terminalized_at = self._now()
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
            try:
                item = self._item_for_custom_id(run, failure.custom_id)
            except KeyError:
                # Replaying a provider-terminal artifact after another
                # consumer already cleared the active correlation is safe.
                continue
            if item.status is not ItemStatus.SUBMITTED:
                continue
            if failure.count_attempt:
                item.attempt_count += 1
            item.status = self._failed_status(
                attempt_count=item.attempt_count,
                error=failure.error,
                max_attempts=max_attempts,
                count_attempt=failure.count_attempt,
            )
            if item.status in self.TERMINAL_ITEM_STATUSES:
                item.terminal_result_sequence = self._next_terminal_sequence(run)
                item.terminalized_at = self._now()
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
            if item.status in self.TERMINAL_ITEM_STATUSES:
                item.terminal_result_sequence = self._next_terminal_sequence(run)
                item.terminalized_at = self._now()
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
        return sum(item.active_submission_tokens for item in run.items.values() if item.status == ItemStatus.SUBMITTED)

    def record_batch_retry_failure(
        self,
        *,
        run_id: str,
        error_class: str,
        base_delay_sec: float,
        max_delay_sec: float,
    ) -> RetryBackoffState:
        run = self._get_run(run_id)
        # Successful work in another batch must not clear this delay.  A new
        # failure after the previous delay has elapsed begins a fresh streak,
        # keeping exponential backoff bounded without cross-batch coupling.
        previous_is_active = run.backoff.next_retry_at is not None and run.backoff.next_retry_at > self._now()
        consecutive = (run.backoff.consecutive_failures if previous_is_active else 0) + 1
        total = run.backoff.total_failures + 1
        backoff_sec = compute_backoff_delay(
            consecutive_failures=consecutive,
            base_delay_sec=base_delay_sec,
            max_delay_sec=max_delay_sec,
        )
        next_retry_at = self._now() + timedelta(seconds=backoff_sec) if backoff_sec > 0 else None
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
            control_state=run.control_state,
            control_reason=run.control_reason,
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
                terminal_result_sequence=item.terminal_result_sequence,
                output_text=item.output_text,
                output_json=item.output_json,
                raw_response=item.raw_response,
                error=item.error,
            )
            for item in sorted(run.items.values(), key=lambda entry: entry.item_index)
        ]

    def get_terminal_item_records(
        self,
        *,
        run_id: str,
        after_sequence: int,
        limit: int | None = None,
    ) -> list[PersistedItemRecord]:
        run = self._get_run(run_id)
        records = [
            PersistedItemRecord(
                item_id=item.item_id,
                item_index=item.item_index,
                status=item.status,
                attempt_count=item.attempt_count,
                metadata=dict(item.metadata),
                terminal_result_sequence=item.terminal_result_sequence,
                output_text=item.output_text,
                output_json=item.output_json,
                raw_response=item.raw_response,
                error=item.error,
            )
            for item in sorted(
                run.items.values(),
                key=lambda entry: entry.terminal_result_sequence if entry.terminal_result_sequence is not None else -1,
            )
            if item.terminal_result_sequence is not None and item.terminal_result_sequence > after_sequence
        ]
        if limit is not None:
            return records[:limit]
        return records

    def mark_nonterminal_items_cancelled(
        self,
        *,
        run_id: str,
        error: ItemFailure,
    ) -> int:
        run = self._get_run(run_id)
        cancelled = 0
        for item in run.items.values():
            if item.status not in {
                ItemStatus.PENDING,
                ItemStatus.QUEUED_LOCAL,
                ItemStatus.FAILED_RETRYABLE,
            }:
                continue
            item.status = ItemStatus.FAILED_PERMANENT
            item.error = error
            item.active_batch_id = None
            item.active_custom_id = None
            item.active_submission_tokens = 0
            item.terminal_result_sequence = self._next_terminal_sequence(run)
            item.terminalized_at = self._now()
            cancelled += 1
        self._refresh_run_status(run)
        return cancelled

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
        all_terminal = all(run.items[item_id].status in self.TERMINAL_ITEM_STATUSES for item_id in run.item_ids)
        active_batches = bool(self.get_active_batches(run_id=run.run_id))
        backoff_remaining = self.get_batch_retry_backoff_remaining_sec(run_id=run.run_id)
        ingestion_complete = run.ingest_checkpoint is None or run.ingest_checkpoint.ingestion_complete
        if all_terminal and not active_batches and backoff_remaining <= 0 and ingestion_complete:
            failed_items = sum(
                1 for item_id in run.item_ids if run.items[item_id].status == ItemStatus.FAILED_PERMANENT
            )
            run.status = (
                RunLifecycleStatus.COMPLETED_WITH_FAILURES if failed_items > 0 else RunLifecycleStatus.COMPLETED
            )
        else:
            run.status = RunLifecycleStatus.RUNNING

    @staticmethod
    def _next_terminal_sequence(run: _StoredRun) -> int:
        current = max(
            (item.terminal_result_sequence for item in run.items.values() if item.terminal_result_sequence is not None),
            default=0,
        )
        return current + 1

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


def _stored_item_matches_materialized(
    stored: _StoredItem,
    materialized: MaterializedItem,
    *,
    min_replay_item_index: int,
) -> bool:
    return (
        materialized.item_index >= min_replay_item_index
        and stored.item_index == materialized.item_index
        and stored.payload == materialized.payload
        and stored.metadata == materialized.metadata
        and stored.prompt == materialized.prompt
        and stored.system_prompt == materialized.system_prompt
    )
