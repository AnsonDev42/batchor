"""Domain models and the abstract :class:`StateStore` interface for run state.

The state store is the durable backbone of batchor runs.  It tracks:

* Run configuration and lifecycle state.
* Per-item status, attempt counts, and terminal results.
* Active provider batches and their remote identifiers.
* Artifact pointers (request JSONL paths, output JSONL paths).
* Retry backoff state for the batch control plane.
* Ingest checkpoints for resumable item sources.

:class:`StateStore` is an abstract base class.  The primary implementations
are :class:`~batchor.SQLiteStorage` (default) and
:class:`~batchor.PostgresStorage`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime

from batchor.core.enums import ItemStatus, RunControlState
from batchor.core.models import ArtifactPolicy, ChunkPolicy, ItemFailure, RetryPolicy, RunSummary
from batchor.core.types import JSONObject, JSONValue
from batchor.providers.base import ProviderConfig


@dataclass(frozen=True)
class PersistedRunConfig:
    """Serialised run configuration stored durably alongside run state.

    Reconstructed from storage when a run is resumed in a new process.

    Attributes:
        provider_config: Provider-specific configuration object.
        chunk_policy: Controls how pending items are split into batch files.
        retry_policy: Item and control-plane retry settings.
        batch_metadata: Arbitrary string metadata attached to provider batches.
        artifact_policy: Controls which raw artifacts are retained.
        schema_name: Optional explicit JSON Schema name for structured output.
        structured_output_module: ``__module__`` of the structured output class,
            used for re-import on resume.
        structured_output_qualname: ``__qualname__`` of the structured output
            class, used for re-import on resume.
    """

    provider_config: ProviderConfig
    chunk_policy: ChunkPolicy
    retry_policy: RetryPolicy
    batch_metadata: dict[str, str]
    artifact_policy: ArtifactPolicy = field(default_factory=ArtifactPolicy)
    schema_name: str | None = None
    structured_output_module: str | None = None
    structured_output_qualname: str | None = None

    @property
    def is_structured(self) -> bool:
        """Return ``True`` when this run uses structured JSON output."""
        return (
            self.structured_output_module is not None
            and self.structured_output_qualname is not None
        )


@dataclass(frozen=True)
class RetryBackoffState:
    """Persisted exponential backoff state for the batch control plane.

    Attributes:
        consecutive_failures: Number of consecutive control-plane failures
            since the last success.
        total_failures: Cumulative total control-plane failures for the run.
        backoff_sec: Current backoff delay in seconds.
        next_retry_at: Wall-clock time at which the next attempt is permitted.
        last_error_class: Error class from the most recent failure.
    """

    consecutive_failures: int = 0
    total_failures: int = 0
    backoff_sec: float = 0.0
    next_retry_at: datetime | None = None
    last_error_class: str | None = None


@dataclass(frozen=True)
class IngestCheckpoint:
    """Persisted checkpoint for a resumable item source.

    Attributes:
        source_kind: Source type identifier (e.g. ``"csv"``).
        source_ref: Human-readable source reference (e.g. absolute file path).
        source_fingerprint: Content fingerprint used to detect source changes.
        next_item_index: 0-based index of the next item to ingest (for
            resumable sources).
        checkpoint_payload: Opaque payload for the source's own resume logic.
        ingestion_complete: ``True`` once all source items have been ingested.
    """

    source_kind: str
    source_ref: str
    source_fingerprint: str
    next_item_index: int = 0
    checkpoint_payload: JSONValue | None = None
    ingestion_complete: bool = False


@dataclass(frozen=True)
class MaterializedItem:
    """An item fully resolved from its source, ready to be persisted.

    Attributes:
        item_id: Caller-assigned unique item identifier.
        item_index: 0-based position within the run's item list.
        payload: Serialised item payload (JSON-compatible).
        metadata: Per-item metadata.
        prompt: Resolved prompt text.
        system_prompt: Optional system/instructions text.
    """

    item_id: str
    item_index: int
    payload: JSONValue
    metadata: JSONObject
    prompt: str
    system_prompt: str | None = None


@dataclass(frozen=True)
class ClaimedItem:
    """An item claimed from pending state for the current submission cycle.

    Attributes:
        item_id: Item identifier.
        metadata: Item metadata.
        prompt: Prompt text for building the request line.
        system_prompt: Optional system prompt text.
        attempt_count: Number of previous attempts for this item.
        request_artifact_path: Relative path to the request artifact from a
            previous attempt, if available.
        request_artifact_line: 1-based line number within the artifact file.
        request_sha256: SHA-256 of the serialised request line from a
            previous attempt (for deduplication / replay).
    """

    item_id: str
    metadata: JSONObject
    prompt: str
    system_prompt: str | None
    attempt_count: int
    request_artifact_path: str | None = None
    request_artifact_line: int | None = None
    request_sha256: str | None = None


@dataclass(frozen=True)
class RequestArtifactPointer:
    """Records the location of a serialised request line in an artifact file.

    Attributes:
        item_id: Item this request was built for.
        artifact_path: Relative path to the JSONL artifact file.
        line_number: 1-based line number within *artifact_path*.
        request_sha256: SHA-256 of the serialised request line.
    """

    item_id: str
    artifact_path: str
    line_number: int
    request_sha256: str


@dataclass(frozen=True)
class BatchArtifactPointer:
    """Records the paths of raw output artifacts for a provider batch.

    Attributes:
        provider_batch_id: Provider-assigned batch identifier.
        output_artifact_path: Relative path to the success output JSONL, or
            ``None`` when not retained.
        error_artifact_path: Relative path to the error output JSONL, or
            ``None`` when not retained.
    """

    provider_batch_id: str
    output_artifact_path: str | None = None
    error_artifact_path: str | None = None


@dataclass(frozen=True)
class RunArtifactInventory:
    """Complete list of retained artifact paths for a run.

    Attributes:
        request_artifact_paths: Relative paths to request JSONL files.
        output_artifact_paths: Relative paths to provider output JSONL files.
        error_artifact_paths: Relative paths to provider error JSONL files.
        exported_at: Timestamp of the last successful artifact export, or
            ``None`` if never exported.
        export_root: Absolute path of the export destination root, or ``None``
            if never exported.
    """

    request_artifact_paths: list[str]
    output_artifact_paths: list[str]
    error_artifact_paths: list[str]
    exported_at: datetime | None = None
    export_root: str | None = None


@dataclass(frozen=True)
class PreparedSubmission:
    """Minimal record of one successfully submitted item.

    Attributes:
        item_id: Item identifier.
        custom_id: Provider ``custom_id`` used in the request line.
        submission_tokens: Estimated token count for this request line.
    """

    item_id: str
    custom_id: str
    submission_tokens: int


@dataclass(frozen=True)
class CompletedItemRecord:
    """Terminal success result for one item, ready to persist.

    Attributes:
        custom_id: Provider ``custom_id`` that matched the output record.
        output_text: Extracted text from the provider response.
        raw_response: Full provider response record.
        output_json: Parsed JSON value for structured output items.
    """

    custom_id: str
    output_text: str
    raw_response: JSONObject
    output_json: JSONValue | None = None


@dataclass(frozen=True)
class ItemFailureRecord:
    """Terminal failure result for one item (identified by ``custom_id``).

    Attributes:
        custom_id: Provider ``custom_id`` that matched the error record.
        error: Structured failure details.
        count_attempt: When ``True`` this failure increments the item's
            attempt counter toward ``max_attempts``.
    """

    custom_id: str
    error: ItemFailure
    count_attempt: bool


@dataclass(frozen=True)
class QueuedItemFailureRecord:
    """Failure for an item still in the local queue (identified by ``item_id``).

    Used when an item is rejected before submission (e.g. token budget
    exceeded) and must be failed without having been assigned a ``custom_id``.

    Attributes:
        item_id: Item identifier.
        error: Structured failure details.
        count_attempt: Whether this failure increments the attempt counter.
    """

    item_id: str
    error: ItemFailure
    count_attempt: bool


@dataclass(frozen=True)
class ActiveBatchRecord:
    """Snapshot of an in-flight provider batch.

    Attributes:
        provider_batch_id: Provider-assigned batch identifier.
        status: Last known batch status string from the provider.
        output_file_id: Provider file ID for the output JSONL once available.
        error_file_id: Provider file ID for the error JSONL once available.
    """

    provider_batch_id: str
    status: str
    output_file_id: str | None = None
    error_file_id: str | None = None


@dataclass(frozen=True)
class PersistedItemRecord:
    """Full persisted state for one item, including terminal result fields.

    Attributes:
        item_id: Item identifier.
        item_index: 0-based position within the run.
        status: Current item lifecycle status.
        attempt_count: Number of attempts consumed.
        metadata: Item metadata.
        terminal_result_sequence: Monotonically increasing sequence number
            assigned when the item reaches a terminal status.
        output_text: Extracted response text (terminal items only).
        output_json: Parsed structured output JSON (structured items only).
        raw_response: Full provider response record (terminal items only).
        error: Failure details (failed items only).
    """

    item_id: str
    item_index: int
    status: ItemStatus
    attempt_count: int
    metadata: JSONObject
    terminal_result_sequence: int | None = None
    output_text: str | None = None
    output_json: JSONValue | None = None
    raw_response: JSONObject | None = None
    error: ItemFailure | None = None


class StateStore(ABC):
    """Abstract interface for durable run state storage.

    All methods are synchronous and transactional within the scope of a single
    call.  Implementations must be safe to use from multiple threads provided
    callers do not share a connection outside a method call.

    The two primary implementations are:

    * :class:`~batchor.SQLiteStorage` — file-backed, zero-dependency (default).
    * :class:`~batchor.PostgresStorage` — shared Postgres control plane.
    """

    @abstractmethod
    def has_run(self, *, run_id: str) -> bool:
        """Return ``True`` if a run with *run_id* exists in storage.

        Args:
            run_id: Run identifier to check.
        """
        ...

    @abstractmethod
    def create_run(
        self,
        *,
        run_id: str,
        config: PersistedRunConfig,
        items: list[MaterializedItem],
    ) -> None:
        """Create a new run record in storage.

        Args:
            run_id: Unique identifier for the new run.
            config: Serialised run configuration.
            items: Optional initial batch of materialized items to insert.
        """
        ...

    @abstractmethod
    def append_items(
        self,
        *,
        run_id: str,
        items: list[MaterializedItem],
    ) -> None:
        """Append additional items to an existing run.

        Args:
            run_id: Identifier of the run to extend.
            items: Materialized items to append.
        """
        ...

    @abstractmethod
    def set_ingest_checkpoint(
        self,
        *,
        run_id: str,
        checkpoint: IngestCheckpoint,
    ) -> None:
        """Persist the initial ingest checkpoint for a checkpointed source.

        Args:
            run_id: Run identifier.
            checkpoint: Checkpoint representing the start-of-source state.
        """
        ...

    @abstractmethod
    def get_ingest_checkpoint(self, *, run_id: str) -> IngestCheckpoint | None:
        """Return the current ingest checkpoint, or ``None`` if not set.

        Args:
            run_id: Run identifier.
        """
        ...

    @abstractmethod
    def update_ingest_checkpoint(
        self,
        *,
        run_id: str,
        next_item_index: int,
        checkpoint_payload: JSONValue | None = None,
        ingestion_complete: bool,
    ) -> None:
        """Advance the ingest checkpoint after a batch of items is ingested.

        Args:
            run_id: Run identifier.
            next_item_index: 0-based index of the next item to ingest.
            checkpoint_payload: Updated source-specific checkpoint payload.
            ingestion_complete: ``True`` once all items have been ingested.
        """
        ...

    @abstractmethod
    def get_run_config(self, *, run_id: str) -> PersistedRunConfig:
        """Return the persisted configuration for a run.

        Args:
            run_id: Run identifier.
        """
        ...

    @abstractmethod
    def get_run_control_state(self, *, run_id: str) -> RunControlState:
        """Return the current operator control state for a run.

        Args:
            run_id: Run identifier.
        """
        ...

    @abstractmethod
    def set_run_control_state(
        self,
        *,
        run_id: str,
        control_state: RunControlState,
    ) -> None:
        """Persist a new operator control state for a run.

        Args:
            run_id: Run identifier.
            control_state: The new control state to set.
        """
        ...

    @abstractmethod
    def claim_items_for_submission(
        self,
        *,
        run_id: str,
        max_attempts: int,
        limit: int | None = None,
    ) -> list[ClaimedItem]:
        """Atomically claim a batch of pending items for the submission cycle.

        Items are moved from ``PENDING`` to ``QUEUED_LOCAL``.  Only items
        whose attempt count is less than *max_attempts* are eligible.

        Args:
            run_id: Run identifier.
            max_attempts: Maximum attempts per item.
            limit: Maximum number of items to claim.

        Returns:
            List of claimed items (may be empty if none are pending).
        """
        ...

    @abstractmethod
    def release_items_to_pending(self, *, run_id: str, item_ids: list[str]) -> None:
        """Return locally-queued items to ``PENDING`` state.

        Called when a submission cycle is interrupted before all claimed items
        could be submitted.

        Args:
            run_id: Run identifier.
            item_ids: Identifiers of items to release.
        """
        ...

    @abstractmethod
    def requeue_local_items(self, *, run_id: str) -> int:
        """Re-queue any items stuck in ``QUEUED_LOCAL`` back to ``PENDING``.

        Called when a run handle is rehydrated to recover items claimed by a
        previous process that terminated unexpectedly.

        Args:
            run_id: Run identifier.

        Returns:
            Number of items re-queued.
        """
        ...

    @abstractmethod
    def record_request_artifacts(
        self,
        *,
        run_id: str,
        pointers: list[RequestArtifactPointer],
    ) -> None:
        """Persist per-item request artifact pointers.

        Args:
            run_id: Run identifier.
            pointers: Artifact pointer records to persist.
        """
        ...

    @abstractmethod
    def get_request_artifact_paths(self, *, run_id: str) -> list[str]:
        """Return all distinct request artifact paths recorded for a run.

        Args:
            run_id: Run identifier.
        """
        ...

    @abstractmethod
    def clear_request_artifact_pointers(
        self,
        *,
        run_id: str,
        artifact_paths: list[str],
    ) -> int:
        """Clear per-item request artifact pointers for the given paths.

        Args:
            run_id: Run identifier.
            artifact_paths: Relative artifact paths to clear.

        Returns:
            Number of pointer records cleared.
        """
        ...

    @abstractmethod
    def record_batch_artifacts(
        self,
        *,
        run_id: str,
        pointers: list[BatchArtifactPointer],
    ) -> None:
        """Persist batch output and error artifact pointers.

        Args:
            run_id: Run identifier.
            pointers: Batch artifact pointer records to persist.
        """
        ...

    @abstractmethod
    def get_artifact_inventory(self, *, run_id: str) -> RunArtifactInventory:
        """Return the complete artifact inventory for a run.

        Args:
            run_id: Run identifier.
        """
        ...

    @abstractmethod
    def clear_batch_artifact_pointers(
        self,
        *,
        run_id: str,
        artifact_paths: list[str],
    ) -> int:
        """Clear batch artifact pointers for the given paths.

        Args:
            run_id: Run identifier.
            artifact_paths: Relative artifact paths to clear.

        Returns:
            Number of pointer records cleared.
        """
        ...

    @abstractmethod
    def mark_artifacts_exported(
        self,
        *,
        run_id: str,
        export_root: str,
    ) -> None:
        """Record that artifacts have been exported to *export_root*.

        Args:
            run_id: Run identifier.
            export_root: Absolute path to the export directory.
        """
        ...

    @abstractmethod
    def register_batch(
        self,
        *,
        run_id: str,
        local_batch_id: str,
        provider_batch_id: str,
        status: str,
        custom_ids: list[str],
    ) -> None:
        """Record a newly submitted provider batch.

        Args:
            run_id: Run identifier.
            local_batch_id: Locally generated batch identifier.
            provider_batch_id: Provider-assigned batch identifier.
            status: Initial batch status string.
            custom_ids: ``custom_id`` values for all items in this batch.
        """
        ...

    @abstractmethod
    def mark_items_submitted(
        self,
        *,
        run_id: str,
        provider_batch_id: str,
        submissions: list[PreparedSubmission],
    ) -> None:
        """Transition items from ``QUEUED_LOCAL`` to ``SUBMITTED``.

        Args:
            run_id: Run identifier.
            provider_batch_id: Batch that now owns these items.
            submissions: Submission records including ``custom_id`` and token
                estimates.
        """
        ...

    @abstractmethod
    def update_batch_status(
        self,
        *,
        run_id: str,
        provider_batch_id: str,
        status: str,
        output_file_id: str | None = None,
        error_file_id: str | None = None,
    ) -> None:
        """Persist the latest polled status for a provider batch.

        Args:
            run_id: Run identifier.
            provider_batch_id: Batch to update.
            status: New status string from the provider.
            output_file_id: Provider file ID for output, if now available.
            error_file_id: Provider file ID for errors, if now available.
        """
        ...

    @abstractmethod
    def get_active_batches(self, *, run_id: str) -> list[ActiveBatchRecord]:
        """Return all in-flight (non-terminal) batch records for a run.

        Args:
            run_id: Run identifier.
        """
        ...

    @abstractmethod
    def get_submitted_custom_ids_for_batch(
        self,
        *,
        run_id: str,
        provider_batch_id: str,
    ) -> list[str]:
        """Return all ``custom_id`` values submitted in *provider_batch_id*.

        Args:
            run_id: Run identifier.
            provider_batch_id: Batch to query.
        """
        ...

    @abstractmethod
    def mark_items_completed(
        self,
        *,
        run_id: str,
        completions: list[CompletedItemRecord],
    ) -> None:
        """Transition items to ``COMPLETED`` and persist their results.

        Args:
            run_id: Run identifier.
            completions: Completed item records to persist.
        """
        ...

    @abstractmethod
    def mark_items_failed(
        self,
        *,
        run_id: str,
        failures: list[ItemFailureRecord],
        max_attempts: int,
    ) -> None:
        """Transition items after a provider-level failure.

        Items whose attempt count reaches *max_attempts* are moved to
        ``FAILED_PERMANENT``; others are moved to ``FAILED_RETRYABLE`` and
        will be re-queued on the next submission cycle.

        Args:
            run_id: Run identifier.
            failures: Failure records identified by ``custom_id``.
            max_attempts: Attempt ceiling from the retry policy.
        """
        ...

    @abstractmethod
    def mark_queued_items_failed(
        self,
        *,
        run_id: str,
        failures: list[QueuedItemFailureRecord],
        max_attempts: int,
    ) -> None:
        """Fail items that are still in the local queue (pre-submission).

        Used for token-budget rejections where items never received a
        ``custom_id``.

        Args:
            run_id: Run identifier.
            failures: Failure records identified by ``item_id``.
            max_attempts: Attempt ceiling from the retry policy.
        """
        ...

    @abstractmethod
    def reset_batch_items_to_pending(
        self,
        *,
        run_id: str,
        provider_batch_id: str,
        error: ItemFailure,
    ) -> None:
        """Reset all items in a failed batch to pending for retry.

        Args:
            run_id: Run identifier.
            provider_batch_id: The batch that failed or was cancelled.
            error: Failure details to record on each item.
        """
        ...

    @abstractmethod
    def get_active_submitted_token_estimate(self, *, run_id: str) -> int:
        """Return the total token estimate for all currently submitted items.

        Args:
            run_id: Run identifier.
        """
        ...

    @abstractmethod
    def record_batch_retry_failure(
        self,
        *,
        run_id: str,
        error_class: str,
        base_delay_sec: float,
        max_delay_sec: float,
    ) -> RetryBackoffState:
        """Record a batch control-plane failure and advance the backoff state.

        Args:
            run_id: Run identifier.
            error_class: Short error category from the failure.
            base_delay_sec: Base backoff delay.
            max_delay_sec: Maximum backoff delay.

        Returns:
            Updated :class:`RetryBackoffState`.
        """
        ...

    @abstractmethod
    def clear_batch_retry_backoff(self, *, run_id: str) -> None:
        """Reset the control-plane backoff state after a successful operation.

        Args:
            run_id: Run identifier.
        """
        ...

    @abstractmethod
    def get_batch_retry_backoff_remaining_sec(self, *, run_id: str) -> float:
        """Return the seconds remaining until the next retry is permitted.

        Args:
            run_id: Run identifier.

        Returns:
            Remaining backoff in seconds, or ``0.0`` if no backoff is active.
        """
        ...

    @abstractmethod
    def get_run_summary(self, *, run_id: str) -> RunSummary:
        """Compute and return the aggregated summary for a run.

        Args:
            run_id: Run identifier.
        """
        ...

    @abstractmethod
    def get_item_records(self, *, run_id: str) -> list[PersistedItemRecord]:
        """Return all item records for a run.

        Args:
            run_id: Run identifier.
        """
        ...

    @abstractmethod
    def get_terminal_item_records(
        self,
        *,
        run_id: str,
        after_sequence: int,
        limit: int | None = None,
    ) -> list[PersistedItemRecord]:
        """Return terminal item records using cursor-based pagination.

        Args:
            run_id: Run identifier.
            after_sequence: Return only records whose
                ``terminal_result_sequence`` is greater than this value.
            limit: Maximum number of records to return.
        """
        ...

    @abstractmethod
    def mark_nonterminal_items_cancelled(
        self,
        *,
        run_id: str,
        error: ItemFailure,
    ) -> int:
        """Move all non-terminal items to ``FAILED_PERMANENT`` with *error*.

        Called when a cancellation is finalised.

        Args:
            run_id: Run identifier.
            error: Cancellation failure details to attach to each item.

        Returns:
            Number of items that were cancelled.
        """
        ...
