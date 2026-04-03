"""Domain model dataclasses for the batchor public API.

Contains the primary configuration and result types consumed by
:class:`~batchor.BatchRunner` callers:

* **Input** — :class:`BatchItem`, :class:`BatchJob`, :class:`PromptParts`
* **Provider config** — :class:`OpenAIProviderConfig`,
  :class:`OpenAIEnqueueLimitConfig`
* **Policies** — :class:`ChunkPolicy`, :class:`RetryPolicy`,
  :class:`ArtifactPolicy`
* **Results** — :class:`StructuredItemResult`, :class:`TextItemResult`,
  :class:`RunSummary`, :class:`RunSnapshot`, :class:`RunEvent`
* **Artifact operations** — :class:`ArtifactPruneResult`,
  :class:`ArtifactExportResult`
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable, Generic, TypeAlias, TypeVar

from pydantic import BaseModel

from batchor.core.enums import (
    ItemStatus,
    OpenAIEndpoint,
    OpenAIModel,
    OpenAIReasoningEffort,
    ProviderKind,
    RunControlState,
    RunLifecycleStatus,
)
from batchor.core.types import JSONObject, JSONValue
from batchor.providers.base import ProviderConfig

if TYPE_CHECKING:
    from collections.abc import Iterable

    from batchor.sources.base import ItemSource

PayloadT = TypeVar("PayloadT")
ModelT = TypeVar("ModelT", bound=BaseModel)


@dataclass(frozen=True)
class BatchItem(Generic[PayloadT]):
    """One logical unit of work inside a batch run.

    Attributes:
        item_id: Caller-assigned identifier, unique within the run.  Used as
            the basis for the ``custom_id`` in provider request lines and for
            correlating results back to items.
        payload: Arbitrary caller-defined data passed to ``build_prompt``.
        metadata: Optional key-value pairs stored alongside the item in state.
            The ``batchor_lineage`` key is reserved for source tracing when
            items originate from a file-backed :class:`~batchor.ItemSource`.
    """

    item_id: str
    payload: PayloadT
    metadata: JSONObject = field(default_factory=dict)


@dataclass(frozen=True)
class PromptParts:
    """Prompt text sent to the provider, with an optional system prompt.

    Attributes:
        prompt: The user-turn or primary input text.
        system_prompt: Optional system/instructions text.  Mapped to
            ``instructions`` for the Responses endpoint and to a
            ``system`` message for the Chat Completions endpoint.
    """

    prompt: str
    system_prompt: str | None = None


PromptBuilder: TypeAlias = Callable[[BatchItem[PayloadT]], PromptParts | str]
BatchItems: TypeAlias = "Iterable[BatchItem[PayloadT]] | ItemSource[PayloadT]"
OpenAIModelName: TypeAlias = OpenAIModel | str
OpenAIReasoningLevel: TypeAlias = OpenAIReasoningEffort | str


@dataclass(frozen=True)
class ArtifactPolicy:
    """Controls which provider artifacts are retained after a batch completes.

    Attributes:
        persist_raw_output_artifacts: When ``True`` (default), the raw
            provider output and error JSONL files are written to the artifact
            store and their paths recorded in state.  Set to ``False`` to
            skip retention and reduce disk usage.
    """

    persist_raw_output_artifacts: bool = True

    def to_payload(self) -> JSONObject:
        """Serialise the policy to a JSON-compatible dictionary.

        Returns:
            A ``JSONObject`` suitable for durable storage.
        """
        return {
            "persist_raw_output_artifacts": self.persist_raw_output_artifacts,
        }

    @classmethod
    def from_payload(cls, payload: JSONObject) -> ArtifactPolicy:
        """Deserialise a previously persisted policy payload.

        Args:
            payload: A ``JSONObject`` produced by :meth:`to_payload`.

        Returns:
            A reconstructed :class:`ArtifactPolicy` instance.

        Raises:
            TypeError: If ``persist_raw_output_artifacts`` is not a ``bool``.
        """
        persist_raw_output_artifacts = payload.get("persist_raw_output_artifacts", True)
        if not isinstance(persist_raw_output_artifacts, bool):
            raise TypeError("persist_raw_output_artifacts must be a bool")
        return cls(
            persist_raw_output_artifacts=persist_raw_output_artifacts,
        )


@dataclass(frozen=True)
class OpenAIEnqueueLimitConfig:
    """OpenAI-specific token budgeting controls for batch submission.

    When ``enqueued_token_limit`` is non-zero, the runner estimates the
    token footprint of each submitted batch and refuses to enqueue more
    tokens than the effective budget allows.  This prevents hitting the
    OpenAI enqueued-token-limit API error.

    Attributes:
        enqueued_token_limit: Hard cap (in tokens) on the OpenAI account's
            enqueue limit.  ``0`` disables token-budget enforcement.
        target_ratio: Fraction of ``enqueued_token_limit`` to use as the
            effective inflight budget.  Defaults to ``0.7`` (70 %).
        headroom: Absolute token buffer subtracted from the limit before
            computing the effective budget.  Defaults to ``0``.
        max_batch_enqueued_tokens: Optional per-batch token ceiling.
            ``0`` means no per-batch limit beyond the inflight budget.
    """

    enqueued_token_limit: int = 0
    target_ratio: float = 0.7
    headroom: int = 0
    max_batch_enqueued_tokens: int = 0

    def __post_init__(self) -> None:
        if self.enqueued_token_limit < 0:
            raise ValueError("enqueued_token_limit must be >= 0")
        if not (0 < self.target_ratio <= 1):
            raise ValueError("target_ratio must be in (0, 1]")
        if self.headroom < 0:
            raise ValueError("headroom must be >= 0")
        if self.enqueued_token_limit > 0 and self.headroom >= self.enqueued_token_limit:
            raise ValueError("headroom must be < enqueued_token_limit")
        if self.max_batch_enqueued_tokens < 0:
            raise ValueError("max_batch_enqueued_tokens must be >= 0")
        if self.enqueued_token_limit > 0:
            effective_budget = min(
                int(self.enqueued_token_limit * self.target_ratio),
                self.enqueued_token_limit - self.headroom,
            )
            if effective_budget <= 0:
                raise ValueError("effective inflight budget must be > 0")
        else:
            effective_budget = None
        if (
            self.max_batch_enqueued_tokens > 0
            and effective_budget is not None
            and self.max_batch_enqueued_tokens > effective_budget
        ):
            raise ValueError("max_batch_enqueued_tokens must be <= effective inflight budget")

    def to_payload(self) -> JSONObject:
        """Serialise the config to a JSON-compatible dictionary.

        Returns:
            A ``JSONObject`` suitable for durable storage.
        """
        return {
            "enqueued_token_limit": self.enqueued_token_limit,
            "target_ratio": self.target_ratio,
            "headroom": self.headroom,
            "max_batch_enqueued_tokens": self.max_batch_enqueued_tokens,
        }

    @classmethod
    def from_payload(cls, payload: JSONObject) -> OpenAIEnqueueLimitConfig:
        """Deserialise a previously persisted config payload.

        Args:
            payload: A ``JSONObject`` produced by :meth:`to_payload`.

        Returns:
            A reconstructed :class:`OpenAIEnqueueLimitConfig` instance.

        Raises:
            TypeError: If any field has the wrong type in *payload*.
        """
        enqueued_token_limit = payload.get("enqueued_token_limit", 0)
        target_ratio = payload.get("target_ratio", 0.7)
        headroom = payload.get("headroom", 0)
        max_batch_enqueued_tokens = payload.get("max_batch_enqueued_tokens", 0)
        if not isinstance(enqueued_token_limit, int):
            raise TypeError("enqueued_token_limit must be an int")
        if not isinstance(target_ratio, int | float):
            raise TypeError("target_ratio must be numeric")
        if not isinstance(headroom, int):
            raise TypeError("headroom must be an int")
        if not isinstance(max_batch_enqueued_tokens, int):
            raise TypeError("max_batch_enqueued_tokens must be an int")
        return cls(
            enqueued_token_limit=enqueued_token_limit,
            target_ratio=float(target_ratio),
            headroom=headroom,
            max_batch_enqueued_tokens=max_batch_enqueued_tokens,
        )


@dataclass(frozen=True)
class OpenAIProviderConfig(ProviderConfig):
    """Configuration for the built-in OpenAI Batch provider.

    Attributes:
        model: OpenAI model name (e.g. ``"gpt-4.1"`` or an
            :class:`~batchor.OpenAIModel` constant).
        api_key: OpenAI API key.  When empty the runner falls back to the
            ``OPENAI_API_KEY`` environment variable.
        endpoint: API endpoint path to use for batch requests.
        completion_window: Maximum time the OpenAI batch is allowed to run,
            e.g. ``"24h"``.
        request_timeout_sec: Timeout in seconds for individual API calls.
        poll_interval_sec: Seconds to sleep between polling cycles when
            :meth:`~batchor.Run.wait` is used without a custom interval.
        reasoning_effort: Reasoning effort level for supporting models.
            ``None`` omits the field from the request.
        enqueue_limits: Token-budget configuration that constrains how many
            tokens may be enqueued at once.
    """

    model: OpenAIModelName
    api_key: str = ""
    endpoint: OpenAIEndpoint = OpenAIEndpoint.RESPONSES
    completion_window: str = "24h"
    request_timeout_sec: int = 30
    poll_interval_sec: float = 1.0
    reasoning_effort: OpenAIReasoningLevel | None = None
    enqueue_limits: OpenAIEnqueueLimitConfig = field(default_factory=OpenAIEnqueueLimitConfig)

    def __post_init__(self) -> None:
        if self.request_timeout_sec <= 0:
            raise ValueError("request_timeout_sec must be > 0")
        if self.poll_interval_sec <= 0:
            raise ValueError("poll_interval_sec must be > 0")

    @property
    def provider_kind(self) -> ProviderKind:
        return ProviderKind.OPENAI

    def to_payload(self) -> JSONObject:
        return {
            "api_key": self.api_key,
            "model": self.model,
            "endpoint": self.endpoint.value,
            "completion_window": self.completion_window,
            "request_timeout_sec": self.request_timeout_sec,
            "poll_interval_sec": self.poll_interval_sec,
            "reasoning_effort": self.reasoning_effort,
            "enqueue_limits": self.enqueue_limits.to_payload(),
        }

    def to_public_payload(self) -> JSONObject:
        """Serialise the config without secret material.

        Returns:
            A ``JSONObject`` identical to :meth:`to_payload` but with
            ``api_key`` removed, safe to persist or log.
        """
        payload = self.to_payload()
        payload.pop("api_key", None)
        return payload

    @classmethod
    def from_payload(cls, payload: JSONObject) -> OpenAIProviderConfig:
        """Deserialise a previously persisted provider config payload.

        Args:
            payload: A ``JSONObject`` produced by :meth:`to_payload` or
                :meth:`to_public_payload`.

        Returns:
            A reconstructed :class:`OpenAIProviderConfig` instance.

        Raises:
            TypeError: If any field has the wrong type in *payload*.
        """
        api_key = payload.get("api_key", "")
        model = payload.get("model")
        endpoint = payload.get("endpoint", OpenAIEndpoint.RESPONSES.value)
        completion_window = payload.get("completion_window", "24h")
        request_timeout_sec = payload.get("request_timeout_sec", 30)
        poll_interval_sec = payload.get("poll_interval_sec", 1.0)
        reasoning_effort = payload.get("reasoning_effort")
        enqueue_limits = payload.get("enqueue_limits", {})
        if not isinstance(api_key, str):
            raise TypeError("api_key must be a string")
        if not isinstance(model, str):
            raise TypeError("model must be a string")
        if not isinstance(endpoint, str):
            raise TypeError("endpoint must be a string")
        if not isinstance(completion_window, str):
            raise TypeError("completion_window must be a string")
        if not isinstance(request_timeout_sec, int):
            raise TypeError("request_timeout_sec must be an int")
        if not isinstance(poll_interval_sec, int | float):
            raise TypeError("poll_interval_sec must be numeric")
        if reasoning_effort is not None and not isinstance(reasoning_effort, str):
            raise TypeError("reasoning_effort must be a string when provided")
        if not isinstance(enqueue_limits, dict):
            raise TypeError("enqueue_limits must be a JSON object")
        return cls(
            api_key=api_key,
            model=model,
            endpoint=OpenAIEndpoint(endpoint),
            completion_window=completion_window,
            request_timeout_sec=request_timeout_sec,
            poll_interval_sec=float(poll_interval_sec),
            reasoning_effort=reasoning_effort,
            enqueue_limits=OpenAIEnqueueLimitConfig.from_payload(enqueue_limits),
        )


@dataclass(frozen=True)
class ChunkPolicy:
    """Submission chunking limits applied before provider batches are created.

    A batch of pending items is split into one or more provider batches such
    that each chunk respects all three limits simultaneously.

    Attributes:
        max_requests: Maximum number of request lines per provider batch file.
            Defaults to ``50_000`` (OpenAI's maximum).
        max_file_bytes: Maximum size in bytes of a single batch input file.
            Defaults to ``150 MiB`` (OpenAI's maximum).
        chars_per_token: Fallback characters-per-token ratio used for token
            estimation when *tiktoken* is unavailable.
    """

    max_requests: int = 50_000
    max_file_bytes: int = 150 * 1024 * 1024
    chars_per_token: int = 4

    def __post_init__(self) -> None:
        if self.max_requests <= 0:
            raise ValueError("max_requests must be > 0")
        if self.max_file_bytes <= 0:
            raise ValueError("max_file_bytes must be > 0")
        if self.chars_per_token <= 0:
            raise ValueError("chars_per_token must be > 0")


@dataclass(frozen=True)
class RetryPolicy:
    """Retry limits for item-level and batch-control-plane recovery.

    Applies to both individual item failures (retryable provider errors) and
    transient batch control-plane failures (rate limits, timeouts).  Backoff
    uses exponential doubling capped at ``max_backoff_sec``.

    Attributes:
        max_attempts: Total attempt budget per item before marking it
            ``FAILED_PERMANENT``.  Defaults to ``3``.
        base_backoff_sec: Starting backoff delay in seconds.  Subsequent
            failures double this up to ``max_backoff_sec``.
        max_backoff_sec: Ceiling on the computed backoff delay in seconds.
    """

    max_attempts: int = 3
    base_backoff_sec: float = 1.0
    max_backoff_sec: float = 300.0

    def __post_init__(self) -> None:
        if self.max_attempts <= 0:
            raise ValueError("max_attempts must be > 0")
        if self.base_backoff_sec < 0:
            raise ValueError("base_backoff_sec must be >= 0")
        if self.max_backoff_sec < 0:
            raise ValueError("max_backoff_sec must be >= 0")


@dataclass(frozen=True)
class BatchJob(Generic[PayloadT, ModelT]):
    """Declarative description of a batch run.

    Passed to :meth:`~batchor.BatchRunner.start` or
    :meth:`~batchor.BatchRunner.run_and_wait` to create or resume a durable
    run.

    Attributes:
        items: An iterable of :class:`BatchItem` objects or an
            :class:`~batchor.ItemSource` that streams them.
        build_prompt: Callable that converts a :class:`BatchItem` to a
            :class:`PromptParts` (or a plain string for simple cases).
        provider_config: Provider-specific configuration, e.g.
            :class:`OpenAIProviderConfig`.
        structured_output: Optional Pydantic model class used to parse and
            validate each item's response as structured JSON.
        schema_name: Optional override for the JSON schema name sent to the
            provider.  Defaults to a snake_case version of the model class
            name.
        chunk_policy: Controls how pending items are split into provider
            batch files.
        retry_policy: Controls retry behaviour for transient failures.
        batch_metadata: Arbitrary string key-value pairs attached to every
            provider batch created for this run.
        artifact_policy: Controls which raw provider artifacts are retained.
    """

    items: BatchItems[PayloadT]
    build_prompt: PromptBuilder[PayloadT]
    provider_config: ProviderConfig
    structured_output: type[ModelT] | None = None
    schema_name: str | None = None
    chunk_policy: ChunkPolicy = field(default_factory=ChunkPolicy)
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    batch_metadata: dict[str, str] = field(default_factory=dict)
    artifact_policy: ArtifactPolicy = field(default_factory=ArtifactPolicy)


@dataclass(frozen=True)
class ItemFailure:
    """Structured failure payload attached to a terminal item result.

    Attributes:
        error_class: Short machine-readable failure category, e.g.
            ``"provider_item_error"`` or ``"structured_output_validation_failed"``.
        message: Human-readable description of the failure.
        retryable: ``True`` if the failure consumed an attempt and the item
            will be retried; ``False`` if the item is immediately permanent.
        raw_error: Optional raw error payload from the provider or parser,
            preserved verbatim for debugging.
    """

    error_class: str
    message: str
    retryable: bool
    raw_error: JSONValue | None = None


@dataclass(frozen=True)
class StructuredItemResult(Generic[ModelT]):
    """Terminal result for one structured-output item.

    Attributes:
        item_id: Identifier matching the originating :class:`BatchItem`.
        status: Final item status (``COMPLETED`` or ``FAILED_PERMANENT``).
        attempt_count: Number of provider attempts consumed.
        output: Validated Pydantic model instance, or ``None`` on failure.
        output_text: Raw text from the provider response before parsing.
        raw_response: Full provider response record for debugging.
        error: Populated when the item reached ``FAILED_PERMANENT``.
        metadata: Item metadata carried through from the source.
    """

    item_id: str
    status: ItemStatus
    attempt_count: int
    output: ModelT | None = None
    output_text: str | None = None
    raw_response: JSONObject | None = None
    error: ItemFailure | None = None
    metadata: JSONObject = field(default_factory=dict)


@dataclass(frozen=True)
class TextItemResult:
    """Terminal result for one text-output item.

    Attributes:
        item_id: Identifier matching the originating :class:`BatchItem`.
        status: Final item status (``COMPLETED`` or ``FAILED_PERMANENT``).
        attempt_count: Number of provider attempts consumed.
        output_text: Extracted text from the provider response, or ``None``
            on failure.
        raw_response: Full provider response record for debugging.
        error: Populated when the item reached ``FAILED_PERMANENT``.
        metadata: Item metadata carried through from the source.
    """

    item_id: str
    status: ItemStatus
    attempt_count: int
    output_text: str | None = None
    raw_response: JSONObject | None = None
    error: ItemFailure | None = None
    metadata: JSONObject = field(default_factory=dict)


type BatchResultItem = StructuredItemResult[BaseModel] | TextItemResult


@dataclass(frozen=True)
class RunSummary:
    """Aggregated durable run state without item payload expansion.

    Returned by :meth:`~batchor.Run.summary` and
    :meth:`~batchor.Run.refresh`.  Contains counters and lifecycle flags but
    not individual item results.

    Attributes:
        run_id: Stable run identifier.
        status: Current lifecycle status of the run.
        control_state: Operator control state (running / paused / cancelling).
        total_items: Total number of items registered for this run.
        completed_items: Number of items in ``COMPLETED`` status.
        failed_items: Number of items in ``FAILED_PERMANENT`` status.
        status_counts: Full per-status item count breakdown.
        active_batches: Number of provider batches currently in-flight.
        backoff_remaining_sec: Seconds until the next submission attempt is
            permitted (``0.0`` when not in backoff).
    """

    run_id: str
    status: RunLifecycleStatus
    control_state: RunControlState
    total_items: int
    completed_items: int
    failed_items: int
    status_counts: dict[ItemStatus, int]
    active_batches: int
    backoff_remaining_sec: float


@dataclass(frozen=True)
class RunEvent:
    """Observer event emitted by the runner during lifecycle transitions.

    Passed to the ``observer`` callable supplied to :class:`~batchor.BatchRunner`
    on each notable state change.

    Attributes:
        event_type: Short identifier for the event kind, e.g.
            ``"batch_submitted"``, ``"items_completed"``, ``"run_resumed"``.
        run_id: The run that produced this event.
        provider_kind: Provider that triggered the event, or ``None`` for
            storage-only events.
        data: Optional extra fields specific to the event type.
    """

    event_type: str
    run_id: str
    provider_kind: ProviderKind | None = None
    data: JSONObject = field(default_factory=dict)


@dataclass(frozen=True)
class RunSnapshot:
    """Expanded durable run state including current terminal item payloads.

    A superset of :class:`RunSummary` that additionally includes the list of
    terminal item results available at query time.  Returned by
    :meth:`~batchor.Run.snapshot`.

    Attributes:
        run_id: Stable run identifier.
        status: Current lifecycle status of the run.
        control_state: Operator control state.
        total_items: Total number of items registered for this run.
        completed_items: Number of items in ``COMPLETED`` status.
        failed_items: Number of items in ``FAILED_PERMANENT`` status.
        status_counts: Full per-status item count breakdown.
        active_batches: Number of provider batches currently in-flight.
        backoff_remaining_sec: Seconds until the next submission is permitted.
        items: All terminal item results available at query time.
    """

    run_id: str
    status: RunLifecycleStatus
    control_state: RunControlState
    total_items: int
    completed_items: int
    failed_items: int
    status_counts: dict[ItemStatus, int]
    active_batches: int
    backoff_remaining_sec: float
    items: list[BatchResultItem]


@dataclass(frozen=True)
class ArtifactPruneResult:
    """Result returned after pruning retained artifacts for a terminal run.

    Attributes:
        run_id: The run whose artifacts were pruned.
        removed_artifact_paths: Relative paths of files successfully deleted.
        missing_artifact_paths: Relative paths that were recorded in state but
            not found on disk (already deleted or never written).
        cleared_item_pointers: Number of per-item artifact pointer records
            cleared in state.
        cleared_batch_pointers: Number of per-batch artifact pointer records
            cleared in state (only non-zero when raw output artifacts are pruned).
    """

    run_id: str
    removed_artifact_paths: list[str]
    missing_artifact_paths: list[str]
    cleared_item_pointers: int
    cleared_batch_pointers: int = 0


@dataclass(frozen=True)
class ArtifactExportResult:
    """Result returned after exporting retained artifacts for a terminal run.

    Attributes:
        run_id: The run whose artifacts were exported.
        destination_dir: Absolute path to the export root directory
            (``<destination>/<run_id>/``).
        manifest_path: Absolute path to the generated ``manifest.json`` file.
        results_path: Absolute path to the generated ``results.jsonl`` file.
        exported_artifact_paths: Relative artifact paths that were copied.
    """

    run_id: str
    destination_dir: str
    manifest_path: str
    results_path: str
    exported_artifact_paths: list[str]


@dataclass(frozen=True)
class TerminalResultsPage:
    """A page of terminal item results for cursor-based streaming.

    Attributes:
        run_id: The run these results belong to.
        items: Terminal item results in this page.
        next_after_sequence: Opaque cursor to pass as ``after_sequence`` in
            the next call to retrieve the following page.
    """

    run_id: str
    items: list[BatchResultItem]
    next_after_sequence: int


@dataclass(frozen=True)
class TerminalResultsExportResult:
    """Result returned after exporting terminal results to a JSONL file.

    Attributes:
        run_id: The run whose results were exported.
        destination_path: Absolute path to the JSONL file written.
        exported_count: Number of result records written in this call.
        next_after_sequence: Cursor value for continuing an incremental export.
    """

    run_id: str
    destination_path: str
    exported_count: int
    next_after_sequence: int
