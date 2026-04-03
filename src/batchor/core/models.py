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
    item_id: str
    payload: PayloadT
    metadata: JSONObject = field(default_factory=dict)


@dataclass(frozen=True)
class PromptParts:
    prompt: str
    system_prompt: str | None = None


PromptBuilder: TypeAlias = Callable[[BatchItem[PayloadT]], PromptParts | str]
BatchItems: TypeAlias = "Iterable[BatchItem[PayloadT]] | ItemSource[PayloadT]"
OpenAIModelName: TypeAlias = OpenAIModel | str
OpenAIReasoningLevel: TypeAlias = OpenAIReasoningEffort | str


@dataclass(frozen=True)
class OpenAIEnqueueLimitConfig:
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
        if (
            self.enqueued_token_limit > 0
            and self.headroom >= self.enqueued_token_limit
        ):
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
        if self.max_batch_enqueued_tokens > 0 and effective_budget is not None:
            if self.max_batch_enqueued_tokens > effective_budget:
                raise ValueError(
                    "max_batch_enqueued_tokens must be <= effective inflight budget"
                )

    def to_payload(self) -> JSONObject:
        return {
            "enqueued_token_limit": self.enqueued_token_limit,
            "target_ratio": self.target_ratio,
            "headroom": self.headroom,
            "max_batch_enqueued_tokens": self.max_batch_enqueued_tokens,
        }

    @classmethod
    def from_payload(cls, payload: JSONObject) -> OpenAIEnqueueLimitConfig:
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
    model: OpenAIModelName
    api_key: str = ""
    endpoint: OpenAIEndpoint = OpenAIEndpoint.RESPONSES
    completion_window: str = "24h"
    request_timeout_sec: int = 30
    poll_interval_sec: float = 1.0
    reasoning_effort: OpenAIReasoningLevel | None = None
    enqueue_limits: OpenAIEnqueueLimitConfig = field(
        default_factory=OpenAIEnqueueLimitConfig
    )

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
        payload = self.to_payload()
        payload.pop("api_key", None)
        return payload

    @classmethod
    def from_payload(cls, payload: JSONObject) -> OpenAIProviderConfig:
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
    items: BatchItems[PayloadT]
    build_prompt: PromptBuilder[PayloadT]
    provider_config: ProviderConfig
    structured_output: type[ModelT] | None = None
    schema_name: str | None = None
    chunk_policy: ChunkPolicy = field(default_factory=ChunkPolicy)
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    batch_metadata: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ItemFailure:
    error_class: str
    message: str
    retryable: bool
    raw_error: JSONValue | None = None


@dataclass(frozen=True)
class StructuredItemResult(Generic[ModelT]):
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
    run_id: str
    status: RunLifecycleStatus
    total_items: int
    completed_items: int
    failed_items: int
    status_counts: dict[ItemStatus, int]
    active_batches: int
    backoff_remaining_sec: float


@dataclass(frozen=True)
class RunEvent:
    event_type: str
    run_id: str
    provider_kind: ProviderKind | None = None
    data: JSONObject = field(default_factory=dict)


@dataclass(frozen=True)
class RunSnapshot:
    run_id: str
    status: RunLifecycleStatus
    total_items: int
    completed_items: int
    failed_items: int
    status_counts: dict[ItemStatus, int]
    active_batches: int
    backoff_remaining_sec: float
    items: list[BatchResultItem]


@dataclass(frozen=True)
class ArtifactPruneResult:
    run_id: str
    removed_artifact_paths: list[str]
    missing_artifact_paths: list[str]
    cleared_item_pointers: int
    cleared_batch_pointers: int = 0


@dataclass(frozen=True)
class ArtifactExportResult:
    run_id: str
    destination_dir: str
    manifest_path: str
    results_path: str
    exported_artifact_paths: list[str]
