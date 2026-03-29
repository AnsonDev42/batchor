from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Generic, Literal, TypeAlias, TypeVar

from pydantic import BaseModel

from batchor.types import JSONObject, JSONValue

PayloadT = TypeVar("PayloadT")
ModelT = TypeVar("ModelT", bound=BaseModel)

type ItemStatus = Literal[
    "pending",
    "queued_local",
    "submitted",
    "completed",
    "failed_retryable",
    "failed_permanent",
]
type RunLifecycleStatus = Literal["running", "completed"]


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


@dataclass(frozen=True)
class OpenAIProviderConfig:
    api_key: str
    model: str
    endpoint: str = "/v1/responses"
    completion_window: str = "24h"
    request_timeout_sec: int = 30
    poll_interval_sec: float = 1.0


@dataclass(frozen=True)
class ChunkPolicy:
    max_requests: int = 50_000
    max_file_bytes: int = 150 * 1024 * 1024
    max_enqueued_tokens: int = 0
    chars_per_token: int = 4


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    base_backoff_sec: float = 1.0
    max_backoff_sec: float = 300.0


@dataclass(frozen=True)
class InflightPolicy:
    enqueued_token_limit: int = 0
    target_ratio: float = 0.7
    headroom: int = 0


@dataclass(frozen=True)
class BatchJob(Generic[PayloadT, ModelT]):
    items: list[BatchItem[PayloadT]]
    build_prompt: PromptBuilder[PayloadT]
    provider_config: OpenAIProviderConfig
    structured_output: type[ModelT] | None = None
    schema_name: str | None = None
    chunk_policy: ChunkPolicy = field(default_factory=ChunkPolicy)
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    inflight_policy: InflightPolicy = field(default_factory=InflightPolicy)
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
    status_counts: dict[str, int]
    active_batches: int
    backoff_remaining_sec: float


@dataclass(frozen=True)
class RunSnapshot:
    run_id: str
    status: RunLifecycleStatus
    total_items: int
    completed_items: int
    failed_items: int
    status_counts: dict[str, int]
    active_batches: int
    backoff_remaining_sec: float
    items: list[BatchResultItem]
