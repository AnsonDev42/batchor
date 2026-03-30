from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

from batchor.core.enums import ItemStatus
from batchor.core.models import ChunkPolicy, ItemFailure, RetryPolicy, RunSummary
from batchor.core.types import JSONObject, JSONValue
from batchor.providers.base import ProviderConfig


@dataclass(frozen=True)
class PersistedRunConfig:
    provider_config: ProviderConfig
    chunk_policy: ChunkPolicy
    retry_policy: RetryPolicy
    batch_metadata: dict[str, str]
    schema_name: str | None = None
    structured_output_module: str | None = None
    structured_output_qualname: str | None = None

    @property
    def is_structured(self) -> bool:
        return (
            self.structured_output_module is not None
            and self.structured_output_qualname is not None
        )


@dataclass(frozen=True)
class RetryBackoffState:
    consecutive_failures: int = 0
    total_failures: int = 0
    backoff_sec: float = 0.0
    next_retry_at: datetime | None = None
    last_error_class: str | None = None


@dataclass(frozen=True)
class MaterializedItem:
    item_id: str
    item_index: int
    payload: JSONValue
    metadata: JSONObject
    prompt: str
    system_prompt: str | None = None


@dataclass(frozen=True)
class ClaimedItem:
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
    item_id: str
    artifact_path: str
    line_number: int
    request_sha256: str


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
    output_json: JSONValue | None = None


@dataclass(frozen=True)
class ItemFailureRecord:
    custom_id: str
    error: ItemFailure
    count_attempt: bool


@dataclass(frozen=True)
class QueuedItemFailureRecord:
    item_id: str
    error: ItemFailure
    count_attempt: bool


@dataclass(frozen=True)
class ActiveBatchRecord:
    provider_batch_id: str
    status: str
    output_file_id: str | None = None
    error_file_id: str | None = None


@dataclass(frozen=True)
class PersistedItemRecord:
    item_id: str
    item_index: int
    status: ItemStatus
    attempt_count: int
    metadata: JSONObject
    output_text: str | None = None
    output_json: JSONValue | None = None
    raw_response: JSONObject | None = None
    error: ItemFailure | None = None


class StateStore(ABC):
    @abstractmethod
    def create_run(
        self,
        *,
        run_id: str,
        config: PersistedRunConfig,
        items: list[MaterializedItem],
    ) -> None: ...

    @abstractmethod
    def append_items(
        self,
        *,
        run_id: str,
        items: list[MaterializedItem],
    ) -> None: ...

    @abstractmethod
    def get_run_config(self, *, run_id: str) -> PersistedRunConfig: ...

    @abstractmethod
    def claim_items_for_submission(
        self,
        *,
        run_id: str,
        max_attempts: int,
        limit: int | None = None,
    ) -> list[ClaimedItem]: ...

    @abstractmethod
    def release_items_to_pending(self, *, run_id: str, item_ids: list[str]) -> None: ...

    @abstractmethod
    def record_request_artifacts(
        self,
        *,
        run_id: str,
        pointers: list[RequestArtifactPointer],
    ) -> None: ...

    @abstractmethod
    def get_request_artifact_paths(self, *, run_id: str) -> list[str]: ...

    @abstractmethod
    def clear_request_artifact_pointers(
        self,
        *,
        run_id: str,
        artifact_paths: list[str],
    ) -> int: ...

    @abstractmethod
    def register_batch(
        self,
        *,
        run_id: str,
        local_batch_id: str,
        provider_batch_id: str,
        status: str,
        custom_ids: list[str],
    ) -> None: ...

    @abstractmethod
    def mark_items_submitted(
        self,
        *,
        run_id: str,
        provider_batch_id: str,
        submissions: list[PreparedSubmission],
    ) -> None: ...

    @abstractmethod
    def update_batch_status(
        self,
        *,
        run_id: str,
        provider_batch_id: str,
        status: str,
        output_file_id: str | None = None,
        error_file_id: str | None = None,
    ) -> None: ...

    @abstractmethod
    def get_active_batches(self, *, run_id: str) -> list[ActiveBatchRecord]: ...

    @abstractmethod
    def get_submitted_custom_ids_for_batch(
        self,
        *,
        run_id: str,
        provider_batch_id: str,
    ) -> list[str]: ...

    @abstractmethod
    def mark_items_completed(
        self,
        *,
        run_id: str,
        completions: list[CompletedItemRecord],
    ) -> None: ...

    @abstractmethod
    def mark_items_failed(
        self,
        *,
        run_id: str,
        failures: list[ItemFailureRecord],
        max_attempts: int,
    ) -> None: ...

    @abstractmethod
    def mark_queued_items_failed(
        self,
        *,
        run_id: str,
        failures: list[QueuedItemFailureRecord],
        max_attempts: int,
    ) -> None: ...

    @abstractmethod
    def reset_batch_items_to_pending(
        self,
        *,
        run_id: str,
        provider_batch_id: str,
        error: ItemFailure,
    ) -> None: ...

    @abstractmethod
    def get_active_submitted_token_estimate(self, *, run_id: str) -> int: ...

    @abstractmethod
    def record_batch_retry_failure(
        self,
        *,
        run_id: str,
        error_class: str,
        base_delay_sec: float,
        max_delay_sec: float,
    ) -> RetryBackoffState: ...

    @abstractmethod
    def clear_batch_retry_backoff(self, *, run_id: str) -> None: ...

    @abstractmethod
    def get_batch_retry_backoff_remaining_sec(self, *, run_id: str) -> float: ...

    @abstractmethod
    def get_run_summary(self, *, run_id: str) -> RunSummary: ...

    @abstractmethod
    def get_item_records(self, *, run_id: str) -> list[PersistedItemRecord]: ...
