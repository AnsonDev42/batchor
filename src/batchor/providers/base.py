from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from batchor.core.enums import ProviderKind
from batchor.core.types import BatchRemoteRecord, BatchRequestLine, JSONObject

if TYPE_CHECKING:
    from batchor.core.models import PromptParts


@dataclass(frozen=True)
class StructuredOutputSchema:
    name: str
    schema: JSONObject


class ProviderConfig(ABC):
    poll_interval_sec: float

    @property
    @abstractmethod
    def provider_kind(self) -> ProviderKind:
        """Stable provider identifier used for runtime dispatch and persistence."""

    @abstractmethod
    def to_payload(self) -> JSONObject:
        """Serialize provider-specific config to JSON for durable storage."""

    def to_public_payload(self) -> JSONObject:
        """Serialize provider config without secret material for durable storage."""
        return self.to_payload()


class BatchProvider(ABC):
    @abstractmethod
    def build_request_line(
        self,
        *,
        custom_id: str,
        prompt_parts: PromptParts,
        structured_output: StructuredOutputSchema | None = None,
    ) -> BatchRequestLine: ...

    @abstractmethod
    def upload_input_file(self, input_path: str | Path) -> str: ...

    @abstractmethod
    def delete_input_file(self, file_id: str) -> None: ...

    @abstractmethod
    def create_batch(
        self,
        *,
        input_file_id: str,
        metadata: dict[str, str] | None = None,
    ) -> BatchRemoteRecord: ...

    @abstractmethod
    def get_batch(self, batch_id: str) -> BatchRemoteRecord: ...

    @abstractmethod
    def download_file_content(self, file_id: str) -> str: ...

    @abstractmethod
    def parse_batch_output(
        self,
        *,
        output_content: str | None,
        error_content: str | None,
    ) -> tuple[dict[str, JSONObject], dict[str, JSONObject], list[JSONObject]]: ...

    @abstractmethod
    def estimate_request_tokens(
        self,
        request_line: BatchRequestLine,
        *,
        chars_per_token: int,
    ) -> int: ...
