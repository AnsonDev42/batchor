"""Abstract base classes for batch provider adapters.

A *provider* is responsible for:

1. Building provider-specific request lines from :class:`~batchor.PromptParts`.
2. Uploading input files and creating remote batches.
3. Polling batch status and downloading results.
4. Parsing raw output into success/error maps.

Implementors sub-class :class:`BatchProvider` and pair it with a
:class:`ProviderConfig` sub-class.
"""

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
    """Validated JSON Schema for a structured output model.

    Attributes:
        name: Schema name sent to the provider (snake_case by convention).
        schema: Strict JSON Schema object with ``additionalProperties: false``
            on all nested objects.
    """

    name: str
    schema: JSONObject


class ProviderConfig(ABC):
    """Abstract configuration for a specific batch provider.

    Sub-classes must implement serialisation and expose the stable
    :attr:`provider_kind` identifier used for runtime dispatch and durable
    storage.

    Attributes:
        poll_interval_sec: Default seconds between polling cycles when
            :meth:`~batchor.Run.wait` is used.
    """

    poll_interval_sec: float

    @property
    @abstractmethod
    def provider_kind(self) -> ProviderKind:
        """Stable provider identifier used for runtime dispatch and persistence."""

    @abstractmethod
    def to_payload(self) -> JSONObject:
        """Serialise provider-specific config to JSON for durable storage.

        Returns:
            A JSON-serialisable dictionary including all configuration fields,
            including secrets.  Use :meth:`to_public_payload` to strip secrets.
        """

    def to_public_payload(self) -> JSONObject:
        """Serialise provider config without secret material for durable storage.

        Returns:
            A JSON-serialisable dictionary safe to persist or log.  The default
            implementation delegates to :meth:`to_payload`; sub-classes should
            override this to omit credentials.
        """
        return self.to_payload()


class BatchProvider(ABC):
    """Abstract adapter between the batchor runtime and a batch API provider.

    Each method maps to one step of the batch submission/polling lifecycle.
    The :class:`~batchor.OpenAIBatchProvider` is the built-in implementation.
    """

    @abstractmethod
    def build_request_line(
        self,
        *,
        custom_id: str,
        prompt_parts: PromptParts,
        structured_output: StructuredOutputSchema | None = None,
    ) -> BatchRequestLine:
        """Build one JSONL request line for a batch input file.

        Args:
            custom_id: Unique identifier for this request within the batch.
            prompt_parts: Prompt text and optional system prompt.
            structured_output: Optional structured output schema to embed.

        Returns:
            A :class:`~batchor.core.types.BatchRequestLine` dict ready to be
            serialised as a JSONL line.
        """
        ...

    @abstractmethod
    def upload_input_file(self, input_path: str | Path) -> str:
        """Upload a prepared JSONL input file to the provider.

        Args:
            input_path: Local path to the JSONL batch input file.

        Returns:
            The provider-assigned file identifier.
        """
        ...

    @abstractmethod
    def delete_input_file(self, file_id: str) -> None:
        """Best-effort deletion of an uploaded input file.

        Implementations should swallow errors — this is a clean-up path.

        Args:
            file_id: Provider-assigned file identifier to delete.
        """
        ...

    @abstractmethod
    def create_batch(
        self,
        *,
        input_file_id: str,
        metadata: dict[str, str] | None = None,
    ) -> BatchRemoteRecord:
        """Create a remote batch from a previously uploaded input file.

        Args:
            input_file_id: Provider file identifier for the batch input.
            metadata: Optional key-value metadata attached to the batch.

        Returns:
            A :class:`~batchor.core.types.BatchRemoteRecord` reflecting the
            initial remote batch state.
        """
        ...

    @abstractmethod
    def get_batch(self, batch_id: str) -> BatchRemoteRecord:
        """Fetch the current remote state for one batch.

        Args:
            batch_id: Provider-assigned batch identifier.

        Returns:
            A :class:`~batchor.core.types.BatchRemoteRecord` with the current
            status and file identifiers.
        """
        ...

    @abstractmethod
    def download_file_content(self, file_id: str) -> str:
        """Download a provider-hosted file and return its text content.

        Args:
            file_id: Provider-assigned file identifier.

        Returns:
            The file's text content (typically JSONL).
        """
        ...

    @abstractmethod
    def parse_batch_output(
        self,
        *,
        output_content: str | None,
        error_content: str | None,
    ) -> tuple[dict[str, JSONObject], dict[str, JSONObject], list[JSONObject]]:
        """Split raw batch output/error JSONL into success and error maps.

        Args:
            output_content: Raw text of the provider output JSONL file, or
                ``None`` if unavailable.
            error_content: Raw text of the provider error JSONL file, or
                ``None`` if unavailable.

        Returns:
            A 3-tuple ``(successes, errors, all_records)`` where *successes*
            and *errors* are dicts keyed by ``custom_id`` and *all_records*
            is the flat list of all parsed records.
        """
        ...

    @abstractmethod
    def estimate_request_tokens(
        self,
        request_line: BatchRequestLine,
        *,
        chars_per_token: int,
    ) -> int:
        """Estimate the token count for a single request line.

        Used by the token-budget enforcement logic to avoid submitting batches
        that exceed the enqueued-token limit.

        Args:
            request_line: The request line whose token count to estimate.
            chars_per_token: Fallback ratio when tiktoken is unavailable.

        Returns:
            Estimated token count as a non-negative integer.
        """
        ...
