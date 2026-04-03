"""OpenAI Batch API provider implementation.

Adapts batchor's generic batch model to the OpenAI Batch API.  Supports both
the ``/v1/responses`` (default) and ``/v1/chat/completions`` endpoints, with
optional structured JSON output via OpenAI's ``json_schema`` response format.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, cast

from batchor.core.models import OpenAIProviderConfig, PromptParts
from batchor.core.types import BatchRemoteRecord, BatchRequestLine, JSONObject
from batchor.providers.base import BatchProvider, StructuredOutputSchema
from batchor.runtime.tokens import estimate_request_tokens


def resolve_openai_api_key(config: OpenAIProviderConfig) -> str:
    """Resolve credentials from explicit config first, then the environment."""
    if config.api_key:
        return config.api_key
    api_key = os.getenv("OPENAI_API_KEY", "")
    if api_key:
        return api_key
    raise ValueError("OpenAI API key is required; pass OpenAIProviderConfig(api_key=...) or set OPENAI_API_KEY")


class OpenAIBatchProvider(BatchProvider):
    """Built-in provider that adapts `batchor` jobs to the OpenAI Batch API."""

    def __init__(self, config: OpenAIProviderConfig, client: Any | None = None) -> None:
        self.config = config
        self.client = client if client is not None else self._build_default_client()

    def _build_default_client(self) -> Any:
        from openai import OpenAI

        return OpenAI(
            api_key=resolve_openai_api_key(self.config),
            timeout=self.config.request_timeout_sec,
        )

    def build_request_line(
        self,
        *,
        custom_id: str,
        prompt_parts: PromptParts,
        structured_output: StructuredOutputSchema | None = None,
    ) -> BatchRequestLine:
        """Build one OpenAI batch JSONL request row for a logical item."""
        structured_output_json_schema = self._structured_output_json_schema(structured_output)
        if self.config.endpoint == "/v1/chat/completions":
            messages: list[dict[str, str]] = []
            if prompt_parts.system_prompt:
                messages.append({"role": "system", "content": prompt_parts.system_prompt})
            messages.append({"role": "user", "content": prompt_parts.prompt})
            body = cast(
                JSONObject,
                {
                    "model": self.config.model,
                    "messages": messages,
                },
            )
            if structured_output_json_schema is not None:
                body["response_format"] = {
                    "type": "json_schema",
                    "json_schema": structured_output_json_schema,
                }
        else:
            body = cast(
                JSONObject,
                {
                    "model": self.config.model,
                    "input": prompt_parts.prompt,
                },
            )
            if prompt_parts.system_prompt:
                body["instructions"] = prompt_parts.system_prompt
            if self.config.reasoning_effort is not None:
                body["reasoning"] = {"effort": self.config.reasoning_effort}
            if structured_output_json_schema is not None:
                body["text"] = {
                    "format": {
                        "type": "json_schema",
                        **structured_output_json_schema,
                    }
                }
        return {
            "custom_id": custom_id,
            "method": "POST",
            "url": self.config.endpoint,
            "body": body,
        }

    @staticmethod
    def _structured_output_json_schema(
        structured_output: StructuredOutputSchema | None,
    ) -> JSONObject | None:
        if structured_output is None:
            return None
        return {
            "name": structured_output.name,
            "strict": True,
            "schema": structured_output.schema,
        }

    def upload_input_file(self, input_path: str | Path) -> str:
        """Upload a prepared local JSONL file to OpenAI and return the file id."""
        with Path(input_path).open("rb") as handle:
            uploaded = self.client.files.create(file=handle, purpose="batch")
        return uploaded.id

    def delete_input_file(self, file_id: str) -> None:
        """Best-effort deletion of an uploaded input file."""
        self.client.files.delete(file_id)

    def create_batch(
        self,
        *,
        input_file_id: str,
        metadata: dict[str, str] | None = None,
    ) -> BatchRemoteRecord:
        """Create an OpenAI batch from a previously uploaded input file."""
        batch = self.client.batches.create(
            input_file_id=input_file_id,
            endpoint=self.config.endpoint,
            completion_window=self.config.completion_window,
            metadata=metadata or {},
        )
        return self._normalize(batch)

    def get_batch(self, batch_id: str) -> BatchRemoteRecord:
        """Fetch the current remote state for one OpenAI batch."""
        batch = self.client.batches.retrieve(batch_id)
        return self._normalize(batch)

    def download_file_content(self, file_id: str) -> str:
        """Download a provider file and normalize it to text."""
        content = self.client.files.content(file_id)
        if hasattr(content, "text"):
            return content.text
        if hasattr(content, "read"):
            raw = content.read()
            return raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
        return str(content)

    @staticmethod
    def parse_jsonl(content: str) -> list[JSONObject]:
        """Parse provider JSONL payloads into JSON objects."""
        records: list[JSONObject] = []
        for raw_line in content.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            record = json.loads(line)
            if not isinstance(record, dict):
                raise ValueError("batch jsonl records must be JSON objects")
            records.append(record)
        return records

    def parse_batch_output(
        self,
        *,
        output_content: str | None,
        error_content: str | None,
    ) -> tuple[dict[str, JSONObject], dict[str, JSONObject], list[JSONObject]]:
        """Split OpenAI output/error payloads into success and error maps."""
        raw_records: list[JSONObject] = []
        success: dict[str, JSONObject] = {}
        errors: dict[str, JSONObject] = {}

        for record in self.parse_jsonl(output_content or ""):
            raw_records.append(record)
            custom_id = record.get("custom_id")
            if not isinstance(custom_id, str) or not custom_id:
                continue
            response = record.get("response")
            status_code = response.get("status_code") if isinstance(response, dict) else None
            if isinstance(status_code, int) and status_code >= 400:
                errors[custom_id] = record
            else:
                success[custom_id] = record

        for record in self.parse_jsonl(error_content or ""):
            raw_records.append(record)
            custom_id = record.get("custom_id")
            if isinstance(custom_id, str) and custom_id:
                errors[custom_id] = record

        return success, errors, raw_records

    def estimate_request_tokens(
        self,
        request_line: BatchRequestLine,
        *,
        chars_per_token: int,
    ) -> int:
        """Estimate submitted tokens for one provider request line."""
        return estimate_request_tokens(
            request_line,
            chars_per_token=chars_per_token,
            model=self.config.model,
        )

    @staticmethod
    def _normalize(obj: Any) -> BatchRemoteRecord:
        """Normalise an OpenAI SDK response object to a plain dict.

        Args:
            obj: An OpenAI SDK response object or a plain dict.

        Returns:
            A :class:`~batchor.core.types.BatchRemoteRecord` dict.

        Raises:
            TypeError: If *obj* cannot be converted.
        """
        if isinstance(obj, dict):
            return cast(BatchRemoteRecord, obj)
        if hasattr(obj, "model_dump"):
            return cast(BatchRemoteRecord, obj.model_dump())
        if hasattr(obj, "__dict__"):
            return cast(BatchRemoteRecord, dict(obj.__dict__))
        raise TypeError(f"cannot normalize object {type(obj)!r}")
