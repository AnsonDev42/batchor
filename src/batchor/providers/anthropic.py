"""Anthropic Message Batches provider implementation."""

from __future__ import annotations

import hashlib
import json
import os
from importlib import import_module
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from batchor.core.models import AnthropicProviderConfig, PromptParts
from batchor.core.types import BatchRemoteRecord, BatchRequestLine, JSONObject
from batchor.providers.base import BatchProvider, StructuredOutputSchema
from batchor.runtime.tokens import estimate_request_tokens

_INPUT_PREFIX = "anthropic-input://"
_RESULTS_PREFIX = "anthropic-results://"


def _anthropic_custom_id(custom_id: str) -> str:
    """Map Batchor identifiers to Anthropic's 64-character safe alphabet."""
    return f"b{hashlib.sha256(custom_id.encode('utf-8')).hexdigest()[:40]}"


def resolve_anthropic_api_key(config: AnthropicProviderConfig) -> str:
    api_key = config.api_key or os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise ValueError("Anthropic API key is required; set ANTHROPIC_API_KEY or pass api_key")
    return api_key


class AnthropicBatchProvider(BatchProvider):
    """Adapter for Anthropic's asynchronous Message Batches API."""

    def __init__(self, config: AnthropicProviderConfig, *, client: Any | None = None) -> None:
        self.config = config
        if client is None:
            anthropic = import_module("anthropic")
            client = anthropic.Anthropic(api_key=resolve_anthropic_api_key(config))
        self.client = client
        self._inputs: dict[str, list[JSONObject]] = {}
        self._results: dict[str, str] = {}

    def build_request_line(
        self,
        *,
        custom_id: str,
        prompt_parts: PromptParts,
        structured_output: StructuredOutputSchema | None = None,
    ) -> BatchRequestLine:
        params: JSONObject = dict(self.config.message_params)
        params.update(
            {
                "model": self.config.model,
                "max_tokens": self.config.max_tokens,
                "messages": [{"role": "user", "content": prompt_parts.prompt}],
            }
        )
        if prompt_parts.system_prompt:
            params["system"] = prompt_parts.system_prompt
        if structured_output is not None:
            params["output_config"] = {"format": {"type": "json_schema", "schema": structured_output.schema}}
        return {"custom_id": _anthropic_custom_id(custom_id), "body": params}

    def with_request_correlation_id(self, request_line: BatchRequestLine, custom_id: str) -> BatchRequestLine:
        updated = dict(request_line)
        updated["custom_id"] = _anthropic_custom_id(custom_id)
        return cast(BatchRequestLine, updated)

    def upload_input_file(self, input_path: str | Path) -> str:
        identifier = f"{_INPUT_PREFIX}{uuid4().hex}"
        self._inputs[identifier] = self._parse_jsonl(Path(input_path).read_text(encoding="utf-8"))
        return identifier

    def delete_input_file(self, file_id: str) -> None:
        self._inputs.pop(file_id, None)

    def create_batch(self, *, input_file_id: str, metadata: dict[str, str] | None = None) -> BatchRemoteRecord:
        del metadata
        try:
            rows = self._inputs.pop(input_file_id)
        except KeyError as exc:
            raise ValueError(f"unknown Anthropic staged input: {input_file_id}") from exc
        requests = []
        for row in rows:
            custom_id = row.get("custom_id")
            params = row.get("body")
            if not isinstance(custom_id, str) or not isinstance(params, dict):
                raise ValueError("Anthropic batch rows require custom_id and body fields")
            requests.append({"custom_id": custom_id, "params": params})
        return self._normalize(self.client.messages.batches.create(requests=requests))

    def get_batch(self, batch_id: str) -> BatchRemoteRecord:
        remote = self.client.messages.batches.retrieve(batch_id)
        normalized = self._normalize(remote)
        if normalized["status"] == "completed":
            result_id = f"{_RESULTS_PREFIX}{batch_id}"
            results = self.client.messages.batches.results(batch_id)
            self._results[result_id] = "".join(
                json.dumps(self._object_to_dict(result), ensure_ascii=False) + "\n" for result in results
            )
            normalized["output_file_id"] = result_id
        return normalized

    def download_file_content(self, file_id: str) -> str:
        try:
            return self._results[file_id]
        except KeyError as exc:
            raise ValueError(f"unknown Anthropic results identifier: {file_id}") from exc

    def parse_batch_output(
        self, *, output_content: str | None, error_content: str | None
    ) -> tuple[dict[str, JSONObject], dict[str, JSONObject], list[JSONObject]]:
        raw = self._parse_jsonl(output_content or "") + self._parse_jsonl(error_content or "")
        successes: dict[str, JSONObject] = {}
        errors: dict[str, JSONObject] = {}
        for record in raw:
            custom_id = record.get("custom_id")
            result = record.get("result")
            if not isinstance(custom_id, str) or not isinstance(result, dict):
                continue
            (successes if result.get("type") == "succeeded" else errors)[custom_id] = record
        return successes, errors, raw

    def extract_response_text(self, response_record: JSONObject) -> str:
        result = response_record.get("result")
        message = result.get("message") if isinstance(result, dict) else None
        content = message.get("content") if isinstance(message, dict) else None
        if not isinstance(content, list):
            return ""
        return "\n".join(
            cast(str, block["text"])
            for block in content
            if isinstance(block, dict) and block.get("type") == "text" and isinstance(block.get("text"), str)
        )

    def estimate_request_tokens(self, request_line: BatchRequestLine, *, chars_per_token: int) -> int:
        return estimate_request_tokens(request_line, chars_per_token=chars_per_token, model=self.config.model)

    @staticmethod
    def _parse_jsonl(content: str) -> list[JSONObject]:
        records: list[JSONObject] = []
        for raw_line in content.splitlines():
            if not raw_line.strip():
                continue
            value = json.loads(raw_line)
            if not isinstance(value, dict):
                raise ValueError("batch jsonl records must be JSON objects")
            records.append(cast(JSONObject, value))
        return records

    @classmethod
    def _normalize(cls, obj: Any) -> BatchRemoteRecord:
        payload = cls._object_to_dict(obj)
        batch_id = payload.get("id")
        processing_status = payload.get("processing_status")
        if not isinstance(batch_id, str) or not isinstance(processing_status, str):
            raise TypeError("Anthropic batch response is missing id or processing_status")
        counts = payload.get("request_counts")
        return BatchRemoteRecord(
            id=batch_id,
            status="completed" if processing_status == "ended" else "in_progress",
            output_file_id=None,
            error_file_id=None,
            request_counts=cast(Any, counts if isinstance(counts, dict) else {}),
            errors=None,
        )

    @staticmethod
    def _object_to_dict(obj: Any) -> JSONObject:
        if isinstance(obj, dict):
            return cast(JSONObject, obj)
        if hasattr(obj, "model_dump"):
            return cast(JSONObject, obj.model_dump(mode="json"))
        raise TypeError(f"cannot normalize object {type(obj)!r}")
