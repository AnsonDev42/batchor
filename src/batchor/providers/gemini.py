"""Gemini Batch API provider implementation.

Adapts batchor's generic batch model to the Gemini Batch API.  The built-in
Gemini provider supports text-only ``GenerateContent`` batch requests with
optional structured JSON output.
"""

from __future__ import annotations

import json
import os
from importlib import import_module
from pathlib import Path
from typing import Any, cast

from batchor.core.models import GeminiProviderConfig, PromptParts
from batchor.core.types import BatchRemoteRecord, BatchRequestLine, JSONObject
from batchor.providers.base import BatchProvider, StructuredOutputSchema
from batchor.runtime.tokens import estimate_request_tokens


def resolve_gemini_api_key(config: GeminiProviderConfig) -> str:
    """Resolve credentials from explicit config first, then the environment."""
    if config.api_key:
        return config.api_key
    api_key = os.getenv("GEMINI_API_KEY", "")
    if api_key:
        return api_key
    raise ValueError("Gemini API key is required; pass GeminiProviderConfig(api_key=...) or set GEMINI_API_KEY")


class GeminiBatchProvider(BatchProvider):
    """Built-in provider that adapts `batchor` jobs to the Gemini Batch API."""

    def __init__(self, config: GeminiProviderConfig, client: Any | None = None) -> None:
        self.config = config
        self._client = client

    @property
    def client(self) -> Any:
        """Return the SDK client, creating it lazily for optional dependency support."""
        if self._client is None:
            self._client = self._build_default_client()
        return self._client

    def _build_default_client(self) -> Any:
        try:
            genai = import_module("google.genai")
        except ImportError as exc:
            raise ImportError('Gemini provider requires the "gemini" extra; install with `batchor[gemini]`.') from exc

        return genai.Client(api_key=resolve_gemini_api_key(self.config))

    def build_request_line(
        self,
        *,
        custom_id: str,
        prompt_parts: PromptParts,
        structured_output: StructuredOutputSchema | None = None,
    ) -> BatchRequestLine:
        """Build one Gemini batch JSONL request row for a logical item."""
        request: JSONObject = {
            "contents": [
                {
                    "parts": [
                        {
                            "text": prompt_parts.prompt,
                        }
                    ]
                }
            ]
        }
        if prompt_parts.system_prompt:
            request["system_instruction"] = {
                "parts": [
                    {
                        "text": prompt_parts.system_prompt,
                    }
                ]
            }
        generation_config = self._generation_config(structured_output)
        if generation_config:
            request["generation_config"] = generation_config
        return {
            "key": custom_id,
            "request": request,
        }

    def _generation_config(
        self,
        structured_output: StructuredOutputSchema | None,
    ) -> JSONObject:
        generation_config = cast(JSONObject, json.loads(json.dumps(self.config.generation_config)))
        if structured_output is not None:
            generation_config["response_mime_type"] = "application/json"
            generation_config["response_json_schema"] = structured_output.schema
        return generation_config

    def request_correlation_id(self, request_line: BatchRequestLine) -> str:
        key = request_line.get("key")
        if not isinstance(key, str) or not key:
            raise ValueError("Gemini request line is missing key")
        return key

    def with_request_correlation_id(
        self,
        request_line: BatchRequestLine,
        custom_id: str,
    ) -> BatchRequestLine:
        updated = dict(request_line)
        updated["key"] = custom_id
        return cast(BatchRequestLine, updated)

    def upload_input_file(self, input_path: str | Path) -> str:
        """Upload a prepared local JSONL file to Gemini and return the file name."""
        try:
            genai = import_module("google.genai")
            types = genai.types

            upload_config: object = types.UploadFileConfig(
                display_name=Path(input_path).name,
                mime_type="jsonl",
            )
        except (AttributeError, ImportError):
            upload_config = {
                "display_name": Path(input_path).name,
                "mime_type": "jsonl",
            }

        uploaded = self.client.files.upload(
            file=Path(input_path).as_posix(),
            config=upload_config,
        )
        return str(uploaded.name)

    def delete_input_file(self, file_id: str) -> None:
        """Best-effort deletion of an uploaded input file."""
        self.client.files.delete(name=file_id)

    def create_batch(
        self,
        *,
        input_file_id: str,
        metadata: dict[str, str] | None = None,
    ) -> BatchRemoteRecord:
        """Create a Gemini batch from a previously uploaded input file."""
        display_name = self._display_name(metadata)
        batch = self.client.batches.create(
            model=self.config.model,
            src=input_file_id,
            config={"display_name": display_name},
        )
        return self._normalize(batch)

    def _display_name(self, metadata: dict[str, str] | None) -> str:
        run_id = (metadata or {}).get("run_id")
        if run_id:
            return f"{self.config.display_name_prefix}-{run_id}"
        return self.config.display_name_prefix

    def get_batch(self, batch_id: str) -> BatchRemoteRecord:
        """Fetch the current remote state for one Gemini batch."""
        batch = self.client.batches.get(name=batch_id)
        return self._normalize(batch)

    def download_file_content(self, file_id: str) -> str:
        """Download a Gemini file and normalize it to text."""
        content = self.client.files.download(file=file_id)
        if isinstance(content, str):
            return content
        if isinstance(content, (bytes, bytearray)):
            return content.decode("utf-8")
        if hasattr(content, "text"):
            return str(content.text)
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
            records.append(cast(JSONObject, record))
        return records

    def parse_batch_output(
        self,
        *,
        output_content: str | None,
        error_content: str | None,
    ) -> tuple[dict[str, JSONObject], dict[str, JSONObject], list[JSONObject]]:
        """Split Gemini output/error payloads into success and error maps."""
        raw_records: list[JSONObject] = []
        success: dict[str, JSONObject] = {}
        errors: dict[str, JSONObject] = {}

        for record in self.parse_jsonl(output_content or "") + self.parse_jsonl(error_content or ""):
            raw_records.append(record)
            key = record.get("key")
            if not isinstance(key, str) or not key:
                continue
            if "error" in record or not isinstance(record.get("response"), dict):
                errors[key] = record
            else:
                success[key] = record

        return success, errors, raw_records

    def extract_response_text(self, response_record: JSONObject) -> str:
        """Extract concatenated text output from a Gemini response record."""
        response = response_record.get("response")
        if not isinstance(response, dict):
            return ""
        direct_text = response.get("text")
        if isinstance(direct_text, str):
            return direct_text
        fragments: list[str] = []
        candidates = response.get("candidates")
        if isinstance(candidates, list):
            for candidate in candidates:
                if not isinstance(candidate, dict):
                    continue
                content = candidate.get("content")
                if not isinstance(content, dict):
                    continue
                parts = content.get("parts")
                if not isinstance(parts, list):
                    continue
                for part in parts:
                    if not isinstance(part, dict):
                        continue
                    text = part.get("text")
                    if isinstance(text, str):
                        fragments.append(text)
        return "\n".join(fragment for fragment in fragments if fragment)

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
        """Normalise a Gemini SDK response object to a plain dict."""
        payload = _object_to_dict(obj)
        batch_id = _string_value(payload, "name") or _string_value(payload, "id")
        state = _state_name(payload.get("state"))
        output_file_id = _nested_string(payload, ("dest", "file_name")) or _nested_string(
            payload,
            ("output", "responses_file"),
        )
        errors = payload.get("error") or payload.get("errors")
        return BatchRemoteRecord(
            id=batch_id or "",
            status=_normalize_status(state),
            output_file_id=output_file_id,
            error_file_id=None,
            errors=cast(JSONObject, errors) if isinstance(errors, dict) else errors,
        )


def _object_to_dict(obj: Any) -> dict[str, Any]:
    if isinstance(obj, dict):
        return dict(obj)
    if hasattr(obj, "model_dump"):
        return dict(obj.model_dump())
    if hasattr(obj, "__dict__"):
        return dict(obj.__dict__)
    raise TypeError(f"cannot normalize object {type(obj)!r}")


def _string_value(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    return value if isinstance(value, str) and value else None


def _nested_string(payload: dict[str, Any], path: tuple[str, str]) -> str | None:
    parent = payload.get(path[0])
    if not isinstance(parent, dict) and hasattr(parent, "__dict__"):
        parent = parent.__dict__
    if not isinstance(parent, dict):
        return None
    value = parent.get(path[1])
    return value if isinstance(value, str) and value else None


def _state_name(value: Any) -> str:
    name = getattr(value, "name", None)
    if isinstance(name, str):
        return name
    if isinstance(value, str):
        return value
    return str(value) if value is not None else ""


def _normalize_status(state: str) -> str:
    normalized = state.upper()
    if normalized in {"JOB_STATE_SUCCEEDED", "JOB_STATE_PARTIALLY_SUCCEEDED", "BATCH_STATE_SUCCEEDED", "SUCCEEDED"}:
        return "completed"
    if normalized in {"JOB_STATE_FAILED", "BATCH_STATE_FAILED", "FAILED"}:
        return "failed"
    if normalized in {"JOB_STATE_CANCELLED", "BATCH_STATE_CANCELLED", "CANCELLED"}:
        return "cancelled"
    if normalized in {"JOB_STATE_EXPIRED", "BATCH_STATE_EXPIRED", "EXPIRED"}:
        return "expired"
    return "submitted"
