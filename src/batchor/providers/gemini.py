"""Gemini Developer API and Vertex AI batch provider."""

from __future__ import annotations

import hashlib
import json
import os
from importlib import import_module
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlparse
from uuid import uuid4

from batchor.core.enums import GeminiBatchInputMode
from batchor.core.models import GeminiProviderConfig, PromptParts
from batchor.core.types import BatchRemoteRecord, BatchRequestLine, JSONObject, JSONValue
from batchor.providers.base import BatchProvider, StructuredOutputSchema
from batchor.runtime.tokens import estimate_request_tokens

_INLINE_INPUT_PREFIX = "batchor-inline-input:"
_INLINE_OUTPUT_PREFIX = "batchor-inline-output:"
_INLINE_REQUEST_LIMIT_BYTES = 20_000_000
_VERTEX_CORRELATION_LABEL = "batchor_key"
_TRUE_VALUES = {"1", "true", "yes", "on"}


def resolve_gemini_api_key(config: GeminiProviderConfig) -> str:
    """Resolve Developer API credentials from config, then the environment."""
    if config.api_key:
        return config.api_key
    api_key = os.getenv("GEMINI_API_KEY", "")
    if api_key:
        return api_key
    raise ValueError("Gemini API key is required; pass GeminiProviderConfig(api_key=...) or set GEMINI_API_KEY")


class GeminiBatchProvider(BatchProvider):
    """Adapt batchor jobs to Gemini Developer API or Vertex AI batches.

    Vertex AI stages JSONL through Cloud Storage. The Developer API uses
    inline requests for payloads below 20 MB and its Files API otherwise.
    """

    def __init__(
        self,
        config: GeminiProviderConfig,
        client: Any | None = None,
        storage_client: Any | None = None,
    ) -> None:
        self.config = config
        self._client = client
        self._storage_client = storage_client
        self._inline_inputs: dict[str, list[JSONObject]] = {}
        self._inline_outputs: dict[str, str] = {}

    @property
    def uses_vertex_ai(self) -> bool:
        """Return whether this provider targets Vertex AI."""
        if self.config.vertexai is not None:
            return self.config.vertexai
        value = os.getenv("GOOGLE_GENAI_USE_VERTEXAI", "")
        if value:
            return value.strip().lower() in _TRUE_VALUES
        return os.getenv("GOOGLE_GENAI_USE_ENTERPRISE", "").strip().lower() in _TRUE_VALUES

    @property
    def client(self) -> Any:
        """Return the SDK client, creating it lazily."""
        if self._client is None:
            self._client = self._build_default_client()
        return self._client

    @property
    def storage_client(self) -> Any:
        """Return the Cloud Storage client, creating it lazily."""
        if self._storage_client is None:
            try:
                storage = import_module("google.cloud.storage")
            except ImportError as exc:
                raise ImportError(
                    'Vertex Gemini support requires the "gemini" extra; install with `batchor[gemini]`.'
                ) from exc
            self._storage_client = storage.Client(project=self._project())
        return self._storage_client

    def _build_default_client(self) -> Any:
        try:
            genai = import_module("google.genai")
        except ImportError as exc:
            raise ImportError('Gemini provider requires the "gemini" extra; install with `batchor[gemini]`.') from exc

        if not self.uses_vertex_ai:
            return genai.Client(vertexai=False, api_key=resolve_gemini_api_key(self.config))

        project = self._project()
        location = self._location()
        if not project:
            raise ValueError("Vertex AI project is required; pass project=... or set GOOGLE_CLOUD_PROJECT")
        if not location:
            raise ValueError("Vertex AI location is required; pass location=... or set GOOGLE_CLOUD_LOCATION")
        return genai.Client(
            vertexai=True,
            project=project,
            location=location,
            http_options=genai.types.HttpOptions(api_version="v1"),
        )

    def _project(self) -> str:
        return self.config.project or os.getenv("GOOGLE_CLOUD_PROJECT", "")

    def _location(self) -> str:
        return self.config.location or os.getenv("GOOGLE_CLOUD_LOCATION", "")

    def build_request_line(
        self,
        *,
        custom_id: str,
        prompt_parts: PromptParts,
        structured_output: StructuredOutputSchema | None = None,
    ) -> BatchRequestLine:
        """Build one text-only Gemini GenerateContent JSONL row."""
        request: JSONObject = {
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": prompt_parts.prompt}],
                }
            ]
        }
        if prompt_parts.system_prompt:
            request["system_instruction"] = {"parts": [{"text": prompt_parts.system_prompt}]}
        generation_config = self._generation_config(structured_output)
        if generation_config:
            request["generation_config"] = generation_config

        if self.uses_vertex_ai:
            request["labels"] = {_VERTEX_CORRELATION_LABEL: _vertex_correlation_id(custom_id)}
            return {"request": request}
        return {"key": custom_id, "request": request}

    def _generation_config(self, structured_output: StructuredOutputSchema | None) -> JSONObject:
        generation_config = cast(JSONObject, json.loads(json.dumps(self.config.generation_config)))
        if structured_output is not None:
            generation_config["response_mime_type"] = "application/json"
            generation_config["response_json_schema"] = structured_output.schema
        return generation_config

    def request_correlation_id(self, request_line: BatchRequestLine) -> str:
        """Return the Developer key or Vertex request label."""
        correlation_id = _record_correlation_id(cast(JSONObject, request_line))
        if correlation_id is None:
            raise ValueError("Gemini request line is missing a correlation identifier")
        return correlation_id

    def with_request_correlation_id(
        self,
        request_line: BatchRequestLine,
        custom_id: str,
    ) -> BatchRequestLine:
        """Return a replayed request with its next-attempt identifier."""
        updated = cast(BatchRequestLine, json.loads(json.dumps(request_line)))
        if "key" in updated:
            updated["key"] = custom_id
            return updated
        request = updated.get("request")
        if not isinstance(request, dict):
            raise ValueError("Gemini request line is missing request")
        labels = request.get("labels")
        if not isinstance(labels, dict):
            labels = {}
            request["labels"] = labels
        labels[_VERTEX_CORRELATION_LABEL] = _vertex_correlation_id(custom_id)
        return updated

    def upload_input_file(self, input_path: str | Path) -> str:
        """Stage JSONL using inline, Files API, or Cloud Storage."""
        path = Path(input_path)
        mode = self._resolved_input_mode(path.stat().st_size)
        if mode is GeminiBatchInputMode.INLINE:
            identifier = f"{_INLINE_INPUT_PREFIX}{uuid4().hex}"
            self._inline_inputs[identifier] = [self._to_inline_request(row) for row in self._read_jsonl(path)]
            return identifier
        if mode is GeminiBatchInputMode.GCS:
            return self._upload_to_gcs(path)

        try:
            genai = import_module("google.genai")
            upload_config: object = genai.types.UploadFileConfig(
                display_name=path.name,
                mime_type="jsonl",
            )
        except (AttributeError, ImportError):
            upload_config = {"display_name": path.name, "mime_type": "jsonl"}
        uploaded = self.client.files.upload(file=path.as_posix(), config=upload_config)
        return str(uploaded.name)

    def _resolved_input_mode(self, input_size: int) -> GeminiBatchInputMode:
        mode = self.config.input_mode
        if self.uses_vertex_ai:
            if mode not in {GeminiBatchInputMode.AUTO, GeminiBatchInputMode.GCS}:
                raise ValueError("Vertex AI Gemini batches require Cloud Storage input")
            if not self.config.gcs_uri:
                raise ValueError("Vertex AI Gemini batches require GeminiProviderConfig(gcs_uri='gs://...')")
            return GeminiBatchInputMode.GCS
        if mode is GeminiBatchInputMode.GCS:
            raise ValueError("Gemini Developer API batches do not support Cloud Storage input")
        if mode is GeminiBatchInputMode.AUTO:
            return (
                GeminiBatchInputMode.INLINE if input_size < _INLINE_REQUEST_LIMIT_BYTES else GeminiBatchInputMode.FILE
            )
        if mode is GeminiBatchInputMode.INLINE and input_size >= _INLINE_REQUEST_LIMIT_BYTES:
            raise ValueError("Gemini inline batch payloads must be smaller than 20 MB")
        return mode

    @staticmethod
    def _read_jsonl(path: Path) -> list[JSONObject]:
        return GeminiBatchProvider.parse_jsonl(path.read_text(encoding="utf-8"))

    @staticmethod
    def _to_inline_request(row: JSONObject) -> JSONObject:
        key = row.get("key")
        request = row.get("request")
        if not isinstance(key, str) or not key or not isinstance(request, dict):
            raise ValueError("Gemini inline rows require key and request fields")
        contents = request.get("contents")
        if not isinstance(contents, list):
            raise ValueError("Gemini inline rows require request.contents")
        inline_request: JSONObject = {
            "contents": contents,
            "metadata": {"key": key},
        }
        config: JSONObject = {}
        generation_config = request.get("generation_config")
        if isinstance(generation_config, dict):
            config.update(generation_config)
        system_instruction = request.get("system_instruction")
        if isinstance(system_instruction, dict):
            config["system_instruction"] = system_instruction
        if config:
            inline_request["config"] = config
        return inline_request

    def _upload_to_gcs(self, path: Path) -> str:
        base = self.config.gcs_uri.rstrip("/")
        destination = f"{base}/inputs/{uuid4().hex}.jsonl"
        bucket_name, blob_name = _parse_gcs_uri(destination)
        blob = self.storage_client.bucket(bucket_name).blob(blob_name)
        blob.upload_from_filename(path.as_posix(), content_type="application/jsonl")
        return destination

    def delete_input_file(self, file_id: str) -> None:
        """Delete a staged input after a failed batch creation."""
        if file_id.startswith(_INLINE_INPUT_PREFIX):
            self._inline_inputs.pop(file_id, None)
            return
        if file_id.startswith("gs://"):
            bucket_name, blob_name = _parse_gcs_uri(file_id)
            self.storage_client.bucket(bucket_name).blob(blob_name).delete()
            return
        self.client.files.delete(name=file_id)

    def create_batch(
        self,
        *,
        input_file_id: str,
        metadata: dict[str, str] | None = None,
    ) -> BatchRemoteRecord:
        """Create a remote batch from staged input."""
        config: dict[str, object] = {"display_name": self._display_name(metadata)}
        src: object = input_file_id
        if input_file_id.startswith(_INLINE_INPUT_PREFIX):
            src = self._inline_inputs[input_file_id]
        elif input_file_id.startswith("gs://"):
            config["dest"] = self._gcs_output_uri(input_file_id)
        batch = self.client.batches.create(model=self.config.model, src=src, config=config)
        if input_file_id.startswith(_INLINE_INPUT_PREFIX):
            self._inline_inputs.pop(input_file_id, None)
        return self._normalize(batch)

    def _display_name(self, metadata: dict[str, str] | None) -> str:
        run_id = (metadata or {}).get("run_id")
        return f"{self.config.display_name_prefix}-{run_id}" if run_id else self.config.display_name_prefix

    def _gcs_output_uri(self, input_uri: str) -> str:
        stem = Path(urlparse(input_uri).path).stem
        return f"{self.config.gcs_uri.rstrip('/')}/outputs/{stem}"

    def get_batch(self, batch_id: str) -> BatchRemoteRecord:
        """Fetch one remote batch."""
        return self._normalize(self.client.batches.get(name=batch_id))

    def download_file_content(self, file_id: str) -> str:
        """Download Developer API or Cloud Storage batch output."""
        if file_id.startswith(_INLINE_OUTPUT_PREFIX):
            return self._inline_outputs[file_id]
        if file_id.startswith("gs://"):
            return self._download_gcs_content(file_id)
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

    def _download_gcs_content(self, uri: str) -> str:
        bucket_name, blob_name = _parse_gcs_uri(uri)
        bucket = self.storage_client.bucket(bucket_name)
        exact = bucket.blob(blob_name)
        blobs = (
            [exact]
            if exact.exists() and not blob_name.endswith("/")
            else list(bucket.list_blobs(prefix=f"{blob_name.rstrip('/')}/"))
        )
        payloads = [
            blob.download_as_text(encoding="utf-8")
            for blob in sorted(blobs, key=lambda value: value.name)
            if not blob.name.endswith("/") and (getattr(blob, "size", None) is None or blob.size > 0)
        ]
        if not payloads:
            raise FileNotFoundError(f"no Gemini batch output objects found under {uri}")
        return "\n".join(payload.rstrip("\n") for payload in payloads if payload) + "\n"

    @staticmethod
    def parse_jsonl(content: str) -> list[JSONObject]:
        """Parse JSONL into objects."""
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
        """Split Developer or Vertex output rows by correlation identifier."""
        raw_records: list[JSONObject] = []
        success: dict[str, JSONObject] = {}
        errors: dict[str, JSONObject] = {}
        for record in self.parse_jsonl(output_content or "") + self.parse_jsonl(error_content or ""):
            raw_records.append(record)
            correlation_id = _record_correlation_id(record)
            if correlation_id is None:
                continue
            status = record.get("status")
            failed = (
                "error" in record
                or (isinstance(status, str) and bool(status.strip()))
                or not isinstance(record.get("response"), dict)
            )
            (errors if failed else success)[correlation_id] = record
        return success, errors, raw_records

    def extract_response_text(self, response_record: JSONObject) -> str:
        """Extract concatenated text from a Gemini response."""
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
                    if isinstance(part, dict) and isinstance(part.get("text"), str):
                        fragments.append(cast(str, part["text"]))
        return "\n".join(fragment for fragment in fragments if fragment)

    def estimate_request_tokens(
        self,
        request_line: BatchRequestLine,
        *,
        chars_per_token: int,
    ) -> int:
        """Estimate submitted tokens for reporting and generic chunking."""
        return estimate_request_tokens(
            request_line,
            chars_per_token=chars_per_token,
            model=self.config.model,
        )

    def _normalize(self, obj: Any) -> BatchRemoteRecord:
        payload = _object_to_dict(obj)
        batch_id = _string_value(payload, "name") or _string_value(payload, "id") or ""
        state = _state_name(payload.get("state"))
        output_file_id = _nested_string(payload, ("output_info", "gcs_output_directory"))
        if output_file_id is None:
            output_file_id = _nested_string(payload, ("outputInfo", "gcsOutputDirectory"))
        if output_file_id is None:
            output_file_id = _nested_string(payload, ("dest", "file_name"))
        if output_file_id is None:
            output_file_id = _nested_string(payload, ("dest", "fileName"))
        if output_file_id is None:
            output_file_id = _nested_string(payload, ("output", "responses_file"))

        inline_responses = _nested_list(payload, ("dest", "inlined_responses"))
        if inline_responses is None:
            inline_responses = _nested_list(payload, ("dest", "inlinedResponses"))
        if inline_responses is not None:
            output_file_id = f"{_INLINE_OUTPUT_PREFIX}{batch_id}"
            self._inline_outputs[output_file_id] = _serialize_inline_responses(inline_responses)

        errors = payload.get("error") or payload.get("errors")
        return BatchRemoteRecord(
            id=batch_id,
            status=_normalize_status(state),
            output_file_id=output_file_id,
            error_file_id=None,
            errors=_json_compatible(errors) if errors is not None else None,
        )


def _vertex_correlation_id(custom_id: str) -> str:
    return f"b{hashlib.sha256(custom_id.encode('utf-8')).hexdigest()[:40]}"


def _record_correlation_id(record: JSONObject) -> str | None:
    key = record.get("key")
    if isinstance(key, str) and key:
        return key
    request = record.get("request")
    if not isinstance(request, dict):
        return None
    labels = request.get("labels")
    if not isinstance(labels, dict):
        return None
    label = labels.get(_VERTEX_CORRELATION_LABEL)
    return label if isinstance(label, str) and label else None


def _parse_gcs_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "gs" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise ValueError(f"invalid Cloud Storage URI: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def _object_to_dict(obj: Any) -> dict[str, Any]:
    if isinstance(obj, dict):
        return dict(obj)
    if hasattr(obj, "model_dump"):
        try:
            return dict(obj.model_dump(mode="json", exclude_none=True))
        except TypeError:
            return dict(obj.model_dump())
    if hasattr(obj, "__dict__"):
        return dict(obj.__dict__)
    raise TypeError(f"cannot normalize object {type(obj)!r}")


def _json_compatible(value: Any) -> JSONValue:
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, list | tuple):
        return [_json_compatible(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_compatible(item) for key, item in value.items()}
    if hasattr(value, "model_dump"):
        try:
            return _json_compatible(value.model_dump(mode="json", exclude_none=True))
        except TypeError:
            return _json_compatible(value.model_dump())
    if hasattr(value, "name") and isinstance(value.name, str):
        return value.name
    if hasattr(value, "__dict__"):
        return _json_compatible(value.__dict__)
    return str(value)


def _serialize_inline_responses(responses: list[Any]) -> str:
    records: list[JSONObject] = []
    for response in responses:
        payload = _json_compatible(response)
        if not isinstance(payload, dict):
            continue
        metadata = payload.get("metadata")
        key = metadata.get("key") if isinstance(metadata, dict) else None
        if not isinstance(key, str) or not key:
            continue
        record: JSONObject = {"key": key}
        if isinstance(payload.get("response"), dict):
            record["response"] = payload["response"]
        elif payload.get("error") is not None:
            record["error"] = payload["error"]
        records.append(record)
    return "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records)


def _string_value(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    return value if isinstance(value, str) and value else None


def _nested_value(payload: dict[str, Any], path: tuple[str, str]) -> Any:
    parent = payload.get(path[0])
    if parent is None:
        return None
    if not isinstance(parent, dict):
        parent = _object_to_dict(parent)
    return parent.get(path[1])


def _nested_string(payload: dict[str, Any], path: tuple[str, str]) -> str | None:
    value = _nested_value(payload, path)
    return value if isinstance(value, str) and value else None


def _nested_list(payload: dict[str, Any], path: tuple[str, str]) -> list[Any] | None:
    value = _nested_value(payload, path)
    return value if isinstance(value, list) else None


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
