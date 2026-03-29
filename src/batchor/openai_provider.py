from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Protocol, cast

from batchor.types import BatchRemoteRecord, BatchRequestLine, JSONObject

from batchor.models import OpenAIProviderConfig, PromptParts


@dataclass(frozen=True)
class StructuredOutputSchema:
    name: str
    schema: JSONObject


class BatchProvider(Protocol):
    def build_request_line(
        self,
        *,
        custom_id: str,
        prompt_parts: PromptParts,
        structured_output: StructuredOutputSchema | None = None,
    ) -> BatchRequestLine: ...

    def write_requests_jsonl(
        self,
        request_lines: list[BatchRequestLine],
        output_path: str | Path,
    ) -> Path: ...

    def upload_input_file(self, input_path: str | Path) -> str: ...

    def create_batch(
        self,
        *,
        input_file_id: str,
        metadata: dict[str, str] | None = None,
    ) -> BatchRemoteRecord: ...

    def get_batch(self, batch_id: str) -> BatchRemoteRecord: ...

    def download_file_content(self, file_id: str) -> str: ...

    def parse_batch_output(
        self,
        *,
        output_content: str | None,
        error_content: str | None,
    ) -> tuple[dict[str, JSONObject], dict[str, JSONObject], list[JSONObject]]: ...


class OpenAIBatchProvider:
    def __init__(self, config: OpenAIProviderConfig, client: Any | None = None) -> None:
        self.config = config
        self.client = client if client is not None else self._build_default_client()

    def _build_default_client(self) -> Any:
        from openai import OpenAI

        return OpenAI(api_key=self.config.api_key, timeout=self.config.request_timeout_sec)

    def build_request_line(
        self,
        *,
        custom_id: str,
        prompt_parts: PromptParts,
        structured_output: StructuredOutputSchema | None = None,
    ) -> BatchRequestLine:
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
                    "temperature": 0,
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
                    "temperature": 0,
                },
            )
            if prompt_parts.system_prompt:
                body["instructions"] = prompt_parts.system_prompt
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

    def write_requests_jsonl(
        self,
        request_lines: list[BatchRequestLine],
        output_path: str | Path,
    ) -> Path:
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("w", encoding="utf-8") as f:
            for line in request_lines:
                f.write(json.dumps(line, ensure_ascii=False))
                f.write("\n")
        return output

    def upload_input_file(self, input_path: str | Path) -> str:
        with Path(input_path).open("rb") as handle:
            uploaded = self.client.files.create(file=handle, purpose="batch")
        return uploaded.id

    def create_batch(
        self,
        *,
        input_file_id: str,
        metadata: dict[str, str] | None = None,
    ) -> BatchRemoteRecord:
        batch = self.client.batches.create(
            input_file_id=input_file_id,
            endpoint=self.config.endpoint,
            completion_window=self.config.completion_window,
            metadata=metadata or {},
        )
        return self._normalize(batch)

    def get_batch(self, batch_id: str) -> BatchRemoteRecord:
        batch = self.client.batches.retrieve(batch_id)
        return self._normalize(batch)

    def download_file_content(self, file_id: str) -> str:
        content = self.client.files.content(file_id)
        if hasattr(content, "text"):
            return content.text
        if hasattr(content, "read"):
            raw = content.read()
            return raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
        return str(content)

    @staticmethod
    def parse_jsonl(content: str) -> list[JSONObject]:
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

    @staticmethod
    def _normalize(obj: Any) -> BatchRemoteRecord:
        if isinstance(obj, dict):
            return cast(BatchRemoteRecord, obj)
        if hasattr(obj, "model_dump"):
            return cast(BatchRemoteRecord, obj.model_dump())
        if hasattr(obj, "__dict__"):
            return cast(BatchRemoteRecord, dict(obj.__dict__))
        raise TypeError(f"cannot normalize object {type(obj)!r}")
