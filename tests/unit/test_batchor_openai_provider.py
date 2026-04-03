from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import BaseModel

from batchor.core.enums import OpenAIEndpoint, OpenAIModel, OpenAIReasoningEffort
from batchor.core.models import OpenAIProviderConfig, PromptParts
from batchor.providers.openai import OpenAIBatchProvider, resolve_openai_api_key
from batchor.providers.base import StructuredOutputSchema
from batchor.runtime.validation import model_output_schema


class _ClassificationResult(BaseModel):
    label: str
    score: float


class _FakeFiles:
    def __init__(self) -> None:
        self.created: list[tuple[str, bytes]] = []
        self.deleted: list[str] = []
        text_content = type("TextContent", (), {"text": "hello"})()

        class _ByteContent:
            def read(self) -> bytes:
                return b"hello"

        self.contents: dict[str, object] = {
            "file_text": text_content,
            "file_bytes": _ByteContent(),
        }

    def create(self, *, file, purpose):  # noqa: ANN001
        self.created.append((purpose, file.read()))
        return type("FileObj", (), {"id": "file_123"})

    def content(self, file_id: str):  # noqa: ANN001
        return self.contents[file_id]

    def delete(self, file_id: str):  # noqa: ANN001
        self.deleted.append(file_id)


class _FakeBatches:
    def create(self, **kwargs):  # noqa: ANN003
        return {"id": "batch_123", "status": "submitted", **kwargs}

    def retrieve(self, batch_id: str):
        return {"id": batch_id, "status": "completed"}


class _FakeClient:
    def __init__(self) -> None:
        self.files = _FakeFiles()
        self.batches = _FakeBatches()


def test_build_request_line_for_responses_with_structured_output() -> None:
    schema_name, schema = model_output_schema(_ClassificationResult)
    provider = OpenAIBatchProvider(
        OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
        client=_FakeClient(),
    )
    line = provider.build_request_line(
        custom_id="abc",
        prompt_parts=PromptParts(prompt="hello"),
        structured_output=StructuredOutputSchema(schema_name, schema),
    )
    assert line["custom_id"] == "abc"
    assert line["body"]["input"] == "hello"
    assert line["body"]["text"]["format"]["type"] == "json_schema"
    assert line["body"]["text"]["format"]["name"] == "classification_result"


def test_build_request_line_for_chat_completions_with_system_prompt() -> None:
    schema_name, schema = model_output_schema(_ClassificationResult)
    provider = OpenAIBatchProvider(
        OpenAIProviderConfig(
            api_key="k",
            model="gpt-4.1",
            endpoint=OpenAIEndpoint.CHAT_COMPLETIONS,
        ),
        client=_FakeClient(),
    )
    line = provider.build_request_line(
        custom_id="abc",
        prompt_parts=PromptParts(prompt="row-json", system_prompt="classify"),
        structured_output=StructuredOutputSchema(schema_name, schema),
    )
    assert line["body"]["messages"][0] == {"role": "system", "content": "classify"}
    assert line["body"]["messages"][1] == {"role": "user", "content": "row-json"}
    assert line["body"]["response_format"]["json_schema"]["name"] == "classification_result"


def test_build_request_line_for_responses_can_include_reasoning_effort() -> None:
    provider = OpenAIBatchProvider(
        OpenAIProviderConfig(
            api_key="k",
            model=OpenAIModel.GPT_5_NANO,
            reasoning_effort=OpenAIReasoningEffort.MINIMAL,
        ),
        client=_FakeClient(),
    )
    line = provider.build_request_line(
        custom_id="abc",
        prompt_parts=PromptParts(prompt="hello"),
    )
    assert line["body"]["reasoning"] == {"effort": "minimal"}


def test_write_requests_jsonl_and_parse_batch_output(tmp_path: Path) -> None:
    provider = OpenAIBatchProvider(
        OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
        client=_FakeClient(),
    )
    output_path = provider.write_requests_jsonl(
        [
            provider.build_request_line(
                custom_id="abc",
                prompt_parts=PromptParts(prompt="hello"),
            )
        ],
        tmp_path / "requests.jsonl",
    )
    assert output_path.exists()
    success, errors, raw = provider.parse_batch_output(
        output_content='{"custom_id":"ok","response":{"status_code":200}}\n',
        error_content='{"custom_id":"bad","response":{"status_code":400}}\n',
    )
    assert "ok" in success
    assert "bad" in errors
    assert len(raw) == 2


def test_upload_create_get_and_download() -> None:
    fake = _FakeClient()
    provider = OpenAIBatchProvider(
        OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
        client=fake,
    )
    uploaded = provider.upload_input_file(Path(__file__))
    assert uploaded == "file_123"
    created = provider.create_batch(input_file_id=uploaded, metadata={"run_id": "r1"})
    assert created["id"] == "batch_123"
    fetched = provider.get_batch("batch_123")
    assert fetched["status"] == "completed"
    assert provider.download_file_content("file_text") == "hello"
    assert provider.download_file_content("file_bytes") == "hello"
    provider.delete_input_file(uploaded)
    assert fake.files.deleted == ["file_123"]


def test_resolve_openai_api_key_prefers_explicit_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "env-key")
    resolved = resolve_openai_api_key(
        OpenAIProviderConfig(api_key="explicit-key", model="gpt-4.1")
    )
    assert resolved == "explicit-key"


def test_resolve_openai_api_key_falls_back_to_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "env-key")
    resolved = resolve_openai_api_key(OpenAIProviderConfig(model="gpt-4.1"))
    assert resolved == "env-key"


def test_resolve_openai_api_key_requires_explicit_or_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with pytest.raises(ValueError, match="OpenAI API key is required"):
        resolve_openai_api_key(OpenAIProviderConfig(model="gpt-4.1"))
