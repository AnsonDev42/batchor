from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import BaseModel

from batchor.core.models import GeminiProviderConfig, PromptParts
from batchor.providers.base import StructuredOutputSchema
from batchor.providers.gemini import GeminiBatchProvider, resolve_gemini_api_key
from batchor.runtime.validation import model_output_schema


class _ClassificationResult(BaseModel):
    label: str
    score: float


class _State:
    def __init__(self, name: str) -> None:
        self.name = name


class _Dest:
    def __init__(self, file_name: str | None = None) -> None:
        self.file_name = file_name


class _Job:
    def __init__(
        self,
        *,
        name: str,
        state: str,
        file_name: str | None = None,
    ) -> None:
        self.name = name
        self.state = _State(state)
        self.dest = _Dest(file_name)


class _Uploaded:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeFiles:
    def __init__(self) -> None:
        self.uploaded: list[tuple[str, object]] = []
        self.deleted: list[str] = []

    def upload(self, *, file: str, config: object) -> _Uploaded:
        Path(file).read_text(encoding="utf-8")
        self.uploaded.append((file, config))
        return _Uploaded("files/input_jsonl")

    def download(self, *, file: str) -> bytes:
        assert file == "files/output_jsonl"
        return b'{"key":"row1:a1","response":{"candidates":[{"content":{"parts":[{"text":"hello"}]}}]}}\n'

    def delete(self, *, name: str) -> None:
        self.deleted.append(name)


class _FakeBatches:
    def __init__(self) -> None:
        self.created: list[dict[str, object]] = []

    def create(self, **kwargs: object) -> _Job:
        self.created.append(dict(kwargs))
        return _Job(name="batches/123", state="JOB_STATE_PENDING")

    def get(self, *, name: str) -> _Job:
        assert name == "batches/123"
        return _Job(
            name="batches/123",
            state="JOB_STATE_SUCCEEDED",
            file_name="files/output_jsonl",
        )


class _FakeClient:
    def __init__(self) -> None:
        self.files = _FakeFiles()
        self.batches = _FakeBatches()


def test_build_request_line_for_text_job() -> None:
    provider = GeminiBatchProvider(
        GeminiProviderConfig(
            api_key="k",
            model="gemini-2.5-flash",
            generation_config={"temperature": 0.1},
        ),
        client=_FakeClient(),
    )

    line = provider.build_request_line(
        custom_id="row1:a1",
        prompt_parts=PromptParts(prompt="hello", system_prompt="classify"),
    )

    assert line["key"] == "row1:a1"
    request = line["request"]
    assert request["contents"][0]["parts"][0]["text"] == "hello"
    assert request["system_instruction"]["parts"][0]["text"] == "classify"
    assert request["generation_config"]["temperature"] == 0.1


def test_build_request_line_for_structured_output() -> None:
    schema_name, schema = model_output_schema(_ClassificationResult)
    provider = GeminiBatchProvider(
        GeminiProviderConfig(api_key="k", model="gemini-2.5-flash"),
        client=_FakeClient(),
    )

    line = provider.build_request_line(
        custom_id="row1:a1",
        prompt_parts=PromptParts(prompt="hello"),
        structured_output=StructuredOutputSchema(schema_name, schema),
    )

    generation_config = line["request"]["generation_config"]
    assert generation_config["response_mime_type"] == "application/json"
    assert generation_config["response_json_schema"]["properties"]["label"]["type"] == "string"


def test_request_correlation_id_uses_gemini_key() -> None:
    provider = GeminiBatchProvider(
        GeminiProviderConfig(api_key="k", model="gemini-2.5-flash"),
        client=_FakeClient(),
    )
    line = provider.with_request_correlation_id(
        {"key": "old", "request": {"contents": []}},
        "new",
    )

    assert line["key"] == "new"
    assert provider.request_correlation_id(line) == "new"


def test_parse_batch_output_and_extract_response_text() -> None:
    provider = GeminiBatchProvider(
        GeminiProviderConfig(api_key="k", model="gemini-2.5-flash"),
        client=_FakeClient(),
    )

    success, errors, raw = provider.parse_batch_output(
        output_content=(
            '{"key":"ok","response":{"candidates":[{"content":{"parts":[{"text":"hello"},{"text":"world"}]}}]}}\n'
            '{"key":"bad","error":{"message":"nope"}}\n'
        ),
        error_content=None,
    )

    assert set(success) == {"ok"}
    assert set(errors) == {"bad"}
    assert len(raw) == 2
    assert provider.extract_response_text(success["ok"]) == "hello\nworld"


def test_upload_create_get_download_and_delete() -> None:
    fake = _FakeClient()
    provider = GeminiBatchProvider(
        GeminiProviderConfig(api_key="k", model="gemini-2.5-flash"),
        client=fake,
    )

    uploaded = provider.upload_input_file(Path(__file__))
    created = provider.create_batch(input_file_id=uploaded, metadata={"run_id": "run1"})
    fetched = provider.get_batch("batches/123")

    assert uploaded == "files/input_jsonl"
    assert created["id"] == "batches/123"
    assert created["status"] == "submitted"
    assert fake.batches.created[0]["src"] == "files/input_jsonl"
    assert fake.batches.created[0]["config"] == {"display_name": "batchor-run1"}
    assert fetched["status"] == "completed"
    assert fetched["output_file_id"] == "files/output_jsonl"
    assert provider.download_file_content("files/output_jsonl").startswith('{"key"')
    provider.delete_input_file(uploaded)
    assert fake.files.deleted == ["files/input_jsonl"]


def test_resolve_gemini_api_key_prefers_explicit_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "env-key")
    resolved = resolve_gemini_api_key(GeminiProviderConfig(api_key="explicit-key", model="gemini-2.5-flash"))
    assert resolved == "explicit-key"


def test_resolve_gemini_api_key_falls_back_to_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "env-key")
    resolved = resolve_gemini_api_key(GeminiProviderConfig(model="gemini-2.5-flash"))
    assert resolved == "env-key"


def test_resolve_gemini_api_key_requires_explicit_or_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    with pytest.raises(ValueError, match="Gemini API key is required"):
        resolve_gemini_api_key(GeminiProviderConfig(model="gemini-2.5-flash"))
