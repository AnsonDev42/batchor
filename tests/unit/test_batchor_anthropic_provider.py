from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import BaseModel

from batchor.core.models import AnthropicProviderConfig, PromptParts
from batchor.providers.anthropic import AnthropicBatchProvider, resolve_anthropic_api_key
from batchor.providers.base import StructuredOutputSchema
from batchor.runtime.validation import model_output_schema


class _ClassificationResult(BaseModel):
    label: str


class _Batches:
    def __init__(self) -> None:
        self.requests: list[dict[str, object]] = []

    def create(self, *, requests):  # noqa: ANN001
        self.requests = requests
        return {"id": "msgbatch_123", "processing_status": "in_progress", "request_counts": {}}

    def retrieve(self, batch_id: str):  # noqa: ANN201
        assert batch_id == "msgbatch_123"
        return {"id": batch_id, "processing_status": "ended", "request_counts": {"succeeded": 1}}

    def results(self, batch_id: str):  # noqa: ANN201
        assert batch_id == "msgbatch_123"
        return iter(
            [
                {
                    "custom_id": "b525575f3ea077094fb3f5f3de2fbab850d5b5db5",
                    "result": {
                        "type": "succeeded",
                        "message": {"content": [{"type": "text", "text": "done"}]},
                    },
                },
                {"custom_id": "b3dfa78c04a9678358e0ba53f3cf4b9723be1df2a", "result": {"type": "expired"}},
            ]
        )


class _Messages:
    def __init__(self) -> None:
        self.batches = _Batches()


class _Client:
    def __init__(self) -> None:
        self.messages = _Messages()


def test_build_request_line_supports_system_and_structured_output() -> None:
    _, schema = model_output_schema(_ClassificationResult)
    provider = AnthropicBatchProvider(
        AnthropicProviderConfig(
            api_key="k",
            model="claude-sonnet-4-5",
            max_tokens=512,
            message_params={"temperature": 0.2},
        ),
        client=_Client(),
    )

    line = provider.build_request_line(
        custom_id="row1:a1",
        prompt_parts=PromptParts(prompt="hello", system_prompt="classify"),
        structured_output=StructuredOutputSchema("classification_result", schema),
    )

    assert line["custom_id"] == "b525575f3ea077094fb3f5f3de2fbab850d5b5db5"
    assert line["body"]["messages"] == [{"role": "user", "content": "hello"}]
    assert line["body"]["system"] == "classify"
    assert line["body"]["temperature"] == 0.2
    assert line["body"]["output_config"]["format"]["schema"] == schema
    replayed = provider.with_request_correlation_id(line, "row2:a1")
    assert replayed["custom_id"] == "b3dfa78c04a9678358e0ba53f3cf4b9723be1df2a"


def test_submit_poll_download_and_parse(tmp_path: Path) -> None:
    client = _Client()
    provider = AnthropicBatchProvider(
        AnthropicProviderConfig(api_key="k", model="claude-sonnet-4-5", max_tokens=512),
        client=client,
    )
    input_path = tmp_path / "requests.jsonl"
    input_path.write_text(
        '{"custom_id":"b525575f3ea077094fb3f5f3de2fbab850d5b5db5",'
        '"body":{"model":"claude-sonnet-4-5","max_tokens":512,'
        '"messages":[{"role":"user","content":"hello"}]}}\n',
        encoding="utf-8",
    )

    input_id = provider.upload_input_file(input_path)
    created = provider.create_batch(input_file_id=input_id)
    remote = provider.get_batch(created["id"])
    output = provider.download_file_content(remote["output_file_id"])
    successes, errors, raw = provider.parse_batch_output(output_content=output, error_content=None)

    assert client.messages.batches.requests[0]["custom_id"] == "b525575f3ea077094fb3f5f3de2fbab850d5b5db5"
    assert created["status"] == "in_progress"
    assert remote["status"] == "completed"
    assert set(successes) == {"b525575f3ea077094fb3f5f3de2fbab850d5b5db5"}
    assert set(errors) == {"b3dfa78c04a9678358e0ba53f3cf4b9723be1df2a"}
    assert provider.extract_response_text(successes["b525575f3ea077094fb3f5f3de2fbab850d5b5db5"]) == "done"
    assert len(raw) == 2


def test_config_validation_and_api_key_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "env-key")
    config = AnthropicProviderConfig(model="claude-sonnet-4-5", max_tokens=1)
    assert resolve_anthropic_api_key(config) == "env-key"
    with pytest.raises(ValueError, match="reserved fields"):
        AnthropicProviderConfig(model="claude-sonnet-4-5", max_tokens=1, message_params={"stream": True})
