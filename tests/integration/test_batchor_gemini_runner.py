from __future__ import annotations

import json
from pathlib import Path
from typing import Callable

from pydantic import BaseModel

from batchor import (
    BatchItem,
    BatchJob,
    BatchRunner,
    GeminiBatchInputMode,
    GeminiProviderConfig,
    MemoryStateStore,
    PromptParts,
)
from batchor.providers.gemini import GeminiBatchProvider


class ClassificationResult(BaseModel):
    label: str
    score: float


class _Uploaded:
    def __init__(self, name: str) -> None:
        self.name = name


class _State:
    def __init__(self, name: str) -> None:
        self.name = name


class _Dest:
    def __init__(self, file_name: str | None = None) -> None:
        self.file_name = file_name


class _BatchJob:
    def __init__(self, *, name: str, state: str, file_name: str | None = None) -> None:
        self.name = name
        self.state = _State(state)
        self.dest = _Dest(file_name)


class _FakeGeminiFiles:
    def __init__(self, response_builder: Callable[[dict[str, object]], str]) -> None:
        self.response_builder = response_builder
        self._next_file = 0
        self.input_lines_by_file: dict[str, list[dict[str, object]]] = {}
        self.batch_file_by_output_file: dict[str, str] = {}

    def upload(self, *, file: str, config: object) -> _Uploaded:
        del config
        file_name = f"files/input_{self._next_file}"
        self._next_file += 1
        self.input_lines_by_file[file_name] = [
            json.loads(raw_line) for raw_line in Path(file).read_text(encoding="utf-8").splitlines() if raw_line
        ]
        return _Uploaded(file_name)

    def download(self, *, file: str) -> bytes:
        input_file = self.batch_file_by_output_file[file]
        records = [self.response_builder(line) for line in self.input_lines_by_file[input_file]]
        return ("\n".join(records) + "\n").encode()

    def delete(self, *, name: str) -> None:
        del name


class _FakeGeminiBatches:
    def __init__(self, files: _FakeGeminiFiles) -> None:
        self.files = files
        self._next_batch = 0
        self.batch_to_input: dict[str, str] = {}

    def create(self, *, model: str, src: str, config: dict[str, object]) -> _BatchJob:
        del model, config
        batch_id = f"batches/{self._next_batch}"
        self._next_batch += 1
        self.batch_to_input[batch_id] = src
        self.files.batch_file_by_output_file[f"files/output_{self._next_batch - 1}"] = src
        return _BatchJob(name=batch_id, state="JOB_STATE_PENDING")

    def get(self, *, name: str) -> _BatchJob:
        assert name in self.batch_to_input
        index = name.rsplit("/", maxsplit=1)[1]
        return _BatchJob(
            name=name,
            state="JOB_STATE_SUCCEEDED",
            file_name=f"files/output_{index}",
        )


class _FakeGeminiClient:
    def __init__(self, response_builder: Callable[[dict[str, object]], str]) -> None:
        self.files = _FakeGeminiFiles(response_builder)
        self.batches = _FakeGeminiBatches(self.files)


def _line_key(line: dict[str, object]) -> str:
    key = line["key"]
    assert isinstance(key, str)
    return key


def _prompt(line: dict[str, object]) -> str:
    request = line["request"]
    assert isinstance(request, dict)
    contents = request["contents"]
    assert isinstance(contents, list)
    first = contents[0]
    assert isinstance(first, dict)
    parts = first["parts"]
    assert isinstance(parts, list)
    first_part = parts[0]
    assert isinstance(first_part, dict)
    text = first_part["text"]
    assert isinstance(text, str)
    return text


def test_batch_runner_completes_text_job_with_gemini_provider() -> None:
    def response_builder(line: dict[str, object]) -> str:
        return json.dumps(
            {
                "key": _line_key(line),
                "response": {
                    "candidates": [
                        {
                            "content": {
                                "parts": [
                                    {
                                        "text": f"seen: {_prompt(line)}",
                                    }
                                ]
                            }
                        }
                    ]
                },
            }
        )

    client = _FakeGeminiClient(response_builder)
    runner = BatchRunner(
        storage=MemoryStateStore(),
        provider_factory=lambda cfg: GeminiBatchProvider(cfg, client=client),
    )

    run = runner.run_and_wait(
        BatchJob(
            items=[BatchItem(item_id="row1", payload={"text": "hello"})],
            build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
            provider_config=GeminiProviderConfig(
                api_key="k",
                model="gemini-2.5-flash",
                input_mode=GeminiBatchInputMode.FILE,
            ),
        )
    )

    result = run.results()[0]
    assert result.output_text == "seen: hello"
    assert result.raw_response is not None
    assert result.raw_response["key"] == "row1:a1"


def test_batch_runner_completes_structured_job_with_gemini_provider() -> None:
    def response_builder(line: dict[str, object]) -> str:
        return json.dumps(
            {
                "key": _line_key(line),
                "response": {
                    "candidates": [
                        {
                            "content": {
                                "parts": [
                                    {
                                        "text": '{"label":"gemini","score":0.8}',
                                    }
                                ]
                            }
                        }
                    ]
                },
            }
        )

    client = _FakeGeminiClient(response_builder)
    runner = BatchRunner(
        storage=MemoryStateStore(),
        provider_factory=lambda cfg: GeminiBatchProvider(cfg, client=client),
    )

    run = runner.run_and_wait(
        BatchJob(
            items=[BatchItem(item_id="row1", payload={"text": "classify"})],
            build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
            provider_config=GeminiProviderConfig(
                api_key="k",
                model="gemini-2.5-flash",
                input_mode=GeminiBatchInputMode.FILE,
            ),
            structured_output=ClassificationResult,
        )
    )

    result = run.results()[0]
    assert result.output is not None
    assert result.output.label == "gemini"
    submitted_line = client.files.input_lines_by_file["files/input_0"][0]
    request = submitted_line["request"]
    assert isinstance(request, dict)
    generation_config = request["generation_config"]
    assert isinstance(generation_config, dict)
    assert generation_config["response_mime_type"] == "application/json"
    assert "response_json_schema" in generation_config
