from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Callable

import pytest
from pydantic import BaseModel

from batchor import (
    BatchItem,
    BatchJob,
    BatchRunner,
    MemoryStateStore,
    OpenAIProviderConfig,
    PromptParts,
    RetryPolicy,
    RunNotFinishedError,
    SQLiteStorage,
)
from batchor.openai_provider import OpenAIBatchProvider


class ClassificationResult(BaseModel):
    label: str
    score: float


class _FakeClock:
    def __init__(self) -> None:
        self.current = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.sleeps: list[float] = []

    def now(self) -> datetime:
        return self.current

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(float(seconds))
        self.current += timedelta(seconds=float(seconds))


def _success_record(text: str) -> dict[str, object]:
    return {
        "custom_id": "",
        "response": {
            "status_code": 200,
            "body": {
                "output": [
                    {
                        "content": [
                            {
                                "text": text,
                            }
                        ]
                    }
                ]
            },
        },
    }


class _FakeBatchProvider:
    def __init__(
        self,
        *,
        record_factory: Callable[[str], dict[str, object] | None],
        create_failures: list[Exception] | None = None,
    ) -> None:
        self.record_factory = record_factory
        self.create_failures = list(create_failures or [])
        self._next_file = 0
        self._next_batch = 0
        self._current_lines: list[dict[str, object]] = []
        self._file_to_lines: dict[str, list[dict[str, object]]] = {}
        self._batch_to_file: dict[str, str] = {}
        self._parser = OpenAIBatchProvider(
            OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
            client=object(),
        )

    def build_request_line(
        self,
        *,
        custom_id: str,
        prompt_parts: PromptParts,
        structured_output=None,  # noqa: ANN001
    ) -> dict[str, object]:
        body: dict[str, object] = {"input": prompt_parts.prompt}
        if prompt_parts.system_prompt:
            body["instructions"] = prompt_parts.system_prompt
        if structured_output is not None:
            body["schema_name"] = structured_output.name
        return {
            "custom_id": custom_id,
            "method": "POST",
            "url": "/v1/responses",
            "body": body,
        }

    def write_requests_jsonl(self, request_lines: list[dict[str, object]], output_path: Path) -> Path:
        self._current_lines = [dict(line) for line in request_lines]
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("\n".join(json.dumps(line) for line in request_lines), encoding="utf-8")
        return output_path

    def upload_input_file(self, _input_path: Path) -> str:
        file_id = f"file_{self._next_file}"
        self._next_file += 1
        self._file_to_lines[file_id] = list(self._current_lines)
        return file_id

    def create_batch(self, *, input_file_id: str, metadata: dict[str, str] | None = None) -> dict[str, object]:
        if self.create_failures:
            raise self.create_failures.pop(0)
        batch_id = f"batch_{self._next_batch}"
        self._next_batch += 1
        self._batch_to_file[batch_id] = input_file_id
        return {"id": batch_id, "status": "submitted", "metadata": metadata or {}}

    def get_batch(self, batch_id: str) -> dict[str, object]:
        return {
            "id": batch_id,
            "status": "completed",
            "output_file_id": f"output_{batch_id}",
            "error_file_id": None,
        }

    def download_file_content(self, file_id: str) -> str:
        if not file_id.startswith("output_"):
            return ""
        batch_id = file_id.replace("output_", "")
        lines = self._file_to_lines[self._batch_to_file[batch_id]]
        records: list[str] = []
        for line in lines:
            custom_id = str(line["custom_id"])
            record = self.record_factory(custom_id)
            if record is None:
                continue
            payload = dict(record)
            payload["custom_id"] = custom_id
            records.append(json.dumps(payload))
        return "\n".join(records) + ("\n" if records else "")

    def parse_batch_output(self, *, output_content: str | None, error_content: str | None):
        return self._parser.parse_batch_output(
            output_content=output_content,
            error_content=error_content,
        )


def test_structured_run_handle_returns_model_instances(tmp_path: Path) -> None:
    provider = _FakeBatchProvider(
        record_factory=lambda custom_id: _success_record(
            json.dumps({"label": custom_id.split(":")[0], "score": 0.9})
        )
    )
    runner = BatchRunner(
        storage="memory",
        provider_factory=lambda _cfg: provider,
        temp_root=tmp_path,
    )
    run = runner.start(
        BatchJob(
            items=[
                BatchItem(item_id="row1", payload={"text": "a"}),
                BatchItem(item_id="row2", payload={"text": "b"}),
            ],
            build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
            structured_output=ClassificationResult,
            provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
        )
    )
    assert run.status == "running"
    with pytest.raises(RunNotFinishedError):
        run.results()
    run.wait()
    assert run.is_finished is True
    summary = run.summary()
    assert summary.completed_items == 2
    results = run.results()
    assert isinstance(results[0].output, ClassificationResult)
    assert results[0].output.label == "row1"


def test_text_run_snapshot_exposes_partial_state(tmp_path: Path) -> None:
    provider = _FakeBatchProvider(record_factory=lambda custom_id: _success_record(f"text:{custom_id}"))
    runner = BatchRunner(
        storage="memory",
        provider_factory=lambda _cfg: provider,
        temp_root=tmp_path,
    )
    run = runner.start(
        BatchJob(
            items=[BatchItem(item_id="row1", payload="hello")],
            build_prompt=lambda item: PromptParts(prompt=item.payload),
            provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
        )
    )
    snapshot = run.snapshot()
    assert snapshot.status == "running"
    assert snapshot.items[0].status == "submitted"
    run.refresh()
    assert run.results()[0].output_text == "text:row1:a1"


def test_invalid_json_retries_until_failed_permanent(tmp_path: Path) -> None:
    provider = _FakeBatchProvider(record_factory=lambda _custom_id: _success_record("{not json}"))
    runner = BatchRunner(
        storage="memory",
        provider_factory=lambda _cfg: provider,
        temp_root=tmp_path,
    )
    run = runner.run_and_wait(
        BatchJob(
            items=[BatchItem(item_id="row1", payload={"text": "a"})],
            build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
            structured_output=ClassificationResult,
            provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
            retry_policy=RetryPolicy(max_attempts=2, base_backoff_sec=0, max_backoff_sec=0),
        )
    )
    result = run.results()[0]
    assert result.status == "failed_permanent"
    assert result.attempt_count == 2
    assert result.error is not None
    assert result.error.error_class == "invalid_json"


def test_missing_output_record_retries_without_consuming_attempt(tmp_path: Path) -> None:
    seen_counts: dict[str, int] = {}

    def record_factory(custom_id: str) -> dict[str, object] | None:
        seen_counts[custom_id] = seen_counts.get(custom_id, 0) + 1
        if seen_counts[custom_id] == 1:
            return None
        return _success_record(json.dumps({"label": "ai", "score": 0.9}))

    provider = _FakeBatchProvider(record_factory=record_factory)
    runner = BatchRunner(
        storage="memory",
        provider_factory=lambda _cfg: provider,
        temp_root=tmp_path,
    )
    run = runner.run_and_wait(
        BatchJob(
            items=[BatchItem(item_id="row1", payload={"text": "a"})],
            build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
            structured_output=ClassificationResult,
            provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
            retry_policy=RetryPolicy(max_attempts=2, base_backoff_sec=0, max_backoff_sec=0),
        )
    )
    result = run.results()[0]
    assert result.status == "completed"
    assert result.attempt_count == 0
    assert seen_counts["row1:a1"] == 2


def test_enqueue_limit_create_failure_recovers_without_consuming_attempts(tmp_path: Path) -> None:
    clock = _FakeClock()
    provider = _FakeBatchProvider(
        record_factory=lambda _custom_id: _success_record('{"label":"ai","score":0.9}'),
        create_failures=[RuntimeError("Enqueued token limit reached for gpt-4.1")],
    )
    runner = BatchRunner(
        storage=MemoryStateStore(now=clock.now),
        provider_factory=lambda _cfg: provider,
        sleep=clock.sleep,
        temp_root=tmp_path,
    )
    run = runner.run_and_wait(
        BatchJob(
            items=[BatchItem(item_id="row1", payload={"text": "a"})],
            build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
            structured_output=ClassificationResult,
            provider_config=OpenAIProviderConfig(
                api_key="k",
                model="gpt-4.1",
                poll_interval_sec=1.0,
            ),
            retry_policy=RetryPolicy(max_attempts=2, base_backoff_sec=1.0, max_backoff_sec=1.0),
        )
    )
    result = run.results()[0]
    assert result.status == "completed"
    assert result.attempt_count == 0
    assert any(sleep >= 1.0 for sleep in clock.sleeps)


def test_sqlite_storage_supports_rehydrating_run_handle(tmp_path: Path) -> None:
    provider = _FakeBatchProvider(
        record_factory=lambda custom_id: _success_record(
            json.dumps({"label": custom_id.split(":")[0], "score": 0.9})
        )
    )
    storage = SQLiteStorage(path=tmp_path / "batchor.sqlite3")
    runner_one = BatchRunner(
        storage=storage,
        provider_factory=lambda _cfg: provider,
        temp_root=tmp_path / "one",
    )
    started = runner_one.start(
        BatchJob(
            items=[BatchItem(item_id="row1", payload={"text": "hello"})],
            build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
            structured_output=ClassificationResult,
            provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
        )
    )
    assert started.status == "running"

    runner_two = BatchRunner(
        storage=SQLiteStorage(path=storage.path),
        provider_factory=lambda _cfg: provider,
        temp_root=tmp_path / "two",
    )
    resumed = runner_two.get_run(started.run_id)
    resumed.wait()
    results = resumed.results()
    assert results[0].output is not None
    assert results[0].output.label == "row1"
