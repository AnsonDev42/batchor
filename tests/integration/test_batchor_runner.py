from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Callable

import pytest
from pydantic import BaseModel
from sqlalchemy import select

from batchor import (
    ArtifactPolicy,
    BatchItem,
    BatchJob,
    BatchRunner,
    ChunkPolicy,
    CompositeItemSource,
    ItemStatus,
    JsonlItemSource,
    MemoryStateStore,
    OpenAIEnqueueLimitConfig,
    OpenAIProviderConfig,
    PromptParts,
    RetryPolicy,
    RunControlState,
    RunEvent,
    RunLifecycleStatus,
    RunNotFinishedError,
    RunPausedError,
    SQLiteStorage,
)
from batchor.providers.openai import OpenAIBatchProvider
from batchor.storage import sqlite as storage_sqlite
from batchor.storage.state import MaterializedItem, RequestArtifactPointer


class ClassificationResult(BaseModel):
    label: str
    score: float


class _FakeClock:
    def __init__(self) -> None:
        self.current = datetime(2026, 1, 1, tzinfo=UTC)
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
        self.created_batches: list[str] = []
        self.deleted_files: list[str] = []
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

    def upload_input_file(self, input_path: Path) -> str:
        self._current_lines = [
            json.loads(raw_line) for raw_line in input_path.read_text(encoding="utf-8").splitlines() if raw_line.strip()
        ]
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
        self.created_batches.append(batch_id)
        return {"id": batch_id, "status": "submitted", "metadata": metadata or {}}

    def get_batch(self, batch_id: str) -> dict[str, object]:
        return {
            "id": batch_id,
            "status": "completed",
            "output_file_id": f"output_{batch_id}",
            "error_file_id": None,
        }

    def delete_input_file(self, file_id: str) -> None:
        self.deleted_files.append(file_id)

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

    def estimate_request_tokens(
        self,
        request_line: dict[str, object],
        *,
        chars_per_token: int,
    ) -> int:
        del chars_per_token
        body = request_line["body"]
        if not isinstance(body, dict):
            raise TypeError("request body must be a dict")
        prompt = body.get("input", "")
        if not isinstance(prompt, str):
            raise TypeError("request input must be a string")
        return max(len(prompt), 1)


class _ArtifactOnlyBatchProvider(_FakeBatchProvider):
    def build_request_line(
        self,
        *,
        custom_id: str,
        prompt_parts: PromptParts,
        structured_output=None,  # noqa: ANN001
    ) -> dict[str, object]:
        del custom_id, prompt_parts, structured_output
        raise AssertionError("request line should be replayed from persisted artifact")


class _TransientPollFailureBatchProvider(_FakeBatchProvider):
    def __init__(
        self,
        *,
        record_factory: Callable[[str], dict[str, object] | None],
    ) -> None:
        super().__init__(record_factory=record_factory)
        self._poll_failures: dict[str, int] = {"batch_0": 1}

    def get_batch(self, batch_id: str) -> dict[str, object]:
        remaining = self._poll_failures.get(batch_id, 0)
        if remaining > 0:
            self._poll_failures[batch_id] = remaining - 1
            raise ConnectionError("connection reset by peer")
        return super().get_batch(batch_id)


class _ControlledBatchProvider(_FakeBatchProvider):
    def __init__(
        self,
        *,
        record_factory: Callable[[str], dict[str, object] | None],
    ) -> None:
        super().__init__(record_factory=record_factory)
        self.released_batches: set[str] = set()
        self.polled_batches: list[str] = []

    def release_batch(self, batch_id: str) -> None:
        self.released_batches.add(batch_id)

    def get_batch(self, batch_id: str) -> dict[str, object]:
        self.polled_batches.append(batch_id)
        if batch_id in self.released_batches:
            return {
                "id": batch_id,
                "status": "completed",
                "output_file_id": f"output_{batch_id}",
                "error_file_id": None,
            }
        return {
            "id": batch_id,
            "status": "submitted",
            "output_file_id": None,
            "error_file_id": None,
        }


def test_sqlite_subprocess_resume_retries_from_persisted_request_artifact(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "subprocess_resume.sqlite3"
    artifact_root = tmp_path / "subprocess_artifacts"
    run_id = "subprocess_resume_run"
    child_code = textwrap.dedent(
        f"""
        import json
        import os
        from pathlib import Path

        from batchor import BatchItem, BatchJob, BatchRunner, OpenAIProviderConfig, PromptParts, RetryPolicy, SQLiteStorage


        class CrashAfterArtifactProvider:
            def __init__(self) -> None:
                self._next_file = 0

            def build_request_line(self, *, custom_id, prompt_parts, structured_output=None):
                del structured_output
                return {{
                    "custom_id": custom_id,
                    "method": "POST",
                    "url": "/v1/responses",
                    "body": {{"input": prompt_parts.prompt}},
                }}

            def upload_input_file(self, input_path):
                Path(input_path).read_text(encoding="utf-8")
                file_id = f"file_{{self._next_file}}"
                self._next_file += 1
                return file_id

            def delete_input_file(self, file_id):
                del file_id

            def create_batch(self, *, input_file_id, metadata=None):
                del input_file_id, metadata
                os._exit(97)

            def get_batch(self, batch_id):
                raise AssertionError(f"unexpected get_batch: {{batch_id}}")

            def download_file_content(self, file_id):
                raise AssertionError(f"unexpected download: {{file_id}}")

            def parse_batch_output(self, *, output_content, error_content):
                del output_content, error_content
                return {{}}, {{}}, []

            def estimate_request_tokens(self, request_line, *, chars_per_token):
                del chars_per_token
                return max(len(json.dumps(request_line)), 1)


        runner = BatchRunner(
            storage=SQLiteStorage(path=Path({str(db_path)!r})),
            provider_factory=lambda _cfg: CrashAfterArtifactProvider(),
            temp_root=Path({str(artifact_root)!r}),
        )
        runner.start(
            BatchJob(
                items=[BatchItem(item_id="row1", payload={{"text": "hello"}})],
                build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
                structured_output=None,
                provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
                retry_policy=RetryPolicy(max_attempts=2, base_backoff_sec=0, max_backoff_sec=0),
            ),
            run_id={run_id!r},
        )
        """
    )
    env = dict(os.environ)
    pythonpath = str(Path(__file__).resolve().parents[2] / "src")
    env["PYTHONPATH"] = f"{pythonpath}{os.pathsep}{env['PYTHONPATH']}" if env.get("PYTHONPATH") else pythonpath
    child = subprocess.run(
        [sys.executable, "-c", child_code],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert child.returncode == 97

    storage = SQLiteStorage(path=db_path)
    with storage.engine.begin() as conn:
        row = (
            conn.execute(
                select(
                    storage_sqlite.ITEMS_TABLE.c.prompt,
                    storage_sqlite.ITEMS_TABLE.c.request_artifact_path,
                    storage_sqlite.ITEMS_TABLE.c.request_artifact_line,
                    storage_sqlite.ITEMS_TABLE.c.request_sha256,
                    storage_sqlite.ITEMS_TABLE.c.status,
                ).where(storage_sqlite.ITEMS_TABLE.c.run_id == run_id)
            )
            .mappings()
            .one()
        )
    assert row["prompt"] == ""
    assert row["request_artifact_path"] is not None
    assert row["request_artifact_line"] == 1
    assert row["request_sha256"] is not None
    assert row["status"] == ItemStatus.QUEUED_LOCAL

    resumed = BatchRunner(
        storage=SQLiteStorage(path=db_path),
        provider_factory=lambda _cfg: _ArtifactOnlyBatchProvider(
            record_factory=lambda custom_id: _success_record(
                json.dumps({"label": custom_id.split(":")[0], "score": 0.9})
            )
        ),
        temp_root=artifact_root,
    ).get_run(run_id)
    resumed.wait(poll_interval=0)
    result = resumed.results()[0]
    assert result.status is ItemStatus.COMPLETED
    assert result.output_text == '{"label": "row1", "score": 0.9}' or result.output_text is not None


def test_request_artifact_replay_reads_shared_file_once_per_submission_cycle(
    tmp_path: Path,
) -> None:
    provider = _ArtifactOnlyBatchProvider(record_factory=lambda custom_id: _success_record(f"text:{custom_id}"))
    runner = BatchRunner(
        storage="memory",
        provider_factory=lambda _cfg: provider,
        temp_root=tmp_path,
    )
    job = BatchJob(
        items=[
            BatchItem(item_id="row1", payload={"text": "a"}),
            BatchItem(item_id="row2", payload={"text": "b"}),
        ],
        build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
        provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
    )
    run_id = "artifact_cache_run"
    runner.state.create_run(
        run_id=run_id,
        config=runner._persisted_config_for_job(job),
        items=[
            MaterializedItem(
                item_id="row1",
                item_index=0,
                payload={"text": "a"},
                metadata={},
                prompt="a",
            ),
            MaterializedItem(
                item_id="row2",
                item_index=1,
                payload={"text": "b"},
                metadata={},
                prompt="b",
            ),
        ],
    )

    request_lines = [
        {
            "custom_id": "row1:a1",
            "method": "POST",
            "url": "/v1/responses",
            "body": {"input": "a"},
        },
        {
            "custom_id": "row2:a1",
            "method": "POST",
            "url": "/v1/responses",
            "body": {"input": "b"},
        },
    ]
    artifact_path = f"{run_id}/requests/requests_a.jsonl"
    runner.artifact_store.write_text(
        artifact_path,
        "".join(json.dumps(line) + "\n" for line in request_lines),
        encoding="utf-8",
    )
    runner.state.record_request_artifacts(
        run_id=run_id,
        pointers=[
            RequestArtifactPointer(
                item_id="row1",
                artifact_path=artifact_path,
                line_number=1,
                request_sha256=runner._request_sha256(request_lines[0]),
            ),
            RequestArtifactPointer(
                item_id="row2",
                artifact_path=artifact_path,
                line_number=2,
                request_sha256=runner._request_sha256(request_lines[1]),
            ),
        ],
    )

    read_count = 0
    original_read_text = runner.artifact_store.read_text

    def counting_read_text(key: str, *, encoding: str = "utf-8") -> str:
        nonlocal read_count
        read_count += 1
        return original_read_text(key, encoding=encoding)

    runner.artifact_store.read_text = counting_read_text  # type: ignore[method-assign]
    run = runner.get_run(run_id)
    submitted = runner._submit_pending_items(run.run_id, run._context)

    assert submitted == 2
    assert read_count == 1
    assert provider.created_batches == ["batch_0"]


def test_transient_poll_failures_do_not_block_new_submissions(tmp_path: Path) -> None:
    provider = _TransientPollFailureBatchProvider(record_factory=lambda custom_id: _success_record(f"text:{custom_id}"))
    runner = BatchRunner(
        storage="memory",
        provider_factory=lambda _cfg: provider,
        temp_root=tmp_path,
    )
    run = runner.start(
        BatchJob(
            items=[
                BatchItem(item_id="row1", payload="a"),
                BatchItem(item_id="row2", payload="b"),
                BatchItem(item_id="row3", payload="c"),
                BatchItem(item_id="row4", payload="d"),
                BatchItem(item_id="row5", payload="e"),
            ],
            build_prompt=lambda item: PromptParts(prompt=item.payload),
            provider_config=OpenAIProviderConfig(
                api_key="k",
                model="gpt-4.1",
                enqueue_limits=OpenAIEnqueueLimitConfig(
                    enqueued_token_limit=5,
                    target_ratio=1.0,
                    headroom=0,
                    max_batch_enqueued_tokens=1,
                ),
            ),
            chunk_policy=ChunkPolicy(max_requests=1, max_file_bytes=1024),
            retry_policy=RetryPolicy(max_attempts=2, base_backoff_sec=1.0, max_backoff_sec=1.0),
        )
    )

    assert provider.created_batches == ["batch_0", "batch_1", "batch_2", "batch_3"]

    run.refresh()

    assert provider.created_batches == ["batch_0", "batch_1", "batch_2", "batch_3", "batch_4"]
    summary = run.summary()
    assert summary.completed_items == 3
    assert summary.status_counts[ItemStatus.SUBMITTED] == 2


def test_pause_blocks_polling_and_wait_raises_paused_error(tmp_path: Path) -> None:
    provider = _ControlledBatchProvider(record_factory=lambda custom_id: _success_record(f"text:{custom_id}"))
    runner = BatchRunner(
        storage="memory",
        provider_factory=lambda _cfg: provider,
        temp_root=tmp_path,
    )
    run = runner.start(
        BatchJob(
            items=[
                BatchItem(item_id="row1", payload="a"),
                BatchItem(item_id="row2", payload="b"),
                BatchItem(item_id="row3", payload="c"),
            ],
            build_prompt=lambda item: PromptParts(prompt=item.payload),
            provider_config=OpenAIProviderConfig(
                api_key="k",
                model="gpt-4.1",
                enqueue_limits=OpenAIEnqueueLimitConfig(
                    enqueued_token_limit=1,
                    target_ratio=1.0,
                    max_batch_enqueued_tokens=1,
                ),
            ),
            chunk_policy=ChunkPolicy(max_requests=1, max_file_bytes=1024),
        )
    )

    paused = run.pause()
    assert paused.control_state is RunControlState.PAUSED
    assert provider.created_batches == ["batch_0"]
    run.refresh()
    assert provider.polled_batches == []
    with pytest.raises(RunPausedError):
        run.wait(timeout=0.1, poll_interval=0)

    provider.release_batch("batch_0")
    resumed = run.resume()
    assert resumed.control_state is RunControlState.RUNNING
    run.refresh()
    assert provider.polled_batches == ["batch_0"]
    assert provider.created_batches == ["batch_0", "batch_1"]


def test_cancel_drains_active_batch_and_marks_remaining_items_cancelled(tmp_path: Path) -> None:
    provider = _ControlledBatchProvider(record_factory=lambda custom_id: _success_record(f"text:{custom_id}"))
    runner = BatchRunner(
        storage="memory",
        provider_factory=lambda _cfg: provider,
        temp_root=tmp_path,
    )
    run = runner.start(
        BatchJob(
            items=[
                BatchItem(item_id="row1", payload="a"),
                BatchItem(item_id="row2", payload="b"),
                BatchItem(item_id="row3", payload="c"),
            ],
            build_prompt=lambda item: PromptParts(prompt=item.payload),
            provider_config=OpenAIProviderConfig(
                api_key="k",
                model="gpt-4.1",
                enqueue_limits=OpenAIEnqueueLimitConfig(
                    enqueued_token_limit=1,
                    target_ratio=1.0,
                    max_batch_enqueued_tokens=1,
                ),
            ),
            chunk_policy=ChunkPolicy(max_requests=1, max_file_bytes=1024),
        )
    )

    cancelled = run.cancel()
    assert cancelled.control_state is RunControlState.CANCEL_REQUESTED
    provider.release_batch("batch_0")
    run.refresh()

    assert run.status is RunLifecycleStatus.COMPLETED_WITH_FAILURES
    results = run.results()
    assert results[0].status is ItemStatus.COMPLETED
    assert results[1].status is ItemStatus.FAILED_PERMANENT
    assert results[1].error is not None
    assert results[1].error.error_class == "run_cancelled"
    assert results[2].status is ItemStatus.FAILED_PERMANENT


def test_read_terminal_results_and_export_terminal_results_use_sequence_cursor(
    tmp_path: Path,
) -> None:
    provider = _ControlledBatchProvider(record_factory=lambda custom_id: _success_record(f"text:{custom_id}"))
    runner = BatchRunner(
        storage="memory",
        provider_factory=lambda _cfg: provider,
        temp_root=tmp_path,
    )
    run = runner.start(
        BatchJob(
            items=[
                BatchItem(item_id="row1", payload="a"),
                BatchItem(item_id="row2", payload="b"),
            ],
            build_prompt=lambda item: PromptParts(prompt=item.payload),
            provider_config=OpenAIProviderConfig(
                api_key="k",
                model="gpt-4.1",
                enqueue_limits=OpenAIEnqueueLimitConfig(
                    enqueued_token_limit=1,
                    target_ratio=1.0,
                    max_batch_enqueued_tokens=1,
                ),
            ),
            chunk_policy=ChunkPolicy(max_requests=1, max_file_bytes=1024),
        )
    )

    initial_page = run.read_terminal_results()
    assert initial_page.items == []
    assert initial_page.next_after_sequence == 0

    provider.release_batch("batch_0")
    run.refresh()
    first_page = run.read_terminal_results()
    assert [result.item_id for result in first_page.items] == ["row1"]
    assert first_page.next_after_sequence == 1

    export_path = tmp_path / "terminal-results.jsonl"
    export = run.export_terminal_results(str(export_path), append=False)
    assert export.exported_count == 1
    assert export.next_after_sequence == 1

    provider.release_batch("batch_1")
    run.refresh()
    second_page = run.read_terminal_results(after_sequence=first_page.next_after_sequence)
    assert [result.item_id for result in second_page.items] == ["row2"]
    assert second_page.next_after_sequence == 2
    followup = run.export_terminal_results(
        str(export_path),
        after_sequence=first_page.next_after_sequence,
        append=True,
    )
    assert followup.exported_count == 1
    lines = export_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert [json.loads(line)["item_id"] for line in lines] == ["row1", "row2"]


def test_disabling_raw_output_artifacts_keeps_results_but_skips_output_files(tmp_path: Path) -> None:
    provider = _FakeBatchProvider(
        record_factory=lambda custom_id: _success_record(json.dumps({"label": custom_id.split(":")[0], "score": 0.9}))
    )
    storage = SQLiteStorage(path=tmp_path / "artifact_policy.sqlite3")
    runner = BatchRunner(
        storage=storage,
        provider_factory=lambda _cfg: provider,
    )
    run = runner.run_and_wait(
        BatchJob(
            items=[BatchItem(item_id="row1", payload={"text": "hello"})],
            build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
            structured_output=ClassificationResult,
            provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
            artifact_policy=ArtifactPolicy(persist_raw_output_artifacts=False),
        )
    )

    inventory = storage.get_artifact_inventory(run_id=run.run_id)
    assert inventory.request_artifact_paths != []
    assert inventory.output_artifact_paths == []
    assert run.results()[0].output is not None


def test_structured_run_handle_returns_model_instances(tmp_path: Path) -> None:
    provider = _FakeBatchProvider(
        record_factory=lambda custom_id: _success_record(json.dumps({"label": custom_id.split(":")[0], "score": 0.9}))
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
    assert run.status is RunLifecycleStatus.RUNNING
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
    assert snapshot.status is RunLifecycleStatus.RUNNING
    assert snapshot.items[0].status is ItemStatus.SUBMITTED
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
    assert result.status is ItemStatus.FAILED_PERMANENT
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
    assert result.status is ItemStatus.COMPLETED
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
    assert result.status is ItemStatus.COMPLETED
    assert result.attempt_count == 0
    assert any(sleep >= 1.0 for sleep in clock.sleeps)
    assert provider.deleted_files == ["file_0"]


def test_sqlite_resume_ignores_api_key_mismatch(tmp_path: Path) -> None:
    provider = _FakeBatchProvider(
        record_factory=lambda custom_id: _success_record(json.dumps({"label": custom_id.split(":")[0], "score": 0.9}))
    )
    storage = SQLiteStorage(path=tmp_path / "resume_key.sqlite3")
    runner = BatchRunner(
        storage=storage,
        provider_factory=lambda _cfg: provider,
    )
    run = runner.start(
        BatchJob(
            items=[BatchItem(item_id="row1", payload={"text": "hello"})],
            build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
            structured_output=ClassificationResult,
            provider_config=OpenAIProviderConfig(api_key="first-key", model="gpt-4.1"),
        )
    )

    resumed = runner.start(
        BatchJob(
            items=[BatchItem(item_id="row1", payload={"text": "hello"})],
            build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
            structured_output=ClassificationResult,
            provider_config=OpenAIProviderConfig(api_key="rotated-key", model="gpt-4.1"),
        ),
        run_id=run.run_id,
    )
    resumed.wait()
    assert resumed.results()[0].output is not None


def test_sqlite_rehydration_loads_provider_config_without_persisted_api_key(tmp_path: Path) -> None:
    provider = _FakeBatchProvider(
        record_factory=lambda custom_id: _success_record(json.dumps({"label": custom_id.split(":")[0], "score": 0.9}))
    )
    storage = SQLiteStorage(path=tmp_path / "persisted_public.sqlite3")
    runner = BatchRunner(
        storage=storage,
        provider_factory=lambda _cfg: provider,
    )
    run = runner.start(
        BatchJob(
            items=[BatchItem(item_id="row1", payload={"text": "hello"})],
            build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
            structured_output=ClassificationResult,
            provider_config=OpenAIProviderConfig(api_key="secret-key", model="gpt-4.1"),
        )
    )
    rehydrated = BatchRunner(
        storage=SQLiteStorage(path=storage.path),
        provider_factory=lambda _cfg: provider,
    ).get_run(run.run_id)

    assert isinstance(rehydrated._context.config.provider_config, OpenAIProviderConfig)
    assert rehydrated._context.config.provider_config.api_key == ""


def test_runner_observer_receives_provider_lifecycle_events(tmp_path: Path) -> None:
    provider = _FakeBatchProvider(
        record_factory=lambda custom_id: _success_record(json.dumps({"label": custom_id.split(":")[0], "score": 0.9}))
    )
    events: list[RunEvent] = []
    runner = BatchRunner(
        storage="memory",
        provider_factory=lambda _cfg: provider,
        observer=events.append,
        temp_root=tmp_path,
    )

    run = runner.run_and_wait(
        BatchJob(
            items=[BatchItem(item_id="row1", payload={"text": "hello"})],
            build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
            structured_output=ClassificationResult,
            provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
        )
    )

    assert run.results()[0].output is not None
    event_types = {event.event_type for event in events}
    assert "run_created" in event_types
    assert "items_ingested" in event_types
    assert "batch_submitted" in event_types
    assert "batch_polled" in event_types
    assert "batch_completed" in event_types
    assert "items_completed" in event_types


def test_sqlite_storage_supports_rehydrating_run_handle(tmp_path: Path) -> None:
    provider = _FakeBatchProvider(
        record_factory=lambda custom_id: _success_record(json.dumps({"label": custom_id.split(":")[0], "score": 0.9}))
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
    assert started.status is RunLifecycleStatus.RUNNING

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


def test_sqlite_resume_retries_from_persisted_request_artifact(tmp_path: Path) -> None:
    first_provider = _FakeBatchProvider(
        record_factory=lambda custom_id: _success_record(json.dumps({"label": custom_id.split(":")[0], "score": 0.9})),
        create_failures=[RuntimeError("temporary service unavailable")],
    )
    storage = SQLiteStorage(path=tmp_path / "artifact_resume.sqlite3")
    first_runner = BatchRunner(
        storage=storage,
        provider_factory=lambda _cfg: first_provider,
    )
    started = first_runner.start(
        BatchJob(
            items=[BatchItem(item_id="row1", payload={"text": "hello"})],
            build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
            structured_output=ClassificationResult,
            provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
            retry_policy=RetryPolicy(max_attempts=2, base_backoff_sec=0, max_backoff_sec=0),
        )
    )

    with storage.engine.begin() as conn:
        row = (
            conn.execute(
                select(
                    storage_sqlite.ITEMS_TABLE.c.prompt,
                    storage_sqlite.ITEMS_TABLE.c.request_artifact_path,
                    storage_sqlite.ITEMS_TABLE.c.request_artifact_line,
                    storage_sqlite.ITEMS_TABLE.c.request_sha256,
                    storage_sqlite.ITEMS_TABLE.c.status,
                ).where(storage_sqlite.ITEMS_TABLE.c.run_id == started.run_id)
            )
            .mappings()
            .one()
        )
    assert row["prompt"] == ""
    assert row["request_artifact_path"] is not None
    assert row["request_artifact_line"] == 1
    assert row["request_sha256"] is not None
    assert row["status"] == ItemStatus.PENDING

    second_provider = _ArtifactOnlyBatchProvider(
        record_factory=lambda custom_id: _success_record(json.dumps({"label": custom_id.split(":")[0], "score": 0.9}))
    )
    resumed = BatchRunner(
        storage=SQLiteStorage(path=storage.path),
        provider_factory=lambda _cfg: second_provider,
    ).get_run(started.run_id)
    resumed.wait(poll_interval=0)
    result = resumed.results()[0]
    assert result.status is ItemStatus.COMPLETED
    assert result.output is not None
    assert result.output.label == "row1"


def test_completed_run_can_prune_persisted_request_artifacts(tmp_path: Path) -> None:
    provider = _FakeBatchProvider(
        record_factory=lambda custom_id: _success_record(json.dumps({"label": custom_id.split(":")[0], "score": 0.9}))
    )
    storage = SQLiteStorage(path=tmp_path / "artifact_prune.sqlite3")
    runner = BatchRunner(
        storage=storage,
        provider_factory=lambda _cfg: provider,
    )
    run = runner.run_and_wait(
        BatchJob(
            items=[BatchItem(item_id="row1", payload={"text": "hello"})],
            build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
            structured_output=ClassificationResult,
            provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
        )
    )

    artifact_paths = storage.get_request_artifact_paths(run_id=run.run_id)
    assert len(artifact_paths) == 1
    assert artifact_paths[0].startswith(f"{run.run_id}/requests/")
    artifact_path = runner.temp_root / artifact_paths[0]
    assert artifact_path.exists()

    report = run.prune_artifacts()
    assert report.run_id == run.run_id
    assert report.removed_artifact_paths == artifact_paths
    assert report.missing_artifact_paths == []
    assert report.cleared_item_pointers == 1
    assert artifact_path.exists() is False
    assert storage.get_request_artifact_paths(run_id=run.run_id) == []
    assert run.results()[0].output is not None
    assert run.prune_artifacts().cleared_item_pointers == 0


def test_completed_run_exports_raw_artifacts_and_allows_pruning_after_export(
    tmp_path: Path,
) -> None:
    provider = _FakeBatchProvider(
        record_factory=lambda custom_id: _success_record(json.dumps({"label": custom_id.split(":")[0], "score": 0.9}))
    )
    storage = SQLiteStorage(path=tmp_path / "artifact_export.sqlite3")
    runner = BatchRunner(
        storage=storage,
        provider_factory=lambda _cfg: provider,
    )
    run = runner.run_and_wait(
        BatchJob(
            items=[BatchItem(item_id="row1", payload={"text": "hello"})],
            build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
            structured_output=ClassificationResult,
            provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
        )
    )

    inventory = storage.get_artifact_inventory(run_id=run.run_id)
    assert len(inventory.request_artifact_paths) == 1
    assert len(inventory.output_artifact_paths) == 1
    assert inventory.error_artifact_paths == []
    raw_output_path = runner.temp_root / inventory.output_artifact_paths[0]
    assert raw_output_path.exists()

    with pytest.raises(ValueError, match="require export before pruning"):
        run.prune_artifacts(include_raw_output_artifacts=True)

    export = run.export_artifacts(str(tmp_path / "exports"))
    assert Path(export.manifest_path).exists()
    assert Path(export.results_path).exists()
    manifest = json.loads(Path(export.manifest_path).read_text(encoding="utf-8"))
    assert manifest["run_id"] == run.run_id
    assert manifest["output_artifact_paths"] == inventory.output_artifact_paths
    exported_output = Path(export.destination_dir) / inventory.output_artifact_paths[0]
    assert exported_output.exists()

    report = run.prune_artifacts(include_raw_output_artifacts=True)
    assert inventory.output_artifact_paths[0] in report.removed_artifact_paths
    assert report.cleared_batch_pointers == 1
    assert raw_output_path.exists() is False
    updated_inventory = storage.get_artifact_inventory(run_id=run.run_id)
    assert updated_inventory.output_artifact_paths == []
    assert run.results()[0].output is not None


def test_prune_artifacts_requires_a_terminal_run(tmp_path: Path) -> None:
    provider = _FakeBatchProvider(
        record_factory=lambda custom_id: _success_record(json.dumps({"label": custom_id.split(":")[0], "score": 0.9}))
    )
    storage = SQLiteStorage(path=tmp_path / "artifact_guard.sqlite3")
    runner = BatchRunner(
        storage=storage,
        provider_factory=lambda _cfg: provider,
    )
    run = runner.start(
        BatchJob(
            items=[BatchItem(item_id="row1", payload={"text": "hello"})],
            build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
            structured_output=ClassificationResult,
            provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
        )
    )

    artifact_path = runner.temp_root / storage.get_request_artifact_paths(run_id=run.run_id)[0]
    assert artifact_path.exists()

    with pytest.raises(RunNotFinishedError):
        run.prune_artifacts()

    assert artifact_path.exists()


def test_auto_splits_large_input_into_multiple_batches(tmp_path: Path) -> None:
    provider = _FakeBatchProvider(
        record_factory=lambda custom_id: _success_record(json.dumps({"label": custom_id.split(":")[0], "score": 0.9}))
    )
    runner = BatchRunner(
        storage="memory",
        provider_factory=lambda _cfg: provider,
        temp_root=tmp_path,
    )
    run = runner.run_and_wait(
        BatchJob(
            items=[
                BatchItem(item_id="row1", payload={"text": "aaa"}),
                BatchItem(item_id="row2", payload={"text": "bbb"}),
                BatchItem(item_id="row3", payload={"text": "cc"}),
            ],
            build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
            structured_output=ClassificationResult,
            provider_config=OpenAIProviderConfig(
                api_key="k",
                model="gpt-4.1",
                enqueue_limits=OpenAIEnqueueLimitConfig(max_batch_enqueued_tokens=5),
            ),
        )
    )
    assert len(provider.created_batches) == 2
    assert [result.status for result in run.results()] == [
        ItemStatus.COMPLETED,
        ItemStatus.COMPLETED,
        ItemStatus.COMPLETED,
    ]


def test_inflight_limit_defers_later_submissions_without_consuming_attempts(tmp_path: Path) -> None:
    provider = _FakeBatchProvider(
        record_factory=lambda custom_id: _success_record(json.dumps({"label": custom_id.split(":")[0], "score": 0.9}))
    )
    runner = BatchRunner(
        storage="memory",
        provider_factory=lambda _cfg: provider,
        temp_root=tmp_path,
    )
    run = runner.run_and_wait(
        BatchJob(
            items=[
                BatchItem(item_id="row1", payload={"text": "aaa"}),
                BatchItem(item_id="row2", payload={"text": "bbb"}),
                BatchItem(item_id="row3", payload={"text": "cc"}),
            ],
            build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
            structured_output=ClassificationResult,
            provider_config=OpenAIProviderConfig(
                api_key="k",
                model="gpt-4.1",
                enqueue_limits=OpenAIEnqueueLimitConfig(
                    enqueued_token_limit=5,
                    target_ratio=1.0,
                    max_batch_enqueued_tokens=5,
                ),
            ),
        )
    )
    assert len(provider.created_batches) == 2
    assert [result.attempt_count for result in run.results()] == [0, 0, 0]


def test_oversized_request_becomes_permanent_failure_instead_of_aborting_run(tmp_path: Path) -> None:
    provider = _FakeBatchProvider(
        record_factory=lambda custom_id: _success_record(json.dumps({"label": custom_id.split(":")[0], "score": 0.9}))
    )
    runner = BatchRunner(
        storage="memory",
        provider_factory=lambda _cfg: provider,
        temp_root=tmp_path,
    )
    run = runner.run_and_wait(
        BatchJob(
            items=[
                BatchItem(item_id="row1", payload={"text": "abcdef"}),
                BatchItem(item_id="row2", payload={"text": "ok"}),
            ],
            build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
            structured_output=ClassificationResult,
            provider_config=OpenAIProviderConfig(
                api_key="k",
                model="gpt-4.1",
                enqueue_limits=OpenAIEnqueueLimitConfig(max_batch_enqueued_tokens=5),
            ),
        )
    )
    assert run.status is RunLifecycleStatus.COMPLETED_WITH_FAILURES
    results = run.results()
    assert results[0].status is ItemStatus.FAILED_PERMANENT
    assert results[0].error is not None
    assert results[0].error.error_class == "openai_request_exceeds_batch_token_limit"
    assert results[0].attempt_count == 0
    assert results[1].status is ItemStatus.COMPLETED
    assert len(provider.created_batches) == 1


def test_completed_with_failures_run_can_export_and_prune_artifacts(tmp_path: Path) -> None:
    provider = _FakeBatchProvider(
        record_factory=lambda custom_id: _success_record(json.dumps({"label": custom_id.split(":")[0], "score": 0.9}))
    )
    storage = SQLiteStorage(path=tmp_path / "artifact_export_with_failures.sqlite3")
    runner = BatchRunner(
        storage=storage,
        provider_factory=lambda _cfg: provider,
    )
    run = runner.run_and_wait(
        BatchJob(
            items=[
                BatchItem(item_id="row1", payload={"text": "abcdef"}),
                BatchItem(item_id="row2", payload={"text": "ok"}),
            ],
            build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
            structured_output=ClassificationResult,
            provider_config=OpenAIProviderConfig(
                api_key="k",
                model="gpt-4.1",
                enqueue_limits=OpenAIEnqueueLimitConfig(max_batch_enqueued_tokens=5),
            ),
        )
    )

    assert run.status is RunLifecycleStatus.COMPLETED_WITH_FAILURES
    inventory = storage.get_artifact_inventory(run_id=run.run_id)
    assert len(inventory.request_artifact_paths) == 1
    assert len(inventory.output_artifact_paths) == 1
    assert inventory.error_artifact_paths == []

    export = run.export_artifacts(str(tmp_path / "exports"))
    assert Path(export.manifest_path).exists()
    assert Path(export.results_path).exists()
    manifest = json.loads(Path(export.manifest_path).read_text(encoding="utf-8"))
    assert manifest["run_id"] == run.run_id
    assert manifest["output_artifact_paths"] == inventory.output_artifact_paths

    report = run.prune_artifacts(include_raw_output_artifacts=True)
    assert sorted(report.removed_artifact_paths) == sorted(
        inventory.request_artifact_paths + inventory.output_artifact_paths
    )
    assert report.missing_artifact_paths == []
    assert report.cleared_item_pointers == 1
    assert report.cleared_batch_pointers == 1
    updated_inventory = storage.get_artifact_inventory(run_id=run.run_id)
    assert updated_inventory.request_artifact_paths == []
    assert updated_inventory.output_artifact_paths == []
    assert run.results()[0].status is ItemStatus.FAILED_PERMANENT
    assert run.results()[1].status is ItemStatus.COMPLETED


def test_all_items_succeed_gives_completed_status(tmp_path: Path) -> None:
    provider = _FakeBatchProvider(
        record_factory=lambda custom_id: _success_record(json.dumps({"label": custom_id.split(":")[0], "score": 0.9}))
    )
    runner = BatchRunner(
        storage="memory",
        provider_factory=lambda _cfg: provider,
        temp_root=tmp_path,
    )
    run = runner.run_and_wait(
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
    assert run.status is RunLifecycleStatus.COMPLETED
    assert run.is_finished is True


def test_file_backed_jsonl_job_matches_in_memory_results(tmp_path: Path) -> None:
    file_path = tmp_path / "items.jsonl"
    file_path.write_text(
        "\n".join(
            [
                json.dumps({"id": "row1", "text": "hello"}),
                json.dumps({"id": "row2", "text": "world"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    provider = _FakeBatchProvider(
        record_factory=lambda custom_id: _success_record(json.dumps({"label": custom_id.split(":")[0], "score": 0.9}))
    )
    source = JsonlItemSource(
        file_path,
        item_id_from_row=lambda row: str(row["id"]) if isinstance(row, dict) else "",
        payload_from_row=lambda row: {"text": row["text"]} if isinstance(row, dict) else {},
    )
    runner = BatchRunner(
        storage="memory",
        provider_factory=lambda _cfg: provider,
        temp_root=tmp_path / "source",
    )
    run = runner.run_and_wait(
        BatchJob(
            items=source,
            build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
            structured_output=ClassificationResult,
            provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
        )
    )
    assert [result.item_id for result in run.results()] == ["row1", "row2"]
    assert [result.output.label if result.output is not None else None for result in run.results()] == [
        "row1",
        "row2",
    ]


def test_composite_item_source_namespaces_duplicate_ids_across_sources(
    tmp_path: Path,
) -> None:
    first_path = tmp_path / "items-a.jsonl"
    second_path = tmp_path / "items-b.jsonl"
    first_path.write_text(
        "\n".join(
            [
                json.dumps({"id": "row1", "text": "alpha"}),
                json.dumps({"id": "row2", "text": "beta"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    second_path.write_text(
        "\n".join(
            [
                json.dumps({"id": "row1", "text": "gamma"}),
                json.dumps({"id": "row2", "text": "delta"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    source = CompositeItemSource(
        [
            JsonlItemSource(
                first_path,
                item_id_from_row=lambda row: str(row["id"]) if isinstance(row, dict) else "",
                payload_from_row=lambda row: {"text": row["text"]} if isinstance(row, dict) else {},
            ),
            JsonlItemSource(
                second_path,
                item_id_from_row=lambda row: str(row["id"]) if isinstance(row, dict) else "",
                payload_from_row=lambda row: {"text": row["text"]} if isinstance(row, dict) else {},
            ),
        ]
    )
    provider = _FakeBatchProvider(
        record_factory=lambda custom_id: _success_record(json.dumps({"label": custom_id.split(":")[0], "score": 0.9}))
    )
    runner = BatchRunner(
        storage="memory",
        provider_factory=lambda _cfg: provider,
        temp_root=tmp_path / "composite-source",
    )

    run = runner.run_and_wait(
        BatchJob(
            items=source,
            build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
            structured_output=ClassificationResult,
            provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
        )
    )

    results = run.results()
    item_records = runner.state.get_item_records(run_id=run.run_id)
    first_namespace = results[0].item_id.split("__", maxsplit=1)[0]
    second_namespace = results[2].item_id.split("__", maxsplit=1)[0]

    assert [record.item_index for record in item_records] == [0, 1, 2, 3]
    assert [result.item_id for result in results] == [
        f"{first_namespace}__row1",
        f"{first_namespace}__row2",
        f"{second_namespace}__row1",
        f"{second_namespace}__row2",
    ]
    assert first_namespace.startswith("src_")
    assert second_namespace.startswith("src_")
    assert first_namespace != second_namespace
    assert [result.metadata["batchor_lineage"]["source_primary_key"] for result in results] == [
        "row1",
        "row2",
        "row1",
        "row2",
    ]
    assert [result.metadata["batchor_lineage"]["source_ref"] for result in results] == [
        str(first_path.resolve()),
        str(first_path.resolve()),
        str(second_path.resolve()),
        str(second_path.resolve()),
    ]
    assert [result.output.label if result.output is not None else None for result in results] == [
        result.item_id for result in results
    ]


def test_start_with_same_run_id_resumes_incomplete_jsonl_ingestion(tmp_path: Path) -> None:
    path = tmp_path / "items.jsonl"
    records = [{"id": f"row{i}", "text": f"text-{i}"} for i in range(1002)]
    path.write_text(
        "\n".join(json.dumps(record) for record in records) + "\n",
        encoding="utf-8",
    )
    source = JsonlItemSource(
        path,
        item_id_from_row=lambda row: str(row["id"]) if isinstance(row, dict) else "",
        payload_from_row=lambda row: {"text": row["text"]} if isinstance(row, dict) else {},
    )
    storage = SQLiteStorage(path=tmp_path / "resume_ingest.sqlite3")
    run_id = "resume_ingest_run"
    provider = _FakeBatchProvider(
        record_factory=lambda custom_id: _success_record(json.dumps({"label": custom_id.split(":")[0], "score": 0.9}))
    )
    runner = BatchRunner(
        storage=storage,
        provider_factory=lambda _cfg: provider,
    )

    def flaky_prompt_builder(item: BatchItem[dict[str, str]]) -> PromptParts:
        if item.item_id == "row1000":
            raise RuntimeError("simulated ingest crash")
        return PromptParts(prompt=item.payload["text"])

    with pytest.raises(RuntimeError, match="simulated ingest crash"):
        runner.start(
            BatchJob(
                items=source,
                build_prompt=flaky_prompt_builder,
                structured_output=ClassificationResult,
                provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
            ),
            run_id=run_id,
        )

    checkpoint = storage.get_ingest_checkpoint(run_id=run_id)
    assert checkpoint is not None
    assert checkpoint.next_item_index == 1000
    assert checkpoint.ingestion_complete is False

    resumed = runner.start(
        BatchJob(
            items=source,
            build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
            structured_output=ClassificationResult,
            provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
        ),
        run_id=run_id,
    )
    resumed.wait(poll_interval=0)

    checkpoint = storage.get_ingest_checkpoint(run_id=run_id)
    assert checkpoint is not None
    assert checkpoint.next_item_index == 1002
    assert checkpoint.ingestion_complete is True
    assert len(resumed.results()) == 1002
    assert resumed.results()[0].item_id == "row0"
    assert resumed.results()[-1].item_id == "row1001"


def test_start_with_same_run_id_resumes_incomplete_composite_ingestion(
    tmp_path: Path,
) -> None:
    first_path = tmp_path / "items-a.jsonl"
    second_path = tmp_path / "items-b.jsonl"
    first_records = [{"id": f"row{i}", "text": f"first-{i}"} for i in range(999)]
    second_records = [{"id": f"row{i}", "text": f"second-{i}"} for i in range(3)]
    first_path.write_text(
        "\n".join(json.dumps(record) for record in first_records) + "\n",
        encoding="utf-8",
    )
    second_path.write_text(
        "\n".join(json.dumps(record) for record in second_records) + "\n",
        encoding="utf-8",
    )

    def build_source() -> CompositeItemSource[dict[str, str]]:
        return CompositeItemSource(
            [
                JsonlItemSource(
                    first_path,
                    item_id_from_row=lambda row: str(row["id"]) if isinstance(row, dict) else "",
                    payload_from_row=lambda row: {"text": row["text"]} if isinstance(row, dict) else {},
                ),
                JsonlItemSource(
                    second_path,
                    item_id_from_row=lambda row: str(row["id"]) if isinstance(row, dict) else "",
                    payload_from_row=lambda row: {"text": row["text"]} if isinstance(row, dict) else {},
                ),
            ]
        )

    storage = SQLiteStorage(path=tmp_path / "resume_composite.sqlite3")
    run_id = "resume_composite_run"
    provider = _FakeBatchProvider(
        record_factory=lambda custom_id: _success_record(json.dumps({"label": custom_id.split(":")[0], "score": 0.9}))
    )
    runner = BatchRunner(
        storage=storage,
        provider_factory=lambda _cfg: provider,
    )

    def flaky_prompt_builder(item: BatchItem[dict[str, str]]) -> PromptParts:
        if item.payload["text"] == "second-1":
            raise RuntimeError("simulated composite ingest crash")
        return PromptParts(prompt=item.payload["text"])

    with pytest.raises(RuntimeError, match="simulated composite ingest crash"):
        runner.start(
            BatchJob(
                items=build_source(),
                build_prompt=flaky_prompt_builder,
                structured_output=ClassificationResult,
                provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
            ),
            run_id=run_id,
        )

    checkpoint = storage.get_ingest_checkpoint(run_id=run_id)
    assert checkpoint is not None
    assert checkpoint.next_item_index == 1000
    assert checkpoint.checkpoint_payload == {
        "source_index": 1,
        "child_checkpoint": 1,
    }
    assert checkpoint.ingestion_complete is False

    resumed = runner.start(
        BatchJob(
            items=build_source(),
            build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
            structured_output=ClassificationResult,
            provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
        ),
        run_id=run_id,
    )
    resumed.wait(poll_interval=0)

    checkpoint = storage.get_ingest_checkpoint(run_id=run_id)
    assert checkpoint is not None
    assert checkpoint.next_item_index == 1002
    assert checkpoint.checkpoint_payload == {
        "source_index": 2,
        "child_checkpoint": None,
    }
    assert checkpoint.ingestion_complete is True

    item_records = storage.get_item_records(run_id=run_id)
    last_three = item_records[-3:]
    assert [record.item_index for record in last_three] == [999, 1000, 1001]
    assert [record.metadata["batchor_lineage"]["source_ref"] for record in last_three] == [
        str(second_path.resolve())
    ] * 3
    assert [record.metadata["batchor_lineage"]["source_primary_key"] for record in last_three] == [
        "row0",
        "row1",
        "row2",
    ]

    results = resumed.results()
    second_namespace = results[-1].item_id.split("__", maxsplit=1)[0]
    assert [result.item_id for result in results[-3:]] == [
        f"{second_namespace}__row0",
        f"{second_namespace}__row1",
        f"{second_namespace}__row2",
    ]
