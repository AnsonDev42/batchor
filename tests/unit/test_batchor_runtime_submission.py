from __future__ import annotations

import json
from pathlib import Path

from batchor import (
    BatchItem,
    BatchJob,
    ChunkPolicy,
    ItemStatus,
    LocalArtifactStore,
    MemoryStateStore,
    OpenAIEnqueueLimitConfig,
    OpenAIProviderConfig,
    PromptParts,
    RetryPolicy,
)
from batchor.runtime.artifacts import request_sha256
from batchor.runtime.context import build_persisted_config, build_run_context
from batchor.runtime.submission import SubmissionDeps, submit_pending_items
from batchor.storage.state import MaterializedItem, RequestArtifactPointer


class _FakeSubmissionProvider:
    def __init__(
        self,
        *,
        assert_no_build: bool = False,
        create_failures: list[Exception] | None = None,
    ) -> None:
        self.assert_no_build = assert_no_build
        self.create_failures = list(create_failures or [])
        self.created_batches: list[str] = []
        self.deleted_files: list[str] = []
        self._next_file = 0
        self._next_batch = 0

    def build_request_line(
        self,
        *,
        custom_id: str,
        prompt_parts: PromptParts,
        structured_output=None,  # noqa: ANN001
    ) -> dict[str, object]:
        if self.assert_no_build:
            raise AssertionError("request line should be replayed from a persisted artifact")
        del structured_output
        body: dict[str, object] = {"input": prompt_parts.prompt}
        if prompt_parts.system_prompt:
            body["instructions"] = prompt_parts.system_prompt
        return {
            "custom_id": custom_id,
            "method": "POST",
            "url": "/v1/responses",
            "body": body,
        }

    def upload_input_file(self, input_path: Path) -> str:
        input_path.read_text(encoding="utf-8")
        file_id = f"file_{self._next_file}"
        self._next_file += 1
        return file_id

    def create_batch(self, *, input_file_id: str, metadata=None):  # noqa: ANN001
        del input_file_id, metadata
        if self.create_failures:
            raise self.create_failures.pop(0)
        batch_id = f"batch_{self._next_batch}"
        self._next_batch += 1
        self.created_batches.append(batch_id)
        return {"id": batch_id, "status": "submitted"}

    def delete_input_file(self, file_id: str) -> None:
        self.deleted_files.append(file_id)

    def estimate_request_tokens(
        self,
        request_line: dict[str, object],
        *,
        chars_per_token: int,
    ) -> int:
        del chars_per_token
        body = request_line["body"]
        assert isinstance(body, dict)
        prompt = body.get("input", "")
        assert isinstance(prompt, str)
        return max(len(prompt), 1)


def _materialized_text_item(item_id: str, item_index: int, text: str) -> MaterializedItem:
    return MaterializedItem(
        item_id=item_id,
        item_index=item_index,
        payload={"text": text},
        metadata={},
        prompt=text,
    )


def test_submit_pending_items_replays_shared_request_artifact_once_per_cycle(tmp_path: Path) -> None:
    provider = _FakeSubmissionProvider(assert_no_build=True)
    storage = MemoryStateStore()
    artifact_store = LocalArtifactStore(tmp_path / "artifacts")
    deps = SubmissionDeps(
        state=storage,
        artifact_store=artifact_store,
        emit_event=lambda *args, **kwargs: None,
    )
    job = BatchJob(
        items=[
            BatchItem(item_id="row1", payload={"text": "a"}),
            BatchItem(item_id="row2", payload={"text": "b"}),
        ],
        build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
        provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
    )
    config = build_persisted_config(job)
    context = build_run_context(
        config=config,
        output_model=None,
        create_provider=lambda _cfg: provider,
    )
    run_id = "artifact_cache_run"
    storage.create_run(
        run_id=run_id,
        config=config,
        items=[
            _materialized_text_item("row1", 0, "a"),
            _materialized_text_item("row2", 1, "b"),
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
    artifact_store.write_text(
        artifact_path,
        "".join(json.dumps(line) + "\n" for line in request_lines),
        encoding="utf-8",
    )
    storage.record_request_artifacts(
        run_id=run_id,
        pointers=[
            RequestArtifactPointer(
                item_id="row1",
                artifact_path=artifact_path,
                line_number=1,
                request_sha256=request_sha256(request_lines[0]),
            ),
            RequestArtifactPointer(
                item_id="row2",
                artifact_path=artifact_path,
                line_number=2,
                request_sha256=request_sha256(request_lines[1]),
            ),
        ],
    )

    read_count = 0
    original_read_text = artifact_store.read_text

    def counting_read_text(key: str, *, encoding: str = "utf-8") -> str:
        nonlocal read_count
        read_count += 1
        return original_read_text(key, encoding=encoding)

    artifact_store.read_text = counting_read_text  # type: ignore[method-assign]

    submitted = submit_pending_items(deps, run_id=run_id, context=context)

    assert submitted == 2
    assert read_count == 1
    assert provider.created_batches == ["batch_0"]


def test_submit_pending_items_marks_oversized_rows_and_releases_unsent_items(tmp_path: Path) -> None:
    provider = _FakeSubmissionProvider()
    storage = MemoryStateStore()
    artifact_store = LocalArtifactStore(tmp_path / "artifacts")
    deps = SubmissionDeps(
        state=storage,
        artifact_store=artifact_store,
        emit_event=lambda *args, **kwargs: None,
    )
    job = BatchJob(
        items=[
            BatchItem(item_id="row1", payload={"text": "abcdef"}),
            BatchItem(item_id="row2", payload={"text": "bbb"}),
            BatchItem(item_id="row3", payload={"text": "cc"}),
        ],
        build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
        provider_config=OpenAIProviderConfig(
            api_key="k",
            model="gpt-4.1",
            enqueue_limits=OpenAIEnqueueLimitConfig(
                enqueued_token_limit=4,
                target_ratio=1.0,
                headroom=0,
                max_batch_enqueued_tokens=4,
            ),
        ),
        chunk_policy=ChunkPolicy(max_requests=10, max_file_bytes=1024),
    )
    config = build_persisted_config(job)
    context = build_run_context(
        config=config,
        output_model=None,
        create_provider=lambda _cfg: provider,
    )
    run_id = "oversized_and_unsent"
    storage.create_run(
        run_id=run_id,
        config=config,
        items=[
            _materialized_text_item("row1", 0, "abcdef"),
            _materialized_text_item("row2", 1, "bbb"),
            _materialized_text_item("row3", 2, "cc"),
        ],
    )

    submitted = submit_pending_items(deps, run_id=run_id, context=context)

    records = storage.get_item_records(run_id=run_id)
    assert submitted == 1
    assert [record.status for record in records] == [
        ItemStatus.FAILED_PERMANENT,
        ItemStatus.SUBMITTED,
        ItemStatus.PENDING,
    ]
    assert records[0].error is not None
    assert records[0].error.error_class == "openai_request_exceeds_batch_token_limit"
    assert provider.created_batches == ["batch_0"]


def test_submit_pending_items_cleans_up_uploaded_input_on_retryable_create_failure(tmp_path: Path) -> None:
    provider = _FakeSubmissionProvider(
        create_failures=[RuntimeError("temporary service unavailable")],
    )
    storage = MemoryStateStore()
    artifact_store = LocalArtifactStore(tmp_path / "artifacts")
    deps = SubmissionDeps(
        state=storage,
        artifact_store=artifact_store,
        emit_event=lambda *args, **kwargs: None,
    )
    job = BatchJob(
        items=[BatchItem(item_id="row1", payload={"text": "hello"})],
        build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
        provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
        retry_policy=RetryPolicy(max_attempts=2, base_backoff_sec=1.0, max_backoff_sec=1.0),
    )
    config = build_persisted_config(job)
    context = build_run_context(
        config=config,
        output_model=None,
        create_provider=lambda _cfg: provider,
    )
    run_id = "create_failure_cleanup"
    storage.create_run(
        run_id=run_id,
        config=config,
        items=[_materialized_text_item("row1", 0, "hello")],
    )

    submitted = submit_pending_items(deps, run_id=run_id, context=context)

    record = storage.get_item_records(run_id=run_id)[0]
    assert submitted == 0
    assert record.status is ItemStatus.PENDING
    assert provider.deleted_files == ["file_0"]
    assert storage.get_run_summary(run_id=run_id).backoff_remaining_sec > 0
