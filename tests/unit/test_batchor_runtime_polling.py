from __future__ import annotations

import json
from pathlib import Path
from typing import Callable

from pydantic import BaseModel

from batchor import (
    BatchItem,
    BatchJob,
    ItemStatus,
    LocalArtifactStore,
    MemoryStateStore,
    OpenAIProviderConfig,
    PromptParts,
    RetryPolicy,
)
from batchor.providers.openai import OpenAIBatchProvider
from batchor.runtime.context import RunContext, build_persisted_config, build_run_context
from batchor.runtime.polling import PollingDeps, consume_completed_batch, poll_once
from batchor.storage.state import MaterializedItem, PreparedSubmission


class ClassificationResult(BaseModel):
    label: str
    score: float


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


class _FakePollingProvider:
    def __init__(
        self,
        *,
        batch_factory: Callable[[str], dict[str, object]] | None = None,
        output_content: str = "",
        error_content: str = "",
        poll_exception: Exception | None = None,
    ) -> None:
        self.batch_factory = batch_factory or (
            lambda batch_id: {
                "id": batch_id,
                "status": "completed",
                "output_file_id": f"output_{batch_id}",
                "error_file_id": None,
            }
        )
        self.output_content = output_content
        self.error_content = error_content
        self.poll_exception = poll_exception
        self._parser = OpenAIBatchProvider(
            OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
            client=object(),
        )

    def get_batch(self, batch_id: str) -> dict[str, object]:
        if self.poll_exception is not None:
            exc = self.poll_exception
            self.poll_exception = None
            raise exc
        return self.batch_factory(batch_id)

    def download_file_content(self, file_id: str) -> str:
        if file_id.startswith("output_"):
            return self.output_content
        return self.error_content

    def parse_batch_output(self, *, output_content: str | None, error_content: str | None):
        return self._parser.parse_batch_output(
            output_content=output_content,
            error_content=error_content,
        )


def _polling_setup(
    tmp_path: Path,
    *,
    provider: _FakePollingProvider,
    structured_output: type[BaseModel] | None = None,
    retry_policy: RetryPolicy | None = None,
) -> tuple[MemoryStateStore, LocalArtifactStore, PollingDeps, RunContext, str]:
    storage = MemoryStateStore()
    artifact_store = LocalArtifactStore(tmp_path / "artifacts")
    events: list[tuple[str, dict[str, object]]] = []
    deps = PollingDeps(
        state=storage,
        artifact_store=artifact_store,
        emit_event=lambda event_type, **kwargs: events.append((event_type, kwargs)),
    )
    job = BatchJob(
        items=[BatchItem(item_id="row1", payload={"text": "hello"})],
        build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
        structured_output=structured_output,
        provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
        retry_policy=retry_policy or RetryPolicy(max_attempts=2, base_backoff_sec=1.0, max_backoff_sec=1.0),
    )
    config = build_persisted_config(job)
    context = build_run_context(
        config=config,
        output_model=structured_output,
        create_provider=lambda _cfg: provider,
    )
    run_id = "polling_run"
    storage.create_run(
        run_id=run_id,
        config=config,
        items=[
            MaterializedItem(
                item_id="row1",
                item_index=0,
                payload={"text": "hello"},
                metadata={},
                prompt="hello",
            )
        ],
    )
    storage.register_batch(
        run_id=run_id,
        local_batch_id="local_batch_0",
        provider_batch_id="batch_0",
        status="submitted",
        custom_ids=["row1:a1"],
    )
    storage.mark_items_submitted(
        run_id=run_id,
        provider_batch_id="batch_0",
        submissions=[
            PreparedSubmission(
                item_id="row1",
                custom_id="row1:a1",
                submission_tokens=1,
            )
        ],
    )
    return storage, artifact_store, deps, context, run_id


def test_poll_once_emits_retry_event_for_transient_poll_errors(tmp_path: Path) -> None:
    provider = _FakePollingProvider(
        poll_exception=ConnectionError("connection reset by peer"),
    )
    storage, _artifact_store, deps, context, run_id = _polling_setup(
        tmp_path,
        provider=provider,
    )
    events: list[tuple[str, dict[str, object]]] = []
    deps = PollingDeps(
        state=storage,
        artifact_store=deps.artifact_store,
        emit_event=lambda event_type, **kwargs: events.append((event_type, kwargs)),
    )

    poll_once(deps, run_id=run_id, context=context)

    assert events[0][0] == "batch_poll_retry"
    assert storage.get_item_records(run_id=run_id)[0].status is ItemStatus.SUBMITTED


def test_poll_once_resets_failed_batches_to_pending_and_records_backoff(tmp_path: Path) -> None:
    provider = _FakePollingProvider(
        batch_factory=lambda batch_id: {
            "id": batch_id,
            "status": "failed",
            "errors": {"message": "temporary service unavailable"},
            "output_file_id": None,
            "error_file_id": None,
        }
    )
    storage, _artifact_store, deps, context, run_id = _polling_setup(
        tmp_path,
        provider=provider,
    )

    poll_once(deps, run_id=run_id, context=context)

    record = storage.get_item_records(run_id=run_id)[0]
    assert record.status is ItemStatus.PENDING
    assert storage.get_run_summary(run_id=run_id).backoff_remaining_sec > 0


def test_consume_completed_batch_marks_structured_validation_failures(tmp_path: Path) -> None:
    provider = _FakePollingProvider(
        output_content=json.dumps(_success_record("{not json}") | {"custom_id": "row1:a1"}) + "\n",
    )
    storage, _artifact_store, deps, context, run_id = _polling_setup(
        tmp_path,
        provider=provider,
        structured_output=ClassificationResult,
        retry_policy=RetryPolicy(max_attempts=1, base_backoff_sec=0.0, max_backoff_sec=0.0),
    )

    consume_completed_batch(
        deps,
        run_id=run_id,
        context=context,
        provider_batch_id="batch_0",
        output_file_id="output_batch_0",
        error_file_id=None,
    )

    record = storage.get_item_records(run_id=run_id)[0]
    assert record.status is ItemStatus.FAILED_PERMANENT
    assert record.error is not None
    assert record.error.error_class == "invalid_json"


def test_consume_completed_batch_retries_missing_output_rows_without_consuming_attempts(tmp_path: Path) -> None:
    provider = _FakePollingProvider(output_content="")
    storage, _artifact_store, deps, context, run_id = _polling_setup(
        tmp_path,
        provider=provider,
        retry_policy=RetryPolicy(max_attempts=2, base_backoff_sec=0.0, max_backoff_sec=0.0),
    )

    consume_completed_batch(
        deps,
        run_id=run_id,
        context=context,
        provider_batch_id="batch_0",
        output_file_id="output_batch_0",
        error_file_id=None,
    )

    record = storage.get_item_records(run_id=run_id)[0]
    assert record.status is ItemStatus.FAILED_RETRYABLE
    assert record.attempt_count == 0
    assert record.error is not None
    assert record.error.error_class == "batch_output_missing_row"
