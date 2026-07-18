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
    RunControlState,
    SQLiteStorage,
)
from batchor.providers.openai import OpenAIBatchProvider
from batchor.runtime.context import RunContext, build_persisted_config, build_run_context
from batchor.runtime.polling import PollingDeps, consume_completed_batch, poll_once, refresh_run
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
        download_exception: Exception | None = None,
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
        self.download_exception = download_exception
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
        if self.download_exception is not None:
            exc = self.download_exception
            self.download_exception = None
            raise exc
        if file_id.startswith("output_"):
            return self.output_content
        return self.error_content

    def parse_batch_output(self, *, output_content: str | None, error_content: str | None):
        return self._parser.parse_batch_output(
            output_content=output_content,
            error_content=error_content,
        )

    def extract_response_text(self, response_record):  # noqa: ANN001
        return self._parser.extract_response_text(response_record)


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


def test_poll_once_auto_pauses_on_insufficient_quota_poll_error(tmp_path: Path) -> None:
    provider = _FakePollingProvider(
        poll_exception=RuntimeError(
            {
                "response": {"status_code": 429},
                "error": {"code": "insufficient_quota", "message": "billing quota exhausted"},
            }
        ),
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

    summary = storage.get_run_summary(run_id=run_id)
    assert summary.control_state is RunControlState.PAUSED
    assert summary.control_reason == "openai_insufficient_quota"
    assert storage.get_item_records(run_id=run_id)[0].status is ItemStatus.SUBMITTED
    assert len(storage.get_active_batches(run_id=run_id)) == 1
    assert events[-1][0] == "run_auto_paused"


def test_refresh_preserves_cancel_requested_on_insufficient_quota_poll_error(tmp_path: Path) -> None:
    provider = _FakePollingProvider(
        poll_exception=RuntimeError(
            {
                "response": {"status_code": 429},
                "error": {"code": "insufficient_quota", "message": "billing quota exhausted"},
            }
        ),
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
    storage.set_run_control_state(
        run_id=run_id,
        control_state=RunControlState.CANCEL_REQUESTED,
    )

    summary = refresh_run(
        deps,
        run_id=run_id,
        context=context,
        submit_pending_items=lambda _run_id, _context: 0,
    )

    assert summary.control_state is RunControlState.CANCEL_REQUESTED
    assert summary.control_reason is None
    assert storage.get_item_records(run_id=run_id)[0].status is ItemStatus.SUBMITTED
    assert len(storage.get_active_batches(run_id=run_id)) == 1
    assert "run_auto_paused" not in {event_type for event_type, _kwargs in events}


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


def test_poll_once_auto_pauses_failed_batch_with_insufficient_quota(tmp_path: Path) -> None:
    provider = _FakePollingProvider(
        batch_factory=lambda batch_id: {
            "id": batch_id,
            "status": "failed",
            "errors": {
                "status_code": 429,
                "code": "insufficient_quota",
                "message": "You exceeded your current quota",
            },
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
    summary = storage.get_run_summary(run_id=run_id)
    assert record.status is ItemStatus.PENDING
    assert record.attempt_count == 0
    assert record.error is not None
    assert record.error.error_class == "openai_insufficient_quota"
    assert summary.control_state is RunControlState.PAUSED
    assert summary.control_reason == "openai_insufficient_quota"
    assert summary.backoff_remaining_sec == 0.0


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


def test_terminal_batch_is_replayed_after_download_failure_across_sqlite_reopen(tmp_path: Path) -> None:
    provider = _FakePollingProvider(
        output_content=json.dumps(_success_record("done") | {"custom_id": "row1:a1"}) + "\n",
        download_exception=ConnectionError("download interrupted"),
    )
    storage, artifact_store, deps, context, run_id = _polling_setup(tmp_path, provider=provider)
    sqlite_path = tmp_path / "durable.sqlite3"
    durable = SQLiteStorage(path=sqlite_path)
    config = storage.get_run_config(run_id=run_id)
    durable.create_run(
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
    durable.register_batch(
        run_id=run_id,
        local_batch_id="local_batch_0",
        provider_batch_id="batch_0",
        status="submitted",
        custom_ids=["row1:a1"],
    )
    durable.mark_items_submitted(
        run_id=run_id,
        provider_batch_id="batch_0",
        submissions=[PreparedSubmission(item_id="row1", custom_id="row1:a1", submission_tokens=1)],
    )
    durable_deps = PollingDeps(state=durable, artifact_store=artifact_store, emit_event=lambda *_args, **_kwargs: None)

    try:
        poll_once(durable_deps, run_id=run_id, context=context)
    except ConnectionError:
        pass
    else:  # pragma: no cover - makes the crash window assertion explicit
        raise AssertionError("expected the initial result download to fail")

    # The remote terminal status was committed before the failure, but the
    # still-submitted item keeps the batch locally active and therefore
    # replayable by a fresh storage instance.
    assert durable.get_item_records(run_id=run_id)[0].status is ItemStatus.SUBMITTED
    assert len(durable.get_active_batches(run_id=run_id)) == 1
    durable.close()

    reopened = SQLiteStorage(path=sqlite_path)
    replay_deps = PollingDeps(state=reopened, artifact_store=artifact_store, emit_event=lambda *_args, **_kwargs: None)
    poll_once(replay_deps, run_id=run_id, context=context)

    record = reopened.get_item_records(run_id=run_id)[0]
    assert record.status is ItemStatus.COMPLETED
    assert record.attempt_count == 1
    assert reopened.get_active_batches(run_id=run_id) == []


def test_terminal_replay_skips_already_consumed_rows_after_partial_item_mutation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    output = "\n".join(
        [
            json.dumps(_success_record("done") | {"custom_id": "row1:a1"}),
            json.dumps(
                {
                    "custom_id": "row2:a1",
                    "response": {"status_code": 500, "body": {"error": {"message": "retry"}}},
                }
            ),
        ]
    )
    provider = _FakePollingProvider(output_content=output + "\n")
    storage, _artifact_store, deps, context, run_id = _polling_setup(tmp_path, provider=provider)
    storage.append_items(
        run_id=run_id,
        items=[
            MaterializedItem(
                item_id="row2",
                item_index=1,
                payload={"text": "later"},
                metadata={},
                prompt="later",
            )
        ],
    )
    storage.register_batch(
        run_id=run_id,
        local_batch_id="local_batch_0",
        provider_batch_id="batch_0",
        status="submitted",
        custom_ids=["row1:a1", "row2:a1"],
    )
    storage.mark_items_submitted(
        run_id=run_id,
        provider_batch_id="batch_0",
        submissions=[PreparedSubmission(item_id="row2", custom_id="row2:a1", submission_tokens=1)],
    )
    original_mark_failed = storage.mark_items_failed
    monkeypatch.setattr(storage, "mark_items_failed", lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("crash")))
    try:
        poll_once(deps, run_id=run_id, context=context)
    except RuntimeError as exc:
        assert str(exc) == "crash"
    else:  # pragma: no cover
        raise AssertionError("expected partial consumption failure")
    monkeypatch.setattr(storage, "mark_items_failed", original_mark_failed)

    poll_once(deps, run_id=run_id, context=context)

    records = storage.get_item_records(run_id=run_id)
    assert [record.status for record in records] == [ItemStatus.COMPLETED, ItemStatus.FAILED_RETRYABLE]
    assert records[0].attempt_count == 1
    assert records[1].attempt_count == 1


def test_poll_once_records_backoff_for_item_level_insufficient_quota(tmp_path: Path) -> None:
    quota_record = {
        "custom_id": "row1:a1",
        "response": {
            "status_code": 429,
            "body": {
                "error": {
                    "code": "insufficient_quota",
                    "message": "You exceeded your current quota",
                }
            },
        },
    }
    provider = _FakePollingProvider(output_content=json.dumps(quota_record) + "\n")
    storage, _artifact_store, deps, context, run_id = _polling_setup(
        tmp_path,
        provider=provider,
        retry_policy=RetryPolicy(max_attempts=2, base_backoff_sec=1.0, max_backoff_sec=1.0),
    )

    poll_once(deps, run_id=run_id, context=context)

    record = storage.get_item_records(run_id=run_id)[0]
    summary = storage.get_run_summary(run_id=run_id)
    assert record.status is ItemStatus.FAILED_RETRYABLE
    assert record.attempt_count == 0
    assert record.error is not None
    assert record.error.error_class == "openai_insufficient_quota"
    assert summary.control_state is RunControlState.RUNNING
    assert summary.control_reason is None
    assert summary.backoff_remaining_sec > 0


def test_completed_batch_does_not_clear_existing_retry_backoff(tmp_path: Path) -> None:
    provider = _FakePollingProvider(
        output_content=json.dumps(_success_record("done") | {"custom_id": "row1:a1"}) + "\n",
    )
    storage, _artifact_store, deps, context, run_id = _polling_setup(
        tmp_path,
        provider=provider,
        retry_policy=RetryPolicy(max_attempts=2, base_backoff_sec=60.0, max_backoff_sec=60.0),
    )
    storage.record_batch_retry_failure(
        run_id=run_id,
        error_class="batch_create_connection_error",
        base_delay_sec=60.0,
        max_delay_sec=60.0,
    )

    poll_once(deps, run_id=run_id, context=context)

    assert storage.get_item_records(run_id=run_id)[0].status is ItemStatus.COMPLETED
    assert storage.get_run_summary(run_id=run_id).backoff_remaining_sec > 0


def test_cancellation_clears_retry_backoff_after_final_batch_drains(tmp_path: Path) -> None:
    quota_record = {
        "custom_id": "row1:a1",
        "response": {
            "status_code": 429,
            "body": {"error": {"code": "insufficient_quota", "message": "quota exhausted"}},
        },
    }
    provider = _FakePollingProvider(output_content=json.dumps(quota_record) + "\n")
    storage, _artifact_store, deps, context, run_id = _polling_setup(
        tmp_path,
        provider=provider,
        retry_policy=RetryPolicy(max_attempts=2, base_backoff_sec=60.0, max_backoff_sec=60.0),
    )
    storage.set_run_control_state(run_id=run_id, control_state=RunControlState.CANCEL_REQUESTED)

    summary = refresh_run(
        deps,
        run_id=run_id,
        context=context,
        submit_pending_items=lambda _run_id, _context: 0,
    )

    assert summary.status.value == "completed_with_failures"
    assert summary.backoff_remaining_sec == 0.0
    assert storage.get_item_records(run_id=run_id)[0].status is ItemStatus.FAILED_PERMANENT


def test_consume_completed_batch_reports_item_level_quota_without_pausing(tmp_path: Path) -> None:
    quota_record = {
        "custom_id": "row1:a1",
        "response": {
            "status_code": 429,
            "body": {
                "error": {
                    "code": "insufficient_quota",
                    "message": "You exceeded your current quota",
                }
            },
        },
    }
    provider = _FakePollingProvider(output_content=json.dumps(quota_record) + "\n")
    storage, _artifact_store, deps, context, run_id = _polling_setup(
        tmp_path,
        provider=provider,
        retry_policy=RetryPolicy(max_attempts=2, base_backoff_sec=0.0, max_backoff_sec=0.0),
    )

    consume_result = consume_completed_batch(
        deps,
        run_id=run_id,
        context=context,
        provider_batch_id="batch_0",
        output_file_id="output_batch_0",
        error_file_id=None,
    )

    summary = storage.get_run_summary(run_id=run_id)
    assert consume_result.quota_error_count == 1
    assert summary.control_state is RunControlState.RUNNING
    assert summary.control_reason is None
