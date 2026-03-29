from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from batchor import (
    BatchRunner,
    ItemStatus,
    ModelResolutionError,
    OpenAIProviderConfig,
    ProviderKind,
    RunLifecycleStatus,
)
from batchor.models import ChunkPolicy, InflightPolicy, ItemFailure, RetryPolicy
from batchor.sqlite_storage import SQLiteStorage
from batchor.state import (
    CompletedItemRecord,
    ItemFailureRecord,
    MaterializedItem,
    PersistedRunConfig,
    PreparedSubmission,
)


class _FrozenClock:
    def __init__(self) -> None:
        self.current = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def now(self) -> datetime:
        return self.current


def _config(*, structured: bool = False) -> PersistedRunConfig:
    return PersistedRunConfig(
        provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
        chunk_policy=ChunkPolicy(),
        retry_policy=RetryPolicy(max_attempts=2, base_backoff_sec=1.0, max_backoff_sec=5.0),
        inflight_policy=InflightPolicy(),
        batch_metadata={"source": "test"},
        schema_name="classification_result" if structured else None,
        structured_output_module="missing.module" if structured else None,
        structured_output_qualname="MissingModel" if structured else None,
    )


def _items() -> list[MaterializedItem]:
    return [
        MaterializedItem(
            item_id="row1",
            item_index=0,
            payload={"text": "one"},
            metadata={"source": "a"},
            prompt="prompt one",
        ),
        MaterializedItem(
            item_id="row2",
            item_index=1,
            payload={"text": "two"},
            metadata={"source": "b"},
            prompt="prompt two",
            system_prompt="system two",
        ),
    ]


def test_sqlite_storage_submission_and_terminal_summary(tmp_path: Path) -> None:
    storage = SQLiteStorage(path=tmp_path / "batchor.sqlite3")
    storage.create_run(run_id="run_1", config=_config(), items=_items())

    initial = storage.get_run_summary(run_id="run_1")
    assert initial.status is RunLifecycleStatus.RUNNING
    assert initial.status_counts == {ItemStatus.PENDING: 2}

    claimed_first = storage.claim_items_for_submission(run_id="run_1", max_attempts=2, limit=1)
    assert [item.item_id for item in claimed_first] == ["row1"]
    storage.release_items_to_pending(run_id="run_1", item_ids=["row1"])

    claimed = storage.claim_items_for_submission(run_id="run_1", max_attempts=2)
    assert [item.item_id for item in claimed] == ["row1", "row2"]
    assert claimed[1].system_prompt == "system two"

    storage.register_batch(
        run_id="run_1",
        local_batch_id="local_1",
        provider_batch_id="provider_1",
        status="submitted",
        custom_ids=["row1:a1", "row2:a1"],
    )
    storage.mark_items_submitted(
        run_id="run_1",
        provider_batch_id="provider_1",
        submissions=[
            PreparedSubmission(item_id="row1", custom_id="row1:a1", submission_tokens=10),
            PreparedSubmission(item_id="row2", custom_id="row2:a1", submission_tokens=20),
        ],
    )
    assert storage.get_active_submitted_token_estimate(run_id="run_1") == 30
    assert storage.get_submitted_custom_ids_for_batch(run_id="run_1", provider_batch_id="provider_1") == [
        "row1:a1",
        "row2:a1",
    ]
    assert len(storage.get_active_batches(run_id="run_1")) == 1

    storage.mark_items_completed(
        run_id="run_1",
        completions=[
            CompletedItemRecord(
                custom_id="row1:a1",
                output_text='{"label":"ai","score":0.9}',
                output_json={"label": "ai", "score": 0.9},
                raw_response={"response": {"status_code": 200}},
            )
        ],
    )
    storage.mark_items_failed(
        run_id="run_1",
        failures=[
            ItemFailureRecord(
                custom_id="row2:a1",
                error=ItemFailure(
                    error_class="invalid_json",
                    message="bad json",
                    retryable=True,
                    raw_error={"raw_text": "{oops}"},
                ),
                count_attempt=True,
            )
        ],
        max_attempts=1,
    )
    storage.update_batch_status(
        run_id="run_1",
        provider_batch_id="provider_1",
        status="completed",
        output_file_id="output_1",
    )

    records = storage.get_item_records(run_id="run_1")
    assert records[0].status is ItemStatus.COMPLETED
    assert records[1].status is ItemStatus.FAILED_PERMANENT
    assert records[1].error is not None
    assert records[1].error.error_class == "invalid_json"

    summary = storage.get_run_summary(run_id="run_1")
    assert summary.status is RunLifecycleStatus.COMPLETED
    assert summary.completed_items == 1
    assert summary.failed_items == 1
    config = storage.get_run_config(run_id="run_1")
    assert config.provider_config.provider_kind is ProviderKind.OPENAI


def test_sqlite_storage_reset_and_backoff(tmp_path: Path) -> None:
    clock = _FrozenClock()
    storage = SQLiteStorage(path=tmp_path / "backoff.sqlite3", now=clock.now)
    storage.create_run(run_id="run_2", config=_config(), items=_items()[:1])
    claimed = storage.claim_items_for_submission(run_id="run_2", max_attempts=2)
    storage.register_batch(
        run_id="run_2",
        local_batch_id="local_1",
        provider_batch_id="provider_1",
        status="submitted",
        custom_ids=["row1:a1"],
    )
    storage.mark_items_submitted(
        run_id="run_2",
        provider_batch_id="provider_1",
        submissions=[
            PreparedSubmission(
                item_id=claimed[0].item_id,
                custom_id="row1:a1",
                submission_tokens=5,
            )
        ],
    )

    storage.reset_batch_items_to_pending(
        run_id="run_2",
        provider_batch_id="provider_1",
        error=ItemFailure(
            error_class="batch_terminal_failed",
            message="batch failed",
            retryable=True,
            raw_error={"status": "failed"},
        ),
    )
    pending_summary = storage.get_run_summary(run_id="run_2")
    assert pending_summary.status_counts[ItemStatus.PENDING] == 1

    backoff = storage.record_batch_retry_failure(
        run_id="run_2",
        error_class="enqueue_token_limit",
        base_delay_sec=1.0,
        max_delay_sec=5.0,
    )
    assert backoff.backoff_sec == 1.0
    assert storage.get_batch_retry_backoff_remaining_sec(run_id="run_2") == 1.0
    storage.clear_batch_retry_backoff(run_id="run_2")
    assert storage.get_batch_retry_backoff_remaining_sec(run_id="run_2") == 0.0


def test_get_run_raises_for_unresolvable_structured_model(tmp_path: Path) -> None:
    storage = SQLiteStorage(path=tmp_path / "model.sqlite3")
    storage.create_run(run_id="run_3", config=_config(structured=True), items=_items()[:1])
    runner = BatchRunner(storage=SQLiteStorage(path=storage.path))
    with pytest.raises(ModelResolutionError, match="structured output model unavailable"):
        runner.get_run("run_3")
