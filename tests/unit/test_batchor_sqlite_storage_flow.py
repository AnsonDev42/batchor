from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import select

from batchor import (
    BatchRunner,
    ItemStatus,
    ModelResolutionError,
    OpenAIEnqueueLimitConfig,
    OpenAIProviderConfig,
    ProviderKind,
    RunLifecycleStatus,
)
from batchor.core.models import ChunkPolicy, ItemFailure, RetryPolicy
from batchor.storage import sqlite as storage_sqlite
from batchor.storage.sqlite import SQLiteStorage
from batchor.storage.state import (
    CompletedItemRecord,
    IngestCheckpoint,
    ItemFailureRecord,
    MaterializedItem,
    PersistedRunConfig,
    PreparedSubmission,
    RequestArtifactPointer,
)


class _FrozenClock:
    def __init__(self) -> None:
        self.current = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def now(self) -> datetime:
        return self.current


def _config(*, structured: bool = False) -> PersistedRunConfig:
    return PersistedRunConfig(
        provider_config=OpenAIProviderConfig(
            api_key="k",
            model="gpt-4.1",
            enqueue_limits=OpenAIEnqueueLimitConfig(
                enqueued_token_limit=1000,
                target_ratio=0.7,
                headroom=50,
                max_batch_enqueued_tokens=500,
            ),
        ),
        chunk_policy=ChunkPolicy(),
        retry_policy=RetryPolicy(max_attempts=2, base_backoff_sec=1.0, max_backoff_sec=5.0),
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
    assert storage.has_run(run_id="run_1") is True

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
    provider_config = config.provider_config
    assert isinstance(provider_config, OpenAIProviderConfig)
    assert provider_config.enqueue_limits.max_batch_enqueued_tokens == 500


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


def test_sqlite_storage_records_request_artifact_pointer_and_prunes_inline_request_fields(
    tmp_path: Path,
) -> None:
    storage = SQLiteStorage(path=tmp_path / "artifacts.sqlite3")
    storage.create_run(run_id="run_4", config=_config(), items=_items()[:1])

    storage.record_request_artifacts(
        run_id="run_4",
        pointers=[
            RequestArtifactPointer(
                item_id="row1",
                artifact_path="run_4/requests/requests_a.jsonl",
                line_number=1,
                request_sha256="abc123",
            )
        ],
    )

    claimed = storage.claim_items_for_submission(run_id="run_4", max_attempts=2)
    assert claimed[0].request_artifact_path == "run_4/requests/requests_a.jsonl"
    assert claimed[0].request_artifact_line == 1
    assert claimed[0].request_sha256 == "abc123"
    assert claimed[0].prompt == ""
    assert claimed[0].system_prompt is None

    with storage.engine.begin() as conn:
        row = conn.execute(
            select(
                storage_sqlite.ITEMS_TABLE.c.payload_json,
                storage_sqlite.ITEMS_TABLE.c.prompt,
                storage_sqlite.ITEMS_TABLE.c.request_artifact_path,
                storage_sqlite.ITEMS_TABLE.c.request_artifact_line,
                storage_sqlite.ITEMS_TABLE.c.request_sha256,
            ).where(storage_sqlite.ITEMS_TABLE.c.run_id == "run_4")
        ).mappings().one()
    assert row["payload_json"] == "null"
    assert row["prompt"] == ""
    assert row["request_artifact_path"] == "run_4/requests/requests_a.jsonl"
    assert row["request_artifact_line"] == 1
    assert row["request_sha256"] == "abc123"


def test_sqlite_storage_lists_and_clears_request_artifact_pointers(tmp_path: Path) -> None:
    storage = SQLiteStorage(path=tmp_path / "artifact_cleanup.sqlite3")
    storage.create_run(run_id="run_5", config=_config(), items=_items())
    storage.record_request_artifacts(
        run_id="run_5",
        pointers=[
            RequestArtifactPointer(
                item_id="row1",
                artifact_path="run_5/requests/requests_a.jsonl",
                line_number=1,
                request_sha256="sha_a",
            ),
            RequestArtifactPointer(
                item_id="row2",
                artifact_path="run_5/requests/requests_a.jsonl",
                line_number=2,
                request_sha256="sha_b",
            ),
        ],
    )

    assert storage.get_request_artifact_paths(run_id="run_5") == [
        "run_5/requests/requests_a.jsonl"
    ]

    cleared = storage.clear_request_artifact_pointers(
        run_id="run_5",
        artifact_paths=["run_5/requests/requests_a.jsonl"],
    )
    assert cleared == 2
    assert storage.get_request_artifact_paths(run_id="run_5") == []

    claimed = storage.claim_items_for_submission(run_id="run_5", max_attempts=2)
    assert claimed[0].request_artifact_path is None
    assert claimed[1].request_artifact_path is None


def test_sqlite_storage_persists_ingest_checkpoint(tmp_path: Path) -> None:
    storage = SQLiteStorage(path=tmp_path / "ingest.sqlite3")
    storage.create_run(run_id="run_6", config=_config(), items=[])

    storage.set_ingest_checkpoint(
        run_id="run_6",
        checkpoint=IngestCheckpoint(
            source_kind="jsonl",
            source_ref=str((tmp_path / "items.jsonl").resolve()),
            source_fingerprint="abc123",
        ),
    )

    initial = storage.get_ingest_checkpoint(run_id="run_6")
    assert initial is not None
    assert initial.next_item_index == 0
    assert initial.ingestion_complete is False

    storage.update_ingest_checkpoint(
        run_id="run_6",
        next_item_index=1000,
        ingestion_complete=False,
    )
    advanced = storage.get_ingest_checkpoint(run_id="run_6")
    assert advanced is not None
    assert advanced.next_item_index == 1000
    assert advanced.ingestion_complete is False

    storage.update_ingest_checkpoint(
        run_id="run_6",
        next_item_index=1002,
        ingestion_complete=True,
    )
    completed = storage.get_ingest_checkpoint(run_id="run_6")
    assert completed is not None
    assert completed.next_item_index == 1002
    assert completed.ingestion_complete is True
