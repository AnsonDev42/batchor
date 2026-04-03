from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text

from batchor import (
    OpenAIEnqueueLimitConfig,
    OpenAIProviderConfig,
    ProviderKind,
    SQLiteStorage,
)
from batchor.core.models import ChunkPolicy, RetryPolicy
from batchor.storage.postgres import PostgresStorage
from batchor.storage.state import (
    BatchArtifactPointer,
    CompletedItemRecord,
    IngestCheckpoint,
    MaterializedItem,
    PersistedRunConfig,
    PreparedSubmission,
    RequestArtifactPointer,
)


def _config() -> PersistedRunConfig:
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
        batch_metadata={"source": "contract"},
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
        ),
    ]


_POSTGRES_DSN = os.getenv("BATCHOR_TEST_POSTGRES_DSN", "")
_STORAGE_BACKENDS = ["sqlite"]
if _POSTGRES_DSN:
    _STORAGE_BACKENDS.append("postgres")


@pytest.fixture(params=_STORAGE_BACKENDS)
def storage(request: pytest.FixtureRequest, tmp_path: Path):
    if request.param == "sqlite":
        store = SQLiteStorage(path=tmp_path / "contract.sqlite3")
        try:
            yield store
        finally:
            store.close()
        return

    schema = f"batchor_test_{uuid4().hex[:8]}"
    store = PostgresStorage(dsn=_POSTGRES_DSN, schema=schema)
    try:
        yield store
    finally:
        with store._base_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        store.close()


def test_storage_contract_claim_release_requeue_and_retry_state(storage) -> None:
    storage.create_run(run_id="run_1", config=_config(), items=_items())
    storage.set_ingest_checkpoint(
        run_id="run_1",
        checkpoint=IngestCheckpoint(
            source_kind="jsonl",
            source_ref="items.jsonl",
            source_fingerprint="abc",
        ),
    )
    claimed = storage.claim_items_for_submission(run_id="run_1", max_attempts=2, limit=1)
    assert [item.item_id for item in claimed] == ["row1"]
    assert storage.requeue_local_items(run_id="run_1") == 1

    claimed = storage.claim_items_for_submission(run_id="run_1", max_attempts=2)
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
            PreparedSubmission(item_id=claimed[0].item_id, custom_id="row1:a1", submission_tokens=10),
            PreparedSubmission(item_id=claimed[1].item_id, custom_id="row2:a1", submission_tokens=20),
        ],
    )
    assert storage.get_active_submitted_token_estimate(run_id="run_1") == 30
    backoff = storage.record_batch_retry_failure(
        run_id="run_1",
        error_class="enqueue_token_limit",
        base_delay_sec=1.0,
        max_delay_sec=5.0,
    )
    assert backoff.backoff_sec == 1.0
    storage.clear_batch_retry_backoff(run_id="run_1")
    assert storage.get_batch_retry_backoff_remaining_sec(run_id="run_1") == 0.0


def test_storage_contract_artifact_pointers_and_summary_rehydration(storage) -> None:
    storage.create_run(run_id="run_2", config=_config(), items=_items()[:1])
    storage.record_request_artifacts(
        run_id="run_2",
        pointers=[
            RequestArtifactPointer(
                item_id="row1",
                artifact_path="run_2/requests/requests_1.jsonl",
                line_number=1,
                request_sha256="abc123",
            )
        ],
    )
    storage.register_batch(
        run_id="run_2",
        local_batch_id="local_1",
        provider_batch_id="provider_1",
        status="completed",
        custom_ids=["row1:a1"],
    )
    storage.record_batch_artifacts(
        run_id="run_2",
        pointers=[
            BatchArtifactPointer(
                provider_batch_id="provider_1",
                output_artifact_path="run_2/outputs/output.jsonl",
                error_artifact_path=None,
            )
        ],
    )
    storage.mark_items_submitted(
        run_id="run_2",
        provider_batch_id="provider_1",
        submissions=[PreparedSubmission(item_id="row1", custom_id="row1:a1", submission_tokens=10)],
    )
    storage.mark_items_completed(
        run_id="run_2",
        completions=[
            CompletedItemRecord(
                custom_id="row1:a1",
                output_text="ok",
                output_json=None,
                raw_response={"response": {"status_code": 200}},
            )
        ],
    )
    inventory = storage.get_artifact_inventory(run_id="run_2")
    assert inventory.request_artifact_paths == ["run_2/requests/requests_1.jsonl"]
    assert inventory.output_artifact_paths == ["run_2/outputs/output.jsonl"]
    assert (
        storage.clear_request_artifact_pointers(
            run_id="run_2",
            artifact_paths=inventory.request_artifact_paths,
        )
        == 1
    )
    assert (
        storage.clear_batch_artifact_pointers(
            run_id="run_2",
            artifact_paths=inventory.output_artifact_paths,
        )
        == 1
    )
    storage.mark_artifacts_exported(run_id="run_2", export_root="/tmp/exported")
    summary = storage.get_run_summary(run_id="run_2")
    assert summary.completed_items == 1
    assert summary.failed_items == 0
    assert storage.get_run_config(run_id="run_2").provider_config.provider_kind is ProviderKind.OPENAI
