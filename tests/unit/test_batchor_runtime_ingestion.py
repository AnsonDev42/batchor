from __future__ import annotations

import json
from pathlib import Path

import pytest

from batchor import (
    BatchItem,
    BatchJob,
    JsonlItemSource,
    MemoryStateStore,
    OpenAIProviderConfig,
    PromptParts,
)
from batchor.runtime.context import build_persisted_config, build_run_context
from batchor.runtime.ingestion import (
    IngestionDeps,
    materialize_item_chunks,
    resume_existing_run,
    validate_checkpoint_source,
)
from batchor.storage.state import IngestCheckpoint, MaterializedItem


class _NoopProvider:
    pass


def test_materialize_item_chunks_rejects_duplicate_item_ids() -> None:
    job = BatchJob(
        items=[
            BatchItem(item_id="row1", payload={"text": "hello"}),
            BatchItem(item_id="row1", payload={"text": "again"}),
        ],
        build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
        provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
    )

    with pytest.raises(ValueError, match="duplicate item_id"):
        list(materialize_item_chunks(job))


def test_validate_checkpoint_source_rejects_changed_source_identity(tmp_path: Path) -> None:
    first_path = tmp_path / "items-a.jsonl"
    second_path = tmp_path / "items-b.jsonl"
    for path in (first_path, second_path):
        path.write_text(json.dumps({"id": "row1", "text": "hello"}) + "\n", encoding="utf-8")
    first_source = JsonlItemSource(
        first_path,
        item_id_from_row=lambda row: str(row["id"]) if isinstance(row, dict) else "",
        payload_from_row=lambda row: {"text": row["text"]} if isinstance(row, dict) else {},
    )
    second_source = JsonlItemSource(
        second_path,
        item_id_from_row=lambda row: str(row["id"]) if isinstance(row, dict) else "",
        payload_from_row=lambda row: {"text": row["text"]} if isinstance(row, dict) else {},
    )
    identity = first_source.source_identity()
    checkpoint = IngestCheckpoint(
        source_kind=identity.source_kind,
        source_ref=identity.source_ref,
        source_fingerprint=identity.source_fingerprint,
        checkpoint_payload=first_source.initial_checkpoint(),
    )

    with pytest.raises(ValueError, match="source path mismatch"):
        validate_checkpoint_source(
            run_id="resume_run",
            source=second_source,
            checkpoint=checkpoint,
        )


def test_resume_existing_run_continues_from_checkpointed_jsonl_source(tmp_path: Path) -> None:
    path = tmp_path / "items.jsonl"
    path.write_text(
        "\n".join(
            [
                json.dumps({"id": "row0", "text": "first"}),
                json.dumps({"id": "row1", "text": "second"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    source = JsonlItemSource(
        path,
        item_id_from_row=lambda row: str(row["id"]) if isinstance(row, dict) else "",
        payload_from_row=lambda row: {"text": row["text"]} if isinstance(row, dict) else {},
    )
    job = BatchJob(
        items=source,
        build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
        provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
    )
    config = build_persisted_config(job)
    storage = MemoryStateStore()
    submitted_runs: list[str] = []
    deps = IngestionDeps(
        state=storage,
        emit_event=lambda *args, **kwargs: None,
        submit_pending_items=lambda run_id, context: submitted_runs.append(run_id) or 0,
        configs_match_for_resume=lambda stored, supplied: stored == supplied,
    )
    context = build_run_context(
        config=config,
        output_model=None,
        create_provider=lambda _cfg: _NoopProvider(),
    )
    run_id = "resume_jsonl_run"
    storage.create_run(
        run_id=run_id,
        config=config,
        items=[
            MaterializedItem(
                item_id="row0",
                item_index=0,
                payload={"text": "first"},
                metadata={},
                prompt="first",
            )
        ],
    )
    identity = source.source_identity()
    storage.set_ingest_checkpoint(
        run_id=run_id,
        checkpoint=IngestCheckpoint(
            source_kind=identity.source_kind,
            source_ref=identity.source_ref,
            source_fingerprint=identity.source_fingerprint,
            next_item_index=1,
            checkpoint_payload=1,
            ingestion_complete=False,
        ),
    )

    resume_existing_run(
        deps,
        run_id=run_id,
        job=job,
        config=config,
        context=context,
    )

    records = storage.get_item_records(run_id=run_id)
    checkpoint = storage.get_ingest_checkpoint(run_id=run_id)
    assert [record.item_id for record in records] == ["row0", "row1"]
    assert checkpoint is not None
    assert checkpoint.next_item_index == 2
    assert checkpoint.ingestion_complete is True
    assert submitted_runs == [run_id]
