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


# ---------------------------------------------------------------------------
# resume_existing_run: cancel and pause paths
# ---------------------------------------------------------------------------


def test_resume_existing_run_rejects_incompatible_config(tmp_path: Path) -> None:
    path = tmp_path / "items.jsonl"
    path.write_text(json.dumps({"id": "row0", "text": "first"}) + "\n", encoding="utf-8")
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
    deps = IngestionDeps(
        state=storage,
        emit_event=lambda *args, **kwargs: None,
        submit_pending_items=lambda run_id, context: 0,
        configs_match_for_resume=lambda stored, supplied: False,  # always mismatch
    )
    context = build_run_context(
        config=config,
        output_model=None,
        create_provider=lambda _cfg: _NoopProvider(),
    )
    run_id = "incompatible_config_run"
    storage.create_run(run_id=run_id, config=config, items=[])

    import pytest

    with pytest.raises(ValueError, match="existing run config does not match"):
        resume_existing_run(deps, run_id=run_id, job=job, config=config, context=context)


def test_resume_existing_run_returns_immediately_when_cancel_requested(tmp_path: Path) -> None:
    from batchor.core.enums import RunControlState

    path = tmp_path / "items.jsonl"
    path.write_text(json.dumps({"id": "row0", "text": "first"}) + "\n", encoding="utf-8")
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
    run_id = "cancel_requested_run"
    storage.create_run(run_id=run_id, config=config, items=[])
    storage._runs[run_id].control_state = RunControlState.CANCEL_REQUESTED

    resume_existing_run(deps, run_id=run_id, job=job, config=config, context=context)

    assert submitted_runs == [], "nothing should be submitted when cancel is requested"


def test_resume_existing_run_returns_without_submit_when_paused_incomplete(tmp_path: Path) -> None:
    """PAUSED + incomplete ingestion: should not submit, not resume ingestion."""
    from batchor.core.enums import RunControlState

    path = tmp_path / "items.jsonl"
    path.write_text(json.dumps({"id": "row0", "text": "first"}) + "\n", encoding="utf-8")
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
    run_id = "paused_incomplete_run"
    storage.create_run(run_id=run_id, config=config, items=[])
    identity = source.source_identity()
    storage.set_ingest_checkpoint(
        run_id=run_id,
        checkpoint=IngestCheckpoint(
            source_kind=identity.source_kind,
            source_ref=identity.source_ref,
            source_fingerprint=identity.source_fingerprint,
            next_item_index=0,
            checkpoint_payload=0,
            ingestion_complete=False,
        ),
    )
    storage._runs[run_id].control_state = RunControlState.PAUSED

    resume_existing_run(deps, run_id=run_id, job=job, config=config, context=context)

    assert submitted_runs == [], "paused run should not submit"


def test_resume_existing_run_submits_pending_when_ingestion_already_complete(tmp_path: Path) -> None:
    """Ingestion is complete and run is RUNNING: only submit_pending_items is called."""
    path = tmp_path / "items.jsonl"
    path.write_text(json.dumps({"id": "row0", "text": "first"}) + "\n", encoding="utf-8")
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
    run_id = "complete_ingestion_run"
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
            ingestion_complete=True,
        ),
    )

    resume_existing_run(deps, run_id=run_id, job=job, config=config, context=context)

    assert submitted_runs == [run_id]


# ---------------------------------------------------------------------------
# Bug fix: ingestion_complete must be False when cancel fires before submit
# ---------------------------------------------------------------------------


def test_ingest_job_items_stores_ingestion_complete_false_when_cancelled_before_submit(
    tmp_path: Path,
) -> None:
    """Regression: cancel before submit_pending_items must record ingestion_complete=False."""
    path = tmp_path / "items.jsonl"
    path.write_text(
        "\n".join(json.dumps({"id": f"r{i}", "text": f"t{i}"}) for i in range(1500)) + "\n",
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
    run_id = "cancel_before_submit"
    storage.create_run(run_id=run_id, config=config, items=[])
    identity = source.source_identity()
    storage.set_ingest_checkpoint(
        run_id=run_id,
        checkpoint=IngestCheckpoint(
            source_kind=identity.source_kind,
            source_ref=identity.source_ref,
            source_fingerprint=identity.source_fingerprint,
            next_item_index=0,
            checkpoint_payload=0,
            ingestion_complete=False,
        ),
    )

    from batchor.core.enums import RunControlState

    call_count = [0]

    def cancel_before_second_submit(run_id: str, context: object) -> int:
        call_count[0] += 1
        # Cancel on the first call so the NEXT iteration's pre-submit check fires
        storage._runs[run_id].control_state = RunControlState.CANCEL_REQUESTED
        return 0

    deps = IngestionDeps(
        state=storage,
        emit_event=lambda *args, **kwargs: None,
        submit_pending_items=cancel_before_second_submit,
        configs_match_for_resume=lambda a, b: a == b,
    )
    context = build_run_context(
        config=config,
        output_model=None,
        create_provider=lambda _cfg: _NoopProvider(),
    )

    from batchor.runtime.ingestion import ingest_job_items

    ingest_job_items(
        deps,
        run_id=run_id,
        job=job,
        context=context,
        start_index=0,
        checkpoint_payload=0,
    )

    checkpoint = storage.get_ingest_checkpoint(run_id=run_id)
    assert checkpoint is not None
    # After the fix: ingestion_complete must be False — not all items were ingested
    assert checkpoint.ingestion_complete is False, (
        "ingestion_complete must be False when cancel fires before all items are ingested"
    )
    # Only the first chunk (1000 items) should be in the DB
    records = storage.get_item_records(run_id=run_id)
    assert len(records) == 1000


# ---------------------------------------------------------------------------
# materialize_item_chunks: error paths
# ---------------------------------------------------------------------------


def test_resume_existing_run_does_not_submit_when_paused_and_ingestion_complete(
    tmp_path: Path,
) -> None:
    """PAUSED + ingestion complete: should not call submit_pending_items."""
    from batchor.core.enums import RunControlState

    path = tmp_path / "items.jsonl"
    path.write_text(json.dumps({"id": "row0", "text": "first"}) + "\n", encoding="utf-8")
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
    run_id = "paused_complete_run"
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
            ingestion_complete=True,
        ),
    )
    storage._runs[run_id].control_state = RunControlState.PAUSED

    resume_existing_run(deps, run_id=run_id, job=job, config=config, context=context)

    assert submitted_runs == [], "should not submit when PAUSED even if ingestion is complete"


def test_materialize_item_chunks_rejects_zero_chunk_size() -> None:
    from batchor.runtime.ingestion import materialize_item_chunks

    job = BatchJob(
        items=[BatchItem(item_id="r1", payload={})],
        build_prompt=lambda item: PromptParts(prompt="x"),
        provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
    )
    import pytest

    with pytest.raises(ValueError, match="chunk_size must be > 0"):
        list(materialize_item_chunks(job, chunk_size=0))


def test_ingest_job_items_stores_ingestion_complete_false_when_cancel_already_set(
    tmp_path: Path,
) -> None:
    """Cancel set BEFORE submit fires the first cancel check (lines 137-138)."""
    from batchor.core.enums import RunControlState
    from batchor.runtime.ingestion import ingest_job_items

    path = tmp_path / "items.jsonl"
    path.write_text(
        "\n".join(json.dumps({"id": f"r{i}", "text": f"t{i}"}) for i in range(3)) + "\n",
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
    run_id = "pre_cancel_run"
    storage.create_run(run_id=run_id, config=config, items=[])
    identity = source.source_identity()
    storage.set_ingest_checkpoint(
        run_id=run_id,
        checkpoint=IngestCheckpoint(
            source_kind=identity.source_kind,
            source_ref=identity.source_ref,
            source_fingerprint=identity.source_fingerprint,
            next_item_index=0,
            checkpoint_payload=0,
            ingestion_complete=False,
        ),
    )
    # Pre-cancel the run so the FIRST cancel check fires immediately
    storage._runs[run_id].control_state = RunControlState.CANCEL_REQUESTED

    deps = IngestionDeps(
        state=storage,
        emit_event=lambda *args, **kwargs: None,
        submit_pending_items=lambda rund, ctx: 0,
        configs_match_for_resume=lambda a, b: a == b,
    )
    context = build_run_context(
        config=config,
        output_model=None,
        create_provider=lambda _cfg: _NoopProvider(),
    )

    ingest_job_items(
        deps,
        run_id=run_id,
        job=job,
        context=context,
        start_index=0,
        checkpoint_payload=0,
    )

    checkpoint = storage.get_ingest_checkpoint(run_id=run_id)
    assert checkpoint is not None
    assert checkpoint.ingestion_complete is False


def test_materialize_item_chunks_uses_chunked_checkpointed_source(
    tmp_path: Path,
) -> None:
    """Checkpointed source with >chunk_size items exercises the mid-source yield path."""
    from batchor.runtime.ingestion import materialize_item_chunks

    path = tmp_path / "items.jsonl"
    path.write_text(
        "\n".join(json.dumps({"id": f"r{i}", "text": f"t{i}"}) for i in range(5)) + "\n",
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
    chunks = list(materialize_item_chunks(job, chunk_size=2))
    # 5 items, chunk_size=2 → 3 chunks: [r0,r1], [r2,r3], [r4]
    assert len(chunks) == 3
    assert [item.item_id for item in chunks[0][0]] == ["r0", "r1"]
    assert [item.item_id for item in chunks[2][0]] == ["r4"]


def test_materialize_item_chunks_uses_chunked_list_source() -> None:
    """Non-checkpointed source with >chunk_size items exercises the list yield path."""
    from batchor.runtime.ingestion import materialize_item_chunks

    job = BatchJob(
        items=[BatchItem(item_id=f"r{i}", payload={"text": f"t{i}"}) for i in range(5)],
        build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
        provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
    )
    chunks = list(materialize_item_chunks(job, chunk_size=2))
    # 5 items, chunk_size=2 → 3 chunks: [r0,r1], [r2,r3], [r4]
    assert len(chunks) == 3
    assert [item.item_id for item in chunks[0][0]] == ["r0", "r1"]


def test_materialize_item_chunks_rejects_duplicate_ids_in_checkpointed_source(
    tmp_path: Path,
) -> None:
    """Checkpointed source with duplicate IDs must raise ValueError."""
    from batchor.runtime.ingestion import materialize_item_chunks

    path = tmp_path / "dup.jsonl"
    path.write_text(
        json.dumps({"id": "r1", "text": "a"}) + "\n" + json.dumps({"id": "r1", "text": "b"}) + "\n",
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
    import pytest

    with pytest.raises(ValueError, match="duplicate item_id"):
        list(materialize_item_chunks(job))


def test_require_checkpointed_source_raises_for_non_checkpointed_job() -> None:
    import pytest

    from batchor.runtime.ingestion import require_checkpointed_source

    job = BatchJob(
        items=[BatchItem(item_id="r1", payload={})],
        build_prompt=lambda item: PromptParts(prompt="x"),
        provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
    )
    with pytest.raises(ValueError, match="checkpointed item source"):
        require_checkpointed_source(job, run_id="test")


def test_validate_checkpoint_source_rejects_kind_mismatch(tmp_path: Path) -> None:
    import pytest

    from batchor.runtime.ingestion import validate_checkpoint_source
    from batchor.sources.files import CsvItemSource

    path = tmp_path / "items.csv"
    path.write_text("id,text\nr1,a\n", encoding="utf-8")
    csv_source = CsvItemSource(path, item_id_from_row=lambda r: r["id"], payload_from_row=lambda r: r)
    identity = csv_source.source_identity()
    checkpoint = IngestCheckpoint(
        source_kind="jsonl",  # mismatch
        source_ref=identity.source_ref,
        source_fingerprint=identity.source_fingerprint,
        next_item_index=0,
        checkpoint_payload=0,
        ingestion_complete=False,
    )
    with pytest.raises(ValueError, match="source kind mismatch"):
        validate_checkpoint_source(run_id="test", source=csv_source, checkpoint=checkpoint)


def test_validate_checkpoint_source_rejects_fingerprint_mismatch(tmp_path: Path) -> None:
    import pytest

    from batchor.runtime.ingestion import validate_checkpoint_source

    path = tmp_path / "items.jsonl"
    path.write_text('{"id":"r1","text":"a"}\n', encoding="utf-8")
    source = JsonlItemSource(
        path,
        item_id_from_row=lambda r: str(r["id"]) if isinstance(r, dict) else "",
        payload_from_row=lambda r: r,
    )
    identity = source.source_identity()
    checkpoint = IngestCheckpoint(
        source_kind=identity.source_kind,
        source_ref=identity.source_ref,
        source_fingerprint="wrong-fingerprint",
        next_item_index=0,
        checkpoint_payload=0,
        ingestion_complete=False,
    )
    with pytest.raises(ValueError, match="source fingerprint mismatch"):
        validate_checkpoint_source(run_id="test", source=source, checkpoint=checkpoint)


def test_normalize_prompt_parts_wraps_plain_string() -> None:
    from batchor.core.models import PromptParts as PP
    from batchor.runtime.ingestion import normalize_prompt_parts

    result = normalize_prompt_parts("just a string")
    assert isinstance(result, PP)
    assert result.prompt == "just a string"


def test_json_value_raises_on_non_serializable_payload() -> None:
    import pytest

    from batchor.runtime.ingestion import json_value

    with pytest.raises(TypeError, match="must be JSON-serializable"):
        json_value(object(), label="test payload")


def test_json_object_raises_when_value_is_not_a_dict() -> None:
    import pytest

    from batchor.runtime.ingestion import json_object

    with pytest.raises(TypeError, match="must be a JSON object"):
        json_object(["not", "a", "dict"], label="test metadata")


def test_json_object_raises_when_batchor_lineage_is_non_dict() -> None:
    import pytest

    from batchor.runtime.ingestion import json_object

    with pytest.raises(TypeError, match="batchor_lineage must be a JSON object"):
        json_object({"batchor_lineage": "bad"}, label="test metadata")


def test_materialize_item_chunks_rejects_non_resumable_source_with_nonzero_start() -> None:
    from batchor.runtime.ingestion import materialize_item_chunks

    job = BatchJob(
        items=[BatchItem(item_id="r1", payload={})],
        build_prompt=lambda item: PromptParts(prompt="x"),
        provider_config=OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
    )
    import pytest

    with pytest.raises(ValueError, match="non-resumable item sources cannot start from a checkpoint"):
        list(materialize_item_chunks(job, start_index=1))
