"""Internal helpers for item-source materialization and resume-aware ingestion."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Iterator, cast

from pydantic import BaseModel

from batchor.core.enums import RunControlState
from batchor.core.models import BatchJob, PromptParts
from batchor.core.types import JSONObject, JSONValue
from batchor.runtime.context import RunContext
from batchor.sources.base import CheckpointedItemSource
from batchor.storage.state import (
    IngestCheckpoint,
    MaterializedItem,
    PersistedRunConfig,
    StateStore,
)


@dataclass(frozen=True)
class IngestionDeps:
    """Dependencies needed by the ingestion and resume layer.

    Attributes:
        state: Durable state store used for ingest checkpoints and items.
        emit_event: Observer callback used for coarse lifecycle events.
        submit_pending_items: Callback used to submit items during ingestion.
        configs_match_for_resume: Predicate used to validate resume
            compatibility.
    """

    state: StateStore
    emit_event: Callable[..., None]
    submit_pending_items: Callable[[str, RunContext], int]
    configs_match_for_resume: Callable[[PersistedRunConfig, PersistedRunConfig], bool]


def resume_existing_run(
    deps: IngestionDeps,
    *,
    run_id: str,
    job: BatchJob[Any, BaseModel],
    config: PersistedRunConfig,
    context: RunContext,
) -> None:
    """Resume an existing durable run, including checkpointed ingestion.

    Args:
        deps: Ingestion-layer dependencies.
        run_id: Durable run identifier.
        job: Caller-supplied job attempting to resume the run.
        config: Persisted config derived from the supplied job.
        context: Runtime context for the run.

    Raises:
        ValueError: If the supplied job is incompatible with the stored run
            config or checkpointed source identity.
    """
    stored_config = deps.state.get_run_config(run_id=run_id)
    if not deps.configs_match_for_resume(stored_config, config):
        raise ValueError(f"existing run config does not match supplied job: {run_id}")
    deps.state.requeue_local_items(run_id=run_id)
    control_state = deps.state.get_run_control_state(run_id=run_id)
    if control_state is RunControlState.CANCEL_REQUESTED:
        return
    checkpoint = deps.state.get_ingest_checkpoint(run_id=run_id)
    if checkpoint is not None and not checkpoint.ingestion_complete:
        source = require_checkpointed_source(job, run_id=run_id)
        validate_checkpoint_source(run_id=run_id, source=source, checkpoint=checkpoint)
        if control_state is RunControlState.PAUSED:
            return
        ingest_job_items(
            deps,
            run_id=run_id,
            job=job,
            context=context,
            start_index=checkpoint.next_item_index,
            checkpoint_payload=checkpoint.checkpoint_payload,
        )
        return
    if control_state is RunControlState.PAUSED:
        return
    deps.submit_pending_items(run_id, context)


def ingest_job_items(
    deps: IngestionDeps,
    *,
    run_id: str,
    job: BatchJob[Any, BaseModel],
    context: RunContext,
    start_index: int,
    checkpoint_payload: JSONValue | None,
) -> None:
    """Materialize source items, persist them, and submit as ingestion proceeds.

    Args:
        deps: Ingestion-layer dependencies.
        run_id: Durable run identifier.
        job: Job whose items should be materialized.
        context: Runtime context for the run.
        start_index: Item index to resume from.
        checkpoint_payload: Source-owned checkpoint payload, if any.
    """
    next_item_index = start_index
    next_checkpoint_payload = checkpoint_payload
    checkpointed = checkpointed_source(job) is not None
    ingestion_complete = True
    for item_chunk, next_item_index, next_checkpoint_payload in materialize_item_chunks(
        job,
        start_index=start_index,
        checkpoint_payload=checkpoint_payload,
    ):
        deps.state.append_items(run_id=run_id, items=item_chunk)
        deps.emit_event(
            "items_ingested",
            run_id=run_id,
            provider_kind=context.config.provider_config.provider_kind,
            data={
                "chunk_item_count": len(item_chunk),
                "last_item_index": item_chunk[-1].item_index,
            },
        )
        if checkpointed:
            deps.state.update_ingest_checkpoint(
                run_id=run_id,
                next_item_index=next_item_index,
                checkpoint_payload=next_checkpoint_payload,
                ingestion_complete=False,
            )
        control_state = deps.state.get_run_control_state(run_id=run_id)
        if control_state is RunControlState.CANCEL_REQUESTED:
            ingestion_complete = False
            break
        deps.submit_pending_items(run_id, context)
        control_state = deps.state.get_run_control_state(run_id=run_id)
        if control_state is not RunControlState.RUNNING:
            ingestion_complete = False
            break
    if checkpointed:
        deps.state.update_ingest_checkpoint(
            run_id=run_id,
            next_item_index=next_item_index,
            checkpoint_payload=next_checkpoint_payload,
            ingestion_complete=ingestion_complete,
        )


def materialize_item_chunks(
    job: BatchJob[Any, BaseModel],
    *,
    start_index: int = 0,
    checkpoint_payload: JSONValue | None = None,
    chunk_size: int = 1000,
) -> Iterator[tuple[list[MaterializedItem], int, JSONValue | None]]:
    """Materialize job items into durable chunks with stable item indexes.

    Args:
        job: Job whose items should be materialized.
        start_index: Starting durable item index.
        checkpoint_payload: Source-owned checkpoint payload, if any.
        chunk_size: Maximum number of items yielded per chunk.

    Yields:
        Tuples of ``(items, next_item_index, next_checkpoint_payload)``.

    Raises:
        ValueError: If ``chunk_size`` is invalid, duplicate item IDs are
            observed, or a non-resumable source is asked to resume.
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    current_chunk: list[MaterializedItem] = []
    seen_ids: set[str] = set()
    source = checkpointed_source(job)
    current_index = start_index
    next_checkpoint = checkpoint_payload
    if source is not None:
        source_checkpoint = checkpoint_payload if checkpoint_payload is not None else source.initial_checkpoint()
        for source_item in source.iter_from_checkpoint(source_checkpoint):
            item_index = current_index
            item = source_item.item
            next_checkpoint = source_item.next_checkpoint
            if item.item_id in seen_ids:
                raise ValueError(f"duplicate item_id: {item.item_id}")
            seen_ids.add(item.item_id)
            prompt_parts = normalize_prompt_parts(job.build_prompt(item))
            current_chunk.append(
                MaterializedItem(
                    item_id=item.item_id,
                    item_index=item_index,
                    payload=json_value(item.payload, label=f"payload for {item.item_id}"),
                    metadata=json_object(item.metadata, label=f"metadata for {item.item_id}"),
                    prompt=prompt_parts.prompt,
                    system_prompt=prompt_parts.system_prompt,
                )
            )
            current_index = item_index + 1
            if len(current_chunk) >= chunk_size:
                yield current_chunk, current_index, next_checkpoint
                current_chunk = []
    else:
        if start_index != 0:
            raise ValueError("non-resumable item sources cannot start from a checkpoint")
        for item_index, item in enumerate(job.items, start=start_index):
            if item.item_id in seen_ids:
                raise ValueError(f"duplicate item_id: {item.item_id}")
            seen_ids.add(item.item_id)
            prompt_parts = normalize_prompt_parts(job.build_prompt(item))
            current_chunk.append(
                MaterializedItem(
                    item_id=item.item_id,
                    item_index=item_index,
                    payload=json_value(item.payload, label=f"payload for {item.item_id}"),
                    metadata=json_object(item.metadata, label=f"metadata for {item.item_id}"),
                    prompt=prompt_parts.prompt,
                    system_prompt=prompt_parts.system_prompt,
                )
            )
            current_index = item_index + 1
            if len(current_chunk) >= chunk_size:
                yield current_chunk, current_index, next_checkpoint
                current_chunk = []
    if current_chunk:
        yield current_chunk, current_index, next_checkpoint


def checkpointed_source(
    job: BatchJob[Any, BaseModel],
) -> CheckpointedItemSource[Any] | None:
    """Return the checkpoint-capable item source for a job when present.

    Args:
        job: Job whose item source should be inspected.

    Returns:
        Checkpoint-capable item source, or ``None``.
    """
    if isinstance(job.items, CheckpointedItemSource):
        return cast(CheckpointedItemSource[Any], job.items)
    return None


def require_checkpointed_source(
    job: BatchJob[Any, BaseModel],
    *,
    run_id: str,
) -> CheckpointedItemSource[Any]:
    """Return the checkpointed source or raise when restart cannot resume it.

    Args:
        job: Job whose source should be resumed.
        run_id: Durable run identifier used in error messages.

    Returns:
        Checkpoint-capable item source for the job.

    Raises:
        ValueError: If the job does not use a checkpoint-capable source.
    """
    source = checkpointed_source(job)
    if source is None:
        raise ValueError(f"run requires a checkpointed item source for restart: {run_id}")
    return source


def validate_checkpoint_source(
    *,
    run_id: str,
    source: CheckpointedItemSource[Any],
    checkpoint: IngestCheckpoint,
) -> None:
    """Validate that a resumed source matches the persisted checkpoint identity.

    Args:
        run_id: Durable run identifier used in error messages.
        source: Candidate checkpoint-capable source supplied by the caller.
        checkpoint: Persisted ingest checkpoint for the run.

    Raises:
        ValueError: If the source kind, reference, or fingerprint changed.
    """
    identity = source.source_identity()
    if identity.source_kind != checkpoint.source_kind:
        raise ValueError(f"source kind mismatch for resumed run: {run_id}")
    if identity.source_ref != checkpoint.source_ref:
        raise ValueError(f"source path mismatch for resumed run: {run_id}")
    if identity.source_fingerprint != checkpoint.source_fingerprint:
        raise ValueError(f"source fingerprint mismatch for resumed run: {run_id}")


def normalize_prompt_parts(prompt_value: PromptParts | str) -> PromptParts:
    """Normalize prompt-builder return values into ``PromptParts``.

    Args:
        prompt_value: Prompt-builder return value.

    Returns:
        Normalized prompt parts.
    """
    if isinstance(prompt_value, PromptParts):
        return prompt_value
    return PromptParts(prompt=str(prompt_value))


def json_value(value: Any, *, label: str) -> JSONValue:
    """Normalize a value through JSON serialization for durable storage.

    Args:
        value: Value to normalize.
        label: Label used in validation errors.

    Returns:
        JSON-serializable normalized value.

    Raises:
        TypeError: If the value is not JSON serializable.
    """
    try:
        return cast(JSONValue, json.loads(json.dumps(value, ensure_ascii=False)))
    except TypeError as exc:
        raise TypeError(f"{label} must be JSON-serializable") from exc


def json_object(value: Any, *, label: str) -> JSONObject:
    """Normalize and validate a metadata object for durable storage.

    Args:
        value: Metadata value to normalize.
        label: Label used in validation errors.

    Returns:
        Normalized JSON object.

    Raises:
        TypeError: If the value is not a JSON object or has invalid reserved
            lineage metadata.
    """
    normalized = json_value(value, label=label)
    if not isinstance(normalized, dict):
        raise TypeError(f"{label} must be a JSON object")
    lineage = normalized.get("batchor_lineage")
    if lineage is not None and not isinstance(lineage, dict):
        raise TypeError(f"{label} batchor_lineage must be a JSON object when provided")
    return normalized
