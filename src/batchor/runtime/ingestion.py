"""Internal helpers for item-source materialization and resume-aware ingestion."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator, Protocol, cast

from pydantic import BaseModel

from batchor.core.enums import RunControlState
from batchor.core.exceptions import RunIngestionSourceRequiredError, RunSubmissionIndeterminateError
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


def _noop_poll_active_batches(
    run_id: str,
    context: RunContext,
    *,
    deadline: float | None = None,
) -> None:
    del run_id, context, deadline
    return


class ActiveBatchPoller(Protocol):
    """Callable seam for reconciling active batches during ingestion."""

    def __call__(
        self,
        run_id: str,
        context: RunContext,
        *,
        deadline: float | None = None,
    ) -> None: ...


@dataclass(frozen=True)
class IngestionDeps:
    """Dependencies needed by the ingestion and resume layer.

    Attributes:
        state: Durable state store used for ingest checkpoints and items.
        emit_event: Observer callback used for coarse lifecycle events.
        submit_pending_items: Callback used to submit items during ingestion.
        poll_active_batches: Callback used to reconcile already-submitted
            batches before resume or durable ingestion chunks submit new local
            work.
        monotonic: Monotonic clock used to enforce ingestion polling cadence.
        configs_match_for_resume: Predicate used to validate resume
            compatibility.
    """

    state: StateStore
    emit_event: Callable[..., None]
    submit_pending_items: Callable[[str, RunContext], int]
    configs_match_for_resume: Callable[[PersistedRunConfig, PersistedRunConfig], bool]
    poll_active_batches: ActiveBatchPoller = _noop_poll_active_batches
    monotonic: Callable[[], float] = time.monotonic
    work_slice_max_items: int = 1000
    work_slice_max_seconds: float | None = None
    work_monotonic: Callable[[], float] = time.monotonic
    non_checkpointed_sessions: dict[str, NonCheckpointedIngestionSession] = field(default_factory=dict)


@dataclass
class NonCheckpointedIngestionSession:
    """In-process cursor for an arbitrary iterable paused at a safe boundary.

    This is deliberately not durable: arbitrary iterables cannot be replayed
    in a fresh process.  It only lets a manual pause stop before consuming the
    next source item and lets the same execution owner continue later.
    """

    iterator: Iterator[Any]
    next_item_index: int
    seen_ids: set[str] = field(default_factory=set)


def finalize_cancelled_ingestion(state: StateStore, *, run_id: str) -> None:
    """Abandon an incomplete source tail after cancellation has drained."""
    checkpoint = state.get_ingest_checkpoint(run_id=run_id)
    if checkpoint is None or checkpoint.ingestion_complete:
        return
    state.update_ingest_checkpoint(
        run_id=run_id,
        next_item_index=checkpoint.next_item_index,
        checkpoint_payload=checkpoint.checkpoint_payload,
        ingestion_complete=True,
    )


def resume_existing_run(
    deps: IngestionDeps,
    *,
    run_id: str,
    job: BatchJob[Any, BaseModel],
    config: PersistedRunConfig,
    context: RunContext,
    deadline: float | None = None,
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
    if deps.state.has_indeterminate_submission_intents(run_id=run_id):
        raise RunSubmissionIndeterminateError(run_id)
    deps.state.requeue_local_items(run_id=run_id)
    control_state = deps.state.get_run_control_state(run_id=run_id)
    if control_state is RunControlState.CANCEL_REQUESTED:
        return
    if _deadline_reached(deps, deadline):
        return
    if control_state is RunControlState.RUNNING and deps.state.get_active_batches(run_id=run_id):
        _poll_active_batches(deps, run_id=run_id, context=context, deadline=deadline)
        if _deadline_reached(deps, deadline):
            return
        summary = deps.state.get_run_summary(run_id=run_id)
        if summary.backoff_remaining_sec > 0:
            return
        control_state = deps.state.get_run_control_state(run_id=run_id)
        if control_state is RunControlState.CANCEL_REQUESTED:
            return
    if deps.state.get_batch_retry_backoff_remaining_sec(run_id=run_id) > 0:
        return
    checkpoint = deps.state.get_ingest_checkpoint(run_id=run_id)
    if checkpoint is not None and not checkpoint.ingestion_complete:
        if control_state is RunControlState.PAUSED:
            return
        source = checkpointed_source(job)
        if source is None:
            if run_id not in deps.non_checkpointed_sessions and checkpoint.next_item_index != 0:
                raise RunIngestionSourceRequiredError(run_id)
            ingest_job_items(
                deps,
                run_id=run_id,
                job=job,
                context=context,
                start_index=checkpoint.next_item_index,
                checkpoint_payload=None,
                deadline=deadline,
            )
            return
        validate_checkpoint_source(run_id=run_id, source=source, checkpoint=checkpoint)
        if checkpoint.checkpoint_payload is not None and source.checkpoint_is_complete(checkpoint.checkpoint_payload):
            deps.state.update_ingest_checkpoint(
                run_id=run_id,
                next_item_index=checkpoint.next_item_index,
                checkpoint_payload=checkpoint.checkpoint_payload,
                ingestion_complete=True,
            )
            if _deadline_reached(deps, deadline):
                return
            deps.submit_pending_items(run_id, context)
            return
        ingest_job_items(
            deps,
            run_id=run_id,
            job=job,
            context=context,
            start_index=checkpoint.next_item_index,
            checkpoint_payload=checkpoint.checkpoint_payload,
            deadline=deadline,
        )
        return
    if control_state is RunControlState.PAUSED:
        return
    if _deadline_reached(deps, deadline):
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
    deadline: float | None = None,
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
    if deps.work_slice_max_items <= 0:
        raise ValueError("work_slice_max_items must be > 0")
    if deps.work_slice_max_seconds is not None and deps.work_slice_max_seconds <= 0:
        raise ValueError("work_slice_max_seconds must be > 0 when provided")

    source = checkpointed_source(job)
    if source is not None:
        _ingest_checkpointed_items(
            deps,
            run_id=run_id,
            job=job,
            context=context,
            source=source,
            start_index=start_index,
            checkpoint_payload=checkpoint_payload,
            deadline=deadline,
        )
        return
    _ingest_non_checkpointed_items(
        deps,
        run_id=run_id,
        job=job,
        context=context,
        start_index=start_index,
        deadline=deadline,
    )


def _ingest_checkpointed_items(
    deps: IngestionDeps,
    *,
    run_id: str,
    job: BatchJob[Any, BaseModel],
    context: RunContext,
    source: CheckpointedItemSource[Any],
    start_index: int,
    checkpoint_payload: JSONValue | None,
    deadline: float | None,
) -> None:
    """Ingest a replayable source in small, atomically checkpointed slices."""
    next_item_index = start_index
    next_checkpoint_payload = checkpoint_payload
    source_checkpoint = checkpoint_payload if checkpoint_payload is not None else source.initial_checkpoint()
    item_chunk: list[MaterializedItem] = []
    seen_ids: set[str] = set()
    last_poll_at = deps.monotonic()
    slice_started_at = _next_slice_started_at(deps) if deps.work_slice_max_seconds is not None else last_poll_at
    complete = True
    for source_item in source.iter_from_checkpoint(source_checkpoint):
        if _deadline_reached(deps, deadline):
            complete = False
            break
        item_chunk.append(_materialize_item(job, source_item.item, next_item_index, seen_ids))
        next_item_index += 1
        next_checkpoint_payload = source_item.next_checkpoint
        if not _slice_due(
            deps, item_count=len(item_chunk), slice_started_at=slice_started_at
        ) and not _cooperative_boundary_due(
            deps,
            run_id=run_id,
            item_count=len(item_chunk),
        ):
            continue
        should_continue, last_poll_at = _persist_and_advance_boundary(
            deps,
            run_id=run_id,
            context=context,
            item_chunk=item_chunk,
            next_item_index=next_item_index,
            checkpoint_payload=next_checkpoint_payload,
            checkpointed=True,
            last_poll_at=last_poll_at,
            deadline=deadline,
        )
        item_chunk = []
        slice_started_at = _next_slice_started_at(deps)
        if not should_continue:
            complete = False
            break
    if complete and item_chunk:
        should_continue, _ = _persist_and_advance_boundary(
            deps,
            run_id=run_id,
            context=context,
            item_chunk=item_chunk,
            next_item_index=next_item_index,
            checkpoint_payload=next_checkpoint_payload,
            checkpointed=True,
            last_poll_at=last_poll_at,
            deadline=deadline,
        )
        if not should_continue:
            complete = False
    deps.state.update_ingest_checkpoint(
        run_id=run_id,
        next_item_index=next_item_index,
        checkpoint_payload=next_checkpoint_payload,
        ingestion_complete=complete,
    )


def _ingest_non_checkpointed_items(
    deps: IngestionDeps,
    *,
    run_id: str,
    job: BatchJob[Any, BaseModel],
    context: RunContext,
    start_index: int,
    deadline: float | None,
) -> None:
    """Ingest an arbitrary iterable, retaining its cursor across manual pause."""
    session = deps.non_checkpointed_sessions.get(run_id)
    if session is None:
        if start_index != 0:
            raise ValueError("non-resumable item sources cannot start from a checkpoint")
        session = NonCheckpointedIngestionSession(iterator=iter(job.items), next_item_index=0)
        deps.non_checkpointed_sessions[run_id] = session
    item_chunk: list[MaterializedItem] = []
    last_poll_at = deps.monotonic()
    slice_started_at = _next_slice_started_at(deps) if deps.work_slice_max_seconds is not None else last_poll_at
    exhausted = False
    while True:
        if _deadline_reached(deps, deadline):
            break
        try:
            item = next(session.iterator)
        except StopIteration:
            exhausted = True
            break
        item_chunk.append(_materialize_item(job, item, session.next_item_index, session.seen_ids))
        session.next_item_index += 1
        if not _slice_due(
            deps, item_count=len(item_chunk), slice_started_at=slice_started_at
        ) and not _cooperative_boundary_due(
            deps,
            run_id=run_id,
            item_count=len(item_chunk),
        ):
            continue
        should_continue, last_poll_at = _persist_and_advance_boundary(
            deps,
            run_id=run_id,
            context=context,
            item_chunk=item_chunk,
            next_item_index=session.next_item_index,
            checkpoint_payload=None,
            checkpointed=False,
            last_poll_at=last_poll_at,
            deadline=deadline,
        )
        item_chunk = []
        slice_started_at = _next_slice_started_at(deps)
        if not should_continue:
            break
    if item_chunk:
        should_continue, _ = _persist_and_advance_boundary(
            deps,
            run_id=run_id,
            context=context,
            item_chunk=item_chunk,
            next_item_index=session.next_item_index,
            checkpoint_payload=None,
            checkpointed=False,
            last_poll_at=last_poll_at,
            deadline=deadline,
        )
        if not should_continue:
            exhausted = False
    deps.state.update_ingest_checkpoint(
        run_id=run_id,
        next_item_index=session.next_item_index,
        checkpoint_payload=None,
        ingestion_complete=exhausted,
    )
    if exhausted or deps.state.get_run_control_state(run_id=run_id) is RunControlState.CANCEL_REQUESTED:
        deps.non_checkpointed_sessions.pop(run_id, None)


def _materialize_item(
    job: BatchJob[Any, BaseModel], item: Any, item_index: int, seen_ids: set[str]
) -> MaterializedItem:
    if item.item_id in seen_ids:
        raise ValueError(f"duplicate item_id: {item.item_id}")
    seen_ids.add(item.item_id)
    prompt_parts = normalize_prompt_parts(job.build_prompt(item))
    return MaterializedItem(
        item_id=item.item_id,
        item_index=item_index,
        payload=json_value(item.payload, label=f"payload for {item.item_id}"),
        metadata=json_object(item.metadata, label=f"metadata for {item.item_id}"),
        prompt=prompt_parts.prompt,
        system_prompt=prompt_parts.system_prompt,
    )


def _slice_due(deps: IngestionDeps, *, item_count: int, slice_started_at: float) -> bool:
    if item_count >= deps.work_slice_max_items:
        return True
    return (
        deps.work_slice_max_seconds is not None
        and deps.work_monotonic() - slice_started_at >= deps.work_slice_max_seconds
    )


def _next_slice_started_at(deps: IngestionDeps) -> float:
    """Avoid unnecessary clock reads when only an item budget is active."""
    return deps.work_monotonic() if deps.work_slice_max_seconds is not None else 0.0


def _cooperative_boundary_due(
    deps: IngestionDeps,
    *,
    run_id: str,
    item_count: int,
) -> bool:
    """Observe control/poll cadence inside a large fast-item slice.

    The normal path stays at the configured item chunk size.  This only
    creates a partial durable append when a manual control change or
    cancellation needs it. Slow source work is independently bounded by the time slice,
    which then runs the normal active-batch poll cadence.
    """
    if deps.work_slice_max_seconds is None or item_count % 100 != 0:
        return False
    control_state = deps.state.get_run_control_state(run_id=run_id)
    return control_state is RunControlState.CANCEL_REQUESTED or _is_manual_pause(
        deps, run_id=run_id, control_state=control_state
    )


def _deadline_reached(deps: IngestionDeps, deadline: float | None) -> bool:
    return deadline is not None and deps.monotonic() >= deadline


def _is_manual_pause(
    deps: IngestionDeps,
    *,
    run_id: str,
    control_state: RunControlState | None = None,
) -> bool:
    if control_state is None:
        control_state = deps.state.get_run_control_state(run_id=run_id)
    if control_state is not RunControlState.PAUSED:
        return False
    summary = deps.state.get_run_summary(run_id=run_id)
    return summary.control_reason == "manual"


def _persist_and_advance_boundary(
    deps: IngestionDeps,
    *,
    run_id: str,
    context: RunContext,
    item_chunk: list[MaterializedItem],
    next_item_index: int,
    checkpoint_payload: JSONValue | None,
    checkpointed: bool,
    last_poll_at: float,
    deadline: float | None,
) -> tuple[bool, float]:
    """Durably append one slice, reconcile active work, then submit if allowed."""
    deps.state.append_items_with_ingest_checkpoint(
        run_id=run_id,
        items=item_chunk,
        next_item_index=next_item_index,
        checkpoint_payload=checkpoint_payload,
        ingestion_complete=False,
    )
    deps.emit_event(
        "items_ingested",
        run_id=run_id,
        provider_kind=context.config.provider_config.provider_kind,
        data={"chunk_item_count": len(item_chunk), "last_item_index": item_chunk[-1].item_index},
    )
    if _deadline_reached(deps, deadline):
        return False, last_poll_at
    control_state = deps.state.get_run_control_state(run_id=run_id)
    if control_state is RunControlState.CANCEL_REQUESTED or _is_manual_pause(
        deps, run_id=run_id, control_state=control_state
    ):
        return False, last_poll_at
    # Submission retry backoff never suppresses reconciliation of active work.
    active_batches = deps.state.get_active_batches(run_id=run_id)
    now = deps.monotonic() if active_batches else last_poll_at
    if (
        control_state is RunControlState.RUNNING
        and active_batches
        and now - last_poll_at >= context.config.provider_config.poll_interval_sec
    ):
        if _deadline_reached(deps, deadline):
            return False, last_poll_at
        _poll_active_batches(deps, run_id=run_id, context=context, deadline=deadline)
        last_poll_at = now
        if _deadline_reached(deps, deadline):
            return False, last_poll_at
        control_state = deps.state.get_run_control_state(run_id=run_id)
        if control_state is RunControlState.CANCEL_REQUESTED or _is_manual_pause(
            deps, run_id=run_id, control_state=control_state
        ):
            return False, last_poll_at
    if deps.state.get_batch_retry_backoff_remaining_sec(run_id=run_id) > 0:
        return False, last_poll_at
    if control_state is RunControlState.RUNNING:
        if _deadline_reached(deps, deadline):
            return False, last_poll_at
        deps.submit_pending_items(run_id, context)
    control_state = deps.state.get_run_control_state(run_id=run_id)
    if control_state is RunControlState.CANCEL_REQUESTED or _is_manual_pause(
        deps, run_id=run_id, control_state=control_state
    ):
        return False, last_poll_at
    if checkpointed and control_state is not RunControlState.RUNNING:
        return False, last_poll_at
    return True, last_poll_at


def _poll_active_batches(
    deps: IngestionDeps,
    *,
    run_id: str,
    context: RunContext,
    deadline: float | None,
) -> None:
    """Reconcile active batches without dropping a wait-cycle deadline.

    The optional deadline keeps older internal test adapters usable for
    ordinary ingestion while ensuring deadline-bound refreshes pass the same
    absolute limit down to the provider-polling seam.
    """
    if deadline is None:
        deps.poll_active_batches(run_id, context)
        return
    deps.poll_active_batches(run_id, context, deadline=deadline)


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
