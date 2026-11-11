"""Internal helpers for pending-item preparation and batch submission."""

from __future__ import annotations

import json
from contextlib import ExitStack
from dataclasses import dataclass
from time import monotonic
from typing import Any, Callable, Protocol, cast
from uuid import uuid4

from batchor.artifacts import ArtifactStore
from batchor.core.enums import RunControlState
from batchor.core.exceptions import RunSubmissionIndeterminateError
from batchor.core.models import ItemFailure, OpenAIProviderConfig, PromptParts, openai_enqueue_quota_scope
from batchor.core.types import BatchRequestLine, JSONObject
from batchor.runtime.artifacts import (
    RequestArtifactCache,
    load_request_artifact_line,
    request_artifact_relative_path,
    request_sha256,
    serialize_jsonl,
)
from batchor.runtime.context import RunContext
from batchor.runtime.retry import (
    classify_batch_error,
    is_enqueue_token_limit_error,
    is_insufficient_quota_error,
)
from batchor.runtime.tokens import (
    chunk_request_rows,
    effective_inflight_token_budget,
    resolve_openai_batch_token_limit,
    split_rows_by_token_limit,
)
from batchor.storage.state import (
    ClaimedItem,
    PreparedSubmission,
    QueuedItemFailureRecord,
    RequestArtifactPointer,
    StateStore,
)


class PendingItemSubmitter(Protocol):
    """Submission callback that carries a cooperative execution deadline."""

    def __call__(self, run_id: str, context: RunContext, *, deadline: float | None = None) -> int: ...


def submit_with_deadline(
    submit: PendingItemSubmitter, run_id: str, context: RunContext, *, deadline: float | None
) -> int:
    # Keep ordinary calls compatible with simple internal/test callbacks.
    if deadline is None:
        return submit(run_id, context)
    if monotonic() >= deadline:
        return 0
    return submit(run_id, context, deadline=deadline)


def _deadline_expired(deadline: float | None) -> bool:
    return deadline is not None and monotonic() >= deadline


@dataclass(frozen=True)
class PreparedRequest:
    """Prepared request payload ready for chunking and submission.

    Attributes:
        item_id: Durable item identifier.
        custom_id: Provider-facing custom identifier for the attempt.
        request_line: Provider request JSON object.
        request_bytes: Serialized request size in bytes.
        submission_tokens: Estimated token count used for budgeting.
    """

    item_id: str
    custom_id: str
    request_line: JSONObject
    request_bytes: int
    submission_tokens: int


@dataclass(frozen=True)
class SubmissionDeps:
    """Dependencies needed by the submission layer.

    Attributes:
        state: Durable state store used for claiming and transitions.
        artifact_store: Artifact store used for request replay and persistence.
        emit_event: Observer callback used for coarse lifecycle events.
    """

    state: StateStore
    artifact_store: ArtifactStore
    emit_event: Callable[..., None]


def submit_pending_items(
    deps: SubmissionDeps,
    *,
    run_id: str,
    context: RunContext,
    deadline: float | None = None,
) -> int:
    """Claim pending items, prepare request artifacts, and submit new batches.

    Args:
        deps: Submission-layer dependencies.
        run_id: Durable run identifier.
        context: Runtime context for the run.

    Returns:
        Number of items submitted in this cycle.
    """
    config = context.config
    if _deadline_expired(deadline):
        return 0
    if deps.state.get_run_control_state(run_id=run_id) is not RunControlState.RUNNING:
        return 0
    if deps.state.get_batch_retry_backoff_remaining_sec(run_id=run_id) > 0:
        return 0
    if deps.state.has_indeterminate_submission_intents(run_id=run_id):
        raise RunSubmissionIndeterminateError(run_id)
    claim_limit = submission_claim_limit(config)
    claimed = deps.state.claim_items_for_submission(
        run_id=run_id,
        max_attempts=config.retry_policy.max_attempts,
        limit=claim_limit,
    )
    if not claimed:
        return 0
    protected_item_ids: set[str] = set()
    try:
        deps.emit_event(
            "items_claimed_for_submission",
            run_id=run_id,
            provider_kind=config.provider_config.provider_kind,
            data={"claimed_item_count": len(claimed)},
        )
        return _submit_claimed_items(
            deps,
            run_id=run_id,
            context=context,
            claimed=claimed,
            protected_item_ids=protected_item_ids,
            deadline=deadline,
        )
    finally:
        # Release only this cycle's claims. Once create may have reached the
        # provider, its intent must remain protected until explicit resolution.
        deps.state.release_items_to_pending(
            run_id=run_id,
            item_ids=[item.item_id for item in claimed if item.item_id not in protected_item_ids],
        )


def _submit_claimed_items(
    deps: SubmissionDeps,
    *,
    run_id: str,
    context: RunContext,
    claimed: list[ClaimedItem],
    protected_item_ids: set[str],
    deadline: float | None,
) -> int:
    config = context.config
    artifact_cache = RequestArtifactCache()
    prepared_items = []
    for item in claimed:
        if _deadline_expired(deadline):
            return 0
        prepared = prepare_claimed_item(
            item,
            context=context,
            artifact_store=deps.artifact_store,
            artifact_cache=artifact_cache,
        )
        if prepared.request_bytes > config.chunk_policy.max_file_bytes:
            deps.state.mark_queued_items_failed(
                run_id=run_id,
                failures=[
                    QueuedItemFailureRecord(
                        item_id=item.item_id,
                        error=ItemFailure(
                            error_class="request_exceeds_max_file_bytes",
                            message="serialized request exceeds chunk_policy.max_file_bytes",
                            retryable=False,
                            raw_error={
                                "request_bytes": prepared.request_bytes,
                                "limit": config.chunk_policy.max_file_bytes,
                            },
                        ),
                        count_attempt=False,
                    )
                ],
                max_attempts=config.retry_policy.max_attempts,
            )
        else:
            prepared_items.append(prepared)
    batch_token_limit = batch_token_limit_for_provider(config.provider_config)
    inflight_budget = inflight_budget_for_provider(config.provider_config)
    if batch_token_limit is not None:
        within_limit_rows, oversized_rows = split_rows_by_token_limit(
            [prepared_request_row(item) for item in prepared_items],
            token_limit=batch_token_limit,
            token_field="submission_tokens",
        )
        prepared_rows = within_limit_rows
        if oversized_rows:
            deps.state.mark_queued_items_failed(
                run_id=run_id,
                failures=[
                    QueuedItemFailureRecord(
                        item_id=str(row["item_id"]),
                        error=oversized_request_failure(
                            provider_config=config.provider_config,
                            limit_type="batch",
                            submission_tokens=int(row["submission_tokens"]),
                            limit=batch_token_limit,
                        ),
                        count_attempt=False,
                    )
                    for row in oversized_rows
                ],
                max_attempts=config.retry_policy.max_attempts,
            )
    else:
        prepared_rows = [prepared_request_row(item) for item in prepared_items]

    if inflight_budget is not None:
        inflight_safe_rows, oversized_rows = split_rows_by_token_limit(
            prepared_rows,
            token_limit=inflight_budget,
            token_field="submission_tokens",
        )
        prepared_rows = inflight_safe_rows
        if oversized_rows:
            deps.state.mark_queued_items_failed(
                run_id=run_id,
                failures=[
                    QueuedItemFailureRecord(
                        item_id=str(row["item_id"]),
                        error=oversized_request_failure(
                            provider_config=config.provider_config,
                            limit_type="inflight",
                            submission_tokens=int(row["submission_tokens"]),
                            limit=inflight_budget,
                        ),
                        count_attempt=False,
                    )
                    for row in oversized_rows
                ],
                max_attempts=config.retry_policy.max_attempts,
            )

    if not prepared_rows:
        return 0

    chunks = chunk_request_rows(
        prepared_rows,
        chunk_policy=config.chunk_policy,
        max_tokens=batch_token_limit,
        estimate_row_bytes=lambda row: int(row["request_bytes"]),
        estimate_row_tokens=lambda row: int(row["submission_tokens"]),
    )
    submitted_count = 0
    for chunk in chunks:
        if not _submission_allowed(deps, run_id=run_id, deadline=deadline):
            break
        if not _submit_chunk(
            deps,
            run_id=run_id,
            context=context,
            chunk=chunk,
            inflight_budget=inflight_budget,
            protected_item_ids=protected_item_ids,
            deadline=deadline,
        ):
            break
        submitted_count += len(chunk)
    return submitted_count


def _submission_allowed(deps: SubmissionDeps, *, run_id: str, deadline: float | None) -> bool:
    return (
        not _deadline_expired(deadline) and deps.state.get_run_control_state(run_id=run_id) is RunControlState.RUNNING
    )


def _submit_chunk(
    deps: SubmissionDeps,
    *,
    run_id: str,
    context: RunContext,
    chunk: list[dict[str, Any]],
    inflight_budget: int | None,
    protected_item_ids: set[str],
    deadline: float | None,
) -> bool:
    """Reserve, persist, upload, create, and finalize one submission intent."""
    config = context.config
    submissions = [
        PreparedSubmission(
            item_id=str(row["item_id"]),
            custom_id=str(row["custom_id"]),
            submission_tokens=int(row["submission_tokens"]),
        )
        for row in chunk
    ]
    chunk_tokens = sum(item.submission_tokens for item in submissions)
    intent_id = f"submission-{uuid4().hex}"
    if not deps.state.begin_submission_intent(
        run_id=run_id,
        intent_id=intent_id,
        submissions=submissions,
        quota_scope=submission_quota_scope(config.provider_config) if inflight_budget is not None else None,
        submission_tokens=chunk_tokens,
        capacity_limit=inflight_budget,
    ):
        return False

    create_started = False
    try:
        if not _submission_allowed(deps, run_id=run_id, deadline=deadline):
            return False
        request_path = request_artifact_relative_path(run_id).as_posix()
        deps.artifact_store.write_text(
            request_path,
            serialize_jsonl([cast(JSONObject, row["request_line"]) for row in chunk]),
            encoding="utf-8",
        )
        deps.state.record_request_artifacts(
            run_id=run_id,
            pointers=[
                RequestArtifactPointer(
                    item_id=str(row["item_id"]),
                    artifact_path=request_path,
                    line_number=line_number,
                    request_sha256=request_sha256(cast(JSONObject, row["request_line"])),
                )
                for line_number, row in enumerate(chunk, start=1)
            ],
        )
        if not _submission_allowed(deps, run_id=run_id, deadline=deadline):
            return False
        with ExitStack() as stack:
            request_file = stack.enter_context(deps.artifact_store.stage_local_copy(request_path))
            if not _submission_allowed(deps, run_id=run_id, deadline=deadline):
                return False
            try:
                remote_input_file_id = context.provider.upload_input_file(request_file)
            except Exception as exc:  # noqa: BLE001
                if not is_insufficient_quota_error(exc):
                    raise
                pause_run_for_insufficient_quota(deps, run_id=run_id, context=context, error=exc, phase="batch_upload")
                return False
            if not _submission_allowed(deps, run_id=run_id, deadline=deadline):
                cleanup_uploaded_input_file(context, remote_input_file_id)
                return False
            intent_item_ids = {item.item_id for item in submissions}
            create_started = True
            protected_item_ids.update(intent_item_ids)
            try:
                batch = context.provider.create_batch(
                    input_file_id=remote_input_file_id,
                    metadata={**config.batch_metadata, "run_id": run_id},
                )
            except Exception as exc:  # noqa: BLE001
                quota_error = is_insufficient_quota_error(exc)
                if not quota_error and not is_enqueue_token_limit_error(exc):
                    # Generic providers cannot prove a failed create did not
                    # create remote work. Keep its intent for operator recovery.
                    raise RunSubmissionIndeterminateError(run_id) from exc
                create_started = False
                protected_item_ids.difference_update(intent_item_ids)
                cleanup_uploaded_input_file(context, remote_input_file_id)
                if quota_error:
                    pause_run_for_insufficient_quota(
                        deps, run_id=run_id, context=context, error=exc, phase="batch_create"
                    )
                else:
                    error_class = classify_batch_error(exc)
                    deps.state.record_batch_retry_failure(
                        run_id=run_id,
                        error_class=error_class,
                        base_delay_sec=config.retry_policy.base_backoff_sec,
                        max_delay_sec=config.retry_policy.max_backoff_sec,
                    )
                    deps.emit_event(
                        "batch_submit_retry",
                        run_id=run_id,
                        provider_kind=config.provider_config.provider_kind,
                        data={"error_class": error_class},
                    )
                return False

        # Finish local linkage even if create used up the deadline. Forgetting
        # a confirmed remote batch would turn a timeout into duplicate work.
        provider_batch_id = str(batch["id"])
        deps.state.finalize_submission_intent(
            run_id=run_id,
            intent_id=intent_id,
            local_batch_id=f"batch-{uuid4().hex[:12]}",
            provider_batch_id=provider_batch_id,
            status=str(batch.get("status", "submitted")),
            custom_ids=[item.custom_id for item in submissions],
            submissions=submissions,
        )
        deps.emit_event(
            "batch_submitted",
            run_id=run_id,
            provider_kind=config.provider_config.provider_kind,
            data={
                "provider_batch_id": provider_batch_id,
                "submitted_item_count": len(chunk),
                "submission_tokens": chunk_tokens,
            },
        )
        return True
    finally:
        if not create_started:
            deps.state.abandon_submission_intent(intent_id=intent_id)


def submission_quota_scope(provider_config: object) -> str:
    """Return the durable capacity scope for an OpenAI submission."""
    if not isinstance(provider_config, OpenAIProviderConfig):
        raise TypeError("OpenAI enqueue capacity requires OpenAIProviderConfig")
    return openai_enqueue_quota_scope(provider_config)


def pause_run_for_insufficient_quota(
    deps: SubmissionDeps,
    *,
    run_id: str,
    context: RunContext,
    error: object,
    phase: str,
) -> bool:
    if deps.state.get_run_control_state(run_id=run_id) is RunControlState.CANCEL_REQUESTED:
        return False
    deps.state.clear_batch_retry_backoff(run_id=run_id)
    deps.state.set_run_control_state(
        run_id=run_id,
        control_state=RunControlState.PAUSED,
        control_reason="openai_insufficient_quota",
    )
    deps.emit_event(
        "run_auto_paused",
        run_id=run_id,
        provider_kind=context.config.provider_config.provider_kind,
        data={
            "control_reason": "openai_insufficient_quota",
            "error_class": classify_batch_error(error),
            "phase": phase,
        },
    )
    return True


def prepare_claimed_item(
    item: ClaimedItem,
    *,
    context: RunContext,
    artifact_store: ArtifactStore,
    artifact_cache: RequestArtifactCache | None = None,
) -> PreparedRequest:
    """Build or replay the provider request line for a claimed item.

    Args:
        item: Claimed item record from storage.
        context: Runtime context for the run.
        artifact_store: Artifact store used for replaying persisted requests.
        artifact_cache: Optional per-cycle replay cache.

    Returns:
        Prepared request payload for chunking and submission.

    Raises:
        ValueError: If a persisted request artifact pointer is incomplete.
    """
    custom_id = make_custom_id(item.item_id, item.attempt_count + 1)
    if item.request_artifact_path is not None:
        if item.request_artifact_line is None or item.request_sha256 is None:
            raise ValueError(f"incomplete request artifact pointer for item {item.item_id}")
        request_line = context.provider.with_request_correlation_id(
            cast(
                BatchRequestLine,
                load_request_artifact_line(
                    artifact_store=artifact_store,
                    artifact_path=item.request_artifact_path,
                    line_number=item.request_artifact_line,
                    expected_sha256=item.request_sha256,
                    artifact_cache=artifact_cache,
                ),
            ),
            custom_id,
        )
    else:
        request_line = context.provider.build_request_line(
            custom_id=custom_id,
            prompt_parts=PromptParts(
                prompt=item.prompt,
                system_prompt=item.system_prompt,
            ),
            structured_output=context.structured_output,
        )
    request_bytes = len((json.dumps(request_line, ensure_ascii=False) + "\n").encode("utf-8"))
    submission_tokens = context.provider.estimate_request_tokens(
        request_line,
        chars_per_token=context.config.chunk_policy.chars_per_token,
    )
    return PreparedRequest(
        item_id=item.item_id,
        custom_id=context.provider.request_correlation_id(request_line),
        request_line=cast(JSONObject, request_line),
        request_bytes=request_bytes,
        submission_tokens=submission_tokens,
    )


def prepared_request_row(item: PreparedRequest) -> dict[str, Any]:
    """Return the chunking row used by the submission pipeline.

    Args:
        item: Prepared request payload.

    Returns:
        Dictionary form used by chunking and budgeting helpers.
    """
    return {
        "item_id": item.item_id,
        "custom_id": item.custom_id,
        "request_line": item.request_line,
        "request_bytes": item.request_bytes,
        "submission_tokens": item.submission_tokens,
    }


def make_custom_id(item_id: str, attempt: int) -> str:
    """Return the stable provider custom ID for an item attempt.

    Args:
        item_id: Durable item identifier.
        attempt: One-based attempt number.

    Returns:
        Provider-facing custom ID for the attempt.
    """
    return f"{item_id}:a{attempt}"


def cleanup_uploaded_input_file(context: RunContext, file_id: str) -> None:
    """Delete an uploaded provider input file on a best-effort basis.

    Args:
        context: Runtime context containing the provider.
        file_id: Provider file identifier to delete.
    """
    try:
        context.provider.delete_input_file(file_id)
    except Exception:  # noqa: BLE001
        return


def submission_claim_limit(config: Any) -> int:
    """Return the number of items to claim in one submission cycle.

    Args:
        config: Persisted run config with a chunk policy.

    Returns:
        Claim limit used for one submission cycle.
    """
    max_requests = int(config.chunk_policy.max_requests)
    return max(1, min(max_requests * 5, 8_192))


def inflight_budget_for_provider(provider_config: Any) -> int | None:
    """Return the effective inflight token budget for OpenAI configs.

    Args:
        provider_config: Provider configuration to inspect.

    Returns:
        Effective inflight token budget, or ``None`` when not applicable.
    """
    if not isinstance(provider_config, OpenAIProviderConfig):
        return None
    return effective_inflight_token_budget(provider_config.enqueue_limits)


def batch_token_limit_for_provider(provider_config: Any) -> int | None:
    """Return the per-batch token ceiling for OpenAI configs.

    Args:
        provider_config: Provider configuration to inspect.

    Returns:
        Per-batch token ceiling, or ``None`` when not applicable.
    """
    if not isinstance(provider_config, OpenAIProviderConfig):
        return None
    return resolve_openai_batch_token_limit(provider_config.enqueue_limits)


def oversized_request_failure(
    *,
    provider_config: Any,
    limit_type: str,
    submission_tokens: int,
    limit: int,
) -> ItemFailure:
    """Build the failure recorded for an item that exceeds token limits.

    Args:
        provider_config: Provider configuration used for naming the error.
        limit_type: Limit category, such as ``"batch"`` or ``"inflight"``.
        submission_tokens: Estimated submitted token count for the item.
        limit: Token ceiling that was exceeded.

    Returns:
        Item failure payload recorded in durable state.
    """
    provider_name = "provider"
    if isinstance(provider_config, OpenAIProviderConfig):
        provider_name = provider_config.provider_kind.value
    if limit_type == "batch":
        error_class = f"{provider_name}_request_exceeds_batch_token_limit"
        message = f"request token estimate exceeds the provider batch token limit ({submission_tokens} > {limit})"
    else:
        error_class = f"{provider_name}_request_exceeds_inflight_token_limit"
        message = f"request token estimate exceeds the provider inflight token budget ({submission_tokens} > {limit})"
    return ItemFailure(
        error_class=error_class,
        message=message,
        raw_error={
            "submission_tokens": submission_tokens,
            "limit": limit,
            "limit_type": limit_type,
        },
        retryable=False,
    )
