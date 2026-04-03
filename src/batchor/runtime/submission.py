"""Internal helpers for pending-item preparation and batch submission."""

from __future__ import annotations

import json
from contextlib import ExitStack
from dataclasses import dataclass
from typing import Any, Callable, cast
from uuid import uuid4

from batchor.artifacts import ArtifactStore
from batchor.core.enums import RunControlState
from batchor.core.models import ItemFailure, OpenAIProviderConfig, PromptParts
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
    is_retryable_batch_control_plane_error,
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
    if deps.state.get_run_control_state(run_id=run_id) is not RunControlState.RUNNING:
        return 0
    if deps.state.get_batch_retry_backoff_remaining_sec(run_id=run_id) > 0:
        return 0
    claim_limit = submission_claim_limit(config)
    claimed = deps.state.claim_items_for_submission(
        run_id=run_id,
        max_attempts=config.retry_policy.max_attempts,
        limit=claim_limit,
    )
    if not claimed:
        return 0
    deps.emit_event(
        "items_claimed_for_submission",
        run_id=run_id,
        provider_kind=context.config.provider_config.provider_kind,
        data={"claimed_item_count": len(claimed)},
    )

    artifact_cache = RequestArtifactCache()
    prepared_items = [
        prepare_claimed_item(
            item,
            context=context,
            artifact_store=deps.artifact_store,
            artifact_cache=artifact_cache,
        )
        for item in claimed
    ]
    batch_token_limit = batch_token_limit_for_provider(config.provider_config)
    inflight_budget = inflight_budget_for_provider(config.provider_config)
    failed_item_ids: set[str] = set()
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
            failed_item_ids.update(str(row["item_id"]) for row in oversized_rows)
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
            failed_item_ids.update(str(row["item_id"]) for row in oversized_rows)

    if not prepared_rows:
        unsent = [item.item_id for item in claimed if item.item_id not in failed_item_ids]
        if unsent:
            deps.state.release_items_to_pending(run_id=run_id, item_ids=unsent)
        return 0

    chunks = chunk_request_rows(
        prepared_rows,
        chunk_policy=config.chunk_policy,
        max_tokens=batch_token_limit,
        estimate_row_bytes=lambda row: int(row["request_bytes"]),
        estimate_row_tokens=lambda row: int(row["submission_tokens"]),
    )
    submitted_item_ids: set[str] = set()
    submitted_count = 0
    active_tokens = deps.state.get_active_submitted_token_estimate(run_id=run_id) if inflight_budget is not None else 0

    for chunk_index, chunk in enumerate(chunks, start=1):
        if deps.state.get_run_control_state(run_id=run_id) is not RunControlState.RUNNING:
            break
        chunk_tokens = sum(int(row["submission_tokens"]) for row in chunk)
        if inflight_budget is not None and chunk_tokens > max(inflight_budget - active_tokens, 0):
            break
        request_lines = [cast(JSONObject, row["request_line"]) for row in chunk]
        request_relative_path = request_artifact_relative_path(run_id)
        deps.artifact_store.write_text(
            request_relative_path.as_posix(),
            serialize_jsonl(cast(list[JSONObject], request_lines)),
            encoding="utf-8",
        )
        deps.state.record_request_artifacts(
            run_id=run_id,
            pointers=[
                RequestArtifactPointer(
                    item_id=str(row["item_id"]),
                    artifact_path=request_relative_path.as_posix(),
                    line_number=line_number,
                    request_sha256=request_sha256(cast(JSONObject, row["request_line"])),
                )
                for line_number, row in enumerate(chunk, start=1)
            ],
        )
        if deps.state.get_run_control_state(run_id=run_id) is not RunControlState.RUNNING:
            break
        with ExitStack() as stack:
            request_file = stack.enter_context(deps.artifact_store.stage_local_copy(request_relative_path.as_posix()))
            remote_input_file_id = context.provider.upload_input_file(request_file)
            try:
                batch = context.provider.create_batch(
                    input_file_id=remote_input_file_id,
                    metadata={"run_id": run_id, **context.config.batch_metadata},
                )
            except Exception as exc:  # noqa: BLE001
                cleanup_uploaded_input_file(context, remote_input_file_id)
                if not is_retryable_batch_control_plane_error(exc):
                    raise
                deps.state.record_batch_retry_failure(
                    run_id=run_id,
                    error_class=classify_batch_error(exc),
                    base_delay_sec=config.retry_policy.base_backoff_sec,
                    max_delay_sec=config.retry_policy.max_backoff_sec,
                )
                deps.emit_event(
                    "batch_submit_retry",
                    run_id=run_id,
                    provider_kind=context.config.provider_config.provider_kind,
                    data={"error_class": classify_batch_error(exc)},
                )
                break

        deps.state.clear_batch_retry_backoff(run_id=run_id)
        provider_batch_id = str(batch["id"])
        local_batch_id = f"batch-{chunk_index:04d}-{uuid4().hex[:8]}"
        deps.state.register_batch(
            run_id=run_id,
            local_batch_id=local_batch_id,
            provider_batch_id=provider_batch_id,
            status=str(batch.get("status", "submitted")),
            custom_ids=[str(row["custom_id"]) for row in chunk],
        )
        deps.state.mark_items_submitted(
            run_id=run_id,
            provider_batch_id=provider_batch_id,
            submissions=[
                PreparedSubmission(
                    item_id=str(row["item_id"]),
                    custom_id=str(row["custom_id"]),
                    submission_tokens=int(row["submission_tokens"]),
                )
                for row in chunk
            ],
        )
        deps.emit_event(
            "batch_submitted",
            run_id=run_id,
            provider_kind=context.config.provider_config.provider_kind,
            data={
                "provider_batch_id": provider_batch_id,
                "submitted_item_count": len(chunk),
                "submission_tokens": chunk_tokens,
            },
        )
        submitted_count += len(chunk)
        submitted_item_ids.update(str(row["item_id"]) for row in chunk)
        active_tokens += chunk_tokens

    unsent = [
        item.item_id
        for item in claimed
        if item.item_id not in submitted_item_ids and item.item_id not in failed_item_ids
    ]
    if unsent:
        deps.state.release_items_to_pending(run_id=run_id, item_ids=unsent)
    return submitted_count


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
        request_line = load_request_artifact_line(
            artifact_store=artifact_store,
            artifact_path=item.request_artifact_path,
            line_number=item.request_artifact_line,
            expected_sha256=item.request_sha256,
            artifact_cache=artifact_cache,
        )
        request_line["custom_id"] = custom_id
    else:
        request_line = context.provider.build_request_line(
            custom_id=custom_id,
            prompt_parts=PromptParts(
                prompt=item.prompt,
                system_prompt=item.system_prompt,
            ),
            structured_output=context.structured_output,
        )
    request_line = cast(BatchRequestLine, request_line)
    request_bytes = len((json.dumps(request_line, ensure_ascii=False) + "\n").encode("utf-8"))
    submission_tokens = context.provider.estimate_request_tokens(
        request_line,
        chars_per_token=context.config.chunk_policy.chars_per_token,
    )
    return PreparedRequest(
        item_id=item.item_id,
        custom_id=str(request_line["custom_id"]),
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
    return max(1, min(max_requests * 4, 8_192))


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
