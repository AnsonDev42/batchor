"""Internal helpers for refresh orchestration and active-batch polling."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Callable, cast

from batchor.artifacts import ArtifactStore
from batchor.core.enums import RunControlState
from batchor.core.models import ItemFailure, RunSummary
from batchor.core.types import BatchRemoteRecord, JSONObject, JSONValue
from batchor.runtime.artifacts import write_batch_result_artifacts
from batchor.runtime.context import RunContext
from batchor.runtime.retry import (
    classify_batch_error,
    is_enqueue_token_limit_error,
    is_retryable_batch_control_plane_error,
)
from batchor.runtime.validation import parse_structured_response, parse_text_response
from batchor.storage.state import (
    BatchArtifactPointer,
    CompletedItemRecord,
    ItemFailureRecord,
    StateStore,
)


@dataclass(frozen=True)
class PollingDeps:
    """Dependencies needed by the refresh and polling layer.

    Attributes:
        state: Durable state store used for polling transitions.
        artifact_store: Artifact store used for raw provider payloads.
        emit_event: Observer callback used for coarse lifecycle events.
    """

    state: StateStore
    artifact_store: ArtifactStore
    emit_event: Callable[..., None]


def refresh_run(
    deps: PollingDeps,
    *,
    run_id: str,
    context: RunContext,
    submit_pending_items: Callable[[str, RunContext], int],
) -> RunSummary:
    """Perform one poll-and-submit cycle and return the updated summary.

    Args:
        deps: Polling-layer dependencies.
        run_id: Durable run identifier.
        context: Runtime context for the run.
        submit_pending_items: Callback used to submit newly claimable items
            after polling completes.

    Returns:
        Updated persisted run summary after the cycle.
    """
    control_state = deps.state.get_run_control_state(run_id=run_id)
    if control_state is RunControlState.PAUSED:
        return deps.state.get_run_summary(run_id=run_id)
    poll_once(deps, run_id=run_id, context=context)
    if control_state is RunControlState.CANCEL_REQUESTED:
        if not deps.state.get_active_batches(run_id=run_id):
            deps.state.mark_nonterminal_items_cancelled(
                run_id=run_id,
                error=item_failure(
                    error_class="run_cancelled",
                    message="run was cancelled before item submission completed",
                    raw_error={"run_id": run_id},
                    retryable=False,
                ),
            )
        return deps.state.get_run_summary(run_id=run_id)
    submit_pending_items(run_id, context)
    return deps.state.get_run_summary(run_id=run_id)


def poll_once(
    deps: PollingDeps,
    *,
    run_id: str,
    context: RunContext,
) -> None:
    """Poll all active provider batches and consume any terminal ones.

    Args:
        deps: Polling-layer dependencies.
        run_id: Durable run identifier.
        context: Runtime context for the run.
    """
    batches = deps.state.get_active_batches(run_id=run_id)
    if not batches:
        return

    remote_by_batch_id: dict[str, BatchRemoteRecord] = {}
    poll_errors: dict[str, Exception] = {}
    if len(batches) == 1:
        batch = batches[0]
        try:
            remote_by_batch_id[batch.provider_batch_id] = context.provider.get_batch(batch.provider_batch_id)
        except Exception as exc:  # noqa: BLE001
            poll_errors[batch.provider_batch_id] = exc
    else:
        with ThreadPoolExecutor(max_workers=min(len(batches), 8)) as executor:
            future_by_batch_id = {
                batch.provider_batch_id: executor.submit(
                    context.provider.get_batch,
                    batch.provider_batch_id,
                )
                for batch in batches
            }
            for provider_batch_id, future in future_by_batch_id.items():
                try:
                    remote_by_batch_id[provider_batch_id] = future.result()
                except Exception as exc:  # noqa: BLE001
                    poll_errors[provider_batch_id] = exc

    for batch in batches:
        if batch.provider_batch_id in poll_errors:
            exc = poll_errors[batch.provider_batch_id]
            if not is_retryable_batch_control_plane_error(exc):
                raise exc
            deps.emit_event(
                "batch_poll_retry",
                run_id=run_id,
                provider_kind=context.config.provider_config.provider_kind,
                data={
                    "provider_batch_id": batch.provider_batch_id,
                    "error_class": classify_batch_error(exc),
                },
            )
            continue

        remote = remote_by_batch_id[batch.provider_batch_id]
        status = str(remote["status"])
        deps.emit_event(
            "batch_polled",
            run_id=run_id,
            provider_kind=context.config.provider_config.provider_kind,
            data={
                "provider_batch_id": batch.provider_batch_id,
                "status": status,
            },
        )
        deps.state.update_batch_status(
            run_id=run_id,
            provider_batch_id=batch.provider_batch_id,
            status=status,
            output_file_id=remote.get("output_file_id"),
            error_file_id=remote.get("error_file_id"),
        )
        if status == "completed":
            consume_completed_batch(
                deps,
                run_id=run_id,
                context=context,
                provider_batch_id=batch.provider_batch_id,
                output_file_id=remote.get("output_file_id"),
                error_file_id=remote.get("error_file_id"),
            )
            deps.state.clear_batch_retry_backoff(run_id=run_id)
            deps.emit_event(
                "batch_completed",
                run_id=run_id,
                provider_kind=context.config.provider_config.provider_kind,
                data={"provider_batch_id": batch.provider_batch_id},
            )
        elif status in {"failed", "cancelled", "expired"}:
            output_content, error_content = download_batch_file_contents(
                context=context,
                output_file_id=remote.get("output_file_id"),
                error_file_id=remote.get("error_file_id"),
            )
            output_artifact_path, error_artifact_path = write_batch_result_artifacts(
                artifact_store=deps.artifact_store,
                run_id=run_id,
                provider_batch_id=batch.provider_batch_id,
                output_content=output_content,
                error_content=error_content,
                persist_raw_output_artifacts=context.config.artifact_policy.persist_raw_output_artifacts,
            )
            if output_artifact_path is not None or error_artifact_path is not None:
                deps.state.record_batch_artifacts(
                    run_id=run_id,
                    pointers=[
                        BatchArtifactPointer(
                            provider_batch_id=batch.provider_batch_id,
                            output_artifact_path=output_artifact_path,
                            error_artifact_path=error_artifact_path,
                        )
                    ],
                )
            error = batch_failure_error(remote)
            deps.state.record_batch_retry_failure(
                run_id=run_id,
                error_class=error.error_class,
                base_delay_sec=context.config.retry_policy.base_backoff_sec,
                max_delay_sec=context.config.retry_policy.max_backoff_sec,
            )
            deps.state.reset_batch_items_to_pending(
                run_id=run_id,
                provider_batch_id=batch.provider_batch_id,
                error=error,
            )
            deps.emit_event(
                "batch_terminal_failure",
                run_id=run_id,
                provider_kind=context.config.provider_config.provider_kind,
                data={
                    "provider_batch_id": batch.provider_batch_id,
                    "error_class": error.error_class,
                },
            )


def download_batch_file_contents(
    *,
    context: RunContext,
    output_file_id: object,
    error_file_id: object,
) -> tuple[str | None, str | None]:
    """Download the output and error files for a provider batch.

    Args:
        context: Runtime context containing the provider.
        output_file_id: Provider output file identifier, if any.
        error_file_id: Provider error file identifier, if any.

    Returns:
        Tuple of ``(output_content, error_content)``. Each value is ``None``
        when the corresponding file is unavailable.
    """
    output_id = output_file_id if isinstance(output_file_id, str) else None
    error_id = error_file_id if isinstance(error_file_id, str) else None
    if output_id is None and error_id is None:
        return None, None
    if output_id is not None and error_id is not None:
        with ThreadPoolExecutor(max_workers=2) as executor:
            output_future = executor.submit(context.provider.download_file_content, output_id)
            error_future = executor.submit(context.provider.download_file_content, error_id)
            return output_future.result(), error_future.result()
    if output_id is not None:
        return context.provider.download_file_content(output_id), None
    if error_id is None:
        return None, None
    return None, context.provider.download_file_content(error_id)


def consume_completed_batch(
    deps: PollingDeps,
    *,
    run_id: str,
    context: RunContext,
    provider_batch_id: str,
    output_file_id: object,
    error_file_id: object,
) -> None:
    """Download, persist, and parse a completed provider batch.

    Args:
        deps: Polling-layer dependencies.
        run_id: Durable run identifier.
        context: Runtime context for the run.
        provider_batch_id: Provider batch identifier being consumed.
        output_file_id: Provider output file identifier, if any.
        error_file_id: Provider error file identifier, if any.
    """
    output_content, error_content = download_batch_file_contents(
        context=context,
        output_file_id=output_file_id,
        error_file_id=error_file_id,
    )
    output_artifact_path, error_artifact_path = write_batch_result_artifacts(
        artifact_store=deps.artifact_store,
        run_id=run_id,
        provider_batch_id=provider_batch_id,
        output_content=output_content,
        error_content=error_content,
        persist_raw_output_artifacts=context.config.artifact_policy.persist_raw_output_artifacts,
    )
    if output_artifact_path is not None or error_artifact_path is not None:
        deps.state.record_batch_artifacts(
            run_id=run_id,
            pointers=[
                BatchArtifactPointer(
                    provider_batch_id=provider_batch_id,
                    output_artifact_path=output_artifact_path,
                    error_artifact_path=error_artifact_path,
                )
            ],
        )
    successes, errors, _raw_records = context.provider.parse_batch_output(
        output_content=output_content or "",
        error_content=error_content or "",
    )
    submitted_custom_ids = set(
        deps.state.get_submitted_custom_ids_for_batch(
            run_id=run_id,
            provider_batch_id=provider_batch_id,
        )
    )

    completions: list[CompletedItemRecord] = []
    failures: list[ItemFailureRecord] = []
    processed_custom_ids: set[str] = set()

    for custom_id, record in successes.items():
        processed_custom_ids.add(custom_id)
        if context.output_model is None:
            completions.append(
                CompletedItemRecord(
                    custom_id=custom_id,
                    output_text=parse_text_response(record),
                    raw_response=record,
                )
            )
            continue
        try:
            output_text, parsed_json, _validated = parse_structured_response(
                record,
                context.output_model,
            )
        except Exception as exc:  # noqa: BLE001
            failures.append(
                ItemFailureRecord(
                    custom_id=custom_id,
                    error=item_failure(
                        error_class=getattr(
                            exc,
                            "error_class",
                            "structured_output_validation_failed",
                        ),
                        message=str(exc),
                        raw_error=getattr(exc, "raw_error", record),
                        retryable=True,
                    ),
                    count_attempt=True,
                )
            )
            continue
        completions.append(
            CompletedItemRecord(
                custom_id=custom_id,
                output_text=output_text,
                raw_response=record,
                output_json=parsed_json,
            )
        )

    for custom_id, error_record in errors.items():
        processed_custom_ids.add(custom_id)
        retryable = is_enqueue_token_limit_error(error_record)
        failures.append(
            ItemFailureRecord(
                custom_id=custom_id,
                error=item_failure(
                    error_class="enqueue_token_limit" if retryable else "provider_item_error",
                    message="provider returned item-level error",
                    raw_error=error_record,
                    retryable=True,
                ),
                count_attempt=not retryable,
            )
        )

    missing_custom_ids = sorted(submitted_custom_ids - processed_custom_ids)
    for custom_id in missing_custom_ids:
        failures.append(
            ItemFailureRecord(
                custom_id=custom_id,
                error=item_failure(
                    error_class="batch_output_missing_row",
                    message="batch completed without a terminal output record for the submitted item",
                    raw_error={"provider_batch_id": provider_batch_id},
                    retryable=True,
                ),
                count_attempt=False,
            )
        )

    if completions:
        deps.state.mark_items_completed(run_id=run_id, completions=completions)
        deps.emit_event(
            "items_completed",
            run_id=run_id,
            provider_kind=context.config.provider_config.provider_kind,
            data={"completed_item_count": len(completions)},
        )
    if failures:
        deps.state.mark_items_failed(
            run_id=run_id,
            failures=failures,
            max_attempts=context.config.retry_policy.max_attempts,
        )
        deps.emit_event(
            "items_failed",
            run_id=run_id,
            provider_kind=context.config.provider_config.provider_kind,
            data={"failed_item_count": len(failures)},
        )


def item_failure(
    *,
    error_class: str,
    message: str,
    raw_error: JSONValue | JSONObject,
    retryable: bool,
) -> ItemFailure:
    """Construct an item failure payload from named parts.

    Args:
        error_class: Machine-readable error class.
        message: Human-readable failure message.
        raw_error: Raw provider or runtime error payload.
        retryable: Whether the failure is retryable.

    Returns:
        Item failure payload.
    """
    return ItemFailure(
        error_class=error_class,
        message=message,
        raw_error=raw_error,
        retryable=retryable,
    )


def batch_failure_error(remote: BatchRemoteRecord) -> ItemFailure:
    """Construct the failure payload recorded for a terminal batch error.

    Args:
        remote: Provider batch record in a terminal failure state.

    Returns:
        Item failure payload used when resetting submitted items.
    """
    errors = remote.get("errors")
    error_class = (
        "enqueue_token_limit"
        if is_enqueue_token_limit_error(errors)
        else f"batch_terminal_{remote.get('status', 'failed')}"
    )
    return ItemFailure(
        error_class=error_class,
        message="batch did not complete successfully",
        raw_error=errors if errors is not None else cast(JSONObject, dict(remote)),
        retryable=True,
    )
