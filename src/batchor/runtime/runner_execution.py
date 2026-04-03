from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

from batchor.core.models import ItemFailure
from batchor.core.models import OpenAIProviderConfig
from batchor.core.models import RunSummary
from batchor.core.types import BatchRemoteRecord, JSONObject, JSONValue
from batchor.runtime.retry import (
    classify_batch_error,
    is_enqueue_token_limit_error,
    is_retryable_batch_control_plane_error,
)
from batchor.runtime.run_handle import _RunContext
from batchor.runtime.tokens import (
    chunk_request_rows,
    effective_inflight_token_budget,
    resolve_openai_batch_token_limit,
    split_rows_by_token_limit,
)
from batchor.runtime.validation import parse_structured_response, parse_text_response
from batchor.storage.state import (
    BatchArtifactPointer,
    CompletedItemRecord,
    ItemFailureRecord,
    PreparedSubmission,
    QueuedItemFailureRecord,
    RequestArtifactPointer,
)

if TYPE_CHECKING:
    from batchor.runtime.runner import BatchRunner


def _refresh_run(self: BatchRunner, run_id: str, context: _RunContext) -> RunSummary:
    _poll_once(self, run_id, context)
    _submit_pending_items(self, run_id, context)
    return self.state.get_run_summary(run_id=run_id)


def _cleanup_uploaded_input_file(context: _RunContext, file_id: str) -> None:
    try:
        context.provider.delete_input_file(file_id)
    except Exception:  # noqa: BLE001
        return


def _submit_pending_items(self: BatchRunner, run_id: str, context: _RunContext) -> int:
    config = context.config
    if self.state.get_batch_retry_backoff_remaining_sec(run_id=run_id) > 0:
        return 0
    claim_limit = _submission_claim_limit(config)
    claimed = self.state.claim_items_for_submission(
        run_id=run_id,
        max_attempts=config.retry_policy.max_attempts,
        limit=claim_limit,
    )
    if not claimed:
        return 0
    self._emit_event(
        "items_claimed_for_submission",
        run_id=run_id,
        provider_kind=context.config.provider_config.provider_kind,
        data={"claimed_item_count": len(claimed)},
    )

    artifact_cache: dict[str, list[str]] = {}
    prepared_items = [
        self._prepare_item(item, context, artifact_cache=artifact_cache)
        for item in claimed
    ]
    batch_token_limit = _batch_token_limit(config.provider_config)
    inflight_budget = _inflight_budget(config.provider_config)
    failed_item_ids: set[str] = set()
    if batch_token_limit is not None:
        within_limit_rows, oversized_rows = split_rows_by_token_limit(
            [self._prepared_dict(item) for item in prepared_items],
            token_limit=batch_token_limit,
            token_field="submission_tokens",
        )
        prepared_rows = within_limit_rows
        if oversized_rows:
            self.state.mark_queued_items_failed(
                run_id=run_id,
                failures=[
                    QueuedItemFailureRecord(
                        item_id=str(row["item_id"]),
                        error=_oversized_request_failure(
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
        prepared_rows = [self._prepared_dict(item) for item in prepared_items]

    if inflight_budget is not None:
        inflight_safe_rows, oversized_rows = split_rows_by_token_limit(
            prepared_rows,
            token_limit=inflight_budget,
            token_field="submission_tokens",
        )
        prepared_rows = inflight_safe_rows
        if oversized_rows:
            self.state.mark_queued_items_failed(
                run_id=run_id,
                failures=[
                    QueuedItemFailureRecord(
                        item_id=str(row["item_id"]),
                        error=_oversized_request_failure(
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
        unsent = [
            item.item_id
            for item in claimed
            if item.item_id not in failed_item_ids
        ]
        if unsent:
            self.state.release_items_to_pending(run_id=run_id, item_ids=unsent)
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
    active_tokens = (
        self.state.get_active_submitted_token_estimate(run_id=run_id)
        if inflight_budget is not None
        else 0
    )

    for chunk_index, chunk in enumerate(chunks, start=1):
        chunk_tokens = sum(int(row["submission_tokens"]) for row in chunk)
        if inflight_budget is not None and chunk_tokens > max(inflight_budget - active_tokens, 0):
            break
        request_lines = [
            cast(JSONObject, row["request_line"])
            for row in chunk
        ]
        request_relative_path = self._request_artifact_relative_path(run_id)
        self.artifact_store.write_text(
            request_relative_path.as_posix(),
            self._serialize_jsonl(cast(list[JSONObject], request_lines)),
            encoding="utf-8",
        )
        self.state.record_request_artifacts(
            run_id=run_id,
            pointers=[
                RequestArtifactPointer(
                    item_id=str(row["item_id"]),
                    artifact_path=request_relative_path.as_posix(),
                    line_number=line_number,
                    request_sha256=self._request_sha256(
                        cast(JSONObject, row["request_line"])
                    ),
                )
                for line_number, row in enumerate(chunk, start=1)
            ],
        )
        with ExitStack() as stack:
            request_file = stack.enter_context(
                self.artifact_store.stage_local_copy(request_relative_path.as_posix())
            )
            remote_input_file_id = context.provider.upload_input_file(request_file)
            try:
                batch = context.provider.create_batch(
                    input_file_id=remote_input_file_id,
                    metadata={"run_id": run_id, **context.config.batch_metadata},
                )
            except Exception as exc:  # noqa: BLE001
                _cleanup_uploaded_input_file(context, remote_input_file_id)
                if not is_retryable_batch_control_plane_error(exc):
                    raise
                self.state.record_batch_retry_failure(
                    run_id=run_id,
                    error_class=classify_batch_error(exc),
                    base_delay_sec=config.retry_policy.base_backoff_sec,
                    max_delay_sec=config.retry_policy.max_backoff_sec,
                )
                self._emit_event(
                    "batch_submit_retry",
                    run_id=run_id,
                    provider_kind=context.config.provider_config.provider_kind,
                    data={"error_class": classify_batch_error(exc)},
                )
                break

        self.state.clear_batch_retry_backoff(run_id=run_id)
        provider_batch_id = str(batch["id"])
        local_batch_id = f"batch-{chunk_index:04d}-{uuid4().hex[:8]}"
        self.state.register_batch(
            run_id=run_id,
            local_batch_id=local_batch_id,
            provider_batch_id=provider_batch_id,
            status=str(batch.get("status", "submitted")),
            custom_ids=[str(row["custom_id"]) for row in chunk],
        )
        self.state.mark_items_submitted(
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
        self._emit_event(
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
        self.state.release_items_to_pending(run_id=run_id, item_ids=unsent)
    return submitted_count


def _submission_claim_limit(config: Any) -> int:
    max_requests = int(config.chunk_policy.max_requests)
    return max(1, min(max_requests * 4, 8_192))


def _inflight_budget(provider_config: Any) -> int | None:
    if not isinstance(provider_config, OpenAIProviderConfig):
        return None
    return effective_inflight_token_budget(provider_config.enqueue_limits)


def _batch_token_limit(provider_config: Any) -> int | None:
    if not isinstance(provider_config, OpenAIProviderConfig):
        return None
    return resolve_openai_batch_token_limit(provider_config.enqueue_limits)


def _oversized_request_failure(
    *,
    provider_config: Any,
    limit_type: str,
    submission_tokens: int,
    limit: int,
) -> ItemFailure:
    provider_name = "provider"
    if isinstance(provider_config, OpenAIProviderConfig):
        provider_name = provider_config.provider_kind.value
    if limit_type == "batch":
        error_class = f"{provider_name}_request_exceeds_batch_token_limit"
        message = (
            "request token estimate exceeds the provider batch token limit "
            f"({submission_tokens} > {limit})"
        )
    else:
        error_class = f"{provider_name}_request_exceeds_inflight_token_limit"
        message = (
            "request token estimate exceeds the provider inflight token budget "
            f"({submission_tokens} > {limit})"
        )
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


def _poll_once(self: BatchRunner, run_id: str, context: _RunContext) -> None:
    batches = self.state.get_active_batches(run_id=run_id)
    if not batches:
        return

    remote_by_batch_id: dict[str, BatchRemoteRecord] = {}
    poll_errors: dict[str, Exception] = {}
    if len(batches) == 1:
        batch = batches[0]
        try:
            remote_by_batch_id[batch.provider_batch_id] = context.provider.get_batch(
                batch.provider_batch_id
            )
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
            self._emit_event(
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
        self._emit_event(
            "batch_polled",
            run_id=run_id,
            provider_kind=context.config.provider_config.provider_kind,
            data={
                "provider_batch_id": batch.provider_batch_id,
                "status": status,
            },
        )
        self.state.update_batch_status(
            run_id=run_id,
            provider_batch_id=batch.provider_batch_id,
            status=status,
            output_file_id=remote.get("output_file_id"),
            error_file_id=remote.get("error_file_id"),
        )
        if status == "completed":
            _consume_completed_batch(
                self,
                run_id=run_id,
                context=context,
                provider_batch_id=batch.provider_batch_id,
                output_file_id=remote.get("output_file_id"),
                error_file_id=remote.get("error_file_id"),
            )
            self.state.clear_batch_retry_backoff(run_id=run_id)
            self._emit_event(
                "batch_completed",
                run_id=run_id,
                provider_kind=context.config.provider_config.provider_kind,
                data={"provider_batch_id": batch.provider_batch_id},
            )
        elif status in {"failed", "cancelled", "expired"}:
            output_content, error_content = _download_batch_file_contents(
                context=context,
                output_file_id=remote.get("output_file_id"),
                error_file_id=remote.get("error_file_id"),
            )
            output_artifact_path, error_artifact_path = self._write_batch_result_artifacts(
                run_id=run_id,
                provider_batch_id=batch.provider_batch_id,
                output_content=output_content,
                error_content=error_content,
            )
            if output_artifact_path is not None or error_artifact_path is not None:
                self.state.record_batch_artifacts(
                    run_id=run_id,
                    pointers=[
                        BatchArtifactPointer(
                            provider_batch_id=batch.provider_batch_id,
                            output_artifact_path=output_artifact_path,
                            error_artifact_path=error_artifact_path,
                        )
                    ],
                )
            error = _batch_failure_error(remote)
            self.state.record_batch_retry_failure(
                run_id=run_id,
                error_class=error.error_class,
                base_delay_sec=context.config.retry_policy.base_backoff_sec,
                max_delay_sec=context.config.retry_policy.max_backoff_sec,
            )
            self.state.reset_batch_items_to_pending(
                run_id=run_id,
                provider_batch_id=batch.provider_batch_id,
                error=error,
            )
            self._emit_event(
                "batch_terminal_failure",
                run_id=run_id,
                provider_kind=context.config.provider_config.provider_kind,
                data={
                    "provider_batch_id": batch.provider_batch_id,
                    "error_class": error.error_class,
                },
            )


def _download_batch_file_contents(
    *,
    context: _RunContext,
    output_file_id: object,
    error_file_id: object,
) -> tuple[str | None, str | None]:
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


def _consume_completed_batch(
    self: BatchRunner,
    *,
    run_id: str,
    context: _RunContext,
    provider_batch_id: str,
    output_file_id: str | None,
    error_file_id: str | None,
) -> None:
    output_content, error_content = _download_batch_file_contents(
        context=context,
        output_file_id=output_file_id,
        error_file_id=error_file_id,
    )
    output_artifact_path, error_artifact_path = self._write_batch_result_artifacts(
        run_id=run_id,
        provider_batch_id=provider_batch_id,
        output_content=output_content,
        error_content=error_content,
    )
    if output_artifact_path is not None or error_artifact_path is not None:
        self.state.record_batch_artifacts(
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
        self.state.get_submitted_custom_ids_for_batch(
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
                    error=_item_failure(
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
                error=_item_failure(
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
                error=_item_failure(
                    error_class="batch_output_missing_row",
                    message=(
                        "batch completed without a terminal output record "
                        "for the submitted item"
                    ),
                    raw_error={"provider_batch_id": provider_batch_id},
                    retryable=True,
                ),
                count_attempt=False,
            )
        )

    if completions:
        self.state.mark_items_completed(run_id=run_id, completions=completions)
        self._emit_event(
            "items_completed",
            run_id=run_id,
            provider_kind=context.config.provider_config.provider_kind,
            data={"completed_item_count": len(completions)},
        )
    if failures:
        self.state.mark_items_failed(
            run_id=run_id,
            failures=failures,
            max_attempts=context.config.retry_policy.max_attempts,
        )
        self._emit_event(
            "items_failed",
            run_id=run_id,
            provider_kind=context.config.provider_config.provider_kind,
            data={"failed_item_count": len(failures)},
        )


def _item_failure(
    *,
    error_class: str,
    message: str,
    raw_error: JSONValue | JSONObject,
    retryable: bool,
) -> ItemFailure:
    return ItemFailure(
        error_class=error_class,
        message=message,
        raw_error=raw_error,
        retryable=retryable,
    )


def _batch_failure_error(remote: BatchRemoteRecord) -> ItemFailure:
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
