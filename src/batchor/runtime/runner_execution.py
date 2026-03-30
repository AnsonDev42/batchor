from __future__ import annotations

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


def _submit_pending_items(self: BatchRunner, run_id: str, context: _RunContext) -> int:
    config = context.config
    if self.state.get_batch_retry_backoff_remaining_sec(run_id=run_id) > 0:
        return 0
    claimed = self.state.claim_items_for_submission(
        run_id=run_id,
        max_attempts=config.retry_policy.max_attempts,
        limit=None,
    )
    if not claimed:
        return 0

    prepared_items = [self._prepare_item(item, context) for item in claimed]
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

    for chunk_index, chunk in enumerate(chunks, start=1):
        chunk_tokens = sum(int(row["submission_tokens"]) for row in chunk)
        if not _has_inflight_capacity(
            self,
            run_id=run_id,
            inflight_budget=inflight_budget,
            chunk_tokens=chunk_tokens,
        ):
            break
        request_lines = [
            cast(JSONObject, row["request_line"])
            for row in chunk
        ]
        request_relative_path = self._request_artifact_relative_path(run_id)
        request_path = self.temp_root / request_relative_path
        request_file = context.provider.write_requests_jsonl(
            cast(list[Any], request_lines),
            request_path,
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
        remote_input_file_id = context.provider.upload_input_file(request_file)
        try:
            batch = context.provider.create_batch(
                input_file_id=remote_input_file_id,
                metadata={"run_id": run_id, **context.config.batch_metadata},
            )
        except Exception as exc:  # noqa: BLE001
            if not is_retryable_batch_control_plane_error(exc):
                raise
            self.state.record_batch_retry_failure(
                run_id=run_id,
                error_class=classify_batch_error(exc),
                base_delay_sec=config.retry_policy.base_backoff_sec,
                max_delay_sec=config.retry_policy.max_backoff_sec,
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
        submitted_count += len(chunk)
        submitted_item_ids.update(str(row["item_id"]) for row in chunk)

    unsent = [
        item.item_id
        for item in claimed
        if item.item_id not in submitted_item_ids and item.item_id not in failed_item_ids
    ]
    if unsent:
        self.state.release_items_to_pending(run_id=run_id, item_ids=unsent)
    return submitted_count


def _has_inflight_capacity(
    self: BatchRunner,
    *,
    run_id: str,
    inflight_budget: int | None,
    chunk_tokens: int,
) -> bool:
    if inflight_budget is None:
        return True
    if chunk_tokens > inflight_budget:
        return False
    active_tokens = self.state.get_active_submitted_token_estimate(run_id=run_id)
    available = max(inflight_budget - active_tokens, 0)
    return chunk_tokens <= available


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
    for batch in self.state.get_active_batches(run_id=run_id):
        try:
            remote = context.provider.get_batch(batch.provider_batch_id)
        except Exception as exc:  # noqa: BLE001
            if not is_retryable_batch_control_plane_error(exc):
                raise
            self.state.record_batch_retry_failure(
                run_id=run_id,
                error_class=classify_batch_error(exc),
                base_delay_sec=context.config.retry_policy.base_backoff_sec,
                max_delay_sec=context.config.retry_policy.max_backoff_sec,
            )
            continue

        status = str(remote["status"])
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
        elif status in {"failed", "cancelled", "expired"}:
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


def _consume_completed_batch(
    self: BatchRunner,
    *,
    run_id: str,
    context: _RunContext,
    provider_batch_id: str,
    output_file_id: str | None,
    error_file_id: str | None,
) -> None:
    output_content = (
        context.provider.download_file_content(output_file_id)
        if output_file_id
        else ""
    )
    error_content = (
        context.provider.download_file_content(error_file_id)
        if error_file_id
        else ""
    )
    successes, errors, _raw_records = context.provider.parse_batch_output(
        output_content=output_content,
        error_content=error_content,
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
    if failures:
        self.state.mark_items_failed(
            run_id=run_id,
            failures=failures,
            max_attempts=context.config.retry_policy.max_attempts,
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
