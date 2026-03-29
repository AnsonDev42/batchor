from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import time
from typing import Any, Callable, cast
from uuid import uuid4

from pydantic import BaseModel

from batchor.types import BatchRemoteRecord, BatchRequestLine, JSONObject

from batchor.models import (
    BatchItem,
    BatchJob,
    PromptParts,
    RunHandle,
    RunStatus,
    RunSummary,
    StructuredBatchJob,
    StructuredItemResult,
    TextBatchJob,
    TextItemResult,
)
from batchor.openai_provider import BatchProvider, OpenAIBatchProvider, StructuredOutputSchema
from batchor.retry import classify_batch_error, is_enqueue_token_limit_error, is_retryable_batch_control_plane_error
from batchor.state import (
    ClaimedItem,
    CompletedItemRecord,
    ItemFailureRecord,
    MemoryStateStore,
    PreparedSubmission,
    StateStore,
)
from batchor.tokens import (
    chunk_request_rows,
    effective_inflight_token_budget,
    estimate_request_tokens,
)
from batchor.validation import model_output_schema, parse_structured_response, parse_text_response


@dataclass(frozen=True)
class _PreparedItem:
    item_id: str
    custom_id: str
    request_line: BatchRequestLine
    request_bytes: int
    submission_tokens: int


@dataclass(frozen=True)
class _RunContext:
    job: BatchJob
    provider: BatchProvider
    structured_output: StructuredOutputSchema | None


def generate_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"batchor_{timestamp}_{uuid4().hex[:8]}"


class BatchRunner:
    def __init__(
        self,
        *,
        state_store: StateStore | None = None,
        provider_factory: Callable[[Any], BatchProvider] | None = None,
        sleep: Callable[[float], None] | None = None,
        temp_root: str | Path | None = None,
    ) -> None:
        self.state = state_store or MemoryStateStore()
        self.provider_factory = provider_factory or (
            lambda provider_config: OpenAIBatchProvider(provider_config)
        )
        self.sleep = sleep or time.sleep
        self.temp_root = Path(temp_root) if temp_root is not None else Path(tempfile.gettempdir()) / "batchor"
        self._contexts: dict[str, _RunContext] = {}

    def run(self, job: BatchJob) -> RunHandle:
        run_id = generate_run_id()
        structured_output = self._structured_output_for_job(job)
        provider = self.provider_factory(job.provider_config)
        self.state.create_run(
            run_id=run_id,
            items=cast(list[Any], job.items),
            structured=structured_output is not None,
        )
        self._contexts[run_id] = _RunContext(
            job=job,
            provider=provider,
            structured_output=structured_output,
        )
        self._submit_pending_items(run_id, allow_wait_for_capacity=False)
        status = self.state.get_run_status(run_id=run_id)
        return RunHandle(run_id=run_id, status=status.status)

    def wait(self, run_id: str) -> RunSummary:
        context = self._context(run_id)
        while True:
            self._poll_once(run_id, context)
            self._submit_pending_items(run_id, allow_wait_for_capacity=False)
            status = self.state.get_run_status(run_id=run_id)
            submit_eligible = status.status_counts.get("pending", 0) + status.status_counts.get("failed_retryable", 0)
            if status.active_batches == 0 and submit_eligible == 0 and status.backoff_remaining_sec <= 0:
                break
            sleep_for = context.job.provider_config.poll_interval_sec
            if status.backoff_remaining_sec > 0:
                sleep_for = min(status.backoff_remaining_sec, max(sleep_for, 0.0))
            if sleep_for > 0:
                self.sleep(sleep_for)
        final_status = self.state.get_run_status(run_id=run_id)
        return RunSummary(
            run_id=run_id,
            status=final_status.status,
            total_items=final_status.total_items,
            completed_items=final_status.status_counts.get("completed", 0),
            failed_items=final_status.status_counts.get("failed_permanent", 0),
        )

    def run_and_wait(self, job: BatchJob) -> RunSummary:
        handle = self.run(job)
        return self.wait(handle.run_id)

    def status(self, run_id: str) -> RunStatus:
        return self.state.get_run_status(run_id=run_id)

    def results(self, run_id: str) -> list[StructuredItemResult[BaseModel] | TextItemResult]:
        structured = self.state.is_structured_run(run_id=run_id)
        results: list[StructuredItemResult[BaseModel] | TextItemResult] = []
        for record in self.state.get_item_snapshot(run_id=run_id):
            if structured:
                results.append(
                    StructuredItemResult(
                        item_id=str(record["item_id"]),
                        status=record["status"],
                        attempt_count=int(record["attempt_count"]),
                        output=cast(BaseModel | None, record["output_model"]),
                        output_text=cast(str | None, record["output_text"]),
                        raw_response=cast(JSONObject | None, record["raw_response"]),
                        error=record["error"],
                        metadata=cast(JSONObject, record["metadata"]),
                    )
                )
            else:
                results.append(
                    TextItemResult(
                        item_id=str(record["item_id"]),
                        status=record["status"],
                        attempt_count=int(record["attempt_count"]),
                        output_text=cast(str | None, record["output_text"]),
                        raw_response=cast(JSONObject | None, record["raw_response"]),
                        error=record["error"],
                        metadata=cast(JSONObject, record["metadata"]),
                    )
                )
        return results

    @staticmethod
    def make_custom_id(item_id: str, attempt: int) -> str:
        return f"{item_id}:a{attempt}"

    @staticmethod
    def _structured_output_for_job(job: BatchJob) -> StructuredOutputSchema | None:
        if isinstance(job, TextBatchJob):
            return None
        schema_name, schema = model_output_schema(
            job.output_model,
            schema_name=job.schema_name,
        )
        return StructuredOutputSchema(name=schema_name, schema=schema)

    def _submit_pending_items(self, run_id: str, *, allow_wait_for_capacity: bool) -> int:
        context = self._context(run_id)
        job = context.job
        backoff_remaining = self.state.get_batch_retry_backoff_remaining_sec(run_id=run_id)
        if backoff_remaining > 0:
            if allow_wait_for_capacity:
                self.sleep(backoff_remaining)
            return 0
        claimed = self.state.claim_items_for_submission(
            run_id=run_id,
            max_attempts=job.retry_policy.max_attempts,
            limit=None,
        )
        if not claimed:
            return 0

        prepared_items = [self._prepare_item(item, context) for item in claimed]
        chunks = chunk_request_rows(
            [self._prepared_dict(item) for item in prepared_items],
            chunk_policy=job.chunk_policy,
            inflight_policy=job.inflight_policy,
            estimate_row_bytes=lambda row: int(row["request_bytes"]),
            estimate_row_tokens=lambda row: int(row["submission_tokens"]),
        )
        submitted_count = 0
        submitted_item_ids: set[str] = set()
        run_dir = self.temp_root / run_id

        for chunk_index, chunk in enumerate(chunks, start=1):
            chunk_tokens = sum(int(row["submission_tokens"]) for row in chunk)
            if not self._wait_for_inflight_capacity(
                run_id=run_id,
                context=context,
                chunk_tokens=chunk_tokens,
                allow_wait=allow_wait_for_capacity,
            ):
                break

            request_lines = [
                cast(JSONObject, row["request_line"])
                for row in chunk
            ]
            request_path = run_dir / f"requests_{chunk_index:04d}.jsonl"
            request_file = context.provider.write_requests_jsonl(
                cast(list[Any], request_lines),
                request_path,
            )
            remote_input_file_id = context.provider.upload_input_file(request_file)

            batch = self._create_batch_with_retry(
                run_id=run_id,
                context=context,
                input_file_id=remote_input_file_id,
                allow_wait=allow_wait_for_capacity,
            )
            if batch is None:
                break

            provider_batch_id = str(batch["id"])
            local_batch_id = f"batch-{chunk_index:04d}-{uuid4().hex[:8]}"
            custom_ids = [str(row["custom_id"]) for row in chunk]
            self.state.register_batch(
                run_id=run_id,
                local_batch_id=local_batch_id,
                provider_batch_id=provider_batch_id,
                status=str(batch.get("status", "submitted")),
                custom_ids=custom_ids,
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
            if item.item_id not in submitted_item_ids
        ]
        if unsent:
            self.state.release_items_to_pending(run_id=run_id, item_ids=unsent)
        return submitted_count

    def _create_batch_with_retry(
        self,
        *,
        run_id: str,
        context: _RunContext,
        input_file_id: str,
        allow_wait: bool,
    ):
        metadata = {"run_id": run_id, **context.job.batch_metadata}
        while True:
            try:
                return context.provider.create_batch(
                    input_file_id=input_file_id,
                    metadata=metadata,
                )
            except Exception as exc:  # noqa: BLE001
                if not is_retryable_batch_control_plane_error(exc):
                    raise
                backoff = self.state.record_batch_retry_failure(
                    run_id=run_id,
                    error_class=classify_batch_error(exc),
                    base_delay_sec=context.job.retry_policy.base_backoff_sec,
                    max_delay_sec=context.job.retry_policy.max_backoff_sec,
                )
                if not allow_wait:
                    return None
                if backoff.backoff_sec > 0:
                    self.sleep(backoff.backoff_sec)

    def _wait_for_inflight_capacity(
        self,
        *,
        run_id: str,
        context: _RunContext,
        chunk_tokens: int,
        allow_wait: bool,
    ) -> bool:
        inflight_budget = effective_inflight_token_budget(context.job.inflight_policy)
        if inflight_budget is None:
            return True
        if chunk_tokens > inflight_budget:
            raise ValueError(
                f"chunk token estimate {chunk_tokens} exceeds inflight budget {inflight_budget}"
            )
        while True:
            active_tokens = self.state.get_active_submitted_token_estimate(run_id=run_id)
            available = max(inflight_budget - active_tokens, 0)
            if chunk_tokens <= available:
                return True
            if not allow_wait:
                return False
            self._poll_once(run_id, context)
            poll_interval = context.job.provider_config.poll_interval_sec
            if poll_interval > 0:
                self.sleep(poll_interval)

    def _poll_once(self, run_id: str, context: _RunContext) -> None:
        for batch in self.state.get_active_batches(run_id=run_id):
            try:
                remote = context.provider.get_batch(batch.provider_batch_id)
            except Exception as exc:  # noqa: BLE001
                if not is_retryable_batch_control_plane_error(exc):
                    raise
                self.state.record_batch_retry_failure(
                    run_id=run_id,
                    error_class=classify_batch_error(exc),
                    base_delay_sec=context.job.retry_policy.base_backoff_sec,
                    max_delay_sec=context.job.retry_policy.max_backoff_sec,
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
                self._consume_completed_batch(
                    run_id=run_id,
                    context=context,
                    provider_batch_id=batch.provider_batch_id,
                    output_file_id=remote.get("output_file_id"),
                    error_file_id=remote.get("error_file_id"),
                )
                self.state.clear_batch_retry_backoff(run_id=run_id)
            elif status in {"failed", "cancelled", "expired"}:
                error = self._batch_failure_error(remote)
                self.state.record_batch_retry_failure(
                    run_id=run_id,
                    error_class=error.error_class,
                    base_delay_sec=context.job.retry_policy.base_backoff_sec,
                    max_delay_sec=context.job.retry_policy.max_backoff_sec,
                )
                self.state.reset_batch_items_to_pending(
                    run_id=run_id,
                    provider_batch_id=batch.provider_batch_id,
                    error=error,
                )

    def _consume_completed_batch(
        self,
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
            if context.structured_output is None:
                completions.append(
                    CompletedItemRecord(
                        custom_id=custom_id,
                        output_text=parse_text_response(record),
                        raw_response=record,
                    )
                )
                continue
            try:
                output_text, _, validated = parse_structured_response(
                    record,
                    cast(StructuredBatchJob[Any, BaseModel], context.job).output_model,
                )
            except Exception as exc:  # noqa: BLE001
                message = str(exc)
                failures.append(
                    ItemFailureRecord(
                        custom_id=custom_id,
                        error=self._item_failure(
                            error_class=getattr(exc, "error_class", "structured_output_validation_failed"),
                            message=message,
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
                    output_model=validated,
                )
            )

        for custom_id, error_record in errors.items():
            processed_custom_ids.add(custom_id)
            retryable = is_enqueue_token_limit_error(error_record)
            failures.append(
                ItemFailureRecord(
                    custom_id=custom_id,
                    error=self._item_failure(
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
                    error=self._item_failure(
                        error_class="batch_output_missing_row",
                        message="batch completed without a terminal output record for the submitted item",
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
                max_attempts=context.job.retry_policy.max_attempts,
            )

    @staticmethod
    def _item_failure(
        *,
        error_class: str,
        message: str,
        raw_error: Any,
        retryable: bool,
    ):
        from batchor.models import ItemFailure

        return ItemFailure(
            error_class=error_class,
            message=message,
            raw_error=raw_error,
            retryable=retryable,
        )

    @staticmethod
    def _batch_failure_error(remote: BatchRemoteRecord):
        from batchor.models import ItemFailure

        errors = remote.get("errors")
        error_class = "enqueue_token_limit" if is_enqueue_token_limit_error(errors) else f"batch_terminal_{remote.get('status', 'failed')}"
        return ItemFailure(
            error_class=error_class,
            message="batch did not complete successfully",
            raw_error=errors if errors is not None else cast(JSONObject, dict(remote)),
            retryable=True,
        )

    @staticmethod
    def _normalize_prompt_parts(prompt_value: PromptParts | str) -> PromptParts:
        if isinstance(prompt_value, PromptParts):
            return prompt_value
        return PromptParts(prompt=str(prompt_value))

    def _prepare_item(self, item: ClaimedItem, context: _RunContext) -> _PreparedItem:
        prompt_parts = self._normalize_prompt_parts(
            context.job.build_prompt(
                BatchItem(
                    item_id=item.item_id,
                    payload=item.payload,
                    metadata=item.metadata,
                )
            )
        )
        request_line = context.provider.build_request_line(
            custom_id=self.make_custom_id(item.item_id, item.attempt_count + 1),
            prompt_parts=prompt_parts,
            structured_output=context.structured_output,
        )
        request_bytes = len((json.dumps(request_line, ensure_ascii=False) + "\n").encode("utf-8"))
        submission_tokens = estimate_request_tokens(
            request_line,
            chars_per_token=context.job.chunk_policy.chars_per_token,
            model=context.job.provider_config.model,
        )
        return _PreparedItem(
            item_id=item.item_id,
            custom_id=str(request_line["custom_id"]),
            request_line=request_line,
            request_bytes=request_bytes,
            submission_tokens=submission_tokens,
        )

    @staticmethod
    def _prepared_dict(item: _PreparedItem) -> dict[str, Any]:
        return {
            "item_id": item.item_id,
            "custom_id": item.custom_id,
            "request_line": item.request_line,
            "request_bytes": item.request_bytes,
            "submission_tokens": item.submission_tokens,
        }

    def _context(self, run_id: str) -> _RunContext:
        if run_id not in self._contexts:
            raise KeyError(f"unknown run_id: {run_id}")
        return self._contexts[run_id]
