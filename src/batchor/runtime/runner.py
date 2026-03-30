from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import importlib
import json
from pathlib import Path
import tempfile
import time
from typing import Any, Callable, Iterator, cast
from uuid import uuid4

from pydantic import BaseModel

from batchor.core.enums import RunLifecycleStatus
from batchor.core.exceptions import ModelResolutionError, RunNotFinishedError
from batchor.core.models import (
    BatchItem,
    BatchJob,
    BatchResultItem,
    OpenAIProviderConfig,
    PromptParts,
    RunSnapshot,
    RunSummary,
    StructuredItemResult,
    TextItemResult,
)
from batchor.core.types import BatchRemoteRecord, BatchRequestLine, JSONObject, JSONValue
from batchor.providers.base import (
    BatchProvider,
    StructuredOutputSchema,
)
from batchor.providers.registry import (
    ProviderRegistry,
    build_default_provider_registry,
)
from batchor.runtime.retry import (
    classify_batch_error,
    is_enqueue_token_limit_error,
    is_retryable_batch_control_plane_error,
)
from batchor.runtime.tokens import (
    chunk_request_rows,
    effective_inflight_token_budget,
    resolve_openai_batch_token_limit,
    split_rows_by_token_limit,
)
from batchor.runtime.validation import (
    model_output_schema,
    parse_structured_response,
    parse_text_response,
)
from batchor.storage.registry import StorageRegistry, build_default_storage_registry
from batchor.storage.state import (
    ClaimedItem,
    CompletedItemRecord,
    ItemFailureRecord,
    MaterializedItem,
    PersistedRunConfig,
    PersistedItemRecord,
    PreparedSubmission,
    QueuedItemFailureRecord,
    RequestArtifactPointer,
    StateStore,
)


@dataclass(frozen=True)
class _PreparedItem:
    item_id: str
    custom_id: str
    request_line: JSONObject
    request_bytes: int
    submission_tokens: int


@dataclass(frozen=True)
class _RunContext:
    config: PersistedRunConfig
    provider: BatchProvider
    output_model: type[BaseModel] | None
    structured_output: StructuredOutputSchema | None


def generate_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"batchor_{timestamp}_{uuid4().hex[:8]}"


class Run:
    def __init__(
        self,
        *,
        runner: BatchRunner,
        run_id: str,
        context: _RunContext,
        summary: RunSummary,
    ) -> None:
        self._runner = runner
        self._context = context
        self.run_id = run_id
        self._summary = summary

    @property
    def status(self) -> RunLifecycleStatus:
        return self._summary.status

    @property
    def is_finished(self) -> bool:
        return self.status is RunLifecycleStatus.COMPLETED

    def refresh(self) -> RunSummary:
        self._summary = self._runner._refresh_run(self.run_id, self._context)
        return self._summary

    def wait(
        self,
        *,
        timeout: float | None = None,
        poll_interval: float | None = None,
    ) -> Run:
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            self.refresh()
            if self.is_finished:
                return self
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"timed out waiting for run {self.run_id}")
            sleep_for = (
                poll_interval
                if poll_interval is not None
                else self._context.config.provider_config.poll_interval_sec
            )
            if self._summary.backoff_remaining_sec > 0:
                if sleep_for > 0:
                    sleep_for = min(sleep_for, self._summary.backoff_remaining_sec)
                else:
                    sleep_for = self._summary.backoff_remaining_sec
            if sleep_for > 0:
                self._runner.sleep(sleep_for)

    def summary(self) -> RunSummary:
        self._summary = self._runner.state.get_run_summary(run_id=self.run_id)
        return self._summary

    def snapshot(self) -> RunSnapshot:
        self._summary = self._runner.state.get_run_summary(run_id=self.run_id)
        return RunSnapshot(
            run_id=self._summary.run_id,
            status=self._summary.status,
            total_items=self._summary.total_items,
            completed_items=self._summary.completed_items,
            failed_items=self._summary.failed_items,
            status_counts=dict(self._summary.status_counts),
            active_batches=self._summary.active_batches,
            backoff_remaining_sec=self._summary.backoff_remaining_sec,
            items=self._runner._results_for_run(self.run_id, self._context),
        )

    def results(self) -> list[BatchResultItem]:
        if not self.is_finished:
            raise RunNotFinishedError(self.run_id)
        return self._runner._results_for_run(self.run_id, self._context)


class BatchRunner:
    def __init__(
        self,
        *,
        storage: str | StateStore | None = None,
        provider_registry: ProviderRegistry | None = None,
        storage_registry: StorageRegistry | None = None,
        provider_factory: Callable[[Any], BatchProvider] | None = None,
        sleep: Callable[[float], None] | None = None,
        temp_root: str | Path | None = None,
    ) -> None:
        self.provider_registry = provider_registry or build_default_provider_registry()
        self.storage_registry = storage_registry or build_default_storage_registry(
            provider_registry=self.provider_registry
        )
        self.state = self._resolve_storage(storage)
        self.provider_factory = provider_factory
        self.sleep = sleep or time.sleep
        if temp_root is not None:
            self.temp_root = Path(temp_root)
        else:
            artifact_root = getattr(self.state, "artifact_root", None)
            self.temp_root = (
                Path(cast(str | Path, artifact_root))
                if artifact_root is not None
                else Path(tempfile.gettempdir()) / "batchor"
            )
        self._contexts: dict[str, _RunContext] = {}

    def start(self, job: BatchJob[Any, BaseModel]) -> Run:
        run_id = generate_run_id()
        config = self._persisted_config_for_job(job)
        self.state.create_run(run_id=run_id, config=config, items=[])
        context = self._context_for_config(config=config, output_model=job.structured_output)
        self._contexts[run_id] = context
        for item_chunk in self._materialize_item_chunks(job):
            self.state.append_items(run_id=run_id, items=item_chunk)
            self._submit_pending_items(run_id, context)
        return Run(
            runner=self,
            run_id=run_id,
            context=context,
            summary=self.state.get_run_summary(run_id=run_id),
        )

    def run_and_wait(self, job: BatchJob[Any, BaseModel]) -> Run:
        run = self.start(job)
        return run.wait()

    def get_run(self, run_id: str) -> Run:
        context = self._contexts.get(run_id)
        if context is None:
            config = self.state.get_run_config(run_id=run_id)
            output_model = self._resolve_output_model(config)
            context = self._context_for_config(config=config, output_model=output_model)
            self._contexts[run_id] = context
        return Run(
            runner=self,
            run_id=run_id,
            context=context,
            summary=self.state.get_run_summary(run_id=run_id),
        )

    @staticmethod
    def make_custom_id(item_id: str, attempt: int) -> str:
        return f"{item_id}:a{attempt}"

    def _resolve_storage(self, storage: str | StateStore | None) -> StateStore:
        if storage is None:
            return self.storage_registry.create("sqlite")
        if isinstance(storage, str):
            return self.storage_registry.create(storage)
        return storage

    def _create_provider(self, provider_config: Any) -> BatchProvider:
        if self.provider_factory is not None:
            return self.provider_factory(provider_config)
        return self.provider_registry.create(provider_config)

    def _persisted_config_for_job(self, job: BatchJob[Any, BaseModel]) -> PersistedRunConfig:
        structured_output_module = None
        structured_output_qualname = None
        if job.structured_output is not None:
            structured_output_module = job.structured_output.__module__
            structured_output_qualname = job.structured_output.__qualname__
        return PersistedRunConfig(
            provider_config=job.provider_config,
            chunk_policy=job.chunk_policy,
            retry_policy=job.retry_policy,
            batch_metadata=dict(job.batch_metadata),
            schema_name=job.schema_name,
            structured_output_module=structured_output_module,
            structured_output_qualname=structured_output_qualname,
        )

    def _materialize_item_chunks(
        self,
        job: BatchJob[Any, BaseModel],
        *,
        chunk_size: int = 1000,
    ) -> Iterator[list[MaterializedItem]]:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be > 0")
        current_chunk: list[MaterializedItem] = []
        seen_ids: set[str] = set()
        for item_index, item in enumerate(job.items):
            if item.item_id in seen_ids:
                raise ValueError(f"duplicate item_id: {item.item_id}")
            seen_ids.add(item.item_id)
            prompt_parts = self._normalize_prompt_parts(job.build_prompt(item))
            current_chunk.append(
                MaterializedItem(
                    item_id=item.item_id,
                    item_index=item_index,
                    payload=self._json_value(item.payload, label=f"payload for {item.item_id}"),
                    metadata=self._json_object(item.metadata, label=f"metadata for {item.item_id}"),
                    prompt=prompt_parts.prompt,
                    system_prompt=prompt_parts.system_prompt,
                )
            )
            if len(current_chunk) >= chunk_size:
                yield current_chunk
                current_chunk = []
        if current_chunk:
            yield current_chunk

    def _context_for_config(
        self,
        *,
        config: PersistedRunConfig,
        output_model: type[BaseModel] | None,
    ) -> _RunContext:
        structured_output = None
        if output_model is not None:
            schema_name, schema = model_output_schema(
                output_model,
                schema_name=config.schema_name,
            )
            structured_output = StructuredOutputSchema(name=schema_name, schema=schema)
        return _RunContext(
            config=config,
            provider=self._create_provider(config.provider_config),
            output_model=output_model,
            structured_output=structured_output,
        )

    def _resolve_output_model(
        self,
        config: PersistedRunConfig,
    ) -> type[BaseModel] | None:
        if not config.is_structured:
            return None
        module_name = config.structured_output_module
        qualname = config.structured_output_qualname
        if module_name is None or qualname is None:
            return None
        if "<locals>" in qualname:
            raise ModelResolutionError(module_name, qualname)
        try:
            module = importlib.import_module(module_name)
        except Exception as exc:  # noqa: BLE001
            raise ModelResolutionError(module_name, qualname) from exc
        target: Any = module
        for attribute in qualname.split("."):
            target = getattr(target, attribute, None)
            if target is None:
                raise ModelResolutionError(module_name, qualname)
        if not isinstance(target, type) or not issubclass(target, BaseModel):
            raise ModelResolutionError(module_name, qualname)
        return cast(type[BaseModel], target)

    def _refresh_run(self, run_id: str, context: _RunContext) -> RunSummary:
        self._poll_once(run_id, context)
        self._submit_pending_items(run_id, context)
        return self.state.get_run_summary(run_id=run_id)

    def _submit_pending_items(self, run_id: str, context: _RunContext) -> int:
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
        batch_token_limit = self._batch_token_limit(config.provider_config)
        inflight_budget = self._inflight_budget(config.provider_config)
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
                            error=self._oversized_request_failure(
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
                            error=self._oversized_request_failure(
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
            if not self._has_inflight_capacity(
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
        self,
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

    @staticmethod
    def _inflight_budget(provider_config: Any) -> int | None:
        if not isinstance(provider_config, OpenAIProviderConfig):
            return None
        return effective_inflight_token_budget(provider_config.enqueue_limits)

    @staticmethod
    def _batch_token_limit(provider_config: Any) -> int | None:
        if not isinstance(provider_config, OpenAIProviderConfig):
            return None
        return resolve_openai_batch_token_limit(provider_config.enqueue_limits)

    @staticmethod
    def _oversized_request_failure(
        *,
        provider_config: Any,
        limit_type: str,
        submission_tokens: int,
        limit: int,
    ):
        from batchor.core.models import ItemFailure

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
                    base_delay_sec=context.config.retry_policy.base_backoff_sec,
                    max_delay_sec=context.config.retry_policy.max_backoff_sec,
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
                        error=self._item_failure(
                            error_class=getattr(exc, "error_class", "structured_output_validation_failed"),
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
                max_attempts=context.config.retry_policy.max_attempts,
            )

    @staticmethod
    def _item_failure(
        *,
        error_class: str,
        message: str,
        raw_error: JSONValue | JSONObject,
        retryable: bool,
    ):
        from batchor.core.models import ItemFailure

        return ItemFailure(
            error_class=error_class,
            message=message,
            raw_error=raw_error,
            retryable=retryable,
        )

    @staticmethod
    def _batch_failure_error(remote: BatchRemoteRecord):
        from batchor.core.models import ItemFailure

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
        custom_id = self.make_custom_id(item.item_id, item.attempt_count + 1)
        if item.request_artifact_path is not None:
            if item.request_artifact_line is None or item.request_sha256 is None:
                raise ValueError(
                    f"incomplete request artifact pointer for item {item.item_id}"
                )
            request_line = self._load_request_artifact_line(
                artifact_path=item.request_artifact_path,
                line_number=item.request_artifact_line,
                expected_sha256=item.request_sha256,
            )
            request_line["custom_id"] = custom_id
        else:
            prompt_parts = PromptParts(
                prompt=item.prompt,
                system_prompt=item.system_prompt,
            )
            request_line = context.provider.build_request_line(
                custom_id=custom_id,
                prompt_parts=prompt_parts,
                structured_output=context.structured_output,
            )
        request_line = cast(BatchRequestLine, request_line)
        request_bytes = len((json.dumps(request_line, ensure_ascii=False) + "\n").encode("utf-8"))
        submission_tokens = context.provider.estimate_request_tokens(
            request_line,
            chars_per_token=context.config.chunk_policy.chars_per_token,
        )
        return _PreparedItem(
            item_id=item.item_id,
            custom_id=str(request_line["custom_id"]),
            request_line=cast(JSONObject, request_line),
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

    @staticmethod
    def _request_sha256(request_line: JSONObject) -> str:
        encoded = json.dumps(
            request_line,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _request_artifact_relative_path(run_id: str) -> Path:
        return Path(run_id) / "requests" / f"requests_{uuid4().hex}.jsonl"

    def _load_request_artifact_line(
        self,
        *,
        artifact_path: str,
        line_number: int,
        expected_sha256: str,
    ) -> JSONObject:
        if line_number <= 0:
            raise ValueError("line_number must be > 0")
        path = self.temp_root / artifact_path
        with path.open("r", encoding="utf-8") as handle:
            for index, raw_line in enumerate(handle, start=1):
                if index != line_number:
                    continue
                record = json.loads(raw_line)
                if not isinstance(record, dict):
                    raise TypeError(
                        f"request artifact line must be a JSON object: {artifact_path}:{line_number}"
                    )
                request_line = cast(JSONObject, record)
                actual_sha256 = self._request_sha256(request_line)
                if actual_sha256 != expected_sha256:
                    raise ValueError(
                        "request artifact hash mismatch for "
                        f"{artifact_path}:{line_number}"
                    )
                return request_line
        raise FileNotFoundError(
            f"request artifact line missing: {artifact_path}:{line_number}"
        )

    def _results_for_run(
        self,
        run_id: str,
        context: _RunContext,
    ) -> list[BatchResultItem]:
        return [
            self._result_from_record(record, context)
            for record in self.state.get_item_records(run_id=run_id)
        ]

    def _result_from_record(
        self,
        record: PersistedItemRecord,
        context: _RunContext,
    ) -> BatchResultItem:
        if context.output_model is None:
            return TextItemResult(
                item_id=record.item_id,
                status=record.status,
                attempt_count=record.attempt_count,
                output_text=record.output_text,
                raw_response=record.raw_response,
                error=record.error,
                metadata=record.metadata,
            )
        output_model = None
        if record.output_json is not None:
            output_model = context.output_model.model_validate(record.output_json)
        return StructuredItemResult(
            item_id=record.item_id,
            status=record.status,
            attempt_count=record.attempt_count,
            output=output_model,
            output_text=record.output_text,
            raw_response=record.raw_response,
            error=record.error,
            metadata=record.metadata,
        )

    @staticmethod
    def _json_value(value: Any, *, label: str) -> JSONValue:
        try:
            return cast(JSONValue, json.loads(json.dumps(value, ensure_ascii=False)))
        except TypeError as exc:
            raise TypeError(f"{label} must be JSON-serializable") from exc

    def _json_object(self, value: Any, *, label: str) -> JSONObject:
        normalized = self._json_value(value, label=label)
        if not isinstance(normalized, dict):
            raise TypeError(f"{label} must be a JSON object")
        return normalized
