from __future__ import annotations

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
    ArtifactPruneResult,
    BatchJob,
    BatchResultItem,
    PromptParts,
    StructuredItemResult,
    TextItemResult,
)
from batchor.core.types import BatchRequestLine, JSONObject, JSONValue
from batchor.providers.base import BatchProvider, StructuredOutputSchema
from batchor.providers.registry import (
    ProviderRegistry,
    build_default_provider_registry,
)
from batchor.runtime.run_handle import Run, _PreparedItem, _RunContext, generate_run_id
from batchor.runtime.runner_execution import _refresh_run, _submit_pending_items
from batchor.runtime.validation import model_output_schema
from batchor.storage.registry import StorageRegistry, build_default_storage_registry
from batchor.storage.state import (
    ClaimedItem,
    MaterializedItem,
    PersistedItemRecord,
    PersistedRunConfig,
    StateStore,
)


class BatchRunner:
    _refresh_run = _refresh_run
    _submit_pending_items = _submit_pending_items

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

    def prune_artifacts(self, run_id: str) -> ArtifactPruneResult:
        summary = self.state.get_run_summary(run_id=run_id)
        if summary.status is not RunLifecycleStatus.COMPLETED:
            raise RunNotFinishedError(run_id)
        artifact_paths = self.state.get_request_artifact_paths(run_id=run_id)
        if not artifact_paths:
            return ArtifactPruneResult(
                run_id=run_id,
                removed_artifact_paths=[],
                missing_artifact_paths=[],
                cleared_item_pointers=0,
            )
        removed: list[str] = []
        missing: list[str] = []
        for artifact_path in artifact_paths:
            path = self._artifact_full_path(artifact_path)
            if path.exists():
                if not path.is_file():
                    raise IsADirectoryError(f"artifact path is not a file: {artifact_path}")
                path.unlink()
                removed.append(artifact_path)
            else:
                missing.append(artifact_path)
        cleared_item_pointers = self.state.clear_request_artifact_pointers(
            run_id=run_id,
            artifact_paths=artifact_paths,
        )
        self._prune_empty_artifact_dirs(artifact_paths)
        return ArtifactPruneResult(
            run_id=run_id,
            removed_artifact_paths=removed,
            missing_artifact_paths=missing,
            cleared_item_pointers=cleared_item_pointers,
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
        path = self._artifact_full_path(artifact_path)
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

    def _artifact_full_path(self, artifact_path: str) -> Path:
        relative_path = Path(artifact_path)
        if relative_path.is_absolute():
            raise ValueError(f"artifact path must be relative: {artifact_path}")
        if ".." in relative_path.parts:
            raise ValueError(f"artifact path must not escape temp_root: {artifact_path}")
        return self.temp_root / relative_path

    def _prune_empty_artifact_dirs(self, artifact_paths: list[str]) -> None:
        candidate_dirs: set[Path] = set()
        for artifact_path in artifact_paths:
            for parent in self._artifact_full_path(artifact_path).parents:
                if parent == self.temp_root:
                    break
                candidate_dirs.add(parent)
        for directory in sorted(candidate_dirs, key=lambda path: len(path.parts), reverse=True):
            try:
                directory.rmdir()
            except OSError:
                continue
