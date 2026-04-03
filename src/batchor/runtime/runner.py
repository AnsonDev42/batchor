"""BatchRunner — the durable orchestrator for creating and managing batch runs.

The :class:`BatchRunner` is the main entry point for library users.  It wires
together a :class:`~batchor.StateStore`, a provider registry, an artifact
store, and the execution layer to provide a single object for:

* Creating new runs (:meth:`BatchRunner.start`).
* Resuming existing runs (:meth:`BatchRunner.get_run`).
* Controlling runs (pause / resume / cancel).
* Exporting and pruning artifacts.
* Reading terminal results in a paginated, cursor-based manner.

The returned :class:`~batchor.Run` handle is a thin wrapper around the run
state that delegates actual work back to the runner.
"""

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

from batchor.artifacts import ArtifactStore, LocalArtifactStore
from batchor.core.enums import ProviderKind, RunControlState, RunLifecycleStatus
from batchor.core.exceptions import ModelResolutionError, RunNotFinishedError
from batchor.core.models import (
    ArtifactExportResult,
    ArtifactPruneResult,
    BatchJob,
    BatchResultItem,
    PromptParts,
    RunEvent,
    StructuredItemResult,
    TerminalResultsExportResult,
    TerminalResultsPage,
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
from batchor.sources.base import CheckpointedItemSource
from batchor.storage.registry import StorageRegistry, build_default_storage_registry
from batchor.storage.state import (
    ClaimedItem,
    IngestCheckpoint,
    MaterializedItem,
    PersistedItemRecord,
    PersistedRunConfig,
    StateStore,
)


class BatchRunner:
    """Durable orchestrator for creating, resuming, and inspecting batch runs."""

    _refresh_run = _refresh_run
    _submit_pending_items = _submit_pending_items

    def __init__(
        self,
        *,
        storage: str | StateStore | None = None,
        provider_registry: ProviderRegistry | None = None,
        storage_registry: StorageRegistry | None = None,
        provider_factory: Callable[[Any], BatchProvider] | None = None,
        observer: Callable[[RunEvent], None] | None = None,
        sleep: Callable[[float], None] | None = None,
        artifact_store: ArtifactStore | None = None,
        temp_root: str | Path | None = None,
    ) -> None:
        self.provider_registry = provider_registry or build_default_provider_registry()
        self.storage_registry = storage_registry or build_default_storage_registry(
            provider_registry=self.provider_registry
        )
        self.state = self._resolve_storage(storage)
        self.provider_factory = provider_factory
        self.observer = observer
        self.sleep = sleep or time.sleep
        self.artifact_store = self._resolve_artifact_store(
            artifact_store=artifact_store,
            temp_root=temp_root,
        )
        self.temp_root = (
            self.artifact_store.root
            if isinstance(self.artifact_store, LocalArtifactStore)
            else Path(tempfile.gettempdir()) / "batchor"
        )
        self._contexts: dict[str, _RunContext] = {}
        self._request_artifact_cache: dict[str, list[str]] = {}

    def start(
        self,
        job: BatchJob[Any, BaseModel],
        *,
        run_id: str | None = None,
    ) -> Run:
        """Create or resume a durable run for the given job."""
        resolved_run_id = run_id or generate_run_id()
        config = self._persisted_config_for_job(job)
        context = self._context_for_config(config=config, output_model=job.structured_output)
        self._contexts[resolved_run_id] = context
        if self.state.has_run(run_id=resolved_run_id):
            self._emit_event(
                "run_resumed",
                run_id=resolved_run_id,
                provider_kind=job.provider_config.provider_kind,
            )
            self._resume_existing_run(
                run_id=resolved_run_id,
                job=job,
                config=config,
                context=context,
            )
        else:
            self.state.create_run(run_id=resolved_run_id, config=config, items=[])
            self._emit_event(
                "run_created",
                run_id=resolved_run_id,
                provider_kind=job.provider_config.provider_kind,
            )
            source = self._checkpointed_source(job)
            if source is not None:
                identity = source.source_identity()
                self.state.set_ingest_checkpoint(
                    run_id=resolved_run_id,
                    checkpoint=IngestCheckpoint(
                        source_kind=identity.source_kind,
                        source_ref=identity.source_ref,
                        source_fingerprint=identity.source_fingerprint,
                        checkpoint_payload=source.initial_checkpoint(),
                    ),
                )
            self._ingest_job_items(
                run_id=resolved_run_id,
                job=job,
                context=context,
                start_index=0,
                checkpoint_payload=source.initial_checkpoint() if source is not None else None,
            )
        return Run(
            runner=self,
            run_id=resolved_run_id,
            context=context,
            summary=self.state.get_run_summary(run_id=resolved_run_id),
        )

    def run_and_wait(
        self,
        job: BatchJob[Any, BaseModel],
        *,
        run_id: str | None = None,
    ) -> Run:
        """Start a run and block until it reaches a terminal state."""
        run = self.start(job, run_id=run_id)
        return run.wait()

    def get_run(self, run_id: str) -> Run:
        """Rehydrate an existing durable run handle from storage."""
        self.state.requeue_local_items(run_id=run_id)
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

    def pause_run(self, run_id: str) -> Run:
        """Suspend execution of an active run.

        The run's control state is set to ``PAUSED``.  Subsequent calls to
        :meth:`~batchor.Run.refresh` return immediately without polling or
        submitting new items.

        Args:
            run_id: Identifier of the run to pause.

        Returns:
            A :class:`~batchor.Run` handle reflecting the paused state.
        """
        self.state.set_run_control_state(
            run_id=run_id,
            control_state=RunControlState.PAUSED,
        )
        return self.get_run(run_id)

    def resume_run(self, run_id: str) -> Run:
        """Resume a previously paused run.

        Args:
            run_id: Identifier of the run to resume.

        Returns:
            A :class:`~batchor.Run` handle with the control state reset to
            ``RUNNING``.

        Raises:
            ValueError: If the run's control state is ``CANCEL_REQUESTED``
                (a cancelling run cannot be resumed).
        """
        control_state = self.state.get_run_control_state(run_id=run_id)
        if control_state is RunControlState.CANCEL_REQUESTED:
            raise ValueError(f"run {run_id} is cancelling and cannot be resumed")
        self.state.set_run_control_state(
            run_id=run_id,
            control_state=RunControlState.RUNNING,
        )
        return self.get_run(run_id)

    def cancel_run(self, run_id: str) -> Run:
        """Request cancellation of an active run.

        The control state is set to ``CANCEL_REQUESTED``.  On the next
        refresh cycle the runner drains remaining in-flight batches and marks
        all non-terminal items as cancelled.

        Args:
            run_id: Identifier of the run to cancel.

        Returns:
            A :class:`~batchor.Run` handle reflecting the cancellation request.
        """
        self.state.set_run_control_state(
            run_id=run_id,
            control_state=RunControlState.CANCEL_REQUESTED,
        )
        return self.get_run(run_id)

    def read_terminal_results(
        self,
        run_id: str,
        *,
        after_sequence: int = 0,
        limit: int | None = None,
    ) -> TerminalResultsPage:
        """Read a page of terminal item results for cursor-based streaming.

        Args:
            run_id: Identifier of the run to read results from.
            after_sequence: Opaque cursor from the previous page's
                ``next_after_sequence``.  Pass ``0`` to start from the
                beginning.
            limit: Maximum number of results to return.  ``None`` returns all
                available results after the cursor.

        Returns:
            A :class:`~batchor.TerminalResultsPage` with items and an updated
            cursor.

        Raises:
            ValueError: If ``after_sequence`` is negative.
        """
        if after_sequence < 0:
            raise ValueError("after_sequence must be >= 0")
        records = self.state.get_terminal_item_records(
            run_id=run_id,
            after_sequence=after_sequence,
            limit=limit,
        )
        context = self._contexts.get(run_id)
        if context is None:
            config = self.state.get_run_config(run_id=run_id)
            output_model = self._resolve_output_model(config)
            context = self._context_for_config(config=config, output_model=output_model)
            self._contexts[run_id] = context
        next_after_sequence = after_sequence
        if records and records[-1].terminal_result_sequence is not None:
            next_after_sequence = records[-1].terminal_result_sequence
        return TerminalResultsPage(
            run_id=run_id,
            items=[self._result_from_record(record, context) for record in records],
            next_after_sequence=next_after_sequence,
        )

    def export_terminal_results(
        self,
        run_id: str,
        *,
        destination: str | Path,
        after_sequence: int = 0,
        append: bool = True,
        limit: int | None = None,
    ) -> TerminalResultsExportResult:
        """Export terminal item results to a JSONL file.

        Each result is serialised as a single JSON object per line.  The file
        can be extended incrementally by calling this method multiple times
        with the cursor returned in the previous result.

        Args:
            run_id: Identifier of the run to export results from.
            destination: Path to the output JSONL file.  Parent directories
                are created automatically.
            after_sequence: Cursor from a previous call.  Pass ``0`` to start
                from the beginning.
            append: When ``True`` (default), the file is opened in append
                mode; when ``False`` the file is overwritten.
            limit: Maximum number of results to export per call.

        Returns:
            A :class:`~batchor.TerminalResultsExportResult` with the file path,
            export count, and updated cursor.
        """
        page = self.read_terminal_results(
            run_id,
            after_sequence=after_sequence,
            limit=limit,
        )
        destination_path = Path(destination).expanduser().resolve()
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        mode = "a" if append else "w"
        with destination_path.open(mode, encoding="utf-8") as handle:
            for result in page.items:
                handle.write(json.dumps(self._serialize_result(result), ensure_ascii=False) + "\n")
        return TerminalResultsExportResult(
            run_id=run_id,
            destination_path=str(destination_path),
            exported_count=len(page.items),
            next_after_sequence=page.next_after_sequence,
        )

    def export_artifacts(
        self,
        run_id: str,
        *,
        destination_dir: str | Path,
    ) -> ArtifactExportResult:
        """Export retained run artifacts and a results manifest for a terminal run."""
        summary = self.state.get_run_summary(run_id=run_id)
        self._require_artifact_terminal_status(run_id=run_id, status=summary.status)
        destination_root = Path(destination_dir).expanduser().resolve()
        export_root = destination_root / run_id
        export_root.mkdir(parents=True, exist_ok=True)
        inventory = self.state.get_artifact_inventory(run_id=run_id)
        exported_artifact_paths: list[str] = []
        for artifact_path in (
            inventory.request_artifact_paths
            + inventory.output_artifact_paths
            + inventory.error_artifact_paths
        ):
            self.artifact_store.export_to_directory(artifact_path, export_root)
            exported_artifact_paths.append(artifact_path)
        results_path = export_root / "results.jsonl"
        self._write_results_export(run_id=run_id, results_path=results_path)
        manifest_path = export_root / "manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "run_id": run_id,
                    "request_artifact_paths": inventory.request_artifact_paths,
                    "output_artifact_paths": inventory.output_artifact_paths,
                    "error_artifact_paths": inventory.error_artifact_paths,
                    "results_path": "results.jsonl",
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        self.state.mark_artifacts_exported(run_id=run_id, export_root=str(export_root))
        self._emit_event(
            "artifacts_exported",
            run_id=run_id,
            provider_kind=context.config.provider_config.provider_kind if (context := self._contexts.get(run_id)) else None,
            data={"destination_dir": str(export_root)},
        )
        return ArtifactExportResult(
            run_id=run_id,
            destination_dir=str(export_root),
            manifest_path=str(manifest_path),
            results_path=str(results_path),
            exported_artifact_paths=exported_artifact_paths,
        )

    def prune_artifacts(
        self,
        run_id: str,
        *,
        include_raw_output_artifacts: bool = False,
    ) -> ArtifactPruneResult:
        """Remove retained artifacts for a terminal run and clear stored pointers."""
        summary = self.state.get_run_summary(run_id=run_id)
        self._require_artifact_terminal_status(run_id=run_id, status=summary.status)
        inventory = self.state.get_artifact_inventory(run_id=run_id)
        removed: list[str] = []
        missing: list[str] = []
        request_artifact_paths = inventory.request_artifact_paths
        if request_artifact_paths:
            removed, missing = self._remove_artifacts(request_artifact_paths)
            cleared_item_pointers = self.state.clear_request_artifact_pointers(
                run_id=run_id,
                artifact_paths=request_artifact_paths,
            )
            self._prune_empty_artifact_dirs(request_artifact_paths)
        else:
            cleared_item_pointers = 0
        cleared_batch_pointers = 0
        if include_raw_output_artifacts:
            if inventory.exported_at is None or inventory.export_root is None:
                raise ValueError(
                    f"raw output artifacts require export before pruning: {run_id}"
                )
            raw_artifact_paths = inventory.output_artifact_paths + inventory.error_artifact_paths
            if raw_artifact_paths:
                raw_removed, raw_missing = self._remove_artifacts(raw_artifact_paths)
                removed.extend(raw_removed)
                missing.extend(raw_missing)
                cleared_batch_pointers = self.state.clear_batch_artifact_pointers(
                    run_id=run_id,
                    artifact_paths=raw_artifact_paths,
                )
                self._prune_empty_artifact_dirs(raw_artifact_paths)
        self._emit_event(
            "artifacts_pruned",
            run_id=run_id,
            provider_kind=context.config.provider_config.provider_kind if (context := self._contexts.get(run_id)) else None,
            data={
                "removed_artifact_count": len(removed),
                "include_raw_output_artifacts": include_raw_output_artifacts,
            },
        )
        return ArtifactPruneResult(
            run_id=run_id,
            removed_artifact_paths=removed,
            missing_artifact_paths=missing,
            cleared_item_pointers=cleared_item_pointers,
            cleared_batch_pointers=cleared_batch_pointers,
        )

    @staticmethod
    def _require_artifact_terminal_status(
        *,
        run_id: str,
        status: RunLifecycleStatus,
    ) -> None:
        if status not in (
            RunLifecycleStatus.COMPLETED,
            RunLifecycleStatus.COMPLETED_WITH_FAILURES,
        ):
            raise RunNotFinishedError(run_id)

    @staticmethod
    def make_custom_id(item_id: str, attempt: int) -> str:
        return f"{item_id}:a{attempt}"

    def _resolve_storage(self, storage: str | StateStore | None) -> StateStore:
        if storage is None:
            return self.storage_registry.create("sqlite")
        if isinstance(storage, str):
            return self.storage_registry.create(storage)
        return storage

    def _resolve_artifact_store(
        self,
        *,
        artifact_store: ArtifactStore | None,
        temp_root: str | Path | None,
    ) -> ArtifactStore:
        if artifact_store is not None:
            return artifact_store
        if temp_root is not None:
            return LocalArtifactStore(temp_root)
        artifact_root = getattr(self.state, "artifact_root", None)
        if artifact_root is not None:
            return LocalArtifactStore(cast(str | Path, artifact_root))
        return LocalArtifactStore(Path(tempfile.gettempdir()) / "batchor")

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
            artifact_policy=job.artifact_policy,
            schema_name=job.schema_name,
            structured_output_module=structured_output_module,
            structured_output_qualname=structured_output_qualname,
        )

    def _resume_existing_run(
        self,
        *,
        run_id: str,
        job: BatchJob[Any, BaseModel],
        config: PersistedRunConfig,
        context: _RunContext,
    ) -> None:
        stored_config = self.state.get_run_config(run_id=run_id)
        if not self._configs_match_for_resume(stored_config, config):
            raise ValueError(f"existing run config does not match supplied job: {run_id}")
        self.state.requeue_local_items(run_id=run_id)
        control_state = self.state.get_run_control_state(run_id=run_id)
        if control_state is RunControlState.CANCEL_REQUESTED:
            return
        checkpoint = self.state.get_ingest_checkpoint(run_id=run_id)
        if checkpoint is not None and not checkpoint.ingestion_complete:
            source = self._require_checkpointed_source(job, run_id=run_id)
            self._validate_checkpoint_source(run_id=run_id, source=source, checkpoint=checkpoint)
            if control_state is RunControlState.PAUSED:
                return
            self._ingest_job_items(
                run_id=run_id,
                job=job,
                context=context,
                start_index=checkpoint.next_item_index,
                checkpoint_payload=checkpoint.checkpoint_payload,
            )
            return
        if control_state is RunControlState.PAUSED:
            return
        self._submit_pending_items(run_id, context)

    def _ingest_job_items(
        self,
        *,
        run_id: str,
        job: BatchJob[Any, BaseModel],
        context: _RunContext,
        start_index: int,
        checkpoint_payload: JSONValue | None,
    ) -> None:
        next_item_index = start_index
        next_checkpoint_payload = checkpoint_payload
        checkpointed = self._checkpointed_source(job) is not None
        ingestion_complete = True
        for item_chunk, next_item_index, next_checkpoint_payload in self._materialize_item_chunks(
            job,
            start_index=start_index,
            checkpoint_payload=checkpoint_payload,
        ):
            self.state.append_items(run_id=run_id, items=item_chunk)
            self._emit_event(
                "items_ingested",
                run_id=run_id,
                provider_kind=context.config.provider_config.provider_kind,
                data={
                    "chunk_item_count": len(item_chunk),
                    "last_item_index": item_chunk[-1].item_index,
                },
            )
            if checkpointed:
                self.state.update_ingest_checkpoint(
                    run_id=run_id,
                    next_item_index=next_item_index,
                    checkpoint_payload=next_checkpoint_payload,
                    ingestion_complete=False,
                )
            control_state = self.state.get_run_control_state(run_id=run_id)
            if control_state is RunControlState.CANCEL_REQUESTED:
                break
            self._submit_pending_items(run_id, context)
            control_state = self.state.get_run_control_state(run_id=run_id)
            if control_state is not RunControlState.RUNNING:
                ingestion_complete = control_state is RunControlState.CANCEL_REQUESTED
                break
        if checkpointed:
            self.state.update_ingest_checkpoint(
                run_id=run_id,
                next_item_index=next_item_index,
                checkpoint_payload=next_checkpoint_payload,
                ingestion_complete=ingestion_complete,
            )

    def _materialize_item_chunks(
        self,
        job: BatchJob[Any, BaseModel],
        *,
        start_index: int = 0,
        checkpoint_payload: JSONValue | None = None,
        chunk_size: int = 1000,
    ) -> Iterator[tuple[list[MaterializedItem], int, JSONValue | None]]:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be > 0")
        current_chunk: list[MaterializedItem] = []
        seen_ids: set[str] = set()
        source = self._checkpointed_source(job)
        current_index = start_index
        next_checkpoint = checkpoint_payload
        if source is not None:
            source_checkpoint = (
                checkpoint_payload
                if checkpoint_payload is not None
                else source.initial_checkpoint()
            )
            for source_item in source.iter_from_checkpoint(source_checkpoint):
                item_index = current_index
                item = source_item.item
                next_checkpoint = source_item.next_checkpoint
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
                current_index = item_index + 1
                if len(current_chunk) >= chunk_size:
                    yield current_chunk, current_index, next_checkpoint
                    current_chunk = []
        if current_chunk:
            yield current_chunk, current_index, next_checkpoint

    @staticmethod
    def _checkpointed_source(
        job: BatchJob[Any, BaseModel],
    ) -> CheckpointedItemSource[Any] | None:
        if isinstance(job.items, CheckpointedItemSource):
            return cast(CheckpointedItemSource[Any], job.items)
        return None

    def _require_checkpointed_source(
        self,
        job: BatchJob[Any, BaseModel],
        *,
        run_id: str,
    ) -> CheckpointedItemSource[Any]:
        source = self._checkpointed_source(job)
        if source is None:
            raise ValueError(f"run requires a checkpointed item source for restart: {run_id}")
        return source

    @staticmethod
    def _validate_checkpoint_source(
        *,
        run_id: str,
        source: CheckpointedItemSource[Any],
        checkpoint: IngestCheckpoint,
    ) -> None:
        identity = source.source_identity()
        if identity.source_kind != checkpoint.source_kind:
            raise ValueError(f"source kind mismatch for resumed run: {run_id}")
        if identity.source_ref != checkpoint.source_ref:
            raise ValueError(f"source path mismatch for resumed run: {run_id}")
        if identity.source_fingerprint != checkpoint.source_fingerprint:
            raise ValueError(f"source fingerprint mismatch for resumed run: {run_id}")

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

    def _emit_event(
        self,
        event_type: str,
        *,
        run_id: str,
        provider_kind: ProviderKind | None = None,
        data: JSONObject | None = None,
    ) -> None:
        if self.observer is None:
            return
        try:
            self.observer(
                RunEvent(
                    event_type=event_type,
                    run_id=run_id,
                    provider_kind=provider_kind,
                    data={} if data is None else data,
                )
            )
        except Exception:  # noqa: BLE001
            return

    def _configs_match_for_resume(
        self,
        stored_config: PersistedRunConfig,
        supplied_config: PersistedRunConfig,
    ) -> bool:
        if (
            stored_config.chunk_policy != supplied_config.chunk_policy
            or stored_config.retry_policy != supplied_config.retry_policy
            or stored_config.batch_metadata != supplied_config.batch_metadata
            or stored_config.artifact_policy != supplied_config.artifact_policy
            or stored_config.schema_name != supplied_config.schema_name
            or stored_config.structured_output_module
            != supplied_config.structured_output_module
            or stored_config.structured_output_qualname
            != supplied_config.structured_output_qualname
        ):
            return False
        return self.provider_registry.dump_config(
            stored_config.provider_config,
            include_secrets=False,
        ) == self.provider_registry.dump_config(
            supplied_config.provider_config,
            include_secrets=False,
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

    def _prepare_item(
        self,
        item: ClaimedItem,
        context: _RunContext,
        *,
        artifact_cache: dict[str, list[str]] | None = None,
    ) -> _PreparedItem:
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
                artifact_cache=artifact_cache,
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

    @staticmethod
    def _serialize_jsonl(records: list[JSONObject]) -> str:
        if not records:
            return ""
        return "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records)

    def _load_request_artifact_line(
        self,
        *,
        artifact_path: str,
        line_number: int,
        expected_sha256: str,
        artifact_cache: dict[str, list[str]] | None = None,
    ) -> JSONObject:
        if line_number <= 0:
            raise ValueError("line_number must be > 0")
        cache = self._request_artifact_cache if artifact_cache is None else artifact_cache
        lines = cache.get(artifact_path)
        if lines is None:
            lines = self.artifact_store.read_text(
                artifact_path,
                encoding="utf-8",
            ).splitlines()
            cache[artifact_path] = lines
        for index, raw_line in enumerate(lines, start=1):
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

    def _write_batch_result_artifacts(
        self,
        *,
        run_id: str,
        provider_batch_id: str,
        output_content: str | None,
        error_content: str | None,
        persist_raw_output_artifacts: bool,
    ) -> tuple[str | None, str | None]:
        if not persist_raw_output_artifacts:
            return None, None
        output_artifact_path = None
        if output_content is not None:
            output_artifact_path = (
                Path(run_id) / "outputs" / f"{provider_batch_id}_output.jsonl"
            ).as_posix()
            self.artifact_store.write_text(output_artifact_path, output_content, encoding="utf-8")
        error_artifact_path = None
        if error_content is not None:
            error_artifact_path = (
                Path(run_id) / "outputs" / f"{provider_batch_id}_error.jsonl"
            ).as_posix()
            self.artifact_store.write_text(error_artifact_path, error_content, encoding="utf-8")
        return output_artifact_path, error_artifact_path

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
        lineage = normalized.get("batchor_lineage")
        if lineage is not None and not isinstance(lineage, dict):
            raise TypeError(f"{label} batchor_lineage must be a JSON object when provided")
        return normalized

    def _artifact_full_path(self, artifact_path: str) -> Path:
        if not isinstance(self.artifact_store, LocalArtifactStore):
            raise ValueError("artifact store does not expose local artifact paths")
        return self.artifact_store.resolve_path(artifact_path)

    def _remove_artifacts(self, artifact_paths: list[str]) -> tuple[list[str], list[str]]:
        removed: list[str] = []
        missing: list[str] = []
        for artifact_path in artifact_paths:
            if self.artifact_store.delete(artifact_path):
                removed.append(artifact_path)
            else:
                missing.append(artifact_path)
        return removed, missing

    def _write_results_export(
        self,
        *,
        run_id: str,
        results_path: Path,
    ) -> None:
        run = self.get_run(run_id)
        lines = [
            json.dumps(self._serialize_result(result), ensure_ascii=False)
            for result in run.results()
        ]
        results_path.write_text(
            ("\n".join(lines) + "\n") if lines else "",
            encoding="utf-8",
        )

    @staticmethod
    def _serialize_result(result: BatchResultItem) -> JSONObject:
        payload: JSONObject = {
            "item_id": result.item_id,
            "status": result.status.value,
            "attempt_count": result.attempt_count,
            "metadata": result.metadata,
            "output_text": result.output_text,
            "raw_response": result.raw_response,
            "error": None,
        }
        if result.error is not None:
            payload["error"] = {
                "error_class": result.error.error_class,
                "message": result.error.message,
                "retryable": result.error.retryable,
                "raw_error": result.error.raw_error,
            }
        if isinstance(result, StructuredItemResult):
            output = result.output
            payload["output_json"] = (
                cast(BaseModel, output).model_dump(mode="json")
                if output is not None
                else None
            )
        return payload

    def _prune_empty_artifact_dirs(self, artifact_paths: list[str]) -> None:
        del artifact_paths
