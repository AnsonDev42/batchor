"""BatchRunner public facade over the internal runtime execution layers."""

from __future__ import annotations

import json
import tempfile
import time
from pathlib import Path
from typing import Any, Callable, cast

from pydantic import BaseModel

from batchor.artifacts import ArtifactStore, LocalArtifactStore
from batchor.core.enums import ProviderKind, RunControlState, RunLifecycleStatus
from batchor.core.exceptions import RunNotFinishedError
from batchor.core.models import (
    ArtifactExportResult,
    ArtifactPruneResult,
    BatchJob,
    BatchResultItem,
    RunEvent,
    RunSummary,
    TerminalResultsExportResult,
    TerminalResultsPage,
)
from batchor.core.types import JSONObject
from batchor.providers.base import BatchProvider
from batchor.providers.registry import ProviderRegistry, build_default_provider_registry
from batchor.runtime.artifacts import remove_artifacts
from batchor.runtime.context import (
    RunContext,
    build_persisted_config,
    build_run_context,
    configs_match_for_resume,
    resolve_output_model,
)
from batchor.runtime.ingestion import (
    IngestionDeps,
    checkpointed_source,
    ingest_job_items,
    resume_existing_run,
)
from batchor.runtime.polling import PollingDeps, refresh_run
from batchor.runtime.results import result_from_record, results_for_records, serialize_result, write_results_export
from batchor.runtime.run_handle import Run, generate_run_id
from batchor.runtime.submission import SubmissionDeps, submit_pending_items
from batchor.storage.registry import StorageRegistry, build_default_storage_registry
from batchor.storage.state import IngestCheckpoint, PersistedRunConfig, StateStore


class BatchRunner:
    """Durable orchestrator for creating, resuming, and inspecting batch runs."""

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
        """Initialize the runner facade and its internal runtime layers.

        Args:
            storage: Storage backend instance or registered storage kind.
            provider_registry: Provider registry used for config dumping and
                provider construction.
            storage_registry: Storage registry used for named storage
                backends.
            provider_factory: Optional provider factory override for tests.
            observer: Optional run event observer callback.
            sleep: Optional sleep function override for polling loops.
            artifact_store: Artifact store override.
            temp_root: Root used for the default local artifact store.
        """
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
        self._contexts: dict[str, RunContext] = {}
        self._submission_deps = SubmissionDeps(
            state=self.state,
            artifact_store=self.artifact_store,
            emit_event=self._emit_event,
        )
        self._polling_deps = PollingDeps(
            state=self.state,
            artifact_store=self.artifact_store,
            emit_event=self._emit_event,
        )
        self._ingestion_deps = IngestionDeps(
            state=self.state,
            emit_event=self._emit_event,
            submit_pending_items=self._submit_pending_items,
            configs_match_for_resume=self._configs_match_for_resume,
        )

    def start(
        self,
        job: BatchJob[Any, BaseModel],
        *,
        run_id: str | None = None,
    ) -> Run:
        """Create or resume a durable run for the given job.

        Args:
            job: Declarative batch job to start or resume.
            run_id: Optional durable run identifier.

        Returns:
            Durable run handle for the run.
        """
        resolved_run_id = run_id or generate_run_id()
        config = build_persisted_config(job)
        context = build_run_context(
            config=config,
            output_model=job.structured_output,
            create_provider=self._create_provider,
        )
        self._contexts[resolved_run_id] = context
        if self.state.has_run(run_id=resolved_run_id):
            self._emit_event(
                "run_resumed",
                run_id=resolved_run_id,
                provider_kind=job.provider_config.provider_kind,
            )
            resume_existing_run(
                self._ingestion_deps,
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
            source = checkpointed_source(job)
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
            ingest_job_items(
                self._ingestion_deps,
                run_id=resolved_run_id,
                job=job,
                context=context,
                start_index=0,
                checkpoint_payload=source.initial_checkpoint() if source is not None else None,
            )
        return self._run_handle(resolved_run_id)

    def run_and_wait(
        self,
        job: BatchJob[Any, BaseModel],
        *,
        run_id: str | None = None,
    ) -> Run:
        """Start a run and block until it reaches a terminal state.

        Args:
            job: Declarative batch job to start.
            run_id: Optional durable run identifier.

        Returns:
            Durable run handle after it becomes terminal.
        """
        run = self.start(job, run_id=run_id)
        return run.wait()

    def get_run(self, run_id: str) -> Run:
        """Rehydrate an existing durable run handle from storage.

        Args:
            run_id: Durable run identifier.

        Returns:
            Rehydrated durable run handle.
        """
        self.state.requeue_local_items(run_id=run_id)
        self._context_for_run(run_id)
        return self._run_handle(run_id)

    def pause_run(self, run_id: str) -> Run:
        """Set a run's control state to paused.

        Args:
            run_id: Durable run identifier.

        Returns:
            Durable run handle with updated control state.
        """
        self.state.set_run_control_state(
            run_id=run_id,
            control_state=RunControlState.PAUSED,
        )
        return self.get_run(run_id)

    def resume_run(self, run_id: str) -> Run:
        """Resume a previously paused run.

        Args:
            run_id: Durable run identifier.

        Returns:
            Durable run handle with updated control state.

        Raises:
            ValueError: If the run is already cancelling.
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

        Args:
            run_id: Durable run identifier.

        Returns:
            Durable run handle with updated control state.
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
            run_id: Durable run identifier.
            after_sequence: Terminal-result cursor from the previous page.
            limit: Optional maximum number of items to return.

        Returns:
            Terminal results page.

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
        context = self._context_for_run(run_id)
        next_after_sequence = after_sequence
        if records and records[-1].terminal_result_sequence is not None:
            next_after_sequence = records[-1].terminal_result_sequence
        return TerminalResultsPage(
            run_id=run_id,
            items=[result_from_record(record, context=context) for record in records],
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

        Args:
            run_id: Durable run identifier.
            destination: Destination JSONL path.
            after_sequence: Terminal-result cursor from the previous page.
            append: Whether to append to the destination file.
            limit: Optional maximum number of items to export.

        Returns:
            Terminal results export report.
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
                handle.write(json.dumps(serialize_result(result), ensure_ascii=False) + "\n")
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
        """Export retained run artifacts and a results manifest for a terminal run.

        Args:
            run_id: Durable run identifier.
            destination_dir: Directory that will receive the exported run
                bundle.

        Returns:
            Artifact export report.
        """
        summary = self.state.get_run_summary(run_id=run_id)
        self._require_artifact_terminal_status(run_id=run_id, status=summary.status)
        destination_root = Path(destination_dir).expanduser().resolve()
        export_root = destination_root / run_id
        export_root.mkdir(parents=True, exist_ok=True)
        inventory = self.state.get_artifact_inventory(run_id=run_id)
        exported_artifact_paths: list[str] = []
        for artifact_path in (
            inventory.request_artifact_paths + inventory.output_artifact_paths + inventory.error_artifact_paths
        ):
            self.artifact_store.export_to_directory(artifact_path, export_root)
            exported_artifact_paths.append(artifact_path)
        results_path = export_root / "results.jsonl"
        write_results_export(
            results_path=results_path,
            results=self._results_for_run(run_id),
        )
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
            provider_kind=context.config.provider_config.provider_kind
            if (context := self._contexts.get(run_id))
            else None,
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
        """Remove retained artifacts for a terminal run and clear stored pointers.

        Args:
            run_id: Durable run identifier.
            include_raw_output_artifacts: Whether raw provider payloads should
                also be pruned.

        Returns:
            Artifact prune report.
        """
        summary = self.state.get_run_summary(run_id=run_id)
        self._require_artifact_terminal_status(run_id=run_id, status=summary.status)
        inventory = self.state.get_artifact_inventory(run_id=run_id)
        removed: list[str] = []
        missing: list[str] = []
        request_artifact_paths = inventory.request_artifact_paths
        if request_artifact_paths:
            removed, missing = remove_artifacts(self.artifact_store, request_artifact_paths)
            cleared_item_pointers = self.state.clear_request_artifact_pointers(
                run_id=run_id,
                artifact_paths=request_artifact_paths,
            )
        else:
            cleared_item_pointers = 0
        cleared_batch_pointers = 0
        if include_raw_output_artifacts:
            if inventory.exported_at is None or inventory.export_root is None:
                raise ValueError(f"raw output artifacts require export before pruning: {run_id}")
            raw_artifact_paths = inventory.output_artifact_paths + inventory.error_artifact_paths
            if raw_artifact_paths:
                raw_removed, raw_missing = remove_artifacts(self.artifact_store, raw_artifact_paths)
                removed.extend(raw_removed)
                missing.extend(raw_missing)
                cleared_batch_pointers = self.state.clear_batch_artifact_pointers(
                    run_id=run_id,
                    artifact_paths=raw_artifact_paths,
                )
        self._emit_event(
            "artifacts_pruned",
            run_id=run_id,
            provider_kind=context.config.provider_config.provider_kind
            if (context := self._contexts.get(run_id))
            else None,
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

    def _run_handle(self, run_id: str) -> Run:
        return Run(
            runner=self,
            run_id=run_id,
            summary=self.state.get_run_summary(run_id=run_id),
        )

    def _context_for_run(self, run_id: str) -> RunContext:
        context = self._contexts.get(run_id)
        if context is not None:
            return context
        config = self.state.get_run_config(run_id=run_id)
        output_model = resolve_output_model(config)
        context = build_run_context(
            config=config,
            output_model=output_model,
            create_provider=self._create_provider,
        )
        self._contexts[run_id] = context
        return context

    def _configs_match_for_resume(
        self,
        stored_config: PersistedRunConfig,
        supplied_config: PersistedRunConfig,
    ) -> bool:
        return configs_match_for_resume(
            stored_config=stored_config,
            supplied_config=supplied_config,
            provider_registry=self.provider_registry,
        )

    def _submit_pending_items(
        self,
        run_id: str,
        context: RunContext | None = None,
    ) -> int:
        return submit_pending_items(
            self._submission_deps,
            run_id=run_id,
            context=context or self._context_for_run(run_id),
        )

    def _refresh_run(self, run_id: str) -> RunSummary:
        return refresh_run(
            self._polling_deps,
            run_id=run_id,
            context=self._context_for_run(run_id),
            submit_pending_items=self._submit_pending_items,
        )

    def _results_for_run(self, run_id: str) -> list[BatchResultItem]:
        return results_for_records(
            self.state.get_item_records(run_id=run_id),
            context=self._context_for_run(run_id),
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
