"""Deep internal module for advancing durable runs through one execution cycle."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable

from pydantic import BaseModel

from batchor.core.enums import RunControlState
from batchor.core.exceptions import RunIngestionSourceRequiredError, RunSubmissionIndeterminateError
from batchor.core.models import BatchJob, RunSummary
from batchor.runtime.context import RunContext, build_persisted_config
from batchor.runtime.ingestion import IngestionDeps, resume_existing_run
from batchor.runtime.polling import PollingDeps, refresh_run
from batchor.storage.state import StateStore


@dataclass(frozen=True)
class CycleOutcome:
    """Observable result of one durable execution cycle."""

    summary: RunSummary
    progressed: bool


class RunExecutor:
    """Own run attachment, recovery, ingestion, polling, and submission ordering."""

    def __init__(
        self,
        *,
        state: StateStore,
        ingestion_deps: IngestionDeps,
        polling_deps: PollingDeps,
        context_for_run: Callable[[str], RunContext],
        submit_pending_items: Callable[[str, RunContext], int],
    ) -> None:
        self._state = state
        self._ingestion_deps = ingestion_deps
        self._polling_deps = polling_deps
        self._context_for_run = context_for_run
        self._submit_pending_items = submit_pending_items
        self._jobs: dict[str, BatchJob[Any, BaseModel]] = {}
        self._owners: set[str] = set()

    def start_or_attach(self, *, run_id: str, job: BatchJob[Any, BaseModel]) -> None:
        """Attach the source-bearing job to an execution owner in this process."""
        self._jobs[run_id] = job

    def resume_attached_ingestion(self, run_id: str, *, deadline: float | None = None) -> None:
        """Continue incomplete ingestion when this process still owns its source."""
        job = self._jobs.get(run_id)
        checkpoint = self._state.get_ingest_checkpoint(run_id=run_id)
        if checkpoint is None or checkpoint.ingestion_complete:
            return
        if job is None:
            raise RunIngestionSourceRequiredError(run_id)
        resume_existing_run(
            self._ingestion_deps,
            run_id=run_id,
            job=job,
            config=build_persisted_config(job),
            context=self._context_for_run(run_id),
            deadline=deadline,
        )
        self._owners.add(run_id)

    def require_attached_source(self, run_id: str) -> None:
        """Fail before changing control state when incomplete ingestion lacks a source."""
        checkpoint = self._state.get_ingest_checkpoint(run_id=run_id)
        if checkpoint is not None and not checkpoint.ingestion_complete and run_id not in self._jobs:
            raise RunIngestionSourceRequiredError(run_id)

    def advance(self, run_id: str, *, deadline: float | None = None) -> CycleOutcome:
        """Advance one run in recovery, poll, control, ingest, submit order."""
        before = self._state.get_run_summary(run_id=run_id)
        if _deadline_expired(deadline):
            return CycleOutcome(summary=before, progressed=False)
        if self._state.has_indeterminate_submission_intents(run_id=run_id):
            raise RunSubmissionIndeterminateError(run_id)
        if run_id not in self._owners:
            self._state.requeue_local_items(run_id=run_id)
            self._owners.add(run_id)

        control_state = self._state.get_run_control_state(run_id=run_id)
        if control_state is RunControlState.PAUSED:
            summary = self._state.get_run_summary(run_id=run_id)
            return CycleOutcome(summary=summary, progressed=False)
        if control_state is not RunControlState.CANCEL_REQUESTED:
            self.require_attached_source(run_id)
            self.resume_attached_ingestion(run_id, deadline=deadline)

        if _deadline_expired(deadline):
            summary = self._state.get_run_summary(run_id=run_id)
            return CycleOutcome(summary=summary, progressed=_summary_made_progress(before, summary))

        summary = refresh_run(
            self._polling_deps,
            run_id=run_id,
            context=self._context_for_run(run_id),
            submit_pending_items=self._submit_pending_items,
            deadline=deadline,
        )
        progressed = _summary_made_progress(before, summary)
        return CycleOutcome(summary=summary, progressed=progressed)


def _summary_made_progress(before: RunSummary, after: RunSummary) -> bool:
    return (
        after.completed_items > before.completed_items
        or after.failed_items > before.failed_items
        or after.active_batches != before.active_batches
        or after.status_counts != before.status_counts
    )


def _deadline_expired(deadline: float | None) -> bool:
    return deadline is not None and time.monotonic() >= deadline
