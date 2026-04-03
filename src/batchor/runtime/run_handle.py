"""Durable :class:`Run` handle and supporting internal types.

The :class:`Run` object is returned by :meth:`~batchor.BatchRunner.start` and
:meth:`~batchor.BatchRunner.get_run`.  It encapsulates a run identifier and a
back-reference to the :class:`~batchor.BatchRunner`, providing the public API
for polling, waiting, controlling, and inspecting a single batch run.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import time
from typing import TYPE_CHECKING
from uuid import uuid4

from pydantic import BaseModel

from batchor.core.enums import RunControlState, RunLifecycleStatus
from batchor.core.exceptions import RunNotFinishedError, RunPausedError
from batchor.core.models import (
    ArtifactExportResult,
    ArtifactPruneResult,
    BatchResultItem,
    RunSnapshot,
    RunSummary,
    TerminalResultsExportResult,
    TerminalResultsPage,
)
from batchor.core.types import JSONObject
from batchor.providers.base import BatchProvider, StructuredOutputSchema
from batchor.storage.state import PersistedRunConfig

if TYPE_CHECKING:
    from batchor.runtime.runner import BatchRunner


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
    """Generate a timestamped durable run identifier."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"batchor_{timestamp}_{uuid4().hex[:8]}"


class Run:
    """Durable handle for polling, waiting on, and inspecting one batch run."""

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
        """Return the cached lifecycle status for the run."""
        return self._summary.status

    @property
    def control_state(self) -> RunControlState:
        """Return the cached operator control state for the run."""
        return self._summary.control_state

    @property
    def is_finished(self) -> bool:
        """Return whether the run is in a terminal lifecycle state."""
        return self.status in (RunLifecycleStatus.COMPLETED, RunLifecycleStatus.COMPLETED_WITH_FAILURES)

    def refresh(self) -> RunSummary:
        """Perform one poll-and-submit cycle and return the updated summary."""
        self._summary = self._runner._refresh_run(self.run_id, self._context)
        return self._summary

    def wait(
        self,
        *,
        timeout: float | None = None,
        poll_interval: float | None = None,
    ) -> Run:
        """Block until the run is terminal or the optional timeout expires."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            self.refresh()
            if self.is_finished:
                return self
            if self.control_state is RunControlState.PAUSED:
                raise RunPausedError(self.run_id)
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
        """Read the latest persisted summary for the run from storage."""
        self._summary = self._runner.state.get_run_summary(run_id=self.run_id)
        return self._summary

    def snapshot(self) -> RunSnapshot:
        """Return the current summary plus expanded terminal item payloads."""
        self._summary = self._runner.state.get_run_summary(run_id=self.run_id)
        return RunSnapshot(
            run_id=self._summary.run_id,
            status=self._summary.status,
            control_state=self._summary.control_state,
            total_items=self._summary.total_items,
            completed_items=self._summary.completed_items,
            failed_items=self._summary.failed_items,
            status_counts=dict(self._summary.status_counts),
            active_batches=self._summary.active_batches,
            backoff_remaining_sec=self._summary.backoff_remaining_sec,
            items=self._runner._results_for_run(self.run_id, self._context),
        )

    def results(self) -> list[BatchResultItem]:
        """Return terminal item results for the run."""
        if not self.is_finished:
            raise RunNotFinishedError(self.run_id)
        return self._runner._results_for_run(self.run_id, self._context)

    def prune_artifacts(
        self,
        *,
        include_raw_output_artifacts: bool = False,
    ) -> ArtifactPruneResult:
        """Prune retained artifacts for this terminal run."""
        self._summary = self._runner.state.get_run_summary(run_id=self.run_id)
        return self._runner.prune_artifacts(
            self.run_id,
            include_raw_output_artifacts=include_raw_output_artifacts,
        )

    def export_artifacts(self, destination_dir: str) -> ArtifactExportResult:
        """Export retained artifacts for this terminal run."""
        self._summary = self._runner.state.get_run_summary(run_id=self.run_id)
        return self._runner.export_artifacts(self.run_id, destination_dir=destination_dir)

    def pause(self) -> RunSummary:
        """Suspend execution of this run.

        Returns:
            Updated :class:`~batchor.RunSummary` with control state
            ``PAUSED``.
        """
        self._summary = self._runner.pause_run(self.run_id).summary()
        return self._summary

    def resume(self) -> RunSummary:
        """Resume this run after it has been paused.

        Returns:
            Updated :class:`~batchor.RunSummary` with control state
            ``RUNNING``.

        Raises:
            ValueError: If the run is in ``CANCEL_REQUESTED`` state.
        """
        self._summary = self._runner.resume_run(self.run_id).summary()
        return self._summary

    def cancel(self) -> RunSummary:
        """Request cancellation of this run.

        Returns:
            Updated :class:`~batchor.RunSummary` with control state
            ``CANCEL_REQUESTED``.
        """
        self._summary = self._runner.cancel_run(self.run_id).summary()
        return self._summary

    def read_terminal_results(
        self,
        *,
        after_sequence: int = 0,
        limit: int | None = None,
    ) -> TerminalResultsPage:
        """Read a page of terminal item results using cursor-based pagination.

        Args:
            after_sequence: Cursor from the previous page's
                ``next_after_sequence``.  Pass ``0`` to start from the
                beginning.
            limit: Maximum number of results to return per call.

        Returns:
            A :class:`~batchor.TerminalResultsPage` with items and an updated
            cursor.
        """
        return self._runner.read_terminal_results(
            self.run_id,
            after_sequence=after_sequence,
            limit=limit,
        )

    def export_terminal_results(
        self,
        destination: str,
        *,
        after_sequence: int = 0,
        append: bool = True,
        limit: int | None = None,
    ) -> TerminalResultsExportResult:
        """Export terminal item results to a JSONL file.

        Args:
            destination: Path to the output JSONL file.
            after_sequence: Cursor from a previous call.
            append: When ``True`` (default), the file is opened in append
                mode.
            limit: Maximum number of results to export per call.

        Returns:
            A :class:`~batchor.TerminalResultsExportResult` with the file
            path, export count, and updated cursor.
        """
        return self._runner.export_terminal_results(
            self.run_id,
            destination=destination,
            after_sequence=after_sequence,
            append=append,
            limit=limit,
        )
