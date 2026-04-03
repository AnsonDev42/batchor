"""Durable :class:`Run` handle and run ID generation helpers.

The :class:`Run` object is returned by :meth:`~batchor.BatchRunner.start` and
:meth:`~batchor.BatchRunner.get_run`. It encapsulates a run identifier and a
back-reference to the :class:`~batchor.BatchRunner`, providing the public API
for polling, waiting, controlling, and inspecting a single durable run.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

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

if TYPE_CHECKING:
    from batchor.runtime.runner import BatchRunner


def generate_run_id() -> str:
    """Generate a timestamped durable run identifier.

    Returns:
        New durable run identifier.
    """
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"batchor_{timestamp}_{uuid4().hex[:8]}"


class Run:
    """Durable handle for polling, waiting on, and inspecting one batch run."""

    def __init__(
        self,
        *,
        runner: BatchRunner,
        run_id: str,
        summary: RunSummary,
    ) -> None:
        """Initialize the durable run handle.

        Args:
            runner: Owning runner facade.
            run_id: Durable run identifier.
            summary: Cached summary for the run.
        """
        self._runner = runner
        self.run_id = run_id
        self._summary = summary

    @property
    def status(self) -> RunLifecycleStatus:
        """Return the cached lifecycle status for the run.

        Returns:
            Cached lifecycle status.
        """
        return self._summary.status

    @property
    def control_state(self) -> RunControlState:
        """Return the cached operator control state for the run.

        Returns:
            Cached control state.
        """
        return self._summary.control_state

    @property
    def is_finished(self) -> bool:
        """Return whether the run is in a terminal lifecycle state.

        Returns:
            True if the run is terminal.
        """
        return self.status in (RunLifecycleStatus.COMPLETED, RunLifecycleStatus.COMPLETED_WITH_FAILURES)

    def refresh(self) -> RunSummary:
        """Perform one poll-and-submit cycle and return the updated summary.

        Returns:
            Updated run summary.
        """
        self._summary = self._runner._refresh_run(self.run_id)
        return self._summary

    def wait(
        self,
        *,
        timeout: float | None = None,
        poll_interval: float | None = None,
    ) -> Run:
        """Block until the run is terminal or the optional timeout expires.

        Args:
            timeout: Optional timeout in seconds.
            poll_interval: Optional polling interval override in seconds.

        Returns:
            The same run handle after it becomes terminal.

        Raises:
            RunPausedError: If the run is paused while waiting.
            TimeoutError: If the timeout elapses before the run is terminal.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            self.refresh()
            if self.is_finished:
                return self
            if self.control_state is RunControlState.PAUSED:
                raise RunPausedError(self.run_id)
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"timed out waiting for run {self.run_id}")
            context = self._runner._context_for_run(self.run_id)
            sleep_for = poll_interval if poll_interval is not None else context.config.provider_config.poll_interval_sec
            if self._summary.backoff_remaining_sec > 0:
                if sleep_for > 0:
                    sleep_for = min(sleep_for, self._summary.backoff_remaining_sec)
                else:
                    sleep_for = self._summary.backoff_remaining_sec
            if sleep_for > 0:
                self._runner.sleep(sleep_for)

    def summary(self) -> RunSummary:
        """Read the latest persisted summary for the run from storage.

        Returns:
            Latest persisted run summary.
        """
        self._summary = self._runner.state.get_run_summary(run_id=self.run_id)
        return self._summary

    def snapshot(self) -> RunSnapshot:
        """Return the current summary plus expanded terminal item payloads.

        Returns:
            Snapshot containing the latest summary plus materialized results.
        """
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
            items=self._runner._results_for_run(self.run_id),
        )

    def results(self) -> list[BatchResultItem]:
        """Return terminal item results for the run.

        Returns:
            Terminal item results in durable item order.

        Raises:
            RunNotFinishedError: If the run is not terminal yet.
        """
        if not self.is_finished:
            raise RunNotFinishedError(self.run_id)
        return self._runner._results_for_run(self.run_id)

    def prune_artifacts(
        self,
        *,
        include_raw_output_artifacts: bool = False,
    ) -> ArtifactPruneResult:
        """Prune retained artifacts for this terminal run.

        Args:
            include_raw_output_artifacts: Whether raw provider payloads should
                also be pruned.

        Returns:
            Artifact prune report.
        """
        self._summary = self._runner.state.get_run_summary(run_id=self.run_id)
        return self._runner.prune_artifacts(
            self.run_id,
            include_raw_output_artifacts=include_raw_output_artifacts,
        )

    def export_artifacts(self, destination_dir: str) -> ArtifactExportResult:
        """Export retained artifacts for this terminal run.

        Args:
            destination_dir: Directory that will receive the exported run
                bundle.

        Returns:
            Artifact export report.
        """
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
