from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import time
from typing import TYPE_CHECKING
from uuid import uuid4

from pydantic import BaseModel

from batchor.core.enums import RunLifecycleStatus
from batchor.core.exceptions import RunNotFinishedError
from batchor.core.models import ArtifactPruneResult, BatchResultItem, RunSnapshot, RunSummary
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

    def prune_artifacts(self) -> ArtifactPruneResult:
        self._summary = self._runner.state.get_run_summary(run_id=self.run_id)
        return self._runner.prune_artifacts(self.run_id)
