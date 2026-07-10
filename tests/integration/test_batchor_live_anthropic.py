"""Opt-in live smoke coverage for Anthropic Message Batches."""

from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

import pytest
from dotenv import find_dotenv, load_dotenv

from batchor import AnthropicProviderConfig, BatchItem, BatchJob, BatchRunner, PromptParts, SQLiteStorage

load_dotenv(find_dotenv(usecwd=True), override=False)

pytestmark = [pytest.mark.integration, pytest.mark.live]


@pytest.mark.skipif(
    os.getenv("BATCHOR_RUN_LIVE_ANTHROPIC") != "1",
    reason="set BATCHOR_RUN_LIVE_ANTHROPIC=1 to run Anthropic smoke coverage",
)
def test_live_anthropic_text_job_smoke(tmp_path: Path) -> None:
    """Complete one real Message Batch through the durable runtime."""
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        pytest.skip("ANTHROPIC_API_KEY is required for live Anthropic smoke coverage")

    storage = SQLiteStorage(path=tmp_path / "anthropic-live.sqlite3")
    try:
        runner = BatchRunner(storage=storage, temp_root=tmp_path / "artifacts")
        run = runner.start(
            BatchJob(
                items=[BatchItem(item_id="anthropic-smoke-1", payload="Reply with exactly: batchor-anthropic-live-ok")],
                build_prompt=lambda item: PromptParts(prompt=item.payload),
                provider_config=AnthropicProviderConfig(
                    api_key=api_key,
                    model=os.getenv("BATCHOR_LIVE_ANTHROPIC_MODEL", "claude-haiku-4-5"),
                    max_tokens=64,
                    poll_interval_sec=5.0,
                    message_params={"temperature": 0},
                ),
            ),
            run_id=f"live_anthropic_{uuid4().hex[:10]}",
        )

        run.wait(
            timeout=float(os.getenv("BATCHOR_LIVE_ANTHROPIC_TIMEOUT_SEC", "900")),
            poll_interval=5.0,
        )
        result = run.results()[0]
        normalized = (result.output_text or "").strip().strip("`\"'").rstrip(".!?").lower()

        assert result.error is None
        assert normalized == "batchor-anthropic-live-ok"
        inventory = storage.get_artifact_inventory(run_id=run.run_id)
        assert len(inventory.request_artifact_paths) == 1
        assert len(inventory.output_artifact_paths) == 1
    finally:
        storage.close()
