from __future__ import annotations

import os
from pathlib import Path
import uuid

from dotenv import find_dotenv, load_dotenv
import pytest

from batchor import (
    BatchItem,
    BatchJob,
    BatchRunner,
    OpenAIModel,
    OpenAIProviderConfig,
    PromptParts,
    SQLiteStorage,
)


load_dotenv(find_dotenv(usecwd=True), override=False)


pytestmark = [
    pytest.mark.live,
    pytest.mark.integration,
]


def _require_live_openai() -> tuple[str, str, str | None]:
    enabled = os.getenv("BATCHOR_RUN_LIVE_TESTS", "").lower()
    if enabled not in {"1", "true", "yes"}:
        pytest.skip("set BATCHOR_RUN_LIVE_TESTS=1 to run live OpenAI smoke tests")
    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        pytest.skip("OPENAI_API_KEY is required for live OpenAI smoke tests")
    model = os.getenv("BATCHOR_LIVE_OPENAI_MODEL", OpenAIModel.GPT_5_NANO.value)
    reasoning_effort = os.getenv("BATCHOR_LIVE_OPENAI_REASONING_EFFORT") or None
    return api_key, model, reasoning_effort


def test_live_openai_text_job_smoke(tmp_path: Path) -> None:
    api_key, model, reasoning_effort = _require_live_openai()
    storage = SQLiteStorage(path=tmp_path / "live.sqlite3")
    runner = BatchRunner(
        storage=storage,
        temp_root=tmp_path / "artifacts",
    )
    run = runner.start(
        BatchJob(
            items=[
                BatchItem(
                    item_id="smoke-row-1",
                    payload="Reply with exactly: batchor-live-smoke-ok",
                )
            ],
            build_prompt=lambda item: PromptParts(prompt=item.payload),
            provider_config=OpenAIProviderConfig(
                api_key=api_key,
                model=model,
                poll_interval_sec=2.0,
                reasoning_effort=reasoning_effort,
            ),
        ),
        run_id=f"live_smoke_{uuid.uuid4().hex[:10]}",
    )

    run.wait(timeout=900, poll_interval=2.0)
    results = run.results()

    assert len(results) == 1
    assert results[0].error is None
    assert results[0].output_text is not None
    assert "batchor-live-smoke-ok" in results[0].output_text.lower()

    inventory = storage.get_artifact_inventory(run_id=run.run_id)
    assert len(inventory.request_artifact_paths) == 1
    assert len(inventory.output_artifact_paths) <= 1
