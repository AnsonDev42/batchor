"""Opt-in live smoke coverage for all Gemini batch transports."""

from __future__ import annotations

import os
from contextlib import suppress
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from dotenv import find_dotenv, load_dotenv

from batchor import (
    BatchItem,
    BatchJob,
    BatchRunner,
    GeminiBatchInputMode,
    GeminiProviderConfig,
    PromptParts,
    SQLiteStorage,
)
from batchor.providers.gemini import GeminiBatchProvider

load_dotenv(find_dotenv(usecwd=True), override=False)

pytestmark = [pytest.mark.integration, pytest.mark.live]


class _RecordingGeminiProvider(GeminiBatchProvider):
    """Remember Developer API files so live smoke can delete them."""

    def __init__(self, config: GeminiProviderConfig, client: Any | None = None) -> None:
        super().__init__(config, client=client)
        self.remote_file_ids: list[str] = []

    def upload_input_file(self, input_path: str | Path) -> str:
        file_id = super().upload_input_file(input_path)
        if file_id.startswith("files/"):
            self.remote_file_ids.append(file_id)
        return file_id

    def download_file_content(self, file_id: str) -> str:
        if file_id.startswith("files/"):
            self.remote_file_ids.append(file_id)
        return super().download_file_content(file_id)


def _developer_config(mode: GeminiBatchInputMode) -> GeminiProviderConfig:
    api_key = os.getenv("GEMINI_API_KEY", "")
    if not api_key:
        pytest.skip("GEMINI_API_KEY is required for live Developer API smoke coverage")
    return GeminiProviderConfig(
        model=os.getenv("BATCHOR_LIVE_GEMINI_DEVELOPER_MODEL", "gemini-2.5-flash"),
        api_key=api_key,
        vertexai=False,
        input_mode=mode,
        poll_interval_sec=10,
        generation_config={"temperature": 0, "max_output_tokens": 128},
    )


def _run_developer_smoke(tmp_path: Path, mode: GeminiBatchInputMode) -> None:
    from google import genai
    from google.genai import types

    config = _developer_config(mode)
    client = genai.Client(
        vertexai=False,
        api_key=config.api_key,
        http_options=types.HttpOptions(
            timeout=30_000,
            retry_options=types.HttpRetryOptions(attempts=1),
        ),
    )
    provider = _RecordingGeminiProvider(config, client=client)
    runner = BatchRunner(
        storage=SQLiteStorage(path=tmp_path / f"gemini-developer-{mode}.sqlite3"),
        provider_factory=lambda _config: provider,
    )
    try:
        run = runner.start(
            BatchJob(
                items=[BatchItem(item_id=f"developer-{mode}", payload="Reply with exactly: batchor-live-ok")],
                build_prompt=lambda item: PromptParts(prompt=item.payload),
                provider_config=config,
            )
        )
        run.wait(timeout=float(os.getenv("BATCHOR_LIVE_GEMINI_TIMEOUT_SEC", "1800")))
        result = run.results()[0]
        assert result.error is None
        assert result.output_text is not None
        assert "batchor-live-ok" in result.output_text.lower()
    finally:
        for file_id in set(provider.remote_file_ids):
            with suppress(Exception):
                provider.client.files.delete(name=file_id)


@pytest.mark.skipif(
    os.getenv("BATCHOR_RUN_LIVE_GEMINI") != "1",
    reason="set BATCHOR_RUN_LIVE_GEMINI=1 to run Gemini smoke coverage",
)
def test_developer_inline_text_job_smoke(tmp_path: Path) -> None:
    """Complete one real Developer API batch using inline requests."""
    _run_developer_smoke(tmp_path, GeminiBatchInputMode.INLINE)


@pytest.mark.skipif(
    os.getenv("BATCHOR_RUN_LIVE_GEMINI") != "1",
    reason="set BATCHOR_RUN_LIVE_GEMINI=1 to run Gemini smoke coverage",
)
def test_developer_file_text_job_smoke(tmp_path: Path) -> None:
    """Complete one real Developer API batch through the Files API."""
    _run_developer_smoke(tmp_path, GeminiBatchInputMode.FILE)


@pytest.mark.skipif(
    os.getenv("BATCHOR_RUN_LIVE_GEMINI") != "1",
    reason="set BATCHOR_RUN_LIVE_GEMINI=1 to run Vertex AI smoke coverage",
)
def test_vertex_text_job_smoke(tmp_path: Path) -> None:
    """Complete one real Vertex AI batch through Cloud Storage."""
    gcs_base = os.environ["BATCHOR_LIVE_GEMINI_GCS_URI"].rstrip("/")
    smoke_prefix = f"{gcs_base}/{uuid4().hex}"
    config = GeminiProviderConfig(
        model=os.getenv("BATCHOR_LIVE_GEMINI_MODEL", "gemini-2.5-flash"),
        vertexai=True,
        project=os.environ["GOOGLE_CLOUD_PROJECT"],
        location=os.environ["GOOGLE_CLOUD_LOCATION"],
        gcs_uri=smoke_prefix,
        poll_interval_sec=10,
        generation_config={"temperature": 0, "max_output_tokens": 128},
    )
    provider = GeminiBatchProvider(config)
    runner = BatchRunner(
        storage=SQLiteStorage(path=tmp_path / "gemini-live.sqlite3"),
        provider_factory=lambda _config: provider,
    )

    try:
        run = runner.start(
            BatchJob(
                items=[BatchItem(item_id="live-row", payload="Reply with exactly: batchor-live-ok")],
                build_prompt=lambda item: PromptParts(prompt=item.payload),
                provider_config=config,
            )
        )
        run.wait(timeout=float(os.getenv("BATCHOR_LIVE_GEMINI_TIMEOUT_SEC", "1800")))
        result = run.results()[0]
        assert result.output_text is not None
        assert "batchor-live-ok" in result.output_text.lower()
    finally:
        bucket_name, prefix = smoke_prefix.removeprefix("gs://").split("/", maxsplit=1)
        bucket = provider.storage_client.bucket(bucket_name)
        for blob in bucket.list_blobs(prefix=prefix):
            blob.delete()
