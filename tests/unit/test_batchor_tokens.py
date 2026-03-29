from __future__ import annotations

import pytest

from batchor.core.models import ChunkPolicy, InflightPolicy
from batchor.runtime.tokens import (
    chunk_request_rows,
    effective_inflight_token_budget,
    estimate_request_tokens,
    estimate_text_tokens,
    resolve_batch_token_limit,
)


def test_effective_inflight_token_budget() -> None:
    assert effective_inflight_token_budget(
        InflightPolicy(enqueued_token_limit=1000, target_ratio=0.7, headroom=50)
    ) == 700


def test_resolve_batch_token_limit_uses_smaller_of_batch_and_inflight() -> None:
    assert resolve_batch_token_limit(
        chunk_policy=ChunkPolicy(max_enqueued_tokens=500),
        inflight_policy=InflightPolicy(enqueued_token_limit=1000, target_ratio=0.7),
    ) == 500


def test_chunk_request_rows_honors_token_limit() -> None:
    rows = [
        {"id": "a", "size": 1, "tokens": 3},
        {"id": "b", "size": 1, "tokens": 3},
        {"id": "c", "size": 1, "tokens": 2},
    ]
    chunks = chunk_request_rows(
        rows,
        chunk_policy=ChunkPolicy(max_requests=10, max_file_bytes=20, max_enqueued_tokens=5),
        inflight_policy=InflightPolicy(),
        estimate_row_bytes=lambda row: int(row["size"]),
        estimate_row_tokens=lambda row: int(row["tokens"]),
    )
    assert [[row["id"] for row in chunk] for chunk in chunks] == [["a"], ["b", "c"]]


def test_estimate_text_tokens_validates_chars_per_token() -> None:
    assert estimate_text_tokens("", use_tiktoken=False) == 0
    with pytest.raises(ValueError, match="chars_per_token must be > 0"):
        estimate_text_tokens("abc", chars_per_token=0, use_tiktoken=False)


def test_estimate_request_tokens_uses_serialized_request() -> None:
    assert (
        estimate_request_tokens(
            {"custom_id": "c1", "method": "POST", "url": "/v1/responses", "body": {"input": "abcd"}},
            chars_per_token=4,
            use_tiktoken=False,
        )
        > 0
    )


def test_chunk_request_rows_rejects_invalid_limits_and_estimates() -> None:
    with pytest.raises(ValueError, match="max_requests must be > 0"):
        chunk_request_rows(
            [{"id": "a", "size": 1, "tokens": 1}],
            chunk_policy=ChunkPolicy(max_requests=0, max_file_bytes=10),
            inflight_policy=InflightPolicy(),
            estimate_row_bytes=lambda row: int(row["size"]),
            estimate_row_tokens=lambda row: int(row["tokens"]),
        )
    with pytest.raises(ValueError, match="estimated row bytes must be > 0"):
        chunk_request_rows(
            [{"id": "a", "size": 0, "tokens": 1}],
            chunk_policy=ChunkPolicy(max_requests=1, max_file_bytes=10),
            inflight_policy=InflightPolicy(),
            estimate_row_bytes=lambda row: int(row["size"]),
            estimate_row_tokens=lambda row: int(row["tokens"]),
        )
    with pytest.raises(ValueError, match="estimated row tokens must be > 0"):
        chunk_request_rows(
            [{"id": "a", "size": 1, "tokens": 0}],
            chunk_policy=ChunkPolicy(max_requests=1, max_file_bytes=10, max_enqueued_tokens=10),
            inflight_policy=InflightPolicy(),
            estimate_row_bytes=lambda row: int(row["size"]),
            estimate_row_tokens=lambda row: int(row["tokens"]),
        )


def test_chunk_request_rows_keeps_progress_for_pathological_single_row() -> None:
    chunks = chunk_request_rows(
        [{"id": "a", "size": 11, "tokens": 99}, {"id": "b", "size": 1, "tokens": 1}],
        chunk_policy=ChunkPolicy(max_requests=5, max_file_bytes=10, max_enqueued_tokens=10),
        inflight_policy=InflightPolicy(),
        estimate_row_bytes=lambda row: int(row["size"]),
        estimate_row_tokens=lambda row: int(row["tokens"]),
    )
    assert [[row["id"] for row in chunk] for chunk in chunks] == [["a"], ["b"]]
