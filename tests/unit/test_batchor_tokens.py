from __future__ import annotations

import pytest

from batchor.core.models import ChunkPolicy, OpenAIEnqueueLimitConfig
from batchor.runtime.tokens import (
    chunk_by_request_limits,
    chunk_request_rows,
    effective_inflight_token_budget,
    estimate_request_tokens,
    estimate_text_tokens,
    resolve_openai_batch_token_limit,
    split_rows_by_token_limit,
)


def test_effective_inflight_token_budget() -> None:
    assert (
        effective_inflight_token_budget(
            OpenAIEnqueueLimitConfig(
                enqueued_token_limit=1000,
                target_ratio=0.7,
                headroom=50,
            )
        )
        == 700
    )


def test_resolve_openai_batch_token_limit_uses_smaller_of_batch_and_inflight() -> None:
    assert (
        resolve_openai_batch_token_limit(
            OpenAIEnqueueLimitConfig(
                enqueued_token_limit=1000,
                target_ratio=0.7,
                max_batch_enqueued_tokens=500,
            )
        )
        == 500
    )


def test_split_rows_by_token_limit_separates_oversized_rows() -> None:
    within_limit, oversized = split_rows_by_token_limit(
        [
            {"id": "a", "submission_tokens": 3},
            {"id": "b", "submission_tokens": 7},
            {"id": "c", "submission_tokens": 5},
        ],
        token_limit=5,
        token_field="submission_tokens",
    )
    assert [row["id"] for row in within_limit] == ["a", "c"]
    assert [row["id"] for row in oversized] == ["b"]


def test_chunk_request_rows_honors_token_limit() -> None:
    rows = [
        {"id": "a", "size": 1, "tokens": 3},
        {"id": "b", "size": 1, "tokens": 3},
        {"id": "c", "size": 1, "tokens": 2},
    ]
    chunks = chunk_request_rows(
        rows,
        chunk_policy=ChunkPolicy(max_requests=10, max_file_bytes=20),
        max_tokens=5,
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
            max_tokens=None,
            estimate_row_bytes=lambda row: int(row["size"]),
            estimate_row_tokens=lambda row: int(row["tokens"]),
        )
    with pytest.raises(ValueError, match="estimated row bytes must be > 0"):
        chunk_request_rows(
            [{"id": "a", "size": 0, "tokens": 1}],
            chunk_policy=ChunkPolicy(max_requests=1, max_file_bytes=10),
            max_tokens=None,
            estimate_row_bytes=lambda row: int(row["size"]),
            estimate_row_tokens=lambda row: int(row["tokens"]),
        )
    with pytest.raises(ValueError, match="estimated row tokens must be > 0"):
        chunk_request_rows(
            [{"id": "a", "size": 1, "tokens": 0}],
            chunk_policy=ChunkPolicy(max_requests=1, max_file_bytes=10),
            max_tokens=10,
            estimate_row_bytes=lambda row: int(row["size"]),
            estimate_row_tokens=lambda row: int(row["tokens"]),
        )
    with pytest.raises(ValueError, match="token_limit must be > 0"):
        split_rows_by_token_limit(
            [{"id": "a", "submission_tokens": 1}],
            token_limit=0,
            token_field="submission_tokens",
        )


@pytest.mark.parametrize(
    "kwargs,match",
    [
        ({"max_requests": 0, "max_bytes": 10}, "max_requests must be > 0"),
        ({"max_requests": 1, "max_bytes": 0}, "max_bytes must be > 0"),
        ({"max_requests": 1, "max_bytes": 10, "max_tokens": 0}, "max_tokens must be > 0"),
    ],
)
def test_chunk_by_request_limits_rejects_invalid_limits(kwargs: dict, match: str) -> None:
    """chunk_by_request_limits itself (not via ChunkPolicy) rejects bad limits."""
    base: dict = {"max_requests": 1, "max_bytes": 10}
    base.update(kwargs)
    with pytest.raises(ValueError, match=match):
        chunk_by_request_limits(
            [{"size": 1, "tokens": 1}],
            estimate_row_bytes=lambda r: int(r["size"]),
            estimate_row_tokens=lambda r: int(r["tokens"]),
            **dict(base),
        )


def test_chunk_by_request_limits_rejects_missing_token_estimator() -> None:
    """max_tokens without estimate_row_tokens must raise."""
    with pytest.raises(ValueError, match="estimate_row_tokens is required"):
        chunk_by_request_limits(
            [{"size": 1}],
            max_requests=1,
            max_bytes=10,
            max_tokens=5,
            estimate_row_bytes=lambda r: int(r["size"]),
            estimate_row_tokens=None,
        )


def test_chunk_by_request_limits_creates_token_singleton_chunk() -> None:
    """A single row exceeding max_tokens is placed in its own singleton chunk."""
    rows = [{"size": 1, "tokens": 10}, {"size": 1, "tokens": 2}]
    chunks = chunk_by_request_limits(
        rows,
        max_requests=10,
        max_bytes=100,
        max_tokens=5,
        estimate_row_bytes=lambda r: int(r["size"]),
        estimate_row_tokens=lambda r: int(r["tokens"]),
    )
    # First row (tokens=10 > max_tokens=5) gets its own singleton chunk
    assert len(chunks) == 2
    assert len(chunks[0]) == 1 and chunks[0][0]["tokens"] == 10
    assert len(chunks[1]) == 1 and chunks[1][0]["tokens"] == 2


def test_chunk_by_request_limits_flushes_on_max_requests_exactly() -> None:
    """When len(current) hits max_requests exactly inside the loop, flush immediately."""
    rows = [{"size": 1, "tokens": 1} for _ in range(4)]
    for i, r in enumerate(rows):
        r["id"] = str(i)
    chunks = chunk_by_request_limits(
        rows,
        max_requests=2,
        max_bytes=1000,
        estimate_row_bytes=lambda r: int(r["size"]),
        estimate_row_tokens=lambda r: int(r["tokens"]),
    )
    # 4 rows, max_requests=2 → 2 chunks of 2
    assert len(chunks) == 2
    assert len(chunks[0]) == 2
    assert len(chunks[1]) == 2


def test_estimate_text_tokens_uses_tiktoken_when_available() -> None:
    """When use_tiktoken=True, tiktoken is used and returns a non-negative count."""
    count = estimate_text_tokens("hello world", use_tiktoken=True)
    assert count > 0


def test_estimate_text_tokens_tiktoken_falls_back_on_unknown_model() -> None:
    """An unrecognized model name falls back to cl100k_base without error."""
    count = estimate_text_tokens("hello", model="gpt-unknown-xxxx", use_tiktoken=True)
    assert count > 0


def test_estimate_text_tokens_tiktoken_returns_zero_for_empty_string() -> None:
    assert estimate_text_tokens("", use_tiktoken=True) == 0


def test_chunk_request_rows_keeps_progress_for_pathological_single_row() -> None:
    chunks = chunk_request_rows(
        [{"id": "a", "size": 11, "tokens": 99}, {"id": "b", "size": 1, "tokens": 1}],
        chunk_policy=ChunkPolicy(max_requests=5, max_file_bytes=10),
        max_tokens=10,
        estimate_row_bytes=lambda row: int(row["size"]),
        estimate_row_tokens=lambda row: int(row["tokens"]),
    )
    assert [[row["id"] for row in chunk] for chunk in chunks] == [["a"], ["b"]]
