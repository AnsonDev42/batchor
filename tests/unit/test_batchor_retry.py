from __future__ import annotations

from batchor.retry import (
    classify_batch_error,
    compute_backoff_delay,
    is_enqueue_token_limit_error,
    is_retryable_batch_control_plane_error,
)


def test_is_enqueue_token_limit_error_detects_nested_messages() -> None:
    payload = {"errors": {"data": [{"message": "Enqueued token limit reached for gpt-4.1"}]}}
    assert is_enqueue_token_limit_error(payload) is True


def test_is_retryable_batch_control_plane_error_detects_transient_messages() -> None:
    assert is_retryable_batch_control_plane_error(RuntimeError("temporary service unavailable")) is True


def test_classify_batch_error_prefers_enqueue_limit() -> None:
    assert classify_batch_error(RuntimeError("enqueue token limit reached")) == "enqueue_token_limit"
    assert classify_batch_error(RuntimeError("timeout talking to API")) == "control_plane_transient"
    assert classify_batch_error(RuntimeError("boom")) == "batch_error"


def test_compute_backoff_delay_uses_exponential_growth() -> None:
    assert compute_backoff_delay(consecutive_failures=1, base_delay_sec=1, max_delay_sec=10) == 1
    assert compute_backoff_delay(consecutive_failures=2, base_delay_sec=1, max_delay_sec=10) == 2
    assert compute_backoff_delay(consecutive_failures=100, base_delay_sec=1, max_delay_sec=10) == 10
