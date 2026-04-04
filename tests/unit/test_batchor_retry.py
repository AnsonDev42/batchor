from __future__ import annotations

import pytest

from batchor.runtime.retry import (
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


@pytest.mark.parametrize(
    "consecutive_failures,base_delay_sec,max_delay_sec",
    [
        (0, 1.0, 10.0),  # zero failures
        (-1, 1.0, 10.0),  # negative failures
        (1, 0.0, 10.0),  # zero base delay
        (1, 1.0, 0.0),  # zero max delay
        (1, -1.0, 10.0),  # negative base delay
    ],
)
def test_compute_backoff_delay_returns_zero_for_degenerate_inputs(
    consecutive_failures: int,
    base_delay_sec: float,
    max_delay_sec: float,
) -> None:
    assert (
        compute_backoff_delay(
            consecutive_failures=consecutive_failures,
            base_delay_sec=base_delay_sec,
            max_delay_sec=max_delay_sec,
        )
        == 0.0
    )


@pytest.mark.parametrize(
    "message,expected",
    [
        ("enqueue token limit exceeded", True),  # branch: "enqueue token limit"
        ("enqueued prompt token limit reached", True),  # branch: "enqueued prompt token limit"
        ("enqueued token exceeded limit", True),  # branch: enqueued+token+limit
        ("enqueue exceeded token limit now", True),  # branch: enqueue+token+limit
        ("quota exceeded", False),
    ],
)
def test_is_enqueue_token_limit_error_covers_all_branches(message: str, expected: bool) -> None:
    assert is_enqueue_token_limit_error(RuntimeError(message)) is expected


def test_is_retryable_batch_control_plane_error_detects_enqueue_limit() -> None:
    """enqueue token limit errors must also be retryable (via is_enqueue_token_limit_error)."""
    assert is_retryable_batch_control_plane_error(RuntimeError("enqueued token limit exceeded")) is True


class _FakeRateLimitError(Exception):
    """Simulates openai.RateLimitError class name match."""


_FakeRateLimitError.__name__ = "RateLimitError"


def test_is_retryable_batch_control_plane_error_detects_by_class_name() -> None:
    """Errors whose class name contains 'ratelimiterror' are retryable."""
    err = _FakeRateLimitError("rate limited")
    assert is_retryable_batch_control_plane_error(err) is True


def test_iter_error_messages_walks_exception_body_and_response() -> None:
    """_iter_error_messages must walk .body and .response attributes on exceptions."""
    from batchor.runtime.retry import _iter_error_messages

    class _ErrorWithBody(Exception):
        body = {"message": "body-text"}
        response = {"detail": "response-text"}

    messages = list(_iter_error_messages(_ErrorWithBody("exc-str")))
    assert "exc-str" in messages
    assert any("body-text" in m for m in messages)
    assert any("response-text" in m for m in messages)


def test_iter_error_messages_handles_none_input() -> None:
    from batchor.runtime.retry import _iter_error_messages

    assert list(_iter_error_messages(None)) == []


def test_iter_error_messages_handles_list_and_tuple_inputs() -> None:
    from batchor.runtime.retry import _iter_error_messages

    assert list(_iter_error_messages(["enqueued token limit", "other"])) == ["enqueued token limit", "other"]
    assert list(_iter_error_messages(("rate limit",))) == ["rate limit"]


def test_iter_error_messages_handles_non_standard_objects() -> None:
    """Non-string, non-dict, non-list, non-exception objects fall back to str()."""
    from batchor.runtime.retry import _iter_error_messages

    # An integer falls back to str(42) = "42"
    messages = list(_iter_error_messages(42))
    assert messages == ["42"]
