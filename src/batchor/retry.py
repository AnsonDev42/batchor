from __future__ import annotations

from collections.abc import Iterable
from typing import Any


def classify_batch_error(error: object) -> str:
    if is_enqueue_token_limit_error(error):
        return "enqueue_token_limit"
    if is_retryable_batch_control_plane_error(error):
        return "control_plane_transient"
    return "batch_error"


def compute_backoff_delay(
    *,
    consecutive_failures: int,
    base_delay_sec: float,
    max_delay_sec: float,
) -> float:
    if consecutive_failures <= 0 or base_delay_sec <= 0 or max_delay_sec <= 0:
        return 0.0
    exponent = min(consecutive_failures - 1, 10)
    return min(base_delay_sec * (2**exponent), max_delay_sec)


def is_enqueue_token_limit_error(error: Any) -> bool:
    for message in _iter_error_messages(error):
        normalized = " ".join(message.lower().split())
        if "enqueued token limit" in normalized:
            return True
        if "enqueue token limit" in normalized:
            return True
        if "enqueued prompt token limit" in normalized:
            return True
        if "enqueued" in normalized and "token" in normalized and "limit" in normalized:
            return True
        if "enqueue" in normalized and "token" in normalized and "limit" in normalized:
            return True
    return False


def is_retryable_batch_control_plane_error(error: Any) -> bool:
    if is_enqueue_token_limit_error(error):
        return True
    class_name = type(error).__name__.lower()
    if any(
        name in class_name
        for name in (
            "ratelimiterror",
            "apitimeouterror",
            "apiconnectionerror",
            "internalservererror",
        )
    ):
        return True
    for message in _iter_error_messages(error):
        normalized = " ".join(message.lower().split())
        if any(
            token in normalized
            for token in (
                "rate limit",
                "too many requests",
                "timed out",
                "timeout",
                "temporarily unavailable",
                "service unavailable",
                "internal server error",
                "connection error",
                "connection reset",
            )
        ):
            return True
    return False


def _iter_error_messages(error: Any) -> Iterable[str]:
    if error is None:
        return
    if isinstance(error, BaseException):
        text = str(error)
        if text:
            yield text
        for arg in getattr(error, "args", ()):
            yield from _iter_error_messages(arg)
        body = getattr(error, "body", None)
        if body is not None:
            yield from _iter_error_messages(body)
        response = getattr(error, "response", None)
        if response is not None:
            yield from _iter_error_messages(response)
        return
    if isinstance(error, str):
        if error:
            yield error
        return
    if isinstance(error, dict):
        for key, value in error.items():
            yield from _iter_error_messages(key)
            yield from _iter_error_messages(value)
        return
    if isinstance(error, (list, tuple, set)):
        for item in error:
            yield from _iter_error_messages(item)
        return
    text = str(error)
    if text:
        yield text
