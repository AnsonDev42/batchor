"""Error classification and exponential backoff helpers for the batch runtime.

These utilities are used by the runner execution layer to decide whether a
provider error warrants a retry and to compute the next backoff delay.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any


def classify_batch_error(error: object) -> str:
    """Return a short machine-readable class name for a batch control-plane error.

    Args:
        error: Any exception or error value from the provider SDK.

    Returns:
        One of:

        * ``"enqueue_token_limit"`` — the account's enqueued-token budget was
          exceeded.
        * ``"openai_insufficient_quota"`` — the OpenAI account has exhausted
          quota, billing credits, or a configured spend limit.
        * ``"control_plane_transient"`` — a retryable transient error such as
          a rate limit or timeout.
        * ``"batch_error"`` — a non-retryable or unrecognised error.
    """
    if is_insufficient_quota_error(error):
        return "openai_insufficient_quota"
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
    """Compute the exponential backoff delay for a given failure count.

    Uses binary exponential backoff: ``min(base * 2^(n-1), max)``.

    Args:
        consecutive_failures: Number of consecutive failures so far.  ``0``
            or negative returns ``0.0`` immediately.
        base_delay_sec: Starting delay in seconds for the first failure.
            ``0`` or negative returns ``0.0``.
        max_delay_sec: Upper bound on the returned delay.  ``0`` or negative
            returns ``0.0``.

    Returns:
        Backoff delay in seconds, clamped to ``[0, max_delay_sec]``.
    """
    if consecutive_failures <= 0 or base_delay_sec <= 0 or max_delay_sec <= 0:
        return 0.0
    exponent = min(consecutive_failures - 1, 10)
    return min(base_delay_sec * (2**exponent), max_delay_sec)


def is_enqueue_token_limit_error(error: Any) -> bool:
    """Return ``True`` if *error* indicates an OpenAI enqueued-token-limit refusal.

    Inspects the error message tree for phrases like ``"enqueued token limit"``
    or ``"enqueued prompt token limit"``.

    Args:
        error: Any value — exception, dict, string, or ``None``.

    Returns:
        ``True`` when the error looks like an enqueued-token-limit violation.
    """
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
    """Return ``True`` if *error* is a transient error that warrants a retry.

    Considers rate-limit errors, API timeouts, connection errors, internal
    server errors, and enqueued-token-limit errors as retryable.

    Args:
        error: Any value — exception, dict, string, or ``None``.

    Returns:
        ``True`` when the error is transient and the operation should be
        retried after a backoff delay.
    """
    if is_enqueue_token_limit_error(error):
        return True
    if is_insufficient_quota_error(error):
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


def is_insufficient_quota_error(error: Any) -> bool:
    """Return ``True`` if *error* indicates exhausted OpenAI quota or billing.

    OpenAI reports both ordinary rate limiting and quota exhaustion as 429s.
    Ordinary rate limits remain retryable with backoff, but insufficient quota
    should pause the run because retrying immediately cannot make progress.
    """
    has_429 = _has_status_code(error, 429)
    for message in _iter_error_messages(error):
        normalized = " ".join(message.lower().replace("-", "_").split())
        if "insufficient_quota" in normalized:
            return True
        if has_429 and (
            "exceeded your current quota" in normalized
            or "current quota" in normalized
            or "billing" in normalized
            or "credits" in normalized
            or "buy more credits" in normalized
            or "monthly spend" in normalized
            or "spend limit" in normalized
            or ("quota" in normalized and "plan" in normalized)
        ):
            return True
    return False


def _has_status_code(error: Any, status_code: int) -> bool:
    if error is None:
        return False
    if isinstance(error, BaseException):
        if getattr(error, "status_code", None) == status_code:
            return True
        for arg in getattr(error, "args", ()):
            if _has_status_code(arg, status_code):
                return True
        body = getattr(error, "body", None)
        if _has_status_code(body, status_code):
            return True
        response = getattr(error, "response", None)
        return _has_status_code(response, status_code)
    if isinstance(error, dict):
        for key in ("status_code", "status", "code"):
            value = error.get(key)
            if isinstance(value, int) and value == status_code:
                return True
            if isinstance(value, str) and value.isdigit() and int(value) == status_code:
                return True
        return any(_has_status_code(value, status_code) for value in error.values())
    if isinstance(error, (list, tuple, set)):
        return any(_has_status_code(item, status_code) for item in error)
    return getattr(error, "status_code", None) == status_code


def _iter_error_messages(error: Any) -> Iterable[str]:
    """Recursively yield text fragments from an arbitrary error value.

    Walks exception args, ``body``, and ``response`` attributes as well as
    nested dicts, lists, and tuples.

    Args:
        error: Any error value to inspect.

    Yields:
        Non-empty string fragments extracted from *error*.
    """
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
        for attr in ("code", "type", "param"):
            value = getattr(error, attr, None)
            if value is not None:
                yield from _iter_error_messages(value)
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
