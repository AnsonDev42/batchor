"""Token estimation and request-chunking utilities.

Used by the runner submission layer to:

* Estimate the token count for a request line (via *tiktoken* with a
  character-ratio fallback).
* Split a batch of prepared request rows into chunks that respect the
  provider's per-file limits (max requests, max bytes, max tokens).
* Compute effective inflight token budgets from
  :class:`~batchor.OpenAIEnqueueLimitConfig`.
"""

from __future__ import annotations

from functools import lru_cache
import json
from math import ceil
from typing import Any, Callable

from batchor.core.models import ChunkPolicy, OpenAIEnqueueLimitConfig
from batchor.core.types import BatchRequestLine


def estimate_text_tokens(
    text: str,
    *,
    chars_per_token: int = 4,
    model: str | None = None,
    use_tiktoken: bool = True,
) -> int:
    """Estimate the number of tokens in *text*.

    Uses *tiktoken* when available and ``use_tiktoken`` is ``True``; falls
    back to a simple ``ceil(len(text) / chars_per_token)`` heuristic.

    Args:
        text: The text to estimate.  Empty string returns ``0``.
        chars_per_token: Characters-per-token ratio for the fallback estimator.
            Must be ``> 0``.
        model: Optional model name passed to tiktoken's
            ``encoding_for_model``.  When ``None``, ``cl100k_base`` is used.
        use_tiktoken: Set to ``False`` to skip tiktoken and always use the
            character-ratio fallback.

    Returns:
        Estimated token count as a non-negative integer.

    Raises:
        ValueError: If ``chars_per_token`` is ``<= 0``.
    """
    if chars_per_token <= 0:
        raise ValueError("chars_per_token must be > 0")
    if not text:
        return 0
    if use_tiktoken:
        try:
            encoding = _encoding_for_model(model)
            return len(encoding.encode(text))
        except Exception:  # noqa: BLE001
            pass
    return ceil(len(text) / chars_per_token)


def estimate_request_tokens(
    request_line: BatchRequestLine,
    *,
    chars_per_token: int = 4,
    model: str | None = None,
    use_tiktoken: bool = True,
) -> int:
    """Estimate the token count for a serialised batch request line.

    Serialises *request_line* to compact JSON and delegates to
    :func:`estimate_text_tokens`.

    Args:
        request_line: The request line to estimate.
        chars_per_token: Fallback characters-per-token ratio.
        model: Optional model name for tiktoken lookup.
        use_tiktoken: When ``False``, bypasses tiktoken.

    Returns:
        Estimated token count.
    """
    serialized = json.dumps(
        request_line,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return estimate_text_tokens(
        serialized,
        chars_per_token=chars_per_token,
        model=model,
        use_tiktoken=use_tiktoken,
    )


def chunk_request_rows(
    rows: list[dict[str, Any]],
    *,
    chunk_policy: ChunkPolicy,
    max_tokens: int | None,
    estimate_row_bytes: Callable[[dict[str, Any]], int],
    estimate_row_tokens: Callable[[dict[str, Any]], int],
) -> list[list[dict[str, Any]]]:
    """Split *rows* into chunks that respect the :class:`~batchor.ChunkPolicy`.

    Delegates to :func:`chunk_by_request_limits` using the policy's
    ``max_requests`` and ``max_file_bytes``.

    Args:
        rows: Prepared request row dicts to partition.
        chunk_policy: Policy supplying ``max_requests`` and ``max_file_bytes``.
        max_tokens: Optional per-chunk token ceiling.  ``None`` disables token
            splitting.
        estimate_row_bytes: Callable returning the byte size for a row.
        estimate_row_tokens: Callable returning the token estimate for a row.

    Returns:
        A list of chunks; each chunk is a non-empty list of rows.
    """
    return chunk_by_request_limits(
        rows,
        max_requests=chunk_policy.max_requests,
        max_bytes=chunk_policy.max_file_bytes,
        estimate_row_bytes=estimate_row_bytes,
        max_tokens=max_tokens,
        estimate_row_tokens=estimate_row_tokens,
    )


def effective_inflight_token_budget(
    limits: OpenAIEnqueueLimitConfig,
) -> int | None:
    """Compute the effective inflight token budget from enqueue limit config.

    The budget is the smaller of the ratio-based and headroom-based values:
    ``min(limit * target_ratio, limit - headroom)``.

    Args:
        limits: Enqueue limit configuration from
            :class:`~batchor.OpenAIEnqueueLimitConfig`.

    Returns:
        Effective inflight budget in tokens, or ``None`` when
        ``enqueued_token_limit`` is ``<= 0`` (budget enforcement disabled).
    """
    if limits.enqueued_token_limit <= 0:
        return None
    by_ratio = int(limits.enqueued_token_limit * limits.target_ratio)
    by_headroom = limits.enqueued_token_limit - limits.headroom
    return min(by_ratio, by_headroom)


def resolve_openai_batch_token_limit(
    limits: OpenAIEnqueueLimitConfig,
) -> int | None:
    """Resolve the per-batch token ceiling, accounting for both config knobs.

    Returns the more restrictive of ``max_batch_enqueued_tokens`` and the
    effective inflight budget.

    Args:
        limits: Enqueue limit configuration.

    Returns:
        Per-batch token ceiling, or ``None`` when no limit applies.
    """
    batch_limit = (
        limits.max_batch_enqueued_tokens
        if limits.max_batch_enqueued_tokens > 0
        else None
    )
    inflight_limit = effective_inflight_token_budget(limits)
    if batch_limit is not None and inflight_limit is not None:
        return min(batch_limit, inflight_limit)
    return batch_limit if batch_limit is not None else inflight_limit


def split_rows_by_token_limit(
    rows: list[dict[str, Any]],
    *,
    token_limit: int,
    token_field: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Partition *rows* into those within the token limit and those that exceed it.

    Args:
        rows: Request row dicts to partition.
        token_limit: Maximum token count allowed per row.  Must be ``> 0``.
        token_field: Name of the field in each row dict that holds the token
            count (as an integer-like value).

    Returns:
        A 2-tuple ``(within_limit, oversized)`` where *within_limit* contains
        rows whose token count is ``<= token_limit`` and *oversized* contains
        the rest.

    Raises:
        ValueError: If ``token_limit`` is ``<= 0``.
    """
    if token_limit <= 0:
        raise ValueError("token_limit must be > 0")
    within_limit: list[dict[str, Any]] = []
    oversized: list[dict[str, Any]] = []
    for row in rows:
        if int(row[token_field]) > token_limit:
            oversized.append(row)
        else:
            within_limit.append(row)
    return within_limit, oversized


@lru_cache(maxsize=32)
def _encoding_for_model(model: str | None):  # noqa: ANN202
    import tiktoken

    if model:
        try:
            return tiktoken.encoding_for_model(model)
        except KeyError:
            pass
    return tiktoken.get_encoding("cl100k_base")


def chunk_by_request_limits(
    rows: list[dict[str, Any]],
    *,
    max_requests: int,
    max_bytes: int,
    estimate_row_bytes: Callable[[dict[str, Any]], int],
    max_tokens: int | None = None,
    estimate_row_tokens: Callable[[dict[str, Any]], int] | None = None,
) -> list[list[dict[str, Any]]]:
    """Split *rows* into chunks respecting request-count, byte, and token limits.

    Each chunk satisfies:
    - ``len(chunk) <= max_requests``
    - ``sum(row_bytes) <= max_bytes``
    - ``sum(row_tokens) <= max_tokens`` (when *max_tokens* is provided)

    Rows that individually exceed ``max_bytes`` or ``max_tokens`` are placed
    in a singleton chunk rather than being discarded.

    Args:
        rows: Request row dicts to partition.
        max_requests: Maximum number of rows per chunk.  Must be ``> 0``.
        max_bytes: Maximum total byte size per chunk.  Must be ``> 0``.
        estimate_row_bytes: Callable returning the byte size for a row.
        max_tokens: Optional per-chunk token ceiling.  ``None`` disables token
            splitting.
        estimate_row_tokens: Callable returning the token estimate for a row.
            Required when *max_tokens* is provided.

    Returns:
        A list of non-empty row chunks.

    Raises:
        ValueError: If ``max_requests`` or ``max_bytes`` are ``<= 0``, if
            ``max_tokens`` is non-positive when provided, if
            ``estimate_row_tokens`` is missing when ``max_tokens`` is set, or
            if any row's estimated bytes or tokens are ``<= 0``.
    """
    if max_requests <= 0:
        raise ValueError("max_requests must be > 0")
    if max_bytes <= 0:
        raise ValueError("max_bytes must be > 0")
    if max_tokens is not None and max_tokens <= 0:
        raise ValueError("max_tokens must be > 0 when provided")
    if max_tokens is not None and estimate_row_tokens is None:
        raise ValueError("estimate_row_tokens is required when max_tokens is provided")

    chunks: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    current_bytes = 0
    current_tokens = 0

    for row in rows:
        row_bytes = int(estimate_row_bytes(row))
        if row_bytes <= 0:
            raise ValueError("estimated row bytes must be > 0")
        row_tokens = int(estimate_row_tokens(row)) if estimate_row_tokens is not None else None
        if row_tokens is not None and row_tokens <= 0:
            raise ValueError("estimated row tokens must be > 0")

        if current and (
            len(current) >= max_requests
            or current_bytes + row_bytes > max_bytes
            or (
                max_tokens is not None
                and row_tokens is not None
                and current_tokens + row_tokens > max_tokens
            )
        ):
            chunks.append(current)
            current = []
            current_bytes = 0
            current_tokens = 0

        if not current and row_bytes > max_bytes:
            chunks.append([row])
            continue
        if (
            not current
            and max_tokens is not None
            and row_tokens is not None
            and row_tokens > max_tokens
        ):
            chunks.append([row])
            continue

        current.append(row)
        current_bytes += row_bytes
        if row_tokens is not None:
            current_tokens += row_tokens

        if len(current) >= max_requests:
            chunks.append(current)
            current = []
            current_bytes = 0
            current_tokens = 0

    if current:
        chunks.append(current)
    return chunks
