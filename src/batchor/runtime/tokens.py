from __future__ import annotations

from functools import lru_cache
import json
from math import ceil
from typing import Any, Callable

from batchor.core.models import ChunkPolicy, InflightPolicy
from batchor.core.types import BatchRequestLine


def estimate_text_tokens(
    text: str,
    *,
    chars_per_token: int = 4,
    model: str | None = None,
    use_tiktoken: bool = True,
) -> int:
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
    inflight_policy: InflightPolicy,
    estimate_row_bytes: Callable[[dict[str, Any]], int],
    estimate_row_tokens: Callable[[dict[str, Any]], int],
) -> list[list[dict[str, Any]]]:
    return chunk_by_request_limits(
        rows,
        max_requests=chunk_policy.max_requests,
        max_bytes=chunk_policy.max_file_bytes,
        estimate_row_bytes=estimate_row_bytes,
        max_tokens=resolve_batch_token_limit(
            chunk_policy=chunk_policy,
            inflight_policy=inflight_policy,
        ),
        estimate_row_tokens=estimate_row_tokens,
    )


def effective_inflight_token_budget(policy: InflightPolicy) -> int | None:
    if policy.enqueued_token_limit <= 0:
        return None
    by_ratio = int(policy.enqueued_token_limit * policy.target_ratio)
    by_headroom = policy.enqueued_token_limit - policy.headroom
    return min(by_ratio, by_headroom)


def resolve_batch_token_limit(
    *,
    chunk_policy: ChunkPolicy,
    inflight_policy: InflightPolicy,
) -> int | None:
    batch_limit = chunk_policy.max_enqueued_tokens if chunk_policy.max_enqueued_tokens > 0 else None
    inflight_limit = effective_inflight_token_budget(inflight_policy)
    if batch_limit is not None and inflight_limit is not None:
        return min(batch_limit, inflight_limit)
    return batch_limit if batch_limit is not None else inflight_limit


@lru_cache(maxsize=32)
def _encoding_for_model(model: str | None):
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
