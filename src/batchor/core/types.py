"""Primitive JSON type aliases and typed dictionaries for provider wire formats.

These types serve two purposes:

* **JSON aliases** — ``JSONScalar``, ``JSONValue``, and ``JSONObject`` are
  convenience type aliases used throughout the codebase wherever arbitrary
  JSON-compatible data is accepted or produced.
* **Typed dicts** — ``BatchRequestCounts``, ``BatchRemoteRecord``, and
  ``BatchRequestLine`` model the exact shapes of provider API payloads so that
  callers get IDE completion and ``ty`` type coverage without pulling in the
  full provider SDK types.
"""

from __future__ import annotations

from typing import TypedDict

type JSONScalar = None | bool | int | float | str
"""A primitive JSON scalar value."""

type JSONValue = JSONScalar | list[JSONValue] | dict[str, JSONValue]
"""Any JSON-compatible value, including nested lists and objects."""

type JSONObject = dict[str, JSONValue]
"""A JSON object represented as a Python dictionary with string keys."""


class BatchRequestCounts(TypedDict, total=False):
    """Request-count summary embedded in a provider batch record.

    Attributes:
        completed: Number of requests that completed successfully.
        failed: Number of requests that failed.
    """

    completed: int
    failed: int


class BatchRemoteRecord(TypedDict, total=False):
    """Normalised shape of a provider batch status response.

    Attributes:
        id: Provider-assigned batch identifier.
        status: Current batch status string (e.g. ``"completed"``).
        output_file_id: Provider file ID for the success output JSONL, or
            ``None`` if not yet available.
        error_file_id: Provider file ID for the error output JSONL, or
            ``None`` if not yet available.
        request_counts: Per-status request count breakdown.
        errors: Raw provider error payload, if any.
    """

    id: str
    status: str
    output_file_id: str | None
    error_file_id: str | None
    request_counts: BatchRequestCounts
    errors: JSONValue


class BatchRequestLine(TypedDict, total=False):
    """One JSONL line in a provider batch input file.

    Attributes:
        custom_id: OpenAI caller-assigned correlation identifier.
        key: Gemini caller-assigned correlation identifier.
        method: HTTP method for providers that batch HTTP-style requests.
        url: Endpoint path for providers that batch HTTP-style requests.
        body: Request body for providers that batch HTTP-style requests.
        request: Gemini ``GenerateContentRequest`` payload.
    """

    custom_id: str
    key: str
    method: str
    url: str
    body: JSONObject
    request: JSONObject
