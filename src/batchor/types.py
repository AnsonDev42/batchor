from __future__ import annotations

from typing import TypedDict


type JSONScalar = None | bool | int | float | str
type JSONValue = JSONScalar | list[JSONValue] | dict[str, JSONValue]
type JSONObject = dict[str, JSONValue]


class BatchRequestCounts(TypedDict, total=False):
    completed: int
    failed: int


class BatchRemoteRecord(TypedDict, total=False):
    id: str
    status: str
    output_file_id: str | None
    error_file_id: str | None
    request_counts: BatchRequestCounts
    errors: JSONValue


class BatchRequestLine(TypedDict):
    custom_id: str
    method: str
    url: str
    body: JSONObject
