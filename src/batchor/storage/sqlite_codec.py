from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, cast

from batchor.core.enums import ItemStatus
from batchor.core.models import ItemFailure
from batchor.core.types import JSONObject, JSONValue


def _encode_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _decode_json(value: object) -> Any:
    if value is None:
        return None
    return json.loads(str(value))


def _encode_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat()


def _decode_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(str(value))


def _decode_object(value: object) -> JSONObject:
    decoded = _decode_json(value)
    if not isinstance(decoded, dict):
        raise TypeError("expected JSON object")
    return cast(JSONObject, decoded)


def _decode_optional_object(value: object) -> JSONObject | None:
    if value is None:
        return None
    return _decode_object(value)


def _decode_string_list(value: object) -> list[str]:
    decoded = _decode_json(value)
    if not isinstance(decoded, list):
        raise TypeError("expected JSON list")
    return [str(entry) for entry in decoded]


def _nullable_str(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _nullable_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    return int(str(value))


def _decode_dict(value: object) -> dict[str, object]:
    decoded = _decode_json(value)
    if not isinstance(decoded, dict):
        raise TypeError("expected JSON object")
    return {str(key): cast(object, entry) for key, entry in decoded.items()}


def _require_int(values: dict[str, object], key: str) -> int:
    value = values[key]
    if not isinstance(value, int):
        raise TypeError(f"expected {key} to be an int")
    return value


def _require_float(values: dict[str, object], key: str) -> float:
    value = values[key]
    if isinstance(value, int | float):
        return float(value)
    raise TypeError(f"expected {key} to be numeric")


def _decode_item_failure(value: object) -> ItemFailure | None:
    decoded = _decode_json(value)
    if decoded is None:
        return None
    if not isinstance(decoded, dict):
        raise TypeError("expected JSON object for item failure")
    return ItemFailure(
        error_class=str(decoded["error_class"]),
        message=str(decoded["message"]),
        retryable=bool(decoded["retryable"]),
        raw_error=cast(JSONValue | None, decoded.get("raw_error")),
    )


def _failed_status(
    *,
    attempt_count: int,
    error: ItemFailure,
    max_attempts: int,
    count_attempt: bool,
) -> ItemStatus:
    if not error.retryable:
        return ItemStatus.FAILED_PERMANENT
    if count_attempt and attempt_count >= max_attempts:
        return ItemStatus.FAILED_PERMANENT
    return ItemStatus.FAILED_RETRYABLE
