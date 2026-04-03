from __future__ import annotations

import json
import re
from typing import Any, cast

from pydantic import BaseModel, ValidationError

from batchor.core.types import JSONObject, JSONValue


class StructuredOutputError(ValueError):
    def __init__(
        self,
        error_class: str,
        message: str,
        *,
        raw_error: JSONValue | None = None,
    ) -> None:
        super().__init__(message)
        self.error_class = error_class
        self.message = message
        self.raw_error = raw_error


def default_schema_name(model: type[BaseModel]) -> str:
    model_name = model.__name__.lstrip("_")
    name = re.sub(r"(?<!^)(?=[A-Z])", "_", model_name).lower()
    return name.replace("__", "_")


def model_output_schema(
    model: type[BaseModel],
    *,
    schema_name: str | None = None,
) -> tuple[str, JSONObject]:
    resolved_schema_name = schema_name or default_schema_name(model)
    return resolved_schema_name, _strict_json_schema(
        cast(JSONObject, model.model_json_schema())
    )


def _strict_json_schema(schema: JSONObject) -> JSONObject:
    normalized = cast(JSONObject, _normalize_json_schema_value(schema))
    schema_type = normalized.get("type")
    if schema_type == "object" or (
        isinstance(schema_type, list) and "object" in schema_type
    ):
        normalized.setdefault("additionalProperties", False)
    return normalized


def _normalize_json_schema_value(value: JSONValue) -> JSONValue:
    if isinstance(value, list):
        return [_normalize_json_schema_value(item) for item in value]
    if not isinstance(value, dict):
        return value
    normalized = cast(
        JSONObject,
        {key: _normalize_json_schema_value(item) for key, item in value.items()},
    )
    schema_type = normalized.get("type")
    if schema_type == "object" or (
        isinstance(schema_type, list) and "object" in schema_type
    ):
        normalized.setdefault("additionalProperties", False)
    return normalized


def _extract_content_text(content: Any) -> list[str]:
    if isinstance(content, str):
        return [content]
    if not isinstance(content, list):
        return []
    fragments: list[str] = []
    for part in content:
        if isinstance(part, str):
            fragments.append(part)
            continue
        if not isinstance(part, dict):
            continue
        text = part.get("text")
        if isinstance(text, str):
            fragments.append(text)
            continue
        if isinstance(text, dict):
            value = text.get("value")
            if isinstance(value, str):
                fragments.append(value)
    return fragments


def extract_response_text(response_record: dict[str, Any]) -> str:
    body = response_record.get("response", {}).get("body") or response_record.get("body")
    if not isinstance(body, dict):
        return ""

    fragments: list[str] = []
    output = body.get("output")
    if isinstance(output, list):
        for output_item in output:
            if not isinstance(output_item, dict):
                continue
            fragments.extend(_extract_content_text(output_item.get("content")))
            text = output_item.get("text")
            if isinstance(text, str):
                fragments.append(text)
            elif isinstance(text, dict):
                value = text.get("value")
                if isinstance(value, str):
                    fragments.append(value)
    output_text = body.get("output_text")
    if isinstance(output_text, str):
        fragments.append(output_text)

    choices = body.get("choices")
    if isinstance(choices, list):
        for choice in choices:
            if not isinstance(choice, dict):
                continue
            msg = choice.get("message", {})
            if not isinstance(msg, dict):
                continue
            fragments.extend(_extract_content_text(msg.get("content")))

    return "\n".join(fragment for fragment in fragments if fragment)


def strip_json_fence(text: str) -> str:
    stripped = text.strip()
    match = re.match(r"^```(?:json)?\s*\n?(.*?)\n?```\s*$", stripped, re.DOTALL)
    if match:
        return match.group(1).strip()
    return stripped


def parse_text_response(response_record: JSONObject) -> str:
    return extract_response_text(response_record)


def parse_structured_response(
    response_record: JSONObject,
    output_model: type[BaseModel],
) -> tuple[str, JSONValue, BaseModel]:
    text = extract_response_text(response_record)
    if not text:
        raise StructuredOutputError(
            "empty_response_text",
            "empty response text for structured output parsing",
        )

    normalized = strip_json_fence(text)
    try:
        parsed = json.loads(normalized)
    except json.JSONDecodeError as exc:
        raise StructuredOutputError(
            "invalid_json",
            f"response is not valid JSON: {exc}",
            raw_error=cast(JSONValue, {"raw_text": text}),
        ) from exc

    try:
        validated = output_model.model_validate(parsed)
    except ValidationError as exc:
        raise StructuredOutputError(
            "structured_output_validation_failed",
            f"response JSON failed Pydantic validation: {exc}",
            raw_error=cast(JSONValue, {"parsed_json": parsed}),
        ) from exc

    return text, cast(JSONValue, parsed), validated
