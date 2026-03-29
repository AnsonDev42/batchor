from __future__ import annotations

import json
import re
from typing import Any, cast

from pydantic import BaseModel, ValidationError

from batchor.types import JSONObject, JSONValue


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
    return resolved_schema_name, cast(JSONObject, model.model_json_schema())


def extract_response_text(response_record: dict[str, Any]) -> str:
    body = response_record.get("response", {}).get("body") or response_record.get("body")
    if not isinstance(body, dict):
        return ""

    output = body.get("output")
    if isinstance(output, list) and output:
        content = output[0].get("content") if isinstance(output[0], dict) else None
        if isinstance(content, list) and content:
            text = content[0].get("text") if isinstance(content[0], dict) else None
            if isinstance(text, str):
                return text

    choices = body.get("choices")
    if isinstance(choices, list) and choices:
        msg = choices[0].get("message", {}) if isinstance(choices[0], dict) else {}
        text = msg.get("content") if isinstance(msg, dict) else None
        if isinstance(text, str):
            return text

    return ""


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
