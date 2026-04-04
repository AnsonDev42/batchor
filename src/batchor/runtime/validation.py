"""Structured output schema validation and response parsing utilities.

Provides:

* :func:`model_output_schema` — derive a strict JSON Schema from a Pydantic
  model and validate it is compatible with the OpenAI structured output API.
* :func:`parse_structured_response` — extract and validate a Pydantic model
  instance from a provider response record.
* :func:`parse_text_response` — extract plain text from a provider response.
* :func:`validate_strict_json_schema` — validate that a schema satisfies
  OpenAI's strict JSON Schema requirements.
"""

from __future__ import annotations

import json
import re
from typing import Any, cast

from pydantic import BaseModel, ValidationError

from batchor.core.exceptions import StructuredOutputSchemaError
from batchor.core.types import JSONObject, JSONValue


class StructuredOutputError(ValueError):
    """Raised when a provider response cannot be parsed as the expected model.

    Attributes:
        error_class: Short machine-readable error category (e.g.
            ``"invalid_json"``, ``"structured_output_validation_failed"``).
        message: Human-readable error description.
        raw_error: Optional raw payload captured for debugging.
    """

    def __init__(
        self,
        error_class: str,
        message: str,
        *,
        raw_error: JSONValue | None = None,
    ) -> None:
        """Initialise the error.

        Args:
            error_class: Short machine-readable error category.
            message: Human-readable description of the parse failure.
            raw_error: Optional raw payload (parsed JSON or raw text) for
                debugging.
        """
        super().__init__(message)
        self.error_class = error_class
        self.message = message
        self.raw_error = raw_error


def default_schema_name(model: type[BaseModel]) -> str:
    """Derive a snake_case schema name from a Pydantic model class.

    Leading underscores are stripped before conversion.  Double underscores
    produced by the conversion are collapsed.

    Args:
        model: The Pydantic model class.

    Returns:
        A snake_case string suitable for use as an OpenAI schema name.

    Example:
        >>> default_schema_name(MyOutputModel)
        'my_output_model'
    """
    model_name = model.__name__.lstrip("_")
    name = re.sub(r"(?<!^)(?=[A-Z])", "_", model_name).lower()
    return name.replace("__", "_")


def model_output_schema(
    model: type[BaseModel],
    *,
    schema_name: str | None = None,
) -> tuple[str, JSONObject]:
    """Derive a validated strict JSON Schema from a Pydantic model.

    Calls :func:`validate_strict_json_schema` to ensure the schema is
    compatible with OpenAI's structured output API before returning it.

    Args:
        model: Pydantic model class to derive the schema from.
        schema_name: Optional explicit schema name.  When ``None`` the name is
            derived via :func:`default_schema_name`.

    Returns:
        A 2-tuple ``(schema_name, schema)`` where *schema_name* is the
        resolved name and *schema* is the strict JSON Schema object.

    Raises:
        StructuredOutputSchemaError: If the schema is not compatible with
            OpenAI's strict structured output requirements.
    """
    resolved_schema_name = schema_name or default_schema_name(model)
    normalized_schema = _strict_json_schema(cast(JSONObject, model.model_json_schema()))
    validate_strict_json_schema(normalized_schema)
    return resolved_schema_name, normalized_schema


def _strict_json_schema(schema: JSONObject) -> JSONObject:
    """Recursively normalise a JSON Schema to add ``additionalProperties: false``.

    Args:
        schema: Root JSON Schema object from ``model.model_json_schema()``.

    Returns:
        A normalised schema with ``additionalProperties: false`` set on every
        object type.
    """
    normalized = cast(JSONObject, _normalize_json_schema_value(schema))
    schema_type = normalized.get("type")
    if schema_type == "object" or (isinstance(schema_type, list) and "object" in schema_type):
        normalized.setdefault("additionalProperties", False)
    return normalized


def _normalize_json_schema_value(value: JSONValue) -> JSONValue:
    """Recursively walk a JSON Schema value and set ``additionalProperties: false``.

    Args:
        value: Any JSON value (dict, list, or scalar) from a schema.

    Returns:
        The normalised value with ``additionalProperties: false`` added to all
        object sub-schemas.
    """
    if isinstance(value, list):
        return [_normalize_json_schema_value(item) for item in value]
    if not isinstance(value, dict):
        return value
    normalized = cast(
        JSONObject,
        {key: _normalize_json_schema_value(item) for key, item in value.items()},
    )
    schema_type = normalized.get("type")
    if schema_type == "object" or (isinstance(schema_type, list) and "object" in schema_type):
        normalized.setdefault("additionalProperties", False)
    return normalized


def validate_strict_json_schema(schema: JSONObject) -> None:
    """Validate that *schema* satisfies OpenAI strict structured output rules.

    Rules enforced:

    * The root schema must be an object type (not ``anyOf``).
    * Every object sub-schema must have ``additionalProperties: false``.
    * Every object with properties must list all property names in
      ``required``.

    Args:
        schema: The root JSON Schema object to validate.

    Raises:
        StructuredOutputSchemaError: If any rule is violated.
    """
    if "anyOf" in schema:
        raise StructuredOutputSchemaError("structured output root schema must be an object and must not use anyOf")
    if not _schema_type_includes(schema.get("type"), "object"):
        raise StructuredOutputSchemaError("structured output root schema must be an object")
    _validate_json_schema_value(schema, path="$")


def _validate_json_schema_value(value: JSONValue, *, path: str) -> None:
    """Recursively validate all object sub-schemas in *value*.

    Args:
        value: A JSON Schema value to validate.
        path: JSONPath-style location string used in error messages.

    Raises:
        StructuredOutputSchemaError: If any object sub-schema is invalid.
    """
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_json_schema_value(item, path=f"{path}[{index}]")
        return
    if not isinstance(value, dict):
        return
    if "anyOf" in value:
        raise StructuredOutputSchemaError(f"{path}: anyOf is not allowed in strict structured output schemas")
    if _schema_type_includes(value.get("type"), "object"):
        additional_properties = value.get("additionalProperties")
        if additional_properties is not False:
            raise StructuredOutputSchemaError(f"{path}: object schemas must set additionalProperties to false")
        properties = value.get("properties")
        if isinstance(properties, dict):
            required = value.get("required")
            if not isinstance(required, list):
                raise StructuredOutputSchemaError(f"{path}: object schemas with properties must define required")
            property_names = set(properties.keys())
            required_names = {name for name in required if isinstance(name, str)}
            missing_names = sorted(property_names - required_names)
            if missing_names:
                raise StructuredOutputSchemaError(f"{path}: properties must all be required; missing {missing_names}")
    for key, item in value.items():
        next_path = f"{path}.{key}" if key.isidentifier() else f"{path}[{key!r}]"
        _validate_json_schema_value(item, path=next_path)


def _schema_type_includes(value: object, expected_type: str) -> bool:
    """Return ``True`` if the schema ``type`` field includes *expected_type*.

    Args:
        value: The ``type`` value from a JSON Schema node.
        expected_type: The type string to look for.

    Returns:
        ``True`` if *value* equals *expected_type* or is a list containing it.
    """
    if value == expected_type:
        return True
    return isinstance(value, list) and expected_type in value


def _extract_content_text(content: Any) -> list[str]:
    """Extract text fragments from an OpenAI response content field.

    Args:
        content: The ``content`` field from a response message, which may be
            a string, a list of content parts, or ``None``.

    Returns:
        A list of non-empty text strings extracted from *content*.
    """
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
    """Extract concatenated text output from a raw provider response record.

    Handles both Responses API (``output`` list) and Chat Completions API
    (``choices`` list) response shapes.

    Args:
        response_record: A single JSONL record from the provider output file.

    Returns:
        Extracted text joined by newlines, or an empty string if nothing is
        found.
    """
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
    r"""Remove a Markdown JSON code fence from *text* if present.

    Args:
        text: Text that may be wrapped in a ``\`\`\`json … \`\`\``` block.

    Returns:
        The content inside the fence, or *text* unchanged if no fence is found.
    """
    stripped = text.strip()
    match = re.match(r"^```(?:json)?\s*\n?(.*?)\n?```\s*$", stripped, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return stripped


def parse_text_response(response_record: JSONObject) -> str:
    """Extract plain text from a provider response record.

    Args:
        response_record: A single JSONL record from the provider output file.

    Returns:
        Extracted response text (may be empty).
    """
    return extract_response_text(response_record)


def parse_structured_response(
    response_record: JSONObject,
    output_model: type[BaseModel],
) -> tuple[str, JSONValue, BaseModel]:
    """Parse and validate a structured JSON response from the provider.

    Args:
        response_record: A single JSONL record from the provider output file.
        output_model: Pydantic model class to validate the parsed JSON against.

    Returns:
        A 3-tuple ``(output_text, parsed_json, validated_model)`` where
        *output_text* is the raw text, *parsed_json* is the deserialized dict,
        and *validated_model* is the Pydantic model instance.

    Raises:
        StructuredOutputError: If the response text is empty, is not valid
            JSON, or fails Pydantic validation.
    """
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
