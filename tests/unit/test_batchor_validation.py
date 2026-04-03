from __future__ import annotations

import pytest
from pydantic import BaseModel

from batchor.runtime.validation import (
    StructuredOutputError,
    default_schema_name,
    model_output_schema,
    parse_structured_response,
    parse_text_response,
)


class _ClassificationResult(BaseModel):
    label: str
    score: float


class NestedPayload(BaseModel):
    label: str


class EnvelopeResult(BaseModel):
    payload: NestedPayload


def _responses_record(text: str) -> dict[str, object]:
    return {
        "response": {
            "body": {
                "output": [
                    {
                        "content": [
                            {
                                "text": text,
                            }
                        ]
                    }
                ]
            }
        }
    }


def test_default_schema_name_uses_snake_case() -> None:
    assert default_schema_name(_ClassificationResult) == "classification_result"


def test_model_output_schema_uses_default_schema_name() -> None:
    schema_name, schema = model_output_schema(_ClassificationResult)
    assert schema_name == "classification_result"
    assert schema["type"] == "object"
    assert schema["additionalProperties"] is False
    assert "label" in schema["properties"]


def test_model_output_schema_marks_nested_objects_as_closed() -> None:
    _, schema = model_output_schema(EnvelopeResult)

    def _collect_object_schemas(value: object) -> list[dict[str, object]]:
        if isinstance(value, list):
            results: list[dict[str, object]] = []
            for item in value:
                results.extend(_collect_object_schemas(item))
            return results
        if not isinstance(value, dict):
            return []
        results = []
        schema_type = value.get("type")
        if schema_type == "object" or (
            isinstance(schema_type, list) and "object" in schema_type
        ):
            results.append(value)
        for item in value.values():
            results.extend(_collect_object_schemas(item))
        return results

    object_schemas = _collect_object_schemas(schema)
    assert object_schemas != []
    assert all(item.get("additionalProperties") is False for item in object_schemas)


def test_parse_structured_response_returns_model_instance() -> None:
    text, parsed, validated = parse_structured_response(
        _responses_record('{"label":"ai","score":0.9}'),
        _ClassificationResult,
    )
    assert text == '{"label":"ai","score":0.9}'
    assert parsed == {"label": "ai", "score": 0.9}
    assert isinstance(validated, _ClassificationResult)
    assert validated.label == "ai"


def test_parse_structured_response_rejects_invalid_json() -> None:
    with pytest.raises(StructuredOutputError, match="not valid JSON"):
        parse_structured_response(_responses_record("{not json}"), _ClassificationResult)


def test_parse_structured_response_rejects_empty_text() -> None:
    with pytest.raises(StructuredOutputError, match="empty response text"):
        parse_structured_response({"response": {"body": {}}}, _ClassificationResult)


def test_parse_structured_response_rejects_validation_error() -> None:
    with pytest.raises(StructuredOutputError, match="Pydantic validation"):
        parse_structured_response(_responses_record('{"label":"ai"}'), _ClassificationResult)


def test_parse_text_response_returns_plain_text() -> None:
    assert parse_text_response(_responses_record("plain text")) == "plain text"


def test_parse_text_response_joins_multiple_response_blocks() -> None:
    record = {
        "response": {
            "body": {
                "output": [
                    {"content": [{"type": "output_text", "text": "hello"}]},
                    {"content": [{"type": "output_text", "text": "world"}]},
                ]
            }
        }
    }
    assert parse_text_response(record) == "hello\nworld"


def test_parse_text_response_supports_chat_content_lists() -> None:
    record = {
        "response": {
            "body": {
                "choices": [
                    {
                        "message": {
                            "content": [
                                {"type": "text", "text": "alpha"},
                                {"type": "text", "text": {"value": "beta"}},
                            ]
                        }
                    }
                ]
            }
        }
    }
    assert parse_text_response(record) == "alpha\nbeta"
