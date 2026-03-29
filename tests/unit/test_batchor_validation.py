from __future__ import annotations

import pytest
from pydantic import BaseModel

from batchor.validation import (
    StructuredOutputError,
    default_schema_name,
    model_output_schema,
    parse_structured_response,
    parse_text_response,
)


class _ClassificationResult(BaseModel):
    label: str
    score: float


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
    assert "label" in schema["properties"]


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
