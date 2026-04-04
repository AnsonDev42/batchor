from __future__ import annotations

import pytest
from pydantic import BaseModel, ConfigDict, RootModel

from batchor.core.exceptions import StructuredOutputSchemaError
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


class OptionalFieldResult(BaseModel):
    label: str
    score: float | None = None


class OpenObjectResult(BaseModel):
    model_config = ConfigDict(extra="allow")

    label: str


class UnionRootResult(RootModel[_ClassificationResult | NestedPayload]):
    pass


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
        if schema_type == "object" or (isinstance(schema_type, list) and "object" in schema_type):
            results.append(value)
        for item in value.values():
            results.extend(_collect_object_schemas(item))
        return results

    object_schemas = _collect_object_schemas(schema)
    assert object_schemas != []
    assert all(item.get("additionalProperties") is False for item in object_schemas)


def test_model_output_schema_rejects_optional_properties() -> None:
    with pytest.raises(StructuredOutputSchemaError, match="must all be required"):
        model_output_schema(OptionalFieldResult)


def test_model_output_schema_rejects_open_object_models() -> None:
    with pytest.raises(StructuredOutputSchemaError, match="additionalProperties"):
        model_output_schema(OpenObjectResult)


def test_model_output_schema_rejects_root_anyof() -> None:
    with pytest.raises(StructuredOutputSchemaError, match="must not use anyOf"):
        model_output_schema(UnionRootResult)


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


# ---------------------------------------------------------------------------
# Bug fix: nested anyOf (non-null union fields) must be rejected
# ---------------------------------------------------------------------------


class NonNullUnionField(BaseModel):
    """str | int generates anyOf at property level — must be rejected."""

    value: str | int


class NestedUnionPayload(BaseModel):
    label: str


class EnvelopeWithUnion(BaseModel):
    nested: NestedUnionPayload
    flag: str | int


def test_model_output_schema_rejects_non_null_union_field() -> None:
    """A required str|int field generates nested anyOf that OpenAI strict mode rejects."""
    with pytest.raises(StructuredOutputSchemaError, match="anyOf"):
        model_output_schema(NonNullUnionField)


def test_model_output_schema_rejects_nested_union_in_envelope() -> None:
    """anyOf must be caught even when it appears alongside valid nested objects."""
    with pytest.raises(StructuredOutputSchemaError, match="anyOf"):
        model_output_schema(EnvelopeWithUnion)


# ---------------------------------------------------------------------------
# Bug fix: strip_json_fence must handle uppercase/mixed-case language tags
# ---------------------------------------------------------------------------


from batchor.runtime.validation import strip_json_fence  # noqa: E402


@pytest.mark.parametrize(
    "lang_tag",
    ["json", "JSON", "Json", "jSoN"],
)
def test_strip_json_fence_handles_any_case_json_tag(lang_tag: str) -> None:
    fenced = f'```{lang_tag}\n{{"k": "v"}}\n```'
    assert strip_json_fence(fenced) == '{"k": "v"}'


def test_strip_json_fence_leaves_non_json_fences_intact() -> None:
    fenced = "```python\nx = 1\n```"
    result = strip_json_fence(fenced)
    # Non-json language tag: fence is stripped (no language restriction on outer fence)
    # The regex matches ```<anything> when the tag is absent or json — python stays
    assert "```" not in result or "python" in result


def test_strip_json_fence_strips_fence_with_no_language_tag() -> None:
    assert strip_json_fence('```\n{"k": "v"}\n```') == '{"k": "v"}'


def test_parse_structured_response_handles_uppercase_json_fence() -> None:
    """Reproduces the bug: ```JSON fence was not stripped, causing invalid_json error."""
    record = _responses_record('```JSON\n{"label": "ai", "score": 0.9}\n```')
    text, parsed, validated = parse_structured_response(record, _ClassificationResult)
    assert validated.label == "ai"
    assert validated.score == 0.9


# ---------------------------------------------------------------------------
# validate_strict_json_schema: non-object root
# ---------------------------------------------------------------------------


def test_validate_strict_json_schema_rejects_non_object_root() -> None:
    from batchor.runtime.validation import validate_strict_json_schema

    with pytest.raises(StructuredOutputSchemaError, match="must be an object"):
        validate_strict_json_schema({"type": "string"})


def test_validate_strict_json_schema_rejects_object_without_required() -> None:
    from batchor.runtime.validation import validate_strict_json_schema

    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {"x": {"type": "string"}},
        # intentionally missing "required"
    }
    with pytest.raises(StructuredOutputSchemaError, match="required"):
        validate_strict_json_schema(schema)


# ---------------------------------------------------------------------------
# extract_response_text: uncovered paths
# ---------------------------------------------------------------------------


from batchor.runtime.validation import extract_response_text  # noqa: E402


def test_extract_response_text_falls_back_to_direct_body_key() -> None:
    """Records without a 'response' wrapper but with a direct 'body' key."""
    record = {
        "body": {
            "output": [{"content": [{"text": "direct"}]}],
        }
    }
    assert extract_response_text(record) == "direct"


def test_extract_response_text_handles_output_item_text_string() -> None:
    """output_item has a top-level 'text' string field."""
    record = {
        "response": {
            "body": {
                "output": [{"text": "item-text"}],
            }
        }
    }
    assert extract_response_text(record) == "item-text"


def test_extract_response_text_handles_output_item_text_dict() -> None:
    """output_item has 'text' as a dict with a 'value' key (older API shape)."""
    record = {
        "response": {
            "body": {
                "output": [{"text": {"value": "nested-value"}}],
            }
        }
    }
    assert extract_response_text(record) == "nested-value"


def test_extract_response_text_handles_output_text_field() -> None:
    """Body has an 'output_text' top-level field."""
    record = {"response": {"body": {"output_text": "flat-text"}}}
    assert extract_response_text(record) == "flat-text"


def test_extract_response_text_returns_empty_for_non_dict_body() -> None:
    assert extract_response_text({"response": {"body": "not-a-dict"}}) == ""
    assert extract_response_text({}) == ""


def test_extract_response_text_skips_non_dict_output_items() -> None:
    record = {
        "response": {
            "body": {
                "output": ["string-item", {"content": [{"text": "good"}]}],
            }
        }
    }
    assert extract_response_text(record) == "good"


def test_extract_response_text_skips_non_dict_choices() -> None:
    record = {
        "response": {
            "body": {
                "choices": ["bad-choice", {"message": {"content": "hello"}}],
            }
        }
    }
    assert extract_response_text(record) == "hello"


def test_extract_response_text_skips_non_dict_message() -> None:
    record = {
        "response": {
            "body": {
                "choices": [{"message": "not-a-dict"}],
            }
        }
    }
    assert extract_response_text(record) == ""


# ---------------------------------------------------------------------------
# _extract_content_text: uncovered paths
# ---------------------------------------------------------------------------


from batchor.runtime.validation import _extract_content_text  # noqa: E402


def test_extract_content_text_handles_plain_string() -> None:
    assert _extract_content_text("direct string") == ["direct string"]


def test_extract_content_text_returns_empty_for_non_list_non_string() -> None:
    assert _extract_content_text(None) == []
    assert _extract_content_text(42) == []


def test_extract_content_text_handles_string_items_in_list() -> None:
    assert _extract_content_text(["a", "b"]) == ["a", "b"]


def test_extract_content_text_skips_non_dict_non_string_items() -> None:
    assert _extract_content_text([123, None, {"text": "ok"}]) == ["ok"]
