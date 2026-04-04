"""Tests for runtime/results.py — serialize_result and write_results_export."""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel

from batchor.core.enums import ItemStatus
from batchor.core.models import ItemFailure, StructuredItemResult, TextItemResult
from batchor.runtime.results import serialize_result, write_results_export


class _Score(BaseModel):
    label: str
    score: float


def _text_result(
    *,
    item_id: str = "r1",
    status: ItemStatus = ItemStatus.COMPLETED,
    output_text: str | None = "hello",
    error: ItemFailure | None = None,
) -> TextItemResult:
    return TextItemResult(
        item_id=item_id,
        status=status,
        attempt_count=1,
        output_text=output_text,
        metadata={"src": "test"},
        error=error,
    )


def _structured_result(
    *,
    item_id: str = "r1",
    status: ItemStatus = ItemStatus.COMPLETED,
    output: _Score | None = _Score(label="ai", score=0.9),
    error: ItemFailure | None = None,
) -> StructuredItemResult[_Score]:
    return StructuredItemResult(
        item_id=item_id,
        status=status,
        attempt_count=1,
        output=output,
        output_text='{"label":"ai","score":0.9}',
        metadata={},
        error=error,
    )


# ---------------------------------------------------------------------------
# serialize_result
# ---------------------------------------------------------------------------


def test_serialize_result_text_completed() -> None:
    result = _text_result()
    payload = serialize_result(result)
    assert payload["item_id"] == "r1"
    assert payload["status"] == "completed"
    assert payload["output_text"] == "hello"
    assert payload["error"] is None
    assert "output_json" not in payload


def test_serialize_result_text_with_error() -> None:
    failure = ItemFailure(
        error_class="provider_item_error",
        message="something failed",
        retryable=False,
        raw_error={"detail": "oops"},
    )
    result = _text_result(status=ItemStatus.FAILED_PERMANENT, output_text=None, error=failure)
    payload = serialize_result(result)
    assert payload["error"] is not None
    assert payload["error"]["error_class"] == "provider_item_error"
    assert payload["error"]["retryable"] is False
    assert payload["error"]["raw_error"] == {"detail": "oops"}


def test_serialize_result_structured_completed() -> None:
    result = _structured_result()
    payload = serialize_result(result)
    assert payload["item_id"] == "r1"
    assert "output_json" in payload
    assert payload["output_json"] == {"label": "ai", "score": 0.9}


def test_serialize_result_structured_none_output() -> None:
    """Failed structured result with no output — output_json must be None."""
    failure = ItemFailure(error_class="structured_output_validation_failed", message="bad", retryable=True)
    result = _structured_result(status=ItemStatus.FAILED_PERMANENT, output=None, error=failure)
    payload = serialize_result(result)
    assert payload["output_json"] is None
    assert payload["error"] is not None


# ---------------------------------------------------------------------------
# write_results_export
# ---------------------------------------------------------------------------


def test_write_results_export_creates_valid_jsonl(tmp_path: Path) -> None:
    results_path = tmp_path / "results.jsonl"
    results = [_text_result(item_id="r1"), _text_result(item_id="r2", output_text="world")]
    write_results_export(results_path=results_path, results=results)

    lines = results_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    first = json.loads(lines[0])
    assert first["item_id"] == "r1"
    second = json.loads(lines[1])
    assert second["output_text"] == "world"


def test_write_results_export_writes_empty_string_for_no_results(tmp_path: Path) -> None:
    results_path = tmp_path / "results.jsonl"
    write_results_export(results_path=results_path, results=[])
    assert results_path.read_text(encoding="utf-8") == ""


def test_write_results_export_includes_structured_output_json(tmp_path: Path) -> None:
    results_path = tmp_path / "results.jsonl"
    write_results_export(results_path=results_path, results=[_structured_result()])
    record = json.loads(results_path.read_text(encoding="utf-8").strip())
    assert record["output_json"] == {"label": "ai", "score": 0.9}
