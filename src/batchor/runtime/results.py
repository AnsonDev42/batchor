"""Internal helpers for mapping persisted item rows into public results."""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

from pydantic import BaseModel

from batchor.core.models import BatchResultItem, StructuredItemResult, TextItemResult
from batchor.core.types import JSONObject
from batchor.runtime.context import RunContext
from batchor.storage.state import PersistedItemRecord


def results_for_records(
    records: list[PersistedItemRecord],
    *,
    context: RunContext,
) -> list[BatchResultItem]:
    """Convert persisted item records into public result models.

    Args:
        records: Persisted item rows from storage.
        context: Runtime context used for structured-output rehydration.

    Returns:
        Public result models in storage order.
    """
    return [result_from_record(record, context=context) for record in records]


def result_from_record(
    record: PersistedItemRecord,
    *,
    context: RunContext,
) -> BatchResultItem:
    """Convert one persisted item row into a public result model.

    Args:
        record: Persisted item row.
        context: Runtime context used for structured-output rehydration.

    Returns:
        Public result model for the record.
    """
    if context.output_model is None:
        return TextItemResult(
            item_id=record.item_id,
            status=record.status,
            attempt_count=record.attempt_count,
            output_text=record.output_text,
            raw_response=record.raw_response,
            error=record.error,
            metadata=record.metadata,
        )
    output_model = None
    if record.output_json is not None:
        output_model = context.output_model.model_validate(record.output_json)
    return StructuredItemResult(
        item_id=record.item_id,
        status=record.status,
        attempt_count=record.attempt_count,
        output=output_model,
        output_text=record.output_text,
        raw_response=record.raw_response,
        error=record.error,
        metadata=record.metadata,
    )


def write_results_export(
    *,
    results_path: Path,
    results: list[BatchResultItem],
) -> None:
    """Write public result models to a JSONL export file.

    Args:
        results_path: Destination JSONL path.
        results: Public result models to serialize.
    """
    lines = [json.dumps(serialize_result(result), ensure_ascii=False) for result in results]
    results_path.write_text(
        ("\n".join(lines) + "\n") if lines else "",
        encoding="utf-8",
    )


def serialize_result(result: BatchResultItem) -> JSONObject:
    """Serialize a public result model into a JSON-compatible dict.

    Args:
        result: Public result model to serialize.

    Returns:
        JSON-compatible dictionary for the result.
    """
    payload: JSONObject = {
        "item_id": result.item_id,
        "status": result.status.value,
        "attempt_count": result.attempt_count,
        "metadata": result.metadata,
        "output_text": result.output_text,
        "raw_response": result.raw_response,
        "error": None,
    }
    if result.error is not None:
        payload["error"] = {
            "error_class": result.error.error_class,
            "message": result.error.message,
            "retryable": result.error.retryable,
            "raw_error": result.error.raw_error,
        }
    if isinstance(result, StructuredItemResult):
        output = result.output
        payload["output_json"] = cast(BaseModel, output).model_dump(mode="json") if output is not None else None
    return payload
