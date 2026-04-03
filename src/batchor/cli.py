"""Typer-based CLI for running batchor jobs from CSV or JSONL files.

Provides the ``batchor`` command with sub-commands for:

* Starting or resuming a batch run (``run``).
* Polling an existing run (``status``).
* Exporting artifacts and results (``export``).
* Pruning retained artifacts (``prune``).

The CLI is primarily aimed at one-off operator workflows.  For production
automation, prefer the Python library API.
"""

from __future__ import annotations

import importlib
import json
import string
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, cast

import typer
from dotenv import find_dotenv, load_dotenv
from pydantic import BaseModel

from batchor.core.enums import OpenAIEndpoint
from batchor.core.models import (
    ArtifactExportResult,
    ArtifactPruneResult,
    BatchJob,
    BatchResultItem,
    OpenAIProviderConfig,
    PromptParts,
    RunSummary,
)
from batchor.core.types import JSONObject
from batchor.providers.base import BatchProvider
from batchor.runtime.runner import BatchRunner
from batchor.runtime.validation import default_schema_name
from batchor.sources.base import CheckpointedItemSource
from batchor.sources.composite import CompositeItemSource
from batchor.sources.files import CsvItemSource, JsonlItemSource
from batchor.storage.sqlite_store import SQLiteStorage

app_name = "batchor"


def _echo_json(payload: object) -> None:
    typer.echo(json.dumps(payload, ensure_ascii=False))


def _echo_error(message: str) -> None:
    typer.echo(message, err=True)


def _required_template_fields(template: str) -> list[str]:
    fields: list[str] = []
    for _, field_name, _, _ in string.Formatter().parse(template):
        if field_name and field_name not in fields:
            fields.append(field_name)
    return fields


def _row_field_map(row: Mapping[object, object]) -> JSONObject:
    return cast(JSONObject, {str(key): value for key, value in row.items()})


def _missing_fields(row: Mapping[str, object], fields: list[str]) -> list[str]:
    return [field for field in fields if field not in row or row[field] is None]


def _require_fields(row: Mapping[str, object], fields: list[str]) -> None:
    missing = _missing_fields(row, fields)
    if missing:
        raise ValueError(f"missing required fields: {', '.join(missing)}")


def _json_object_row(row: object) -> JSONObject:
    if not isinstance(row, dict):
        raise ValueError("JSONL input rows must be JSON objects")
    return cast(JSONObject, {str(key): value for key, value in row.items()})


def _build_prompt_factory(
    *,
    prompt_field: str | None,
    prompt_template: str | None,
) -> Callable[[Any], PromptParts]:
    if bool(prompt_field) == bool(prompt_template):
        raise ValueError("exactly one of --prompt-field or --prompt-template is required")

    template_fields = _required_template_fields(prompt_template) if prompt_template else []

    def build_prompt(item: Any) -> PromptParts:
        payload = item.payload
        if not isinstance(payload, dict):
            raise TypeError("CLI item payload must be a mapping")
        row = _row_field_map(payload)
        if prompt_field is not None:
            _require_fields(row, [prompt_field])
            return PromptParts(prompt=str(row[prompt_field]))
        _require_fields(row, template_fields)
        return PromptParts(prompt=cast(str, prompt_template).format_map(row))

    return build_prompt


def _load_output_model(import_path: str) -> type[BaseModel]:
    module_name, sep, qualname = import_path.partition(":")
    if not sep or not module_name or not qualname:
        raise ValueError("structured output class must use the form module.path:ClassName")
    if "<locals>" in qualname:
        raise ValueError("structured output class must be importable at module scope")
    module = importlib.import_module(module_name)
    target: Any = module
    for attribute in qualname.split("."):
        target = getattr(target, attribute, None)
        if target is None:
            raise ValueError(f"structured output class not found: {import_path}")
    if not isinstance(target, type) or not issubclass(target, BaseModel):
        raise ValueError(f"structured output class must be a Pydantic BaseModel subclass: {import_path}")
    return cast(type[BaseModel], target)


def _storage_for_path(db_path: Path | None) -> SQLiteStorage:
    if db_path is None:
        return SQLiteStorage(name="default")
    return SQLiteStorage(path=db_path)


def _runner_for_path(
    *,
    db_path: Path | None,
    provider_factory: Callable[[Any], BatchProvider] | None,
) -> tuple[BatchRunner, SQLiteStorage]:
    storage = _storage_for_path(db_path)
    return (
        BatchRunner(storage=storage, provider_factory=provider_factory),
        storage,
    )


def _serialize_summary(summary: RunSummary) -> dict[str, object]:
    return {
        "run_id": summary.run_id,
        "status": summary.status.value,
        "total_items": summary.total_items,
        "completed_items": summary.completed_items,
        "failed_items": summary.failed_items,
        "status_counts": {status.value: count for status, count in summary.status_counts.items()},
        "active_batches": summary.active_batches,
        "backoff_remaining_sec": summary.backoff_remaining_sec,
    }


def _serialize_error(error: object) -> dict[str, object] | None:
    if error is None:
        return None
    error_class = getattr(error, "error_class", None)
    message = getattr(error, "message", None)
    retryable = getattr(error, "retryable", None)
    raw_error = getattr(error, "raw_error", None)
    if not isinstance(error_class, str) or not isinstance(message, str) or not isinstance(retryable, bool):
        raise TypeError("unexpected error payload")
    return {
        "error_class": error_class,
        "message": message,
        "retryable": retryable,
        "raw_error": raw_error,
    }


def _serialize_result(result: BatchResultItem) -> dict[str, object]:
    payload: dict[str, object] = {
        "item_id": result.item_id,
        "status": result.status.value,
        "attempt_count": result.attempt_count,
        "metadata": result.metadata,
        "output_text": result.output_text,
        "raw_response": result.raw_response,
        "error": _serialize_error(result.error),
    }
    structured_output = getattr(result, "output", None)
    if isinstance(structured_output, BaseModel):
        payload["output"] = structured_output.model_dump(mode="json")
    return payload


def _serialize_export(result: ArtifactExportResult) -> dict[str, object]:
    return {
        "run_id": result.run_id,
        "destination_dir": result.destination_dir,
        "manifest_path": result.manifest_path,
        "results_path": result.results_path,
        "exported_artifact_paths": result.exported_artifact_paths,
    }


def _serialize_prune(result: ArtifactPruneResult) -> dict[str, object]:
    return {
        "run_id": result.run_id,
        "removed_artifact_paths": result.removed_artifact_paths,
        "missing_artifact_paths": result.missing_artifact_paths,
        "cleared_item_pointers": result.cleared_item_pointers,
        "cleared_batch_pointers": result.cleared_batch_pointers,
    }


def _write_results(
    *,
    results: list[BatchResultItem],
    output: Path | None,
) -> None:
    lines = [json.dumps(_serialize_result(result), ensure_ascii=False) for result in results]
    if output is None:
        for line in lines:
            typer.echo(line)
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(("\n".join(lines) + "\n") if lines else "", encoding="utf-8")


def _source_for_input(
    *,
    input_path: Path,
    id_field: str,
) -> CheckpointedItemSource[JSONObject]:
    suffix = input_path.suffix.lower()
    required_fields = [id_field]
    if suffix == ".csv":
        return CsvItemSource(
            input_path,
            item_id_from_row=lambda row: (_require_fields(row, required_fields), str(row[id_field]))[1],
            payload_from_row=lambda row: _row_field_map(row),
            metadata_from_row=lambda row: _row_field_map(row),
        )
    if suffix == ".jsonl":
        return JsonlItemSource(
            input_path,
            item_id_from_row=lambda row: (
                _require_fields(_json_object_row(row), required_fields),
                str(_json_object_row(row)[id_field]),
            )[1],
            payload_from_row=_json_object_row,
            metadata_from_row=_json_object_row,
        )
    raise ValueError("input file must end with .csv or .jsonl")


def _source_for_inputs(
    *,
    input_paths: list[Path],
    id_field: str,
) -> CheckpointedItemSource[JSONObject]:
    sources = [_source_for_input(input_path=input_path, id_field=id_field) for input_path in input_paths]
    if len(sources) == 1:
        return sources[0]
    return CompositeItemSource(sources)


def create_app(
    *,
    provider_factory: Callable[[Any], BatchProvider] | None = None,
) -> typer.Typer:
    cli = typer.Typer(
        name=app_name,
        add_completion=False,
        no_args_is_help=True,
        pretty_exceptions_enable=False,
    )

    @cli.command("start")
    def start_command(
        input_paths: list[Path] = typer.Option(..., "--input", exists=True, dir_okay=False, readable=True),
        id_field: str = typer.Option(..., "--id-field"),
        prompt_field: str | None = typer.Option(None, "--prompt-field"),
        prompt_template: str | None = typer.Option(None, "--prompt-template"),
        model: str = typer.Option(..., "--model"),
        db_path: Path | None = typer.Option(None, "--db-path", dir_okay=False),
        run_id: str | None = typer.Option(None, "--run-id"),
        structured_output_class: str | None = typer.Option(None, "--structured-output-class"),
        schema_name: str | None = typer.Option(None, "--schema-name"),
        endpoint: OpenAIEndpoint = typer.Option(OpenAIEndpoint.RESPONSES, "--endpoint"),
        completion_window: str = typer.Option("24h", "--completion-window"),
        request_timeout_sec: int = typer.Option(30, "--request-timeout-sec"),
        poll_interval_sec: float = typer.Option(1.0, "--poll-interval-sec"),
        reasoning_effort: str | None = typer.Option(None, "--reasoning-effort"),
    ) -> None:
        storage: SQLiteStorage | None = None
        try:
            load_dotenv(find_dotenv(usecwd=True), override=False)
            source = _source_for_inputs(input_paths=input_paths, id_field=id_field)
            build_prompt = _build_prompt_factory(
                prompt_field=prompt_field,
                prompt_template=prompt_template,
            )
            output_model = None
            resolved_schema_name = schema_name
            if structured_output_class is not None:
                output_model = _load_output_model(structured_output_class)
                if resolved_schema_name is None:
                    resolved_schema_name = default_schema_name(output_model)
            provider_config = OpenAIProviderConfig(
                model=model,
                endpoint=endpoint,
                completion_window=completion_window,
                request_timeout_sec=request_timeout_sec,
                poll_interval_sec=poll_interval_sec,
                reasoning_effort=reasoning_effort,
            )
            runner, storage = _runner_for_path(
                db_path=db_path,
                provider_factory=provider_factory,
            )
            run = runner.start(
                BatchJob(
                    items=source,
                    build_prompt=build_prompt,
                    provider_config=provider_config,
                    structured_output=output_model,
                    schema_name=resolved_schema_name,
                ),
                run_id=run_id,
            )
            _echo_json(_serialize_summary(run.summary()))
        except Exception as exc:  # noqa: BLE001
            _echo_error(str(exc))
            raise typer.Exit(code=1) from exc
        finally:
            if storage is not None:
                storage.close()

    @cli.command("status")
    def status_command(
        run_id: str = typer.Option(..., "--run-id"),
        db_path: Path | None = typer.Option(None, "--db-path", dir_okay=False),
    ) -> None:
        storage: SQLiteStorage | None = None
        try:
            runner, storage = _runner_for_path(
                db_path=db_path,
                provider_factory=provider_factory,
            )
            run = runner.get_run(run_id)
            _echo_json(_serialize_summary(run.summary()))
        except Exception as exc:  # noqa: BLE001
            _echo_error(str(exc))
            raise typer.Exit(code=1) from exc
        finally:
            if storage is not None:
                storage.close()

    @cli.command("wait")
    def wait_command(
        run_id: str = typer.Option(..., "--run-id"),
        db_path: Path | None = typer.Option(None, "--db-path", dir_okay=False),
        timeout: float | None = typer.Option(None, "--timeout"),
        poll_interval: float | None = typer.Option(None, "--poll-interval"),
    ) -> None:
        storage: SQLiteStorage | None = None
        try:
            runner, storage = _runner_for_path(
                db_path=db_path,
                provider_factory=provider_factory,
            )
            run = runner.get_run(run_id)
            finished = run.wait(timeout=timeout, poll_interval=poll_interval)
            _echo_json(_serialize_summary(finished.summary()))
        except Exception as exc:  # noqa: BLE001
            _echo_error(str(exc))
            raise typer.Exit(code=1) from exc
        finally:
            if storage is not None:
                storage.close()

    @cli.command("results")
    def results_command(
        run_id: str = typer.Option(..., "--run-id"),
        db_path: Path | None = typer.Option(None, "--db-path", dir_okay=False),
        output: Path | None = typer.Option(None, "--output", dir_okay=False),
    ) -> None:
        storage: SQLiteStorage | None = None
        try:
            runner, storage = _runner_for_path(
                db_path=db_path,
                provider_factory=provider_factory,
            )
            run = runner.get_run(run_id)
            results = run.results()
            _write_results(results=results, output=output)
            if output is not None:
                _echo_json(
                    {
                        "run_id": run_id,
                        "status": run.status.value,
                        "result_count": len(results),
                        "output_path": str(output),
                    }
                )
        except Exception as exc:  # noqa: BLE001
            _echo_error(str(exc))
            raise typer.Exit(code=1) from exc
        finally:
            if storage is not None:
                storage.close()

    @cli.command("export-artifacts")
    def export_artifacts_command(
        run_id: str = typer.Option(..., "--run-id"),
        destination_dir: Path = typer.Option(..., "--destination-dir", file_okay=False),
        db_path: Path | None = typer.Option(None, "--db-path", dir_okay=False),
    ) -> None:
        storage: SQLiteStorage | None = None
        try:
            runner, storage = _runner_for_path(
                db_path=db_path,
                provider_factory=provider_factory,
            )
            export_result = runner.export_artifacts(run_id, destination_dir=destination_dir)
            _echo_json(_serialize_export(export_result))
        except Exception as exc:  # noqa: BLE001
            _echo_error(str(exc))
            raise typer.Exit(code=1) from exc
        finally:
            if storage is not None:
                storage.close()

    @cli.command("prune-artifacts")
    def prune_artifacts_command(
        run_id: str = typer.Option(..., "--run-id"),
        db_path: Path | None = typer.Option(None, "--db-path", dir_okay=False),
        include_raw_output_artifacts: bool = typer.Option(
            False,
            "--include-raw-output-artifacts",
        ),
    ) -> None:
        storage: SQLiteStorage | None = None
        try:
            runner, storage = _runner_for_path(
                db_path=db_path,
                provider_factory=provider_factory,
            )
            prune_result = runner.prune_artifacts(
                run_id,
                include_raw_output_artifacts=include_raw_output_artifacts,
            )
            _echo_json(_serialize_prune(prune_result))
        except Exception as exc:  # noqa: BLE001
            _echo_error(str(exc))
            raise typer.Exit(code=1) from exc
        finally:
            if storage is not None:
                storage.close()

    return cli


app = create_app()
