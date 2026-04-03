"""Internal helpers for request replay and raw artifact persistence."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast
from uuid import uuid4

from batchor.artifacts import ArtifactStore
from batchor.core.types import JSONObject


@dataclass
class RequestArtifactCache:
    """Per-cycle cache for replayable request JSONL contents.

    Attributes:
        lines_by_path: Cached request-artifact lines keyed by artifact path.
    """

    lines_by_path: dict[str, list[str]] = field(default_factory=dict)


def request_sha256(request_line: JSONObject) -> str:
    """Return a stable digest for one request JSON object.

    Args:
        request_line: Request JSON object to hash.

    Returns:
        Stable SHA-256 hex digest for the serialized request line.
    """
    encoded = json.dumps(
        request_line,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def request_artifact_relative_path(run_id: str) -> Path:
    """Return the relative path used for a new request artifact.

    Args:
        run_id: Durable run identifier.

    Returns:
        Relative artifact path for a newly written request JSONL file.
    """
    return Path(run_id) / "requests" / f"requests_{uuid4().hex}.jsonl"


def serialize_jsonl(records: list[JSONObject]) -> str:
    """Serialize JSON objects into newline-delimited JSON.

    Args:
        records: JSON records to serialize.

    Returns:
        Newline-delimited JSON text.
    """
    if not records:
        return ""
    return "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records)


def load_request_artifact_line(
    *,
    artifact_store: ArtifactStore,
    artifact_path: str,
    line_number: int,
    expected_sha256: str,
    artifact_cache: RequestArtifactCache | None = None,
) -> JSONObject:
    """Load and validate one request line from a replay artifact.

    Args:
        artifact_store: Artifact store that owns the replayable request file.
        artifact_path: Relative artifact path to read.
        line_number: One-based line number within the JSONL file.
        expected_sha256: Expected digest for the stored request line.
        artifact_cache: Optional per-cycle request cache.

    Returns:
        The validated request JSON object.

    Raises:
        ValueError: If the line number is invalid or the stored hash does not
            match.
        TypeError: If the target line is not a JSON object.
        FileNotFoundError: If the requested line does not exist.
    """
    if line_number <= 0:
        raise ValueError("line_number must be > 0")
    cache = artifact_cache or RequestArtifactCache()
    lines = cache.lines_by_path.get(artifact_path)
    if lines is None:
        lines = artifact_store.read_text(
            artifact_path,
            encoding="utf-8",
        ).splitlines()
        cache.lines_by_path[artifact_path] = lines
    for index, raw_line in enumerate(lines, start=1):
        if index != line_number:
            continue
        record = json.loads(raw_line)
        if not isinstance(record, dict):
            raise TypeError(f"request artifact line must be a JSON object: {artifact_path}:{line_number}")
        request_line = cast(JSONObject, record)
        actual_sha256 = request_sha256(request_line)
        if actual_sha256 != expected_sha256:
            raise ValueError(f"request artifact hash mismatch for {artifact_path}:{line_number}")
        return request_line
    raise FileNotFoundError(f"request artifact line missing: {artifact_path}:{line_number}")


def write_batch_result_artifacts(
    *,
    artifact_store: ArtifactStore,
    run_id: str,
    provider_batch_id: str,
    output_content: str | None,
    error_content: str | None,
    persist_raw_output_artifacts: bool,
) -> tuple[str | None, str | None]:
    """Persist raw provider output and error files when retention is enabled.

    Args:
        artifact_store: Artifact store used to persist raw provider payloads.
        run_id: Durable run identifier.
        provider_batch_id: Provider batch identifier used in artifact names.
        output_content: Raw output JSONL content, if any.
        error_content: Raw error JSONL content, if any.
        persist_raw_output_artifacts: Whether raw payload retention is enabled.

    Returns:
        Tuple of ``(output_artifact_path, error_artifact_path)``. Each element
        is ``None`` when the corresponding artifact was not written.
    """
    if not persist_raw_output_artifacts:
        return None, None
    output_artifact_path = None
    if output_content is not None:
        output_artifact_path = (Path(run_id) / "outputs" / f"{provider_batch_id}_output.jsonl").as_posix()
        artifact_store.write_text(output_artifact_path, output_content, encoding="utf-8")
    error_artifact_path = None
    if error_content is not None:
        error_artifact_path = (Path(run_id) / "outputs" / f"{provider_batch_id}_error.jsonl").as_posix()
        artifact_store.write_text(error_artifact_path, error_content, encoding="utf-8")
    return output_artifact_path, error_artifact_path


def remove_artifacts(
    artifact_store: ArtifactStore,
    artifact_paths: list[str],
) -> tuple[list[str], list[str]]:
    """Delete artifacts and return removed vs missing paths.

    Args:
        artifact_store: Artifact store used to delete artifacts.
        artifact_paths: Relative artifact paths to delete.

    Returns:
        Tuple of ``(removed_paths, missing_paths)``.
    """
    removed: list[str] = []
    missing: list[str] = []
    for artifact_path in artifact_paths:
        if artifact_store.delete(artifact_path):
            removed.append(artifact_path)
        else:
            missing.append(artifact_path)
    return removed, missing
