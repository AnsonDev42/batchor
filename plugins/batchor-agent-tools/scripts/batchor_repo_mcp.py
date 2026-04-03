#!/usr/bin/env python3
"""Minimal stdio MCP server with repo-specific guidance for batchor contributors."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
SERVER_NAME = "batchorRepo"
SERVER_VERSION = "0.1.0"
DEFAULT_PROTOCOL_VERSION = "2025-03-26"


def _repo_path(relative_path: str) -> str:
    return str(REPO_ROOT / relative_path)


def _project_guide() -> str:
    lines = [
        "batchor is a durable OpenAI Batch runner with typed Pydantic outputs, resumable runs, durable artifacts, and a narrow operator CLI.",
        "",
        f"Repo root: {REPO_ROOT}",
        "Read these first:",
        f"- AGENTS.md: {_repo_path('AGENTS.md')}",
        f"- README.md: {_repo_path('README.md')}",
        f"- Architecture: {_repo_path('docs/design_docs/ARCHITECTURE.md')}",
        f"- OpenAI batching: {_repo_path('docs/design_docs/OPENAI_BATCHING.md')}",
        f"- Storage and runs: {_repo_path('docs/design_docs/STORAGE_AND_RUNS.md')}",
        f"- Smoke tests: {_repo_path('docs/smoke-test.md')}",
        "",
        "Core code map:",
        f"- runtime: {_repo_path('src/batchor/runtime')}",
        f"- providers: {_repo_path('src/batchor/providers')}",
        f"- storage: {_repo_path('src/batchor/storage')}",
        f"- sources: {_repo_path('src/batchor/sources')}",
        f"- artifacts: {_repo_path('src/batchor/artifacts')}",
        f"- cli: {_repo_path('src/batchor/cli.py')}",
        "",
        "Required validation baseline:",
        "- uv run pytest -q",
        "- Add uv run ty check src for provider wiring, storage wiring, token budgeting, or run lifecycle changes.",
    ]
    return "\n".join(lines)


VALIDATION_GUIDES = {
    "default": [
        "Use after refactors or behavior-changing edits.",
        "Commands:",
        "- uv run pytest -q",
    ],
    "runtime": [
        "Use when changing run lifecycle, orchestration, retries, artifact flow, or provider polling behavior.",
        "Commands:",
        "- uv run ty check src",
        "- uv run pytest -q",
        "- uv run pytest tests/integration/test_batchor_runner.py --no-cov -q",
    ],
    "provider": [
        "Use when changing OpenAI request shaping, batching, file upload/download handling, or provider normalization.",
        "Commands:",
        "- uv run ty check src",
        "- uv run pytest -q",
        "- uv run pytest tests/unit/test_batchor_openai_provider.py tests/unit/test_batchor_tokens.py --no-cov -q",
    ],
    "storage": [
        "Use when changing SQLite/Postgres state handling, lifecycle persistence, or storage contracts.",
        "Commands:",
        "- uv run ty check src",
        "- uv run pytest -q",
        "- uv run pytest tests/unit/test_batchor_storage.py tests/unit/test_batchor_storage_contracts.py tests/unit/test_batchor_sqlite_storage_flow.py --no-cov -q",
    ],
    "docs": [
        "Use when editing docs, navigation, or API docstrings.",
        "Commands:",
        "- uv run pytest -q",
        "- uv run mkdocs build --strict",
    ],
    "cli": [
        "Use when changing the operator CLI surface or .env loading behavior.",
        "Commands:",
        "- uv run pytest -q",
        "- uv run pytest tests/unit/test_batchor_cli.py --no-cov -q",
    ],
}


DOC_TOPICS = {
    "all": [
        ("README", "README.md", "Public scope, quickstart, and mental model."),
        ("Architecture", "docs/design_docs/ARCHITECTURE.md", "Package layout and runtime boundaries."),
        ("OpenAI batching", "docs/design_docs/OPENAI_BATCHING.md", "Provider request construction and enqueue policy."),
        (
            "Storage and runs",
            "docs/design_docs/STORAGE_AND_RUNS.md",
            "Durable state, run lifecycle, export, and prune behavior.",
        ),
        ("Smoke tests", "docs/smoke-test.md", "Required and targeted validation commands."),
    ],
    "runtime": [
        ("Architecture", "docs/design_docs/ARCHITECTURE.md", "Runtime boundaries and invariants."),
        ("Storage and runs", "docs/design_docs/STORAGE_AND_RUNS.md", "Run lifecycle and durable state semantics."),
        ("Smoke tests", "docs/smoke-test.md", "Targeted runtime validation."),
    ],
    "provider": [
        (
            "OpenAI batching",
            "docs/design_docs/OPENAI_BATCHING.md",
            "Batch shaping, token budgeting, and provider behavior.",
        ),
        ("Architecture", "docs/design_docs/ARCHITECTURE.md", "Provider layer boundary."),
        ("Smoke tests", "docs/smoke-test.md", "Targeted provider validation."),
    ],
    "storage": [
        ("Storage and runs", "docs/design_docs/STORAGE_AND_RUNS.md", "Storage model, checkpoints, and retention."),
        ("Architecture", "docs/design_docs/ARCHITECTURE.md", "Storage boundary."),
        ("Smoke tests", "docs/smoke-test.md", "Targeted storage validation."),
    ],
}


def list_tools() -> list[dict[str, Any]]:
    return [
        {
            "name": "batchor_project_guide",
            "description": "Return the batchor repo summary, key file paths, and baseline validation commands.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
        },
        {
            "name": "batchor_validation_guide",
            "description": "Return the recommended validation commands for a specific change area.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "area": {
                        "type": "string",
                        "enum": sorted(VALIDATION_GUIDES),
                        "description": "Change area to validate.",
                    }
                },
                "additionalProperties": False,
            },
        },
        {
            "name": "batchor_docs_index",
            "description": "Return the main docs to load for a given topic.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "topic": {
                        "type": "string",
                        "enum": sorted(DOC_TOPICS),
                        "description": "Documentation topic to index.",
                    }
                },
                "additionalProperties": False,
            },
        },
    ]


def _docs_index(topic: str) -> str:
    selected = DOC_TOPICS.get(topic, DOC_TOPICS["all"])
    lines = [f"Documentation index for topic: {topic}"]
    for title, relative_path, summary in selected:
        lines.append(f"- {title}: {_repo_path(relative_path)}")
        lines.append(f"  {summary}")
    return "\n".join(lines)


def _validation_guide(area: str) -> str:
    selected = VALIDATION_GUIDES.get(area, VALIDATION_GUIDES["default"])
    return "\n".join([f"Validation profile: {area}", *selected])


def call_tool(name: str, arguments: dict[str, Any] | None) -> dict[str, Any]:
    args = arguments or {}
    if name == "batchor_project_guide":
        text = _project_guide()
    elif name == "batchor_validation_guide":
        text = _validation_guide(str(args.get("area", "default")).lower())
    elif name == "batchor_docs_index":
        text = _docs_index(str(args.get("topic", "all")).lower())
    else:
        return {
            "content": [
                {
                    "type": "text",
                    "text": f"Unknown tool: {name}",
                }
            ],
            "isError": True,
        }

    return {"content": [{"type": "text", "text": text}]}


def _success_response(message_id: Any, result: dict[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": message_id, "result": result}


def _error_response(message_id: Any, code: int, message: str) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": message_id,
        "error": {"code": code, "message": message},
    }


def handle_request(request: dict[str, Any]) -> dict[str, Any] | None:
    method = request.get("method")
    params = request.get("params", {})
    message_id = request.get("id")

    if method == "notifications/initialized":
        return None
    if method == "ping":
        return _success_response(message_id, {})
    if method == "initialize":
        protocol_version = params.get("protocolVersion") or DEFAULT_PROTOCOL_VERSION
        return _success_response(
            message_id,
            {
                "protocolVersion": protocol_version,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
            },
        )
    if method == "tools/list":
        return _success_response(message_id, {"tools": list_tools()})
    if method == "tools/call":
        tool_name = params.get("name")
        arguments = params.get("arguments")
        return _success_response(message_id, call_tool(str(tool_name), arguments))
    return _error_response(message_id, -32601, f"Method not found: {method}")


def _read_message() -> dict[str, Any] | None:
    content_length: int | None = None
    while True:
        header = sys.stdin.buffer.readline()
        if not header:
            return None
        if header in (b"\r\n", b"\n"):
            break
        name, _, value = header.decode("utf-8").partition(":")
        if name.lower() == "content-length":
            content_length = int(value.strip())
    if content_length is None:
        raise ValueError("Missing Content-Length header")
    payload = sys.stdin.buffer.read(content_length)
    if not payload:
        return None
    return json.loads(payload.decode("utf-8"))


def _write_message(message: dict[str, Any]) -> None:
    body = json.dumps(message, separators=(",", ":")).encode("utf-8")
    sys.stdout.buffer.write(f"Content-Length: {len(body)}\r\n\r\n".encode("ascii"))
    sys.stdout.buffer.write(body)
    sys.stdout.buffer.flush()


def main() -> int:
    while True:
        try:
            request = _read_message()
        except Exception as exc:  # pragma: no cover - defensive stdio loop
            _write_message(_error_response(None, -32700, f"Parse error: {exc}"))
            continue
        if request is None:
            return 0
        response = handle_request(request)
        if response is not None:
            _write_message(response)


if __name__ == "__main__":
    raise SystemExit(main())
