#!/usr/bin/env python3
"""Repo-independent MCP guidance for people integrating Batchor."""

from __future__ import annotations

import json
import sys
from typing import Any

SERVER_NAME = "batchorUser"
SERVER_VERSION = "0.1.0"
DEFAULT_PROTOCOL_VERSION = "2025-03-26"


def list_tools() -> list[dict[str, Any]]:
    return [
        {
            "name": "batchor_choose_workflow",
            "description": "Choose the smallest reliable Batchor workflow for a dataset or pipeline.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "input_kind": {
                        "type": "string",
                        "enum": ["csv", "jsonl", "parquet", "python_records"],
                    },
                    "structured_output": {"type": "boolean"},
                    "shared_workers": {"type": "boolean"},
                    "provider": {
                        "type": "string",
                        "enum": ["openai", "anthropic", "gemini", "vertex"],
                    },
                },
                "required": ["input_kind", "structured_output", "shared_workers", "provider"],
                "additionalProperties": False,
            },
        },
        {
            "name": "batchor_starter",
            "description": "Return a safe CLI or Python starter for a Batchor workflow without submitting it.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "surface": {"type": "string", "enum": ["cli", "python"]},
                    "provider": {"type": "string", "enum": ["openai", "anthropic", "gemini", "vertex"]},
                    "model": {"type": "string"},
                    "id_field": {"type": "string"},
                    "prompt_field": {"type": "string"},
                },
                "required": ["surface", "provider", "model", "id_field", "prompt_field"],
                "additionalProperties": False,
            },
        },
        {
            "name": "batchor_safety_checklist",
            "description": "Return preflight checks for cost, credentials, data handling, resume, and retention.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
        },
    ]


def _choose_workflow(args: dict[str, Any]) -> str:
    input_kind = str(args["input_kind"])
    structured = bool(args["structured_output"])
    shared = bool(args["shared_workers"])
    provider = str(args["provider"])
    use_cli = input_kind in {"csv", "jsonl"} and not structured and not shared
    surface = "CLI" if use_cli else "Python API"
    install = {
        "anthropic": 'python -m pip install "batchor[anthropic]"',
        "gemini": 'python -m pip install "batchor[gemini]"',
        "vertex": 'python -m pip install "batchor[gemini]"',
    }.get(provider, "python -m pip install batchor")
    storage = "Postgres plus an artifact root shared by all workers" if shared else "default SQLite durability"
    source = {
        "csv": "CsvItemSource",
        "jsonl": "JsonlItemSource",
        "parquet": "ParquetItemSource",
        "python_records": "stable BatchItem values or a custom checkpointed source",
    }[input_kind]
    return "\n".join(
        [
            f"Recommended surface: {surface}",
            f"Install: {install}",
            f"Input adapter: {source}",
            f"Storage: {storage}",
            "Keep a stable run_id outside the process so a later process can resume or rehydrate the run.",
            "Do not submit real data until the user confirms provider, estimated scope, credentials, and retention policy.",
        ]
    )


def _starter(args: dict[str, Any]) -> str:
    surface = str(args["surface"])
    provider = str(args["provider"])
    model = str(args["model"])
    id_field = str(args["id_field"])
    prompt_field = str(args["prompt_field"])
    if surface == "cli":
        backend = "vertex" if provider == "vertex" else "developer"
        if provider == "anthropic":
            provider_flag = " --provider anthropic --anthropic-max-tokens 1024"
        elif provider in {"gemini", "vertex"}:
            provider_flag = f" --provider gemini --gemini-backend {backend}"
        else:
            provider_flag = ""
        return (
            "Inspect `batchor start --help` for the installed version, then adapt this without running it:\n\n"
            "batchor start \\\n"
            "  --input input/items.jsonl \\\n"
            f"  --id-field {id_field} \\\n"
            f"  --prompt-field {prompt_field} \\\n"
            f"  --model {model}{provider_flag}\n"
        )
    if provider == "anthropic":
        config = "AnthropicProviderConfig"
        install = '"batchor[anthropic]"'
        provider_args = f'model="{model}", max_tokens=1024'
    elif provider in {"gemini", "vertex"}:
        config = "GeminiProviderConfig"
        install = '"batchor[gemini]"'
        provider_args = f'model="{model}", vertexai=True' if provider == "vertex" else f'model="{model}"'
    else:
        config = "OpenAIProviderConfig"
        install = "batchor"
        provider_args = f'model="{model}"'
    return f'''Install {install} with the project's dependency manager, then adapt this module:\n\nfrom batchor import BatchItem, BatchJob, BatchRunner, {config}, PromptParts\n\n\ndef start_job(records: list[dict[str, str]], run_id: str):\n    runner = BatchRunner()\n    job = BatchJob(\n        items=[\n            BatchItem(item_id=row["{id_field}"], payload=row)\n            for row in records\n        ],\n        build_prompt=lambda item: PromptParts(prompt=item.payload["{prompt_field}"]),\n        provider_config={config}({provider_args}),\n    )\n    return runner.start(job, run_id=run_id)\n\nDo not call this against real records until the user approves the upload and cost.\n'''


def _safety_checklist() -> str:
    return "\n".join(
        [
            "Batchor preflight:",
            "- Confirm provider, model, input row count, and which fields leave the machine.",
            "- Keep credentials in the project's secret mechanism; never hardcode or print them.",
            "- Use stable item IDs and persist a stable run ID.",
            "- Use SQLite for one machine; use Postgres plus shared artifacts for multiple machines.",
            "- Use deterministic file/checkpointed sources when restart-safe ingestion matters.",
            "- Keep structured-output models importable at module scope.",
            "- Test with fake providers; do not spend credits during implementation tests.",
            "- Decide export and retention policy before pruning artifacts.",
            "- Check the PolyForm Noncommercial license if the use may be commercial.",
        ]
    )


def call_tool(name: str, arguments: dict[str, Any] | None) -> dict[str, Any]:
    args = arguments or {}
    try:
        if name == "batchor_choose_workflow":
            result = _choose_workflow(args)
        elif name == "batchor_starter":
            result = _starter(args)
        elif name == "batchor_safety_checklist":
            result = _safety_checklist()
        else:
            return {"content": [{"type": "text", "text": f"Unknown tool: {name}"}], "isError": True}
    except (KeyError, TypeError, ValueError) as exc:
        return {"content": [{"type": "text", "text": f"Invalid arguments: {exc}"}], "isError": True}
    return {"content": [{"type": "text", "text": result}]}


def _success(message_id: Any, result: dict[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": message_id, "result": result}


def handle_request(request: dict[str, Any]) -> dict[str, Any] | None:
    method = request.get("method")
    params = request.get("params", {})
    message_id = request.get("id")
    if method == "notifications/initialized":
        return None
    if method == "ping":
        return _success(message_id, {})
    if method == "initialize":
        protocol = params.get("protocolVersion") or DEFAULT_PROTOCOL_VERSION
        return _success(
            message_id,
            {
                "protocolVersion": protocol,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
            },
        )
    if method == "tools/list":
        return _success(message_id, {"tools": list_tools()})
    if method == "tools/call":
        return _success(message_id, call_tool(str(params.get("name")), params.get("arguments")))
    return {
        "jsonrpc": "2.0",
        "id": message_id,
        "error": {"code": -32601, "message": f"Method not found: {method}"},
    }


def main() -> None:
    for line in sys.stdin:
        if not line.strip():
            continue
        try:
            request = json.loads(line)
            response = handle_request(request)
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            response = {
                "jsonrpc": "2.0",
                "id": None,
                "error": {"code": -32700, "message": f"Parse error: {exc}"},
            }
        if response is not None:
            print(json.dumps(response), flush=True)


if __name__ == "__main__":
    main()
