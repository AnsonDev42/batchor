from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_module(relative_path: str, module_name: str):
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / relative_path
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_list_tools_exposes_repo_guides():
    module = _load_module(
        "plugins/batchor-agent-tools/scripts/batchor_repo_mcp.py",
        "batchor_repo_mcp",
    )

    tools = {tool["name"] for tool in module.list_tools()}

    assert tools == {
        "batchor_docs_index",
        "batchor_project_guide",
        "batchor_validation_guide",
    }


def test_validation_guide_for_runtime_mentions_required_checks():
    module = _load_module(
        "plugins/batchor-agent-tools/scripts/batchor_repo_mcp.py",
        "batchor_repo_mcp",
    )

    result = module.call_tool("batchor_validation_guide", {"area": "runtime"})
    text = result["content"][0]["text"]

    assert "uv run ty check src" in text
    assert "uv run pytest -q" in text
    assert "tests/integration/test_batchor_runner.py" in text


def test_project_guide_points_to_repo_docs():
    module = _load_module(
        "plugins/batchor-agent-tools/scripts/batchor_repo_mcp.py",
        "batchor_repo_mcp",
    )

    result = module.call_tool("batchor_project_guide", {})
    text = result["content"][0]["text"]

    assert "AGENTS.md" in text
    assert "docs/design_docs/ARCHITECTURE.md" in text
    assert "docs/design_docs/ANTHROPIC_BATCHING.md" in text
    assert "uv run pytest -q" in text


def test_user_tools_expose_consumer_workflows():
    module = _load_module(
        "plugins/batchor/scripts/batchor_user_mcp.py",
        "batchor_user_mcp",
    )

    tools = {tool["name"] for tool in module.list_tools()}

    assert tools == {
        "batchor_choose_workflow",
        "batchor_safety_checklist",
        "batchor_starter",
    }


def test_user_workflow_chooses_cli_for_simple_jsonl():
    module = _load_module(
        "plugins/batchor/scripts/batchor_user_mcp.py",
        "batchor_user_mcp",
    )

    result = module.call_tool(
        "batchor_choose_workflow",
        {
            "input_kind": "jsonl",
            "structured_output": False,
            "shared_workers": False,
            "provider": "openai",
        },
    )
    text = result["content"][0]["text"]

    assert "Recommended surface: CLI" in text
    assert "default SQLite durability" in text
    assert "stable run_id" in text


def test_user_workflow_chooses_python_for_structured_shared_run():
    module = _load_module(
        "plugins/batchor/scripts/batchor_user_mcp.py",
        "batchor_user_mcp",
    )

    result = module.call_tool(
        "batchor_choose_workflow",
        {
            "input_kind": "parquet",
            "structured_output": True,
            "shared_workers": True,
            "provider": "gemini",
        },
    )
    text = result["content"][0]["text"]

    assert "Recommended surface: Python API" in text
    assert 'pip install "batchor[gemini]"' in text
    assert "Postgres plus an artifact root shared by all workers" in text


def test_user_starter_does_not_claim_to_execute_batch():
    module = _load_module(
        "plugins/batchor/scripts/batchor_user_mcp.py",
        "batchor_user_mcp",
    )

    result = module.call_tool(
        "batchor_starter",
        {
            "surface": "cli",
            "provider": "gemini",
            "model": "gemini-2.5-flash",
            "id_field": "record_id",
            "prompt_field": "abstract",
        },
    )
    text = result["content"][0]["text"]

    assert "without running it" in text
    assert "--provider gemini" in text
    assert "--id-field record_id" in text


def test_user_starter_supports_vertex_python_config():
    module = _load_module(
        "plugins/batchor/scripts/batchor_user_mcp.py",
        "batchor_user_mcp",
    )

    result = module.call_tool(
        "batchor_starter",
        {
            "surface": "python",
            "provider": "vertex",
            "model": "gemini-2.5-flash",
            "id_field": "record_id",
            "prompt_field": "abstract",
        },
    )
    text = result["content"][0]["text"]

    assert "GeminiProviderConfig" in text
    assert "vertexai=True" in text
    assert 'Install "batchor[gemini]"' in text


def test_user_tools_support_anthropic_workflows():
    module = _load_module(
        "plugins/batchor/scripts/batchor_user_mcp.py",
        "batchor_user_mcp",
    )

    workflow = module.call_tool(
        "batchor_choose_workflow",
        {
            "input_kind": "jsonl",
            "structured_output": False,
            "shared_workers": False,
            "provider": "anthropic",
        },
    )["content"][0]["text"]
    cli = module.call_tool(
        "batchor_starter",
        {
            "surface": "cli",
            "provider": "anthropic",
            "model": "claude-haiku-4-5",
            "id_field": "record_id",
            "prompt_field": "abstract",
        },
    )["content"][0]["text"]
    python = module.call_tool(
        "batchor_starter",
        {
            "surface": "python",
            "provider": "anthropic",
            "model": "claude-haiku-4-5",
            "id_field": "record_id",
            "prompt_field": "abstract",
        },
    )["content"][0]["text"]

    assert 'pip install "batchor[anthropic]"' in workflow
    assert "--provider anthropic --anthropic-max-tokens 1024" in cli
    assert "AnthropicProviderConfig" in python
    assert "max_tokens=1024" in python
