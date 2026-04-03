from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_module():
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "plugins" / "batchor-agent-tools" / "scripts" / "batchor_repo_mcp.py"
    spec = importlib.util.spec_from_file_location("batchor_repo_mcp", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_list_tools_exposes_repo_guides():
    module = _load_module()

    tools = {tool["name"] for tool in module.list_tools()}

    assert tools == {
        "batchor_docs_index",
        "batchor_project_guide",
        "batchor_validation_guide",
    }


def test_validation_guide_for_runtime_mentions_required_checks():
    module = _load_module()

    result = module.call_tool("batchor_validation_guide", {"area": "runtime"})
    text = result["content"][0]["text"]

    assert "uv run ty check src" in text
    assert "uv run pytest -q" in text
    assert "tests/integration/test_batchor_runner.py" in text


def test_project_guide_points_to_repo_docs():
    module = _load_module()

    result = module.call_tool("batchor_project_guide", {})
    text = result["content"][0]["text"]

    assert "AGENTS.md" in text
    assert "docs/design_docs/ARCHITECTURE.md" in text
    assert "uv run pytest -q" in text
