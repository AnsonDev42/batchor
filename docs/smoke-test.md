# Smoke Test Guide

This guide defines the minimum validation bar for `batchor`.

## Goals

- Catch obvious regressions in the package API.
- Verify durable run handling and SQLite persistence still work.
- Verify OpenAI-specific batching logic through fake-provider integration tests.

## Prerequisites

- Dependencies installed:

```bash
uv sync --all-groups
```

If you are working from the monorepo, run commands from `batchor/`. If you are working from the extracted repo, run them from the repo root.

## Level 1: Fast Smoke

```bash
uv run ty check src
uv run pytest -q
```

Expected:

- exit code `0`
- type checks pass
- coverage gate passes

## Level 2: Targeted Runtime Smoke

Use this when touching provider/storage/runtime wiring:

```bash
uv run pytest tests/unit/test_batchor_tokens.py tests/unit/test_batchor_sqlite_storage_flow.py tests/unit/test_batchor_validation.py --no-cov -q
uv run pytest tests/integration/test_batchor_runner.py --no-cov -q
```

Expected:

- durable `Run` lifecycle still works
- OpenAI request splitting and enqueue-limit logic still behave as expected
- structured-output parsing remains stable

## Live OpenAI Smoke

`TBD`

Current package tests rely on fake providers. When the extracted repo grows a stable live-API smoke harness, document it here with cost controls and required environment variables.
