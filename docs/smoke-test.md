# Smoke Test Guide

This guide defines the minimum validation bar for `batchor`.

## Goals

- Catch obvious regressions in the package API.
- Verify durable run handling and SQLite persistence still work.
- Verify artifact-store wiring still supports replay, export, and prune.
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
uv build
```

Expected:

- exit code `0`
- type checks pass
- coverage gate passes at `85%` or higher
- pytest runs in parallel through the default project config
- sdist and wheel both build successfully

GitHub pull request CI runs this default smoke across Python `3.12` and `3.13`. The build step runs in a dedicated Python `3.13` job so packaging is still verified without duplicating build work across the full matrix.

## Level 2: Targeted Runtime Smoke

Use this when touching provider/storage/runtime wiring:

```bash
uv run pytest tests/unit/test_batchor_tokens.py tests/unit/test_batchor_sqlite_storage_flow.py tests/unit/test_batchor_validation.py --no-cov -q
uv run pytest tests/unit/test_batchor_artifacts.py tests/unit/test_batchor_storage_contracts.py --no-cov -q
uv run pytest tests/integration/test_batchor_runner.py --no-cov -q
```

Expected:

- durable `Run` lifecycle still works
- subprocess crash + resume can recover `queued_local` work and replay persisted request artifacts
- file-backed ingestion can resume from a persisted checkpoint when rerun with the same `run_id`
- retry/resume from persisted request artifacts still works for SQLite-backed runs
- raw output/error artifacts can be exported and require export before raw pruning
- terminal runs, including `completed_with_failures`, can prune request artifacts without losing persisted results
- shared storage-contract behavior remains aligned across SQLite and opt-in Postgres
- OpenAI request splitting and enqueue-limit logic still behave as expected
- structured-output parsing remains stable

Notes:

- The default pytest configuration includes `-n auto` and the `85%` coverage gate.
- `--no-cov` is only for supplemental targeted runs after the main smoke test already passed.
- Postgres contract tests run in required GitHub Actions CI through an ephemeral PostgreSQL service.
- Local Postgres contract tests still require `BATCHOR_TEST_POSTGRES_DSN` to be set.

## CLI Smoke

The CLI auto-loads a local `.env` for operator usage. The Python library does not.

Local example:

```bash
echo "OPENAI_API_KEY=sk-..." > .env
batchor start --input input/items.jsonl --id-field id --prompt-field text --model gpt-4.1
```

This is not part of default CI. Prefer fake-provider tests for automated coverage and run live CLI smoke manually when changing CLI/provider wiring.

## Live OpenAI Smoke

Manual only. Recommended local flow:

```bash
export OPENAI_API_KEY=sk-...
export BATCHOR_RUN_LIVE_TESTS=1
export BATCHOR_LIVE_OPENAI_MODEL=gpt-5-nano
uv run pytest tests/integration/test_batchor_live_openai.py --no-cov -q
```

Behavior:

- runs one single-item text job against the real OpenAI Batch API
- uses SQLite durability and the normal `BatchRunner` flow
- defaults to `gpt-5-nano` unless `BATCHOR_LIVE_OPENAI_MODEL` is set
- sends no reasoning field unless `BATCHOR_LIVE_OPENAI_REASONING_EFFORT` is set
- loads `.env` when present through the test harness for local use
- is skipped unless `BATCHOR_RUN_LIVE_TESTS=1`
- requires an OpenAI account with Batch API access and available billing quota

Cost controls:

- one item only
- text output only
- manual/local only
- not part of default CI or release automation
