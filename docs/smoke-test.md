# Smoke Test Guide

This guide defines the minimum validation bar for `batchor`.

## Goals

- catch obvious regressions in the package API
- verify durable run handling and SQLite persistence still work
- verify artifact-store wiring still supports replay, export, and prune
- verify OpenAI-specific batching logic through fake-provider integration tests
- verify the documentation site still builds cleanly in strict mode

## Prerequisites

Install dependencies:

```bash
uv sync --all-groups
```

If you are working from the monorepo, run commands from `batchor/`. If you are working from the extracted repo, run commands from the repo root.

## Required smoke path

Run this after refactors or behavior-changing code edits:

```bash
uv run pytest -q
```

This path uses the default pytest configuration, which:

- runs in parallel
- enforces strict pytest config/marker handling
- enforces the `85%` coverage gate

## Recommended repo-level validation

In practice, the project expects more than just pytest for a clean change:

```bash
uv run ty check src
uv run pytest -q
uv run mkdocs build --strict
uv build
```

Expected:

- exit code `0`
- type checks pass
- coverage gate passes at `85%` or higher
- MkDocs builds without missing pages or bad internal references
- sdist and wheel both build successfully
- current durable features remain covered, including deterministic source checkpoints, run control, incremental terminal-result APIs, and artifact-retention policy wiring

GitHub pull request CI runs the main smoke across Python `3.12` and `3.13`, builds the docs site in a dedicated docs job, and runs packaging in a dedicated Python `3.13` build job so each validation path stays explicit.

## Targeted runtime smoke

Use this when touching provider wiring, storage wiring, token budgeting, or run lifecycle behavior:

```bash
uv run ty check src
uv run pytest tests/unit/test_batchor_tokens.py tests/unit/test_batchor_sqlite_storage_flow.py tests/unit/test_batchor_validation.py --no-cov -q
uv run pytest tests/unit/test_batchor_artifacts.py tests/unit/test_batchor_storage_contracts.py --no-cov -q
uv run pytest tests/integration/test_batchor_runner.py --no-cov -q
```

Expected:

- durable `Run` lifecycle still works
- subprocess crash + resume can recover `queued_local` work and replay persisted request artifacts
- replaying multiple items from the same persisted request artifact does not require rereading that artifact file for each item
- deterministic source ingestion can resume from a persisted checkpoint when rerun with the same `run_id`
- Parquet source adapters can resume from opaque checkpoints and project only required columns
- retry/resume from persisted request artifacts still works for SQLite-backed runs
- transient batch-poll failures do not block unrelated pending submissions from being sent when capacity remains
- paused runs stop polling/submission until resumed
- cancelled runs drain already-submitted work and mark remaining local items as `run_cancelled`
- incremental terminal-result reads/exports remain sequence-based and idempotent across repeated calls
- raw output/error artifacts can be exported and require export before raw pruning
- raw output/error artifact persistence can be disabled without breaking parsed terminal results or request-artifact replay
- terminal runs, including `completed_with_failures`, can prune request artifacts without losing persisted results
- shared storage-contract behavior remains aligned across SQLite and opt-in Postgres
- OpenAI request splitting and enqueue-limit logic still behave as expected
- structured-output parsing remains stable

Notes:

- the default pytest configuration already includes `-n auto` and the `85%` coverage gate
- `--no-cov` is only for supplemental targeted runs after the main smoke test already passed
- Postgres contract tests run in required GitHub Actions CI through an ephemeral PostgreSQL service
- local Postgres contract tests still require `BATCHOR_TEST_POSTGRES_DSN` to be set

## Docs smoke

If a change touches docs, navigation, or API docstrings, also run:

```bash
uv run mkdocs build --strict
```

This catches:

- broken navigation entries
- unresolved page links
- generated API pages that fail to render

## CLI smoke

The CLI auto-loads a local `.env` for operator usage. The Python library does not.

Local example:

```bash
echo "OPENAI_API_KEY=sk-..." > .env
batchor start --input input/items.jsonl --id-field id --prompt-field text --model gpt-4.1
```

This is not part of default CI. Prefer fake-provider tests for automated coverage and run live CLI smoke manually when changing CLI/provider wiring.

## Live OpenAI smoke

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
