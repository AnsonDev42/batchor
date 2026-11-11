# Smoke Test Guide

This guide defines the minimum validation bar for `batchor`.

## Goals

- catch obvious regressions in the package API
- verify durable run handling and SQLite persistence still work
- verify artifact-store wiring still supports replay, export, and prune
- verify OpenAI-specific batching logic through fake-provider integration tests
- verify Anthropic Message Batches wiring through fake-client and opt-in live tests
- verify Gemini provider wiring through fake-client integration tests
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
- collects this package's `tests/` directory by default, so local reference checkouts are not treated as part of the smoke suite
- enforces strict pytest config/marker handling
- enforces the `85%` coverage gate

## Recommended repo-level validation

In practice, the project expects more than just pytest for a clean change:

```bash
uv run ty check src
uv run ruff check src tests
uv run ruff format --check src tests
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
- repeated `--input` CLI flows and `CompositeItemSource` resume semantics remain covered when those paths change

Before publication, inspect the built source archive as well as the wheel.
Private `handoff/` logs and local run databases must not be shipped; the sdist
explicitly excludes `handoff/` because local Git exclusions alone do not govern
Hatchling's file selection. Check that packaged source matches the release
commit and that the version in both package metadata files matches the tag.

When agent skills, plugins, or MCP helpers change, also run:

```bash
uv run pytest tests/unit/test_agent_tooling.py --no-cov -q
uv run python ~/.codex/skills/.system/skill-creator/scripts/quick_validate.py plugins/batchor/skills/use-batchor
uv run python ~/.codex/skills/.system/plugin-creator/scripts/validate_plugin.py plugins/batchor
```

The user-facing `batchor` plugin must remain repo-independent. The contributor-only `batchor-agent-tools` plugin may point at checkout paths and repository validation commands.

GitHub pull request CI runs the main smoke across Python `3.12`, `3.13`, and `3.14`, builds the docs site in a dedicated docs job, and runs packaging in a dedicated Python `3.13` build job so each validation path stays explicit.

## Targeted runtime smoke

Use this when touching provider wiring, storage wiring, token budgeting, or run lifecycle behavior:

```bash
uv run ty check src
uv run pytest tests/unit/test_batchor_tokens.py tests/unit/test_batchor_sqlite_storage_flow.py tests/unit/test_batchor_validation.py --no-cov -q
uv run pytest tests/unit/test_batchor_artifacts.py tests/unit/test_batchor_storage_contracts.py --no-cov -q
uv run pytest tests/unit/test_batchor_runtime_ingestion.py --no-cov -q
uv run pytest tests/integration/test_batchor_runner.py --no-cov -q
uv run pytest tests/unit/test_batchor_gemini_provider.py tests/integration/test_batchor_gemini_runner.py --no-cov -q
uv run pytest tests/unit/test_batchor_anthropic_provider.py --no-cov -q
```

Expected:

- durable `Run` lifecycle still works
- subprocess crash + resume can recover `queued_local` work and replay persisted request artifacts
- replaying multiple items from the same persisted request artifact does not require rereading that artifact file for each item
- deterministic source ingestion can resume from a persisted checkpoint when rerun with the same `run_id`
- incomplete checkpointed ingestion cannot become terminal, and fresh-process advancement requires reattaching the source with `start(job, run_id=...)`
- generic ingestion markers keep empty and partially materializing arbitrary-iterable runs non-terminal
- schema upgrade conservatively backfills missing empty and partially materialized ingestion markers
- composite deterministic sources can namespace duplicate row IDs across explicit inputs and resume across source boundaries
- Parquet source adapters can resume from opaque checkpoints and project only required columns
- retry/resume from persisted request artifacts still works for SQLite-backed runs
- resumed runs reconcile existing active batches and persisted backoff before materializing more source rows
- long-running ingestion polls active batches on a monotonic cadence at durable chunk boundaries, before submission, so completed remote work frees local enqueue-token capacity before full source materialization
- slow ingestion flushes bounded partial work slices while fast ingestion retains the normal 1,000-item batch granularity
- submission backoff blocks new materialization/submission without suppressing active-batch reconciliation
- poll-induced pause, cancellation, or retry backoff leaves checkpointed ingestion incomplete at its last durable chunk and prevents unsafe submission
- transient batch-poll failures do not block unrelated pending submissions from being sent when capacity remains
- paused runs stop polling/submission until resumed
- manual pause stops a non-checkpointed iterator and same-process resume continues it exactly once
- OpenAI control-plane and batch-level insufficient-quota provider failures auto-pause with a durable `control_reason` and do not consume item attempts
- OpenAI row-level insufficient-quota records in completed batch output remain retryable, do not consume attempts, and back off without pausing the run
- quota auto-pause preserves `cancel_requested` and does not strand non-checkpointed input rows during initial ingestion
- cancelled runs drain already-submitted work and mark remaining local items as `run_cancelled`
- cancelling incomplete checkpointed ingestion finalizes the checkpoint and reaches a terminal state without materializing the source tail
- incremental terminal-result reads/exports remain sequence-based and idempotent across repeated calls
- raw output/error artifacts can be exported and require export before raw pruning
- raw output/error artifact persistence can be disabled without breaking parsed terminal results or request-artifact replay
- terminal runs, including `completed_with_failures`, can prune request artifacts without losing persisted results
- shared storage-contract behavior remains aligned across SQLite and opt-in Postgres
- completed submitted items report consumed attempts consistently across storage backends
- OpenAI request splitting and enqueue-limit logic still behave as expected
- concurrent runs atomically share OpenAI capacity by quota scope, and terminal replay releases reservations idempotently
- pre-intent active batches continue consuming shared capacity after schema upgrade
- terminal batches replay after download/parse/partial-state crashes without duplicating consumed attempts
- ambiguous provider creation requires explicit created/not-created resolution and never silently submits a duplicate
- Gemini text-only request construction, batch polling normalization, response parsing, and structured-output validation still behave as expected
- Anthropic request construction, safe correlation IDs, polling normalization, result parsing, and structured-output validation still behave as expected
- wait-mode refresh cycles keep draining immediately after poll/submission progress, without introducing idle poll sleeps while more local work can be sent
- wait deadlines cap sleeps and prevent new ingestion/poll/download work after expiry, subject to already-running callback/SDK operation timeouts
- structured-output parsing remains stable

Notes:

- the default pytest configuration already includes `-n auto` and the `85%` coverage gate
- `--no-cov` is only for supplemental targeted runs after the main smoke test already passed
- Postgres contract tests run in required GitHub Actions CI through an ephemeral PostgreSQL service
- local Postgres contract tests still require `BATCHOR_TEST_POSTGRES_DSN` to be set

## Review regression coverage

The standard smoke includes regressions for:

- rejected resume preserving the existing context and validating changed
  composite sources before polling
- prompt/storage failures preserving uncommitted arbitrary-iterator rows,
  source exceptions staying incomplete, and releasing fully ingested jobs
- checkpoint deadlines preserving partial slices, and submission deadlines
  preventing another remote create while finalizing a confirmed create
- pre-create failures releasing claims/reservations and capacity waits not
  accumulating artifacts
- file-size rejection before upload and provider row-error/null-error parsing
- OpenAI creation using one HTTP attempt under the real installed SDK with a
  local HTTP mock, Anthropic disabling create retries, and Anthropic separating
  status polling from result download
- rejected raw prune preserving pointers/files, atomic replacement preserving
  old data on encoding failure, and symlink/traversal rejection, including
  preflight of generated result/manifest export files before any export writes
- irreversible cancellation, including a pause after cancel
- concurrent SQLite claims and distinct terminal-result cursors in both SQL
  backends, plus rejection of newer schema versions

Run the Postgres contract suite against an isolated test database via
`BATCHOR_TEST_POSTGRES_DSN`; it creates and removes test-only schemas. Do not
point the contract suite at a production database. Fake-provider tests do not
validate live SDK/API behavior or provider billing. See the
[review record](design_docs/CODE_REVIEW_2026_11.md) for this change's exact
validation outcomes and remaining boundaries.

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
batchor start --input input/items-a.jsonl --input input/items-b.jsonl --id-field id --prompt-field text --model gpt-4.1
```

This is not part of default CI. Prefer fake-provider tests for automated coverage and run live CLI smoke manually when changing CLI/provider wiring.

## Live OpenAI smoke

Manual only. Recommended local flow:

```bash
export BATCHOR_RUN_LIVE_TESTS=1
export BATCHOR_LIVE_OPENAI_MODEL=gpt-5-nano
uv run pytest tests/integration/test_batchor_live_openai.py --no-cov -q
```

Minimum single-item live smoke:

```bash
export BATCHOR_RUN_LIVE_TESTS=1
uv run pytest tests/integration/test_batchor_live_openai.py -k text_job_smoke --no-cov -q
```

Behavior:

- runs a single-item text smoke and a two-CSV composition smoke against the real OpenAI Batch API
- uses SQLite durability and the normal `BatchRunner` flow
- defaults to `gpt-5-nano` unless `BATCHOR_LIVE_OPENAI_MODEL` is set
- sends no reasoning field unless `BATCHOR_LIVE_OPENAI_REASONING_EFFORT` is set
- loads `.env` when present through the test harness for local use, so `OPENAI_API_KEY` may come from the shell or `.env`
- is skipped unless `BATCHOR_RUN_LIVE_TESTS=1`
- requires an OpenAI account with Batch API access and available billing quota

## Live Gemini smoke

Manual only. This runs three one-item jobs through the normal SQLite-backed runtime: Developer API inline, Developer Files API, and Vertex GCS. Developer files and temporary GCS objects are removed afterward:

```bash
export GOOGLE_CLOUD_PROJECT="my-project"
export GOOGLE_CLOUD_LOCATION="europe-west8"
export GOOGLE_GENAI_USE_VERTEXAI="true"
export BATCHOR_LIVE_GEMINI_GCS_URI="gs://my-bucket/batchor-live"
export BATCHOR_RUN_LIVE_GEMINI=1
uv run --extra gemini pytest tests/integration/test_batchor_live_gemini.py --no-cov -q
```

The root `.env` or shell must also provide `GEMINI_API_KEY` with available Developer API billing/prepayment credits. The bucket must be writable by the active Application Default Credentials and should be in the same region as the Vertex job. The tests default to `gemini-2.5-flash` and a 30-minute timeout; override them with `BATCHOR_LIVE_GEMINI_DEVELOPER_MODEL`, `BATCHOR_LIVE_GEMINI_MODEL`, and `BATCHOR_LIVE_GEMINI_TIMEOUT_SEC`.

Cost controls:

- three total items across the Gemini live transports
- text output only
- manual/local only
- not part of default CI or release automation

## Live Anthropic smoke

Manual only. This submits one minimal text request through the normal SQLite-backed runtime:

```bash
export BATCHOR_RUN_LIVE_ANTHROPIC=1
uv run --extra anthropic pytest tests/integration/test_batchor_live_anthropic.py --no-cov -q
```

The root `.env` or shell must provide `ANTHROPIC_API_KEY`. The test defaults to `claude-haiku-4-5`, 64 output tokens, and a 15-minute timeout. Override these with `BATCHOR_LIVE_ANTHROPIC_MODEL` and `BATCHOR_LIVE_ANTHROPIC_TIMEOUT_SEC`.
