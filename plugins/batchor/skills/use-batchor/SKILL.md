---
name: use-batchor
description: Turn datasets, prompts, or existing Python data pipelines into durable OpenAI, Anthropic, or Gemini Batch workflows with Batchor. Use when a researcher or downstream project needs to process CSV, JSONL, Parquet, or application records with resumable LLM batches, typed outputs, result export, or run operations; also use to diagnose or improve an existing Batchor integration. Do not use for contributing to the Batchor library itself.
---

# Use Batchor

Build the smallest reliable Batchor workflow that fits the user's data and operating environment. Treat Batchor as the durable batch-execution step; keep dataset discovery, research logic, and downstream analysis in the user's project.

## Start with the user's outcome

1. Inspect the project and a small sample of the input schema when available.
2. Identify or safely infer:
   - provider: OpenAI, Anthropic, Gemini Developer API, or Vertex AI
   - input: CSV, JSONL, Parquet, or application records
   - stable item identifier and prompt fields
   - plain text versus structured Pydantic output
   - one-machine versus shared-worker execution
   - expected result destination and artifact-retention needs
3. Explain any assumption that affects cost, data handling, or resume behavior.
4. Do not submit a real provider batch unless the user explicitly asks to run it. Building and testing the integration does not authorize spending provider credits or uploading research data.

If the Batchor MCP tools are available, call `batchor_choose_workflow` before implementation and `batchor_starter` when a concrete starter is useful.

## Choose the surface

- Use the CLI for a direct CSV or JSONL job driven by one prompt field or template. Read [CLI workflows](references/cli-workflows.md).
- Use the Python API for structured output, Parquet, application records, pipeline integration, custom storage, or programmatic run control. Read [Python pipelines](references/python-pipelines.md).
- Use SQLite durability for one machine. Use Postgres plus a shared artifact root for workers on multiple machines.
- Use `BatchRunner(storage="memory")` only for disposable examples and tests.

## Implement safely

1. Add `batchor` to the project's normal dependency manager. Use `batchor[anthropic]` for Anthropic or `batchor[gemini]` for Gemini.
2. Keep credentials in the project's existing secret mechanism. Python callers do not get automatic `.env` loading from Batchor.
3. Preserve a stable `run_id` outside the process so a fresh process can call `get_run()` or resume `start(..., run_id=...)`.
4. Prefer `CsvItemSource`, `JsonlItemSource`, `ParquetItemSource`, or another checkpointed source when restart-safe ingestion matters.
5. Keep structured-output Pydantic models importable at module scope so durable rehydration can resolve them.
6. Keep Batchor behind one project-owned adapter or pipeline step instead of scattering calls through domain code.
7. Make result export and artifact retention explicit. Never prune raw output artifacts before the required export.

## Verify without spending money

- Test prompt construction, stable IDs, output validation, and downstream persistence locally.
- Use a fake or mocked provider in automated tests; do not make live provider calls.
- Run the project's normal tests and type checks.
- For CLI integrations, verify `batchor --help` and construct the command before offering to execute it.
- For Python integrations, verify imports and exercise the adapter with test records and a fake provider.

## Guardrails

- Never hardcode API keys or print secrets.
- Never upload a full dataset merely to inspect its schema.
- Do not silently choose ephemeral storage for a workflow described as resumable.
- Do not use arbitrary generators or live cursors as though they support durable ingestion checkpoints.
- Do not invent a new workflow engine around Batchor. Let the user's pipeline own scheduling and downstream side effects.
- Flag Batchor's current PolyForm Noncommercial license when the intended use may be commercial.
