---
name: batchor-dev
description: Use when working in the batchor repository to implement or review library, CLI, runtime, provider, storage, source, or documentation changes. Follow AGENTS.md smoke-test rules, prefer the relevant design docs before editing, and keep README plus design docs aligned when behavior changes.
---

# Batchor Dev

Use this skill for normal contributor work in this repository.

## Start here

1. Read `AGENTS.md` for required validation and doc-maintenance rules.
2. Read `README.md` for the public mental model and current scope.
3. Load only the design docs that match the task:
   - `docs/design_docs/ARCHITECTURE.md` for package layout and runtime boundaries
   - `docs/design_docs/OPENAI_BATCHING.md` for provider-specific batching behavior
   - `docs/design_docs/STORAGE_AND_RUNS.md` for durable state, run lifecycle, and retention
   - `docs/smoke-test.md` for the full validation matrix

## Repo map

- `src/batchor/runtime/`: orchestration, run handles, validation, retry, token budgeting
- `src/batchor/providers/`: provider interfaces and OpenAI Batch implementation
- `src/batchor/storage/`: SQLite, Postgres, and in-memory durability backends
- `src/batchor/sources/`: file-backed checkpointable item sources
- `src/batchor/artifacts/`: durable request/output artifact handling
- `src/batchor/cli.py`: operator CLI surface
- `tests/unit/` and `tests/integration/`: main regression coverage

## Validation rules

- Required smoke path after refactors or behavior-changing edits: `uv run pytest -q`
- Also run `uv run ty check src` when changing provider wiring, storage wiring, token budgeting, or run lifecycle behavior.
- Do not weaken the `85%` coverage gate or disable coverage on the main smoke path.
- If docs or navigation change, also run `uv run mkdocs build --strict`.

## Working style

- Keep changes library-first. New orchestration behavior should usually land in the runtime or provider layers before the CLI.
- Preserve durable-run invariants. Resume, replay, artifact retention, and terminal-result behavior are core repo constraints.
- When behavior changes, update `README.md` and the relevant docs in `docs/design_docs/` in the same change.
