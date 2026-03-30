# batchor Docs

This folder is the package-local documentation set for `batchor`.

Some documents describe current behavior. Others intentionally mark extension work as `TBD` so the extracted repository has a clear design backlog instead of undocumented gaps.

## Doc Map

| Doc | Purpose |
| --- | --- |
| `smoke-test.md` | Fast validation flow for local development and extraction readiness. |
| `design_docs/ARCHITECTURE.md` | High-level package structure, runtime boundaries, and public API shape. |
| `design_docs/OPENAI_BATCHING.md` | OpenAI Batch request construction, token estimation, enqueue-limit handling, and splitting semantics. |
| `design_docs/STORAGE_AND_RUNS.md` | Durable `Run` model, SQLite persistence, and storage abstraction surface. |
| `design_docs/ROADMAP.md` | Planned work and intentionally unimplemented areas. |
| `plans/README.md` | Index of forward-looking implementation plans for future agents. |
| `plans/DURABLE_EXECUTION_BOUNDARY.md` | Proposed boundary between `batchor`, SQLite, and user pipelines for durable retry/resume. |

## Scope

Today `batchor` is OpenAI-first and SQLite-first:

- provider abstraction exists, but only OpenAI ships today
- storage abstraction exists, but only SQLite and in-memory storage ship today
- structured output is Pydantic v2-based
- file-backed inputs currently support CSV and JSONL

Anything outside that scope should be documented as `TBD` rather than implied as supported.
