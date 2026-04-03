# Docs Map

This page mirrors the package-local documentation map and points to the main design and policy documents that ship with `batchor`.

## Primary docs

| Doc | Purpose |
| --- | --- |
| `smoke-test.md` | Fast validation flow for local development and extraction readiness. |
| `design_docs/BOUNDARY_AND_PHILOSOPHY.md` | Concrete ownership boundary between `batchor`, SQLite/artifacts, and user pipelines. |
| `design_docs/ARCHITECTURE.md` | High-level package structure, runtime boundaries, and public API shape. |
| `design_docs/OPENAI_BATCHING.md` | OpenAI Batch request construction, token estimation, enqueue-limit handling, and splitting semantics. |
| `design_docs/STORAGE_AND_RUNS.md` | Durable `Run` model, SQLite persistence, and storage abstraction surface. |
| `design_docs/STORAGE_MIGRATIONS.md` | Current SQLite schema-versioning and migration guidance. |
| `design_docs/ROADMAP.md` | Planned work and intentionally unimplemented areas. |
| `plans/README.md` | Index of forward-looking implementation plans for future agents. |
| `plans/DURABLE_EXECUTION_BOUNDARY.md` | Proposed boundary between `batchor`, SQLite, and user pipelines for durable retry/resume. |
| `policies/support.md` | Published support policy for the latest released `0.x` minor. |
| `policies/versioning.md` | Published versioning policy for the Python API and CLI. |
