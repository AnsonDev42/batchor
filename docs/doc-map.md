# Docs Map

This page explains what each document is for so readers do not have to guess which page contains narrative guidance versus generated reference.

## Start here

| Doc | Purpose |
| --- | --- |
| `index.md` | Landing page and reading order. |
| `getting-started/how-it-works.md` | Runtime mental model: jobs, runs, storage, artifacts, and resume. |
| `getting-started/python-api.md` | End-to-end Python usage patterns. |
| `getting-started/cli.md` | Operator CLI behavior and examples. |
| `reference/api.md` | Public symbols and generated API reference. |

## Design docs

| Doc | Purpose |
| --- | --- |
| `design_docs/BOUNDARY_AND_PHILOSOPHY.md` | Ownership boundary between `batchor`, storage/artifacts, and user pipelines. |
| `design_docs/ARCHITECTURE.md` | Package structure, main flows, and extension seams. |
| `design_docs/OPENAI_BATCHING.md` | OpenAI request construction, token budgeting, splitting, and batch polling behavior. |
| `design_docs/STORAGE_AND_RUNS.md` | Durable `Run` lifecycle, rehydration, checkpoints, and artifact retention. |
| `design_docs/STORAGE_MIGRATIONS.md` | SQLite schema-versioning and migration guidance. |
| `design_docs/ROADMAP.md` | Intentionally unimplemented areas and planned work. |

## Validation and project policy

| Doc | Purpose |
| --- | --- |
| `smoke-test.md` | Minimum validation bar for local work and CI. |
| `policies/support.md` | Published support policy for the latest released `0.x` minor. |
| `policies/versioning.md` | Versioning expectations for the Python API and CLI. |
| `policies/contributing.md` | Contribution guidance. |

## Planning docs

| Doc | Purpose |
| --- | --- |
| `plans/README.md` | Index of forward-looking implementation plans for future agents. |
| `plans/DURABLE_EXECUTION_BOUNDARY.md` | Proposed boundary between `batchor`, SQLite, and user pipelines for durable retry/resume. |
