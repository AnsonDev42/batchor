# Installation

## Install the package

```bash
pip install batchor
```

Supported Python versions:

- `3.12`
- `3.13`

## What gets installed

The package includes:

- the Python library
- the `batchor` CLI
- the built-in OpenAI provider integration
- SQLite and Postgres storage implementations

It does not provision external infrastructure for you. If you use Postgres or a shared artifact root, you still manage those resources yourself.

## Authentication

For Python API usage, authentication resolution is:

1. `OpenAIProviderConfig(api_key=...)`
2. `OPENAI_API_KEY`

The Python library does not auto-load `.env`.

The CLI loads a local `.env` as a convenience for operator usage, then resolves `OPENAI_API_KEY`.

## Storage defaults

- `BatchRunner()` uses SQLite durability by default.
- `BatchRunner(storage="memory")` uses ephemeral in-memory state.
- `PostgresStorage(...)` is available as an opt-in durable control-plane backend when you need shared state.

Artifact defaults follow storage:

- SQLite defaults to a local artifact root beside the SQLite database
- explicit `temp_root=` or `artifact_store=` can override that
- Postgres users should usually provide an explicit shared `LocalArtifactStore(...)`

If you use Postgres across machines or fresh processes, configure a shared artifact root so replayable request artifacts remain visible everywhere they need to be.

## Where to go next

- Read [Architecture](../design_docs/ARCHITECTURE.md) for the runtime model and diagrams.
- Read [Storage & Runs](../design_docs/STORAGE_AND_RUNS.md) for durable-run and artifact semantics.
- Read [Python API](python-api.md) if you are integrating `batchor` into application code.
- Read [CLI Usage](cli.md) if you want file-backed operator workflows.
