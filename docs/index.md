# batchor

<div class="hero" markdown>

`batchor` is a durable OpenAI Batch runner for Python teams that want typed results, resumable runs, replayable artifacts, and a narrow operator CLI.

[Get Started](getting-started/installation.md){ .md-button .md-button--primary }
[Read The API](reference/api.md){ .md-button }

</div>

## Why batchor?

<div class="grid cards" markdown>

-   **Durable by default**

    SQLite-backed runs can be rehydrated across fresh processes, with explicit export and prune steps for request and raw output artifacts.

-   **Typed structured results**

    Python API users can pass a Pydantic v2 model and receive parsed typed results instead of hand-rolled JSON plumbing.

-   **Operator-friendly CLI**

    The built-in CLI handles CSV and JSONL text or structured-output jobs while keeping library behavior and CLI behavior clearly separated.

-   **Narrow, documented scope**

    The package is intentionally OpenAI-first, SQLite-first, and explicit about what is implemented today versus what remains `TBD`.

</div>

## Current surface

- Built-in provider: OpenAI Batch
- Durable storage: SQLite by default, Postgres as an opt-in control-plane backend
- Ephemeral storage: in-memory state store
- Artifact backend: local filesystem via `LocalArtifactStore`
- File-backed sources: CSV and JSONL

## Documentation map

- Start with [Installation](getting-started/installation.md) if you are evaluating the package.
- Go to [Python API](getting-started/python-api.md) or [CLI Usage](getting-started/cli.md) for first-use flows.
- Use the [API Reference](reference/api.md) for the public package surface.
- Use the Design section for implementation details and extension boundaries.
