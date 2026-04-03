# batchor

<div class="hero" markdown>

`batchor` is a durable OpenAI Batch runner for Python teams that want typed results, resumable runs, replayable artifacts, and a narrow operator CLI.

[Get Started](getting-started/installation.md){ .md-button .md-button--primary }
[How It Works](getting-started/how-it-works.md){ .md-button }
[Read The API](reference/api.md){ .md-button }

</div>

## Why batchor?

<div class="grid cards" markdown>

-   **Durable by default**

    Runs are persisted through a control-plane store and can be rehydrated across fresh processes.

-   **Typed structured results**

    Python API users can pass a Pydantic v2 model and receive parsed typed outputs instead of manually validating JSON strings.

-   **Replayable request artifacts**

    Submitted request JSONL is stored as an artifact so retry and resume can replay the exact provider payload that was already prepared.

-   **Explicit retention model**

    Request artifacts and raw provider outputs can be exported or pruned deliberately instead of being hidden inside implicit cleanup logic.

</div>

## The short version

`batchor` gives you four main concepts:

- `BatchItem`: one logical unit of work
- `BatchJob`: how to turn items into provider requests
- `BatchRunner`: the durable orchestrator
- `Run`: the handle you refresh, wait on, inspect, export, and prune

If that is the mental model you were missing from the generated docs, go straight to [How It Works](getting-started/how-it-works.md).

## Current surface

- Built-in provider: OpenAI Batch
- Durable storage: SQLite by default, Postgres as an opt-in control-plane backend
- Ephemeral storage: in-memory state store
- Artifact backend: local filesystem via `LocalArtifactStore`
- File-backed sources: CSV and JSONL

## Reading order

- Start with [Installation](getting-started/installation.md) if you are evaluating the package.
- Read [How It Works](getting-started/how-it-works.md) for the runtime mental model.
- Use [Python API](getting-started/python-api.md) or [CLI Usage](getting-started/cli.md) for concrete workflows.
- Use [API Reference](reference/api.md) for symbols and signatures.
- Use the Design section for implementation details and extension boundaries.
