# Installation

## Install the package

```bash
pip install batchor
```

Supported Python versions:

- `3.12`
- `3.13`
- `3.14`

## Optional provider extras

OpenAI support is installed by default. Gemini support uses Google's optional SDK dependency:

```bash
pip install "batchor[gemini]"
```

Gemini support is text-only and is available through both the Python API and CLI.

## What gets installed

The package includes:

- the Python library
- the `batchor` CLI
- the built-in OpenAI provider integration
- the built-in Gemini provider integration when `batchor[gemini]` is installed
- SQLite and Postgres storage implementations

It does not provision external infrastructure for you. If you use Postgres or a shared artifact root, you still manage those resources yourself.

## Agent-assisted setup

The Python package and the agent integration are intentionally separate:

- PyPI installs the Batchor runtime and CLI into a project.
- The user-facing `batchor` Codex plugin supplies the `$use-batchor` skill and repo-independent MCP helpers that help an agent choose the CLI or Python API, generate a starter, and check cost, data, resume, and retention decisions.
- The `batchor-agent-tools` plugin and `$batchor-dev` skill are only for contributors changing Batchor itself.

From a local Batchor checkout, install the user plugin with:

```bash
codex plugin marketplace add /path/to/batchor
codex plugin add batchor@batchor-local
```

Start a new Codex task so the installed skill and tools are loaded. Installing `batchor` from PyPI alone does not modify an agent's global configuration.

## Authentication

For Python API usage, authentication resolution is:

1. `OpenAIProviderConfig(api_key=...)`
2. `OPENAI_API_KEY`

For Gemini Developer API usage, authentication resolution is:

1. `GeminiProviderConfig(api_key=...)`
2. `GEMINI_API_KEY`

Vertex AI uses Application Default Credentials and resolves project/location from explicit `GeminiProviderConfig` fields or `GOOGLE_CLOUD_PROJECT` and `GOOGLE_CLOUD_LOCATION`. Vertex batches also require a writable `gs://` staging prefix.

The Python library does not auto-load `.env`.

The CLI loads a local `.env` as a convenience for operator usage. It resolves `OPENAI_API_KEY` for OpenAI, `GEMINI_API_KEY` for the Gemini Developer API, and the documented Google Cloud environment variables for Vertex AI. Rehydrated `status`, `wait`, and result operations load the same `.env` before reconstructing a provider.

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

- Read [Use Cases](use-cases.md) for concrete end-to-end examples.
- Read [Architecture](../design_docs/ARCHITECTURE.md) for the runtime model and diagrams.
- Read [Storage & Runs](../design_docs/STORAGE_AND_RUNS.md) for durable-run and artifact semantics.
- Read [Python API](python-api.md) if you are integrating `batchor` into application code.
- Read [CLI Usage](cli.md) if you want file-backed operator workflows.
