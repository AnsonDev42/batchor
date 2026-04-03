# Installation

## Install the package

```bash
pip install batchor
```

Supported Python versions:

- `3.12`
- `3.13`

## Authentication

For Python API usage, authentication resolution is:

1. `OpenAIProviderConfig(api_key=...)`
2. `OPENAI_API_KEY`

The Python library does not auto-load `.env`.

The CLI loads a local `.env` as a convenience for interactive/operator use, then uses `OPENAI_API_KEY`.

## Storage defaults

- `BatchRunner()` uses SQLite durability by default.
- `BatchRunner(storage="memory")` uses ephemeral in-memory state.
- `PostgresStorage(...)` is available as an opt-in durable control-plane backend when you need shared state.

If you use Postgres across machines or fresh processes, configure a shared artifact root with `LocalArtifactStore(...)` so replayable request artifacts remain visible everywhere they need to be.
