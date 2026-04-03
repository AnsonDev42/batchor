# Architecture

This folder is intentionally split into focused design docs rather than one giant reference file.

Status: reflects the current `batchor` implementation in the monorepo before extraction.

## Package Shape

```text
batchor/
  README.md
  AGENTS.md
  docs/
  src/batchor/
    core/
    providers/
    runtime/
    sources/
    storage/
  tests/
```

## Runtime Boundaries

### `core/`

Domain types and public configuration models:

- `BatchItem`
- `BatchJob`
- `RunSummary`
- `RunSnapshot`
- provider and storage enums
- provider config types such as `OpenAIProviderConfig`

### `providers/`

Provider-facing abstractions and implementations:

- base provider contract
- provider registry
- OpenAI Batch implementation

### `runtime/`

Execution and validation behavior:

- `BatchRunner`
- `Run`
- token estimation and request chunking
- durable request-artifact replay for retry/resume
- resumable file-backed ingestion checkpoints
- explicit terminal-run artifact pruning
- explicit raw-artifact export before raw-artifact pruning
- retry helpers
- response validation and structured-output parsing

### `sources/`

Streaming input adapters:

- `ItemSource`
- `CsvItemSource`
- `JsonlItemSource`

### `storage/`

Durable and ephemeral state backends:

- `StateStore`
- SQLite implementation
- in-memory implementation
- storage registry
- request-artifact pointers for replayable submissions

## Current Invariants

1. Public execution is run-oriented: `start()`, `get_run()`, `run_and_wait()`.
2. OpenAI Batch is the only built-in provider.
3. SQLite is the default durable backend.
4. Structured outputs require a module-level Pydantic v2 model for rehydration.
5. `Run.results()` is terminal-only.
6. `Run.refresh()` is explicit; status properties do not implicitly hit the provider.
7. SQLite-backed runs can replay prepared request JSONL artifacts without rebuilding prompts from the original item source.
8. `Run.prune_artifacts()` is explicit and terminal-only; it is not automatic garbage collection.
9. File-backed source resume requires a caller-supplied `run_id` plus a stable source fingerprint.
10. Raw output/error artifacts persist by default and require export before raw-artifact pruning.

## Extension Seams

The code is intentionally shaped for future providers and storage backends:

- provider config serialization goes through the provider registry
- storage creation goes through the storage registry
- runtime code works in terms of provider/store contracts instead of direct OpenAI/SQLite branches
- durable request replay is provider-agnostic at the runner/store boundary and currently materializes as local JSONL artifacts for SQLite
- file-backed resume uses source-specific checkpoints and currently supports the built-in CSV and JSONL sources

## TBD

- multi-provider capability matrix doc
- durable Postgres backend design
- resumable mid-ingest file sourcing
- live OpenAI smoke workflow
