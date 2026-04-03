# How It Works

This page explains the runtime model behind `batchor`. If the API reference feels too skeletal, start here first.

## Core concepts

### `BatchItem`

One logical unit of work. It has:

- `item_id`: stable item identifier inside the run
- `payload`: your application data
- `metadata`: optional JSON metadata carried through to results

### `BatchJob`

The full execution plan for a run. It bundles:

- the input items or an `ItemSource`
- `build_prompt`, which converts each item into provider prompt text
- provider config such as `OpenAIProviderConfig`
- optional `structured_output` Pydantic model
- chunking and retry policy
- optional batch metadata

`BatchJob` is declarative. It does not perform work by itself.

### `BatchRunner`

The orchestration entrypoint. `BatchRunner`:

- resolves storage and provider implementations
- creates or resumes durable runs
- ingests items into storage
- prepares request artifacts
- submits OpenAI batch files
- polls remote batches
- parses results back into terminal item records

### `Run`

The durable handle returned by `start()` and `get_run()`. A `Run` gives you:

- current summary state
- explicit refresh/wait control
- terminal results
- artifact export and prune operations

Treat `Run` as the long-lived handle for operator workflows.

## The execution lifecycle

At a high level, `batchor` follows this loop:

1. Materialize input items into durable state.
2. Claim a bounded set of pending items for submission.
3. Build OpenAI request rows or replay them from stored request artifacts.
4. Persist request JSONL as an artifact before upload.
5. Upload the request file and create a provider batch.
6. Poll active provider batches.
7. Download output and error files when batches finish.
8. Parse text or structured responses into durable terminal item results.

Two design choices matter here:

- submission is bounded, so large pending queues do not get fully materialized into upload-ready artifacts all at once
- request artifacts are durable, so retries and fresh-process resume can replay already-built requests instead of rebuilding prompts from source inputs

## Storage vs artifacts

`batchor` deliberately keeps two persistence layers:

- the state store is the control plane
- the artifact store is the payload plane

The state store keeps:

- run config
- item status and attempts
- batch records
- ingest checkpoints
- pointers to artifacts
- parsed terminal outputs and failures

The artifact store keeps:

- request JSONL files used for submission and replay
- downloaded raw output JSONL
- downloaded raw error JSONL

This split is why `batchor` can remain durable without stuffing large request files directly into SQLite or Postgres rows.

## Resume behavior

Resume happens at multiple layers:

- `runner.get_run(run_id)` rehydrates an existing durable run
- `start(job, run_id=...)` resumes the same run instead of creating a new one
- fresh-process recovery requeues local-but-not-submitted items back to `pending`
- built-in file sources can resume ingestion from a checkpoint when the source fingerprint still matches

Resume safety depends on stable inputs:

- reuse the same `run_id`
- keep the same storage backend
- keep the same artifact root when artifacts are needed for replay
- keep structured output models importable at module scope

## Structured outputs

When `structured_output=MyModel` is provided:

- `batchor` derives JSON Schema from the Pydantic model
- the OpenAI request includes strict schema instructions
- terminal results include parsed `output` objects of type `MyModel`

If parsing fails, the item records a structured-output failure instead of silently returning unchecked JSON text.

## CLI versus Python API

The Python API is the main product surface. It supports:

- in-memory, SQLite, and Postgres control-plane storage
- custom item iterables and built-in file sources
- custom artifact roots
- direct access to `Run`

The CLI is intentionally narrower. It is designed for operator workflows around:

- CSV and JSONL files
- SQLite durability
- JSON summaries and result export

If you need custom providers, custom sources, or tighter application integration, use the Python API.

## When to export and prune artifacts

Request artifacts are useful for retry and resume. Raw output artifacts are useful for audit and evidence.

After a terminal run:

- call `export_artifacts(...)` if you want a portable bundle containing raw files plus `results.jsonl`
- call `prune_artifacts()` to remove replayable request artifacts
- call `prune_artifacts(include_raw_output_artifacts=True)` only after export if you also want to remove raw provider payloads

That lifecycle is explicit by design. `batchor` does not silently delete artifacts in the background.
