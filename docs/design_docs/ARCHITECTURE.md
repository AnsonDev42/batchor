# Architecture

This document describes the current package shape and the main runtime boundaries in the implementation that ships today.

Status: reflects the current public-package implementation.

## Package shape

```text
batchor/
  .agents/
  docs/
  plugins/
  src/batchor/
    artifacts/
    cli.py
    core/
    providers/
    runtime/
    sources/
    storage/
  tests/
```

Repo-local contributor tooling also lives outside the shipped Python package:

- `.agents/skills/batchor-dev/` contains the repo skill for AI-agent onboarding
- `.agents/plugins/marketplace.json` registers repo-local plugins for Codex-style discovery
- `plugins/batchor-agent-tools/` contains the repo-local MCP server and plugin manifest

These directories are contributor tooling only and are not part of the published `src/batchor` package.

The package is organized around one core concern: durable batch execution. Most modules exist to support one of five responsibilities:

- domain models
- request/provider adaptation
- execution orchestration
- input streaming
- durable state and artifacts

## High-Level Architecture

```mermaid
graph TB
    User["User / CLI"]

    subgraph runtime["runtime/"]
        BatchRunner
        Run["Run handle"]
    end

    subgraph core["core/"]
        BatchJob
        BatchItem
        Models["Models, Enums, Exceptions"]
    end

    subgraph providers["providers/"]
        BatchProvider["BatchProvider (ABC)"]
        OpenAIProvider["OpenAIBatchProvider"]
        ProviderRegistry
    end

    subgraph sources["sources/"]
        ItemSource["ItemSource (ABC)"]
        FileAdapters["CSV / JSONL / Parquet"]
    end

    subgraph storage["storage/"]
        StateStore["StateStore (ABC)"]
        SQLiteStorage
        PostgresStorage
        MemoryStateStore
        StorageRegistry
    end

    subgraph artifacts["artifacts/"]
        ArtifactStore["ArtifactStore (ABC)"]
        LocalArtifactStore
    end

    User -->|"start() / run_and_wait()"| BatchRunner
    BatchRunner --> Run
    BatchRunner -->|reads| BatchJob
    BatchJob -->|contains| BatchItem
    BatchJob -->|uses| ItemSource
    FileAdapters -.->|implements| ItemSource
    BatchRunner -->|persists state| StateStore
    SQLiteStorage -.->|implements| StateStore
    PostgresStorage -.->|implements| StateStore
    MemoryStateStore -.->|implements| StateStore
    BatchRunner -->|submits/polls| BatchProvider
    OpenAIProvider -.->|implements| BatchProvider
    BatchRunner -->|stores artifacts| ArtifactStore
    LocalArtifactStore -.->|implements| ArtifactStore
    BatchRunner -->|creates via| ProviderRegistry
    BatchRunner -->|creates via| StorageRegistry
```

This is the canonical diagram page for the current package shape. Keep the
README diagrams compact and reader-facing; put the detailed runtime and module
boundary view here.

## Core runtime concepts

The public runtime model centers on four types:

- `BatchItem`: one logical unit of work with stable `item_id`, application
  `payload`, and optional metadata.
- `BatchJob`: the declarative execution plan that bundles items or an
  `ItemSource`, prompt-building logic, provider config, and retry/artifact
  policy.
- `BatchRunner`: the orchestrator that resolves implementations, persists run
  state, builds or replays request artifacts, submits provider batches, polls
  remote status, and writes terminal results back into durable storage.
- `Run`: the durable handle returned by `start()` or `get_run()` for refresh,
  wait, pause/resume/cancel, terminal result reads, and artifact export/prune.

`CompositeItemSource` keeps the runner contract narrow: the runner still sees
one logical source, while callers remain responsible for selecting and ordering
the child sources up front.

## Main user-facing flow

The normal public flow is:

1. Construct a `BatchJob`.
2. Create a `BatchRunner`.
3. Call `start()` or `run_and_wait()`.
4. Work with the returned `Run`.

Internally that expands to:

1. Resolve provider and storage implementations.
2. Persist run config and ingest items into durable state.
3. Claim a bounded submission window from pending items.
4. Build or replay request JSONL rows.
5. Persist request artifacts before upload.
6. Submit one or more OpenAI batch files.
7. Poll active batches.
8. Download output/error files.
9. Parse terminal item results back into the state store.

## Execution Sequence

```mermaid
sequenceDiagram
    participant User
    participant BatchRunner
    participant StateStore
    participant ItemSource
    participant ArtifactStore
    participant Provider as BatchProvider

    User->>BatchRunner: start(job) / run_and_wait(job)
    BatchRunner->>StateStore: create_run(run_id, config)
    BatchRunner->>ItemSource: iterate items
    ItemSource-->>BatchRunner: BatchItem stream
    BatchRunner->>StateStore: append_items(materialized_items)
    BatchRunner->>StateStore: set_ingest_checkpoint()

    loop refresh() cycle (polling + submission)
        BatchRunner->>StateStore: get_active_batches()
        StateStore-->>BatchRunner: [ActiveBatchRecord...]

        opt active batches exist
            BatchRunner->>Provider: get_batch(batch_id)
            Provider-->>BatchRunner: BatchRemoteRecord

            alt status == "completed"
                BatchRunner->>Provider: download_file_content(output_file_id)
                Provider-->>BatchRunner: JSONL content
                BatchRunner->>ArtifactStore: write_text(output_artifact)
                BatchRunner->>StateStore: mark_items_completed(completions)
            else status in {failed, cancelled, expired}
                BatchRunner->>StateStore: reset_batch_items_to_pending()
                BatchRunner->>StateStore: record_batch_retry_failure()
            end
        end

        BatchRunner->>StateStore: claim_items_for_submission(limit)
        StateStore-->>BatchRunner: [ClaimedItem...]
        BatchRunner->>ArtifactStore: write_text(request JSONL artifact)
        BatchRunner->>Provider: upload_input_file(local_path)
        Provider-->>BatchRunner: remote_file_id
        BatchRunner->>Provider: create_batch(remote_file_id)
        Provider-->>BatchRunner: BatchRemoteRecord (status=validating)
        BatchRunner->>StateStore: register_batch()
        BatchRunner->>StateStore: mark_items_submitted()
    end

    BatchRunner-->>User: Run (terminal)
    User->>Run: results()
    Run->>StateStore: get_item_records()
    StateStore-->>Run: [PersistedItemRecord...]
    Run-->>User: [BatchResultItem...]
```

## Batch Lifecycle

### Item state machine

Each item in a run transitions through the following statuses:

```mermaid
stateDiagram-v2
    [*] --> PENDING : item ingested from source

    PENDING --> QUEUED_LOCAL : claimed for submission cycle
    QUEUED_LOCAL --> PENDING : released — batch submission failed or cycle ended early
    QUEUED_LOCAL --> FAILED_PERMANENT : rejected pre-submission (e.g. token budget exceeded, max attempts)
    QUEUED_LOCAL --> SUBMITTED : batch created and registered with provider

    SUBMITTED --> COMPLETED : batch completed, result parsed OK
    SUBMITTED --> FAILED_RETRYABLE : batch error / item error — attempt count below max
    SUBMITTED --> FAILED_PERMANENT : batch error / item error — attempt count reached max

    FAILED_RETRYABLE --> PENDING : re-queued after backoff delay

    COMPLETED --> [*]
    FAILED_PERMANENT --> [*]
```

### Run lifecycle

A run has two orthogonal state axes — **lifecycle status** (progress toward completion) and **control state** (operator signal).

```mermaid
stateDiagram-v2
    direction LR

    state "Lifecycle Status" as ls {
        [*] --> RUNNING : run created
        RUNNING --> COMPLETED : all items completed successfully
        RUNNING --> COMPLETED_WITH_FAILURES : run finished with ≥1 FAILED_PERMANENT item
    }

    state "Control State" as cs {
        [*] --> RUNNING2 : run created
        RUNNING2 --> PAUSED : pause_run() called
        PAUSED --> RUNNING2 : resume_run() called
        RUNNING2 --> CANCEL_REQUESTED : cancel_run() called

        note right of CANCEL_REQUESTED
            Not reversible.
            Runner drains in-flight batches;
            lifecycle then becomes terminal.
        end note
    }
```

Detailed storage, resume, and artifact-retention semantics live in
[`STORAGE_AND_RUNS.md`](STORAGE_AND_RUNS.md).

## Module boundaries

### `core/`

Owns domain types and public configuration models:

- `BatchItem`
- `BatchJob`
- `PromptParts`
- `RunSummary`
- `RunSnapshot`
- provider and storage enums
- provider config types such as `OpenAIProviderConfig`
- retry, chunk, artifact, and terminal-result models

`core/` should stay mostly declarative. It describes what a run is, not how the runtime executes it.

### `providers/`

Owns provider-facing abstractions and implementations:

- base provider contract
- provider registry
- OpenAI Batch implementation

The provider layer is responsible for:

- building provider request rows
- uploading input files
- creating batches
- polling batches
- downloading provider files
- normalizing provider output records

Durable artifact writing is not owned by the provider layer. The runner persists artifacts and hands staged local files to the provider.

### `runtime/`

Owns execution behavior:

- `BatchRunner`
- `Run`
- Typer CLI entrypoint for operator workflows
- persisted run control state with `pause`, `resume`, and drain-style `cancel`
- optional observer callback for provider lifecycle events
- token estimation and request chunking
- bounded pending-item claim windows before submission
- durable request-artifact replay for retry/resume
- per-refresh request-artifact file caching during replay
- artifact-store staging/export/delete orchestration
- incremental terminal-result paging/export for already-terminal items
- resumable deterministic-source checkpoints
- fresh-process recovery of `queued_local` items back to pending submission
- bounded concurrent provider polling and file download handling
- explicit terminal-run artifact pruning
- explicit raw-artifact export before raw-artifact pruning
- retry helpers
- response validation and structured-output parsing

This is where the durable lifecycle lives. It bridges the domain models, providers, storage, and artifact store.

### `sources/`

Owns streaming input adapters:

- `ItemSource`
- `CheckpointedItemSource`
- `CompositeItemSource`
- `CsvItemSource`
- `JsonlItemSource`
- `ParquetItemSource`

The built-in file sources support durable resume through a source fingerprint plus an ingest checkpoint stored in the control plane.
`CompositeItemSource` composes explicit checkpointed sources into one logical source without moving source discovery or partition ordering into the runner.

### `storage/`

Owns the durable and ephemeral state backends:

- `StateStore`
- SQLite implementation
- Postgres implementation
- in-memory implementation
- storage registry

The storage layer persists:

- run config
- run control state
- item state and attempts
- active batch metadata
- ingest checkpoints
- parsed terminal outputs
- terminal result sequence metadata
- pointers to durable artifacts

### `artifacts/`

Owns the payload plane for large durable files:

- request JSONL used for submission/replay
- raw output JSONL downloaded from the provider
- raw error JSONL downloaded from the provider

The built-in implementation is `LocalArtifactStore`.

### `cli.py`

Owns a deliberately narrow operator interface over the runtime:

- one or more explicit CSV and JSONL inputs only
- SQLite only
- JSON summaries
- explicit status, wait, results, export, and prune commands

The CLI is intentionally not the place where new orchestration behavior should be invented first.

## Why storage and artifacts are split

This is one of the most important design choices in the repo.

The state store holds indexed, queryable control-plane data.
The artifact store holds larger opaque files that must survive retry/resume/export workflows.

That split gives `batchor`:

- resumable request replay
- smaller durable control-plane rows
- clearer retention rules
- backend flexibility for future non-local artifact stores

## Current invariants

1. Public execution is run-oriented: `start()`, `get_run()`, `run_and_wait()`.
2. OpenAI Batch is the only built-in provider.
3. SQLite is the default durable backend.
4. Postgres is an opt-in durable backend for shared control-plane state.
5. Structured outputs require a module-level Pydantic v2 model for rehydration.
6. `Run.results()` is terminal-only.
7. `Run.refresh()` is explicit; summary properties do not implicitly hit the provider.
8. Durable artifacts flow through the `ArtifactStore` contract; the built-in implementation is `LocalArtifactStore`.
9. Stored item rows keep artifact keys, not absolute filesystem paths.
10. Fresh-process resume requeues `queued_local` items before attempting submission again.
11. `Run.prune_artifacts()` is explicit and terminal-only; it is not automatic garbage collection.
12. File-backed source resume requires a caller-supplied `run_id` plus a stable source fingerprint.
13. Raw output/error artifacts persist by default and require export before raw-artifact pruning.
14. A terminal run may be either `completed` or `completed_with_failures`; both statuses allow artifact export/prune and final result access.
15. Provider secrets may exist in in-memory config objects, but durable storage persists public provider config only.
16. CLI `.env` loading is a CLI-only convenience and not part of library runtime behavior.
17. Run lifecycle status and run control state are separate; pause/cancel do not redefine terminal lifecycle semantics.
18. Incremental terminal-result reads are sequence-based and only return items that have already reached a terminal item state.
19. Built-in deterministic-source resume currently covers CSV, JSONL, and Parquet; arbitrary iterables still do not become durable by magic.
20. `BatchItem.metadata["batchor_lineage"]` is reserved for lightweight source/join metadata when provided by built-in adapters or callers.

## Extension seams

The code is shaped for future providers and backends, but within explicit boundaries:

- provider config serialization goes through the provider registry
- storage creation goes through the storage registry
- runtime code works in terms of provider/store contracts instead of direct OpenAI/SQLite branches
- durable request replay is provider-agnostic at the runner/store boundary and materializes through the artifact-store contract
- deterministic-source resume uses source-specific checkpoints and currently supports the built-in CSV, JSONL, and Parquet sources
- provider observability hooks are callback-based and currently emit coarse lifecycle events from the runner

## Current gaps

- the only built-in provider is OpenAI Batch
- the only built-in artifact backend is local filesystem storage
- arbitrary non-checkpointable iterables do not support mid-ingest crash recovery
- the CLI does not expose the full Python API surface

## TBD

- multi-provider capability matrix doc
- remote/object-store artifact backend
- provider-side remote cancellation
