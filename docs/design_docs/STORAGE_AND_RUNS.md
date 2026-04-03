# Storage And Runs

This document describes durable execution state in `batchor`.

If you only read one implementation-oriented document after the getting-started guides, this should probably be it. The central idea in the repo is that a `Run` is durable, rehydratable state, not just an in-memory helper object.

## Run model

`BatchRunner.start(job)` returns a durable `Run` handle immediately.

The public handle exposes:

- `run_id`
- cached `status`
- `is_finished`
- `refresh()`
- `wait()`
- `summary()`
- `snapshot()`
- `results()`
- `export_artifacts()`
- `prune_artifacts()`

Important semantics:

- `status` is cached from the last summary read or refresh
- `refresh()` performs one poll-and-submit pass
- `wait()` repeatedly refreshes until the run is terminal
- `results()` and artifact lifecycle operations are terminal-only
- terminal currently means either `completed` or `completed_with_failures`

## Control plane and payload plane

`batchor` persists durable state in two layers.

The control plane is the state store:

- run config
- item rows and attempts
- active batch rows
- parsed outputs and failure records
- artifact pointers
- ingest checkpoints

The payload plane is the artifact store:

- request JSONL files
- raw output JSONL
- raw error JSONL

This split is intentional. Large artifacts remain files, while the durable store keeps the indexed state needed for orchestration and rehydration.

## Current durable backends

SQLite is the default durable backend.

Postgres is also implemented for shared control-plane state when callers explicitly construct `PostgresStorage(...)`.

In-memory storage exists for tests and short-lived local runs.

The SQLite/OpenAI path is covered by the default smoke test. Postgres storage compatibility is validated in a dedicated storage-contract CI job and requires `BATCHOR_TEST_POSTGRES_DSN` for equivalent local coverage.

## Storage responsibilities

Current storage responsibilities include:

- persisting public run config
- persisting item state and attempts
- persisting file-source ingest checkpoints when available
- persisting submitted batch metadata
- persisting request-artifact pointers for replayable submissions
- persisting batch output/error artifact pointers for raw provider payload retention
- persisting provider outputs/errors needed for rehydration
- reconstructing structured results on reload
- persisting storage metadata such as schema version

For provider configs that contain secrets, durable storage persists only the public provider config. Secret material such as the API key must come from in-memory config or the environment when a rehydrated run needs to talk to the provider again.

## SQLite behavior

SQLite is the default because the package is optimized first for local durable runs.

Current SQLite characteristics:

- WAL mode is enabled by default
- hot-path indexes exist for pending-item, active-batch, and artifact-pointer queries
- schema version is exposed through `SQLiteStorage.schema_version`
- startup migration behavior is additive and documented separately in `STORAGE_MIGRATIONS.md`

For SQLite-backed runs, the default artifact root is a sibling `*_artifacts/` directory beside the database.

## Postgres behavior

Postgres exists as an opt-in control-plane backend for cases where SQLite is not enough, such as shared state across processes or hosts.

Important operational rule:

- if you use Postgres across machines or fresh processes, provide a shared artifact root explicitly

Postgres stores the control plane, not the large request/output files themselves.

## Rehydration rules

`runner.get_run(run_id)` must work from a fresh runner when it points at the same durable storage.

Successful rehydration depends on:

- the durable storage still containing the run rows
- the configured artifact store still containing any artifacts needed for retry/resume
- structured output model classes still being importable
- credentials being available when a refresh needs to talk to the provider

Fresh-process resume also requeues any `queued_local` items back to `pending` before submission resumes.

Resume compatibility intentionally ignores non-persisted secret fields such as provider API keys.

## File-source checkpoints

For the built-in CSV and JSONL sources, storage persists a source checkpoint with:

- source kind
- source path/reference
- source fingerprint
- next durable item index
- ingestion completion flag

This enables `start(job, run_id=...)` to resume ingestion rather than rematerializing already-ingested rows, as long as the source still matches the stored identity.

Non-file iterables do not yet support the same mid-ingest crash recovery behavior.

## Artifact lifecycle

Once an item has a durable request artifact pointer, `batchor` may rely on that artifact for later retries instead of rebuilding the prompt from source data.

Once the whole run is terminal:

- users may call `Run.prune_artifacts()` or `BatchRunner.prune_artifacts(run_id)` to remove replayable request files and clear their pointers
- this is explicit lifecycle management, not automatic garbage collection

Raw output/error artifacts follow a stricter rule:

- users must call `Run.export_artifacts(...)` first
- only then may they call `Run.prune_artifacts(include_raw_output_artifacts=True)`

That rule keeps destructive cleanup of raw provider evidence explicit.

## Summary recomputation

Storage mutations do not force a full run-summary recomputation after every write. Summary aggregation happens on explicit summary reads and refresh boundaries instead. That reduces write amplification for larger runs.

## Current gaps

- file-source ingestion is synchronous during `start()`
- only the built-in CSV and JSONL sources support ingest checkpoints
- the only built-in artifact backend today is `LocalArtifactStore`
- non-file item iterables do not support mid-ingest crash recovery
- there is no partial-result API for non-terminal runs

## TBD

- SQLite/Postgres migration story
- automated retention windows and archive/export workflow beyond explicit manual export + prune
- partial-result read APIs for non-terminal runs
- resumable ingestion for non-file/custom sources
