# Storage And Runs

This document describes durable execution state in `batchor`.

## Run Model

`BatchRunner.start(job)` returns a durable `Run` handle immediately.

The public handle exposes:

- `run_id`
- cached `status`
- `is_finished`
- `refresh()`
- `wait()`
- `snapshot()`
- `summary()`
- `results()`
- `prune_artifacts()`

`results()` and `prune_artifacts()` are intentionally terminal-only.
Today terminal means either `completed` or `completed_with_failures`.

## Current Durable Backends

SQLite is the default durable backend.
Postgres is also implemented for shared control-plane state when callers explicitly construct `PostgresStorage(...)`.

The SQLite backend now enables WAL-mode defaults and ships explicit hot-path indexes so durable local runs spend less time on repeated pending-item, active-batch, and artifact-pointer scans.

Current storage responsibilities include:

- persisting public run config
- persisting file-source ingest checkpoints when available
- persisting item state and attempts
- persisting submitted batch metadata
- persisting request-artifact pointers for replayable submissions
- persisting batch output/error artifact pointers for raw provider payload retention
- persisting provider outputs/errors needed for rehydration
- reconstructing structured results on reload
- persisting storage metadata such as schema version

Durable artifacts now flow through the `ArtifactStore` contract. The built-in implementation is `LocalArtifactStore`, which stores replayable request JSONL and raw output/error files on local disk or a shared mounted volume.

For SQLite-backed runs, the default local artifact root is still a sibling `*_artifacts/` directory beside the database. SQLite remains the control-plane ledger and stores item-to-artifact pointers rather than treating the database itself as the long-term request file store.

For Postgres-backed runs, callers must provide a shared artifact root explicitly if they expect multiple machines or fresh processes to see the same replayable artifacts.

For provider configs that contain secrets, SQLite stores the public provider config only. Runtime credential lookup must come from the explicit in-memory config or ambient environment when a rehydrated run needs to talk to the provider again.

`SQLiteStorage.schema_version` exposes the effective schema version after startup checks. Current migration behavior is additive at startup; see `docs/design_docs/STORAGE_MIGRATIONS.md` for guidance and limits.

In-memory storage exists for tests and short-lived local runs.

For the built-in CSV and JSONL sources, storage now also persists a source checkpoint with:

- source kind
- source path/reference
- source fingerprint
- next durable item index
- ingestion completion flag

## Rehydration Rules

- `runner.get_run(run_id)` must work from a fresh runner if it points at the same SQLite database
- rehydration expects the configured artifact store to still contain replayable pending/retryable items
- fresh-process rehydration requeues any `queued_local` items back to `pending` before submission resumes
- file-backed ingestion resume expects the caller to reuse the same `run_id` and provide the same source file contents
- structured output rehydration requires importable model classes
- if a model class cannot be resolved, `batchor` raises a clear model-resolution error
- resume compatibility ignores non-persisted secret fields such as provider API keys

Once an item has a durable request artifact pointer, `batchor` may prune large inline request-building fields from the control-plane store and rely on the artifact for later retries.

Storage mutations no longer force a full run-summary recomputation after each write. Instead, summary aggregation happens on explicit summary reads and refresh boundaries, which reduces control-plane write amplification for large runs.

Once the whole run is terminal, users may explicitly call `Run.prune_artifacts()` or `BatchRunner.prune_artifacts(run_id)` to remove replayable request files and clear their storage pointers. This is a manual lifecycle step today; `batchor` does not auto-delete artifacts behind the user's back.

Raw output/error artifacts follow a stricter rule: users must call `Run.export_artifacts(...)` first, and only then may they call `Run.prune_artifacts(include_raw_output_artifacts=True)`. This keeps raw provider payload retention explicit for both terminal success and terminal partial-failure runs.

## Current Gaps

- file-source ingestion is synchronous during `start()` and checkpointed only for the built-in CSV/JSONL sources
- the only artifact backend implemented today is `LocalArtifactStore`; there is no remote/object-store backend yet
- non-file item iterables still do not support mid-ingest crash recovery

## TBD

- SQLite/Postgres migration story
- automated retention windows and archive/export workflow beyond explicit manual export + prune
- partial-result read APIs for non-terminal runs
- resumable ingestion for non-file/custom sources
