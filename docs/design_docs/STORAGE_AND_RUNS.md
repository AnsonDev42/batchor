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

## Current Durable Backend

SQLite is the default durable backend.

Current storage responsibilities include:

- persisting run config
- persisting item state and attempts
- persisting submitted batch metadata
- persisting request-artifact pointers for replayable submissions
- persisting provider outputs/errors needed for rehydration
- reconstructing structured results on reload

For SQLite-backed runs, replayable request JSONL artifacts are stored durably beside the database under a sibling `*_artifacts/` directory. SQLite remains the control-plane ledger and stores item-to-artifact pointers rather than treating the database itself as the long-term request file store.

In-memory storage exists for tests and short-lived local runs.

## Rehydration Rules

- `runner.get_run(run_id)` must work from a fresh runner if it points at the same SQLite database
- SQLite-backed rehydration expects the sibling request-artifact directory to still be present for replayable pending/retryable items
- structured output rehydration requires importable model classes
- if a model class cannot be resolved, `batchor` raises a clear model-resolution error

Once an item has a durable request artifact pointer, `batchor` may prune large inline request-building fields from SQLite and rely on the artifact for later retries.

Once the whole run is terminal, users may explicitly call `Run.prune_artifacts()` or `BatchRunner.prune_artifacts(run_id)` to remove replayable request files and clear their storage pointers. This is a manual lifecycle step today; `batchor` does not auto-delete artifacts behind the user's back.

## Current Gaps

- SQLite is the only durable backend implemented today
- file-source ingestion is synchronous during `start()`
- artifact storage is local-filesystem only today; there is no remote/object-store abstraction yet
- mid-ingest crash recovery is not yet implemented

## TBD

- Postgres backend and migration story
- automated retention windows and archive/export workflow beyond explicit terminal pruning
- explicit schema migration/versioning guidance
- partial-result read APIs for non-terminal runs
