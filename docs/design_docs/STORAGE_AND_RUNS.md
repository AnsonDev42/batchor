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
- persisting file-source ingest checkpoints when available
- persisting item state and attempts
- persisting submitted batch metadata
- persisting request-artifact pointers for replayable submissions
- persisting batch output/error artifact pointers for raw provider payload retention
- persisting provider outputs/errors needed for rehydration
- reconstructing structured results on reload

For SQLite-backed runs, replayable request JSONL artifacts are stored durably beside the database under a sibling `*_artifacts/` directory. SQLite remains the control-plane ledger and stores item-to-artifact pointers rather than treating the database itself as the long-term request file store.

In-memory storage exists for tests and short-lived local runs.

For the built-in CSV and JSONL sources, storage now also persists a source checkpoint with:

- source kind
- source path/reference
- source fingerprint
- next durable item index
- ingestion completion flag

## Rehydration Rules

- `runner.get_run(run_id)` must work from a fresh runner if it points at the same SQLite database
- SQLite-backed rehydration expects the sibling request-artifact directory to still be present for replayable pending/retryable items
- file-backed ingestion resume expects the caller to reuse the same `run_id` and provide the same source file contents
- structured output rehydration requires importable model classes
- if a model class cannot be resolved, `batchor` raises a clear model-resolution error

Once an item has a durable request artifact pointer, `batchor` may prune large inline request-building fields from SQLite and rely on the artifact for later retries.

Once the whole run is terminal, users may explicitly call `Run.prune_artifacts()` or `BatchRunner.prune_artifacts(run_id)` to remove replayable request files and clear their storage pointers. This is a manual lifecycle step today; `batchor` does not auto-delete artifacts behind the user's back.

Raw output/error artifacts follow a stricter rule: users must call `Run.export_artifacts(...)` first, and only then may they call `Run.prune_artifacts(include_raw_output_artifacts=True)`. This keeps raw provider payload retention explicit.

## Current Gaps

- SQLite is the only durable backend implemented today
- file-source ingestion is synchronous during `start()` and checkpointed only for the built-in CSV/JSONL sources
- artifact storage is local-filesystem only today; there is no remote/object-store abstraction yet
- non-file item iterables still do not support mid-ingest crash recovery

## TBD

- Postgres backend and migration story
- automated retention windows and archive/export workflow beyond explicit manual export + prune
- explicit schema migration/versioning guidance
- partial-result read APIs for non-terminal runs
- resumable ingestion for non-file/custom sources
