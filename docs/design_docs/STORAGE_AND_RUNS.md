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

`results()` is intentionally terminal-only.

## Current Durable Backend

SQLite is the default durable backend.

Current storage responsibilities include:

- persisting run config
- persisting item state and attempts
- persisting submitted batch metadata
- persisting provider outputs/errors needed for rehydration
- reconstructing structured results on reload

In-memory storage exists for tests and short-lived local runs.

## Rehydration Rules

- `runner.get_run(run_id)` must work from a fresh runner if it points at the same SQLite database
- structured output rehydration requires importable model classes
- if a model class cannot be resolved, `batchor` raises a clear model-resolution error

## Current Gaps

- SQLite is the only durable backend implemented today
- file-source ingestion is synchronous during `start()`
- mid-ingest crash recovery is not yet implemented

## TBD

- Postgres backend and migration story
- artifact retention policy
- explicit schema migration/versioning guidance
- partial-result read APIs for non-terminal runs
