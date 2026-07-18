# Storage Migrations

This document describes the current SQLite schema-versioning story for `batchor`.

## Current State

- SQLite remains the default durable backend
- the SQLite schema is additive and self-healing for supported columns/tables
- storage metadata now persists a `schema_version`
- the current published schema version is `6`
- schema version `4` added the nullable `runs.control_reason` column for durable pause reasons
- schema version `5` makes the existing `run_ingest_state` row a generic, mandatory
  incomplete-ingestion marker for every run; startup backfills one for every legacy
  run that lacks it
- schema version `6` adds durable `submission_intents` and shared
  `capacity_scopes` reservations for crash-safe submission reconciliation and
  cross-run OpenAI enqueue accounting

## Version 5: generic ingestion markers

Version 5 changes the meaning of `run_ingest_state` from a checkpoint-only
record into a generic ingestion lifecycle marker. New runs create the marker in
the same transaction as the run row. This prevents an empty run from being
calculated as terminal while its first item slice is still being rendered.

On startup, SQLite and Postgres backfill every run without a marker as
`ingestion_complete = 0`, with `next_item_index` set to its durable item count
and an unattached source identity. This is deliberately conservative: pre-v5
state has no durable fact that distinguishes a completed arbitrary iterable
from an interrupted one. An empty source can be reattached at index zero; a
partially materialized non-checkpointable source must be cancelled/abandoned
explicitly after its active batches drain. Deterministic sources retain their
normal checkpoint-resume behavior.

## Version 6: submission intents and capacity scopes

Version 6 adds two additive tables:

- `submission_intents` is written before a provider `create_batch` call. A
  `creating` intent represents an ambiguous remote outcome and blocks automatic
  retry until an operator resolves it as created or not created. A successful
  create finalizes the batch registration and item linkage together.
- `capacity_scopes` holds an atomic OpenAI enqueue-token reservation keyed by
  provider/model/quota scope. The reservation spans runs and is released only
  after terminal consumption or reset succeeds.

The tables are created by the normal additive metadata startup path. Existing
submitted rows that predate intents are counted dynamically when a scope admits
new work, so an upgrade cannot over-admit capacity on top of an active legacy
batch. No data is deleted or rewritten during either migration.

## Compatibility Rules

- opening an existing SQLite database with the current package will create any missing additive tables or columns
- `SQLiteStorage.schema_version` reports the effective schema version after startup checks
- opening an existing Postgres schema applies the same additive metadata tables
  and generic-ingest-marker backfill
- durable run data should be considered compatible only within the documented supported package line
- secret provider fields are not part of durable schema compatibility

## Current Guidance

- for local upgrades, open the database with the newer package before relying on it in automation
- before upgrading a partially ingested legacy arbitrary iterable, decide whether
  to reattach an empty source or cancel/abandon the legacy tail; it is not safe
  to infer completion from its pre-v5 rows
- inspect and resolve any `RunSubmissionIndeterminateError` rather than retrying
  the run blindly after an ambiguous provider create
- treat schema updates as one-way unless explicit downgrade guidance is published
- keep artifact directories beside the same SQLite database during upgrades
- if you need hard rollback guarantees, export terminal artifacts and results before changing package versions

## Current Gaps

- there is no standalone migration CLI yet
- there is no downgrade tooling
- there is no cross-version compatibility matrix beyond the published support/versioning policy

## TBD

- migration tooling beyond automatic additive startup upgrades
- explicit downgrade guidance
- versioned migration notes per release
