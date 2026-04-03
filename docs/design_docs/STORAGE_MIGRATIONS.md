# Storage Migrations

This document describes the current SQLite schema-versioning story for `batchor`.

## Current State

- SQLite remains the default durable backend
- the SQLite schema is additive and self-healing for supported columns/tables
- storage metadata now persists a `schema_version`
- the current published schema version is `1`

## Compatibility Rules

- opening an existing SQLite database with the current package will create any missing additive tables or columns
- `SQLiteStorage.schema_version` reports the effective schema version after startup checks
- durable run data should be considered compatible only within the documented supported package line
- secret provider fields are not part of durable schema compatibility

## Current Guidance

- for local upgrades, open the database with the newer package before relying on it in automation
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
