# Boundary And Philosophy

Status: current implementation direction as of 2026-03-30.

This document defines the concrete boundary between `batchor`, its durable storage, and the user's pipeline.

## Core Philosophy

`batchor` is a durable batch execution library.

It is not:

- a full workflow orchestrator
- a general-purpose data warehouse
- the user's source-of-truth dataset store
- the user's downstream business-side effect engine

The library should stay narrow and reliable: accept work items, persist enough state to resume safely, execute provider batches, and surface typed results plus lineage back to the caller.

## Boundary

### What `batchor` owns

- durable run and item lifecycle state
- provider request construction and batch submission
- retry classification and attempt tracking
- durable replayable request persistence once a request artifact has been prepared
- result parsing, validation, and run rehydration
- lightweight lineage metadata needed to join results back to user systems

### What the user pipeline owns

- discovering or partitioning source data
- transforming domain rows into `BatchItem`s
- business-specific decisions about what to run and when
- applying terminal results back into application tables, files, or APIs
- long-term storage of business data outside `batchor`'s execution window

## Durability Boundary

Durability begins when an item is accepted into a `Run`.

In the current implementation, `batchor` may still hold inline prompt data before the first request artifact is written. Once a request artifact exists, that artifact becomes the replay source for later retries and resumes.

For SQLite-backed runs:

- SQLite is the control-plane ledger
- request/output JSONL files are durable artifacts beside the database
- item rows store artifact pointers, not the full long-term replay payload

This means a fresh worker can resume a run from the same SQLite database plus sibling artifact directory without rebuilding prompts from the original input source after the request artifact has been created.

## Non-Goals

- requiring users to adopt Temporal, Airflow, or any specific orchestrator
- rebuilding the entire upstream source-generation workflow during resume
- persisting arbitrarily large source payloads inline in SQLite forever
- hiding all pipeline boundaries behind one monolithic library API

## Current Implementation Notes

- SQLite-backed runs persist per-item request artifact path, line number, and request hash.
- Once that pointer exists, `batchor` can prune large inline request-building fields from SQLite.
- Artifact storage is local filesystem only today.
- Mid-ingest crash recovery before the first request artifact exists is still `TBD`.
