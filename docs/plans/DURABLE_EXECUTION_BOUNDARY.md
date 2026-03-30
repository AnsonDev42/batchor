# Durable Execution Boundary Plan

Status: proposed on 2026-03-30.

`batchor` should be a durable batch execution library, not a full pipeline orchestrator and not a user data warehouse. The right boundary is: once a user item is materialized into a `BatchItem` and accepted by `BatchRunner.start(...)`, `batchor` owns durable execution of that item until it reaches a terminal state. Upstream input discovery and downstream business-side application of results remain the user's responsibility.

## Scope

- In:
  - durable run state and item lifecycle
  - retry and resume semantics for long-lived OpenAI Batch jobs
  - storage boundaries between SQLite and heavier request/output artifacts
  - lineage metadata needed for users to map outputs back to their own systems
- Out:
  - user-specific source discovery and partitioning workflows
  - user-specific business transformations before item materialization
  - user-specific side effects after results are available
  - turning SQLite into a general-purpose warehouse for raw source data

## Decision Summary

### Boundary

`batchor` should own:

- item-level state machine and attempts
- provider batch submission, polling, and retry classification
- durable idempotency and resume from `run_id`
- replayable request persistence for non-terminal items
- raw output and error persistence needed for result rehydration
- lightweight lineage fields that let users join results back to their inputs

Users should own:

- discovering source records and deciding which records become `BatchItem`s
- building domain-specific payloads before handoff to `batchor`
- applying terminal results back into application tables, files, or APIs
- long-term storage of business-source data outside the execution window

### Storage Boundary

SQLite should be the control-plane store, not the heavy-payload store.

SQLite should persist small, queryable execution data:

- `run_id`
- `item_id`
- status and attempt count
- provider batch/custom IDs
- request hash or fingerprint
- join metadata such as `source_ref`, `source_row_index`, or user-defined keys
- pointers to request/output/error artifacts
- timestamps, counters, retry state, and summary data

Artifacts should persist heavy replayable payloads:

- rendered request JSONL chunks
- output JSONL and error JSONL
- optional lineage manifests or export manifests

### Retry Model

Retry should not require recomputing the user's upstream workflow.

Instead, retry should work by reading the previously persisted request body for a retryable item from durable request artifacts and resubmitting it in a new provider batch. This makes `resume(run_id)` depend on `batchor` state plus artifacts, not on whether the user's original CSV/JSONL generation pipeline is still available or unchanged.

## Proposed Architecture

### Item Durability Contract

For each accepted item, `batchor` should durably persist either:

1. the exact request body to resend, or
2. enough normalized data to deterministically rebuild the exact request body

For OpenAI Batch, the preferred default is option 1, because it minimizes coupling to user code versions and prompt-generation drift.

### Recommended Default Model

Use a hybrid model:

- SQLite keeps an item ledger and execution state.
- Request artifacts keep the replayable request payloads.
- Result artifacts keep raw output/error payloads.

Each item row should point to its replayable request using fields such as:

- `request_artifact_id`
- `request_line_no` or byte offset
- `request_sha256`

This lets `batchor` retry a single item without needing the original source file.

### Payload Retention Modes

Support two explicit durability modes:

1. `inline_inputs`
   - store payload/prompt inline for small jobs and tests
   - simplest ergonomics
   - not appropriate for large production runs
2. `artifact_backed_inputs`
   - store only ledger metadata in SQLite
   - write rendered requests to chunked artifact files such as `requests_0001.jsonl.gz`
   - preferred for large or long-lived runs

The library should default toward `artifact_backed_inputs` for future production-oriented behavior.

## Action Items

[ ] Add a design-doc update in `docs/design_docs/STORAGE_AND_RUNS.md` that explicitly states the durability boundary begins when an item is accepted by `start(...)`.
[ ] Add a public storage contract for request artifacts, including artifact IDs, request hashes, and item-to-artifact pointers.
[ ] Refactor ingestion so large file-backed runs stream into durable request artifacts instead of persisting full payloads inline in SQLite by default.
[ ] Add a retry path that reconstructs retryable submissions from persisted request artifacts rather than from the original source iterator.
[ ] Add retention rules so request artifacts for active or retryable items are kept, while terminal artifacts can be pruned or archived after export.
[ ] Add public guidance for users on what metadata to include for downstream joins, such as source keys, row numbers, and partition IDs.
[ ] Add smoke and integration coverage for kill-and-resume behavior, including worker termination after request persistence and before batch completion.
[ ] Add large-input tests that verify SQLite growth stays bounded relative to ledger data while request artifacts absorb heavy payload storage.

## Edge Cases And Risks

- Crash before an item is written to a request artifact: the item is not yet durable and must be re-read from the source if ingestion restarts.
- Crash after artifact write but before SQLite pointer commit: artifact garbage collection and commit ordering must avoid orphaned data or invisible durable items.
- Non-deterministic prompt builders: if exact requests are not persisted, retries may drift across deploys or code changes.
- Sensitive inputs: artifact storage may need encryption, redaction rules, or configurable retention windows.
- Single-item oversized requests: the permanent-failure behavior still applies, but failure records should preserve enough lineage for user debugging.

## Open Questions

- Should `artifact_backed_inputs` become the default immediately, or only once a Postgres backend and artifact abstraction exist?
- Should request artifacts live beside SQLite on local disk first, or should the abstraction be designed for remote object storage from the start?
- How much user metadata should `batchor` persist by default versus requiring explicit opt-in for larger metadata blobs?
