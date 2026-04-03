# Storage And Runs

This document describes durable execution state in `batchor`.

If you only read one implementation-oriented document after the getting-started guides, this should probably be it. The central idea in the repo is that a `Run` is durable, rehydratable state, not just an in-memory helper object.

## Run model

`BatchRunner.start(job)` returns a durable `Run` handle immediately.

The public handle exposes:

- `run_id`
- cached `status`
- cached `control_state`
- `is_finished`
- `refresh()`
- `wait()`
- `summary()`
- `snapshot()`
- `pause()`
- `resume()`
- `cancel()`
- `results()`
- `read_terminal_results()`
- `export_terminal_results()`
- `export_artifacts()`
- `prune_artifacts()`

Important semantics:

- `status` is cached from the last summary read or refresh
- `refresh()` performs one poll-and-submit pass
- `wait()` repeatedly refreshes until the run is terminal
- `results()` and artifact lifecycle operations are terminal-only
- terminal currently means either `completed` or `completed_with_failures`
- `read_terminal_results()` and `export_terminal_results()` are incremental APIs for already-terminal items and are safe to call before the whole run finishes

## Control plane and payload plane

`batchor` persists durable state in two layers.

The control plane is the state store:

- run config
- run control state
- item rows and attempts
- active batch rows
- parsed outputs and failure records
- terminal result sequence metadata
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
- persisting run control state
- persisting deterministic-source ingest checkpoints when available
- persisting item state and attempts
- persisting terminal result sequence metadata for incremental reads
- persisting submitted batch metadata
- persisting request-artifact pointers for replayable submissions
- persisting batch output/error artifact pointers for raw provider payload retention
- persisting provider outputs/errors needed for rehydration
- reconstructing structured results on reload
- persisting storage metadata such as schema version

Durable artifacts now flow through the `ArtifactStore` contract. The built-in implementation is `LocalArtifactStore`, which stores replayable request JSONL and optional raw output/error files on local disk or a shared mounted volume.
New local directories/files are created with owner-only permissions where the platform supports them.

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

## File-source checkpoints

For deterministic built-in sources, storage persists a source checkpoint with:

- source kind
- source path/reference
- source fingerprint
- next durable item index
- opaque checkpoint payload
- ingestion completion flag

The opaque checkpoint payload is source-owned. `batchor` persists it but does not attempt to interpret arbitrary custom-source resume tokens.
`CompositeItemSource` keeps this storage contract unchanged by presenting one logical source to the runner:

- `source_kind` is `composite`
- `source_ref` is canonical JSON of the ordered child source identities
- `source_fingerprint` is the hash of that ordered child identity list
- `checkpoint_payload` tracks the active child source plus that child's opaque checkpoint

There is still one ingest-checkpoint row per run, not one row per child source.

This enables `start(job, run_id=...)` to resume ingestion rather than rematerializing already-ingested rows, as long as the source still matches the stored identity.

Non-checkpointable iterables do not yet support the same mid-ingest crash recovery behavior.

## Rehydration rules

`runner.get_run(run_id)` must work from a fresh runner when it points at the same durable storage.

Successful rehydration depends on:

- the durable storage still containing the run rows
- the configured artifact store still containing any artifacts needed for retry/resume
- structured output model classes still being importable
- credentials being available when a refresh needs to talk to the provider

Fresh-process resume also requeues any `queued_local` items back to `pending` before submission resumes.

Resume compatibility intentionally ignores non-persisted secret fields such as provider API keys.

For deterministic-source resume, the caller must also reuse the same `run_id` and provide the same source identity/fingerprint.
For composite sources, that includes the same ordered child identities; changing the child order or swapping one file changes the logical source identity.

Once an item has a durable request artifact pointer, `batchor` prunes large inline request-building fields from the control-plane store and relies on the artifact for later retries.

## Artifact lifecycle

Once an item has a durable request artifact pointer, `batchor` may rely on that artifact for later retries instead of rebuilding the prompt from source data.

Once the whole run is terminal:

- users may call `Run.prune_artifacts()` or `BatchRunner.prune_artifacts(run_id)` to remove replayable request files and clear their pointers
- this is explicit lifecycle management, not automatic garbage collection

Raw output/error artifacts follow a stricter rule:

- when raw retention is enabled, users must call `Run.export_artifacts(...)` first
- only then may they call `Run.prune_artifacts(include_raw_output_artifacts=True)`

That rule keeps destructive cleanup of raw provider evidence explicit.

## Run control semantics

Run lifecycle status and control state are separate:

- lifecycle status answers whether the run is still active or terminal
- control state answers whether local execution should keep ingesting/submitting/polling

Current control states are:

- `running`
- `paused`
- `cancel_requested`

Semantics:

- `pause` stops new ingestion, new item claiming/submission, and provider polling
- `resume` restarts those local activities from persisted state
- `cancel` stops new ingestion/submission, keeps polling active provider batches, and then permanently fails remaining local non-terminal items with `error_class="run_cancelled"`

`wait()` fails fast on paused runs instead of sleeping indefinitely.
Provider-side remote cancellation is still `TBD`.

## Incremental terminal results

Storage assigns a monotonic `terminal_result_sequence` the first time an item reaches a terminal item state.
Incremental readers/exporters page on that sequence rather than `item_index`, so late completion of lower-index items does not break downstream consumption.

The public incremental APIs are:

- `Run.read_terminal_results(after_sequence=..., limit=...)`
- `BatchRunner.read_terminal_results(run_id, after_sequence=..., limit=...)`
- `Run.export_terminal_results(destination, after_sequence=..., append=True)`

The returned/exported cursor is `next_after_sequence`.
This API is library-first today; the CLI does not expose it yet.

## Lineage conventions

`BatchItem.metadata` remains user-owned, but `batchor` reserves `metadata["batchor_lineage"]` as an object when built-in sources or callers want to persist lightweight join metadata.
Recommended keys are:

- `source_ref`
- `partition_id`
- `source_item_index`
- `source_primary_key`
- `source_namespace`

Built-in deterministic sources populate what they know without replacing user metadata.
When `CompositeItemSource` is used, the durable public `item_id` becomes the auto-namespaced run identifier, and the original child-source row ID is preserved in `source_primary_key`.

## Summary recomputation

Storage mutations do not force a full run-summary recomputation after every write. Summary aggregation happens on explicit summary reads and refresh boundaries instead. That reduces write amplification for larger runs.

## Current gaps

- deterministic-source ingestion is synchronous during `start()` and only a small set of built-in source adapters implement crash-safe mid-ingest resume today
- the only built-in artifact backend today is `LocalArtifactStore`
- arbitrary non-checkpointable iterables do not support mid-ingest crash recovery
- there is no partial-result API for non-terminal provider state
- provider-side remote cancellation is not implemented
- the CLI does not expose run control or incremental terminal-result APIs yet

## TBD

- SQLite/Postgres migration story
- automated retention windows and archive/export workflow beyond explicit manual export + prune
- partial-result read APIs for non-terminal provider state
- resumable ingestion for additional deterministic non-file/custom sources beyond the built-in set
