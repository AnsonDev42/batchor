# OpenAI Batching

This document describes the OpenAI-specific behavior inside `batchor`.

OpenAI remains the default and most feature-complete provider path. Gemini has its own design note in [`GEMINI_BATCHING.md`](GEMINI_BATCHING.md), while this page focuses only on OpenAI Batch semantics.

## Current behavior

The Python API and CLI both support OpenAI. The CLI is OpenAI-only today.

## Request construction

The built-in provider converts each prepared item into one OpenAI Batch JSONL request row.
The request `custom_id` is derived from the run's durable `item_id`, so multi-source runs must make those item IDs unique before request construction.

Current endpoint support:

- Responses API
- Chat Completions API

Behavior by mode:

- text jobs omit structured-output schema instructions
- structured-output jobs derive JSON Schema from a Pydantic v2 model
- Responses API requests place strict schema instructions under `body.text.format`
- Chat Completions requests place strict schema instructions under `response_format`
- Responses API requests forward `reasoning_effort` when configured

Authentication resolution is:

1. explicit `OpenAIProviderConfig(api_key=...)`
2. `OPENAI_API_KEY`

`OpenAIProviderConfig.model` accepts either raw strings or the exported `OpenAIModel` enum for IDE-friendly model selection.

## Durable request artifacts

Prepared request rows are written to durable JSONL artifacts before upload. The runner records, per item:

- artifact path
- line number within the artifact
- request hash

That enables retry and fresh-process resume to replay the exact prepared request body without rebuilding the prompt from the original item payload or source file.

The runner now owns request JSONL serialization and stages a local copy from the artifact store for provider upload. The provider contract builds request rows and uploads local files, but it no longer owns durable request-file writes.
Once those request-artifact pointers exist, the control-plane store can aggressively shed large inline prompt/request-building fields and rely on the artifact for retry/resume replay.

Important consequences:

- the runner owns durable request-file writes
- the provider uploads local files staged from the artifact store
- retries can survive failures that happen after artifact persistence but before durable batch registration
- shared request-artifact contents are cached within one refresh/submission cycle to avoid repeated rereads

Before request artifacts exist, built-in deterministic sources can resume ingestion from a persisted source checkpoint when the caller re-enters `start(job, run_id=...)` with the same source identity and config.
That currently includes CSV, JSONL, Parquet, and `CompositeItemSource` for ordered composition of checkpointed sources.
When `CompositeItemSource` is used, child-source row IDs are auto-namespaced into run-unique `item_id` values, while the original row ID is preserved in lineage metadata.
Custom non-file sources must implement a durable checkpoint contract explicitly; arbitrary iterables and live DB cursors are still `TBD`.
Before resumed ingestion continues, active OpenAI batches are polled once. If that poll consumes terminal batches or records enqueue/backoff failures, those state changes are applied before any new prompt rendering, request replay, or submission happens.
The execution cycle reports durable progress explicitly to `Run.wait()`, so the
wait loop immediately drains productive poll/submission cycles and sleeps only
when the executor reports an idle or backoff halt.

## Raw output retention

When OpenAI output or error files are downloaded, `batchor` persists them as raw artifacts unless the run opts out through `ArtifactPolicy(persist_raw_output_artifacts=False)`.

These files are meant to support:

- audit trails
- export bundles
- debugging partial failures

Raw artifacts are retained by default. They are not deleted automatically at terminal completion.

Terminal runs may end as either:

- `completed`
- `completed_with_failures`

Both are considered exportable and prunable terminal outcomes.
When raw retention is enabled, those files are only prunable after `Run.export_artifacts(...)` has been called.

## Response parsing

`batchor` treats OpenAI response parsing as a compatibility surface rather than assuming one rigid payload shape.

For text extraction it currently handles:

- Responses API `output` content blocks
- Responses API `output_text`
- Chat Completions `choices[].message.content`
- content-part lists as well as direct strings

Only after those supported locations are checked does empty text become a parse failure for structured-output runs.

For structured outputs:

- the generated JSON Schema is normalized to close object schemas with `additionalProperties: false`
- schema compatibility is validated before submission; unsupported root/optional/open-object shapes fail fast locally
- text is extracted first
- Markdown JSON fences are stripped when present
- the payload is parsed as JSON
- the result is validated through the declared Pydantic model

Validation failures consume item attempts because they represent item-level response failure, not a transient provider control-plane problem.
Successful submitted responses also consume one item attempt. A successful first provider response is exported with `attempt_count == 1`; a counted retryable item failure followed by success is exported with the total consumed attempts.

## Token estimation

Token estimation is `tiktoken`-first:

1. resolve the model encoding through `tiktoken`
2. estimate prompt/request tokens from encoded text when possible
3. fall back to `chars_per_token` heuristics only if tokenizer resolution fails

This matters because OpenAI enqueue-limit behavior is based on estimated submitted tokens, not just request count or file size.

## Enqueue-limit controls

OpenAI-specific enqueue settings live on `OpenAIProviderConfig.enqueue_limits`:

- `enqueued_token_limit`
- `target_ratio`
- `headroom`
- `max_batch_enqueued_tokens`

From those settings, `batchor` derives:

- an effective inflight token budget
- an effective per-batch token limit

Submission is constrained by both token budget and generic chunking rules such as max request count and max request file size.
During `Run.wait()`, each drain cycle polls active batches before submitting new work, so capacity freed by completed batches is visible before the next submit attempt. If that cycle makes durable progress, the wait loop immediately runs another cycle instead of sleeping.

## Batch splitting

`batchor` automatically splits large logical jobs into multiple provider batches when needed.

Splitting considers:

- request count
- request file bytes
- estimated request tokens

Submission claims only a bounded pending-item window per refresh. This is intentional: large backlogs should not pay full prompt-build and token-estimation cost long before provider capacity exists to send them.
The default claim window is still large enough to prepare multiple provider batches per cycle, so runs with high remote enqueue headroom can keep filling capacity without waiting for one local batch at a time.

If a single request exceeds the allowed OpenAI token limit by itself, that item is marked as a permanent item failure instead of aborting the whole run.

## Error handling and retry behavior

Not all failures are treated the same way.

Control-plane failures:

- transient upload/create/poll failures are treated as retryable batch-control-plane failures
- retryable control-plane failures do not consume item attempts
- batch submit failures can trigger batch-level backoff
- transient poll failures do not stall unrelated submissions when other capacity remains
- resume honors existing batch backoff before materializing more source rows
- OpenAI 429 insufficient-quota or billing exhaustion during upload/create/polling auto-pauses the run with `control_reason="openai_insufficient_quota"` instead of continuing through the backlog
- batch-level terminal insufficient-quota failures also auto-pause after submitted rows are reset to pending

Item-level failures:

- successful submitted responses consume attempts
- structured-output parse failures consume attempts
- validation failures consume attempts
- oversized requests become permanent item failures
- item-level insufficient-quota records inside completed batch output are retryable without consuming attempts
- row-level insufficient-quota records emit `openai_insufficient_quota` failure telemetry and record retry backoff before resubmission instead of pausing the whole run

Cleanup behavior:

- if upload succeeds but batch creation fails, `batchor` makes a best-effort attempt to delete the uploaded OpenAI input file
- if upload or batch creation fails because OpenAI reports insufficient quota, local queued items are released back to pending before the run is paused
- if a process dies after local artifact persistence but before durable batch registration, fresh-process resume requeues those items and resubmits from persisted request artifacts

## Run control

Run control is local control-plane state, not a separate OpenAI provider feature:

- `pause` stops new ingestion, new submission, and provider polling
- `resume` restarts those local activities
- `cancel` stops new ingestion/submission, continues polling already-submitted batches, and then marks any remaining local non-terminal items as `run_cancelled`
- automatic quota pause uses the same `paused` control state as manual pause for control-plane and batch-level quota failures, but records `control_reason="openai_insufficient_quota"` so operators can tell why `wait()` exited
- automatic quota pause is ignored once `cancel_requested` is set, because cancellation is not reversible

Provider-side remote batch cancellation is not implemented in v1.

## Current limits

- the docs do not yet provide a full capability matrix across all OpenAI endpoint features
- artifact storage is still local-filesystem-only

## TBD

- exact behavior doc for provider-specific retry classification
- multi-endpoint OpenAI capability matrix
- remote/object-store artifact backend
- provider-side remote cancellation
- richer metrics/export integrations beyond the current callback-based observability hooks
