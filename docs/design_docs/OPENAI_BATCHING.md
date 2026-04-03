# OpenAI Batching

This document describes the OpenAI-specific behavior inside `batchor`.

## Current Behavior

### Request Construction

`batchor` builds JSONL request rows for the OpenAI Batch API and supports the OpenAI endpoints currently implemented by the provider layer.

Structured-output jobs derive JSON Schema from a Pydantic v2 model and send strict schema instructions in the request body. Text jobs omit structured output configuration.

`OpenAIProviderConfig.api_key` stays in the public API, but durable storage persists only the public provider config. Runtime auth resolution prefers the explicit config value and then falls back to `OPENAI_API_KEY`.

`OpenAIProviderConfig.model` accepts either raw strings or the exported `OpenAIModel` enum for IDE-friendly model selection.

For Responses API requests, `OpenAIProviderConfig.reasoning_effort` is forwarded as `body.reasoning.effort` when provided.

### Durable Request Artifacts

Prepared OpenAI request rows are written to durable JSONL artifacts before upload. `batchor` stores per-item pointers to the artifact key, line number, and request hash in the control-plane store.

This lets retry/resume replay the prepared request body without rebuilding the prompt from the original CSV/JSONL source after the request artifact already exists.

The runner now owns request JSONL serialization and stages a local copy from the artifact store for provider upload. The provider contract builds request rows and uploads local files, but it no longer owns durable request-file writes.

Within a single refresh/submission cycle, replay now caches shared request-artifact file contents so multiple items that point at the same JSONL artifact do not reread that file from disk line-by-line.

After a run reaches a terminal state, users can call `Run.prune_artifacts()` to delete those replay files and clear their SQLite pointers. That preserves terminal results while reclaiming the request-side disk footprint.

Before request artifacts exist, built-in CSV and JSONL sources can now resume ingestion from a persisted source checkpoint when the caller re-enters `start(job, run_id=...)` with the same file and config.

Raw OpenAI batch output and error file contents are also persisted locally as artifacts when they are downloaded. Those files are intended for audit/export workflows and are only prunable after `Run.export_artifacts(...)` has been called.
Terminal OpenAI runs may end as either `completed` or `completed_with_failures`. Both states are considered exportable/prunable terminal outcomes.

### Response Parsing

`batchor` treats provider output parsing as a compatibility surface:

- Responses API output can be reconstructed from multiple text/content blocks
- Chat Completions output can be reconstructed from either string content or content-part lists
- empty text is only treated as a parse error after all supported text locations have been checked
### Token Estimation

Token estimation is `tiktoken`-first:

1. resolve the model encoding through `tiktoken`
2. estimate prompt/request tokens from encoded text when possible
3. fall back to `chars_per_token` heuristics only if tokenizer resolution fails

This matches the practical path already used in the parent application and is important for enqueue-limit accuracy.

### Enqueue-Limit Controls

OpenAI-specific enqueue settings live on `OpenAIProviderConfig.enqueue_limits`:

- `enqueued_token_limit`
- `target_ratio`
- `headroom`
- `max_batch_enqueued_tokens`

Effective inflight budget is derived from those settings, and batch submission is constrained by both:

- per-batch request/file limits from `ChunkPolicy`
- effective OpenAI token budget from `enqueue_limits`

### Batch Splitting

`batchor` automatically splits large logical jobs into multiple provider batches when needed.

Splitting currently considers:

- request count
- request file bytes
- estimated request tokens

Submission now claims a bounded pending-item window per refresh instead of materializing the entire pending queue up front. That keeps large durable backlogs from paying full prompt-build and token-estimation cost before there is provider capacity to submit them.

If a single request exceeds the allowed OpenAI token limit by itself, that item is marked as a permanent failure with an OpenAI-specific error instead of aborting the whole run.

### Provider Errors

Transient provider errors and enqueue-capacity failures do not consume item attempts. Validation failures and parse failures do consume item attempts.

If a retryable control-plane failure happens after a request artifact has been written but before the batch is registered, the item can still be retried from the stored request artifact on the next refresh/resume cycle.

If the failure happens after the input file upload but before successful batch creation, `batchor` makes a best-effort attempt to delete the uploaded input file so retries do not accumulate orphaned uploads.

If a process dies after local item claim and artifact persistence but before batch registration, fresh-process resume requeues those `queued_local` items and resubmits from the stored request artifact.

Batch polling uses bounded concurrency when multiple active provider batches exist. Transient `get_batch(...)` failures now emit retry observability but do not apply the run-level submission backoff gate, so one flaky poll does not stall unrelated pending submissions.

## TBD

- exact behavior doc for provider-specific retry classification
- multi-endpoint OpenAI capability matrix
- remote/object-store artifact backend
- richer metrics/export integrations beyond the current callback-based observability hooks
