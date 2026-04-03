# OpenAI Batching

This document describes the OpenAI-specific behavior inside `batchor`.

## Current Behavior

### Request Construction

`batchor` builds JSONL request rows for the OpenAI Batch API and supports the OpenAI endpoints currently implemented by the provider layer.

Structured-output jobs derive JSON Schema from a Pydantic v2 model and send strict schema instructions in the request body. Text jobs omit structured output configuration.

### Durable Request Artifacts

For SQLite-backed runs, prepared OpenAI request rows are written to durable JSONL artifacts before upload. `batchor` stores per-item pointers to the artifact path, line number, and request hash in SQLite.

This lets retry/resume replay the prepared request body without rebuilding the prompt from the original CSV/JSONL source after the request artifact already exists.

After a run reaches a terminal state, users can call `Run.prune_artifacts()` to delete those replay files and clear their SQLite pointers. That preserves terminal results while reclaiming the request-side disk footprint.

Before request artifacts exist, built-in CSV and JSONL sources can now resume ingestion from a persisted source checkpoint when the caller re-enters `start(job, run_id=...)` with the same file and config.

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

If a single request exceeds the allowed OpenAI token limit by itself, that item is marked as a permanent failure with an OpenAI-specific error instead of aborting the whole run.

### Provider Errors

Transient provider errors and enqueue-capacity failures do not consume item attempts. Validation failures and parse failures do consume item attempts.

If a retryable control-plane failure happens after a request artifact has been written but before the batch is registered, the item can still be retried from the stored request artifact on the next refresh/resume cycle.

## TBD

- exact behavior doc for provider-specific retry classification
- multi-endpoint OpenAI capability matrix
- provider metrics and observability contract
