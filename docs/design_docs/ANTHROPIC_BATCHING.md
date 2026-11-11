# Anthropic Batching

The built-in Anthropic provider targets the Claude Message Batches API.

## Request mapping

Each Batchor item attempt becomes one Anthropic batch request:

- Batchor's durable correlation identifier is deterministically hashed into Anthropic's 64-character-safe `custom_id` alphabet
- `PromptParts.prompt` becomes one user message
- `PromptParts.system_prompt` becomes the top-level `system` parameter
- `model` and `max_tokens` come from `AnthropicProviderConfig`
- `message_params` supplies optional Messages API parameters
- structured output schemas use `output_config.format`

Batchor rejects provider-owned fields in `message_params` so replayed requests cannot silently change model, token limit, messages, system prompt, or streaming behavior.

## Submission and lifecycle

Anthropic accepts request objects directly rather than an uploaded JSONL file. The provider therefore stages Batchor's durable request artifact in memory only for the upload/create boundary, then calls `messages.batches.create(requests=...)`.

Anthropic `in_progress` maps to Batchor's active status and `ended` maps to `completed`. Once ended, polling returns a deterministic result identifier. The separate download call streams `messages.batches.results(...)` into Batchor's raw output artifact, so the runtime can check its deadline between status retrieval and download. Result payloads are not retained in a provider-wide cache, and a fresh provider can resolve the identifier. Result order is not assumed; correlation always uses `custom_id`.

The four Anthropic result types map as follows:

- `succeeded`: Batchor success
- `errored`, `canceled`, and `expired`: Batchor item error, subject to the configured retry policy

Batch creation disables automatic SDK retries through
`with_options(max_retries=0)` when supported by the client. A failed create is
then handled by the runtime's indeterminate-submission flow instead of another
implicit Messages Batch POST.

## Limits and retention

Anthropic currently limits a Message Batch to 100,000 requests or 256 MB. Batchor's default chunk limits are lower than both limits. Claude batch results remain available from Anthropic for 29 days; Batchor downloads and retains them according to its artifact policy, so durable runs do not depend on that remote retention window after ingestion.

Message Batches are not eligible for Anthropic Zero Data Retention. Users should account for Anthropic's batch retention policy when selecting workloads.
