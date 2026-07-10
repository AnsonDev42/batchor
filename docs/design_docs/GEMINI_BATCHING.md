# Gemini Batching

This document describes the Gemini-specific behavior inside `batchor`.

Status: implemented for text-only Python API and CLI jobs through both the Gemini Developer API and Vertex AI. Multimodal request construction remains out of scope.

## API split

The two Google backends do not expose the same batch transport:

| Backend | Input | Output | Authentication |
| --- | --- | --- | --- |
| Gemini Developer API | inline requests below 20 MB; Files API JSONL for larger batches | inline responses or a Files API JSONL file | API key |
| Vertex AI | Cloud Storage JSONL (BigQuery is supported by Google but not by batchor) | Cloud Storage JSONL | Application Default Credentials |

`GeminiProviderConfig.input_mode="auto"` follows that table. `inline`, `file`, and `gcs` are explicit overrides, and incompatible backend/mode combinations fail locally.

Vertex selection is explicit through `vertexai=True` or ambient through `GOOGLE_GENAI_USE_VERTEXAI=true`. Project and location resolve from config first, then `GOOGLE_CLOUD_PROJECT` and `GOOGLE_CLOUD_LOCATION`. Vertex also requires `gcs_uri="gs://bucket/prefix"` for request and output staging.

## Request construction and correlation

Developer file requests use the documented JSONL shape:

```json
{"key":"row1:a1","request":{"contents":[{"role":"user","parts":[{"text":"Summarize this text"}]}]}}
```

Inline mode converts those durable request artifacts to SDK `InlinedRequest` objects and carries the key in request metadata. Returned inline metadata is converted back to the same JSONL-style record before the generic runtime consumes it.

Vertex output returns the original `request`, a `response`, and a string `status`; it does not return the Developer API `key`. For Vertex, batchor hashes the internal attempt identifier into a GCP-safe `request.labels.batchor_key` value. The original request and label round-trip through GCS output, so the runtime can correlate each row without relying on output ordering.

`PromptParts.prompt` becomes a user text part. `PromptParts.system_prompt` becomes `system_instruction`. `GeminiProviderConfig.generation_config` is copied into every request.

Structured jobs additionally set:

```json
{
  "generation_config": {
    "response_mime_type": "application/json",
    "response_json_schema": {"type": "object"}
  }
}
```

The schema comes from the same `structured_output=` Pydantic model used by OpenAI jobs and returned JSON is validated with that model.

## Lifecycle

Developer API auto mode:

1. use inline requests when the staged JSONL is under 20 MB, otherwise upload it through the Files API
2. create and poll the batch with `client.batches`
3. normalize inline responses or download the result file

Vertex AI:

1. upload the durable request JSONL under `gcs_uri/inputs/`
2. create a batch with that `gs://` source and a unique `gcs_uri/outputs/` destination
3. poll with `client.batches.get`
4. read every output object from the returned `output_info.gcs_output_directory`

Terminal states normalize to batchor's generic `completed`, `failed`, `cancelled`, and `expired` states. Vertex rows with a non-empty `status` become item errors; rows with an object `response` and empty status become successes.

## Durable replay

Provider-specific request rows remain in batchor's local artifact store. Retry or fresh-process resume replaces only the provider correlation identifier. Developer rows update `key`; Vertex rows update the hashed correlation label. The prompt and generation request otherwise remain unchanged.

GCS input/output objects are remote provider payloads, separate from batchor's durable local artifact store. A failed create deletes its just-uploaded input on a best-effort basis. Successfully submitted Vertex payloads follow the bucket's retention/lifecycle policy.

## Limits verified July 10, 2026

Current official Vertex behavior:

- no predefined Gemini batch inference quota or enqueue-token limit; work can queue against a shared resource pool
- at most 200,000 requests per job
- at most 1 GB for a Cloud Storage input file
- jobs can queue for up to 72 hours and generally process within 24 hours after starting

Batchor therefore does not apply OpenAI enqueue-token budgeting to Gemini. Generic `ChunkPolicy` defaults remain more conservative at 50,000 requests and 150 MiB, and callers may raise them within Google's limits.

Developer API limits differ: inline batch creation must stay below 20 MB, input files may be up to 2 GB, concurrent batches are currently limited to 100, and model/tier-specific enqueued-token limits still apply. Batchor automatically switches Developer auto mode from inline to Files API by request size; it does not yet model Developer tier-specific token quotas.

## Current limits

- the CLI exposes the common text-job configuration; advanced source, storage, and lifecycle composition remains Python-first.
- text request construction only.
- Vertex BigQuery input/output is not implemented.
- provider-side remote cancellation is not implemented.

## References

- [Vertex AI Gemini batch inference](https://cloud.google.com/vertex-ai/generative-ai/docs/multimodal/batch-prediction-gemini)
- [Vertex AI batch inference from Cloud Storage](https://cloud.google.com/vertex-ai/generative-ai/docs/multimodal/batch-prediction-from-cloud-storage)
- [Vertex AI quotas and limits](https://cloud.google.com/vertex-ai/docs/quotas)
- [Gemini Developer API Batch API](https://ai.google.dev/gemini-api/docs/batch-api)
- [Google Gen AI Python SDK batch documentation](https://googleapis.github.io/python-genai/#batch-prediction)
