# batchor

`batchor` is a durable OpenAI Batch runner for Python teams that want:

- typed Pydantic results
- resumable durable runs
- replayable request artifacts
- a small operator CLI for CSV and JSONL jobs

It is intentionally narrow today: OpenAI-first, SQLite-first, and library-first.

## What problem it solves

Most OpenAI Batch examples stop at "upload a JSONL file and poll until it finishes." Real workloads usually need more than that:

- durable state so a process restart does not lose the run
- typed result parsing for structured outputs
- artifact retention so submitted requests can be replayed or audited
- clear export and prune steps once a run is done
- a stable run handle that can be rehydrated later

`batchor` packages those concerns behind a small public surface:

- `BatchItem` describes one logical item of work.
- `BatchJob` describes how to turn items into provider requests.
- `BatchRunner` owns durable execution.
- `Run` is the durable handle you refresh, wait on, inspect, export, and prune.

## Current scope

Built-in implementations:

- `OpenAIProviderConfig` + `OpenAIBatchProvider`
- `SQLiteStorage`
- `PostgresStorage` as an opt-in durable control-plane backend
- `MemoryStateStore`
- `LocalArtifactStore`
- `CsvItemSource`
- `JsonlItemSource`

Important constraints:

- the Python API is broader than the CLI
- the CLI supports file-backed inputs only
- the built-in CLI uses SQLite durability only
- structured-output rehydration requires an importable module-level Pydantic model
- raw output artifacts are retained by default and must be exported before raw pruning

## Mental model

The normal lifecycle is:

1. Build a `BatchJob` with items, prompt-building logic, and provider config.
2. Call `BatchRunner.start(...)` to create or resume a durable run.
3. Keep the returned `Run` handle and call `refresh()` or `wait()`.
4. Read `summary()`, `snapshot()`, or terminal `results()`.
5. When the run is finished, optionally `export_artifacts(...)`.
6. When retention requirements are satisfied, `prune_artifacts(...)`.

Durability is split on purpose:

- storage tracks run state, item state, attempts, batches, and artifact pointers
- the artifact store keeps replayable request JSONL and downloaded raw batch payloads

That split is what allows retries and fresh-process resume without keeping every request inline in the control-plane store.

## Install

```bash
pip install batchor
```

Supported Python versions:

- `3.12`
- `3.13`

## Authentication

For Python API usage, auth resolution is:

1. explicit `OpenAIProviderConfig(api_key=...)`
2. ambient `OPENAI_API_KEY`

The Python library does not auto-load `.env`.

The CLI loads a local `.env` as a convenience for interactive/operator usage.

## Python quickstart

### Text job

```python
from batchor import BatchItem, BatchJob, BatchRunner, OpenAIProviderConfig, PromptParts


runner = BatchRunner(storage="memory")
run = runner.run_and_wait(
    BatchJob(
        items=[BatchItem(item_id="row1", payload="Summarize this text")],
        build_prompt=lambda item: PromptParts(prompt=item.payload),
        provider_config=OpenAIProviderConfig(
            model="gpt-4.1",
            api_key="YOUR_OPENAI_API_KEY",
        ),
    )
)

print(run.results()[0].output_text)
```

### Structured output

```python
from pydantic import BaseModel

from batchor import (
    BatchItem,
    BatchJob,
    BatchRunner,
    OpenAIEnqueueLimitConfig,
    OpenAIProviderConfig,
    PromptParts,
)


class ClassificationResult(BaseModel):
    label: str
    score: float


runner = BatchRunner()
run = runner.start(
    BatchJob(
        items=[BatchItem(item_id="row1", payload={"text": "classify this"})],
        build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
        structured_output=ClassificationResult,
        provider_config=OpenAIProviderConfig(
            model="gpt-4.1",
            api_key="YOUR_OPENAI_API_KEY",
            enqueue_limits=OpenAIEnqueueLimitConfig(
                enqueued_token_limit=2_000_000,
                target_ratio=0.7,
                headroom=50_000,
                max_batch_enqueued_tokens=500_000,
            ),
        ),
    )
)

run.wait()
print(run.results()[0].output)
```

### Rehydrate a durable run

```python
from batchor import BatchRunner, SQLiteStorage


storage = SQLiteStorage(name="default")
runner = BatchRunner(storage=storage)

run = runner.get_run("batchor_20260329T120000Z_ab12cd34")
print(run.summary())
```

### File-backed sources

```python
from batchor import BatchJob, BatchRunner, JsonlItemSource, OpenAIProviderConfig, PromptParts


source = JsonlItemSource(
    "input/items.jsonl",
    item_id_from_row=lambda row: str(row["id"]) if isinstance(row, dict) else "",
    payload_from_row=lambda row: {"text": row["text"]} if isinstance(row, dict) else {},
)

runner = BatchRunner()
run = runner.start(
    BatchJob(
        items=source,
        build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
        provider_config=OpenAIProviderConfig(model="gpt-4.1"),
    ),
    run_id="customer_export_20260403",
)
```

If the source file and job config still match the persisted checkpoint, calling `start(job, run_id=...)` again resumes ingestion from the last durable source position instead of duplicating already-materialized items.

## CLI quickstart

The CLI is intentionally narrower than the Python API:

- file-backed inputs only
- CSV and JSONL only
- SQLite-backed durable runs only

Start a run from JSONL:

```bash
batchor start \
  --input input/items.jsonl \
  --id-field id \
  --prompt-field text \
  --model gpt-4.1
```

Inspect and operate on the durable run:

```bash
batchor status --run-id batchor_20260403T120000Z_ab12cd34
batchor wait --run-id batchor_20260403T120000Z_ab12cd34
batchor results --run-id batchor_20260403T120000Z_ab12cd34 --output results.jsonl
batchor export-artifacts --run-id batchor_20260403T120000Z_ab12cd34 --destination-dir exports
batchor prune-artifacts --run-id batchor_20260403T120000Z_ab12cd34
```

The CLI prints JSON summaries by default.

## Documentation guide

- `docs/getting-started/how-it-works.md`: the runtime mental model
- `docs/getting-started/python-api.md`: Python-first workflows
- `docs/getting-started/cli.md`: operator CLI workflows
- `docs/reference/api.md`: generated API surface
- `docs/design_docs/ARCHITECTURE.md`: package boundaries
- `docs/design_docs/OPENAI_BATCHING.md`: OpenAI-specific behavior
- `docs/design_docs/STORAGE_AND_RUNS.md`: durability, rehydration, and artifacts
- `docs/smoke-test.md`: minimum validation bar

## License

This project is licensed under PolyForm Noncommercial 1.0.0. See [LICENSE](LICENSE).
