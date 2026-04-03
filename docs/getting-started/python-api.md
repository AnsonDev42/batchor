# Python API

The Python API is the primary product surface. The CLI is intentionally a subset of it.

## Typical workflow

Most applications follow this shape:

1. Define the input items.
2. Define `build_prompt`.
3. Choose a storage backend.
4. Call `runner.start(...)` or `runner.run_and_wait(...)`.
5. Inspect `Run.summary()` while in flight.
6. Call terminal `Run.results()` when the run finishes.

## Minimal text job

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

Use `storage="memory"` only for tests or short-lived local experiments. For durable runs, use the default SQLite storage or an explicit backend.

## Structured output job

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
result = run.results()[0]
print(result.output)
print(result.output_text)
```

Notes:

- `structured_output` must be a module-level Pydantic model class if you want fresh-process rehydration to work
- `output` is the parsed Pydantic object
- `output_text` preserves the raw text that was parsed

## Durable run lifecycle

`BatchRunner.start(...)` returns immediately with a durable `Run`.

```python
from batchor import BatchRunner


runner = BatchRunner()
run = runner.start(job)

print(run.run_id)
print(run.status)
print(run.summary())

run.wait(timeout=300)
results = run.results()
```

Important behavior:

- `status` is cached on the `Run` handle
- `refresh()` explicitly talks to storage/provider and updates the cached summary
- `wait()` repeatedly refreshes until the run is terminal
- `results()` is terminal-only and raises if the run is still active

## Rehydrate an existing run

```python
from batchor import BatchRunner, SQLiteStorage


storage = SQLiteStorage(name="default")
runner = BatchRunner(storage=storage)

run = runner.get_run("batchor_20260329T120000Z_ab12cd34")
print(run.summary())
```

Rehydration succeeds only if the runner can still resolve what the run needs:

- the same durable storage
- access to any required artifacts for retry/resume
- importable structured-output model classes
- usable credentials when a refresh needs to talk to the provider

## File-backed sources

Use `CsvItemSource` or `JsonlItemSource` when the input already exists on disk.

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

If the source file and job config still match the persisted checkpoint, rerunning `start(job, run_id=...)` resumes from the last durable source position instead of duplicating already-materialized items.

## Choosing storage

Use:

- `BatchRunner()` for default SQLite durability
- `BatchRunner(storage="memory")` for ephemeral tests
- `BatchRunner(storage=SQLiteStorage(...))` when you want explicit database placement
- `BatchRunner(storage=PostgresStorage(...), artifact_store=LocalArtifactStore(...))` when you need a shared control plane

Example:

```python
from batchor import BatchRunner, LocalArtifactStore, PostgresStorage


runner = BatchRunner(
    storage=PostgresStorage(dsn="postgresql+psycopg://localhost/batchor"),
    artifact_store=LocalArtifactStore("/mnt/batchor-artifacts"),
)
```

## Artifacts, export, and prune

`batchor` stores request artifacts for replay and raw output artifacts for audit/export.

```python
export = run.export_artifacts("exports")
print(export.manifest_path)

prune = run.prune_artifacts()
print(prune.removed_artifact_paths)
```

Rules:

- artifact export/prune is terminal-only
- request artifacts can be pruned directly after terminal completion
- raw output/error artifacts require `export_artifacts(...)` before they can be pruned

## Which API should you read next?

- Read [How It Works](how-it-works.md) if you want the mental model behind the lifecycle.
- Read [API Reference](../reference/api.md) for exact signatures.
- Read [Storage & Runs](../design_docs/STORAGE_AND_RUNS.md) if you are operating durable runs at scale.
