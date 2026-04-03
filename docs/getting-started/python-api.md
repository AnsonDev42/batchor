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

## Deterministic sources

Use `CsvItemSource`, `JsonlItemSource`, or `ParquetItemSource` when the input already exists on disk.
Use `CompositeItemSource` when you have already selected and ordered multiple checkpointed sources and want them to behave like one logical run input.

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

If the source file and job config still match the persisted checkpoint, rerunning `start(job, run_id=...)` resumes from the last durable source position instead of duplicating previously materialized items.

Built-in deterministic sources today are:

- `CompositeItemSource`
- `CsvItemSource`
- `JsonlItemSource`
- `ParquetItemSource`

`ParquetItemSource` supports column projection so large datasets can expose only the columns needed for `item_id`, payload, and metadata extraction:

```python
from batchor import BatchJob, BatchRunner, OpenAIProviderConfig, ParquetItemSource, PromptParts


source = ParquetItemSource(
    "input/items.parquet",
    item_id_from_row=lambda row: str(row["id"]),
    payload_from_row=lambda row: {"text": str(row["text"])},
    columns=["id", "text"],
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

To combine multiple deterministic inputs into one logical run, wrap them explicitly:

```python
from batchor import (
    BatchJob,
    BatchRunner,
    CompositeItemSource,
    CsvItemSource,
    JsonlItemSource,
    OpenAIProviderConfig,
    PromptParts,
)


source = CompositeItemSource(
    [
        CsvItemSource(
            "input/items-a.csv",
            item_id_from_row=lambda row: row["id"],
            payload_from_row=lambda row: {"text": row["text"]},
        ),
        JsonlItemSource(
            "input/items-b.jsonl",
            item_id_from_row=lambda row: str(row["id"]) if isinstance(row, dict) else "",
            payload_from_row=lambda row: {"text": row["text"]} if isinstance(row, dict) else {},
        ),
    ]
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

`CompositeItemSource` auto-namespaces each child source's `item_id`, so duplicate row IDs across files can coexist in one run.
The original per-source row ID stays in `metadata["batchor_lineage"]["source_primary_key"]`, and the namespace used for the durable run `item_id` is stored in `metadata["batchor_lineage"]["source_namespace"]`.
Changing the child source order changes the logical source identity for resume.

For custom deterministic adapters, implement `CheckpointedItemSource`.
`CompositeItemSource` can wrap those adapters too.
Arbitrary iterables and live DB cursors are still outside the durable-resume contract unless they can provide a stable source identity plus opaque resume checkpoint.

## Run control

```python
from batchor import BatchRunner, RunControlState, SQLiteStorage


runner = BatchRunner(storage=SQLiteStorage(name="default"))
run = runner.get_run("batchor_20260403T120000Z_ab12cd34")

run.pause()
assert run.summary().control_state is RunControlState.PAUSED

run.resume()
run.cancel()
```

`cancel()` is drain-style in v1: `batchor` stops new ingestion and submission, keeps polling already-submitted provider batches, then permanently fails remaining local non-terminal items with `run_cancelled`.
Provider-side remote batch cancellation is still `TBD`.

## Incremental terminal results

```python
page = run.read_terminal_results(after_sequence=0, limit=100)

for item in page.items:
    print(item.item_id, item.status)

export = run.export_terminal_results(
    "exports/partial-results.jsonl",
    after_sequence=0,
    append=False,
    limit=100,
)
print(export.next_after_sequence)
```

Incremental reads and exports only return items that are already in a terminal item state. `Run.results()` remains the full terminal-run API.

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

## Artifacts, export, prune, and retention

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

Built-in sources reserve `metadata["batchor_lineage"]` for lightweight join metadata such as:

- `source_ref`
- `partition_id`
- `source_item_index`
- `source_primary_key`
- `source_namespace`

Runs can also opt out of raw output/error artifact retention while keeping durable request replay:

```python
from batchor import ArtifactPolicy, BatchJob


job = BatchJob(
    ...,
    artifact_policy=ArtifactPolicy(persist_raw_output_artifacts=False),
)
```

This policy is library-first in the current release; the CLI still uses the default raw-artifact retention behavior.

## Which API should you read next?

- Read [How It Works](how-it-works.md) if you want the mental model behind the lifecycle.
- Read [API Reference](../reference/api.md) for exact signatures.
- Read [Storage & Runs](../design_docs/STORAGE_AND_RUNS.md) if you are operating durable runs at scale.
