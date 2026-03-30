# batchor

Structured-first OpenAI Batch runner with durable `Run` handles, Pydantic v2 validation, and local SQLite persistence by default.

## Status

`batchor` lives here as an internal subproject that is ready to be split out later. The current API is designed around long-lived OpenAI Batch jobs:

- one `BatchJob` type with optional `structured_output`
- iterable or file-backed item inputs via `CsvItemSource` and `JsonlItemSource`
- `runner.start(...) -> Run` returns immediately with a durable `run_id`
- `runner.get_run(run_id)` rehydrates a run from storage
- local SQLAlchemy 2 SQLite storage is the default backend
- `BatchRunner(storage="memory")` is available for ephemeral tests and one-off runs
- OpenAI-specific enqueue-limit controls live on `OpenAIProviderConfig.enqueue_limits`

Structured-output rehydration requires a module-level Pydantic model class. If a stored structured model cannot be imported later, `batchor` raises a clear model-resolution error instead of silently returning raw dicts.

## Architecture

The runtime is now built on explicit extension seams instead of hardcoded OpenAI/SQLite branches:

- `ProviderConfig` and `BatchProvider` are the provider-side base contracts
- `ProviderRegistry` owns provider config persistence and provider construction
- `StateStore` is an abstract durable state contract
- `StorageRegistry` owns storage backend construction
- `ItemStatus`, `RunLifecycleStatus`, `ProviderKind`, and `StorageKind` are exported enums

Today the built-in implementations are:

- `OpenAIProviderConfig` + `OpenAIBatchProvider`
- `SQLiteStorage`
- `MemoryStateStore`
- `CsvItemSource`
- `JsonlItemSource`

That keeps the current behavior stable while making future provider and database additions a focused implementation task rather than a runner rewrite.

## Project Layout

```text
batchor/
  README.md
  pyproject.toml
  src/batchor/
    core/
    providers/
    runtime/
    sources/
    storage/
  tests/
```

## Quickstart

```bash
cd batchor
uv sync --all-groups
uv run ty check src
uv run pytest -q
```

## Structured Output

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
        items=[
            BatchItem(item_id="row1", payload={"text": "classify this"}),
        ],
        build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
        structured_output=ClassificationResult,
        provider_config=OpenAIProviderConfig(
            api_key="YOUR_OPENAI_API_KEY",
            model="gpt-4.1",
            enqueue_limits=OpenAIEnqueueLimitConfig(
                enqueued_token_limit=2_000_000,
                target_ratio=0.7,
                headroom=50_000,
                max_batch_enqueued_tokens=500_000,
            ),
        ),
    )
)

print(run.run_id)
run.wait()
results = run.results()
print(results[0].output)
```

`batchor` estimates request tokens with `tiktoken` when available and only falls back to the configured `chars_per_token` heuristic if tokenizer resolution fails.

## Text Mode

```python
from batchor import BatchItem, BatchJob, BatchRunner, OpenAIProviderConfig, PromptParts


runner = BatchRunner(storage="memory")
run = runner.run_and_wait(
    BatchJob(
        items=[BatchItem(item_id="row1", payload="Summarize this text")],
        build_prompt=lambda item: PromptParts(prompt=item.payload),
        provider_config=OpenAIProviderConfig(
            api_key="YOUR_OPENAI_API_KEY",
            model="gpt-4.1",
        ),
    )
)

print(run.results()[0].output_text)
```

## Rehydrating A Run

```python
from batchor import BatchRunner, SQLiteStorage


storage = SQLiteStorage(name="default")
runner = BatchRunner(storage=storage)

run = runner.get_run("batchor_20260329T120000Z_ab12cd34")
snapshot = run.snapshot()

if not run.is_finished:
    run.wait()

print(run.results())
```

## File Sources

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
        provider_config=OpenAIProviderConfig(
            api_key="YOUR_OPENAI_API_KEY",
            model="gpt-4.1",
        ),
    )
)
```

CSV sources work the same way through `CsvItemSource`, with explicit row-to-item mapping callbacks.

## Extension Hooks

```python
from batchor import (
    BatchRunner,
    ProviderRegistry,
    StorageRegistry,
    StorageKind,
)


provider_registry = ProviderRegistry()
storage_registry = StorageRegistry()

runner = BatchRunner(
    provider_registry=provider_registry,
    storage_registry=storage_registry,
)
```

Most users should keep the defaults. The registries exist so new vendors and durable backends can plug in cleanly later.
