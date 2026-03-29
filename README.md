# batchor

Structured-first OpenAI Batch runner with durable `Run` handles, Pydantic v2 validation, and local SQLite persistence by default.

## Status

`batchor` lives here as an internal subproject that is ready to be split out later. The current API is designed around long-lived OpenAI Batch jobs:

- one `BatchJob` type with optional `structured_output`
- `runner.start(...) -> Run` returns immediately with a durable `run_id`
- `runner.get_run(run_id)` rehydrates a run from storage
- local SQLAlchemy 2 SQLite storage is the default backend
- `BatchRunner(storage="memory")` is available for ephemeral tests and one-off runs

Structured-output rehydration requires a module-level Pydantic model class. If a stored structured model cannot be imported later, `batchor` raises a clear model-resolution error instead of silently returning raw dicts.

## Project Layout

```text
batchor/
  README.md
  pyproject.toml
  src/batchor/
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

from batchor import BatchItem, BatchJob, BatchRunner, OpenAIProviderConfig, PromptParts


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
        ),
    )
)

print(run.run_id)
run.wait()
results = run.results()
print(results[0].output)
```

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
