# Python pipelines

Use the Python API when Batchor is one durable step inside a research or application pipeline.

## Minimal durable adapter

```python
from collections.abc import Iterable

from batchor import BatchItem, BatchJob, BatchRunner, OpenAIProviderConfig, PromptParts


def start_summaries(records: Iterable[dict[str, str]], dataset_version: str):
    runner = BatchRunner()
    job = BatchJob(
        items=[
            BatchItem(
                item_id=record["id"],
                payload={"text": record["text"]},
                metadata={"dataset_version": dataset_version},
            )
            for record in records
        ],
        build_prompt=lambda item: PromptParts(
            prompt=f"Summarize this research record:\n\n{item.payload['text']}"
        ),
        provider_config=OpenAIProviderConfig(model="gpt-4.1"),
    )
    return runner.start(job, run_id=f"summaries_{dataset_version}")
```

Persist or expose the returned run ID. Use `run.wait()` only where blocking is acceptable; otherwise refresh the run from a worker or operator process.

## Structured output

Define the model at module scope:

```python
from pydantic import BaseModel


class Finding(BaseModel):
    theme: str
    evidence: str
    confidence: float
```

Pass it as `structured_output=Finding` on `BatchJob`. Consume validated `result.output`, while handling failed items explicitly. For OpenAI strict schemas, object fields must satisfy the provider's required/closed-object rules.

For Anthropic, install `batchor[anthropic]` and use `AnthropicProviderConfig(model=..., max_tokens=...)`. Extra Messages parameters belong in `message_params`; Batchor maps structured output through `output_config.format`.

## Durable file sources

Prefer `CsvItemSource`, `JsonlItemSource`, or `ParquetItemSource` for large files. Give every row a stable `item_id`. Re-enter `start(job, run_id=the_same_id)` only with the same logical source and compatible job configuration.

Use `CompositeItemSource` for several ordered files in one run. Changing child order changes source identity.

## Storage choice

- `BatchRunner()` uses durable SQLite state and local artifacts by default.
- `BatchRunner(storage="memory")` is for tests and disposable examples.
- For multi-machine workers, use `PostgresStorage` and an artifact root visible to every worker that may resume or retry the run.

## Project tests

Keep provider calls behind the adapter boundary. Test with a fake provider and small synthetic records. Assert stable item IDs, deterministic prompt construction, structured-output validation, run-ID persistence, and downstream handling of both successful and failed items.
