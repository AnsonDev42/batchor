# Python API

## Text jobs

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

## Structured output

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

## Rehydrate a durable run

```python
from batchor import BatchRunner, SQLiteStorage


storage = SQLiteStorage(name="default")
runner = BatchRunner(storage=storage)

run = runner.get_run("batchor_20260329T120000Z_ab12cd34")
print(run.summary())
```

## File-backed sources

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
