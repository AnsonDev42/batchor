# Use Cases

This page collects practical `batchor` patterns for teams that want to get from
"what is the runtime model?" to "what code do I actually write?" quickly.

## 1. Simplest possible library run

Use this when you want the smallest end-to-end example: one item in memory, one
prompt, one result.

```python
from batchor import BatchItem, BatchJob, BatchRunner, OpenAIProviderConfig, PromptParts


runner = BatchRunner(storage="memory")
run = runner.run_and_wait(
    BatchJob(
        items=[BatchItem(item_id="row1", payload={"text": "Summarize this text"})],
        build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
        provider_config=OpenAIProviderConfig(
            model="gpt-4.1",
            api_key="YOUR_OPENAI_API_KEY",
        ),
    )
)

print(run.results()[0].output_text)
```

Why this is useful:

- fastest way to verify your API key and prompt wiring
- good for local experiments and tests
- keeps all state in memory, so there is nothing to rehydrate later

Use `storage="memory"` only for short-lived experiments. For durable runs, use
the default SQLite storage or an explicit backend.

## 2. Single file in, durable run out

Use this when your input already lives in one CSV or JSONL file and you want the
smallest durable workflow.

```python
from batchor import BatchJob, BatchRunner, CsvItemSource, OpenAIProviderConfig, PromptParts


source = CsvItemSource(
    "input/customers.csv",
    item_id_from_row=lambda row: row["id"],
    payload_from_row=lambda row: {"text": row["text"]},
)

runner = BatchRunner()
run = runner.start(
    BatchJob(
        items=source,
        build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
        provider_config=OpenAIProviderConfig(model="gpt-4.1"),
    ),
    run_id="customers_20260403",
)

run.wait()
for result in run.results():
    print(result.item_id, result.output_text)
```

Why this is the normal production baseline:

- SQLite durability is enabled by default
- the file source supports deterministic resume
- rerunning `start(job, run_id=...)` can continue from a persisted checkpoint

## 3. Multiple CSV files, one runner, one logical run

Use this when you have already selected the files you want to process and want
one durable run across all of them.

```python
from batchor import (
    BatchJob,
    BatchRunner,
    CompositeItemSource,
    CsvItemSource,
    OpenAIProviderConfig,
    PromptParts,
)


source = CompositeItemSource(
    [
        CsvItemSource(
            "input/customers-part-1.csv",
            item_id_from_row=lambda row: row["id"],
            payload_from_row=lambda row: {"text": row["text"]},
        ),
        CsvItemSource(
            "input/customers-part-2.csv",
            item_id_from_row=lambda row: row["id"],
            payload_from_row=lambda row: {"text": row["text"]},
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
    run_id="customer_export_april_20260403",
)
```

Important behavior:

- file order is part of the logical source identity for resume
- duplicate row IDs across files can coexist because `CompositeItemSource`
  namespaces them for the durable run
- the original per-file row ID is kept in
  `metadata["batchor_lineage"]["source_primary_key"]`

## 4. Structured outputs for application code

Use this when downstream code wants validated objects instead of raw text.

```python
from pydantic import BaseModel

from batchor import BatchItem, BatchJob, BatchRunner, OpenAIProviderConfig, PromptParts


class TicketSummary(BaseModel):
    label: str
    priority: str


runner = BatchRunner()
run = runner.run_and_wait(
    BatchJob(
        items=[
            BatchItem(
                item_id="ticket-1",
                payload={"text": "Customer says checkout fails on Safari."},
            )
        ],
        build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
        structured_output=TicketSummary,
        provider_config=OpenAIProviderConfig(model="gpt-4.1"),
    )
)

result = run.results()[0]
print(result.output.label)
print(result.output.priority)
```

Keep the model class importable at module scope if you want fresh-process
rehydration to keep working.

## 5. Integrate `batchor` into a pipeline step

`batchor` works best as the execution layer inside a larger application or data
pipeline. The pipeline decides which records to process; `batchor` owns durable
execution once you hand those items to a `BatchJob`.

```python
from batchor import BatchItem, BatchJob, BatchRunner, OpenAIProviderConfig, PromptParts


def run_partition(records: list[dict], partition_id: str) -> None:
    runner = BatchRunner()
    run = runner.start(
        BatchJob(
            items=[
                BatchItem(
                    item_id=str(record["id"]),
                    payload={"text": record["body"]},
                    metadata={"partition_id": partition_id},
                )
                for record in records
            ],
            build_prompt=lambda item: PromptParts(
                prompt=f"Summarize this customer note:\n\n{item.payload['text']}"
            ),
            provider_config=OpenAIProviderConfig(model="gpt-4.1"),
        ),
        run_id=f"notes_{partition_id}",
    )

    run.wait()

    for result in run.results():
        persist_summary(
            item_id=result.item_id,
            partition_id=result.metadata["partition_id"],
            summary=result.output_text,
        )
```

This split is the intended boundary:

- your pipeline owns input discovery and downstream side effects
- `batchor` owns durable batch execution, retry, resume, and artifact handling

## Choosing the right pattern

- Start with the minimal in-memory example if you are only proving the prompt
  path.
- Use a single deterministic file source when the input already exists on disk.
- Use `CompositeItemSource` when you want one run across multiple explicit
  files.
- Use structured outputs when application code consumes typed objects.
- Use a pipeline wrapper when `batchor` is one durable step inside a broader
  workflow.
