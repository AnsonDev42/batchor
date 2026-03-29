# batchor

Structured-first OpenAI Batch runner with Pydantic v2-validated results, retry-aware polling, and a simple in-memory state layer.

## Status

`batchor` is packaged here as a standalone subproject so it can evolve independently from `ai_adoption`. The current implementation is focused on:

- OpenAI Batch request construction and polling
- strict structured output using Pydantic v2 models
- text-mode fallback for non-structured jobs
- batch-level backoff for transient and enqueue-limit failures
- item-level retry/permanent-failure handling for invalid JSON and validation failures

The current v1 ships with an in-memory state store. The public runner and state interfaces are designed so a durable backend can be added later without changing the core API.

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
uv run pytest -q
```

### Structured Output

```python
from pydantic import BaseModel

from batchor import (
    BatchItem,
    BatchRunner,
    OpenAIProviderConfig,
    PromptParts,
    StructuredBatchJob,
)


class ClassificationResult(BaseModel):
    label: str
    score: float


runner = BatchRunner()
summary = runner.run_and_wait(
    StructuredBatchJob(
        items=[
            BatchItem(item_id="row1", payload={"text": "classify this"}),
        ],
        build_prompt=lambda item: PromptParts(prompt=item.payload["text"]),
        output_model=ClassificationResult,
        provider_config=OpenAIProviderConfig(
            api_key="YOUR_OPENAI_API_KEY",
            model="gpt-4.1",
        ),
    )
)

results = runner.results(summary.run_id)
print(results[0].output)
```

### Text Mode

```python
from batchor import (
    BatchItem,
    BatchRunner,
    OpenAIProviderConfig,
    PromptParts,
    TextBatchJob,
)


runner = BatchRunner()
summary = runner.run_and_wait(
    TextBatchJob(
        items=[BatchItem(item_id="row1", payload="Summarize this text")],
        build_prompt=lambda item: PromptParts(prompt=item.payload),
        provider_config=OpenAIProviderConfig(
            api_key="YOUR_OPENAI_API_KEY",
            model="gpt-4.1",
        ),
    )
)

results = runner.results(summary.run_id)
print(results[0].output_text)
```
