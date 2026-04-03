# batchor

`batchor` is a durable OpenAI Batch runner with typed Pydantic results, local SQLite persistence, and a small operator CLI for file-backed text jobs.

## Status

`batchor` is published as an alpha `0.x` package:

- library-first, OpenAI-first, SQLite-first
- durable `Run` handles with rehydration through local storage
- typed structured-output parsing for Python API users
- replayable request artifacts plus explicit export/prune lifecycle for terminal runs, including partial-failure runs
- Typer-based CLI for CSV/JSONL text jobs and run operations

The current built-in implementations are:

- `OpenAIProviderConfig` + `OpenAIBatchProvider`
- `SQLiteStorage`
- `MemoryStateStore`
- `CsvItemSource`
- `JsonlItemSource`

For IDE-friendly model selection, `batchor` exports typed OpenAI model names through `OpenAIModel`, while still accepting raw strings in `OpenAIProviderConfig(model=...)`.

## License

This project is licensed under PolyForm Noncommercial 1.0.0. See [LICENSE](LICENSE).

## Install

```bash
pip install batchor
```

Python support:

- `3.12`
- `3.13`

## Auth

For Python API usage, auth resolution is:

1. explicit `OpenAIProviderConfig(api_key=...)`
2. ambient `OPENAI_API_KEY`

The Python library does not auto-load `.env`.

The CLI loads a local `.env` as a convenience for interactive/operator use, then uses `OPENAI_API_KEY`.

## Python Quickstart

### Text jobs

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

You can also use typed model names:

```python
from batchor import OpenAIModel, OpenAIProviderConfig

config = OpenAIProviderConfig(model=OpenAIModel.GPT_5_NANO)
```

### Rehydrate a durable run

```python
from batchor import BatchRunner, SQLiteStorage


storage = SQLiteStorage(name="default")
runner = BatchRunner(storage=storage)

run = runner.get_run("batchor_20260329T120000Z_ab12cd34")
print(run.summary())
```

### File-backed input sources

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

## CLI Quickstart

The CLI is intentionally narrow in v1:

- file-backed inputs only
- CSV and JSONL only
- SQLite-backed durable runs only

It supports both text jobs and structured-output jobs. Structured output requires an importable module-level Pydantic model class.

Create a local `.env`:

```bash
echo "OPENAI_API_KEY=sk-..." > .env
```

Start a run from JSONL:

```bash
batchor start \
  --input input/items.jsonl \
  --id-field id \
  --prompt-field text \
  --model gpt-4.1
```

Start a run from CSV using a prompt template:

<<<<<<< HEAD
```bash
batchor start \
  --input input/items.csv \
  --id-field id \
  --prompt-template "Summarize: {text}" \
  --model gpt-4.1
||||||| parent of d539c5d (Handle completed-with-failures terminal runs)
## Raw Output Export And Retention

For completed batches, `batchor` also persists raw provider output and error JSONL beside the run artifacts. Those raw files are treated as user-facing evidence, not just replay state.

```python
export = run.export_artifacts("exports")
print(export.manifest_path)

run.prune_artifacts(include_raw_output_artifacts=True)
=======
## Raw Output Export And Retention

For terminal batches, `batchor` also persists raw provider output and error JSONL beside the run artifacts. Those raw files are treated as user-facing evidence, not just replay state.

```python
export = run.export_artifacts("exports")
print(export.manifest_path)

run.prune_artifacts(include_raw_output_artifacts=True)
>>>>>>> d539c5d (Handle completed-with-failures terminal runs)
```

Start a structured-output run:

```bash
batchor start \
  --input input/items.jsonl \
  --id-field id \
  --prompt-field text \
  --structured-output-class your_package.models:ClassificationResult \
  --schema-name classification_result \
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

## Observability

`BatchRunner` accepts an optional observer callback for coarse lifecycle telemetry:

```python
from batchor import BatchRunner, RunEvent


def observer(event: RunEvent) -> None:
    print(event.event_type, event.run_id, event.data)


runner = BatchRunner(observer=observer)
```

Current events include run creation/resume, item ingestion, batch submission/polling/completion, item completion/failure, and artifact export/prune.

## Durable artifacts

For SQLite-backed runs, `batchor` stores replayable request JSONL artifacts on disk beside the database under a sibling `*_artifacts/` directory. Once a request artifact has been written, retry and resume no longer depend on the original item iterator.

Completed runs can:

- export raw request/output/error artifacts plus final results
- prune replayable request artifacts
- prune raw output/error artifacts only after export

## Development

```bash
uv sync --all-groups
uv run ty check src
uv run pytest -q
uv build
```

The default pytest configuration enforces an `85%` coverage floor.

Manual live OpenAI smoke:

```bash
export OPENAI_API_KEY=sk-...
export BATCHOR_RUN_LIVE_TESTS=1
uv run pytest tests/integration/test_batchor_live_openai.py --no-cov -q
```

If `BATCHOR_LIVE_OPENAI_MODEL` is unset, the harness defaults to `gpt-5-nano`.
If `BATCHOR_LIVE_OPENAI_REASONING_EFFORT` is unset, no reasoning field is sent.
The live smoke also requires an OpenAI account with Batch API access and available billing quota.

## Docs

- [docs/README.md](docs/README.md)
- [docs/design_docs/ARCHITECTURE.md](docs/design_docs/ARCHITECTURE.md)
- [docs/design_docs/OPENAI_BATCHING.md](docs/design_docs/OPENAI_BATCHING.md)
- [docs/design_docs/STORAGE_AND_RUNS.md](docs/design_docs/STORAGE_AND_RUNS.md)
- [docs/design_docs/STORAGE_MIGRATIONS.md](docs/design_docs/STORAGE_MIGRATIONS.md)
- [docs/design_docs/ROADMAP.md](docs/design_docs/ROADMAP.md)
- [docs/smoke-test.md](docs/smoke-test.md)
- [SUPPORT.md](SUPPORT.md)
- [VERSIONING.md](VERSIONING.md)

## Scope

Today `batchor` is intentionally narrow:

- one built-in provider: OpenAI Batch
- one durable backend: SQLite
- one ephemeral backend: in-memory storage
- structured output is Python API-first
- CLI job creation is text-only and file-backed

Anything outside that scope should be treated as out of scope or `TBD`, not implied support.
