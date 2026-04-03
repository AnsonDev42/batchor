# batchor

`batchor` is a durable OpenAI Batch runner for Python teams that want:

- typed Pydantic results
- resumable durable runs
- replayable request artifacts
- deterministic source checkpoints
- library-first run controls
- a small operator CLI for CSV and JSONL jobs

It is intentionally narrow today: OpenAI-first, SQLite-first, and library-first.

## What problem it solves

Most OpenAI Batch examples stop at "upload a JSONL file and poll until it finishes." Real workloads usually need more than that:

- durable state so a process restart does not lose the run
- typed result parsing for structured outputs
- artifact retention so submitted requests can be replayed or audited
- clear export and prune steps once a run is done
- stable source checkpoints before request artifacts exist
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
- `ParquetItemSource`

Important constraints:

- the Python API is broader than the CLI
- the CLI supports file-backed inputs only
- the built-in CLI uses SQLite durability only
- structured-output rehydration requires an importable module-level Pydantic model
- raw output artifacts are retained by default and must be exported before raw pruning
- pause/resume/cancel and incremental terminal-result APIs are library-first today

## Mental model

The normal lifecycle is:

1. Build a `BatchJob` with items, prompt-building logic, and provider config.
2. Call `BatchRunner.start(...)` to create or resume a durable run.
3. Keep the returned `Run` handle and call `refresh()` or `wait()`.
4. Read `summary()`, `snapshot()`, or terminal `results()`.
5. Optionally pause, resume, cancel, or consume terminal results incrementally.
6. When the run is finished, optionally `export_artifacts(...)`.
7. When retention requirements are satisfied, `prune_artifacts(...)`.

Durability is split on purpose:

- storage tracks run state, item state, attempts, batches, checkpoints, and artifact pointers
- the artifact store keeps replayable request JSONL and downloaded raw batch payloads

That split is what allows retries and fresh-process resume without keeping every request inline in the control-plane store.

## Install

```bash
pip install batchor
```

## Repo Agent Setup

This repo now includes local AI-agent scaffolding so a contributor agent can pick up repo conventions without extra global setup:

- repo-local skill: `.agents/skills/batchor-dev/`
- repo-local plugin marketplace: `.agents/plugins/marketplace.json`
- repo-local MCP plugin: `plugins/batchor-agent-tools/`
- VS Code workspace MCP config: `.vscode/mcp.json`

The skill captures repo-specific workflow and validation guidance. The MCP plugin exposes a small repo-aware guide for project overview, docs entry points, and validation commands.

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

### Deterministic sources

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

### Parquet input sources

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

Parquet support is library-only today and follows the same durable checkpoint rule as CSV and JSONL: resume requires the same `run_id`, the same job config, and the same source identity/fingerprint.
Custom deterministic sources can implement `CheckpointedItemSource`; arbitrary generators and live DB cursors are still out of scope unless they can provide a durable resume checkpoint.

### Run control and incremental terminal results

```python
from batchor import BatchRunner, RunControlState, SQLiteStorage


runner = BatchRunner(storage=SQLiteStorage(name="default"))
run = runner.get_run("batchor_20260403T120000Z_ab12cd34")

run.pause()
assert run.summary().control_state is RunControlState.PAUSED

run.resume()
page = run.read_terminal_results(after_sequence=0, limit=100)
export = run.export_terminal_results(
    "exports/partial-results.jsonl",
    after_sequence=0,
    append=False,
    limit=100,
)
print(page.next_after_sequence, export.exported_count)

run.cancel()
```

Run control and incremental terminal-result APIs are Python-first in this release. The CLI does not yet expose `pause`, `resume`, `cancel`, or incremental terminal-result export commands.

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

## Observability

`BatchRunner` accepts an optional observer callback for coarse lifecycle telemetry:

```python
from batchor import BatchRunner, RunEvent


def observer(event: RunEvent) -> None:
    print(event.event_type, event.run_id, event.data)


runner = BatchRunner(observer=observer)
```

Current events include run creation/resume, item ingestion, batch submission/polling/completion, item completion/failure, and artifact export/prune.

## Storage notes

- SQLite remains the default durable backend.
- `PostgresStorage` is available for shared control-plane state, but the CLI remains SQLite-only today.
- Durable artifacts now go through an `ArtifactStore` seam. The built-in implementation is `LocalArtifactStore`, intended for local disk or a shared mounted volume.
- Fresh-process resume requeues any locally claimed but not yet submitted items before continuing, so a process crash after request-artifact persistence does not strand work in `queued_local`.

## Durable artifacts

For SQLite-backed runs, `batchor` stores replayable request JSONL artifacts on disk beside the database under a sibling `*_artifacts/` directory. Once a request artifact has been written, retry and resume no longer depend on the original item iterator.

Completed runs can:

- export raw request/output/error artifacts plus final results
- prune replayable request artifacts
- prune raw output/error artifacts only after export

Built-in sources also populate reserved lineage under `metadata["batchor_lineage"]` so downstream joins can recover source references, source item indexes, and source primary keys without replacing user metadata.

## Retention and privacy

Raw provider output/error artifacts persist by default, but runs can opt out when those files are too sensitive or too expensive to retain:

```python
from batchor import ArtifactPolicy, BatchJob


job = BatchJob(
    ...,
    artifact_policy=ArtifactPolicy(persist_raw_output_artifacts=False),
)
```

Request artifacts remain durable replay state even when raw output retention is disabled.
`LocalArtifactStore` creates new directories and files with owner-only permissions where the local platform supports them.

## Development

```bash
uv sync --all-groups
uv run ty check src
uv run pytest -q
uv run mkdocs build --strict
uv build
```

The default pytest configuration enforces an `85%` coverage floor.

GitHub Actions pull requests run:

- `test (3.12)` and `test (3.13)`: `uv run ty check src` plus `uv run pytest -q`
- `docs`: `uv run mkdocs build --strict`
- `build (3.13)`: `uv build`
- `postgres-contract`: `uv run pytest tests/unit/test_batchor_storage_contracts.py --no-cov -q` with an ephemeral PostgreSQL service

The live OpenAI smoke remains manual-only and is not part of required CI.

Manual live OpenAI smoke:

```bash
export OPENAI_API_KEY=sk-...
export BATCHOR_RUN_LIVE_TESTS=1
uv run pytest tests/integration/test_batchor_live_openai.py --no-cov -q
```

If `BATCHOR_LIVE_OPENAI_MODEL` is unset, the harness defaults to `gpt-5-nano`.
If `BATCHOR_LIVE_OPENAI_REASONING_EFFORT` is unset, no reasoning field is sent.
The live smoke also requires an OpenAI account with Batch API access and available billing quota.

## Documentation guide

- `docs/getting-started/how-it-works.md`: the runtime mental model
- `docs/getting-started/python-api.md`: Python-first workflows
- `docs/getting-started/cli.md`: operator CLI workflows
- `docs/reference/api.md`: generated API surface
- `docs/design_docs/ARCHITECTURE.md`: package boundaries
- `docs/design_docs/OPENAI_BATCHING.md`: OpenAI-specific behavior
- `docs/design_docs/STORAGE_AND_RUNS.md`: durability, rehydration, and artifacts
- `docs/smoke-test.md`: minimum validation bar

## Scope

Today `batchor` is intentionally narrow:

- one built-in provider: OpenAI Batch
- one default durable backend: SQLite
- one opt-in durable backend: Postgres
- one ephemeral backend: in-memory storage
- structured output is Python API-first
- CLI job creation is file-backed and supports both text and structured-output jobs

Anything outside that scope should be treated as out of scope or `TBD`, not implied support.

## License

This project is licensed under PolyForm Noncommercial 1.0.0. See [LICENSE](LICENSE).
