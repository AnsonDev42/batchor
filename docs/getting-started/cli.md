# CLI Usage

The CLI is intentionally narrower than the Python API.

It is designed for operator workflows around:

- file-backed inputs only
- CSV and JSONL only
- SQLite-backed durable runs only

It supports both text jobs and structured-output jobs. Structured output requires an importable module-level Pydantic model class.

## What the CLI actually does

`batchor start` is a thin layer over the Python runtime:

- it loads `.env` for local convenience
- it builds either `CsvItemSource` or `JsonlItemSource`
- it creates a `BatchJob`
- it starts or resumes a durable SQLite-backed run
- it prints a JSON run summary

The rest of the commands rehydrate that durable run by `run_id`.

## Local setup

```bash
echo "OPENAI_API_KEY=sk-..." > .env
```

## Start a run from JSONL

```bash
batchor start \
  --input input/items.jsonl \
  --id-field id \
  --prompt-field text \
  --model gpt-4.1
```

Use `--prompt-field` when the prompt comes directly from one field in the row.

## Start a run from CSV with a prompt template

```bash
batchor start \
  --input input/items.csv \
  --id-field id \
  --prompt-template "Summarize: {text}" \
  --model gpt-4.1
```

Use `--prompt-template` when the prompt should be derived from several fields.

Exactly one of `--prompt-field` or `--prompt-template` is required.

## Start a structured-output run

```bash
batchor start \
  --input input/items.jsonl \
  --id-field id \
  --prompt-field text \
  --structured-output-class your_package.models:ClassificationResult \
  --schema-name classification_result \
  --model gpt-4.1
```

If `--schema-name` is omitted, the CLI derives one from the model class name.

## Reuse or pin the run id

You can pass your own `--run-id` when you want a stable durable identifier:

```bash
batchor start \
  --input input/items.jsonl \
  --id-field id \
  --prompt-field text \
  --model gpt-4.1 \
  --run-id customer_export_20260403
```

That matters when you want file-source ingestion resume instead of creating a new run.

## Point the CLI at a specific SQLite database

```bash
batchor start \
  --input input/items.jsonl \
  --id-field id \
  --prompt-field text \
  --model gpt-4.1 \
  --db-path state.sqlite3
```

All later commands must use the same `--db-path` if the run is stored there.

## Inspect and operate on a durable run

```bash
batchor status --run-id batchor_20260403T120000Z_ab12cd34
batchor wait --run-id batchor_20260403T120000Z_ab12cd34
batchor results --run-id batchor_20260403T120000Z_ab12cd34 --output results.jsonl
batchor export-artifacts --run-id batchor_20260403T120000Z_ab12cd34 --destination-dir exports
batchor prune-artifacts --run-id batchor_20260403T120000Z_ab12cd34
```

Notes:

- `status` returns the current durable summary
- `wait` blocks until the run is terminal
- `results` is terminal-only
- `export-artifacts` creates an export bundle rooted at `destination-dir/run_id`
- `prune-artifacts --include-raw-output-artifacts` requires a prior export

The CLI prints JSON summaries by default, which makes it easy to pipe into shell tooling.

## When not to use the CLI

Use the Python API instead when you need:

- non-file inputs
- custom application integration
- explicit artifact-store control
- Postgres-backed control-plane storage
- direct access to `Run.snapshot()` or other in-process orchestration hooks
