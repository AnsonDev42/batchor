# CLI Usage

The CLI is intentionally narrower than the Python API:

- file-backed inputs only
- CSV and JSONL only
- SQLite-backed durable runs only

It supports both text jobs and structured-output jobs. Structured output requires an importable module-level Pydantic model class.

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

## Start a run from CSV with a prompt template

```bash
batchor start \
  --input input/items.csv \
  --id-field id \
  --prompt-template "Summarize: {text}" \
  --model gpt-4.1
```

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

## Inspect and operate on a durable run

```bash
batchor status --run-id batchor_20260403T120000Z_ab12cd34
batchor wait --run-id batchor_20260403T120000Z_ab12cd34
batchor results --run-id batchor_20260403T120000Z_ab12cd34 --output results.jsonl
batchor export-artifacts --run-id batchor_20260403T120000Z_ab12cd34 --destination-dir exports
batchor prune-artifacts --run-id batchor_20260403T120000Z_ab12cd34
```

The CLI prints JSON summaries by default.
