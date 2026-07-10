# CLI workflows

Use the CLI for a researcher who has CSV or JSONL files and wants a direct file-to-results workflow. The CLI uses SQLite durability and is narrower than the Python API.

## Install and authenticate

```bash
python -m pip install batchor
export OPENAI_API_KEY="..."
```

Use `python -m pip install "batchor[gemini]"` and the relevant Gemini or Google Cloud credentials for Gemini.

For Anthropic, install `batchor[anthropic]`, set `ANTHROPIC_API_KEY`, and select `--provider anthropic`. Anthropic also requires `--anthropic-max-tokens`.

## Start a run

For JSONL containing `id` and `text`:

```bash
batchor start \
  --input input/items.jsonl \
  --id-field id \
  --prompt-field text \
  --model gpt-4.1
```

Use `--prompt-template` when several fields form the prompt. Inspect `batchor start --help` for the installed version before composing advanced provider options.

When several input files belong to one logical run, repeat `--input` in stable order. File order participates in resume compatibility.

## Operate and export

Keep the returned run ID. A later process can use it:

```bash
batchor status --run-id RUN_ID
batchor wait --run-id RUN_ID
batchor results --run-id RUN_ID --output results.jsonl
batchor export-artifacts --run-id RUN_ID --destination-dir exports
```

Only prune artifacts after the user has decided what must be retained. Raw output/error artifacts require export before raw pruning.

## CLI limits

Switch to the Python API when the workflow needs Parquet, structured Pydantic output inside application code, Postgres, custom artifact storage, pause/resume/cancel, incremental result consumption, or a project-owned pipeline adapter.
