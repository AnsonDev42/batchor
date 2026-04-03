# API Reference

This page is split in two parts:

- a short handwritten guide to the public surface
- generated API reference from `mkdocstrings`

If the generated reference feels terse, read [How It Works](../getting-started/how-it-works.md) first. The generated section reflects the code docstrings, so it is best for signatures and symbol discovery rather than architectural explanation.

## Start here

Most users only need a small subset of the package:

- `BatchItem`: input item wrapper
- `BatchJob`: execution definition
- `BatchRunner`: start, resume, and run orchestration
- `Run`: refresh, wait, inspect, export, and prune
- `OpenAIProviderConfig`: built-in provider config
- `SQLiteStorage` and `PostgresStorage`: durable control-plane backends
- `CsvItemSource` and `JsonlItemSource`: file-backed item streaming

## Public package

Use imports from `batchor` first. That is the intended consumer surface.

::: batchor
    options:
      show_root_heading: true
      heading_level: 2

## Runner

`BatchRunner` is the main entrypoint for the Python API, and `Run` is the durable handle returned by `start()` or `get_run()`.

::: batchor.runtime.runner
    options:
      show_root_heading: true
      heading_level: 2

## OpenAI provider

This is the built-in provider implementation. Most consumers only need `OpenAIProviderConfig`, but the lower-level provider class is documented here for extension work and tests.

::: batchor.providers.openai
    options:
      show_root_heading: true
      heading_level: 2

## Sources

These sources stream items from CSV or JSONL and support durable resume through source fingerprints and checkpoints.

::: batchor.sources.files
    options:
      show_root_heading: true
      heading_level: 2

## Storage backends

The generated reference below describes the built-in storage entrypoints. The state-store protocol and detailed storage lifecycle are explained in [Storage & Runs](../design_docs/STORAGE_AND_RUNS.md).

::: batchor.storage.sqlite
    options:
      show_root_heading: true
      heading_level: 2

::: batchor.storage.postgres
    options:
      show_root_heading: true
      heading_level: 2
