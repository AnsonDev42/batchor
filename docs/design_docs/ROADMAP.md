# Roadmap

This file tracks important work that should be explicit in the extracted repository.

## Next Practical Steps

- add Postgres storage backend
- extend resumable ingestion beyond the built-in CSV/JSONL sources
- add automated retention windows on top of the explicit export/prune lifecycle
- add more input adapters beyond CSV and JSONL
- add CLI structured-output workflows if the importability story can stay durable and predictable

## Longer-Term Ideas

- additional providers beyond OpenAI
- artifact store abstraction
- partial-result streaming APIs
- richer CLI or operator workflow beyond the current file-backed text-job surface

## Intentionally TBD

- migration tooling for storage schema changes beyond the current additive startup upgrade path
- benchmark guidance for very large jobs
