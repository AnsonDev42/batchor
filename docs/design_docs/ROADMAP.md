# Roadmap

This file tracks important work that should be explicit in the extracted repository.

## Next Practical Steps

- add a live OpenAI smoke harness with low-cost fixtures
- add provider-level observability hooks
- add Postgres storage backend
- extend resumable ingestion beyond the built-in CSV/JSONL sources
- add automated retention windows on top of the explicit export/prune lifecycle
- add more input adapters beyond CSV and JSONL
- document API stability expectations before public release

## Longer-Term Ideas

- additional providers beyond OpenAI
- artifact store abstraction
- partial-result streaming APIs
- richer CLI or operator workflow

## Intentionally TBD

- published versioning policy
- migration tooling for storage schema changes
- benchmark guidance for very large jobs
