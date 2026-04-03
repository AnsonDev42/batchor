# Contributing

Issues and pull requests are welcome for the noncommercial open-source package.

Before submitting changes:

- run `uv run ty check src`
- run `uv run pytest -q`
- run `uv build`
- update docs when behavior, workflow, or validation policy changes

Please keep changes aligned with the documented package boundary:

- durable batch execution
- OpenAI Batch provider support
- SQLite-backed local durability by default

Out-of-scope proposals are still useful, but should usually start as an issue discussing fit before implementation.
