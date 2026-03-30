# AGENTS.md

## Mandatory Smoke Tests

- After any refactor or behavior-changing code edit, run a smoke test before marking work complete.
- Minimum required smoke test:
  - extracted repo root: `uv run pytest -q`
  - monorepo subproject: `cd batchor && uv run pytest -q`
- For changes to provider wiring, storage wiring, token budgeting, or run lifecycle behavior, also run:
  - `uv run ty check src`
  - the targeted fake-provider integration suite when relevant
- In the final report, include:
  - the smoke command(s) executed
  - pass/fail outcome
  - any blocker if a smoke test could not be executed

## Mandatory Doc Maintenance

- When adding a major feature or changing library behavior, update relevant docs in `docs/` in the same change.
- At minimum, check and update:
  - `README.md`
  - `docs/design_docs/ARCHITECTURE.md`
  - `docs/design_docs/OPENAI_BATCHING.md`
  - `docs/design_docs/STORAGE_AND_RUNS.md`
  - `docs/smoke-test.md`
- If a design area is not implemented yet, keep the section and mark it `TBD` instead of omitting it.
