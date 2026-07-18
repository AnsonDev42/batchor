# Periodic reconciliation during ingestion

Status: implementation specification for the `0.0.6` bug-fix release.

## Outcome

Long-running item ingestion must continue reconciling active provider batches so
completed remote work releases locally accounted enqueue tokens and permits new
batches to be submitted before the entire source is materialized.

## Non-goals

- making ingestion asynchronous
- changing storage schemas or public APIs
- changing provider token-accounting rules
- introducing a background polling thread

## Constraints and affected flow

The affected path is `BatchRunner.start()` / resume -> checkpointed ingestion ->
chunk persistence -> submission. Reconciliation must happen only at durable chunk
boundaries, respect the provider polling interval, and preserve pause, cancellation,
retry-backoff, checkpoint, and fresh-process resume behavior.

The implementation should keep orchestration in `runtime/ingestion.py` and use the
existing injected `poll_active_batches` callback. A monotonic clock may be injected
for deterministic cadence tests. Polling must occur before the chunk's submission
attempt when active batches exist and the cadence is due, so newly freed capacity
can be reused immediately.

## Change list

- Add cadence-aware active-batch polling at durable ingestion chunk boundaries.
- Add regression tests that reproduce a capacity-blocked multi-chunk ingest and
  prove terminal reconciliation permits another submission before ingestion ends.
- Cover no-active-batch, cadence, pause/cancel, and retry-backoff safety as needed.
- Update README and runtime/provider/storage design documentation.
- Update the package and lock metadata from `0.0.5` to `0.0.6`.

## Acceptance criteria and evidence

1. During a multi-chunk ingest with active batches, a due reconciliation occurs
   before the next submission attempt. Evidence: focused ingestion unit test.
2. A remotely completed batch releases locally counted enqueue tokens and another
   pending batch can be submitted before all source rows are ingested. Evidence:
   fake-provider integration regression test on head, plus a failing BASE
   reproduction where practical.
3. Poll cadence avoids a provider request per fast chunk and uses monotonic time
   while preserving the existing positive-interval provider configuration
   contract. Evidence: deterministic cadence unit tests.
4. Poll-induced pause, cancellation, or retry backoff prevents unsafe subsequent
   submission/materialization behavior and checkpoint completion remains correct.
   Evidence: focused control/backoff regression tests and existing suite.
5. Public behavior and the `0.0.6` release are documented without a storage schema
   change. Evidence: README/design-doc diff, strict MkDocs build, package build.
6. Coverage remains at least 85%, types pass, and the fake-provider/runtime suite
   remains green. Evidence: required local smoke and GitHub CI on the immutable PR
   head.

## Environment contract

All deterministic proof uses the in-memory or SQLite stores and fake providers; no
real provider credentials or Postgres instance are required. GitHub CI supplies the
ephemeral Postgres contract job. A live OpenAI smoke is post-merge only when the
documented credentials are available.

## Local smoke decision

`local_smoke: skipped` for paid live-provider execution because all fix criteria are
directly reproducible with deterministic fake-provider integration tests. The
repository-mandated pre-push smoke remains required: `uv run ty check src`, focused
runtime tests, `uv run pytest -q`, `uv run mkdocs build --strict`, and `uv build`.

## Review and repair

High-risk concurrency/run-lifecycle review is required. Repair cap: 3 cycles.
