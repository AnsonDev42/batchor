# Runtime Durability 0.0.7

Status: released design and verification record for `0.0.7`.

## Outcome

`0.0.7` closes durability and liveness gaps in the run lifecycle without
changing the provider request formats or introducing background execution. The
release makes remote terminal consumption replayable, keeps reconciliation live
during ingestion and submit backoff, records submission ambiguity durably, and
enforces OpenAI enqueue capacity across runs sharing a quota scope.

## Design boundaries

- Batchor owns durable local reconciliation; providers remain the authority for
  remote batch status and result payloads.
- Result consumption is at-least-once/replayable locally. A terminal remote
  batch remains active until submitted items are terminalized and its capacity
  reservation is released.
- Generic provider creation cannot promise exactly-once remote execution. A
  timeout or crash after the create call becomes an explicit
  `RunSubmissionIndeterminateError`, requiring operator resolution as created
  or not created rather than an unsafe automatic retry.
- Cooperative deadlines prevent starting another Batchor-controlled operation
  after expiry and cap sleeps. They cannot interrupt an arbitrary prompt
  callback or provider SDK call already running in the current thread; those
  calls retain their own timeout contract.
- Non-checkpointable iterators are retained only by their in-process execution
  owner. Manual pause can stop and resume that iterator in the same process,
  but fresh-process mid-ingestion recovery remains unsupported.
- Capacity is shared only among runs using the same durable store and explicit
  quota scope. The default scope is conservative for one account/model; users
  sharing a store across accounts or projects must configure distinct scopes.

## Change list and acceptance criteria

| Finding | Design/fix boundary | Acceptance evidence |
| --- | --- | --- |
| Terminal batch lost after a local failure | Terminal batches with submitted items or unreleased intent capacity remain pollable. Consumption filters already-terminal rows and persists item changes conditionally so it can replay after download, parse, or partial-write failure. | Fake-provider terminal-replay and partial-mutation tests in polling/storage suites. |
| Submit backoff suppresses polling | Submission backoff blocks materialization/submission only; active remote batches are reconciled at each due work boundary. | Ingestion regression with pre-existing submit backoff and a completed active batch. |
| A slow 1,000-row slice blocks progress | Materialization uses a normal 1,000-item fast path plus a cooperative time budget that flushes partial atomic slices. | Slow-source clock tests proving polling/control observation before another full chunk. |
| Empty non-checkpointed run appears terminal | Every new run receives an incomplete generic ingest marker atomically; v5 backfill treats unknown legacy state as incomplete. | Fresh SQLite rehydration and legacy migration tests. |
| Remote create and local linkage can split | Persist a submission intent before create; atomically finalize intent, batch, and item linkage after confirmed success. | Crash-window/indeterminate-resolution storage and submission tests. |
| Manual pause drains arbitrary ingestion | Manual pause stops at a cooperative boundary and retains the iterator for same-process resume; quota auto-pause still drains finite arbitrary input. | Non-checkpointed pause/resume and quota-pause tests. |
| OpenAI capacity is per-run | Atomically reserve tokens in a durable provider/model/quota-scope counter and include active legacy submitted rows. | Multi-run scope and legacy-reservation tests. |
| A success clears another failure's backoff | Polling aggregates its cycle before retry-state mutation; a completed batch does not clear unrelated retry delay. | Mixed failed/completed batch backoff tests. |
| `wait(timeout)` overruns | Thread an absolute deadline through refresh, ingestion, polling, and sleep decisions. | Blocked-ingestion/provider and sleep deadline tests. |
| Cancellation waits only on stale backoff | Cancellation clears retry backoff after its final active batch drains. | Cancelled terminal quota/error regression test. |

## Storage migration contract

The schema advances from 4 to 6. Version 5 establishes the generic
`run_ingest_state` invariant and conservative marker backfill. Version 6 adds
`submission_intents` and `capacity_scopes`. Both upgrades are additive,
performed during storage startup, and retain all existing rows. See
[`STORAGE_MIGRATIONS.md`](STORAGE_MIGRATIONS.md) for operator guidance.

## Non-goals

- Background polling, asynchronous ingestion, or forcibly cancelling arbitrary
  user/provider calls.
- Provider-side remote cancellation guarantees.
- Fresh-process resume for arbitrary non-checkpointable iterables.
- Universal provider idempotency/discovery for `create_batch`.
- Cross-store or credential-derived capacity coordination.

## Evidence contract

The deterministic evidence for this release is the fake-provider, memory,
SQLite, and storage-contract suite. It must retain at least 85% coverage and
pass type checking, linting, strict documentation build, package build, and
`git diff --check` on the release commit.

No paid live-provider smoke is required before push: all release criteria are
directly represented by deterministic fake-provider/storage regressions. A live
provider smoke, when credentials are available, remains an operational
post-merge check rather than proof of these local state-machine invariants.
