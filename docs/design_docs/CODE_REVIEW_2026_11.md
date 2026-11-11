# Code review and verified refactoring — November 2026

Base: `45e420e506f1f91f9dd91f8cb343d24b5dfb4080`.
Scope: the local Batchor checkout, with no publication, deployment, push, or
production-provider submission during the initial review. The starting working
tree was clean. The subsequent authorized live checks are recorded below.

## Assessment

The project already has useful boundaries between runtime, providers, sources,
state, and artifacts, plus substantial deterministic durability coverage.
Baseline smoke passed at **89.46% coverage**. The evidence supports targeted
repairs to failure boundaries, not replacing the library or its public model.
The review covered the source package, tests, CLI, configuration/build/CI,
contributor and user MCP helpers, and the design/validation documentation.

The highest-impact findings were input loss after interrupted materialization,
unsafe recovery side effects, and SQL transactions that could duplicate claims
or result cursors. A second group involved artifact retention, provider
normalization, and SDK retries. These have executable regressions.

## Findings and implemented changes

| Priority | Reproduction / consequence | Implemented boundary | Regression evidence |
| --- | --- | --- | --- |
| P1 | A prompt raises on the second item of an arbitrary iterator; restarting the attached job previously returned only the third item. An append failure similarly lost the consumed buffer. | The ingestion session retains the current item and uncommitted slice until atomic append succeeds. Iterator-originated exceptions remain incomplete instead of looking like EOF. | `test_noncheckpointed_prompt_failure_keeps_unpersisted_rows`, `test_noncheckpointed_append_failure_retries_entire_buffer`, `test_noncheckpointed_source_exception_cannot_become_success` on memory and SQLite. |
| P1 | Deadline expiry between checkpointed source reads advanced the checkpoint while dropping a partially rendered slice. | Persist that slice before advancing its checkpoint; stop subsequent remote work. | `test_checkpoint_deadline_persists_partial_slice_before_advancing`. |
| P1 | Two SQLite connections selected the same pending item before either updated it. | Obtain the write transaction before selecting claim candidates. Postgres retains its existing row-lock/skip-locked implementation. | `test_sqlite_concurrent_claims_do_not_duplicate_items`: baseline claimed `row1` twice. |
| P1 | Concurrent SQLite/Postgres completion writers both allocated cursor 1, so a consumer paging after 1 skipped one result. | Lock the run before cursor allocation and keep the lock through the terminal writes. | `test_sql_storage_concurrent_terminal_results_have_distinct_cursors`, verified against SQLite and PostgreSQL 17. |
| P1 | A rejected job config replaced the runner's cached context/job before raising. Changed source identity was checked only after polling. | Validate resume compatibility before provider construction, attachment, requeue, or polling. Composite identity refreshes child fingerprints. | `test_rejected_resume_preserves_attached_context`, `test_mismatched_source_rejected_before_poll_and_context_replacement`, `test_composite_identity_rechecks_existing_child_file`. |
| P1 | `cancel()` followed by `pause()` erased cancellation and allowed `resume()` to restart the run. | Cancellation is irreversible at the state-store write boundary; SQL uses a conditional update. | `test_cancel_cannot_be_reversed_via_pause`, `test_cancellation_cannot_be_reversed_by_a_later_control_write`. |
| P1 | Raw prune rejected a missing export only after deleting request artifacts and pointers. | Check all export prerequisites before deletion. | `test_rejected_raw_prune_leaves_all_artifacts_and_pointers` on memory and SQLite. |
| P1 | Artifact keys rejected `..` but followed escaping symlinks; unsafe run IDs could escape an export directory for an empty run. | Validate run IDs before state creation/export and check resolved artifact/export containment. | `test_unsafe_run_id_rejected_before_creating_state`, artifact symlink read/write/delete/stage/export tests. |
| P1 | OpenAI's installed SDK sent three create POSTs for one transient failure before returning to the intent layer. | OpenAI and Anthropic use `with_options(max_retries=0)` for create when supported by the client. Other operations retain their existing settings. | Real installed OpenAI SDK with `httpx.MockTransport`: `test_create_disables_sdk_retries_to_preserve_submission_ambiguity`; Anthropic client-contract regression. No network/provider request is made. |
| P2 | Preparation, artifact, staging, or upload failures stranded `queued_local` claims; staging could also strand a capacity reservation. | One outer claim cleanup owner and one per-chunk intent boundary. Abandon pre-create reservations; preserve possibly-created remote work. | `test_precreate_failure_releases_claims_and_capacity` plus existing indeterminate-create/local-linkage regressions. |
| P2 | `wait(timeout)` reached polling but not the submission loop; one slow upload/create allowed more remote work after expiry. | Thread the absolute deadline through submission, check before each upload/create, and finalize a confirmed create even after expiry. | `test_submission_deadline_stops_new_remote_work_but_finalizes_created_batch`; existing deadline integration tests. |
| P2 | Every capacity-blocked refresh wrote another request file; oversized individual requests bypassed the configured file-size cap. | Reserve capacity before file creation; reject an oversized request locally without consuming an attempt. | `test_capacity_blocked_cycles_do_not_accumulate_request_artifacts`, `test_request_exceeding_file_limit_fails_locally_without_an_attempt`. |
| P2 | An OpenAI output row with `response: null` and top-level `error` became a successful empty text result; Gemini treated `error: null` as failure. | Respect non-null errors in both parsers. | `test_output_file_top_level_error_is_not_a_text_success`, `test_gemini_null_error_does_not_override_success`. |
| P2 | Encoding/write failure truncated an existing raw artifact before replacement completed. | Write a private temporary file, flush/fsync, then atomically replace; clean up failed temporaries. Restrict all created parent directories. | `test_artifact_write_failure_preserves_previous_file` and existing artifact lifecycle tests. |
| P2 | Startup silently relabeled a newer database schema as this release's version. | Reject newer schema metadata before migration/version updates. Schema remains version 6. | `test_sql_storage_refuses_to_downgrade_a_newer_schema` on SQLite and Postgres. |
| P2 | Completed jobs remained attached to the runner, retaining their input lists. Anthropic downloaded results during status polling and cached every completed payload. | Detach fully ingested jobs; download Anthropic results at the runtime download boundary using a rehydratable identifier. | `test_fully_ingested_job_is_not_retained_by_runner`, `test_poll_does_not_download_results_and_result_pointer_can_be_rehydrated`. |
| P3 | Cached request replay still scanned from line 1 per item; Parquet lineage recomputed all preceding group sizes per row. | Index cached lines directly and accumulate a row-group offset once per group. | Existing replay/hash/source/lineage tests and full smoke. No benchmark claim is made. |

Submission now separates claiming, preparation/chunking, and one chunk's remote
transaction boundary. The existing materializer is shared by ingestion paths.
The architecture, state schema, public job/run objects, and provider request
formats remain intact. No coverage gate was changed.

## Validation

All local Python checks use CPython **3.13.18** on macOS. The required main smoke
retains its configured parallel execution, strict markers/config, and 85% gate.
`UV_CACHE_DIR=/tmp/batchor-review-uv` was used because the normal user cache was
outside the writable sandbox. Dependency installation and package build needed
network-enabled execution after sandbox DNS/cache failures; those environment
failures were resolved.

| Check | Result |
| --- | --- |
| `uv run pytest -q --junitxml=/tmp/batchor-review-final-smoke.xml` | **341 passed, 9 skipped**, no failures; **90.00%** coverage. Six live-provider skips and three backend-specific SQL-test skips. |
| `uv run ty check src` | Passed. |
| `uv run ruff check src tests` | Passed. |
| `uv run ruff format --check src tests` | Passed; 72 Python files. |
| Targeted fake-provider/client suite (command below) | **75 passed**, no skips/failures. |
| `BATCHOR_TEST_POSTGRES_DSN=... uv run pytest tests/unit/test_batchor_storage_contracts.py --no-cov -q --junitxml=/tmp/batchor-review-final-postgres.xml` | **45 passed, 4 backend-specific skips** across memory, SQLite and isolated PostgreSQL 17. |
| `uv run mkdocs build --strict` | Passed. Existing unlisted `INGESTION_POLLING_FIX.md` is an informational notice. |
| `uv build` | Passed: local sdist and wheel. |
| `git diff --check` | Passed. |

Targeted fake-provider/client command:

```bash
uv run pytest tests/integration/test_batchor_runner.py \
  tests/integration/test_batchor_gemini_runner.py \
  tests/unit/test_batchor_anthropic_provider.py \
  tests/unit/test_batchor_openai_provider.py --no-cov -q \
  --junitxml=/tmp/batchor-review-final-providers.xml
```

The supplemental suites used `--no-cov` only after the main smoke passed. No
coverage policy or test gate was weakened. The initial review added **53 parametrized
cases** relative to the baseline (50 regressions plus three backend-specific
skips in the ordinary smoke). Intermediate red runs were repaired; the final
checks above have no outstanding failures or environment blockers. The isolated
Postgres test container was stopped and removed after validation.

For negative control, the new regressions were run against an untouched archive
of `45e420e` in `/tmp`, using the current test definitions. The initial set
produced **42 expected failures / 3 backend-specific skips**; the two SDK retry
regressions also fail on the base. The six cancellation cases fail before their
state-transition repair. This distinguishes discovered behavior from tests that
merely restate an implementation.

### Authorized live-provider follow-up

The owner subsequently authorized paid smoke testing, a draft pull request,
merging conditional on passing CI, and a patch release. The existing locked optional SDKs were
installed without changing the dependency lockfile. The live command was:

```bash
BATCHOR_RUN_LIVE_TESTS=1 BATCHOR_RUN_LIVE_GEMINI=1 \
  BATCHOR_RUN_LIVE_ANTHROPIC=1 \
  uv run --extra gemini --extra anthropic pytest \
  tests/integration/test_batchor_live_openai.py \
  tests/integration/test_batchor_live_gemini.py \
  tests/integration/test_batchor_live_anthropic.py \
  -k 'not vertex' -n 4 --no-cov -vv --tb=short \
  --junitxml=/tmp/batchor-review-paid-live.xml
```

Result: **2 passed, 2 failed, 1 skipped** in 80.82 seconds. Both OpenAI tests
completed successfully: one text request and two requests from composed CSV
sources using `gpt-5-nano`. Gemini Developer inline and file transport both
returned **HTTP 402 `RESOURCE_EXHAUSTED`**, explicitly reporting depleted
prepayment credits. These are failed live checks, not passing or skipped checks.
A subsequent read-only Gemini batch listing succeeded and contained no batches.
The inline create rejection retained the existing conservative indeterminate
intent behavior for generic provider errors; no retry or extra paid submission
was attempted. The file transport failed during upload.

Anthropic was skipped because `ANTHROPIC_API_KEY` was absent. Vertex was excluded
because its project, location and GCS URI were not configured. These live
limitations do not alter the deterministic CI requirements or coverage gate.
No credentials or local test databases are included in the pull request.

After these live checks, the full default smoke was repeated with both optional
SDKs installed (`uv run --extra gemini --extra anthropic pytest -q`, with JUnit
output at `/tmp/batchor-review-pr-smoke.xml`): **341 passed, 9 skipped**, coverage
**89.96%**, with the original 85% gate intact. Type checking, Ruff lint/format,
strict documentation build, and whitespace checks also passed again.

The retained output artifacts confirm `gpt-5-nano-2025-08-07`, **48 input tokens
and 571 output tokens** (including reasoning), and one consumed attempt for each
of the three OpenAI items. Using the full undiscounted rates of $0.05 per million
input tokens and $0.40 per million output tokens yields a conservative inference
charge of **$0.0002308**, below the owner's aggregate **under $2** budget. The
Batch discount lowers this amount further. This is a usage-based calculation,
not a claim to have reconciled an invoice. See the
[official model pricing](https://developers.openai.com/api/docs/models/gpt-5-nano).
Gemini rejected both requests before inference; no Anthropic or Vertex request
was submitted. No live smoke was resubmitted during delivery preparation.

One existing deadline test now creates a genuinely interrupted source instead
of manually changing a completed checkpoint back to incomplete. This retains
its original assertion while allowing completed jobs to be detached.

### Independent delivery review

Independent review found that generated export leaves still followed escaping
symlinks. Both `results.jsonl` and `manifest.json` are now preflighted before any
export writes. Four additional memory/SQLite regressions failed before this
repair and verify that outside files, earlier export files and export metadata
remain unchanged on rejection. The complete change now adds **57 parametrized
cases** relative to the baseline.

Inspection of the actual locally built sdist also found ignored private
`handoff/` records included by Hatchling. The sdist now excludes that directory
explicitly. The rebuilt archive was inspected for handoff, dotenv and SQLite
files, and the wheel's runtime sources were compared with the checkout before
publication. No private handoff files were committed or pushed.

After these repairs, the main configured smoke passed **345 tests, 9 skipped**,
with **89.96% coverage**; the targeted fake-provider/client suite passed **79
tests**. Type, Ruff, strict MkDocs, package build and whitespace checks passed.
No provider behavior changed in this delivery repair, so earlier live evidence
was retained without new billable calls. Current-head hosted CI and a fresh
independent verifier are the final merge gates.

## Remaining boundaries and risks

- **Newer schemas must use matching library versions.** The version guard
  preserves future version metadata and rejects migration, but constructor
  `create_all` can create a missing legacy table before the guard runs. This
  independently reviewed minor remains outside supported-version behavior;
  do not open newer databases with this release.

- **One execution owner per run.** Storage claim/cursor fixes do not add a
  distributed run lease. Two independent runners simultaneously advancing the
  same run can still interfere through crash-recovery requeue. Run leases are
  `TBD`; parallelize independent runs instead.
- **Remote create ambiguity remains explicit.** A process crash or uncertain
  provider response can still leave an indeterminate intent requiring verified
  operator resolution. Custom clients/providers and Gemini SDK retry settings
  must preserve the intended creation boundary. Exactly-once remote execution
  is not guaranteed.
- **Timeouts are cooperative.** Already-running prompt callbacks, SDK calls,
  result streams, and best-effort upload cleanup can finish after the deadline.
- **Arbitrary iterators are not crash-resumable.** The new buffer is in-process.
  Use checkpointed sources for fresh-process recovery. File identity still uses
  path/size/mtime rather than a content digest, and input files must be immutable.
- **Retention is not a full historical artifact ledger.** Current item pointers
  support replay. Superseded request files after counted retries, and files
  written immediately before a process crash, can remain unreferenced. General
  orphan discovery/collection is `TBD`; this change specifically eliminates
  repeated file creation while capacity admission is denied.
- **Provider semantics remain partly unverified live.** OpenAI live checks
  passed. Gemini Developer checks were blocked by depleted prepayment credits;
  Anthropic and Vertex lacked required local configuration. Failed/expired/cancelled batch
  handling retains the existing batch-level retry policy, including possible
  reprocessing of provider partial successes. Provider quota accuracy and remote
  cancellation are outside this repair.
- **Other supported Python versions were not run locally.** Python 3.12 and 3.14
  were not installed; the repository CI matrix still covers them. Existing
  intermittent SQLite `ResourceWarning` messages also appeared in the baseline;
  they did not fail smoke or the coverage gate.

## Review and use

Inspect `git diff --stat` and `git diff`, starting with
`runtime/ingestion.py`, `runtime/submission.py`, and SQL transaction changes,
then the regression tests and provider/artifact fixes. The report is linked
from the README and documentation navigation.

The checkout's editable `.venv` contains the local code. Run `uv run pytest -q`
from the repository to repeat the required smoke. This change prepares patch
version `0.0.8`, preserving storage schema 6. The initial review was local-only;
the owner subsequently authorized the live checks, a draft PR, merge after green
CI, and publication using the repository's existing `v*` tag/PyPI workflow.
Local wheel/sdist files are validation artifacts until publication is verified.
