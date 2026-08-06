# M5 Job Lifecycle Persistence Handoff

Status: isolated R6 candidate; live PostgreSQL gate PASS; not integrated

Date: 2026-08-06

Branch: `workstream/m5-job-lifecycle`

Branch base: `b1e94bdbdbfeee9277f2003333343b62c56483c9`

Candidate head: the commit containing this handoff; resolve with
`git rev-parse HEAD` before integration.

Authority: M5-D23, runtime-addendum revision 4, and Lane R6 in
`docs/workstreams/m5_completion/EXECUTION_PLAN_2026-08-06.md`.

## Owned result

R6 changes only its three assigned paths:

1. `src/groundloop/m5/runtime/persistence.py`
2. `tests/m5/postgres_runtime/test_job_lifecycle.py`
3. this handoff

No migration, contract/digest, structural-open, discovery/barrier, verifier,
seal/failure, M4, shared-status, or user-owned presentation path is changed.

`PostgresM5RuntimeStore` now provides the frozen production methods:

- `acquire_m5_job`;
- `mark_m5_retryable_failure`;
- read-only `current_revision`;
- read-only canonical `verifier_jobs`; and
- read-only `current_event_work`.

## Transaction and identity behavior

Acquisition owns one transaction and locks the base/runtime epoch, exact job,
and dense attempt history in the frozen order. It reconstructs and validates
the complete immutable `M5LogicalJobSpec`, including event, policy/manifest,
pair/null shape, scope, snapshots, role/execution, expandability, payload, and
logical-job identity. A normal `declared -> running` or
`retryable_failed -> running` transition calls
`groundloop_m5_authorize_checked_transition`, inserts exactly one deterministic
dense attempt with an opaque random lease-token hash, increments both epoch
revisions once, leaves the base statuses pending, moves the runtime to
`semantic_pending`, and rebinds every unchanged owner/answer PENDING row to
the resulting revision.

A committed RUNNING job returns its durable live attempt with
`should_execute=false` and zero writes. A terminal job likewise returns a
zero-write no-dispatch replay. This is duplicate-dispatch safety, not lease
recovery; the missing recovery contract is recorded below.

Retryable failure validates the complete epoch/job/attempt/lease tuple,
including the lease-token and execution hashes. It calls the checked
transition procedure, changes only the current `dispatched` attempt to
`failed`, stores the caller's lowercase SHA-256 in the distinct `error_hash`
column, leaves `attempt_output_digest` NULL, moves the job
`running -> retryable_failed`, and performs the same single revision increment
and unchanged PENDING revision rebinding. Same tuple/same error is a zero-write
exact replay; another error, lease token, attempt, or stale active transition
conflicts without changing a row.

The read projections use read-only transactions. `current_revision` rejects a
base/runtime identity or revision divergence. `verifier_jobs` rebuilds complete
DTOs in logical-job-ID order. `current_event_work` reads the immutable
persisted `event` work row; before terminal work exists it returns the
canonical zero value and it rejects a terminal epoch lacking its required
work row.

## Executable evidence

The new five-test live suite proves:

- complete acquisition identity and attempt persistence;
- exact base/runtime revision 1-to-2 transition with unchanged PENDING values
  rebound to revision 2;
- read-only hydration with unchanged row values and PostgreSQL `xmin`;
- exact RUNNING acquisition replay with no second dispatch;
- stale acquisition conflict with no writes;
- separate retry `error_hash`, NULL result identity, and exact revision 3;
- same-error replay, different-error conflict, and forged-lease conflict with
  unchanged row values and `xmin`;
- a new dense ordinal/token after an accepted retry;
- two concurrent acquisitions producing one attempt and one executable lease;
  and
- two concurrent identical failure reports producing one mutation and one
  exact replay.

Commands executed from this worktree:

```bash
PYTHONPATH=src:. \
GROUNDLOOP_TEST_DATABASE_URL='postgresql://groundloop:groundloop@localhost:5432/groundloop' \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -q tests/m5/postgres_runtime/test_job_lifecycle.py
```

Result: **5 passed in 5.71s**, zero skips and zero failures.

The pre-existing migration-015 and structural-open/failure/replay regressions
were then run against the changed production file:

```bash
PYTHONPATH=src:. \
GROUNDLOOP_TEST_DATABASE_URL='postgresql://groundloop:groundloop@localhost:5432/groundloop' \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -q \
  tests/m5/postgres_runtime/test_open_failure_replay.py \
  tests/m5/postgres_runtime/test_migration_015.py
```

Result: **53 passed in 62.89s**, zero skips and zero failures.

The final composed rerun collected the new and pre-existing suites together:

```bash
PYTHONPATH=src:. \
GROUNDLOOP_TEST_DATABASE_URL='postgresql://groundloop:groundloop@localhost:5432/groundloop' \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -q \
  tests/m5/postgres_runtime/test_job_lifecycle.py \
  tests/m5/postgres_runtime/test_open_failure_replay.py \
  tests/m5/postgres_runtime/test_migration_015.py
```

Result: **58 passed in 67.11s**, zero skips and zero failures.

Static gates use the repository environment from the main checkout because
the isolated worktree intentionally has no duplicate `.venv`:

```bash
/home/kassym/Desktop/groundloop/.venv/bin/ruff check \
  src/groundloop/m5/runtime/persistence.py \
  tests/m5/postgres_runtime/test_job_lifecycle.py
/home/kassym/Desktop/groundloop/.venv/bin/ruff format --check \
  src/groundloop/m5/runtime/persistence.py \
  tests/m5/postgres_runtime/test_job_lifecycle.py
MYPYPATH=src:tests \
/home/kassym/Desktop/groundloop/.venv/bin/mypy --strict \
  src/groundloop/m5/runtime/persistence.py \
  tests/m5/postgres_runtime/test_job_lifecycle.py
python3 -m compileall -q \
  src/groundloop/m5/runtime/persistence.py \
  tests/m5/postgres_runtime/test_job_lifecycle.py
git diff --check
```

All static gates pass. The final commit/status check is recorded after this
handoff is committed.

## Confirmed reconnect blocker: no lease expiry or takeover API

The frozen API cannot recover a committed RUNNING/dispatched attempt after the
worker process loses its opaque lease token or disappears permanently.

Exact evidence:

- migration 015 admits `attempt_state='expired'` and its checked trigger allows
  `dispatched -> expired`;
- no frozen runtime-port method expires an attempt, transfers a lease, or
  reacquires a RUNNING job;
- the attempt row has `dispatched_at` but no lease deadline, worker owner,
  heartbeat, recovery generation, or expiration policy; and
- addendum Section 14.6 requires reconnect to call only missing work, while
  Section 14.2 requires every retry to use a new attempt ordinal.

R6 therefore does not invent a timeout, classify a still-running dispatch as
failed/expired, or dispatch it twice. Reconnect can observe it safely and will
return `should_execute=false`, but cannot make progress if the original worker
will never return. Closing this gap requires a numbered contract amendment and
an authorized schema/API lane defining ownership, expiry evidence, the
checked transition, revision/counter effects, and the exact race with a late
worker return.

## Confirmed work-hydration composition gap

Migration 015's `groundloop_m5_runtime_work` rows are immutable and are
currently written only with terminal results. The frozen lifecycle/staging/
verifier APIs do not accept `M5RuntimeWork`, and attempt/artifact rows do not
retain every model/token/byte counter. Consequently R6 can read an existing
persisted event-work row exactly, but cannot preserve arbitrary nonterminal
per-call work across process reconnect or reconstruct it byte-totally from the
authorized schema.

Returning canonical zero before a work row exists is deterministic and
read-only, but it is not evidence for the full reconnect work-accounting gate.
The application handoff already lists exact production work/error hashes,
token counts, and durable call/event separation as a required composition
item. Integration must retain that boundary rather than treating this R6 read
projection as closure of the end-to-end work-accounting gate.

## Integration boundary

This candidate is ready for coordinator review and path-scoped integration.
It proves only acquisition, retryable failure, and the three read projections
against the current migration. Positive verifier-job hydration still needs a
composed barrier-created child in the barrier/verifier integration suite.
Discovery staging, root closure, verifier completion, cancellation, sparse
seal, terminal races, crash injection, lease recovery, complete reconnect work
accounting, and maintained evaluation remain separate pending gates.
