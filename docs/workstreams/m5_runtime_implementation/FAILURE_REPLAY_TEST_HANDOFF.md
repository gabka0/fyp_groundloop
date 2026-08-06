# M5 Failure/Replay Acceptance Test Handoff

Status: test-only candidate, live-PostgreSQL validated, not integrated

Date: 2026-08-06

Branch: `workstream/m5-failure-replay-tests`

Branch base: `64352dcc4b1f0a5aaae860b77dc53b3ee5a8071e`

Production checkpoint under test: `516d0ae` (`Complete M5 staged failure
persistence`). Main later advanced to a path-authorization commit without
changing this production slice.

Candidate head: the commit containing this handoff; resolve with
`git rev-parse HEAD` before integration.

Live DSN:
`postgresql://groundloop:groundloop@localhost:5432/groundloop`

## Owned result

Lane R3 adds only the three paths authorized by
`docs/workstreams/m5_completion/EXECUTION_PLAN_2026-08-06.md`:

1. `tests/m5/postgres_runtime/conftest.py`
2. `tests/m5/postgres_runtime/test_open_failure_replay.py`
3. this handoff

There are no persistence, migration, contract, status, or user-owned-path
edits. The branch remains intentionally unrebased from its coordinator base;
the tests were run against the integrated main production source and migration
instead of copying production changes into the lane.

## Executable matrix

The 27 live tests cover the following M5.3-07 falsifiers.

### Rejected declarations

- a missing retirement target is rejected;
- a retirement declaration whose frozen requirement snapshot disagrees with
  the effective post-retirement structure is rejected;
- neither case leaves a durable event/epoch row; and
- every table row plus its PostgreSQL `xmin` remains unchanged across a fresh
  connection. This is the durable projection contract, not a claim that a
  rolled-back PostgreSQL sequence allocation is transactional.

### Complete production-open injection matrix

Every injection point present in the integrated group-lifecycle opener is
exercised at a lifecycle shape that reaches it:

| Event | Injection point |
|---|---|
| RETIRE | `typed_open_epoch_inserted` |
| RETIRE | `typed_open_update_inserted` |
| RETIRE | `typed_open_runtime_header_inserted` |
| REGISTER | `typed_open_family_staged` |
| REGISTER | `typed_open_group_staged` |
| REPLACE | `typed_open_deactivation_staged` |
| REGISTER | `typed_open_roots_persisted` |
| REGISTER | `typed_open_snapshots_persisted` |
| REGISTER | `typed_open_before_commit` |

At each point the injected exception leaves every table row/`xmin` identical
after reconnect, leaves no event row, and permits one clean non-replayed retry.

### Durable retire failure

The rootless retirement path proves exact revision-2 terminal rows:

- base epoch `failed/failed/failed`, runtime state `failed`, and all runtime
  counts zero;
- immutable RETIRE deactivation overlay retained;
- both zero-work event/call rows and the hash-bound failed event result
  retained with zero delta/state-reference children;
- original group remains `PUBLISHED`, its validity remains open, and no family
  retirement is published; and
- M4/M5 heads, strict validity/currency, materialized/published state,
  certificates, and public deltas remain row-and-`xmin` identical.

### Failed staged REGISTER and REPLACE

REGISTER declares two forward requirement roots; REPLACE declares one. For
both event kinds the tests independently rebuild the frozen scope, logical-job,
root-set, payload, and cancellation-completion identities and assert:

- revision-1 runtime work/scope counts equal the exact root count;
- owner and required-answer forward counters equal that count;
- every root scope is OPEN and every root job is DECLARED with its exact
  snapshots, role, execution spec, and payload;
- terminal failure retains the declaration but changes staged family/group/
  requirement rows to `FAILED` as applicable;
- each job and scope is cancelled exactly once with `epoch_failed`, revision 2,
  the exact completion digest, and exact cancelling event/epoch;
- runtime and owner/answer counters become zero at revision 2;
- both durable work rows charge exactly `requirement_cancelled_job_count=N`;
- a failed REPLACE retains its immutable REPLACE overlay while the old group
  remains published with an open validity interval; and
- filtered strict published structure plus all other strict publication
  surfaces remain row-and-`xmin` identical.

Fresh-connection replay of each staged failure returns the stored event work
and logical result, zero call work, and performs no row rewrite.

### Complete terminal-failure injection matrix

A two-root staged REGISTER drives every failure-transition injection point:

```text
typed_fail_authorized
typed_fail_jobs_cancelled
typed_fail_counters_updated
typed_fail_structure_failed
typed_fail_work_inserted
typed_fail_result_inserted
typed_fail_base_updated
typed_fail_runtime_updated
typed_fail_before_constraints
typed_fail_after_constraints
```

Each injected attempt rolls back to the complete revision-1 row/`xmin`
snapshot across reconnect, including DECLARED jobs, OPEN scopes, staged
structure, and nonzero counters. Retrying without injection commits one exact
terminal failure with two charged cancellations.

### Replay, conflict, and race

- a fresh connection reads the immutable failed result, reopens the exact event
  as an already-failed replay, and re-invokes failure as a replay with zero
  writes, zero new calls, unchanged revisions/`xmin`, and the original logical
  result hash;
- same event ID/different payload, same payload/different immutable candidate
  declaration, and result-read wrong-payload cases raise
  `EventConflictError` and change no row; and
- two connections racing the same exact RETIRE open commit one epoch: one
  receipt is original and one is replayed, both bind the same epoch, and one
  durable event row exists.

## Validation evidence

The worktree's pytest configuration prepends its own `src`, so merely setting
`PYTHONPATH` while running from the lane would test the old branch-base
persistence. Final production validation therefore ran from main with main's
configuration while naming the lane test path explicitly:

```bash
cd /home/kassym/Desktop/groundloop
GROUNDLOOP_TEST_DATABASE_URL='postgresql://groundloop:groundloop@localhost:5432/groundloop' \
PYTHONPATH='/home/kassym/Desktop/groundloop/src' \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -c /home/kassym/Desktop/groundloop/pyproject.toml -vv \
  /home/kassym/Desktop/groundloop-worktrees/m5-failure-replay-tests/tests/m5/postgres_runtime/test_open_failure_replay.py
```

Result: **27 passed in 31.76s**, zero skips and zero failures.

The migration/runtime and existing core race regressions were then composed:

```bash
cd /home/kassym/Desktop/groundloop
GROUNDLOOP_TEST_DATABASE_URL='postgresql://groundloop:groundloop@localhost:5432/groundloop' \
PYTHONPATH='/home/kassym/Desktop/groundloop/src' \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -c /home/kassym/Desktop/groundloop/pyproject.toml -q \
  tests/m5/postgres_runtime/test_migration_015.py \
  /home/kassym/Desktop/groundloop-worktrees/m5-failure-replay-tests/tests/m5/postgres_runtime/test_open_failure_replay.py \
  tests/m5/postgres/test_bundle_and_races.py
```

Result: **67 passed**: 25 migration-015 tests, 27 R3 tests, and 15 existing
bundle/race tests; exit code 0, zero skips and zero failures.

Static validation:

```bash
/home/kassym/Desktop/groundloop/.venv/bin/ruff check \
  tests/m5/postgres_runtime/conftest.py \
  tests/m5/postgres_runtime/test_open_failure_replay.py
/home/kassym/Desktop/groundloop/.venv/bin/ruff format --check \
  tests/m5/postgres_runtime/conftest.py \
  tests/m5/postgres_runtime/test_open_failure_replay.py
MYPYPATH='/home/kassym/Desktop/groundloop/src' \
  /home/kassym/Desktop/groundloop/.venv/bin/mypy --strict \
  --explicit-package-bases \
  tests/m5/postgres_runtime/conftest.py \
  tests/m5/postgres_runtime/test_open_failure_replay.py
PYTHONPATH='/home/kassym/Desktop/groundloop/src' \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m compileall -q \
  tests/m5/postgres_runtime/conftest.py \
  tests/m5/postgres_runtime/test_open_failure_replay.py
git diff --check
```

Result: Ruff check/format, strict mypy over both test files, compileall, and
diff check all PASS.

One discarded invocation ran from the lane root and therefore imported the
old branch-base source despite `PYTHONPATH`. Its verbatim failure was:

```text
groundloop.errors.InvalidEventError: the M5.3-07 production slice admits retire_group only
```

That was a wrong-source validation command, not a failure of `516d0ae`; the
main-root/config commands above exercise the intended integrated code.

## Boundary and integration guidance

This handoff is executable M5.3-07 acceptance evidence, not a unilateral gate
status change. The coordinator must inspect and integrate the three owned
paths, rerun them from main using relative paths, and map the result to the
frozen acceptance matrix.

The tests do not claim document/direct-M4 composition, ObserveRequirement
execution, successful sealing/publication, worker attempts, neural calls, the
full M5.4 runtime, maintained M5.5 evaluation, or M5.6 closure. They also do
not modify or validate the user's dirty presentation work.
