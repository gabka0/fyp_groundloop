# M5 Root Transitions Handoff

Date: 2026-08-06

Lane: R7 (`workstream/m5-root-transitions`)

Authority base: `e8196d0b782e4c1069934f93a4e0629a3df1ecb2`

Required schema unblocks incorporated from the coordinator:

- `da5f14c` / local cherry-pick `ea6b222` fixes the admitted-pair validator
  record/alias collision.
- `9f40bbd` / local cherry-pick `8734098` makes the DISTINCT reason ordering a
  valid PostgreSQL expression.

## Owned implementation

`src/groundloop/m5/runtime/postgres_roots.py` exposes two cursor-local,
transaction-neutral helpers:

```python
stage_m5_discovery_result(
    cursor, epoch_id, expected_revision, lease, job, result, attempt_output,
    *, failure_injector=None,
) -> M5AttemptCompletionReceipt

close_m5_requirement_roots(
    cursor, epoch_id, expected_revision, requirement_root_set_hash,
    *, failure_injector=None,
) -> M5RootBarrierReceipt
```

The persistence wrapper remains responsible for opening exactly one
transaction around either helper. Neither helper commits, rolls back, or runs
external work.

Root-result staging now:

- validates the complete epoch, manifest, scope, job, lease, attempt, output,
  result, snapshot membership, direction, rank, policy, and activity identity;
- reserves the attempt output, archives the immutable activity-classified
  attempt-result artifact, channel hits, selections, and result header;
- moves only the scope from `open` to `result_staged`, leaving the root job
  `running` and its open-work/PENDING contribution intact;
- completes the dispatched attempt and advances the shared base/runtime
  revision exactly once; and
- validates exact replay from durable rows with zero writes and no revision
  change, including after the root barrier has completed the root.

The event-wide barrier now:

- locks and validates the complete frozen root/scope/result/artifact set;
- rejects open, retryable, cancelled, terminal-failed, or partial root sets;
- derives inactive roots from the immutable attempt-result activity snapshot;
- uses the frozen pure deduplication/barrier functions to choose the least
  UTF-8 root owner, retain every source selection, and bind one verifier child
  and dependency to each admitted pair;
- installs active/inactive root completions and canonical closures, including
  empty and short successful results;
- advances only successful active forward frontier heads;
- atomically replaces root PENDING multiplicities with verifier-child
  multiplicities, recomputes the three runtime counters, and advances one
  shared revision; and
- reconstructs and validates the complete barrier on exact replay with zero
  writes.

Both transitions force all deferred invariants before returning and then
restore deferred mode so the outer typed transaction can apply its final
combined-readiness projection.

## Live evidence

The focused live suite is
`tests/m5/postgres_runtime/test_root_transitions.py` and uses a fresh isolated
schema for every case. It covers:

- empty and short `snapshot_exhausted` roots;
- exact stage and barrier replay with `xmin`-sensitive zero-write snapshots;
- conflicting attempt output and incomplete-barrier rejection;
- forward/reverse overlap, least-root ownership, complete two-source
  provenance, and one verifier-child bijection;
- stage and barrier exception injection after each durable statement group,
  full rollback snapshots, and clean retry;
- concurrent identical stage and barrier calls, with exactly one writer and
  one replay; and
- both serializations of the final-stage/barrier race, proving no partial
  closure commits.

Run:

```bash
GROUNDLOOP_TEST_DATABASE_URL=postgresql://groundloop:groundloop@localhost:5432/groundloop \
  .venv/bin/python -m pytest -q \
  tests/m5/postgres_runtime/test_root_transitions.py
```

Recorded result: `15 passed`.

Additional local gates:

```text
python -m pytest -q tests/m5/runtime                         75 passed
live test_migration_015.py                                   26 passed
live test_open_failure_replay.py + test_runtime_races.py     33 passed
python -m mypy src/groundloop/m5/runtime/postgres_roots.py  success
python -m ruff check <owned Python paths>                    success
python -m compileall -q src tests/m5                         success
```

## Reconnect and lease boundary

A committed staged root result is reconnect-safe: the barrier hydrates the
scope, job, discovery header, ordered hits/selections, completed attempt, and
attempt-result artifact entirely from PostgreSQL. No process-local lease or
retrieval result is needed and no second retrieval/model call is authorized.

Recovery of a merely `running`/`dispatched` attempt is intentionally not
invented in R7. Migration 015 durably stores `dispatched_at`, attempt state,
and the opaque lease token hash, but frozen revision-4 has no lease duration,
expiry authority, owner identity, or running-attempt reacquisition edge. A
crashed worker therefore cannot be declared expired or safely redispatched by
these helpers. That contract belongs to the separately reviewed D24/job-
lifecycle lane; R7 does not weaken dense-attempt or lease identity rules to
work around it.

## Integration boundary

The persistence lane should import these helpers and call them inside its
public `*_atomically` transaction wrappers. No migration, persistence wrapper,
contract/digest/frontier, application, M4, or shared status file is owned by
this lane. The coordinator-provided schema commits above remain distinct
prerequisites, not R7-authored changes.
