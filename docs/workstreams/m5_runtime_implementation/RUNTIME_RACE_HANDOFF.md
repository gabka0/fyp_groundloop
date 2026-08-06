# M5 Activation and Route Race Acceptance Handoff

Status: test-only candidate, live-PostgreSQL validated, not integrated

Date: 2026-08-06

Branch: `workstream/m5-runtime-races`

Branch base: `675cb945a671a8a5f3a5822b113f719e74a7dbc2`

Production activation checkpoint under test: `187463f` (`Complete public M5
activation transaction`). The lane base also includes the integrated M5.3-07
failure/replay acceptance checkpoint.

Candidate head: the commit containing this handoff; resolve with
`git rev-parse HEAD` before integration.

Live DSN:
`postgresql://groundloop:groundloop@localhost:5432/groundloop`

## Owned result

Lane R4 adds only the two paths authorized by
`docs/workstreams/m5_completion/EXECUTION_PLAN_2026-08-06.md`:

1. `tests/m5/postgres_runtime/test_runtime_races.py`
2. this handoff

There are no implementation, migration, shared-fixture, other-test, status, or
user-owned-path edits. The test fixture installs the production legacy/core/
runtime bundles in a fresh schema, seeds one sealed M4 head and one published
evidence group, registers canonical M4 and M5 candidate policies through their
production stores, and prepares the public activation request through
`PostgresM5RuntimeStore.prepare_activation_request`.

Every worker uses a separate PostgreSQL connection bound to the isolated
schema. `threading.Event` barriers hold production injection points only after
the relevant durable route locks have been acquired. The 250 ms blocked-worker
checks establish the intended interleaving before the winner is released.

## Executable race matrix

The six live tests cover both frozen orders for each race family.

### Activation versus activation

- **Exact request:** the first public activation holds the total-order locks at
  `activation_before_bootstrap`; the second exact activation reaches the same
  route and blocks. The winner commits one activation and the loser returns an
  exact replay. Both receipts have the same receipt hash, one is original and
  one is replayed, mode revision is exactly 1, M4/M5 heads remain at the base
  epoch, no synthetic epoch is created, and a later exact replay leaves every
  row and `xmin` unchanged.
- **Conflicting request:** a same-ID request with a different immutable
  bootstrap hash blocks behind the winner, then raises `EventConflictError`.
  Exactly one activation and the original base epoch remain. Repeating the
  conflict after reconnect leaves the complete database row/`xmin` snapshot
  unchanged.

### Activation versus public v1 durable open

- **v1 wins:** `PostgresM4RuntimeStore.open_epoch` reaches
  `open_structural_written` after its M4 update trigger has taken the runtime
  mode lock. Public activation blocks, then observes the committed live epoch
  and rejects it. The v1 event is consumed exactly once, while the complete
  activation surface (mode, heads, activation row, bootstrap states, and
  certificates, including `xmin`) is identical to its pre-race snapshot.
- **activation wins:** public activation holds the route locks before
  bootstrap. The production v1 opener blocks, then migration 015 rejects its
  M4 update after activation commits. PostgreSQL rolls back the loser's prior
  epoch insert: the losing event count is zero, the only epoch is the sealed
  base, mode revision remains 1, and both publication heads remain at the base
  epoch. Exact activation replay subsequently changes no row or `xmin`.

### Activation versus typed open

- **typed arrival before activation:** the production typed opener rejects
  inactive mode before event consumption. A concurrent activation proceeds
  only after that rejection. The typed event count remains zero, the only
  epoch remains the sealed base, and activation creates head-equal M4/M5
  routing at that base.
- **activation arrival before typed open:** public activation holds the route
  locks and the production typed opener blocks. After activation commits, the
  typed opener is admitted at equal M4/M5 heads and pauses after its epoch
  insert. The activation route snapshot is captured while the typed write is
  still uncommitted; after typed commit, mode, activation, and both heads have
  identical values and `xmin`. Exactly one typed epoch, M5 update, and runtime
  header are durable, while the unsealed open does not advance either head.

These tests exercise public production methods where available. Test SQL is
limited to schema isolation, deterministic fixture construction, and
postcondition snapshots; it does not emulate activation or either runtime
opener.

## Validation evidence

Focused live gate:

```bash
cd /home/kassym/Desktop/groundloop-worktrees/m5-runtime-races
GROUNDLOOP_TEST_DATABASE_URL='postgresql://groundloop:groundloop@localhost:5432/groundloop' \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m pytest -vv \
  tests/m5/postgres_runtime/test_runtime_races.py
```

Result: **6 passed in 8.55s**, zero skips and zero failures.

The composed migration/runtime gate was then run:

```bash
GROUNDLOOP_TEST_DATABASE_URL='postgresql://groundloop:groundloop@localhost:5432/groundloop' \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  tests/m5/postgres_runtime/test_migration_015.py \
  tests/m5/postgres_runtime/test_typed_runtime.py \
  tests/m5/postgres_runtime/test_open_failure_replay.py \
  tests/m5/postgres/test_bundle_and_races.py \
  tests/m5/postgres_runtime/test_runtime_races.py
```

Result: **81/81 passed in 148.37s**, exit code 0, zero skips and zero failures.
A separate collection check confirmed exactly 81 tests across those five
files.

Static validation:

```bash
/home/kassym/Desktop/groundloop/.venv/bin/ruff check \
  tests/m5/postgres_runtime/test_runtime_races.py
/home/kassym/Desktop/groundloop/.venv/bin/ruff format --check \
  tests/m5/postgres_runtime/test_runtime_races.py
MYPYPATH='/home/kassym/Desktop/groundloop-worktrees/m5-runtime-races/src' \
  /home/kassym/Desktop/groundloop/.venv/bin/mypy --strict \
  --explicit-package-bases \
  tests/m5/postgres_runtime/test_runtime_races.py
PYTHONPYCACHEPREFIX='/tmp/groundloop-m5-runtime-races-pyc' \
  PYTHONPATH='/home/kassym/Desktop/groundloop-worktrees/m5-runtime-races/src' \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m compileall -q src tests
git diff --check
```

Result: Ruff check/format, strict mypy over the owned test, full `src`/`tests`
compileall, and diff check all PASS.

## Non-gate authoring invocations

The first attempted focused command used system `python3`; collection stopped
before tests because that interpreter lacks `psycopg`. The repository virtual
environment above is the validated environment. An initial fixture draft also
used a non-frozen M5 fusion tag and was rejected during fixture setup; the
candidate now uses the required `rank-interleave-v1`. Neither was a production
failure, and neither invocation is counted as gate evidence.

No frozen winner/loser assertion was weakened and no production defect was
observed.

## Boundary and integration guidance

This handoff supplies executable R4 concurrency acceptance evidence. It does
not unilaterally close M5.4 or M5, change gate/status documents, or claim crash
recovery, direct document-event composition, worker execution, sealing,
publication, maintained evaluation, model quality, or end-to-end utility.

The coordinator must inspect the two owned paths, record branch/base/head and
patch identity, integrate the committed candidate, and rerun the focused and
composed gates from main. The historical PostgreSQL and incremental-overlay
WIP worktrees and the user's dirty presentation paths were not edited or
integrated by this lane.
