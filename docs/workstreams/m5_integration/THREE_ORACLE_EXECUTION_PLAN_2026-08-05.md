# M5.3-06 integrated three-oracle execution plan

Status: active, coordinator-owned follow-on from integrated checkpoint
`58f43dd`

Date: 2026-08-05

## Evidence target

Close the narrow M5.3-06 gate by checking, after every prefix of one bounded
deterministic history, that:

```text
incremental overlay state == independent Python reference state
                          == independent PostgreSQL SQL-oracle state
```

The history must cover all nine committed overlay event variants, requirement
supersession, policy-only certificate rebinding, same-edge provenance repair,
selected-edge rebuild with an alternate covering matching, and exact replay.
Independent oracles compare matching completeness and certificate validity,
not the identity of alternate valid perfect matchings.

## Path-exclusive ownership

This coordinator lane owns only:

- `docs/workstreams/m5_integration/THREE_ORACLE_EXECUTION_PLAN_2026-08-05.md`;
- `tests/m5/postgres/test_three_oracle_history.py`;
- an optional result note under `docs/workstreams/m5_integration/` after the
  executable gate passes.

No production source, migration, SQL-oracle, frozen contract, top-level status,
acceptance matrix, evaluation, presentation, or candidate-worktree path may be
edited in this lane. The user-owned unstaged `pyproject.toml` and untracked
`docs/presentations/` files remain outside the lane.

## Test construction

1. Advance one continuous in-memory repository and overlay through the
   deterministic history; apply each event through the repository event layer
   and the already-committed overlay transition.
2. Compare the four maintained state families exactly with
   `compute_reference_states()` and validate current group/claim certificates
   with the Python validators.
3. In a rollback-only transaction inside the installed test schema, load a
   `PostgresSnapshot` followed by the M5 semantic repository snapshot.
4. Read and compare the SQL oracle before writing any M5 materialized state.
   Do not use `build_m5_bootstrap_projection()`, whose states are themselves
   derived from the SQL oracle.
5. Persist the overlay-derived states and current certificate artifacts with
   `publish=False`, force deferred constraints, and require all seven mismatch
   counters to be zero. Those counters cover the four state families, current
   SQL certificate validity, assignment disagreement, and assignment-cap
   exhaustion without repeating the same expensive oracle views.
6. Add exact transition assertions for provenance repair, alternate-cover
   rebuild, zero-flip policy rebind, structural replacement/retirement, and
   replay with zero new work, bindings, artifacts, or state mutation.

## Boundary

This gate loads each logical checkpoint into fresh rollback-isolated database
rows. It validates the independent SQL recomputation and persisted certificate
validators against the same checkpoint-specific versioned base snapshot used
by the incremental and Python oracles.
It is not a durable same-schema mutation history, an M5.3-07 failure/replay
runtime, an M5.4 activation/publication transaction, or production latency
evidence. Existing adapters are snapshot/insert oriented; durable working-state
interval maintenance remains a later coordinator/runtime task.

## Gates

- focused offline test: live test skips explicitly when no database DSN exists;
- focused live three-oracle test against the repository PostgreSQL service;
- complete live `tests/m5` suite;
- complete ordinary repository suite with opt-in model/long gates disabled;
- Ruff lint and owned-file format checks;
- strict mypy for source plus the owned test file;
- compileall, `pip check`, `git diff --check`, and protected-file hash audit;
- independent read-only review before the lane is committed.
