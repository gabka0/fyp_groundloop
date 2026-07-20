# M4 Crash-Atomicity Acceptance Matrix Handoff

Status: independent test-only lane

Owned paths:

- `tests/m4/crash_matrix/**`
- `docs/workstreams/m4_crash_matrix/**`

Forbidden and untouched paths:

- production source and migrations;
- existing tests;
- shared M4 design, status, roadmap and decision documents.

## Contract tested

The harness creates a new committed PostgreSQL schema for every `(execution
mode, failure point)` case. It does not wrap a test in one outer transaction.
Setup is committed, the production microtransaction is crashed, the original
connection is closed, and a new connection compares the entire logical
contents of every base table in the schema with the pre-operation projection.

The canonical projection contains:

- every base table in the isolated schema, including M1-M4 base, working,
  publication, provenance, evaluation and execution-accounting tables;
- every user column, including timestamps and JSON manifests;
- every row as canonical `jsonb`, sorted independently of physical row order;
- table and column identity, so a missing table or column cannot disappear
  silently.

It intentionally excludes only PostgreSQL system-catalog/physical metadata
(`xmin`, `ctid`, table OIDs, transaction IDs) and sequence allocator state.
Those are not logical table contents; PostgreSQL sequences are deliberately
non-transactional. Derived views are not copied because exact equality of all
their base tables implies equality of deterministic views.

## Discovered production failure points

Structural open, runtime store:

- `open_structural_written`
- `open_rows_written`

Runtime child closure:

- `completion_children_written`
- `completion_parent_written`

Pipeline discovery/expansion:

- `expansion_discovery_persisted`
- `expansion_runtime_completed`
- `expansion_evaluation_synced`

Pipeline verifier completion:

- `verifier_runtime_completed`
- `verifier_observation_archived`
- `verifier_overlay_written`
- `verifier_state_written`

Pipeline publication/seal, including prefixed runtime-store hooks:

- `publication_store_seal_checked`
- `publication_structure_promoted`
- `publication_currency_promoted`
- `publication_states_written`
- `publication_head_advanced`
- `publication_evaluation_promoted`
- `publication_store_seal_publication_written`
- `publication_store_seal_epoch_written`

Each exposed point is parameterized in both `AUDIT` and `MEASURED` modes when
the coordinator-owned `M4ExecutionMode` contract is present. On the lane's
older baseline, the `MEASURED` cases are executable xfails rather than false
passes; they become ordinary cases after the incrementality commit is merged.

Validation against the coordinator's current M4 incrementality worktree
state, which includes migration 009 and `M4ExecutionMode`, produced:

```text
38 passed, 1 xfailed
```

That is 19 exposed failure points in each of the two execution modes. The
current integrated schema contains 58 base tables; every case compared all 58
before and after rollback/reconnect. The one xfail is the structural hook
granularity gap below, not a failed rollback case.

Validation on this lane's older committed baseline produced 19 AUDIT passes,
19 expected MEASURED capability xfails, and the same structural-contract
xfail. These mode xfails disappear when the lane commit is integrated after
the coordinator's incrementality commit.

## Contract gap

`PostgresM4ApplicationPorts.open_event` does not currently expose injection
points after its individual structural callback groups. The lower-level
runtime store provides only one point after the entire callback and one after
root/scope declaration. This cannot prove the acceptance-matrix requirement
to inject after every logical structural SQL step.

The harness therefore contains a strict executable xfail requesting these
application-level hooks:

- `structural_registry_written`
- `structural_versions_written`
- `structural_withdrawal_written`
- `structural_working_states_written`
- `structural_evaluation_written`

The coordinator should add these calls inside the existing structural-open
transaction and then remove the xfail marker. The hook must raise through the
same transaction; it must not create intermediate commits or savepoint-owned
partial state.

## Validation command

```bash
set -a
source /home/kassym/Desktop/groundloop/.env
set +a
PYTHONPATH=src GROUNDLOOP_TEST_DATABASE_URL="$GROUNDLOOP_DATABASE_URL" \
  /home/kassym/Desktop/groundloop/.venv/bin/pytest -q tests/m4/crash_matrix
```

Also run:

```bash
/home/kassym/Desktop/groundloop/.venv/bin/ruff check tests/m4/crash_matrix
/home/kassym/Desktop/groundloop/.venv/bin/python -m compileall -q \
  tests/m4/crash_matrix
```
