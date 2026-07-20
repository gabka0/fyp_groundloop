# M4.4i Event-Audit Persistence Handoff

Status: implemented and validated in the isolated event-audit lane.

## Scope

This lane adds one independent component:

- `src/groundloop/m4/event_audit.py`
- `tests/m4/event_audit/`

It does not edit or import selective admission, the M4 pipeline, runtime delta
code, migrations, shared contracts, the top-level CLI, or `pyproject.toml`.

## Contract

`run_and_persist_event_audit(...)` accepts:

1. a PostgreSQL autocommit connection;
2. an `EventAuditSpec` containing the complete frozen `Bw`, selective `Bs`,
   registered-claim, active-chunk, inserted-chunk, refresh-policy, split and
   deliberate-probe inputs;
3. an exhaustive pair judge; and
4. an independent snapshot-refresh judge.

The runner:

1. independently recomputes and validates the supplied `Bw` and `Bs` states;
2. verifies that the referenced row is an M4 event in `committed / sealed /
   complete` state;
3. runs `registered claims x inserted active chunks` through
   `run_full_pair_audit`;
4. builds `Bx` with `compute_exhaustive_additive_delta`;
5. runs exact brute-force `SnapshotRefresh_k` from the active claim/chunk
   inputs;
6. compares selective state separately with `Bx` and `SnapshotRefresh_k`;
7. records all omitted verifier-positive pairs and the explicitly designated
   deliberate probes detected among them;
8. persists the complete inputs, judgments, states, affected sets and event
   provenance in `groundloop_impact_evaluation_run.manifest`;
9. returns exact replay from the persisted record without invoking either
   judge; and
10. rejects changed frozen inputs or corrupted stored content.

The deterministic run ID binds protocol version, event, treatment, split and
audit/refresh configuration. The manifest separately binds every input and
result with canonical JSON SHA-256 digests. The baseline manifest combines the
full-pair and snapshot-refresh manifest identities; it does not collapse their
different semantics.

## Exact/empirical boundary

The runner makes no neural-quality claim. The pair judges and refresh policy
are empirical inputs. Exactness covers:

- exhaustive Cartesian pair enumeration;
- deterministic state recomputation from the returned judgments;
- same-baseline affected-set computation;
- event-link validation;
- content validation, conflict rejection and idempotent persistence.

## Validation

The lane-specific gate is:

```bash
set -a
source .env
set +a
GROUNDLOOP_TEST_DATABASE_URL="${GROUNDLOOP_TEST_DATABASE_URL:-$GROUNDLOOP_DATABASE_URL}" \
  .venv/bin/pytest -q tests/m4/event_audit
.venv/bin/ruff check src/groundloop/m4/event_audit.py tests/m4/event_audit
.venv/bin/mypy --strict src/groundloop/m4/event_audit.py
```

Observed result on 2026-07-20:

- PostgreSQL tests: `6 passed`;
- Ruff: passed;
- strict mypy: passed for the new source module.

The tests create and remove a unique live PostgreSQL schema. They prove sealed
event linkage, deliberate positive-miss detection, independent refresh
comparison, read-only replay with zero judge calls, changed-input conflict
rejection, stored-result corruption detection, unsealed-event rejection, and
the absence of selective admission/pipeline/runtime imports.

## Limitations

- The current table carries the event link inside a content-validated JSONB
  manifest. PostgreSQL does not enforce that link with a typed foreign key.
- Exact replay assumes the declared judge identities refer to immutable judge
  artifacts. A changed judge must use a changed identity.
- The component runs one post-event `SnapshotRefresh_k`. A paired before/after
  refresh study should invoke it for each frozen snapshot as two evaluation
  records or receive a future typed paired-run contract.
- JSONB is adequate for bounded event audits but is not an efficient analytic
  schema for large history-level experiments. Event-level metrics should be
  normalized before M4.6 scale runs.
- No real models are loaded or evaluated by this lane.
