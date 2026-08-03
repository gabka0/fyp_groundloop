# M5.3 PostgreSQL Oracle Workstream Plan

Status: owned implementation complete; coordinator integration remains NO-GO

Branch: `workstream/m5-postgres-oracle`

Base: `9df1dcedcfa454f3a686c115444cadf8e1cd3c8d`

## Owned paths

- `migrations/014_m5_evidence_groups.sql`
- `sql/m5/`
- `src/groundloop/postgres/m5.py`
- `src/groundloop/postgres/migrations.py`
- narrowly scoped `src/groundloop/postgres/snapshot.py` changes
- `tests/m5/postgres/`
- `docs/workstreams/m5_postgres_oracle/`

All core M5.1 contracts, matching/runtime code, migrations 000--013 and 015,
M4 code, dependency configuration, presentations, and top-level status/contract
documents are forbidden.

## Execution

1. Pin legacy schema construction to exactly migrations 000--013 and inventory
   every altered/shared PostgreSQL surface.
2. Implement the transactional, hash-ledgered 013-to-014 schema-plus-oracle
   bundle and frozen lock/open-epoch protocol.
3. Implement typed subject integrity, lifecycle/history, M5 semantic state and
   immutable certificate persistence, activation/head barriers, and effective
   views.
4. Implement the independent base-relation SQL oracle, capped recursive
   assignment audit, certificate validators, and mismatch views.
5. Add typed Python bundle/snapshot/oracle adapters and live schema-isolated
   tests for upgrade, rollback, concurrency, history, constraints, plans, and
   oracle agreement.
6. Run focused tests, relevant pre-M5 PostgreSQL regressions, Ruff, strict mypy,
   compileall, and repository hygiene checks. Record every skip/failure.

Cross-lane changes that prove necessary will be recorded as coordinator patch
requests rather than edited here.
