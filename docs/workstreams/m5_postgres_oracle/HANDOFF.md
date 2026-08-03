# M5.3 PostgreSQL schema and third-oracle handoff

Status: owned implementation complete; integrated M5.3 exit gate **NO-GO**

Date: 2026-08-03

Branch: `workstream/m5-postgres-oracle`

Base: `9df1dcedcfa454f3a686c115444cadf8e1cd3c8d`

## Outcome

This lane delivers migration 014, the independent PostgreSQL full-
recomputation oracle, atomic content-ledgered installation, typed persistence
adapters, and live schema-isolated tests. The owned implementation is locally
green. It does not declare the integrated M5.3 milestone complete because the
coordinator still owns the M4 shared-reader/writer compatibility patch, the
accepted public activation transaction/receipt, and the post-merge
incremental/Python/SQL history.

## Delivered paths

- `migrations/014_m5_evidence_groups.sql`
- `sql/m5/full_recompute_oracle.sql`
- `src/groundloop/postgres/m5.py`
- `src/groundloop/postgres/migrations.py`
- narrow explicit-order/explicit-column changes in
  `src/groundloop/postgres/snapshot.py`
- `tests/m5/postgres/`
- `docs/workstreams/m5_postgres_oracle/`

No M4-owned source, M5.1/M5.2/M5.4 contract/runtime source, migration 015,
dependency configuration, top-level status, or presentation file was edited.

## Core behavior

1. The frozen ordered 000--013 source set is literal rather than discovered by
   glob. M5 uses an explicit ordered two-file bundle.
2. `install_m5_core_bundle()` checks the live 013 catalog shape, acquires the
   seven frozen `ACCESS EXCLUSIVE` locks before its first ledger read, executes
   migration and oracle atomically, forces deferred constraints, and writes an
   immutable content ledger. Two concurrent installers serialize to one apply
   and one exact replay.
3. Migration 014 backfills and maintains typed claim/requirement subjects,
   enforces subtype integrity, immutable eligibility, all currency-holder
   guards, canonical normalization, group lifecycle/lineage/retirement,
   revision history, state, certificate, binding, activation/head, and runtime-
   mode surfaces without adding a new enum.
4. Persisted text arrays reject NULL/blank elements. Requirement witness arrays
   additionally require sorted unique lowercase SHA-256 values. Canonical SQL
   ordering uses `COLLATE "C"` where it must agree with Python lexical order.
5. Working currency reads use exact revision intervals, explicit tombstones,
   and published fallback. A first change compares the effective prior holder,
   so redundant fallback reinsertion and absent-to-tombstone no-ops fail.
6. The SQL oracle derives decisions, edges, requirement/group/combined claim/
   answer state from base relations. It uses Hall subset deficiency and a
   separate recursive unmatched-branch `UNION` audit with the frozen H/E/state
   caps. It never reads a Hall materialization.
7. Group and v2 claim certificate validators are epoch/revision aware. Claim
   as-of validation resolves the group certificate actually bound at the
   requested revision, not the latest working state pointer; positive-to-
   positive certificate repair preserves earlier history.
8. `postgres.m5` deliberately does not define a public activation DTO, receipt,
   payload recipe, or competing bootstrap digest. It exposes digest-neutral
   bootstrap projection/persistence primitives for the accepted coordinator-
   owned `m5-activation-request-v2` transaction.
9. The only Python reference-oracle import is function-local inside bootstrap
   certificate construction. Checked-in SQL is the independent third oracle
   and never invokes Python.

## Bundle identity

```text
bundle_id:                    m5-core-schema-bundle-v1
bundle_sha256:                e303d16b914dc36dde0851bdd2602d42f359cdfa6aa38795458f156481a03264
migration_sha256:             ef3265578f587516ff6aeebd45af4057b180efa1179d50769eb145ca0a6f3a64
oracle_sha256:                a68cf814ace82d68e15b08611f0f040c8b713dce1a8b64ce89a8895ba42cce33
prerequisite_source_sha256:   187f2b0ca5ac10fa9e1d745d061ab7adad11e9c45db76d5f0147198174332179
```

The prerequisite value is the identity of the exact local ordered 000--013
source set. Historical migrations had no database content ledger, so it is not
claimed as byte-provenance proof for an arbitrary pre-existing database. The
installer separately performs a live structural catalog preflight.

## Validation evidence

Database: PostgreSQL `16.14 (Debian 16.14-1.pgdg12+1)` with `vector 0.8.5`,
`btree_gist 1.7`, and `pgcrypto 1.3`. The shared Docker service was not started,
stopped, or restarted by this lane during validation; every test creates and
drops its own schema.

```text
tests/m5/postgres: 43 passed
tests/m5/reference: 73 passed
targeted pre-M5 PostgreSQL/M4 runtime regression: 172 passed, 1 skipped
Ruff check: passed
Ruff format --check: passed, 11 files already formatted
mypy --strict: passed, 3 source files
compileall: passed
git diff --check: passed
```

The single pre-M5 skip is
`tests/m4/integration/test_real_postgres_models.py`; it explicitly requires
`GROUNDLOOP_RUN_M4_REAL_POSTGRES_SMOKE=1` and real model execution. It is not
counted as live-model evidence.

Important live adversaries include:

- fresh/populated upgrade, exact replay, hash conflict, failure after schema,
  failure after oracle, open-epoch rejection/retry, concurrent legacy writer,
  and two concurrent installers;
- both runtime-mode race orders: durable v1 open wins and blocks activation,
  or activation wins and rolls back the losing v1 open;
- committed `publish=True` bootstrap followed by reconnect/read of every
  published state family and group/claim certificate binding;
- all 29 normalization code points, typed raw-ID collision, orphan/wrong
  subtype, ineligible holders on all four surfaces, NULL/blank/non-SHA arrays,
  empty and ninth-requirement groups, semantic duplicates, live lineage forks,
  failed replacement/retry, retirement reopen, and successor-after-retirement;
- Hall counterexample, isolated-left partial matching, group-only composition,
  assignment cap, current mismatch/certificate validation, selected-edge loss,
  and historical old/new certificate bindings across a provenance-only repair;
- catalog-valid indexes plus an `EXPLAIN` proof that the group/policy
  certificate lookup index is usable with sequential scans disabled.

One broader command was attempted with an incomplete `PYTHONPATH` and stopped
during collection with five missing `experiments.m4_change_aware_verifier`
imports. It is recorded as a failed/non-evidence command. The corrected,
PostgreSQL-relevant 173-test command above passed with the one explicit real-
model skip.

## Gate assessment

| Gate | Local result | Boundary |
|---|---|---|
| M5.3-01 bundle/upgrade/races | PASS | Includes two-installer and writer serialization |
| M5.3-02 typed subjects/eligibility | PASS | All shared and M5 history holder guards tested |
| M5.3-03 group integrity/lifecycle | PASS | Empty, >8, fork, failure/retry, retirement tested |
| M5.3-04 loader/currency/coexistence | NO-GO integrated | Owned loader/history passes; M4 patch inventory remains |
| M5.3-05 SQL Hall/assignment oracle | PASS | Base-edge Hall and capped recursive audit tested |
| M5.3-06 three-oracle event history | PENDING integration | Requires accepted M5.2 incremental lane merged with this SQL lane |
| M5.3-07 runtime failure/replay | PENDING integration | Bundle rollback and group failure pass; full M5.4 durable runtime not in scope |
| M5.3-08 indexes/no Hall truth read | PASS | Static SQL audit and live plan evidence |
| M5.3-09 extensions/regressions | PASS locally | 172 passed, one explicit real-model skip |

## Mandatory coordinator work

The canonical machine-readable inventory is
`m4_shared_surface_inventory.json`; `M4_SHARED_SURFACE_PATCH_PLAN.md` explains
the classifications. It records ten production blockers in
`src/groundloop/m4/pipeline.py`: eight claim-only reads without a mechanical
`subject_kind = 'claim'` boundary and two positional shared-currency INSERTs.
Audit counters, job-bound reads, benchmark seeding, tests, and scripts are
separately classified so they are not confused with runtime contamination.

Before integrated M5.3 can be GO, the coordinator must:

1. apply the M4 compatibility patch without changing v1 digest/event/replay
   identities;
2. run activated coexistence with live requirement currency through v1
   bootstrap, reconnect, withdrawal, publication, and exact replay;
3. implement and race-test the accepted public activation request/receipt
   transaction using these schema-owned relations and digest-neutral projection;
4. merge M5.2 and run incremental == Python reference == SQL after every frozen
   event class, including policy and provenance-only certificate changes;
5. integrate migration 015 and rerun the full PostgreSQL/runtime suite.

No pre-TEE, privacy, neural-quality, or production-readiness claim is made by
this workstream.
