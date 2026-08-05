# M5.3 PostgreSQL schema and third-oracle handoff

Status: owned implementation and P1 audit complete; candidate **GO** for
sequential coordinator integration; integrated M5.3 exit gate **NO-GO**

Date: 2026-08-05

Branch: `workstream/m5-postgres-oracle`

Current merge base with `main`: `c6dd2a85478511ff066b1e98653cc3c676e2305b`

Restart checkpoint: `abca7e3935bb46b18773cbff0cc7d68c104048f2`

P1 repair committed after independent audit and a post-format focused rerun:
`1e33c019bb48921c369f2f7808a34e0414de2630`

## Outcome

This lane delivers migration 014, the independent PostgreSQL full-
recomputation oracle, atomic content-ledgered installation, typed persistence
adapters, and live schema-isolated tests. The current candidate is locally
green and independently audited after the restart checkpoint. The M4 runtime
shared-reader/writer compatibility repairs from `f7394f7` and `c6dd2a8` are
present and have live coexistence coverage. This does not declare the
integrated M5.3 milestone complete: the M5.2 incremental overlay, accepted
public activation transaction/receipt, and post-merge incremental/Python/SQL
histories remain separate gates.

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
2. `install_m5_core_bundle()` checks its required 013 relations and exact
   critical-trigger metadata, acquires the seven frozen `ACCESS EXCLUSIVE`
   locks before its first ledger read, and uses deterministic `ROW EXCLUSIVE`
   locks for the remaining trigger-catalog surfaces. It executes migration and
   oracle atomically, forces deferred constraints, and writes an immutable
   content ledger. Two concurrent installers serialize to one apply and one
   exact replay. The preflight is deliberately scoped to critical M5/M4
   prerequisite surfaces; it is not a byte-proven assertion about every legacy
   trigger.
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
bundle_sha256:                9bce4ae68a434aefbc196b6367b77454f4ec65d531f4ff206f37fb4d69e9167a
migration_sha256:             4c37626f24524316c7990b2f3d213573bbe0f805a2c0f884585bf6aa325f0330
oracle_sha256:                00c533e1789a1415a5571ee5bfe9df4f36577089754ca8be00ad6c7172b11b7b
prerequisite_source_sha256:   187f2b0ca5ac10fa9e1d745d061ab7adad11e9c45db76d5f0147198174332179
```

The prerequisite value is the identity of the exact local ordered 000--013
source set. Historical migrations had no database content ledger, so it is not
claimed as byte-provenance proof for an arbitrary pre-existing database. The
installer separately performs a live structural catalog preflight.

## Validation evidence

Database: PostgreSQL `16.14 (Debian 16.14-1.pgdg12+1)` with `vector 0.8.5`,
`btree_gist 1.7`, and `pgcrypto 1.3`. The coordinator started only the repository
`db` service with `docker compose up -d db`; it was not restarted or stopped.
Every test creates and drops its own schema.

```text
corrected coexistence regression: 1 passed
tests/m5/postgres: 55 passed
targeted pre-M5 PostgreSQL/M4 runtime regression: 247 passed, 1 skipped
independent final P1/trigger/lock audit: 20 passed
post-format focused trigger/replay/lock selection: 9 passed
Ruff check: passed
Ruff format --check: passed, 11 files already formatted
mypy --strict: passed, 10 source files
compileall: passed
git diff --check: passed
pip check: no broken requirements
```

The single pre-M5 skip is
`tests/m4/integration/test_real_postgres_models.py`; it explicitly requires
`GROUNDLOOP_RUN_M4_REAL_POSTGRES_SMOKE=1` and real model execution. It is not
counted as live-model evidence.

Important live adversaries include:

- fresh/populated upgrade, exact replay, hash conflict, failure after schema,
  failure after oracle, open-epoch rejection/retry, concurrent legacy writer,
  two concurrent installers, dropped/disabled critical triggers, both deferred
  required-claim triggers, and semantic-observation immutability;
- catalog stabilization that permits ordinary DML while serializing trigger
  DDL behind the installer; an additional ad hoc, non-checked-in DDL-first race
  blocked the installer and then failed closed at preflight;
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

The full repository suite was not run at this checkpoint. The commands above
are the bounded PostgreSQL-owned, relevant compatibility, and static evidence;
the independent audit did not edit the candidate.

## Gate assessment

| Gate | Local result | Boundary |
|---|---|---|
| M5.3-01 bundle/upgrade/races | PASS | Includes exact critical-trigger preflight, two-installer, writer, and trigger-DDL serialization |
| M5.3-02 typed subjects/eligibility | PASS | Includes immutable eligibility and legacy required-claim prerequisite enforcement |
| M5.3-03 group integrity/lifecycle | PASS | Empty, >8, fork, failure/retry, retirement tested |
| M5.3-04 loader/currency/coexistence | PASS locally | Populated requirement rows stay invisible to v1 bootstrap/reconnect/withdrawal/publication/replay |
| M5.3-05 SQL Hall/assignment oracle | PASS | Base-edge Hall and capped recursive audit tested |
| M5.3-06 three-oracle event history | PENDING integration | Requires accepted M5.2 incremental lane merged with this SQL lane |
| M5.3-07 runtime failure/replay | PENDING integration | Bundle rollback and group failure pass; full M5.4 durable runtime not in scope |
| M5.3-08 indexes/no Hall truth read | PASS | Static SQL audit and live plan evidence |
| M5.3-09 extensions/regressions | PASS locally | 247 passed, one explicit real-model skip |

## Mandatory coordinator work

The canonical machine-readable inventory is
`m4_shared_surface_inventory.json`; `M4_SHARED_SURFACE_PATCH_PLAN.md` explains
the classifications. It records the ten formerly blocking runtime findings
and their resolved mechanical evidence in `src/groundloop/m4/pipeline.py`:
eight claim-only reads now have a `subject_kind = 'claim'` boundary and two
shared-currency INSERTs now have explicit columns. Audit counters, job-bound
reads, benchmark seeding, tests, and scripts are separately classified so they
are not confused with runtime contamination.

Before integrated M5.3 can be GO, the coordinator must:

1. integrate this audited PostgreSQL candidate on current `main` without
   absorbing unrelated presentation work, then rerun the merge gates;
2. finish and independently audit M5.2, then run incremental == Python
   reference == SQL after every frozen event class, including policy and
   provenance-only certificate changes;
3. implement and race-test the accepted public activation request/receipt
   transaction using these schema-owned relations and digest-neutral projection;
4. integrate migration 015 and rerun the full PostgreSQL/runtime suite;
5. resolve or explicitly disposition the non-runtime inventory debt recorded
   in `M4_SHARED_SURFACE_PATCH_PLAN.md`.

No pre-TEE, privacy, neural-quality, or production-readiness claim is made by
this workstream.
