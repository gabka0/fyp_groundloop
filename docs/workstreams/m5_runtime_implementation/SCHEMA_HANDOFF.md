# M5 Runtime Schema Bundle Handoff

Status: integrated on main; M5-D22 state-artifact amendment locally validated

Branch: `workstream/m5-runtime-schema-015`

Base: `d3dcc8e07094e014637016b736e87b263bd21030`

Candidate head: the commit containing this handoff; resolve with
`git rev-parse HEAD` before integration.

Contract read: frozen M5-D1--M5-D22, including revision 3 of
`docs/workstreams/m5_runtime_contract/CANDIDATE_RUNTIME_ADDENDUM.md` as
recorded on main at `40579a0` during lane execution.

## Owned result

The lane adds the atomic `m5-runtime-schema-bundle-v2` installer and migration
015. It creates the 26 concrete runtime relations enumerated by the frozen
Section 16 schema:

1. `groundloop_m5_candidate_policy`
2. `groundloop_m5_requirement_registry_snapshot`
3. `groundloop_m5_requirement_registry_snapshot_member`
4. `groundloop_m5_active_chunk_snapshot`
5. `groundloop_m5_active_chunk_snapshot_member`
6. `groundloop_m5_runtime_epoch`
7. `groundloop_m5_discovery_scope`
8. `groundloop_m5_requirement_channel_hit`
9. `groundloop_m5_requirement_scope_selection`
10. `groundloop_m5_requirement_discovery_result`
11. `groundloop_m5_requirement_admitted_pair`
12. `groundloop_m5_requirement_admitted_pair_source`
13. `groundloop_m5_semantic_job`
14. `groundloop_m5_job_dependency`
15. `groundloop_m5_job_attempt`
16. `groundloop_m5_attempt_result_artifact`
17. `groundloop_m5_requirement_pair_input`
18. `groundloop_m5_requirement_verifier_artifact`
19. `groundloop_m5_requirement_verifier_execution`
20. `groundloop_m5_requirement_frontier_head`
21. `groundloop_m5_owner_pending_counter`
22. `groundloop_m5_answer_pending_counter`
23. `groundloop_m5_runtime_work`
24. `groundloop_m5_event_result`
25. `groundloop_m5_event_result_delta`
26. `groundloop_m5_event_result_state_reference`

The schema implements immutable payload tables, guarded expected-revision
transitions, deferred aggregate/counter/digest checks, frozen-snapshot and
semantic-pair bindings, dense canonical child sets, attempt/result and
verifier identities, terminal event-result replay artifacts, and point-lookup
indexes.

## Installation and compatibility

`install_m5_runtime_bundle()`:

- verifies the exact accepted migration-014 ledger row, required catalog, and
  immutable ledger trigger before any 015 DDL;
- locks runtime mode, the M4 head, the M5 head, and the epoch writer surface in
  the frozen order;
- rejects any committed pending/complete epoch;
- installs DDL, forces deferred constraints, and writes the content-bound
  runtime bundle ledger in one transaction;
- supports exact idempotent rerun, rejects content/hash conflicts, and leaves
  no partial schema or replacement function after injected failure; and
- preserves runtime mode, both publication heads, and activation state. It
  does not activate M5.

Migration 015 does not edit migration 014. M5-D21 authorizes its sole
replacement of an existing object:
`CREATE OR REPLACE FUNCTION groundloop_m5_guard_v1_open()`. The existing M4
trigger remains byte-definition-identical. In `v1_only`, legacy direct M4
opens remain valid. In `m5_active`, a document M4 row is accepted only with an
exact epoch/M5-update/revision-1 runtime sidecar inserted in the same SQL
transaction. Deferred validation enforces document/M4 bijection and forbids an
M4 row for non-document typed updates. The outer typed coordinator remains the
final combined-state authority.

M5-D22 adds four SQL helpers whose bytes match the frozen Python
`m5-*-state-artifact-v2` recipes for requirement, group, claim, and answer
state. The deferred event-result validator now resolves every changed-state
reference to the exact historical published state/certificate coordinate and
rejects a missing or mismatched artifact hash. Certificate references use the
immutable certificate digest directly. This amendment changes the
content-bound migration-015 bundle identity but no migration-014 byte or
semantic-state recipe.

## Corrections captured during review

- `candidate_policy_id` is the exact `m5-candidate-policy-v2` manifest hash;
  the ID itself is not hashed into that content identity.
- Both runtime-header snapshot foreign keys are initially deferred, preserving
  the frozen header -> staged structure -> snapshots insertion order.
- Runtime work is keyed by `(structural_event_id, work_kind)`. Its digest is a
  content identity, not an occurrence identity: event and call work may have
  equal digests, and different events may repeat a digest.
- Every terminal result requires exactly one event-work and one call-work row.
- A failed runtime binds to base structural/semantic/evaluation states
  `failed/failed/failed`; nonfailed states bind to the corresponding committed
  base projection, and sealed binds to `committed/sealed/complete`.
- Durable results bind exactly to the base event, epoch, payload, runtime
  terminal outcome, and (when sealed) the frozen M4 publication identity.
- A typed epoch may open and terminally fail in the same transaction: the
  deferred INSERT-side bridge validates the recorded revision-1 header while
  allowing the current row to advance before commit.

## Validation evidence

Live PostgreSQL DSN:
`postgresql://groundloop:groundloop@localhost:5432/groundloop`

```bash
GROUNDLOOP_TEST_DATABASE_URL='postgresql://groundloop:groundloop@localhost:5432/groundloop' \
PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python -m pytest -q \
  tests/m5/postgres_runtime/test_migration_015.py \
  tests/m5/postgres/test_bundle_and_races.py
```

Result: PASS, 39 tests (24 migration-015 acceptance tests and 15 existing
bundle/race regressions).

Post-integration M5-D22/activation composition on 2026-08-06 passed all 75
collected live tests across migration 015, bundle/race regression,
failure/replay, and public activation modules. The activation subset was 8/8
and independently compared all six Python changed-state reference kinds with
the SQL artifact functions.

```bash
/home/kassym/Desktop/groundloop/.venv/bin/ruff check \
  src/groundloop/postgres/migrations.py \
  tests/m5/postgres_runtime/test_migration_015.py
PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/mypy --strict \
  src/groundloop/postgres/migrations.py
PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  -m compileall -q src tests
git diff --check
```

Result: all PASS.

The live acceptance matrix includes fresh and populated install, exact rerun,
hash conflict, absent/wrong 014 prerequisite, injected rollback, live-epoch
guard, exact relation catalog, immutable payloads, candidate identity,
snapshot digest/order, header-first register-group commit and rollback, D21
v1 and activated bridge cases, each recorded one-field bridge mismatch,
reordered/prior-committed rejection, document/non-document bijection,
state-specific terminal failure, wrong-payload rejection, missing-call-work
rejection, equal event/call work, and repeated work digest across events.

## Post-integration trigger correction

A coordinator production smoke test that inserted an actual discovery-scope /
root-job pair exposed a PL/pgSQL record-field defect in the shared deferred
root-closure trigger: a SQL `CASE` referenced fields belonging to both trigger
table row types. Migration 015 now selects the table-specific `OLD`/`NEW`
field through PL/pgSQL `IF` branches. The corrected migration was exercised by
a live register-group open, durable root cancellation/failure, and exact
terminal replay before the persistence checkpoint was accepted.

A later nonempty root-barrier smoke test exposed a second PL/pgSQL name-
resolution defect in `groundloop_m5_validate_admitted_pair_integrity()`: the
function declared its certificate-source loop variable as `source_row` and
also used `source_row` as a nested SQL table alias. PostgreSQL resolved the
nested references as the as-yet-unassigned record variable and rejected every
nonempty admitted-pair commit with `record "source_row" is not assigned yet`.
The coordinator changed only that nested alias to `source_entry`; the loop
variable and every SQL predicate/digest byte remain unchanged. The live R7
nonempty/overlapping-root barrier is the executable regression for this
correction.

That same live falsifier then reached the nested aggregate and exposed a
separate PostgreSQL syntax constraint: `SELECT DISTINCT reason.value` cannot
order by the different expression `reason.value COLLATE "C"`. The corrected
query selects the collated value under the same `value` name and orders by
that selected value. It preserves the bytewise `C` ordering required by the
digest contract and does not add, remove, or rewrite any reason value.

The same concurrent rerun exposed a test-isolation defect rather than another
schema defect: the catalog assertion for CHECK definitions filtered relation
names but not their namespace, so concurrent disposable schemas with the same
table names could disappear between catalog lookup and
`pg_get_constraintdef`, yielding `could not open relation with OID ...`.
The query now restricts `pg_namespace.nspname = current_schema()` like the
adjacent index query. This changes no production SQL and keeps concurrent
schema-isolated gates independent.

## Integration boundary

This is schema/installer evidence only. It does not implement the production
typed open/fail/read/replay coordinator and does not independently close
M5.3-07 or any M5.4--M5.6 gate. Integrate only these four R2-owned paths after
review; do not merge unrelated worktree state and do not treat this candidate
as activation or end-to-end runtime evidence.
