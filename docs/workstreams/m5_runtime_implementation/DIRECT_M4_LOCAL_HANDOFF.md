# M5 Cursor-Local Direct-M4 Handoff

Status: integration-ready Lane D2 cursor-local adapter after the reconnect
audit; outer typed coordinator composition remains intentionally outside this
lane

Date: 2026-08-06

Branch: `workstream/m5-direct-m4-local`

Exact base: `a66d29fa44e2eeaede7759b09a9fda36c5f8729e`

Reconnect-audit parent: `efd975cf13eb76d188de60fd77262873ab83d817`

Authority read: M5-D21, M5-D23, runtime addendum revision 4, the D1 public-M4
barrier handoff, and the coordinator's D2 path manifest.

## Implemented boundary

`PostgresM5DirectM4Adapter` exposes the six frozen cursor-local operations:

1. exact `StructuralPayload`-bound direct open;
2. deterministic attempt acquisition/dispatch;
3. discovery expansion completion;
4. verifier observation completion;
5. direct failure projection; and
6. direct seal projection.

Each method verifies that its cursor belongs to the same PostgreSQL connection
as the M4 application ports. The extracted persistence/evaluation helpers use
the caller's already-open transaction. They do not call `commit`, `rollback`,
or `Connection.transaction`, and the unchanged public M4 methods retain their
single-transaction wrappers.

The direct open runs the existing M4 payload, registry, withdrawal, immutable
replay, structural-row, working-state, and compact-evaluation recipes. It is
therefore the migration-015 current-transaction sidecar route authorized by
M5-D21; it does not reconstruct or synthesize structural content from the
smaller event DTO.

Exact open replay is valid at the current shared revision, including after a
job has been acquired and after a discovery scope has closed. Replay compares
the immutable update, policy/registry, root jobs, original open-scope
declarations, scope membership, and structural manifest. It normalizes only
the stored mutable `closed_revision` to the original open declaration. A
caller-supplied `DiscoveryScope(closed=True)` remains invalid. The
revision-1/committed guard remains mandatory whenever no M4 declaration exists;
it is not incorrectly reapplied to an exact existing declaration.

Acquisition preserves the exact M4-v1 logical attempt ID and requires the
caller-supplied lease hash to equal the deterministic
`m4-lease-token-v1` digest. Expansion and verifier completion preserve the
point/CAS revision, child closure, artifact, observation, working currency,
affected-state, frontier, provenance-hook, and compact-evaluation recipes.

## Authority retained by the outer M5 coordinator

This adapter does not provide a combined coordinator and does not claim an M5.4
PASS by itself. In particular:

- direct failure writes only the immutable M4 failure reason and compact M4
  evaluation failure projection; it does not fail or revise the shared base
  epoch;
- direct seal checks M4 readiness and promotes the direct structural,
  observation-currency, grounding-state, and compact-evaluation projections;
- direct seal does **not** advance either publication head;
- direct seal does **not** revise, seal, or otherwise finalize
  `groundloop_epoch`;
- direct seal deliberately emits no `groundloop_status_delta` rows; and
- no local method writes the combined M5 result, combined deltas, typed state,
  or final base semantic/evaluation state.

The outer typed coordinator must authorize and reconcile each shared
base/runtime revision, and at seal must complete the combined publication,
result, state references, both heads, and final base transition in the same
transaction.

## Failure-safe process-cache boundary

The SQL rows are semantic authority. Direct open and effective verifier
completion build detached process-cache candidates. The outer owner calls the
private `_after_outer_commit()` hook only after durable commit, or
`_after_outer_rollback()` after rollback. A cache-adoption exception clears
the active-cache identity and leaves committed SQL untouched; an exact open
replay can then hydrate the working cache from the durable overlay. A pending
cache outcome blocks later adapter operations, preventing accidental use of
ambiguous process state.

These hooks are internal composition plumbing, not additional mutation
operations. The detached cache construction is not physical-runtime evidence;
no new performance claim is made for the typed composition path.

## Live falsification coverage

`tests/m5/postgres_runtime/test_direct_m4_composition.py` runs against an
isolated activated migration-015 PostgreSQL schema and proves:

- the revision-1 epoch, M5 update, typed header, exact M4 update, structural
  rows, and both snapshot declarations commit only as one outer transaction;
- rollback after a successful direct open leaves no epoch, M4 update, document
  version, or adopted working cache;
- injected post-commit cache adoption failure cannot erase durable SQL and is
  recoverable through exact replay hydration;
- a missing first M4 declaration is still rejected if the shared typed epoch
  has already advanced beyond revision 1;
- caller-supplied closed-scope declarations are rejected rather than
  normalized into an accepted identity;
- a fresh ports/adapter instance accepts exact replay after root acquisition
  at shared revision 2, returns that current header, and hydrates the durable
  structural working overlay without changing SQL;
- another fresh instance accepts the same immutable declaration after the
  scope has durably closed at revision 3, keeps the closure intact, hydrates
  the working overlay, and successfully continues child acquisition and
  effective verifier completion to shared revision 5;
- direct acquisition, expansion, child acquisition, and verifier completion
  stay synchronized with explicit outer base/typed revision reconciliation;
- failure projection changes neither the shared base revision/state nor a
  publication head and rolls back exactly; and
- seal projection leaves both heads and the entire shared base row byte-equal,
  emits zero public status deltas, stages the expected direct state/evaluation
  promotion, and rolls all of that back with the outer transaction.

The fixture begins with an unsupported M4 claim and completes a fresh support
observation, so the zero-public-delta seal assertion is non-vacuous.

## Validation evidence

Executed on PostgreSQL 16.14 with
`GROUNDLOOP_TEST_DATABASE_URL=postgresql://groundloop:groundloop@localhost:5432/groundloop`:

```text
python -m pytest -o addopts='' -q \
  tests/m5/postgres_runtime/test_direct_m4_composition.py
  -> 3 passed

python -m pytest -o addopts='' -q \
  tests/m5/postgres_runtime/test_direct_m4_composition.py \
  tests/m5/postgres_runtime/test_m4_typed_barrier.py \
  tests/m4/point_runtime/test_point_runtime.py \
  tests/m4/evaluation_overlay \
  tests/m4/persistence/test_runtime_store.py
  -> 32 passed

python -m pytest -o addopts='' -q tests/m4
  -> 478 passed, 7 skipped
```

The full M4 run includes the measured-kernel source guards, physical statement
fingerprint/history gates, public pipeline suites, and the exact point-runtime
scale test. The point-runtime test retains the frozen `(9, 9)` statement-count
result across unrelated job scale; public SQL remains routed through the same
connection executor so its counting/fingerprint instrumentation is unchanged.

```text
ruff check <four changed source files> \
  tests/m5/postgres_runtime/test_direct_m4_composition.py
  -> PASS

ruff format --check \
  tests/m5/postgres_runtime/test_direct_m4_composition.py
  -> PASS

MYPYPATH=src mypy --strict <four changed source files>
  -> Success: no issues found in 4 source files

python -m compileall -q src tests
  -> PASS

git diff --check
  -> PASS
```

The seven M4 skips are pre-existing environment/optional-artifact skips; they
are not hidden failures or direct-bridge evidence.

## Integration notes

Owned paths in this commit are limited to:

- `src/groundloop/m5/runtime/direct_m4.py`;
- `src/groundloop/m4/persistence.py`;
- `src/groundloop/m4/pipeline.py`;
- `src/groundloop/m4/evaluation_overlay.py`;
- `tests/m5/postgres_runtime/test_direct_m4_composition.py`; and
- this handoff.

The coordinator should wire the adapter into the outer typed application only
after resolving its exact cache-hook call sites. It must not route typed epochs
back through the public M4 methods, relax the D1 barrier, or treat this local
projection commit as independent publication authority.
