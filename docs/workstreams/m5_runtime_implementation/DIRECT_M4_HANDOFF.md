# M5 Public-M4 Barrier Handoff

Status: safe Lane D1 barrier slice; direct adapter blocked on a contract
amendment and intentionally not implemented

Date: 2026-08-06

Branch: `workstream/m5-direct-m4-bridge`

Base: `9d673d7ea3aa7add9d9b8eb706b601efab265012`

Authority read: M5-D21, the complete candidate runtime addendum revision 2,
and the coordinator manifest in
`docs/workstreams/m5_completion/EXECUTION_PLAN_2026-08-06.md`.

## Integration-ready boundary

`src/groundloop/m4/persistence.py` now rejects an epoch carrying a row in
`groundloop_m5_runtime_epoch` at every public M4 coordination mutation
entrypoint before that entrypoint can write:

- exact event resume through `open_epoch`;
- audit and point job acquisition;
- audit and point retryable failure;
- audit and point completion;
- audit and point terminal epoch failure; and
- audit and point seal.

The guard first resolves `groundloop_m5_runtime_epoch` through
`to_regclass`. A pre-015 schema therefore never references a missing
relation. With migration 015 installed but the database in `v1_only`, an
ordinary M4 epoch has no typed header and follows the unchanged v1 path.

The focused activated-schema test invokes all eleven public audit/point
surfaces and compares a canonical digest snapshot of every `groundloop_%`
base table before and after each rejection. The snapshots remain identical
and the seal callback is never entered. A separate live test proves that the
installed-015/`v1_only` M4 failure path still commits.

## Required contract amendment before a direct adapter

No `src/groundloop/m5/runtime/direct_m4.py` API is added in this checkpoint.
Implementing the current five-method draft would require either inventing
structural data or calling the public M4 acquirer that this barrier correctly
rejects. Neither is an acceptable implementation of M5-D21.

### 1. The direct-open signature omits the structural payload

The frozen draft currently says:

```text
stage_direct_open(
  cursor, event: DynamicEventPlan, withdrawal: StructuralWithdrawal,
  roots: tuple[LogicalJobSpec, ...], scopes: tuple[DiscoveryScope, ...]
) -> OpenEventReceipt
```

`DynamicEventPlan` carries only update identity, inserted/deactivated chunk
IDs, registered claim IDs, and the claim-registry snapshot ID. Exact M4
structural staging additionally requires the existing
`groundloop.m4.pipeline.StructuralPayload`, including:

- the deactivated document-version ID;
- inserted document ID, document-version ID, and content hash;
- every inserted chunk's ID, document-version ID, ordinal, text, and text
  hash;
- source URI and authority class;
- chunker version; and
- optional chunker artifact ID and matching input hash.

Those values produce the frozen `groundloop-m4-structural-payload-v1`
manifest and the document, metadata-overlay, document-version, chunk-version,
chunk-provenance, and structural-deactivation rows. They cannot be recovered
losslessly from `DynamicEventPlan` or `M5TypedEventPlan`.

The narrow amendment must carry one exact `StructuralPayload` for document
events through the typed open boundary and into `stage_direct_open`. One
explicit contract shape is:

```text
open_typed_event_atomically(
  event, direct_payload: StructuralPayload | None,
  direct_withdrawal, requirement_withdrawal,
  direct_roots, requirement_roots, requirement_root_set_hash
) -> OpenEventReceipt

stage_direct_open(
  cursor, event: DynamicEventPlan, payload: StructuralPayload,
  withdrawal: StructuralWithdrawal,
  roots: tuple[LogicalJobSpec, ...], scopes: tuple[DiscoveryScope, ...]
) -> OpenEventReceipt
```

The amendment must also state the non-document rule: `direct_payload`,
`direct_plan`, and all direct roots/scopes are absent together. The adapter
must persist `payload.manifest` byte-for-byte and run the existing M4
payload/registry/withdrawal validation and row-writing recipes under the
outer cursor. It must not synthesize a replacement manifest.

### 2. The five-method adapter surface omits direct job acquisition

The current draft has open, expansion completion, verifier completion,
failure, and seal, but no cursor-local operation that transitions a direct
job from `declared` or `retryable_failed` to `running`. The existing
public `acquire_job` / `start_attempt_point` route owns its transaction and
is now forbidden for typed epochs.

The narrow amendment therefore also needs a cursor-local acquisition
operation with the exact state-changing inputs:

```text
stage_direct_acquire(
  cursor, epoch_id: int, expected_revision: int,
  job: LogicalJobSpec
) -> JobLease
```

It must preserve the v1 deterministic attempt ID, attempt ordinal, lease-token
hash, exact replay rules, point/CAS revision, and compact evaluation
transition. Like every direct adapter method, it must use the already-held
typed transaction and must not commit, roll back, open a nested transaction,
advance a publication head, or finalize the combined base state.

## Remaining composition work after amendment

Once those two contract gaps are resolved, the coordinator-owned
implementation still has to:

1. extract cursor-local M4 declaration, acquisition, completion, failure, and
   seal helpers without changing any v1 DTO, digest, SQL invariant, or
   `v1_only` behavior;
2. preserve the D21 statement order: runtime-mode/publication locks, base
   epoch, M5 update, revision-1 typed header, M4 update, then both subgraphs;
3. maintain M4 compact evaluation and working observation currency inside
   the typed transaction;
4. reconcile the shared base semantic/evaluation state from direct readiness
   plus all typed counters after every mutation; and
5. promote direct and typed state, write the combined result, and advance
   both heads atomically at seal.

This checkpoint proves only the public routing barrier and legacy
compatibility. It is not a direct-adapter implementation and does not make
M5.4 PASS.

## Evidence

Executed against the final narrowed diff:

```text
GROUNDLOOP_TEST_DATABASE_URL=... python -m pytest -o addopts='' -q \
  tests/m5/postgres_runtime/test_m4_typed_barrier.py
  -> 2 passed

GROUNDLOOP_TEST_DATABASE_URL=... python -m pytest -o addopts='' -q \
  tests/m5/postgres_runtime/test_migration_015.py \
  tests/m5/postgres_runtime/test_m4_typed_barrier.py
  -> 27 passed

GROUNDLOOP_TEST_DATABASE_URL=... python -m pytest -o addopts='' -q \
  tests/m4/persistence/test_runtime_store.py \
  tests/m4/point_runtime/test_point_runtime.py \
  tests/m4/complexity_contract/test_measured_kernel_contract.py
  -> 22 passed

GROUNDLOOP_TEST_DATABASE_URL=... python -m pytest -o addopts='' -q \
  tests/m4/integration/test_postgres_pipeline.py
  -> 29 passed

python -m pytest -o addopts='' -q \
  tests/m4/test_m4_contracts.py tests/m5/runtime/test_contracts.py \
  -k 'compatibility or golden or identity or completion or candidate_policy'
  -> 7 passed, 22 deselected

python -m ruff check src/groundloop/m4/persistence.py \
  tests/m5/postgres_runtime/test_m4_typed_barrier.py
  -> PASS

python -m ruff format --check \
  tests/m5/postgres_runtime/test_m4_typed_barrier.py
  -> PASS

python -m mypy --strict src/groundloop/m4/persistence.py
  -> PASS

python -m compileall -q src tests
  -> PASS

git diff --check
  -> PASS
```

The current Ruff formatter would mechanically rewrite unrelated pre-existing
lines in `m4/persistence.py`; the same drift is present at this branch's base.
This lane retains the base formatting to keep the integration diff limited to
the barrier. Ruff lint passes for both changed Python files.
