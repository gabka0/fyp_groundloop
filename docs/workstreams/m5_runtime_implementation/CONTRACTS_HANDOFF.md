# M5 Runtime Contracts and Pure Frontier Handoff

Status: implementation complete in Lane R1; ready for coordinator inspection

Date: 2026-08-06

Branch: `workstream/m5-runtime-contracts-v2`

Lane base: `d3dcc8e` (`Authorize M5 completion lanes`), whose integration base
is `d0f4bdc`.

The coordinator subsequently recorded M5-D21 on main at `40579a0`. The
coordinator confirmed that decision does not change any R1-owned contract,
digest, frontier, test, or handoff path. This lane therefore did not merge or
rebase main and preserves its authorized base.

## Delivered scope

The lane edits only the seven R1-owned paths in
`docs/workstreams/m5_completion/EXECUTION_PLAN_2026-08-06.md`:

- `src/groundloop/m5/runtime/contracts.py`
- `src/groundloop/m5/runtime/digests.py`
- `src/groundloop/m5/runtime/frontier.py`
- `tests/m5/runtime/test_contracts.py`
- `tests/m5/runtime/test_digests.py`
- `tests/m5/runtime/test_frontier.py`
- this handoff

It does not create the coordinator-owned runtime `__init__.py`, export public
types, add persistence or model adapters, edit migration 015, alter M4, or
touch main/user presentation work.

The implementation supplies:

- every exact enum in addendum Section 4;
- the frozen M5 normalizer singleton and required
  `d91b94f256f79c6bc7b29fafa41c7a608b90c17bd479b29a64be00fa538c49fb`
  provenance hash;
- byte-total candidate-policy, snapshot, semantic-pair, scope, job,
  completion, discovery, verifier, attempt, activation, changed-state, work,
  receipt/result, withdrawal, root/barrier, and runtime-schema-bundle recipes;
- immutable DTOs and builders with kind/nullability, ordering, membership,
  finite-F64, score-sum, activity, replay, and identity validation;
- vector-first `rank-interleave-v1`, lineage-only append, explicit successful
  exhaustion, event-wide pair deduplication, least-root ownership,
  closure/child bijection, root/barrier planning, exact reverse-edge fallback
  coalescing, activity precedence, and latest-forward-frontier rules; and
- literal golden vectors, one-field mutation falsifiers, every frozen
  whitespace code point, nullable branches, signed integers, negative zero,
  nonfinite rejection, legal/illegal job and completion shapes, exhaustive
  activity truth combinations, permutation-stable dedup/barrier plans,
  no-hidden-reserve behavior, empty/short success, and M4 identity regression.

## Stable coordinator-facing names

The principal immutable envelope/result records are:

```text
M5TypedEventPlan(
  structural_event_id, event, payload_hash, direct_plan,
  candidate_policy_id, candidate_policy_manifest_hash,
  requirement_registry_snapshot, active_chunk_snapshot,
  expected_previous_published_epoch_id
)

M5EventRunResult(
  event_id, payload_hash, epoch_id, state, replayed_outcome,
  open_receipt, publication_receipt, event_work, call_work,
  event_timing, call_timing, combined_deltas,
  changed_state_references, failure_reason, logical_result_hash
)
```

`M5EventRunResult.build(...)` constructs the canonical logical result hash;
the direct constructor validates a hydrated durable hash.

The public activation and transition helper records are:

```text
M5ActivationRequest
M5ActivationReceipt
M5JobLease(logical_job_id, attempt, resulting_revision,
           should_execute, exact_replay)
M5AttemptCompletionReceipt(logical_job_id, attempt_id,
                           resulting_revision, exact_replay)
M5RootBarrierReceipt(requirement_root_set_hash, barrier_completion_hash,
                     resulting_revision, exact_replay)
M5CancellationReceipt(cancelled_job_ids, resulting_revision, exact_replay)
```

`M5ActivationPort` and `M5TypedApplication` expose the exact Section 17
activation and application call shapes. `M5RuntimeWork` contains the exact 32
counters in addendum order plus `work_digest`; `M5RuntimeTiming` contains the
exact nine timing/physical fields. `M5OwnerPendingCounter` exposes the four
independent owner counts and their exact summed multiplicity.

The scope DTO is named `M5DiscoveryScopeContract`; the compatibility alias
`M5RequirementScopeContract` names the same class. Snapshot DTOs are
`RequirementRegistrySnapshot` / `RequirementRegistrySnapshotEntry` and
`ActiveChunkSnapshot` / `ActiveChunkSnapshotEntry`.

## Required persistence-side calls

Several frozen invariants necessarily depend on separately stored rows and
cannot be inferred from one immutable payload alone. Persistence/application
composition must call the provided validators at its checked boundary:

- `M5DiscoveryScopeContract.validate_snapshots(...)` and
  `SemanticPairKey.validate_snapshots(...)` for snapshot membership;
- `M5DiscoveryScopeContract.validate_pair(...)` for direction-specific pair
  restriction;
- `M5LogicalJobSpec.validate_manifest_and_scope(...)` for manifest, role,
  execution, direction, and scope binding (the normal builder already calls
  it);
- `M5RequirementDiscoveryResult.validate_policy(...)` with the frozen budget,
  lineage flag, and explicit snapshot-exhaustion evidence;
- `M5RequirementPairInput.validate_bound_rows(...)` against immutable
  requirement and chunk members;
- `M5RequirementVerifierArtifact.build_checked(...)` or
  `.validate_decision_policy(...)`; the stored verifier DTO intentionally
  contains only the frozen policy version/hash, not policy thresholds, so an
  unchecked model label must never be admitted through direct hydration;
- `M5JobCompletion.validate_job(...)` and
  `M5AttemptResultArtifact.validate_job_shape(...)`; and
- full replay comparison against the stored event-result row in addition to
  `M5EventRunResult`'s self-contained receipt/logical-hash validation.

The runtime persistence protocol itself remains coordinator-owned because its
transactions, cancellation-plan storage, and direct-M4 cursor adapter are not
pure R1 concerns. No persistence-facing contract ambiguity remains in the
named receipt fields above.

## Validation evidence

Executed from the lane worktree:

```text
python3 -m pytest \
  tests/m5/runtime/test_digests.py \
  tests/m5/runtime/test_contracts.py \
  tests/m5/runtime/test_frontier.py
  -> 62 passed

python3 -m pytest \
  tests/m5/runtime/test_digests.py \
  tests/m5/runtime/test_contracts.py \
  tests/m5/runtime/test_frontier.py \
  tests/m5/reference/test_legacy_regression.py \
  tests/m4/test_m4_contracts.py
  -> 75 passed

python3 -m ruff check <all six R1 source/test Python files>
  -> All checks passed

python3 -m mypy --strict <three R1 source files>
  -> Success: no issues found in 3 source files

MYPYPATH=src python3 -m mypy --strict --explicit-package-bases \
  <three R1 test files>
  -> Success: no issues found in 3 source files

python3 -m compileall -q src tests
  -> passed

git diff --check
  -> passed
```

A whole-package mypy attempt is not evidence in this environment: the host
Python lacks the declared `psycopg` dependency, producing unrelated import
failures in pre-existing PostgreSQL/M4 modules. The owned strict-mypy surfaces
are green.

## Claim boundary

This handoff proves pure construction, validation, byte identity, and
deterministic frontier planning only. It is not persistence, transaction,
race, crash/reconnect, live PostgreSQL, model-quality, or end-to-end M5.4
evidence. Those gates remain with R2/coordinator and remain PENDING until their
own executable evidence is recorded.
