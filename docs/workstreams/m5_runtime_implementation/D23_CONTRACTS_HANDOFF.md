# M5-D23 Pure Transition Contracts Handoff

Status: implementation complete in Lane R5; ready for coordinator inspection

Date: 2026-08-06

Branch: `workstream/m5-d23-contracts`

Lane base: `993fda07cc1b91643743c5e42969dd1aa8455d82`

Candidate head: the commit containing this handoff; resolve with
`git rev-parse HEAD` before integration.

## Delivered scope

This lane edits only the five R5-owned paths in
`docs/workstreams/m5_completion/EXECUTION_PLAN_2026-08-06.md`:

- `src/groundloop/m5/runtime/contracts.py`
- `src/groundloop/m5/runtime/digests.py`
- `tests/m5/runtime/test_contracts.py`
- `tests/m5/runtime/test_digests.py`
- this handoff

It does not edit persistence, migration 015, application orchestration,
package exports, live PostgreSQL tests, shared status/design documents, M4,
or user-owned paths.

The implementation adds the frozen immutable record:

```text
M5CancellationPlan(
  structural_event_id: str,
  epoch_id: int,
  cancelled_job_ids: tuple[SHA256, ...],
  reason: M5TerminalReason,
  plan_digest: SHA256
)
```

The direct constructor and `build(...)` enforce a nonempty structural event,
a positive non-boolean epoch, an immutable nonempty tuple of lowercase SHA-256
job IDs strictly increasing by UTF-8 byte order, and exactly one of
`subject_inactive`, `scope_retired`, or `epoch_failed`. The supplied digest is
recomputed and checked on construction.

`cancellation_plan_digest(...)` implements the exact frozen recipe:

```text
stable_m5_digest(
  "m5-cancellation-plan-v2",
  *TEXT(structural_event_id),
  *INT(epoch_id),
  *SEQ(HASH(cancelled_job_id) for cancelled_job_id in cancelled_job_ids),
  *ENUM(reason)
)
```

Independent framing and literal goldens cover all three allowed reasons. The
vectors reject empty, duplicate, reversed, mutable-container, malformed-hash,
invalid-epoch, raw-string-reason, disallowed-reason, and stale-digest shapes;
they also prove that each field and the exact job-ID order changes identity.

## Retry-error and receipt boundary

M5-D23 reuses the existing
`M5AttemptCompletionReceipt(logical_job_id, attempt_id,
resulting_revision, exact_replay)` for retryable failure and the existing
`M5CancellationReceipt(cancelled_job_ids, resulting_revision, exact_replay)`
for cancellation. R5 adds exact topology, fresh/replay, and invalid-shape
vectors for those records without changing either DTO.

No pure error DTO or error digest was added. The caller-supplied `error_hash`
is a persistence-side nullable lowercase SHA-256 sidecar. Per M5-D23 it enters
neither `attempt_id`, `attempt_output_digest`, nor any semantic-result
identity. Its durable storage, same-hash zero-write replay, differing-hash
conflict, and transaction revision behavior remain R6/persistence evidence.

## Golden vectors

For structural event `event-λ`, epoch 7, and ordered IDs `("1" * 64,
"2" * 64)`, the exact digests are:

```text
subject_inactive fc138b8971e2dcc1f540e96149404bf7ded8f595307a1bdc7f109508b289317a
scope_retired    e80e49a4e500ae84f44c35c066726572e84e87eb00c7cb3ec61754d41b78a8f6
epoch_failed     1110b91294ebd76349b14f0e3c2e48c72e8d8e546c02f4598655f09d3ff7db16
```

The `scope_retired` value is also rebuilt by an independent length-prefixed
framing helper rather than by the production digest helper.

## Validation evidence

Executed from the lane worktree:

```text
python3 -m pytest -q \
  tests/m5/runtime/test_contracts.py \
  tests/m5/runtime/test_digests.py
  -> 53 passed

python3 -m pytest -q tests/m5/runtime
  -> 89 passed

ruff check \
  src/groundloop/m5/runtime/contracts.py \
  src/groundloop/m5/runtime/digests.py \
  tests/m5/runtime/test_contracts.py \
  tests/m5/runtime/test_digests.py
  -> All checks passed

MYPYPATH=src mypy --strict \
  src/groundloop/m5/runtime/contracts.py \
  src/groundloop/m5/runtime/digests.py \
  tests/m5/runtime/test_contracts.py
  -> Success: no issues found in 3 source files

python3 -m compileall -q src tests/m5/runtime
  -> passed

git diff --check
  -> passed
```

Whole-file strict mypy on `tests/m5/runtime/test_digests.py` reports exactly
34 pre-existing errors in the earlier M5-D22 heterogeneous dictionary-splat
mutation vectors. The same 34-error baseline existed before R5 edits; current
errors are confined to those shifted D22 lines 503--595. The new D23 digest
tests use fully typed direct calls and introduce no additional mypy error.
Per coordinator direction, R5 did not rewrite unrelated D22 vectors merely to
make that whole legacy test file strict-mypy clean.

## Claim boundary

This handoff proves only pure DTO shape, validation, immutable digest identity,
and existing receipt compatibility for M5-D23. It is not schema, persistence,
replay/CAS, cancellation transaction, acquisition, direct-M4 bridge, live
PostgreSQL, application, evaluation, or gate-PASS evidence. Those surfaces
remain with their path owners and the coordinator until independently
executed and integrated.
