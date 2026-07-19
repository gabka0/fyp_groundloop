# Contract request: persist logical job targets

Status: blocking Wave 1 runtime implementation

Baseline: `m4-contract-baseline-2026-07-19`

## Defect

`LogicalJobSpec` cannot persist the target used to derive a non-verifier job's
identity.

The shared static method accepts `claim_id` and `chunk_version_id`, but the
dataclass stores neither field (`src/groundloop/m4/contracts.py`,
`LogicalJobSpec`). Its validation permits `pair` only for `VERIFY_PAIR`.

This makes two frozen semantics unimplementable without an unversioned side
map:

1. M4-8 says an open required job explicitly targeting claim `c` makes `c`
   PENDING. `FRONTIER_RETRIEVE` is claim-scoped, but its `LogicalJobSpec`
   cannot identify that claim.
2. Admission creates impact-discovery work for inserted chunks. The derivation
   function can hash a chunk ID, but the resulting spec cannot later prove
   which chunk was hashed or validate that its VERIFY_PAIR children reference
   the correct inserted chunk.

The runtime must not invent a parallel target DTO because shared contracts are
the frozen cross-lane identity surface.

## Minimal failing example

```python
job_id = LogicalJobSpec.derive_job_id(
    event_id="event",
    kind=JobKind.FRONTIER_RETRIEVE,
    candidate_policy_id="policy",
    execution_spec_hash=execution_hash,
    claim_id="claim-a",
)

job = LogicalJobSpec(
    job_id=job_id,
    event_id="event",
    kind=JobKind.FRONTIER_RETRIEVE,
    candidate_policy_id="policy",
    payload_hash=payload_hash,
    execution_spec_hash=execution_hash,
    expandable=True,
)

# There is no legal operation that can recover or validate "claim-a" from job.
```

Two specs derived for `claim-a` and `claim-b` differ only in opaque `job_id`.
The runtime cannot evaluate `ClaimPending`, validate a child pair against the
parent scope, or reproduce the ID from the persisted spec.

## Proposed shared signature

Add explicit optional target fields:

```python
@dataclass(frozen=True, slots=True)
class LogicalJobSpec:
    ...
    target_claim_id: str | None = None
    target_chunk_version_id: str | None = None
```

Freeze these shape rules:

```text
IMPACT_DISCOVERY:
  target_claim_id is None
  target_chunk_version_id is required
  pair is None

FRONTIER_RETRIEVE:
  target_claim_id is required
  target_chunk_version_id is None
  pair is None

VERIFY_PAIR:
  pair is required
  target_claim_id and target_chunk_version_id are None
  the derivation uses pair.claim_id and pair.chunk_version_id
```

`__post_init__` should recompute the expected ID using the stored targets (or
the pair for `VERIFY_PAIR`) and reject a mismatch. This prevents callers from
supplying a correct-looking payload with a job ID derived from different
targets.

If a single update legitimately needs a second job of the same kind for the
same target and execution specification, that is a different logical input
and must be represented explicitly in the frozen identity—not hidden in an
attempt ID.

## Compatibility impact

- Existing `VERIFY_PAIR` construction remains source-compatible because the
  new fields default to `None`.
- New impact-discovery/frontier specs must provide their frozen target.
- Admission and runtime lanes consume the same fields, eliminating a private
  cross-lane mapping.
- Persistence gains two nullable columns or equivalent normalized target
  fields; the coordinator already owns that migration.
- `derive_job_id` retains its current parameters and becomes verifiable from
  the constructed record.

## Acceptance tests

1. `IMPACT_DISCOVERY` without an inserted chunk target is rejected.
2. `FRONTIER_RETRIEVE` without a claim target is rejected.
3. `VERIFY_PAIR` continues to require a pair and rejects redundant target
   fields.
4. Specs for two claim targets derive distinct IDs and preserve the targets.
5. A spec whose stored target does not match the supplied `job_id` is rejected.
6. An impact-discovery child whose pair chunk differs from the parent's target
   is rejected by the runtime.
7. A frontier child whose pair claim differs from the parent's target is
   rejected by the runtime.
8. Claim-level PENDING is derivable from a `FRONTIER_RETRIEVE` spec without a
   side table.

## Requested resolution

Apply the shared contract correction, add coordinator-owned contract tests,
create a new contract-baseline commit/tag, and instruct all affected lanes to
rebase. The epoch/runtime lane will not implement a workaround against the
defective baseline.
