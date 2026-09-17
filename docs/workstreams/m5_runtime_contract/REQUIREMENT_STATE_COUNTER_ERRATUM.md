# M5-D27 Requirement-State Counter-Ownership Erratum

Status: **candidate wording-only correction; not implementation authority until
two independent same-byte audits return `GO`, `P0=0`, and `P1=0`.**

Date: 2026-09-17

Scope: one contradiction between M5-D24 Section 7.4 and M5-D25 Section 12.
This document changes no runtime type, digest, schema, migration, lock order,
transition, or measured value.

## 1. Authority and discovered contradiction

M5-D24 freezes the complete `M5RuntimeWork` counter-owner matrix. Its state
write row names only:

```text
group/claim/answer state and certificate-binding writes
```

The frozen `M5RuntimeWork` value, its `m5-runtime-work-v2` digest, migration
015, and migration 016 therefore contain these persistence-owned coordinates:

```text
group_state_write_count
claim_state_write_count
answer_state_write_count
certificate_binding_write_count
public_delta_count
```

They contain no `requirement_state_write_count` coordinate.

M5-D25 Section 12 later says that corrected M5-D24 remains sole owner of
actual `requirement/group/claim/answer/certificate/public-delta` SQL write
counts in `M5RuntimeWork`. The word `requirement` has no corresponding frozen
runtime field, digest position, database column, contribution coordinate, or
accumulator coordinate. A persisted-matching transition that physically
writes a requirement-state row cannot obey that sentence without either
omitting a supposedly mandatory counter or silently charging that row write
to an unrelated coordinate.

M5-D24 is the earlier owner of `M5RuntimeWork`, and its exact type, digest, and
migration bytes are already frozen. This erratum resolves only that conflict.

## 2. Corrected counter-ownership rule

The M5-D24 Section-7.4 matrix remains exact and exhaustive.

In M5-D25 Section 12, the conflicting bullet is read as:

> actual group-state, claim-state, answer-state, certificate-binding, and
> public-delta SQL write counts represented in `M5RuntimeWork`;

It is not read as assigning a requirement-state row-write coordinate to
M5-D24.

There is deliberately no persisted `requirement_state_write_count` in the
current M5 runtime-work vector. A requirement-state row write must not be:

- charged to `group_state_write_count` or another D24 counter;
- inferred from `bytes_hashed`, `bytes_serialized`, a model counter, or a
  timing/physical PostgreSQL measurement;
- added as an extra field to an existing runtime DTO, digest, contribution,
  accumulator, result, or report row; or
- reconstructed during replay and treated as if it had been a frozen D24
  coordinate.

This is an explicit measurement limitation, not a zero-write assertion.
GroundLoop may physically insert or update requirement-state rows while the
D24 runtime-work vector has no dedicated row-count coordinate for them.

## 3. D25 counters do not alias the missing D24 coordinate

M5-D25's 37-counter `M5OverlayWork` remains byte-for-byte unchanged.

In particular:

- `group_local_state_operations` retains its existing logical matching-kernel
  meaning and is not renamed or reinterpreted as a requirement-state SQL row
  count;
- `requirement_state_only_changes` retains its existing logical-patch meaning
  and is not a physical write count; and
- `output_bytes` remains the size of the canonical D25 logical-output image,
  not a database row-count surrogate.

The D25 patch must still include every exact requirement-state before/after
change, its canonical artifact hash and logical-output record. Migration 017's
transition bijection must still reject a requirement-state mutation without
the one exact D25 patch and contribution. This preserves correctness and
replay evidence even though the separate D24 physical row-count vector has no
requirement-state coordinate.

An implementation may compute a transaction-local requirement-state write
count for assertions, failure injection, or test diagnostics. That value is
not persisted, hashed, added to event work, exposed as a frozen metric, or
used as an authority input.

## 4. Non-change boundary

M5-D27 introduces no change to:

- `M5RuntimeWork`, `M5OverlayWork`, or any public/private frozen DTO;
- `m5-runtime-work-v2`, `m5-matching-work-v1`, event-result, patch,
  contribution, accumulator, or timing digests;
- migrations 014, 015, 016, or 017, including their ledgers and columns;
- the 37 D25 counters or their order;
- the D24 contribution kinds, timing anchors, ambiguity accounting, replay,
  terminalization, or counter-owner rows;
- the D25 source kinds, lock order, present/absence recipes, transition
  bijection, replay, promotion, or audit;
- the six changed-state-reference kinds or M5-D26;
- M4-v1 behavior, runtime mode, provider/model behavior, or deployment; or
- model accuracy, utility, performance, objective truth, novelty, security,
  or superiority claims.

Adding the missing counter in a future version would require a separately
versioned DTO/digest/schema/migration decision and compatibility plan. M5-D27
does not authorize that expansion.

## 5. Task-2 implementation consequence

Before this erratum is frozen, the first planned D25 transition that physically
writes requirement state remains a hard stop. Group registration is the first
such transition in the current Task-2 sequence; an empty/no-change structural
patch with its mandatory `output_bytes=71` is not blocked by this erratum.

After this erratum is frozen, the existing Task-2 path grants may implement
transitions that physically write requirement state without inventing a D24
requirement-state counter. The transaction must still:

1. derive and validate every requirement-state row from persisted authority;
2. bind every change into the exact D25 logical patch and output;
3. count the existing D24 group-state, claim-state, answer-state,
   certificate-binding, and public-delta writes exactly once at the
   transaction that physically writes them;
4. persist the independently derived 37-counter D25 contribution exactly
   once; and
5. report the absence of a dedicated requirement-state physical row-count
   coordinate wherever work results are interpreted.

No current implementation lane may edit a contract, migration, DTO, digest,
or schema to work around this erratum. A package-private prepared transition
may carry a non-authoritative requirement-state write count solely to compare
planned and actual writes inside one transaction.

## 6. Mandatory falsifiers

Implementation or authority evidence is rejected unless all applicable cases
below pass with no skip or silent deselection:

1. Static inventory proves that `M5RuntimeWork`, its digest encoding, migration
   015, and migration 016 contain no requirement-state write coordinate and
   still contain the five exact state/certificate/delta coordinates listed in
   Section 1.
2. A group registration that writes one or more requirement-state rows leaves
   every unrelated D24 write counter unchanged except for actual group-state,
   claim-state, answer-state, certificate-binding, and public-delta writes
   performed by that transaction. Immutable certificate-artifact insertion is
   not a certificate-binding write.
3. Requirement-state writes are still present in the canonical D25 logical
   patch/output, guarded by the transition bijection, and replay exactly.
4. Mutating `group_state_write_count`, `group_local_state_operations`, or
   `requirement_state_only_changes` as a surrogate for the missing coordinate,
   while recomputing every dependent digest consistently, still fails the
   independently derived expected-work and counter-owner validation.
5. Existing D24 runtime-work and D25 matching-work golden vectors remain
   byte-for-byte unchanged.
6. Existing migration-016 and migration-017 fresh-install, rerun, conflict,
   rollback, and ledger checks remain unchanged and pass.
7. Event-result replay returns the stored D24 and D25 work images without
   reconstructing a requirement-state row count.
8. Reports and handoffs do not claim that D24 measures a requirement-state
   physical row count.

## 7. Acceptance and claim ceiling

This candidate becomes authoritative only after two independent reviewers
audit identical bytes and each returns `GO`, `P0=0`, and `P1=0`. Any byte
change restarts both audits. Only the exact reviewed bytes may then be added
to an authority-freeze/status tranche and used to resume the
requirement-state-writing Task-2 paths.

Acceptance would resolve one counter-ownership contradiction. It would not
establish D24, D25, D26, Task 2, M5.4, M5.5, M5.6, deployment, performance, or
AI-quality completion. Runtime remains `v1_only` outside isolated fixtures.
