# M5-D26 Changed-State Absence Artifact Candidate

Status: contract candidate for independent same-byte adversarial review; not
frozen and not implementation authority

Date: 2026-09-06

Dependency: M5-D22 and M5-D25 remain accepted. The authoritative M5-D25
candidate SHA-256 is
`bac12ab5e74632c04f1bd70d0ef0d00522ba9d268eb8b73d11845bbf3b873aae`.
Migration 017 and M5-D25 implementation remain `PENDING`; the retained
`workstream/m5-d25-schema-017` branch is read-only investigation evidence and
is not authority for this amendment.

Authority boundary: this candidate proposes one narrow identity and
validation correction to the changed-state-reference contract. It changes no
M5 semantic state, lifecycle, present-state digest, certificate digest,
reference kind, schema column, M4-v1 byte, D24 accounting rule, D25 physical
matching rule, runtime mode, or empirical claim. It becomes authoritative only
after two independent reviews return `GO` on one exact candidate SHA-256 and a
separate coordinator authority-freeze tranche is accepted.

## 1. Confirmed contradiction

The accepted contracts jointly require all of the following:

1. M5-D25 encodes each logical state or artifact change as a before/after pair
   and uses an explicit `after=None` for removal. Its retained
   `m5-overlay-logical-output-v2` record likewise contains
   `(kind, object_id, None)`.
2. The changed-state set includes full-state and certificate-only changes even
   when no public status enum changes.
3. Migration 015 validates a changed-state reference only by finding a newly
   published state row or certificate binding for the same object at the
   reference epoch and revision.
4. A successful group `REPLACE` or `RETIRE` closes the predecessor state and
   applicable group-certificate binding deliberately without publishing a
   successor row for that same immutable predecessor object ID.

Consequently the required reference for a removed predecessor cannot satisfy
the migration-015 present-row validator. Reusing the predecessor digest would
misrepresent absence as presence. Omitting the reference would violate the
complete changed-state-set rule. Creating a tombstone semantic row, nullable
hash, synthetic certificate, new kind, or implementation-private hash would
change a wider frozen contract.

## 2. Proposed M5-D26 decision

### M5-D26 -- byte-total changed-state absence artifact

For a qualifying structural removal only, `state_artifact_hash` is the exact
absence artifact:

```text
stable_m5_digest(
  "m5-changed-state-absence-artifact-v1",
  *ENUM(kind),
  *TEXT(object_id)
)
```

The domain string is exact lowercase UTF-8. `ENUM` and `TEXT` use the frozen
M5-D1 typed expansion and unsigned eight-byte big-endian field framing. The
object ID is the existing nonempty immutable object ID without trimming,
normalization, case folding, JSON, `repr`, delimiter joining, implicit NULL,
or implementation-specific serialization.

The only qualifying `kind` values are the existing values:

```text
requirement_state
group_state
group_certificate
```

No other kind may use this domain. In particular `claim_state`,
`answer_state`, and `claim_certificate` always require their existing present
row or binding. The enum remains the existing six-value
`M5StateReferenceKind`; M5-D26 adds no seventh kind.

The outer reference remains byte-for-byte:

```text
stable_m5_digest(
  "m5-changed-state-reference-v2", *ENUM(kind), *TEXT(object_id),
  *INT(epoch_id), *INT(revision), *HASH(state_artifact_hash)
)
```

The existing `m5-changed-state-set-v2` construction, sort order
`(kind.value, object_id, reference_digest)`, and uniqueness by
`(kind, object_id)` also remain unchanged. The absence digest occupies the
existing non-null `state_artifact_hash`; it is not a nullable sentinel.

## 3. Closed qualifying transition

An absence artifact is valid if and only if every condition in this section
holds. Application convention or a matching digest alone is insufficient.

### 3.1 Sealed structural source

The reference belongs to one `groundloop_m5_event_result` whose outcome is
`sealed`. Let its event ID be `E`, epoch be `K`, and the exact sealed revision
be `S`.

Let `H` be the result's `payload_hash` and let `P` be its update's
`previous_published_epoch_id`. The complete identity must agree:

- the result has `structural_event_id=E`, `epoch_id=K`, `payload_hash=H`, and
  `outcome='sealed'`;
- `groundloop_epoch(K)` has `event_id=E`, `payload_hash=H`,
  `structural_status='committed'`, `semantic_status='sealed'`,
  `evaluation_state='complete'`, non-NULL `sealed_at`, and `revision=S`;
- `groundloop_m5_runtime_epoch(K)` has `structural_event_id=E`,
  `runtime_state='sealed'`, non-NULL `terminal_at`, `revision=S`, and
  `expected_previous_published_epoch_id=P`;
- `groundloop_m5_update(K)` exists, has
  `previous_published_epoch_id=P`, and has one of the two mappings below;
- both publication heads name `K` and the M5 head has
  `sealed_revision=S`; and
- the reference has `epoch_id=K` and `revision=S`.

The only event/update/deactivation mappings are:

| M5 update kind | Deactivation action | Successor field |
|---|---|---|
| `replace_group` | `REPLACE` | one non-NULL successor group version |
| `retire_group` | `RETIRE` | NULL |

There must be exactly one `groundloop_m5_group_deactivation` row for epoch
`K`. That row must have `event_id=E`, the mapped action, and the exact
predecessor group version. No second deactivation at `K`, even for another
group, is permitted by this branch.

The validator must independently derive the structural payload rather than
merely compare stored copies of `H`. For `RETIRE`, it requires exactly:

```text
H = stable_m5_digest(
  "m5-retire-group-event-v1", *TEXT(predecessor_group_version_id))
```

For `REPLACE`, let `J` be that same deactivation row's non-NULL
`successor_group_version_id`. The exact `groundloop_m5_group_version(J)` must
be the migration-014-valid successor: created by `K`, in the predecessor's
family, superseding that predecessor, `PUBLISHED` at seal, and paired with its
complete dense requirement set. Its stored `record_payload_hash=R` must equal
the independently recomputed frozen `m5-group-record-v1` digest over that
persisted successor and requirement set. The event payload then requires
exactly:

```text
H = stable_m5_digest(
  "m5-replace-group-event-v1", *TEXT(predecessor_group_version_id),
  *HASH(R))
```

The successor used for `R` and the payload digest must be exactly `J`; another
row, caller-supplied hash, or merely self-consistent stored `H` is invalid.
The recomputed payload must equal the result payload, base-epoch payload, and
D25 `structural_open` source identity hash already required above.
Migration-014's existing whole-group validator remains responsible for the
same-family successor and retirement facts, but its checks do not substitute
for this payload recomputation. A registration, document event, observation,
policy change, failed event, activation bootstrap, or any other action cannot
authorize an absence artifact.

### 3.2 Exact D25 logical change

The validator must locate the unique D25 `structural_open` contribution and
patch for `(epoch_id=K, source_id=E)`. It must prove that:

- the source identity hash equals the structural event payload hash;
- the patch before epoch equals
  `groundloop_m5_update(K).previous_published_epoch_id`;
- the structural patch resulting point is exactly `(K, 1)` under the accepted
  D25 structural-open rule;
- every retained child and outer preimage passes the exact D25 canonical
  decoder/re-encoder and digest checks; and
- the sorted logical state/artifact change set contains exactly one change for
  `(kind, object_id)` with a present `before` hash and `after=None`, while the
  decoded `m5-overlay-logical-output-v2` contains exactly the corresponding
  `(kind, object_id, None)` record.

The stored `before` hash must equal the independently recomputed predecessor
hash in Section 3.3. A missing, duplicated, malformed, noncanonical,
wrong-source, wrong-point, `before=None`, present-after, or mismatched logical
change rejects the reference and the seal transaction.

The mapping from reference to deactivated predecessor is exact:

- `requirement_state`: `object_id` is a published requirement version owned by
  the deactivated predecessor group;
- `group_state`: `object_id` is the deactivated predecessor group version; and
- `group_certificate`: `object_id` is the deactivated predecessor group
  version and the predecessor had an applicable published certificate
  binding.

For each qualifying D25 `before -> None` logical change, seal must emit exactly
one absence reference. Every absence reference must map back to exactly one
such logical change. This bijection preserves full-state and certificate-only
changes even when claim and answer status enums do not change.

### 3.3 Present predecessor and exact closure

The predecessor group must be `PUBLISHED`, have one group-validity interval
that covered `P`, and that interval must now close exactly at `K`.

For `requirement_state`, all of the following are required:

- the requirement version is `PUBLISHED`, belongs to that predecessor group,
  and was active through the parent group at `P`;
- exactly one published requirement-state interval for the object covered
  `P` and now has `valid_to_epoch=K`;
- its complete M5-D22 present-state artifact digest equals the D25 logical
  change's `before` hash; and
- no published requirement-state row for that same requirement starts at
  `K`, covers `K`, or remains open after the closure.

For `group_state`, all of the following are required:

- exactly one published group-state interval for the predecessor group
  covered `P` and now has `valid_to_epoch=K`;
- its complete M5-D22 present-state artifact digest equals the D25 logical
  change's `before` hash; and
- no published group-state row for that same immutable group version starts
  at `K`, covers `K`, or remains open after the closure.

For `group_certificate`, all of the following are required:

- exactly one published group-certificate binding for the predecessor group
  covered `P` and now has `valid_to_epoch=K`;
- the binding's immutable certificate digest equals the D25 logical change's
  `before` hash, and the referenced immutable certificate artifact and rows
  remain valid historical content; and
- no published group-certificate binding for that same immutable group
  version starts at `K`, covers `K`, or remains open after the closure.

The certificate artifact is not deleted and is not itself called absent.
Absence means that the retired predecessor group has no applicable successor
binding at the post-event snapshot.

A `REPLACE` may and normally does publish state and a later certificate for a
new group/requirement version with different immutable object IDs. Those
successor objects use the existing present-state/certificate recipes. Their
existence does not satisfy, replace, or invalidate the required absence
reference for the old object ID.

## 4. Reference hash and validation branch

For a qualifying absence reference, the validator recomputes:

```text
absence_hash = stable_m5_digest(
  "m5-changed-state-absence-artifact-v1",
  *ENUM(reference.kind),
  *TEXT(reference.object_id))
```

It requires `reference.state_artifact_hash=absence_hash`, then recomputes the
unchanged outer `m5-changed-state-reference-v2` digest and unchanged set hash.
The structural and historical validation in Section 3 is mandatory even when
the digest matches. An absence digest is not a general proof of nonexistence.

For all nonqualifying references, the present-state behavior is unchanged:

- the four M5-D22 semantic-row recipes remain exact;
- group and claim certificate references still use the immutable certificate
  digest directly;
- the reference must still match the named newly published row or binding at
  its event epoch/revision; and
- no present row may be hashed using the absence domain.

Activation bootstrap emits only present references and cannot use the absence
branch. A failed event cannot emit it. Exact replay reads and validates the
already stored sealed reference and performs zero writes.

## 5. Migration-017 replacement authority

Migration 015 file bytes, bundle/ledger identity, tables, columns, constraints,
and triggers remain immutable. Migration 017 receives exactly one additional
authority against a migration-015 object:

```text
CREATE OR REPLACE FUNCTION groundloop_m5_validate_event_result_children()
```

The replacement may change only that function body and only to add the
qualifying absence branch in Sections 3 and 4. It must preserve all existing
delta, ordering, subtype, work, present-reference, receipt, result, and digest
validation behavior. The three installed constraint triggers
`groundloop_m5_event_result_children`,
`groundloop_m5_event_delta_set`, and
`groundloop_m5_event_reference_set` remain installed under their existing
names and continue to call that function; migration 017 must not drop,
recreate, rename, disable, defer, or retarget them.

No other migration-014, migration-015, or migration-016 function, trigger,
constraint, table, column, enum, ledger row, or digest recipe may be replaced
or weakened under M5-D26. The replacement is part of the one top-level
migration-017 transaction, occurs before its ledger insert, and rolls back
with every other migration-017 change. Exact migration-017 rerun remains a
ledger-first no-op.

The function must derive the absence hash with the existing canonical M5
digest primitive and validate the accepted D25 retained preimages and
relations. It must not introduce a helper table, tombstone row, nullable hash,
ambient JSON/`repr` hash, caller-authored absence flag, or application-only
bypass.

## 6. Non-change boundary

M5-D26 does not change:

- the six existing state-reference kinds or their sort/uniqueness rules;
- any present requirement/group/claim/answer state-artifact recipe;
- either immutable group/claim certificate recipe;
- `m5-changed-state-reference-v2`, `m5-changed-state-set-v2`,
  `m5-event-run-logical-result-v2`, or activation request/receipt recipes;
- D25 logical-output bytes, patch preimages, physical points, matching state,
  37-counter work, contribution identity, or seal promotion;
- migration-014 lifecycle, state-interval, certificate, or publication
  semantics;
- D24 dispatch, work, timing, replay, or ambiguity accounting;
- any M4-v1 identity or never-activated behavior; or
- the distinction between stored model judgments and objective truth.

It introduces no tombstone table, no synthetic semantic row, no new
certificate artifact, no nullable `state_artifact_hash`, no seventh reference
kind, and no change to runtime mode.

## 7. Mandatory falsifiers

Implementation evidence is rejected unless every applicable case below
passes with no skip or silent deselection:

1. Python and PostgreSQL produce identical golden hashes for all three legal
   kinds, non-ASCII object IDs, prefix/boundary adversaries, and one-field
   kind/object/domain mutations.
2. The exact same object ID under each legal kind produces a distinct absence
   digest; all three excluded kinds reject even when supplied a correctly
   computed absence-domain hash.
3. A sealed `REPLACE` and a sealed `RETIRE` each emit and validate the exact
   requirement-state absence reference for every present requirement of the
   deactivated predecessor.
4. A sealed `REPLACE` and `RETIRE` each emit and validate the predecessor
   group-state absence reference while any new version uses the unchanged
   present recipe.
5. A complete predecessor group under `REPLACE` and `RETIRE` closes its
   binding, retains the immutable artifact, and emits the exact
   group-certificate absence reference.
6. An incomplete predecessor with no certificate binding emits no
   group-certificate absence reference; fabricating one is rejected.
7. A missing, wrong, duplicated, malformed, noncanonical, `before=None`, or
   present-after D25 logical change rejects the reference.
8. A predecessor state or certificate digest that differs from the logical
   change's before hash rejects, including a self-consistent recomputation of
   the wrong absence reference.
9. A predecessor that did not cover the exact previous published head, is not
   published, belongs to another group, or lacks the required group-validity
   closure rejects.
10. A state interval or binding left open, closed at another epoch, reopened,
    or accompanied by a same-object successor row/binding rejects.
11. Wrong update kind, deactivation action, event ID, epoch ID, predecessor
    group, requirement owner, REPLACE successor/nullability/record payload,
    RETIRE successor nullability, arbitrary or wrong structural payload, or a
    second deactivation row at the epoch rejects. One-field mutations of the
    predecessor ID, successor ID, successor `record_payload_hash`, and
    recomputed event payload are mandatory negatives.
12. A failed/nonsealed event, activation, document event, policy change,
    registration, observation, or audit-only result cannot use absence.
13. A reference whose epoch/revision differs from the exact sealed epoch,
    durable epoch revision, or M5-head sealed revision rejects; a one-head-only
    promotion cannot pass.
14. Omitting one qualifying absence reference, adding an extra one, changing
    its order, duplicating it, or changing its digest makes the changed-state
    set or logical-result validation fail.
15. All six present-reference kinds retain their accepted positive and
    one-field negative vectors; no present recipe or direct certificate digest
    changes.
16. Static object inventory proves migration 017 replaced only
    `groundloop_m5_validate_event_result_children()` among pre-017 enforcement
    objects, kept all three constraint-trigger identities, and changed no
    migration-015 bytes or ledger field.
17. Migration-017 fresh install, exact rerun, content conflict, and every
    injected rollback leave the replacement function and the rest of the
    schema in one atomic before-or-after image.
18. Exact sealed reconnect/replay returns the stored absence references and
    logical-result hash with zero mutation, model call, or reference
    regeneration.

## 8. Claim and release boundary

If accepted and later implemented, M5-D26 would establish a byte-total,
structurally validated changed-state reference for the intentional absence of
retired predecessor requirement/group state and applicable group-certificate
bindings. It would not establish D25 implementation, M5.4 completion,
deployment, runtime activation, model accuracy, objective truth, utility,
latency, security, novelty, or superiority over another system.

M5-D25, M5.0-25, M5-D26, and the associated migration/runtime evidence remain
implementation-`PENDING`. M5.4-05 through M5.4-09, M5.5, and M5.6 remain
`PENDING`. Runtime remains `v1_only` outside isolated fixtures.

## 9. Candidate verdict

**Decision status: candidate awaiting two independent same-byte adversarial
reviews; not frozen and not implementation authority.**

Required reviews are:

1. semantic/digest/lifecycle/replay completeness; and
2. PostgreSQL validator/migration/interval/binding enforcement.

Both must read one exact candidate commit and one exact candidate SHA-256 and
return `GO` with `P0=0` and `P1=0`. Any candidate byte change invalidates both
verdicts. Only after both GOs may a separate authority-freeze tranche update
the design freeze, runtime addendum, acceptance matrix, decision log, plans,
status, roadmap, agent routing, and a new path-exclusive implementation plan.
