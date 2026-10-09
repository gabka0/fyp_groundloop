# Task 17 / D31 Pending-Timing Prerequisite Repair Activation

Status: docs-only candidate. Implementation is forbidden until these exact
bytes receive two independent `GO`, `P0=0`, `P1=0` audits, one exact commit,
two independent postcommit identity checks, and atomic branch/main push.

Date: 2026-10-09

## 1. Authority and exact parent

The user approved the narrow additional path activation and repair after the
Task-17 stop. This approval covers this prerequisite and resumed C1 only;
it does not activate public composition, deployment or AI-model changes.

```text
activation_branch = workstream/m5-d31-pending-anchor-activation
activation_worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d31-pending-anchor-activation
required_parent = 8510470176f0396472d2f125fb8bd010dc3a61c4
required_parent_tree = 610a8680945c2822238527fb4070fcad760ecbdb
required_origin_main = 8510470176f0396472d2f125fb8bd010dc3a61c4
sole_owned_activation_path = docs/workstreams/m5_runtime_implementation/D31_PENDING_TIMING_REPAIR_ACTIVATION.md
runtime_mode = v1_only
```

Authority remains AGENTS.md, the frozen M5 design, runtime addendum revision
12, D24--D31, the acceptance matrix and the accepted D31 implementation
activation. This document adds path authority and a prerequisite gate only.
It changes no frozen semantic contract or previous accepted test receipt.

## 2. Confirmed contradiction and exact permitted correction

The accepted `_load_preterminal_seal_envelope` rejects any present pending
timing coordinate. D24 Section 9.2 permits a coherent prior pending anchor
after an unreported nonterminal invocation and requires terminal seal to
classify it missing before freezing coverage. Both preliminary independent
Task-17 audits confirmed this P1. The current C1 diagnostic proves a genuine
structural open produces such a persisted anchor; it does not prove a seal.

Clearing the accumulator early is not an alternative: migration 016's
deferred per-row validator compares each original NEW revision/terminal flag
against the final runtime cutoff. An intermediate `(N,false)` update does
not validate against the final `(N+1,sealed)` runtime. The existing fused
terminal finalizer must therefore retain its one final accumulator CAS.

The repair replaces only the blanket pending-is-NULL predicate with exact
read-only validation. Accept either all four pending coordinates absent, or
one coherent prior anchor satisfying all of the following:

1. All four coordinates are present, with the exact supported nonterminal
   contribution kind, nonempty source ID, strict positive integer revision
   not greater than the expected preterminal runtime revision, and valid
   SHA-256 key.
2. The key equals the unchanged typed contribution-key recipe for the exact
   epoch, kind and source ID; no caller-authored identity substitutes for it.
3. A point read proves the exact immutable work contribution at that epoch,
   kind, source ID, key and applied revision. Unsupported terminal anchor
   kinds, missing or conflicting rows and coordinate drift fail closed.
4. A point read proves no immutable transition-call timing row already exists
   for that complete anchor identity. Partial, stale, already-reported or
   cross-event anchors fail before child derivation.
5. All reads remain bound to the single captured schema. Existing D31
   attestation, independent half-terminal checks, promotion journal,
   terminal child derivation and result-bound comparison remain unchanged.

Do not resolve, clear, report, terminalize, mutate or force constraints in
this reader. Do not scan contribution/timing histories, add privileges or
transactions, read private promotion tables, change digest recipes, weaken
validators, introduce caller-authoritative hashes, or change public APIs.
The unchanged C1 finalizer remains responsible for inserting the prior
missing timing point and terminalizing once after contribution/runtime order.

## 3. Strict sequential path ownership

```text
this docs-only activation -> R-T prerequisite repair -> resumed C1
repair_branch = workstream/m5-d31-pending-timing-repair
repair_worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d31-pending-timing-repair
repair_base = exact pushed commit of this activation
```

R-T owns exactly four paths:

1. `src/groundloop/m5/runtime/postgres_matching_publication.py`;
2. `tests/m5/postgres_runtime/d25_publication/test_changed_state_references.py`;
3. `tests/m5/postgres_runtime/d25_publication/test_seal_promotion.py`; and
4. `docs/workstreams/m5_runtime_implementation/D31_PENDING_TIMING_REPAIR_HANDOFF.md`
   (new).

No source implementation runs in parallel. C1's nine held paths are read-only
until R-T is accepted and pushed. All other paths, including migrations,
installers, recovery finalizers, matching planner, public facades, DTOs and
frozen contracts, are read-only. Discovery of a fifth necessary repair path
requires a new explicit manifest amendment; no silent ownership expansion.

After acceptance, C1 may fast-forward to the exact pushed repair commit only
after rechecking the ledger below, proving ancestry and proving every upstream
changed path disjoint from all nine held C1 paths. No reset, stash, clean,
checkout replacement, rebase, cherry-pick, copy or patch transplant is allowed.
Its existing D31 Section-6 ownership and final gates then govern completion.

## 4. Required repair evidence

On final identical candidate bytes, prove:

1. Deterministic all-absent and coherent-pending positives; all partial-null
   combinations, unsupported kinds, malformed/key mismatch, source/revision
   drift, absent/conflicting contribution, existing timing and schema drift
   fail closed before child derivation. Reads are point-keyed, read-only and
   qualified to the captured schema; accessor capture/invocation remain once.
2. Genuine PostgreSQL owner and separately connected distinct non-owner
   positives with exact migrations 018/019 and intact privileges/validators.
   Obtain the pending anchor through real nonterminal persistence, not an
   authored accumulator UPDATE or fabricated positive anchor/hash.
3. REPLACE and RETIRE: pending anchor survives preterminal preparation;
   seal contribution precedes runtime terminalization; one final timing CAS
   classifies the prior point missing and terminal seal missing; no pending
   anchor remains; immutable result/work/coverage and unchanged terminal
   builder children agree; deferred validation last and commit succeeds.
4. Exact persisted missing timing identity, coverage counts, no duplicate
   contribution/timing/result children on retained replay, rollback at an
   injected preterminal boundary, and terminal/preexisting-row rejection.
5. Both owned modules, retained D25 publication/store, D29/D30 and migration
   019 compatibility; compile, owned format/lint, source typing, diff and
   wheel payload checks. Report exact failures/skips and separate diagnostics;
   do not relabel whole-repository baseline failures as passing gates.

No positive proof may disable triggers, rewrite derived published state,
author child hashes, monkeypatch validators, swallow errors, skip or xfail
the new regression. Existing rollback-contained corruption negatives remain
negative-only evidence. Any fixture adapter must preserve genuine persisted
state and exact transaction ordering, not manufacture pending correctness.

Freeze an external exact four-path SHA-256 manifest. Both independent audits
must review those identical bytes and return `GO`, `P0=0`, `P1=0` before an
exact four-path commit with the sole repair base parent. Two independent
postcommit checks verify parent/tree/path/mode/blob/hash and unchanged protected
work. Push the exact repair commit atomically to its branch and origin/main.
No acceptance result follows from preliminary or different-byte reviews.

## 5. Current held C1 custody, replacing only its restart snapshot

```text
c1_worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d30-structural-composition
c1_branch = workstream/m5-d30-structural-composition
c1_head = 8510470176f0396472d2f125fb8bd010dc3a61c4
c1_index = empty
c1_raw_porcelain_v2_z_sha256 = 736f23f62b8ae744763219ab592266318662636dec4bcbc8a7be93e56ed0d58d
c2cd7387c0fe8287825e3252afdd7b010f921eb61c548fd2dba290f585d8b298  src/groundloop/m5/runtime/persistence.py
532df5887c20f8d40291b45a5d359d32752803b9784e5e160351aeddd83e8032  src/groundloop/m5/runtime/postgres_recovery.py
3d101bd3e5b49fd4acfc68687321f4a93a65c73385453393a3b02e75b04b36c1  tests/m5/postgres_runtime/d30_application/conftest.py
958674002910b546132cba94048b4dbb172f08735b0e6662be0ebdfbe1586935  tests/m5/postgres_runtime/d30_application/test_seal_atomicity.py
ac940cb67050ae2a4faff972e765113888b53fef5514425f1e4e4a8e6f6930e5  tests/m5/postgres_runtime/d30_application/test_store_composition.py
f9e609ad7253be10d2bd71c0421d830a855119be62853089e90ed4b4db420154  tests/m5/postgres_runtime/d30_application/test_store_races.py
abaee8d515c8e167ec954db32812315553db2912ca5ac6f5ee8c07d5abba79b5  tests/m5/postgres_runtime/d30_application/test_structural_open.py
9c135c6250d1b572a611d2c57f84db1646b8e742a4642601728cda003fc0b30c  tests/m5/postgres_runtime/d30_application/test_structural_order.py
33af1fe019898e5be8847892ec9d9e599aeadf22bb086937f7113b23e07cfefd  docs/workstreams/m5_runtime_implementation/D30_STRUCTURAL_COMPOSITION_HANDOFF.md

protected_primary_head = 14598ae51562006eaf67850b19e8212f38997903
protected_primary_index = empty
protected_primary_raw_status_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
protected_ai_study_head = a1dab3e5ebfe3bb65c521acd84758297a589f11a
protected_ai_study_index = empty
protected_ai_study_raw_status_sha256 = 279b7ebf78bc4b19c71be49031ecc1a3415d1033864869344af98f07e6a438a7
```

Recheck these live before commit and advance. Raw status hashes describe exact
`git status --porcelain=v2 -uall -z` bytes, not file-content identity; both the
raw status and all nine file hashes must agree. Any mismatch stops advancement.

## 6. Claim ceiling and next stop

This activation is not implementation evidence. Task 17/C1, public composition,
Task 2, M5-D24--D31 implementation, M5.4--M5.6, deployment and AI-quality remain
PENDING. Defaults stay `v1_only`. No performance, cost, security, scalability,
utility, novelty, objective-truth or named-system superiority claim follows.
After C1 acceptance, stop and reassess the smallest public vertical slice;
no later lane is activated by this user's prerequisite approval.
