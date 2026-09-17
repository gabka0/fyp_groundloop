# M5-D25/D26 Store and Runtime Composition Activation

Status: docs-only Task 2 activation candidate; no source or test ownership is
granted until the exact activation commit and tree receive two independent
same-byte `GO`, `P0=0`, `P1=0` audits, are integrated to `main`, and are
pushed to `origin/main`

Date: 2026-09-17

## 1. Exact starting point and authority

```text
required_activation_parent = 781f667bc041ec771181d4ba8cca86b0bad5fa9b
required_activation_parent_tree = 0d894c29d731bbe9bc50cff43ecd6ba19931b3e5
accepted_d25_candidate_sha256 = bac12ab5e74632c04f1bd70d0ef0d00522ba9d268eb8b73d11845bbf3b873aae
accepted_d26_candidate_sha256 = 85372d4c2f9108810bd75c3e5611de541d0f31c8a096421f30e68fad84676721
integrated_migration_017_commit = 781f667bc041ec771181d4ba8cca86b0bad5fa9b
runtime_addendum_revision = 7
runtime_mode = v1_only
```

The required parent is the independently audited and pushed Wave R6
integration of the D25/D26 pure contracts and migration-017 database tranche.
Its accepted evidence does not include a public persisted-matching store,
application composition, production seal, reconnect without regeneration, or
the remaining end-to-end D24/D25/D26 falsifiers.

This file must be the only path changed by the activation commit, and that
commit must have the exact sole parent above. Because a commit cannot contain
its own identity, the two independent activation audits record the containing
commit and tree. No implementation branch or worktree may be created before
that exact audited activation commit is on `origin/main`.

Read and apply these authorities in order:

1. `AGENTS.md` and its complete required reading sequence;
2. `docs/m5_design_freeze.md`;
3. `docs/workstreams/m5_runtime_contract/CANDIDATE_RUNTIME_ADDENDUM.md`
   revision 7;
4. `docs/workstreams/m5_runtime_contract/RECOVERY_WORK_AMENDMENT.md`;
5. `docs/workstreams/m5_runtime_contract/EXECUTION_DISPOSITION_RECEIPT_CORRECTION.md`
   (M5-D24-C1);
6. `docs/workstreams/m5_runtime_contract/LEGACY_TERMINAL_COVERAGE_CORRECTION.md`
   (M5-D24-C2);
7. `docs/workstreams/m5_runtime_contract/CANCELLATION_EXPIRED_OUTPUT_CORRECTION.md`
   (M5-D24-C3);
8. `docs/workstreams/m5_runtime_contract/TERMINAL_SUCCESSOR_EXPIRED_OUTPUT_CORRECTION.md`
   (M5-D24-C4);
9. `docs/workstreams/m5_runtime_contract/TERMINAL_RACE_INVOCATION_WORK_CORRECTION.md`
   (M5-D24-C5);
10. `docs/workstreams/m5_runtime_contract/ACTIVE_TERMINAL_CUTOFF_INVOCATION_WORK_COMPLETION_CORRECTION.md`
    (M5-D24-C6);
11. `docs/workstreams/m5_runtime_contract/DIRECT_ACQUISITION_TERMINAL_CUTOFF_CORRECTION.md`
    (M5-D24-C7);
12. `docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT.md` at
   the accepted D25 bytes above;
13. `docs/workstreams/m5_runtime_contract/CHANGED_STATE_ABSENCE_AMENDMENT.md`
   at the accepted D26 bytes above;
14. `docs/m5_acceptance_matrix.md`;
15. `docs/m5_implementation_status.md`;
16. `docs/workstreams/m5_runtime_implementation/D25_SCHEMA_017_HANDOFF.md`;
17. `docs/workstreams/m5_runtime_implementation/D26_SCHEMA_017_HANDOFF.md`;
18. `docs/workstreams/m5_runtime_implementation/D26_MIGRATION_017_ACTIVATION.md`
    as historical ownership and environment evidence; and
19. this activation.

Frozen authority overrides this plan. This plan allocates paths and acceptance
work only; it does not alter a digest, DTO, SQL relation, migration ledger,
semantic rule, lifecycle rule, or publication identity.

## 2. Task 2 outcome and strict boundary

Task 2 implements the first public PostgreSQL store/runtime composition that
the Wave R6 handoff deliberately left pending. It is one complete vertical
slice, not the whole D25 event matrix. The required outcome is:

1. cursor-local, cacheless D25 current/working point readers, intent
   derivation, transition application, replay, and accumulated-work reads;
2. atomic integration of the D25 transition with group register/replace/
   retire and typed document insert/delete/replace structural opens, active
   requirement completion, and typed-direct semantic completion;
3. D25 activation/bootstrap installation through the already accepted public
   activation transaction;
4. one measured typed-seal transaction that validates persisted readiness and
   promotes physical matching state, semantic/certificate/currency/direct-M4
   state, public deltas, complete D22/D26 changed-state references, both
   publication heads, the immutable event result, and terminal epoch state;
5. exact reconnect/replay from PostgreSQL with zero model calls, zero reference
   regeneration, and zero writes; and
6. independent post-commit Python, SQL, and actual-image/provenance audit
   adapters that are excluded from measured seal latency.

### 2.1 Discovered event-envelope gap

The accepted D25 contract also names policy change, rootless
`ObserveRequirementEvent`, and standalone claim `ObserveEvent` as revision-1
structural sources. Migration 014 and `_m5_update_kind` already represent
`policy_change` and `observe_requirement`, but their public store/runtime
composition is not implemented and is deliberately deferred from this first
vertical slice. A later path-exclusive implementation activation may complete
those two forms under the existing frozen contract and schema.

Standalone claim `ObserveEvent` is different: migration 014's frozen
`groundloop_m5_update.update_kind` allowlist has no standalone claim-
observation value, and `_m5_update_kind` deliberately rejects it. Guessing
that event into another update kind would change frozen semantics; editing
migration 014/017 in this wave is forbidden. That one form needs a separate
contract/schema correction before implementation.

This activation claims none of those three structural forms. They remain
fail-closed and `PENDING`. Every D25 falsifier is still inventoried, but a row
requiring one of those forms is reported as uncovered rather than silently
treated as passing. This scope restriction is mandatory even if an
implementation shortcut appears easy.

The store derives affected keys, before/after points, patches, references,
certificates, work, and identities. Callers may supply only the compare-only
values allowed by D25. No application/provider caller may supply a physical
patch, logical patch, changed-state set, or D25 work counters.

This task does not authorize:

- a migration or any edit to migration 014, 015, 016, or 017;
- a contract, digest, DTO, enum, present-state recipe, or schema change;
- a seventh changed-state kind, tombstone table, nullable state hash,
  `repr`/JSON hashing, or process-local `M5IncrementalOverlay` cache;
- an inline Python/SQL full oracle, repository hydration, aggregate job scan,
  model call, retrieval call, commit, rollback, or nested transaction inside a
  cursor-local matching helper;
- provider implementation, dashboard/API/CLI expansion, deployment, or a real
  database `v1_only -> m5_active` transition;
- model-quality, objective-truth, latency, utility, security, novelty, or
  named-system-superiority claims; or
- promotion of any M5.4, D24, D25, D26, M5.5, or M5.6 status row merely
  because a scoped Task 2 test passes.

Runtime remains `v1_only` outside isolated disposable fixtures.

## 3. Frozen read-only implementation inputs

All lanes may read but must not edit these paths:

```text
migrations/014_m5_evidence_groups.sql
migrations/015_m5_runtime.sql
migrations/016_m5_runtime_recovery.sql
migrations/017_m5_persisted_matching.sql
src/groundloop/postgres/migrations.py
src/groundloop/m5/runtime/application.py
src/groundloop/m5/runtime/contracts.py
src/groundloop/m5/runtime/digests.py
src/groundloop/m5/incremental_overlay.py
src/groundloop/postgres/m5.py
tests/m5/postgres_runtime/test_migration_014.py
tests/m5/postgres_runtime/test_migration_015.py
tests/m5/postgres_runtime/test_migration_016.py
tests/m5/postgres_runtime/test_migration_017.py
```

Migration 017 already owns the exact D25/D26 relations, guards,
authorizations, deferred validation, ledger checks, and the sole authorized
migration-015 child-validator replacement. Runtime code consumes that surface;
it must not duplicate or weaken it. Existing migration tests remain immutable
regression gates for this wave.

## 4. Path-exclusive implementation sequence

Each lane starts from the exact integrated activation commit or from the exact
coordinator integration commit named by its dependency below. A path belongs
to exactly one lane. A lane may change fewer than its granted paths, but it may
not add a path. Discovering a necessary unlisted path is a hard stop requiring
a one-path docs-only correction, two new same-byte audits, integration, and
push before work resumes.

### 4.1 Lane A — cursor-local persisted-matching store core

```text
branch = workstream/m5-d25-store-core
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d25-store-core
base = exact integrated activation commit
```

Exact owned paths:

1. `src/groundloop/m5/runtime/postgres_matching.py` (new);
2. `tests/m5/postgres_runtime/d25_store_core/conftest.py` (new);
3. `tests/m5/postgres_runtime/d25_store_core/test_points.py` (new);
4. `tests/m5/postgres_runtime/d25_store_core/test_transition_apply.py` (new);
5. `tests/m5/postgres_runtime/d25_store_core/test_replay_work.py` (new); and
6. `docs/workstreams/m5_runtime_implementation/D25_STORE_CORE_HANDOFF.md`
   (new).

Lane A implements the exact D25 Section-8 cursor-local surface:

```text
effective_matching_image
resolved_matching_observation_point
resolved_matching_edge_point
resolved_matching_mask_point
resolved_matching_hall_point
effective_matching_observation
effective_matching_edge
effective_matching_mask
effective_matching_hall
least_effective_observation
representative_effective_hashes
derive_matching_transition_intent
apply_matching_transition
current_matching_work
```

It also owns private, cursor-local ledger and lock assertions needed by those
functions. It derives all keys, changes, logical output, work, artifacts, and
contributions from persisted authority. It may not expose a caller-authored
patch API or mutate runtime revisions independently. Initial acceptance must
include empty structural open, exact replay, conflicting replay, working
tombstones, current shadowing, representative order/cardinality, and
contribution/accumulator checks through the real migration-017 authorizer and
deferred guards.

### 4.2 Lane B — activation, promotion, reference, and audit helpers

```text
branch = workstream/m5-d25-publication-helpers
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d25-publication-helpers
base = exact integrated activation commit
```

Exact owned paths:

1. `src/groundloop/m5/runtime/postgres_matching_publication.py` (new);
2. `src/groundloop/m5/runtime/postgres_matching_audit.py` (new);
3. `tests/m5/postgres_runtime/d25_publication/conftest.py` (new);
4. `tests/m5/postgres_runtime/d25_publication/test_activation_projection.py`
   (new);
5. `tests/m5/postgres_runtime/d25_publication/test_seal_promotion.py` (new);
6. `tests/m5/postgres_runtime/d25_publication/test_changed_state_references.py`
   (new);
7. `tests/m5/postgres_runtime/d25_publication/test_physical_audit.py` (new);
   and
8. `docs/workstreams/m5_runtime_implementation/D25_PUBLICATION_AUDIT_HANDOFF.md`
   (new).

Lane B implements cursor-local helpers for activation projection installation,
`promote_matching_overlay`, deterministic event-result delta/reference
construction, and post-commit actual-image/provenance audit. D26 absence is
legal only for `requirement_state`, `group_state`, and `group_certificate`
under exact sealed structural `REPLACE`/`RETIRE`, with every predecessor,
closure, no-successor, payload, event/update/deactivation, and seal-coordinate
check required by the D26 amendment. Present references keep their existing
recipes. Promotion helpers do not own runtime headers, heads, result insertion,
transaction boundaries, commit, or rollback.

Lane B may develop in parallel with Lane A because it imports only frozen DTO/
digest contracts. The coordinator freezes its callable API before the first
composition lane starts.

### 4.3 Lane C1 — store transaction and seal composition

```text
branch = workstream/m5-d25-store-composition
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d25-store-composition
base = coordinator integration of accepted Lane A then accepted Lane B
```

Exact owned paths:

1. `src/groundloop/m5/runtime/persistence.py`;
2. `src/groundloop/m5/runtime/postgres_recovery.py`;
3. `tests/m5/postgres_runtime/d25_application/test_store_composition.py`
   (new);
4. `tests/m5/postgres_runtime/d25_application/test_seal_atomicity.py` (new);
5. `tests/m5/postgres_runtime/d25_application/test_store_races.py` (new); and
6. `docs/workstreams/m5_runtime_implementation/D25_STORE_COMPOSITION_HANDOFF.md`
   (new).

Lane C1 is the sole Task 2 owner of `persistence.py`. It composes matching
activation into `activate`, structural D25 transition into
`open_typed_event_atomically`, and adds the concrete public
`request_typed_seal_atomically` transaction. The seal uses persisted
point/CAS checks and one-epoch prefix ranges only. It must not use an inline
oracle, model call, aggregate job/contribution scan, or process cache.

The same transaction must either publish the complete D25 Section-9.4 image
or publish none of it. Replay point-reads the stored terminal result and exact
children and performs zero writes, work/timing additions, model calls, or
reference regeneration. Failure retains the working D25 image and advances no
publication head. `postgres_recovery.py` may change only where necessary to
compose the already-frozen D24 readiness/accounting into the one outer seal;
it may not redefine D24 counters, timing anchors, or dispositions.

### 4.4 Lane C2 — active requirement-completion composition

```text
branch = workstream/m5-d25-requirement-composition
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d25-requirement-composition
base = coordinator integration of accepted Lane C1
```

Exact owned paths:

1. `src/groundloop/m5/runtime/postgres_verifier.py`;
2. `tests/m5/postgres_runtime/d25_application/test_requirement_completion.py`
   (new);
3. `tests/m5/postgres_runtime/d25_application/test_requirement_races.py`
   (new); and
4. `docs/workstreams/m5_runtime_implementation/D25_REQUIREMENT_COMPOSITION_HANDOFF.md`
   (new).

Lane C2 atomically replaces the intentional active-completion hard stop with
one coalesced D25 transition before the shared revision advance. It must
preserve D24 attempt/output/work/timing rules. Inactive, expired, cancelled,
failed, and audit-only returns perform no D25 image, patch, contribution, or
counter mutation. Exact replay and both same-edge and different-edge races are
mandatory.

### 4.5 Lane C3 — typed-direct semantic composition

```text
branch = workstream/m5-d25-direct-composition
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d25-direct-composition
base = coordinator integration of accepted Lane C2
```

Exact owned paths:

1. `src/groundloop/m5/runtime/postgres_direct_recovery.py`;
2. `src/groundloop/m5/runtime/direct_m4.py`;
3. `tests/m5/postgres_runtime/d25_application/test_direct_transition.py`
   (new);
4. `tests/m5/postgres_runtime/d25_application/test_direct_races.py` (new);
   and
5. `docs/workstreams/m5_runtime_implementation/D25_DIRECT_COMPOSITION_HANDOFF.md`
   (new).

Lane C3 composes the direct expansion logical-only patch and the direct
verifier physical/logical patch before their existing revision advances. It
preserves the D24 direct-acquisition and terminal-cutoff authority, performs no
independent revision/header advance, and leaves late/inactive/loser paths D25-
inert. `direct_m4.py` may remain byte-identical; its grant exists only for
constructor/adapter wiring that cannot be placed in the recovery module, not
for changing frozen M4 identities.

### 4.6 Lane D — public façade and reconnect route

```text
branch = workstream/m5-d25-public-composition
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d25-public-composition
base = coordinator integration of accepted Lane C3
```

Exact owned paths:

1. `src/groundloop/m5/runtime/postgres_application.py`;
2. `src/groundloop/m5/runtime/postgres_direct_application.py`;
3. `tests/m5/postgres_runtime/d25_application/test_group_public_composition.py`
   (new);
4. `tests/m5/postgres_runtime/d25_application/test_direct_public_composition.py`
   (new);
5. `tests/m5/postgres_runtime/d25_application/test_reconnect.py` (new); and
6. `docs/workstreams/m5_runtime_implementation/D25_PUBLIC_COMPOSITION_HANDOFF.md`
   (new).

Lane D adds production-capable façade classes for the Task 2 group-lifecycle
and typed-document slice and keeps the historical `*PreSealPorts` classes
fail-closed as explicit regression evidence. Policy change, rootless
requirement observation, and standalone claim observation remain rejected.
The new classes delegate the unchanged frozen `M5RuntimePersistencePort`
protocol and must not recreate state, work, or references. A fresh
store/process reconnect must reuse durable completed work and execute only
missing external work under D24. Exact terminal replay performs zero
provider/model calls and zero writes.

## 5. Mandatory implementation evidence

Every applicable row is mandatory. A skipped, unavailable, no-match,
environment-failed, timeout-without-diagnosis, or silently deselected case is
not a pass.

### 5.1 D25 and D26

- an exact inventory of all 40 D25 Section-13 falsifiers, with each applicable
  Task 2 clause mapped to test node(s) and evidence bytes and every clause that
  requires policy change, rootless requirement observation, or standalone
  claim observation explicitly recorded as uncovered `PENDING`;
- all 18 D26 Section-7 falsifiers, including application-side emission,
  complete changed-state-set validation, and F18 reconnect/replay with zero
  mutation, model call, or reference regeneration;
- exact current/working/tombstone resolution, policy binding, C-order,
  representative cardinality, replay, patch/contribution coverage, counter
  separation, failure isolation, next-epoch isolation, crash cuts, concurrent
  completion, structural lifecycle, document withdrawal, activation, point
  plans, audit separation, and raw-DML guard success; policy flip/no-flip and
  rootless-observation rows remain explicitly uncovered `PENDING`; and
- the exact migration-017 ledger and accepted migration-015 sole D26 child-
  validator replacement remain byte-identical.

### 5.2 D24 and public-runtime composition

- every D24 recovery/accounting falsifier applicable to an active transition,
  terminalization, seal, reconnect, cutoff, or replay, mapped explicitly to
  the base D24 amendment and each accepted C1, C2, C3, C4, C5, C6, and C7
  correction above;
- all required serial race orders and crash-injection cuts around D24 work,
  D25 contribution, revision, job terminalization, publication, heads, result,
  and epoch state;
- provider/model execution proven outside PostgreSQL transaction/lock scope;
- current invocation work added exactly once at checked active cutoffs and
  never reconstructed from canonical totals; and
- no new timing anchor for D25 logical work.

### 5.3 Compatibility and regression

- migration 014, 015, 016, and 017 static/live regression selections;
- M4-v1 and never-activated byte stability, including public-M4 route guards;
- pure D25/D26 contract/digest suites;
- strict mypy, Ruff, compileall, import smoke, and exact changed-path checks;
- clean rollback inventories and no new caches, generated data, weights,
  secrets, volumes, or untracked lane artifacts; and
- after each measured successful history seal, independent Python, SQL, and
  actual-image/provenance comparison outside measured seal latency.

## 6. Environment and gate discipline

Python gates must use the repository source explicitly:

```bash
MYPYPATH=src python3 -m mypy <exact-owned-source-paths>
python3 -m ruff check <exact-owned-source-and-test-paths>
python3 -m pytest <exact-focused-selection>
python3 -m compileall src tests
```

PostgreSQL gates run serially in a fresh isolated schema/container path that
shares the database container network. On the currently qualified PostgreSQL
16.14 environment, use:

```text
PGOPTIONS='-c jit=off'
```

That qualification must be reported explicitly. The known default-JIT
SIGSEGV is not a product pass and must not be hidden. The prior host-published-
port cascade is invalid evidence and must not be pooled with qualified runs.
Parallel database suites are prohibited unless a later activation proves
isolation and deterministic cleanup.

Before and after every database tranche, record the exact database/schema
inventory and prove cleanup. Any crash, timeout, unexplained deselection,
leftover relation, or failed deferred constraint stops the lane.

## 7. Handoff and integration order

Each handoff must record:

1. activation commit/tree and exact lane base;
2. final commit/tree and clean index/worktree;
3. exact base-to-candidate name-status proved to be a subset of its grant,
   with every path necessary for the claimed lane outcome present;
4. blob SHA-256 for every changed path;
5. exact commands, environment, selected/collected/passed/failed/skipped
   counts, durations, and database inventory;
6. mapping from frozen falsifiers to test nodes and honest uncovered rows;
7. failure-injection/race ordering and reconnect evidence;
8. review findings and their resolution; and
9. explicit nonclaims and remaining PENDING rows.

The coordinator integrates in this order:

```text
audited activation
  -> Lane A
  -> Lane B
  -> Lane C1
  -> Lane C2
  -> Lane C3
  -> Lane D
  -> combined serial regression and independent same-byte integration audits
```

No lane self-accepts. Before each integration, one independent semantic/
correctness reviewer and one independent PostgreSQL/race reviewer audit the
same candidate bytes and both return `GO`, `P0=0`, `P1=0`. A byte edit after
review restarts both reviews. The coordinator re-runs relevant gates on the
integrated ancestry; lane-local green results are not pooled across different
bytes.

Main may fast-forward only from the exact previously integrated barrier.
Every accepted barrier is pushed before its dependent worktree is created.
No force push, rebase of held evidence, or merge from the dirty local main
checkout is allowed.

## 8. Acceptance and status ceiling

Task 2 is accepted only when the full combined path set has clean same-byte
audits, the complete qualified regression matrix for the declared vertical
slice passes, and reconnect plus terminal replay prove zero external calls and
zero writes. A smaller accepted lane is useful implementation evidence but not
Task 2 completion. Task 2 acceptance does not mean all 40 D25 falsifiers pass;
the event-envelope gap above remains an explicit subsequent gate.

Even after a technically successful Task 2 integration:

- runtime remains `v1_only` outside isolated fixtures;
- no real database activation or deployment is authorized;
- M5.4-07 maintained controlled history, M5.4-08 pinned-model diagnostic,
  M5.5 maintained evaluation, and M5.6 closure remain `PENDING`;
- D24/D25/D26 implementation and M5.4-05/-06/-09 may change from `PENDING`
  only in a separate coordinator-owned status reconciliation that maps every
  acceptance row to accepted evidence; and
- model/AI accuracy remains the bounded M4.12/M4.13 negative result until a
  separately preregistered provider/model evaluation is executed.

## 9. Activation-plan acceptance

Before this document grants ownership:

1. the activation commit has sole parent
   `781f667bc041ec771181d4ba8cca86b0bad5fa9b` and changes exactly this file;
2. `git diff --check`, exact-path, commit-parent, branch/worktree collision,
   authority-hash, migration-ledger, status/nonclaim, and protected-dirt hash
   checks pass;
3. one independent authority/ownership reviewer and one independent D24/D25/
   D26/PostgreSQL reviewer audit identical commit/tree bytes;
4. both return `GO`, `P0=0`, `P1=0` and state that no implementation branch
   existed during review;
5. any byte change restarts both audits;
6. the exact reviewed commit is fast-forwarded to `main` and pushed to
   `origin/main`; and
7. only then may Lane A and Lane B worktrees be created.

## 10. Protected local-main state

The dirty local main checkout is user-owned and excluded from every activation,
lane, test, review, commit, and integration operation:

```text
2af4b19962dc8a7d22e377be17f342530a06ee6395bbbf2a092eab36599c8fc2  pyproject.toml
45c20ca46e9ad5bcd86b22c0d8882d1d611497f57ca8c45d3dea149260c110cd  docs/presentations/groundloop_fyp_professor_feedback.pdf
59a13cd8d4bbb017e712c0f39e70f2eba136557e945f64f1bfc3891742a79f0  docs/presentations/groundloop_fyp_professor_feedback_v2.pdf
c12929c349a5c0be9793159143b09da40ea2a0b27df37b92d61d9ed6483d8c2a  docs/presentations/render_groundloop_fyp_professor_deck.py
167d1e7df5a720041fe0ff51879d08357f0dfbe3a7781ffaa08c0d958a47aa94  docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT_DRAFT.md
```

Never reset, clean, stash, reformat, stage, commit, copy, or delete these paths.
Before and after every coordinator integration, recheck both the porcelain
inventory and these hashes. Any drift stops the wave.
