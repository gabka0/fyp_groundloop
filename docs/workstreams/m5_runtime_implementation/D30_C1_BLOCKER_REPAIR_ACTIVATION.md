# M5-D30 C1 Prerequisite Blocker Repair Activation

Status: docs-only activation candidate; no source or test edit under this
activation is authorized until these exact bytes receive two independent
same-byte `GO`, `P0=0`, `P1=0` reviews, are committed as the sole changed
path, receive two postcommit identity checks, and are pushed to this branch
and `origin/main`

Date: 2026-09-25

## 1. Exact authority barrier

```text
required_parent = 81a642e9e60f2e33e2dbe4cb62ef699a380cc26b
required_parent_tree = 8c974b185460fce98620454cbd18fab140d684d6
required_origin_main = 81a642e9e60f2e33e2dbe4cb62ef699a380cc26b

activation_branch = workstream/m5-d30-c1-blocker-repair-activation
activation_changed_path_count = 1
runtime_mode = v1_only
```

The sole owned path is this file. Its commit must have the exact sole parent
above and may change no other path. This activation is subordinate, in order,
to the M5 design freeze, runtime addendum revision 11, M5-D24 through M5-D30,
the acceptance matrix, the D30 Task-2 activation, the accepted C1 interface
clarification, and the exact pushed Lane-R prerequisite commit. Frozen
contract authority wins without exception. This activation supersedes only
the specifically named C1 implementation-order wording and authorizes the two
path-exclusive prerequisite source repairs below; every other frozen rule
remains exact.

This activation changes no schema, migration, digest, public API, DTO,
present/absence recipe, timing coordinate, runtime mode, provider, model, or
claim boundary. The one new helper authorized below is package-private and
does not replace or weaken either accepted public matching-publication helper.

## 2. Confirmed implementation contradictions

### 2.1 Empty predecessor overcounts group-local work

Lane C1's live group `REPLACE` and `RETIRE` first-application tests reach the
migration-017 deferred validator with an otherwise coherent staged image, but
the validator rejects the persisted matching contribution with:

```text
persisted matching derived logical work counter mismatch
```

The contradiction is limited to `_structural_retirement_plan(...)` in
`postgres_matching.py`. It currently adds exactly one predecessor
`group_local_state_operations` operation for every replacement or retirement.
That is false when the predecessor has no ordinary hash-mask transition.

Migration 017 is the frozen executable oracle. It derives the counter as:

```text
count(distinct ordinary mask-change group IDs)
+ count(distinct group-binding logical-output object IDs)
```

The structural-retirement planner never emits a group-binding logical output:
it may remove an existing group-certificate binding by structural interval
closure, and it may emit a claim-binding output, but neither is a new
`group_binding` logical output. Therefore this planner's predecessor-local
contribution is exactly:

```text
len({change.group_version_id for change in mask_changes})
```

The already-computed `successor_initialization_work` remains additive and
unchanged. A predecessor with one or more ordinary mask rows still contributes
one operation because every row belongs to the one retired predecessor. An
empty predecessor contributes zero. The logical removal of requirement state,
group state, or group certificate does not independently increment this
matching-kernel counter.

The hard-coded value happened to agree with nonempty predecessor fixtures and
therefore escaped the earlier store-core gate. C1 exposed the empty-predecessor
case while composing the real deferred validator into the single transaction.
This is a prerequisite implementation defect, not a new C1 semantic, and it
cannot be repaired inside C1 because `postgres_matching.py` is outside C1's
exact path ownership.

### 2.2 Document declarations stale the prepared header image

The accepted C1 order derives and prepares D25 before writing the already-
reserved direct and M5 scope/job declarations. `_prepare_matching_transition`
correctly seals the exact current base/runtime header images into its
single-use authority. The direct M4 scope/job callbacks then insert rows in
`groundloop_discovery_scope` and `groundloop_semantic_job`; the frozen
migration-012 triggers correctly increment
`groundloop_epoch.open_scope_count` and `open_job_count`. D25 tier-12 staging
then compares the live header with its sealed pre-declaration image and
correctly fails:

```text
matching header after-image changed before stage
```

The runtime header's M5 counts are established before the declaration writes;
the contradictory mutation is the legitimate direct-M4 base-header trigger
effect. Document `INSERT` provides the smallest reproducer because its D25
logical patch is otherwise empty. Delete/replace must obey the same correction.

A caller-provided count, mutable prepared-authority refresh, post-seal snapshot
rewrite, disabled trigger, weakened equality check, or moving D25 stage before
declarations is nonconforming. The narrow correction is to write only the
declaration coordinates already reserved and locked by the D29 continuation,
then derive and prepare D25 from the resulting exact persisted header, before
any tier-11a stage. No locator, reservation, source, plan, semantic after-image,
or D25 target is rediscovered by this move.

### 2.3 The accepted seal order has an impossible dependency cycle

The in-progress, untracked C1 execution-handoff draft orders terminal
base/runtime/head advancement before matching-publication child preparation
and the D24 seal contribution. The accepted C1 interface clarification also
requires one atomic private seal without supplying a legal suborder for the
current helpers. No legal implementation order exists with the current
helpers and migration guards:

1. `prepare_matching_publication_children(...)` reaches
   `_load_seal_envelope(...)`, which accepts only the fully terminal base,
   runtime, head and matching-current image.
2. `persist_seal_contribution(...)` needs the combined-delta-set hash and
   changed-state-set hash derived from those children for the immutable
   `m5-seal-contribution-source-v1` identity.
3. Migration 016's immediate `BEFORE INSERT` guard rejects every event-accounted
   work contribution once the runtime is `sealed`/`failed` or its accumulator
   is terminalized. Its seal-result binding check is deliberately deferred.
4. Moving accumulator finalization earlier is also invalid: the immediate work
   and timing accumulator guards require the runtime to be terminal at the
   resulting revision.
5. `build_matching_publication_children(...)` is not an escape hatch. It
   requires the later immutable event result and exact immutable event-work row.

Thus terminal-first prevents contribution insertion, while contribution-first
lacks store-derived child hashes. Disabling a guard, supplying hashes from the
caller, changing a migration, inserting the contribution after terminal, or
weakening the final result-bound derivation would violate D24/D25.

The narrow correction is one package-private preterminal preparation helper.
It must derive through the exact existing six-kind present-state and D26
absence recipes from a deliberately half-terminal same-transaction image:
the base epoch, both heads, matching current image and published semantic rows
are already at final seal coordinates, while the runtime and both accounting
accumulators remain nonterminal at the prior revision. The outer transaction
then inserts the contribution, terminalizes runtime/accounting, inserts the
result, and uses the unchanged result-bound builder to rederive and compare
the exact children before commit.

### 2.4 Planned transition deltas and seal-owned event accounting

D25's frozen prepared authority deliberately records all five D24-owned
planned coordinates, including
`public_delta_write_count=len(status_deltas)`. D27 executable tests bind that
field to `prepared.d25_contribution_work.public_status_deltas`; it must not be
zeroed or removed merely because runtime accounting belongs to a later phase.
Migration 017 also requires the exact logical-output `status_delta` multiset in
`groundloop_status_delta` at tier 16 before the transition can commit.

D24's frozen owner matrix is equally explicit: `public_delta_count` is owned
by the successful `seal` contribution, not `structural_open`.
`StructuralOpenWorkInputs` therefore has no public-delta field, and seal is the
only allowed D24 surface that carries this runtime-work coordinate on the C1
route. This later seal-owned event-publication count is distinct from the
earlier D25 transition-history row and its prepared equality authority; it is
not permission to double-count the same logical delta at open and seal.

C1 currently reads and validates all five prepared coordinates, but rejects
every nonzero `public_delta_write_count` before constructing structural-open
work. It also does not yet persist the prepared tier-16 status-delta rows. A
document delete/replace with a real status change therefore cannot open.

The conforming correction is C1-local: preserve and validate the exact
nonnegative planned field; after D25 finalization persist only the exact
prepared status-delta output rows at tier 16; but neither require the field to
be zero nor map it into `StructuralOpenWorkInputs`. At seal, rederive the
publication children and set the seal contribution's `public_delta_count` from
the exact combined-delta child cardinality. The D25 planned count remains
transition/output validation authority; the D24 event-publication count is
charged exactly once at its frozen seal owner.

## 3. Corrected C1 composition rules

### 3.1 Document structural-open sequence

This subsection supersedes only Steps 12 and 13 of
`D30_C1_PHASED_STRUCTURAL_INTERFACE_CLARIFICATION.md` Section 7 and their
duplicate in the C1 execution handoff. Steps 1--11 and 14--19 remain exact.
It is the narrow docs-first ordering amendment that makes the simpler lexical
move authoritative; until this activation passes its barrier, the previously
accepted prepare-before-declaration wording remains in force.

After the D29 locked continuation has reserved/locked the complete declaration
coordinate superset and returned the recomputed direct/requirement plans:

```text
12. write every already-reserved direct scope, then every sorted M5 scope;
    write every already-reserved direct job, then every sorted M5 job; persist
    the structural recovery identity. No new locator or lock is acquired.
13. derive the official D25 intent and prepare it exactly once from the
    complete direct-plus-group authority and the post-declaration persisted
    headers. This acquires only its remaining tier-11a and tier-11b--14
    authority.
14. invoke direct tier 11a, then D25 tier 11a through tier 12, unchanged.
```

This keeps all tier-8 scopes before every tier-9 job, preserves direct-before-
M5 ordering within each family, leaves D25 preparation before tier-11a staging,
and lets the sealed prepared header equal the only legitimate persisted header
image. The group-event branch is unchanged because it has no direct M4
declaration-trigger effect.

The declaration writes must consume only the exact D29-held coordinates. They
may acquire no lock or source authority, and their presence must not change the
already-held affected-key set, semantic before/after plan, D25 patch bytes or
matching work. Focused tests must compare those values across the lexical move.

C1 may implement only this lexical move inside its existing `persistence.py`
ownership and adjust/add assertions only in its existing six owned test files
and handoff. It gains no new path. It must not add a prepared-header refresh
helper or change any migration.

### 3.2 Atomic private seal sequence

This subsection narrowly supersedes the seal-composition sentence in the
accepted C1 interface clarification. The in-progress, untracked
`D30_STRUCTURAL_COMPOSITION_HANDOFF.md` is not authority at this activation's
base; its Section 3.8 must adopt this corrected order when C1 resumes. The
complete transaction order is:

1. validate and lock the exact seal readiness image, both point accumulators,
   publication serializers, and every previously accepted bounded coordinate;
2. call `promote_matching_overlay(...)` once;
3. publish only the bounded prepared M5 state/certificate after-images and
   exact predecessor closures;
4. compose the fixture-local M4 semantic seal where applicable;
5. advance the base epoch and both publication heads to the exact sealed
   revision/status, but leave `groundloop_m5_runtime_epoch` at
   `semantic_complete`, the expected prior revision and `terminal_at IS NULL`;
   both accumulators also remain nonterminal;
6. call the new package-private preterminal matching-publication preparation
   helper exactly once and derive its exact two set hashes;
7. derive the seal-owned work, including
   `public_delta_count=len(prepared_children.combined_deltas)`, and insert the
   sole D24 seal contribution while runtime is nonterminal; derive/select its
   sole terminal timing anchor without persisting a pending anchor;
8. advance the runtime to `sealed` at the resulting revision, then freeze both
   accumulators through the existing seal finalizers;
9. insert the exact immutable event/call work, event result and timing coverage;
10. call the unchanged result-bound
    `build_matching_publication_children(...)`, require DTO/byte equality with
    the preterminal children, then insert the exact public delta and
    changed-state-reference result children;
11. force all deferred validation last and return the existing receipt/result
    types.

The half-terminal image is never externally visible: every step is in the one
existing outer transaction and any error rolls the full image back. No
constraint may be forced between matching promotion and the final complete
envelope because migration 017 seals its private promotion journal when
validation begins and rejects later guarded DML.

This sequence creates no second D25 transition/contribution, no second timing
anchor and no mutable prepared authority. The existing terminal preparation
and result-bound build helpers retain their current signatures, validation and
behavior. Public seal wiring and post-seal cross-layer audits remain Lane D/I.

### 3.3 Tier-16 and public-delta accounting split

Within C1's existing `persistence.py` ownership, structural-open derivation
must continue to validate the type and nonnegative value of all five D25
planned coordinates. It then consumes only the group/claim/answer-state and
certificate-binding fields that structural open accounts. It neither zeros
nor adds the prepared public-delta field to structural-open work.

After the exact D25 15i--15k finalizer, C1 must extract only `status_delta`
records from the prepared canonical logical-output multiset, byte/type-validate
each `StatusDelta`, validate the cardinality against both the prepared
`public_delta_write_count` and D25 `public_status_deltas`, and insert those exact
rows into `groundloop_status_delta` at the intent epoch/revision before forcing
constraints. It may not recompute deltas from staged after-state or accept them
from a callback/caller.

C1's existing test and handoff paths must prove on a real nonzero-delta
document event that:

1. D25 retains the exact nonzero prepared count and logical counter;
2. the exact tier-16 multiset exists at the transition point;
3. the committed `structural_open` contribution and current nonterminal event
   work have `public_delta_count=0`;
4. the later seal contribution and immutable terminal event work contain the
   exact combined-delta cardinality once, not twice; and
5. open and seal replay add no duplicate row, contribution, child or work.

Missing, extra or changed tier-16 rows, count disagreement, a nonzero open
runtime-work coordinate, or a duplicate seal count must fail atomically.

## 4. Narrow prerequisite repair lane

```text
lane = C1-R -- C1 prerequisite blocker repair
branch = workstream/m5-d30-c1-prerequisite-blocker-repair
base = exact pushed commit of this activation
```

The lane owns exactly these six paths:

1. `src/groundloop/m5/runtime/postgres_matching.py`;
2. `src/groundloop/m5/runtime/postgres_matching_publication.py`;
3. `tests/m5/postgres_runtime/d25_store_core/test_retirement_work_counter.py`
   (new);
4. `tests/m5/postgres_runtime/d25_publication/test_changed_state_references.py`;
5. `tests/m5/postgres_runtime/d25_publication/test_seal_promotion.py`; and
6. `docs/workstreams/m5_runtime_implementation/D30_C1_PREREQUISITE_BLOCKER_REPAIR_HANDOFF.md`
   (new).

Every other path is read-only. In particular, the lane may not edit any C1
composition path, any migration, frozen amendment, public facade, DTO/digest,
or the protected checkout. Discovery of another required edit is a hard stop
for a new docs-only activation; it is not implicit path expansion.

### 4.1 Group-local counter source change

The `postgres_matching.py` source edit is limited to replacing the hard-coded
predecessor `group_local_state_operations=1` inside
`_structural_retirement_plan(...)` with the exact distinct `mask_changes`
group count from Section 2.1. It must not alter:

- observation, edge, mask, Hall, state, certificate, binding, status-delta,
  output-byte, touched-object, or D24-owned planned counts;
- retirement/replacement logical changes or present/absence recipes;
- `successor_initialization_work` or the addition order;
- transition derivation, lock order, staging, finalization, journal rows, or
  deferred-constraint behavior; or
- any other `group_local_state_operations` producer.

No helper refactor is required. If a helper is introduced, it must remain
package-private, pure, local to this file, and byte-for-byte equivalent to the
set-cardinality expression above.

The new store-core test must use real migration-017 PostgreSQL validation, not
a mocked cursor or source-text assertion. On fresh isolated schemas it must
prove zero-hash `RETIRE`, zero-hash `REPLACE`, and a nonempty predecessor with
at least two ordinary mask-change rows for that same one predecessor all
persist the exact counter, accumulator, patch and replay intent, force every
deferred constraint, and commit coherently without rewriting derived evidence.
The last case must still prove `group_local_state_operations == 1`, thereby
distinguishing the required distinct-group cardinality from `len(mask_changes)`.

### 4.2 Package-private preterminal child preparation

`postgres_matching_publication.py` may add exactly this package-private seam:

```python
def _prepare_preterminal_matching_publication_children(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    expected_revision: int,
    sealed_revision: int,
) -> M5MatchingPublicationChildren: ...
```

It is not exported. Before deriving any child it must fail closed unless all
of the following are exact:

1. all coordinates are strict positive integers and
   `sealed_revision == expected_revision + 1`;
2. runtime mode is `m5_active`;
3. the migration-017 private promotion tables and transaction-local OID/GUC
   bindings are genuine, and there is exactly one row for the current backend,
   transaction and session role with mode `seal`, the supplied epoch/revisions,
   exact decision policy, and both `validation_started=false` and
   `validation_done=false`;
4. that row's captured predecessor M4/M5 heads, revision, sealed timestamp and
   current matching policy agree with the update, runtime predecessor, working
   image and exact sealed predecessor row;
5. the current base epoch is already exactly
   `committed/sealed/complete/strict`, has a non-null seal time and the supplied
   sealed revision, while the runtime has the same event ID, remains exactly
   `semantic_complete` at `expected_revision`, has null terminal time and has
   zero open-work, open-scope and blocking-failure counts;
6. both current publication heads equal the new epoch and the M5 head carries
   `sealed_revision`;
7. the matching current image is installed at the new epoch/sealed revision
   under the exact policy, while the working image retains the captured
   predecessor base/policy and has not advanced beyond `expected_revision`;
8. the work and timing accumulators remain nonterminal at
   `expected_revision`;
9. no event-result parent, result delta or changed-state-reference child exists;
   and
10. deactivation cardinality, event identity, action and successor mapping are
    the same exact 0/1 envelope accepted by the terminal loader.

The implementation must refactor only enough for the preterminal and existing
terminal loaders to feed one shared child-derivation body. The six present
reference kinds, every present-state artifact recipe, D26 absence candidate,
logical-change, predecessor-closure/no-successor validation, combined-delta
recipe, ordering and set-digest inputs remain byte-for-byte semantic equals.
The caller supplies no key, state hash, delta or child set.

`prepare_matching_publication_children(...)` and
`build_matching_publication_children(...)` retain their current signatures and
terminal behavior. The latter still rederives after the immutable result and
event-work row exist, validates their exact bindings, and is the only child set
eligible for insertion. The preterminal result is compare authority, not an
independent publication source.

The two owned publication test modules must include both deterministic/fake
fail-closed coverage and real PostgreSQL proof. At minimum they must establish:

1. wrong type, nonadjacent revision, absent/spoofed context, wrong transaction,
   policy/head/predecessor/runtime/accumulator coordinate, started validation,
   pre-promotion call, missing/corrupt promoted row and pre-existing result or
   child all fail;
2. after exact matching plus semantic promotion and base/head advancement, but
   before runtime terminalization, the helper derives children while the
   result remains absent and the runtime/accumulators remain nonterminal;
3. the exact seal contribution can then be inserted before terminalization,
   while the retained immediate guard still rejects an attempted insertion
   after terminalization;
4. after completing the accepted terminal envelope, the unchanged result-bound
   builder returns the identical DTO/bytes for present-state, certificate-only
   and D26 absence cases; and
5. forcing deferred constraints only after the full envelope validates the
   contribution/result source identity and complete promotion journal.

No new positive-path proof may disable or monkeypatch a trigger, manually
author child hashes, rewrite a derived row, weaken an existing assertion, skip,
or xfail the new path. The retained rollback-contained negative test that
temporarily disables the event-result user trigger only to inject corruption,
then re-enables it and proves fail-closed rejection, remains required and is
not success evidence for the new helper.

## 5. Required evidence and integration barrier

On identical final C1-R bytes, record:

1. exact parent, path, mode, blob, byte, line, SHA-256, aggregate framed
   digest, and clean-index ledgers;
2. the focused live counter regression and both focused publication-helper
   modules;
3. the complete retained D25 store-core and D25 publication suites;
4. retained migration-016/017 plus D29/D30 prerequisite suites needed to detect
   trigger, planner, promotion and result-binding drift;
5. compile, Ruff-format/check, strict relevant mypy, and package-content
   checks;
6. two independent whole-byte `GO`, `P0=0`, `P1=0` audits;
7. one exact repair commit and two postcommit identity checks; and
8. push of that exact commit to the repair branch and `origin/main`.

Only after those gates may the dirty C1 worktree be advanced by an exact
fast-forward that preserves its disjoint nine-path working tree. Before that
fast-forward, the coordinator must prove that C1 has no modification at any
C1-R owned path. No reset, checkout, stash, clean, patch transplant, or manual
copy may be used to bypass the lineage check.

The resumed C1 candidate must apply only the Section-3 corrections, then rerun
at least:

1. the focused group register/replace/retire first-application test;
2. real document insert/delete/replace paths proving declaration-trigger order,
   tier-16 output insertion and exact prepared-header validation;
3. one real nonzero-delta document open/seal proving the Section-3.3 ownership
   split and exact replay;
4. a real atomic seal proving the half-terminal image, contribution-before-
   runtime-terminal order, result-bound child equality and rollback at every
   injected cut;
5. the complete C1 owned suite and retained D25 store-core/publication suites;
   and
6. the final unchanged-byte C1 gates and two independent audits.

Evidence from pre-repair, pre-sequence-correction or pre-ownership-correction
bytes cannot be pooled with final evidence.

## 6. Claim ceiling

This activation does not itself pass any pending correction or C1:

```text
matching_planner_repair = PASS
document_structural_prerequisite_repair = PASS
group_local_retirement_counter_repair = PENDING_C1_R
preterminal_matching_child_preparation = PENDING_C1_R
document_declaration_header_sequence = PENDING_LANE_C1
tier16_public_delta_open_seal_ownership = PENDING_LANE_C1
package_private_structural_composition = PENDING_LANE_C1
matching_aware_public_facades = PENDING_LANE_D
real_m4_phase_producers = PENDING_LANE_M_AND_C3
cross_layer_publication_races = DEFERRED_TO_LANE_D_AND_LANE_I
whole_route_output_nonchange = DEFERRED_TO_LANE_D_AND_LANE_I
M5-D30 implementation = PENDING
Task 2 = PENDING
runtime_mode = v1_only
```

The broader ceilings remain exactly:

```text
M5-D24 through M5-D30 = implementation-PENDING
M5.0-24 through M5.0-30 = implementation-PENDING
M5.4-05 through M5.4-09 = PENDING
M5.5 and M5.6 = PENDING
deployment/performance/utility/security/objective-truth/novelty/
maintained-history/named-system-superiority/AI-quality = PENDING
```

No performance, scalability, utility, security, novelty, deployment, AI
quality, or whole-M5 claim follows from any correction in this activation.
