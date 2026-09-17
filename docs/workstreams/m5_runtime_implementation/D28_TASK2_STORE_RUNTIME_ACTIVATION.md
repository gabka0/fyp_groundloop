# M5-D28 Task-2 Store/Runtime Composition Activation

Status: docs-only activation candidate; it grants no implementation ownership
until this exact file is committed as the sole change on the exact pushed
parent below, two independent reviewers audit identical commit/tree bytes and
each returns GO with P0=0 and P1=0, and that reviewed activation commit is
fast-forwarded and pushed to origin/main

Date: 2026-09-18

## 1. Exact activation barrier

    required_activation_parent = 003b45d18be0f1221179c6689cbd65f03f082c50
    required_activation_parent_tree = ff28f54965a68dad12049686cf72ee6cac047c9d

    d28_candidate_commit = cc0b9cf25110992320dc0b7f81499a3930d1a565
    d28_candidate_tree = 760107f9b6cbd70d37839144980cf23b8f5859ea
    d28_candidate_parent = 8ecdc8e361cf4a8c055fa00e9916e1704cbb736d
    d28_amendment_sha256 = 8a2bafd3478cf2cac6ac7c8de7ca7779a6d9ace7fbbe98a7dc3ff08afdb67eae
    d28_amendment_lines = 810
    d28_amendment_bytes = 48053
    d28_freeze_commit = 003b45d18be0f1221179c6689cbd65f03f082c50
    d28_freeze_tree = ff28f54965a68dad12049686cf72ee6cac047c9d
    d28_freeze_handoff_sha256 = 73388376a1be24bfc7ea5dfad2dc055c66c0e8f03ab5031622f7ba86b3dce1bc

    d24_amendment_sha256 = 3fe36a38c3c789143542f8b8cf6614a1df5e7807b9dffa99f0d2cc5118dc1209
    d25_amendment_sha256 = bac12ab5e74632c04f1bd70d0ef0d00522ba9d268eb8b73d11845bbf3b873aae
    d26_amendment_sha256 = 85372d4c2f9108810bd75c3e5611de541d0f31c8a096421f30e68fad84676721
    d27_erratum_sha256 = 7a51afc1f1b6c249572222a023814de8b22220058e00f09564de64f552bd411c
    runtime_addendum_revision = 9
    runtime_addendum_sha256 = 4a99e255d266835794638cc3385220ba951b3a22cbbd364076f0eec26ea46d7f

    d24_c1_execution_disposition_sha256 = 9961c87b29e47742b4dcf00df5ad9b6e88afdefc225a6103c7907e8bf91246d1
    d24_c2_legacy_terminal_sha256 = e86e48f3720d088c64781909b1cb24ac7149ed6b31cfff080cae59e11f7939ab
    d24_c3_cancel_expired_sha256 = 5724682eb53c4b599a3724f6b030c5e9f29122246c08ae2a917b440d22fc144d
    d24_c4_successor_expired_sha256 = e43754ccd962c000b9ec5bc7b0e0508f9fe5f55d401110e8fa594ad513713808
    d24_c5_terminal_race_sha256 = 661f8d5fc4b4ccafdc941908ba66a9aa6badd0d4806efdb0f5aefc5cfafce648
    d24_c6_active_cutoff_sha256 = 998b4f9c29babd33ec233a943fd7e538714372d96e44bf675771190e29ee564e
    d24_c7_direct_cutoff_sha256 = e76b36d15092a47ef531968cdd6689d6adaf9cf10862e8e997759b59e613e15c

    lane_a_candidate_commit = acdbaf53e804fd6182e37eb6c6cee95efb42dcc3
    lane_a_candidate_tree = 93c32e1a127bb9aaa47d9bf30d6bee5e3b86f3fc
    lane_a_handoff_sha256 = ce548f3c927476d1160fe93b0684b564aa3f8c82d06986590246daaa33e8c724
    lane_a_integration_commit = 9e58fcdc3f5b1507b0d42559077cf35d5024b475
    lane_a_integration_tree = ed25309cf846fef9e50216e6c4d3a8a0aa9c0a52

    lane_b_candidate_commit = 72240c33fe2e2d42c115c29541943b2968d620f0
    lane_b_candidate_tree = a3fc3b467d14e5f3e9b9ba3cb8f7ca9095309e90
    lane_b_handoff_sha256 = 4bebda8d34979a3d366c8be746825dbe38f50f5ac91b7424772e51c37384783a
    lane_b_integration_commit = 8ecdc8e361cf4a8c055fa00e9916e1704cbb736d
    lane_b_integration_tree = 60318ff77fbb4838872b26a340f619e32a5ff5ed

    migration_013_sha256 = ffe1a403039b78006aef6d8da005f77d9d8ca103f52822d81e9242b669a328fb
    migration_014_sha256 = 4c37626f24524316c7990b2f3d213573bbe0f805a2c0f884585bf6aa325f0330
    migration_015_sha256 = 85cb7f8e6a33273ce67fc6b4160e74a3aff314cd084df7ac3647cadae930185c
    migration_016_sha256 = a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7
    migration_017_sha256 = e387b01fa80145273ba40d2d83581bc54762d3a4a2edd34669c095076c52154c

    original_task2_activation_sha256 = af5cdb923f3caf57df0dac12379c97c9dfe583c0be30659ac887622eab3dba22
    original_task2_activation_commit = e3d83e36570efd65703472e3022c252c0d0fc008
    original_task2_activation_tree = 9f98d9c84b01a21be8fd727eaca2930f26388aab
    d27_freeze_commit = bf61009f5857dfcb7641e89ee128960f8587d297
    d27_freeze_tree = fa69f808d04c365b5d63c3b5a92840702a953c65
    d27_freeze_handoff_sha256 = 4748d8196e5b72b46f1745d79acebf1b4e0841ef573332385ae654691670214d
    held_d27_draft_sha256 = 797dc5320e1dbc6dc787f0cfaa0b85b358bbb041c980b6a16fe4999062aca25d
    runtime_mode = v1_only

The required parent is the pushed D28 authority-freeze barrier. The D28
candidate and freeze each received independent same-byte precommit and
postcommit GO reviews with P0=0, P1=0, and P2=0. Lane A and Lane B likewise
remain accepted, integrated, scoped foundation/helper evidence. They are not a
public Task-2 composition.

This activation changes exactly this file. A commit cannot name its own
identity, so its two external audit records must pin the containing commit,
tree, sole parent, file SHA-256, line/byte counts, and clean worktree. No
implementation lane branch or worktree named in Section 7 may exist until that
reviewed activation commit is present on origin/main.

The uncommitted D27 continuation draft remains untouched historical evidence
at its recorded SHA-256. It was never authority. This activation explicitly
supersedes and revokes:

1. Sections 4.3 through 4.6 of
   D25_D26_STORE_RUNTIME_COMPOSITION_ACTIVATION.md;
2. every proposed P/C1/C2/M/C3/D grant in the held D27 continuation draft;
3. the D27 draft's monolithic or wrapper first-application wording;
4. its direct sequence wherever the complete plan, both header after-images,
   all three tier-15c mutation families, or lexical stage result were not
   explicit; and
5. its replay wording wherever it required the original transient no-op lock
   plan or an accumulator fixed to the historical transition revision.

No historical implementation branch may be merged, rebased, cherry-picked,
copied, or revived. It may be inspected only as labelled read-only evidence.

The controlling reading order is AGENTS.md and its complete list, then the
accepted D24 through D28 contracts and corrections, runtime-addendum revision
9, the acceptance matrix, migration-017 activation/schema handoffs, the
accepted Lane-A/Lane-B handoffs, the D28 freeze handoff, and finally this
activation. Frozen authority always overrides this path plan.

## 2. Authorized outcome

This activation authorizes only the first complete PostgreSQL Task-2
store/runtime composition for the already frozen event subset. The combined
result must provide:

1. store-derived, nonempty D25 transition planning for group
   register/replace/retire, typed document insert/delete/replace, active
   requirement completion, direct expansion, and direct verifier completion;
2. explicit cursor-local first-application phases that interleave one D25
   semantic transition with the existing D24 accounting in one caller-owned
   PostgreSQL transaction;
3. activation, structural open, requirement completion, typed-direct
   expansion/completion, seal, result/children, heads, and terminalization
   without a second revision or timing anchor;
4. a fresh-process, store-derived direct-M4 route that does not hydrate or
   retain the historical M4 repository/engine cache;
5. production-capable public group and typed-direct facades while retaining
   both historical pre-seal facades as fail-closed regression surfaces;
6. exact historical replay with zero writes, zero provider/model calls, zero
   regeneration, and the current validated retained accumulator; and
7. post-commit, outside-latency Python, SQL, and physical/provenance audits
   over one consistent sealed snapshot with a final publication-head recheck.

The store, never a caller, derives sources, affected keys, before/after images,
patches, certificates, references, artifacts, work, counts, status deltas,
contributions, and accumulators. Provider/model calls remain outside
PostgreSQL transactions and lock scope.

### 2.1 Deliberately unsupported event forms

Policy change and rootless ObserveRequirementEvent remain schema-representable
but outside this slice. Standalone claim ObserveEvent remains unrepresentable
by migration 014's frozen update-kind allowlist. All three must fail closed
and remain PENDING. No lane may map one form to another update kind or call its
falsifier passed.

### 2.2 Counter ownership

D24 owns exactly five existing physical coordinates: group-state,
claim-state, answer-state, certificate-binding, and public-delta writes. It
has no requirement-state coordinate. Requirement-state rows remain mandatory
D25 patch/bijection/replay evidence. A transaction-local diagnostic count may
compare planned with actual requirement-state writes, but it is never
persisted, hashed, returned, reported, reconstructed, or aliased. Certificate
artifact insertion is not a certificate-binding write.

## 3. Frozen D28 composition protocol

### 3.1 Source-present first application

Structural-open and requirement-completion first application must use the
following explicit sequence:

1. establish the operation-specific prefix: structural open persists/locks
   its exact tier-5 event source, inserts/locks its revision-1 base/runtime
   rows, then invokes the authorizer; requirement completion invokes the
   tier-6 authorizer with a compare-only proposed attempt ID first, then
   persists/locks the exact immutable attempt-result and output/execution/
   verifier closure through tier 10 and proves the official attempt ID;
2. that operation-specific migration-017 authorizer invocation occurs exactly
   once at its frozen tier-6 position; never reset or reacquire its context;
3. invoke the unchanged derive_matching_transition_intent callable exactly
   once; that call owns pre-discovery gathering, tier 11a, current then working
   tier 11b, the sole bounded representative discovery, exact tier 11c, and
   the remaining tier-11c-through-14 locks;
4. prepare from the official intent and held rows, without a second derive
   call, the exact prewrite patch, full physical/logical plan, D25 work,
   D24-owned counts, D27 diagnostic, both complete header after-images, and
   prewrite matching-image semantic revision;
5. for requirement completion only, install the complete groundloop_epoch
   after-image first and complete groundloop_m5_runtime_epoch after-image
   second, advancing N to N+1 exactly once before resulting-revision rows;
   structural open remains at inserted revision 1 and does not advance again;
6. revalidate and stage tier-11a observation/currency DML first and only then
   the remaining lower-tier D25 matching/state/certificate writes; stage may
   not write 15i, 15j, or 15k;
7. terminalize the already-locked requirement job/attempt after stage and
   before tier 15a, with no new earlier lock;
8. execute exact 15a, 15b, and applicable 15c work, then D24 15d through 15h;
9. finalize D25 at 15i, 15j, and 15k using the explicit staged authority and
   without acquiring an earlier lock;
10. write the exact tier-16 groundloop_status_delta multiset and remaining
    later public/result rows; and
11. force accepted deferred validation before commit.

The accumulator is first loaded/locked as authority only at 15k. A later
transition validates its prior updated_revision against the captured prewrite
matching-image semantic revision, not a coordination-only runtime revision.

### 3.2 Direct first application

Direct completion has no immutable source until tier 15c and must use this
different exact sequence:

1. hold the existing epoch/runtime serializer and invoke the unchanged
   migration-017 authorizer once using the compare-only proposed source ID;
2. lock and validate every frozen precursor and deterministically derive the
   official expected source ID/hash;
3. gather and lock the complete joint M4/D25 tier-11a-through-14 plan,
   including the one bounded representative discovery;
4. acquire and validate every applicable 15a, then 15b, then 15c
   row/absence/advisory/unique-conflict coordinate;
5. bind the plan and before images into an explicit cursor/backend/xid/context
   and single-use reservation, with no D25 DML;
6. pass that reservation lexically to the private M4 stage; using only held
   locks, it writes lower-tier/job closure, 15a, 15b, then the 15c
   evaluation-epoch counter, every ordered override counter, and finally the
   immutable evaluation-counter transition;
7. receive a lexical DirectM4StageResult covering first-old/final-new images
   and actual counts for every mutated family, including all three 15c
   families;
8. pass both values explicitly to source completion, which loads the exact
   15c source, derives the unchanged official D25 intent, proves full
   reservation equality, revalidates stage evidence, derives the complete
   patch/work/count/header plan, and returns official intent plus a
   ready-to-advance prepared authority while doing zero D25 DML;
9. install complete base then runtime header after-images, N to N+1 once;
10. explicitly stage lower-tier D25 rows, returning staged prepared authority
    without discovery, lock acquisition, or plan expansion;
11. execute D24 15d through 15h, finalize D25 15i through 15k, write exact
    tier-16 status deltas and later rows, then force deferred validation.

No reservation, prepared value, or stage result may be public, persisted,
serialized, attached to a cursor, stored in a registry, reconstructed, or
reused across cursor/connection/backend/transaction/context/phase boundaries.

### 3.3 Replay

The unchanged apply_matching_transition signature remains the general replay
entrypoint. It must fail closed before write if the named contribution is
absent. There is no conforming production monolithic first-application call.

Replay constructs the unchanged intent type as the canonical retained
changed-key projection decoded only from validated patch preimages. It does
not regenerate nonpersisted no-op lock-plan keys. It validates the exact
source-specific closure, checked read envelope, retained image, 15i artifact,
both-key 15j contribution, historical work, and current 15k accumulator in
frozen order. Structural replay locks its source/predecessor prefix at tier 5
before the tier-6 read envelope. Requirement replay uses that tier-6 envelope,
then locks and validates the terminal semantic job at tier 9 and the completed
attempt/result/output/execution/verifier closure at tier 10 before image
locks. Direct replay locks the retained images before its tier-15c immutable
source. The checked envelope validates the current legal state but does not
require the current header revision to equal historical R.

An unlocked source-key contribution read may supply only a candidate
patch-digest hint. Authoritative replay locks the 15i artifact before the 15j
contribution and then the 15k accumulator. For an older transition R, the
receipt retains its historical patch/contribution/resulting revision R and
returns the current cumulative accumulator whose updated revision equals the
retained matching-image semantic revision and is at least R. Replay creates no
transition context/journal and forces no transition validator. No aggregate
scan or prefix reconstruction is allowed.

## 4. Global exclusions

No lane may:

- edit migrations 013 through 017, their ledgers, schema, functions, triggers,
  constraints, privileges, or indexes;
- change a public API, persistence protocol, DTO, enum, digest, source kind,
  changed-state kind, present/absence recipe, result shape, or runtime mode;
- introduce a seventh reference kind, tombstone table, nullable hash,
  repr/JSON hashing, or a requirement-state counter alias;
- accept caller-authored keys, plan, patch, output, certificate, reference,
  artifact, work, status delta, count, or header image;
- add M5IncrementalOverlay, a full oracle, full repository hydration, retained
  engine/cache, aggregate job/contribution scans, hidden transaction state, or
  model/provider/retrieval calls inside a transaction;
- add nested transactions, internal commit/rollback, a second timing anchor,
  or measured work outside the frozen counters;
- expand dashboard, HTTP API, CLI, provider, model, migration, or deployment
  scope;
- transition any real database from v1_only to m5_active; or
- claim Task-2/M5.4 completion, deployment, latency, performance, utility,
  model/AI quality, objective truth, security, novelty, or superiority.

Runtime remains v1_only outside isolated disposable fixtures. D24 through
D28 implementation, Task 2, M5.4-05 through M5.4-09, M5.5, M5.6, deployment,
and AI-quality claims remain PENDING until separate final reconciliation.

## 5. Frozen read-only paths

Every lane may read but must not edit:

    AGENTS.md
    docs/m5_design_freeze.md
    docs/m5_acceptance_matrix.md
    docs/m5_implementation_plan.md
    docs/m5_implementation_status.md
    docs/m5_multiagent_execution_plan.md
    docs/decision_log.md
    docs/roadmap.md
    docs/workstreams/m5_runtime_contract/CANDIDATE_RUNTIME_ADDENDUM.md
    docs/workstreams/m5_runtime_contract/RECOVERY_WORK_AMENDMENT.md
    docs/workstreams/m5_runtime_contract/EXECUTION_DISPOSITION_RECEIPT_CORRECTION.md
    docs/workstreams/m5_runtime_contract/LEGACY_TERMINAL_COVERAGE_CORRECTION.md
    docs/workstreams/m5_runtime_contract/CANCELLATION_EXPIRED_OUTPUT_CORRECTION.md
    docs/workstreams/m5_runtime_contract/TERMINAL_SUCCESSOR_EXPIRED_OUTPUT_CORRECTION.md
    docs/workstreams/m5_runtime_contract/TERMINAL_RACE_INVOCATION_WORK_CORRECTION.md
    docs/workstreams/m5_runtime_contract/ACTIVE_TERMINAL_CUTOFF_INVOCATION_WORK_COMPLETION_CORRECTION.md
    docs/workstreams/m5_runtime_contract/DIRECT_ACQUISITION_TERMINAL_CUTOFF_CORRECTION.md
    docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT.md
    docs/workstreams/m5_runtime_contract/CHANGED_STATE_ABSENCE_AMENDMENT.md
    docs/workstreams/m5_runtime_contract/REQUIREMENT_STATE_COUNTER_ERRATUM.md
    docs/workstreams/m5_runtime_contract/PHASED_PERSISTED_MATCHING_COMPOSITION_AMENDMENT.md
    docs/workstreams/m5_runtime_implementation/D28_CONTRACT_FREEZE_HANDOFF.md
    migrations/013_m4_evaluation_overlay.sql
    migrations/014_m5_evidence_groups.sql
    migrations/015_m5_runtime.sql
    migrations/016_m5_runtime_recovery.sql
    migrations/017_m5_persisted_matching.sql
    src/groundloop/m5/runtime/application.py
    src/groundloop/m5/runtime/contracts.py
    src/groundloop/m5/runtime/digests.py
    src/groundloop/m5/incremental_overlay.py
    src/groundloop/postgres/m5.py
    src/groundloop/postgres/migrations.py
    tests/m5/postgres/test_static_contract.py
    tests/m5/postgres_runtime/test_migration_015.py
    tests/m5/postgres_runtime/test_migration_016.py
    tests/m5/postgres_runtime/test_migration_017.py

Existing Lane-B publication/audit source and tests remain read-only. Existing
Lane-A paths remain read-only except where Lane P explicitly reopens them.

## 6. Dependency barriers

Every lane begins only after the immediately preceding reviewed integration is
pushed. A lane may edit fewer granted paths but no unlisted path. Discovering
a necessary path is a hard stop for a one-file docs-only activation amendment,
two same-byte audits, integration, and push.

    audited D28 activation
      -> Lane P
      -> Lane C1
      -> Lane C2
      -> Lane M
      -> Lane C3
      -> Lane D
      -> combined serial regression and two final same-byte audits

No implementation lane develops ahead of its barrier. This keeps every
private cross-module interface reviewable on the exact integrated predecessor
and prevents a historical branch from supplying hidden protocol authority.

## 7. Path-exclusive lanes

### 7.1 Lane P — nonempty planner, phases, and replay

    branch = workstream/m5-d28-matching-planner
    worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d28-matching-planner
    base = exact pushed D28 activation commit

Exact owned paths:

1. src/groundloop/m5/runtime/postgres_matching.py;
2. tests/m5/postgres_runtime/d25_store_core/conftest.py;
3. tests/m5/postgres_runtime/d25_store_core/test_points.py;
4. tests/m5/postgres_runtime/d25_store_core/test_transition_apply.py;
5. tests/m5/postgres_runtime/d25_store_core/test_replay_work.py;
6. tests/m5/postgres_runtime/d25_store_core/test_nonempty_planner.py (new);
7. tests/m5/postgres_runtime/d25_store_core/test_d27_counter_ownership.py
   (new);
8. tests/m5/postgres_runtime/d25_store_core/test_d28_phased_composition.py
   (new); and
9. docs/workstreams/m5_runtime_implementation/D28_MATCHING_PLANNER_HANDOFF.md
   (new).

Lane P implements store-owned nonempty intent derivation and private
cursor-local values/functions for:

- source-present prepare from one official derive call;
- direct complete pre-source reservation;
- direct reservation plus lexical M4-stage-evidence completion;
- post-header D25 stage;
- post-D24 D25 finalization;
- exact retained-projection replay; and
- shared retained-byte source/artifact/contribution/accumulator validation.

Prepare derives the complete patch, plan, work, five D24-owned counts,
requirement diagnostic, both complete header images, and prewrite semantic
revision. Stage writes only prelocked tier-11a-through-14 D25 rows. Finalize
writes only 15i through 15k and checks all journals/counts/bytes. Direct
completion performs zero D25 DML and returns official intent plus explicit
ready prepared authority. All private values are cursor/backend/xid/context
and phase bound and single-use.

Lane P converts apply_matching_transition into exact retained replay when
contribution authority exists and fail-closed no-write behavior otherwise.
It does not retain the old empty structural first-application fixture as
production evidence. It may keep a clearly labelled foundation regression
only if the production-phase tests independently prove the D28 order.

Policy change, rootless requirement observation, and standalone claim
observation remain rejected.

### 7.2 Lane C1 — activation, structural open, recovery, and seal

    branch = workstream/m5-d28-structural-composition
    worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d28-structural-composition
    base = exact pushed Lane-P integration

Exact owned paths:

1. src/groundloop/m5/runtime/persistence.py;
2. src/groundloop/m5/runtime/postgres_recovery.py;
3. tests/m5/postgres_runtime/d25_application/conftest.py (new);
4. tests/m5/postgres_runtime/d25_application/test_store_composition.py (new);
5. tests/m5/postgres_runtime/d25_application/test_structural_order.py (new);
6. tests/m5/postgres_runtime/d25_application/test_seal_atomicity.py (new);
7. tests/m5/postgres_runtime/d25_application/test_store_races.py (new); and
8. docs/workstreams/m5_runtime_implementation/D28_STRUCTURAL_COMPOSITION_HANDOFF.md
   (new).

Lane C1 installs the accepted Lane-B activation projection in the existing
activation transaction and composes group/document structural opens through
Lane P. Structural open proves tier-5 source, revision-1 base/runtime rows,
one (1,1) authorizer call, no source reacquisition, no second header advance,
explicit prepare/stage, existing 15a-through-15h outer work, D25 finalization,
exact tier-16 deltas, and forced validation.

It implements the unchanged public seal protocol and a package-private
transaction-local direct-seal callback for later lanes without changing any
public protocol:

    request_typed_seal_atomically(epoch_id, expected_revision, event, call_work)

Seal performs no D25 semantic transition and no second timing anchor. Replay
uses stored result/children with zero writes/regeneration. Failure retains
working/history state and advances no head.

### 7.3 Lane C2 — active requirement-completion composition

    branch = workstream/m5-d28-requirement-composition
    worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d28-requirement-composition
    base = exact pushed Lane-C1 integration

Exact owned paths:

1. src/groundloop/m5/runtime/postgres_verifier.py;
2. tests/m5/postgres_runtime/d25_application/test_requirement_completion.py
   (new);
3. tests/m5/postgres_runtime/d25_application/test_requirement_order.py (new);
4. tests/m5/postgres_runtime/d25_application/test_requirement_races.py (new);
   and
5. docs/workstreams/m5_runtime_implementation/D28_REQUIREMENT_COMPOSITION_HANDOFF.md
   (new).

Lane C2 replaces only the intentional active-completion D25 hard stop. It
invokes tier-6 authorization with only the proposed attempt ID, keeps
job/attempt preterminal while the exact tier-10 source closure is persisted
and locked, proves the official attempt ID, derives once, prepares the
complete plan before either header update, installs base then runtime complete
after-images, stages tier-11a observation/currency then the rest of D25 lower
tiers, terminalizes the already-locked job/attempt, executes 15a/15b/applicable
15c plus D24 15d-through-15h, finalizes 15i-through-15k, writes exact tier-16
deltas, and forces validation.

Inactive, expired, cancelled, failed, loser, and audit-only paths remain
D25-inert. Replay validates the terminal job/attempt/source closure and both
same-edge and different-edge race orders.

### 7.4 Lane M — store-derived direct-M4 private bridge

    branch = workstream/m5-d28-m4-store-derived-direct
    worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d28-m4-store-derived-direct
    base = exact pushed Lane-C2 integration

Exact owned paths:

1. src/groundloop/m4/pipeline.py;
2. tests/m4/integration/test_m5_typed_store_derived_direct.py (new);
3. tests/m4/integration/test_m5_d28_direct_stage_evidence.py (new); and
4. docs/workstreams/m5_runtime_implementation/D28_M4_DIRECT_STORE_DERIVED_HANDOFF.md
   (new).

Lane M adds only a package-private typed-store-derived construction/stage
route. The default PostgresM4ApplicationPorts constructor, public M4-v1
entrypoints, payloads, receipts, projections, digests, and replay remain
byte-compatible.

The private route uses the explicit Lane-P reservation, performs no startup or
working repository hydration, deep copy, retained engine/cache, full oracle,
or aggregate scan, and returns an explicit lexical stage result. Its evidence
includes all lower-tier/job mutations, 15a and 15b, and the evaluation-epoch,
every ordered override-counter, and immutable transition 15c families. It may
not mutate the reservation or hide the result on a cursor/registry.

If existing indexes cannot support required point/affected-key reads, or if
the route requires any public signature/export/schema/index/receipt/digest
change, Lane M stops for a new contract/activation decision.

### 7.5 Lane C3 — typed-direct D28 composition

    branch = workstream/m5-d28-direct-composition
    worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d28-direct-composition
    base = exact pushed Lane-M integration

Exact owned paths:

1. src/groundloop/m5/runtime/postgres_direct_recovery.py;
2. src/groundloop/m5/runtime/direct_m4.py;
3. tests/m5/postgres_runtime/d25_application/test_direct_transition.py (new);
4. tests/m5/postgres_runtime/d25_application/test_direct_order.py (new);
5. tests/m5/postgres_runtime/d25_application/test_direct_races.py (new);
6. tests/m5/postgres_runtime/d25_application/test_direct_store_reconnect.py
   (new); and
7. docs/workstreams/m5_runtime_implementation/D28_DIRECT_COMPOSITION_HANDOFF.md
   (new).

Lane C3 orchestrates the exact Section-3.2 sequence. Direct expansion produces
its exact logical-only D25 patch; direct verifier completion produces its
exact physical/logical transition. It passes reservation and stage result
lexically, completes official source-derived intent before headers, installs
complete base then runtime after-images once, stages D25 after headers,
executes D24 accounting, finalizes D25, writes exact status deltas/later rows,
and forces validation without acquiring an earlier lock.

Late, inactive, replay, serialized-loser, and combined-failure paths remain
D25-inert. D24-C7 acquisition/cutoff and timing authority remain exact. C3
uses C1's private direct-seal callback and does not reopen persistence.py.

### 7.6 Lane D — public facades, reconnect, and post-seal audit

    branch = workstream/m5-d28-public-composition
    worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d28-public-composition
    base = exact pushed Lane-C3 integration

Exact owned paths:

1. src/groundloop/m5/runtime/postgres_application.py;
2. src/groundloop/m5/runtime/postgres_direct_application.py;
3. src/groundloop/m5/runtime/postgres_matching_postseal.py (new);
4. tests/m5/postgres_runtime/d25_application/test_group_public_composition.py
   (new);
5. tests/m5/postgres_runtime/d25_application/test_direct_public_composition.py
   (new);
6. tests/m5/postgres_runtime/d25_application/test_reconnect.py (new);
7. tests/m5/postgres_runtime/d25_application/test_postseal_audit.py (new); and
8. docs/workstreams/m5_runtime_implementation/D28_PUBLIC_COMPOSITION_HANDOFF.md
   (new).

Lane D adds production-capable PostgreSQL facades while preserving both
historical PreSealPorts classes as fail-closed regressions. It builds the
unchanged M5TypedApplication; application.py and runtime package exports remain
read-only. A fresh nonterminal process reuses committed D24/D25/M4 work and
performs only missing external work. Terminal replay performs zero calls and
zero writes.

The post-seal adapter owns snapshot export/import and the final
publication-head recheck. Independent Python and SQL readers use only base
semantic relations; accepted Lane-B physical/provenance audit reads D25.
Audit executes after commit and outside measured seal latency. No dashboard,
HTTP API, CLI, deployment, or runtime-mode change is part of Lane D.

## 8. Lane acceptance and integration

Every lane handoff must record exact base/commit/tree, name-status subset,
blob SHA-256 values, clean state, commands, tool versions, selected/passed/
failed/skipped/deselected counts, duration, PostgreSQL inventories, falsifier
mapping, race orders, crash cuts, reconnect behavior, findings, fixes,
nonclaims, and remaining acceptance rows.

Before each lane integration, two independent reviewers audit identical
committed bytes:

1. semantic/digest/counter/oracle/M4-v1 compatibility; and
2. PostgreSQL lock/order/race/replay/schema/environment correctness.

Both must return GO with P0=0 and P1=0. Any byte change restarts both audits.
The coordinator integrates the exact reviewed commit without cherry-picking or
rebasing, reruns applicable gates on integrated ancestry, verifies protected
local-main hashes, and pushes the accepted barrier before creating the next
lane worktree. No lane self-accepts.

## 9. Mandatory evidence

The combined tranche must map exact tests to all applicable:

- 15 D28 falsifier groups, including frozen bytes, source-present order,
  direct 15c order, representative discovery, complete-plan equality,
  no-reservation authority, transaction binding, stage/finalize separation,
  counter ownership, accumulator position, crash matrix, historical replay,
  races, private-versus-SQL rejection, and no hidden expansion;
- 8 D27 counter-ownership falsifiers;
- 18 D26 absence-reference falsifiers for supported replace/retire histories;
- 40 D25 persisted-matching falsifiers, with the three unsupported forms
  explicitly PENDING rather than silently counted;
- every applicable D24-C1-through-C7 recovery/accounting/cutoff case;
- migration-013 frozen-byte/ledger inventory plus migration-014-through-017
  fresh/rerun/conflict/rollback/ledger regressions on byte-identical files;
- crash cuts after authorization/reservation, each lower-tier plan and write,
  both header updates, job/attempt closure, 15a, 15b, each 15c family,
  15d-through-15h, 15i/15j/15k, tier-16 deltas/later rows, and forced
  validation;
- direct reservation and stage evidence rejection for every missing/extra/
  reordered/changed plan, before image, source, phase, or 15c family;
- historical replay after two or more transitions, failure, seal, reconnect,
  and later-head advance, including a nonpersisted no-op original key;
- both race orders for same-edge/different-edge completion, replay, seal,
  failure, and reconnect;
- static/runtime rejection of registries, full-oracle/hydration/cache paths,
  aggregate scans, and in-transaction provider/model calls;
- EXPLAIN evidence for point/affected-key reads using existing indexes;
- end-to-end group and typed-document histories through seal, fresh-process
  reconnect, and exact terminal replay;
- independent Python, SQL, and physical/provenance audit after each measured
  successful seal; and
- M4-v1 never-activated and public-route byte/receipt/projection compatibility.

No skipped, xfailed, unavailable, environment-failed, timeout-without-
diagnosis, no-match, or silently deselected mandatory case is a pass.

## 10. Environment qualification

Use repository source and isolated caches:

    MYPYPATH=src
    PYTHONPATH=src:.
    PYTHONDONTWRITEBYTECODE=1

PostgreSQL suites run serially through an isolated runner sharing the database
container network. Currently qualified PostgreSQL 16.14 evidence requires:

    PGOPTIONS='-c jit=off'

Record exact pre/post database, non-temporary-schema, client, and test-role
inventories for every tranche. The default-JIT SIGSEGV is unresolved and is
not a product pass. Host-published-port cascades and overlapping schema
fixtures are invalid evidence and cannot be pooled.

## 11. Acceptance ceiling

Task 2 is accepted only after the complete supported vertical slice and final
combined regression matrix receive two independent same-byte GO reviews with
P0=0 and P1=0. A green P, C1, C2, M, C3, or D lane is scoped evidence only.

Even after technical Task-2 acceptance, runtime remains v1_only outside
isolated fixtures. D24-through-D28 implementation rows and M5.4-05, M5.4-06,
and M5.4-09 may change only in a separate status-reconciliation tranche.
M5.4-07, M5.4-08, M5.5, M5.6, deployment, latency, utility, and model/AI
quality remain PENDING.

## 12. Activation audit and protected local main

Before any implementation lane exists:

1. this file is the only changed path and its commit has sole parent
   003b45d18be0f1221179c6689cbd65f03f082c50;
2. exact parent/tree, authority hashes, migration hashes, collision inventory,
   diff check, status/nonclaim boundaries, and protected dirt checks pass;
3. one authority/ownership reviewer and one D24-through-D28/PostgreSQL
   reviewer audit identical commit/tree bytes;
4. both return GO with P0=0 and P1=0 and confirm that no P/C1/C2/M/C3/D branch
   or worktree existed during review;
5. any byte change restarts both reviews; and
6. origin/main is fast-forwarded to the exact reviewed activation commit
   before Lane P is created; no unaudited merge commit may intervene.

The dirty local-main checkout is user-owned and excluded:

    2af4b19962dc8a7d22e377be17f342530a06ee6395bbbf2a092eab36599c8fc2  pyproject.toml
    45c20ca46e9ad5bcd86b22c0d8882d1d611497f57ca8c45d3dea149260c110cd  docs/presentations/groundloop_fyp_professor_feedback.pdf
    59a13cd8d4bbb017e712c0f39e70f2eba136557e945f64f1b1fc3891742a79f0  docs/presentations/groundloop_fyp_professor_feedback_v2.pdf
    c12929c349a5c0be9793159143b09da40ea2a0b27df37b92d61d9ed6483d8c2a  docs/presentations/render_groundloop_fyp_professor_deck.py
    167d1e7df5a720041fe0ff51879d08357f0dfbe3a7781ffaa08c0d958a47aa94  docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT_DRAFT.md

Never reset, clean, stash, reformat, stage, commit, copy, or delete those
paths. Any protected hash or porcelain drift stops activation or integration.

All pre-existing implementation worktrees are historical read-only evidence,
including the direct-M4, D24 direct/requirement, R2C, and R2E branches. No lane
may merge, rebase, cherry-pick, copy source from, or revive them.
