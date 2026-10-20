# M5-D36 Nonempty-REPLACE Direct-Counter Clock Amendment

Status: DOCS-ONLY CANDIDATE, not frozen or implementation authority. Two
independent COMPLETE audits of identical bytes must each return GO/P0=0/P1=0
before separately authorized authority-freeze updates and source activation.
No migration, source, test, privilege or default changes occur in this tranche.

Date: 2026-10-20 (Asia/Hong_Kong)

## 1. Evidence, scope and exact precedence

The accepted [DOC-F report](../m5_runtime_implementation/D33_NONEMPTY_DOCUMENT_FEASIBILITY.md)
is at commit906697b7ad37062f4d9ef35657b93c5e819e5e60,
tree3ac278e78fac2c9f6c9a8e92adb62114aa50171b, report SHA256
f3ffc967abc95783cc8d14f3ea9c1ce715f698c4601ebaef64aee849ac176057.
Both complete report audits and fresh postcommit checks passed before atomic
no-force branch/main publication. That report diagnoses a contract conflict,
not a runtime positive or authority to implement this candidate.

Runtime-addendum Sections14.1/17 require one shared revision advance for each
real typed state-changing transaction and the native M4 direct state logic.
M4's counter also requires each of its own transitions to advance once. Native
M5-only work advances the former but not the latter. D25's direct source and
D24's contribution validator currently equate the two. D33 Section2.3 permits
only job-free SEAL1->2; it expressly excludes direct-job history. Nonempty
REPLACE necessarily has both direct IMPACT and M5 reverse-discovery roots.
Neither execution order composes those unchanged constraints.

This candidate chooses SEPARATE CLOCKS, not synthetic synchronization. Its
scope is ONLY a newly opened, genuine, matching-aware `document_replace` with
nonempty inserted/deactivated chunk-version sets and inserted chunks, exact
D29 source/declarations and a Section3 origin created in that same OPEN.
It does not retrofit an existing epoch, adopt partial history, cover INSERT/
DELETE with direct history, or change group-only, observation or policy routes.
All other documents retain their existing eligibility and explicit limitations.

Only within this origin-bound scope, supersede:

1. Runtime-addendum Section17's same-SQL direct-adapter requirement to the
   extent it conflates shared runtime N with M4 counter C. Native semantics,
   canonical identities and all nonclock checks remain mandatory.
2. D28 Section5 / D25 direct-source coordinate equality and its Python stage
   image checks: M4 counter/override/transition coordinates are C, while patch,
   job, working-state, pending and contribution coordinates remain N.
3. D24's direct-acquisition canonical-zero rule ONLY for actual new clock-proof
   producer bytes, and retry-failure accounting ONLY as Section6 specifies.
4. D32/D33 readiness eligibility and D33's final no-history1->2 proof ONLY for
   the distinct origin-bound branch below. Existing branches remain exact.
5. D35's exclusion of the Python direct pre-source context reader ONLY to
   allow Section8's use of the existing022 bridge, not a third SQL accessor.
6. Negative schema/digest inventories ONLY for Section9's closed additive023
   delta, two typed clock-proof recipes and its administrative bundle identity.

Migrations000--022, public store/application/v1 DTOs and methods, and existing
digest recipes remain byte-identical. Only the named private/administrative
body exceptions in this candidate may later be allocated. No new M4 transition
kind, D25 source kind, D24 work/timing kind,
counter field, seventh reference kind, tombstone or nullable hash is introduced.
All frozen present-state, certificate, absence and outer reference recipes stay
exact. A new source activation is required even after eventual contract PASS.

## 2. Clock ownership and allowed edges

Let E be the event epoch, P its genuine sealed predecessor, N the shared
base/runtime revision, and C the M4 evaluation counter's own revision.

| Persisted surface | Revision authority |
| --- | --- |
| Base/runtime headers; direct/M5 dispatch and job completion; direct terminal projections; scopes and working grounding/currency; M5 owner/answer pending rows; D24 contribution/applied/timing anchors; D25 patch/context/contribution; published heads/states and final result | Shared N |
| M4 evaluation epoch counter; its override counter_updated_revision; immutable evaluation transition from_revision/to_revision and transition.expected_revision in its canonical payload | Own C |

OPEN establishes N=C=1 and active M4 counter confirmed at P. Thereafter:

- A genuine M4-counter operation advances (N,C)->(N+1,C+1), backed by exactly
  one real canonical M4 transition and one immutable clock binding.
- A genuine M5-only operation advances (N,C)->(N+1,C), with no M4 transition
  or binding. Its existing job/source, pending, contribution and CAS rules stay.
- Read-only/exact replay and audit-only late return do not advance either
  clock. Actual allowed audit work remains separately accounted, not free.
- Failure/seal advance the shared revision once and the native counter once;
  no other operations may be bundled to make several M4 transitions share one
  N edge. Targeted direct terminal failure plus combined epoch failure uses
  ONE canonical FAIL, not a terminal DELTA plus FAIL under one revision.

Both clocks are monotone; 1<=C<=N. N remains the application CAS/lease/result
coordinate. Callers never supply C as authority. A private typed adapter derives
C from locked persisted authority; it must not call a legacy wrapper at a fake
base revision. Legacy M4 `_acquire_direct_job_local`, completion/failure/seal
wrappers and their equal-clock assertions remain unchanged for their old routes.

## 3. Immutable OPEN origin and transition bindings

Two small permanent immutable relations make the selected clock interpretation
explicit and historically checkable. They are coordination proof, NOT semantic
tombstones, replacement observations, public result children or matching state.
All displayed fields are NOT NULL; hashes are lowercase 64-hex content hashes.

### 3.1 Origin

`groundloop_m5_document_clock_origin` has exactly these identity columns:

```text
epoch_id bigint
structural_event_id text
event_payload_hash char(64)
previous_published_epoch_id bigint
m4_declaration_hash char(64)
origin_digest char(64)
```

Primary key(epoch_id), unique(origin_digest); deferred FKs to the runtime epoch
and predecessor base epoch. P must be the actual existing sealed predecessor,
not a caller-provided current head substituted on replay. Its exact digest is:

```text
stable_m5_digest(
  "m5-document-direct-clock-origin-v1",
  INT(epoch_id), TEXT(structural_event_id), HASH(event_payload_hash),
  INT(previous_published_epoch_id), HASH(m4_declaration_hash)
)
```

Create it once during the genuine fresh OPEN, after real evaluation declaration
creation and before D24 accounting, under the original source/header locks.
Its BEFORE INSERT guard proves actual N=C=1, SC, pending/unsealed base, active
counter1/P, the exact M4 declaration hash, metadata-preserving nonempty REPLACE,
all D29 arrays/source/snapshots and matching-aware activation/schema authority.
It requires current-transaction epoch, M4 update, typed update and runtime
tuples, using the existing015 bridge's native `xmin = assigned_xact_id::xid`
freshness proof after confirming an already assigned transaction. No nested
transaction, timestamp/caller flag or mere N=1 substitutes for fresh insertion.
No prior M4 transition or clock binding may exist. Final deferred validation
also requires the genuine structural_open D24 and D25 closures at revision1.
It is forbidden to insert an origin later, repair an old source or change mode.

### 3.2 Binding

`groundloop_m5_document_clock_binding` has exactly these identity columns:

```text
epoch_id bigint
m4_transition_id text
m4_transition_payload_hash char(64)
shared_before_revision bigint
shared_after_revision bigint
counter_before_revision bigint
counter_after_revision bigint
contribution_kind text
contribution_source_id text
contribution_source_identity_hash char(64)
binding_digest char(64)
```

Primary key(epoch_id,m4_transition_id); SEPARATE uniques(epoch_id,
shared_after_revision), (epoch_id,counter_after_revision), and(binding_digest).
Both after values equal their own before+1; Cbefore>=1 and Cbefore<=Nbefore.
Deferred FKs use EXISTING keys, not new constraints on old relations:
epoch_id->origin; (epoch_id,m4_transition_id)->M4 evaluation transition PK;
(epoch_id,contribution_kind,contribution_source_id)->D24 contribution PK.
Guards compare the referenced hashes/coordinates in full. There is no FK to
a hash pretending that an old table already had a supporting unique key.

```text
stable_m5_digest(
  "m5-document-direct-clock-binding-v1",
  INT(epoch_id), TEXT(m4_transition_id), HASH(m4_transition_payload_hash),
  INT(shared_before_revision), INT(shared_after_revision),
  INT(counter_before_revision), INT(counter_after_revision),
  ENUM(contribution_kind), TEXT(contribution_source_id),
  HASH(contribution_source_identity_hash)
)
```

`contribution_kind` is exactly direct_acquisition, direct_transition,
epoch_failure or seal. Actual operation/source bijections are in Section4.
All fields come from real stored source/receipt values, not asserted hashes.
M4 transition IDs and payload hashes remain canonical M4 values; binding_digest
does not replace them or any existing source_identity_hash. Typed framing is
the existing stable_m5_digest expansion, never repr/JSON serialization.

### 3.3 Chain, reverse closure and bounded historical lookup

For a fixed epoch, C_at(r) is the counter_after_revision of the binding with
greatest shared_after_revision<=r, or1 from the validated origin if none exists.
The unique(epoch,shared_after_revision) btree supports a descending bound/limit1
lookup. This is bounded indexed work, not a claimed constant-time operation.
Never scan/sum the complete history in a measured transition or terminal seal.

Each binding proves its Cbefore equals C_at(Nbefore). If Cbefore>1, point-read
the unique predecessor binding with counter_after=Cbefore, whose shared_after
must be <=Nbefore. The first transition starts at counter1. No skipped counter,
reused source, overlapping shared edge, or binding at a future N is legal.

NEW deferred reverse guards cover every M4 evaluation epoch-counter
INSERT/UPDATE, evaluation-override INSERT/UPDATE/DELETE, evaluation-transition
INSERT, typed-runtime INSERT/UPDATE and origin/binding INSERT for an origin
epoch. They
require a bijection between real counter advances, real immutable transitions
and bindings at that shared edge, exact current counter=C_at(resulting N),
and the correct zero-binding M5-only/readiness versus one-binding real direct/
failure/seal branch. They also prove actual operation/source closure. A binding
FK alone is insufficient: raw counter advance, unbound transition, fake empty
DELTA, manually relabelled M5 operation or late counter catch-up must reject.
Override-only DML must itself fire a guard even if no epoch counter or runtime
row changes. Require the real binding/transition to have been inserted in the
SAME assigned outer transaction, not a referenced historical binding at today's
N. Reconstruct the canonical operation's affected claim/required-answer deltas
and compare full OLD/NEW rows and physical effects: a changed old override has
counter_updated_revision<=Cbefore; its nonzero successor has exactly Cafter;
DELETE is legal only for the actual zero-result delta. Unaffected keys, duplicate
or compensating writes, false counts, and override changes during an empty
retry DELTA, SEAL or native FAIL must reject. Native FAIL retains historical
overrides; it does not manufacture their deletion. Complete source/after-image
proof remains required, not just C<=N or a foreign-key join.
Initial counter/runtime INSERT is the sole origin1/1 case, not an M4 transition.
No UPDATE/DELETE of either new relation is legal. Missing/mismatched proof
rolls back the entire source, state, accounting and header transaction.

## 4. Real operations, canonical sources and lock/materialization order

These are private typed compositions, not public-v1 API changes. Reuse the
native operation's actual state, payload, dense attempt, lease, source and
evaluation semantics while passing N to its runtime pieces and C to the
unchanged canonical EvaluationTransition/apply_transition_local. Do not call
the old combined wrapper with different arguments to bypass its base check.

| Operation | Real M4 transition | Binding's existing D24 source |
| --- | --- | --- |
| New acquisition or takeover | Existing attempt/successor attempt ID, native DELTA at C; exact scope/claim effects, including first frontier pending unit | direct_acquisition, source_id=real dispatch-record digest, source hash=same digest; join dispatch->attempt->M4 transition, NEVER equate dispatch digest with attempt ID |
| Active/inactive direct discovery or verifier settlement | Existing completion_digest, native DELTA at C and exact actual job/attempt/artifact/execution/eligibility/child closure; terminal projection completed_revision=N+1 | direct_transition, source_id=completion_digest, source hash=actual M4 transition payload hash |
| Genuine retryable failure | Existing stable_m4_digest("m4-evaluation-retryable-failure-v1", E,job,attempt), native empty DELTA at C plus real leased-attempt failure | Section6 byte-only direct_transition companion keyed by that real transition ID/hash |
| Combined epoch failure, including targeted terminal job failure | Existing canonical m4-evaluation-failure-v1 ID and FAIL payload at C; exact current failure plan and cancellation/terminal projections | Existing epoch_failure source ID/hash, unchanged recipe and real reason/closure |
| Strict seal | Existing canonical m4-evaluation-seal-v1 ID and SEAL payload at C | Existing seal structural-event source ID and m5-seal-contribution-source-v1 hash of final delta/reference sets |

No new standalone direct terminal-failure operation is admitted. Existing
unsupported terminal settlement remains rejected; the checked combined FAIL
path handles it. A separately committed real settlement and subsequent failure
would each need its own legal N edge and binding, not several transitions under
one N. Audit-only inactive/expired/cancelled/terminal returns keep their accepted
dispositions; they create no normal-transition binding unless an actual native
counter-changing operation is independently eligible. Exact replay is not one.

Keep the D29/D30 OPEN sequence: metadata/source, tier7, one prepare, tier8
requirement-registry/chunk snapshots, one continuation, reserved scopes before
jobs, D25 families12/13/14 and frozen accounting/finalization. Origin's new row
is inserted under its reserved epoch identity with no old-row/history adoption.
This candidate supplies no missing nonempty M4 phase producer or typed baseline.

For later direct operations, D28's joint pre-source reservation still takes all
M4/D25 keys through11a--14 before15a/b and the actual M4 evaluation family15c.
Derive C_at(N) by immutable indexed point/seek under epoch serialization, then
compare it to the real counter when locked at15c. A locator is not authority;
do not move the counter lock to tier6, retry from a changed C inside the same
proposal, or acquire later D25 locks after a monolithic M4 stage.

Reserve clock-origin/binding coordinates at15c in deterministic epoch/source
order after the actual M4 evaluation keys and before D24 ledger families.
Insert each binding only when its COMPLETE real source/hash exists, under
those held reservations, before the one work/timing accumulator CAS. Seal's
source hash uses final combined-delta/reference hashes, not the M4 SEAL hash:
preserve the original genuine half-terminal publication stage FIRST: base
sealed at N+1 and both heads at E/N+1, typed runtime still SCOMP/N and work/timing
at N, all within the ONE uncommitted outer transaction. Only then use the trusted
preterminal envelope/child reader on actual promoted rows and exact predecessor
proof, produce the final child hashes, and materialize the binding/contribution
under the already held reservations before the sole accounting CAS and final
typed-runtime/result advance. No caller-prehashed child set, fixture authority,
placeholder/null hash, backward lock, second accounting pass or EXTRA/speculative
head advance is allowed. Do not move the trusted reader before its genuine
half-terminal prerequisite. D24 source recipes depend on final children,
not event_work; clock hashes exclude work_digest, so actual producer-byte
accounting is acyclic. If the actual producer cannot honor this order, stop
before implementation rather than moving source or ledger authority silently.

D34 archival-before-derivation and no source-observation relock stay exact.
An active direct transition entering the combined overlay still requires the
official once-derived D25 intent, exact before/after images and staged source
evidence; its patch/context revisions are N/N+1 while counter and override
image revisions are C/C+1. Normal inactive/coordination-only transitions have
no D25 patch or matching-accumulator advance. Native M4 canonical payloads use
C without a new serialization/hash recipe; completed job/currency/state rows
retain their shared N coordinates. Pure replay validates the retained binding
and original C/N pair, not today's counter or a recomputed transition at new C.

## 5. Mixed readiness, strict seal and failure

New readiness eligibility is origin-bound nonempty REPLACE only. Validate the
entire genuine document source, D29 declarations, policies/metadata and both
subgraphs, not zero M5 counts alone. Direct evaluation must be active at C_at(N),
confirmed at P, with zero open discovery scopes and overrides; every direct
job/scope must have its genuine terminal/child/projection closure and no
blocking terminal failure. Independently require all three M5 counters zero,
all owner/answer pending values zero, and complete durable M5 root/job/scope/
failure/cancellation proofs. All existing source-corruption negatives remain.

If direct work finishes last, its lawful outer combined-state projector may
enter SCOMP at N+1 ONLY when that complete combined proof holds. If M5 work
finishes last, C2 still commits SP; a NEW separately committed SP->SCOMP
readiness transaction advances N once and leaves C unchanged. It reuses D32's
semantic_readiness kind, source_id=semantic_complete, unchanged typed source/
key/work/timing recipes and contribution-BEFORE-header predecessor guard.
No readiness-start or SC->SCOMP shortcut is added for nonempty REPLACE.

Lock base then runtime. Validate earlier-tier closure under epoch serialization
at its lawful source/lock cuts, before owner15a/answer15b, evaluation15c and
accounting. Do not acquire a lower-tier job/scope lock after pending/evaluation
locks; immutable point checks do not authorize a backward lock or an early
evaluation-counter lock. Retain all actual pending/accounting family order.
Exact resulting-state validation proves C_at(N)=C_at(N+1)=actual C and zero new
M4 transitions/bindings on this readiness edge. Readiness creates no origin or
clock binding. Its only producer preimages remain the unchanged source and
contribution key: bytes_hashed=bytes_serialized=EXACTLY S+K, every other counter0,
independently derived by Python and SQL as D32 Section4 requires. Clock indexed
reads, counter checks and stored-proof revalidation/rehashing are validation,
not extra producer-byte work. Their actual elapsed time is still measured.
No unspecified additional-byte allowance or invented canonical preimage exists;
if implementation needs a new producer region here, stop for separate authority.
Owner/answer revision bindings advance to N+1 without changing zero values.

Strict seal requires separately committed SCOMP, full direct/M5 closure and
genuine adopted measured M4 working-cache/publication proof at N. Its private
adapter retains every nonclock M4 check and actual structure/currency/grounding
promotion, but proves active counter C_at(N)/P rather than counter=N. It creates
the ONE genuine canonical SEAL at C->C+1 and its binding to shared N->N+1,
counter sealed/complete confirmed at E, scopes/overrides zero. Public state,
both heads, final six-kind children/result and D24 work/timing remain at E,N+1.
The binding's seal source must exactly match the final delta/reference hashes.
Existing J33 remains ONLY job-free1->2; do not route this history through it.

Failure similarly uses the ONE native canonical FAIL at C->C+1, with genuine
locked failure/cancellation plan, shared FAILED at N+1, unchanged failure-result
and timing/accounting rules, and no publication-head advance. Preserve native
failed-counter audit values; do not zero historical counts merely to seal.
Terminal-accounting closure is forced last; pending timing is never cleared
early. Normal observations/executions were archived in earlier completion
transactions: seal/failure does not create, repair or re-archive them.

Both execution orders must agree with full relational recomputation on actual
semantic effects and combined pending/status, including shared claims/answers.
Their real revision/receipt histories may differ; do not force equal result
hashes by erasing a genuine readiness transaction. No transient direct-complete
state may commit while M5 remains pending. No full oracle runs inside measured
transactions. Combined aggregation, certificate-only changes and actual binding
close/open rowcounts remain C1/P33 obligations, not supplied by this amendment.

## 6. Exact work/timing ownership; no accounting workaround

No new M5RuntimeWork field or work/timing contribution kind is added. New
origin/binding rows have diagnostic actual rowcounts, not an alias to semantic
state-write or certificate-binding counters. Their actual canonical producer
hash/serialization bytes and measured time must be recorded once:

- Origin: structural_open.
- New acquisition/takeover binding: direct_acquisition. ONLY for a genuine
  origin/binding, relax both allowed-ordinal and explicit zero-digest checks to
  permit bytes_hashed/bytes_serialized (26/27) actually produced by that clock
  proof. All other counters remain0 and dispatch identity/key stay exact.
  M5 acquisitions and every non-D36 direct acquisition remain canonical-zero.
- Normal direct completion binding: existing direct_transition.
- Retry failure binding: create one byte-only direct_transition companion
  keyed by its REAL retry DELTA ID/hash, at the same N+1 as the unchanged
  direct_attempt_execution contribution. This is not a synthetic M4 DELTA,
  D25 patch, second model attempt or second timing anchor.
- Seal/failure binding: respective existing seal/epoch_failure contribution.

Retry companion work must not enter attempt_work, evidence_digest or the
model-attempt timing observation. Preserve those exact attempt-owned inputs.
A D36-private receipt/outcome must carry and validate BOTH actual contribution
identities/work, original N/C binding and total call_work. Do not return the old
None-only `_DirectRetryableFailureOutcome` as if it proved the companion.
Its private downstream checks/accumulation/replay must be explicitly allocated
later; this does not change public v1 JobLease or existing wire DTOs.

All contemporaneous contributions are folded into ONE existing work CAS on
the exact sum, with the ONE original operation timing anchor; byte-only
companions add no expected timing point. Existing timing attempt aggregation
uses only actual attempt-execution evidence. event_work=prior+call, including
every new producer byte, and terminal CAS/pending-timing race rules stay exact.
Validation/replay is not new producer work. D27 requirement-write ownership,
D25's37 matching coordinates and every other D24 counter owner remain exact.
No figure in this candidate is a measured cost or latency result.

## 7. Retained replay and historical coordinates

Check native exact terminal/replay identity before fresh source authorization.
Retained binding/source/contribution hashes, canonical M4 payload at original
C, actual N job/projection/dispatch coordinates and work are immutable authority.
A later current C cannot be substituted into the original transition payload.
Acquisition replay retains its original dispatch/binding byte work without
producing new bytes or dispatching a model; retry replay returns both original
contributions with canonical-zero new call work and no duplicate anchor.

Readiness replay uses original applied_revision-1, its unchanged recipe and
C_at(original coordinate); after terminalization additionally prove the exact
seal/failure binding and counter lifecycle. Final reconnect returns original
stored result, work, timing, delta/reference children and E/N coordinates.
No event reconstruction from today's heads, no sum scan or repair insertion.
After legitimate later L>E restoration, D26/D33 as-of-E absence and closed
predecessor intervals remain authoritative; a later binding at L is legal.
Every exact replay is zero semantic/clock/accumulator/publication writes and
zero model calls. A conflicting source/work/timing/hash/coordinate rejects.

## 8. Supported role/context boundary

Owner OPEN only; this candidate grants no nonowner OPEN. Later execution,
readiness, seal and reconnect require owner and distinct separately connected
LOGIN qualification under explicit ordinary permanent-runtime grants.

For D36 direct pre-source composition, explicitly allow adapting ONLY the
Python `_capture_direct_context_binding` reader and its identity-preserving
validation glue to use the unchanged022
groundloop_m5_matching_read_transition_context bridge. It already admits
direct_transition at shared N/N+1 with exact session/XID/OID/GUC and pristine
context proof. D35's earlier exclusion of that Python repair is superseded ONLY
here. Keep actual expected/result N, direct source ID and all binding comparisons.
Do not read private pg_temp directly as a distinct role, change its ACL, create
a third SQL accessor or broaden the existing context/journal projections.

New permanent tables default to owner-only; no PUBLIC table privileges.
Future admin activation must explicitly name each supported existing runtime
role and may grant SELECT/INSERT on ONLY these two new relations. No UPDATE,
DELETE, TRUNCATE, REFERENCES, role creation or private-temp/helper grant is
inferred. Their guarded trigger functions have no PUBLIC direct EXECUTE grant;
ordinary trigger invocation remains authoritative. Existing permanent access
and019/022 bridges are not expanded. Record exact role/grant projections in the
future qualification manifest; missing access fails closed, not a superuser
fallback labelled distinct-LOGIN success. This is not a security claim.

## 9. Sole future additive migration023 authority

Only AFTER separate freeze/activation, one opt-in023 may add exactly:

1. The TWO permanent relations in Section3, their displayed NOT NULL/type/
   canonical-text/hash/edge/enum CHECKs, named PKs/uniques/deferred FKs and
   supporting indexes. No new column/index/unique on an old table is authorized.
   The epoch/shared-after unique index supplies the historical descending seek;
   the epoch/counter-after unique index supplies the predecessor point lookup.
2. TWO new trigger functions:
   `groundloop_m5_validate_document_clock_origin()` and
   `groundloop_m5_validate_document_clock_binding()`. BOTH have no SQL arguments,
   RETURNS trigger, LANGUAGE plpgsql, SECURITY INVOKER, VOLATILE, CALLED ON NULL
   INPUT, PARALLEL UNSAFE and NOT LEAKPROOF, owned by the existing trusted
   installation/schema-bundle owner. Their sole proconfig is the captured,
   safely quoted trusted installation schema followed by pg_catalog search_path;
   no $user or explicit pg_temp. Schema-qualify permanent sensitive objects and
   functions and validate exact TG_RELID/TG_TABLE_SCHEMA, caller/transition proof.
   They must not trust GUCs alone or temporary lookalikes. Revoke ALL direct
   EXECUTE from PUBLIC and all nonowner roles; trigger invocation adds no
   accessor privilege. Pin complete flags/owner/config/ACL in the closed catalog.
3. Exactly these NINE NEW triggers, without replacing old trigger definitions:

| New trigger identity | Exact permanent target and event |
| --- | --- |
| groundloop_m5_document_clock_origin_guard | groundloop_m5_document_clock_origin BEFORE INSERT; origin validator |
| groundloop_m5_document_clock_origin_immutable | groundloop_m5_document_clock_origin BEFORE UPDATE/DELETE; existing immutable-row rejector |
| groundloop_m5_document_clock_origin_final | groundloop_m5_document_clock_origin AFTER INSERT deferred constraint; binding validator's origin branch |
| groundloop_m5_document_clock_binding_guard | groundloop_m5_document_clock_binding AFTER INSERT deferred constraint; binding validator |
| groundloop_m5_document_clock_binding_immutable | groundloop_m5_document_clock_binding BEFORE UPDATE/DELETE; existing immutable-row rejector |
| groundloop_m5_document_clock_epoch_counter_guard | groundloop_m4_evaluation_epoch_counter AFTER INSERT/UPDATE deferred constraint; binding validator |
| groundloop_m5_document_clock_transition_guard | groundloop_m4_evaluation_counter_transition AFTER INSERT deferred constraint; binding validator |
| groundloop_m5_document_clock_override_guard | groundloop_m4_evaluation_override_counter AFTER INSERT/UPDATE/DELETE deferred constraint; binding validator with full OLD/NEW and deletion proof |
| groundloop_m5_document_clock_runtime_guard | groundloop_m5_runtime_epoch AFTER INSERT/UPDATE deferred constraint; binding validator |

All deferred constraints are initially deferred and forced at the original
guards-last point. Every trigger is FOR EACH ROW. Unrelated nonorigin epochs
take the exact legacy branch; no new privilege or lock/row mutation is inferred.
4. Replace ONLY FOUR existing function BODIES, retaining signatures, owners,
   privileges, search-path settings and all non-D36 branches/guards:

| Existing function | Sole D36 delta |
| --- | --- |
| groundloop_m5_matching_deferred_validate() (017) | Origin-bound direct source from/to uses real C binding while patch/context remain N; all source/journal/bijection checks retained. |
| groundloop_m5_validate_semantic_readiness_predecessor() (current021) | New origin-bound mixed eligibility and present-predecessor proof BEFORE advance, no broadened old branch. |
| groundloop_m5_validate_work_contribution() (current021) | Binding-backed direct source, actual-byte acquisition exception, retry companion, and its duplicated mixed-readiness resulting-state proof. |
| groundloop_m5_validate_event_result_children() (current021) | Origin-bound real SEAL/FAIL two-clock final/replay proof rather than D33's no-history1->2 branch; all six-kind/D26/D33 metadata/absence/promotion checks retained. |

5. The single023 schema-bundle ledger registration defined below and the
   explicit NEW table/function privilege policy of Section8; no other schema object, DDL,
   old ACL or validator replacement. Existing013 counter+1/immutability,
   work/timing accumulators and terminal-accounting-closure guards stay exact.

Require the exact accepted022 bundle BEFORE installation. Preserve strict020,
021 and022 checkers byte-for-byte. A separately pinned CLOSED023 successor
catalog must account for exactly the four replaced bodies, enumerated new
objects/constraints/triggers/indexes, ledger and role/ACL projections; it cannot
pass by weakening the old022 checker or learning hashes from the live target.
Literal SQL/object/catalog hashes and exact object names for all catalog
children are frozen in a separately audited PREINSTALL source tranche before
any installation. No assertion of an unchanged174 object count is made.
New origin routes require exact023 and fail before authorization/source DML
if absent/malformed; no mixed-schema fallback. This file installs nothing.

### 9.1 Exact five-field administrative identity

The accepted prerequisite is the FULL022 row, not just its bundle hash:

```text
bundle_id = m5-transition-context-access-schema-bundle-v1
bundle_sha256 = 44990a145629abbf4212a58b8d235631799cbf8dd56b1dc4ed012bdc2e3ab1d7
migration_sha256 = f6de8e14635addb4ac61c266fd10f0a5bbc1d163858bbffb77e39daf3edf484e
oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
prerequisite_sha256 = 893baf9d50cbb84c27b356cbc329f97bc714ca8c280778ece920b5a4dc728eb5
```

The sole new administrative identity is:

```text
bundle_id = m5-document-direct-clock-schema-bundle-v1
bundle_sha256 = stable_m5_digest(
  "m5-document-direct-clock-schema-bundle-v1",
  TEXT("migrations/023_m5_document_direct_clock.sql"),
  HASH(SHA256(exact_migration_023_bytes)),
  HASH("44990a145629abbf4212a58b8d235631799cbf8dd56b1dc4ed012bdc2e3ab1d7")
)
migration_sha256 = SHA256(exact_migration_023_bytes)
oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
prerequisite_sha256 = 44990a145629abbf4212a58b8d235631799cbf8dd56b1dc4ed012bdc2e3ab1d7
```

TEXT/HASH use existing typed framing. Literal023 migration/bundle/body/catalog/
dependency/ACL pins belong to the separately audited PREINSTALL freeze after
exact reviewed SQL exists; no future literal is invented or learned at runtime.
The ledger is the existing permanent groundloop_m5_schema_bundle; its shape,
old rows and immutable guard remain unchanged.

### 9.2 Ledger-first atomic installer and transition barrier

The opt-in admin installer owns ONE idle read-write READ COMMITTED outer
transaction, never an ambient/savepoint transaction. Capture one trusted
permanent schema and safely qualify all ledger/catalog/DDL there. Before the
initial023 ledger decision, do only connection/schema-envelope checks and raw
caller-byte identity calculation, not UTF-8/content/prerequisite/catalog
rejection or installation locks. Classify the exact five-field023 row FIRST:
conflicting caller identity rejects as ledger conflict with no DDL/write;
exact replay validates literal accepted identity and the full023 successor
catalog, without install locks, DDL or ledger changes. Malformed installed
objects are not replay success and are never repaired automatically.

Only on absent023 ledger validate reviewed source/statements/literal pins and
exact022/transitive prerequisites. Take SHARE ROW EXCLUSIVE NOWAIT on the
permanent schema-bundle ledger, THEN EXCLUSIVE NOWAIT on the permanent typed
runtime table. Checked runtime transitions hold their genuine ROW SHARE on
that table through the outer transaction, so first023 installation cannot
change validator authority between native phases. Reread023 ledger under BOTH
locks and reclassify exact replay/conflict. For a still-absent row require the
exact pre023 closed022 catalog; reject any orphan new table/function/trigger/
overload or partial upgrade, even if schema-owner-owned.

Execute ONLY Section9's complete DDL/ACL delta, validate the exact post023
closed catalog/owners/flags/dependencies/grants, then insert the full023 ledger
LAST and commit atomically. Every injected cut/race failure rolls back tables,
triggers, replaced bodies, ACLs and ledger together. No backfill, old-data
repair/history scan, role creation, head update or runtime-mode activation.
Explicit supported-role grants use the separately approved fixed role manifest;
replay cannot add a role or widen grants. Full catalog/ACL validation must prove
that manifest, not accept an observed target snapshot as its expected image.

### 9.3 Explicit successor dispatch; old strict checkers stay strict

ONLY these existing administrative prerequisite dispatcher BODIES may later
be adapted for023; their signatures/exports stay unchanged:

```text
src/groundloop/postgres/migrations.py:
  require_m5_document_update_completion_bundle
  require_m5_transition_context_access_bundle
```

In the same captured permanent schema, an absent023 ledger AND all023-only
relation/function/trigger names wholly absent take the exact prior dispatch/
strict022 route. An exact023 ledger selects a NEW strict023 require utility
with the closed successor catalog and all exact prior ledger/source projections;
it must not recursively invoke either dispatcher or run the unchanged strict022
catalog checker against a023 catalog. A malformed/conflicting023 row, orphan,
missing/extra new object or partial replacement rejects with NO old fallback.
Preserve the strict020/021/022 catalog verifier bodies byte-for-byte; explicitly
validate the023 projection/replacements instead of weakening their inventories.

New schema-admin identity/install/require helpers and administrative exports may
be allocated only in the later source activation. The D36 fresh runtime port
always requires exact023 before authorization/source mutation. Native C2 and
other accepted consumers reach023 through the two named dispatchers without
changing their own business semantics. Private phase readers perform bounded
same-schema ledger/context metadata checks, not a new whole-catalog scan per
phase. Full catalog validation remains an admin/strict prerequisite boundary.
The held runtime-table ROW SHARE and installer NOWAIT barrier bind one authority
choice through the whole call; no cached/mixed-schema route switch. Default
installers/public store/application/v1 routes are not activated by this delta.

## 10. Mandatory falsifiers; all implementation gates PENDING

Reject implementation unless ALL relevant pure, differential and live gates
pass without deselection, xfail, undercharge or fixture-authored READY:

1. Typed origin/binding Python/SQL digest equality, Unicode/framing collisions,
   all field mutations; immutable/nonnullable shape, independent uniques/FKs,
   missing/reused/cross-epoch/source/clock predecessor and duplicate edges.
2. Genuine fresh metadata-preserving nonempty REPLACE OPEN, exact D29 arrays,
   NULL/Unicode URI/nondefault authority, scopes-before-jobs/phased trace and
   complete rollback cuts; old/partial epoch late opt-in and forged origin
   freshness must reject. Real typed complete predecessor required, not the
   incomplete-only REGISTER helper or forged BOOTSTRAP execution provenance.
3. Direct-first, M5-first and interleaved histories, including direct-only,
   requirement-only and mixed semantic effects on shared claims/answers;
   N/C vectors and both lawful readiness outcomes; independent Python/SQL full
   state/currency/binding/pending oracles after each committed boundary.
4. Real new/takeover acquisition, retries, active/inactive expansion/verifier,
   exact first-frontier pending effects; dispatch-digest versus attempt-ID
   mismatch, stale leases/revisions and terminal/expired losing returns.
5. Every raw M4 counter change/unbound transition/fake catch-up, skipped C,
   several M4 transitions under one N, false zero-binding operation, forged
   payload/override coordinate or early/late context must roll back. Prove the
   full counter/override before/after semantics, not merely C<=N.
6. Retry actual companion byte work and original attempt evidence independence;
   independent producer-preimage byte counts, actual mutation rowcounts,
   event=prior+call, one work/timing CAS and sole original timing anchor.
   Both acquisition zero-only legacy branches must remain unchanged.
7. Genuine SP->SCOMP contribution-before-header and independent combined
   closure; zero counts with missing artifacts/direct declarations reject.
   Pending timing missing/observed closure, both forced race orders and no
   model call inside a database transaction.
8. Actual canonical SEAL/FAIL at C, real promotions/failure plan, source-hash
   materialization without circular accounting/late backward locks, exact
   shared result coordinates, fresh connection and concurrent terminal loser.
9. Original K/E replay after later L restoration: exact retained children and
   original C/N/work/timing, zero new writes/model calls; full P33 initial loss,
   certificate-only change, restoration/recurrence/later-only-loss negatives
   and every original six-kind/D26/D33 gate remain mandatory.
10. Owner/distinct-LOGIN real direct pre-source bridge and execution/readiness/
    seal/replay; missing grants, wrong OIDs/schema/session/XID/GUC/temp owner,
    malformed022/023 and PUBLIC privilege leakage all reject; no nonowner OPEN.
11. Exact five-field023 vectors, ledger-first invalid-byte/hash/conflict ordering,
    fresh/replay/conflicting/orphan installs, every DDL/ACL/ledger rollback cut,
    both installer race orders and a real held transition's NOWAIT barrier;
    exact000--022 files/ledgers and prior strict-checker preservation, closed023
    catalog/ACL inventory, byte-exact legacy/group/R33/J33 coexistence, pure/
    live applicable suites, typing/lint/compile, installed-wheel payload and
    whole-repository diagnostics with failures/skips/package limits retained.

No controlled SQL terminalizer or authored header/counter proves a production
positive. Source-only seams and fixtures keep their ceilings. All actual new
coordinator/source producers and roles must be qualified under later ownership.

## 11. Path-exclusive draft custody and later barriers

This NEW docs-only worktree starts from exact accepted906697b7 above:

```text
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d36-document-clock-contract-candidate
branch = workstream/m5-d36-document-clock-contract-candidate
sole_owned_path = docs/workstreams/m5_runtime_contract/DOCUMENT_DIRECT_CLOCK_AMENDMENT.md
editor = root
auditors = two independent read-only agents; ZERO edit paths
precreation_receipt = /home/kassym/Desktop/groundloop-d36-document-clock.yjceA0ox/PRECREATION.json
precreation_sha256 = ae57aba629b63cc128a60399ad0473eaf15a0958a827ab0c48f8c30b8d3aa59c
```

All146 pre-existing worktrees, their HEADs/indexes/dirty complete-file pins,
all770 parent blobs, P33's original four-path state, all9 held C1 pins and
all15 containers remain protected. No held-source advance/import/extraction,
reset/clean/stash/transplant/rebase/cherry-pick or automatic main-checkout
advance. Existing P33/C1 failures and overlap remain; no future path is granted
by naming a private adapter or schema object here.

Freeze this candidate's COMPLETE SHA/blob/bytes/lines/mode/parent/path in an
external manifest. Get two independent complete identical-byte GOs/P0=0/P1=0;
any edit revokes BOTH. Design advisories are not those audits. Then STOP and
hand off the reviewed candidate. No candidate staging/commit/push or authority
freeze is included in the present drafting approval.

After separate authorization and those audits, prepare authority-freeze updates
with the new decision-log entry, addendum precedence, acceptance falsifiers,
status/ownership/handoff and exact accepted digest. Only after that barrier may
a NEW path-exclusive activation allocate sequential schema/literal preinstall,
clock-aware private producers/receipts/context glue, genuine complete baseline
and nonempty OPEN/execution/readiness/seal, checked P33 resumption, then C1's
separate overlap reconciliation/public composition. Never allocate by copying
held C1 convenience code or silently editing the old wrappers. If another old
function/schema change is needed, stop for new explicit audited authority.

## 12. Claim ceiling

This is a proposed contract correction only. D25 integration, P33, C1, Task2/
Task17, M5.4--M5.6, deployment/security, AI quality and end-to-end utility/
speed/cost remain PENDING. Runtime remains v1_only outside isolated fixtures.
No SQL/tests/provider/model/install experiment occurs; APIcalls/spend/reserved
remain0 under the existing aggregate US$5 cap. Task14 labels retain self-reported
AI-assisted project-author review, not independent/blinded human adjudication.
No objective-truth, novelty, named-system superiority or measured-performance
claim follows. The four-deliverable completion campaign is not finished.
