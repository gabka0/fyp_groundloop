# M5-D28 Phased Persisted-Matching Composition Amendment

Status: **candidate wording-only correction; not implementation authority until
two independent same-byte audits return `GO`, `P0=0`, and `P1=0`.**

Date: 2026-09-17

Scope: one sequencing contradiction among M5-D24's tier-15 accounting order,
M5-D25's exhaustive cursor-local API/source-first rule, and the frozen M4
direct-transition source inserted at tier 15c, plus the receipt interpretation
required when that composition creates more than one immutable contribution
but retains one current accumulator. This amendment changes no public API,
DTO, digest, schema, migration, source kind, reference kind, semantic rule,
counter, measured value, or runtime mode.

Dependency and candidate boundary:

```text
candidate_parent = 8ecdc8e361cf4a8c055fa00e9916e1704cbb736d
candidate_parent_tree = 60318ff77fbb4838872b26a340f619e32a5ff5ed
d24_sha256 = 3fe36a38c3c789143542f8b8cf6614a1df5e7807b9dffa99f0d2cc5118dc1209
d25_sha256 = bac12ab5e74632c04f1bd70d0ef0d00522ba9d268eb8b73d11845bbf3b873aae
d26_sha256 = 85372d4c2f9108810bd75c3e5611de541d0f31c8a096421f30e68fad84676721
d27_sha256 = 7a51afc1f1b6c249572222a023814de8b22220058e00f09564de64f552bd411c
runtime_addendum_revision = 8
runtime_addendum_sha256 = 940f54671b44b3c1aa93d46aadcb1b040aefaf685e98d9c99b2017d5db881e76
migration_013_sha256 = ffe1a403039b78006aef6d8da005f77d9d8ca103f52822d81e9242b669a328fb
migration_014_sha256 = 4c37626f24524316c7990b2f3d213573bbe0f805a2c0f884585bf6aa325f0330
migration_015_sha256 = 85cb7f8e6a33273ce67fc6b4160e74a3aff314cd084df7ac3647cadae930185c
migration_016_sha256 = a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7
migration_017_sha256 = e387b01fa80145273ba40d2d83581bc54762d3a4a2edd34669c095076c52154c
runtime_mode = v1_only
```

The candidate commit must change exactly this file and have the exact sole
parent above. The hashes pin the accepted authority and SQL bytes used to
establish the contradiction; they are not implementation evidence.

## 1. Authority and confirmed contradiction

M5-D25 Section 8 says migration 017 adds only the listed persistence-internal
cursor-local operations. It requires:

1. `derive_matching_transition_intent(...)` to load the immutable persisted
   source and derive every affected key itself; and
2. `apply_matching_transition(...)` to recompute the complete intent from the
   same locked rows and compare it before the first write.

Those two exact signatures are frozen. No caller may provide affected keys,
before/after state, a patch, work, a certificate, or an output image.

The same contract freezes one global order. All applicable semantic,
currency, matching, combined-state, and certificate row/conflict keys at
tiers 11a through 14 must be known and locked before tier 15. D24 work/timing
accounting occupies 15d through 15h. D25's patch artifact, contribution, and
accumulator then occupy 15i through 15k. D25 may not reach 15i and later
reacquire D24's earlier 15d--15h rows.

A monolithic first-application call cannot both write lower-tier D25 state,
return exact D24-owned planned physical-write counts to the outer transaction,
allow that transaction to persist 15d--15h, and then resume at 15i--15k.
Private implementation factoring alone is insufficient because Section 8 says
the displayed operations are exhaustive.

The accepted SQL adds one more required placement. Migration 017's immediate
working-state and revision-interval guards require the base epoch to name the
resulting revision before those D25 rows. Migration 016 requires both base and
typed runtime headers to name that revision for the 15d work contribution and
requires the typed runtime header at the same point for the 15e work and 15h
timing accumulators, including every applicable timing guard. The later-
transition `(N,N+1)` authorization must therefore occur while both headers
still name `N`, but the one atomic header advance must occur after complete
lock/plan validation and before the first resulting-revision guarded row.
D24 Sections 7.2 and 10, runtime-addendum Section 14.3, and D25 Section 9.2
group that physical advance with later accounting or terminalization in an
order that the accepted triggers cannot execute. Migration 017 also requires
the exact tier-16 status-delta multiset before its deferred transition
validation.

The direct path adds a stricter cycle. Its exact D25 `direct_transition`
source is the immutable
`groundloop_m4_evaluation_counter_transition` row whose lock/insert position is
15c. Source-first derivation after that insert would have to acquire affected
matching/combined-state keys at tiers 11b--14, violating the global order.
Locking those keys before 15c cannot use the frozen source-first derivation
because the immutable source row does not yet exist.

Therefore the first nonempty Task-2 composition cannot satisfy all frozen
sentences simultaneously. No implementation lane may guess a private protocol
under path-allocation authority alone.

The same first multi-transition implementation exposes two replay ambiguities.
D25 retains immutable per-transition contributions but only one current
per-epoch accumulator; exact replay forbids prefix reconstruction and aggregate
contribution scans. Therefore an older transition cannot reproduce a
historical accumulator snapshot that is not stored. Section 6 fixes the only
point-read interpretation without adding schema: historical patch/contribution
identity remains exact, while `receipt.accumulated_work` is the validated
current retained accumulator and `expected_work` remains contribution-scoped.

D25 also makes the first-application intent a complete transient lock plan,
including affected keys that can remain unchanged. Migration 017 stores the
resulting patch and contribution, not that intent or its digest. After a later
transition changes the working image, exact replay both cannot reconstruct the
old no-op lock plan from current rows and must not pretend that it did. Section
6 therefore defines one canonical changed-key replay projection from retained
bytes. It validates every projected field and digest while making no false
claim about nonpersisted no-op affected keys.

## 2. Narrow precedence rule

M5-D28 supersedes only:

1. M5-D25 Section 8 where its exhaustive-operation, direct-source-first
   derivation, single-call wording, or full-original-intent replay wording
   prevents the protocol below, and Section 9.2 items 8--11 only for the
   relative physical DML placement corrected here;
2. M5-D24 Section 7.2's sentence that physically applies a revision's sum
   before the revision advance and Section 10 items 4--6 only for a D25 first
   application: the complete contribution sum and write plan are derived and
   frozen before the advance, while the accepted contribution/accumulator rows
   are physically written afterward at 15d--15h; and
3. runtime-addendum revision 8 Section 14.3 items 6--7 only where they group
   job terminalization and revision advance after state installation. State
   installation still precedes job terminalization, but the already-locked
   base and runtime headers advance earlier as the separate step required by
   the accepted guards.

For `direct_transition` only, M5-D28 permits a store-derived pre-source lock
reservation followed by an explicit private, reservation-consuming source load
and official-intent completion after the exact tier-15c source insert. These
exceptions change relative DML placement, not lock acquisition order: the held
tier-5 base row installs its complete planned resulting after-image first and
the held tier-6 typed-runtime row installs its complete planned resulting
after-image second, including all already-derived counters/status rather than a
revision-only intermediate. This acquires no new earlier lock and performs no
second revision advance. D25 Section 10's tier order and bounded
representative-discovery exception otherwise remain exact. Every other
D24--D27 and runtime-addendum rule remains exact.

The Section-8 callable signatures remain byte-for-byte unchanged:

```text
derive_matching_transition_intent(
  cursor, epoch_id, expected_runtime_revision, resulting_revision,
  source_kind, source_id,
  expected_source_identity_hash: SHA256 | None = None
) -> M5PersistedMatchingTransitionIntent

apply_matching_transition(
  cursor, intent: M5PersistedMatchingTransitionIntent,
  expected_patch_digest: SHA256 | None = None,
  expected_work: M5OverlayWork | None = None
) -> M5PersistedMatchingPatchReceipt
```

The existing public `M5TypedApplication`, persistence-port, and M4-v1 APIs are
unchanged. M5-D28 permits only package-private, cursor-local phases used by the
existing outer persistence transaction. They are not public methods, contract
DTOs, provider inputs, persisted authority, or a process cache.

## 3. Transaction-local prepared authority

One first application may use a package-private prepared authority containing
only store-derived values:

```text
_PreparedMatchingTransition(
  cursor_object_identity,
  backend_identity,
  transaction_identity,
  matching_context_identity,
  source_kind,
  source_id,
  source_identity_hash,
  official_intent,
  prewrite_matching_revision,
  prewrite_patch_artifact,
  physical_and_logical_write_plan,
  base_header_after_image,
  runtime_header_after_image,
  d25_contribution_work,
  d24_owned_planned_write_counts,
  requirement_state_write_count_diagnostic,
  direct_m4_stage_evidence_or_none,
  phase
)
```

This is descriptive private state, not a new frozen DTO or digest recipe. Its
concrete class/member names are not wire authority. It exists only in one
Python call chain while the same database transaction remains open.

The direct-only reservation in Section 5 is likewise an explicit package-
private value in that call chain. The caller MUST pass it as an argument to
the private source-loading completion operation after 15c; it may not be
attached implicitly to a cursor or recovered through a module/global registry.

The pre-source M4 stage returns a second explicit package-private value:

```text
_DirectM4StageResult(
  cursor_object_identity,
  backend_identity,
  transaction_identity,
  matching_context_identity,
  reservation_identity,
  source_id,
  source_identity_hash,
  first_old_by_key,
  final_new_by_key,
  actual_write_counts,
  phase
)
```

This is also descriptive private state rather than a frozen DTO. The M4 stage
derives it from its locked reservation and actual cursor-local mutations,
returns it lexically, and does not mutate the reservation to hide evidence.
The direct matching completion accepts both values explicitly. Cross-cursor,
cross-transaction, changed, copied, missing, or double-consumed stage evidence
fails before D25 DML.

For the direct path, `first_old_by_key`, `final_new_by_key`, and
`actual_write_counts` cover every mutated lower-tier and job/attempt family,
the applicable 15a and 15b rows, and all three exhaustive 15c families: the
evaluation-epoch counter, every ordered evaluation-override counter, and the
immutable evaluation-counter transition. Omitting or coalescing one family is
invalid stage evidence.

The implementation must bind it to the exact cursor object, PostgreSQL
backend, assigned transaction identity, migration-017 matching-context
identity, epoch, expected/resulting revision, source kind/ID/hash, and single-
use phase. A different cursor object, connection, backend, transaction,
matching context, epoch, revision, source, or phase fails before D25 DML. It
must not be placed in module/global state, serialized, persisted, hashed,
returned through a public protocol, accepted from a caller, or retained after
commit/rollback.

`d24_owned_planned_write_counts` contains only the five physical coordinates
that D24 actually owns: group-state, claim-state, answer-state, certificate-
binding, and public-delta writes. The requirement-state diagnostic is allowed
only by D27 and remains nonpersisted, unhashed, unreported, and unaliased.
Certificate-artifact insertion is not a certificate-binding write.

## 4. Source-present phased composition

`structural_open` and `requirement_completion` both have an immutable source
before the first D25 write, but their source and authorization order is not
the same:

- for `structural_open`, the exact `groundloop_epoch` event row containing
  the structural event ID and payload hash is persisted and locked at frozen
  tier 5, before tier 6. The base and typed runtime rows are then inserted and
  locked at revision 1 as already frozen. The tier-5 source authority remains
  held and MUST NOT be reacquired after tier 6;
- for `requirement_completion`, the tier-6 authorization/context may bind the
  proposed attempt ID only as a compare-only source ID. The exact immutable
  attempt-result source and immutable output/execution/verifier closure are
  then persisted and locked at their frozen positions through tier 10. The
  mutable job and attempt rows are locked and their terminal after-images are
  planned, but they remain preterminal until the explicit post-state step
  below. The source's official attempt ID MUST equal the proposed ID before
  tier 11.

The accepted SQL authorization is exactly
`groundloop_m5_authorize_persisted_matching_transition(epoch_id,
expected_runtime_revision, resulting_revision, source_kind, source_id)`. It:

- requires transaction-local `groundloop.m5_checked_transition=on`, validates
  the source kind/identifier and the structural `(1,1)` versus later
  `(N,N+1)` revision law, and locks/rechecks both epoch and runtime rows with
  `FOR UPDATE` against the exact expected revision;
- invokes `groundloop_m5_matching_begin_transition_context`, which creates
  `pg_temp.groundloop_m5_matching_transition_context`,
  `pg_temp.groundloop_m5_matching_change_journal`, and
  `pg_temp.groundloop_m5_matching_expected_changes` as `ON COMMIT DROP`
  relations bound to backend PID, `pg_current_xact_id()`, and `session_user`;
- sets transaction-local `groundloop.m5_matching_mode`,
  `groundloop.m5_matching_epoch_id`,
  `groundloop.m5_matching_expected_revision`,
  `groundloop.m5_matching_resulting_revision`,
  `groundloop.m5_matching_source_kind`, and
  `groundloop.m5_matching_source_id`, while the context initializer sets the
  transaction-local `groundloop.m5_matching_context_oid`,
  `groundloop.m5_matching_journal_oid`, and
  `groundloop.m5_matching_expected_oid`; and
- accepts no source hash, queries no immutable source row, and establishes no
  source/hash authority. The accepted deferred validator later validates the
  exact persisted structural, requirement, or direct source, including the
  direct row inserted at 15c.

M5-D28 grants no SQL change. Each first application invokes that authorizer
exactly once at its frozen tier-6 point; the resulting context is bound and is
never reset or reacquired.

After the applicable source and authorization steps above, the outer
transaction must execute:

1. call the unchanged `derive_matching_transition_intent(...)` exactly once.
   The call itself loads the immutable source, gathers/sorts/deduplicates every
   pre-discovery affected row, referenced-key, absent-row advisory, and unique-
   conflict key, acquires tier 11a, then current and working image rows at
   11b, performs the sole frozen bounded representative discovery under those
   locks, materializes the exact sorted 11c set, acquires 11c through 14 in
   order, revalidates every before image, and only then returns the official
   intent. It retains all locks in the caller-owned transaction; it accepts no
   plan argument and stores no implicit plan/token;
2. through the apply-style shared private core, recompute the source and
   complete intent from the locked rows without invoking the frozen
   `derive_matching_transition_intent(...)` callable a second time; derive the
   exact prewrite patch, physical/logical write plan, D25 work, D24-owned
   planned counts, D27 diagnostic, complete base/runtime header after-images,
   and prewrite matching-image semantic revision; independently validate every
   held reference/advisory/conflict
   coordinate not represented in the frozen intent DTO; and bind the cursor-
   bound prepared authority;
3. for `requirement_completion`, using the already-held tier-5 and tier-6
   rows and the validated tier-10 source closure, install the prepared
   `groundloop_epoch` after-image first and prepared
   `groundloop_m5_runtime_epoch` after-image second, advancing exactly once
   from `N` to `N+1` before the first resulting-revision row. The prepared
   images include combined status and runtime counters; neither is a revision-
   only intermediate. This
   narrow position is
   required by the migration-017 state/binding guards and migration-016
   15d/15e/15h guards. It does not terminalize the job. Structural open
   remains at its already-inserted revision 1 and performs no second advance;
4. immediately before the first D25 stage write, revalidate the official
   intent, all before images, cursor/transaction/context identities, and
   single-use phase;
5. stage, under the already-held keys, every applicable tier-11a semantic-
   observation and currency write first, followed by only the lower-tier
   matching, semantic-state, semantic certificate-artifact, and certificate-
   binding writes assigned through tier 14; the D25 immutable patch artifact
   and D25 work tables are not staged here;
6. for `requirement_completion`, terminalize the already-locked job and
   attempt to their planned resulting-revision after-images. This obtains no
   new tier-10-or-earlier lock. Structural open has no such completion step;
7. from the prepared sorted key plan, let the existing outer mutator acquire,
   validate, and mutate every applicable tier-15a owner-pending coordinate,
   then 15b answer-pending coordinate, then every applicable 15c coordinate,
   followed by D24 15d--15h work/timing accounting in exact sub-tier order,
   without acquiring or reacquiring an earlier lock;
8. invoke package-private
   `_finalize_prepared_matching_transition(cursor, intent, prepared,
   expected_patch_digest=None, expected_work=None)`. It consumes the prepared
   authority through the exact cursor object, validates immutable source/
   context, stage journal, actual lower-tier row counts, compare-only
   expectations, and prewrite artifact bytes, then writes only the patch
   artifact at 15i, contribution at 15j, and accumulator at 15k and returns
   the exact frozen `M5PersistedMatchingPatchReceipt`;
9. persist every exact `groundloop_status_delta` represented by the D25
   logical-output multiset at tier 16, followed by every other applicable
   later public/result row in its already-frozen order; and
10. force the accepted deferred guards before the outer transaction commits.

At 15k only, the implementation locks/loads accumulator authority. For a
first structural transition it requires accumulator absence and creates the
exact contribution vector at `updated_revision=resulting_revision`. For a
later transition it requires the prior accumulator's `updated_revision` to
equal the captured prewrite matching-image semantic revision, not the possibly
later runtime expected revision; recomputes the digest from all 37 counters;
adds the exact one contribution; and sets
`updated_revision=resulting_revision`. An unlocked earlier accumulator read
cannot become authority.

Finalization validates the patch derived from the locked prewrite image and
the stage journal. It must not reconstruct a patch from already-mutated after
state. Any missing/extra/different key, row count, byte, source, context,
phase, work value, or compare-only expectation rolls back the whole outer
transaction, including D24 rows.

The unchanged `apply_matching_transition(...)` signature and receipt semantics
remain the general exact-replay entrypoint, but D28 makes it fail closed before
write when the named contribution is absent. Every first application has a
mandatory tier-11b working-image insert/update and therefore requires the
explicit private stage/D24/finalizer composition; there is no valid monolithic
production first-apply case. The accepted empty structural store-core fixture
that pre-persisted 15d--15h before later 11b derivation is scoped historical
foundation evidence, not conforming production use, and must be updated or
remain non-production. First application MUST use the explicit cursor-bound
phases and private finalizer above; it MUST NOT call the replay-only operation
or deliver a token through implicit lookup. Replay and first application share
the exact retained-byte decoders, source/artifact/contribution/accumulator
validators, and receipt construction. No module/global registry is permitted.
First application may not bypass checked transition authorization,
migration-017 guards/journals, transition bijection, or deferred validation.
Historical zero-DML replay instead uses Section 6's checked read envelope; it
creates no transition authorization/context/journal and schedules or forces no
transition deferred validator.

## 5. Direct-only pre-source reservation

Only `source_kind=direct_transition` may use a pre-source reservation. It is
needed because the immutable M4 evaluation-counter transition appears at 15c.
No structural or requirement source may use this exception.

Before any tier-11 DML and before 15c, the typed outer transaction must:

1. hold the frozen epoch/runtime CAS serializer and establish the existing
   checked-transition plus persisted-matching transition authorization/context
   for the exact epoch, expected/resulting revision, source kind, and
   compare-only proposed source ID; migration 017's authorizer may not be
   changed and the context may not be reset or reacquired later; the proposed
   ID is not source authority;
2. at each precursor's frozen lock position, lock the exact persisted M4 job,
   attempt, completion/result, observation/discovery/child-closure, policy,
   and related authority; derive the deterministic official expected M4
   transition ID and payload hash from that locked precursor closure, never
   from caller-authored affected keys or patch bytes; and require the official
   ID to equal the provisional context source ID before continuing;
3. privately gather, sort, and deduplicate the complete joint M4+D25 candidate
   keys knowable before representative discovery, then acquire every
   applicable tier-11a M4 semantic-observation/currency key and the current
   matching-image row followed by the requested epoch's working matching-
   image row at tier 11b;
4. under the held tier-6 and tier-11b image locks, perform only the frozen
   bounded representative discovery when applicable; merge its exact tier-11c
   observation keys, or the empty set when it is inapplicable, with the pre-
   discovery set; sort/deduplicate and acquire that complete set current before
   working for each key; and then acquire tiers 11d, 11e, 12a, 12b, 13, and
   14, including matching, semantic-state, certificate, referenced-authority,
   insert-on-absence advisory, and unique-conflict keys at their frozen
   positions with numeric/key-component order and `COLLATE "C"`;
5. only after every tier-14 lock and before any M4-stage DML, gather, sort,
   deduplicate, acquire, and validate every applicable tier-15a owner-pending
   row or insert-on-absence conflict coordinate, then every tier-15b answer-
   pending row or insert-on-absence conflict coordinate, then the frozen tier-
   15c evaluation-epoch row, ordered override rows, transition primary key,
   and `(epoch_id,to_revision)` unique conflict/advisory coordinate. No tier-
   15 key may be acquired before the complete tier-11a--14 set; and
6. bind that now-complete key plan, precursor identities, expected source
   ID/hash, and all locked before images into a cursor/backend/transaction/
   context-bound, single-use reservation. It performs no D25 DML and is not an
   official intent.

Using only those held tier-11a--15c locks, the M4 typed-private stage accepts
the reservation explicitly, writes its exact already-frozen lower-tier
observation/currency/state/certificate and job/attempt closure, writes every
applicable already-locked M5 owner-pending row at 15a and answer-pending row at
15b with the resulting revision, then at 15c updates the locked evaluation-
epoch counter, applies every planned evaluation-override-counter insert,
update, or delete in frozen key order, and finally inserts the immutable
evaluation-counter transition. It returns one exact `_DirectM4StageResult`
whose old/new images and actual counts include every one of those families.
The pending-counter writes precede 15c; their deferred validation observes the
completed header advance below. The stage neither mutates the reservation nor
hides the result on a cursor/registry.

After that exact 15c source exists and before either header update or any D25
DML, the caller must pass both the reservation and stage result explicitly to
a package-private reservation-consuming source-load operation. That operation
must:

1. reject a different cursor/backend/transaction/context, reservation identity
   or phase, M4 stage-result identity or phase, or already-consumed pair before
   reading or writing D25 state;
2. load and byte-validate the immutable source row;
3. execute the same store-owned source-to-intent derivation used by
   `derive_matching_transition_intent(...)` and produce the exact frozen
   `M5PersistedMatchingTransitionIntent`, without changing that public
   signature or pretending it accepts the reservation;
4. require exact equality between the official intent's D25 affected-key
   fields and the reservation's exact D25 projection. Separately rederive and
   require equality for the reservation remainder, including M4 tier-11a
   keys, precursor and referenced-key authority, absent-row advisory keys,
   unique-conflict keys, and ordering coordinates not represented in the
   frozen intent DTO; an extra, missing, reordered, or different coordinate
   in either comparison is a conflict;
5. revalidate the captured bounded-discovery result and every joint before/
   after image. For a row already mutated by the pre-source M4 stage, require
   the reservation's captured before image to equal the stage evidence's
   first-old image and the locked live row to equal its final-new image; for an
   unmutated row, require the locked live row to equal the captured before
   image;
6. derive and freeze the official prewrite patch, complete physical/logical
   write plan, D25 contribution work, D24-owned planned counts, D27 diagnostic,
   and complete base/runtime header after-images from the persisted source,
   captured prewrite authority, and validated M4 staged after image. It MUST
   NOT reconstruct a before image from an already-mutated live row;
7. consume the reservation phase and bind the
   official intent plus cursor/backend/transaction/context-bound prepared
   authority, including the captured prewrite patch/artifact, complete write
   plan, both header after-images, D25 work, D24-owned planned counts, D27
   diagnostic, and verified M4 stage evidence, in a single-use
   `ready_to_advance` phase; and
8. return the pair lexically as `(official_intent, ready_prepared)` while
   performing zero D25 DML.

This private operation is the direct-only D28 equivalent of frozen source-
loading intent derivation. It is not a new public API and may not obtain its
reservation through implicit cursor state. Only after receiving that exact
pair may the outer direct completion install the prepared
`groundloop_epoch` after-image first and prepared
`groundloop_m5_runtime_epoch` after-image second, advancing exactly once from
`N` to `N+1`. The images include complete planned combined status and runtime
counters rather than revision-only intermediates. This placement preserves the
accepted M4 stage order and derives the complete contribution/write plan before
the advance while satisfying the accepted D25/D24 guards; it acquires no
earlier lock.

Immediately after the header updates and before the first D25 DML, the caller
passes `(official_intent, ready_prepared)` explicitly to the private D25 stage
operation. That operation revalidates the cursor/backend/transaction/context,
source, stage evidence, exact installed header after-images, intent, plan, and
phase; writes only the prelocked lower-tier D25 keys; and returns the exact
next-phase `staged_prepared` without discovering, acquiring, reacquiring, or
expanding any tier-11a--14 key. The outer call chain retains
`(official_intent, staged_prepared)` across D24 15d--15h and passes it
explicitly to the Section-4 finalizer. It MUST NOT place any value on a cursor
attribute, in the migration-017 context tables, in serialized state, or in any
implicit registry.

The transaction then performs D24 15d--15h, finalizes D25 at 15i--15k, writes
the exact tier-16 `groundloop_status_delta` multiset and every applicable later
public/result row, and forces deferred validation as Section 4 specifies. Any
source/reservation/stage-result/intent/before-image mismatch rolls back the M4
source and every staged row. The reservation and stage result never substitute
for the official persisted-source intent.

The existing migration-017 authorizer already validates the held epoch/runtime
CAS and creates transaction-local context without querying the source row.
Its deferred transition-bijection validator later requires the exact immutable
15c direct source. M5-D28 relies on those accepted bytes and grants no SQL or
migration edit.

## 6. Replay, failure, and concurrency

Exact replay bypasses first-application preparation, reservation, staging, D24
count construction, transition authorization, migration-017 temporary context/
journals, and transition deferred validation. It uses a checked read envelope,
not transition mode. A package-private point reader may make unlocked reads of
one source-key contribution and its named artifact to construct a candidate
replay intent, but those reads are hints only and confer no authority.

The replay intent uses the unchanged exact
`M5PersistedMatchingTransitionIntent` type and its unchanged digest recipe, but
is a canonical retained changed-key projection rather than a claim that the
original transient no-op lock plan was persisted. It is constructed only from
the artifact's validated canonical preimages as follows:

```text
source/before/result/policy fields = exact patch fields
group_shapes = decoded group_shape_set_preimage
observation_ids = outer keys of observation_change_preimages
edge_keys = decoded D25 sorted keys of edge_change_preimages
mask_keys = decoded D25 sorted keys of mask_change_preimages
hall_group_ids = outer keys of hall_change_preimages
requirement_state_ids = object IDs of requirement_state logical changes
group_state_ids = object IDs of group_state logical changes
claim_state_ids = object IDs of claim_state logical changes
answer_state_ids = object IDs of answer_state logical changes
group_certificate_ids = sorted unique object IDs in either a
  group_certificate logical change or decoded group_binding row
claim_certificate_ids = sorted unique object IDs in either a
  claim_certificate logical change or decoded claim_binding row
intent_digest = the unchanged D25 intent recipe over exactly these fields
```

Every sequence uses D25's exact sort, uniqueness, repeated-key, typed-decoder,
and exact-re-encoding rules. The authoritative route recomputes the supplied
intent digest, rejects a noncanonical tuple, and later requires every supplied
field and digest to equal this projection. Thus an extra, omitted, changed, or
reordered projected key fails. A key that was part of the original full first-
application lock plan but produced no physical, logical, artifact, or binding
change is deliberately absent: neither migration 017 nor D25 patch bytes
persist it, so historical replay does not assert or regenerate it. This narrow
replay projection does not weaken the complete official-intent equality check
before a first-application write.

Using the supplied projection only as a locator, `apply_matching_transition`
validates the immutable source and its closure in frozen order: structural
source plus predecessor prefix at tier 5 before the tier-6 envelope;
requirement terminal semantic job at tier 9 followed by the completed attempt
and attempt-result/output/execution/verifier closure at tier 10, after tier 6
and before image locks; or direct source at 15c after image locks and before
15i. Requirement replay byte-compares the exact job/attempt/source bindings,
active terminal job state, result artifact ID/hash, `completed_revision=R`,
completed attempt state, attempt-output digest, execution-spec hash, payload,
semantic-pair/subject/chunk identities, pair-input hash, and produced epoch.
The checked tier-6 read envelope locks/rechecks the current
base/runtime identity and legal nonterminal/terminal state but does not require
the current header to equal historical revision `R`. It then locks current and
retained working-image headers at 11b. It does not set
`groundloop.m5_matching_mode`, invoke the transition authorizer, create context
or journal tables, or schedule a transition validator.

The route makes one non-authoritative, unlocked point read of the contribution
primary key solely to obtain a candidate patch digest. After the source/image
steps above, it locks and byte-validates that patch at 15i, reconstructs and
compares the canonical replay projection, then locks and re-reads the
contribution once through both its source primary key and resulting-revision
unique key at 15j, requires the authoritative row to match the hint and patch,
and only then locks/validates the current retained accumulator at 15k. A
missing/changed hint, source mismatch, projection mismatch, or any 15j
authority lock before the 15i artifact lock conflicts. `expected_patch_digest`,
when supplied, compares with the retained validated patch; `expected_work`
compares with the retained historical contribution. Replay returns the
unchanged receipt shape with `exact_replay=true` and performs zero DML,
constraint forcing, external/model/provider calls, work/timing additions,
history regeneration, or counter reconstruction.

For a retained transition at revision `R`, replay preserves that transition's
historical patch, contribution digest, and `resulting_revision=R` while
`receipt.accumulated_work` is the one current retained cumulative accumulator
read for the epoch. The accumulator may have `updated_revision>R` after later
semantic transitions; it MUST validate against the retained matching-image
semantic revision selected by D25's checked nonterminal/terminal envelope and
against its own 37-counter digest. Exact validation requires
`accumulator.updated_revision = retained_image.updated_revision >= R` and each
of the 37 cumulative counters componentwise greater than or equal to the
historical contribution counter. The accumulator MUST NOT be required to equal
the historical contribution. `expected_work`, when supplied, compares
only with the historical transition's stored contribution work, not with the
current cumulative accumulator. No prefix reconstruction or aggregate
contribution scan is permitted. Latest-transition replay is the equal-revision
special case of this rule.

Failure before commit retains no token or reservation and exposes the exact
prior database image. A token is consumed even if a later operation fails; it
cannot be retried inside an aborted transaction. Reconnect starts from
persisted authority and creates a new transaction-local plan only for genuinely
missing work. It never adopts a token from another process.

The tier-6 epoch/runtime lock serializes same-epoch semantic transitions.
Contenders must either observe exact replay after the winner commits or derive
from the unchanged prior image after the winner rolls back. A waiter may not
reuse the winner's prepared authority. Different-epoch patch-artifact reuse
retains the accepted tier-15i advisory/conflict serialization.

## 7. Non-change boundary

M5-D28 introduces no change to:

- `M5PersistedMatchingTransitionIntent`, `M5PersistedMatchingPatchReceipt`,
  `M5RuntimeWork`, `M5OverlayWork`, or any other frozen/public DTO;
- the Section-8 callable signatures or the public M5/M4 application and
  persistence protocols;
- any digest domain, typed field order, counter, source kind, changed-state
  kind, present/absence recipe, patch/contribution/accumulator identity, or
  event-result identity;
- migrations 013--017, migration ledgers, relation, column, trigger, guard,
  authorizer, temporary journal, constraint, privilege, or index bytes;
- the D24 15a--15h or D25 15i--15k suborder, row contents, counters, or
  digests, or any outer lock-acquisition tier; only the explicit header/
  terminalization relative DML placement in Section 2 changes;
- D27's absence of a requirement-state physical-write counter;
- M4-v1 default constructor, public route, payload, receipt, observation,
  transition, publication, replay, or result bytes;
- the deferred policy-change, rootless requirement-observation, or standalone
  claim-observation event forms;
- runtime mode, provider/model behavior, deployment, or measured latency; or
- model/AI accuracy, utility, objective truth, security, novelty, or
  named-system-superiority claims.

No module/global token registry, process cache, full repository hydration,
full oracle, aggregate job/contribution scan, caller-authored patch, new
timing anchor, nested transaction, internal commit/rollback, or external call
inside the transaction is authorized.

## 8. Mandatory falsifiers

Implementation or authority evidence is rejected unless every applicable case
passes without skip, xfail, no-match, or silent deselection:

1. **Frozen-byte inventory.** Section-8 signatures, DTOs, digests, D24/D25
   vectors, migration 013--017 bytes/ledgers, source/reference kinds, and M4-v1
   public golden bytes remain unchanged.
2. **Source-present order.** Structural and requirement transitions prove
   their distinct source order. Structural proves the tier-5 event source,
   revision-1 base/runtime rows, exact `(1,1)` authorization, no source-lock
   reacquisition, and no second revision advance. Requirement proves tier-6
   authorization, immutable tier-10 source closure, locked preterminal job/
   attempt, and planned terminal after-images. The unchanged derive call itself
   proves pre-discovery gathering, 11a, current/working 11b, sole bounded
   discovery, exact 11c, and 11c--14 before it returns. The requirement path
   then proves base-header followed by runtime-header `N` to `N+1`, tier-11a
   observation/currency DML, remaining lower-tier D25 stage, job/attempt
   terminalization, 15a, 15b, every applicable 15c, 15d--15h, 15i--15k, exact
   tier-16 status deltas and later rows, and forced deferred validation in that
   order.
3. **Direct 15c order.** Direct completion proves authorizer/context and the
   complete joint M4+D25 reservation. Frozen tier-10-or-earlier precursor
   persistence may precede the reservation. After the complete tier-11a--14
   lock set, it proves ordered acquisition of every planned 15a, 15b, and 15c
   row/absence/advisory/unique-conflict coordinate before stage DML. The
   explicit-reservation M4 stage writes its frozen lower-tier/job closure,
   every applicable 15a then 15b pending-counter row, then mutates the 15c
   evaluation-epoch counter, every ordered override counter, and finally the
   immutable source transition, and returns exact lexical old/new/count stage
   evidence for all three 15c families. The explicit two-
   input source completion then proves both equality checks, derives the
   complete patch/work/count/header plan, returns
   `(official_intent, ready_prepared)`, and performs zero D25 DML. Base-header
   followed by runtime-header `N` to `N+1`, D25 stage, 15d--15h, 15i--15k,
   exact tier-16 status deltas and later rows, and forced deferred validation
   follow without another discovery or earlier lock.
4. **Representative-discovery order.** Source-present and direct paths prove
   all pre-discovery keys are gathered/sorted first, followed by tier 11a,
   current then working image locks at 11b, only the frozen bounded indexed
   representative discovery, the merged/sorted/deduplicated exact observation
   key set, and remaining 11c--14 locks. Source-present official derivation owns
   that sole sequence and the private recomputation agrees from held rows;
   direct official completion equals the reservation's D25 projection. An
   extra discovered key, discovery before image locks, another earlier-tier
   key after 11c, or any newly discovered, acquired, reacquired, or expanded
   tier-11a--14 or other reservation-prefix key after 15c fails before D25
   DML.
5. **Complete-plan equality.** Adding, removing, reordering, or changing any
   official D25 affected key, joint-plan remainder key/conflict coordinate,
   planned 15a--15c key, precursor, source ID/hash, bounded-discovery result,
   before image, or prepared header after-image while recomputing every caller-
   visible value still fails before D25 DML.
6. **No reservation authority.** A self-consistent reservation or stage result
   with no exact 15c source, a different source, or a source whose official
   intent differs rolls back every M4/D25/D24 write.
7. **Transaction binding.** Cross-cursor-object, cross-connection, cross-
   backend, cross-transaction, context, epoch, revision, source, phase, double-
   M4 stage, D25 stage, and finalize reuse all fail closed. Direct composition
   proves the lexical reservation plus `_DirectM4StageResult` handoff, exact
   `(official_intent, ready_prepared)` return, explicit post-header
   `staged_prepared` return, and explicit finalizer handoff;
   changed/copied/missing evidence or cursor-attribute, context-table,
   serialized, mutated-reservation, or registry token transfer fails closed.
8. **Stage/finalize separation.** Stage cannot write the D25 patch artifact,
   contribution, or accumulator. Finalization cannot acquire an earlier lock;
   it may mutate only the exact prepared finalizer-plan keys at 15i--15k, while
   every lower-tier mutation must match the stage journal and every finalizer
   mutation must match the completed migration journal. Unchanged-signature
   `apply_matching_transition` rejects absent contribution authority before
   write and performs canonical retained-projection replay only. The historical
   fixture's first-apply and 15d--15h-before-11b order is rejected as production
   evidence.
9. **Counter ownership.** Actual group/claim/answer/binding/delta counts reach
   only their five D24 coordinates; requirement-state writes remain exact D25
   patch evidence plus a transaction-local diagnostic and never alias another
   counter; certificate artifacts are not binding writes.
10. **Accumulator position.** The D25 accumulator is first locked/read as
   authority at 15k; first structural apply requires absence, while later apply
   requires `updated_revision` equal to the captured prewrite matching-image
   semantic revision rather than runtime revision. A valid coordination-only
   runtime advance beyond that semantic revision succeeds only with the latter
   coordinate; substituting the runtime expected revision, a stale revision/
   digest, or a consistently rehashed wrong value fails and rolls back.
11. **Crash matrix.** Injection after authorization/reservation, each lower-
    tier plan, each tier-11a observation/currency write, each operation-
    specific job/attempt closure, base and runtime revision-advance positions,
    every M4/D25 family, 15a, 15b, the 15c evaluation-epoch counter, each
    ordered override counter, the final 15c source transition, every D24
    15d--15h group, D25 15i/15j/15k, tier-16 status deltas/later rows, and
    forced deferred validation exposes only the complete before or after image.
12. **Replay.** After at least two semantic transitions, exact replay of the
    older structural, requirement, and direct source, including after failure,
    seal, reconnect, and later-head advance, preserves its historical patch,
    contribution digest, and resulting revision; returns the validated current
    cumulative accumulator at its later semantic revision; compares
    `expected_work` only with the historical contribution; and performs zero
    writes/calls/regeneration. It proves canonical candidate-projection
    construction, normal digest recomputation, exact equality of every retained
    projection field, the source-specific tier-5, tier-9--10, or tier-15c
    closure check, checked read envelope with no transition context/journal,
    unlocked source-key contribution hint, authoritative 15i patch lock, both-
    key 15j contribution lock, and 15k accumulator lock in order. An extra,
    missing, reordered, or changed projection field, changed hint,
    15j-before-15i authority lock, differing source/patch/contribution work,
    changed requirement job state/result/completed revision, changed attempt
    state/output/execution identity, attempted transition context, accumulator
    revision not equal to retained-
    image revision or below historical `R`, any cumulative counter below its
    historical contribution, or bad accumulator digest conflicts atomically.
    A fixture whose original official intent contains an affected key with no
    physical, logical, artifact, or binding change proves that the key is
    absent from the retained projection even after a later transition changes
    that row; adding the nonpersisted no-op key to the supplied replay
    projection conflicts rather than pretending to validate historical lock-
    plan authority.
13. **Races.** Both commit/rollback orders for same-edge and different-edge
    completion, same-source replay, and two different transitions preserve
    all refcounts, states, certificates, D24/D25 counters, journals, and one
    revision per winner.
14. **Private/SQL rejection boundary.** Before D25 DML, the private direct
    source-completion operation rejects any reservation, precursor, transition
    ID/hash, captured discovery, or locked-before-image mismatch. Separately,
    first-application migration-017 guards/deferred validation reject forged GUC/context,
    missing or malformed persisted source, patch/contribution/source identity
    or revision mismatch, and unauthorized raw D25 stage/finalize DML. No
    claim is made that PostgreSQL sees the transient reservation/precursor
    closure, distinguishes which Python helper inserted an otherwise byte-
    identical immutable M4 source row, or independently performs the private
    equality check. Historical replay separately proves that the private
    checked read envelope rejects malformed retained authority while creating
    no transition context/journal and invoking no transition deferred validator.
15. **No hidden expansion.** Static and runtime tripwires reject a process
    cache, `M5IncrementalOverlay`, full repository hydration, oracle/aggregate
    scans, model/provider calls, new schema/API fields, or a new timing anchor.

## 9. Acceptance and claim ceiling

This candidate becomes authoritative only after two independent reviewers
audit identical committed bytes and each returns `GO`, `P0=0`, and `P1=0`.
Any byte change restarts both audits. Only the exact reviewed bytes may then be
added to a separate authority-freeze/status tranche and used by a new
path-exclusive Task-2 activation.

Acceptance resolves only the phased-composition sequencing contradiction and
the required current-accumulator replay interpretation. It does not accept a
source/test implementation, D24, D25, D26, D27, Task 2, M5.4, M5.5, M5.6,
deployment, performance, utility, or AI-quality result. Runtime remains
`v1_only` outside isolated disposable fixtures.
