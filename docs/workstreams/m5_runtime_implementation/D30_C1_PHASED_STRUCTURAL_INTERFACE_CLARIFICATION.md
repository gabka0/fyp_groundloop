# M5-D30 Document-Structural Prerequisite and C1 Interface Activation

Status: docs-only activation candidate; no prerequisite-repair or Lane-C1
source/test edit is authorized until these exact bytes receive two independent
same-byte `GO`, `P0=0`, `P1=0` reviews, are committed as the sole changed path,
receive two postcommit identity checks, and are pushed to this branch and
`origin/main`

Date: 2026-09-24

## 1. Exact authority barrier

```text
required_parent = 9d803beca5455b9de96604548a01c1666b766ee4
required_parent_tree = c20872d10e9a38f59487729439863ccfc6111fd3
required_origin_main = 9d803beca5455b9de96604548a01c1666b766ee4

task2_activation_sha256 = a39b8aace948ab06e70f8bd2adfe5a6251ae282a87bcc49fff421081be0e6be4
lane_p_handoff_sha256 = d29012dab3920131934ee67e36244bbe8baaf6dee786b273b66aaf5fa9207991

activation_branch = workstream/m5-d30-c1-phased-structural-interface-clarification
activation_changed_path_count = 1
runtime_mode = v1_only
```

The sole owned path is this file. Its commit must have the exact sole parent
above and may change no other path. This activation is subordinate, in order,
to the M5 design freeze, runtime addendum revision 11, M5-D24 through M5-D30,
the acceptance matrix, the D29/D30 authority handoffs, the Task-2
store/runtime activation, and the accepted Lane-P handoff. Frozen authority
wins except for the two exact implementation-sequencing corrections below.

### 1.1 Narrow precedence over the Task-2 activation

This file supersedes only:

1. Task-2 activation Section 2's sequence by inserting the prerequisite repair
   lane in Section 4 below immediately after Lane P; and
2. Task-2 activation Section 6.2 and the Lane-P handoff's C1-base sentence by
   making the exact pushed prerequisite-repair commit, rather than the Lane-P
   commit, the sole C1 base.

The corrected linear sequence is:

```text
matching-planner repair
  -> document-structural prerequisite repair
  -> structural composition
  -> requirement composition
  -> direct-M4 store composition
  -> direct application composition
  -> public composition
  -> clean integration and evidence reconciliation
```

Every other path, lane, semantic, lock, digest, schema, migration, API and
claim boundary remains exact. This file grants no public-protocol, public-DTO,
result-byte, counter, timing-coordinate, runtime-mode, provider, deployment,
performance, utility, security or AI-quality change.

## 2. Confirmed implementation contradictions

### 2.1 The retained direct-open callback crosses frozen cuts

D29 requires one document first application to complete its source-first
prefix through tier 7, gather every nonlocking locator and prospective
declaration coordinate immediately after tier 7, and do both before any
tier-8 scope or tier-9 job authority is acquired. Its locked continuation then
owns its D29/D30 authority through tier 11a. D25 derivation separately acquires
its distinct remaining tier-11a working-currency targets and the matching and
logical-state locks through tiers 11b--14 before staged declaration and
semantic DML.

The retained concrete store accepts one `direct_stage(cursor)` callback. Its
current M4 implementation writes the M4 update and tier-7 structural source,
then also writes withdrawal/working rows, root jobs, discovery scopes and
evaluation rows in the same call. It crosses the tier-6 authorization cut,
writes jobs before scopes, and crosses the tier-8/9 reservation and
tier-11--15 staging cuts. The store also lacks the execution policy and
compare-only direct/requirement proposals required by accepted Lane-P
`_derive_locked_document_open(...)`.

### 2.2 The Lane-P document plan omits direct-M4 after-state

The current document D25 projection derives affected claim/answer IDs only
from owners of affected requirement groups. Its first-application plan copies
the published combined claim's direct support/refute counts, scores and
observation IDs unchanged, then applies only group-completeness changes.

A document delete/replace may independently withdraw current direct-claim
observations. The M4 structural stage then changes the direct claim and answer
after-images. A direct-only affected claim can therefore be omitted from the
D25 intent, while a claim affected by both paths can receive stale direct
fields, status, certificate and answer aggregation. Preparing D25 before that
after-state is independently derived and bound is nonconforming.

Tests that use only requirement observations, only document insert, or caller-
authored expected M4 rows do not resolve this contradiction. No detached M4
repository, in-memory engine, preview object or callback return may become
authority.

### 2.3 Global declaration and lower-tier order needs private phases

One direct declaration callback followed by M5 declaration persistence writes
direct scope (tier 8), direct job (tier 9), then M5 scope (tier 8). One
monolithic M4 lower stage followed by monolithic D25 staging can likewise
write a tier-13/14 M4 row before a tier-11/12 D25 row. Both are forbidden
backward transitions even when every key was precomputed.

The retained D29 helper currently combines the required pre-tier-8 locator
gather with its tier-8--11a locked continuation. C1 must place new-event
snapshot DML at the start of tier 8 between those actions, so that helper also
needs a private single-use prepare/continue cut.

The prerequisite repair and C1 composition therefore require exact private
phase cuts. They change no public API and create no new persisted authority.

## 3. Nonconforming alternatives

The following do not satisfy the frozen contracts:

1. invoking the locator gather before the tier-7 source exists or after any
   tier-8 DML/acquisition;
2. treating `groundloop_m4_update` as the tier-5 source, inserting it before
   the typed runtime header, invoking the authorizer before that M4 update, or
   invoking the authorizer after tier 7;
3. invoking the retained monolithic D29 helper after tier-8 snapshot DML;
4. inserting a scope/job before its prospective coordinate is reserved;
5. writing any direct or M5 tier-9 job before every tier-8 scope write;
6. using supplied previews to select a locator, affected key or after-image;
7. rerunning a public preview inside the opener;
8. copying the predecessor's combined direct fields after a direct withdrawal;
9. using a nested/adjacent transaction, cursor attribute, global registry,
   process cache or serialized token for private authority;
10. treating the direct declaration byte measurement or D28 five-coordinate
   counts as the complete D24 structural-open work; or
11. routing a locked terminal replay through a D29 hydration-cutoff signal.

## 4. New prerequisite repair lane

```text
lane = R -- document-structural prerequisite repair
branch = workstream/m5-d30-document-structural-prerequisite-repair
base = exact pushed commit of this activation
```

Exact owned paths:

1. `src/groundloop/m5/runtime/postgres_matching.py`;
2. `src/groundloop/m5/runtime/postgres_withdrawal.py`;
3. `tests/m5/postgres_runtime/d29_store/test_bounded_withdrawal.py`;
4. `tests/m5/postgres_runtime/d30_store/test_owner_topology.py`;
5. `tests/m5/postgres_runtime/d30_store/test_query_plans_and_races.py`;
6. `tests/m5/postgres_runtime/d30_store/test_replay_nonchange.py`;
7. `tests/m5/postgres_runtime/d30_store/test_total_claim_currency.py`;
8. `tests/m5/postgres_runtime/d30_store/test_document_direct_combined_state.py`
   (new);
9. `tests/m5/postgres_runtime/d30_store/test_structural_stage_phases.py`
   (new); and
10. `docs/workstreams/m5_runtime_implementation/D30_DOCUMENT_STRUCTURAL_PREREQUISITE_REPAIR_HANDOFF.md`
   (new).

Every other path is read-only. Lane R may change `postgres_withdrawal.py` only
to split the accepted private D29 locator/lock helper at the pre-tier-8 cut and
to carry the complete direct-claim authority described below. It may change
`postgres_matching.py` only for that store-derived projection/plan and the
private D25 stage split. It must not change a frozen public callable signature,
DTO, public enum, digest recipe, relation, column, migration, counter or output
recipe.

The five existing test paths above are owned only to redirect their exact
static source-order assertions from the former inline wrapper to the new
prepare/continuation functions and to add the corresponding phase falsifiers.
Their semantic expectations may not be weakened, deleted, skipped or xfailed.
An exact repository search at these activation bytes found no other retained
wrapper-source assertion whose expected target necessarily changes; discovery
of another affected path is a hard stop for a new docs-only activation rather
than an implicit ownership expansion.

### 4.1 Complete direct-withdrawal projection

For document delete/replace, Lane R must extend the store-derived structural
projection to include the sorted union of:

1. claims/answers owning every affected requirement group; and
2. claims/answers owning every current direct observation withdrawn from a
   deactivated chunk.

Starting only from D30's locked total current-currency rows, immutable
observation/source closure and current published M4 state, it must derive the
exact direct claim and answer before/after images. It removes exactly the
withdrawn direct observation IDs, preserves every other exact support/refute
ID, recomputes counts and best scores from immutable point-read observation
authority, recomputes direct status/certificate identity, then combines that
direct after-image with the independently derived group after-image and
required-answer aggregation.

Every remaining observation named by an affected current direct state is
gathered before tier 11a, sorted/deduplicated, point-locked at its frozen
authority tier, and revalidated. The route performs no whole observation,
currency, claim, answer, repository or engine scan; invokes no provider/model;
and reconstructs no unavailable root/classic artifact or raw preimage. A
missing, extra, duplicated, wrong-owner, wrong-task, inactive, unpublished,
wrong-policy or changed observation/state/certificate row conflicts before
D25 DML.

Candidate-only direct withdrawal with no current direct observation remains
state-inert. A direct-only current observation still names its claim and
answer in the affected projection even when no requirement group changes.
Mixed direct-plus-group effects coalesce to one claim, one answer and one
certificate transition from the common before image to the complete after
image.

The locator phase gathers the exact direct claim, observation and answer
coordinates before tier 8 without treating them as authority. The D29 locked
continuation binds its D29/D30 observations/currency through tier 11a. D25
derivation then reserves/locks its distinct working-currency targets at the
remaining tier-11a position, followed by the tier-11b/12 matching and state
keys and the expected direct-M4 claim/certificate and answer-state keys at
their exact tier-13 and tier-14 positions. That D25 tier-11a--14 acquisition
occurs exactly once.

The resulting expected direct M4 tier-13/14 after-images are held only in the
existing package-private cursor-bound prepared authority. They are not new
DTOs, digest fields or caller inputs. C1 later validates that the real staged
M4 rows equal those prepared after-images before it writes the corresponding
combined M5 rows.

### 4.2 Private D29 locator/lock split

Lane R may refactor the existing private
`_derive_locked_document_open(...)` into one single-use, cursor-bound prepare
phase and one single-use locked continuation while retaining that helper as a
wrapper that invokes both consecutively with byte-identical results.

After the tier-7 structure is present and before any tier-8 DML or lock, the
prepare phase must:

1. validate the D29 route, event-local closure and exact held tier-7 source;
2. gather the complete sorted nonlocking locator set and prospective direct/M5
   scope/job-coordinate superset;
3. gather the direct-withdrawal claim/observation/answer coordinates required
   by Section 4.1;
4. validate the existing zero-cancellation and immutable bootstrap authority
   that precede tier 8; and
5. return only a non-public identity-bound package-private authority bound to
   the exact cursor, backend, transaction, event, closure, source and gathered
   locator image in phase `locators_gathered`.

The authority may not be serialized, persisted, hashed, copied, reconstructed,
stored on the cursor, placed in process/module state or accepted from a public
caller. It confers no row authority. After C1 persists the exact new-event
snapshot headers/members at the start of tier 8, the continuation validates
the same binding and consumes that authority exactly once. It performs no new
locator discovery; point-validates touched predecessor snapshot members and
reserves/locks every tier-8 scope, then every direct-before-M5 tier-9 job, the
tier-10 closure, and the tier-11a observation/currency authority in the frozen
order. It derives the exact direct and requirement withdrawals and declaration
roots from those held rows, validates every compare-only proposal, and ends at
tier 11a in phase `consumed`.

A wrong cursor/backend/transaction/event/source, changed locator, missing or
different snapshot image, premature/repeated/reordered continuation, copy, or
post-rollback use fails before new DML. The retained monolithic wrapper retains
its own route barrier and typed-event validation, then calls prepare and
continuation consecutively and preserves every current private caller and
result; only C1 may place the owned snapshot step between the two phases.

### 4.3 Private D25 stage split

Lane R may refactor the existing private
`_stage_prepared_matching_transition(...)` into exact single-use private
phases while keeping that retained wrapper and all public signatures/bytes
unchanged:

```text
ready_to_advance
  -> staged_through_tier_12
  -> staged_through_tier_13
  -> staged
  -> consumed
```

The first phase performs the existing pre-stage revalidation, then writes the
D25 tier-11a currency rows and these distinct families in exact order:

1. tier 11b matching image;
2. tier 11c matching observations;
3. tier 11d matching edges;
4. tier 11e matching masks;
5. tier 12a Hall rows; and
6. tier 12b requirement/group state plus applicable group-certificate artifact
   and binding rows.

The second phase writes only tier-13 combined claim state, claim-certificate
artifact and binding rows after the expected direct M4 claim after-image is
present. The third writes only tier-14 combined answer state after the expected
direct M4 answer after-image is present and then exposes the existing `STAGED`
finalizer condition. Existing monolithic callers invoke those phases
consecutively and retain identical results.

Every phase validates the same cursor/backend/transaction/context, immutable
prepared snapshot, exact prior phase and single-use identity. Cross-cursor,
copied, reordered, skipped, repeated or post-rollback evidence fails before
its DML. Final stage-journal validation remains an exact bijection over the
whole prepared plan.

### 4.4 Lane-R executable falsifiers

On identical final Lane-R bytes, focused tests must prove:

1. direct-only, requirement-only and mixed document withdrawal projections;
2. support, refute, best-score, status, certificate and required-answer changes;
3. candidate-only direct edges remain state-inert;
4. remaining-observation point authority and no whole-relation scan;
5. stale/missing/extra/wrong-owner/wrong-policy direct closure fails before DML;
6. exactly one coalesced claim/answer change for mixed effects;
7. locator prepare runs after tier 7 and before all tier-8 work, permits the
   exact snapshot step, and continuation performs no new discovery;
8. every wrong D29 phase/cursor/transaction/event/copy/reuse fails, and the
   retained monolithic D29 wrapper remains byte-equivalent;
9. the first D25 phase follows exact tier 11a -> 11b -> 11c -> 11d -> 11e ->
   12a -> 12b family order;
10. the three D25 stage phases equal the retained monolithic wrapper's final
   rows, journal, patch, work and receipt;
11. every wrong D25 phase/cursor/transaction/copy/reuse fails;
12. retained D24--D30 planner, differential, replay and counter tests remain
   exact; and
13. public APIs, DTOs, migrations, digest vectors and package bytes remain
    unchanged except for the two owned source files, five exactly named
    retained tests and three new evidence paths.

Lane R ends with two independent whole-byte audits, one commit, two
postcommit identity checks and push to its branch and `origin/main`. Only then
may C1 start.

## 5. C1 base and unchanged path ownership

```text
lane = C1 -- structural composition
branch = workstream/m5-d30-structural-composition
base = exact pushed Lane-R commit
```

Lane C1 retains exactly the nine Task-2 activation Section-6.2 paths:

1. `src/groundloop/m5/runtime/persistence.py`;
2. `src/groundloop/m5/runtime/postgres_recovery.py`;
3. `tests/m5/postgres_runtime/d30_application/conftest.py`;
4. `tests/m5/postgres_runtime/d30_application/test_store_composition.py`;
5. `tests/m5/postgres_runtime/d30_application/test_structural_order.py`;
6. `tests/m5/postgres_runtime/d30_application/test_seal_atomicity.py`;
7. `tests/m5/postgres_runtime/d30_application/test_store_races.py`;
8. `tests/m5/postgres_runtime/d30_application/test_structural_open.py`; and
9. `docs/workstreams/m5_runtime_implementation/D30_STRUCTURAL_COMPOSITION_HANDOFF.md`.

Every other path remains read-only. If Lane R or C1 needs another path, it
stops for another one-file docs-only activation amendment with two same-byte
audits and push.

## 6. Narrow package-private C1 interface

C1 may add one package-private matching-aware activation sibling and one
package-private phased structural-open sibling inside
`PostgresM5RuntimeStore`. Existing public protocols, concrete public open
signatures, DTOs and retained pre-D25 fixture behavior remain unchanged until
Lane D owns public wiring.

The structural sibling may accept only existing typed event, execution-policy,
direct-open, requirement-withdrawal, root/root-hash, recovery configuration,
fallback-map and failure-injector values plus explicit cursor-local M4 phase
callbacks. All supplied plans are compare-only.

A descriptive document phase surface has these separate cuts:

```text
stage_m4_update_declaration(cursor, epoch_id)
stage_tier7_structure(cursor, epoch_id)
stage_reserved_direct_scopes(cursor, epoch_id, recomputed_direct_open)
stage_reserved_direct_jobs(cursor, epoch_id, recomputed_direct_open)
stage_reserved_tier11a(cursor, epoch_id, recomputed_direct_open)
stage_reserved_tier13(cursor, epoch_id, recomputed_direct_open)
stage_reserved_tier14(cursor, epoch_id, recomputed_direct_open)
stage_reserved_tier15c(cursor, epoch_id, recomputed_direct_open)
measure_direct_declaration(cursor, epoch_id) -> M5RuntimeWork
```

For a group event every document-only value/callback is exactly absent. For a
document event every required value and phase has its exact existing type.
Concrete private names are implementation detail rather than wire authority.

No callback return value is database authority. Lexical, transaction-local
implementation evidence may be returned only for exact comparison with the
store-prepared persisted after-images. It is not accepted from a public
caller, serialized, persisted, hashed, stored on the cursor, placed in
module/process state, or retained after commit/rollback.

## 7. Exact C1 first-application order

One new document event uses one outer exact-`READ COMMITTED` transaction and
one cursor in this order:

1. verify exact accepted migrations 017 and 018 before event-ID/epoch
   consumption or the first D30 locator;
2. lock runtime mode, equal M4/M5 heads, activation, event/idempotency,
   predecessor, live-epoch and other frozen tier-1--5 serializers;
3. validate the legacy payload and insert/hold the revision-1
   `groundloop_epoch` event row as the sole tier-5 structural source;
4. insert/hold `groundloop_m5_update` with the exact provisional declaration
   commitment, then the revision-1 typed runtime header/counter row, in the
   frozen D21 order;
5. invoke `stage_m4_update_declaration` only to insert and validate the exact
   `groundloop_m4_update`; it may write no tier-7-or-later row;
6. invoke `_authorize_matching_transition(...)` exactly once as
   `(epoch_id,1,1,structural_open,structural_event_id)` at tier 6;
7. invoke `stage_tier7_structure` only for exact document/version/chunk/
   lifecycle/deactivation rows assigned to tier 7;
8. invoke the Lane-R D29 prepare phase exactly once to revalidate the held
   tier-7 source, gather/bind every nonlocking locator and prospective
   declaration coordinate, and acquire no tier-8-or-later authority;
9. persist and validate exact new-event requirement/chunk snapshot headers and
   members at the start of tier 8; no snapshot row is part of tier 7;
10. invoke the Lane-R D29 locked continuation exactly once with the prepared
   authority, execution policy and compare-only proposals; it performs no new
   discovery and owns only the frozen D29/D30 tier-8--11a reservations/locks;
11. use only its recomputed direct open, requirement withdrawal, roots and
   root-set hash as write authority and require exact proposal/manifest
   equality;
12. invoke `derive_matching_transition_intent(...)` and prepare exactly once,
    using the repaired complete direct-plus-group after-state; it owns the
    distinct remaining D25 working-currency targets at tier 11a and the D25 and
    expected direct-M4 locks through tiers 11b--14;
13. invoke `stage_reserved_direct_scopes`, then persist every sorted M5 scope;
    only after all tier-8 scopes exist invoke `stage_reserved_direct_jobs`,
    then persist every sorted M5 job at tier 9;
14. invoke `stage_reserved_tier11a`, then the repaired D25 stage in exact D25
    tier-11a currency -> 11b image -> 11c observations -> 11d edges -> 11e
    masks -> 12a Hall -> 12b state/certificate order;
15. invoke `stage_reserved_tier13`, byte-validate its direct M4 claim rows
    against the prepared after-images, then execute D25 tier 13;
16. invoke `stage_reserved_tier14`, byte-validate its direct M4 answer rows
    against the prepared after-images, then execute D25 tier 14 and reach the
    existing staged condition;
17. write every store-owned tier-15a owner-pending row, then every tier-15b
    answer-pending row, then invoke `stage_reserved_tier15c` only for already-
    locked evaluation families;
18. derive the complete structural-open `M5RuntimeWork` described in Section
    8, perform D24 tier-15d--15h accounting, finalize D25 tiers 15i--15k,
    write exact tier-16 output rows, and force all deferred validators; and
19. return the unchanged `OpenEventReceipt`.

No callback may discover, acquire, reserve or reacquire an earlier-tier key.
Any proposal, prepared-image or staged-row mismatch rolls back the source
prefix and every later row.

The group branch uses the same store transaction without document callbacks.
It inserts/holds its tier-5 revision-1 epoch source, then its M5 update and
runtime header, creates no M4 update, authorizes at tier 6, stages group
structure at tier 7, handles tier-8 snapshots,
derives/prepares/stages/accounts/finalizes once, remains revision 1, and forces
validation before commit.

## 8. Complete structural-open work ownership

The direct declaration measurement is one input, not the complete structural
work. C1 must derive the exact `structural_open` contribution from persisted
effects and recomputed plans, including:

1. deactivated chunks;
2. withdrawn candidate and current direct/requirement edges;
3. structural hashing/serialization bytes;
4. M4 and M5 scope/job declarations;
5. root/fallback provenance creation;
6. every physical group/claim/answer-state and certificate-binding write owned
   by structural open;
7. any structural-open cancellation, which is exact zero for the currently
   supported Task-2 form; and
8. every other D24 Section-7.3 structural-open counter at its sole owner.

The D28 prepared five-coordinate counts feed only their exact existing D24
components. Requirement-state writes have no `M5RuntimeWork` coordinate: they
remain exact D25 patch/bijection/replay evidence, and their transaction-local
diagnostic is used only for exact comparison. That diagnostic is never added,
aliased, persisted, reported or reconstructed as work. The named work
components do not alias withdrawal, declaration, serialization or cancellation
work. Caller-supplied work is compare-only; model/provider work cannot enter
this contribution. The exact work is measured after the complete tier-15c
image exists and before D24 15d, persists through the existing one timing
anchor, and validates identically on replay.

## 9. Matching-aware activation and seal seams

The public activation signature and DTO remain unchanged. C1 may add a private
matching-aware activation sibling that requires exact migration 017, preserves
existing activation locks and independent bootstrap projection, invokes
`install_matching_activation_projection(...)` in the same transaction, then
installs the M5 head, activation record and mode flip and forces validation.
Document-capable composition additionally requires exact migration 018.
Activation replay is zero-write.

C1 may add a private seal-composition sibling using accepted cursor-local
matching-publication helpers. It validates readiness first, promotes the
matching overlay, composes existing semantic/certificate publication, both
heads, immutable result and terminal state in one transaction, creates no D25
transition or second timing anchor, and forces validation. Public seal wiring
and post-seal cross-layer audits remain Lane D/I work.

Retained migration-014--016 fixtures may keep their historical paths but are
not D25/D30 production evidence. A historical monolithic open cannot create a
D29/D30 first application after matching-aware activation.

## 10. Exact-existing routing

The locked idempotency gate first validates event/payload and the complete
typed closure. If the canonical event result is terminal, it validates and
returns the existing terminal receipt without calling
`_load_retained_document_open(...)` and without emitting/catching the private
D29 hydration-cutoff signal.

Only a locked nonterminal exact-existing document route calls
`_load_retained_document_open(...)`. The held serializer prevents concurrent
terminalization at that point, so the helper's terminal signal is unreachable;
an observed terminal row is a conflict rather than a new signal origin. This
route invokes no source, locator, reservation, preparation, declaration,
lower-tier, 15c or measurement callback and performs zero semantic DML. It
validates retained declarations, matching artifact/contribution/accumulator,
recovery identity and work before returning the canonical receipt.

A stale preview is never replay authority. Same ID with different payload,
legacy-only/incomplete closure, malformed declaration, missing/corrupt
matching evidence or changed recovery identity conflicts atomically.

## 11. Mandatory C1 falsifiers

On identical final C1 bytes, owned tests must prove at least:

1. atomic matching-aware activation and zero-write replay;
2. 017/018 and exact-`READ COMMITTED` barriers before event consumption;
3. tier-5 epoch -> M5 update -> runtime header -> M4-update-only callback ->
   one tier-6 authorizer -> tier 7 -> one nonlocking locator prepare -> tier-8
   snapshots -> one locked continuation with all direct/M5 scopes, all direct
   then M5 jobs and D29/D30 tiers 10--11a -> one D25 derive through its
   remaining tier-11a targets and tiers 11b--14 -> exact staged 11a currency ->
   11b image -> 11c observations -> 11d edges -> 11e masks -> 12a Hall -> 12b
   state/certificate -> interleaved M4/M5 tier 13 -> interleaved M4/M5 tier 14
   -> 15a -> 15b -> 15c -> 15d--15h -> 15i--15k -> tier 16 -> forced
   validation;
4. no source/scope/job/lower callback can successfully write before its cut;
5. authorizer, locator prepare, locked continuation, matching derive/prepare,
   each stage and finalizer execute exactly once;
6. direct-only, requirement-only and mixed delete/replace after-images equal
   the Lane-R store-derived plan;
7. one-value proposal, manifest, root-hash or staged-row drift rolls back all
   source work;
8. wrong cursor/backend/transaction/phase/copy/reuse fails before later DML;
9. group register/replace/retire and document insert/delete/replace retain
   revision 1 and exactly one matching contribution;
10. exact nonterminal replay invokes zero phase callbacks/DML, while terminal
    replay takes the canonical result branch without a cutoff signal;
11. same-ID/different-payload, competing-head and source/planner interleavings
    serialize or conflict without deadlock, partial declaration or duplicate
    contribution;
12. injected open/seal cuts expose only complete before/after images;
13. complete D24 structural work has no omission or double count and excludes
    the requirement-state diagnostic exactly;
14. matching seal is atomic/replay-stable, creates no transition contribution
    and adds no timing anchor;
15. D26 absence references remain exact; and
16. public APIs/DTOs, migrations 001--018, result bytes, model/provider
    behavior and runtime default remain unchanged.

## 12. Per-lane evidence and claim ceiling

Lane R and C1 each require final unchanged candidate bytes, exact path/mode/
hash ledgers, focused and retained regressions, compile/lint/type/package
checks, two independent whole-byte `GO`, `P0=0`, `P1=0` audits, one commit,
two postcommit identity checks, and push to the lane branch and `origin/main`.
Tests from different commits are not pooled.

After C1, the exact local result ceiling is:

```text
matching_planner_repair = PASS
document_structural_prerequisite_repair = PASS
package_private_structural_composition = PASS
matching_aware_public_facades = PENDING_LANE_D
real_m4_phase_producers = PENDING_LANE_M_AND_C3
cross_layer_publication_races = DEFERRED_TO_LANE_D_AND_LANE_I
whole_route_output_nonchange = DEFERRED_TO_LANE_D_AND_LANE_I
M5-D30 implementation = PENDING
Task 2 = PENDING
runtime_mode = v1_only
```

The broader claim ceilings remain exactly:

```text
M5-D24 through M5-D30 = implementation-PENDING
M5.0-24 through M5.0-30 = implementation-PENDING
M5.4-05 through M5.4-09 = PENDING
M5.5 and M5.6 = PENDING
deployment/performance/utility/security/objective-truth/novelty/
maintained-history/named-system-superiority/AI-quality = PENDING
```

`PASS` above is limited to the named package-private lane evidence. `DEFERRED`
is not a pass, skip, xfail, waiver or implementation claim. No security,
objective-truth, novelty, maintained-history, population-quality, latency,
scalability or named-system-superiority claim follows.
