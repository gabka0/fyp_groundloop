# M5-D33 Narrow Document-Update Completion Amendment

Status: docs-only candidate, not frozen or implementation authority. Two
independent audits of identical complete bytes must each return GO/P0=0/P1=0
before authority updates or a new path-exclusive implementation activation.

Date: 2026-10-10

## 1. Evidence, scope and precedence

Candidate base is accepted main 8290d47cba71350cc550cf18cb12c32b47cb7b2a.
Read-only C1 evidence is the held nine-path candidate described in
D30_STRUCTURAL_COMPOSITION_HANDOFF.md, not accepted implementation. Its
retained document REPLACE positive fails on metadata defaults. Its real DELETE
with zero direct roots and nonempty requirement roots reaches zero counters
after genuine cancellation but cannot use D32's group-only readiness. Correct
closure of its lost group-certificate bindings would then be rejected by
D26's group-retirement-only absence authority. The third finding is source
confirmed, not a successful live document seal. These are three prerequisites,
not an AI-quality finding or evidence that C1 is complete.

This decision supersedes only:

1. D32 Sections 2, 3, 5 and 7's exclusion of document/M4 surfaces, for the
   precisely job-free document branch in Section 2 below; the old group branch
   and both old identity recipes stay exact;
2. D29's derivation of a legacy REPLACE structural sidecar using InsertedDocument
   metadata defaults, for the store-derived preservation rule in Section 3;
3. D26's exclusion of document events, only for group_certificate absence under
   the DELETE/REPLACE withdrawal proof in Section 4; and
4. the same-runtime/evaluation-revision rule in the M4 evaluation-overlay
   handoff and typed direct-seal composition, ONLY for Section 2.3's genuine
   no-direct-history two-clock seal; the legacy M4 wrapper stays untouched; and
5. prior complete migration-inventory/new-function-replacement prohibitions,
   only for the additive 021 delta in Section 5.

All other D24--D32 contracts, D29's exact three-array declaration, D30 provenance
closure, legacy M4 bytes and behavior, D31 privilege boundary, D32 group-only
020 behavior and accounting, and present-state recipes remain unchanged.
There is no authority for source edits in this candidate. Runtime remains
v1_only outside isolated fixtures. No active-verifier C2 integration is added.

## 2. Job-free document semantic readiness

### 2.1 Closed eligibility

Reuse the existing semantic_readiness contribution/timing kind, source IDs
semantic_pending/semantic_complete and D32 typed source recipe, without a new
enum, domain, DTO or public method. The epoch/event/payload/root-set/state-pair/
revision inputs keep exactly their D32 order and framing. A locked immutable
declaration determines branch eligibility; a caller cannot select a branch.

The additional eligible update kinds are document_insert, document_delete and
document_replace, iff ALL of the following hold:

- The exact base/runtime/update/event/payload/predecessor/policy/snapshot and
  revision-1 structural_open contribution bindings pass D29/D30 source closure.
  The typed M5 and genuine M4 update agree on document action and predecessor.
- The exact retained D29 declaration has all THREE arrays empty: direct roots,
  direct scope roots and direct fallback claims. Its M4 runtime manifest and
  retained structural sidecars agree. Empty counters alone are insufficient.
- There are NO groundloop_semantic_job or groundloop_discovery_scope rows for
  this epoch, no groundloop_m5_direct_terminal_projection, and no M4 evaluation
  override-counter or counter-transition rows. This excludes a document whose
  previously nonempty direct jobs have finished; that route is not expanded.
- Exactly one genuine groundloop_m4_evaluation_epoch_counter exists, with its
  validated declaration_hash, lifecycle_state=active, revision=1,
  open_discovery_scope_count=0, default_evaluation_state=complete and
  confirmed_as_of_epoch=P. Validate the unchanged M4 declaration recipe
  from its retained declaration inputs; no new M4 hash or fake job is allowed.
- Every existing D32 independent M5 root/job/scope, closure/cancellation,
  runtime-count, owner/answer-count, event/declaration, predecessor and
  half-terminal check passes. A pending, result_staged, failed, unclosed or
  wrong-root-set state rejects even when some counters say zero.

Existing group events still require the complete absence of M4 surfaces under
D32. A malformed document surface cannot fall back to that group branch.
Policy/observation events, rootless ObserveRequirementEvent, cross-policy
withdrawal and documents with any direct job history remain outside this
additional readiness route. No direct job is created or impersonated.

### 2.2 Edges, transaction and immutable proof

The graph remains structural_committed -> semantic_pending -> semantic_complete
-> sealed. Both readiness edges own SEPARATE idle read-write READ COMMITTED
outer transactions:

| target/source_id | predecessor | condition |
|---|---|---|
| semantic_pending | structural_committed at N=1 | genuinely empty M5 root set and no M5 jobs/scopes, plus Section 2.1 |
| semantic_complete | semantic_pending at N>=2 | all genuine M5 work terminal/closed or cancelled and all counts zero, plus Section 2.1 |

Nonempty M5 roots enter semantic_pending through their existing genuine job
progression, not an extra readiness-start call. There is no SC-to-complete
shortcut, fake zero-delta direct transition or unaccounted header CAS.

Retain D32's base-before-runtime, owner-C-order, answer-C-order, then work/timing
point lock order. Validate document authority at its existing frozen tiers;
earlier-tier terminal immutable rows are nonlocking point validations, not
late lock reacquisitions. The epoch lock serializes lawful job/header changes.
Insert the guarded immutable contribution BEFORE either header advance; its
BEFORE guard independently proves the present predecessor and all eligibility.
Advance base/runtime and owner/answer revision bindings once to N+1. Base
semantic/evaluation stays pending for the first edge and becomes complete for
the second. The unchanged 015 transition trigger validates the actual edge;
the deferred work validator proves the exact resulting state and source.

The genuine M4 update, evaluation counter (including revision=1), metadata and
all M4 jobs/scopes/transition ledgers remain byte/value unchanged by readiness.
It is M5 coordination, not an M4 transition. No matching state, certificate,
observation, patch, matching-work contribution, public delta, head or runtime
mode changes. Matching-work revision remains at its last semantic change.

Work and timing are EXACTLY D32: execute source preimage S and unchanged
contribution-key preimage K once each, hash each once, charge S+K to both byte
counters and zero to all other counters. SQL independently derives this exact
vector. Keep validation-rehash and database-byte exclusions. Resolve an older
pending point as missing and install the one new nonterminal readiness anchor
using one final timing CAS; one work CAS applies the contribution. No early
pending clear, second anchor, history SUM authority or zero-work substitution.
Force deferred checks last; every failure rolls back the whole transaction.

Replay uses the immutable original contribution, original input N, state pair,
event/payload/root-set, exact original work and key. Validate retained document
declaration/source authority without requiring the current M4 counter still
active/revision=1 after a later legitimate seal. Validate its later terminal
state through the accepted result/replay envelope instead. Return the ORIGINAL
anchor with replay=true and zero DML after legal progress/seal. An independently
authorized failure envelope may be validated, but this amendment implements or
activates no new job-free failure coordinator. Changed
identity/phase/N/work rejects; terminal epochs cannot create a new readiness
contribution. Before and after the serializer lock, recheck a concurrent winner
so an exact losing call is zero-write replay, not a duplicated contribution.

### 2.3 Genuine job-free document seal: two explicit clocks

Requirement-only progression and readiness advance M5/base but do not run an
M4 evaluation transition. Therefore the eligible direct evaluation counter is
still at its genuine declaration revision 1 while base/runtime are at N>=3.
The old M4 _stage_direct_seal_local wrapper cannot be used here: both its
_assert_incremental_evaluation check and its SEAL call assume equal revisions.
Do not conceal that incompatibility with a test callback, counter overwrite,
catch-up DELTAs, fake job or a weakened legacy assertion.

Authorize ONE new package-private M5 cursor-local job-free-document seal
adapter, composed ONLY inside the separately activated C1 outer seal. Require
exact 021 authority, the same locked document/declaration/source eligibility
as Section 2.1, separately committed genuine semantic_complete, and the
unchanged M4 runtime validate_seal_point_local at actual epoch revision N.
Validate the genuine adopted measured M4 working cache and persisted update.
Replace ONLY the equal-counter/header-revision assertion with the explicit
two-clock proof: counter active at 1/P, no overrides, no scopes/jobs/projections,
no prior evaluation transitions, complete direct evaluation, and all M5 work
ready. Retain every other direct publication/readiness/identity check.

Reuse unchanged M4 cursor-local structural promotion, currency promotion and
grounding-state publication primitives in their original order; published
state revision is N+1 and public direct deltas remain suppressed for the one
combined M5 result. Then call the genuine unchanged evaluation_store
apply_transition_local with its EXISTING canonical M4 SEAL transition ID,
recipe, kind=SEAL and expected_revision=1. This creates exactly one immutable
counter transition 1->2 and a sealed/complete counter confirmed at K, with zero
scope/override counts. No raw counter DML, new transition kind/hash recipe or
second transition/anchor is permitted. All actual writes/producer bytes are
charged once to C1's existing seal accounting; no synthetic work is introduced.

Base/runtime, published state, heads, result and D24 work/timing points still
advance N->N+1 under the unchanged C1 order. The evaluation counter's OWN
terminal coordinate is 2, not N+1. This is the sole synchronization exception;
legacy M4 and typed documents with any direct-job history retain existing
behavior and are not routed through this adapter. The adapter owns no outer
transaction, head/base/runtime advance, result, pending timing clear or extra
seal contribution. C1 owns all of those atomically and forces checks last.

Final/replay validation proves BOTH coordinates and the exact canonical M4
SEAL1->2 ledger/payload, counter sealed at K, absent overrides/jobs/scopes,
unchanged original declaration and exact terminal M5 envelope. Readiness
replay before seal still requires counter1/P; after seal it validates this
counter2/K proof. A separately accepted legitimate failure envelope uses its
accepted counter lifecycle, never a fabricated active counter. The legacy
failure wrapper's shared-revision assumption is not bypassed here; job-free
failure composition remains PENDING under separate authority. Exact
losing concurrent seal returns the retained combined result without rerunning
the adapter or writing a second evaluation transition. Owner and distinct
LOGIN production histories must prove this actual path, not an SQL test-only
terminalizer. If any other nonrevision M4 check cannot be retained without
new authority, stop rather than broadening the adapter silently.

## 3. Preserve replacement document metadata

ReplaceDocumentVersionEvent has no source_uri or authority_class fields. Its
frozen payload hash remains exactly the admitted legacy hash. Do not reconstruct
or rehash that event from sidecars, change its DTO, or treat defaults (None,
dynamic) as authority over an existing document.

For first application of document_replace, at the established document-source
authority tier, derive source_uri and authority_class from the exact locked
groundloop_document row owning the locked old document version at predecessor
P. Preserve both values exactly, including a legitimate NULL URI, Unicode and
nondefault authority. The new version belongs to that SAME document ID.
Missing/ambiguous/wrong-parent or changed source authority conflicts. Carry
this pair in the private prepared structural payload through preview comparison,
source revalidation, M4 runtime event_manifest, metadata overlay, retained
direct-open reconstruction and M4 seal. Public M4 metadata is not overwritten
or defaulted. Existing M4 promotion's metadata-equality rejection stays intact.

The nonauthoritative preview may read the pair but retains no lock/token;
first application rederives under locks and rejects a changed proposal. On
replay, derive the pair from the exact event-local retained metadata overlay
and runtime event_manifest, validate equality with the owning public document
and unchanged version/chunk/provenance sidecars, and use that SAME pair for all
comparisons. Do not assume the old version is still current after legal later
progress, or reconstruct from a newer version. No extra D29 declaration key,
new table, hash field, JSON/representation hash or mutable caller metadata is
added. Existing INSERT defaults and DELETE no-insert behavior remain exact.
Any one-field sidecar/manifest/public-metadata mismatch rejects with zero writes.

## 4. Document-induced applicable group-certificate absence

### 4.1 Exact source and final absence

This is an additional narrow applicability branch for the EXISTING D26 artifact:

```text
stable_m5_digest(
  "m5-changed-state-absence-artifact-v1",
  ENUM(kind), TEXT(object_id)
)
```

ENUM/TEXT use the unchanged typed expansion/framing. Here kind MUST be exactly
group_certificate and object_id the same immutable active group version G.
The six reference kinds, non-null state_artifact_hash, outer
m5-changed-state-reference-v2, set ordering/uniqueness and every present-state
and certificate recipe remain exact. This branch authorizes no requirement,
group, claim or answer STATE absence and no claim_certificate absence.

At sealed event E, epoch K, revision S, predecessor P, independently prove:

1. All D26 Section 3.1 sealed result/base/runtime/head coordinates agree,
   adapted ONLY to update_kind document_delete or document_replace with the
   genuine M4 DELETE/REPLACE mapping. D29 binds the exact immutable legacy
   payload; do NOT run the group-event payload recipe or rehash legacy text.
   There is no group deactivation at K. G is PUBLISHED and its group-validity
   interval covers both P and K without closing at K.
2. D29/D30's exact document withdrawal source closure identifies the old
   document version and deactivated chunks. DELETE has no inserted version;
   REPLACE has the exact same-document new version/chunks and preserved Section-3
   metadata. Deactivation/version/overlay identity, epoch, event, predecessor,
   same-policy and retained declaration checks are mandatory, not an asserted
   absence flag or merely equal copies of a payload hash.
3. Exactly one valid published complete predecessor group-state row and one
   applicable group-certificate binding for G covered P. Independently
   recompute the immutable certificate and its dense requirement/observation/
   text-hash provenance closure at P. The artifact and its rows remain intact.
4. The UNIQUE canonical D25 structural_open contribution/patch for (K,E)
   binds the same payload and before point P, result point (K,1). Decode and
   re-encode all relevant retained children/outer preimages under unchanged
   D25 rules. Its logical change set contains exactly one group_certificate/G
   change with before equal to that predecessor digest and after=None, and
   logical-output contains exactly (group_certificate,G,None). Its actual
   withdrawal contribution removals include predecessor certificate witness
   currency from the exact deactivated chunk set. Validate final incompleteness
   through the existing bounded Hall/matching/state guards, not solely from
   removal of one observation. Independent Python/SQL recomputation is required
   acceptance evidence, never production authority or a full-oracle fallback.
5. At final promotion/seal G still has valid PRESENT requirement/group state;
   its effective final group state is incomplete with certificate_digest=None.
   Its working image (or exact effective image for unchanged keys), current
   matching image and promoted state agree. No final working/applicable binding
   remains. Exactly one predecessor published binding closes at K and no
   same-G binding starts at K or covers K. This is absence AS OF K, not a ban
   on later legitimate restoration: a binding starting at L>K, including one
   still open today, is allowed during historical replay and cannot cover K.
   At first seal an unclosed predecessor necessarily covers K and rejects.
   The predecessor group
   state closes at K and its PRESENT incomplete successor starts at K,S.
   Requirement/group state successors use only unchanged present recipes.

The structural loss proof and final snapshot proof are BOTH necessary. No
whole-history scan is required or authorized: read the indexed unique open
patch, bounded affected keys, source closure and exact current/final images.
Intervening legal semantic progress remains subject to existing D25 guards.
If a certificate is restored by final seal, emit its PRESENT reference under
the unchanged rules, not absence. A transient loss is not final absence. If
loss recurs and final state is absent, the same initial withdrawal proof plus
exact final absence governs; no intermediate patch is itself publication.
A loss first introduced only by a later unrelated completion is outside this
branch. Incomplete-before/absent-before produces no absence reference.

### 4.2 Complete reference set and promotion

For every qualifying final document binding loss emit EXACTLY one
group_certificate absence reference at (K,S); require a bijection between the
independently derived qualifying logical changes, closed-no-successor bindings
and stored absence references. Extra, omitted, duplicate, reordered,
wrong-before, before=None, present-after, wrong-kind, wrong-source or wrong-point
records reject the seal. Present and absent keys cannot overlap.

The C1 promoter must close affected predecessor bindings using the complete
affected group-state key set as well as working binding/deactivation keys;
absence of a working binding row must not leave a stale published certificate.
Actual closure rowcounts are charged once to the existing binding-write counter;
do not count an absence reference as a synthetic state/artifact write. Exact
work, full-state changes and certificate-only changes remain included even when
claim/answer status enums stay equal. No tombstone, nullable hash, synthetic
certificate, seventh kind, repr/JSON hashing or changed present recipe is allowed.

The preterminal reader validates these proofs through D31's unchanged trusted
accessor and one captured schema. The unchanged public result-bound builder
must produce identical children after immutable result creation. Terminal
replay validates original historical intervals/coordinates and stored children,
not today's heads or a regenerated reference set; it performs zero writes.
Every existing D26 exact group REPLACE/RETIRE branch and negative remains intact
except its blanket document rejection is narrowed by this section.

## 5. Sole additive migration-021 authority and schema custody

Future file: migrations/021_m5_document_update_completion.sql. Migrations
000--020 and their ledger rows stay byte-identical. The ONLY schema delta is
CREATE OR REPLACE FUNCTION for these THREE existing signatures:

1. groundloop_m5_validate_semantic_readiness_predecessor(): add Section-2's
   mutually exclusive exact document eligibility to the unchanged group branch;
2. groundloop_m5_validate_work_contribution(): add the document resulting-state,
   retained-source and unchanged D32 identity/S+K checks for semantic_readiness,
   preserving ALL other work kinds and every old group branch; and
3. groundloop_m5_validate_event_result_children(): add Section-4's document
   group-certificate-only absence proof/set bijection, preserving every D26
   group absence, present reference, delta, ordering, work, receipt/result and
   digest check.

Preserve signatures, return types, language, security flags, ownership, ACLs,
trigger identities/attachments, constraint deferrability and pinned search-path
behavior. No timing validator replacement is necessary: kind/source/anchor and
work vectors are unchanged. No other function/trigger/CHECK/table/column/index/
type/view/sequence/ACL/backfill or old migration byte may change. No clone,
alternate validator or caller GUC/temp-table trust may bypass predecessor proof.
Use schema-qualified permanent authority and canonical typed framing in new
branches; no public-schema fallback or privilege expansion.

The opt-in installer owns one idle read-write READ COMMITTED transaction,
captures one trusted permanent schema and performs ledger-first identity checks.
Require the exact five-field accepted 020 row and transitive 000--019 authority
with exact pre-021 catalog BEFORE replacing anything. Accepted 020 migration
SHA256 is df0a3c0c9b228a4a22903479896326d27fbd6f98f5878e34d73182ba007bf837;
020 bundle SHA256 is b7706feb7d54fcf9fdb4f9f38350a32967e0b460229b6f264493fb428be8ddfc.
Retain the established installer lock and reread ledger under lock; install all
three replacements and ledger last atomically. Exact replay requires original
caller-byte identity AND exact post-021 catalog; conflict, partial/tampered
catalog, wrong schema/prerequisite or different first-install bytes fails
closed. Injected failures restore all three old functions and the absent ledger.
Concurrent different-byte installers cannot both succeed. No runtime mode changes.

021's five ledger fields use bundle_id m5-document-update-completion-schema-bundle-v1,
migration_sha256=SHA256(exact reviewed 021 bytes), unchanged empty-oracle SHA256,
prerequisite_sha256=the exact accepted 020 bundle above, and:

```text
bundle_sha256 = stable_m5_digest(
  "m5-document-update-completion-schema-bundle-v1",
  TEXT("migrations/021_m5_document_update_completion.sql"),
  HASH(migration_sha256), HASH(prerequisite_sha256)
)
```

Freeze literal reviewed migration/bundle/catalog pins in a separately audited
docs-only hash-freeze tranche BEFORE the production installer accepts them.
Never infer acceptance authority from mutable local SQL. The existing exact
020 catalog checker must NOT be weakened to accept 021 replacements. Add an
exact 021 verifier: retained group readiness may select exact 020 when 021 is
absent or exact 021 when its ledger is present; a present wrong 021 must not
fall back. New document readiness/publication composition requires exact 021.
The complete installed catalog must differ only by the three enumerated bodies
and the ledger. Schema-admin install is not a new public application route.

## 6. Required executable falsifiers

On identical final implementation bytes, with failures/skips retained:

1. SQL/Python old source/key/S+K vectors remain exact, including Unicode;
   old group positives/negatives run on 020 and 021. Document readiness rejects
   020, wrong/missing/partial 021, mixed schemas and lookup/temp shadowing.
2. Genuine owner and separately connected distinct-LOGIN job-free document
   histories: rootless start/complete and nonempty M5 roots genuinely
   closed/cancelled -> complete. No authored positive headers, fake direct jobs,
   disabled guards, copied pending hashes, zero-work override or test adapter
   presented as production composition. Direct manifests/counters unchanged.
3. Every declaration-array/surface/counter/hash/revision/job/scope/failure/root/
   predecessor mutation, after-advance insert, terminal-new-transition, wrong
   replay and concurrent winner case rejects or exactly replays as specified.
   Zero counts with a nonempty direct declaration MUST reject.
4. Actual source/key byte-region instrumentation, one revision/contribution/
   anchor, one work and timing CAS, prior missing/observed timing, replay after
   later seal, and rollback after EVERY mutation boundary. Genuine job-free
   production seal proves M5/base N->N+1 versus the sole canonical M4 SEAL1->2,
   all original nonrevision checks, actual promotions/writes, exact replay and
   no catch-up DELTA or legacy-wrapper weakening. No pooled counts.
5. Real metadata-preserving REPLACE with NULL/non-NULL Unicode URI and nondefault
   authority through preview/open/retained replay/seal/later progress. Mutate
   each retained/public/manifest field independently: reject, not overwrite.
   INSERT/DELETE and old M4 payload/source/provenance recipes remain exact.
6. Genuine complete-group DELETE and REPLACE withdrawal -> final incomplete
   PRESENT state and exact group-certificate absence; old immutable artifacts
   remain. Compare complete Python/SQL reference sets, work and production C1
   preterminal/result-bound children, not merely hashes/status enums.
7. Incomplete predecessor/no binding, temporary loss restored by seal,
   complete-to-complete certificate change, initial loss/recovery/final loss,
   later-only unrelated loss, wrong source/witness/policy/group/deactivation,
   stale/open/reopened/successor binding, bad closure coordinate, malformed
   logical preimages, omitted/extra/wrong-kind references and both race orders.
   C2-dependent active-completion positives wait for C2; do not fabricate them
   or call this amendment/C1 complete because their boundary tests pass.
8. All six present recipes and D26 group REPLACE/RETIRE absence vectors remain
   unchanged; document requirement/group/claim/answer-state absence and
   claim-certificate absence reject. Exact historical reconnect/replay is zero
   DML/model calls, including after later publication heads advance AND a
   later event restores the same active group's certificate with an open
   binding starting after K. Do not import retired-group D26's timeless
   no-open-successor predicate into this active-group historical branch.
9. Exact three-body catalog delta and immutable 000--020 hashes/ledgers;
   ledger-first install/replay/conflict/prerequisite/catalog/schema/tampering,
   every DDL/ledger rollback cut and both installer race orders. Old 020 checker
   still rejects modified catalogs; exact 021 checker accepts only its pins.
10. Relevant differential/pure/live M4/M5 coexistence, typing, lint/format,
    compile, wheel-module/payload and full-repository diagnostic gates, with
    honest package migration limitations; final complete-path manifests, two
    independent GO/P0=0/P1=0 audits, exact commit/postcommit checks per tranche.

## 7. Freeze, activation and claim ceiling

After two candidate GOs, the separate authority freeze may mark ONLY
M5-D33/M5.0-33 contract-PASS / implementation-PENDING and runtime-addendum
revision 14. Then a separately audited NEW path-exclusive docs-only activation
must allocate sequential schema/shared primitives (with separate preinstall
hash freeze), metadata, readiness, private job-free-seal adapter,
publication-reader and finally held-C1
composition lanes. Name exact paths, parents and whole-byte custody before any
source edit. No parallel source work or silent ownership expansion is allowed.

All nine held C1 files, protected primary/AI and accepted R32/R-T checkouts stay
read-only during docs work. A held worktree advances only with live full pins,
empty-index/raw-status proof, ancestry and exact upstream/owned disjointness;
every failed prerequisite stops before advance. No reset/clean/stash/rebase/
cherry-pick/copy/patch transplant. Preserve diagnostic failures and the earlier
disclosed C1 precheck process incident; later proofs do not retroactively pass it.

This freeze/activation implements nothing. C1, Task17, Task2, M5-D24--D33
implementation evidence and remaining M5.4--M5.6 remain PENDING. Deployment,
AI/model quality, independently adjudicated natural histories and full
end-to-end utility/speed/cost remain PENDING. No objective-truth, security,
novelty, scalability or named-system superiority claim follows. Reassess the
next public vertical slice after scoped C1 acceptance; no later lane is implied.
