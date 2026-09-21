# M5-D29 Bounded Document-Withdrawal Authority Amendment

Status: **candidate wording-only correction; not implementation authority until
two independent same-byte audits return `GO`, `P0=0`, and `P1=0`.**

Date: 2026-09-22

Scope: one persisted-source, bounded-enumeration, reconnect-completeness,
checked terminal-cutoff, and same-policy admissibility correction for typed
legacy document insert/delete/replace planning. This candidate identifies the
already locked immutable legacy payload hash as source identity, defines the
exact persisted reverse-candidate and current-observation projections used by
document withdrawal, retains a typed-M5-only direct-declaration commitment in
the existing update manifest, and authorizes exactly two migration-018
indexes. It changes no public API, public or frozen DTO, semantic/runtime digest
recipe, source/reference kind, counter, present-state recipe, or runtime mode.
Its sole semantic narrowing is explicit: supported document withdrawal
requires every qualifying historical candidate policy to equal the locked new-
event policy; cross-policy withdrawal remains `PENDING`. Section 7's required
migration-ledger bundle identity is schema custody only and enters no event,
state, work, result, or replay identity.

Candidate boundary:

```text
candidate_parent = 28a1392ccba9592ef41b1e51969c094f1f4b922a
candidate_parent_tree = d96723faeee2bd3cce2098df77fb6dbdf8f3f561
d25_sha256 = bac12ab5e74632c04f1bd70d0ef0d00522ba9d268eb8b73d11845bbf3b873aae
d28_sha256 = 8a2bafd3478cf2cac6ac7c8de7ca7779a6d9ace7fbbe98a7dc3ff08afdb67eae
runtime_addendum_revision = 9
runtime_addendum_sha256 = 4a99e255d266835794638cc3385220ba951b3a22cbbd364076f0eec26ea46d7f
d28_activation_sha256 = eab9f23f267e783b318d9a488556bc3ba3e26bb678fa3e5e15ad71a4979e04a0
migration_015_sha256 = 85cb7f8e6a33273ce67fc6b4160e74a3aff314cd084df7ac3647cadae930185c
migration_017_sha256 = e387b01fa80145273ba40d2d83581bc54762d3a4a2edd34669c095076c52154c
runtime_mode = v1_only
```

The candidate commit must change exactly this file and have the exact sole
parent above. These identities pin the authority against which the
contradiction was found; they are not implementation evidence.

## 1. Confirmed contradiction

M5-D25 and M5-D28 require the store to derive structural source authority and
the complete withdrawal plan from locked persisted rows without a caller plan,
full repository hydration, model work, or a full scan. Revision-9 runtime
authority also preserves the exact legacy M4 payload digest for a typed
document event and says document withdrawal enumerates both persisted reverse
candidate edges and current-observation edges.

The normalized document/version/chunk rows and M4/M5 structural sidecars are
not a second encoding of the original legacy event payload. Reconstructing a
Python legacy event from those relational rows and requiring its newly
computed payload digest to equal `groundloop_epoch.payload_hash` incorrectly
promotes sidecar reconstruction to payload identity. The immutable epoch row
already contains the accepted legacy payload identity. Its semantic sidecars
must still be validated independently and exactly; they must not be used to
invent or replace that identity.

The current-observation half is bounded by the existing
`groundloop_current_observations_by_chunk` index on
`groundloop_observation_currency(chunk_version_id)`. The reverse-candidate
half has no chunk-leading index: migration 015 indexes admitted pairs by epoch
and pair and by epoch and owner only. Therefore a conforming document delete
or replace cannot enumerate historical admitted-pair locators for one changed
chunk without a relation scan. Treating only current currency rows as both
candidate and observation edges also loses candidate-policy provenance and
silently omits admitted pairs that have no current observation.

Exact-existing direct continuation must also prove the complete persisted M4
root declaration set, including terminal `impact_discovery` roots whose
`claim_id` is null. Migration 003 has only an open-job epoch/state index and a
claim index whose predicate excludes null-claim impact roots; discovery scope
has no epoch index. It also permits a discovery scope to reference any same-
epoch job, including a child, and M4 retains no independent direct-root-set
commitment. Enumerating only present roots is circular: a missing or extra
self-consistent frontier root becomes its own expected declaration. A closed
scope attached to a child evades both a root-only locator and the open-scope
counter. D29 therefore requires one retained typed-update declaration
commitment plus a total-width-safe all-job-by-epoch locator; together they
prove root identity and permit one scope point-read for every event-local job.

Runtime-addendum revision 9 Section 11.1 asks for one fallback key per distinct
historical candidate policy, while migration 015 permits every scope/job of one
typed event to bind only the new runtime event's single candidate policy. The
current Task-2 schema cannot satisfy both rules for cross-policy history. D29
therefore makes same-policy history an explicit supported-form precondition and
fails closed on qualifying cross-policy history pending a separate contract and
schema decision; it does not rebase that history.

Finally, the application checks for a terminal result before invoking the two
planners, but either planner or the post-open hydration can race a concurrent
terminalization. Exact-existing declaration projection must therefore be
defined for any event state reached at those cuts. After a nonterminal open
receipt, an arbitrary result read cannot authorize C5/C6/C7's active-cutoff
envelope, and the frozen direct receipt has no generic-result branch. D29 must
therefore add one exact package-private, no-payload hydration-cutoff origin;
canonical result authority and the existing two replay shapes remain exact.

The in-progress D28 Lane-P files were authored under that contradiction. They
are preserved evidence only. No source or test byte in that worktree is
authority, and this candidate does not accept, integrate, or repair it.

## 2. Narrow precedence

M5-D29 supersedes only:

1. D25 Section 8's two-range-adapter limit and Sections 9.1 and 9.3, D28
   Section 4's source-present derivation/lock wording, and runtime-addendum
   revision 9 Sections 15 and 17.2 where they leave the persisted legacy-
   document source check, exact reverse/current withdrawal enumeration, or
   retained root enumeration under-specified;
2. runtime-addendum revision 9 Section 11.1's per-distinct-historical-policy
   fallback rule only for supported typed document withdrawal: every qualifying
   historical candidate edge and verifier-produced current observation MUST
   instead bind the locked new-event policy or the event conflicts; cross-policy
   document withdrawal remains `PENDING` and no historical policy is rebased;
3. M5-D24-C5 Section 3's final checked-origin exclusion, M5-D24-C6 Section 3's
   exactly-three-origin limit and Section 6's arbitrary-read exclusion, and
   M5-D24-C7 Sections 7 and 9's unlisted-direct-origin exclusion only to add
   Section 4.1's exact hydration-cutoff origin; the two replay envelope shapes,
   canonical result validation, held-receipt projection, work, timing, and
   telemetry rules are not otherwise changed;
4. the two D28 sentences saying that D28 grants no SQL or migration change,
   the matching runtime-addendum Section-25 sentence, and the D28 Section-7/8
   frozen-schema inventory only to authorize the separate migration 018,
   existing-ledger record, and exact two indexes in Section 7; and
5. the D28 activation's prohibition on every migration/index edit only after a
   new D29 authority freeze and a new path-exclusive reactivation explicitly
   replace that activation.

Every other D24--D28, runtime-addendum, migration-013--017, lock-order,
source-first, phased-composition, replay, counter, and claim boundary remains
exact. The same-policy fail-closed rule above is D29's sole semantic
admissibility correction; Section 4.1 is only a runtime provenance completion.
Migration 018 may add no object other than the two Section-7 indexes and its
one schema-ledger record.

## 3. Legacy document source identity

For `source_kind=structural_open` and update kind `document_insert`,
`document_delete`, or `document_replace`, the source identity is exactly:

```text
source_id = locked groundloop_epoch.event_id
source_identity_hash = locked groundloop_epoch.payload_hash
```

The epoch row is locked at tier 5 under the already-held event/CAS prefix.
`groundloop_epoch.payload_hash` is immutable accepted authority. A planner,
replay reader, or private D28 preparation phase MUST NOT reconstruct a legacy
event from relational sidecars and recompute, normalize, reinterpret, or
replace this hash. In particular it MUST NOT call the legacy event payload
recipe over reconstructed `groundloop_document`, document-version, chunk,
metadata-overlay, or deactivation-overlay values.

This does not weaken admission. Before the epoch row is first inserted, the
existing typed opener still validates the caller's legacy event and its exact
frozen payload recipe. After persistence, the locked epoch hash is the retained
identity of those admitted bytes.

The following remain independent mandatory authority, not payload-hash
preimages:

- the exact document-kind mapping and predecessor in `groundloop_m5_update`
  and `groundloop_m4_update`;
- the runtime structural event, predecessor, policy/manifest, registry
  snapshot, active-chunk snapshot, root-set hash, counters, and revision-1
  state;
- the immutable M4 and M5 candidate-policy rows and every manifest binding;
- the exact document, predecessor/successor document-version, ordered chunk,
  chunk-provenance, metadata-overlay, and deactivation-overlay rows required by
  the event kind;
- every requirement/group lifecycle and predecessor-validity row reached from
  the locked snapshots;
- the exact typed-M5 document-declaration commitment in
  `groundloop_m5_update.manifest` required by Section 4;
- every declared fallback/reverse scope, root job, and root-provenance row; and
- the direct M4 declaration/sidecar bijection and every accepted deferred
  guard.

Rows already known before their frozen authority tier, including the new-event
epoch/runtime and structural source sidecars, are point- or affected-set locked
at that tier and byte/value validated against their accepted identities,
foreign keys, digests, intervals, event, epoch, predecessor, policy, snapshots,
roots, and scopes.

A row first named only by a D29 post-tier-7 locator is different. The locator
and any preliminary primary-key read are nonauthoritative. Snapshot members,
scopes, jobs, admitted/source/selection rows, verifier closure, observations,
and currency rows whose frozen tiers remain ahead must later be locked and
exactly revalidated at tiers 8 through 11a as applicable. Historical
epoch/runtime/result and other terminal immutable identity rows whose tier has
already passed may receive only nonlocking primary-key validation; they MUST
NOT be locked, reacquired, or reserved after that tier. Their complete retained
row and digest closure must nevertheless validate exactly. A locator that
would require a mutable or nonterminal earlier-tier row to become authority
conflicts instead of taking a late lock.

No preliminary read is authority. A missing, extra, reordered, cross-event,
wrong-policy, malformed, changed, or noncanonical authority conflicts before
D25 DML even when the epoch payload hash is unchanged. Conversely, exact
sidecars do not authorize a different epoch payload hash. The complete affected
closure is a first-application requirement; historical replay is limited by
Section 6.1 to retained named source/change authority.

## 4. First-application predecessor authority

Except for the explicitly bounded existing-event declaration hydration below,
Sections 4--6 govern first application only. Document withdrawal is evaluated
at the already-locked sealed predecessor publication point `P`. The store validates
equal M4/M5 heads at `P`, the predecessor's sealed identity, and the immutable
requirement-registry and active-chunk snapshot identities retained by that
predecessor. These are not the new event's post-mutation snapshots; the latter
remain independently exact-validated sidecars.

`new_event_candidate_policy_id` below is the one value loaded from the locked
new runtime header and exact immutable manifest and cross-validated with the
typed M4/M5 update sidecars. It is never supplied as withdrawal authority by a
caller.

The frozen public surface is deliberately a two-call surface. Its first
`plan_exact_requirement_withdrawal(event)` call is a nonauthoritative store
preview in a separate read transaction. It retains no lock, opens no
authorization context, and returns no token. The application may use that
preview to propose the requirement roots and requirement-root-set hash and may
likewise pass its deterministic direct payload, withdrawal, roots, and scopes,
but every such argument to `open_typed_event_atomically(...)` is compare-only.

For a D29 document event, the early `groundloop_m5_update.manifest` insert has
exactly this typed value:

```text
JSONB_OBJECT_EXACT(
  "m5_d29_document_declaration_v1",
  JSONB_OBJECT_EXACT(
    "direct_root_job_ids",
      JSONB_ARRAY_TEXT(sorted unique direct root job IDs),
    "direct_scope_root_job_ids",
      JSONB_ARRAY_TEXT(sorted unique impact-root scope IDs),
    "direct_fallback_claim_ids",
      JSONB_ARRAY_TEXT(sorted unique frontier-root claim targets)))
```

There is exactly one top-level key and exactly the three displayed nested keys.
Every array contains only exact strings in `COLLATE "C"` sorted-unique order.
The values are respectively all proposed parentless direct job IDs, all
proposed impact-root scope IDs, and all proposed frontier-root claim targets.
On the absent-event path they are built from the compare-only direct proposal
because the typed update row precedes later authority tiers. They MUST NOT
select a locator, reservation, root, or plan. The same outer transaction later
compares them with the store-recomputed declarations; any difference rolls the
row and all source work back. Only a successfully committed comparison makes
this existing manifest value the retained declaration commitment. Production
code never updates or deletes that commitment. It is compared as a typed JSONB
object and typed arrays; it is never serialized, hashed with JSON/`repr`, or
made a payload-hash preimage. Non-document update manifests and public M4-v1
manifest bytes are unchanged.

On first application, the opener's one outer transaction performs the frozen
event-idempotency and pre-persistence legacy-payload checks, acquires the
head/event/runtime and tier-7 source authority, and makes the exact epoch,
M4/M5 update, manifest, snapshot, and document sidecars available at their
frozen source-first positions. Starting from those locked rows and proceeding
through Section 8's ordered locator, reservation, and validation sequence, it
recomputes Sections 4--6's complete withdrawal plan, the complete direct and
requirement root/scope declarations, and the requirement-root-set hash. It
must exact-type/byte/value/digest compare every supplied preview and
declaration, plus the provisional typed-update declaration commitment, with
those store-recomputed values before any new tier-8 scope, tier-9 root/job, or
D25 DML. A stale, extra, missing, reordered, or different value rolls back the
epoch/source work and conflicts. Only the recomputed values authorize writes.
The opener MUST NOT call the public preview again or use a nested, adjacent, or
helper-owned transaction to perform this recomputation. No lock, object
identity, cursor state, reservation, context, or token crosses the two public
calls, and neither frozen public signature changes.

An exact already-existing event is selected at the opener's initial locked
idempotency gate and follows the retained D28/open-receipt replay authority. It
does not rerun first-application withdrawal against later heads or currency
and never promotes a preview to replay authority. A same-ID wrong payload,
legacy-only row, missing typed closure, or retained declaration conflict fails
atomically.

Because the frozen application invokes both planners before that opener gate,
each of `plan_exact_requirement_withdrawal(event)` and
`plan_direct_open(event)` MUST first pass Section 7's exact migration-018 route
barrier and then point-read `groundloop_epoch` by event ID and compare its
stored payload hash. Absence continues to the ordinary nonauthoritative
preview. A same ID with another payload conflicts. A matching row MUST then
point-read and exact-validate its event-local typed document closure: the M4/M5
updates, runtime header, policy/manifest, snapshot identities, and
event/epoch/predecessor coordinates. A row without that complete typed closure
conflicts instead of being mistaken for a replay.

For any exact existing event reached by a planner, these reads MUST stop before
any current head, current currency, changed-chunk admitted-history,
publication-history, or predecessor-state query. The planner first requires the
exact retained typed-update declaration commitment above. It may read only
immutable declaration fields and immutable provenance keyed by the retained
epoch:

- the requirement planner uses `groundloop_m5_job_by_epoch_state` to enumerate
  every persisted M5 job for the retained epoch across all seven exact job
  states, loads full rows, and explicitly sorts them by
  `logical_job_id COLLATE "C"`. For every enumerated job it point-reads
  `groundloop_m5_discovery_scope` by `root_job_id` and
  `groundloop_m5_requirement_root_provenance` by `(epoch_id,root_job_id)`.
  Every parentless forward or reverse root has exactly one scope, every verifier
  child has none, every document-event forward root has exactly one provenance
  row with `fallback_required=true`, and reverse roots and children have none.
  The same-epoch foreign keys make those per-job points complete. It loads the
  runtime `requirement_root_set_hash` and exact-validates the event, policy,
  manifest, snapshot, scope/job, parent/child, inserted-chunk reverse-root,
  root-set, and provenance identities. It returns the event ID and exact sorted
  deactivated-chunk tuple; candidate, observation, and cancellation tuples are
  empty; `fallback_keys` contains exactly the sorted requirement/policy keys of
  persisted forward roots, and the unchanged plan recipe computes the digest;
- the direct planner uses `groundloop_m4_job_by_epoch` to enumerate every
  `groundloop_semantic_job` row for the retained epoch in all seven exact SQL
  states: `declared`, `running`, `completed_active`, `completed_inactive`,
  `retryable_failed`, `terminal_failed`, and `cancelled`. It loads full rows and
  explicitly sorts them by `job_id COLLATE "C"`; index order is not authority.
  It also uses the existing `groundloop_semantic_job_dependency` primary key to
  enumerate every dependency row with that exact leading `epoch_id`, explicitly
  sorting full edges by `(parent_job_id COLLATE "C",child_job_id COLLATE "C")`.
  The dependency set MUST equal exactly one `(epoch_id,parent_job_id,job_id)`
  edge for every enumerated child and no edge for any root. It point-reads
  `groundloop_discovery_scope` by `root_job_id` for every enumerated job.
  Exactly one scope is required if and only if the job is a
  parentless `impact_discovery` root; every frontier root and every child has no
  scope. Every parentless job must be `impact_discovery` or
  `frontier_retrieve`; every child must be a `verify_pair` child of an
  enumerated root. It exact-validates event, policy, payload, execution,
  parent, target, registry, dependency, child-set, completion/result, and root/
  scope identities under the frozen M4 recipes, including the exact dependency
  bijection and child-set closure, while treating mutable progress fields only
  as continuation state. The sorted parentless job IDs,
  impact-scope root IDs, and frontier-root claim targets MUST respectively
  equal the three retained commitment arrays. It then returns those exact
  roots/scopes with the deterministic structural payload and the existing M4
  empty-reverse-index withdrawal plan for the event's exact deactivated-chunk
  tuple. A missing or extra row, commitment entry, scope, dependency, child,
  parent, or target conflicts; and
- the application-composed M5 declarations reconstructed from those fallback
  keys plus event-intrinsic inserted-chunk roots MUST equal the complete
  persisted declaration set and its retained root-set hash byte for byte.

Those existing-event values authorize no first-application write and create no
work/accounting evidence. They exist so continuation cannot omit a persisted
root. The opener still owns exact-existing classification and MUST ignore an
absent-event preview if the event appeared after its point check.

At the end of either exact-existing planner transaction, after the complete
declaration projection validates, the planner performs one event-local
canonical terminal-result point read. If no result exists, it returns its
frozen plan type. If an exact canonical terminal result for the same event,
payload, and epoch exists, it raises the exact package-private
`_M5D29HydrationTerminalCutoff` signal instead of returning a plan. That signal
has exact type, empty `args`, and no contract field or payload. It is not a DTO,
receipt, result, token, authority value, persisted value, or digest input. A
subclass, nonempty instance, signal raised before declaration validation, or
signal from any other call site is invalid.

Before the opener, the application catches that signal only around the two
planner calls, rereads and validates the canonical C5 ordinary terminal result,
and finishes the terminal invocation without opening. If the result appears
after a planner's terminal point read, the next planner or opener selects it.
If the opener itself returns a terminal receipt, its already-frozen branch
selects and validates the canonical result. None of these pre-opener routes has
received a nonterminal receipt, so they retain the ordinary terminal-known-at-
entry envelope rather than an active projection.

Immediately after a nonterminal `replayed=true` open receipt, the application
MUST invoke `plan_exact_requirement_withdrawal(event)` once more as a read-only
persisted-continuation hydration, reconstruct and exact-validate the retained
M5 roots and root-set hash, and replace every preview-derived requirement
continuation value with those hydrated bytes. This requirement hydration
performs its final terminal check before the application's first post-open
current-revision read. `run_pending_direct(...)` MUST first pass Section 7.1's
route barrier, then validate the exact caller-supplied `expected_revision`
through its frozen current-revision point read, and only then load and validate
the retained direct job/dependency/scope declarations through the same all-job
package-private reader; it MUST NOT call `plan_direct_open` again. Direct
hydration performs its final terminal check in the declaration transaction and
raises the exact signal before any acquisition or external call. The required
revision point read is nonauthoritative, creates no work/accounting evidence,
and is the sole ordering exception; neither frozen signature nor DTO changes.

### 4.1 Checked hydration terminal-cutoff origin

After an actual nonterminal open receipt, the exact signal from either
post-open hydration call is one new checked active-cutoff origin. The
application catches it only at those two literal call sites, requires that the
held receipt is the exact nonterminal receipt returned to this invocation and
that accumulated `call_work` is canonical zero, then calls
`read_typed_event_result(event_id,payload_hash)`. It requires the exact same-
epoch canonical C5 ordinary result with terminal-projected receipt and zero
call work. Only then does it invoke the existing shared active-terminal
projection, overlaying the held nonterminal receipt and the actual zero call
work. It validates that existing envelope and applies unchanged D24 terminal-
invocation timing/coverage and postcommit telemetry.

The signal carries no authority; the canonical reread remains terminal-result
authority. Its checked provenance is the exact completed hydration operation
and literal catch site, not terminal knowledge alone. A missing/malformed
canonical result, wrong event/payload/epoch, terminal-looking held receipt,
nonzero work, signal from another method or phase, arbitrary later result read,
or ordinary terminal envelope returned after the held receipt conflicts. The
direct method raises the signal before constructing an
`M5DirectExecutionReceipt`, so no frozen receipt branch or signature changes.
If terminalization occurs after a hydration transaction returns normally, no
generic read gains authority; the next existing C5/C6/C7 acquisition, return,
failure, barrier, or CAS origin governs the race.

Only post-open retained declarations from a still-nonterminal event drive
acquisition, missing-work dispatch, and root-barrier closure. No token, cursor,
lock, cache, hidden registry, new replay shape, public signature, or DTO crosses
calls. A race in which the event appears after either pre-opener point check is
resolved by the opener gate followed, only for a nonterminal receipt, by
retained-declaration hydration. An event terminalized at an exact hydration cut
reaches retained durable-result authority without reconstructing withdrawal or
executing external work.

On the absent-event first-application branch, immediately after tier-7 source
authority and before any tier-8 acquisition, the opener derives a private
prospective declaration-coordinate superset. It uses only locked event-
intrinsic direct/inserted-chunk inputs plus the store-derived direct-M4,
admitted-pair, and current-currency locator keys; it never uses the caller's
preview to choose a coordinate. For every possible direct or M5 fallback/root
declaration it computes the frozen scope/job IDs and every existing
insert-on-absence advisory and unique-conflict coordinate. This private set is
sorted/unique, cursor/transaction bound, contributes no DTO, counter, or digest,
and its cardinality is exposed only in the Section-8 affected-history audit
trace.

At tier 8 the opener reserves every prospective new scope coordinate alongside
the touched snapshot and existing-scope authority. At tier 9 it reserves every
prospective new direct-M4 job coordinate first and every prospective new M5 job
coordinate second alongside existing jobs. A prospective coordinate may later
be unused only because exact lineage, snapshot activity, provenance, or
deduplication excludes its locator. After tier-11a validation, the exact plan
and declarations must be a subset of those reservations. An unreserved exact
coordinate, or a reservation not derivable from the locked source/locator
superset, conflicts; the transaction never discovers or acquires it late.
Actual declaration inserts use only already-held coordinates at their frozen
staged position.

Let `R_P` and `C_P` denote the predecessor requirement and chunk membership
sets, and let `D` be the sorted unique predecessor chunk IDs deactivated by the
exact locked document/version/chunk rows. The store MUST NOT materialize or
recompute either complete predecessor snapshot. For each touched requirement
or chunk it uses the immutable snapshot header plus one primary-key member
point lookup. It validates the retained header/count/digest identity already
enforced when that immutable snapshot was created; it does not rescan all
members. Every chunk in `D` must have predecessor membership in `C_P`. Insert
has `D=()`.

If `P` is the activation base and has no typed predecessor snapshots, there is
no preactivation admitted-pair history. Only touched-key active membership may
be point-validated against the locked strict activation/current image. The
store MUST NOT scan that image or borrow the new event's post-mutation
snapshot.

The accepted one-live-epoch, equal-head open barrier, and atomic typed-seal
CAS, together with monotonically allocated epoch identity, establish a linear-
publication theorem: an owner with `epoch_id <= P` is on `P`'s unique
predecessor lineage exactly when its point-read epoch/runtime/event-result
closure proves a structurally committed, semantically sealed successful typed
publication. The numeric bound is necessary but not sufficient. Failed or
nonterminal epochs never advance either head. The store applies that theorem
separately to only the owner epoch IDs returned by the changed-chunk locator
ranges. It does not walk the predecessor chain. For an owner that claims a
successful seal, a missing or internally malformed terminal/publication
closure conflicts. A failed, open, or otherwise unsealed owner in its exact
state-appropriate shape, including one allocated after `P`, is classified as
nonlineage and excluded. A successfully sealed owner later than the locked
head `P` contradicts the head/CAS theorem and conflicts.

## 5. Active reverse candidate edges

For each chunk in `D`, the store first performs a nonlocking locator read
through the Section-7 index. The range returns only the exact
`chunk_version_id` and each matching `admitted_pair_digest`, sorted by admitted
digest. Each digest then names one nonlocking primary-key read which must agree
with the locator chunk and gathers the complete admitted row. Neither the index
entry nor the preliminary row is authority.

Each locator is classified in this order:

1. point-read its owner epoch/runtime/result closure and apply Section 4's
   linear-publication theorem; a well-formed nonlineage row is ignored;
2. point-test its requirement in `R_P` and chunk in `C_P`; a well-formed row
   missing either predecessor membership is inactive history and is ignored;
3. for a qualifying row, gather its tier-8 scope, tier-9 root/job, and tier-10
   admitted/source/selection coordinates, then lock/revalidate them only at
   those frozen tiers; and
4. require `subject_kind=requirement`, exact semantic-pair recomputation, the
   immutable candidate-policy manifest, every admitted-pair source, reasons,
   owner, selection, and the existing admitted-pair digest. The qualifying
   historical policy MUST equal `new_event_candidate_policy_id`; another policy
   conflicts because cross-candidate-policy document withdrawal is not
   authorized by the current Task-2 path. Any malformed qualifying closure
   conflicts before D25 DML.

Thus unrelated or inactive but well-formed history does not poison a later
withdrawal, while malformed authority that would contribute to this
withdrawal cannot be hidden by deduplication.

The private qualifying evidence identity is:

```text
(chunk_version_id, requirement_version_id,
 historical_candidate_policy_id, semantic_pair_digest,
 admitted_pair_digest)
```

The active reverse-candidate evidence set is the sorted-unique sequence of
those five-tuples in the displayed C-order. All historical policy, semantic-
pair, and admitted identities remain in that cursor-local evidence. This
preserves provenance without changing a DTO or digest. Migration 015 permits
only the new runtime epoch's single `candidate_policy_id` on every new
scope/job, and the accepted Task-2 path does not authorize cross-candidate-
policy fallback. Therefore equality is mandatory rather than an implicit
rebase. The operational existing candidate edge is:

```text
(semantic_pair_digest, requirement_version_id, chunk_version_id,
 new_event_candidate_policy_id)
```

Duplicate sealed-lineage admissions under that one policy collapse to one
sorted-unique operational edge for the semantic pair. Every distinct admitted
identity is still validated and remains in the transient evidence. Cross-policy
history conflicts and requires a separately scoped cross-candidate-policy
withdrawal decision; it is never silently collapsed or rewritten. No current
observation is required for this edge.

The existing withdrawal plan stores sorted-unique semantic-pair digests, and
its fallback key is the unchanged validated
`(requirement_version_id,new_event_candidate_policy_id)` key. It never creates
a scope/job under a different historical policy.

## 6. Current observation edges and cancellation

For each chunk in `D`, the store first uses the existing
`groundloop_current_observations_by_chunk` index for a nonlocking locator read
of current `groundloop_observation_currency` keys with
`subject_kind=requirement`. The locator is not authority. At frozen tier 11a
the store point-locks/revalidates each current row and the exact unclosed
`groundloop_published_observation_currency` interval at `P`; the full currency
key and observation ID must agree. An interval scan is not authority, and
migration 018 adds no observation index.

Before tier 11a, each observation locator is assigned exactly one currently
supported immutable provenance branch:

1. **Verifier-produced.** Observation to
   `groundloop_m5_requirement_verifier_execution` to
   `groundloop_m5_requirement_pair_input` is complete. The store recovers the
   historical candidate-policy ID from the pair input and validates the exact
   artifact, semantic pair, job, attempt/result, eligibility, produced epoch,
   sealed predecessor lineage, and manifest closure. The recovered historical
   policy MUST equal `new_event_candidate_policy_id`.
2. **Activation bootstrap.** There is no verifier execution and no typed
   producing runtime/update closure. The observation is an exact current
   requirement observation in the installed strict activation-base image,
   `produced_epoch <= base_m4_epoch_id`, and predates typed runtime history. Its
   historical candidate policy is explicitly absent. Exact activation/base,
   observation, currency, task, eligibility, and touched requirement/chunk
   membership must validate.

The rootless `ObserveRequirementEvent` form remains deferred by D28. A locator
with a typed rootless producing epoch conflicts in D29 instead of activating or
partially implementing that public event form. A later explicit rootless
activation may replace only this fail-closed branch after its own contract and
path-exclusive implementation gate.

A row matching zero or more than one supported branch conflicts. Every branch
requires the touched requirement and chunk to be active at `P`. The verifier
policy is cursor-local provenance and is equality-checked; bootstrap policy
absence is exact activation provenance, not permission to infer an old policy.
As required by the one-event-policy invariant, the existing operational
observation edge is:

```text
(observation_id, requirement_version_id, chunk_version_id,
 new_event_candidate_policy_id)
```

It is sorted/unique under the existing DTO rules. A verifier-produced row is
never rebased from another historical policy; a bootstrap row uses the event
policy only for its new operational edge while retaining explicit historical
policy absence. Candidate and observation edges remain independent; neither is
synthesized from the other.

Every valid supported current requirement-currency key is an observation edge
regardless of task. In particular, an eligible noncanonical-task activation-
bootstrap observation remains in the withdrawal observation IDs and produces
the event-policy fallback key. Its currency holder is withdrawn, but because
D25 already treats that task as matching-inert, it causes no D25 physical
observation-membership, refcount, mask, or Hall removal. An ineligible
observation cannot lawfully be a current holder and conflicts.

For supported Task-2 structural open, `cancelled_job_ids` is exactly empty
only after the sealed predecessor header/counters and existing epoch/state
indexes prove zero open/nonterminal direct-M4 jobs, M5 jobs, discovery scopes,
and owner/answer PENDING authority. These are indexed zero-existence probes,
not aggregate or full-relation scans. A D24-valid unresolved dispatch/attempt
whose job, scope, and predecessor epoch are already terminal is historical
at-least-once ambiguity, not nonterminal semantic authority: it is permitted,
is not a new cancellation target, and remains eligible only for D24's exact
late postterminal audit/archive route. Historical terminal jobs are immutable
and are not cancelled again. Any actual nonterminal predecessor authority
conflicts instead of being cancelled in the new event.

Inside the opener, the final existing `M5RequirementWithdrawalPlan` is
recomputed from exact `D`, the two operational projections, validated empty
cancellation, touched predecessor membership, and the one new-event policy.
Delete/replace declares at most one fallback root for each touched active
requirement under that policy. Insert has empty
withdrawal/fallback/cancellation projections. The existing plan fields and
digest recipe are unchanged.

Counter and digest ownership is unchanged. `withdrawn_candidate_edge_count` is
exactly the cardinality of the final sorted-unique withdrawn semantic-pair-
digest tuple. `withdrawn_current_observation_count` is exactly the cardinality
of the final sorted-unique withdrawn-observation-ID tuple, including an eligible
matching-inert noncanonical-task holder. Cancellation remains zero here.

Beyond those unchanged logical output counts, D29's locator ranges, historical
source-prefix reads, snapshot and authority point reads, zero-existence probes,
prospective tier-8/tier-9 reservations, and sorting or deduplication of their
private keys contribute no D29-specific increment to any `M5RuntimeWork` or
`M5OverlayWork` field. In particular they do not increment D25
`ordered_policy_range_probes`, `ordered_index_operations`, or
`canonical_sort_items`; those fields retain their frozen M5-T2 overlay/kernel
meanings. Actual downstream matching operations, persisted writes, and frozen
structural-open serialization continue to contribute exactly the D24/D25 work
already assigned to them.

All authoritative D29 first-application planning executes inside the opener's
existing sole `structural_open` transition-call timing point. When observed,
its aggregate database cost may appear only in the already-frozen PostgreSQL
timing/physical fields and existing timing identity. The separate compare-only
preview and existing-event declaration hydration are read-only current-
invocation work; if the frozen measurement boundary observes them, they may
appear only in the existing `call_timing`, never as event work or a new
transition point. D29 creates no second timing anchor or new timing field.
`EXPLAIN` plans and per-query locator, point, probe, reservation, and planner-
sort cardinalities are separate audit/evaluation evidence only; they enter no
work, timing, patch, result, or semantic digest.

### 6.1 Historical replay boundary

Historical replay does not execute Sections 4--6. It validates the retained
structural source ID and locked epoch payload hash, immutable historical
document/M4/M5 source sidecars at the patch's recorded coordinates, the D28
canonical retained changed-key projection, patch artifact, historical
contribution, and current retained accumulator under D28 Section 6.

The Section-4 event-ID/payload check and event-local immutable declaration/
provenance hydration are permitted replay and nonterminal-continuation routing,
not withdrawal reconstruction. They read no head, current currency, changed-
chunk admitted history, publication history, predecessor state, or original
transient plan. Ordinary terminal replay uses the durable result before either
planner. If terminalization races the initial result lookup, a planner or post-
open hydrator may finish only the same immutable event-local declaration
projection; the opener or immediate event-local result check then selects the
durable result before any continuation work. Nonterminal continuation uses only
the exact retained declarations after the opener selects the existing event.

Replay may point-validate a historical currency interval named by retained
change/source authority and may validate the persisted structural root closure
through its already-frozen event/epoch-prefix route. It MUST NOT require
today's M4/M5 heads to equal the old predecessor, query today's current-
currency map, rerun a changed-chunk admitted-pair range, reconstruct the
original withdrawal plan, regenerate a no-op candidate key, or require the
original transient first-application lock plan. A later sealed head or changed
current holder is therefore valid replay context. Missing or changed retained
historical authority conflicts. Valid replay performs zero semantic, event,
withdrawal, or history DML, zero provider/model calls, and zero history
regeneration. It may append exactly the frozen D24 timing/coverage-only
`groundloop_m5_postcommit_invocation_telemetry` row for the current invocation;
that row is not event, work, result, history, or digest authority.

## 7. Migration 018 authority

The only authorized migration path is:

```text
migrations/018_m5_bounded_document_withdrawal.sql
```

Its only DDL objects are exactly, in this creation order:

```sql
CREATE INDEX groundloop_m5_admitted_pair_by_chunk_edge
    ON groundloop_m5_requirement_admitted_pair (
        chunk_version_id COLLATE "C"
    );

CREATE INDEX groundloop_m4_job_by_epoch
    ON groundloop_semantic_job (epoch_id);
```

No other table, column, index, constraint, function, trigger, type, privilege,
or data backfill is authorized. Both indexes are locators only. The admitted-
pair index does not make a history row current and does not replace the point
validation, lineage, snapshot, digest, or logical-dedup rules above.

Each changed-chunk locator query constrains `chunk_version_id COLLATE "C"` to
one exact value, returns only `(chunk_version_id, admitted_pair_digest)`, and
explicitly orders the returned digests by `admitted_pair_digest COLLATE "C"`.
The one-key shape is mandatory: `chunk_version_id` is already accepted by its
existing primary-key index, while adding any other unbounded or fixed-width
identity to this locator could make migration 018 reject a row that migrations
001--017 accept under PostgreSQL's B-tree tuple-width limit.

Each returned digest names one nonlocking admitted-pair primary-key read. That
row must agree with the locator chunk and supplies epoch, subject, historical
policy, semantic pair, owner, reasons, and the remaining closure coordinates.
The complete gathered rows are then independently sorted/unique by Section 5's
five-field evidence identity. At tier 10, every qualifying admitted row is
locked by primary key in C order and every column is exact-compared with the
gathered row before its source closure can authorize withdrawal. Index order
and preliminary reads are never row authority. A multi-column, `INCLUDE`,
predicate, or expression form is not conforming.

Each retained-direct declaration query constrains `epoch_id` to one exact
value, uses `groundloop_m4_job_by_epoch`, reads the full matching job rows in
all seven Section-4 states, and explicitly sorts those rows by
`job_id COLLATE "C"`. It also performs one exact-epoch prefix range through
existing `groundloop_semantic_job_dependency_pkey` and explicitly sorts
the returned full edges by parent then child ID in C order. The edge set must
equal the parent relation in the enumerated child rows exactly. It point-reads
discovery scope by the existing `root_job_id` primary key for every returned
job. Exactly one scope exists if and only if that job is a parentless
`impact_discovery` root; every frontier root and child has none. Because the
same-epoch foreign keys require every dependency endpoint and every scope to
name one of the enumerated jobs, the dependency range and per-job points prove
edge and scope completeness without either relation scan. The retained manifest
independently commits the exact root, scope, and fallback-target sets. Index
order and scope absence are not assumed. Migration 018's job-index key is only
the fixed-width `epoch_id`; no unbounded job, claim, target, or scope identifier
enters its B-tree tuple.

Each retained-requirement declaration query constrains `epoch_id` to one exact
value and uses existing `groundloop_m5_job_by_epoch_state` across all seven job
states to enumerate full M5 job rows. It explicitly sorts them by
`logical_job_id COLLATE "C"` and point-reads
`groundloop_m5_discovery_scope_pkey` and
`groundloop_m5_requirement_root_provenance_pkey` once for every job. The same-
epoch scope/job and provenance/scope
foreign keys make those points complete. The root subset, scope bijection,
forward-only provenance set, and recomputed root-set hash must equal the frozen
runtime declarations exactly. These are existing migration-015/016 indexes and
add no migration-018 object.

Migration 018 must require the exact accepted migration-017 ledger row:

```text
accepted_017_bundle_id = "m5-persisted-matching-schema-bundle-v1"
accepted_017_bundle_sha256 = 52240e19968926d0c051fe6146b3c7d877cf582014341efbfcc78637f3ff5761
accepted_017_migration_sha256 = e387b01fa80145273ba40d2d83581bc54762d3a4a2edd34669c095076c52154c
accepted_017_oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
accepted_017_prerequisite_sha256 = 28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565
```

Let `migration_018_sha256` be SHA-256 of the exact migration bytes. Its ledger
identity is:

```text
bundle_id = "m5-bounded-document-withdrawal-schema-bundle-v1"
bundle_sha256 = stable_m5_digest(
  "m5-bounded-document-withdrawal-schema-bundle-v1",
  *TEXT("migrations/018_m5_bounded_document_withdrawal.sql"),
  *HASH(migration_018_sha256),
  *HASH(accepted_017_bundle_sha256))
migration_sha256 = migration_018_sha256
oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
prerequisite_sha256 = accepted_017_bundle_sha256
```

The empty oracle hash is required because migration 018 adds no oracle member.
Before first-install implementation may be accepted, its handoff MUST record
literal `migration_018_sha256` and `bundle_sha256` values recomputed from the
reviewed migration bytes. The installer MUST pin and compare both literals on
an absent-ledger first install; a caller-derived, checkout-derived, placeholder,
wildcard, or current-ledger value cannot become accepted bytes.

The installer owns one top-level read-write `READ COMMITTED` transaction,
performs the ledger-first exact-rerun/conflict decision, validates all five
accepted-017 literals before DDL, and then attempts exactly:

```sql
LOCK TABLE groundloop_m5_requirement_admitted_pair,
           groundloop_semantic_job
    IN SHARE ROW EXCLUSIVE MODE NOWAIT;
```

The table list and lock mode are exact and canonical. After both locks it re-
reads the migration-018 ledger identity before any DDL: an exact row returns
the ordinary no-op result, a differing row conflicts, and only continued
absence may build the indexes. The installer creates both indexes in the
displayed order and the ledger row atomically and leaves none of the three on
failure. An exact initial rerun takes neither target-table lock. A same-ID
differing row, missing/mismatched prerequisite, ambient/nested/savepoint/read-
only/wrong-isolation invocation, unavailable required lock, invalid UTF-8/C-
order environment, or failure before, between, or after the two index builds
conflicts without partial state or in-transaction retry. A caller may retry
only in a new transaction from the ledger-first decision.

### 7.1 Migration-018 runtime route barrier

After migration-018 implementation bytes and their literal ledger hashes are
accepted, every future activation exposing D29 document routes, every D29
legacy-document preview planner, first typed open, and nonterminal typed resume
MUST verify the exact accepted migration-018 ledger row. Each pre-opener planner
performs that check before its event-ID/epoch point read. The opener
independently repeats it before first-application event-ID/epoch consumption.
Post-open requirement hydration verifies it before its declaration range and
before the application's first post-open current-revision read. The direct
runner independently verifies it at method entry before validating its supplied
revision; its package-private reader then performs the direct declaration ranges
after that exact revision read and before acquisition or external work. Missing
or one-field-different 018 authority conflicts; no route may fall back to a
sequential scan.

The application's initial durable-result lookup remains before this barrier.
An exact result found there may perform frozen audit/terminal replay without
migration 018 and invokes neither planner nor hydration. The only write that
route may perform is D24's optional append-only postcommit invocation timing/
coverage telemetry; it changes no event, work, result, or logical identity. If
that initial lookup did not find a terminal result, a later terminalization
does not waive the already-required 018 check; Section 4's race route then uses
only event-local declarations and the durable result. An already activated
database without exact 018 authority may perform audit and ordinary terminal
replay, including that D24 telemetry, but cannot preview, first-open, or
nonterminally resume a D29 document mutation. Never-activated public M4-v1
behavior remains unchanged.

## 8. Bounded SQL and lock order

Document first application retains the frozen D28 order and separates locator
reads from authority locks:

1. hold the already-frozen head/event tier-1--5 prefix, exact legacy source,
   and tier-6 serializer/context; then lock document, document-version, chunk,
   group/requirement lifecycle, and deactivation rows in exact tier-7 order;
2. derive sorted-unique `D` and perform only nonlocking chunk-index locator
   reads for admitted pairs and current currency. Nonlocking primary-key reads
   classify candidate owner epochs under Section 4 and collect every lower-
   tier provenance key. These locator reads confer no authority. Sealed owner
   epoch/runtime/result rows are immutable, so this post-tier-6 lineage proof
   does not acquire an earlier lock. From the event-intrinsic and direct/M5
   locator keys, derive and bind Section 4's exact prospective declaration-
   coordinate superset without consulting the preview;
3. at tier 8, point-validate only touched predecessor snapshot members, then
   lock all required existing discovery scopes and reserve every prospective
   new-scope absence/advisory/unique-conflict coordinate in `COLLATE "C"` key
   order;
4. at tier 9, lock all required existing direct-M4 jobs first and M5 jobs
   second, each by canonical job ID, including every candidate-source root and
   verifier job known from the locator phase, and reserve every prospective
   new direct-M4 job coordinate first and M5 job coordinate second with all
   applicable absence/advisory/unique-conflict keys; an activation-bootstrap
   observation invents no provenance job, while a rootless typed source
   conflicts under Section 6;
5. at tier 10, lock/revalidate the complete already-gathered closure in the
   accepted order: attempts and outputs/results, dependencies, discovery
   results, channel hits, selections, admitted pairs and their source rows,
   verifier executions, verifier artifacts, pair inputs, then frontier heads.
   For each verifier closure this retains the accepted attempt/result ->
   execution -> verifier -> pair-input suborder. No tier-8 or tier-9 key may
   be discovered or acquired after this step;
6. at tier 11a, lock semantic observations first and then the exact current
   and published currency point keys in the accepted typed-key order. Only
   these locked rows, not the earlier chunk-range hints, authorize current
   observation edges; and
7. finish the store-recomputed withdrawal plan and direct/requirement
   declarations, require every exact scope/job coordinate to be in the bound
   prospective reservations, compare every two-call proposal and the root-set
   hash, and only then continue with D28's current/working image locks, sole
   bounded representative discovery, remaining tier-11c--14 locks,
   preparation, staged declaration inserts, D24 accounting, and D25
   finalization without acquiring or reacquiring an earlier key. No tier-8 or
   tier-9 coordinate may first be discovered, reserved, locked, or acquired
   after step 4.

The additional permitted bounded reads and coordinate reservations are exactly:

1. primary-key/unique point reads for touched heads, epochs, terminal results,
   snapshots, sidecars, policies, provenance, jobs, observations, and
   intervals;
2. one Section-7 admitted-pair chunk-prefix range per changed chunk;
3. one admitted-pair-source primary-key-prefix range per qualifying admitted
   pair, followed by point reads of its named selections;
4. one existing current-currency chunk range per changed chunk;
5. on an exact-existing planner or hydration route only, one Section-7 all-job
   epoch range and one existing dependency-primary-key epoch-prefix range per
   direct projection, followed by one discovery-scope primary-key point read per
   enumerated job and mandatory explicit full-job and dependency-edge sorts;
6. on an exact-existing planner or hydration route only, one existing
   `groundloop_m5_job_by_epoch_state` epoch-prefix range per requirement
   projection across all seven job states, followed by one M5 discovery-scope
   and one root-provenance primary-key point read per enumerated job and the
   mandatory explicit full-job-row sort;
7. `LIMIT 1` zero-existence probes using the existing epoch/state job and
   scope indexes plus point/header counters for owner/answer PENDING authority;
8. the existing absence/advisory/unique-conflict point reservations for the
   prospective declaration coordinates; and
9. the already frozen affected-group and bounded representative reads.

Every changed chunk and every gathered or reserved key set is sorted/unique
before its authority tier. For boundedness evidence, the trace must expose every
row returned by both changed-chunk locator families, every exact-existing
direct all-job row, dependency edge, and per-job scope point, every exact-
existing M5 all-job row and per-job scope/provenance point, every qualifying
admitted-source-prefix row, every touched snapshot or authority point read,
every distinct prospective
declaration reservation, every indexed zero probe, and every input item to a
D29-private sort. This is an affected-history query-cardinality boundary only,
not a persisted counter or digest preimage. Neither a complete predecessor
snapshot nor the complete publication lineage is traversed.
`EXPLAIN` must prove the named indexes. A sequential scan or a bitmap plan that
degenerates to a full-relation read is not accepted measured evidence; a
bounded bitmap index/heap plan over only the changed-chunk ranges is not
rejected merely for being bitmap. No full admitted-pair, epoch, currency, job,
dependency, scope, provenance, observation, repository, contribution, or oracle
scan is allowed.

M5-D29 changes no D28 staged/finalizer position, authorizer, context, journal,
header advance, direct 15c protocol, D24 15d--15h accounting, D25 15i--15k
order, replay projection, or accumulator interpretation.

## 9. Non-change boundary

M5-D29 introduces no:

- public signature, persisted row family, schema column, DTO, enum,
  source/reference kind, counter, result field, or semantic/runtime digest
  recipe. The exact Section-4 key in the existing typed-update JSONB manifest
  and the exact package-private empty-argument hydration-cutoff signal are the
  only private additions; neither is a public/frozen field, DTO, digest input,
  or authority carrier. Sections 4 and 8 otherwise change only planning/replay
  routing and private cursor-local lock composition, and Section 7's schema-
  ledger identity is the sole new custody hash;
- tombstone table, nullable hash, `repr`/JSON hashing, seventh changed-state
  kind, or change to a present/absence recipe;
- mutation of migrations 013--017 or their ledger rows;
- process cache, retained engine, `M5IncrementalOverlay`, full repository
  hydration, full oracle, aggregate scan, hidden reserve, or caller-authored
  authoritative affected key/patch/plan; the frozen compare-only preview is
  not authority;
- provider/model/retrieval call inside a database transaction;
- public runtime activation, deployment, timing/performance result, utility,
  objective-truth, security, novelty, superiority, or AI-quality claim.

Runtime remains `v1_only` outside isolated disposable fixtures. D24 through
D29 implementation, Task 2, M5.4 and later gates, deployment, and AI-quality
claims remain `PENDING`.

## 10. Mandatory falsifiers

Implementation or authority evidence is rejected unless every applicable case
passes without skip, xfail, no-match, or silent deselection:

1. **Source identity.** Insert/delete/replace uses the locked epoch event ID
   and payload hash verbatim. Changing the hash conflicts; changing any
   sidecar conflicts independently. A fixture whose exact admitted payload
   cannot be reconstructed from normalized sidecars succeeds only with the
   retained hash and exact sidecars. Any call to legacy payload recomputation
   in persisted document planning/replay fails a static/runtime tripwire.
2. **Sidecar closure.** One-field mutations across every M4/M5 update, the exact
   D29 typed-update declaration commitment, runtime, policy/manifest, snapshot,
   document/version/chunk/provenance,
   metadata/deactivation, lifecycle/validity, scope/root/provenance, and
   direct-sidecar coordinate fail before D25 DML despite an unchanged payload
   hash.
3. **Lineage filtering.** Identical admitted pairs in sealed predecessor
   lineage, failed history, a live/open epoch, and a later failed epoch prove
   the linear-publication point theorem: only the sealed lineage row qualifies;
   every well-formed nonlineage row is excluded. Numeric ordering alone, a
   predecessor-chain walk, or a relation scan fails. A malformed qualifying
   terminal/publication closure or a forged successful seal later than locked
   `P` conflicts.
4. **Historical dedup and policy equality.** Repeated valid admissions for one
   semantic pair under the locked new-event policy collapse only after every
   admitted digest and source is validated. A malformed qualifying duplicate or
   any qualifying admission under another policy conflicts before D25 DML. The
   transient evidence retains every admitted identity; no historical policy is
   rebased, rewritten, or silently collapsed into the new event policy.
5. **Snapshot activity.** Candidate rows with inactive requirement, inactive
   chunk, or membership in only one predecessor snapshot are well-formed
   inactive history and are excluded. A qualifying row with wrong subject
   kind, semantic pair, policy manifest, admitted digest, or source closure
   conflicts. A valid row with both point memberships is included without
   enumerating either complete snapshot.
6. **Candidate without observation.** A valid active admitted pair under the
   same candidate policy and having no current observation is still withdrawn
   and yields the exact unchanged requirement/policy fallback key.
7. **Observation provenance.** Verifier-produced and activation-bootstrap
   current observations each select exactly their one Section-6 branch. The
   first recovers a policy equal to the new-event policy from execution/pair
   input; the second proves explicit historical policy absence from the exact
   installed activation image. Zero/multiple branches, another verifier policy,
   changed pair input/artifact/job/attempt/result, invalid bootstrap authority,
   typed rootless provenance, an ineligible current holder, or a closed/wrong
   published interval fails. An eligible noncanonical-task bootstrap holder
   remains an exact withdrawn observation and fallback input while producing
   zero D25 physical matching removal. Public rootless open/replay remains
   rejected and its deferred D28 event-form gate remains `PENDING`.
8. **Independent projections.** Candidate-only, observation-only, both, and
   neither cases produce exact existing withdrawal fields without
   synthesizing one edge family from the other.
9. **Cancellation boundary.** A sealed predecessor with zero open/nonterminal
   M4/M5 jobs, scopes, and owner/answer PENDING authority produces exact
   `cancelled_job_ids=()`. Separate direct and requirement fixtures whose
   cancellation-first predecessor has a terminal cancelled job/scope but a
   D24-valid unresolved durable dispatch/attempt also succeed with that exact
   empty tuple; the ambiguity record remains intact,
   a later exact provider return uses only D24's postterminal archive route, and
   neither is re-cancelled. A provider that never returns cannot block the later
   document event. Any actual nonterminal predecessor authority conflicts;
   historical terminal jobs remain byte-identical.
10. **Event forms.** Insert produces empty withdrawal/fallback/cancellation;
    delete and replace enumerate exactly their deactivated predecessor chunks,
    observations, candidate pairs, and at most one fallback root per touched
    requirement under the new event policy. Changed first-application
    relational before images conflict before write.
11. **Index bytes, point validation, and retained declaration.** Fresh migration
    018 creates exactly `groundloop_m5_admitted_pair_by_chunk_edge` and
    `groundloop_m4_job_by_epoch`, in that order, and no other schema object. The
    first has sole key attribute `chunk_version_id COLLATE "C"`, with no second
    key, `INCLUDE` attribute, predicate, or expression. The second has sole key
    attribute `epoch_id`, with no `INCLUDE` attribute, predicate, or expression.
    Catalog evidence proves `indnkeyatts=1`, `indnatts=1`, `indpred IS NULL`,
    and `indexprs IS NULL` for each. A populated fixture whose long chunk key is
    already valid under the existing chunk primary key and whose requirement/
    subject, job, claim, target, and scope IDs are long but schema-valid succeeds
    during installation and on post-install inserts; only fixed-width `epoch_id`
    enters the all-job index.

    `EXPLAIN` proves one named chunk-equality range per changed chunk and the
    existing current-currency chunk index. The admitted range emits only chunk
    and admitted digest, may use a bounded explicit Sort, performs one admitted-
    pair primary-key read per digest, independently sorts full rows into the
    five-field evidence order, and at tier 10 locks and revalidates the complete
    row/source closure. A multi-column/`INCLUDE` form, a full-relation read,
    index data treated as authority, missing point validation, or a second
    observation index fails. A bounded index/bitmap plus heap and Sort plan over
    only changed-chunk ranges remains eligible.

    `EXPLAIN` also proves exact epoch equality through the named all-job index
    and through existing `groundloop_semantic_job_dependency_pkey`. Full job
    rows in each exact state -- `declared`, `running`,
    `completed_active`, `completed_inactive`, `retryable_failed`,
    `terminal_failed`, and `cancelled` -- are explicitly sorted by job ID, and
    full dependency edges are explicitly sorted by parent then child ID.
    Discovery scope is primary-key point-read once for every enumerated job.
    Exactly one scope exists if and only if a job is a parentless
    `impact_discovery` root; parentless `frontier_retrieve` roots and every
    `verify_pair` child have none. The same-epoch foreign key plus total epoch
    enumeration makes an extra closed child scope visible. The dependency range
    equals exactly one edge matching each child's `parent_job_id` and no root
    edge; a valid expected A-to-C edge plus an extra schema-valid B-to-C edge is
    detected. A missing or extra root, scope, child, dependency, parent, or
    target; an invalid parentless or child job shape; or a scope attached to a
    frontier root or child conflicts. Parent child-set/dependency/result closure
    is recomputed and exact-validated where present. A semantic-job, dependency,
    or discovery-scope relation scan fails.

    `EXPLAIN` separately proves one exact-epoch prefix range through existing
    `groundloop_m5_job_by_epoch_state` for requirement hydration. Full M5 job
    rows in all seven exact job states are sorted by logical job ID, followed by
    one `groundloop_m5_discovery_scope_pkey` and one
    `groundloop_m5_requirement_root_provenance_pkey` point for every enumerated
    job.
    Fixtures cover the six exact scope states -- `open`, `result_staged`,
    `closed_active`, `closed_inactive`, `terminal_failed`, and `cancelled` --
    and prove the complete root/scope bijection, forward-only provenance set,
    and retained root-set hash. Missing/extra roots, child-shaped roots, scopes
    on children, provenance on reverse roots or children, and missing/false/
    extra forward provenance conflict. An M5 job, scope, or provenance relation
    scan fails.

    The retained typed-update manifest has exactly one top-level key and the
    exact three nested text-array keys from Section 4, with no missing or extra
    key, non-text member, duplicate, or non-C-sorted member. Its arrays equal
    the independently projected parentless-root IDs, impact-scope root IDs, and
    frontier-root claim targets. Missing or extra self-consistent frontier and
    impact roots and missing, extra, malformed, duplicate, or reordered
    commitment members conflict. On first application, proposal-derived
    provisional bytes select no row or plan and commit only after comparison
    with the store-recomputed projection in the same outer transaction; an injected mismatch
    rolls back the typed update and all source work. Production code has no
    update/delete route for the commitment, and no serialization, JSON hash, or
    `repr` comparison may replace typed JSONB key/array validation.
12. **Migration ledger.** Golden pinned first-install migration/bundle identity,
    arbitrary-byte rejection, fresh install, exact rerun, same-ID conflict,
    each one-field accepted-017 mismatch, wrong transaction mode/isolation,
    concurrent installers, the exact one-statement
    `SHARE ROW EXCLUSIVE NOWAIT` lock over both canonically ordered target
    tables, commit-between-ledger-reads race, either-table lock conflict and
    release, new-transaction retry, injected `CREATE INDEX` failures, and
    failure before, between, or after the two index creations prove atomic DDL
    plus ledger behavior and the exact empty oracle hash. Exact initial rerun
    takes neither target-table lock.
13. **Oracle independence.** Python and SQL semantic oracle bytes and outputs
    remain unchanged, contain no migration-018 index dependency, and agree
    before/after install; physical/provenance audit independently detects each
    seeded candidate/observation provenance error.
14. **Frozen inventory.** Public signatures, DTOs, all digest vectors, source
    and reference enums, counters, migrations 013--017, M4-v1 golden bytes,
    D28 phase/replay behavior, and runtime mode remain byte-identical.
15. **No hidden expansion.** Static/runtime tripwires reject caller plans,
    except as nonauthoritative two-call comparison inputs, and reject process
    caches, full hydration/oracles, aggregate scans, model calls, payload
    reconstruction, a third index, and any new table/function/trigger/DTO/
    semantic-or-runtime-digest/counter/public field. The exact existing-manifest
    key and exact package-private no-payload signal in Section 4, plus the exact
    Section-7 schema-ledger custody identity, are the only additions.
16. **Crash and race.** Failure after either locator range, every point
    validation, plan creation, authorization, each D28 stage, either migration
    index creation, and ledger insertion exposes only the complete before or
    after image. Both same-chunk delete/replace races serialize under the
    frozen epoch/header locks with one exact winner and no lost withdrawal.
17. **Terminal replay.** After a later seal changes heads and current currency,
    fresh-process terminal replay validates the historical epoch hash, retained
    immutable source sidecars, D28 changed-key projection, patch, contribution,
    and current accumulator without executing Sections 4--6. A current-head
    equality check, current-currency read, admitted-pair range, original
    withdrawal/no-op-plan reconstruction, regeneration, declaration hydration,
    or semantic/event/withdrawal/history write fails; changed retained authority
    conflicts atomically. Frozen D24 may append exactly its timing/coverage-only
    postcommit invocation-telemetry row, which changes no event, work, result,
    history, or digest. The only projection exception is a terminalization that
    occurs after the initial result lookup: the reached planner or post-open
    hydrator may complete only Section 4's immutable event-local declaration
    validation and exact terminal-cutoff signal route before the application
    returns the canonical durable result, with no first-application/current-
    state read, acquisition, or external work.
18. **Lock order.** Trace evidence distinguishes every nonlocking preliminary
    read from authority locks and proves nonauthoritative locator reads,
    touched snapshots/existing scopes plus prospective new-scope reservations
    at tier 8, existing and prospective direct-M4 then M5 job coordinates at
    tier 9, the complete accepted tier-10 closure, semantic observations then
    currency at tier 11a, and remaining D28 tiers in order. A tier-8/9
    coordinate first discovered/reserved/acquired after tier 9, an
    authoritative currency read before tier 11, or a historical tier-5/tier-6/
    terminal-result row first discovered after tier 7 read with `FOR UPDATE`,
    advisory-locked, reserved, or reacquired fails before D25 DML. Every
    discovered row whose tier remains ahead is locked and fully revalidated at
    that exact tier; needing a mutable earlier-tier row conflicts.
19. **Bounded affected history.** Query trace reports every changed-chunk
    locator row, exact-existing direct all-job row, dependency edge, and per-job
    scope point, exact-existing M5 all-job row and per-job scope/provenance point,
    qualifying admitted-source-prefix row, touched snapshot or authority point,
    indexed zero probe, prospective declaration reservation, and D29-private
    sort input.
    Tests with large unrelated snapshots, publication history, admitted history
    for other chunks, and root/child job tables prove no complete snapshot,
    lineage, job, scope, or other relation traversal; deleting a hot chunk
    exposes its actual affected-history query cardinality without calling it a
    persisted work counter.
20. **Counter non-aliasing.** Additional well-formed nonlineage, inactive, or
    historically duplicate locators may increase only separate query-trace
    cardinality and observed database timing. When they do not change the final
    operational withdrawal or downstream patch, the complete D24 and D25 work
    vectors and digests remain byte-identical. Qualifying operational candidate
    and observation edges change only their existing logical counts; actual
    downstream overlay operations retain their frozen D25 counters. No locator,
    source row, point read, zero probe, reservation, or D29-private sort item is
    assigned to another public counter.
21. **Two-call authority.** The preview call retains no lock/context/token.
    After it returns, independently race a predecessor-head advance, a current-
    currency change, and a newly visible qualifying admitted-history locator,
    and mutate each supplied withdrawal field, direct
    payload/withdrawal/root/scope, requirement declaration, ordering
    coordinate, and root-set hash. On first application the opener recomputes
    all authority after its source/tier-7 locks, detects every stale or
    different proposal before new root or D25 DML, and rolls back its source
    work. An exact proposal succeeds but cannot influence the recomputed bytes.
    A spy proves exactly one pre-opener public preview per planner and no nested/
    adjacent first-application recompute transaction.

    For an exact existing nonterminal DELETE, persist both M5 fallback-forward
    roots and direct frontier roots with no inserted roots, crash immediately
    after open, and resume in a fresh application instance. For REPLACE, combine
    those fallback families with M5 reverse and direct impact roots. Across the
    seven exact direct and M5 job states -- `declared`, `running`,
    `completed_active`, `completed_inactive`, `retryable_failed`,
    `terminal_failed`, and `cancelled` -- and all six exact M5 scope states,
    the post-open requirement hydration and package-private direct hydration
    validate the exact retained declarations, fallback provenance, and root-set
    hash; acquire or call only genuinely unfinished work; and close the retained
    barrier. Insert/no-fallback and zero-root shapes also remain exact.

    Every persisted document-event M5 forward root has exactly one valid
    immutable provenance row with `fallback_required=true` and yields its
    fallback key; false, missing, duplicate, or extra provenance conflicts, and
    reverse roots have none.
    Persisted parentless direct frontier roots yield exactly sorted-unique
    fallback claim IDs, impact roots equal inserted chunks, child jobs never
    leak into either root set, and scopes cover exactly impact roots. Missing,
    extra, corrupt, wrong-event/policy/snapshot/payload/execution, changed-
    manifest, wrong-provenance, or wrong-root-set bytes conflict before external
    work. Mutable progress fields do not change reconstructed declarations.

    Read spies permit only migration-ledger and event/epoch/runtime/update plus
    epoch-keyed immutable declaration, manifest, snapshot, direct all-job and
    dependency-prefix ranges, M5 all-job range, and per-job scope/provenance
    point reads. They reject current heads, currency/frontier, changed-chunk
    admitted history,
    current observations, full repository/oracle reads, and provider/model
    calls. A wrong payload conflicts before a declaration range. The requirement
    planner is invoked exactly once after a nonterminal `replayed=true` receipt;
    direct continuation uses its package-private retained reader and never
    re-enters `plan_direct_open`. Neither hydration adds structural work or
    counters.

    Race the event into existence after either pre-opener point check. The
    opener selects exact retained authority, ignores provisional continuation
    bytes, and a nonterminal receipt hydrates the same persisted roots without
    duplicate work/accounting. A same-ID wrong payload or mismatched typed event
    closure conflicts.

    Independently terminalize the event (a) after the application's initial
    result lookup but before the first planner, (b) between the two planners,
    (c) after both planners but before the opener's locked classification,
    (d) after a nonterminal replay receipt but before or during requirement
    hydration, and (e) after the direct runner's exact route-barrier/revision
    validation and during its package-private hydration. At cuts (a)
    and (b), the reached planner completes its immutable declaration validation,
    raises the exact empty-argument `_M5D29HydrationTerminalCutoff`, and the
    literal pre-opener catch canonical-rereads the same event/payload/epoch and
    returns the ordinary terminal-known-at-entry envelope. At cut (c), the
    opener returns its existing terminal receipt and result branch. At cuts (d)
    and (e), the literal post-open catch requires the held exact nonterminal
    receipt and canonical zero accumulated call work, canonical-rereads the
    same event/payload/epoch result, and uses the existing shared active-
    terminal projection; direct hydration raises before constructing any
    `M5DirectExecutionReceipt`.

    Negative cases for a subclass, nonempty signal, wrong raise/catch site,
    signal before complete declaration validation, arbitrary terminal read
    without the exact signal, wrong/missing/malformed canonical result, wrong
    event/payload/epoch, terminal-projected held receipt, or nonzero accumulated
    work conflict. Terminalization after a hydration returns normally gains no
    new origin and remains governed by the next existing C5/C6/C7 checked
    origin. The exact revision point reads before direct hydration confer no
    declaration or terminal authority and contribute canonical-zero work. Every
    cut performs zero current-state/withdrawal reconstruction,
    external work, semantic/event/withdrawal/history DML, work accounting, or
    transition timing. Frozen D24 call timing/coverage and its sole optional
    postcommit invocation-telemetry row remain permitted. An ordinary terminal
    result found by the initial lookup invokes neither planner nor hydration.
22. **Prospective reservation closure.** Event-intrinsic insert roots,
    direct-M4 locator claims, admitted-pair locator requirements, and current-
    currency locator requirements produce the exact sorted/unique prospective
    scope/job coordinate superset without reading the preview. Omitting any
    derivable reservation or injecting one unrelated to the locked source and
    locators conflicts. Well-formed nonlineage/inactive locators may leave
    reserved-but-unused coordinates and insert no row; every exact final
    declaration is a reserved subset. Trace evidence proves zero tier-8/9
    discovery, lock, advisory acquisition, or unique-conflict acquisition
    after tier 9.
23. **Timing ownership.** Instrumented first application proves exactly one
    `structural_open` transition timing anchor and places all authoritative D29
    planning inside that existing transition-call interval. Injected preview,
    pre-opener existing-event projection, post-open requirement hydration, and
    package-private direct-hydration latency creates no second anchor, no
    transition point, and no event-timing or event-work contribution. If the
    frozen measurement boundary observes that read-only latency, it appears
    only in the current invocation's existing `call_timing`; disabling that
    observation may change only frozen D24 invocation timing/coverage telemetry,
    never event bytes, event timing, work, result, history, or digest. The
    terminalization cuts in falsifier 21 create no transition anchor; they may
    emit only that same D24 invocation telemetry.
24. **Migration-018 route barrier.** Missing and every one-field-different
    accepted-018 ledger value fail future activation exposing D29 routes, each
    document preview planner, first open, and nonterminal resume before, apart
    from the explicitly permitted initial durable-result lookup, event-ID/epoch
    consumption, declaration range, current-revision read, acquisition, or
    external work. Exact authority takes only the indexed routes and never
    silently scans. A direct-runner order spy proves ledger barrier, exact
    supplied-revision validation/current-revision point read, declaration
    ranges and terminal check, then acquisition; the revision read supplies no
    declaration authority or work. An exact terminal result found by the application's initial
    lookup remains available for terminal audit/replay with 018 absent and
    invokes no planner/hydration. Its only permitted write is the frozen D24
    timing/coverage-only postcommit invocation-telemetry row; never-activated
    M4-v1 behavior is unchanged.

## 11. Acceptance and reactivation barrier

This candidate becomes authority only after two independent reviewers audit
identical committed bytes and each returns `GO`, `P0=0`, and `P1=0`. Any byte
change restarts both reviews. A separate authority-freeze tranche must then
amend the design freeze, runtime addendum, acceptance matrix, plans, status,
roadmap, decision log, AGENTS reading order, and handoff without modifying the
reviewed candidate.

Only after that authority freeze is integrated and pushed may a new docs-only,
path-exclusive activation allocate migration 018, installer/constants/tests,
the document planner, or any affected composition path. It must start from the
exact pushed D29 authority barrier, preserve existing Lane-P WIP as read-only
non-authority evidence, define disjoint ownership and sequential integration,
and require two same-byte `GO`, `P0=0`, `P1=0` audits per lane.

The current D28 Lane-P worktree must not be committed, merged, rebased,
cherry-picked, copied, or represented as accepted under this candidate. Its
source/test work may be reimplemented only after the new activation grants
exact paths on the new base.

Acceptance resolves only persisted legacy-document source identity, bounded
withdrawal enumeration, exact-existing root completeness, terminalization
routing, and the same-policy admissibility correction. It does not accept
migration 018 bytes, an installer, source/tests, D24--D29 implementation,
Task 2, M5.4, M5.5, M5.6, deployment, performance, utility, or AI quality.
Runtime remains `v1_only` outside isolated disposable fixtures.
