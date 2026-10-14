# M5-D34 Requirement Observation Source-Order Amendment

Status: **candidate; not frozen authority or implementation permission**.
Date: 2026-10-14. Proposed decision: M5-D34 / M5.0-34.

Scope: resolve the requirement-verifier source/observation/FK sequencing cycle
without changing inference, grounding semantics, persisted identities, SQL,
public APIs, work ownership, or runtime defaults. Two independent audits of
identical complete bytes must return GO, P0=0/P1=0 before authority-freeze
updates are prepared. Implementation still requires a separate path-exclusive
activation after the accepted authority barrier.

## 1. Pinned evidence and docs-only boundary

```text
candidate_parent = 4f50dc6701b462024e125a45505eb7be17bd4112
candidate_parent_tree = 69d9b96b3309f6c0621db51040568c9c7922f30f
branch = workstream/m5-d34-requirement-source-order-candidate
sole_owned_path = docs/workstreams/m5_runtime_contract/REQUIREMENT_OBSERVATION_SOURCE_ORDER_AMENDMENT.md
d24_sha256 = 3fe36a38c3c789143542f8b8cf6614a1df5e7807b9dffa99f0d2cc5118dc1209
d25_sha256 = bac12ab5e74632c04f1bd70d0ef0d00522ba9d268eb8b73d11845bbf3b873aae
d28_sha256 = 8a2bafd3478cf2cac6ac7c8de7ca7779a6d9ace7fbbe98a7dc3ff08afdb67eae
runtime_addendum_revision = 14
runtime_addendum_sha256 = 6e850f8006156c23c8887ba86ebe486fe99047bb2be8e3ca76e075b1facac83b
migration_015_sha256 = 85cb7f8e6a33273ce67fc6b4160e74a3aff314cd084df7ac3647cadae930185c
runtime_mode = v1_only
```

Root owns only this new document in the isolated candidate worktree. The two
independent contract/custody auditors own zero paths. All source, tests,
migrations, existing authority files, held checkouts, databases, containers, providers,
and other branches remain read-only. This tranche does not authorize stage,
commit, push, authority publication, or implementation.

The prior audited C2 dependency candidate has SHA-256
`64be6f28b586743047063771de0026f9774bc83470fb938d55407c76bc94c720`;
it records the blocker but grants no source ownership. The B33 complete
five-file diagnostic manifest remains
`/tmp/groundloop-b33-resumption.czMgRQ2L/FROZEN_MANIFEST_ATTEMPT_2.json`, SHA-256
`0699d07c659a80802e623af59ab55bbdf62c1d89aa8f36fb977526751b992552`.
Preserve all five full contents/modes, its empty index/HEAD/raw inventory,
the current nine C1 pins, protected inventories, and accepted prerequisites.
Temporary receipt availability must be checked again at each later barrier;
missing evidence is not reconstructed or silently treated as available.

## 2. Confirmed contradiction

D28 Section 4 requires the immutable requirement attempt result and verifier/
execution closure persisted and locked through tier 10 before the sole
`derive_matching_transition_intent(...)` call. The accepted native source
loader requires the actual execution and semantic observation, not just their
proposed identifiers. Migration 015 gives execution an immediate observation
FK. Therefore a fresh observation must exist before that source can be loaded.

Runtime-addendum Section 15 and D25 Section 10 place semantic observations at
11a. D28 preserves that order while placing applicable observation/currency
stage writes after derivation, complete preparation, and header installation.
Its direct-only pre-source reservation cannot be borrowed by requirements.
The existing controlled fixture's observation-before-execution prefix and
later explicit observation relock illustrate the discrepancy; tests do not
create a missing contract exception.

The correction below distinguishes a **new immutable source archive** from
the later **currency and grounding-state change**. It does not defer the FK,
introduce a requirement pre-source intent, or treat archival eligibility as
currency authority.

## 3. Exact narrow precedence

Only the following conflicting wording is superseded:

1. Runtime-addendum Section 15 tiers 10/11 and D25 Section 10's unchanged-tier-
   10 and semantic-observation-11a wording, only for the exact new source
   observation/archive tail in Section 4. Old/base observations, currency,
   matching images, bounded representative discovery, and later tiers stay put.
2. D25 Section 9.2 items 2/4 and runtime-addendum Section 14.3 items 2/5, only
   where their coarse lock/archive outline implies currency or matching locks
   before the explicit source tail, or leaves its relation order unspecified.
3. D28 Section 2's preservation sentence, Section 4's requirement source
   closure and sole-derive/stage wording, Section 7's outer-lock non-change
   sentence, and Section 8's source-present-order falsifier, only to permit
   Section 4's archive placement and Section 5's no-relock rule.
4. D28 Section 6's requirement replay closure wording and corresponding
   falsifier, only to make explicit Section 6's immutable ordinary-read
   observation/pair validation. Replay gains no early observation lock.

This is one bounded source-archive exception, not a general permission to
reorder locks. It is not a second dynamic affected-key discovery exception.
Every remaining D24--D33 rule and every schema/ledger byte remains exact.

## 4. New immutable requirement-source archive tail

### 4.1 Entry authority and scope

After waiting for tier-5/6 serialization, recheck the complete retained D24/D25
source/result/contribution tuple under the checked read envelope, before any
fresh transition authorizer or tier-7+ completion prefix. A winner that became
visible during that wait takes the native retained replay route there. Once
fresh authorization/prefix has begun, any archive preflight occupancy conflicts
and rolls back; it must not switch to replay with a fresh context already open.

The existing checked outer completion/return transaction holds its applicable
frozen tier-5--9 prefix and ordinary tier-10 attempt/output/dependency and
provenance parents before entering this tail. It must validate the held job,
attempt, lease/output binding, immutable pair input and verifier artifact,
decision/model/execution identities, and activity classification through the
already-frozen native rules. A normal active first application still uses its
single tier-6 compare-only authorization at expected revision N; job/attempt
remain preterminal until D28's later terminalization step.

Exactly one requirement-verifier result closure may enter this tail per outer
completion transaction. No multi-completion batch, return to tier 10 after
tier 11, caller-authored affected-key list, or implicit plan lookup is added.
The store derives every archive coordinate from the validated job/attempt,
output, pair input, and verifier artifact using the existing recipes, including
the source observation ID derived from artifact ID and task type.

All FK parent coordinates at an earlier frozen position must already have the
required protection, including structural/snapshot/scope and immutable policy,
model, subject and provenance parents. The tail may not discover and acquire
an earlier authority key. Missing or mismatching parents fail the whole call.
PostgreSQL's checks of already-protected references remain enabled; no later
implicit FK check may introduce a previously unheld earlier coordinate or
lock-strength upgrade that can wait on an earlier tier.

### 4.2 Complete global conflict reservation, then fresh-closure preflight

Before any archive-tail row lock or insert, gather the complete bounded
archive-conflict set from those validated values. Canonical logical coordinates
use relation order R, P, V, O, E below, alias order as displayed, then numeric
components and UTF-8 bytes under `COLLATE "C"`. Deduplicate identical logical
coordinates globally within this tail. Compute their complete physical
advisory mapping, deduplicate the actual PostgreSQL lock identifiers, and
acquire those actual identifiers in ascending numeric order, not logical-hash
input order. Reservations form subphase 10A; ordered archive rows follow in
10B--10F. No reservation is discovered after the row phase starts.

| Relation order | Complete existing uniqueness/reference aliases, in order |
| --- | --- |
| R: `groundloop_m5_attempt_result_artifact` | `attempt_result_artifact_id`; `attempt_result_artifact_hash`; `attempt_output_digest`; `attempt_id` |
| P: `groundloop_m5_requirement_pair_input` | `pair_input_hash`; (`semantic_pair_digest`, `scope_contract_digest`, `candidate_policy_id`); (`pair_input_hash`, `semantic_pair_digest`) |
| V: `groundloop_m5_requirement_verifier_artifact` | `artifact_id`; `artifact_hash` |
| O: `groundloop_semantic_observation` | `observation_id`; (`observation_id`, `subject_kind`, `subject_id`, `chunk_version_id`, `task_type`) |
| E: `groundloop_m5_requirement_verifier_execution` | `observation_id`; `artifact_id`; (`logical_job_id`, `attempt_id`); (`observation_id`, `artifact_id`, `pair_input_hash`) |

Aliases are namespaced by the exact displayed relation and constraint-column
tuple and their full typed values. The alias label is the comma-separated
column names in displayed tuple order, with no spaces or parentheses. Canonical
scalar encodings are those accepted by the existing matching-absence helper
(checked decimal integers, exact enum/text values and checked trimmed hashes).
An epoch component must not hide a global PK/UNIQUE collision. No new
public/persisted digest, schema object, or nullable identity is introduced.
Use the two-int transaction advisory namespace `1144206401` (`0x44333441`,
`D34A`), distinct from the accepted D28/D29 namespace constants. Map the second
signed-int32 component with the accepted matching-absence key algorithm and
length-framed UTF-8 encoding, using the exact relation name and an alias label
as the first key component followed by its canonical scalar values. This
reuses that private hash/framing algorithm, not a new persisted identity domain;
no repr/JSON encoding is allowed. Keep the complete logical-to-physical map
for validation. A truncated-hash collision may only over-serialize; full byte
comparison, never the advisory value, establishes identity. Physical-ID sorting
prevents such collisions from reversing acquisition order. The separate first
namespace component prevents collision with later 11a/15i lock namespaces.
Later implementation audits must recheck the complete namespace inventory.
Unlocked point reads can supply locators before reservation, never authority
or a caller-selected skip list.

Under all those held reservations, before any archive-tail row lock or DML,
point-probe and validate **all displayed aliases**. Every R, V, O and E alias
must be globally absent for a genuinely new closure; only an existing exact
P pair-input row may be reused. An existing verifier artifact without its
atomic observation/execution closure is an orphan, not a reusable completion.
An existing complete closure is retained-replay/conflict territory. No result,
observation or execution may be adopted, repaired or reparented into a new
completion. An exact already-committed result takes its existing retained
replay route before this fresh prefix; inconsistent/partial closure or a
different epoch/job/attempt/eligibility binding conflicts. These checks use
bounded exact point probes, not table/history scans.

All already-authorized writers of a new requirement-verifier closure must use
this same reservation/preflight protocol; otherwise implementation cannot be
accepted. This does not authorize a new writer or new late-return disposition.
Identical pair-input reuse is not a partial completion. Matching artifact
payload bytes are not permission to reuse an existing artifact/execution.
In particular, a new completion conflicting with an existing execution's
artifact alias must fail **before** locking any archive row, rather than
holding verifier artifact and then waiting for a replay-owned execution.

### 4.3 Ordered source persistence and locking

After successful fresh-closure preflight, acquire/validate and insert the
archive rows in exactly R -> P -> V -> O -> E order. The reusable existing P
row is locked once at its canonical primary identity and compared in full to
the existing typed pair-input contract; all aliases must identify that same
row. An alias naming a different row conflicts without following it into an
unplanned lock. All new rows must still be absent under the reservations and
must be inserted once, not upserted or updated.

The newly inserted observation is protected by its owned insertion and held
global conflict reservations. Execution is inserted only after both verifier
artifact and observation exist, satisfying its unchanged immediate FKs. The
result row may precede verifier closure because its accepted schema does not
FK-reference verifier/execution; all deferred result/source integrity guards
still validate the complete closure at the original final guard point.

The complete source must be byte-validated before sole derivation: official
attempt ID/result hash, job/attempt/output/execution/payload bindings, semantic
pair, pair-input and artifact hashes, subject/chunk/task, model/decision
policy, scores/output/label, produced epoch and eligibility. The accepted
observation comparison includes all 15 persisted fields represented by the
native verifier `_observation_values`/replay validator, not only the matching
loader's current nine-column subset. The accepted
`eligible_for_currency=true` active value is classification metadata only;
it installs no currency, witness, certificate, or publication by itself.

No base/runtime revision advance, currency/history DML, D25 image/row/state/
certificate write, owner/answer pending update, or work/timing accumulator
write occurs in this archive tail. It retains its source locks/reservations
through the caller-owned transaction. It grants no external call or internal
commit/rollback. A committed `result_reserved` attempt remains forbidden.

## 5. Sole derivation, no source-observation reacquisition, unchanged stage

After the source tail, call the unchanged public
`derive_matching_transition_intent(...)` exactly once as D28 requires. Its
source loader byte-checks the complete persisted closure. It gathers/sorts
the full ordinary private 11a plan before acquiring the remaining 11a keys.

Exclude **only the exact validated new source observation** from physical
11a row/conflict acquisition: its protection is already held from tier 10.
Read/revalidate its complete bytes without another `FOR UPDATE`, advisory
reservation, or conflict-key acquisition. No caller-held flag or supplied key
can cause that exclusion. All other semantic observations, including the old
currency holder, and all current/working currency/reference/conflict keys
remain at their existing sorted 11a positions. No old holder is moved early.

The full private source/currency-before-image coordinates and equality checks
remain complete. Do not force a REFUTE/NEUTRAL source ID into the frozen
intent's SUPPORT matching-observation sequence. The native official intent,
its digest, affected matching keys, patch bytes, and retained replay projection
remain exactly those derived by the existing typed recipes. The source-row
placement exception changes neither a DTO field nor its meaning.

Derivation then takes 11b, performs the sole frozen bounded representative
discovery, and takes 11c--14 in order. D28 preparation/recomputation freezes
the complete intent/patch/work and complete header after-images. Install base
then typed-runtime N -> N+1 exactly once. At the original stage point, write
currency close/open and all applicable lower-tier matching/state/certificate
changes under the held plan; **do not insert or rewrite the archived source
observation again**. Terminalize the already-locked job/attempt afterward.

Keep exact 15a--15k accounting/finalization and tier-16 logical deltas, result
rows and final deferred validation. D24 execution/completion work remains
distinct from D25 contribution work and is counted once by its existing owner;
archival placement creates no new counter or timing anchor. D27's diagnostic-
only requirement-state row count is unchanged. Any failed equality or guard
rolls back the entire archive and every subsequent state/accounting write.

Preserve work definitions and ownership, not a claim of unchanged numerical
byte totals: genuinely new alias/advisory-key serialization and hash production
belongs once to `verifier_completion` under D24 Section 7.4, not to attempt
execution, replay validation, or a duplicate persistence charge. Record actual
new producer bytes and measured time. Distinct logical coordinates whose
physical IDs collide still charge each actually executed preimage/hash once;
deduplicating physical locks does not erase executed producer work. This
amendment reports no measurements.

## 6. Retained replay and non-active returns

### 6.1 Retained active replay is not fresh archival

Use D28's native checked retained-replay route before a fresh authorization/
completion prefix. It preserves its existing job -> attempt/result ->
execution -> verifier row-lock order, then the existing 11b and 15i--15k
artifact/contribution/current-accumulator validation. No new source-observation
or pair-input `FOR UPDATE`, global archive reservation, or 11a acquisition is
added to replay. Byte-validate their immutable persisted contents by ordinary
point reads and require the exact retained job/attempt/epoch/eligibility and
source/pair/artifact/observation bindings. Missing or changed closure conflicts;
replay is not an opportunity to repair or reconstruct an archive.

Do not apply Section 4's fresh-absence test to a valid retained replay. It uses
the original source/resulting revision R even after later progress; D28's
retained changed-key projection, current checked header, retained matching
image and cumulative-accumulator interpretation remain unchanged. It invokes
no fresh derivation/authorizer/context, performs no DML or constraint forcing,
adds no work/timing, and makes no retrieval/model/provider call.

### 6.2 Why replay must not lock the old observation early

An old source observation can be discovered only after 11b as a certificate
representative in a later completion. The existing certificate artifact has
an immediate selected-observation FK. If replay held that old observation
`FOR UPDATE` at tier 10 while waiting for the global image held by the later
completion, the completion's FK `KEY SHARE` could wait for replay: a cycle.
Immutable ordinary reads avoid introducing that observation lock. Epoch
serialization alone is not a cross-epoch proof.

Conversely a genuinely new source observation is absent globally under the
complete conflict reservation, cannot be a committed old holder or retained
representative, and is invisible to other transactions until commit. This is
the bounded justification for the new archive-only placement, not permission
to move any existing observation to tier 10. Source writers serialize globally
on complete aliases; exact replay never waits for an absent execution.

### 6.3 Inactive, expired, cancelled, failed and audit-only paths

Where an existing frozen route already permits a **new** requirement-verifier
observation/execution archive, its archive operation uses Section 4's common
reservation/order/absence checks, with its existing disposition and eligibility.
Normal inactive completion remains ineligible and makes no currency, matching,
state, certificate, D25 patch/image/contribution, or publication change; its
already-required D24 execution/work/timing and revision behavior remain exact.

The accepted inactive helper currently persists execution accounting before
calling `_persist_verifier_closure`. Inserting 10A inside that late call would
invert the corrected order. Future activation must own a bounded rearrangement
that acquires the entire archive prefix before any applicable 15d--15h lock,
while retaining inactive D24 contribution/timing/cutoff semantics. The current
bundled path is not newly qualifying production evidence for D34.

Late/expired/cancelled/terminal audit routes gain no normal-completion or
semantic archival permission. They retain their existing sidecar/output,
eligibility, revision cutoff, D24 accounting and exact replay rules. A route
that already forbids a normal archive still forbids it. Failed returns without
a verifier result cannot manufacture a closure. Nothing in D34 converts a
loser, duplicate, retry, or audit-only result into another active completion of
the same semantic pair. Their retained replay is not fresh archival.

## 7. Mandatory implementation falsifiers — all PENDING

Future activation must allocate and execute at least these checks; none is
claimed executed by accepting this document:

1. Genuine fresh SUPPORT, REFUTE and NEUTRAL completions with real checked
   attempt/output/result/verifier closure. Trace all actual SQL row/conflict
   acquisitions: prefix -> complete global reservations -> absence preflight
   -> R/P/V/O/E -> sole derive -> remaining11a -> 11b/discovery/11c--14 ->
   prepared complete headers -> currency/state -> terminalization -> 15a--15k
   -> exact16 deltas -> final guards -> caller commit. No explicit source
   observation relock, second INSERT, or forced SUPPORT key is allowed.
2. Every PK/UNIQUE alias collision, including different epochs with identical
   artifact/observation IDs and conflicting produced epoch/attempt/eligibility;
   reused exact pair-input cases; orphan artifact/partial source closure;
   foreign execution artifact alias. Fail before any tail row lock on failed fresh preflight.
   Never rewrite immutable rows or introduce epoch-salted IDs. Inject physical
   advisory-hash collisions/reversed logical orders and verify deduplicated
   physical-ID ordering plus namespace separation. Exercise all 15 observation
   field mutations, not just the native matching loader's partial projection.
3. Missing/wrong FK parents, artifact/output/result bytes, policy/model/label,
   source ID, preterminal authority or held-prefix order; arbitrary skip lists,
   early old-holder/currency locks, late reservation, batch re-entry, ambient/
   nested transaction, savepoint or internal commit misuse.
4. Both serializer orders for fresh alias contenders and fresh versus retained
   replay. Include the cross-epoch old-observation representative/certificate
   FK scenario above and execution/verifier inversion; exercise actual
   separate connections, blocking/wakeup and guarded commit/rollback, not only
   a recording cursor. Include active/inactive contention and a winner that
   becomes visible while waiting at tier5/6: retained classification must occur
   before fresh authorization. A timeout or avoided fixture is not a deadlock
   proof.
5. Inject rollback after every actual archive mutation and each later phase;
   reconnect must expose the complete old or committed new image, never a
   durable reserved attempt, partial source closure or orphan accounting.
   Retry after rollback uses the existing lawful attempt/output rules.
6. Latest and older retained replay after later progress and reconnect: exact
   historical source/projection/patch/contribution, validated current retained
   accumulator, zero DML/constraint forcing/model calls/work/timing additions,
   and no source-observation row lock or archive repair.
7. Retained inactive/late/expired/cancelled/failed/loser/duplicate matrices,
   including immutable identity conflicts and D24 accounting/cutoff; no
   semantic/D25 effect from ineligible or audit-only archival.
8. Independent full recomputation after committed active histories and exact
   D24/D25 row/byte/counter/timing ownership. Preserve all existing deferred
   bijection, publication, owner/LOGIN, failure and replay gates. No full
   oracle or aggregate job/contribution scan enters the production transaction.

## 8. Non-change boundary and next gate

No public/frozen signature or DTO, persisted digest/domain/field order,
source/reference kind, present/absence recipe, tombstone relation, nullable hash, schema object,
FK/guard/trigger/privilege/index, migration 000--021 bytes/ledgers, runtime
mode, model/provider behavior, or neural-quality claim changes. No global
token registry, implicit cursor attachment, process cache, reconstruction of
nonpersisted original intents, new timing anchor, or direct-only pre-source
reservation for requirements is introduced.

This correction does not fix the bounded header helper, implement C2 or
failure coordination, qualify B33 positives/carry/absence, or discharge the
separate group full postseal independent-image oracle. No new N/A waiver,
future B33 parent, source ownership, C1 reconciliation or publication is granted.

Freeze complete candidate bytes externally and obtain two independent
identical-byte audits before preparing a separate authority-freeze tranche
(decision log, addendum, acceptance/status/AGENTS and handoff consistency).
After that tranche is separately accepted, a new audited path-exclusive C2/
dependency-order activation must pin every source/test/handoff path and future
B33 ancestry rule while preserving its entire checkpoint. Existing frozen
contracts remain current until that acceptance; this candidate is not a
shortcut around either barrier.

D34 implementation, D25 integration, full B33/P33/C1/C2, Task 2, M5.4--M5.6,
deployment, security, AI quality, independently adjudicated natural-history
utility, end-to-end latency/cost, and named-system superiority remain PENDING.
