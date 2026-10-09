# M5-D32 Group-Only Semantic Readiness Amendment

Status: docs-only contract candidate. No implementation is authorized by this
file alone. Two independent reviews of identical bytes must each return GO,
P0=0/P1=0 before a separately reviewed authority freeze and path-exclusive
implementation activation. Task 17, C1, M5.4--M5.6 and AI quality remain PENDING.

Date: 2026-10-09

## 1. Confirmed gap and narrow precedence

Runtime-addendum Section 12 requires zero-root events to retain the typed
result/seal route without a closure barrier. Section 14.1 and migration 015
require structural_committed -> semantic_pending -> semantic_complete ->
sealed, with one revision increment per state-changing outer transaction.
Retirement creates no requirement roots. The root barrier rejects an empty
root set, and cancellation requires jobs. Nonempty requirement-root closure
also leaves semantic_pending after its last counter becomes zero. Existing
semantic_complete projectors require a genuine direct document job.

A group-only event cannot use a fabricated direct transition: migration 013
binds the M4 evaluation counter to groundloop_m4_update, and group lifecycle
events have no such document-update row. Test-authored header updates, fake
direct jobs, post-hoc zero accounting and early clearing of pending timing are
not production readiness and cannot satisfy the Task-17 history gate.

This amendment adds only a dedicated, accounted group-only readiness route.
It extends D24 Sections 7.2, 7.4 and 9.2 with one runtime-work/timing kind,
semantic_readiness, and adds one typed source-identity recipe. It does NOT add
a changed-state reference kind: D26's six kinds, absence rules and present
recipes remain exact. D25 coordination-only revisions create no matching
patch/contribution and do not advance its matching-work accumulator.

The only superseded negative inventory clauses are those that forbid a
migration after 019, this new work/timing kind, this new identity recipe, or
the exact objects in Section 6. Migrations 000--019 remain byte-identical;
their ledger identities, historical results and every other contract remain
unchanged. No public application route is activated by this amendment.

## 2. Eligibility and the two edges

Eligibility is restricted to a persisted M5 update whose update_kind is
register_group, replace_group or retire_group. The runtime and base epoch
must agree on event ID, payload binding, structural commitment, expected
revision, nonterminal state and the immutable runtime declaration. There must
be no M4 update/evaluation-counter/direct-job surface for this epoch. Never
infer direct readiness from zero M5 counts when a direct surface exists.
Document, policy and observation events are outside this correction.

The two possible readiness transactions are:

| source_id / target | required predecessor | additional condition |
|---|---|---|
| semantic_pending | structural_committed at N=1 | canonical empty root-set digest; no M5 jobs/scopes; all runtime and owner/answer PENDING/failure counts zero |
| semantic_complete | semantic_pending at N>=2 | no unresolved or failed M5 job/scope; all runtime and owner/answer PENDING/failure counts zero; all required roots have their genuine terminal closure/cancellation artifacts |

The first edge exists only to start a job-free, rootless group event. A
nonempty-root event starts semantic_pending through its existing genuine job
transition; it must not create an extra readiness-start contribution. The
second edge may complete either history. Readiness does not cancel, close,
stage, acquire, retry or complete a job, perform verification, or change
observations, relational grounding, certificates or deltas. All existing
root/result/cancellation integrity validators remain authoritative.

Each call owns one read-write READ COMMITTED outer transaction on an idle
connection. Lock base epoch before runtime epoch, then owner counters in C
order, answer counters in C order, then work/timing accumulators, retaining
the established lock order. Compare all input images before mutation. The
exact three runtime counts and independent durable job/scope checks must
agree; an open/result_staged scope, declared/running/retryable_failed job,
terminal failure, positive owner/answer count, stale revision or half-terminal
header blocks completion. Invalid state rejects without durable writes.

Advance base and runtime revision exactly once to N+1. For the first edge,
base semantic/evaluation state stays pending; for the second, both become
complete. Runtime target is the source_id above. Update owner/answer counter
revision bindings to N+1 without changing their values. Publication mode,
sealed_at, publication/lifecycle heads and result rows remain unchanged.
There is no shortcut from structural_committed to semantic_complete.

## 3. Exact identity and immutable proof

Use the existing immutable groundloop_m5_runtime_work_contribution as the
transition artifact, not a new table. Its kind is semantic_readiness; source_id
is exactly semantic_pending or semantic_complete; applied_revision is N+1.
One target is possible at most once per epoch under its existing primary key.

Derive source_identity_hash only from locked persisted values:

```text
stable_m5_digest(
  "m5-semantic-readiness-transition-v1",
  INT(epoch_id), TEXT(structural_event_id), HASH(event_payload_hash),
  HASH(requirement_root_set_hash),
  ENUM(from_runtime_state), ENUM(to_runtime_state),
  INT(expected_revision), INT(resulting_revision)
)
```

The only legal state pairs are those in Section 2; resulting_revision equals
expected_revision+1. Existing D24 contribution-key, work-vector, timing-anchor,
observation and transition-call digest recipes are unchanged. No repr/JSON,
nullable hash, inferred caller hash or caller-authored work is permitted.

A new BEFORE INSERT guard on the contribution relation applies only to this
kind and otherwise returns NEW without changing existing behavior. Under the
epoch locks it validates the present predecessor, exact group event and
declaration, source/target, input revision, recipe, eligibility and zero-count
conditions BEFORE the runtime/base advances. Inserting the contribution after
the advance must fail. The unchanged migration-015 transition trigger checks
the actual edge. The deferred contribution validator then requires the exact
resulting base/runtime revision and target, source identity and allowed work.
This separates present-predecessor proof from resulting-state proof; a digest
of an asserted predecessor is not by itself validation of that predecessor.

Exact replay first reads the immutable contribution by epoch/kind/source,
rederives its identity using applied_revision-1 and the fixed state pair,
validates stored work/key and event/declaration identity, and proves the
current epoch is consistent and no earlier than that coordinate. The supplied
expected revision must equal the original applied_revision-1, not a later
coordinate. Return the original anchor with replay=true and zero writes, even
after later legal progress or terminalization; never return an invented
current-revision anchor. Conflicting event, phase, revision, hash or work
rejects. If there is no contribution, terminal epochs reject a new transition.

## 4. Exact work and one timing point

Only bytes_hashed and bytes_serialized may be nonzero in readiness work.
No model-call, token, observation, admission, cancellation, state-artifact,
certificate-binding or public-delta counter is charged by this route. Pending
counter revision bindings are coordination rows, not claim/answer state writes.
Build and hash the source-identity preimage and contribution-key preimage once
each at explicit disjoint persistence boundaries using stable_m5_preimage.
Let S and K be their exact framed byte lengths. Both bytes_hashed and
bytes_serialized equal S+K; every other counter is zero. Recompute this formula
in SQL as well as Python; a different vector fails. Work/key validation
rehashing, the work-vector digest itself and database bytes keep the existing
validation/accounting exclusion. Do not substitute canonical-zero work for
these operations or charge these bytes again to D25.

After locking exact work/timing images at N, resolve an older pending anchor
as one immutable missing transition point, without an intermediate accumulator
UPDATE. Insert the readiness contribution at N+1 before advancing the headers.
Apply its nonnegative work once and set the work accumulator revision to N+1.
One final timing CAS increments expected counts once, increments missing
counts for the older unresolved point if any, and installs this readiness
anchor as the sole pending anchor at N+1. It is a nonterminal transaction.
Force deferred validation only after the complete write sequence, then commit.

The sole outer anchor is semantic_readiness/source_id at N+1. Existing
append_transition_call_timing accepts it after commit, with unchanged observed
versus missing semantics. Replay creates neither work nor a timing point.
Readiness performs no terminal accounting and must not classify its own
unfinished call interval as an observed zero. A later seal uses the unchanged
D24 fused terminal timing CAS to classify an unreported readiness anchor
missing, count seal expected/missing, clear pending, and freeze exact coverage.
The D31 pending-anchor reader must accept and validate this new nonterminal
kind by the same immutable work/timing point checks; it must not clear it.

## 5. Composition boundary

Implement readiness in one package-private module. Reuse existing DTOs for
timing anchors and work; a package-private receipt may carry anchor and replay
status, but no new public method/signature, exported DTO or facade is allowed.
Existing public wrappers and direct-M4 methods are unchanged. C1 may later
compose this helper under its own separately activated paths; it must not
merge both readiness edges into one transaction/revision.

The helper captures one trusted schema once, requires the exact pinned 020
ledger and catalog there, and binds every persistent read, write, authorizer
and function call to that same schema. Reject absent, temporary, ambiguous or
mixed-schema authority; do not fall back through caller search_path. The new
invoker guard's search path is pinned at installation to that trusted schema
plus pg_catalog, with no caller-selected relation lookup or privilege change.

The new route changes no D25 semantic image and inserts no D25 matching patch,
matching-work contribution, certificate or changed-state reference. Its
matching accumulator may remain at its last genuine semantic revision. All
preterminal and result-bound comparisons retain their existing coordinate
rules. No early seal-promotion context, terminal result or lifecycle-head
advance is allowed. D26 RETIRE/REPLACE absence validation remains exact.

## 6. Sole additive migration-020 authority

The future file is migrations/020_m5_semantic_readiness.sql. The exact allowed
schema delta is:

1. Extend only the kind allowlist CHECK on runtime_work_contribution, the kind
   CHECK on transition_call_timing, and the complete-or-all-absent pending
   anchor CHECK on runtime_timing_accumulator, adding semantic_readiness and
   preserving every previous value and every other predicate. Identify each
   existing constraint uniquely by exact relation/expression; ambiguity fails.
2. Add groundloop_m5_validate_semantic_readiness_predecessor() RETURNS trigger,
   LANGUAGE plpgsql, SECURITY INVOKER, SET search_path FROM CURRENT (the trusted
   installation schema plus pg_catalog), and one BEFORE INSERT FOR EACH ROW
   trigger groundloop_m5_semantic_readiness_predecessor on the work-contribution
   relation, implementing Section 3 without privileges, GUC trust or temp state.
3. Replace only migration-016's groundloop_m5_validate_work_contribution(): add
   the new source/target/identity/result checks and allow only work ordinals
   26/27 for this kind, preserving every existing branch and check.
4. Replace only migration-016's groundloop_m5_validate_timing_accumulator(): add
   this kind to its existing pending-contribution allowlist and preserve all
   other code. Its per-row deferred cutoff checks must not be relaxed.

No other function, trigger, constraint, table, column, index, type, view,
sequence, backfill, ACL or existing migration byte may change. In particular,
migration-015 runtime transitions and counters, migration-017 promotion/result
validators and the migration-019 trusted accessor are untouched. Do not clone
or rename old validator functions to leave a bypass. Migration 020 remains
opt-in and ledgered; no legacy installer discovers it by globbing.

The ledger-first installer must require an idle read-write READ COMMITTED
connection, capture/bind one trusted schema, require exact accepted bundles
through 019, take the established installer/epoch-before-runtime lock order,
recheck the ledger under locks, and atomically install/ledger the exact pinned
020 bytes. Exact replay verifies identity and exact catalog, including the
preserved old branches and unchanged objects; conflicting/missing/tampered
prerequisites, partial objects, wrong schema or changed first-install bytes
fail closed. Failure injection rolls back every DDL/ledger write. Concurrent
installers cannot admit different bytes. No runtime mode is changed.

The future implementation activation must pin final migration/bundle hashes
after schema review, not derive acceptance authority from mutable local bytes.
The opt-in installer is a schema-administration helper, not a new public
GroundLoop application port. Its five ledger fields use bundle ID
m5-semantic-readiness-schema-bundle-v1, migration label above, SHA-256 of the
exact migration bytes, the unchanged empty-oracle SHA-256, and prerequisite
bundle SHA-256 e12d4abd95a9b2ef49010a43d6b708824462a80dfed2548107e175920dabe481
(accepted 019). Its bundle digest is stable_m5_digest(bundle_id,
TEXT(migration_label), HASH(migration_sha256), HASH(prerequisite_bundle_sha256)),
exactly the existing additive-bundle framing pattern.

## 7. Executable acceptance and custody barriers

Required evidence on final identical implementation bytes:

1. Python/SQL golden identity equality and strict type/state/revision matrices;
   wrong event/payload/root-set/phase/source/predecessor/revision/key/work,
   insert-after-advance and terminal-new-transition negatives.
2. Exact 020 catalog delta, unchanged 000--019 hashes and old validator branches;
   prerequisite/ledger/schema/partial-install/rerun/rollback/concurrency checks.
3. Genuine owner and separately connected non-owner histories: zero-root
   RETIRE open -> pending -> complete, and nonempty REPLACE open -> acquisition
   -> staging -> real closure -> complete; no authored readiness header or
   accumulator, fake job, disabled trigger or fabricated positive hash.
4. Rootless start rejects nonempty roots; completion rejects every unresolved,
   result_staged, failed or positive-counter case and every direct/document
   surface. Matching patch/contribution/image bytes remain unchanged.
5. One-revision/one-contribution/one-anchor assertions, older missing point,
   observed/missing append, exact replay before/after seal, conflict and injected
   rollback at each boundary, readiness-versus-readiness/seal race orders.
6. After separately accepting readiness, finish the held R-T owner/non-owner
   REPLACE/RETIRE pending-anchor seal regressions and then C1 atomic composition:
   genuine seal contribution before terminal runtime, one fused timing CAS,
   retained D26 children, exact terminal work/coverage and zero-write replay.
7. Relevant pure/live differential, compatibility, compile, typing, format/lint,
   package payload and full-repository diagnostic gates with failures/skips
   reported, not relabeled as passes; two same-byte independent GO audits and
   postcommit identity checks for each separately accepted tranche.

Sequence: candidate audit -> authority freeze -> new explicit docs-only
activation -> schema/shared contract primitives -> private readiness runtime
-> held R-T -> held C1. No source work runs in parallel. Readiness source paths
must be disjoint from the four held R-T and nine held C1 paths; updating R-T's
new-kind reader or C1 composition occurs only in their later resumed lane.
Declare exact paths, parents, ledger/hash gates and custody digests in that
activation. A new necessary path/object or contradictory contract stops work
for a written scope correction; it is not added silently.

The protected primary and AI-study checkouts, all held R-T/C1 files, databases
and implementation branches remain read-only during this contract tranche.
Do not reset, clean, stash, rebase, cherry-pick or transplant WIP. Advance held
worktrees only after live custody and ancestor/disjoint-path proofs.

## 8. Claim ceiling

Authority freeze may mark only M5-D32/M5.0-32 contract PASS, implementation
PENDING, and runtime-addendum revision 13. It does not close Task 17, Task 2,
C1, any remaining M5.4--M5.6 gate, deployment, AI quality or natural-history
evaluation. Defaults remain v1_only. No new model/provider call, performance,
cost, security, scalability, utility, objective-truth, novelty or named-system
superiority claim follows. Stop after scoped Task-17/C1 acceptance and reassess
the next public vertical slice; this approval does not activate a later lane.
