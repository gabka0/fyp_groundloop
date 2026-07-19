# M4 Epoch/Runtime Lane Audit

Status: audit-barrier deliverable; implementation is not authorized

Date: 2026-07-19

Audited baseline: `4e5c146ede2976089714457a10349c890e950e0d`
(`Plan M4 multi-agent execution`)

## 1. Verdict

**AGREE WITH REQUIRED CONTRACT CHANGES. Confidence: high.**

M4 can preserve D-19, D-20, immutable observations, exact withdrawal,
idempotent replay, and failure-atomic publication. It cannot do so by extending
the current `EpochCoordinator` with a mutable set of required job IDs. The
current coordinator is a deliberately small M2 oracle with a fixed required
set, epoch-level evaluation state, and no durable result identity. M4 needs a
new job-DAG state machine and a versioned publication boundary.

Four P0 ambiguities must be resolved before any lane writes implementation:

1. dynamic child declaration has no replay/conflict identity;
2. strict publication has no versioned claim/answer payload to serve;
3. per-claim PENDING is undefined while discovery has not yet declared its
   claim children;
4. the proposed in-flight replacement scenario conflicts with the implemented
   one-open-epoch single-writer rule.

These are M4 contract blockers, not evidence that the accepted synchronous
M1 structured semantics are wrong. The existing M1 replacement and M2
structured differential paths are sound for their declared scope.

## 2. Evidence inspected

The audit inspected the frozen design and M4 plans, then the actual M1-M3
implementation rather than relying on milestone summaries. High-signal code
paths were:

- `src/groundloop/epochs.py:31-250` and `tests/unit/test_epochs.py:10-128`;
- `src/groundloop/events.py:136-249` and
  `tests/integration/test_event_atomicity.py:31-131`;
- `src/groundloop/repository.py:62-422`;
- `src/groundloop/incremental.py:312-440`;
- `src/groundloop/differential.py:48-84`;
- `migrations/001_m2_base.sql:11-31,204-285`;
- `migrations/002_m3_static_ai.sql:98-125,163-184`;
- `src/groundloop/postgres/snapshot.py:1-12,77-151,188-223,255-278`;
- `src/groundloop/ai/application.py:190-216,343-395`;
- `src/groundloop/ai/persistence.py:258-349,496-509,939-1124`.

Validation run from this worktree, using the linked environment without
printing credentials:

```text
GROUNDLOOP_TEST_DATABASE_URL="$GROUNDLOOP_DATABASE_URL" .venv/bin/pytest -q
PASS: 187 tests collected; all progress reached 100% with no failure

.venv/bin/pytest -q tests/unit/test_epochs.py \
  tests/differential/test_incremental_engine.py \
  tests/integration/test_event_atomicity.py
PASS: 15 tests
```

A direct state-machine probe reproduced this legal current state:

```text
semantic_status=sealed evaluation_state=degraded
pending_job_ids=['unfinished']
```

That result follows the currently tested M2 degraded semantics; it is not safe
as the default M4 CORE sealing rule.

## 3. What is already sound

### 3.1 D-19 in the in-memory reference path

`apply_event` computes against a deep staged repository and replaces live state
only after mutation, recomputation, delta construction, and event recording
succeed (`src/groundloop/events.py:192-249`). Replacement validates the old
active version, deactivates it, and registers the new version only on that
staged copy (`src/groundloop/events.py:136-185`). The rollback regression
explicitly proves that a duplicate new chunk after old-version deactivation
does not leak the half replacement into live state
(`tests/integration/test_event_atomicity.py:110-131`).

Conclusion: preserve this semantic shape in one PostgreSQL structural
transaction. Do not reopen M1's replacement semantics.

### 3.2 Exact structured withdrawal relative to stored observations

The M2 engine enumerates each old document's chunks and each chunk's current
observation IDs, then removes only active contributions
(`src/groundloop/incremental.py:328-390`). The repository distinguishes all
historical observations from the current holder of each currency key
(`src/groundloop/repository.py:398-422`). Distinct-content crossings and
certificate repair are differentially tested
(`tests/differential/test_incremental_engine.py:46-117`).

Conclusion: the signed withdrawal kernel is a valid base. M4 must extend the
reverse index to candidate/frontier dependencies and keep ANN completely out
of withdrawal.

### 3.3 Atomic structured differential publication

The differential runner stages both the repository and incremental engine,
checks complete claim/answer state and certificate validity, and publishes the
pair only after equality (`src/groundloop/differential.py:48-84`). This is the
right pattern for deterministic M4 microtransaction tests.

### 3.4 M3 publication is atomic but static

M3 locks one staged run and publishes all static artifacts, oracle-checked
states, and the terminal manifest in one transaction
(`src/groundloop/ai/persistence.py:258-349`). It deliberately inserts an
already sealed epoch (`src/groundloop/ai/persistence.py:496-509`). This is
valid for M3 and must not be mistaken for an asynchronous M4 runtime.

## 4. P0 findings: contract freeze blockers

### P0-1 — Fixed-set completion cannot safely become dynamic expansion

**Evidence.** `SemanticEpoch` stores immutable `required_job_ids` and
`completed_job_ids` (`src/groundloop/epochs.py:31-45`). `complete_job` accepts
only IDs in the initial set and treats a repeated ID as a no-op without a
completion payload (`src/groundloop/epochs.py:132-165`). The M4 plan correctly
acknowledges that discovery must create verifier children
(`docs/m4_implementation_plan.md:344-365`), but it does not freeze an identity
for the declared child set or the completion result.

**Failure.** Parent `D` first completes with child set `{V1}`. A retry of the
same parent reports `{V2}` or a different ranking artifact. A job-ID-only
dedupe either silently accepts inconsistent work or silently ignores the
conflict. If child insertion and parent completion are separate, a seal can
observe no open work between them.

**Required correction.** Freeze separate identities for:

```text
LogicalJobId      = H(epoch, kind, policy, subject?, chunk?, parent?)
PayloadHash       = H(execution spec and all other immutable job inputs)
ExecutionSpecHash = H(model/prompt/config/input/seed identities)
AttemptId         = operational retry identity; never semantic identity
CompletionDigest  = H(result artifact and exact sorted child declaration)
ChildClosure      = (parent_job_id, completion_digest,
                     sorted child_job_ids, child_set_hash)
```

An expandable job completes through one atomic operation that:

1. locks the parent and owning epoch revision;
2. checks the immutable payload and execution-spec hashes;
3. inserts every child and dependency edge idempotently;
4. records explicit closure, including a zero-child closure;
5. installs any result artifact;
6. marks the parent terminal; and
7. increments the owning epoch revision exactly once.

Exact replay of the same `CompletionDigest` returns the recorded result and
advances nothing. A different digest for the same logical job is a conflict.
No API may append children after closure.

**Falsifying tests.** T01 atomic rollback after the first child insert; T02
same parent/same child set replay; T03 same parent/different child set conflict;
T04 explicit zero-child closure; T05 concurrent parent completion and seal;
T06 same VERIFY_PAIR job with a different score/result digest.

### P0-2 — Strict publication has an epoch pointer but no historical state

**Evidence.** `publication_snapshot()` returns only an epoch ID, evaluation
state, and confirmation pointer (`src/groundloop/epochs.py:48-52,224-250`).
PostgreSQL has one mutable materialized row per claim and answer, keyed only by
object ID (`migrations/001_m2_base.sql:247-275`). The frozen design requires
the API to serve both the last sealed state and provisional PENDING state
(`docs/technical_design.md:263-281,453-455`). Merely adding
`evaluation_state` and `confirmed_as_of_epoch` to the singleton rows, as the
M4 plan proposes (`docs/m4_implementation_plan.md:404-414`), does not retain
the last sealed values after structural withdrawal modifies working state.

**Failure.** Epoch 1 seals a SUPPORTED claim. Epoch 2 structurally deletes its
final witness, updates the singleton row to UNSUPPORTED/PENDING, then fails.
A strict read can return epoch 1's ID but cannot return epoch 1's lost claim
and answer payload.

**Required correction.** Freeze two logical relations:

```text
WorkingClaimState / WorkingAnswerState
PublishedClaimState(claim_id, published_epoch, complete state, certificate)
PublishedAnswerState(answer_id, published_epoch, complete state)
```

Equivalent validity-interval storage is acceptable. Sealing must atomically
append the new published snapshot (or changed rows plus a complete inherited
snapshot mapping), append net public deltas, and move the sealed pointer by a
revision compare-and-swap. Strict reads resolve only the last sealed snapshot.
Provisional reads resolve working state plus its evaluation state and the last
sealed confirmation pointer. A failed epoch never changes published rows.

**Falsifying tests.** T07 seal e1, withdraw in e2, fail e2, then assert strict
state is byte-for-byte e1; T08 provisional e2 exposes changed working state
with `confirmed_as_of_epoch=e1`; T09 crash during seal rolls back published
state, deltas, and sealed pointer together.

### P0-3 — Discovery creates a false COMPLETE interval for every unknown child

**Evidence.** D-12 currently says a claim is PENDING when an admitted job for
that claim is incomplete (`docs/technical_design.md:263-285`). M4 begins with
an `EMBED_DISCOVER` job, which has not yet admitted any claim and declares its
claim-specific VERIFY_PAIR children only on completion
(`docs/m4_implementation_plan.md:348-360`). The implementation exposes only
one epoch-level evaluation state (`src/groundloop/epochs.py:31-52`); no
per-claim or per-answer evaluation relation exists (`rg` finds no such state
outside the epoch and M3 manifest).

**Failure.** Immediately after structural commit of an insertion, no claim has
an admitted child job. A literal application of D-12 labels every claim
COMPLETE even though discovery may shortly admit and change any of them.

**Required correction.** Freeze a discovery-scope barrier:

```text
DiscoveryScope(root_job, registry_snapshot_id,
               scope = ALL_REGISTERED_CLAIMS_AT_STRUCTURAL_COMMIT)

ClaimPending(c, e) :=
    an open discovery scope for e contains c
    OR an open required job explicitly targets c

AnswerPending(a, e) := any required claim of a is ClaimPending
```

The global scope may be represented lazily; it must not require `O(C)` row
writes merely to mark every claim. When discovery atomically closes, claims
with no declared children become COMPLETE under the recorded candidate policy;
claims with children remain PENDING. Deletion-only epochs can use the exact
known affected-claim scope.

**Falsifying tests.** T10 an open zero-child-yet discovery makes a registered
claim and required answer PENDING; T11 atomic zero-child closure changes them
to COMPLETE; T12 child declaration leaves only child-targeted claims PENDING;
T13 replay of closure does not alter evaluation-state counters.

### P0-4 — In-flight replacement conflicts with one-open-epoch execution

**Evidence.** The current coordinator rejects a second corpus epoch until the
active one seals (`src/groundloop/epochs.py:96-100`). The M2 test freezes this
behavior (`tests/unit/test_epochs.py:88-115`). The design calls this a
single-writer assumption (`docs/technical_design.md:457`) and D-18 requires
late inactive completions (`docs/technical_design.md:343-345,683`). The M4
workload nevertheless requires replacement while a verifier job is in flight
(`docs/m4_implementation_plan.md:463-478`).

**Failure.** Keeping the current rule makes that workload impossible. Removing
the rule permits overlapping structural snapshots without defining which
epoch owns current observation currency, how an older epoch seals after a
newer one, or whether an older completion may overwrite newer state.

**Required correction.** For CORE, retain serial structural epochs:

- a new corpus event is queued or rejected while the active epoch is pending;
- it may start only after the prior epoch is SEALED or terminal FAILED;
- an already running worker may still return after its owning epoch fails;
- such a result is archived under its original stable EpochId, and if the
  chunk is now inactive its job becomes `COMPLETED_INACTIVE`, may update only
  the D-8 currency key for that inactive chunk, emits no active-view delta,
  and cannot resurrect or seal the failed epoch;
- after an epoch fails, it launches no new attempts. Handling a late result
  whose target is still active must be frozen separately; it must never be
  installed into a newer epoch implicitly.

Rewrite the in-flight replacement test as: fail e1 without cancelling its
already running attempt, commit replacement e2, then deliver e1's late worker
result. Supporting genuinely
overlapping open epochs would require MVCC publication and cross-epoch
currency arbitration and is not justified in M4 CORE. If the coordinator
chooses overlap instead, it is a new decision after D-20 and must specify those
semantics before implementation.

**Falsifying tests.** T14 second structural epoch is rejected/queued while e1
is pending; T15 after e1 failure, e2 deactivates its chunk and seals; T16 e1's
late completion is stored `COMPLETED_INACTIVE`, changes no active or published
state, and leaves e1 FAILED; T17 an older completion can never supersede a
newer active-key observation or be installed into a newer epoch.

## 5. P1 findings: required before M4 CORE integration

### P1-1 — DEGRADED currently seals with genuinely open work

`mark_degraded` can transition directly from PENDING and retains the unfinished
set (`src/groundloop/epochs.py:167-183`). `seal` then accepts DEGRADED without
checking that set (`src/groundloop/epochs.py:199-216`), and
`tests/unit/test_epochs.py:73-85` endorses this path. The direct probe recorded
a SEALED epoch with `pending_job_ids=['unfinished']`.

**Contract correction.** M4 CORE disables degraded sealing. If an evaluation
experiment enables it, every required job must still be terminal; an explicit
failure-policy manifest lists which `FAILED_TERMINAL` jobs are tolerated.
DEGRADED means complete accounting with declared failures, never unclosed work.

**Falsifying tests.** T18 DEGRADED with an open/retryable job cannot seal; T19
DEGRADED with all jobs terminal and a matching failure manifest can seal only
when non-CORE mode is explicit.

### P1-2 — Job retries need separate logical, execution, and attempt identity

The M4 plan says a failed job retries under the same immutable execution
identity (`docs/m4_implementation_plan.md:377-387`), but operational retries
need leases and attempt records. M3 treats a surviving STAGED run as a conflict
on retry (`src/groundloop/ai/application.py:190-216`), so it is not a crash
recovery substrate.

**Contract correction.** Keep one immutable logical job and execution-spec
hash. Persist attempts separately with lease owner/expiry and attempt ID. A
lease expiry authorizes another attempt but never another semantic job. The
first valid completion digest wins; exact repeats are no-ops and different
digests conflict. A deliberate new seed/model/prompt is a new execution spec
and new observation, not an operational retry. Observation installation,
currency supersession, signed deltas, job terminalization, and revision
increment are one transaction.

**Falsifying tests.** T20 crash after inference before completion commit,
lease expiry, and successful reattempt produce one observation and one
revision; T21 two workers race with identical digest and only one commits; T22
different output digests for one execution spec conflict.

### P1-3 — Public status deltas are not distinguished from provisional flaps

Completions can arrive in arbitrary order. Recording every working boundary as
the public `StatusDelta` would make user-visible history depend on worker order,
even when the sealed state is identical. The frozen design says changed states,
certificates, and the delta log publish atomically at seal
(`docs/technical_design.md:449-452`), while the M4 persistence list asks for
event/job/revision provenance (`docs/m4_implementation_plan.md:404-414`).

**Contract correction.** Maintain an optional `WorkingTransition` trace for
debugging, but define public `StatusDelta(e, object)` as the net comparison
between the previous sealed snapshot and e's sealed snapshot. At most one
public old/new transition per object and epoch is emitted. It cites the event,
seal revision, and the set/digest of causative completions. Reversing worker
completion order may change the working trace, never public history.

**Falsifying tests.** T23 complete SUPPORT then REFUTE and in reverse order;
sealed state and public deltas must match exactly. T24 a failed epoch may leave
an audit trace but emits no public delta.

### P1-4 — Frontier repair has no invariant and one step is redundant

The plan says that after final-witness loss it first reuses an already verified
current alternative (`docs/m4_implementation_plan.md:331-339`). Under the
canonical view, every verified current observation on an active chunk already
contributes. If such a same-polarity alternative exists, the lost witness was
not final. The plan also does not say whether deleting a REFUTE or NEUTRAL
frontier member refills the frontier, or how M3 claims with no unverified
reserve are bootstrapped. M3's candidates are immutable run-local rows and all
claim top-k candidates were verified; there is no current reserve-state
relation (`migrations/002_m3_static_ai.sql:98-125`).

**Contract correction.** Freeze both invariants:

1. every active current observation contributes immediately; there is no
   hidden verified alternative to promote;
2. for `(claim, candidate_policy)`, the active frontier has a declared target
   depth `F`; after any frontier-entry deactivation, refill to `F` through
   active reserve entries or mandatory fresh retrieval. A separate repair
   trigger may prioritize loss of the final SUPPORT or REFUTE witness, but it
   does not alter the depth invariant.

An M3-imported claim starts with `reserve=EMPTY`; its first required refill
must use fresh retrieval. Fresh retrieval must freeze corpus snapshot, rank,
score, and policy identity. Frontier phase is derived from immutable candidate,
job, observation-currency, and chunk-activity facts; do not overwrite history
with an unconstrained enum transition.

**Falsifying tests.** T25 deleting a neutral frontier member refills depth; T26
loss of final support with empty reserve creates a mandatory retrieval job and
blocks sealing; T27 below-floor reserve also falls back; T28 an M3 bootstrap
has no invented reserve; T29 no ANN/retrieval adapter is invoked during exact
withdrawal itself.

### P1-5 — The structured-work bound omits unavoidable input and edge terms

M4 defines `P-` but states
`O(sum d(p) + A + R + Delta)` (`docs/m4_implementation_plan.md:108-123,423-435`).
Even when every deleted chunk has degree zero, the runtime must enumerate the
`P-` chunks. Candidate and observation degrees also need separation, and job,
frontier, and answer-fanout writes are not all represented by status-boundary
`Delta`.

**Contract correction.** Use, under expected O(1) hash lookups:

```text
O(P- + sum_p(d_obs(p) + d_candidate(p))
  + A + J_frontier + J_children + X_claim + X_answer)
```

where `X_claim` and `X_answer` count complete maintained rows touched, not only
enum transitions. State ordered-index and PostgreSQL B-tree factors
separately. Neural, embedding, lexical, and ANN work remains excluded and is
reported independently. The bound is output-sensitive, not sublinear in
dense/high-fanout cases.

**Falsifying tests.** T30 delete many zero-degree chunks and confirm work grows
with `P-`; T31 one high-fanout chunk visits every stored reverse edge exactly
once; T32 candidate and observation overlap is deduplicated for claim repair
but not omitted from edge-read accounting.

### P1-6 — Replacement needs an explicit durable transaction boundary

The in-memory replacement is correct, but M4 has no durable implementation yet.
The structural transaction must include event conflict checking, old validity
closure, new immutable rows, exact withdrawal, working-state deltas, root-job
declaration, discovery scope, and epoch PENDING status. Model work must not be
inside it. Any failure rolls back all rows and does not consume the event ID,
matching D-19.

**Falsifying tests.** T33 inject failure after old validity closure; T34 after
new chunks; T35 after withdrawal; T36 after the first root job. Each leaves the
pre-event database snapshot and event registry unchanged. Exact replay returns
the same epoch; same event ID/different payload conflicts.

## 6. P2 findings and integration warnings

### P2-1 — The M2 PostgreSQL adapter is a snapshot serializer, not M4 runtime

`PostgresSnapshot.capture` requires a one-to-one mapping between M1 revisions
and processed events (`src/groundloop/postgres/snapshot.py:94-113`), and
`load_snapshot` inserts every row as already sealed
(`src/groundloop/postgres/snapshot.py:255-278`). D-20 completion revisions do
not fit that convention. Keep this adapter as an oracle fixture; do not add M4
jobs to it.

### P2-2 — Existing M3 candidates need a new M4 activation layer

M3 candidates are tied to a static pipeline run, have no admitted/retired
epochs, and are indexed by claim but not chunk
(`migrations/002_m3_static_ai.sql:98-125`). They also have an immutability
trigger. M4 must append separate policy-versioned impact/frontier facts and add
reverse indexes on current candidate and observation currency by chunk. It
must not mutate or reinterpret historical M3 ranks as M4 admission results.

### P2-3 — Revision provenance must be real in M4

M3 installs observation currency with `installed_revision=0`
(`src/groundloop/ai/persistence.py:1063-1080`) because its epoch is atomically
sealed. M4 completions must write the actual owning epoch revision. Status
deltas, currency changes, job completions, and publication records must agree
on that revision.

## 7. Frozen state-machine proposal

The coordinator should adopt the following minimum state vocabulary at the
contract baseline.

```text
Epoch:
  RECEIVED -> STRUCTURAL_COMMITTED -> SEMANTIC_PENDING
           -> SEMANTIC_COMPLETE -> SEALED
           \-> FAILED
  SEMANTIC_PENDING -> DEGRADED_READY only after all jobs are terminal

Job:
  DECLARED -> RUNNING -> COMPLETED_ACTIVE
                      -> COMPLETED_INACTIVE
                      -> RETRYABLE_FAILED -> RUNNING
                      -> TERMINAL_FAILED
                      -> CANCELLED
```

Rules:

- terminal job states never transition;
- attempts and leases do not change semantic identity;
- every expandable job has exactly one immutable child closure;
- dependencies stay within one epoch and candidate policy;
- allowed CORE edges are only
  `EMBED_DISCOVER -> VERIFY_PAIR` and
  `FRONTIER_RETRIEVE -> VERIFY_PAIR`;
- a child cannot be its ancestor; depth is at most one expansion edge;
- child insertion and parent terminalization are one transaction;
- a zero-child parent is not closed until its empty closure is stored;
- completion on an inactive chunk stores audit output and no active delta;
- no job can be declared after epoch `SEMANTIC_COMPLETE`;
- strict CORE sealing requires all required jobs in an allowed successful
  terminal state, every expandable job closed, policy/artifact manifests
  complete, all invariants valid, and all three structured oracles equal;
- seal uses `(epoch_id, expected_revision, expected_status)` compare-and-swap
  and atomically publishes versioned state and public net deltas.

## 8. Acceptance test matrix for the later runtime lane

| Area | Required tests |
|---|---|
| Child closure | T01-T06 |
| Versioned publication | T07-T09 |
| PENDING scope | T10-T13 |
| Single-writer and late inactive | T14-T17 |
| Degraded/failure policy | T18-T19 |
| Lease/retry/crash | T20-T22 |
| Public delta determinism | T23-T24 |
| Frontier/fallback | T25-T29 |
| Cost and withdrawal | T30-T32 |
| Replacement D-19 | T33-T36 |

Property/state-machine tests must additionally generate arbitrary legal job
orders and assert:

1. epoch ID never changes;
2. revision increases exactly once per new committed semantic transition;
3. exact replay changes nothing;
4. payload/result conflict changes nothing and raises;
5. terminal states never roll back;
6. a sealed epoch has no open job or unclosed expandable parent;
7. working and published state never alias;
8. late inactive completion can affect only its inactive-chunk currency key,
   never active maintained views;
9. replacement never exposes a half structural state;
10. equivalent completion orders produce identical sealed state and public
    deltas.

## 9. Coordinator decisions required at the contract barrier

The coordinator must resolve these items in `docs/m4_design_freeze.md` before
releasing implementation:

1. accept serial structural epochs for CORE, or explicitly fund and specify
   overlapping-epoch MVCC;
2. select append-only published-state rows or equivalent validity intervals;
3. freeze job, execution-spec, attempt, completion, and child-closure identity;
4. freeze global discovery-scope PENDING semantics;
5. disable degraded sealing in CORE and define any experimental failure policy;
6. define public net deltas separately from working transition traces;
7. freeze the frontier depth/refill invariant and M3 bootstrap behavior;
8. correct the structured-work bound;
9. require one transactional replacement operation and one transactional
   completion/expansion operation;
10. assign all shared DTOs, migration rows, and publication queries to the
    coordinator, consistent with the multi-agent ownership plan.

Until those decisions are frozen, the epoch/runtime lane should not implement
around the ambiguity. After they are frozen, the lane can implement a pure
transition planner and property tests without editing existing M1-M3 modules,
migrations, or coordinator-owned persistence.
