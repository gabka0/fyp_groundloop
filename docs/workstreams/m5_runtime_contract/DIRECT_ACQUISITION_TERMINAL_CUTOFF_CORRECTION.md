# M5-D24-C7 Direct Acquisition and Terminal-Cutoff Provenance Correction

Status: authoritative accepted M5-D24-C7 correction; implementation evidence
is pending and no implementation lane is authorized by this document

Date: 2026-08-18

Candidate base:
`254e9c27b0dfc74df1e02ba2d8cd04c7fa9a2c6a`

Accepted reviewed-candidate SHA-256:
`633f4fa8cb789b7a0245cb87e7f602d3946457a7448692dc1bc6ae88c1ff4441`

Authority: this correction amends the typed-direct operational
acquisition boundary and the checked provenance gate for the already-accepted
M5-D24-C5/C6 active-invocation terminal-cutoff projection. It also narrowly
supersedes D24/C1 permission for standalone direct terminal-attempt settlement,
completes the runtime-addendum Section 14.4 M4 failure closure by terminalizing
all remaining direct jobs for combined and generic typed failure, supersedes
C6's no-receipt generic failure-mutator origin with an exact-held-receipt
production method, and freezes one outer revision/anchor law for each typed
failure transaction. M5-D24-C1 through M5-D24-C7 are accepted.
Every other M5-D24 lease, dispatch, evidence, accounting, timing, migration,
replay, receipt, event-total, telemetry, and public M4-v1 rule remains
unchanged. Two independent exact-byte audits accepted the reviewed candidate
with no unresolved P0/P1. This correction does not authorize M5-D25, migration
017, an implementation lane, or an M5.4/M5.0-24 implementation promotion.

M5.0-24 is contract-`PASS` / implementation-`PENDING`. There is no active D24
implementation lane.

## 1. Defects being corrected

### 1.1 The production typed-direct acquirer cannot own attempt selection

The frozen cursor-local acquisition signature is:

~~~text
acquire_direct_job(
  cursor, epoch_id, expected_revision, job, lease_token_hash
) -> M5TypedDirectJobLease
~~~

The store locks the job and samples the database clock before it can know
whether the result is `DISPATCH_NEW`, `DISPATCH_TAKEOVER`, `LIVE_LEASE`, or
`TERMINAL`. For a dispatch it also discovers the dense next attempt ordinal
under that lock. A production caller outside the transaction therefore cannot
know which attempt and token to supply without a preflight read, inference, or
retry on an error message. All three would move the selection decision outside
the transaction that owns the lock and database-time cutoff.

The existing method validates a supplied deterministic token, but its return
contains only the M5 lease projection. It does not return the exact epoch,
unchanged requested M4 `LogicalJobSpec`, or unchanged M4 `JobAttempt` selected
or observed by the transaction. A lease has no epoch, and a zero-attempt
terminal lease cannot independently prove the requested execution
specification. A production adapter therefore lacks a byte-total operational
receipt with which to invoke the preserved M4 worker surface and validate a
later settlement.

### 1.2 Direct failing-terminal acquisition lacks a C6 checked origin

Accepted C6 admits a later requirement acquisition with exact
`TERMINAL/EPOCH_FAILED` provenance to the active-cutoff projection. It
explicitly excludes typed-direct terminal acquisition. The production direct
adapter can reach the same cutoff after the invocation already obtained a
nonterminal `OpenEventReceipt` and accumulated current-invocation work. An
unchanged ordinary reconnect would drop that work, while projecting from the
terminal state or reason alone would invent M5 failure authority that the M4
projection does not carry. The reachable direct failure cases are an exact
`TERMINAL_FAILED` job with an arbitrary nonempty direct reason, or any valid
terminal job whose exact direct reason is `epoch_failed`.

### 1.3 Direct terminal failure lacks one checked outer result

The existing cursor-local `mark_direct_terminal_failure` correctly persists
M4 terminal failure and M5-owned attempt evidence under a caller transaction.
Its `M5DirectCursorContributionReceipt` intentionally has no outer revision,
commit, replay, or event-result authority. Calling the generic application
`_fail` path afterward cannot prove that direct settlement and the M5 failed
result belong to one checked operation. C6 correctly rejects that route.

A production direct invocation must be able to settle its exact terminal
attempt and request typed epoch failure in one transaction-owning operation.
The returned provenance must retain the requested `M5RunFailureReason`
separately from the M4 terminal-reason string and must identify either the
first `FAILED` result or the exact canonical `REPLAYED` failed result.

### 1.4 Two already-frozen work-in-progress outcomes are not yet total

M5-D24 and C1 already require `LIVE_LEASE` and a direct
`expired_preterminal` successful return to stop the current invocation as
`BLOCKED/WORK_IN_PROGRESS`. The production typed-direct bridge does not yet
exist, and the current pure `M5DirectExecutionReceipt` blocked-reason check
does not admit `WORK_IN_PROGRESS`. This is implementation debt, not a new
scheduler decision.

## 2. Immutable operational acquisition receipt

The M5 runtime contract gains exactly this immutable operational DTO:

~~~text
M5TypedDirectAcquisitionReceipt(
  epoch_id: int,
  job: LogicalJobSpec,
  lease: M5TypedDirectJobLease,
  attempt: JobAttempt | None
)
~~~

`M5TypedDirectJobLease` is unchanged. `JobAttempt` is the exact unchanged
public M4-v1 value from `groundloop.m4.contracts`; `LogicalJobSpec` is the
exact unchanged public M4-v1 job supplied to and validated by the transaction.
Their fields, validation, identity recipes, and bytes do not change. The new
receipt is an in-process M5 operational value. It has no independent digest,
relation, column, ledger field, serialization format, or semantic identity.

The receipt validates exactly:

1. `epoch_id` is an exact positive integer, `job` has exact type
   `LogicalJobSpec`, `lease` has exact type `M5TypedDirectJobLease`, and
   `attempt`, when present, has exact type `JobAttempt`;
2. `job.job_id == lease.job_id`, so even a zero-attempt terminal receipt is
   bound to the exact requested and persisted execution specification;
3. `attempt` is present if and only if `lease.attempt_id` is present;
4. a present attempt has `attempt_id == lease.attempt_id`,
   `job_id == job.job_id == lease.job_id`,
   `execution_spec_hash == job.execution_spec_hash`, and
   `lease_token_hash == lease.lease_token_hash`;
5. a present attempt also satisfies the unchanged M4 deterministic recipes:

   ~~~text
   attempt.attempt_id == stable_m4_digest(
     "m4-job-attempt-v1", job.job_id, str(attempt.attempt_ordinal)
   )
   attempt.lease_token_hash == stable_m4_digest(
     "m4-lease-token-v1", job.job_id, str(attempt.attempt_ordinal)
   )
   ~~~

6. `DISPATCH_NEW`, `DISPATCH_TAKEOVER`, and `LIVE_LEASE` therefore always
   carry the exact active M4 attempt;
7. `TERMINAL` carries the exact latest durable M4 attempt when one exists and
   carries `None` only when the terminal job has no attempt; and
8. `RESULT_RESERVED` remains illegal for typed-direct acquisition.

The transaction returns the exact validated input `LogicalJobSpec`; it does
not reconstruct a job from the lease or a subset of database columns. Neither
the receipt nor a caller may reconstruct an attempt from the lease. The
`JobAttempt` is the exact value read or created under the acquisition
transaction. The application completes every receipt validation above,
including both existing deterministic recipes, before any provider call.

## 3. Transaction-owning tokenless acquisition

The production M5 typed-direct surface becomes:

~~~text
acquire_direct_job_atomically(
  epoch_id, expected_revision, job
) -> M5TypedDirectAcquisitionReceipt
~~~

This method, not its caller, owns the database transaction, checked migration-
016 route barrier, lock order, database clock sample, current-attempt read,
dense successor selection, M4 attempt/token construction, dispatch record,
zero acquisition contribution, revision advance, and transition anchor. It
returns only after that transaction commits. A rolled-back transaction yields
no usable receipt.

The method has no attempt ordinal, attempt ID, lease token, deadline, or
disposition input. Under the locked cutoff it behaves exactly:

| locked outcome | receipt and writes |
|---|---|
| new or retryable job | create the exact dense M4 attempt internally; return `DISPATCH_NEW` plus that attempt; perform the unchanged one-revision acquisition closure |
| expired running attempt | expire it and create the exact dense successor internally; return `DISPATCH_TAKEOVER` plus that successor; perform the unchanged one-revision takeover closure |
| nonexpired running attempt | return `LIVE_LEASE` plus the exact current M4 attempt; zero writes and no timing anchor |
| terminal job with prior attempt | return `TERMINAL` plus the exact latest M4 attempt and total terminal projection; zero writes and no timing anchor |
| terminal job without an attempt | return `TERMINAL` plus `attempt=None` and the total terminal projection; zero writes and no timing anchor |

Database-time equality, stale/current revision behavior, disposition flags,
dispatch and transition identities, and every M4/M5 row written by a new or
takeover acquisition remain exactly as frozen in D24. Tokenless means only
that the transaction owner constructs or reads the token; it does not change
the deterministic M4 token recipe or weaken token checking at settlement.

The existing token-bearing cursor API remains available only as a checked
compatibility surface for already-authorized lower-level composition and
regression tests:

~~~text
acquire_direct_job(
  cursor, epoch_id, expected_revision, job, lease_token_hash
) -> M5TypedDirectJobLease
~~~

It must share the same locked decision logic and continue rejecting a wrong
token. It is not a total production acquisition API. The production adapter
must not call it by pre-reading an ordinal, constructing a token outside the
transaction, retrying with another token, or interpreting an exception to
discover the selected attempt.

## 4. Checked direct failing-`TERMINAL` cutoff origin

C7 adds one acquisition origin to the existing C5/C6 active-cutoff
`REPLAYED` envelope. It creates no new envelope shape or validator.

The origin is legal only when the same application invocation:

1. retains the exact nonterminal `OpenEventReceipt` returned by its checked
   typed open or resume;
2. has accumulated the exact current-invocation `call_work`, including a
   legitimate all-zero value;
3. obtains the terminal acquisition receipt either from the Section 3
   tokenless method for the exact requested direct M4 job or from the exact
   same-lock Section 5.2.1 combined-operation loser branch;
4. validates the complete `M5TypedDirectAcquisitionReceipt`, including its
   epoch, exact requested M4 job/execution binding, lease, and exact M4 attempt
   when present;
5. receives an unchanged lease with `disposition=TERMINAL`,
   `should_execute=false`, `exact_replay=true`, a valid exact terminal
   projection, and the inclusive qualifying direct failure predicate
   `terminal_state=JobState.TERMINAL_FAILED` with its arbitrary exact nonempty
   direct reason **or** `terminal_reason` equal to the exact M4 string
   `epoch_failed` for any otherwise-valid direct terminal state. One or both
   predicates may hold; and
6. obtains `read_typed_event_result(event_id, payload_hash)` and validates a
   canonical ordinary `REPLAYED` result whose durable outcome is `FAILED` for
   the same event, payload, and held-receipt epoch.

The canonical same-event/payload/epoch failed result is the sole M5 terminal
authority. The application must not cast, translate, or map the direct
projection's terminal state or string `terminal_reason` into
`M5RunFailureReason`, and it must not require a guessed relationship between
either M4 fact and the durable M5 failure reason. The active projection
preserves the canonical result's exact durable failure reason and every other
durable field, overlaying only the invocation's actual nonterminal open
receipt and exact accumulated `call_work`, as already frozen by C5/C6.

The projection is constructed and validated before terminal-invocation
timing/coverage is completed or timing-only telemetry is appended. A missing,
sealed, malformed, wrong-event, wrong-payload, wrong-epoch, or otherwise
noncanonical result conflicts with zero fallback. It cannot become BLOCKED,
redispatch, generic failure, or ordinary zero-work replay.

A direct terminal acquisition known at initial entry/open remains an ordinary
terminal-known-at-entry replay with a terminal-projected receipt and zero
`call_work`. A valid direct terminal projection that is neither
`TERMINAL_FAILED` nor reason `epoch_failed` follows the unchanged exact M4
disposition rules and does not gain active-cutoff authority from this section.

## 5. Checked combined direct terminal-failure operation

The production M5 typed-direct surface also gains:

~~~text
fail_typed_epoch_after_direct_terminal_failure_atomically(
  epoch_id, expected_revision, job, lease,
  direct_terminal_reason, error_hash,
  attempt_work, attempt_timing,
  requested_failure_reason, open_receipt, call_work
) -> M5CheckedDirectTerminalFailureReceipt | M5TypedDirectAcquisitionReceipt

M5CheckedDirectTerminalFailureReceipt(
  direct_failure: M5DirectCursorContributionReceipt,
  requested_failure_reason: M5RunFailureReason,
  resulting_revision: int,
  terminal_result: M5EventRunResult
)
~~~

The checked receipt is an immutable M5 operational value with no digest, row, ledger
field, or semantic identity. `requested_failure_reason` is a distinct required
field. It is never inferred from `direct_terminal_reason`, `error_hash`, the
M4 job state, or the direct terminal projection.

The exact enum wire of `requested_failure_reason` is stored losslessly both as
the existing typed event result's `failure_reason` and as the existing M4
failure-projection metadata text written by the typed outer transaction. It is
re-read and validated against the receipt on every combined replay. The
arbitrary M4 `direct_terminal_reason` remains its own exact text value in the
target direct job's existing terminal projection. Those wires are neither
aliased nor required to have the same spelling, and no new sidecar or schema
field is needed to bind them.

The receipt constructor validates exactly:

1. `direct_failure` has exact type `M5DirectCursorContributionReceipt`,
   `requested_failure_reason` has exact type `M5RunFailureReason`,
   `resulting_revision` is an exact positive non-boolean integer, and
   `terminal_result` has exact type `M5EventRunResult`;
2. each immutable nested value is reconstructed through its existing exact
   dataclass validation rather than accepted by structural/duck typing;
3. `terminal_result.epoch_id == direct_failure.epoch_id`;
4. `requested_failure_reason` is terminal: it is none of
   `WORK_IN_PROGRESS`, `RETRIEVAL_UNAVAILABLE`, or `VERIFIER_UNAVAILABLE`;
5. `terminal_result.failure_reason is requested_failure_reason`, and the
   result is exactly one of the first/replay branches below; and
6. `direct_failure.direct_transition_source_id`,
   `direct_transition_source_identity_hash`,
   `direct_transition_contribution_key_digest`, and
   `observation_completion` are all NULL.

Before locks or writes, the transaction-owning method validates these common
inputs for every return branch exactly:

1. `epoch_id` and `expected_revision` are exact positive non-boolean integers;
   the input `job` has exact type `LogicalJobSpec`; the input lease has exact
   type `M5TypedDirectJobLease` and is reconstructed through its validator;
2. `job.job_id == lease.job_id`; the lease has a complete
   attempt/token/deadline/dispatch binding, and is an executable
   `DISPATCH_NEW` or `DISPATCH_TAKEOVER` lease rather than live, terminal, or
   replayed;
3. `open_receipt` has exact type `OpenEventReceipt`, is the caller-held
   nonterminal receipt with both terminal flags false and publication/failure
   fields NULL, and `open_receipt.epoch_id == epoch_id` while preserving its
   actual fresh/resumed `replayed` value;
4. `direct_terminal_reason` is exact nonempty text, `error_hash` is an exact
   valid hash, `attempt_work` and `call_work` have exact type `M5RuntimeWork`
   and exact reconstruction, and `attempt_timing` is exact
   `M5RuntimeTiming | None` with exact reconstruction when present; and
5. `requested_failure_reason` has exact type `M5RunFailureReason` and is
   terminal: it is none of `WORK_IN_PROGRESS`, `RETRIEVAL_UNAVAILABLE`, or
   `VERIFIER_UNAVAILABLE`.

For the checked-receipt return branch, the method then validates exactly:

1. `epoch_id == direct_failure.epoch_id == terminal_result.epoch_id`;
2. `job.job_id == lease.job_id == direct_failure.job_id` and
   `direct_failure.attempt_id == lease.attempt_id`;
3. the locked M4 job, exact attempt, execution-spec hash, lease-token hash,
   deadline, dispatch record, error hash, arbitrary direct reason, attempt
   work, and attempt timing all equal the supplied input and the returned
   direct evidence/contribution identities;
4. the terminal result's event ID and payload hash equal the exact durable
   typed epoch declaration, and its epoch, reason, state/outcome, open-receipt
   branch, logical identity, and `call_work` satisfy the applicable branch
   below; and
5. the application separately binds that same event/payload/epoch to this
   exact held `open_receipt` before any active-cutoff projection.

No constructor or runtime path may infer a missing job, attempt, reason,
result branch, replay fact, or call-work value from another field.

The return is an exact concrete-type XOR. A first write or exact replay of the
same checked settlement returns `M5CheckedDirectTerminalFailureReceipt`. A
terminal-cutoff loser returns `M5TypedDirectAcquisitionReceipt` only under the
exact branch in Section 5.2.1. A subclass, duck type, tuple, both-branch
wrapper, NULL branch, or exception-and-reacquire protocol is invalid.

Before constructing the checked-receipt branch, the transaction re-reads by
the returned direct receipt's exact epoch/job/attempt/evidence/contribution keys and
requires: the target M4 attempt is `failed`; the target M4 job and M5 direct
projection are `TERMINAL_FAILED`; the projection's
`terminal_reason == direct_terminal_reason`, completion digest is NULL,
completed revision is the combined final revision, and terminal identity hash
recomputes; the execution evidence disposition is `TERMINAL_FAILURE` and its
error, attempt-work, and attempt-timing identities equal the supplied values;
and the exact direct-attempt contribution binds that evidence and final
revision. The same re-read is mandatory on exact replay. Thus the compact
receipt is independently checkable from durable keys without adding a copied
projection field, and neither a forged cursor receipt nor unrelated terminal
row can authorize application projection.

For a checked first write, the transaction-owning method validates the complete
lease, M4 job/attempt, dispatch, error, work, and timing identity; stages the
checked cursor-local direct terminal-failure rows and contribution candidates
without advancing an outer header or accumulator; then performs typed epoch
failure under the same outer transaction. It commits or rolls back the direct
attempt evidence/contribution, M4 terminal state/projection, typed failure
closure, event result, work/timing accumulators, and cache-adoption decision as
one unit. It opens no independently committing nested operation.

For the checked-receipt branch, the direct cursor receipt must bind the exact epoch, lease job and attempt,
execution evidence, and direct-attempt contribution. Its three direct-
transition fields and observation completion remain NULL. The outer operation
selects only the existing `epoch_failure` terminal anchor; cursor-local direct
failure never creates a second transition anchor.

### 5.1 Exact first-write revision law and no standalone terminal state

Let the checked input `expected_revision` be `r`. The combined operation is
one successful state-changing outer runtime transaction and therefore advances
exactly once, `r -> r+1`, regardless of its row count. This is the controlling
runtime-addendum Section 14.1 and D24 Sections 7.2/9.2 law; cursor-local work
inside one outer transaction is not a sequence of independently visible
runtime transitions.

At revision `r`, the checked direct primitive stages the target M4 attempt/job
terminal-failure image, direct terminal projection, execution evidence,
attempt work/timing contribution candidates, and cache-adoption candidate. It
does so only after the full Section 5.2 lock set and target identity validate.
It does not independently advance the M4/base or M5 runtime header, work/
timing accumulator, or transition anchor. The outer epoch-failure finalizer
validates those candidates, applies them together with requirement/direct
cancellation and epoch-failure contributions, advances each applicable
accumulator once, and advances the aligned base/runtime revision once to
`r+1`.

The target direct job's `terminal_failed` row and projection, every newly
cancelled direct job and projection, the attempt evidence/contribution, event
failure, and terminal result all bind the single final revision `r+1`. The
receipt's `resulting_revision` is exactly `r+1`. The only transition timing
anchor is `EPOCH_FAILURE` at `r+1`; the direct-attempt contribution is applied
at that same revision but is not a second transition anchor. No direct
`terminal_failed`/nonterminal-epoch intermediate revision exists, even inside
the committed history.

The implementation uses one private M4 two-phase cursor-local protocol; it
never duplicates M4 lock/state SQL in an M5 module:

~~~text
PostgresM4ApplicationPorts._lock_typed_direct_epoch_failure_jobs_local(
  cursor, epoch_id, expected_revision
) -> _TypedDirectEpochFailureJobLocks

PostgresM4ApplicationPorts._lock_typed_direct_epoch_failure_details_local(
  cursor, job_locks,
  target_lease: JobLease | None
) -> _TypedDirectEpochFailureLockedPlan

PostgresM4ApplicationPorts._stage_typed_direct_epoch_failure_local(
  cursor, locked_plan,
  failure_reason: str,
  target_terminal_reason: str | None
) -> _TypedDirectEpochFailureStage

_TypedDirectEpochFailureJobLocks(
  epoch_id: int,
  expected_revision: int,
  jobs: tuple[_TypedDirectEpochFailureJobImage, ...]
)

_TypedDirectEpochFailureLockedPlan(
  job_locks: _TypedDirectEpochFailureJobLocks,
  target_job: LogicalJobSpec | None,
  target_attempt: JobAttempt | None,
  cancelled_jobs: tuple[LogicalJobSpec, ...]
)

_TypedDirectEpochFailureStage(
  target_job: LogicalJobSpec | None,
  target_attempt: JobAttempt | None,
  cancelled_jobs: tuple[LogicalJobSpec, ...],
  resulting_revision: int
)
~~~

These private immutable values have exact concrete types and reconstruction,
no digest/row/serialization, and no authority outside the live cursor and
transaction that created them. `jobs` is the complete unique C-byte-ordered
epoch job set with each exact job ID/spec/state/latest-attempt binding.
`job_locks` binds the exact epoch, input revision, cursor transaction, and
locked row images. A stale, partial, reordered, cross-cursor, structurally
copied, subclass, or duck-typed lock value conflicts.

The first helper is invoked at tier 9 after tiers 1--8. It locks every M4
direct job in C order and returns the exact snapshot without any write. The
outer M5 owner then locks the complete M5 job set through a read-only job-lock
phase in `postgres_roots.py`. At the exact tier-10+ positions fixed by the
runtime lock order, it invokes the second read-only M4 helper and the matching
read-only M5 detail-lock phase to lock attempts/evidence and produce immutable
locked plans in their frozen suborder. None of these lock helpers mutates a
job, attempt, scope, header, accumulator, projection, evidence row, or anchor.
Only after the complete tier-1-through-tier-16 set is held may the outer owner
invoke the M4 staging helper and M5 apply/finalization phase. Neither apply
phase acquires a new lock, and both revalidate the exact lock snapshots/plans
before their first write.

The existing `terminalize_m5_requirement_work_for_epoch_failure` logic in
`postgres_roots.py` is therefore split into private read-only job/detail-lock
planning and write-only apply phases. Its exact requirement/scope cancellation
semantics and contribution bytes do not change. A single helper that locks M5
jobs and attempts and then mutates before the M4 job set is known is forbidden;
so is copying that SQL into persistence or the direct adapter. The private M4
and M5 plans are cursor-local, mutually cross-validated against epoch/revision
and final job sets, and consumed once by the shared outer finalizer.

The protocol and its persistence implementation are private activated-typed-
M5 composition surfaces, not public M4-v1 API. `failure_reason` is the exact
nonempty requested typed failure-reason wire. `target_lease`, when supplied to
the detail-lock phase, is the exact unchanged public M4 lease derived from the
validated M5 operational lease; `target_terminal_reason` is then the exact
separate arbitrary direct reason supplied to staging. Target lease, plan
target fields, and terminal reason are all jointly present or all NULL. In the
combined branch staging marks the already-locked target attempt `failed` and
job `terminal_failed` with that exact reason, and cancels every other open
direct job from the plan. In the generic typed-failure branch the target is
absent and it cancels every open direct job in the plan. In both branches it
writes the exact M4 failure-projection metadata; applies exactly one existing
M4 evaluation transition,
`m4-evaluation-failure-v1` with kind `FAIL` and expected revision `r`; and
advances the M4 base header exactly once to failed revision `r+1`. It does not
also apply the target terminal-failure DELTA transition. The target terminal
state is durably represented by the job/attempt rows plus the M5 direct
projection/evidence. The staging helper's immutable private return contains the exact
optional persisted target job/attempt pair, sorted cancelled M4 job specs, and
final revision; every value is revalidated by the M5 outer owner.

M5 then installs the applicable optional target projection/evidence and every
direct-cancellation terminal projection, work/timing contributions, aligned M5
runtime/accumulator finalization, and event result at that same `r+1` before
the outer transaction commits. No public M4 DTO, method, protocol, digest, v1
route, or standalone v1 behavior changes. Without the two future M4 private-
helper paths, an M5 implementation would have to duplicate protected M4 state
SQL and must stop.

This is an explicit narrow supersession of the runtime addendum Section 17
helper-equivalence rule only for activated typed-M5 epoch failure, including
both the combined target-present branch and the generic target-absent branch.
The previously frozen M4 failure projection leaves M4 jobs and scopes
unchanged; C7 intentionally gives this private typed-M5 helper authority to
terminalize the direct job rows so postfailure direct acquisition is total. A
never-activated database, every public/v1 route, and standalone v1 failure
retain the old M4 SQL/state behavior byte-for-byte. C7 authorizes no general
replacement of M4 helper equivalence.

An exact combined replay performs zero writes and returns
`resulting_revision` equal to the exact locked durable current revision. It
must replay both the direct settlement identity and the terminal failed result.
A mixed branch -- replaying one half while first-writing the other -- is
illegal. The exact terminal-cutoff loser in Section 5.2.1 is not a mixed
branch: it performs neither half of the losing settlement and returns checked
terminal acquisition provenance from the same locked operation. Any other
partial state, including one manufactured by legacy internal calls or test
setup, conflicts.

This rule narrowly supersedes D24/C1 wherever they can be read to permit a
standalone nonretryable typed-direct attempt settlement. The cursor-local
`mark_direct_terminal_failure` remains an internal primitive, but it is legal
only while called by this combined transaction-owning operation. No caller
may commit it alone, and no production state may expose a direct
`terminal_failed` job while its typed M5 epoch remains nonterminal. Retryable
direct failure retains its unchanged standalone nonterminal behavior.

### 5.2 Total direct-job epoch-failure closure

Every production typed-failure transaction on an epoch that can contain direct
jobs follows the frozen total runtime lock order before any target or
cancellation write:

1. acquire every required tier-1-through-tier-8 row in order: runtime mode,
   both publication heads, activation, event-idempotency/base epoch, typed
   runtime header/counters, structural rows, and snapshot/scope rows;
2. at tier 9, lock the **complete** M4 direct-job set for the epoch, including
   the target, by `job_id COLLATE "C"`, then lock the complete M5 job set by
   logical job ID in the same byte order;
3. only after both job sets are fixed, acquire tier-10 attempts/evidence and
   every later required tier through tier 16 in its frozen suborder; and
4. validate the target lease/job/attempt when present and the complete locked
   job/detail plan before the private M4 staging helper writes a target,
   cancellations, failure projection, or base advance and before M5 writes any
   sidecar/finalization row.

The private lock/stage protocol uses the outer cursor and opens no nested
transaction. No implementation may lock only the target, mutate it, then
discover another direct/M5 job, acquire any earlier tier after a later tier,
or make M5 duplicate the M4 job/detail lock SQL. This complete-set-before-
write rule is why the private M4 two-phase protocol is required.

At that locked revision-`r` cutoff, the transaction derives, rather than
accepts from the caller, one of these exact direct cancellation sets:

~~~text
direct_epoch_failure_job_ids = sorted(
  job.job_id
  for every direct job in the epoch
  if (target_job_id is None or job.job_id != target_job_id)
  and job.state in {declared, running, retryable_failed}
)
~~~

For a combined direct failure the set contains every and only the other still-
nonterminal direct jobs: it excludes the validated target, which the same
helper changes to `terminal_failed`. For a generic typed failure there is no
target and the set contains every still-nonterminal direct job. Both exclude
every previously terminal direct job. The transaction validates the complete
persisted direct-job set and its epoch binding before mutation; a missing,
extra, duplicate, wrong-epoch, or changed-state member conflicts.

At the single final revision `r+1`, every selected job is changed to exact M4
state `cancelled`, with no fabricated completion or result artifact, and one
existing M5 typed-direct terminal projection is installed with exactly:

~~~text
job_id: selected job ID
terminal_state: JobState.CANCELLED
terminal_reason: "epoch_failed"
m4_completion_digest: None
completed_revision: r+1
terminal_identity_hash: existing typed-direct projection recipe
~~~

The selected job set and the newly installed projections must be a bijection.
Together they are the durable direct cancellation-plan binding: replay derives
the same sorted set from the exact `epoch_failed` projections at the terminal
revision and validates the job rows, rather than trusting a process-local
list. Existing terminal jobs and their projections are unchanged. Existing
attempt rows other than the target's required `leased -> failed` transition,
and all prior dispatch, result/evidence, and late-return rows, are not
rewritten; an in-flight output from a newly cancelled job is handled only by
the already-frozen postterminal return rules.

The existing requirement-job `M5CancellationPlan` and its work counter remain
requirement-only and unchanged. Direct cancellations are not charged to
`requirement_cancelled_job_count`, do not create a new work counter, and do
not create a second cancellation contribution or timing anchor. The outer
typed failure also writes the exact typed `failure_reason.value` (the combined
call's `requested_failure_reason.value`) to the existing M4 failure-projection
metadata and the same enum value to the M5 event result. No test-only terminal
fixture, manual row patch, inferred job
set, new plan digest, or schema change may substitute for this production
closure.

Every production typed failure on an epoch that can contain direct jobs uses
this target-optional closure. The internal production persistence protocol
therefore gains exactly:

~~~text
fail_typed_epoch_with_open_receipt_atomically(
  epoch_id, expected_revision, failure_reason, open_receipt, call_work
) -> M5EventRunResult
~~~

The method requires exact positive non-boolean epoch/revision integers,
`failure_reason` of exact type `M5RunFailureReason` excluding
`WORK_IN_PROGRESS`, `RETRIEVAL_UNAVAILABLE`, and `VERIFIER_UNAVAILABLE`, and
`call_work` of exact type/reconstruction `M5RuntimeWork`. `open_receipt` is the
exact caller-held `OpenEventReceipt`: exact concrete
type, positive matching epoch, both terminal flags false, publication and
failure fields NULL, and its actual fresh/resumed `replayed` flag preserved.
Validation and reconstruction occur before any write. The application passes
the receipt explicitly; reconstruction from database state, process caching,
an optional/default receipt, or hardcoding `replayed=false` is forbidden. The
first `FAILED` result contains that exact receipt while its durable logical
identity retains the frozen original-open `replayed=false` binding. A
canonical replay retains the frozen terminal-projected receipt and is eligible
for the already-checked active projection only through its exact provenance.
First write stores the exact requested failure enum wire; replay is legal only
when the canonical durable failed result carries that same reason. A different
reason or any malformed input conflicts with zero fallback.

This production method explicitly supersedes C6 Section 3.2's no-receipt
checked failure-mutator origin. It is the only qualifying generic typed-
failure mutator in `M5RuntimePersistencePort`, `M5TypedApplication`, and every
production adapter. The old no-receipt concrete-store method is absent from
that Protocol and is non-qualifying legacy/test compatibility. There is no
application fallback to it.

The direct-aware PostgreSQL facade implements this production method with the
private target-absent M4 helper and exact M5 projection/finalization. The R2c
group-only facade validates the held receipt and delegates the same production
signature for its provably no-direct events. The old concrete-store
`fail_typed_epoch_atomically(epoch_id, expected_revision, failure_reason,
call_work)` remains only as checked legacy/test compatibility; it is not the
production persistence protocol and is never used for an epoch that can
contain direct jobs. No optional port, hidden capability detection, or raw-
store direct failure is allowed. The new production method and checked
combined method share one cursor-local failure finalizer and open no nested
transaction.

For either target-present or target-absent first failure, the outer finalizer
applies the direct-attempt contribution when one exists and the requirement-
cancellation contribution once. In the combined case its terminal work is
the exact sum of `attempt_work` and requirement cancellation work; direct job
cancellation adds no requirement-job count. The fused timing finalizer adds
the attempt observation once, projects any prior pending transition anchor as
missing, projects the terminal `EPOCH_FAILURE` point as missing, clears the
anchor, and performs the single `r -> r+1` CAS. It never advances a separate
direct accumulator. Concretely, the shared
`finish_epoch_failure_timing_accounting` in `postgres_recovery.py` gains an
exact optional direct-attempt observation input and performs all three
operations in its one update; the separate
`_advance_timing_accumulator_for_attempt` path is forbidden for this outer
failure. Replay validates that any direct contribution's
`applied_revision` equals the terminal revision and performs zero writes.

### 5.2.1 Serialized terminal-cutoff loser

After a provider returns a nonretryable failure, another typed-failure
transaction may win first and cancel the selected running job. The combined
method decides this under its own complete lock and evidence set. Its terminal
decision table is exact:

| locked target/evidence image | result |
|---|---|
| target remains the exact executable attempt and epoch is nonterminal | perform the checked first write |
| target is `TERMINAL_FAILED` and the exact attempt evidence, contribution, direct reason, error, work, timing, typed requested reason, cancellation image, and terminal result all match | return exact `M5CheckedDirectTerminalFailureReceipt` replay |
| target is `TERMINAL_FAILED` but that complete checked identity is absent or differs | conflict; never reinterpret it as acquisition-loser provenance |
| target is exact `CANCELLED/epoch_failed`; the input attempt remains its exact latest attempt with unchanged leased state, token and dispatch binding; that attempt has no execution-evidence row; and the epoch has the canonical failed result | return the zero-write terminal-acquisition loser |
| target is `CANCELLED/epoch_failed` but its input attempt has any execution evidence, or any other terminal/evidence combination exists | conflict |

Only the fourth row returns a freshly validated
`M5TypedDirectAcquisitionReceipt` at the durable current revision. That
receipt carries the exact epoch and input job, an exact nonexecuting
`TERMINAL`/replay lease with `terminal_state=CANCELLED` and
`terminal_reason=epoch_failed`, and the exact latest `JobAttempt` or `None`
under the Section 2 rules; for this loser it is necessarily the present input
attempt. The operation also validates the canonical same-
event, same-payload, same-epoch `REPLAYED/FAILED` result under those locks.

The losing operation writes no target attempt state, direct evidence,
contribution, event work/timing, transition anchor, projection, header, or
cache. Its requested direct reason and requested M5 failure reason need not
equal the already-durable reasons and are never mapped to them. The direct
adapter selects the Section 5.3 terminal-acquisition provenance branch with
the invocation's exact accumulated `call_work`; the application then performs
its own canonical read and Section 4 active projection. The method does not
throw a stale error for the adapter to catch, reacquire the job in a second
transaction, or synthesize a combined receipt. Any terminal state without the
exact fourth-row projection, evidence absence, and canonical failed result
conflicts with zero fallback.

Exactly two checked-receipt terminal-result branches are legal:

1. **First terminal failure.** `terminal_result.state=FAILED`, with no replayed
   outcome; its failure reason is exactly `requested_failure_reason`, its open
   receipt is exactly the supplied caller-held nonterminal `open_receipt`,
   including its actual fresh/resumed `replayed` value; its `call_work` is
   exactly the supplied complete current-invocation work; and
   `resulting_revision` follows the exact `r+1` law above. The durable logical
   result identity still uses the frozen original-open binding with
   `replayed=false`; the returned resumed receipt does not alter that identity.
2. **Exact terminal replay.** `terminal_result.state=REPLAYED`, its replayed
   outcome is `FAILED`, its failure reason is exactly
   `requested_failure_reason`, its open receipt is the canonical terminal-
   projected receipt, its `call_work` is canonical zero, and
   `resulting_revision` is the locked durable current revision. It must be the
   complete canonical same-event/payload/epoch result returned by this checked
   combined operation.

A sealed result, another failure reason, a noncanonical replay, a direct
receipt for another job/attempt/evidence identity, or a partial/mixed first-
write/replay result conflicts. Exact replay performs zero writes. Changed
direct terminal reason, error, attempt work, attempt timing, requested M5 run
failure reason, cancellation set/projection, or any replay-bound identity also
conflicts.

When the checked combined operation returns the first `FAILED` branch, the
application validates and finishes that terminal result normally. When it
returns the canonical `REPLAYED` branch to the same invocation that retains
its actual earlier nonterminal receipt and accumulated work, this receipt is
the required C5/C6 provenance for the existing active-cutoff projection. The
application validates the combined receipt and canonical failed result,
constructs and validates the active envelope, and only then appends timing-
only terminal telemetry. It must not call the generic `_fail` path with a
boolean escape hatch, and an arbitrary cursor-local direct failure receipt or
later terminal read remains insufficient.

For a committed or exactly replayed checked settlement, `attempt_work` is
persisted once through the direct attempt evidence and contribution.
`call_work` is the complete application-invocation accumulator and includes
that invocation's direct attempt work exactly once. Neither value is re-added
to frozen event totals on replay or written to terminal telemetry. For the
zero-write terminal-acquisition loser, no attempt work becomes confirmed event
work; the actual provider work remains invocation-only in the active
`call_work` projection.

### 5.3 Total `run_pending_direct` provenance selection

The internal direct-subgraph protocol changes to the exact held-receipt
signature:

~~~text
M5DirectSubgraphPort.run_pending_direct(
  epoch_id, expected_revision, event, open_receipt
) -> M5DirectExecutionReceipt
~~~

`M5TypedApplication` passes the exact caller-held nonterminal
`OpenEventReceipt` returned by open/resume. The direct runner validates its
exact concrete type and reconstruction, matching positive epoch, both terminal
flags false, NULL publication/failure fields, and actual fresh/resumed
`replayed` flag before acquisition or provider work. It may not reconstruct,
cache, rediscover, omit, or default this receipt. The R2c group-only
`postgres_application.py` facade adopts the same signature, performs the same
validation, and otherwise ignores the receipt only because its validated event
has no direct subgraph. The return type remains `M5DirectExecutionReceipt`;
that app-local immutable receipt gains exactly two optional provenance fields:

~~~text
selected_terminal_acquisition_receipt:
  M5TypedDirectAcquisitionReceipt | None = None

selected_checked_combined_failure_receipt:
  M5CheckedDirectTerminalFailureReceipt | None = None
~~~

These are two distinct branches, not one union or a generic terminal flag.
Each present value has its exact concrete type and is reconstructed through
the complete nested receipt validation in Sections 2 and 5. Exactly one of
the following six application outcome shapes may be selected:

1. ordinary success with neither selected receipt, blocked reason, generic
   terminal reason, nor existing successful-outer selection;
2. one blocked reason;
3. one legacy/pure generic `terminal_failure_reason` without checked direct
   persistence provenance;
4. the existing jointly present selected-successful-outer receipt/return-
   kind/job triple;
5. one selected terminal-acquisition receipt; or
6. one selected checked-combined-failure receipt.

The two new fields are mutually exclusive with each other, with
`blocked_reason`, with `terminal_failure_reason`, and with every member of the
existing selected-successful-outer triple. A partial successful triple, two
selected provenance branches, or any selected-plus-generic/blocked mixture is
invalid.

For `selected_terminal_acquisition_receipt`, the wrapper validates:

- the nested epoch/job/lease/attempt receipt completely, including both M4
  deterministic attempt/token recipes;
- its lease is exact `TERMINAL`, nonexecuting, exact replay, and satisfies the
  inclusive `TERMINAL_FAILED OR terminal_reason=epoch_failed` predicate;
- wrapper `resulting_revision == receipt.lease.resulting_revision`;
- wrapper `call_work` is the exact prior current-invocation accumulator and
  the terminal acquisition adds zero; and
- the receipt epoch and job are the exact epoch and requested job selected by
  the direct runner, whether the receipt came from ordinary acquisition or the
  same-lock Section 5.2.1 combined-operation loser branch.

The application then validates this selected acquisition origin, performs the
canonical same-event/payload/epoch `FAILED` read, and follows Section 4. It
must not route the M4 state/reason through `terminal_failure_reason` or generic
`_fail`.

For `selected_checked_combined_failure_receipt`, the wrapper validates:

- the combined receipt and its durable re-reads completely;
- wrapper `resulting_revision == receipt.resulting_revision`;
- the direct receipt epoch/job/attempt equal the runner's exact epoch,
  selected job, and acquired lease;
- wrapper `call_work` equals the exact accumulated invocation work supplied to
  the combined operation; and
- on first `FAILED`, the nested terminal result carries that same work and the
  exact held nonterminal open receipt; on canonical `REPLAYED/FAILED`, the
  nested result carries canonical-zero work while the wrapper retains the
  exact active invocation accumulator.

The application validates a first `FAILED` branch and finishes it normally.
For the canonical replay branch only, it uses the selected combined receipt as
C7 active-cutoff authority, overlays the actual held nonterminal receipt and
wrapper `call_work`, validates the active envelope, and then finishes terminal
timing/telemetry. It never re-invokes generic failure and never converts the
combined result into `terminal_failure_reason`.

The generic `terminal_failure_reason` field remains only for already-existing
pure orchestration that carries no checked direct-persistence receipt. It
cannot represent a committed direct terminal attempt, authorize an active-
cutoff projection, or bypass the combined operation. Production typed-direct
failure with a committed/replayed exact settlement must select the checked
combined receipt branch; the exact Section 5.2.1 zero-write loser must select
the terminal-acquisition branch.

## 6. Existing work-in-progress obligations

The production bridge must preserve these already-authoritative outcomes:

1. A tokenless acquisition returning `LIVE_LEASE` invokes no provider,
   settlement, failure, barrier, or seal. It returns
   `M5DirectExecutionReceipt(blocked_reason=WORK_IN_PROGRESS)` at the exact
   durable revision, retains the invocation's exact previously accumulated
   `call_work`, adds canonical-zero work for this live acquisition, and makes
   no acquisition timing append.
2. A successful direct outer settlement returning
   `expired_preterminal` first appends its sole returned transition timing
   anchor when present, then returns
   `BLOCKED/WORK_IN_PROGRESS`. It does not retry the expired attempt, continue
   to another direct job, fail the epoch, or seal.

The pure `M5DirectExecutionReceipt` validator must therefore admit
`M5RunFailureReason.WORK_IN_PROGRESS` as a nonterminal blocked reason while
continuing to reject it as a terminal failure reason. Existing retrieval and
verifier unavailability behavior remains unchanged.

`terminal_audit_preterminal`, postterminal late returns, successful normal
returns, and other total terminal projections retain their accepted C1--C7
handling. This section does not convert every nonexecuting disposition into
work-in-progress.

## 7. Ordering, replay, and failure atomicity

The production direct application sequence is strict:

1. retain the checked nonterminal open receipt and exact current invocation
   accumulator;
2. obtain and validate the tokenless acquisition receipt before provider use;
3. add each provider execution or reuse work value once;
4. settle a successful return through the existing normal-versus-late outer
   transaction, or settle terminal failure through the Section 5 combined
   operation;
5. validate the complete selected receipt and any canonical terminal result;
6. construct and validate an active-cutoff envelope only for an accepted C5,
   C6, or C7 checked origin; and
7. only afterward complete terminal call timing/coverage and append the
   unchanged timing-only telemetry once.

No validation failure may leave an acquisition, direct attempt settlement,
event failure, work/timing contribution, anchor, result, telemetry row, head,
or process cache partially applied. Ordinary reconnect continues to read the
stored canonical result with zero current-invocation work and does not
redispatch a provider.

## 8. Post-acceptance implementation barrier

### 8.1 Mandatory read-only history guard

Even after C7 acceptance, activation must first run a read-only, consistent-
snapshot history guard against every database on which the production bridge
could be enabled. Because no pre-C7 history can have been written by the C7
combined/closure operations, the guard reports exact epoch/job/projection/
attempt/evidence/contribution IDs and revisions and rejects any of these
preexisting shapes:

1. any direct `groundloop_semantic_job` in `terminal_failed` **or** any M5
   direct terminal projection with `terminal_state=TERMINAL_FAILED`, regardless
   of whether its joined runtime is open, sealed, or failed;
2. any M5 direct terminal projection whose exact
   `terminal_reason='epoch_failed'`, regardless of terminal state; or
3. a typed runtime epoch in `failed` with any joined direct M4 job still in
   `declared`, `running`, or `retryable_failed`, proving incomplete direct-job
   epoch-failure closure.

The unconditional first two shapes also catch a formerly permitted standalone
direct terminal failure followed later by legacy epoch failure: a failed
runtime alone cannot legitimize its earlier terminal/job/contribution
revision. Those histories require an explicit separately reviewed grandfather
or backfill contract; C7 checked replay must not adopt them.

The guard also verifies that each inspected job belongs to the joined epoch;
it does not infer membership from IDs alone. It performs no UPDATE, DELETE,
backfill, projection insertion, ledger write, cache adoption, or other repair.
An exact zero-row result for all three shapes is required before activation and is
recorded with database identity, transaction timestamp, query hash, and result
hash.

If any shape exists, work **STOPS**. C7 supplies no grandfathering,
apportionment, or backfill authority. The coordinator must open and
independently audit a separate history-compatibility contract before changing
any row or enabling the bridge. Unit fixtures that intentionally construct an
illegal shape cannot stand in for the production guard.

### 8.2 Exact later manifest

This accepted correction grants no path ownership. After the coordinator
commits this accepted correction, a later separate activation must pin a fresh
branch, persistent worktree, exact
accepted-C7 base commit, focused/live gates, and an explicit path-exclusive
manifest. That manifest must name exactly these required paths:

1. `src/groundloop/m4/persistence.py`;
2. `src/groundloop/m4/pipeline.py`;
3. `src/groundloop/m5/runtime/contracts.py`;
4. `src/groundloop/m5/runtime/application.py`;
5. `src/groundloop/m5/runtime/direct_m4.py`;
6. `src/groundloop/m5/runtime/persistence.py`;
7. `src/groundloop/m5/runtime/postgres_direct_recovery.py`;
8. `src/groundloop/m5/runtime/postgres_recovery.py`;
9. `src/groundloop/m5/runtime/postgres_direct_application.py` (new);
10. `src/groundloop/m5/runtime/postgres_application.py`;
11. `src/groundloop/m5/runtime/postgres_roots.py`;
12. `tests/m5/runtime/fake_ports.py`;
13. `tests/m5/runtime/test_contracts.py`;
14. `tests/m5/runtime/test_d24_application_composition.py`;
15. `tests/m5/postgres_runtime/d24_application/test_group_requirement_composition.py`;
16. `tests/m5/postgres_runtime/d24_application/test_group_requirement_races.py`;
17. `tests/m5/postgres_runtime/d24_direct_application/conftest.py` (new);
18. `tests/m5/postgres_runtime/d24_direct_application/test_typed_direct_composition.py`
    (new);
19. `tests/m5/postgres_runtime/d24_direct_application/test_typed_direct_races.py`
    (new);
20. `tests/m5/postgres_runtime/test_direct_m4_composition.py`; and
21. `docs/workstreams/m5_runtime_implementation/D24_R2E_POSTGRES_TYPED_DIRECT_BRIDGE_HANDOFF.md`
    (new).

This required future set is not an activation and names no owner today. In
particular, the two M4 paths are mandatory for the private composite helper,
and `contracts.py` and `test_contracts.py` are mandatory because both
new immutable operational receipts and their illegal branch combinations must
be pinned before production composition begins. `postgres_application.py` is
mandatory so its group-only direct-port signature and exact-held-receipt
production failure delegation remain Protocol-compatible without gaining
direct semantics. `postgres_roots.py` is mandatory to split its bundled M5
failure locks/mutation into the cooperative read-only-plan/write-only-apply
topology. `postgres_recovery.py` is mandatory to extend the shared
`finish_epoch_failure_timing_accounting` finalizer with the optional direct-
attempt observation instead of using a second direct-accumulator CAS or copied
SQL. The existing group-requirement composition test is mandatory
because its concrete group-facade call must supply and validate the new exact
fourth held-open argument. The group-requirement race test is mandatory so its
two failure-interception subclasses override the new exact-held production
method and continue exercising real C6 same/different-reason races without a
legacy fallback. The existing direct-M4 composition test is
mandatory because its formerly legal standalone terminal-
failure/`DIRECT_ATTEMPT_EXECUTION`-anchor expectation must become a rejection/
rollback assertion or move under the combined operation. If implementation
preflight finds that another path is necessary, work stops and the coordinator
amends the committed manifest before any edit; a lane may not expand its own
scope.

The later focused evidence must cover at least:

- new, takeover, live, terminal-with-attempt, and zero-attempt-terminal
  tokenless acquisition, with exact returned M4 attempt and no caller token;
- wrong-token compatibility rejection and proof that production never uses
  token guessing, preflight selection, exception parsing, or token retry;
- exact `run_pending_direct(..., open_receipt)` fresh/resumed propagation,
  group-facade validation/no-direct no-op behavior, and rejection of a missing,
  reconstructed, cached, wrong-epoch, terminal, subclass, or duck receipt;
- both serial orders for acquisition/output/takeover and terminal cutoffs;
- direct `TERMINAL` acquisition after fresh/resumed nonterminal open for both
  a `TERMINAL_FAILED` job with arbitrary direct reason and any valid terminal
  job with exact reason `epoch_failed`, including their legal overlapping
  `TERMINAL_FAILED/epoch_failed` case, with exact zero/nonzero prior work,
  canonical failed authority, and no state/string-to-run-reason mapping;
- both serial orders where a provider's nonretryable failure races another
  generic or combined epoch failure: winner closure versus the same-lock,
  zero-write `CANCELLED/epoch_failed` terminal-acquisition loser, exact
  zero/nonzero active call work, no reason equality requirement, and no
  exception/reacquisition protocol; plus rejection of loser-shaped evidence,
  `TERMINAL_FAILED` with absent/mismatched evidence, and every other ambiguous
  target/evidence combination, including malformed common inputs and an
  expired, replaced, nonlatest, nonleased, wrong-token, or wrong-dispatch input
  attempt;
- first and replayed combined direct terminal failure, same requested reason,
  exact single-advance first-write `r+1` and replay-current revision, no mixed
  branch, private M4 two-phase lock/stage ownership of the sole base CAS, no
  independent M5 header/accumulator/anchor advance by the direct candidate,
  changed reason/error/work/timing conflicts, one anchor, rollback injection,
  reconnect, and ordinary zero-work replay, including fresh and resumed held
  nonterminal receipts on the first-write branch;
- full tier-9 M4-job snapshot before M5-job locking, read-only M4 detail locks
  and split `postgres_roots.py` M5 job/detail plans at their exact positions,
  no write until all tiers are held, and rejection of stale/partial/reordered/
  copied/cross-cursor lock plans or any new lock acquired by staging/apply;
- target-absent generic typed failure cancelling all open direct jobs and
  target-present combined failure cancelling all other open direct jobs, with
  exact fresh/resumed held-open first results; canonical replay; rejection of
  legacy no-receipt production use; and shared cursor-finalizer atomicity;
- exact `M5DirectExecutionReceipt` selection for each new provenance branch,
  including wrapper revision/work/epoch/job/attempt bindings and rejection of
  both-selected, selected-plus-success/blocked/generic, partial, malformed,
  subclass, and duck-typed combinations;
- exactly one `groundloop_m4_evaluation_counter_transition` at `r+1`, with
  kind `fail` and the exact existing `m4-evaluation-failure-v1` identity and
  payload, plus proof that no target `m4-evaluation-terminal-failure-v1` DELTA
  row exists; this must satisfy migration 013's
  `UNIQUE(epoch_id, to_revision)` constraint;
- one shared `finish_epoch_failure_timing_accounting` update containing the
  optional direct-attempt observation, prior pending-anchor missing point,
  terminal `EPOCH_FAILURE` missing point and anchor clear, with no separate
  direct accumulator advance;
- production epoch failure cancelling the exact locked sorted set of every
  remaining `declared`/`running`/`retryable_failed` direct job, installing the
  bijective `CANCELLED/epoch_failed` projections at the terminal revision,
  preserving prior terminal jobs and attempts, and making later tokenless
  acquisition return the exact direct `TERMINAL/epoch_failed` receipt;
- exact zero-row read-only guard queries and database/timestamp/query/result
  hashes for any pre-C7 direct `terminal_failed` job/projection, any direct
  `epoch_failed` projection, and any failed epoch with an open direct job,
  including the standalone-then-legacy-fail history falsifier;
- `LIVE_LEASE` and `expired_preterminal` returning
  `BLOCKED/WORK_IN_PROGRESS` without provider redispatch or seal;
- successful direct discovery and verifier outer outcomes, including the
  already-integrated R2d active-cutoff behavior;
- active-envelope validation before exactly one timing-only telemetry append;
  and
- unchanged event totals, logical identity, migration-016 hashes/ledger,
  public M4-v1 API/signature/DTO/digest snapshots, and protected unrelated
  files.

No scoped lane result by itself promotes M5.0-24 implementation to `PASS` or
any M5.4 row.

## 9. Explicit exclusions and stop conditions

C7 does not authorize:

- any public/never-activated/standalone-v1 M4 DTO, protocol, method signature,
  digest, row, or behavior change outside the exact private activated-typed-M5
  composite job transitions in Section 5;
- a field change to `M5TypedDirectJobLease`,
  `M5DirectCursorContributionReceipt`, or `M5EventRunResult`;
- a new semantic or operational digest recipe, schema object, migration,
  migration-016 byte/ledger change, migration 017, or data backfill;
- caller-selected or inferred acquisition attempt ordinals, attempt IDs,
  tokens, deadlines, or dispositions, and any inferred, derived, translated,
  or guessed direct terminal reason or M5 run failure reason. The Section 5
  combined call explicitly requires the caller's exact
  `direct_terminal_reason` and exact typed `requested_failure_reason` as two
  separate inputs;
- a process-clock lease decision, preflight SQL outside the owning
  transaction, exception-message protocol, retry-with-guessed-token loop, or
  process-local hidden authority;
- an optional, cached, reconstructed, database-inferred, or hardcoded-fresh
  `OpenEventReceipt`; production use or fallback to the legacy no-receipt
  concrete-store failure method; or a direct-capable generic failure that
  bypasses the target-optional private closure;
- an active projection from an arbitrary direct terminal read, a projection
  that is neither `TERMINAL_FAILED` nor exact reason `epoch_failed`, an
  unchecked cursor receipt, a legacy no-receipt/unchecked generic `_fail` or
  `terminal_failure_reason` return, or another unlisted origin. The new exact-
  held-receipt generic failure mutator retains only its checked C6 authority;
- rejection of the legal overlap where a `TERMINAL_FAILED` projection's exact
  arbitrary direct reason is also `epoch_failed`;
- a standalone committed `mark_direct_terminal_failure`, an externally
  visible direct `terminal_failed`/nonterminal-epoch intermediate state, a
  mixed combined first-write/replay branch, or a final first-write revision
  other than `expected_revision + 1`;
- treating the exact same-lock, zero-write terminal-cutoff loser as a mixed
  conflict; returning it through the checked-combined field or generic reason;
  requiring its requested reasons to equal the winner's durable reason; or
  discovering it by exception, second-transaction reacquisition, or polling;
- a malformed or mixed `M5DirectExecutionReceipt` selection, including both
  new provenance fields, either new field with any successful/blocked/generic
  branch, or a nested/wrapper revision, work, epoch, job, attempt, or held-open
  mismatch;
- more than one M4 evaluation transition at the combined revision, a missing/
  changed `m4-evaluation-failure-v1` `FAIL` transition, or a target
  `m4-evaluation-terminal-failure-v1` DELTA transition;
- conversion of the M4 string `epoch_failed` into an
  `M5RunFailureReason`;
- terminal epoch failure that leaves a direct job nonterminal, omits or
  mismatches one exact `CANCELLED/epoch_failed` projection, mutates an already
  terminal direct job, guesses the direct cancellation set, or uses a
  test-only/manual terminal fixture;
- double accounting of direct attempt work, mutation of frozen terminal event
  totals, work-bearing terminal telemetry, or telemetry before envelope
  validation;
- production seal/publication, provider-specific discovery/verifier/
  measurement adapters beyond the separately manifested bridge, M5-D25,
  migration 017, deployment, or a complete M5.4 claim; or
- source/test work before a separate committed post-C7 activation.

Implementation preflight or audit returns `NO_GO` if any receipt/API branch
remains non-total, if the transaction owner lacks the exact M4 attempt, if a
direct cutoff can project without its specified checked provenance, or if
compatibility is mistaken for the production acquisition surface. Any such
result keeps M5.0-24 implementation-`PENDING` and leaves all implementation
lanes stopped.

## 10. Claim boundary

C7 closes the remaining contract-level operational provenance ambiguities and makes
the already-required direct work-in-progress outcomes executable. Acceptance
alone would not prove the production bridge, recovery, exactly-once provider
execution, production seal, persisted matching, performance, neural quality,
utility, security, novelty, or M5 completion.
