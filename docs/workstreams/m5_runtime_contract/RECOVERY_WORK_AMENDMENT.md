# M5-D24 Recoverable Dispatch and Durable Accounting

Status: frozen contract; implementation evidence pending

Date: 2026-08-06

Authority: this amendment supersedes runtime-addendum revision 4 where lease
recovery, durable dispatch/execution evidence, point work/timing accounting,
late-return archival, or migration-016 routing differs. It changes no M5
semantic or M4-v1 identity. The audited pre-freeze content SHA-256 was
`7fbcb57ae8a1e71d17457409f9f864418b42cc506ebc191211f476caa59e2475`.

## 1. Blocking production evidence

Runtime-addendum revision 4 and migration 015 do not jointly implement their
own reconnect and work-accounting requirements.

First, groundloop_m5_job_attempt admits attempt_state=expired, and its
transition trigger admits dispatched -> expired, but a dispatched M5 attempt
has no deadline and neither the public M5 port nor the cursor-local typed-direct
port has a total checked takeover result. A process loss can therefore leave a
RUNNING job nondispatchable forever, while redispatch without a serialized
expiry would violate the two-acquirer race contract.

Second, groundloop_m5_runtime_work is an immutable terminal relation. The
runtime mutators carry neither durable execution work nor timing coverage, and
no mutable point accumulator exists. A reconnect cannot recover confirmed work
from a blocked invocation or prove that replay did not add that work twice.

Third, committing a dispatch marker is not evidence that an external provider
was called. A process can fail after the marker commits and before it invokes
the port. Conversely, an immutable model artifact can be reused after
acquisition with no provider call. Durable dispatch, confirmed work, and
unresolved call ambiguity must therefore be reported separately.

These are contract omissions, not implementation discretion. M5.4-05,
M5.4-06, the crash/reconnect matrix, and measured evaluation remain blocked
until a numbered amendment is accepted and executable evidence passes.

## 2. Proposed M5-D24 decision and non-change boundary

M5-D24 adds:

- recoverable database-clock leases for M5 attempts and typed-direct M4
  attempts;
- total acquisition dispositions;
- immutable dispatch, execution-evidence, work-contribution, timing-
  contribution, expired-return, and post-terminal audit records;
- point-maintained event work and timing accumulators;
- exact ambiguity bounds for external-call counters; and
- migration 016 and route barriers.

It does not change:

- any M5-D1 through M5-D23 semantic observation, currency, matching,
  certificate, state, publication, or logical-result digest recipe;
- any M4-v1 pair, job, attempt, artifact, observation, receipt, publication, or
  replay identity;
- the M5 job-attempt ID, attempt-output, attempt-result, completion, work, or
  logical-result digest recipes; or
- the rule that public M4-v1 mutation behavior and bytes remain unchanged.

The new hashes below identify operational accounting artifacts. They do not
enter semantic pair, observation, state, certificate, publication, or event
logical-result identity except that the already-frozen event-work digest
continues to enter the terminal logical-result hash.

## 3. Exact operational types and wire values

### 3.1 Operational configuration

Each typed epoch binds this immutable, nonsemantic configuration:

~~~text
M5RuntimeOperationalConfig(
  lease_duration_ms: int,
  config_digest: SHA256
)
~~~

lease_duration_ms MUST be an integer in 1..86400000. Its digest MUST be:

~~~text
stable_m5_digest(
  "m5-runtime-operational-config-v1",
  *INT(lease_duration_ms))
~~~

The config is persisted in groundloop_m5_runtime_operational_config during
structural open and is recorded in evaluation provenance. It does not enter a
semantic identity.

### 3.2 Acquisition dispositions

The new wire enum M5AcquisitionDisposition has exactly:

~~~text
dispatch_new
dispatch_takeover
live_lease
result_reserved
terminal
~~~

M5JobAttempt retains its five frozen identity fields and additionally exposes
operational fields:

~~~text
lease_expires_at: timestamptz
attempt_work_digest: SHA256
~~~

Neither field enters m5-job-attempt-v2. A newly dispatched attempt stores the
canonical-zero M5RuntimeWork digest. Normal attempt settlement replaces it
once with the exact confirmed external-attempt work digest. An expired
attempt's later work is stored in the expired-return sidecar and does not
rewrite its expired row.

M5JobLease becomes:

~~~text
M5LeaseTerminalProjection(
  terminal_state: M5JobState,
  terminal_reason: M5TerminalReason | None,
  completion_digest: SHA256,
  terminal_identity_hash: SHA256
)

M5JobLease(
  logical_job_id: SHA256,
  attempt: M5JobAttempt | None,
  lease_expires_at: timestamptz | None,
  dispatch_record_digest: SHA256 | None,
  resulting_revision: int,
  disposition: M5AcquisitionDisposition,
  should_execute: bool,
  exact_replay: bool,
  terminal_projection: M5LeaseTerminalProjection | None
)
~~~

terminal_identity_hash MUST be:

~~~text
stable_m5_digest(
  "m5-lease-terminal-projection-v1",
  *ENUM("requirement"), *TEXT(logical_job_id),
  *ENUM(terminal_state), *OPTION(ENUM(terminal_reason)),
  *HASH(completion_digest))
~~~

For completed_active, terminal_reason is NULL. For completed_inactive,
terminal_failed, and cancelled it is exactly the durable completion/archive or
cancellation reason. The projection MUST agree with the complete durable M5
semantic-job completion columns and their M5JobCompletion digest; it is not
reconstructed from current semantic state.

Its total shape is:

| disposition | attempt/deadline | dispatch digest | terminal projection | should_execute | exact_replay |
|---|---:|---:|---:|---:|---:|
| dispatch_new | present | present new record | NULL | true | false |
| dispatch_takeover | present successor | present successor record | NULL | true | false |
| live_lease | present current | present current record | NULL | false | true |
| result_reserved | present completed attempt | present current record | NULL | false | true |
| terminal | latest attempt optional | matching record optional | present | false | true |

The BLOCKED-only wire value work_in_progress is added to
M5RunFailureReason. It is invalid for a durable FAILED result and is never
stored as groundloop_m5_event_result.failure_reason.

### 3.3 Typed-direct lease wrapper

Public M4 JobLease is unchanged. Cursor-local typed composition uses:

~~~text
M5TypedDirectJobLease(
  job_id: str,
  attempt_id: str | None,
  lease_token_hash: SHA256 | None,
  lease_expires_at: timestamptz | None,
  dispatch_record_digest: SHA256 | None,
  resulting_revision: int,
  disposition: M5AcquisitionDisposition,
  should_execute: bool,
  exact_replay: bool,
  already_completed: bool,
  terminal_projection: M5TypedDirectTerminalProjection | None
)
~~~

The M5-owned typed-direct projection is:

~~~text
M5TypedDirectTerminalProjection(
  terminal_state: M4JobState,
  terminal_reason: str | None,
  m4_completion_digest: SHA256 | None,
  completed_revision: int,
  terminal_identity_hash: SHA256
)

terminal_identity_hash = stable_m5_digest(
  "m5-typed-direct-terminal-projection-v1",
  *ENUM("direct"), *TEXT(job_id), *ENUM(terminal_state),
  *OPTION(TEXT(terminal_reason)),
  *OPTION(HASH(m4_completion_digest)), *INT(completed_revision))
~~~

result_reserved is illegal for a direct job. completed_active carries NULL
reason and a present frozen M4 completion digest. completed_inactive carries
terminal_reason=inactive_at_completion and a present M4 completion digest.
terminal_failed and cancelled carry the exact reason installed by the
cursor-local typed transition; their completion digest is optional according
to the unchanged M4 row. Migration 016 persists those typed-only terminal
projections in groundloop_m5_direct_terminal_projection, with primary key
(epoch_id, job_id), unique terminal_identity_hash, and a deferred validator
against the exact frozen M4 job/completion row. The row is inserted atomically
with the cursor-local terminal transition and is immutable. terminal requires
a projection for every terminal M4 state, and acquisition validates both the
sidecar and frozen M4 row before returning it. already_completed is true
exactly for completed_active or completed_inactive. This wrapper is M5-owned
and does not alter public M4 DTOs.

### 3.4 Expired-return archive reason

M5AttemptArchiveReason gains exactly:

~~~text
attempt_expired
~~~

This expands a legal ENUM value under the existing
m5-attempt-result-artifact-v2 recipe; it does not change the recipe. For a
return from an attempt that a committed takeover already replaced,
attempt_expired has precedence over epoch_failed, subject_inactive,
chunk_inactive, and job_already_terminal.

## 4. Exact lease and takeover semantics

### 4.1 Database clock boundary

Every acquisition locks, in the frozen order, the epoch/runtime header, job,
and latest attempt. Only after those locks are held does it sample PostgreSQL
clock_timestamp() exactly once as decision_time.

An attempt is nonexpired when:

~~~text
decision_time < lease_expires_at
~~~

It is takeover-eligible when:

~~~text
decision_time >= lease_expires_at
~~~

Deadline passage alone does not invalidate the old lease. Only a committed
checked takeover does. Therefore an old result or failure that locks first,
even after wall-clock deadline, may settle normally. If takeover locks and
commits first, the old return uses the expired-return path.

A new attempt deadline is:

~~~text
lease_expires_at =
  decision_time + lease_duration_ms * interval '1 millisecond'
~~~

The stored value MUST be later than decision_time. The database expression,
not a process timestamp, computes it.

### 4.2 M5 acquisition state machine

acquire_m5_job(epoch_id, expected_revision, job) is total:

1. declared or retryable_failed:
   - require expected_revision equal to the locked current revision;
   - create the next dense dispatched attempt;
   - insert its immutable dispatch record;
   - insert the canonical-zero m5_acquisition contribution keyed to that
     dispatch-record digest;
   - set the job running;
   - advance the epoch and accumulator coverage revision once;
   - return dispatch_new.
2. running plus latest dispatched and nonexpired:
   - perform zero writes;
   - return live_lease and the current revision.
3. running plus latest dispatched and takeover-eligible:
   - require exact current-revision CAS;
   - set the old attempt expired and finished_at=decision_time;
   - create the next dense dispatched attempt with a distinct lease-token hash;
   - insert its dispatch record;
   - insert the canonical-zero m5_acquisition contribution keyed to the
     successor dispatch-record digest;
   - leave the job running;
   - advance the epoch and accumulator coverage revision once;
   - return dispatch_takeover.
4. running root job plus scope=result_staged plus latest attempt=completed:
   - perform zero writes;
   - require exactly one matching root_result_staged attempt-result artifact,
     discovery-result artifact, scope.staged_result_artifact_hash, attempt
     output digest, job payload/execution identity, and epoch binding;
   - return result_reserved so the barrier/completion path consumes the
     durable result.
5. a committed latest attempt_state=result_reserved:
   - reject as an invariant failure with zero writes; result_reserved is a
     transaction-internal reservation state that MUST become completed or roll
     back before commit.
6. a terminal job:
   - perform zero writes;
   - return terminal with the exact terminal state/reason/identity projection
     and never create an attempt.
7. every other job/attempt combination:
   - reject as an invariant failure with zero writes.

For a no-write disposition, expected_revision may be behind the current
revision but MUST NOT exceed it. The complete job identity must still match.
Every acquisition that writes requires exact current-revision equality.

Two takeover acquirers serialize on the same header/job/attempt rows. The first
may expire and replace. The second must then observe the nonexpired successor
and return live_lease without another dispatch.

### 4.3 Failure and result races

The serialized winner determines behavior:

- old output/failure first: normal settlement; later acquisition observes
  result_reserved, retryable_failed, or terminal state;
- takeover first: old output is expired audit-only; old retryable or terminal
  failure conflicts with zero writes because no exact worker artifact exists
  to archive;
- cancellation or epoch failure first: the existing revision-4 terminal
  audit-only rules apply unless the attempt was already replaced, in which case
  attempt_expired remains the archive reason; and
- root-result transaction first: its committed attempt is completed and its
  scope is result_staged, so acquisition returns result_reserved without
  redispatch; a committed attempt_state=result_reserved is always rejected.

On reconnect, a terminal projection with terminal_state=terminal_failed MUST
cause the typed application to invoke fail_typed_epoch_atomically. It MUST NOT
skip the job, continue to another job, retry, enter the barrier, or seal.

## 5. Typed-direct M4 recovery

groundloop_semantic_job_attempt already stores lease_expires_at. Migration 016
does not change its identity columns or public behavior.

Only the cursor-local typed adapter may apply the D24 rule to an M4 attempt
whose epoch has a groundloop_m5_runtime_epoch header. It uses the same
decision_time comparison and outer expected-revision CAS:

~~~text
declared | retryable_failed -> leased successor
running + live leased        -> live_lease
running + expired leased     -> old expired + leased successor
any terminal M4 job           -> terminal with typed-direct projection
~~~

The existing M4 attempt IDs and deterministic lease-token hashes remain
unchanged. Public M4-v1 acquisition retains its current behavior.

Every typed-direct attempt receives an M5-owned dispatch record and execution
evidence. No attempt-work column is added to an M4-v1 contract table.
Every new or takeover direct dispatch also inserts a canonical-zero
direct_acquisition work contribution keyed to the new dispatch-record digest
before the single outer epoch revision advance.
Typed-direct stale output after takeover cannot update the M4 job, scope,
PENDING, currency, state, evaluation, or head. If it carries a complete worker
artifact, it is retained in the M5-owned expired-return sidecar; otherwise it
conflicts with zero writes.

A terminal_failed typed-direct projection requires the outer typed application
to fail the epoch immediately. It cannot be represented as already_completed,
skipped, retried, or sealed. A cancelled projection with reason=epoch_failed
has the same requirement. Other exact cancelled projections are skipped only
when their durable reason matches the typed cancellation plan.

## 6. Durable dispatch, confirmed work, and ambiguity bounds

### 6.1 Immutable dispatch record

Migration 016 creates groundloop_m5_dispatch_record with one immutable row per
M5 or typed-direct attempt:

~~~text
M5DispatchRecord(
  epoch_id: int,
  subgraph: direct | requirement,
  attempt_id: str,
  logical_job_id: str,
  attempt_ordinal: int,
  job_kind: str,
  fallback_required: bool,
  dispatched_revision: int,
  lease_expires_at: timestamptz,
  maximum_ambiguous_call_work: M5RuntimeWork,
  record_digest: SHA256
)
~~~

Its primary key is (subgraph, attempt_id), with a unique
(epoch_id, subgraph, logical_job_id, attempt_ordinal) key and FKs to the typed
epoch plus the applicable M5 or M4 attempt. The checked acquisition procedure
inserts it in the same transaction as the attempt marker.

fallback_required MUST be false for direct, reverse, and verifier jobs. A
requirement forward root reads it from the persisted root provenance in
Section 6.2.

record_digest MUST be:

~~~text
stable_m5_digest(
  "m5-dispatch-record-v1",
  *INT(epoch_id), *ENUM(subgraph), *TEXT(attempt_id),
  *TEXT(logical_job_id), *INT(attempt_ordinal), *TEXT(job_kind),
  *BOOL(fallback_required), *INT(dispatched_revision),
  *HASH(maximum_ambiguous_call_work.work_digest))
~~~

lease_expires_at is validated relationally but excluded from this digest
because it is operational wall-clock metadata.

The maximum ambiguous call vector contains only:

| job | maximum counters |
|---|---|
| direct discovery root | direct_discovery_call_count=1; embedding_model_call_count=1 |
| direct verifier | direct_verifier_call_count=1; verifier_model_call_count=1 |
| requirement forward | requirement_forward_retrieval_call_count=1; embedding_model_call_count=1; fallback counter=1 iff fallback_required |
| requirement reverse | requirement_reverse_retrieval_call_count=1; embedding_model_call_count=1 |
| requirement verifier | requirement_verifier_call_count=1; verifier_model_call_count=1 |

Every other maximum-work counter is zero. These are upper bounds, not event
work and not evidence that a call happened.

An attempt may invoke each call represented in its maximum vector at most
once. An external adapter MUST NOT hide an internal retry inside one attempt;
every retry requires a checked dense successor attempt and its own dispatch
record. This makes the upper vector a falsifiable bound rather than an
assumption about provider behavior.

### 6.2 Persisted fallback provenance

Structural open creates exactly one immutable row per requirement forward root:

~~~text
M5RequirementRootProvenance(
  epoch_id: int,
  root_job_id: SHA256,
  fallback_required: bool,
  provenance_digest: SHA256
)
~~~

Its primary key is (epoch_id, root_job_id), and it has an FK to that epoch's
M5 root job and discovery scope.

fallback_required is true exactly when the structural-open withdrawal plan
contains the root's requirement/policy fallback key, including when the same
root also satisfies another declaration reason. Its digest is:

~~~text
stable_m5_digest(
  "m5-requirement-root-provenance-v1",
  *INT(epoch_id), *TEXT(root_job_id), *BOOL(fallback_required))
~~~

Replay validates the row exactly. This provenance does not enter the M5-D14
root identity.

### 6.3 Immutable execution evidence

At most one immutable execution-evidence row exists per attempt:

~~~text
M5AttemptExecutionEvidence(
  epoch_id: int,
  subgraph: direct | requirement,
  attempt_id: str,
  disposition:
    returned | reused_artifact | retryable_failure | terminal_failure,
  result_or_error_hash: SHA256,
  attempt_work: M5RuntimeWork,
  attempt_timing_digest: SHA256,
  evidence_digest: SHA256
)
~~~

The relation's primary key is (subgraph, attempt_id), with FKs to the immutable
dispatch record and applicable attempt. An execution-evidence row and an
expired-return row may share an attempt only through the exact binding in
Section 8.

evidence_digest MUST be:

~~~text
stable_m5_digest(
  "m5-attempt-execution-evidence-v1",
  *INT(epoch_id), *ENUM(subgraph), *TEXT(attempt_id),
  *ENUM(disposition), *HASH(result_or_error_hash),
  *HASH(attempt_work.work_digest), *HASH(attempt_timing_digest))
~~~

attempt_work is external-execution work only. It MUST satisfy the counter-owner
matrix in Section 7.4. A reused artifact has every external-call/model-call and
token counter zero. A returned or failed attempt may charge at most the
dispatch record's maximum call vector for each call counter. A fallback
forward call has fallback count equal to its confirmed forward-retrieval call
count.

### 6.4 Exact ambiguity report

At any audit cutoff:

~~~text
D = all durable dispatch records
C = dispatches having execution evidence
U = D minus C

confirmed_call_lower[counter] =
  sum execution-evidence attempt_work[counter]

possible_call_upper[counter] =
  confirmed_call_lower[counter]
  + sum dispatch.maximum_ambiguous_call_work[counter] for dispatch in U
~~~

This calculation applies only to the frozen external/retrieval/model call
counters listed in Section 6.1. Tokens, bytes, and timing for an unresolved
dispatch are reported unknown, not guessed.

The point event-work accumulator contains confirmed work only. Dispatch count,
U, lower bounds, and upper bounds are a separate audit/evaluation table and do
not enter the terminal logical-result hash.

## 7. Exact durable event work

### 7.1 Immutable contribution and point accumulator

Migration 016 creates:

~~~text
groundloop_m5_runtime_work_contribution(
  epoch_id,
  contribution_kind,
  source_id,
  source_identity_hash,
  every M5RuntimeWork counter in frozen order,
  work_digest,
  applied_revision,
  PRIMARY KEY(epoch_id, contribution_kind, source_id)
)

groundloop_m5_runtime_work_accumulator(
  epoch_id PRIMARY KEY,
  every M5RuntimeWork counter in frozen order,
  work_digest,
  updated_revision,
  terminalized
)
~~~

Contribution rows are immutable. The contribution key digest is:

~~~text
stable_m5_digest(
  "m5-runtime-work-contribution-key-v1",
  *INT(epoch_id), *ENUM(contribution_kind), *TEXT(source_id))
~~~

source_identity_hash binds the immutable source payload separately. First
application inserts the contribution and adds its counters componentwise to
the accumulator in the same checked transaction. Exact replay requires the
same key, source identity, and work digest and makes zero writes. A mismatch
conflicts and changes no row.

Every revision-changing typed transition installs a contribution, including a
canonical-zero contribution, and sets updated_revision to the resulting epoch
revision. Thus updated_revision proves coverage even when work is zero.

An audit-only preterminal expired return locks the nonterminal header, inserts
its contribution, and updates the accumulator without changing the epoch
revision; updated_revision remains equal to that locked revision. No other
zero-revision accumulator mutation is legal.

### 7.2 Contribution kinds and source identities

The exact contribution_kind values and source IDs are:

| kind | source_id | source_identity_hash |
|---|---|---|
| structural_open | structural event ID | event payload hash |
| m5_acquisition | dispatch-record digest | dispatch-record digest |
| direct_acquisition | dispatch-record digest | dispatch-record digest |
| m5_attempt_execution | M5 attempt ID | execution-evidence digest |
| direct_attempt_execution | M4 attempt ID | execution-evidence digest |
| root_result_stage | M5 attempt ID | attempt-output digest |
| root_barrier | structural event ID | barrier-completion hash |
| verifier_completion | M5 attempt ID | attempt-result-artifact hash |
| cancellation | cancellation-plan digest | cancellation-plan digest |
| terminal_job_failure | logical job ID | terminal completion/error digest |
| direct_transition | frozen M4 transition ID | frozen M4 transition digest |
| preterminal_late_return | expired attempt ID | expired-return digest |
| epoch_failure | structural event ID | failure-reason hash |
| seal | structural event ID | changed-state-set hash |

One transaction may insert multiple differently keyed contributions. The sum
is applied once before its single epoch revision advance.

m5_acquisition and direct_acquisition MUST contain canonical-zero
M5RuntimeWork. They exist for every revision-advancing dispatch_new and
dispatch_takeover transition, including a takeover whose only logical work is
attempt expiry/replacement. Their source_id is the exact immutable
dispatch-record digest, so acquisition replay cannot alias another ordinal or
lease. live_lease, result_reserved, and terminal are zero-write observations
and create no acquisition contribution.

### 7.3 Structural-open contribution

The accumulator row may be inserted internally as zero, but revision-1
structural open MUST commit with the exact structural_open contribution already
applied. It includes deactivation, withdrawal, structural hashing/
serialization, root/fallback provenance creation, dispatch-independent direct
declaration work, and any cancellation performed by that open. A reconnect
immediately after structural open therefore reads nonzero work whenever those
operations occurred.

### 7.4 Frozen counter-owner matrix

Each counter has exactly these owners:

| counters | sole owning contribution surface |
|---|---|
| deactivated chunks; withdrawn candidate/current edges | structural_open |
| direct discovery/verifier calls and their model calls/tokens | direct_attempt_execution |
| requirement forward/reverse/fallback/verifier calls and their model calls/tokens | m5_attempt_execution |
| requirement channel hits and pre-dedup selections | root_result_stage |
| admitted pairs | root_barrier |
| requirement observation artifact/effective/inactive counts | verifier_completion |
| direct observation artifact/effective/inactive counts | direct_transition |
| cancelled requirement jobs | cancellation or structural_open, never both for one job |
| late-attempt artifact count | preterminal_late_return only |
| group/claim/answer state and certificate-binding writes | the verifier_completion, direct_transition, structural_open, epoch_failure, or seal transaction that physically writes the row; one count per physical row write |
| public deltas | seal |
| bytes hashed/serialized | the contribution whose code boundary performs those exact bytes; external attempt bytes and persistence bytes are disjoint instrumentation regions |

No caller-supplied attempt_work may contain a persistence-owned counter.
Persistence computes its own contribution from rows/bytes actually touched.
This validation prevents both omission and double counting.

### 7.5 Terminal cutoff

Seal or failure:

1. applies its own contribution;
2. validates accumulator.updated_revision against the resulting terminal
   revision;
3. copies the exact accumulator image to immutable
   groundloop_m5_runtime_work(work_kind=event);
4. stores the current invocation call_work separately without adding it again
   to event work;
5. sets accumulator.terminalized=true; and
6. inserts the immutable event result in the same transaction.

current_event_work reads the accumulator for a nonterminal epoch and the
immutable event row for a terminal epoch. It never aggregates contributions or
attempts.

The non-preexisting source hashes used above are exactly:

~~~text
epoch failure = stable_m5_digest(
  "m5-epoch-failure-contribution-source-v1",
  *TEXT(structural_event_id), *ENUM(failure_reason))

terminal job failure = stable_m5_digest(
  "m5-terminal-job-failure-contribution-source-v1",
  *TEXT(logical_job_id), *ENUM(terminal_reason), *HASH(error_hash))

seal = stable_m5_digest(
  "m5-seal-contribution-source-v1",
  *TEXT(structural_event_id), *HASH(combined_status_delta_set_hash),
  *HASH(changed_state_set_hash), *TEXT(publication_id))
~~~

All other source_identity_hash values are the already-frozen hashes named in
the table. No work digest is used as a contribution key; otherwise different
work could evade conflict by creating another key.

## 8. Expired and post-terminal returns

### 8.1 Expired-return sidecar

Migration 016 creates one immutable
groundloop_m5_expired_attempt_return row per expired M5 or typed-direct
attempt. It stores:

- subgraph, epoch, attempt, job, and original lease binding;
- the exact M5 attempt output or Section 8.4 typed-direct late-return envelope;
- the referenced immutable discovery/verifier artifact closure;
- activity snapshot and cancellation attribution;
- archive_reason=attempt_expired;
- exact execution-evidence digest;
- whether the event was nonterminal or terminal at receipt; and
- an expired_return_digest.

expired_return_digest MUST be:

~~~text
stable_m5_digest(
  "m5-expired-attempt-return-v1",
  *ENUM(subgraph), *INT(epoch_id), *TEXT(attempt_id),
  *TEXT(logical_job_id), *HASH(worker_output_digest),
  *HASH(worker_artifact_hash), *INT(activity_snapshot_epoch_id),
  *INT(activity_snapshot_revision), *ENUM("attempt_expired"),
  *BOOL(received_after_terminal))
~~~

For subgraph=direct, worker_output_digest is exactly the typed-direct envelope
digest from Section 8.4; worker_artifact_hash is that envelope's exact returned
discovery/verifier artifact hash. No generic or caller-invented M4 worker hash
is legal.

This sidecar reserves the output without changing migration-015's rule that an
expired M5 attempt row has NULL attempt_output_digest. Exact output/evidence
replay is a no-op; a different output, artifact, work, or timing digest
conflicts.

For an expired requirement attempt, the transaction MUST also insert the
existing M5AttemptResultArtifact wire with terminal_audit_only and
attempt_expired. It uses running -> running when the current successor job is
nonterminal and the exact terminal state -> same state after terminal. The
replacement validator in Section 8.3 validates its attempt-output binding
through the immutable expired-return sidecar instead of the intentionally-NULL
expired attempt column. It accepts that exception only when the named attempt
has state expired, a later dense successor exists, and the expired-return plus
execution-evidence rows match. A typed-direct expired attempt has no M5
attempt-result row and uses only the M5-owned sidecars.

### 8.2 Preterminal versus post-terminal accounting

If the event is nonterminal at receipt, the expired return:

- inserts execution evidence if none exists;
- for a typed-direct attempt, inserts the Section 8.4 late-return envelope;
- inserts the expired-return sidecar;
- for a requirement attempt, inserts its matching
  terminal_audit_only/attempt_expired attempt-result artifact;
- adds its confirmed external work through the relevant attempt-execution
  contribution;
- adds exactly one preterminal_late_return persistence contribution; and
- updates the work and timing accumulators, but changes no epoch revision,
  job, scope, PENDING, currency, edge, matching, certificate, semantic state,
  or publication row.

Those rows and accumulator updates are one transaction. Exact replay validates
the whole set and writes nothing; any differing evidence, return, work, timing,
or contribution identity conflicts and rolls the whole transaction back.

If the event is terminal at receipt, it MUST NOT update the terminalized
accumulator or immutable event result. Migration 016 instead inserts this
general sidecar:

~~~text
groundloop_m5_post_terminal_attempt_audit(
  epoch_id,
  subgraph,
  attempt_id,
  return_kind,               -- expired_return | terminal_audit_only
  return_artifact_digest,
  execution_evidence_digest,
  work_digest,
  timing_digest,
  terminal_logical_result_hash,
  archived_at,
  PRIMARY KEY(epoch_id, subgraph, attempt_id)
)
~~~

For return_kind=expired_return, return_artifact_digest identifies the exact
groundloop_m5_expired_attempt_return row. For
return_kind=terminal_audit_only, it identifies the exact migration-015 M5
attempt-result artifact or the Section 8.4 typed-direct late-return envelope,
according to subgraph. A deferred validator enforces that exclusive reference,
the attempt/job/epoch/output bindings, the evidence digest, the work/timing
digests, and the already-frozen terminal logical-result hash.

A post-terminal expired return atomically inserts execution evidence, the
typed-direct envelope when applicable, the expired-return sidecar, the
matching requirement attempt-result artifact when applicable, the Section 8.5
timing observation, and this general sidecar. A post-terminal nonexpired return
atomically inserts execution evidence, the ordinary terminal_audit_only M5
attempt-result artifact or exact typed-direct envelope, the Section 8.5 timing
observation, and this general sidecar. Neither transaction inserts event-work
or event-timing contributions, touches either event accumulator, advances the
epoch revision, or changes job, scope, PENDING, currency, semantic,
publication, head, or immutable result rows.

The execution-evidence insert removes that dispatch from U in Section 6.4 and
therefore tightens the current audit lower/upper bounds even after terminal.
It does not retroactively change terminal event work or the terminal logical
result. Exact replay validates the complete atomic set and writes nothing; a
differing member conflicts.

Terminal event work is exactly the confirmed work observed through the
terminal transaction cutoff. Post-terminal work remains separately queryable
through this sidecar.

### 8.3 Narrow migration-015 replacement authority

Migration 015 bytes and its ledger row remain immutable. Migration 016 is,
however, explicitly authorized to replace only the migration-015 enforcement
objects that otherwise reject attempt_expired:

~~~text
DROP CONSTRAINT
  groundloop_m5_attempt_result_artifact_archive_reason_check
DROP CONSTRAINT
  groundloop_m5_attempt_result_artifact_check1
CREATE OR REPLACE FUNCTION
  groundloop_m5_validate_attempt_result_shape()
~~~

Migration 016 recreates both constraints under those exact names. The archive
constraint adds only attempt_expired. The disposition-shape constraint adds
only terminal_audit_only with running -> running when archive_reason is
attempt_expired. The replaced validator additionally requires the named
attempt to be expired, a later dense successor attempt to exist, and matching
expired-return plus execution-evidence rows; only for that shape, it validates
attempt_output_digest against the sidecar rather than the expired attempt's
intentionally-NULL output column. The already-installed constraint trigger
groundloop_m5_attempt_result_shape remains installed and points to the replaced
function; migration 016 MUST NOT drop or recreate that trigger.

No other preexisting migration-015 constraint, function, trigger, column,
digest recipe, byte, or ledger field may be replaced, relaxed, or modified.
This does not prohibit the explicitly additive migration-016 columns and
relations in Section 11.3. Failure anywhere in the replacement or sidecar DDL
rolls back the complete migration-016 transaction.

### 8.4 Byte-total typed-direct late-return envelope

A generic M4 worker-artifact reference is insufficient because discovery and
verifier returns have different frozen fields. Migration 016 therefore creates
one immutable groundloop_m5_typed_direct_late_return_envelope row for every
typed-direct expired or post-terminal return. Its wire shape is:

~~~text
M5TypedDirectLateReturnEnvelope(
  epoch_id: int,
  return_kind: discovery | verifier,
  job_id: str,
  attempt_id: str,
  result_artifact_id: str,
  result_artifact_hash: SHA256,
  verification_execution_present: bool | None,
  observation_eligible_for_currency: bool | None,
  requested_make_effective: bool | None,
  job_binding_digest: SHA256,
  attempt_binding_digest: SHA256,
  completion_binding_digest: SHA256,
  discovery_binding_digest: SHA256 | None,
  scope_binding_digest: SHA256 | None,
  verifier_binding_digest: SHA256 | None,
  envelope_digest: SHA256
)
~~~

The relation stores every decoded field hashed below, not only these digests.
It has primary key (epoch_id, attempt_id), unique envelope_digest, and exact FKs
to the unchanged M4 job and attempt. All sequence members use the frozen M4
values and are sorted by the keys stated below before hashing. The three
verifier-only shape fields are all NULL for discovery and all non-NULL for a
verifier return.

job_binding_digest hashes every frozen LogicalJobSpec field:

~~~text
stable_m5_digest(
  "m5-typed-direct-late-job-binding-v1",
  *TEXT(job_id), *TEXT(event_id), *ENUM(job_kind),
  *TEXT(candidate_policy_id), *HASH(payload_hash),
  *HASH(execution_spec_hash), *OPTION(TEXT(parent_job_id)),
  *OPTION(TEXT(pair.claim_id)), *OPTION(TEXT(pair.chunk_version_id)),
  *OPTION(TEXT(target_claim_id)),
  *OPTION(TEXT(target_chunk_version_id)), *BOOL(expandable))
~~~

attempt_binding_digest hashes every frozen JobAttempt field:

~~~text
stable_m5_digest(
  "m5-typed-direct-late-attempt-binding-v1",
  *TEXT(attempt_id), *TEXT(job_id), *HASH(execution_spec_hash),
  *INT(attempt_ordinal), *HASH(lease_token_hash))
~~~

completion_binding_digest hashes every frozen JobCompletion and optional
ChildClosure field. child_job_ids are sorted ascending by UTF-8 bytes:

~~~text
stable_m5_digest(
  "m5-typed-direct-late-completion-binding-v1",
  *TEXT(completion.job_id), *HASH(completion.payload_hash),
  *HASH(completion.execution_spec_hash),
  *TEXT(completion.result_artifact_id),
  *HASH(completion.result_artifact_hash),
  *ENUM(completion.terminal_state), *HASH(completion.completion_digest),
  *OPTION(TEXT(child_closure.parent_job_id)),
  *OPTION(HASH(child_closure.completion_digest)),
  *OPTION(HASH(child_closure.child_set_hash)),
  *SEQ(TEXT(child_job_id) for child_job_id in child_job_ids))
~~~

For return_kind=discovery, discovery_binding_digest and scope_binding_digest
are present and verifier_binding_digest is NULL. The discovery digest hashes
every frozen DiscoveryResult, ChannelHit, and AdmittedPair field. Channel hits
are sorted by (channel.value, rank, pair.claim_id, pair.chunk_version_id,
candidate_policy_id, channel_artifact_hash). Admitted pairs are sorted by
(fused_rank, pair.claim_id, pair.chunk_version_id, candidate_policy_id); each
reasons sequence retains its already-frozen enum order:

~~~text
stable_m5_digest(
  "m5-typed-direct-late-discovery-binding-v1",
  *TEXT(discovery.root_job_id), *TEXT(discovery.result_artifact_id),
  *HASH(discovery.result_artifact_hash),
  *BOOL(discovery.fallback_satisfied),
  *INT(channel_hit_count), *INT(admitted_pair_count),
  *HASH(channel_set_hash), *HASH(admitted_pair_set_hash),
  *SEQ(SEQ((INT(hit.epoch_id), TEXT(hit.pair.claim_id),
            TEXT(hit.pair.chunk_version_id),
            TEXT(hit.candidate_policy_id), ENUM(hit.channel), INT(hit.rank),
            OPTION(F64(hit.score)), HASH(hit.channel_artifact_hash)))
       for hit in canonical_channel_hits),
  *SEQ(SEQ((INT(pair.epoch_id), TEXT(pair.pair.claim_id),
            TEXT(pair.pair.chunk_version_id),
            TEXT(pair.candidate_policy_id), INT(pair.fused_rank),
            SEQ(ENUM(reason) for reason in pair.reasons),
            BOOL(pair.mandatory_lineage)))
       for pair in canonical_admitted_pairs))

scope_binding_digest = stable_m5_digest(
  "m5-typed-direct-late-scope-binding-v1",
  *TEXT(scope.root_job_id), *INT(epoch_id),
  *TEXT(scope.registry_snapshot_id),
  *SEQ(TEXT(claim_id) for claim_id in scope.registered_claim_ids),
  *BOOL(scope.closed), *ENUM(persisted_scope_kind),
  *OPTION(SEQ(TEXT(claim_id) for claim_id in explicit_claim_ids)),
  *OPTION(INT(closed_revision)))
~~~

The counts, channel_set_hash, admitted_pair_set_hash, scope kind, explicit ID
shape, and closed revision are recomputed with the unchanged M4 procedures;
they are stored even though they are derivable so replay is byte-total rather
than dependent on a future M4 query.

For return_kind=verifier, verifier_binding_digest is present and both
discovery digests are NULL. It hashes the exact returned result artifact,
unchanged M4 verification-execution artifact fields, SemanticObservation,
ModelStamp, persisted observation provenance, and requested activity
disposition:

~~~text
stable_m5_digest(
  "m5-typed-direct-late-verifier-binding-v1",
  *TEXT(result_artifact_id), *HASH(result_artifact_hash),
  *BOOL(verification_execution_present),
  *OPTION(SEQ((
    TEXT(verification_execution.observation_id),
    TEXT(verification_execution.job_id),
    HASH(verification_execution.admitted_pair_id),
    TEXT(verification_execution.model_artifact_id),
    TEXT(verification_execution.prompt_artifact_id),
    HASH(verification_execution.execution_spec_hash),
    HASH(verification_execution.pair_input_hash),
    TEXT(verification_execution.calibration_version),
    HASH(verification_execution.calibration_artifact_sha256),
    F64(verification_execution.temperature),
    SEQ(F64(logit) for logit in verification_execution.raw_logits),
    HASH(verification_execution.raw_output_hash),
    OPTION(TEXT(verification_execution.reused_from_observation_id))))),
  *TEXT(observation.observation_id), *ENUM(observation.subject_kind),
  *TEXT(observation.subject_id), *TEXT(observation.chunk_version_id),
  *TEXT(observation.task_type), *F64(observation.support_score),
  *F64(observation.refute_score), *F64(observation.neutral_score),
  *TEXT(observation.producer.model_id),
  *TEXT(observation.producer.model_version),
  *TEXT(observation.producer.prompt_version),
  *TEXT(observation.input_hash), *INT(observation_produced_epoch),
  *HASH(observation_raw_output_hash),
  *BOOL(observation_eligible_for_currency),
  *BOOL(requested_make_effective))
~~~

verification_execution_present is true exactly when the single OPTION branch
is present. In that branch every groundloop_m4_verification_execution column
shown above is present as one tuple; only reused_from_observation_id is
independently optional inside the present tuple. raw_logits has exactly three
finite F64 members. SQL rejects a partially NULL tuple, a present tuple with a
false flag, or an absent tuple with a true flag.

When the branch is present, verification_execution.observation_id and job_id
equal the enclosing observation/job, and observation_raw_output_hash equals
verification_execution.raw_output_hash. When it is absent,
observation_raw_output_hash MUST equal completion.result_artifact_hash, exactly
matching the unchanged direct insertion fallback when no M4 verification
execution writer is configured.

observation_eligible_for_currency binds the would-be
groundloop_semantic_observation.eligible_for_currency column and MUST be true
for the unchanged direct insertion path. It is distinct from
requested_make_effective: an inactive/late observation may remain eligible for
currency while requested_make_effective=false because it is not installed as
current. The validator compares every observation column, including
eligible_for_currency, with an already-existing immutable observation row when
one exists; absence of that M4 row does not authorize inserting it on the late
path.

The envelope digest is:

~~~text
stable_m5_digest(
  "m5-typed-direct-late-return-envelope-v1",
  *INT(epoch_id), *ENUM(return_kind), *HASH(job_binding_digest),
  *HASH(attempt_binding_digest), *HASH(completion_binding_digest),
  *OPTION(HASH(discovery_binding_digest)),
  *OPTION(HASH(scope_binding_digest)),
  *OPTION(HASH(verifier_binding_digest)))
~~~

The checked procedure reconstructs and validates the unchanged M4 DTOs and
their existing job, completion, child-set, admitted-pair, channel-set, and
observation identities before inserting the envelope. It requires the job,
attempt, epoch, execution spec, lease token, artifact, completion, scope, and
observation closure to agree field-for-field. It never inserts or changes an
M4 job, attempt, discovery result, scope, observation, currency, completion,
or public M4 DTO byte.

The typed application derives this total envelope at the worker-return
boundary, while it still has the exact discovery/scope or
verifier/artifact/observation objects. It transports it through M5-owned types
and this cursor-local M5-only port:

~~~text
archive_direct_late_return(
  cursor,
  epoch_id,
  expected_revision,
  lease: M5TypedDirectJobLease,
  envelope: M5TypedDirectLateReturnEnvelope,
  attempt_work: M5RuntimeWork,
  attempt_timing: M5RuntimeTiming | None
) -> M5DirectLateReturnReceipt
~~~

The method receives verification_execution_present plus the total optional
tuple and both observation_eligible_for_currency and requested_make_effective
as explicit M5-owned envelope fields; it does not attempt to infer absent
worker bytes from a terminal M4 row. It inserts the applicable Section 8
sidecars under the caller's outer cursor and never commits or opens a nested
transaction. No public M4 protocol, JobLease, LogicalJobSpec, JobCompletion,
DiscoveryResult, SemanticObservation, or method signature changes.

Required digest goldens cover:

1. verification_execution_present=true with every required execution field
   present and reused_from_observation_id=NULL;
2. the same present tuple with a non-NULL reused_from_observation_id producing
   a distinct digest;
3. verification_execution_present=false with the entire OPTION branch absent
   and observation_raw_output_hash=completion.result_artifact_hash;
4. deletion of each required member from a present tuple, either flag/OPTION
   mismatch, a non-three/nonfinite raw_logits sequence, and any non-NULL member
   in an absent branch all rejecting;
5. an absent branch with a differing observation_raw_output_hash rejecting;
6. observation_eligible_for_currency=false rejecting on unchanged direct
   insertion; and
7. requested_make_effective=true and false producing distinct verifier and
   envelope digests while observation_eligible_for_currency remains true.

The golden suite also asserts that deriving, transporting, replaying, and
conflicting this M5-owned envelope leaves the public M4 contract serialization
and API-signature snapshot byte-identical.

### 8.5 Post-terminal attempt timing bytes

A digest without its timing fields is not independently checkable. Every
post-terminal return therefore inserts this immutable relation in the same
transaction as execution evidence and the general audit row:

~~~text
groundloop_m5_post_terminal_attempt_timing(
  epoch_id,
  subgraph,
  attempt_id,
  required_interval_observed,
  coordinator_non_db_non_neural_ns,
  neural_wall_ns,
  postgres_roundtrip_wall_ns,
  external_io_wall_ns,
  end_to_end_wall_ns,
  postgres_server_execution_ns,
  postgres_lock_wait_ns,
  postgres_wal_bytes,
  postgres_shared_block_reads,
  observation_digest,
  attempt_timing_digest,
  recorded_at,
  PRIMARY KEY(epoch_id, subgraph, attempt_id)
)
~~~

SQL recomputes observation_digest from the complete fields using Section 9.1,
enforces its all-or-none shape, and recomputes attempt_timing_digest using the
exact epoch/subgraph/attempt recipe there. That attempt_timing_digest MUST
equal both M5AttemptExecutionEvidence.attempt_timing_digest and
groundloop_m5_post_terminal_attempt_audit.timing_digest. Exact replay requires
the same complete timing fields and both digests; a difference conflicts.

This post-terminal timing row is audit evidence only. Its transaction MUST NOT
insert groundloop_m5_runtime_timing_contribution or
groundloop_m5_transition_call_timing, update
groundloop_m5_runtime_timing_accumulator or event_timing, replace
groundloop_m5_event_timing_coverage, or change any event result/logical hash.

## 9. Executable timing boundary

Timing is measurement evidence, not correctness or semantic identity. D24 does
not claim crash-perfect client wall-clock reconstruction.

### 9.1 Timing points and all-or-none observation

There are exactly two event-timing point classes:

1. attempt_execution, whose interval ended before the settlement call and
   whose attempt_timing is inserted atomically with execution evidence; and
2. transition_call, whose client interval ends only after a state-changing
   transaction commits and is appended by the checked postcommit operation in
   Section 9.3.

For either class the five required fields are one observation unit:

~~~text
coordinator_non_db_non_neural_ns
neural_wall_ns
postgres_roundtrip_wall_ns
external_io_wall_ns
end_to_end_wall_ns
~~~

All five MUST be observed together or all five MUST be missing. SQL stores
them as either five nonnegative integers or five NULLs and rejects every
partial shape. Each optional physical field is independently observed when a
nonnegative integer is present and missing when NULL:

~~~text
postgres_server_execution_ns
postgres_lock_wait_ns
postgres_wal_bytes
postgres_shared_block_reads
~~~

The canonical observation and digest are:

~~~text
M5RuntimeTimingObservation(
  required_interval_observed: bool,
  timing: M5RuntimeTiming | None,
  observation_digest: SHA256
)

observation_digest = stable_m5_digest(
  "m5-runtime-timing-observation-v1",
  *BOOL(required_interval_observed),
  *OPTION(INT(coordinator_non_db_non_neural_ns)),
  *OPTION(INT(neural_wall_ns)),
  *OPTION(INT(postgres_roundtrip_wall_ns)),
  *OPTION(INT(external_io_wall_ns)),
  *OPTION(INT(end_to_end_wall_ns)),
  *OPTION(INT(postgres_server_execution_ns)),
  *OPTION(INT(postgres_lock_wait_ns)),
  *OPTION(INT(postgres_wal_bytes)),
  *OPTION(INT(postgres_shared_block_reads)))
~~~

When required_interval_observed=false, timing is NULL and all nine OPTION
values are NULL. When true, timing is present, the first five OPTION values are
non-NULL, and each physical OPTION is independently present or NULL. No other
shape is legal.

An attempt_execution point is keyed by its execution-evidence digest. It uses
the exact attempt_timing supplied with settlement and is immutable. Exact
replay validates that original timing digest and adds no point. Timing from the
new replay invocation is call timing, never attempt timing.

The noncircular attempt_timing_digest referenced by execution evidence is:

~~~text
stable_m5_digest(
  "m5-attempt-runtime-timing-v1",
  *INT(epoch_id), *ENUM(subgraph), *TEXT(attempt_id),
  *HASH(observation_digest))
~~~

The timing row is relationally bound to the evidence digest after both digests
have been validated.

### 9.2 Exactly one designated anchor per outer transaction

Each state-changing outer typed transaction designates exactly one
already-inserted work contribution as its transition-call anchor, even when it
invokes several cursor-local mutators and inserts several work contributions:

| outer transaction | sole designated contribution kind |
|---|---|
| structural open, including cursor-local direct open | structural_open |
| M5 new/takeover acquisition | m5_acquisition |
| standalone typed-direct new/takeover acquisition | direct_acquisition |
| M5 retryable/terminal attempt settlement | m5_attempt_execution |
| standalone typed-direct retryable settlement | direct_attempt_execution |
| standalone typed-direct terminal-job settlement while the epoch remains nonterminal | direct_attempt_execution |
| standalone outer typed-direct expansion or verifier completion | direct_transition |
| root result stage | root_result_stage |
| requirement-root barrier | root_barrier |
| requirement verifier completion | verifier_completion |
| cancellation | cancellation |
| preterminal expired return | preterminal_late_return |
| typed epoch failure, including cursor-local direct failure/terminal settlement | epoch_failure |
| successful typed seal, including cursor-local direct seal | seal |

direct_transition remains a valid work contribution whenever direct state is
physically changed, but it is a timing anchor only for a standalone outer
direct expansion or verifier-completion transaction. Cursor-local direct open,
failure, seal, expansion, completion, or attempt settlement never creates a
second timing point inside an outer structural_open, epoch_failure, seal, or
other principal transition. If direct terminal-job settlement and typed epoch
failure share one transaction, epoch_failure is the sole anchor; if the direct
job settlement commits first while the epoch remains nonterminal,
direct_attempt_execution is that transaction's sole anchor and a later epoch
failure has its own epoch_failure anchor.

Its timing anchor is:

~~~text
M5TransitionTimingAnchor(
  epoch_id: int,
  contribution_kind: str,
  source_id: str,
  contribution_key_digest: SHA256,
  anchor_revision: int,
  terminal_transition: bool
)
~~~

The contribution-key digest is the Section 7.1 recipe. The outer transaction,
not a nested cursor method, selects the anchor after all of its contributions
are known. It rejects zero or multiple selected anchors. Every nonterminal
state-changing outer transaction first resolves any older pending anchor as
one missing point, then increments expected point counts once for its sole
anchor and stores that anchor as the sole pending anchor in the timing
accumulator. Its outer receipt returns or makes derivable the exact anchor.
This is point maintenance under the held epoch row lock; it never scans timing
or work contributions.

A no-write exact replay, live_lease, result_reserved, or terminal acquisition
creates no anchor and no event timing point. An attempt_execution point and a
transition_call point for its settlement are distinct nonoverlapping
intervals, so a settlement may add one observed attempt point and one pending
transition point.

Before a terminal seal/failure freezes coverage, it resolves an older pending
anchor as missing. It then counts its own terminal transition as expected and
missing immediately and leaves no pending anchor, because its client interval
cannot end before the terminal commit. Terminal call telemetry is governed
only by Section 9.5 and never repairs immutable event coverage.

### 9.3 Checked postcommit transition append

Immediately after a nonterminal mutator returns and before external work or a
later mutation, the application invokes:

~~~text
append_transition_call_timing(
  epoch_id,
  contribution_kind,
  source_id,
  contribution_key_digest,
  anchor_revision,
  observed_timing: M5RuntimeTiming | None
) -> M5TransitionTimingReceipt
~~~

~~~text
transition_timing_digest = stable_m5_digest(
  "m5-transition-call-timing-v1",
  *INT(epoch_id), *ENUM(contribution_kind), *TEXT(source_id),
  *HASH(contribution_key_digest), *INT(anchor_revision),
  *HASH(observation_digest))

M5TransitionTimingReceipt(
  anchor: M5TransitionTimingAnchor,
  transition_timing_digest: SHA256,
  event_timing: M5RuntimeTiming,
  event_timing_coverage: M5RuntimeTimingCoverage,
  resulting_revision: int,
  exact_replay: bool
)
~~~

The operation locks the runtime header and timing accumulator and derives the
contribution key and transition_timing_digest again. It first looks up the
immutable groundloop_m5_transition_call_timing row by the complete anchor key.
An existing row with the same digest returns exact replay with zero writes; an
existing row with a different digest conflicts. Only when no row exists does
the operation require the sole pending anchor to match the complete key and
anchor_revision. observed_timing present means all five required fields were
observed; observed_timing=None means the whole required point and all four
optional fields are missing. It then inserts one immutable row, updates the
point sums and observed or missing counts, clears the pending anchor, and
changes no epoch revision.

If a later mutator wins first, that mutator inserts the missing observation row
for the pending anchor and clears it. A late append with that same missing
digest is exact replay; a late observed append has a different digest and
conflicts, so it cannot rewrite missing as observed. Therefore a crash between
the mutator commit and this append produces explicit missing coverage, not a
zero duration and not an unbounded pending chain.

Acquisition, root-barrier, and cancellation APIs gain no timing parameter.
Their exact anchor is deterministically derived from or returned with their
receipt, and the application uses this separate postcommit operation.
Transition-call timing is likewise not folded into attempt_timing.

### 9.4 Exact coverage and aggregate values

groundloop_m5_runtime_timing_accumulator stores observed sums, the sole
nullable pending anchor, updated_revision, terminalized, and these point
counts. The exact public value is:

~~~text
M5RuntimeTimingCoverage(
  required_expected_count: int,
  required_observed_count: int,
  required_missing_count: int,
  postgres_server_execution_expected_count: int,
  postgres_server_execution_observed_count: int,
  postgres_server_execution_missing_count: int,
  postgres_lock_wait_expected_count: int,
  postgres_lock_wait_observed_count: int,
  postgres_lock_wait_missing_count: int,
  postgres_wal_bytes_expected_count: int,
  postgres_wal_bytes_observed_count: int,
  postgres_wal_bytes_missing_count: int,
  postgres_shared_block_reads_expected_count: int,
  postgres_shared_block_reads_observed_count: int,
  postgres_shared_block_reads_missing_count: int,
  terminal_client_roundtrip_included: bool
)
~~~

For the required unit and each optional field, expected equals observed plus
missing at every returned cutoff. A nonterminal read reports a still-pending
anchor as missing in that read snapshot without mutating it. Terminal coverage
has no pending anchor and is frozen in
groundloop_m5_event_timing_coverage. terminal_client_roundtrip_included is
always false in that immutable sidecar.

Required aggregate fields sum observed points only. The event-level
end_to_end_wall_ns is the sum of observed active invocation intervals; it
excludes idle time between reconnects and MUST be labelled that way.
Components are independent and are not asserted to sum to it. An optional
aggregate is NULL when its observed count is zero or its missing count is
nonzero; otherwise it is the nonnegative observed sum. Canonical zero never
means missing.

M5EventRunResult gains exactly these nonsemantic fields:

~~~text
event_timing_coverage: M5RuntimeTimingCoverage
call_timing_coverage: M5RuntimeTimingCoverage
~~~

Neither field, event_timing, nor call_timing enters logical_result_hash.
event_timing and event_timing_coverage describe the durable event cutoff.
call_timing and call_timing_coverage describe only the current invocation. A
measured current call has required expected=1, observed=1, missing=0; an
unavailable measurement has expected=1, observed=0, missing=1. Each optional
field similarly has expected=1 and exactly one of observed/missing=1.
call_timing_coverage.terminal_client_roundtrip_included is true exactly when
the current terminal persistence/read database roundtrip is present in
call_timing, and false otherwise.

### 9.5 Terminal postcommit telemetry

Only after terminal commit may the application persist that terminal
invocation's call timing:

~~~text
append_terminal_invocation_telemetry(
  invocation_id,
  event_id,
  epoch_id,
  terminal_logical_result_hash,
  call_timing: M5RuntimeTiming | None,
  call_timing_coverage: M5RuntimeTimingCoverage
)
~~~

groundloop_m5_postcommit_invocation_telemetry is append-only and keyed by the
caller-generated invocation ID. It binds the exact terminal result and current
call envelope. It never invokes append_transition_call_timing, changes event
timing or event_timing_coverage, advances the epoch revision, or rewrites the
event result/logical hash. Same invocation/same bytes is a no-op; different
bytes conflict. Process loss may leave it absent.

The application may overlay the measured call_timing and
call_timing_coverage on the result object returned to that caller only after
the telemetry append. Terminal replay returns the stored event timing and
event coverage, measures only the current read call, and may append a new
invocation telemetry row. Reconnect never resets event timing to zero.

The read port adds:

~~~text
current_event_timing(epoch_id)
  -> (M5RuntimeTiming, M5RuntimeTimingCoverage)
~~~

## 10. Retryable and terminal attempt settlement

Retryable failure, root-result staging, verifier completion, and direct
completion accept exact attempt_work and
attempt_timing: M5RuntimeTiming | None. None is the all-missing observation in
Section 9.1; it is never converted to a zero timing. They:

1. validate the complete epoch/job/attempt/lease/output-or-error tuple;
2. insert immutable execution evidence;
3. replace a normal M5 attempt's canonical-zero attempt_work_digest with the
   supplied digest;
4. insert the attempt-execution work contribution and the immutable
   attempt_execution timing point keyed to the evidence digest;
5. apply persistence-owned contributions;
6. advance the epoch revision once; and
7. return the exact receipt with the designated transition-call anchor.

Exact replay requires the same error/output, work, and original timing digests
and performs zero writes. The application measures the settlement invocation
separately and uses append_transition_call_timing after the commit. Current
replay-invocation timing is never added to the original attempt point.

Nonretryable external failure must settle its attempt before or atomically with
epoch failure:

~~~text
mark_m5_terminal_failure(
  epoch_id, expected_revision, lease, terminal_reason, error_hash,
  attempt_work, attempt_timing
) -> M5AttemptCompletionReceipt
~~~

It changes running -> terminal_failed, dispatched -> failed, moves one open
owner unit to blocking failure, inserts evidence/contributions, and advances
revision once. Reconnect seeing terminal_failed must invoke the explicit typed
epoch-failure decision; it cannot retry or seal.

The typed-direct adapter exposes the corresponding cursor-local retryable and
terminal failure operations without changing public M4 APIs. Those methods
insert their execution/work evidence but do not independently select a timing
anchor. The outer transaction applies the Section 9.2 rule: standalone
nonterminal settlement uses direct_attempt_execution; settlement combined with
typed epoch failure uses epoch_failure only.

## 11. Migration 016 boundary

### 11.1 Exact bundle identity

The migration path is exactly:

~~~text
migrations/016_m5_runtime_recovery.sql
~~~

The accepted migration-015 prerequisite values are pinned, not looked up as
whatever happens to be present:

~~~text
accepted_015_bundle_id = "m5-runtime-schema-bundle-v2"
accepted_015_migration_sha256 =
  85cb7f8e6a33273ce67fc6b4160e74a3aff314cd084df7ac3647cadae930185c
accepted_015_bundle_sha256 =
  b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd
accepted_015_oracle_sha256 =
  e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
accepted_015_prerequisite_sha256 =
  9bce4ae68a434aefbc196b6367b77454f4ec65d531f4ff206f37fb4d69e9167a
~~~

Before any migration-016 DDL, the installer requires one exact ledger row
matching all five values. A missing or mismatched field aborts. The new bundle
identity is:

~~~text
bundle_id = "m5-runtime-recovery-schema-bundle-v1"

bundle_sha256 = stable_m5_digest(
  "m5-runtime-recovery-schema-bundle-v1",
  *TEXT("migrations/016_m5_runtime_recovery.sql"),
  *HASH(sha256_of_exact_016_file_bytes),
  *HASH(accepted_015_bundle_sha256))

migration_sha256 = sha256_of_exact_016_file_bytes
oracle_sha256 =
  e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
prerequisite_sha256 = accepted_015_bundle_sha256
~~~

Migration 015 and its ledger row remain unchanged historical evidence.

### 11.2 First-install preconditions and locks

The installer locks tables in this order:

1. groundloop_runtime_mode;
2. groundloop_m4_publication_head;
3. groundloop_m5_publication_head;
4. groundloop_epoch;
5. groundloop_m5_runtime_epoch;
6. groundloop_semantic_job_attempt; and
7. groundloop_m5_job_attempt.

It uses SHARE ROW EXCLUSIVE mode for each explicit installation lock, matching
migration 015.
It then rejects:

- any committed epoch whose semantic status is pending or complete;
- any existing groundloop_m5_job_attempt row; and
- any groundloop_semantic_job_attempt row whose epoch has a
  groundloop_m5_runtime_epoch header.

This is intentionally stricter than accepting terminal attempts. Migration 015
contains no exact per-attempt work/timing evidence, so migration 016 MUST NOT
infer or apportion it from terminal aggregate rows. Supporting such rows would
require a separately frozen byte-total external backfill manifest.

The no-existing-attempt precondition permits populated activation/runtime
metadata and terminal zero-attempt epochs.

### 11.3 Installed relations and alterations

Migration 016 adds lease_expires_at and attempt_work_digest to the empty
groundloop_m5_job_attempt relation, installs the checked transition rules in
Sections 4 and 10, performs only the exact constraint/function replacements in
Section 8.3, and creates exactly these relation families:

~~~text
groundloop_m5_runtime_operational_config
groundloop_m5_requirement_root_provenance
groundloop_m5_dispatch_record
groundloop_m5_direct_terminal_projection
groundloop_m5_attempt_execution_evidence
groundloop_m5_runtime_work_contribution
groundloop_m5_runtime_work_accumulator
groundloop_m5_runtime_timing_contribution
groundloop_m5_transition_call_timing
groundloop_m5_runtime_timing_accumulator
groundloop_m5_expired_attempt_return
groundloop_m5_typed_direct_late_return_envelope
groundloop_m5_post_terminal_attempt_timing
groundloop_m5_post_terminal_attempt_audit
groundloop_m5_postcommit_invocation_telemetry
groundloop_m5_event_timing_coverage
~~~

groundloop_m5_runtime_timing_contribution stores only precompleted
attempt_execution points. groundloop_m5_transition_call_timing stores checked
postcommit transition points. The timing accumulator stores their point sums,
expected/observed/missing counts, and at most one pending anchor.

groundloop_m5_typed_direct_late_return_envelope and
groundloop_m5_post_terminal_attempt_timing are immutable audit sidecars with
the exact validators in Sections 8.4 and 8.5. Neither relation is an event
timing contribution or accumulator input.

groundloop_m5_event_timing_coverage is immutable, one-to-one with
groundloop_m5_event_result, and stores the terminal
M5RuntimeTimingCoverage with no pending point. The existing timing columns on
groundloop_m5_event_result remain the event-timing values; no semantic result
hash includes the coverage sidecar.

Every enum, digest, counter, FK, uniqueness, immutability, transition, terminal
cutoff, and cross-row binding above is enforced by SQL checks, deferred
constraints, or checked procedures. Comments are not enforcement.

Exact bundle rerun checks the ledger first and is a no-op even if attempts were
created after successful installation. Same ID/different content conflicts.
Injected failure after every DDL/backfill/constraint/ledger group rolls back
all DDL and the ledger row.

### 11.4 Route barriers

Future activation requires the exact accepted 016 ledger row but does not
change activation-request or bootstrap identity. In addition, every typed open
and nonterminal typed resume checks the 016 ledger before event-ID or epoch
consumption. Thus a database activated under 015 but not upgraded to 016
cannot start or resume typed work. Read-only audit and exact terminal replay
remain available.

## 12. Corrected public and internal APIs

The M5 runtime persistence port becomes:

~~~text
acquire_m5_job(
  epoch_id, expected_revision, job
) -> M5JobLease

mark_m5_retryable_failure(
  epoch_id, expected_revision, lease, error_hash,
  attempt_work, attempt_timing
) -> M5AttemptCompletionReceipt

mark_m5_terminal_failure(
  epoch_id, expected_revision, lease, terminal_reason, error_hash,
  attempt_work, attempt_timing
) -> M5AttemptCompletionReceipt

stage_m5_discovery_result_atomically(
  epoch_id, expected_revision, lease, job, result, attempt_output,
  attempt_work, attempt_timing
) -> M5AttemptCompletionReceipt

close_m5_requirement_roots_atomically(
  epoch_id, expected_revision, requirement_root_set_hash
) -> M5RootBarrierReceipt

complete_m5_verifier_atomically(
  epoch_id, expected_revision, lease, job, pair_input,
  verifier_artifact, attempt_output, attempt_work, attempt_timing
) -> M5AttemptCompletionReceipt

cancel_m5_work_atomically(
  epoch_id, expected_revision, cancellation_plan
) -> M5CancellationReceipt

fail_typed_epoch_atomically(
  epoch_id, expected_revision, failure_reason,
  call_work
) -> M5EventRunResult

request_typed_seal_atomically(
  epoch_id, expected_revision, event,
  call_work
) -> M5EventRunResult

append_transition_call_timing(
  epoch_id, contribution_kind, source_id,
  contribution_key_digest, anchor_revision, observed_timing
) -> M5TransitionTimingReceipt

append_terminal_invocation_telemetry(
  invocation_id, event_id, epoch_id, terminal_logical_result_hash,
  call_timing, call_timing_coverage
) -> None

read_typed_event_result(
  event_id, payload_hash
) -> M5EventRunResult | None

current_revision(epoch_id) -> int
verifier_jobs(epoch_id) -> tuple[M5LogicalJobSpec, ...]
current_event_work(epoch_id) -> M5RuntimeWork
current_event_timing(epoch_id)
  -> (M5RuntimeTiming, M5RuntimeTimingCoverage)
~~~

Structural open additionally receives and persists
M5RuntimeOperationalConfig and the exact requirement-root fallback provenance.
It commits the structural_open work contribution and creates the revision-1
pending transition-call anchor. It does not accept transition-call timing
before that call has returned.

M5DiscoveryExecution, M5VerifierExecution, and M5ExternalWorkFailure carry
attempt_timing: M5RuntimeTiming | None in addition to attempt/call work. A
reused-artifact execution explicitly identifies reuse and carries zero
external-call counters.

The typed-direct cursor adapter becomes:

~~~text
acquire_direct_job(
  cursor, epoch_id, expected_revision, job, lease_token_hash
) -> M5TypedDirectJobLease

mark_direct_retryable_failure(
  cursor, epoch_id, expected_revision, lease, error_hash,
  attempt_work, attempt_timing
) -> None

mark_direct_terminal_failure(
  cursor, epoch_id, expected_revision, lease, terminal_reason, error_hash,
  attempt_work, attempt_timing
) -> None

stage_direct_expansion(
  cursor, epoch_id, expected_revision, lease,
  discovery, completion, children, attempt_work, attempt_timing
) -> None

stage_direct_verifier_completion(
  cursor, epoch_id, expected_revision, lease,
  verifier_job, completion, observation, make_effective,
  attempt_work, attempt_timing
) -> ObservationCompletionReceipt

archive_direct_late_return(
  cursor, epoch_id, expected_revision, lease,
  envelope, attempt_work, attempt_timing
) -> M5DirectLateReturnReceipt
~~~

The existing stage_direct_open, stage_direct_failure, and stage_direct_seal
remain cursor-local. All direct methods use M5-owned accounting sidecars under
the outer typed transaction and never commit, roll back, open a nested
transaction, advance a head independently, or register their own timing
anchor. They return their contribution identities to the outer owner. The
outer owner selects exactly one Section 9.2 anchor and rejects zero or multiple
selection.

Every state-changing nonterminal outer receipt exposes or deterministically
derives that sole transition anchor. Acquisition derives it from
dispatch_record_digest; root barrier derives it from the structural event ID
and barrier-completion hash; cancellation derives it from the cancellation-plan
digest. Therefore those methods need no timing parameter. The application
calls append_transition_call_timing exactly once after the outer transaction
returns and before external execution or a later outer mutator. attempt_timing
remains an independent precompleted interval carried with execution evidence.

The application handles acquisition dispositions exactly:

- dispatch_new or dispatch_takeover: append acquisition call timing, then
  invoke the external port once;
- live_lease: return BLOCKED/work_in_progress with zero model work;
- result_reserved: validate and invoke only the required barrier/completion
  consumer for the exact committed staged result;
- terminal with terminal_failed or any terminal_reason=epoch_failed:
  immediately fail the typed epoch;
- every other terminal projection: skip only according to its exact durable
  state/reason; and
- no branch spins, sleeps in a transaction, or infers expiry locally.

The same terminal projection rules apply to typed-direct leases. A terminal
state is never treated as an untyped already-completed boolean.

M5EventRunResult carries the exact event_timing_coverage and
call_timing_coverage fields from Section 9.4. Terminal mutators never accept
call_timing; the application can know it only postcommit and stores it only via
append_terminal_invocation_telemetry.

Invocation call_work is for the returned invocation envelope. Confirmed event
work is already installed through contribution rows and MUST NOT be added again
by seal or failure.

## 13. Replay, seal, and failure invariants

- Every acquisition, execution evidence, contribution, expired return, and
  post-terminal sidecar, timing point, and terminal result has one exact replay
  identity.
- An exact replay performs zero semantic, accumulator, artifact, delta,
  publication, or epoch writes.
- A differing output, error, work, original timing, contribution source,
  fallback provenance, or takeover identity conflicts and rolls back.
- Every returned timing coverage satisfies expected=observed+missing for the
  required point and each optional physical point. Required timing is
  all-observed or all-missing.
- A transition-call timing append is keyed to one prior contribution, changes
  no epoch revision, and cannot repair a pending anchor already classified as
  missing.
- Each state-changing outer transaction selects exactly one timing anchor;
  cursor-local contributions never increment expected timing coverage on their
  own.
- Measured seal reads event work/timing only by primary-key accumulator lookup.
  It never scans attempts, dispatches, or contributions.
- Terminal seal/failure freezes the accumulator before inserting the durable
  result.
- Post-terminal audit writes cannot change event work, logical result, strict
  state, either head, or any PENDING counter.
- Terminal reconnect returns stored event work, event_timing_coverage, deltas,
  references, failure, and logical-result hash; call_work is canonical zero and
  current read timing/coverage are call_timing/call_timing_coverage.

## 14. Falsifiers

Implementation is rejected if any of these can occur:

1. a nonexpired second M5 or typed-direct acquirer dispatches external work;
2. an expired attempt cannot produce exactly one dense successor;
3. two concurrent takeover acquirers both dispatch;
4. deadline equality is decided with a process clock or a timestamp sampled
   before the attempt lock;
5. a result winning before takeover is archived as expired;
6. a result losing to takeover changes current job, scope, PENDING, currency,
   matching, certificate, state, evaluation, revision, or head;
7. a retryable/terminal failure from a replaced attempt changes a row;
8. live_lease proceeds to barrier/seal instead of BLOCKED/work_in_progress;
9. result_reserved is redispatched;
10. a committed M5 attempt_state=result_reserved is accepted as durable;
11. running/result_staged/latest-completed maps to result_reserved without
    the exact matching root-result artifact, discovery artifact, scope,
    output, job, and epoch closure;
12. a revision-advancing new/takeover acquisition lacks exactly one canonical-
    zero m5_acquisition or direct_acquisition contribution keyed by its
    dispatch-record digest;
13. terminal_failed, or any terminal projection with reason=epoch_failed, is
    skipped, retried, sent to a barrier, or sealed instead of failing the typed
    epoch;
14. a terminal acquisition omits its total state/reason/identity projection or
    that projection disagrees with durable terminal rows;
15. crash after dispatch commit but before port entry increments a confirmed
    call counter;
16. immutable artifact reuse increments a model-call counter;
17. a fallback forward root lacks persisted fallback provenance or counts
    fallback twice when declarations coalesce;
18. unresolved dispatches are described as confirmed physical calls;
19. tokens, bytes, or timing for unresolved dispatches are guessed;
20. structural-open withdrawal/deactivation work disappears on immediate
    reconnect;
21. a contribution replay increments the accumulator twice;
22. the same contribution key accepts different source/work/timing bytes;
23. a zero-work revision leaves accumulator coverage behind the epoch;
24. a caller-supplied attempt_work contains persistence-owned counters;
25. a nonretryable external failure leaves its active attempt unsettled;
26. execution evidence plus the applicable envelope, artifact, expired,
    post-terminal timing, and general audit sidecars are not inserted
    atomically, do not remove the dispatch from U, or mutate event work/timing,
    revision, state, head, result, or logical hash;
27. a post-terminal still-current/nonexpired attempt result has no legal
    terminal_audit_only path through the general sidecar;
28. terminal event work is reconstructed by an attempt/contribution aggregate
    scan or process cache;
29. reconnect resets cumulative work or observed timing to zero;
30. a replay invocation's new timing is added to original event timing;
31. any subset of the five required timing fields is accepted as an observed
    point;
32. required or optional expected timing-point count differs from observed
    plus missing in a returned coverage value;
33. a later mutation can leave an older pending transition anchor unresolved
    instead of atomically classifying it missing;
34. append_transition_call_timing changes the epoch revision, scans
    contributions, accepts a different key/revision, or repairs a point
    already marked missing;
35. acquisition, barrier, or cancellation requires its own not-yet-ended call
    timing as a mutator parameter instead of returning/deriving an anchor;
36. missing terminal client-roundtrip timing is stored as zero or called
    complete;
37. terminal invocation telemetry changes event timing, event timing coverage,
    epoch revision, immutable result, or logical hash;
38. M5EventRunResult omits event_timing_coverage or call_timing_coverage, or
    either coverage field enters logical_result_hash;
39. typed-direct worker loss cannot recover without changing public M4-v1
    behavior or bytes;
40. first installation of 016 accepts an existing M5 or typed-direct attempt;
41. migration 016 guesses or apportions legacy per-attempt work/timing;
42. migration 016 accepts a migration-015 ledger row unless all five pinned
    fields in Section 11.1 match exactly: bundle_id, migration_sha256,
    bundle_sha256, oracle_sha256, and prerequisite_sha256;
43. migration 016 fails to replace exactly
    groundloop_m5_attempt_result_artifact_archive_reason_check,
    groundloop_m5_attempt_result_artifact_check1, and
    groundloop_m5_validate_attempt_result_shape(), drops/recreates the existing
    trigger, or changes any other preexisting migration-015 enforcement object
    or existing column outside the explicit additive Section 11.3 authority;
44. exact bundle rerun is not a no-op, a hash conflict is accepted, or injected
    DDL failure leaves a partial schema/ledger;
45. activation or typed open/resume proceeds without the accepted 016 ledger;
46. either public M4-v1 bytes, an existing M5 semantic digest recipe, or the
    migration-015 bytes/ledger row changes;
47. duplicate durable dispatches, confirmed calls, unresolved dispatches, and
    lower/upper call bounds are not reported separately;
48. a post-terminal timing digest is accepted without the complete immutable
    timing fields, independent digest recomputation, and exact evidence/general
    audit binding, or it enters an event timing contribution/accumulator;
49. a typed-direct late return uses a generic worker-artifact hash, aliases
    discovery and verifier shapes, omits any frozen M4 job, attempt,
    completion, artifact, scope, or observation field, or changes an M4 row or
    public DTO byte;
50. one outer state-changing transaction selects zero or multiple transition
    anchors, or cursor-local direct work creates a direct_transition timing
    anchor inside structural_open, epoch_failure, seal, or another outer
    principal transition; or
51. verification_execution_present disagrees with its whole-tuple OPTION,
    any required execution field is partially absent, the absent branch's
    observation raw-output hash differs from the completion artifact hash,
    observation_eligible_for_currency is false or conflated with
    requested_make_effective, or cursor-local late-return transport changes a
    public M4 API or byte.

Required race tests execute both serial orders for:

- output versus takeover;
- retryable failure versus takeover;
- two takeover acquirers;
- cancellation versus expired output;
- epoch failure versus expired output;
- terminalization versus expired output; and
- typed-direct completion versus takeover.

Required crash tests inject after dispatch record commit and before port entry,
after provider acceptance but before execution-evidence commit, after evidence
but before accumulator update, after contribution/accumulator update but before
revision advance, after a nonterminal mutator commits but before its timing
append, after the timing append, between each member of the atomic expired and
general post-terminal sets, and after terminal commit but before terminal
telemetry/client response.

## 15. Claim boundary

This amendment establishes recoverable at-least-once durable dispatch and
idempotent semantic effects. It does not establish exactly-once provider
execution or exact provider-call counts for an unresolved dispatch.

GroundLoop reports:

- every durable dispatch attempt;
- every confirmed execution/reuse receipt;
- confirmed external/model-call lower counts;
- possible upper counts induced by unresolved dispatches;
- exact confirmed event work through the terminal cutoff;
- separately archived post-terminal work; and
- timing values with explicit coverage and missing intervals.

Duplicate durable dispatch attempts are exact. Duplicate physical provider
calls are reported only when confirmed by execution evidence; otherwise they
remain inside the explicit ambiguity bounds. No semantic, performance,
utility, or provider-exactness claim follows from the lease mechanism alone.
