# M5-D24-C4 Terminal-Successor and Late-Artifact Closure Correction

Status: accepted narrow correction; implementation evidence remains pending

Date: 2026-08-12

Accepted reviewed-candidate SHA-256:
`f2e23d0bc4c245004f677f771e45123a619d4b1ad88da207e803cb89dcfec80c`

Authority: this document amends only the requirement-attempt
expired-return state branch and the meaning of requirement late-return
artifact closure in `RECOVERY_WORK_AMENDMENT.md` Sections 4.3, 8.1--8.3, and
14, as already narrowed for cancellation by accepted M5-D24-C3. Although the
title names the terminal-successor defect that exposed the correction, the
same rule below explicitly covers the reachable interim successor state
`retryable_failed`. M5-D24-C3 remains accepted. Every other M5-D24,
M5-D24-C1, M5-D24-C2, and M5-D24-C3 identity, lease, accounting, timing,
migration, replay, receipt, typed-direct, and M4-v1 compatibility rule remains
unchanged. This correction requires no migration-016 or migration-017 change
and no public contract or digest change.

## 1. Defects being corrected

### 1.1 A terminal or retryable successor is not a running successor

After a checked takeover commits, the old requirement attempt is durably
`expired` and a later dense successor attempt exists. The successor job may
then become the interim state `retryable_failed`, or `completed_active`,
`completed_inactive`, `terminal_failed`, or `cancelled`, while other event
work keeps the event runtime nonterminal.

M5-D24 Section 8.1 truthfully permits a preterminal expired artifact with
`running -> running` only while the locked successor job is itself `running`.
It has no `retryable_failed -> retryable_failed` artifact shape. Accepted
migration 016 permits an expired artifact with an exact terminal-state image
only when `received_after_terminal=true`; its deferred validator binds that
flag to an event runtime state of `sealed` or `failed`. It does not bind the
flag merely to a terminal successor job, and its postterminal branch contains
no `retryable_failed` state.

No artifact is honest while the successor is `retryable_failed` or terminal
and the event remains nonterminal. `running -> running` would contradict the
locked job. A `retryable_failed` image is not an accepted artifact shape, and
an exact terminal-state image with `received_after_terminal=false` would
contradict the accepted SQL closure. M5-D24-C3 already resolves this
contradiction for cancellation-first. The same delayed boundary applies to
the interim retryable state and every terminal successor state.

### 1.2 Artifact closure was underspecified for requirement late returns

M5-D24 Section 8.1 says that an expired-return sidecar references an immutable
discovery/verifier artifact closure. That wording is ambiguous for an
audit-only requirement return: it can be read either as content-address
validation of the worker DTO supplied at the call boundary or as authority to
populate the normal discovery/verifier semantic artifact relations.

The late-only path must not create semantic results that never won normal job
settlement. Its audit closure is the fully validated caller-supplied DTO plus
the immutable artifact ID, artifact hash, attempt-output digest, evidence
digest, and late-row identities. It is not normal semantic-artifact
publication.

## 2. Corrected serialized outcomes

This correction applies after a checked takeover has committed a dense
successor and the old requirement attempt is durably `expired`.

| Locked successor job state | Locked event runtime state | Legal old-output outcome |
|---|---|---|
| `running` | nonterminal | unchanged `EXPIRED_PRETERMINAL` commit with `running -> running` |
| `retryable_failed` | nonterminal | conflict with zero writes; later reacquisition may return the job to `running` |
| any of `completed_active`, `completed_inactive`, `terminal_failed`, or `cancelled` | nonterminal | conflict with zero writes |
| the same exact terminal job state | `sealed` or `failed` | first legal `EXPIRED_POSTTERMINAL` commit with exact terminal state -> same state |

### 2.1 Nonterminal successor

When the old output locks first while the dense successor job remains
`running` and the event remains nonterminal, the unchanged D24 preterminal
path applies. It commits the exact expired-return audit closure with a
`terminal_audit_only/attempt_expired` artifact whose state image is
`running -> running`, applies the accepted preterminal work/timing accounting,
and advances no epoch revision.

### 2.2 Retryable or terminal successor while the event is nonterminal

When the dense successor job is `retryable_failed` or already in any terminal
state but the event runtime is still nonterminal, neither the stale old-output
call nor a retry at the current revision is archivable. Both conflict with
zero writes.

In particular, the rejected call inserts no attempt-result artifact,
expired-return row, execution evidence, work or timing contribution,
postterminal timing row, general-audit row, normal discovery/verifier semantic
artifact row, or new timing anchor. It changes no job, attempt, scope, PENDING
counter, accumulator, revision, state, result, publication, or head row.

If the job is `retryable_failed`, a later checked reacquisition may create the
next dense attempt and return the job to `running`. The old expired output may
then take the unchanged preterminal path if it locks while the job is still
`running`. If the reacquired job instead becomes terminal before the old
output locks, the output remains delayed under this section.

The caller may also retain the provider return and resubmit it after the event
is durably terminal. As in M5-D24-C3, the zero-write rejection creates no
durable proof that a later submission is the same provider return. The first
legal postterminal call therefore validates and freezes the complete supplied
identity rather than treating the rejected call as a prior replay.

### 2.3 First legal postterminal archive

After the event is durably `sealed` or `failed`, the retained expired output
may commit only against the exact locked successor job state
`completed_active`, `completed_inactive`, `terminal_failed`, or `cancelled`.
A `retryable_failed` successor must first be resolved by the outer
terminalization transaction into one of those four durable terminal states;
migration 016 defines no postterminal `retryable_failed` artifact. The first
legal archive transaction atomically inserts exactly the five applicable
requirement late-return rows:

1. `groundloop_m5_attempt_result_artifact`;
2. `groundloop_m5_expired_attempt_return`;
3. `groundloop_m5_attempt_execution_evidence`;
4. `groundloop_m5_post_terminal_attempt_timing`; and
5. `groundloop_m5_post_terminal_attempt_audit`.

The attempt-result artifact uses
`terminal_audit_only/attempt_expired`, the exact terminal job state before and
after, exact cancellation attribution when the job state is `cancelled`, and
the accepted canonical-null attribution otherwise. The expired-return row has
`received_after_terminal=true`.

This five-row archive changes no frozen event work, event timing, coverage,
logical result, revision, PENDING counter, semantic state, publication, or
head. A subsequent exact replay writes nothing. A changed worker DTO,
artifact identity, attempt output, evidence disposition, work, timing, job
state, or attribution conflicts.

`attempt_expired` retains precedence whenever the old expired output becomes
legally archivable. That precedence does not authorize a row whose state image
contradicts the locked job or event.

## 3. Requirement late-return artifact closure

For any audit-only requirement discovery or verifier return, the typed call
must still supply the complete worker DTO available at the return boundary:

- discovery supplies the complete `M5RequirementDiscoveryResult` plus the
  explicit boolean `eligible_snapshot_exhausted`; and
- verifier supplies the complete `M5RequirementPairInput` and
  `M5RequirementVerifierArtifact`.

The existing application-owned `M5DiscoveryExecution` already carries the
discovery result, attempt output, snapshot-exhaustion boolean, and work. R1-P
may receive that complete execution or equivalent explicit M5-owned
parameters at its internal persistence boundary. This is an internal checked-
persistence signature requirement, not a public M4 DTO/API or digest change.

Construction and persistence validate every DTO invariant and recompute each
declared content-addressed identity: the discovery artifact ID/hash, the pair-
input hash, and the verifier artifact ID/hash. Those identities must also
agree exactly with the job, attempt, lease, execution spec, `M5AttemptOutput`,
execution evidence, and applicable late-return rows.

Full discovery validation includes the frozen direction, candidate-policy
manifest, budget/lineage rules, and snapshot-exhaustion fact enforced by
`M5RequirementDiscoveryResult.validate_policy`. The explicit
`eligible_snapshot_exhausted` value must be supplied on every call. For
`termination=snapshot_exhausted`, it must be true and is revalidated on replay;
the termination enum alone is not exhaustion evidence. For
`termination=budget_filled`, the boolean is non-material to the policy check
and is not stored in any accepted late-row digest, so its true/false value is
not replay-bound and cannot cause a conflict. Every nested hit and selection
must also match the locked epoch and candidate policy, obey the direction/
scope restriction, and belong to the locked requirement-registry and active-
chunk snapshots.

Full verifier validation includes
`M5RequirementPairInput.validate_bound_rows` against the locked requirement-
registry and active-chunk snapshot rows and an equivalent of
`groundloop_m5_validate_pair_input_core` over all immutable core values,
including requirement ordinal and document/chunk text, index, stored hash,
M5-normalized hash, and chunker provenance. The SQL trigger cannot provide
that check on a late-only path because no pair-input row is inserted.
`M5RequirementVerifierArtifact.validate_decision_policy` also validates the
artifact against the epoch's locked decision policy. Constructor/self-digest
validity alone is insufficient.

An audit-only late call does **not** populate the normal requirement discovery
result, channel-hit, selection/admitted-pair, pair-input, verifier-artifact,
verifier-execution, semantic-observation, or currency relations. Those tables
remain reserved for a normal winning semantic settlement. The five rows in
Section 2.3 freeze the late output's artifact ID/hash and content-addressed
identity; they do not materialize its complete semantic payload or make it a
current observation.

Every replay must resupply the complete discovery or verifier DTOs and, for
discovery, the explicit snapshot-exhaustion evidence. The call revalidates
their contextual invariants, self-digests, and exact durable hashes before
returning the zero-write replay. Persistence must not infer omitted DTO bytes
or exhaustion evidence from the five audit rows or from an optional normal
semantic artifact row. Changed DTO content conflicts and necessarily changes
or invalidates its content-addressed identity. Changed exhaustion evidence
conflicts only for `snapshot_exhausted`, where false is invalid on every call;
the `budget_filled` boolean is explicitly non-material and not replay-bound.

This section narrowly supersedes the phrase "referenced immutable
discovery/verifier artifact closure" in M5-D24 Section 8.1 for audit-only
requirement late returns. It does not change normal requirement completion,
typed-direct late-return envelopes in Section 8.4, any DTO/digest recipe, or
the accepted migration-016 row set.

## 4. Relationship to M5-D24-C3

M5-D24-C3 remains accepted without qualification. Its cancellation-first
branch is one member of the successor-state rule in Section 2.2. C4 extends
the same delayed zero-write behavior to `retryable_failed`,
`completed_active`, `completed_inactive`, and `terminal_failed`; it does not
reopen C3's output-first, cancellation attribution, epoch-failure, or claim
boundaries.

## 5. Ownership and evidence split

The existing R1 path manifest is unchanged. R1-P owns the checked requirement
source and nested tests for the `retryable_failed` interim state and all four
terminal-successor states, the no-normal-semantic-artifact invariant, DTO
replay/conflict checks, and the postterminal five-row persistence shape. It
may use only the clearly labelled SQL-only terminal fixture in its owned
nested-test paths. That fixture is storage-shape evidence, not evidence of
production terminalization.

R2 owns production seal/failure composition and the end-to-end continuation
that reaches the postterminal archive after a real terminal transaction. R1-P
must not edit migration 016, add migration 017, change a public
contract/digest, or claim that its SQL-only fixture closes R2.

## 6. Executable falsifiers

Acceptance requires at least these cases for both discovery and verifier late
returns where applicable:

1. prepare one expired old requirement attempt and one dense successor;
2. with the successor still `running`, commit exactly one unchanged
   `EXPIRED_PRETERMINAL` closure using `running -> running`;
3. place the successor in `retryable_failed` while the event remains
   nonterminal, prove stale- and current-revision calls both conflict against
   a byte-identical full snapshot, then prove that checked reacquisition may
   return the job to `running` and admit the unchanged preterminal archive if
   the old output locks before any later terminal transition;
4. independently place the successor in each of `completed_active`,
   `completed_inactive`, `terminal_failed`, and `cancelled` while the event
   remains nonterminal, then prove stale- and current-revision calls both
   conflict against a byte-identical full snapshot;
5. in R1-P's clearly labelled SQL-only terminal fixture, preserve the
   zero-write rejection while the job is `retryable_failed`, resolve the
   test-local job image to one of the four accepted terminal states, and only
   then admit the five-row storage-shape archive;
6. in R2, prove that production outer terminalization cannot leave a
   `retryable_failed` job in a terminal event and that the end-to-end
   continuation observes one of the four accepted terminal states;
7. after the clearly labelled terminal fixture, commit exactly the five rows in
   Section 2.3 with the exact terminal job state and no mutation of frozen
   terminal event rows;
8. prove every normal requirement discovery/verifier semantic artifact and
   currency table is unchanged by a late-only call;
9. reject a malformed/self-digest-mismatched DTO, discovery whose explicit
   exhaustion evidence, nested epoch/policy, direction/scope, or snapshot
   membership is invalid, verifier pair input whose immutable core binding is
   invalid, or a DTO whose artifact ID/hash disagrees with the job, attempt
   output, evidence, or late rows;
10. require the complete DTO and explicit discovery-exhaustion evidence again
   for exact zero-write replay; reject false exhaustion evidence for
   `snapshot_exhausted`, treat the `budget_filled` boolean as non-material and
   not replay-bound, and conflict on changed DTO, output, evidence disposition,
   work, timing, state, or attribution; and
11. preserve the accepted C3 output-first and cancellation-first results, the
   migration-016 bytes/ledger, migration-017 absence, and public contract/
   digest snapshots.

Cases 1--5 and 7--11 are R1-P evidence, including only fixture-backed
postterminal storage shape. Case 6 is R2 production-terminalization and
end-to-end continuation evidence. The R1-P fixture cannot close case 6.

## 7. Claim boundary

This correction restores a truthful distinction among running, retryable and
terminal successor state and terminal event state, and makes the audit-only
artifact boundary explicit.
It does not establish that a zero-write rejected return was durably observed,
provider exactly-once execution, semantic publication of a losing artifact,
complete audit evidence before terminalization, M5.4 closure, or any semantic-
quality, performance, novelty, or utility claim.
