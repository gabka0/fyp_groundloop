# M5-D24-C1 Execution Disposition and Return Receipt Correction

Status: frozen narrow correction after independent exact-byte review;
implementation evidence remains pending

Date: 2026-08-06

Authority: this document amends only the omitted successful
execution-disposition input and successful-return receipt topology in
`RECOVERY_WORK_AMENDMENT.md`. Every other M5-D24 byte, identity, lease,
accounting, timing, migration, replay, and M4-v1 compatibility rule remains
unchanged. It does not authorize M5-D25 persisted matching.

The independently accepted pre-freeze content SHA-256 is
`741ce0de897099164eb877684bc12209f347eb920185be5ed4c3c3d5395bf25a`.

## 1. Defects being corrected

M5-D24 requires execution evidence to distinguish `returned` from
`reused_artifact` and says a reused execution explicitly identifies reuse.
The frozen successful-settlement signatures nevertheless carry only the
artifact/output, work, and timing. Zero work is not an injective reuse signal:
a returned execution is bounded above by its dispatch maximum but is not
defined by a nonzero counter. Persistence therefore cannot construct the
frozen evidence disposition without guessing.

M5-D24 also lets a successful requirement or typed-direct return race a dense
takeover or terminal cutoff. The frozen `M5AttemptCompletionReceipt` cannot
identify normal semantic application versus preterminal expired audit versus
post-terminal audit, and it cannot return the designated transition timing
anchor. `M5DirectLateReturnReceipt` is named but has no field topology. The
application consequently cannot decide whether to proceed, return BLOCKED, or
append transition-call timing without reconstructing mutable state.

## 2. Explicit successful execution disposition

`M5DiscoveryExecution` and `M5VerifierExecution` gain exactly these required
operational fields:

~~~text
execution_disposition: M5ExecutionEvidenceDisposition
attempt_timing: M5RuntimeTiming | None
~~~

For either successful DTO, `execution_disposition` MUST be exactly `returned`
or `reused_artifact`. `retryable_failure` and `terminal_failure` are illegal in
a successful DTO. The field has no default and is supplied by the external
adapter that knows whether it executed or reused an immutable artifact.
`call_work` is the exact external-attempt work passed to persistence and is
also added once to the current invocation's call work. A reused execution must
satisfy M5-D24's existing zero external-call/model-call/token rule. Neither
field enters an existing semantic artifact or result digest.

`M5ExternalWorkFailure` gains the required
`attempt_timing: M5RuntimeTiming | None` field. Its evidence disposition is
derived without ambiguity from the invoked settlement method:

- `mark_m5_retryable_failure` installs `retryable_failure`; and
- `mark_m5_terminal_failure` installs `terminal_failure`.

Those failure methods do not accept a caller-selected disposition.

The successful requirement persistence signatures become:

~~~text
stage_m5_discovery_result_atomically(
  epoch_id, expected_revision, lease, job, result, attempt_output,
  execution_disposition, attempt_work, attempt_timing
) -> M5RequirementAttemptReturnReceipt

complete_m5_verifier_atomically(
  epoch_id, expected_revision, lease, job, pair_input,
  verifier_artifact, attempt_output,
  execution_disposition, attempt_work, attempt_timing
) -> M5RequirementAttemptReturnReceipt
~~~

The successful cursor-local typed-direct expansion, verifier-completion, and
late-return methods receive the same explicit `execution_disposition`, limited
to `returned | reused_artifact`. This is an M5-owned cursor API change only;
no public M4-v1 DTO, protocol, method, digest, or row identity changes.

## 3. Requirement successful-return receipt

The immutable first-receipt classification is:

~~~text
M5RequirementReturnDisposition =
  applied |
  expired_preterminal |
  expired_postterminal |
  terminal_audit_preterminal |
  terminal_audit_postterminal
~~~

`terminal_audit_preterminal` is the existing D24 Section 4.3 case in which
cancellation makes the job terminal before its still-current worker return
while the owning event remains nonterminal. It is not silently rejected or
mislabelled expired. Both preterminal audit-only shapes use the already-frozen
`preterminal_late_return` contribution: for an expired attempt its
source-identity hash is the expired-return digest; for a still-current terminal
job it is the terminal-audit-only attempt-result artifact hash. Both add one
late-attempt artifact, update work/timing accumulators at the locked current
revision, and change no epoch revision, semantic state, PENDING counter,
currency, matching, certificate, publication, or head.

Both successful requirement settlement methods return:

~~~text
M5RequirementAttemptReturnReceipt(
  disposition: M5RequirementReturnDisposition,
  logical_job_id: SHA256,
  attempt_id: SHA256,
  resulting_revision: int,
  exact_replay: bool,
  execution_evidence_digest: SHA256,
  return_artifact_digest: SHA256,
  current_terminal_logical_result_hash: SHA256 | None,
  transition_anchor: M5TransitionTimingAnchor | None
)
~~~

This is an operational receipt and has no digest recipe. `disposition` records
the immutable first successful-return outcome and never changes on replay:

| disposition | immutable return artifact | first-write anchor |
|---|---|---|
| applied root | attempt-result artifact hash | `root_result_stage` |
| applied verifier | attempt-result artifact hash | `verifier_completion` |
| expired_preterminal | expired-return digest | `preterminal_late_return` |
| expired_postterminal | expired-return digest | NULL |
| terminal_audit_preterminal | terminal-audit-only attempt-result artifact hash | `preterminal_late_return` |
| terminal_audit_postterminal | terminal-audit-only attempt-result artifact hash | NULL |

A present anchor binds the same epoch, attempt/source identity, resulting
revision, and contribution key returned by the checked transaction. It is
present only for the first nonterminal write. Exact replay always returns
`transition_anchor=NULL` and creates no timing point.

`current_terminal_logical_result_hash` is a current-cutoff projection, not
part of the immutable receipt identity. It is NULL while the event is
nonterminal and otherwise equals the immutable terminal result. A replay of an
originally applied or preterminal return after later terminalization therefore
keeps its original `disposition` but returns the now-present terminal hash.
For postterminal first receipt, the hash also equals the general audit sidecar.

`resulting_revision` is always the revision read under the call's locked
cutoff. A first normal semantic settlement advances once. Either preterminal
audit-only settlement changes no revision. A postterminal return reports the
immutable terminal-result revision. A later no-write replay may consequently
return a newer current/terminal revision and terminal projection while its
immutable disposition, evidence, and artifact digests remain exact.

`M5AttemptCompletionReceipt` remains the receipt for retryable and terminal
failure settlement. It is not used for a successful return.

## 4. Typed-direct cursor and outer receipts

### 4.1 Cursor contribution receipt

Cursor-local direct methods do not advance the outer revision, select an
anchor, or claim commit/replay outcome. They return only M5-owned contribution
candidates:

~~~text
M5DirectCursorContributionReceipt(
  epoch_id: int,
  job_id: str,
  attempt_id: str,
  execution_evidence_digest: SHA256,
  attempt_execution_contribution_key_digest: SHA256,
  direct_transition_source_id: str | None,
  direct_transition_source_identity_hash: SHA256 | None,
  direct_transition_contribution_key_digest: SHA256 | None,
  observation_completion: ObservationCompletionReceipt | None
)
~~~

The three direct-transition fields are jointly present for normal expansion
or verifier completion and jointly NULL for retryable/terminal attempt
failure. `observation_completion` is present only for normal verifier
completion and is the unchanged public M4-v1 receipt. A cursor receipt has no
resulting revision, replay flag, or transition anchor. The outer owner derives
and validates those only after all cursor-local contributions are known.

`mark_direct_retryable_failure` and `mark_direct_terminal_failure` return this
receipt instead of `None`. A standalone nonterminal outer settlement selects
`direct_attempt_execution` as its sole anchor. When terminal settlement and
typed epoch failure share one outer transaction, `epoch_failure` is the sole
anchor and the cursor receipt cannot create a second point.

### 4.2 Late-return disposition and receipt

~~~text
M5DirectLateReturnDisposition =
  expired_preterminal |
  expired_postterminal |
  terminal_audit_preterminal |
  terminal_audit_postterminal

M5DirectLateCursorContributionReceipt(
  disposition: M5DirectLateReturnDisposition,
  epoch_id: int,
  job_id: str,
  attempt_id: str,
  envelope_digest: SHA256,
  execution_evidence_digest: SHA256,
  expired_return_digest: SHA256 | None,
  attempt_execution_contribution_key_digest: SHA256 | None,
  preterminal_late_contribution_key_digest: SHA256 | None,
  postterminal_logical_result_hash: SHA256 | None
)

M5DirectLateReturnReceipt(
  disposition: M5DirectLateReturnDisposition,
  epoch_id: int,
  job_id: str,
  attempt_id: str,
  resulting_revision: int,
  exact_replay: bool,
  envelope_digest: SHA256,
  execution_evidence_digest: SHA256,
  expired_return_digest: SHA256 | None,
  current_terminal_logical_result_hash: SHA256 | None,
  transition_anchor: M5TransitionTimingAnchor | None
)
~~~

`archive_direct_late_return` returns the cursor contribution receipt, not the
outer receipt. The two contribution-key digests are jointly present exactly
for either preterminal disposition and jointly NULL postterminal. The
postterminal hash is present exactly for either postterminal disposition and
binds the general audit sidecar. The cursor receipt has no resulting revision,
outer replay flag, or anchor.

Only after the outer owner has selected its sole anchor, updated the revision
when applicable, and committed does it construct `M5DirectLateReturnReceipt`.
The expired digest is present exactly for either expired disposition. A
preterminal first write has the sole `preterminal_late_return` anchor; a
postterminal first write has none. Exact replay always has no anchor. The
current terminal hash and resulting revision follow the same dynamic-cutoff
rules as Section 3, so replay after later terminalization is unambiguous.
For a still-current preterminal terminal-audit return, the
`preterminal_late_return` source-identity hash is the envelope digest.

### 4.3 Atomic outer successful return

A separate normal-stage call followed by a late-archive call is forbidden.
The M5-owned outer direct settlement receives the complete byte-total envelope
even when the lease is expected to be current, locks once, selects normal
versus late/postterminal handling, and returns exactly one branch:

~~~text
M5DirectNormalReturnReceipt(
  epoch_id: int,
  job_id: str,
  attempt_id: str,
  resulting_revision: int,
  exact_replay: bool,
  execution_evidence_digest: SHA256,
  return_artifact_digest: SHA256,
  direct_transition_source_id: str,
  direct_transition_source_identity_hash: SHA256,
  direct_transition_contribution_key_digest: SHA256,
  observation_completion: ObservationCompletionReceipt | None,
  current_terminal_logical_result_hash: SHA256 | None,
  transition_anchor: M5TransitionTimingAnchor | None
)

M5DirectAttemptReturnReceipt(
  return_kind: discovery | verifier,
  normal: M5DirectNormalReturnReceipt | None,
  late: M5DirectLateReturnReceipt | None
)
~~~

Exactly one branch is present. A normal discovery receipt MUST have
`observation_completion=NULL`; a normal verifier receipt MUST have the
unchanged public M4 observation-completion receipt present. A first normal
write returns the sole `direct_transition` anchor;
normal replay returns none. The normal receipt's dynamic terminal hash and
resulting revision use the Section 3 current-cutoff rules.

`M5DirectNormalReturnReceipt.return_artifact_digest` is exactly
`M5TypedDirectLateReturnEnvelope.result_artifact_hash`, which MUST equal
`envelope.completion.result_artifact_hash`. For discovery it MUST also equal
`envelope.discovery.result_artifact_hash`; for verifier it is covered by the
exact recomputation of `envelope.verifier_binding_digest` over the same
`result_artifact_id` and `result_artifact_hash`. The outer method validates the
complete frozen discovery or verifier envelope closure before constructing the
normal receipt. The digest is not a separately caller-selected value.

`M5DirectAttemptReturnReceipt.return_kind` MUST equal both the invoked outer
method and `envelope.return_kind`: `settle_direct_expansion_atomically`
accepts and returns only `discovery`, while
`settle_direct_verifier_atomically` accepts and returns only `verifier`. This
equality applies to normal, late, and exact-replay branches; a mismatch rejects
the transaction before either branch is written or returned.

The two outer transaction-owning methods accept the complete envelope,
explicit successful disposition, attempt work, and original attempt timing:

~~~text
settle_direct_expansion_atomically(...) -> M5DirectAttemptReturnReceipt
settle_direct_verifier_atomically(...) -> M5DirectAttemptReturnReceipt
~~~

They invoke normal cursor-local M4 staging only while the attempt is current;
otherwise they insert the exact late/postterminal closure in the same checked
transaction. Public M4 methods and return bytes do not change.

## 5. Application behavior

After a successful requirement or typed-direct outer settlement:

1. if `current_terminal_logical_result_hash` is present in the selected
   branch, hydrate that exact terminal result and use only terminal invocation
   telemetry;
2. otherwise, if `transition_anchor` is present, append exactly one
   transition-call timing point before external work or another mutation;
3. an `applied`/normal receipt continues from the returned revision;
4. an expired-preterminal receipt returns `BLOCKED/work_in_progress` after its
   timing append and never acts for the replaced attempt; and
5. a terminal-audit-preterminal receipt discards the returned semantic value,
   appends its timing point, and resumes only from durable scheduler state.

No application path infers a disposition from work, performs a normal/late
TOCTOU precheck, guesses a missing receipt field, treats replay as a new
anchor, scans contribution history to reconstruct an anchor, or changes public
M4-v1 behavior.

## 6. Migration and identity boundary

Migration 016 already stores all four execution-evidence disposition values
and needs no new relation or column for this correction. Its exact accepted
file and bundle hashes are determined only after the independently accepted
schema repair. Its deferred `preterminal_late_return` validator must accept
exactly either the existing expired-return closure or the nonexpired
terminal-audit-only requirement artifact/direct envelope closure described
above; it must still reject every normal semantic or postterminal use of that
contribution. This correction changes no relation family, M5 semantic digest,
M4-v1 byte, migration-015 byte/ledger value, or M5-D24 work/timing digest
recipe.

## 7. Executable falsifiers

Implementation is rejected if any of these can occur:

1. persistence derives `returned` versus `reused_artifact` from zero work,
   artifact existence, or another nonexplicit heuristic;
2. a successful DTO accepts a failure disposition or omits disposition/timing;
3. a failure caller can select a disposition inconsistent with its settlement
   method;
4. a reused-artifact DTO charges an external call, model call, or token;
5. a successful return racing takeover or terminal cutoff is represented by
   an ambiguous `M5AttemptCompletionReceipt`;
6. a receipt disposition/branch combination outside the tables above is
   accepted, or normal typed-direct staging performs a separate late-return
   TOCTOU decision;
7. a receipt anchor names a different epoch, attempt, contribution key, kind,
   or resulting revision, or an exact replay/post-terminal receipt returns an
   anchor;
8. a preterminal expired return proceeds to barrier, verifier, failure, or seal
   instead of returning BLOCKED after its timing append;
9. a nonexpired terminal-audit-only return while the event is nonterminal is
   rejected, applied semantically, or omitted from exact work/timing evidence;
10. a later replay changes immutable disposition/digests, reuses the original
    revision as current, omits an available terminal projection, or returns a
    new anchor;
11. a cursor-local direct method selects an outer anchor/revision, returns no
    contribution identity, normal and late branches are both/neither present,
    the wrapper/method/envelope return kinds disagree, or a normal receipt's
    return artifact differs from its exact completion/discovery/verifier
    envelope closure;
12. a post-terminal receipt omits or changes the current immutable terminal
    logical result hash, appends transition timing, or changes frozen event
    work, timing, coverage, result, revision, state, or head; or
13. any correction changes a public M4-v1 DTO/API signature or existing M4/M5
    semantic digest byte.

Required tests pin every DTO/enum topology and legal/illegal branch, explicit
returned/reused evidence digests with equal zero work producing distinct
identities, nonexpired preterminal audit, exact replay before and after later
terminalization, normal/expired/postterminal races for root/verifier/direct
returns, cursor-candidate versus outer-anchor separation, one-anchor behavior,
and the public M4-v1 signature/byte snapshot.

## 8. Claim boundary

This correction closes an implementation ambiguity. It does not itself prove
recovery, exactly-once provider execution, maintained matching, performance,
neural quality, utility, or M5 completion.
