# GroundLoop M5 Implementation Status

Status date: 2026-08-16

Milestone status: **M5.0 contract through accepted C6 and M5.1--M5.3 are
complete; M5.4 is partially complete.**
M5.4-01 passes. M5.4-02 through M5.4-09 and every M5.5--M5.6
implementation/evaluation closure remain pending. M5-D24-C1 through
M5-D24-C6 are accepted, so M5.0-24 is contract-`PASS` /
implementation-`PENDING`. C6's accepted reviewed-candidate SHA-256 is
`b6d69c7c6db63b6a726b2787639b0b15851441efc0c2f3d17091e1b6de34c393`.
C5's accepted reviewed-candidate SHA-256 remains
`b4afb8fbbdabcf970ac03865fa1510cd8219cac630dbc5ee2d5dc506889f3b67`.
C4's accepted reviewed-candidate SHA-256 remains
`f2e23d0bc4c245004f677f771e45123a619d4b1ad88da207e803cb89dcfec80c`.
C3's accepted reviewed-candidate SHA-256 remains
`59fca1859e55af3ff9ffe818205c0ed61cacd17876525cddfc60d448f9eaf723`.
The R0 contracts and migration 016 remain accepted on main. R1-D, R1-P, R2a,
and R1-C are integrated at `1838316`, `56dd2d4`, `6f1ae89`, and `f5902ff`,
respectively. The C5 validator lane integrated at `69a00e4`, accepted C6 at
`ab56178`, and the freshly reactivated R2b pure-orchestration tranche at
`bfeef3f`. Its 87/87 focused and 237/237 pure gates used no live database.
The R2b grant is closed with no current edit ownership, and overall decision-
row implementation evidence remains pending.

The exact disjoint R1-P requirement and R1-D typed-direct persistence paths
were governed by
`docs/workstreams/m5_runtime_implementation/D24_R1_ACTIVATION.md`. R1-D is
integrated at `1838316`; R1-P is integrated at `56dd2d4`. R2a failure
terminalization and R1-C shared compatibility then integrated at `6f1ae89` and
`f5902ff`. Those completed lane manifests are historical ownership evidence,
not current edit grants or whole-stage implementation evidence.
The R2b activation and handoff are likewise historical after integration at
`bfeef3f`; none of their five paths remains owned.

## 1. Honest current verdict

M5 is no longer an informal "AND over requirement counts" extension. The
frozen contract defines exact bounded bipartite-matching semantics, a
stateful Hall-mask delta operator, independent Python and SQL oracles,
versioned certificate artifacts, typed runtime identities, a PostgreSQL
upgrade/activation boundary, and a retrospective controlled WiCE evaluation.

Accepted bounded evidence now covers the pure reference, optimized matching and
incremental overlay, migration 014/PostgreSQL integrity, the independent SQL
oracle, a 100,000-event in-memory differential, and a snapshot-per-prefix
three-oracle history, durable failure/replay coordination, byte-total v2
runtime contracts, public activation/bootstrap, requirement job/root
transitions, a cursor-local typed-direct slice, and the exact migration-016
recovery schema/installer boundary. There is still no accepted
M5 evidence for complete dynamic requirement execution, combined sparse seal,
lost-worker takeover, durable event work/timing, sealed reconnect replay,
maintained-runtime model quality, production latency, call savings, utility,
novelty, or publishing potential. Those claims remain
blocked by the executable gates in `docs/m5_acceptance_matrix.md`.

## 2. M5.0 evidence

The pre-M5 implementation baseline was revalidated before the freeze:

```text
PostgreSQL 16.14 / pgvector 0.8.5 validator: passed
full pytest: 680 passed, 7 intentional opt-in skips
Ruff, strict mypy, compileall, git diff check: passed
```

Three independent `gpt-5.6-sol` ultra audits examined theory, pure-reference
and runtime semantics, and PostgreSQL/evaluation integrity. The first audit
round returned NO-GO and forced corrections including:

- Hall matching instead of global witness-union cardinality;
- every requirement/hash edge crossing as a material update;
- historical revision currency and close-once certificate bindings;
- an explicit policy-range-probe term in M5-T2;
- failure-safe staged group structure and activation serialization;
- a bounded SQL assignment cross-check with an explicit cap result;
- an exact cross-language 29-code-point normalizer;
- byte-total forward/reverse scope shapes and controlled projections;
- REQUIREMENT-only WiCE projections with no direct-claim bypass; and
- source/ablation metrics that share the same independent direct-support
  disjunct.

After correction, all three auditors returned GO with high confidence against
the same semantic-content hashes:

```text
m5_design_freeze.md          db88cad47f33710dfb3cc07a501710d9e14c312a8966e06f373bd70461af852d
m5_implementation_plan.md    319fc28a59df65bc3009f34bcca966bbffbe3659e4411335c9df2218ba427677
m5_acceptance_matrix.md       7861cf4146f190843560e1cdf912a9f862372fc891a4851d72b56251bfac6a71
m5_multiagent_execution_plan faffae5f839623efd9e387f63e951881cf1f46c5b66f37c3db642cc416a9e20a
```

Those hashes identify the initial audited M5.0 semantic candidate. Later
accepted amendments M5-D21 through M5-D24-C6 extend or correct that contract
and are identified by their own decision and audit records below. Final
implementation artifact hashes and the decision-row cross-stage mapping will
be recorded at M5.6.

## 3. Frozen M5.0 result

The accepted governing documents are:

- `docs/m5_design_freeze.md` -- decisions M5-D1 through M5-D24 and theorems
  M5-T1/M5-T2;
- `docs/workstreams/m5_runtime_contract/RECOVERY_WORK_AMENDMENT.md` -- exact
  M5-D24 lease, dispatch/evidence, work/timing, late-return, and migration-016
  contract;
- `docs/workstreams/m5_runtime_contract/EXECUTION_DISPOSITION_RECEIPT_CORRECTION.md`
  -- accepted explicit successful-disposition and total return-receipt
  correction;
- `docs/workstreams/m5_runtime_contract/LEGACY_TERMINAL_COVERAGE_CORRECTION.md`
  -- accepted first-install rejection of unbackfillable terminal M5 history;
- `docs/workstreams/m5_runtime_contract/CANCELLATION_EXPIRED_OUTPUT_CORRECTION.md`
  -- accepted cancellation-first expired-output and delayed postterminal
  archival correction;
- `docs/workstreams/m5_runtime_contract/TERMINAL_SUCCESSOR_EXPIRED_OUTPUT_CORRECTION.md`
  -- accepted retryable/terminal-successor and requirement late-artifact
  closure correction;
- `docs/workstreams/m5_runtime_contract/TERMINAL_RACE_INVOCATION_WORK_CORRECTION.md`
  -- accepted active-invocation terminal-cutoff replay projection correction;
- `docs/workstreams/m5_runtime_contract/ACTIVE_TERMINAL_CUTOFF_INVOCATION_WORK_COMPLETION_CORRECTION.md`
  -- accepted provenance completion for three additional active-invocation
  cutoff origins;
- `docs/m5_implementation_plan.md` -- stages M5.1 through M5.6;
- `docs/m5_multiagent_execution_plan.md` -- path-exclusive ownership and
  integration order; and
- `docs/m5_acceptance_matrix.md` -- falsifiers and executable evidence gates.

The strongest proposed exact statement is conditional: after immutable group,
requirement, chunk, observation-currency and policy inputs are fixed, the M5
incremental state must equal independent Python and SQL recomputation at every
serialized committed semantic revision. Neural retrieval and verification
remain empirical.

The proposed logical affected-group bound is parameterized by the number of
policy probes, changed observations, coalesced hash-mask transitions,
certificate repairs/rebuilds, touched states and output bytes, with
`r <= 8`. It is not a claim of general dynamic-matching novelty or superiority
over DBSP, F-IVM, CROWN, or another system.

## 4. M5.1--M5.3 result and partial M5.4

M5.1 implements the pure semantic foundation:

1. typed digest and normalization primitives;
2. immutable family/group/requirement records and lifecycle history;
3. typed requirement observations and historical currency;
4. independent unmatched-branch Python recomputation;
5. register/replace/retire/observe events with exact replay and rollback; and
6. frozen M4 v1 regression vectors.

An independent adversarial audit returned GO with high confidence after 73
focused tests, an exact 74,958-graph oracle enumeration, a successful full
repository suite, Ruff, and strict mypy. It specifically confirmed global
event/observation identifier collision safety, lazy epoch synchronization,
failure-atomic currency rejection, typed prevalidation, immutable records, and
alternative valid certificate acceptance.

Those were M5.1-only boundaries. The later integrated work closes them only at
the following bounded stages:

- M5.2 passes all nine incremental-algorithm gates. Exact bounded matching,
  Hall-mask and multiplicity suites, sparse/unrelated-key assertions,
  certificate repair/rebuild/history, failure-atomic injection, replay, and
  every-term complexity guards pass. The frozen seed-`20260802` run records
  100,000 committed events, 7,731 exact replays, 20,726 generator rejections,
  1,000 audits of each maintained index/history class, and zero mismatches.
  Its ignored artifact was revalidated against the integrated runner, config,
  and manifest rather than regenerated.
- M5.3-01 through M5.3-05 and M5.3-08 through M5.3-09 pass after sequential
  integration of migration 014, the PostgreSQL repository and SQL oracle. The
  owned live PostgreSQL suite passed 55 tests; 247 relevant live M4/pre-M5 tests
  passed with one explicit real-model skip.
- M5.3-06 passes at integrated executable commit `e266696`. One deterministic
  16-checkpoint history covers all nine overlay event variants and exact
  replay, with incremental, Python, and SQL state/certificate checks agreeing.
  The complete live M5 suite passed 339 tests with only the explicit 100k gate
  skipped, and the ordinary full suite passed 816 tests with 211 classified
  skips. See
  `docs/workstreams/m5_integration/THREE_ORACLE_RESULT_2026-08-05.md`.

M5.3-06 loads fresh rollback-isolated PostgreSQL rows for each logical prefix
and materializes derived state with `publish=False`. It is not a durable
same-schema mutation history. M5.3-07 now separately passes through the
production persistence slice: 27 adversarial live tests cover every typed
group open/failure injection point, REGISTER/REPLACE/RETIRE staged failure,
exact cancellation/PENDING accounting, strict/published immutability,
read-only reconnect replay, conflict rejection, and concurrent exact open.
The composed migration/failure/bundle-race gate passed 67/67 tests.

M5.4-01 passes through the integrated byte-total contract, digest, direction,
shape, nullability, F64, ordering, and pure-frontier suite. M5-D22 closes the
previously undefined inner state-artifact identity without changing M4-v1 or
M5 semantic state. Runtime-addendum revision 5 and M5-D23 now freeze the
previously missing retry-error, cancellation-plan, direct-payload, direct-
acquisition, and read-only hydration contracts; their implementation evidence
remains pending. Public activation is implemented and passes 8/8 live tests,
including all six bootstrap reference kinds, cross-language SQL/Python hashes,
no synthetic epoch, read-only replay, conflicts, and six failure-atomic
injection points. Activation alone does not close M5.4-05.

The subsequent production slice implements checked requirement acquisition,
retry, root-result staging/barrier, root/result replay, typed application
projections, and cursor-local direct open/acquire/expansion/verifier behavior.
On the integrated main checkpoint, the live M5 PostgreSQL runtime suite passed
92/92 and the PostgreSQL-enabled full M4 regression passed 478 tests with 7
recorded skips. Those are intermediate implementation results, not M5.4 row
closure: dispatched workers were not recoverable and confirmed work/timing was
not durable.

M5-D24 now freezes that missing operational boundary after independent broad
and M4-envelope audits returned GO on the same pre-freeze SHA-256
`7fbcb57ae8a1e71d17457409f9f864418b42cc506ebc191211f476caa59e2475`.
M5-D24-C1 then corrected the non-inferable successful evidence disposition and
ambiguous successful-return receipts after independent exact-byte GO on
pre-freeze SHA-256
`741ce0de897099164eb877684bc12209f347eb920185be5ed4c3c3d5395bf25a`.
M5-D24-C2 removes the contradictory allowance for pre-016 terminal M5 history
that cannot be given exact point coverage without guessing. Two independent
exact-byte reads returned GO on pre-freeze SHA-256
`de9a56439d4f1995339c72917c52dc7726c9fdd6095c27c8923493193abb5aad`.
The pure C1 receipt DTO checkpoint is integrated. Migration 016 is accepted at
main commit `61875894172c8e0b36866d6b215ecab7a57b76ec` with bundle SHA-256
`28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565`
and migration SHA-256
`a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7`.
The exact committed main bytes passed 200/200 migration-016 tests, 26/26
migration-015 regressions and 292/292 broader live PostgreSQL runtime tests;
Ruff, strict mypy, compileall and diff-check also passed. That checkpoint
accepted only the R0 schema/installer boundary. Checked R1 persistence,
R2a failure terminalization, and shared compatibility later integrated at the
commits recorded above; bounded R2b pure application composition later
integrated at `bfeef3f`. Production application composition and the remaining
D24 race/crash/reconnect gates remain implementation-PENDING. The proposed
M5-D25 persisted-matching draft remains
non-authoritative and has unresolved adversarial blockers; migration-016
acceptance alone does not authorize it.

During R1-P execution, the cancellation-first half of the required
already-expired-output race proved that immediate preterminal archival cannot
match accepted migration 016: cancellation has already made the job/scope
terminal, while the SQL closure permits only `running -> running` until the
event itself is terminal. Accepted M5-D24-C3 specifies output-first
preterminal archival, cancellation-first zero-write conflict, and delayed
`EXPIRED_POSTTERMINAL` archival after seal/failure. R1-P owned the two
preterminal orders and may use a clearly labelled owned terminal fixture for
persistence-shape evidence; production seal/failure and end-to-end
continuation remain R2. That cancellation checkpoint passed 34/34 live
nested PostgreSQL tests, but that is tranche evidence rather than whole-R1-P
or M5.4 closure.

Subsequent R1-P verifier work exposed the broader M5-D24-C4 defect.
The same impossible preterminal image occurs when the dense successor is
`retryable_failed`, `completed_active`, `completed_inactive`, or
`terminal_failed`, not only when it is `cancelled`: exact `running -> running`
is false, migration 016 has no retryable-failed artifact shape, and it admits
an exact terminal-state expired artifact only after the event is sealed or
failed. Accepted C4 therefore keeps preterminal archival only for a still-
running successor and requires zero writes for every retryable or terminal
successor while the event is nonterminal. Checked reacquisition may restore
`running` and reopen the unchanged preterminal path. Otherwise outer
terminalization must resolve `retryable_failed` into one of the four accepted
terminal states before the first exact five-row postterminal archive. C4 also
requires complete context/self-digested discovery/verifier DTOs on first call
and replay, explicit discovery snapshot-exhaustion evidence and nested
snapshot/scope validation, and an immutable-core verifier pair-input check
equivalent to the normal SQL validator, while forbidding a late-only call from
populating normal semantic artifact tables. The discovery boolean is required
true and revalidated for `snapshot_exhausted`; it is non-material and not
replay-bound for `budget_filled`. C3 remains accepted; C4 changes no migration
or public contract. R1-P's SQL-only fixture proves the storage barrier after
test-local state resolution; R2 retains proof of production
`retryable_failed` resolution and the end-to-end continuation.

The bounded D24 R1 execution sequence is integrated through main commit
`f5902ff`: R1-D direct persistence integrated at `1838316`, R1-P requirement
persistence at `56dd2d4`, R2a failure terminalization at `6f1ae89`, and R1-C
shared compatibility at `f5902ff`. Their focused evidence remains tranche
evidence. The later R2b pure tranche adds requirement-application and fake-
seal orchestration evidence but still does not close production composition,
production seal, the complete race/crash/reconnect matrix, or M5.0-24
implementation.

R2b application preflight then exposed a returned-envelope contradiction not
covered by C1--C4. A requirement or typed-direct invocation can open/resume
while nonterminal, perform or reuse exact external work, and receive an
applicable checked successful-return receipt whose terminal projection names
another transaction's terminal result. The canonical replay envelope is
required to carry zero call work, so returning it unchanged would silently
drop this invocation's work; adding the work to event totals would double
count it.

Accepted M5-D24-C5 adds no new marker or storage. Ordinary terminal-known-at-
entry replay remains terminal-projected with zero call work. Only after the
application validates a canonical terminal read against the checked successful
return receipt may it return C5's active-cutoff `REPLAYED` projection retaining
the invocation's actual earlier nonterminal open receipt and exact accumulated
call work, including zero. The generic C5 validator integrated at `69a00e4`,
accepted C6 integrated at `ab56178`, and the coordinator freshly reactivated
the five-path R2b lane at `62bfb03`.

R2b composition subsequently proved three reachable work-loss origins outside
C5: later requirement acquisition returning checked
`TERMINAL/EPOCH_FAILED` after earlier work; checked failure-mutator replay
after current terminal-attempt work; and checked fake-only seal-mutator replay
after current work. Accepted M5-D24-C6 applies the same existing active
envelope only after exact route validation and canonical ordinary replay
validation. It additionally requires the active envelope to validate before
timing-only terminal telemetry is written. The acquisition route requires a
same-event/payload/epoch canonical `FAILED` result without inventing a run
failure reason; the failure route requires the same requested failure reason;
and seal remains fake-only while preserving the exact durable outcome branch.

C6 makes no DTO/validator, marker, persistence, telemetry-work, schema,
migration, digest, event-total, or public-M4 change. Two independent exact-byte
audits returned GO. The subsequent exact five-path R2b tranche integrated at
`bfeef3f`; its 87/87 focused composition suite, 237/237 full pure M5 runtime
gate, and static gates passed without a live database.

That result is scoped pure requirement-application/fake-seal orchestration
evidence, not M5-D24 implementation `PASS`. M5.0-24 remains contract-`PASS` /
implementation-`PENDING`. The R2b activation is historical and owns no path.
Typed-direct outer-settlement application composition, cursor-local direct
failure, the concrete production application adapter, production seal/
publication, and remaining live crash/reconnect and end-to-end evidence remain
deferred under separate fresh manifests.

## 5. Remaining closure boundary

M5 completes only after M5.1--M5.6 pass. Closure still requires production M5.4
dynamic jobs and application composition, typed-direct outer settlement,
sparse publication and production seal, recoverable at-least-once dispatch
with idempotent semantic effects, and live reconnect gates; a real maintained-
runtime M5.5 controlled WiCE execution; and M5.6 reproduction, artifact,
documentation, and final acceptance audits.

M5 is not complete. Even after technical closure, without a fresh blinded,
independently adjudicated cohort the strongest permitted semantic conclusion
will remain **implementation complete with controlled/retrospective semantic
evidence only**. A representative utility claim remains M6 debt.
