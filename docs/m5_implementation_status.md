# GroundLoop M5 Implementation Status

Status date: 2026-09-03

Milestone status: **M5.0 contract through accepted M5-D25 and M5.1--M5.3 are
complete; M5.4 is partially complete.**
M5.4-01 through M5.4-04 pass. M5.4-05 through M5.4-09 and every M5.5--M5.6
implementation/evaluation closure remain pending. M5-D24-C1 through M5-D24-C7
are accepted. M5.0-24 and M5.0-25 are each contract-`PASS` /
implementation-`PENDING`. M5-D25's accepted candidate SHA-256 is
`bac12ab5e74632c04f1bd70d0ef0d00522ba9d268eb8b73d11845bbf3b873aae`.
C7's accepted reviewed-candidate SHA-256 is
`633f4fa8cb789b7a0245cb87e7f602d3946457a7448692dc1bc6ae88c1ff4441`.
C6's accepted reviewed-candidate SHA-256 remains
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
The subsequent R2c group/requirement PostgreSQL pre-seal bridge is integrated
at `0e0ff43`. Its scoped tranche gate is `PASS` after an 81/81 post-integration
live run, static gates, and immutable-audit `GO`. The subsequent R2d pure typed-
direct application-outcome tranche is integrated at `c892cc8`; its scoped gate
is `PASS` after 101/101 focused, 189/189 complete-composition, and 339/339 pure-
runtime candidate gates plus immutable-audit `GO`; the exact integrated main
reruns passed 101/101 focused, 339/339 pure runtime, 14/14 non-database M4/
legacy, and the applicable static/hash gates. The subsequent R2e PostgreSQL
typed-direct pre-seal bridge is integrated at `2c2aed9`; its scoped gate is
`PASS`, its exact integrated-main focused live rerun passed 105/105 in 160.93
seconds, and its immutable commit/post-integration audits returned `GO`. The
R2b, R2c, exact four-path R2d, and exact 21-path R2e grants are closed with no
current edit ownership, and overall decision-row implementation evidence
remains pending.

The later M5.4-02/-03/-04 evidence tranche is integrated on main at
`290dbb306394d4a6f18ef26c6fa66a96de2ad9ac` with exact ancestry
`3200b39 -> b9d251b -> a41386f -> eb8a314 -> 290dbb3`. Its deterministic
fake-history, pure activity and live PostgreSQL activity evidence closes only
those three stage rows. Both technical lanes, the inventory-location repair
lane and their path grants are closed; two independent immutable integrated
audits returned `GO` with no unresolved P0/P1.

C7 is an accepted six-document authoritative correction on exact candidate
base `254e9c27b0dfc74df1e02ba2d8cd04c7fa9a2c6a`. Its separately activated R2e
bridge is now integrated and supplies scoped implementation evidence without
promoting M5.0-24 or an M5.4 row. No D24 lane is active after the R2e grant
closed.

The exact disjoint R1-P requirement and R1-D typed-direct persistence paths
were governed by
`docs/workstreams/m5_runtime_implementation/D24_R1_ACTIVATION.md`. R1-D is
integrated at `1838316`; R1-P is integrated at `56dd2d4`. R2a failure
terminalization and R1-C shared compatibility then integrated at `6f1ae89` and
`f5902ff`. Those completed lane manifests are historical ownership evidence,
not current edit grants or whole-stage implementation evidence.
The R2b activation and handoff are likewise historical after integration at
`bfeef3f`; none of their five paths remains owned.
The R2c activation and handoff are historical after integration at `0e0ff43`;
none of their five paths remains owned. Its persistent worktree may be retained
for audit but grants no implementation authority.
The R2d activation and handoff are historical after integration at `c892cc8`;
none of their four paths remains owned. Its persistent worktree may be retained
for audit but grants no implementation authority.
The R2e activation and handoff are historical after integration at `2c2aed9`;
none of their 21 paths remains owned. Its persistent worktree may be retained
for audit but grants no implementation authority.

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
recovery schema/installer boundary. It also covers the scoped R2c
group/requirement pre-seal path through the concrete PostgreSQL store,
including durable `BLOCKED`/`FAILED`, applicable live cutoff/reconnect races,
and exact work/timing evidence, plus the scoped pure R2d selected typed-direct
application-outcome cutoff and ordinary reconnect. The scoped R2e tranche adds
the live PostgreSQL typed-direct pre-seal bridge, including transaction-owned
acquisition, exact-held direct execution/failure routing, checked combined
failure, all-direct failure closure, WIP stops, cooperative locks, fused timing,
rollback and reconnect. The M5.4-02/-03/-04 tranche additionally supplies one
retained-world deterministic requirement history, exact no-parent-refutation
evidence, and maintained pure/live activity, late-archive, rollback and
reconnect falsifiers. There is still no accepted M5 evidence for complete
production end-to-end dynamic requirement execution, combined sparse seal,
successful production seal/publication and lifecycle-head advancement,
deployed production providers or runtime enablement, maintained-runtime model
quality, production latency, call savings, utility, novelty, or publishing
potential. Those claims remain blocked by the executable gates in
`docs/m5_acceptance_matrix.md`.

Production typed-direct preflight found that the pre-C7 accepted contract did
not make the bridge total: the cursor acquisition requires a caller token
before the locked attempt decision; direct failing-terminal acquisition and
direct terminal-failure replay lack checked active-cutoff provenance; generic
failure lacks the exact held fresh/resumed open receipt; epoch failure can
leave direct jobs nonterminal; a serialized terminal-cutoff loser lacks
provenance for actual call work; and the pure bridge cannot yet return the
required WIP outcomes. Accepted C7 freezes exact operational receipts, a tokenless
transaction owner, an inclusive
`TERMINAL_FAILED OR terminal_reason=epoch_failed` canonical-failed origin, and
one target-optional private-M4/outer-M5 `N -> N+1` failure closure. It also
requires exact-held-receipt internal production failure/direct-runner methods,
mutually exclusive terminal-acquisition/checked-combined application
provenance, and a same-lock zero-write `CANCELLED/epoch_failed` loser branch.
It requires cooperative read-only M4/M5 job/detail plans before write-only
apply, exactly one M4 failure transition and one fused timing-accumulator CAS,
and a preactivation guard rejecting every pre-C7 direct `terminal_failed` job/
projection, every direct `epoch_failed` projection, and failed epochs with open
direct jobs. R2e now supplies scoped implementation evidence for those C7
requirements through the concrete stores. It does not close the complete
M5.0-24 implementation row or any M5.4 stage row.

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
accepted amendments M5-D21 through M5-D25 extend or correct that contract
and are identified by their own decision and audit records below. Final
implementation artifact hashes and the decision-row cross-stage mapping will
be recorded at M5.6.

## 3. Frozen M5.0 result

The accepted governing documents are:

- `docs/m5_design_freeze.md` -- decisions M5-D1 through M5-D25 and theorems
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
- `docs/workstreams/m5_runtime_contract/DIRECT_ACQUISITION_TERMINAL_CUTOFF_CORRECTION.md`
  -- accepted M5-D24-C7 tokenless direct acquisition, failing-terminal/
  combined-failure cutoff provenance, exact held-open production failure,
  target-optional private activated-M5 M4 failure closure, serialized loser
  provenance, and exact WIP stops;
- `docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT.md` --
  accepted M5-D25 current/working matching-image, patch/contribution/work,
  migration-017, seal, replay, and physical/provenance-audit contract;
- `docs/m5_implementation_plan.md` -- stages M5.1 through M5.6;
- `docs/m5_multiagent_execution_plan.md` -- path-exclusive ownership and
  integration order; and
- `docs/m5_acceptance_matrix.md` -- falsifiers and executable evidence gates.

Accepted C7 restores M5.0-24 to contract-`PASS` / implementation-`PENDING`.
It grants no implementation ownership and promotes no M5.4 row.

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
M5 semantic state. Runtime-addendum revision 6, M5-D23 through M5-D25, and
their authoritative amendments now freeze retry/cancellation, recoverable
dispatch/accounting, and persisted-matching/reconnect contracts; their complete
implementation evidence remains pending. Public activation is implemented and
passes 8/8 live tests,
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
integrated at `bfeef3f`, followed by the scoped R2c group/requirement
PostgreSQL pre-seal bridge at `0e0ff43` and the scoped pure R2d typed-direct
application outcome at `c892cc8`. The scoped R2e PostgreSQL typed-direct pre-
seal bridge then integrated at `2c2aed9`. Production seal/publication and
lifecycle-head advancement, deployed production provider adapters/runtime
enablement, and the remaining end-to-end D24 gates remain implementation-
PENDING. M5-D25's persisted-matching contract is now authoritative at exact
SHA-256
`bac12ab5e74632c04f1bd70d0ef0d00522ba9d268eb8b73d11845bbf3b873aae`,
but its implementation remains `PENDING`; migration 017 and technical paths
require a separate activation.

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
evidence, not M5-D24 implementation `PASS`. At that pre-C7 integration point,
M5.0-24 remained contract-`PASS` / implementation-`PENDING`. The R2b
activation is historical and owns no path.

The subsequent R2c integration at `0e0ff43` adds an internal
`PostgresM5GroupRequirementPreSealPorts` facade over the unchanged concrete
store for group register, replace, and retire events only. Its post-integration
live suite passed 81/81, its static/type/compile/hash gates passed, and the
immutable commit audit returned `GO`. Candidate regressions recorded in its
handoff include 237/237 pure M5 runtime, 163/163 D24 requirement, 589/589 full
PostgreSQL runtime, 48/48 public-M4 route/direct, and 14/14 DTO/legacy/M4
contract tests.

That scoped group/requirement PostgreSQL pre-seal bridge gate is `PASS`. It
proves exact structural preflight, store-owned transactions, durable retryable
`BLOCKED`, nonretryable production `FAILED`, applicable C5/C6 cutoffs,
database-clock takeover, reconnect identity, exact work/timing coverage, and a
zero-write fail-closed seal boundary. It does not provide typed-direct outer
settlement, cursor-local direct failure, successful production seal or
combined publication, production discovery/verifier/measurement adapters, or
successful terminal application completion. M5.0-24 therefore remains
historically contract-`PASS` / implementation-`PENDING` at the pre-C7 point;
at that R2c checkpoint all M5.4 rows were unchanged. The R2c activation is
historical and owns no path.

The subsequent exact four-path R2d integration at `c892cc8` adds the pure
checked application outcome for one explicitly selected successful typed-
direct discovery/verifier outer receipt that loses the active terminal cutoff.
Its frozen candidate evidence records 101/101 focused, 189/189 complete-
composition, and 339/339 complete pure M5 runtime gates. The unchanged live R2c
application, D24 requirement, complete PostgreSQL runtime, public-M4 route/
direct, and non-database DTO/legacy/M4 selections passed 81/81, 163/163,
589/589, 48/48, and 14/14. Static/type/compile/diff/hash gates passed and the
independent immutable audit returned `GO` on the frozen bytes.

On exact integrated main, the 101/101 focused, 339/339 pure-runtime, 14/14 non-
database M4/legacy, Ruff, format, strict-mypy, cache-isolated compile, diff, and
four-hash gates passed again. The 189/189 composition-file and four live counts
remain frozen-candidate evidence rather than separate post-integration reruns.

That scoped pure typed-direct application-outcome gate is `PASS`. It proves
selected-origin validation, exact one-add call work, canonical hydration before
active projection, validation before timing-only telemetry, stable durable
identity, and ordinary reconnect without redispatch. It does not provide the
production PostgreSQL typed-direct application/outer-settlement bridge,
cursor-local direct failure, seal/publication, provider adapters, or combined
end-to-end recovery. M5.0-24 remains contract-`PASS` /
implementation-`PENDING`; at that checkpoint accepted C7 by itself changed no
implementation evidence, and all M5.4 rows were unchanged. The R2d activation
is historical and owns no path.

The subsequent exact 21-path R2e integration at
`2c2aed91b5c91f2f6a107fc856d646794a1654c9` implements the bounded C7
PostgreSQL typed-direct pre-seal bridge. Its sole parent is activation
`3630f444ed4ff5b3ffe312926aa7aaa77c801d02`, its immutable tree is
`30d256769eec84738acaf710d543f0083153af40`, and its frozen handoff SHA-256 is
`9d518d1caeaceabdf5c4ef2e0a52950005f2dd6f7a860053cce9c7b77fde0e4d`.

The handoff records 105/105 final focused candidate tests, 390/390 pure-runtime,
172/172 complete typed-direct application, 104/104 group application, 163/163
requirement, and 789/789 full PostgreSQL runtime tests, plus the migration,
public-M4 compatibility and static gates on the exact candidate bytes. On exact
integrated main, the focused live selection passed 105/105 in 160.93 seconds.
The accepted-C7 history guard again returned exact zero rows in a read-only
transaction, and two independent immutable commit/post-integration audits
returned `GO` with no unresolved P0/P1.

That scoped PostgreSQL typed-direct pre-seal bridge gate is `PASS`. It proves
transaction-owned tokenless acquisition, exact-held direct execution/failure
routing, checked terminal acquisition and combined failure, all-direct epoch-
failure closure, exact WIP stops, cooperative lock ordering, fused timing,
rollback and reconnect through the concrete stores. Runtime mode remains
`v1_only` outside isolated test fixtures. It does not provide production seal/
publication or lifecycle-head advancement, deployed discovery/verifier/
measurement providers, runtime enablement, M5-D25/migration 017, or complete
end-to-end M5.4 evidence. M5.0-24 remains contract-`PASS` / implementation-
`PENDING`; at that R2e checkpoint every M5.4 row was unchanged. The R2e
activation is historical, its exact grant is closed, and none of its paths has
current edit ownership.

### 4.1 Integrated M5.4-02/-03/-04 late-result activity evidence

The subsequent coordinator activation at `3200b39` produced the exact Lane A
commit `b9d251b` and Lane B commit `a41386f`. A complete-suite metadata failure
then required the separate one-path activation `eb8a314` and scalar-only
inventory-location repair `290dbb3`; neither changed production or test
semantics. The resulting exact technical ancestry is:

```text
3200b39 -> b9d251b -> a41386f -> eb8a314 -> 290dbb3
```

M5.4-02 is `PASS`. The named retained-world fake history enters every semantic
event through `M5TypedApplication.run_event` and composes forward/reverse
retrieval, overlap deduplication, requirement verification and retained
observations, withdrawal/fallback, cancellation/failure, fake sealing, complete
state/certificate oracle comparison, and fresh-application/fresh-facade replay.

M5.4-03 is `PASS`. The same history and focused node retain exact requirement
REFUTE and NEUTRAL observations while proving they create no SUPPORT witness,
parent-refutation edge, group/claim certificate input, direct `refute_count`, or
status delta. A direct claim-subject REFUTE still refutes normally.

M5.4-04 is `PASS`. The exhaustive 16-case pure classifier and pure inactive-
verifier node are joined by a live eight-row PostgreSQL activity matrix,
inactive root and verifier settlement, all four pre/postterminal and expired/
nonexpired audit-only archive shapes, injected rollback/conflict and fresh-
connection replay. The evidence binds exact activity precedence, cancellation
attribution, artifacts, work/timing and PENDING accounting while preserving
prior currency, edges, matching/reference state, certificates, semantic state,
publication and heads. The all-active verifier remains an intentional
zero-write M5-D25 stop.

Final integrated static/pure gates passed 39/39 focused and 392/392 complete
runtime tests. Because the later repair changes only evidence metadata,
byte-identical `a41386f` live dependencies carry exact passing selections of
40/40 focused activity, 122/122 recovery, 171/171 D24 requirement, 200/200
migration 016 and 797/797 complete PostgreSQL runtime. Every live gate restored
the exact pre-gate inventory with no remaining session, lock or orphan delta.

The repaired complete repository suite passed **2,210 tests with nine
pre-existing opt-in skips, zero failures and zero xfails in 2,111.33 seconds**.
The literal M4/legacy compatibility selection passed 14/14. On exact main
`290dbb3`, focused reruns passed 39/39 in 1.68 seconds and 40/40 live with 82
deselected in 72.15 seconds; the live rerun again restored the exact database
inventory. The durations are validation wall times, not runtime-performance
evidence. Two independent same-byte integrated audits returned `GO` with no
unresolved P0/P1.

The complete-suite record preserves its command-preflight and two pre-existing
environment/metadata blockers as non-results. The first command failed during
collection because four nested M4 flat-harness import paths were absent. The
next complete attempt stopped on persisted Dynagox remote metadata drift; an
independently accepted process-only Git overlay avoided changing or fetching
that checkout. The following run exposed stale line anchors in the existing M4
shared-surface inventory. A separately activated ten-integer location repair
fixed only those anchors, after which the complete suite restarted from test
one and passed.

The serialized integration Gate 4 record also retains one strict non-result.
Its preguard rejected one competing session and four locks with `rc=91`, but a
wrapper missing fail-fast `-e` allowed the migration-016 command to begin. It
was interrupted after 30 partial passes (`pytest rc=2`, 38.82 seconds), restored
the exact baseline, and was not pooled. The whole 200-test gate restarted from
test one under the corrected fail-closed guard and passed.

No production source, schema, migration, provider/model, deployment or runtime
mode changed. Runtime remains `v1_only` outside isolated fixtures. M5.0-24's
implementation half, M5.4-05 through M5.4-09, and all M5.5/M5.6 gates remain
pending.

### 4.2 Accepted M5-D25 persisted-matching contract

The separately reviewed D25 candidate at commit `002dcac`, SHA-256
`bac12ab5e74632c04f1bd70d0ef0d00522ba9d268eb8b73d11845bbf3b873aae`,
is accepted as the authoritative persisted-matching contract. Two independent
same-byte audits returned `GO` with no unresolved P0/P1/P2 after independently
checking its migration-016 prerequisites, 41-relation installation lock set,
point/digest/output/counter recipes, transition/seal/activation authorization,
crash/replay boundary, physical/provenance audit, and 40 falsifiers. Focused
non-database gates passed 112/112 and 14/14.

This closes only the D25 contract gate and advances the runtime addendum to
revision 6. M5.0-25 is contract-`PASS` / implementation-`PENDING`. No
migration-017 object, store, runtime composition, live PostgreSQL falsifier,
provider, deployment, or runtime activation is implemented. Migration 017 is
the next separately activated barrier. M5.0-24 remains implementation-
`PENDING`; M5.4-05 through M5.4-09 and every M5.5/M5.6 gate remain `PENDING`;
runtime remains `v1_only` outside isolated fixtures.

## 5. Remaining closure boundary

M5 completes only after M5.1--M5.6 pass. Closure still requires M5.4-05 through
M5.4-09, including the remaining production dynamic paths and provider/runtime
enablement, sparse publication, lifecycle-head advancement and production
seal, and the complete end-to-end crash/reconnect matrix; the accepted but
unimplemented M5-D25/migration-017 persisted-matching boundary; a real
maintained-runtime M5.5 controlled WiCE execution; and M5.6 reproduction,
artifact, documentation, and final acceptance audits.

M5 is not complete. Even after technical closure, without a fresh blinded,
independently adjudicated cohort the strongest permitted semantic conclusion
will remain **implementation complete with controlled/retrospective semantic
evidence only**. A representative utility claim remains M6 debt.
