# GroundLoop Decision Log

## 2026-09-03 — M5-D25 Persisted-Matching Contract Accepted

Decision status: **accepted authoritative contract amendment**. M5-D25 and
acceptance row M5.0-25 are contract-`PASS` / implementation-`PENDING`, and the
M5.4 runtime addendum advances to revision 6. The complete authoritative
contract is
`docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT.md` at
accepted content SHA-256
`bac12ab5e74632c04f1bd70d0ef0d00522ba9d268eb8b73d11845bbf3b873aae`.

The reviewed candidate has exact commit
`002dcace2f89e71ef3a56955647b4d8077e9c91f`, 2,404 lines and 115,141 bytes,
on sole-parent ancestry from integrated main `8b3c006`. Two independent
same-byte reviews—semantic/digest/counter/oracle and PostgreSQL/migration/lock/
replay—each returned `GO` with `P0=0`, `P1=0`, and `P2=0`. Both reproduced
112/112 runtime contract/digest tests, 14/14 M4/legacy compatibility tests, the
71-byte empty logical-output digest, all 37 counters, all 40 D25 falsifiers,
and the five accepted migration-016 prerequisite values. The protected
untracked draft remained byte-identical and was never staged.

M5-D25 freezes reconnectable PostgreSQL current/working bounded-Hall images,
immutable patch and contribution artifacts, durable work accumulators,
store-derived transition authority, scoped transition/seal/activation DML,
deterministic seal promotion, and a separate physical/provenance audit while
preserving independent semantic oracles. It also freezes migration 017's exact
literal prerequisite, relation/lock order, transaction/retry boundary, and
falsifiers. It changes no M5-D1--D24-C7 semantics, M4-v1 identity, objective-
truth boundary, or provider-execution claim.

Acceptance implements nothing. Migration 017 is the next separately activated
implementation barrier; no source, migration, test, database, provider,
deployment, or runtime-mode change is part of this decision. M5.0-24 remains
implementation-`PENDING`; M5.4-05 through M5.4-09 and every M5.5/M5.6 gate
remain `PENDING`; runtime remains `v1_only` outside isolated fixtures. No M5.4
or M5 completion, model-quality improvement, security, novelty, or named-
system superiority follows from this contract result.

## 2026-09-02 — M5.4-02/-03/-04 Late-Result Activity Evidence Integrated

Decision status: the separately activated M5.4-02/-03/-04 evidence tranche is
integrated on main at
`290dbb306394d4a6f18ef26c6fa66a96de2ad9ac`. The exact ancestry is
`3200b39cfe01a4f41fdd1cd1492e85afc468cc2e` ->
`b9d251bfdcc4e617240b820f1da9ad289f0d6a23` ->
`a41386fcf18a96536c0be23aef99cac1ff9d96e3` ->
`eb8a314ee22b566fb586f4a95165711e01e1282d` -> `290dbb3`. The first two
implementation commits change exactly the activated five test/handoff paths;
the last two commits activate and apply only the mechanical M4 inventory-line
repair required by the complete-suite gate. Two independent immutable
integrated audits returned `GO` with no unresolved P0/P1.

M5.4-02 is `PASS`: one retained-world deterministic history composes
requirement forward/reverse retrieval, deduplication, verification,
observation, withdrawal, fallback, cancellation, failure, sealing, complete
state/certificate oracle comparison, and fresh-facade replay through the
accepted fake ports. M5.4-03 is `PASS`: exact retained requirement REFUTE and
NEUTRAL observations create no witness, parent-refutation edge, certificate
input, direct `refute_count`, or status delta, while a direct claim REFUTE still
refutes normally. M5.4-04 is `PASS`: the exhaustive pure classifier and live
eight-row PostgreSQL activity matrix cover epoch, requirement/group, chunk,
and terminal-job precedence; inactive verifier/root settlement; all four
audit-only archive shapes; rollback, conflict and reconnect; exact work/timing
and PENDING accounting; and preservation of prior currency and every protected
semantic/publication surface.

On the final integrated bytes, the focused pure and complete pure-runtime gates
passed 39/39 and 392/392. The unchanged `a41386f` live inputs carried exact
passing gates of 40/40 focused activity, 122/122 recovery, 171/171 D24
requirement, 200/200 migration 016, and 797/797 complete PostgreSQL runtime,
each with exact inventory restoration. The repaired complete repository suite
then passed 2,210 tests with nine pre-existing opt-in skips, zero failures and
zero xfails in 2,111.33 seconds; the literal M4/legacy compatibility selection
passed 14/14. After the main fast-forward, the focused pure rerun passed 39/39
in 1.68 seconds and the focused live rerun passed 40/40 with 82 deselected in
72.15 seconds, again with exact pre/post inventory equality. These durations
are validation wall times, not system-performance measurements.

The complete-suite record retains three non-results rather than pooling partial
counts: the original command failed collection because nested M4 harness paths
were absent; the corrected command stopped on pre-existing persisted Dynagox
remote metadata drift; and the next run exposed stale M4 inventory line anchors.
Only process-local import/Git overlays and the separately activated ten-scalar
inventory-location repair were accepted before restarting the complete suite
from test one. Separately, the serialized integration Gate 4 preguard correctly
rejected one competing session and four locks (`rc=91`), but a wrapper missing
fail-fast `-e` allowed migration-016 pytest to start. That attempt was
interrupted after 30 partial passes (`pytest rc=2`, 38.82 seconds), is a strict
`NON-RESULT`, restored the exact baseline, and was not pooled with the complete
200/200 rerun under the corrected fail-closed guard.

No production source, runtime behavior, model, provider, schema, migration, or
runtime mode changed in this evidence tranche. Runtime remains `v1_only`
outside isolated fixtures, and the all-active PostgreSQL verifier remains
fail-closed behind M5-D25/migration 017. M5.0-24 remains contract-`PASS` /
implementation-`PENDING`; M5.4-01 remains `PASS`; M5.4-05 through M5.4-09 and
every M5.5/M5.6 row remain `PENDING`. Production seal/publication,
lifecycle-head advancement, provider composition, runtime activation,
maintained-history/model evidence, deployment, utility, security, and novelty
are not established. The technical lane grants are closed, and this
reconciliation grants no implementation ownership.

## 2026-08-19 — M5-D24 R2e PostgreSQL Typed-Direct Pre-Seal Bridge Integrated

Decision status: the bounded R2e PostgreSQL typed-direct pre-seal bridge is
integrated on main at
`2c2aed91b5c91f2f6a107fc856d646794a1654c9`. Its scoped tranche gate is
`PASS`. This is an implementation-status record, not a contract amendment, an
M5.4 row promotion, or an implementation-`PASS` promotion for M5.0-24.

The integration has sole parent and exact activation base
`3630f444ed4ff5b3ffe312926aa7aaa77c801d02`. It changes exactly the 21
activated R2e paths and has immutable Git tree
`30d256769eec84738acaf710d543f0083153af40`. The frozen handoff has SHA-256
`9d518d1caeaceabdf5c4ef2e0a52950005f2dd6f7a860053cce9c7b77fde0e4d`,
623 lines and 29,283 bytes. The manifest-ordered paths 1--20 SHA ledger has
SHA-256 `42c50e79c54e72c521abafb60a30a838206afb29d78c43eb9aacc1d45e4f7cf9`.
Two independent immutable commit/post-integration audits returned `GO` with
no unresolved P0/P1.

The candidate handoff records the complete serial PostgreSQL, pure-runtime,
migration, public-M4 compatibility and static gates on its exact bytes. It also
discloses the intermediate full-PostgreSQL result of 684 passed / 105 failed
from one shared import-order compatibility defect in manifested path 20, the
bounded path-20 correction, and the subsequent 218/218 ordered compatibility
and 789/789 full-PostgreSQL passing reruns. On the exact integrated main tree,
the focused live PostgreSQL selection passed 105/105 in 160.93 seconds.

The post-integration accepted-C7 history guard again ran read-only and returned
exact row count `0`, canonical JSON `[]`, and result SHA-256
`4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945`.
Its transaction was explicitly rolled back, so it changed no persistent runtime
state.

This evidence closes only the scoped C7 transaction-owned tokenless
acquisition, exact-held direct execution/failure routing, checked terminal
acquisition and combined failure, all-direct epoch-failure closure, exact WIP
stops, cooperative lock ordering, fused timing, rollback and reconnect paths
through the concrete PostgreSQL stores. Runtime mode remains `v1_only` outside
isolated tests. The tranche does not provide production seal/publication or
lifecycle-head advancement, deployed discovery/verifier/measurement providers,
runtime enablement, M5-D25/migration 017, or complete end-to-end M5.4 evidence.
M5.0-24 remains contract-`PASS` / implementation-`PENDING`; every M5.4 row is
unchanged and M5.4 remains partial.

The R2e activation and handoff are now historical evidence. Their exact
21-path grant is closed and gives no current edit ownership. The persistent
worktree, if retained for audit, is not implementation authority. Any later
lane requires a fresh committed path-exclusive activation from the then-current
integrated barrier and may not inherit R2b, R2c, R2d, or R2e ownership.

## 2026-08-18 — M5-D24-C7 Direct Acquisition/Cutoff Correction Accepted

Decision status: **accepted authoritative contract correction**. Two
independent exact-byte audits returned `GO` with no unresolved P0/P1 against
reviewed-candidate core SHA-256
`633f4fa8cb789b7a0245cb87e7f602d3946457a7448692dc1bc6ae88c1ff4441`.
M5-D24-C1 through M5-D24-C7 are accepted. No source/test edit is authorized and
no implementation lane is active. M5.0-24 is contract-`PASS` /
implementation-`PENDING`.

Production typed-direct preflight on exact integrated base
`254e9c27b0dfc74df1e02ba2d8cd04c7fa9a2c6a` exposed remaining acquisition,
failure-closure, and active-invocation provenance gaps. The existing cursor
acquisition requires a caller-selected
lease token before its transaction locks the job, samples database time, and
discovers the dense attempt. Its lease-only return also omits the epoch, exact
requested M4 job, and exact M4 attempt. Separately, direct
`TERMINAL` acquisition whose projection is `TERMINAL_FAILED` or has reason
`epoch_failed`, and cursor-local direct terminal failure, are not checked C5/C6
active-cutoff origins.

The accepted correction freezes a transaction-owning tokenless production acquisition
returning an immutable operational receipt with exact `epoch_id`, unchanged M4
`LogicalJobSpec`, unchanged `M5TypedDirectJobLease`, and exact unchanged M4
`JobAttempt | None`. The old token-bearing cursor API remains a checked
compatibility surface, not the total production entrypoint. No caller may
pre-read or infer an ordinal/token, parse an exception as protocol, or retry
with a guessed token.

For a qualifying direct failing-terminal acquisition, the exact canonical
same-event/payload/epoch `FAILED` result is the M5 authority; the application
never maps the arbitrary M4 terminal state or reason string into
`M5RunFailureReason`. For terminal attempt failure, the accepted correction adds one
checked transaction and receipt that binds direct settlement to typed epoch
failure, carries the exact requested `M5RunFailureReason` separately and
losslessly through existing M4 failure metadata and the event-result failure
wire, and returns either the first `FAILED` result or its canonical same-reason
`REPLAYED` result.

The application passes its exact held fresh/resumed nonterminal
`OpenEventReceipt` through the internal direct runner and a new required-held-
receipt production generic-failure method; the old no-receipt concrete-store
method becomes non-qualifying legacy/test compatibility. Two mutually
exclusive `M5DirectExecutionReceipt` fields carry either checked terminal-
acquisition or checked-combined-failure provenance, never the generic terminal
reason.

The target-optional private M4 helper makes every activated typed-M5 failure
total. A combined first write terminalizes its target and cancels every other
open direct job; a generic typed failure cancels all open direct jobs. M5
installs the bijective `CANCELLED/epoch_failed` projections. Both advance the
whole outer transaction once from `N` to `N+1`, apply exactly one existing
`m4-evaluation-failure-v1` `FAIL` transition and no target terminal-failure
DELTA, and use only the existing `EPOCH_FAILURE` anchor. Standalone committed
direct terminal failure and test-only terminal shortcuts are forbidden.

If another typed failure wins after provider failure and cancels the selected
target, the same locked combined call returns an exact zero-write
`CANCELLED/epoch_failed` terminal-acquisition receipt only when that input
attempt remains the exact latest leased token/dispatch, has no execution
evidence, and the canonical failed result validates.
Exact matching `TERMINAL_FAILED` evidence is checked replay; mismatched or
ambiguous evidence conflicts. The loser keeps actual invocation work only in
the active projection and never uses exception/reacquisition protocol.

The accepted correction also records already-frozen implementation obligations:
typed-direct `LIVE_LEASE` and `expired_preterminal` stop as
`BLOCKED/WORK_IN_PROGRESS`. It changes no public M4-v1 byte, existing lease or
event-result field, digest, schema, migration-016 byte/ledger value, event
total, or timing-only telemetry shape. Production seal/publication, provider
adapters, M5-D25, migration 017, and any stage-row promotion remain excluded.

The accepted correction text is
`docs/workstreams/m5_runtime_contract/DIRECT_ACQUISITION_TERMINAL_CUTOFF_CORRECTION.md`.
Acceptance grants no implementation ownership. After the accepted correction
is committed, a separate activation may pin the exact expanded 21-path
implementation manifest only with the mandatory zero-row read-only forbidden-
history guard rejecting any pre-C7 direct `terminal_failed` job/projection, any
direct `epoch_failed` projection, and any failed epoch with an open direct job.
Its two private M4 paths implement
the activated-typed-M5 composite helper without changing public or standalone
v1 behavior; `contracts.py` and `test_contracts.py` pin both new receipt
topologies, and `postgres_application.py` preserves the group-only facade's
exact held-receipt protocol compatibility. `postgres_roots.py` splits its
bundled M5 failure lock/mutation helper into read-only plan and write-only apply
phases so the global lock order is executable without duplicated SQL;
`postgres_recovery.py` extends the shared terminal timing finalizer with the
optional attempt observation so one fused accumulator CAS remains possible.
The accepted correction and this governance record grant no current ownership,
implementation promotion, or M5.4 promotion.

## 2026-08-17 — M5-D24 R2d Typed-Direct Application Outcome Integrated

Decision status: the bounded R2d pure typed-direct application-outcome tranche
is integrated on main at
`c892cc8a547a9c0248ad735e11270daa0e1acf4e`. Its scoped tranche gate is
`PASS`. This is an implementation-status record, not a contract amendment, an
M5.4 row promotion, or an implementation-`PASS` promotion for M5.0-24.

The integration has sole parent and exact activation base
`b5c4c06ee81f79638771831d80b8ca21df58e0bd`. It changes exactly the four
activated R2d paths and has immutable Git tree
`30d8cfdec0cf49e6e5aa42685f555cdc0692a8d5`. The frozen source/test SHA-256
pins are:

- `application.py`:
  `6911e237a1727897f2d8241eee939a48e43e42e8c1258dacc9b4ded6739b5bd2`;
- `fake_ports.py`:
  `dd7c70975ee807f340551da6a0ff104405b9e2aae4cc38d95003c14c9fa373af`;
  and
- `test_d24_application_composition.py`:
  `90ea5f805cdfd2db60b6c8811ecfebbff2191320e6767f9a997d783ae7a7e0d4`.

The integrated pre-conversion handoff has SHA-256
`ba3c235cecf6b305a30a6e15fad71a9fbdfc3ea06b0795d56b98fc0f183f8c1f`.
The immutable integration audit returned `GO` with no unresolved P0/P1.

On the exact integrated main bytes, the focused R2d selection passed 101/101,
the complete pure M5 runtime gate passed 339/339, and the non-database DTO/
legacy/M4 selection passed 14/14. Ruff, format, strict-mypy, cache-isolated
compile, diff, and exact-hash gates also passed. The frozen candidate handoff
separately records the 189/189 complete owned composition file and the 81/81,
163/163, 589/589, and 48/48 live compatibility regressions.

This evidence closes only the pure checked C1/C5 application sequence for a
selected successful typed-direct discovery/verifier outer receipt that loses
the active terminal cutoff, plus ordinary reconnect preservation. It does not
provide a PostgreSQL typed-direct application bridge, cursor-local direct
failure, production seal/publication, production provider adapters, or
combined end-to-end recovery. M5.0-24 remains contract-`PASS` /
implementation-`PENDING`; every M5.4 row is unchanged and M5.4 remains partial.

The R2d activation and handoff are historical evidence. Their exact four-path
grant is closed and gives no current edit ownership. The persistent
worktree, if retained for audit, is not implementation authority. Any later
lane requires a fresh committed path-exclusive activation from the then-current
integrated barrier and may not inherit R2b, R2c, or R2d ownership.

## 2026-08-17 — M5-D24 R2c Group/Requirement PostgreSQL Pre-Seal Bridge Integrated

Decision status: the bounded R2c group/requirement PostgreSQL pre-seal bridge
is integrated on main at
`0e0ff4385b4f5e5145788f59cc55411b39d659c1`. Its scoped tranche gate is
`PASS`. This is an implementation-status record, not a contract amendment, a
M5.4 row promotion, or an implementation-`PASS` promotion for M5.0-24.

The integration has sole parent and exact activation base
`abe22e6d1cf844ca7bc63697f089cc77fcb4397f`. It adds exactly the five activated
R2c paths and has immutable Git tree
`021ba1d5c58434c9b8129dce257af302a810e0e9`. The source and three test/fixture
SHA-256 values exactly match the frozen handoff pins. The immutable integration
audit returned `GO` with no unresolved P0/P1.

On the exact integrated main bytes, the new live PostgreSQL suite passed
81/81. Ruff check, Ruff format check, strict mypy, cache-isolated compile,
diff-check, and exact-hash checks also passed. The candidate handoff separately
records the 237/237 pure-runtime, 163/163 requirement-runtime, 589/589 complete
PostgreSQL-runtime, 48/48 public-M4 route/direct, and 14/14 DTO/legacy/M4
contract regression gates.

The original activated `/tmp` worktree disappeared during an environment
restart before candidate commit. At the user's explicit direction, the same
branch at the unchanged activation base was recreated at
`/home/kassym/Desktop/groundloop-worktrees/m5-d24-r2c-group-requirement-bridge`.
The exact in-flight bytes were recovered and revalidated there. This
operational relocation changed no Git base, contract, owned byte, or evidence
claim.

This evidence closes only the scoped group-lifecycle requirement path through
the concrete PostgreSQL store up to an explicit fail-closed pre-seal boundary.
M5.0-24 remains contract-`PASS` / implementation-`PENDING`; every M5.4 row is
unchanged and M5.4 remains partial. Typed-direct outer settlement,
cursor-local direct failure, production seal and combined publication,
production discovery/verifier/measurement adapters, M5-D25/migration 017, and
the remaining end-to-end closure evidence remain pending under separate fresh
manifests.
It does not establish deployment readiness, exactly-once external provider
execution, performance or call savings, model quality, representative utility,
human approval, security, novelty, or publishing potential.

The R2c activation and handoff are historical evidence. Their five-path grant
is closed and gives no current edit ownership. The persistent worktree, if
retained for audit, is not implementation authority. Any later lane requires a
fresh committed path-exclusive activation from the then-current integrated
barrier and may not inherit R2b or R2c ownership.

## 2026-08-16 — M5-D24 R2b Pure Application Composition Integrated

Decision status: the bounded R2b pure-orchestration tranche is integrated on
main at `bfeef3f87ea79137a8ce956eccd47ced1744e4f0`. This is an
implementation-status record, not a contract amendment or an implementation-
`PASS` promotion for M5.0-24.

The integration has sole parent
`62bfb03ea652cf817c2eb3aa0888b7e7502e0af9` and contains exactly the five
activated R2b paths. Its exact source/test bytes match the audited handoff.
The focused composition suite passed 87/87 and the full pure M5 runtime gate
passed 237/237; Ruff, format, strict mypy, cache-isolated compile, collection,
and diff-check also passed. No live database was used.

This evidence closes only the scoped pure requirement-application and fake-
seal orchestration gate. M5.0-24 remains contract-`PASS` /
implementation-`PENDING`. A concrete PostgreSQL/production application
adapter, typed-direct outer-settlement composition, cursor-local direct
failure, production seal/publication, and the remaining live crash/reconnect
and end-to-end evidence are still pending under separate future manifests.

The R2b activation and handoff are now historical evidence. Their five-path
grant is closed and gives no current edit ownership. Any later implementation
lane must have a fresh committed path-exclusive activation based on the then-
current integrated barrier; it may not inherit the R2b branch, worktree, or
ownership grant.

## 2026-08-16 — M5-D24-C6 Active-Terminal-Cutoff Work Completion Accepted

Decision status: accepted narrow correction after two independent exact-byte
reviews. Accepted reviewed-candidate SHA-256:
`b6d69c7c6db63b6a726b2787639b0b15851441efc0c2f3d17091e1b6de34c393`.
No source or test edit is authorized by this entry.

R2b application composition exposed three reachable current-invocation work
losses outside accepted C5. An invocation may already hold its exact
nonterminal `OpenEventReceipt` and accumulated work when a later requirement
acquisition returns checked `TERMINAL/EPOCH_FAILED`, when the checked failure
mutator returns a same-reason canonical failed replay after terminal-attempt
work, or when the fake-only seal mutator returns a canonical replay after
current work. Returning the ordinary terminal replay unchanged loses that
invocation's work even though frozen event totals must not change.

Accepted M5-D24-C6 reuses, rather than revises, C5's existing active-cutoff
`REPLAYED` envelope and validator. For exactly those three checked origins,
the application validates the complete origin and canonical ordinary replay,
constructs and validates the active envelope using the invocation's actual
nonterminal receipt and exact accumulated `call_work` including zero, and only
then writes the unchanged timing-only terminal telemetry. Ordinary terminal-
known-at-entry/open replay remains terminal-projected with zero work.

The acquisition origin requires exact job/execution and terminal-projection
identity, `TERMINAL` exact replay, and reason `EPOCH_FAILED`, followed by a
same-event/payload/epoch canonical `FAILED` result. It may not invent an
`EPOCH_FAILED` to run-failure-reason mapping. The failure origin requires the
checked mutator's failed replay to retain the exact requested failure reason.
The seal origin preserves whichever exact canonical sealed/failed branch the
checked fake returns and remains fake-only evidence, not production seal.

The correction changes no DTO, validator, marker, digest, schema, migration,
persistence transaction, event total, telemetry-work shape, public M4-v1 byte,
or accepted C1--C5 behavior outside the three named provenance additions.
Cursor-local direct failure, typed-direct successful-outer application,
production seal, arbitrary terminal reads, and every unlisted origin remain
excluded.

M5.0-24's contract half is restored to `PASS`; its implementation half remains
`PENDING`. The prior R2b five-path lane stays paused and has no current edit
ownership. After the coordinator commits this accepted C6 freeze, the generic
C5 validator needs no new micro-lane; the coordinator must separately commit
a fresh exact C6-based R2b activation, recreate or reset its literal branch/
worktree to that activation, and revalidate the same five paths. Neither
current activation commit `5ccd615` nor historical base `f5902ff` may be
reused as authority. Typed-direct remains separately manifested.

The authoritative accepted text is
`docs/workstreams/m5_runtime_contract/ACTIVE_TERMINAL_CUTOFF_INVOCATION_WORK_COMPLETION_CORRECTION.md`.

## 2026-08-16 — M5-D24-C5 Terminal-Race Invocation Work Accepted

Decision status: accepted narrow correction after two independent exact-byte
reviews. Accepted reviewed-candidate SHA-256:
`b4afb8fbbdabcf970ac03865fa1510cd8219cac630dbc5ee2d5dc506889f3b67`.
No implementation lane is authorized by this entry.

R2b preflight exposed a returned-envelope contradiction after a successful
requirement result or typed-direct outer settlement loses the terminal cutoff.
The invocation has already opened or resumed the event while nonterminal and
may have performed exact external-attempt work. Its checked successful-return
receipt names the current terminal logical result, so the application must
return that durable outcome.
The existing replay shape, however, requires canonical-zero call work and a
terminal-projected open receipt, which would silently drop this invocation's
work. Adding the work to frozen event totals would double count it, and
terminal timing telemetry has no work field.

Accepted M5-D24-C5 uses the invocation's existing checked
`OpenEventReceipt` as the discriminator. Ordinary terminal-known-at-entry
replay remains terminal-projected with zero call work. The narrow active-
cutoff projection remains `REPLAYED`, retains the actual earlier nonterminal
open receipt, and returns exact accumulated current-invocation call work,
including zero, only after validating a canonical terminal read against the
successful-return receipt's terminal logical-result hash. Durable event work,
result bytes, publication/failure identity, timing/coverage, and logical hash
remain unchanged.

The same semantic projection applies to a checked selected normal/late branch
of a successful typed-direct outer receipt, as already required by C1 Section
5. Typed-direct application implementation is explicitly outside R2b and
requires a separate future path-exclusive manifest; R2b covers only the
requirement discovery/verifier application race.

The correction requires no marker, schema, migration, digest, persistence, or
telemetry-work change. R1-D, R1-P, R2a, and R1-C are already integrated at
`1838316`, `56dd2d4`, `6f1ae89`, and `f5902ff`, respectively; those tranche
results do not close R2 composition or M5.0-24.

After the coordinator commits this accepted freeze, it must separately commit
a new activation note with the literal branch/worktree/accepted-freeze base
for the three-path contract micro-lane. After that lane integrates, the
coordinator must repin and recreate/reactivate the unchanged four-path R2b
lane from the exact integrated C5 contract commit; the historical `f5902ff`
activation base may not be reused. Typed-direct composition remains separately
manifested. M5.0-24's contract half is restored to `PASS`, its implementation
half remains `PENDING`, R2b is paused, C1--C4 remain accepted, and neither
future lane may edit without its separate committed activation.

The authoritative accepted text is
`docs/workstreams/m5_runtime_contract/TERMINAL_RACE_INVOCATION_WORK_CORRECTION.md`.

## 2026-08-12 — M5-D24-C4 Terminal-Successor/Artifact Boundary Accepted

Decision status: accepted narrow correction after independent exact-byte
review. Accepted reviewed-candidate SHA-256:
`f2e23d0bc4c245004f677f771e45123a619d4b1ad88da207e803cb89dcfec80c`.

R1-P exposed the same accepted-schema contradiction beyond C3's
cancellation-first case. After takeover, the dense successor job can become
`retryable_failed`, `completed_active`, `completed_inactive`,
`terminal_failed`, or `cancelled` while the event remains nonterminal. D24's
`running -> running` expired artifact is then false. Migration 016 has no
`retryable_failed` artifact shape and admits the exact terminal-state expired
artifact only after the event runtime is sealed or failed.

Accepted M5-D24-C4 keeps the unchanged preterminal archive only while
the successor is `running`. A `retryable_failed` or terminal successor while
the event is nonterminal conflicts with zero writes. Checked reacquisition may
return a retryable job to `running`, after which the old output may archive
preterminal if it locks before a later terminal transition. Outer
terminalization must resolve `retryable_failed` into one of migration 016's
four terminal states before the first legal postterminal call atomically
freezes the attempt-result artifact, expired-return, execution evidence,
postterminal timing, and general audit using that exact terminal state. R1-P's
clearly labelled SQL-only fixture proves only rejection of the retryable image
and acceptance of the five-row storage shape after test-local state
resolution. R2 owns proof that production terminalization performs that
resolution and owns the end-to-end continuation.

C4 also resolves the phrase "referenced immutable worker artifact closure"
for audit-only requirement returns. The complete discovery or verifier DTO is
resupplied and context/self-digest validated on first call and replay;
discovery additionally resupplies explicit snapshot-exhaustion evidence, and
verifier pair input receives an immutable-core check equivalent to its normal
SQL validator. The five late rows freeze artifact ID/hash and content-
addressed identity. A late-only call does not populate normal discovery/
verifier semantic artifact or currency tables. The authoritative accepted
text is
`docs/workstreams/m5_runtime_contract/TERMINAL_SUCCESSOR_EXPIRED_OUTPUT_CORRECTION.md`.

Independent review accepted the exact candidate bytes with no unresolved
P0/P1. M5.0-24's contract half is restored to `PASS`, its implementation half
remains `PENDING`, and R1-P resumes under C4. The accepted correction changes
no migration-016/017 byte or status, public contract or digest, M4-v1
behavior, D25 boundary, or accepted C3 result.

## 2026-08-10 — M5-D24-C3 Cancellation/Expired-Output Boundary Accepted

Decision status: accepted narrow correction after independent exact-byte
review. Accepted reviewed-candidate SHA-256:
`59fca1859e55af3ff9ffe818205c0ed61cacd17876525cddfc60d448f9eaf723`.

The R1-P cancellation race gate exposed a contradiction between the D24 race
prose and the already-accepted migration-016 validators. After a committed
takeover, D24 gives `attempt_expired` precedence. If cancellation commits
before the old output while the event remains nonterminal, however, the job
and root scope are durably `cancelled`. Migration 016 accepts only
`running -> running` for a preterminal expired artifact and accepts an exact
terminal-state artifact only after the event is sealed or failed. Neither
preterminal artifact can truthfully describe the locked rows.

M5-D24-C3 preserves migration 016. Output-first keeps
the existing `EXPIRED_PRETERMINAL` closure. Cancellation-first conflicts with
zero writes while the event is nonterminal. The caller may retain and resubmit
the provider output after seal or failure, but persistence cannot prove
continuity with either zero-write rejection. The first legal postterminal
commit freezes the complete returned identity as the attempt-result artifact,
expired-return row, execution evidence, postterminal timing, and general
audit; only subsequent changed replay can conflict. This closure uses exact
cancelled attribution and cannot change frozen terminal totals. The
authoritative accepted text is
`docs/workstreams/m5_runtime_contract/CANCELLATION_EXPIRED_OUTPUT_CORRECTION.md`.

Independent review accepted the exact candidate bytes with no unresolved
P0/P1. R1-P resumes from its 34/34 live-green cancellation checkpoint under
the correction. This decision changes no accepted migration-016 byte, ledger
value, public M4-v1 contract, D25 boundary, or migration-017 status.

## 2026-08-08 — M5-D24 R0 Migration 016 Accepted; R1 Authorized

Decision status: exact migration-016 schema/installer boundary accepted on
main; exact path-exclusive R1-P and R1-D checked-persistence lanes authorized
by a committed manifest. R2 composition, M5-D25, and overall M5-D24
implementation evidence remain pending.

The complete audited migration-016 topology was integrated in order as main
commits `4e7f3e4`, `c10cf8d`, and
`61875894172c8e0b36866d6b215ecab7a57b76ec`. On that exact final commit, the
live migration-016 suite passed 200/200, migration-015 passed 26/26, and the
broader PostgreSQL runtime suite passed 292/292. Ruff formatting and lint,
strict mypy, compileall, bundle-identity recomputation, and diff-check passed.

The accepted migration-016 ledger identity is:

```text
bundle_id = m5-runtime-recovery-schema-bundle-v1
bundle_sha256 = 28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565
migration_sha256 = a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7
oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
prerequisite_sha256 = b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd
```

This accepts raw schema and installer invariants only. It does not prove
checked attempt acquisition/settlement, reconnect recovery, exactly-once
provider execution, M5.4 closure, or a utility claim. The exact R1 paths,
branches, worktrees, accepted tuple, shared pins and gates are frozen in
`docs/workstreams/m5_runtime_implementation/D24_R1_ACTIVATION.md`. No lane owns
the user-held M5-D25 draft.

## 2026-08-07 — M5-D24-C2 Legacy Terminal Coverage Boundary Accepted

Decision status: narrow correction accepted after two independent exact-byte
reviews; implementation evidence remains pending.

M5-D24 Section 11.2 permitted migration 016 to install over a terminal
zero-attempt M5 epoch while also forbidding guessed legacy work/timing
backfill. Migration 015 has only terminal aggregate timing; it does not have
the point observations, expected/observed/missing coverage, pending-anchor
history, or accumulator transition needed for the one-to-one terminal cutoff
required by Sections 9 and 11.3. Those requirements cannot both hold without
inventing evidence.

The accepted correction makes first installation reject any pre-016 terminal
M5 runtime header or M5 event result after the existing live/attempt checks.
It preserves the exact seven-lock order, attempt-family error precedence,
failure atomicity, bare terminal base epochs with no M5 runtime/result, and
both ledger-first exact-rerun paths. It authorizes no legacy backfill and does
not weaken the exact post-016 terminal work/timing/coverage closure.

The authoritative text is
`docs/workstreams/m5_runtime_contract/LEGACY_TERMINAL_COVERAGE_CORRECTION.md`.
Two independent exact-byte reads returned GO on pre-freeze content SHA-256
`de9a56439d4f1995339c72917c52dc7726c9fdd6095c27c8923493193abb5aad`.
This correction changes no M4-v1 contract and does not authorize M5-D25.

## 2026-08-06 — M5-D24-C1 Execution Disposition and Return Receipt Accepted

Decision status: narrow correction accepted after independent exact-byte
review; implementation evidence remains pending.

The M5-D24 implementation preflight found linked omissions in its otherwise
frozen recovery/accounting contract. Successful requirement and typed-direct
settlement must persist `returned` versus `reused_artifact`, but the named
method inputs do not carry that non-inferable fact. Successful returns can also
race takeover or the terminal cutoff, while the existing completion receipt
does not identify normal application versus preterminal/post-terminal audit or
return the sole transition timing anchor. The direct late-return receipt was
named without a topology, cursor-local direct methods were said to return
contribution identities while several return `None`, and replay after later
terminalization was not separated from the immutable first-return outcome.

The accepted correction requires adapters to supply an explicit successful
execution disposition and original attempt timing, keeps failure dispositions
method-derived, separates cursor contribution candidates from the outer
revision/anchor, and freezes total operational receipts for requirement and
typed-direct successful returns, including nonexpired preterminal audit and
replay after later terminalization. It changes no semantic digest,
migration-015 byte, M4-v1 DTO/API, or provider-exactness claim. Its
authoritative text is
`docs/workstreams/m5_runtime_contract/EXECUTION_DISPOSITION_RECEIPT_CORRECTION.md`.
Independent re-audit returned exact-byte GO on pre-freeze content SHA-256
`741ce0de897099164eb877684bc12209f347eb920185be5ed4c3c3d5395bf25a`.
Executable evidence remains pending and must follow the committed
path-exclusive manifest.

## 2026-08-06 — M5-D24 Recoverable Dispatch and Durable Accounting Frozen

Decision status: runtime recovery/accounting amendment accepted after
adversarial review; implementation evidence remains pending.

Migration 015 can durably mark a dispatched attempt but does not give M5
attempts a deadline, a total takeover result, or a nonterminal point record of
confirmed work and timing. A crash after dispatch can therefore leave a job
permanently RUNNING, while treating the dispatch marker as a confirmed provider
call would overstate work. These are production-contract gaps, not permission
to infer lost work or redispatch without serialization.

M5-D24 freezes database-clock leases; dense checked takeover; total
`dispatch_new`, `dispatch_takeover`, `live_lease`, `result_reserved`, and
`terminal` projections; immutable dispatch and execution evidence; exact
confirmed-work and timing contributions/accumulators; explicit unresolved-call
bounds; byte-total expired and post-terminal audit sidecars; and one timing
anchor per outer transaction. Terminal event work/timing remains frozen at the
terminal cutoff, while later evidence is queryable separately. Typed-direct
recovery uses M5-owned wrappers and sidecars and changes no public M4-v1 DTO,
digest, row identity, or route behavior.

Migration 016 is exactly
`migrations/016_m5_runtime_recovery.sql` with bundle ID
`m5-runtime-recovery-schema-bundle-v1`. It requires all five literal accepted
migration-015 ledger fields, rejects any pre-upgrade M5 or typed-direct attempt,
and may replace only the two named attempt-result constraints and the named
validator function required for `attempt_expired`. The authoritative contract
is `docs/workstreams/m5_runtime_contract/RECOVERY_WORK_AMENDMENT.md`; its
audited pre-freeze content SHA-256 is
`7fbcb57ae8a1e71d17457409f9f864418b42cc506ebc191211f476caa59e2475`.

This decision establishes recoverable at-least-once dispatch and idempotent
semantic effects. It does not establish exactly-once provider execution,
objective truth, performance superiority, or a representative utility result.

## 2026-08-06 — M5-D23 Runtime Transition Completeness Frozen

Decision status: narrow runtime-contract/schema completeness amendment
accepted; implementation evidence remains pending.

The first production transition audit found three omissions in runtime-
addendum revision 3. `mark_m5_retryable_failure` accepted an `error_hash` but
migration 015 had no durable error field and the API named no receipt. The
cancellation mutator referred to an undefined `cancellation_plan`. Finally,
the cursor-local M4 open adapter received `DynamicEventPlan` but not the
`StructuralPayload` containing the document/chunk/metadata bytes it must stage,
and the five-method adapter had no transaction-local acquisition operation.
Overloading `attempt_output_digest`, reconstructing missing document metadata,
or calling the public M4 store would violate audit fidelity, M4-v1 stability,
or atomic typed composition.

M5-D23 freezes the missing pieces. A failed attempt stores a separate nullable
lowercase SHA-256 `error_hash`; retryable failure returns
`M5AttemptCompletionReceipt` and exact replay validates that hash with zero
writes. `M5CancellationPlan` binds the structural event, target epoch, sorted
nonempty job-ID set and one allowed cancellation reason under
`m5-cancellation-plan-v2`. The direct open helper now receives the exact
payload-bound `StructuralPayload`, and the internal direct adapter adds a
cursor-local acquisition method so its dispatch marker commits under the
outer typed transaction. Public M4 mutation entrypoints remain forbidden on a
typed epoch.

The typed application may use a read-only hydration port for current revision,
canonical verifier jobs, and persisted work. These reads own no transaction
spanning an external call and cannot mutate or reconstruct terminal semantic
results. No legacy digest/DTO, M4 never-activated behavior, M5 semantic state,
or migration-014 byte changes under this amendment.

## 2026-08-06 — M5-D22 Changed-State Artifact Digests Frozen

Decision status: narrow runtime-contract completeness amendment accepted;
implementation evidence remains pending.

Runtime-addendum revision 2 required activation and every sealed typed result
to bind a canonical `m5-changed-state-set-v2`, but specified only the outer
reference/set recipes. It did not define how the referenced requirement,
group, claim, or answer row becomes `state_artifact_hash`. The activation
request therefore could not be independently constructed or verified without
an implementation-private serialization. Guessing that serialization would
violate the byte-total M5-D14 boundary.

M5-D22 freezes four semantic-row artifact domains. Each digest binds the
object ID and every persisted semantic field in schema order, including the
decision-policy version and certificate binding where those columns exist;
optional scores use exact `OPTION(F64)`, sequences retain their already
canonical order, and NULL remains the typed NULL. Requirement, group, claim,
and answer artifact hashes exclude publication coordinates because the outer
changed-state reference already binds epoch and revision. A
`group_certificate` or `claim_certificate` reference uses the immutable
certificate's already byte-total `certificate_digest` directly as its
`state_artifact_hash`; it is not hashed again under another domain.

Activation must derive all six reference kinds from the independently built
base-head projection and reject a request whose bootstrap set differs. Typed
publication must use the same recipes, and live persistence validation must
reject a reference whose hash does not match the named historical state or
certificate. No M4-v1 identity, M5 semantic state, certificate recipe, or
migration-014 byte changes under this amendment.

## 2026-08-06 — M5-D21 Typed Direct Bridge Frozen

Decision status: narrow runtime-contract amendment accepted; implementation
evidence remains pending.

Migration 014 intentionally makes `groundloop_m5_guard_v1_open()` reject every
`groundloop_m4_update` insert after activation. Runtime-addendum revision 1
also required an activated typed document event to preserve the exact M4-v1
direct declaration inside the same transaction as its M5-v2 sidecar, while
forbidding migration 015 from changing any 014 object semantics. Those
requirements are jointly unsatisfiable; application code cannot safely bypass
the database guard, omit the direct declaration, or commit it separately.

M5-D21 authorizes one exact exception. Migration 015 may replace only the body
of `groundloop_m5_guard_v1_open()` while leaving its trigger installed. The
`v1_only` branch stays unchanged. In `m5_active`, an M4 update is accepted only
after the current SQL transaction has installed an exact matching typed
document update and revision-1 runtime header for the same event epoch,
update-kind mapping, prior publication head, candidate-policy manifest,
decision policy, and M4 registry binding. A previously committed sidecar is
insufficient. A deferred runtime-header validation requires the typed document
sidecar and M4 row to commit as a bijection, so no committed sidecar can become
a reusable bypass. Missing or mismatched declarations and injected failures
roll back the epoch and all child/PENDING state; a public v1 opener still
consumes nothing after activation.

The outer typed transaction is also frozen as the final authority for the
shared epoch state. A cursor-local M4 transition may compute direct readiness,
but `semantic_status=complete` may commit only when the direct coordination
surface is complete and all three M5 epoch counters are zero. Neither direct
completion nor a subgraph helper may advance a publication head or expose
strict state independently. Public M4 resume, completion, failure, and seal
entrypoints reject typed epochs before changing a row. No v1 digest, DTO,
public receipt, never-activated behavior, M5 evidence-group semantics, or
migration-014 file byte is changed by this decision.

## 2026-08-03 — M5.1 Pure Reference Semantics Accepted

Decision status: M5.1 accepted after an independent high-confidence audit GO;
M5.2 integration and M5.3 PostgreSQL implementation are active.

The accepted implementation adds exact M5 digest and 29-code-point
normalization primitives, immutable evidence-family/group/requirement records,
typed requirement observations, lifecycle and currency history, atomic
register/replace/retire/observe events, and an independent unmatched-branch
Python oracle. Its finite-domain gate enumerates all 74,958 simple bipartite
graphs through `r<=4,H<=4`. The 73 focused tests, full repository suite, Ruff,
and strict mypy pass.

The audit forced two failure-atomicity fixes before acceptance: a rejected
same-point currency write may neither archive nor globally reserve the failed
observation, and snapshot coordinates reject booleans and fractional aliases.
It also confirmed shared global event and observation identifier namespaces,
intervening legacy-epoch reconstruction, typed validation before mutation, and
validation of noncanonical but valid covering certificates.

M5.1 is not full M5-D9/D12 persistence. The preserved direct M1 state is
current-only, so historical combined claim-certificate binding validation
remains mandatory in M5.2/M5.3. The Python currency intervals are a
total-order reference abstraction; migration 014 must implement the frozen
epoch-local working-history and fallback rules. No optimized matching,
PostgreSQL, runtime, model, performance, or evaluation claim follows from this
decision.

## 2026-08-02 — M5.0 Bounded Evidence-Group Contract Frozen

Decision status: M5.0 accepted after three independent high-confidence audit
GOs; M5.1 implementation is active and all later evidence gates remain
pending.

The original evidence-group sketch was not implementable as written. Counting
nonempty requirements or comparing the global union of witness hashes with the
number of requirements does not establish a system of distinct
representatives. The frozen M5 semantics therefore define one bipartite graph
per group and require a covering matching. Every distinct
`(requirement_version_id,text_hash)` zero crossing is material, even when no
requirement count or global-union count crosses a boundary.

The optimized operator uses the fixed left bound `r <= 8`. It maintains one
adjacency mask per distinct content hash, a mask histogram, Hall neighbor
counts for all nonempty requirement subsets, maximum deficiency and exact
matching size. A coalesced hash-mask transition touches at most `2^r-1` Hall
entries. Immutable matching-certificate artifacts have exact epoch/revision
bindings and stateful repair/rebuild rules. Independent Python unmatched-branch
backtracking and a base-edge SQL Hall oracle are forbidden from reading this
incremental state.

The accepted complexity statement is M5-T2 in
`docs/m5_design_freeze.md`. It charges ordered policy probes even when no
candidate flips, changed observations, hash-mask transitions, provenance
repairs, certificate reconstruction and representative-index work, touched
group/claim/answer state, structural construction, and output bytes.
PostgreSQL I/O/WAL/locks and neural inference remain outside that logical RAM
bound. No general dynamic-matching novelty or superiority over DBSP, F-IVM,
CROWN, or another named system is accepted.

Physical M5 state is additive and versioned. Group and requirement semantic
records are immutable; requirement activity derives from one group-validity
sidecar. PostgreSQL receives a typed subject registry, historical
revision-level M5 currency, immutable eligibility, staged/effective/published
group overlays, temporal duplicate/lineage constraints, certificate artifacts,
and a separately activated v2 publication head. Migration and the independent
SQL oracle form one content-hashed transactional bundle. Existing M4 v1
digests and receipts remain unchanged on the never-activated route; activation
serializes with v1/M5 durable opens and rejects new v1 mutations thereafter.

WiCE is accepted only as a retrospective controlled substrate. A supporting
sentence set remains atomic; identical textual content coalesces as an SDR
representative while every source annotation retains its original zero-based
ordinal. Only the least-ordinal duplicate is projected, and every projected
row is a REQUIREMENT-subject observation. A subclaim label can never create
direct claim support. Source-semantic labels and GroundLoop SDR completeness
remain separate, Hall-failing source-positive examples remain in the result,
and model-proposed groups cannot enter the primary table.

Three read-only audit lanes initially returned NO-GO and exposed concrete
theory, schema, runtime and evaluation contradictions. After correction, all
three returned GO with high confidence against identical file hashes. The
audited hashes and complete current boundary are recorded in
`docs/m5_implementation_status.md`.

This decision initially froze M5-D1 through M5-D20 and the falsification
contract; the later M5-D21 entry records the narrow typed-runtime correction.
Neither decision marks matching, PostgreSQL, runtime, real-model, evaluation,
or closure evidence complete. Those cells remain `PENDING` until their
M5.1--M5.6 gates execute.

## 2026-07-21 — M4 Closed with Negative/Preliminary Scientific Verdict; V0 Retained

Decision status: M4 implementation and bounded evaluation complete; M5 is
unblocked with explicit scientific debt.

M4.10 closed the executable naturally versioned-history path on three pinned
Git histories, but not the population-level selective-maintenance hypothesis.
All non-exhaustive treatments recovered only `1/4` model-relative positive
pairs and `0/1` answer-status effects at the single frozen `L=1` budget. The
pilot has fourteen exhaustive pairs and no independent human labels. It is
reproducible evaluation evidence, not a defensible recall/call-saving or
generalization result.

M4.12 independently established that the frozen M3 verifier is weak on
revision-sensitive evidence. On 128 label-stratified real-revision VitaminC
cases (512 endpoints), it achieved `0.5059` accuracy, `0.4655` macro-F1,
`0.3281` detected label flips and `0.1992` joint endpoint correctness. The
`0.6836` bidirectional-margin result showed latent score signal but did not
rescue the poor decision behavior. This diagnostic is consumed and was not
used for M4.13 terminal selection.

M4.13 executed V0, replay control V1, three V2 CE-mix seeds, three V3
paired-margin seeds and the A1 no-replay ablation under clean execution commit
`2bf686d70ba1be5a2b2ad7f3f6e960e338d36373`. Development-only selection chose
V2. Both V2 and V3 passed the development forgetting guards, but V3 improved
median VitaminC joint correctness by only `0.001953125`, below the frozen
`0.01` requirement. The paired-margin objective is therefore recorded as
`PAIRED_MARGIN_NOT_USEFUL` for this design and budget.

The single sealed terminal execution returned `NO_GO`. V2 passed clauses
G1--G7, G9 and G10; only G8 failed. Its paired M3 macro-F1-delta lower bound
was `-0.0571125531`, below the pre-registered `-0.05` retention floor, although
the accuracy-delta lower bound was positive. Promotion and terminal tuning are
both unauthorized. V0 remains GroundLoop's default verifier; V2 remains an
experimental development-selected checkpoint only. No threshold adjustment,
post-terminal retraining or repeat candidate selection is accepted under the
M4.13 name.

This negative result does not alter the exact structured-maintenance result.
For a prebuilt registry, fixed policy and identical immutable stored model
observations, successfully sealed measured events agree with independent
structured recomputation. It also does not improve the corrected complexity
claim: affected-accumulator/witness work, score-index work, sorting, bytes and
PostgreSQL index/I/O/WAL/lock costs remain explicit; no worst-case sublinear
event theorem or superiority over DBSP, F-IVM, CROWN or another named system
is accepted.

Final validation passed: focused M4.13 tests `109 passed, 1 skipped`; full
repository tests `680 passed, 7 skipped`; Ruff; strict mypy over 19 focused and
110 repository source files; compileall; and the live validator on PostgreSQL
16.14 with pgvector 0.8.5. The validator reported zero claim mismatches, zero
answer mismatches and zero invalid certificates.

M5 bounded evidence groups may begin because all frozen M4 implementation and
bounded-evaluation gates executed. This sequencing decision carries mandatory
scientific debt: larger independently adjudicated real histories, disjoint
development/test history clusters, multi-budget recall/work curves, meaningful
frontier histories, end-to-end cost measurement, and a new verifier study with
a new held-out reserve are still required before thesis-level effectiveness or
generalization claims.

Evidence: `docs/m4_implementation_status.md`,
`docs/workstreams/m4_10_real_history_study/HANDOFF.md`,
`docs/workstreams/m4_12_public_ai_gate/README.md`,
`docs/m4_13_change_aware_verifier_plan.md`, and the hash-bound M4.13
development/terminal bundles produced by execution commit `2bf686d`.

## 2026-07-20 — M4 Physical and Real-Dynamic Implementation Gates Accepted; Scientific Gate Remains Open

Decision status: implementation gates accepted through M4.11; M4 CORE remains
active pending the M4.10 real-history verdict.

The production measured application now composes prebuilt registry identity,
point/CAS runtime transitions, signed evaluation counters, affected-key
grounding patches and sparse publication. The M4.11 gate covers zero-admission
and multi-child insertion, support deletion with exact frontier closure,
neutral-to-refute replacement, retry/exact/conflicting replay, failed epoch
plus late inactive completion, required/optional children and transaction
rollback at two unrelated-state scales. Full runtime and grounding audits run
after, never inside, each guarded successful kernel. This is adversarial
regression evidence for the concrete implementation, not an asymptotic proof
or a database-page/latency result.

The M4.8 bounded pinned-model history completed INSERT, DELETE and REPLACE,
agreed with the Python and independent SQL structured oracles after every
event, and replayed each event through a fresh connection with zero discovery,
embedding or verifier calls. This proves integration and exactness relative to
the stored model observations. It does not measure retrieval recall, verifier
accuracy or calibration transfer.

The M4.9 controlled harness is accepted as executable evaluation
infrastructure. Its seven policies use identical event IDs, immutable
persisted-audit identities, explicit misses/timeouts and deterministic raw
artifacts. Its table-judgment recall and work values are fixture mechanics;
token and latency measurements are absent. A naturally versioned real-history
pilot, M4.10, is still in progress and must be evaluated before any final M4
recall/call-saving or generalization statement. M5 remains blocked on that
explicit verdict.

Evidence: `docs/m4_implementation_status.md`,
`docs/workstreams/m4_8_real_dynamic_history/HANDOFF.md`,
`docs/workstreams/m4_9_empirical_study/HANDOFF.md`, and
`docs/workstreams/m4_11_physical_history_gate/README.md`.

## 2026-07-20 — Amend M4 Complexity Claim; Reject the Simple Whole-Kernel Formula

Decision status: accepted correction to `docs/m4_design_freeze.md` Section 13
and the initial `docs/m4_7_physical_runtime_plan.md` target. The frozen text is
retained as audit history and is not silently rewritten.

The proposed expression

```text
O(P+ + P- + D_obs + D_candidate + H + A + J + X_claim + X_answer)
```

is not a proved time bound for the composed measured kernel. It treats a
touched claim/answer row as unit cost and omits repeated affected-accumulator
copies, complete witness-array materialization, canonical sorts, ordered score
index work and variable-sized artifact/SQL payloads.

For a fresh successful measured event with a fixed policy, prebuilt registry,
bootstrapped publication head, serialized mutation and expected Python
hash-map access, the accepted Python-work implementation bound is:

```text
O(
    P+ + P- + D_obs + D_candidate
  + sort(R)
  + sum_roots sort(H_root) + sum_roots sort(A_root)
  + sort(A)
  + J_attempt
  + G
  + T_score
  + W_claim + W_answer + U_claim + U_answer
  + B
)
```

`G` charges affected claim-accumulator copies and witness-ID
materialization/sorting. The AVL score index gives
`T_score = O(Q log(E + Q + 1))`. `B` charges bytes compared, hashed, copied or
serialized. The separate logical sparse-row statement must not be called a
physical time bound: PostgreSQL B-tree factors, row width, result sets,
triggers, query planning, WAL, I/O, network and lock waits remain additional
costs. Registry construction and startup/recovery hydration are explicitly
outside the fresh successful-event theorem.

A high-degree claim may incur repeated growing accumulator copies and witness
sorts, including quadratic aggregate work across completions. Dense deletion
and large admitted/publication output remain output-linear in materialized
data. Therefore no worst-case sublinear update theorem, general speedup, or
superiority over DBSP, F-IVM, CROWN or another named system is accepted.

Evidence: `docs/workstreams/m4_7_complexity_proof/README.md` and
`tests/m4/complexity_contract/test_measured_kernel_contract.py`.

## 2026-07-19 — M4 Lane-Local Wave 2 Accepted; Coordinator Integration Remains

Decision status: lane-local evidence accepted; M4 CORE remains active.

The runtime lane now differentially checks indexed withdrawal against an
independent full scan under seeded event shapes and skew. The result supports
the output-sensitive logical-work expression
`Theta(|D| + |E_obs(D)| + |E_cand(D)|)` but explicitly shows no sublinear
worst-case bound for dense fanout.

The admission lane now exercises real PostgreSQL `simple` lexical search,
`ts_rank_cd(..., 32)`, exhaustive materialized pgvector search and a separately
identified HNSW path. Physical index/search settings are provenance-bound.
The tiny fixture's perfect recall is accepted only as a wiring check, not a
quality result.

The evaluation lane now supplies immutable controlled histories, leakage-safe
development/test validation, exact event-metric construction, explicit miss
diagnostics and canonical paired reports. It remains structurally independent
of runtime and selective admission.

The integrated live gate passes 341 collected tests, Ruff, strict mypy over 83
source files, compileall, the PostgreSQL three-oracle validator and dependency
checking. The next blocker is coordinator-owned persistence and the M4.1
deterministic insert/delete/replacement vertical slice.

Evidence: `docs/m4_implementation_status.md` and the three M4 lane handoffs.

## 2026-07-19 — M4 Deterministic Wave 1 Integrated; Performance Claims Deferred

Decision status: implementation foundation accepted; M4 CORE remains active.

The corrected M4 contracts, migration 003, pure dynamic epoch/job runtime,
exact withdrawal/frontier planners, deterministic admission reference,
independent semantic oracles and provenance-safe evaluation mechanics are now
integrated. The live repository gate passes 266 collected tests, Ruff, strict
mypy over 78 source files, compileall, the M2 PostgreSQL three-oracle validator
and dependency checking.

This is not evidence of end-to-end verifier-call savings. The PostgreSQL M4
repository adapter, coordinator pipeline/CLI, production admission adapters,
real-model dynamic history and recall/work experiment remain outstanding.
Therefore no M4 latency, recall or savings claim is accepted at this point.

The next parallel wave is path-exclusive: runtime proves randomized
withdrawal behavior under skew, admission implements live PostgreSQL
lexical/pgvector boundaries, and evaluation implements controlled history
workloads and paired reports. The coordinator alone implements shared
persistence and the deterministic insert/delete/replacement vertical slice.

Evidence: `docs/m4_implementation_status.md` and the three M4 lane handoffs.

## 2026-07-19 — M4 Dynamic Impact Contracts Frozen After Three-Lane Audit

Decision status: accepted for M4 CORE implementation.

Three independent worktree audits agreed that M1–M3 are sound enough to build
on but rejected the original M4 proposal as an executable contract until its
affected-set, empirical-baseline, publication and dynamic-job ambiguities were
resolved. The authoritative correction is `docs/m4_design_freeze.md`.

M4 now separates exhaustive additive update audit from policy-relative
top-`k` snapshot refresh; affected sets are baseline-qualified and not falsely
called nested. Exactness remains three-way structured equality over an
identical stored-observation snapshot, with separate coordination and
evaluation-completeness surfaces.

CORE retains serialized structural epochs. Working grounding state and
append-only published snapshots are separate, so a failed provisional epoch
cannot destroy the last sealed payload. Expandable discovery jobs declare and
close their child set atomically under content-derived completion identity.
Open discovery creates a lazy global PENDING scope. Degraded sealing is
disabled in CORE.

Admission uses deterministic vector/lexical fusion plus mandatory lineage.
`L` is an approximate-channel cap; actual unique verifier calls are the
evaluation budget. Teacher and human judgments are typed separately, and M4
teacher labels use the operational threshold policy rather than verifier
argmax. The learned impact retriever remains a TARGET after CORE audit labels
are frozen; verifier retraining does not precede CORE.

Evidence: the three `docs/workstreams/m4_*/AUDIT.md` files and
`docs/m4_design_freeze.md`.

## 2026-07-18 — M3 Static AI Pipeline Accepted

Decision status: implemented and accepted.

M3 now provides a complete static path from local files through immutable
chunks, BGE/pgvector retrieval, cited Qwen generation, atomic claim
extraction, a genuinely fine-tuned and development-calibrated MiniLM2
verifier, immutable score observations, the unchanged M2 maintenance engine,
and atomic PostgreSQL publication. The top-level CLI writes complete model,
prompt, calibration, candidate, score, state, epoch, timing, and reuse
provenance.

The acceptance claim is systems completeness and exact structured maintenance
over stored scores, not neural truth. The first real integrated answer was
incomplete and remained UNSUPPORTED because its support probability was below
the frozen threshold. Public verifier evaluation lacks REFUTE examples and all
358 public-test inputs truncated; the 18-row transfer fixture is too small for
a strong quality claim. These are recorded results, not hidden failures.

Identical replay is checked before model loading. Calibration identity and
temperature participate in run identity. The real replay produced zero new
and 17 reused artifacts. Dynamic affected-claim discovery and verifier-call
savings remain M4.

Evidence: `docs/m3_implementation_status.md` and the three M3 workstream
handoffs.

## 2026-07-18 — M3 Static AI Contracts Frozen

Decision status: accepted for parallel implementation; M3 is not yet complete.

M3 is a static, locally reproducible grounding pipeline. The exactness claim
still begins only after immutable verifier score observations exist. Retrieval,
generation, extraction, and verification quality remain empirical.

The contract baseline selects fixed-char-v1 chunking, 384-dimensional
BGE-small-en-v1.5 retrieval, Qwen2.5-0.5B-Instruct generation/extraction, and a
MiniLM2 three-way NLI verifier adapted on a SciFact/WiCE-derived design and
temperature-calibrated on development data. All model revisions, prompt
content, decoding settings, inputs, and retrieval settings are immutable
provenance. WiCE `not_supported` is NEUTRAL, never REFUTE, because WiCE does
not annotate contradiction.

Long model calls run outside the database publication transaction. A complete
run publishes atomically from STAGED to PUBLISHED; failures publish no partial
answer/claim/observation state. Identical run inputs reuse immutable artifacts.
Dynamic impact discovery, update admission, scheduling, and verifier-call
savings remain M4+.

Evidence: `docs/m3_model_dataset_audit.md`, `docs/m3_design_freeze.md`, and
`docs/m3_multiagent_execution_plan.md`.

Implementation correction: the coordinator accepted two lane-raised
provenance gaps without changing M3 semantics. `CitedAnswer` and
`ClaimExtractionResult` now record bounded-repair provenance, while each
`VerificationResult` identifies its retrieval candidate, model, prompt,
calibration version, and temperature. The manifest now carries complete
structured claim/answer states and confirmed-epoch provenance. These are
schema-completeness fixes, not changes to D-1 through D-20.

## 2026-07-18 — M2 Accepted; Exact-Flip Result Conservatively Classified

Decision status: implemented and accepted.

The live PostgreSQL 16.14 gate passes with pgvector 0.8.5: the Python
full-recomputation oracle, signed-delta engine, and independent SQL oracle agree
on the tested snapshots, with zero claim mismatches, zero answer mismatches,
and zero invalid certificates. Current-observation and policy-score indexes are
usable. The integrated suite passes 100 tests with the live DSN.

The additional exact-flip prototype partitions observations by their
threshold-independent winning score and uses balanced ordered indexes to
enumerate exactly the labels changed by frozen tie rule v1. Under the explicit
model in `docs/theory/exact_flip_theorem.md`, a threshold-only policy update is
expected `O(log E + f + p)`, and explicit maintenance has an `Omega(f+p)`
write lower bound. Dense flips and high-fanout withdrawals remain linear.

Classification: **known mechanism specialized to GroundLoop**. This is not
recorded as a novel IVM algorithm, a result faster than DBSP/F-IVM/CROWN, or a
publication-level theorem. It is retained as a rigorous specialization,
portfolio artifact, and evaluation target.

Evidence: `docs/m2_implementation_status.md`, `docs/theory/`, and the three
workstream handoffs under `docs/workstreams/`.

## 2026-07-18 — M2 In-Memory Delta Contract Implemented

Decision status: implementation accepted; superseded only with respect to the
now-closed PostgreSQL gate by the entry above.

The optimized path is independent of the Python reference oracle: it consumes
event-local signed contributions, maintains distinct-content reference counts
and score multisets, propagates only changed claim/answer boundaries, and uses
the reference functions only in the differential checker. Policy changes use
the frozen monotone-threshold candidate intervals; the Python sorted-list
index has O(E) point updates and is not misrepresented as the final physical
index. PostgreSQL uses score B-trees.

D-20 is implemented as a separate semantic-epoch coordinator oracle. Semantic
job completions advance a revision while retaining the owning EpochId; strict
and provisional publication boundaries are tested independently from M1
snapshot revisions.

Evidence: `docs/m2_implementation_status.md`, 62 automated tests, and a seeded
100,000-event differential run with equality checked after every event.

## 2026-07-17 — M1.1 Failure-Atomicity Hardening Accepted

Decision status: implemented and frozen as D-19 and D-20.

Independent verification found that M1 validation passed but rejected events
could still advance `current_epoch`, duplicate identifiers within one batch
were not rejected, caller-supplied content hashes could disagree with text,
and the M1 revision counter could be mistaken for the asynchronous semantic
epoch described by v0.2.

Resolution:

- Events apply to a deep staged repository and replace live state only after
  mutation, recomputation, delta construction, and event recording succeed.
- A rejected event leaves the live revision, records, activity, indexes,
  currency state, policy, event registry, and delta log unchanged; its event ID
  remains reusable with a corrected payload.
- Claim and chunk identifiers must be unique both against history and within a
  single registration batch.
- A supplied chunk `text_hash` must equal normalization-v1 SHA-256 of its text;
  negative chunk indexes are rejected.
- M1 `current_epoch` is frozen as a synchronous snapshot-revision counter.
  M2 must introduce a semantic-epoch coordinator whose corpus `EpochId`
  remains stable across completion microtransactions and sealing.

Evidence: `docs/m1_1_hardening.md` and
`tests/integration/test_event_atomicity.py`.

## 2026-07-17 — M0.5 Design Review Resolved; v0.2 Design Frozen

Decision status: accepted and frozen.

An adversarial algorithm and design review of the v0.1 candidate was conducted
per `docs/claude_algorithm_design_review_prompt.md`, self-checked, and
recorded in `docs/claude_algorithm_design_review.md`. The student accepted the
review in full. The frozen design is `docs/technical_design.md` (v0.2);
`docs/initial_technical_design.md` is superseded and retained for audit only.

Headline resolutions (full table: v0.2 Section 19):

- Verdict: proceed with major changes. DB contribution ADEQUATE, AI
  contribution ADEQUATE; upgrade paths identified.
- Schema fixes required before M1 and now frozen: one current observation per
  (subject, chunk, task) key with supersession deltas (D-8);
  requirement-subject observations for witnesses (D-9); validity intervals on
  evidence groups and requirements (D-10); distinct-content witness counting
  (D-11); `importance_weight` deleted (D-7).
- Pending semantics simplified: EvaluationState + confirmed_as_of_epoch +
  monotone lower bounds; numeric upper bounds dropped as vacuous (D-12).
- Heavy-light partitioning removed from FYP scope (D-13). "BSDJ" and
  "RB-NIVM" retired as novelty labels; mechanisms retained as "asymmetric
  impact discovery" and a transparent STRETCH scheduler (D-14, D-15).
- Policy-delta maintenance via score-range indexes promoted to CORE; the
  incremental path is restricted to declared monotone policy classes (D-16).
- Completeness statements are always policy-relative (D-17); late job
  completions for inactive chunks are stored but never active (D-18).
- Open student choices resolved: distinct witnesses per requirement (D-1),
  direct-support fast path in M1 (D-2), fixed top-L sealing (D-3), software
  documentation demo corpus (D-4), fine-tuned calibrated NLI verifier (D-5),
  content-defined chunking as measured TARGET (D-6).

M1 is unblocked. `docs/first_implementation_prompt.md` and
`docs/m1_implementation_plan.md` are revised to match v0.2.

## 2026-07-17 — Open M0.5 Detailed Design Review (superseded by the entry above)

Decision status: proposed, not frozen.

The candidate design is `docs/initial_technical_design.md`, with a rendered PDF
at `docs/initial_technical_design.pdf`. Its Section 22 proposes immutable score
observations plus versioned decision policies, signed delta maintenance,
bounded factorized evidence views, explicit pending-work semantics, asymmetric
insert/delete impact discovery, and a layered implementation. These proposals
must be accepted or amended before M1 begins.

## 2026-07-17 — Select GroundLoop

Decision: build a self-updating RAG system that maintains old answer grounding
under document insertions, deletions, and replacements.

Reason: best balance of meaningful IVM, strong AI content, clear deliverable,
FYP feasibility, portfolio value, and future research potential.

## 2026-07-17 — Reject Generic Agent Memory

Decision: do not frame the project as IVM over general LLM or agent memory.

Reason: deployed memory is usually hybrid text, vectors, metadata, summaries,
and graphs. Treating all memory as a deterministic relational view would be an
unrealistic abstraction.

## 2026-07-17 — Separate Neural Observations from Exact IVM

Decision: store model outputs as immutable, versioned semantic observations.
Provide exact maintenance only over the current stored observations.

Reason: neural extraction and verification are uncertain and model-dependent;
their downstream relational consequences can still be defined exactly.

## 2026-07-17 — GroundLoop over Full GroundGraph

Decision: use bounded evidence-group DAGs and exclude arbitrary recursive
reasoning graphs from the FYP.

Reason: full GroundGraph would require reliable graph extraction, multi-hop
reasoning, recursive deletion maintenance, shared graph construction, and a new
dynamic benchmark. The evaluation risk exceeds the likely FYP benefit.

## 2026-07-17 — Start a Clean Repository

Decision: GroundLoop is independent of Dynagox.

Reason: Dynagox is a C++/MP-SPDZ Secure CROWN research repository with unrelated
security and benchmark scaffolding. The intellectual IVM connection is useful,
but a source dependency is not currently justified.

## Decisions Requiring Explicit Reconsideration

Do not silently change any of these:

- Add recursive reasoning graphs.
- Add multi-agent memory.
- Make model-checkpoint updates an efficient delta workload.
- Add cryptographic privacy or inherit Secure CROWN security claims.
- Replace the full-recomputation correctness oracle.
- Treat hosted LLM output as ground truth.
- Claim publication-level novelty or `first` status.
