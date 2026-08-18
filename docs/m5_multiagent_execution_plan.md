# GroundLoop M5 Multi-Agent Execution Plan

Status: frozen M5 ownership contract; historical Waves 0--2 complete;
M5-D24 recovery R0-C, pure R0-C1, and R0-S accepted on main; R1-D, R1-P, R2a,
and R1-C integrated at `1838316`, `56dd2d4`, `6f1ae89`, and `f5902ff`;
M5-D24-C1 through M5-D24-C6 accepted; R2b pure orchestration integrated at
`bfeef3f`, and R2c group/requirement PostgreSQL pre-seal composition integrated
at `0e0ff43`; R2d pure typed-direct application outcome integrated at
`c892cc8`; all three grants are closed with no current edit ownership

Date: 2026-08-02; M5-D24 path amendments and integration records 2026-08-06
through 2026-08-18

Authority: `docs/m5_design_freeze.md` defines semantics and
`docs/m5_implementation_plan.md` defines gates. This document defines only
ownership, integration order, and evidence required from parallel lanes.

## 1. Non-negotiable execution rule

M5 uses at most three active implementation lanes, including the coordinator.
Parallel work begins only after M5.0 is frozen and only where paths and input
contracts are disjoint. An agent may read any path but may edit only its owned
paths. A needed cross-lane change becomes a written proposal to the
coordinator; it is not implemented opportunistically.

Every implementation lane uses a dedicated Git branch and worktree rooted
outside the main checkout. The main checkout is the integration worktree and
contains pre-existing user presentation changes that no M5 lane owns.

No lane may claim completion from its local tests alone. The coordinator owns
merge order, conflict resolution, full-suite validation, live PostgreSQL
validation, research claims, and milestone status.

## 2. Shared contract barrier

The coordinator exclusively owns these shared contracts for all of M5:

- `AGENTS.md`
- `README.md`
- `pyproject.toml`
- `src/groundloop/domain.py`
- `src/groundloop/repository.py`
- `src/groundloop/reference.py`
- `src/groundloop/events.py`
- `src/groundloop/incremental.py`
- `src/groundloop/differential.py`
- `src/groundloop/cli.py`
- `src/groundloop/postgres/snapshot.py`
- every file under `migrations/`
- `docs/technical_design.md`
- `docs/decision_log.md`
- `docs/roadmap.md`
- all top-level M5 contract/status documents

The coordinator also owns creation of `src/groundloop/m5/__init__.py` and any
shared public protocol imported by multiple lanes. Lane-local modules depend
on those interfaces; they do not redefine them.

## 3. Lane A -- bounded matching kernel and proof artifacts

Branch/worktree:

```text
branch:   workstream/m5-matching
worktree: /home/kassym/Desktop/groundloop-worktrees/m5-matching
```

Owned paths:

- `src/groundloop/m5/matching.py`
- `tests/m5/matching/`
- `docs/workstreams/m5_matching/`

Forbidden edits include all shared contracts, PostgreSQL code, M4 code,
evaluation code, and dependency configuration.

Inputs frozen by the coordinator:

- requirements use dense ordinals `0..r-1`, with `1 <= r <= 8`;
- an active distinct edge is `(requirement_ordinal, text_hash)`;
- edge multiplicity is maintained outside the kernel;
- one coalesced kernel update is a text hash moving from `old_mask` to
  `new_mask`;
- completeness means a matching that covers every requirement;
- certificate inputs expose ordered concrete hashes per mask, ordered active
  observation IDs per edge, the prior immutable certificate artifact, exact
  epoch/revision binding, and policy identity.

Deliverables:

1. deterministic affected-group augmenting-path baseline;
2. exact Hall-mask state with initialization and mask transitions;
3. deficiency-derived maximum matching size and completeness;
4. deterministic stateful certificate-artifact construction/validation, local
   observation repair, zero-flip policy rebinding, selected-edge rebuild, and
   revision-binding transition results;
5. exhaustive small-graph comparison and randomized transition tests;
6. executable work counters and a proof handoff covering M5-T1/M5-T2.

Lane A must not import the Python full-state oracle or SQL oracle. Its baseline
may cross-check the optimized kernel but is not an independent GroundLoop
oracle.

## 4. Lane B -- PostgreSQL M5 adapter and independent SQL oracle

Branch/worktree:

```text
branch:   workstream/m5-postgres-oracle
worktree: /home/kassym/Desktop/groundloop-worktrees/m5-postgres-oracle
```

Owned paths after the coordinator lands migration 014 and the shared Python
contracts:

- `src/groundloop/postgres/m5.py`
- `sql/m5/` (including the bundle member
  `sql/m5/full_recompute_oracle.sql`)
- `tests/m5/postgres/`
- `docs/workstreams/m5_postgres_oracle/`

Forbidden edits include migrations, the generic snapshot adapter, core domain
and event files, matching code, M4 persistence, and dependency configuration.

Inputs frozen by the coordinator:

- names and columns of group, requirement, typed-subject, observation,
  working, published, and certificate relations;
- the ordered, atomically ledgered schema-plus-oracle bundle contract;
- revision-interval working currency, `eligible_for_currency`, group-only
  validity, durable retirement, and temporal duplicate constraints;
- Python snapshot records and loader order;
- one-to-eight requirement bound and lifecycle constraints;
- canonical policy and task-type semantics;
- expected result records for requirement, group, claim, answer, and
  certificate state.

Deliverables:

1. explicit-column M5 snapshot loading and state reading;
2. base-edge Hall-subset SQL oracle plus the capped unmatched-branch recursive
   `UNION` assignment cross-check with `H<=16`, `E<=128`, the frozen 100,000-
   state preflight, and explicit cap-exceeded output;
3. independent SQL policy, witness-edge, group, claim, and answer derivation;
4. certificate-validity and mismatch queries;
5. migration/backfill, rollback, and live-database tests;
6. query-plan and index evidence with limitations.

Lane B must not read Hall-mask materialized state to decide oracle truth and
must not port the Python backtracking or matching-kernel control flow into
SQL. SQL may be exponential in the fixed bound because it is an independent
correctness oracle.

## 5. Lane C -- controlled data adapter and evaluation harness

Branch/worktree:

```text
branch:   workstream/m5-controlled-evaluation
worktree: /home/kassym/Desktop/groundloop-worktrees/m5-controlled-evaluation
```

Owned paths after the coordinator freezes the evaluation-record protocol:

- `src/groundloop/m5/evaluation/`
- `tests/m5/evaluation/`
- `scripts/m5/`
- `configs/m5/`
- `docs/workstreams/m5_evaluation/`

Forbidden edits include core contracts, matching, PostgreSQL, M4, migrations,
dependency configuration, and all raw/generated dataset directories.

Inputs frozen by the coordinator:

- event-stream and state-result records;
- baseline interface and signed-work-counter names;
- official WiCE source revision, expected file hashes, and license rules;
- exact sentence-set-to-evidence-unit mapping;
- split/tuning prohibition and report schema.

Deliverables:

1. pinned, hash-checked WiCE controlled adapter;
2. eligibility, rejection, overlap, and SDR-distribution audit;
3. deterministic controlled dynamic histories;
4. the seven frozen baselines with their semantic-policy versus systems-
   comparator roles and cohort-specific UNAVAILABLE states preserved;
5. raw-count, denominator, clustered-interval, work, latency, and state-size
   reports;
6. separate source/human labels, controlled score projections, exact SDR
   states, and frozen-model diagnostic outputs;
7. a handoff that states every exclusion and unsupported claim.

Lane C may consume the public matching protocol but may not use M5 test labels
to select policies, thresholds, prompts, models, or implementation variants.
It does not commit downloaded data, generated event corpora, model weights, or
large result artifacts.

## 6. Coordinator lane

The coordinator performs work that crosses semantic boundaries:

- M5.0 freeze, decision log, and acceptance matrix;
- shared Python domain, history, events, reference oracle, and composition;
- migration 014 and typed-subject integrity;
- M4-compatible typed v2 runtime/job/publication integration;
- public imports and CLI composition;
- lane contract publication and merge order;
- three-oracle differential and failure-injection harnesses;
- full validation, final evaluation execution, and closure report.

The coordinator does not duplicate a lane's implementation while that lane is
active. If a lane blocks, it is stopped and its committed state is inspected
before ownership is reassigned.

## 7. Contract handoffs and waves

### Wave 0 -- M5.0 freeze

Only the coordinator edits. Independent agents perform read-only theory,
schema/runtime, and data audits. Exit requires no unresolved P0/P1 and a
falsifying acceptance row for every M5-D decision.

### Wave 1 -- pure semantics and matching

- Coordinator: M5.1 shared types, history, events, and independent Python
  oracle.
- Lane A: matching baseline, Hall-mask kernel, certificates, and proof tests.
- Lane B/C: read-only contract preparation; no implementation until their
  inputs are frozen.

Barrier: the Python oracle and structural-event suite pass, and Lane A passes
its exhaustive kernel gate. Coordinator integrates Lane A and runs differential
tests before Wave 2.

### Wave 2 -- persistence and controlled-data plumbing

- Coordinator first lands the migration-014 relational schema contract and M5
  combined engine/protocol, but does not claim or execute the atomic bundle yet.
- Lane B: snapshot adapter plus `sql/m5/full_recompute_oracle.sql` against that
  landed schema contract.
- Lane C: pinned adapter and evaluation mechanics against the landed event and
  report protocols.

Barrier: Lane B and Lane C rebase onto the same integration commit. SQL and
data audits pass independently. After Lane B integration, the coordinator
computes the exact two-file bundle digest, lands the transactional upgrader and
bundle-ledger tests, and only then may the schema bundle execute or M5.3 PASS.

### Wave 3 -- runtime integration

- Coordinator: typed v2 jobs, M4 composition, PENDING, sealing, replay, and
  failure atomicity.
- Lane A: only focused performance/proof falsification if requested.
- Lane B: only SQL/live-DB defect fixes within owned paths.
- Lane C: deterministic controlled histories and reports, not model tuning.

Barrier: deterministic fake-port end-to-end history passes, followed by an
out-of-band three-oracle audit, before any frozen-model diagnostic.

### Wave 4 -- evaluation and closure

No independent lane changes semantics. The coordinator freezes code, executes
the registered gates, records negative results, and updates status documents.
Defects reopen their owning wave; failed scientific results do not trigger
post-hoc threshold or dataset tuning.

## 8. Required lane handoff

Each lane writes a handoff under its owned workstream directory containing:

- branch, worktree, base and head commits;
- files changed;
- interface assumptions consumed;
- tests and exact results;
- lint/type/compile results for owned code;
- benchmark or data commands with seeds and hashes;
- limitations, failed attempts, skips, and environment dependencies;
- explicit statement that forbidden paths were not changed;
- recommended merge order and any coordinator action.

The lane then commits all intended source/test/doc changes and stops. Untracked
generated artifacts are removed or documented, never silently merged.

## 9. Merge order and collision checks

Default integration order:

1. coordinator shared M5.1 contracts;
2. Lane A matching;
3. coordinator group overlay and migration-014 relational schema contract;
4. Lane B PostgreSQL adapter and SQL oracle;
5. coordinator atomic schema-plus-oracle upgrader, frozen bundle digest, and
   ledger tests;
6. Lane C controlled adapter/evaluation;
7. coordinator M5.4 runtime composition and closure docs.

Before every merge, the coordinator checks `git diff --name-only` against the
lane manifest. A forbidden-path edit rejects the handoff until split. After
every merge, focused tests run first, then the full non-model suite at the
wave barrier. Database and long randomized gates run only where specified.

## 10. Current execution note

Three explicitly configured `gpt-5.6-sol` ultra lanes completed the read-only
theory, pure-reference/runtime/evaluation, and PostgreSQL architecture audits.
After an initial NO-GO correction cycle, all three returned final GO with high
confidence. Wave 1 is now active. Implementation ownership begins only after
the coordinator creates the exact path-exclusive worktree/branch named in this
plan; completed audit membership alone grants no edit ownership.

## 11. M5-D24 recoverable-runtime path manifest

This section supersedes the stale "Wave 1 is now active" sentence above for
current work. Historical worktrees and their WIP remain preserved; none grants
new ownership. The coordinator creates every branch from the exact committed
M5-D24 contract barrier and validates name-status before integration.

### Wave R0 -- contracts and migration 016

Lane R0-C uses branch `workstream/m5-d24-contracts` and worktree
`/home/kassym/Desktop/groundloop-worktrees/m5-d24-contracts`. It owns only:

- `src/groundloop/m5/runtime/contracts.py`;
- `src/groundloop/m5/runtime/digests.py`;
- `tests/m5/runtime/test_contracts.py`;
- `tests/m5/runtime/test_digests.py`; and
- `docs/workstreams/m5_runtime_implementation/D24_CONTRACTS_HANDOFF.md`.

Lane R0-S uses branch `workstream/m5-d24-schema-016` and worktree
`/home/kassym/Desktop/groundloop-worktrees/m5-d24-schema-016`. It owns only:

- `migrations/016_m5_runtime_recovery.sql`;
- `src/groundloop/postgres/migrations.py`;
- `tests/m5/postgres_runtime/test_migration_016.py`; and
- `docs/workstreams/m5_runtime_implementation/D24_SCHEMA_016_HANDOFF.md`.

Its migration input includes the accepted narrow first-install correction in
`docs/workstreams/m5_runtime_contract/LEGACY_TERMINAL_COVERAGE_CORRECTION.md`.
That correction grants no ownership of the contract document or another lane's
paths.

Lane R0-C1 uses branch `workstream/m5-d24-c1-contracts` and worktree
`/home/kassym/Desktop/groundloop-worktrees/m5-d24-c1-contracts`. Its accepted
contract input is main commit
`46e7794ae1290ea7e80418c64f5e587efdc9147b`, and it owns only:

- `src/groundloop/m5/runtime/contracts.py`;
- `tests/m5/runtime/test_d24_c1_contracts.py`; and
- `docs/workstreams/m5_runtime_implementation/D24_C1_CONTRACTS_HANDOFF.md`.

Its exact lane base is
`f1ceedd7fd94503c36707da9a3867e55e0e37927`; R0-C1 must validate that HEAD
before editing. This lane implements and
falsifies only the accepted C1 enums and receipt DTO topology. It must not edit
`application.py`, fake ports, PostgreSQL/persistence code, direct-M4 code, or
existing tests. Method/envelope contextual validation and atomic settlement
remain owned by R1/R2. Acceptance of R0-C1 is therefore a pure-contract
checkpoint, not complete C1 or D24 implementation evidence.

The coordinator owns this documentation freeze and integration. R0-C must not
edit migrations or PostgreSQL persistence. R0-S must not redefine Python DTOs
or weaken migration 015. R0-S independently encodes SQL checks and may consume
R0-C only after both commits are inspected and integrated. R0-C1 must preserve
the accepted R0-C bytes except for the additive receipt contracts explicitly
named above.

R0 exits only when golden byte/null/order vectors and the live fresh,
populated-no-attempt, exact-rerun, five-field prerequisite-conflict,
same-ID/content-conflict, replacement-object, and injected-rollback migration
matrix pass on main. The migration matrix must also reject legacy failed and
sealed M5 runtime/results with zero attempts, preserve attempt-family error
precedence, permit a bare terminal base epoch, and rerun exactly after valid
post-016 terminalization.

R0 exited at main commit
`61875894172c8e0b36866d6b215ecab7a57b76ec`. On those exact committed bytes,
migration 016 passed 200/200 live tests, migration 015 passed 26/26, the
broader PostgreSQL runtime suite passed 292/292, and the static/type/compile
gates passed. At that checkpoint, R1 remained blocked until the literal
accepted migration-016 identity and exact path manifest were committed in
`docs/workstreams/m5_runtime_implementation/D24_R1_ACTIVATION.md`.

### Wave R1 -- requirement and typed-direct persistence

R1 was activated only from the commit containing
`docs/workstreams/m5_runtime_implementation/D24_R1_ACTIVATION.md`; its exact
accepted parent is `15415edadb70201ef26f1e5e50b7fe0d63279210`. That note freezes
the literal accepted migration-016 tuple, exact branch/worktree names, eight
R1-P paths, nine R1-D paths, shared operational pins and focused gates. No
broad directory grant is implied. Each lane records the manifest commit as its
exact branch base before editing.

The R1 manifests required both serial orders of takeover/result/failure races,
exact replay, dispatch-versus-evidence ambiguity, post-terminal isolation,
work/timing point maintenance, and frozen public M4-v1 regression. Their
integrated results are recorded below. Cross-lane wrappers remained
coordinator work after both lane commits.

Accepted M5-D24-C3 resumed R1-P without changing lane paths. An
already-replaced requirement attempt archives preterminal when output wins
before cancellation, while cancellation-first conflicts with zero writes while
the event remains nonterminal. R1-P owned the two preterminal orders and
fixture-backed postterminal persistence-shape evidence. R2 owns production
seal/failure and the end-to-end postterminal continuation. No R1 lane may edit
migration 016 or invent a terminal preterminal artifact to bypass that
accepted closure.

Accepted M5-D24-C4 changed no path ownership. R1-P owned the requirement source,
nested tests, and clearly labelled SQL-only terminal fixture needed to prove
that `retryable_failed` and every terminal successor (`completed_active`,
`completed_inactive`, `terminal_failed`, or `cancelled`) cause zero-write
rejection while the event is nonterminal. R1-P also owned checked-reacquisition
coverage from `retryable_failed` back to `running`, and SQL-only fixture proof
that persistence rejects the output while `retryable_failed` and inserts the
exact five rows only after test-local resolution to one of the four accepted
terminal states. It also owned proof that a late-only requirement return
validates the complete resupplied discovery/verifier DTO, explicit discovery
exhaustion and nested context, and the verifier pair-input immutable core, but
inserts no normal semantic artifact rows. Snapshot-exhaustion evidence is true
and revalidated for `snapshot_exhausted`; the `budget_filled` boolean is non-
material and not replay-bound. R2 retains production seal/failure composition,
including proof that production terminalization resolves `retryable_failed`,
and end-to-end continuation.

R1-P subsequently resumed under the accepted correction and integrated at
`56dd2d4`; R1-D had integrated at `1838316`. R2a failure terminalization and
R1-C shared compatibility then integrated at `6f1ae89` and `f5902ff`.
These completed manifests are historical evidence, not active ownership. No
lane may edit migration 016/017 or a public contract/digest under an old R1
grant.

### Wave R2 -- application composition

Only after R1 integration may the coordinator or one newly manifested lane
edit `application.py`, fake ports/history, typed open/resume/failure/seal
composition, or shared integration tests. R2 must exercise reconnect at every
nonterminal cutoff and preserve one timing anchor per outer transaction.

The historical R2b activation preflight exposed the successful-return gap
corrected by accepted M5-D24-C5. The separately manifested C5 contract micro-
lane integrated its generic two-shape `M5EventRunResult` validator at
`69a00e4`. After accepted C6 integrated at `ab56178`, the coordinator freshly
reactivated the five-path R2b lane at `62bfb03`. Those grants are now closed
and provide no current edit authority.

During application composition, R2b proved three additional reachable active-
invocation work-loss origins outside C5: later requirement acquisition with
checked `TERMINAL/EPOCH_FAILED`, checked same-reason failure-mutator replay
after terminal-attempt work, and checked fake-only seal-mutator replay after
current work. Accepted M5-D24-C6 admits exactly those origins to
C5's existing active-cutoff `REPLAYED` shape. It preserves ordinary entry/open
replay as terminal-projected and zero-work, requires complete origin and
canonical ordinary-replay validation, and requires the active envelope to
validate before timing-only terminal telemetry is appended.

C6 changes no validator, marker, persistence, migration, digest,
schema, event total, telemetry-work shape, or public M4 byte. The acquisition
route must prove the exact job/execution-bound total lease, valid terminal
identity, reason `EPOCH_FAILED`, and canonical same-event/payload/epoch
`FAILED` result without guessing a run failure reason. The failure route must
retain the exact requested failure reason and may not blindly authorize the
generic cursor-local direct failure path. The seal route remains fake-only and
preserves its exact durable terminal branch. Accepted C1--C5 otherwise remain
unchanged.

Two independent exact-byte audits accepted C6 with no unresolved P0/P1. The
subsequent R2b tranche integrated on main at `bfeef3f`; its 87/87 focused and
237/237 pure gates are bounded requirement-application/fake-seal orchestration
evidence, with no live database. M5.0-24 remains contract-`PASS` /
implementation-`PENDING`.

The historical R2b lane contained exactly five paths:

1. `src/groundloop/m5/runtime/application.py`;
2. `tests/m5/runtime/fake_ports.py`;
3. `tests/m5/runtime/test_d24_application_composition.py` (new);
4. `docs/workstreams/m5_runtime_implementation/D24_R2B_APPLICATION_FAILURE_HANDOFF.md`
   (new); and
5. `tests/m5/runtime/test_typed_history.py`, limited to the already-authorized
   single `CANCELLED` to `TERMINAL_FAILED` expectation correction.

The integrated R2b gate preserves all C5 discovery/verifier receipt/hash tests and
covers the three C6 races with exact zero/nonzero one-add call work, fresh/
resumed held receipts, canonical ordinary replay, active-envelope validation
before telemetry, unchanged event totals/logical identity, one timing-only
telemetry append, ordinary reconnect zero work, and no redispatch. It rejects
wrong job/execution/disposition/reason/outcome/epoch, same-reason failure
mismatch, malformed replay, generic direct-failure projection, and every
unlisted origin.

The coordinator then activated the exact five-new-path R2c tranche at
`abe22e6d1cf844ca7bc63697f089cc77fcb4397f` and integrated it on main at
`0e0ff4385b4f5e5145788f59cc55411b39d659c1`. Its historical manifest is:

1. `src/groundloop/m5/runtime/postgres_application.py`;
2. `tests/m5/postgres_runtime/d24_application/conftest.py`;
3. `tests/m5/postgres_runtime/d24_application/test_group_requirement_composition.py`;
4. `tests/m5/postgres_runtime/d24_application/test_group_requirement_races.py`;
   and
5. `docs/workstreams/m5_runtime_implementation/D24_R2C_GROUP_REQUIREMENT_BRIDGE_HANDOFF.md`.

The original activated `/tmp` worktree disappeared during an environment
restart. At the user's explicit direction, the same branch at the unchanged
activation base was recreated at the persistent
`/home/kassym/Desktop/groundloop-worktrees/m5-d24-r2c-group-requirement-bridge`
path before recovery and final validation. This relocation changed no Git
base, owned path, contract, or evidence authority.

The exact integrated R2c bytes passed the 81/81 live PostgreSQL gate and all
recorded static/type/compile/hash checks; the immutable integration audit
returned `GO`. This is scoped `PASS` evidence for the group register/replace/
retire requirement path through the concrete store up to its explicit
fail-closed pre-seal boundary. It is not typed-direct outer settlement,
cursor-local direct failure, production seal/publication, production provider,
M5-D25, or whole-M5.4 evidence. M5.0-24 remains contract-`PASS` /
implementation-`PENDING`, and every M5.4 row is unchanged.

The coordinator then activated the exact four-path R2d tranche at
`b5c4c06ee81f79638771831d80b8ca21df58e0bd` and integrated it on main at
`c892cc8a547a9c0248ad735e11270daa0e1acf4e`. Its historical manifest is:

1. `src/groundloop/m5/runtime/application.py`;
2. `tests/m5/runtime/fake_ports.py`;
3. `tests/m5/runtime/test_d24_application_composition.py`; and
4. `docs/workstreams/m5_runtime_implementation/D24_R2D_TYPED_DIRECT_APPLICATION_OUTCOME_HANDOFF.md`.

The frozen R2d candidate evidence records the 101/101 focused selection,
189/189 complete composition file, 339/339 complete pure M5 runtime gate,
unchanged 81/81 R2c application, 163/163 D24 requirement, 589/589 complete
PostgreSQL runtime, 48/48 public-M4 route/direct, and 14/14 DTO/legacy/M4
selections. The recorded static/type/compile/hash gates passed and the immutable
integration audit returned `GO`.

On exact integrated main, the 101/101 focused, 339/339 pure-runtime, 14/14 non-
database M4/legacy, and applicable static/hash gates passed again. The 189/189
composition-file and four live compatibility counts remain frozen-candidate
evidence.

This is scoped `PASS` evidence for the pure checked application outcome of a
selected successful typed-direct discovery/verifier outer receipt that loses
the active terminal cutoff, plus ordinary reconnect preservation. It is not a
production PostgreSQL typed-direct application/outer-settlement bridge,
cursor-local direct failure, seal/publication, provider, M5-D25, or whole-M5.4
result. M5.0-24 remains contract-`PASS` / implementation-`PENDING`, and every
M5.4 row is unchanged.

Integration closed the R2b, R2c, and exact four-path R2d grants. No D24
implementation lane is currently active. A retained historical worktree grants
no edit authority. The production PostgreSQL typed-direct application/outer-
settlement bridge, cursor-local direct failure, production seal/publication,
production discovery/verifier/measurement adapters, and remaining end-to-end
evidence each require a fresh committed path-exclusive manifest from the then-
current integration barrier; none may inherit R2b, R2c, or R2d ownership.

No D24 wave owns the untracked persisted-matching draft. M5-D25 and migration
017 remain blocked until migration 016 is accepted, all five 016 ledger values
are literal, and a new independent GO plus path manifest is committed.
