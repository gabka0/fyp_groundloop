# GroundLoop M5 Implementation Plan

Status: M5.0 contract accepted through M5-D24-C6 and M5.1--M5.3 complete;
M5.4 is partially complete, M5.0-24 is contract-`PASS` /
implementation-`PENDING`, and the integrated R2c group/requirement PostgreSQL
pre-seal bridge is historical with no current edit ownership

Date: 2026-08-02; M5-D24 execution and R2c integration current through
2026-08-17

Authority: `docs/m5_design_freeze.md` governs. M5-D24 recovery/accounting work
also obeys
`docs/workstreams/m5_runtime_contract/RECOVERY_WORK_AMENDMENT.md`.
Implementation stops on any conflict with those contracts rather than silently
choosing new semantics.

Accepted M5-D24-C3 and M5-D24-C4 are authoritative. C4 governs the
retryable/terminal-successor expired-output and requirement late-artifact
closure; its implementation evidence remains pending.

Accepted M5-D24-C5 remains authoritative. Its generic replay-shape validator
is integrated at `69a00e4`, and accepted C1--C4 behavior is unchanged.
Accepted M5-D24-C6 closes three additional reachable active-cutoff origins and
restores M5.0-24 to contract-`PASS` / implementation-`PENDING`. The bounded
R2b pure requirement-application/fake-seal orchestration tranche is integrated
at `bfeef3f`; its five-path activation is closed and grants no current edit
ownership.

The bounded R2c group/requirement PostgreSQL pre-seal bridge is integrated at
`0e0ff4385b4f5e5145788f59cc55411b39d659c1`. Its scoped tranche gate is
`PASS`; M5.0-24 remains contract-`PASS` / implementation-`PENDING`, every M5.4
row is unchanged, and its five-path grant is closed with no current edit
ownership.

## 1. Outcome

M5 completes only when GroundLoop can register and version bounded evidence
groups, store requirement-subject observations, maintain exact distinct-
representative completeness incrementally, publish combined claim/answer
state through PostgreSQL, integrate requirement jobs into the dynamic path,
and execute a controlled evidence-group evaluation with explicit limitations.

M5 is not complete merely because group dataclasses or a matching function
exist. It requires all deterministic, persistence, runtime, evaluation, replay,
and documentation gates below.

## 2. Preconditions

Before M5 code:

1. live PostgreSQL validator passes;
2. full M1--M4 suite passes with only recorded opt-in skips;
3. M5.0 theory, schema/runtime, and data audits are resolved;
4. `m5_design_freeze.md`, this plan, the ownership plan, and acceptance matrix
   agree;
5. user presentation changes remain outside M5 ownership;
6. no open M4 structural epoch exists before migration validation.

Recorded M5 starting baseline on 2026-08-02:

```text
PostgreSQL 16.14 / pgvector 0.8.5 validator: passed
full pytest: 680 passed, 7 intentional opt-in skips
Ruff, strict mypy, compileall, diff-check: passed
```

These counts are a traceable preflight copied from the M4 closure and the live
M5.0 rerun, not the final M5 evidence bundle. M5.6 records exact commands,
working-tree/commit identity, UTC timestamp, DSN mode, versions, durations,
skips, and output hashes.

## 3. M5.0 -- contract, theory, schema, and data freeze

Deliverables:

- `docs/m5_design_freeze.md`
- `docs/m5_implementation_plan.md`
- `docs/m5_multiagent_execution_plan.md`
- `docs/m5_acceptance_matrix.md`
- M5 decisions in `docs/decision_log.md`
- corrected M5 routing in `AGENTS.md`, `README.md`, and `docs/roadmap.md`
- current primary-source additions in `docs/literature_matrix.md`

Required adversarial resolutions:

- global union cardinality is replaced by Hall/perfect-matching semantics;
- every requirement/hash edge crossing, not only requirement satisfaction,
  updates matching state;
- stable family and immutable group/requirement version identities are total;
- requirement witnesses are a derived relation;
- typed subject referential integrity is explicit;
- direct and group provenance remain separate;
- v1 M4 digests/replays are preserved; typed M5 structural/runtime/certificate
  digests have exact field/null/order/F64 recipes and golden vectors;
- forward/reverse scope nullability, root kind, and requirement-pair ownership
  are total rather than optional-field conventions;
- immutable certificate artifacts use epoch-revision bindings, and a policy
  version with zero decision flips still rebinds every applicable v2 state;
- shared typed observations coexist with mechanically claim-filtered M4
  readers and byte-identical v1 semantics;
- pending structure uses failure-safe staged/effective/published overlays;
- WiCE is explicitly retrospective, pinned to the official revision, and its
  content-only evidence-set mapping, original-ordinal duplicate projection,
  requirement-only subject route, and positive eligibility rule are fixed;
- absence of fresh adjudication downgrades semantic claims rather than being
  hidden.

Exit gate: the M5.0 contract column for every row in Section 1 of the M5
acceptance matrix is `PASS`, the later implementation column remains honestly
`PENDING`, and no audit has an unresolved P0/P1.

Historical base result: **passed on 2026-08-02** after three independent final
audit GOs. Later accepted amendments through C6 preserve that historical
result. C6 restores M5.0-24's current contract-completeness gate to `PASS`; it
does not change completed M5.1--M5.3 evidence or supply implementation proof.

## 4. M5.1 -- pure reference semantics and structural events

### 4.1 Domain records

Add immutable records for:

- immutable evidence-group families, group versions, and source/provenance
  kind;
- evidence requirement versions;
- requirement state;
- group state and matching certificate;
- combined claim support fields;
- versioned/tagged claim certificate;
- group register, replace, retire, and requirement-observation events.

Validation includes:

- one to eight requirements;
- dense `0..r-1` ordinals and unique normalized requirement texts;
- the exact 29-code-point normalization-v1 predicate and shared Python/SQL
  golden vectors;
- valid constructor provenance by source kind;
- semantic-structure and record-payload hash integrity;
- same-family adjacent successor-lineage and no-overlap integrity;
- canonical `verify_requirement_v1` witness task type; other task types remain
  stored/current under their own currency keys but inert for witnesses.

### 4.2 Repository history and lifecycle

Extend the historical repository with:

- group-family/current-version index;
- group validity sidecars with requirement activity derived from the parent;
- group-by-claim and requirement-by-group indexes;
- requirement-to-owner-claim lookup;
- typed requirement-observation registration;
- observation-currency validity intervals and exact as-of queries;
- append-only event history and atomic copy-and-commit behavior.

Late observations against inactive requirements are archived ineligible for
currency and preserve the prior holder. Whole-group replacement and its
runtime scope declarations are one atomic event.

### 4.3 Independent Python oracle

The Python oracle derives current requirement witness edges from base records
and uses exhaustive/backtracking assignment with an explicit unmatched branch
over at most eight requirements.
It must not import the Hall-mask kernel or affected-group baseline.

It returns complete requirement/group states and combined claim/answer states,
and independently validates the correctness of supplied persisted certificates
at their bound snapshot. Its diagnostic maximum assignment need not equal the
stateful incremental certificate identity.

M5.1 exit gate:

- all M5 reference/event tests pass;
- Hall and lifecycle counterexamples pass;
- rejected events leave complete repository snapshots byte-equivalent;
- all pre-M5 unit/integration tests remain unchanged and pass.

## 5. M5.2 -- incremental Hall-mask maintenance

### 5.1 Simple baseline kernel

Implement an affected-group full matching kernel using deterministic
augmenting paths. This is the simpler algorithmic baseline and a local oracle
for Hall-kernel unit tests, but not one of the independent full-state oracles.

### 5.2 Optimized Hall-mask kernel

Maintain per group:

- requirement/hash edge multiplicities;
- hash adjacency masks and mask buckets;
- Hall neighbor counts for all requirement subsets;
- deficiency and exact maximum matching size;
- current completeness and valid matching certificate.

Coalesce a microtransaction by group/hash so one hash mask transitions at
most once. Update at most `2^r-1` Hall entries per transition. Retain valid
stateful certificates; repair selected provenance on positive-to-positive
multiplicity changes; rebuild on completeness gain or selected-edge loss;
remove on completeness loss. A selected group-certificate change republishes
the claim certificate even with constant statuses.

### 5.3 Group overlay engine

Compose the existing direct-witness incremental engine with the M5 group
overlay. Preserve direct fields and add complete group IDs/count. Update only
affected group, claim, and answer keys. Expose signed work counters for:

- contribution changes;
- ordered policy-range probes, including zero-candidate probes;
- edge and hash-mask crossings;
- Hall subset entries touched;
- certificate rebuild/repair work;
- groups, claims, and answers touched;
- status changes.

### 5.4 Differential and complexity gates

Required:

- the frozen `seed=20260802` mixed stream with at least 100,000 committed
  events and hash-bound event weights;
- every simple graph with `r<=4,H<=4`, every mask pair through `r=4`, and
  focused multiplicities `0,1,2`;
- observation supersession, policy, chunk, and group-lifecycle changes;
- equality after every event against the independent Python oracle;
- Hall kernel equality with affected-group matching on every affected group;
- failure injection before publication;
- counters proving unrelated groups are untouched;
- empirical comparison across group size, overlap, degree, duplicates, and
  version churn;
- executable guards for every M5-T2 term, including zero-candidate policy
  probes, ordered-index work, and logical output work.

M5.2 exit gate: zero state/certificate mismatches over the frozen randomized
gate and no pre-M5 regression.

## 6. M5.3 -- PostgreSQL schema, persistence, and SQL oracle

### 6.1 Migration

Add `014_m5_evidence_groups.sql` with:

- group-family, group-version, and requirement-version relations;
- group-only published validity sidecars (requirement activity derives from
  the parent group), epoch-local `REPLACE | RETIRE` overlays, a durable
  one-per-family retirement fact, and temporal GiST exclusions for both
  family-version and active semantic-duplicate overlap;
- the frozen ordered `ACCESS EXCLUSIVE` table-lock protocol, concurrent-writer
  serialization including `groundloop_claim` and the legacy working-observation
  delta, open-epoch rejection, and transactional migration;
- `pgcrypto` and `btree_gist` before any dependent digest or temporal object;
- one atomic, hash-ledgered `m5-core-schema-bundle-v1` containing the ordered
  pair `migrations/014_m5_evidence_groups.sql` and
  `sql/m5/full_recompute_oracle.sql`, plus an explicit install/upgrade
  entrypoint for populated 013 databases rather than only fresh sorted-file
  initialization;
- a one-time M5 activation/bootstrap and separate M5 publication head; after
  activation new v1 structural events are rejected before durable open and
  every typed seal advances M4/M5 heads atomically;
- STAGED/PUBLISHED/FAILED group lifecycle, epoch-local deactivation overlays,
  effective working views, and seal/fail promotion rules;
- deferrable dense-cardinality, lineage, semantic-duplicate, subtype, and
  interval constraints;
- typed semantic-subject registry, existing/future claim/requirement
  maintenance, and backfill;
- composite subject foreign keys plus deferred subtype validation;
- database-enforced normalization v1 plus stored-text hash checks, with direct
  SQL duplicate/Unicode/whitespace adversaries;
- immutable/backfilled `eligible_for_currency`, enforced on every shared,
  legacy-working, and M5-working currency surface;
- typed M5 working-currency history with half-open revision intervals,
  explicit tombstones, exact as-of resolution, and final-holder promotion at
  seal; only `valid_to_revision: NULL -> closing_revision` may mutate once,
  closed rows are immutable, terminal epochs reject writes, and the old
  one-row-per-key M4 delta is not an M5 historical oracle;
- M5 working/materialized/published state, immutable certificate artifacts,
  nullable support-kind fields, and exact epoch-revision bindings;
- current/ownership/edge indexes;
- v2 certificate/digest metadata;
- no reinterpretation of M4 v1 identities, plus required claim-only filters on
  every M4 reader of shared typed observation/currency relations.

A repo-wide static inventory covers every source/test/script reader and writer
for every altered/shared table. All INSERTs use explicit columns; claim-only
M4 bootstrap/reconnect/reverse-dependency/withdrawal/publication reads filter
`subject_kind='claim'`. Migration tests exercise them against populated v1
history plus active requirement currency. Existing M4 rows remain v1 rather
than receiving a new interpretation.

### 6.2 Independent SQL oracle

Add a SQL oracle that:

1. derives current policy labels independently with SQL CASE;
2. derives active requirement/hash edges from base tables;
3. enumerates all requirement subsets and counts distinct base-edge neighbors
   without reading Hall-mask materialization;
4. computes exact Hall deficiency and maximum matching size/completeness;
5. runs a separate unmatched-branch recursive assignment audit using `UNION`
   deduplication over `(group,next_ordinal,used_hash_mask)` and an unmatched
   transition only when `H<=16`, `E<=128`, and the frozen combinatorial
   preflight bound is at most 100,000 states; otherwise it records
   `ASSIGNMENT_AUDIT_CAP_EXCEEDED` rather than PASS;
6. recomputes combined claim and answer states;
7. validates epoch/policy-bound group and v2 claim certificates;
8. exposes mismatch views for every M5 state level.

The subset SQL oracle is exponential only in the frozen left bound of eight;
the assignment audit has an explicit right-side/edge cap. Neither reads
Hall-mask materialized state.

### 6.3 Snapshot and live validation

Add an M5 snapshot adapter that preserves M2 loading order, inserts group/
requirement subjects before requirement observations, and reads typed oracle
states. Validate:

```text
incremental == Python oracle == SQL oracle
```

after insert, delete, replace, policy change, group registration/replacement/
retirement, supersession, and replay.

M5.3 exit gate:

- fresh 014 installation and live populated 013-to-014 upgrade/rerun/rollback
  under a concurrent writer pass;
- failed staged group registration/replacement preserves prior published truth;
- on a schema-upgraded but never-activated compatibility fixture, the original
  v1 restart/delete/reconnect/publication/replay route claim-filters coexisting
  requirement currency and preserves frozen v1 bytes;
- on an activated database, legacy read/replay audit and claim-filtered direct
  components remain usable, while every new legacy v1 mutation open/resume is
  rejected by the activation barrier;
- zero requirement/group/claim/answer mismatches;
- zero invalid certificates;
- rollback and exact/conflicting replay pass;
- full repository PostgreSQL suite passes.

## 7. M5.4 -- dynamic requirement-job integration

### 7.1 Typed v2 runtime contracts

Introduce parallel M5 contracts rather than changing the meaning of M4 v1:

- `SemanticPairKey(subject_kind, subject_id, chunk_version_id)`;
- owner-claim projection for requirement subjects;
- subject-registry snapshot identity;
- active requirement ownership snapshot identity;
- requirement-specific role-template and candidate-policy identity;
- exact typed v2 registry/chunk snapshot, semantic-pair, scope, logical-job,
  payload, sorted-child-set, completion, and certificate digest recipes;
- a frozen direction-to-target/root-kind table, requirement-only verifier-pair
  ownership, job-kind/null/result/reason shapes, and golden vectors.

### 7.2 Dynamic path

Integrate:

- requirement vector and lexical retrieval;
- forward scopes for new requirement versions and reverse scopes for new
  chunks against frozen snapshots;
- deterministic fusion and fixed requirement budget;
- requirement verifier inputs and immutable score observations;
- exact withdrawal by changed chunk;
- frontier key `(requirement_version_id,candidate_policy_id)`, empty-scope,
  retry/failure, cancellation, and fallback accounting;
- late inactive requirement/group/failed-epoch completion without currency
  supersession;
- owner-projected claim PENDING and required-claim-only answer PENDING;
- working state, sparse publication, and sealed reconnect replay.

Reverse discovery roots lazily pend every owner in their frozen requirement
registry until atomic child-set closure, then only owners of open children.
Cancellation decrements work once; a late attempt for a terminal CANCELLED job
is archived without transitioning the job, while `COMPLETED_INACTIVE` is
reserved for a still-RUNNING job. The single-open-epoch rule forbids group
lifecycle during semantic PENDING.

Requirement REFUTE/NEUTRAL creates no parent refutation. Model work stays
outside the publication transaction.

Measured sealing executes local persisted invariants only. Python/SQL full
oracles run immediately afterward in audit mode and are excluded from the
incremental latency measurement.

### 7.3 Runtime acceptance history

Run a controlled insert/delete/replace history containing:

- alternative-witness repair;
- a matching-only loss with no requirement zero-crossing;
- final assignment loss;
- surviving alternative group;
- direct support surviving group loss;
- group support conflicting with direct refutation;
- a stale worker from a terminal failed/cancelled prior epoch returning after a
  later group replacement (never two concurrent open structural epochs);
- optional-owner PENDING, empty search, scope retirement, and failed-epoch
  late completion;
- certificate/full-state publication with no public status change;
- exact reconnect replay with zero model calls;
- out-of-band Python/incremental/SQL equality after every measured seal.

### 7.4 M5-D24 recovery and accounting barrier

Before full dynamic composition, implement in order:

1. immutable operational DTOs/digests plus total acquisition, terminal, direct
   late-return, work, timing, coverage, and ambiguity-bound golden tests;
2. exact migration 016 install/rerun/conflict/rollback/no-guess upgrade with
   all five migration-015 prerequisite ledger fields checked literally;
3. recoverable M5 and typed-direct database-clock leases, dense takeover,
   dispatch/evidence records, point work/timing accumulators, and checked
   postcommit timing anchors;
4. retryable/terminal settlement plus expired, inactive, and post-terminal
   audit paths with exact replay and terminal cutoff isolation; and
5. application reconnect and both-order race/crash tests with unchanged public
   M4-v1 behavior.

Accepted C3 delays the cancellation-first expired output until the event is
terminal. Accepted M5-D24-C4 generalizes that same truthful boundary to a
dense successor in `retryable_failed`, `completed_active`,
`completed_inactive`, `terminal_failed`, or `cancelled` while the event remains
nonterminal. The now-integrated R1-P tranche was required to prove zero writes
for all five states, checked
reacquisition from `retryable_failed` back to the unchanged
`running -> running` preterminal path, and the exact five-row archive only
after its SQL-only fixture resolves the test-local job from the rejected
retryable state to one of migration 016's four terminal states. Audit-only
requirement calls must fully validate and self-digest the supplied discovery/
verifier DTO on first call and replay, and must not populate normal semantic
artifact tables. R2 owns proof that production
terminalization resolves `retryable_failed` and owns the end-to-end
continuation. Discovery late calls must also resupply explicit snapshot-
exhaustion evidence and validate every nested epoch/policy, scope and snapshot
binding. The boolean must be true for `snapshot_exhausted`; it is non-material
and not replay-bound for `budget_filled`. Verifier late calls must validate the
pair-input immutable core equivalently to its normal SQL trigger. No
migration-016/017, public M4 contract, digest, or path-manifest change is
authorized; only the internal M5 checked-persistence signature may carry the
missing explicit evidence.

R1-D, R1-P, R2a, and R1-C are integrated on main at `1838316`, `56dd2d4`,
`6f1ae89`, and `f5902ff`, respectively. These are bounded direct persistence,
requirement persistence, failure-terminalization, and shared-compatibility
tranche results. They preceded, but did not themselves close, R2 application
composition or M5.0-24.

R2b preflight first exposed the successful-return envelope defect corrected by
accepted C5. The three-path C5 contract micro-lane integrated at `69a00e4`.
After accepted C6 integrated at `ab56178`, the coordinator freshly reactivated
the five-path R2b lane at `62bfb03`. Those facts are historical evidence, not
current ownership.

During R2b composition, three more reachable routes were found to return an
ordinary canonical replay after the same invocation had already opened or
resumed nonterminal and accumulated exact work: a later requirement
acquisition with checked `TERMINAL/EPOCH_FAILED`, a checked same-reason
failure-mutator replay after terminal-attempt work, and a checked fake-only
seal-mutator replay after current work. Accepted M5-D24-C6 reuses
the existing C5 active-cutoff `REPLAYED` shape for exactly those origins. The
application must validate the origin and canonical ordinary replay, construct
and validate the active envelope using the held nonterminal receipt and exact
zero/nonzero accumulator, and only then append timing-only terminal telemetry.
Ordinary entry/open replay remains terminal-projected and zero-work.

C6 makes no marker, DTO/validator, persistence, telemetry-work, schema,
migration, digest, or public-M4 change. The acquisition route requires the
exact job/execution-bound total lease, a valid terminal identity and reason
`EPOCH_FAILED`, plus a same-event/payload/epoch canonical `FAILED` replay; it
cannot invent a run failure reason. The failure-mutator replay must be failed
with the exact requested reason and may not grant cursor-local direct failure
authority. The seal route remains fake-only and preserves its exact durable
sealed/failed branch.

Two independent exact-byte audits accepted C6 with no unresolved P0/P1. The
subsequent exact five-path R2b tranche integrated at `bfeef3f`. Its 87/87
focused composition suite and 237/237 full pure M5 runtime gate provide scoped
requirement-application and fake-seal orchestration evidence; no live database
was used. M5.0-24 therefore remains contract-`PASS` /
implementation-`PENDING`.

The subsequent exact five-new-path R2c tranche integrated at `0e0ff43` from
activation base `abe22e6`. On the exact integrated bytes, its 81-case live
PostgreSQL suite and static/type/compile/hash gates passed, and the immutable
integration audit returned `GO`. The tranche proves only the group lifecycle
path through concrete requirement persistence: exact register/replace/retire,
durable `BLOCKED` and `FAILED`, applicable C5/C6 cutoffs, reconnect/takeover,
work/timing coverage, and a zero-write fail-closed pre-seal boundary.

The R2b and R2c activations, branches/worktrees, paths, and handoffs are
historical evidence and own nothing after integration. Typed-direct outer-
settlement application composition, cursor-local direct failure, production
seal and combined publication, production discovery/verifier/measurement
adapters, and remaining end-to-end gates require later separate path-exclusive
manifests. No future lane may inherit either closed grant.

The contracts and migration lanes may run in parallel only under the explicit
path manifest in `docs/m5_multiagent_execution_plan.md`. Persistence/direct
composition starts only after both are integrated. A green D24 lane does not
close M5.4: durable persisted matching remains a separate numbered amendment
and migration-017 barrier, currently non-authoritative.

M5.4 exit gate: deterministic fake-port path passes first, then a bounded
maintained PostgreSQL history and frozen-model diagnostic pass with complete
provenance. Frozen-model output is not treated as semantic gold.

## 8. M5.5 -- controlled data and evaluation

### 8.1 Pinned WiCE adapter

Pin the official source or a provenance-audited mirror by immutable revision
and file hash. Parse parent claims, subclaims, human evidence labels, and
supporting-sentence sets. Each sentence set becomes one atomic
`wice-evidence-unit-v1` value with adapter-derived source provenance outside a
content-only canonical JSON chunk, exact member-count/digest encoding, model
render, identical-content coalescing, original zero-based annotation ordinal,
least-ordinal projection selection, and overlength rejection. The primary
cohort requires a source-supported parent, 1..8 final source-supported
subclaims, and at least one valid unit per requirement. Emit:

- a source manifest and license record;
- split/parent/subclaim counts;
- mapping-rejection reasons;
- requirement, edge, overlap, and assignment distributions;
- exact SDR applicability counts and separate
  `source_semantic_label`/`groundloop_sdr_complete`;
- immutable `ControlledObservationProjection` rows linking deterministic score
  inputs to source annotations without overloading model IDs, always through a
  REQUIREMENT-subject `ObserveRequirementEvent` and never direct claim support;
- deterministic controlled event streams.

WiCE results are labelled retrospective because M3 already consumed WiCE.

### 8.2 Baselines and reports

Run identical event histories through every baseline applicable to the cohort:

1. source invalidation;
2. direct-citation any-loss invalidation;
3. direct-witness-only state;
4. per-requirement conjunction without distinctness;
5. Hall/SDR GroundLoop;
6. affected-group full matching;
7. all-group/Python/SQL recomputation.

Baseline 3 is explicitly UNAVAILABLE on WiCE unless independent whole-claim
evidence units exist; it runs on controlled/adjudicated cohorts that provide
such units. Baseline 4 and source truth include the same independently
annotated direct-support disjunct as GroundLoop when such units exist, so the
ablation changes only distinctness. Baselines 6/7 are same-semantics systems
comparators, while 1--4 have the distinct policy/ablation roles frozen in the
design.

Report raw counts, frozen false-invalidation/retention denominators, paired
claim-history/source-cluster bootstrap intervals,
false invalidation, false retention, status agreement, work amplification,
latency, state bytes, and certificate sizes. Stratify by requirement count,
degree, overlap, SDR pass/fail, alternative assignment, and provenance.
Define `y_source` as independent whole-claim direct source support OR the
dynamic source-witness conjunction with reuse, and `y_hat` as the tested
support predicate; GroundLoop CONFLICTED has `y_hat=1` because its support
predicate remains true. The WiCE primary cohort has no direct disjunct.

Source/human labels, controlled score projections, exact SDR states, and
frozen-model diagnostics occupy separate outputs.
Pure invalidation baselines remain honest zero-call baselines.

### 8.3 Semantic claim boundary

If a fresh blinded two-annotator/adjudicated cohort is not available, M5.5
closes as controlled/retrospective evaluation. No real-world semantic-
validation or population claim is permitted. This limitation does not block
the roadmap's gold-or-controlled M5 implementation gate; it remains explicit
M6 scientific debt.

## 9. M5.6 -- closure audit

Required closure actions:

- full M5 acceptance matrix;
- focused and full pytest;
- Ruff, strict mypy, compileall, `pip check`, and `git diff --check`;
- live PostgreSQL validator and migration/backfill test;
- 100,000-event randomized differential gate;
- crash/reconnect/replay matrix;
- algorithmic counter and complexity report;
- deterministic evaluation reproduction and artifact hashes;
- direct-only v1 digest/replay regression;
- README, AGENTS, architecture, roadmap, decision log, evaluation protocol,
  literature matrix, and M5 implementation-status updates;
- explicit separation of implemented, validated, controlled, model-relative,
  and independently adjudicated results.

The closure manifest records every exact command, Git commit plus dirty-tree
diff hash, UTC start/end time, DSN/local-container mode, PostgreSQL/pgvector
versions, random seeds/config hash, pass/fail/skip counts, durations, and
artifact SHA-256 values. Copied milestone summaries are not closure evidence.

M5 closes only if no required acceptance row is unresolved. Negative empirical
results are retained and reported; they do not get replaced by tuning after
the gate.

## 10. Stop conditions

Stop implementation and reopen M5.0 if work would require:

- more than eight active requirements per group;
- writable witness links or untyped subject IDs;
- recursive groups or group refutation;
- changing old M4 v1 event/job/digest meaning;
- importing Hall-mask code into either full oracle;
- using WiCE test data for tuning;
- calling model output gold;
- hiding a mismatch, skip, timeout, or absent human evaluation;
- claiming named-system superiority without a matched experiment.
