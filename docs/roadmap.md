# GroundLoop Roadmap

Only move to the next milestone after satisfying the current exit criteria.

## M0 — Repository and Research Freeze

Status: complete

Deliverables:

- Read-first onboarding context.
- Research plan.
- Architecture.
- Evaluation protocol.
- Literature matrix.
- Decision log.
- Python and PostgreSQL scaffold.

Exit criterion: a new agent can explain the exact/empirical boundary and FYP
scope without consulting Dynagox.

## M0.5 — Technical Design Freeze

Status: complete (2026-07-17)

Design source: `docs/technical_design.md` (v0.2, frozen)
Review: `docs/claude_algorithm_design_review.md`
Superseded candidate: `docs/initial_technical_design.md` (audit only)

Delivered:

- Adversarial review with literature verification and self-check.
- Frozen v0.2 schema: observation currency/supersession, requirement-subject
  observations, versioned evidence groups, distinct-content counting.
- Frozen decision rule, policy classes, and policy-delta semantics.
- Simplified pending semantics (EvaluationState + confirmed_as_of_epoch).
- Scope table (CORE/TARGET/STRETCH/POST-FYP/DELETE) and slip plan.
- Frozen decisions D-1..D-18 recorded in the decision log; M1.1 later added
  D-19 and D-20 without reopening the M0.5 resolutions.
- Revised M1 prompt and plan matching v0.2.

## M1 — Deterministic Reference Semantics

Status: complete (2026-07-17). 43 tests pass; ruff and strict mypy clean.
Modules: `domain.py`, `errors.py`, `policy.py`, `repository.py`,
`reference.py`, `events.py`; tests in `tests/unit/` and
`tests/integration/test_vertical_slice.py`.

Prompt: `docs/first_implementation_prompt.md` (revised for v0.2)

Deliverables:

- Versioned in-memory domain model with immutable score observations and a
  versioned decision policy (frozen tie rule v1).
- Currency/supersession rule: one current observation per
  (subject, chunk_version, task) key.
- Validity intervals for document and chunk versions; policy validity.
- Distinct-content (text_hash) support and refute counting.
- Reference recomputation of claim and answer state (full truth tables).
- Insert, delete, replace, policy-change, and observe events with
  idempotence and payload-conflict detection.
- Explicit `StatusDelta` output with event provenance.

Exit criteria:

- Replacing a supporting chunk changes claim and answer state in a
  deterministic test (Nimbus scenario).
- A policy-change event flips a label and emits a StatusDelta with zero new
  observations.
- A duplicate completion supersedes rather than double-counts.
- Two chunks with identical normalized text count as one witness.
- An observation completing for an inactive chunk is stored but never active.
- Old versions remain auditable; event replay is idempotent; payload
  conflicts are rejected.

## M1.1 — Reference Atomicity and Invariant Hardening

Status: complete (2026-07-17). 50 tests pass; ruff and strict mypy clean.

Deliverables:

- Copy-and-commit in-memory event transactions.
- Failed-event rollback covering revision, history, activity, indexes, current
  policy, currency, event registry, and status deltas.
- Batch-local uniqueness for claim and chunk stable identifiers.
- Normalized content-hash integrity and nonnegative chunk indexes.
- Explicit D-20 separation between M1 snapshot revisions and M2 semantic
  epochs/publication.
- Regression tests for failed observation, deletion, policy change, insert,
  and replacement paths.

Exit criteria:

- Rejected events leave the repository equal to its pre-event snapshot.
- Failed event identifiers are not consumed and may be retried with corrected
  payloads.
- A failure after staged old-version deactivation does not affect live state.
- Malformed batches and forged content hashes are rejected.
- All M1 and M1.1 validation gates pass.

## M2 — Relational IVM and Differential Testing

Status: complete (2026-07-18). All integrated tests pass against live
PostgreSQL 16.14 with pgvector 0.8.5; the Python reference, signed-delta engine,
and SQL oracle agree on the validated snapshots and randomized DB stream. See
`docs/m2_implementation_status.md`.

Deliverables:

- PostgreSQL schema and migrations.
- Direct and alternative witness counts.
- Incremental state transition implementation.
- Full relational recomputation query or reference function.
- Generated update streams.
- Differential comparison after every event.
- Separate semantic-epoch/publication coordinator oracle (D-20).
- Structured exact and heuristic baseline harness.
- Exact-flip policy-index prototype with an explicit output-sensitive bound,
  adversarial tests, and conservative literature positioning.

Exit criteria:

- Incremental and full results agree for randomized insert/delete/replace
  sequences.
- Zero-crossing and conflict tests pass.
- PostgreSQL schema, SQL oracle, and three-path fixture execute successfully on
  PostgreSQL 16.

## M3 — Static AI Pipeline

Status: complete (2026-07-18). The real pinned-model path, fine-tuned and
temperature-calibrated verifier, live PostgreSQL publication, exact replay,
and all 187 integrated tests pass. See `docs/m3_implementation_status.md`.

Deliverables:

- Passage chunking and embeddings.
- RAG answer with citations.
- Atomic claim extraction.
- Candidate evidence retrieval.
- Versioned support/refute/neutral verifier outputs.
- Model and prompt registry.
- Atomic staged publication and machine-readable run manifest.
- Real-model evaluation and explicit neural-quality limitations.

Exit criteria:

- One command ingests a corpus, asks a question, persists an answer, and shows
  claims with evidence and verifier scores.
- Repeated runs reuse versioned observations rather than silently overwriting
  them.
- The Python incremental engine, Python full recomputation, and SQL oracle
  agree over every published score observation.

## M4 — Selective Semantic Maintenance

Status: complete (2026-07-21), with strong conditional systems evidence and
preliminary/negative AI and end-to-end evidence. The implementation gates,
natural-history pilot, public AI diagnostic and sealed change-aware adaptation
experiment have all executed. See `docs/m4_implementation_status.md` and the
M4.10/M4.12/M4.13 result documents for the exact boundary.

Result routing:

- `docs/workstreams/m4_10_real_history_study/HANDOFF.md`
- `docs/workstreams/m4_12_public_ai_gate/README.md`
- `docs/m4_13_change_aware_verifier_plan.md`
- `docs/workstreams/m4_13_change_aware_verifier/RESULTS.md`

Deliverables:

- Exact reverse dependency handling for deletions (withdrawal path).
- Asymmetric admission discovery: reverse-ANN + lexical union at fixed top-L.
- Candidate frontier with mandatory retrieval fallback.
- Independent exhaustive pair/delta audit and policy-relative snapshot-refresh
  baseline.
- Dynamic job-DAG coordination with versioned working/published state.
- Optional learned impact retriever TARGET after CORE labels are frozen.

Implemented stages:

- **M4.1--M4.4:** PostgreSQL runtime/application/CLI, working/publication
  overlays, durable execution and model provenance, exact withdrawal/fresh
  fallback, event audit and crash atomicity.
- **M4.5--M4.6:** pinned-model application ports, real insertion/replay smoke
  and controlled history evaluation mechanics.
- **M4.7:** point/CAS measured runtime, signed evaluation counters,
  affected-key grounding patches and sparse publication. The original
  linear-looking whole-kernel formula is explicitly rejected and replaced by
  the corrected conditional bound in
  `docs/workstreams/m4_7_complexity_proof/README.md`.
- **M4.8:** pinned-model INSERT/DELETE/REPLACE history with three-oracle
  equality and zero-model-call reconnect replay.
- **M4.9 (controlled):** seven-treatment, same-event evaluation harness with
  deterministic output, explicit misses/timeouts and optional telemetry.
- **M4.11:** two-scale adversarial event histories that forbid full-state
  paths during successful measured kernels and audit afterwards.
- **M4.10:** executed naturally versioned real-history pilot using persisted
  exhaustive audits, pinned real models and actual telemetry. Non-exhaustive
  policies recovered `1/4` model-relative positive pairs and `0/1` answer-
  status effects on three unadjudicated histories; these are descriptive
  negative results, not population estimates.
- **M4.12:** executed page-disjoint VitaminC diagnostic of the frozen M3
  verifier: `0.5059` endpoint accuracy, `0.1992` joint endpoint correctness
  and `0.3281` detected label changes.
- **M4.13:** executed the preregistered eight-run adaptation, development
  selection, calibration and one-shot terminal evaluation. V2 cross-entropy
  mix was selected over V3 paired margin, but the terminal verdict was
  `NO_GO` because retention gate G8 failed. No adapted checkpoint is promoted;
  frozen M3 remains the default.

Exit criteria:

- Exact structured equality is demonstrated after measured insert, delete and
  replacement histories without running the full oracles inside the kernel.
  **Passed.**
- Exact replay, retry, conflict, failure, late-inactive completion and
  required/optional PENDING semantics survive adversarial history tests.
  **Passed.**
- Missed affected pairs/claims and status effects are inspectable under all
  seven treatments on identical event IDs. **Passed on controlled fixtures
  and the bounded real-history pilot.**
- Impact recall, actual verifier work and available latency/token telemetry are
  measured on pinned naturally versioned update streams. **Passed as an
  executable pilot; too small and unadjudicated for a general quality claim.**
- Every final performance or AI-quality statement names its workload and
  uncertainty, and does not promote controlled fixture values to scientific
  evidence. **Passed; final verdict is preliminary/negative.**

M4 implementation exactness and M4 scientific evidence remain separate. The
former passed. The latter did not establish neural completeness, useful call
savings on a representative population, objective truth or superiority to an
existing IVM system. A larger independently adjudicated natural-history study
is explicit pre-dissertation/M6 debt, not a hidden extension of M4.

## M5 — Bounded Evidence Groups

Status: active. The M5.0 contract freeze is complete through accepted
M5-D24-C3; M5.1 pure reference semantics completed on 2026-08-03 after an
independent high-confidence audit GO. M5.2 and M5.3 are complete. M5.4-01
passes; public activation and partial root/direct transitions have live
evidence but do not independently close M5.4-05. M5-D24's
recoverable-dispatch/accounting contract through C3 is frozen, and its R0
contracts plus migration 016 are accepted on main. C3 reconciles the
cancellation-first expired-output race with the immutable accepted schema.
R1-D is integrated; R1-P is resumed from its live-green cancellation
checkpoint. R2 composition and every remaining M5.4--M5.6
implementation/evaluation closure remain pending. See
`docs/m5_implementation_status.md`.

Deliverables:

- Immutable versioned evidence requirements and groups with group-derived
  temporal activity.
- Requirement-subject observations for witnesses.
- Exact covering-matching completeness, not global witness-union cardinality.
- A bounded Hall-mask incremental operator, affected-group matching baseline,
  stateful certificates, and explicit work bounds/counters.
- Alternative-group support composed with the direct M1--M4 path.
- Independent Python and PostgreSQL recomputation at every tested revision.
- Typed v2 requirement jobs, owner-projected PENDING, sparse publication,
  activation, failure, and exact replay.
- Pinned controlled/retrospective WiCE evaluation before any model-proposed
  group result.

Stages:

- **M5.0:** design/theory/schema/data freeze. **Complete through accepted
  M5-D24-C3.** Contract rows pass; decision-row implementation halves remain
  pending until the M5.6 cross-stage evidence mapping.
- **M5.1:** pure records, history, structural events, and independent Python
  oracle. **Complete.**
- **M5.2:** incremental Hall-mask engine, certificates, exhaustive bounded
  tests, and 100,000-event differential gate. **Complete.**
- **M5.3:** transactional 013-to-014 PostgreSQL upgrade, typed persistence, and
  independent SQL oracle. **Complete.** M5.3-01 through M5.3-09 pass,
  including production rejected-declaration, durable failure, reconnect
  replay, conflict, strict-state immutability, and concurrent exact-open
  evidence for M5.3-07.
- **M5.4:** dynamic requirement retrieval/verification/publication/replay,
  recoverable at-least-once dispatch with durable work/timing, persisted Hall
  state, and bounded frozen-model diagnostic. **Active; partial only.**
- **M5.5:** controlled dataset adapter, seven qualified baselines, and
  reproducible systems/semantic reports.
- **M5.6:** full validation, evidence bundle, documentation and honest verdict.

Exit criteria:

- Incremental, independent Python, and independent SQL state/certificates agree
  after every required update class.
- Exhaustive simple graphs through `r<=4,H<=4` and the frozen 100,000-event
  stream have zero mismatch.
- Deleting one required edge invalidates only the affected group; a matching-
  only loss is detected even without a requirement zero crossing.
- A claim remains supported when direct support or an alternative complete
  group survives.
- Fresh/upgrade/rerun/rollback/concurrency PostgreSQL gates and v1 compatibility
  pass.
- Controlled evaluation reports false invalidation, false retention, work,
  latency, state/certificate size, all exclusions, and qualified baseline
  availability on identical event histories.
- Without fresh independent adjudication, the conclusion remains explicitly
  controlled/retrospective and the representative utility claim stays M6 debt.

## M6 — Application, Experiments, and Dissertation

Deliverables:

- Cost-based refresh switch (TARGET); transparent priority scheduler with
  stratified audit (STRETCH only).
- Answer-health dashboard.
- Document version and status-delta inspector.
- Reproducible dataset/update-stream adapters.
- Baseline and ablation runners.
- Systems and AI analysis.
- Error taxonomy.
- Larger independently adjudicated natural-history evaluation before any
  dissertation-level selective-maintenance or end-to-end utility claim.
- Dissertation-ready figures and tables.

Exit criteria:

- A clean-machine setup can reproduce the principal result.
- Every headline claim maps to a recorded experiment.
- Limitations distinguish neural errors from maintenance errors.

## Post-FYP Research Path

- Learned impact scheduling under explicit risk budgets.
- Probabilistic or semiring-valued grounding maintenance.
- Shared bounded multi-hop evidence structures.
- Recursive GroundGraph maintenance.
- Private or secure-shape grounding maintenance under a new threat model.
