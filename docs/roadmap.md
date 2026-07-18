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

Status: in progress. In-memory implementation and SQL artifacts complete on
2026-07-18; live PostgreSQL validation is blocked because this host has neither
PostgreSQL nor Docker. See `docs/m2_implementation_status.md`.

Deliverables:

- PostgreSQL schema and migrations.
- Direct and alternative witness counts.
- Incremental state transition implementation.
- Full relational recomputation query or reference function.
- Generated update streams.
- Differential comparison after every event.
- Separate semantic-epoch/publication coordinator oracle (D-20).

Exit criteria:

- Incremental and full results agree for randomized insert/delete/replace
  sequences.
- Zero-crossing and conflict tests pass.
- PostgreSQL schema, SQL oracle, and three-path fixture execute successfully on
  PostgreSQL 16.

## M3 — Static AI Pipeline

Deliverables:

- Passage chunking and embeddings.
- RAG answer with citations.
- Atomic claim extraction.
- Candidate evidence retrieval.
- Versioned support/refute/neutral verifier outputs.
- Model and prompt registry.

Exit criteria:

- One command ingests a corpus, asks a question, persists an answer, and shows
  claims with evidence and verifier scores.
- Repeated runs reuse versioned observations rather than silently overwriting
  them.

## M4 — Selective Semantic Maintenance

Deliverables:

- Exact reverse dependency handling for deletions (withdrawal path).
- Asymmetric admission discovery: reverse-ANN + lexical union at fixed top-L.
- Candidate frontier with mandatory retrieval fallback.
- Full end-to-end recomputation baseline.

Exit criteria:

- Impact recall and verifier-call savings are measured on recorded update
  streams.
- Missed affected claims are inspectable.

## M5 — Bounded Evidence Groups

Deliverables:

- Versioned evidence requirements and groups (validity intervals).
- Requirement-subject observations for witnesses.
- Requirement-satisfaction and group-completeness maintenance with the
  distinct-representative rule (D-1).
- Alternative group support.
- Gold or controlled group evaluation before model-proposed groups.

Exit criteria:

- Deleting one required witness invalidates only its group.
- A claim remains supported when an alternative complete group survives.
- False invalidation is compared with direct-citation invalidation.

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
