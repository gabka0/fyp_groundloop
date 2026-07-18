# GroundLoop Decision Log

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
