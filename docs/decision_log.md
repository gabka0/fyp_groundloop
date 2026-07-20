# GroundLoop Decision Log

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
