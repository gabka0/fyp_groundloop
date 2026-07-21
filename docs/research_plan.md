# GroundLoop Research Plan

Status: project direction and v0.2 design frozen; M1 through M4 complete.
M4 closed with strong conditional systems evidence but preliminary and
negative AI/end-to-end evidence. M5 is unblocked but not started; the frozen
M3 verifier remains the default.

## Working Title

**GroundLoop: Exact Incremental Maintenance of Claim Grounding for RAG Answers
over Evolving Document Collections**

## Problem

RAG systems normally answer against the current state of a corpus and then
discard the dependency structure behind the answer. Previously generated
answers can become stale when documents are inserted, deleted, corrected, or
replaced. Regenerating and reverifying every answer after every corpus update is
correct but can require a prohibitive number of retrieval, verifier, and LLM
calls. Coarse invalidation policies avoid some computation but unnecessarily
discard answers that retain alternative supporting evidence.

GroundLoop treats old answers as registered results over an evolving evidence
collection. AI models materialize atomic claims, candidate evidence,
support/refutation judgments, and bounded evidence groups. IVM maintains the
consequences of changes to those versioned semantic observations.

## Primary Research Question

> How can a RAG system maintain the claim-level grounding status of previously
> generated answers over an evolving document collection while invoking
> substantially fewer neural evaluations than full recomputation?

## Secondary Questions

1. Can incremental relational maintenance remain exactly equivalent to full
   recomputation over identical stored neural judgments?
2. Which impact-selection policy offers the best affected-claim recall for a
   fixed neural-call budget?
3. When do alternative and conjunctive evidence groups prevent false
   invalidation?
4. At what answer-registry size and update locality does maintenance become
   cheaper than full recomputation?
5. How do verifier accuracy, calibration, latency, and cost affect end-to-end
   maintenance quality?

## Proposed Contributions

1. A versioned data model for passages, claims, neural verification
   observations, evidence requirements, evidence groups, claim states, and
   answer states.
2. An incremental maintenance engine for alternative witnesses, conjunctive
   requirements, alternative evidence groups, and answer-level propagation.
3. A selective semantic re-verification policy for document insertions,
   deletions, and replacements.
4. A differential correctness framework comparing incremental state with full
   relational recomputation after every update.
5. A benchmark generator that converts factual-verification and versioned
   document collections into reproducible update streams.
6. A user-facing answer-health dashboard with status-delta provenance and
   computation-savings reports.

## Hypotheses

- **H1 — Exact maintenance:** For identical stored semantic observations,
  incremental and full relational recomputation produce identical claim and
  answer states after every update.
- **H2 — Selective savings:** At high update locality, GroundLoop substantially
  reduces verifier calls and update latency without materially reducing
  affected-claim recall.
- **H3 — Alternative support:** Witness counting and evidence groups reduce
  false invalidation relative to source- and direct-citation invalidation.
- **H4 — Break-even:** Maintenance overhead becomes beneficial as the number of
  registered answers grows faster than the number of claims affected per
  update.

## Exact and Empirical Claims

Exact claim:

> Given versioned `CandidateEvidence`, current `SemanticObservation` score
> records, the current `DecisionPolicy`, `EvidenceRequirement`, and
> `RequirementWitness` relations, the maintained claim and answer states equal
> full evaluation of the same relational semantics.

Empirical claims:

- The impact selector finds semantically affected claims with measured recall.
- The verifier classifies support, refutation, and insufficient evidence with
  measured accuracy and calibration.
- GroundLoop reduces neural computation under specified workloads.

The exact guarantee must never be extended to unrestricted natural-language
truth or neural semantic correctness.

## Current Evidence Boundary after M4

M4 executed, rather than merely implemented, its final empirical gates:

- M4.10 ran all seven treatments on three naturally versioned software-
  repository histories with pinned models and persisted audits. The
  non-exhaustive policies found only `1/4` model-relative positive pairs and
  `0/1` answer-status effects. The pilot has no independent human labels and
  is too small for a population claim.
- M4.12 measured the frozen M3 verifier on a page-disjoint VitaminC revision
  diagnostic. It obtained `0.5059` endpoint accuracy, `0.1992` joint endpoint
  correctness and `0.3281` detected label changes, establishing that the
  semantic producer was the dominant observed weakness.
- M4.13 trained and evaluated replay, cross-entropy-mix, paired-margin and
  no-replay controls under a sealed protocol. The cross-entropy mix improved
  revision sensitivity, but the terminal result was `NO_GO` because the
  preregistered original-domain retention interval failed G8. The adapted
  checkpoint is therefore not promoted.

These results close M4 honestly: exact structured maintenance remains the
strong result, while useful selective recall and a production-safe neural
improvement are not established. A larger independently adjudicated natural-
history study remains required before dissertation-level end-to-end claims.
That study is pre-dissertation/M6 work, not a reason to reopen the completed
M4 implementation milestone.

## FYP Scope

Required:

- Versioned document ingestion.
- Static grounded-answer generation.
- Atomic claim extraction.
- Candidate evidence retrieval.
- Versioned support/refutation observations.
- Document insertion, deletion, and replacement.
- Direct and alternative support maintenance.
- Full-recomputation oracle and differential tests.
- Selective neural re-verification.
- At least one rigorously evaluated verifier.
- Reproducible systems and AI experiments.
- An answer-health dashboard.

Target extension:

- Bounded conjunctive requirements and alternative evidence groups.

Explicitly excluded:

- Arbitrary recursive reasoning graphs.
- Cycles and general recursive Datalog.
- General agent memory or multi-agent coordination.
- Incremental model-weight maintenance.
- Model-checkpoint changes as an efficient delta workload.
- Multimodal evidence in the initial FYP.
- ORAM, TEE, MPC, or cryptographic security claims.

## Success Standard

The project is successful when it produces a reproducible application and
evaluation showing both:

1. exact database maintenance relative to stored neural judgments; and
2. a credible reduction in expensive semantic recomputation at acceptable
   affected-claim recall.

Publication-level novelty is a possible outcome, not a prerequisite for a
successful FYP.
