# GroundLoop Research Plan

Status: project direction and v0.2 design frozen; M1, M1.1, and M2 complete,
including live PostgreSQL validation; M3 is next

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
