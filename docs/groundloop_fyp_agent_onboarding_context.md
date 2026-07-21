# GroundLoop FYP Agent Onboarding Context

**Read this file first when starting work on the GroundLoop repository.**

> **Authority notice:** this document preserves the original project selection
> and broad context. Its proposed schema, labels, phases, and first-task list
> predate the adversarial review. `docs/technical_design.md` v0.2 plus decisions
> D-1 through D-20 govern implementation. In particular, observations store
> scores rather than permanent labels, and M1 through M4 are complete. The
> frozen M3 verifier remains the default after the M4.13 adaptation returned
> `NO_GO`; M5 is unblocked but not started. Read the current milestone routing
> below before planning new work.

Last updated: 2026-07-21

## Current Milestone Routing

This document preserves the original project selection and much of its
historical plan. It is not the current status authority. For the executed M4
boundary, read in this order:

1. `docs/m4_implementation_status.md` — integrated implementation and final
   milestone boundary.
2. `docs/workstreams/m4_10_real_history_study/HANDOFF.md` — executed natural-
   history pilot and its severe sample/adjudication limits.
3. `docs/workstreams/m4_12_public_ai_gate/README.md` — executed frozen-M3
   revision-sensitive diagnostic.
4. `docs/m4_13_change_aware_verifier_plan.md` — preregistered adaptation and
   stop/go gates.
5. `docs/workstreams/m4_13_change_aware_verifier/RESULTS.md` — sealed terminal
   `NO_GO`, non-promotion decision and final M4 verdict.

M4 is closed with strong conditional structured-correctness evidence and
preliminary/negative AI and end-to-end evidence. M4.10's three natural
histories were not independently adjudicated. M4.13 improved the held-out
revision diagnostic but failed the preregistered original-domain retention
interval, so no adapted checkpoint replaces the frozen M3 default. M5 bounded
evidence groups may begin; the larger independently adjudicated natural-
history study remains explicit pre-dissertation/M6 debt.

This document records the project decision, research boundaries, proposed
architecture, evaluation plan, and relationship to the earlier Secure CROWN
work. It is intended to be moved into a new GroundLoop repository and used as
the primary onboarding document for future agents and collaborators.

## 1. Decision Summary

The selected FYP is:

> **GroundLoop: Incremental Claim and Evidence-Group Maintenance for
> Self-Updating Retrieval-Augmented Generation**

GroundLoop is a RAG system that remembers previously generated answers. When
the underlying document collection changes, it identifies potentially affected
claims, selectively reruns neural retrieval or verification, and incrementally
maintains whether each old answer is still supported, unsupported,
contradicted, or conflicted.

The central research question is:

> How can a RAG system maintain the claim-level grounding status of previously
> generated answers over an evolving document collection while invoking
> substantially fewer neural evaluations than full recomputation?

The project sits at the intersection of AI/NLP and databases:

- AI/NLP produces atomic claims, evidence candidates, support/refutation
  judgments, confidence estimates, and bounded evidence groups.
- Database incremental view maintenance (IVM) propagates changes through
  versioned semantic records to claim and answer states.
- The system measures both semantic quality and saved neural computation.

The final project must be a concrete, demonstrable application, not only an
algorithm library or a collection of experiments.

## 2. Repository Decision

Start GroundLoop in a **new, clean repository**. Do not copy the complete
Dynagox repository into it.

Dynagox contains Secure CROWN C++ experiments, MP-SPDZ baselines, security
scaffolding, build artifacts, and assumptions that are not natural foundations
for a Python-first RAG/NLP application. Copying it wholesale would make the new
project harder to understand and would falsely suggest that GroundLoop depends
on the Secure CROWN implementation.

Recommended approach:

1. Create an empty `groundloop` repository.
2. Move this file into `docs/groundloop_fyp_agent_onboarding_context.md`.
3. Add a short root `AGENTS.md` pointing future agents to this document.
4. Build the first implementation independently in Python.
5. Keep `/home/kassym/dynagox` as a read-only intellectual and implementation
   reference.
6. Reuse or port a Dynagox component only after a measured need is established.

GroundLoop may later include a standalone C++ maintenance kernel or compare a
CROWN-inspired factorized implementation with simpler alternatives. That is an
optimization or research extension, not a prerequisite for the first working
system.

## 3. User Goals and Evaluation Priorities

The project is intended to:

- Earn strong FYP marks through a clear question, rigorous implementation,
  defensible experiments, and honest claims.
- Demonstrate both AI and database competence.
- Produce an impressive portfolio artifact for research-oriented AI master's
  applications, including programs such as MBZUAI.
- Use IVM as a central mechanism, not as a decorative SQL trigger.
- Leave credible continuations into a master's thesis or PhD.
- Be implementable by one student within an FYP timeline.

Novelty is valuable but is no longer the only or dominant criterion. A polished,
well-evaluated system with moderate novelty is preferable to an ambitious but
unverifiable prototype.

## 4. What GroundLoop Is

GroundLoop manages a registry of previously generated RAG answers.

A typical user workflow is:

1. Upload a collection of documents.
2. Ask a question.
3. Receive an answer with citations.
4. Inspect atomic claims and their supporting or refuting evidence.
5. Insert, delete, or replace documents.
6. Observe which old answers remain valid and which change status.
7. Inspect why a status changed and which computation was avoided.
8. Optionally regenerate selected invalid answers.

The intended visible states are:

```text
VALID
PARTIALLY_SUPPORTED
UNSUPPORTED
CONFLICTED
CONTRADICTED
REGENERATION_RECOMMENDED
```

The status names and precise policy can be refined, but the underlying counts,
scores, witnesses, and provenance must remain inspectable.

## 5. What GroundLoop Is Not

GroundLoop is not:

- A general agent-memory framework.
- An attempt to represent all natural-language meaning relationally.
- A system that applies IVM directly to raw text or LLM weights.
- A general recursive reasoning-graph or Datalog engine.
- A complete truth-detection system.
- A guarantee that a neural verifier is semantically correct.
- A security, ORAM, MPC, or TEE project in its FYP scope.
- A generic chatbot with document upload and an SQL trigger.
- A claim of being the first dynamic, temporal, or freshness-aware RAG system.

The system maintains **grounding relative to a versioned evidence collection
and versioned model judgments**, not objective truth in the unrestricted world.

## 6. The Fundamental Representation Boundary

Raw natural language is unstructured and neural judgments may be uncertain or
nondeterministic. GroundLoop handles this using an explicit boundary:

```text
Unstructured documents and generated text
        |
        | AI extraction, retrieval, and verification
        v
Versioned structured semantic observations
        |
        | exact relational delta propagation
        v
Maintained claim and answer states
```

Examples of structured semantic observations are:

```text
Claim C1 belongs to Answer A1.
Passage P7 version V3 is active.
P7 is a candidate evidence passage for C1.
Verifier M2 labels (C1, P7, V3) as SUPPORT with score 0.93.
Evidence group G4 contains requirements R1 and R2.
Passage P7 satisfies R1.
```

The model result is stored with its model, prompt, input, and document versions.
Once stored, it is an immutable observation. Rerunning a model creates a
database delta: delete or deactivate the old observation and insert a new one.

The exact claim is:

> Given the current versioned semantic relations, incremental maintenance must
> produce the same claim and answer states as full relational recomputation.

The project must not claim that the neural extraction and verification process
is exact. Its quality is evaluated empirically.

## 7. Bounded Evidence Groups, Not Full GroundGraph

The FYP should include a bounded evidence-group DAG, but not arbitrary recursive
reasoning graphs.

A simple claim may have independent direct witnesses:

```text
Claim C1 is supported by P1 OR P2.
```

A compound or multi-document claim may require a complete group:

```text
Evidence group G1 supports C2 if:
    requirement R1 is satisfied by P3 or P4
    AND
    requirement R2 is satisfied by P5
```

There may be alternative sufficient groups:

```text
Claim C2 is supported if G1 OR G2 is complete.
```

The maintained hierarchy is bounded:

```text
PassageVersion
    -> Verification / RequirementWitness
        -> EvidenceRequirement
            -> EvidenceGroup
                -> Claim
                    -> Answer
```

This provides conjunction, alternative derivations, factorization, provenance,
and nontrivial zero-crossing updates without requiring recursive inference,
cycles, unrestricted rule generation, or general truth maintenance.

Implementation sequence:

1. First implement single-passage and alternative-witness support.
2. Then add bounded multi-evidence groups.
3. Treat unrestricted recursive reasoning graphs as future research.

## 8. Proposed Data Model

The initial schema should resemble the following. Exact names may change, but
the version and provenance fields must not be discarded.

```text
Document(
    document_id,
    title,
    active_version_id
)

DocumentVersion(
    version_id,
    document_id,
    content_hash,
    created_at,
    active
)

Passage(
    passage_id,
    version_id,
    passage_index,
    passage_text,
    embedding,
    active
)

Question(
    question_id,
    question_text,
    created_at
)

Answer(
    answer_id,
    question_id,
    answer_text,
    generator_version,
    prompt_version,
    created_at
)

Claim(
    claim_id,
    answer_id,
    claim_text,
    importance,
    extractor_version,
    prompt_version
)

CandidateEvidence(
    claim_id,
    passage_id,
    retrieval_score,
    rank,
    embedding_model_version
)

Verification(
    claim_id,
    passage_id,
    passage_version,
    support_score,
    refute_score,
    neutral_score,
    verifier_version,
    prompt_version
)

EvidenceGroup(
    group_id,
    claim_id,
    group_model_version
)

EvidenceRequirement(
    requirement_id,
    group_id,
    requirement_text
)

RequirementWitness(
    requirement_id,
    passage_id,
    passage_version,
    verification_id
)

ClaimState(
    claim_id,
    supporting_witness_count,
    complete_group_count,
    refuting_witness_count,
    best_support_score,
    best_refute_score,
    status
)

AnswerState(
    answer_id,
    supported_claim_count,
    unsupported_claim_count,
    refuted_claim_count,
    conflicted_claim_count,
    status
)

UpdateEvent(
    event_id,
    operation,
    document_id,
    old_version_id,
    new_version_id,
    created_at
)

StatusDelta(
    event_id,
    object_type,
    object_id,
    old_status,
    new_status,
    reason
)
```

All thresholds used to convert scores into labels or states must be configured,
recorded, and evaluated. They must not be presented as universal truth
boundaries.

## 9. Incremental Semantics

At minimum, maintain the following logical quantities.

```text
RequirementSatisfied(r) =
    at least one active supporting witness exists for r

GroupComplete(g) =
    every requirement belonging to g is satisfied

ClaimSupported(c) =
    c has an active direct support witness
    OR at least one evidence group for c is complete

ClaimRefuted(c) =
    c has at least one sufficiently confident active refuting witness

ClaimStatus(c) =
    CONFLICTED   if ClaimSupported(c) and ClaimRefuted(c)
    SUPPORTED    if ClaimSupported(c) and not ClaimRefuted(c)
    REFUTED      if not ClaimSupported(c) and ClaimRefuted(c)
    UNSUPPORTED  otherwise

AnswerStatus(a) =
    derived from the states and importance of a's claims
```

An update should propagate only through affected dependencies:

```text
delta PassageVersion
    -> delta CandidateEvidence / Verification / RequirementWitness
        -> delta RequirementSatisfied
            -> delta GroupComplete
                -> delta ClaimState
                    -> delta AnswerState
                        -> StatusDelta
```

Important examples:

- Deleting one of three direct support witnesses changes a count from three to
  two and must not invalidate the claim.
- Deleting the final witness for one requirement makes its group incomplete.
- A claim must remain supported if another complete evidence group survives.
- Introducing a refuting witness while support remains creates a conflict,
  rather than silently choosing one side.
- Replacing a passage is modeled as deletion/deactivation of its old version
  followed by insertion of a new version.

## 10. Selective Neural Re-Verification

Maintaining relational counts is not the main research difficulty. The central
AI-systems problem is deciding which semantic judgments should be recomputed
after a document update.

Use different strategies by update type.

### Passage or document deletion

Reverse provenance identifies existing candidate, verification, and witness
records that directly depend on the deleted version. These can be withdrawn
without a neural call. Affected claims may require retrieval of replacement
evidence.

### Passage replacement

Treat as deletion of the old version and insertion of the new version. Existing
dependents of the old version are known. The new text must be compared against
registered claims to discover new or changed dependencies.

### Passage insertion

The system must discover which existing claims may gain new support or
refutation. Possible mechanisms include:

- Reverse approximate-nearest-neighbor search from passages to registered
  claim embeddings.
- Entity or keyword overlap.
- Existing document- or source-level provenance.
- Learned semantic impact prediction.
- A bounded top-L candidate policy.

The selected candidate pairs are passed to the verifier. The resulting inserted
or deleted `Verification` records drive exact downstream IVM.

The semantic impact selector is approximate and must be evaluated using recall,
cost, and stale-answer exposure. It is not covered by the relational exactness
guarantee.

## 11. Research Questions and Hypotheses

The FYP should evaluate at least these questions.

### RQ1: Relational correctness

Does incremental maintenance produce exactly the same claim and answer states
as full recomputation over the same stored semantic judgments after every
update?

Expected result: equality for every tested update sequence.

### RQ2: Neural computation savings

How many retrieval and verification calls does GroundLoop avoid relative to
full end-to-end recomputation?

Expected result: substantial savings when updates affect a small portion of a
large answer registry.

### RQ3: Semantic impact quality

How accurately does the impact selector identify claims whose grounding status
would change?

Measure recall carefully; missing an affected claim is more serious than
performing an unnecessary verifier call.

### RQ4: Alternative and conjunctive evidence

Do evidence groups reduce false invalidation relative to direct-citation or
single-witness invalidation?

### RQ5: Break-even behavior

At what workload sizes and update rates does maintaining dependency state cost
less than full recomputation?

### RQ6: Verifier quality

How do zero-shot NLI, a fine-tuned verifier, a cross-encoder, and an LLM judge
compare in accuracy, calibration, latency, and cost?

## 12. Baselines

Implement meaningful baselines, not deliberately weak comparisons.

Required baselines:

1. **Full recomputation**: rerun retrieval and verification for every registered
   claim after each update.
2. **Full answer regeneration**: regenerate all stored answers after each
   update where feasible.
3. **Source-level invalidation**: invalidate every answer associated with a
   changed document.
4. **Direct-citation invalidation**: reconsider only claims directly citing a
   changed passage.
5. **TTL or freshness-risk policy**: invalidate or refresh using age/risk but
   without claim-level dependency maintenance.
6. **GroundLoop without evidence groups**: independent support witnesses only.
7. **GroundLoop with bounded evidence groups**.

If a baseline cannot be run at full scale, report the limitation rather than
silently reducing its work.

## 13. Metrics

### Systems metrics

- Update latency.
- Update throughput.
- Number and fraction of claims touched.
- Number of answers touched.
- Neural verifier calls.
- Embedding or retrieval operations.
- Generated tokens or estimated inference cost.
- Materialized-state size.
- Speedup over full recomputation.
- Break-even point.
- Status-delta propagation depth and fanout.

### AI and application metrics

- Evidence retrieval recall.
- Support/refutation/neutral precision, recall, and F1.
- Affected-claim recall.
- Unsupported-answer detection rate.
- Contradiction-detection rate.
- False invalidation rate.
- Stale-answer exposure time.
- Confidence calibration, such as expected calibration error or Brier score.
- Regeneration precision: fraction of regenerated answers that actually needed
  regeneration.

### Correctness metric

After every event:

```text
incrementally maintained relational state
    ==
full relational recomputation over identical stored neural judgments
```

Keep this check separate from end-to-end neural accuracy.

## 14. Dataset Plan

Use at least one factual-verification dataset and one realistically versioned
document collection.

Candidate factual-verification resources:

- FEVER.
- SciFact.
- WiCE.
- AVeriTeC.
- HoH for outdated RAG evidence.
- Minimal Evidence Group data or methodology for multi-evidence support.

Static datasets can be converted into controlled update streams:

```text
insert supporting evidence
delete one of several support witnesses
delete the final support witness
insert contradictory evidence
replace evidence with an updated version
revoke a source
insert an alternative complete evidence group
```

For the application demo, prefer a naturally versioned corpus such as:

- Software documentation and release notes.
- API documentation.
- University regulations.
- Product manuals.
- Public policy documents.

Software documentation is attractive because version changes are frequent,
inspectable, and often have clear downstream consequences.

## 15. AI Component Plan

At least one AI component must be trained, adapted, or rigorously evaluated.
Using external LLM APIs for every semantic operation would create a polished
application but a weak AI research contribution.

The preferred trainable component is the claim-evidence verifier.

Possible plan:

1. Start with a pretrained NLI or cross-encoder model.
2. Fine-tune it on claim-verification data.
3. Evaluate domain transfer between general factual claims, scientific claims,
   and the selected demo corpus.
4. Calibrate its confidence scores.
5. Compare against embedding similarity, zero-shot NLI, and an LLM judge.
6. Store every result with model and prompt version metadata.

Claim extraction may initially use a prompted local or hosted LLM, but it must
produce atomic, inspectable claims and its output must be cached and versioned.

Evidence-group construction should be phased:

- Start from gold or controlled groups where available.
- Then test LLM- or model-proposed groups.
- Verify group sufficiency separately from individual passage relevance.

## 16. Suggested Technical Stack

Initial recommendation:

```text
Language:           Python
API:                FastAPI
Relational store:   PostgreSQL
Vector support:     pgvector initially; FAISS is an alternative
ML:                 PyTorch and Hugging Face Transformers
Frontend:           React or Next.js
Experiments:        Python CLI plus reproducible configuration files
Packaging:          Docker Compose
Testing:            pytest
```

PostgreSQL plus pgvector is preferable initially because it keeps versions,
provenance, dependencies, model judgments, and vector references in one
inspectable system. A specialized vector database should be introduced only if
experiments show that it is needed.

Do not begin by implementing a custom database engine. First establish a
correct end-to-end workload and full-recomputation baseline.

## 17. Suggested New Repository Structure

```text
groundloop/
  AGENTS.md
  README.md
  pyproject.toml
  docker-compose.yml
  configs/
  docs/
    groundloop_fyp_agent_onboarding_context.md
    research_plan.md
    literature_matrix.md
    evaluation_protocol.md
  src/groundloop/
    api/
    ingestion/
    generation/
    claims/
    retrieval/
    verification/
    impact/
    maintenance/
    provenance/
    storage/
  frontend/
  migrations/
  tests/
    unit/
    integration/
    differential/
  experiments/
    datasets/
    streams/
    baselines/
    analysis/
  scripts/
```

`tests/differential/` should compare incremental state against full
recomputation after each event in generated and recorded update sequences.

## 18. Implementation Phases

### Phase 0: Proposal and literature matrix

- Freeze terminology and claims.
- Record closest work and exact distinctions.
- Select initial datasets and models.
- Define update semantics and evaluation protocol.

### Phase 1: Static grounded-answer pipeline

- Ingest and version documents.
- Chunk and embed passages.
- Generate answers with citations.
- Extract atomic claims.
- Retrieve and verify evidence.
- Persist all intermediate semantic records.

### Phase 2: Exact relational maintenance

- Implement document insert, delete, and replace events.
- Maintain direct support/refutation counts.
- Maintain claim and answer states.
- Implement full relational recomputation.
- Add differential tests after every event.

### Phase 3: Selective semantic re-verification

- Build reverse dependency indices.
- Implement candidate impact strategies.
- Rerun the verifier only for selected delta pairs.
- Measure affected-claim recall and neural-call savings.

### Phase 4: Bounded evidence groups

- Add requirements, groups, and alternative derivations.
- Maintain requirement satisfaction and group completeness.
- Measure false invalidation against simpler baselines.

### Phase 5: Application and experiments

- Build the answer-health dashboard.
- Construct reproducible update streams.
- Run system and AI evaluations.
- Perform error analysis and ablations.

### Phase 6: Dissertation and release

- Write exact and empirical claims separately.
- Package the demo and experiment runner.
- Document limitations and reproducibility.
- Prepare a short demonstration video and architecture diagram.

## 19. Concrete Demo Scenario

The final demo should make the project understandable without reading the
dissertation.

Example:

```text
Original documentation:
"Nimbus supports Python 3.10 and later."

Stored answer:
"Nimbus supports Python 3.10 and newer versions."

Claim C1:
"Nimbus supports Python 3.10 and newer versions."

Initial evidence:
P1 SUPPORT 0.94
P2 SUPPORT 0.86

Initial state:
SupportCount(C1) = 2
AnswerStatus(A1) = VALID
```

Deleting P1 changes the support count from two to one, so the answer remains
valid. Replacing P2 with documentation stating that Nimbus requires Python 3.12
causes selective re-verification and produces:

```text
SupportCount(C1): 1 -> 0
RefuteCount(C1):  0 -> 1
ClaimStatus(C1):  SUPPORTED -> REFUTED
AnswerStatus(A1): VALID -> CONTRADICTED
```

The UI should show the document diff, affected claim, previous and new
evidence, status transition, verifier calls used, and calls avoided relative to
full recomputation.

## 20. Closest Prior Work and Required Positioning

Do not claim that GroundLoop invents dynamic RAG, claim verification,
provenance, evidence grouping, or streaming vector retrieval.

Important nearby work includes:

1. **FreshCache: Risk-Constrained Freshness-Aware Semantic Caching for
   Open-Web Retrieval-Augmented LLMs**
   - <https://arxiv.org/abs/2607.04281>
   - Close on stale cached RAG results and selective refresh.
   - GroundLoop differs by maintaining claim-level evidence dependencies and
     explicit status deltas rather than only cache-entry freshness risk.

2. **HoH: A Dynamic Benchmark for Evaluating the Impact of Outdated
   Information on Retrieval-Augmented Generation**
   - <https://aclanthology.org/2025.acl-long.301/>
   - Establishes outdated RAG evidence as a serious problem and provides a
     possible evaluation resource.

3. **ProvenanceGuard: Source-Aware Factuality Verification for MCP-Based LLM
   Agents**
   - <https://arxiv.org/abs/2606.18037>
   - Decomposes answers into claims and performs source-aware verification.
   - GroundLoop's distinction is maintenance over evidence updates.

4. **GenProve: Learning to Generate Text with Fine-Grained Provenance**
   - <https://aclanthology.org/2026.acl-long.228/>
   - Provides generation-time structured provenance concepts.

5. **How and Why is an Answer Still Correct? Maintaining Provenance in Dynamic
   Knowledge Graphs (HUKA)**
   - <https://arxiv.org/abs/2007.14864>
   - Maintains provenance for standing queries over structured dynamic KGs.
   - GroundLoop operates over neural semantic predicates derived from text.

6. **Minimal Evidence Group Identification for Claim Verification**
   - <https://aclanthology.org/2025.trustnlp-main.8/>
   - Formalizes alternative minimal groups of evidence that collectively
     support claims.

7. **VectraFlow: Integrating Vectors into Stream Processing**
   - <https://vldb.org/cidrdb/2025/vectraflow-integrating-vectors-into-stream-processing.html>
   - Addresses streaming vector filters, top-k, and joins. Streaming vector
     maintenance alone is not GroundLoop's novelty.

8. **F-IVM: Incremental View Maintenance with Triple Lock Factorization
   Benefits**
   - <https://arxiv.org/abs/1703.07484>
   - Relevant database foundation for factorized and higher-order IVM.

The defensible positioning is:

> Existing work studies answer freshness, dynamic provenance, claim
> verification, streaming vector retrieval, and evidence grouping. GroundLoop
> studies how versioned neural judgments and relational dependencies can be
> combined to maintain the grounding of previously generated answers while
> minimizing semantic recomputation.

Before making any `first`, `novel`, or state-of-the-art claim, conduct a fresh
systematic literature review. The research landscape is changing quickly.

## 21. Relationship to Secure CROWN and Dynagox

Secure CROWN is the user's earlier project in `/home/kassym/dynagox`. It
explores secure-shape dynamic conjunctive-query maintenance. Its current
implementation is not cryptographically secure; it is pre-TEE/ORAM
access-shape scaffolding with narrow query and security claims.

Useful intellectual carryover:

- Think in terms of standing queries and deltas.
- Preserve alternative witnesses rather than invalidating on the first
  deletion.
- Factor shared dependencies.
- Compare incremental maintenance with full recomputation.
- Separate exact correctness claims from security or model-quality claims.
- Measure update shape, fanout, state size, and break-even behavior.

GroundLoop's bounded hierarchy can be expressed using joins and aggregates:

```text
RequirementWitness
  JOIN EvidenceRequirement
  JOIN EvidenceGroup
  JOIN Claim
  JOIN Answer
```

This creates a legitimate conceptual connection with CROWN-style maintenance.

However:

- GroundLoop is not a Secure CROWN security extension.
- Do not claim that the current Secure CROWN code protects GroundLoop data.
- Do not import ORAM, TEE, or MP-SPDZ work into the FYP unless the scope is
  explicitly changed later.
- Do not make Dynagox a mandatory build or runtime dependency.
- Do not force the existing C++ executor onto the workload before measuring
  whether a simpler implementation is insufficient.

If future work introduces private grounding maintenance, it must define a new
threat model and leakage profile rather than inheriting Secure CROWN claims by
association.

## 22. Main Risks and Mitigations

### Risk: IVM degenerates into maintaining two counters

Mitigation:

- Maintain standing answers at meaningful scale.
- Include versioned dependencies, reverse impact indices, alternative
  witnesses, conjunctive evidence groups, and answer-level propagation.
- Evaluate neural calls and break-even points, not only SQL latency.

### Risk: The AI component is only an API call

Mitigation:

- Fine-tune or adapt the verifier.
- Evaluate calibration and domain transfer.
- Compare multiple verification approaches.
- Perform detailed error analysis.

### Risk: Semantic impact selection misses affected claims

Mitigation:

- Optimize for high recall.
- Include conservative fallback policies.
- Report stale exposure and false negatives explicitly.
- Compare exact reverse dependencies with approximate discovery of new edges.

### Risk: Evidence groups become unrestricted reasoning graphs

Mitigation:

- Enforce a bounded DAG schema.
- Disallow cycles and recursive inference in the FYP.
- Separate group sufficiency verification from arbitrary chain-of-thought.

### Risk: Full recomputation is not reproducible because models are stochastic

Mitigation:

- Cache and version neural outputs.
- Fix seeds and decoding settings where possible.
- Use a relational recomputation oracle over identical stored judgments for
  exact correctness.
- Report end-to-end stochastic variation separately.

### Risk: The project overclaims truth or novelty

Mitigation:

- Say `grounded relative to the active evidence and verifier`, not `true`.
- Distinguish exact database guarantees from empirical AI performance.
- Maintain a literature comparison matrix.

## 23. Success Criteria

The minimum successful FYP should provide:

- A working document-versioned RAG application.
- Persistent atomic claims and claim-evidence judgments.
- Insert, delete, and replace update handling.
- Incrementally maintained claim and answer states.
- Differential correctness tests against full relational recomputation.
- Selective neural re-verification with measured savings.
- At least one rigorously evaluated AI verifier.
- A realistic update-stream benchmark.
- A clear answer-health and provenance interface.
- Reproducible experiments and a paper-style dissertation.

A strong result additionally provides:

- Bounded conjunctive and alternative evidence groups.
- A calibrated impact-selection model.
- Clear break-even analysis.
- Open-source datasets or update-stream generation tools.
- A short workshop-quality paper draft.

## 24. Instructions for Future Agents

When entering the new GroundLoop repository:

1. Read this file completely before proposing architecture or editing code.
2. Inspect the current repository state and existing documentation before
   creating new files.
3. Preserve the boundary between unstructured text, neural observations, and
   exact IVM.
4. Do not reopen the full GroundGraph design unless new evidence or an explicit
   user decision changes the scope.
5. Do not describe ordinary document upload plus RAG as a research
   contribution.
6. Keep model, prompt, data, and passage versions explicit.
7. Maintain a full-recomputation oracle from the beginning.
8. Add differential tests whenever an incremental rule is introduced.
9. Make claims proportional to implemented evidence.
10. Prefer a simple end-to-end vertical slice before optimizing individual
    components.
11. Treat the UI and reproducibility as core deliverables, not optional polish.
12. Preserve unrelated user changes in dirty worktrees.

## 25. Historical First Tasks and Current Handoff

The list below records the repository-bootstrap sequence that has already been
completed. It is not the current work queue. A current agent should follow the
milestone routing near the top of this document, preserve the frozen M3 model
as the default, and begin M5 only from an explicit new execution plan. The
larger independently adjudicated natural-history evaluation belongs on the
pre-dissertation/M6 schedule.

The original bootstrap tasks were:

1. Create `AGENTS.md` pointing to this onboarding document.
2. Create `docs/research_plan.md` containing the frozen title, research
   questions, hypotheses, and scope exclusions.
3. Create `docs/literature_matrix.md` comparing GroundLoop with FreshCache,
   HoH, ProvenanceGuard, GenProve, HUKA, Minimal Evidence Groups, and
   VectraFlow.
4. Scaffold the Python package and tests without implementing models yet.
5. Define versioned Pydantic/domain models and initial PostgreSQL migrations.
6. Implement an in-memory reference recomputation engine.
7. Implement event semantics for document insert, delete, and replace.
8. Add synthetic differential tests for alternative support witnesses.
9. Build the smallest static pipeline: one document, one answer, one claim,
   one verification record, one maintained answer state.
10. Only then select concrete embedding, generation, and verification models.

The first milestone is not a sophisticated model. It is a complete vertical
slice in which a document update causes an explainable, tested answer-status
delta.
