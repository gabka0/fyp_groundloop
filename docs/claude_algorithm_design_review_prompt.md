# Prompt for Claude: Adversarial Algorithm and Design Review of GroundLoop

You are acting as a senior database-systems and AI-systems researcher reviewing
an FYP design before implementation. Your job is not to endorse the proposal.
Your job is to determine whether its algorithms are coherent, whether IVM is
doing substantive work, what is already known, what is underspecified or wrong,
and what the strongest feasible design should be.

Be intellectually adversarial. If an idea is weak, decorative, incorrectly
formalized, infeasible, or merely a renamed standard technique, say so
directly. Do not preserve an idea just because the project documents currently
recommend it. Prefer a smaller defensible system over an ambitious but
unprovable one.

## Repository and required reading

The repository root is:

```text
/home/kassym/Desktop/groundloop
```

Read these files completely, in this order, before giving recommendations:

1. `AGENTS.md`
2. `docs/groundloop_fyp_agent_onboarding_context.md`
3. `docs/initial_technical_design.md`
4. `docs/research_plan.md`
5. `docs/architecture.md`
6. `docs/evaluation_protocol.md`
7. `docs/literature_matrix.md`
8. `docs/roadmap.md`
9. `docs/decision_log.md`
10. `docs/first_implementation_prompt.md`
11. `docs/m1_implementation_plan.md`

Treat `docs/initial_technical_design.md` as a design candidate, not an approved
specification. Inspect the current source tree only to understand how much has
already been implemented. Do not modify code or documents during this review.

## Project objective

GroundLoop maintains the grounding status of previously generated RAG answers
when a versioned document collection changes. Neural components produce atomic
claims, candidate evidence, support/refutation scores, and bounded evidence
groups. Exact incremental view maintenance begins only after these outputs are
stored as immutable, versioned semantic observations.

The intended exact claim is:

> Given identical active corpus versions, stored neural observations, decision
> policies, and bounded evidence structures, incremental maintenance produces
> the same claim and answer states as full relational recomputation.

Candidate discovery and verifier correctness remain empirical, not exact.

## Designs that require critical evaluation

Evaluate at least these proposed mechanisms:

1. Immutable neural score observations with versioned decision policies.
2. Signed integer delta maintenance for insertions, deletions, and replacement
   batches.
3. The factorized view hierarchy:

   ```text
   ChunkVersion
     -> ObservationDecision
       -> RequirementWitnessCount
         -> RequirementSatisfied
           -> GroupSatisfiedCount
             -> GroupComplete
               -> ClaimState
                 -> AnswerState
   ```

4. Zero-crossing propagation that stops when Boolean existence or completeness
   does not change.
5. Bidirectional Semantic Delta Join, or BSDJ:
   - exact reverse-dependency withdrawal for deleted evidence;
   - approximate reverse claim retrieval for inserted evidence;
   - candidate frontiers and reserve candidates;
   - exact downstream propagation after neural observations are materialized.
6. Risk-Bounded Neural IVM, or RB-NIVM:
   - schedule semantic jobs by expected stale-risk reduction per unit cost;
   - combine empirical scheduling with exact downstream maintenance.
7. Incremental policy changes using stored scores and score-range indexes.
8. Pending-work lower and upper grounding bounds during asynchronous neural
   evaluation.
9. Compact explanation certificates instead of materializing complete
   provenance polynomials.
10. Heavy-light execution for skewed source-to-claim or claim-to-evidence
    fanout.
11. Cost-based selection among direct incremental maintenance, selective
    verification, batched partition refresh, and full semantic refresh.
12. The proposed bounded OR-AND-OR evidence-group semantics.

## Mandatory research procedure

Conduct a fresh literature search using primary sources. Prefer peer-reviewed
database/NLP papers, official proceedings, and authoritative technical papers.
Search through the present date rather than relying only on the repository's
literature matrix.

At minimum, investigate the closest work in:

- classical and higher-order IVM;
- DBSP, differential dataflow, OpenIVM, and industrial refresh planning;
- factorized IVM, CROWN, dynamic conjunctive-query evaluation, and heavy-light
  partitioning;
- provenance semirings and dynamic provenance;
- materialized-view selection and adaptive/full-versus-incremental refresh;
- streaming vector search and continuous top-k maintenance;
- semantic/AI query operators and expensive-predicate scheduling;
- dynamic RAG, stale-answer detection, semantic caching, claim verification,
  evidence groups, answer provenance, and cascade repair;
- selective prediction, active testing, value-of-information scheduling, and
  risk-constrained inference.

For every novelty statement, classify it as one of:

```text
KNOWN TECHNIQUE
KNOWN COMBINATION IN A NEW DOMAIN
NONTRIVIAL ADAPTATION
PLAUSIBLY NOVEL HYPOTHESIS
NOT ESTABLISHED
```

Provide direct links and exact citations. Do not use author names as evidence
of correctness. Do not claim `first`, novel, optimal, or state of the art
without direct evidence.

## Part I: Reconstruct and formalize the proposed system

Before criticizing it, state the strongest precise interpretation of the
design.

1. Define all base relations and maintained views mathematically.
2. Define epoch, insertion, deletion, replacement, policy-change, and semantic
   observation events.
3. State set versus bag semantics and where signed multiplicities are needed.
4. Define the canonical claim and answer truth tables.
5. Define the meaning of pending lower and upper bounds.
6. Separate exact structured operations from approximate neural operations.
7. State the precise full-recomputation oracle.
8. Identify all hidden assumptions about independence, determinism, duplicate
   observations, passage identity, model versioning, and evidence-group
   correctness.

If the current proposal does not uniquely determine one of these, mark it
`UNDERSPECIFIED` and explain why the ambiguity matters.

## Part II: Algorithm-by-algorithm adversarial review

For each proposed mechanism, provide the following table:

| Field | Required analysis |
|---|---|
| Purpose | What problem it actually solves |
| Inputs and outputs | Exact data contract |
| Correctness property | What can be proved or differentially tested |
| Non-guarantees | What remains approximate or heuristic |
| Time complexity | Best, expected, and worst case where meaningful |
| Space complexity | Including indexes, frontiers, and provenance state |
| Skew behavior | What happens for a passage shared by many claims |
| Failure cases | Concrete counterexamples or adversarial workloads |
| Closest prior work | Primary-source comparison |
| Novelty classification | Using the required categories |
| FYP value | Research value relative to implementation cost |
| Recommendation | Keep, modify, postpone, or delete |

Do not give only asymptotic claims. Define variables such as:

```text
A  registered answers
C  registered claims
P  active passage versions
E  stored claim-passage observation edges
G  evidence groups
R  evidence requirements
W  requirement-witness edges
k  retrieval frontier size
L  reverse-discovery candidate budget
d_p dependency fanout of passage p
d_c evidence degree of claim c
B  neural-call or token budget
```

Then derive meaningful update-cost expressions using them.

## Part III: Search for correctness failures

Try to break the design. Construct minimal counterexamples for at least:

1. deleting one of several alternative witnesses;
2. replacing a document while its chunks are split or merged;
3. inserting evidence that contradicts an uncited old claim;
4. support and refutation arriving in the same epoch;
5. duplicate or correlated verifier observations;
6. a threshold change that moves observations between all three labels;
7. an arbitrary calibration-function change;
8. a source-trust policy change affecting many observations;
9. partial semantic-job failure followed by retry;
10. two corpus events completing semantic work out of order;
11. an evidence group with an empty, duplicated, or shared requirement;
12. a high-fanout passage affecting most registered answers;
13. a candidate frontier whose omitted item becomes the best witness;
14. refutation that requires multiple pieces of evidence rather than one;
15. a model or prompt version change;
16. stale observations reused across content-similar but nonidentical chunks;
17. a pending bound that appears safe but candidate discovery missed the true
    affected claim.

For every counterexample, say whether it breaks:

- relational correctness;
- semantic quality;
- completeness/freshness reporting;
- idempotence or transaction safety;
- only performance;
- or only the claimed novelty.

Propose the smallest correction and state its cost.

## Part IV: Evaluate whether IVM is genuinely central

Answer directly:

1. Is the maintained query nontrivial enough for a database FYP?
2. Where does IVM provide an asymptotic or measured benefit beyond caching?
3. Which zero-crossing or factorization cases demonstrate that benefit most
   clearly?
4. Could PostgreSQL indexes and ordinary application updates achieve almost the
   same result? If yes, what must GroundLoop add to remain technically strong?
5. Would using Feldera/DBSP make the custom maintenance engine redundant?
6. Which components are database research and which are normal application
   engineering?
7. What result would convince a skeptical database professor?
8. What result would convince a skeptical AI professor?

Give an explicit `STRONG`, `ADEQUATE`, or `WEAK` rating for the current database
contribution and AI contribution, with reasons.

## Part V: Improve the algorithms

Propose concrete improvements, not vague directions. For each improvement,
provide:

- modified relations or state;
- updated delta rule or pseudocode;
- required index or physical structure;
- correctness effect;
- expected cost effect;
- new failure modes;
- implementation effort;
- experiment that would validate it.

Consider, but do not automatically accept:

- threshold/range indexes for policy deltas;
- top-k or skyline maintenance for reserve evidence;
- batched delta processing;
- fanout-aware heavy/light partitioning with hysteresis;
- workload-adaptive materialized views;
- source-level or claim-cluster factorization;
- uncertainty-aware or value-of-information scheduling;
- conservative barriers before republishing changed answers;
- periodic audits to estimate missed-impact risk;
- stratified exploration in the semantic scheduler;
- interval or three-valued semantics for pending work;
- bounded provenance antichains or minimal witness certificates;
- sharing equivalent claims across answers;
- semantic-result reuse based on exact content hashes;
- hybrid lexical/entity/vector impact discovery;
- cost-based switching to full refresh.

Reject improvements whose complexity is not justified by the likely FYP
workload.

## Part VI: Decide the final FYP design

Produce a ranked decision table with these categories:

```text
CORE: required for the thesis claim
TARGET: implement if core is stable
STRETCH: useful research extension
POST-FYP: explicitly exclude
DELETE: not useful or not defensible
```

The final core must be implementable by one student and must produce a complete
demo and rigorous evaluation. State what should be removed if the schedule
slips by 25 percent and by 50 percent.

Recommend one primary research contribution and no more than two secondary
contributions. Phrase each as a falsifiable contribution, not marketing.

Also provide:

- the final exact claim;
- the final empirical claims;
- claims that must not be made;
- the most defensible project title;
- a one-paragraph positioning statement against the closest work;
- explicit confidence levels for all major judgments.

## Part VII: Produce the step-by-step execution plan

Only after completing the critique and final design, create an implementation
and research plan. Do not merely repeat the current roadmap.

For every phase include:

1. objective;
2. design decisions that must already be frozen;
3. concrete files/modules or database objects to create;
4. algorithms to implement;
5. unit, property, differential, integration, and failure-injection tests;
6. datasets or synthetic workloads;
7. baselines and ablations;
8. metrics and expected plots/tables;
9. completion gate;
10. risks and fallback scope;
11. dependencies on later phases;
12. estimated relative effort: `S`, `M`, `L`, or `XL`--do not invent calendar
    durations without knowing the student's availability.

The plan must have at least these logical stages, but you may reorder or merge
them if justified:

- design freeze;
- deterministic reference semantics;
- signed relational delta engine;
- persistent database/event model;
- static AI pipeline;
- dynamic impact discovery;
- bounded evidence groups;
- risk/cost scheduling;
- optimized physical execution;
- end-to-end application;
- experiments and dissertation artifacts.

For the first implementation stage, give acceptance tests precise enough that
another coding agent can implement them without making new semantic decisions.

## Required final response structure

Use this exact top-level structure:

1. **Executive verdict**
2. **Reconstructed formal design**
3. **Fatal or high-priority problems**
4. **Algorithm-by-algorithm review**
5. **Counterexamples and corrections**
6. **IVM centrality assessment**
7. **Novelty and closest-work analysis**
8. **Recommended algorithmic improvements**
9. **Final scoped design**
10. **Ranked contribution statement**
11. **Step-by-step implementation and evaluation plan**
12. **Open decisions requiring the student's choice**
13. **Primary-source bibliography**

End with a short decision block:

```text
PROCEED WITH GROUNDLOOP: YES / YES WITH MAJOR CHANGES / NO
DATABASE CONTRIBUTION: STRONG / ADEQUATE / WEAK
AI CONTRIBUTION: STRONG / ADEQUATE / WEAK
TOP ALGORITHMIC IDEA TO KEEP:
TOP IDEA TO MODIFY:
TOP IDEA TO DROP:
FIRST IMPLEMENTATION MILESTONE:
OVERALL CONFIDENCE:
```

Do not edit the repository. Deliver the complete review first so the student
can decide which recommendations to accept.
