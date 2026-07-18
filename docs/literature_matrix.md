# GroundLoop Literature Matrix

Last reviewed: 2026-07-17 (updated after the M0.5 adversarial review's fresh
literature search; see `docs/claude_algorithm_design_review.md` Section 13 for
the verified primary-source bibliography)

This is a starting map, not an exhaustive systematic review. Refresh it before
making novelty claims because this area changes rapidly.

| Work | Relevant contribution | Overlap with GroundLoop | GroundLoop distinction or use |
|---|---|---|---|
| [FreshCache](https://arxiv.org/abs/2607.04281) | Risk-constrained freshness-aware semantic caching for open-web RAG | Selective reuse or refresh of potentially stale answers | Maintain claim-level evidence dependencies, explicit verdict deltas, and alternative evidence rather than only cache-entry freshness risk |
| [HoH](https://aclanthology.org/2025.acl-long.301/) | Dynamic benchmark for outdated information in RAG | Evolving evidence and stale answers | Candidate benchmark source; GroundLoop studies persistent claim-level maintenance and saved neural recomputation |
| [ProvenanceGuard](https://arxiv.org/abs/2606.18037) | Source-aware atomic-claim verification for MCP outputs | Claim decomposition, provenance, NLI verification | Maintain those semantic judgments across document updates |
| [GenProve](https://aclanthology.org/2026.acl-long.228/) | Fine-grained generation-time provenance | Structured claim-to-source attribution | Potential producer of initial provenance; GroundLoop focuses on update-time maintenance |
| [HUKA](https://arxiv.org/abs/2007.14864) | Incremental provenance for standing queries on dynamic knowledge graphs | Persistent answers, alternative derivations, dynamic provenance | Text requires versioned neural predicates and semantic impact discovery rather than a fully structured KG |
| [Minimal Evidence Groups](https://aclanthology.org/2025.trustnlp-main.8/) | Identifies alternative minimal sets of evidence that collectively support a claim | Conjunctive and alternative evidence | Foundation for GroundLoop's bounded evidence-group maintenance |
| [VectraFlow](https://vldb.org/cidrdb/2025/vectraflow-integrating-vectors-into-stream-processing.html) | Streaming vector filters, top-k, and joins | Continuous candidate maintenance | Vector maintenance is an enabling component, not GroundLoop's novelty |
| [SPEAR](https://www.vldb.org/cidrdb/2026/making-prompts-first-class-citizens-for-adaptive-llm-pipelines.html) | Structured, versioned prompt views and adaptive refinement | Versioned LLM-pipeline artifacts | GroundLoop maintains answer grounding, not prompt definitions |
| [F-IVM](https://arxiv.org/abs/1703.07484) | Factorized higher-order incremental view maintenance | IVM foundation and factorized state | Possible later optimization for repeated GroundLoop joins and aggregates |
| [HoVer](https://aclanthology.org/2020.findings-emnlp.309/) | Many-hop fact extraction and verification | Multi-document reasoning | Useful future GroundGraph extension; arbitrary recursive reasoning is outside FYP scope |
| [EX-FEVER](https://aclanthology.org/2024.findings-acl.556/) | Explainable multi-hop fact verification dataset | Multi-hop evidence and explanations | Possible bounded evidence-group evaluation, not proof of dynamic maintenance |
| [Belief-R](https://aclanthology.org/2024.emnlp-main.586/) | Tests LLM belief revision under new evidence | Conclusions changing under evolving evidence | Motivation and possible auxiliary evaluation; GroundLoop externalizes state instead of editing model beliefs |
| [MemoRepair](https://arxiv.org/abs/2605.07242) | Cascade repair for derived agent-memory artifacts | Dependency repair after source invalidation | General agent memory and executable-procedure repair are excluded from GroundLoop; MemoRepair assumes complete influence provenance, GroundLoop's admission discovery is approximate and measured |
| [MemStrata](https://arxiv.org/abs/2606.26511) | Deterministic (subject, relation, object) supersession retiring stale facts in a bi-temporal ledger | Closest in spirit to GroundLoop's exact layer; eliminates stale-fact serving | Operates on fact triples with a fixed supersession rule, not generated answers with claim-evidence dependency structure, alternative/conjunctive evidence, or budgeted re-verification; mandatory head-to-head discussion in related work |
| [STALE](https://arxiv.org/abs/2605.06527) | Benchmark showing LLM agents cannot reliably detect invalidated memories (best model 55.2%) | Motivation for externalized grounding state | GroundLoop externalizes and maintains state rather than relying on model self-detection |
| [Heavy-Light Partitioning IVM](https://arxiv.org/abs/2605.08397) | O(sqrt(N)) update maintenance for join queries via heavy-light key partitioning | IVM under updates | Its asymptotic case targets cyclic join maintenance; GroundLoop's hierarchy is acyclic and already O(1) per affected edge — heavy-light removed from scope (D-13) |
| [Semirings in IVM](https://arxiv.org/abs/2606.07795) | Insert-only IVM complexity dichotomy depends on the semiring | Algebra choice for payloads | Insert-only results do not transfer to GroundLoop's deletion-heavy workload; supports keeping signed integer counts and rejecting probabilistic payloads in the FYP |
| [Enzyme](https://arxiv.org/abs/2603.27775) | Cost-based refresh planning for data-engineering pipelines | Full-vs-incremental refresh choice | GroundLoop's two-rule refresh switch is a scoped instance; Enzyme does not decide which neural observations to acquire |
| [Active Testing via Neyman Allocation](https://arxiv.org/abs/2605.10075) / [LLM-as-Judge on a Budget](https://arxiv.org/abs/2602.15481) | Budgeted allocation of expensive model evaluations | Verifier-call budgeting | Prior art for any scheduler claim; GroundLoop's STRETCH scheduler is a transparent priority with stratified audit, positioned against this line |

## Defensible Positioning

> Existing work studies answer freshness, claim verification, generation-time
> provenance, dynamic structured provenance, streaming vectors, and evidence
> grouping. GroundLoop investigates how versioned neural judgments and
> relational dependencies can be combined to maintain the grounding of
> previously generated answers while minimizing semantic recomputation.

## Claims Not Supported by This Matrix

- GroundLoop is the first self-updating RAG system.
- GroundLoop is the first dynamic claim-verification system.
- GroundLoop is the first use of IVM with AI or neural predicates.
- Existing systems cannot handle document updates.
- GroundLoop establishes objective truth.

## Review Questions for Each New Paper

1. What is the maintained object: cache entry, context, claim, reasoning graph,
   answer, prompt, or model state?
2. Are updates insertions, deletions, replacements, time passage, model changes,
   or tool changes?
3. Does it maintain old outputs or only answer new queries?
4. Does it selectively reduce neural inference?
5. What is exact and what is approximate?
6. Does it preserve alternative or conjunctive evidence?
7. What code, datasets, and baselines are available?
