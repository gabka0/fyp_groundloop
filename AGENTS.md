# GroundLoop Agent Instructions

This repository contains the GroundLoop FYP project. Before proposing
architecture, editing code, or making research claims, read these documents in
order:

1. `docs/agent_expert_operating_principles.md`
2. `docs/groundloop_fyp_agent_onboarding_context.md`
3. `docs/technical_design.md` (v0.2, frozen — the authoritative design)
4. `docs/claude_algorithm_design_review.md` (why v0.2 is shaped this way)
5. `docs/research_plan.md`
6. `docs/architecture.md`
7. `docs/evaluation_protocol.md`
8. `docs/literature_matrix.md`
9. `docs/roadmap.md`
10. `docs/decision_log.md`
11. `docs/local_environment_status.md`
12. `docs/first_implementation_prompt.md`
13. `docs/m1_implementation_plan.md`
14. `docs/m1_1_hardening.md`
15. `docs/m2_implementation_status.md`
16. `docs/m3_model_dataset_audit.md`
17. `docs/m3_design_freeze.md`
18. `docs/m3_multiagent_execution_plan.md`
19. `docs/m3_implementation_status.md`
20. `docs/m4_design_freeze.md` (authoritative M4 contracts)
21. `docs/m4_implementation_plan.md` (phasing; freeze overrides its proposal)
22. `docs/m4_multiagent_execution_plan.md` (ownership and contract barriers)
23. `docs/m4_implementation_status.md` (implemented evidence and next gate)
24. `docs/m4_1_multiagent_implementation_plan.md` (historical M4.1 ownership)
25. `docs/m4_1_acceptance_matrix.md` (accepted deterministic/live contract)
26. `docs/m4_7_physical_runtime_plan.md` (measured-kernel scope and gates)
27. `docs/workstreams/m4_7_complexity_proof/README.md` (corrected theorem)
28. `docs/workstreams/m4_8_real_dynamic_history/HANDOFF.md` (real-model history)
29. `docs/workstreams/m4_9_empirical_study/HANDOFF.md` (controlled study boundary)
30. `docs/workstreams/m4_11_physical_history_gate/README.md` (history matrix)
31. `docs/workstreams/m4_10_real_history_study/HANDOFF.md` (executed
    natural-history pilot)
32. `docs/workstreams/m4_12_public_ai_gate/README.md` (executed frozen-M3 AI
    diagnostic)
33. `docs/m4_13_change_aware_verifier_plan.md` (preregistered adaptation
    design and gates)
34. `docs/workstreams/m4_13_change_aware_verifier/RESULTS.md` (executed
    adaptation result and M4 verdict)

`docs/initial_technical_design.md` (v0.1) is superseded and retained for audit
only. The M0.5 design freeze and M1.1 amendments are complete; frozen decisions
D-1..D-20 are in
the decision log and v0.2 Section 19. M1 and the M1.1 hardening pass are
complete. Changing a frozen decision requires a new decision-log entry.

## Non-Negotiable Boundaries

- GroundLoop maintains claim grounding relative to a versioned evidence
  collection and versioned model judgments. It does not maintain objective
  truth.
- Raw text is not treated as a deterministic relational view. Neural inference
  creates versioned semantic observations; exact IVM begins after those
  observations are stored.
- The FYP uses bounded evidence-group DAGs, not arbitrary recursive reasoning
  graphs.
- Keep the full-recomputation reference path from the first implementation.
- Every incremental rule requires differential tests against full relational
  recomputation.
- Do not claim `first`, state of the art, or publication-level novelty without
  a fresh literature review and supporting experiments.
- Do not import Dynagox, Secure CROWN, ORAM, TEE, MPC, or MP-SPDZ code unless a
  measured requirement and explicit user decision justify it.
- Do not claim that GroundLoop is secure merely because it is conceptually
  related to Secure CROWN.
- Preserve unrelated user changes and do not commit generated datasets, model
  weights, secrets, database volumes, or build caches.

## Development Posture

- Begin with one complete vertical slice: one document, one answer, one claim,
  one verification observation, one update, and one answer-status delta.
- Prefer explicit versions, immutable observations, deterministic reference
  functions, and inspectable provenance.
- Separate system correctness metrics from neural quality metrics.
- Optimize only after recording a reproducible baseline.
- Use type hints, tests, small modules, and configuration files rather than
  hard-coded thresholds or model identifiers.

## Initial Validation

```bash
python3 -m pytest
python3 -m compileall src tests
```

M1, M1.1, and M2 are implemented and tested. M2 includes the signed-delta
engine, differential harness, semantic-epoch coordinator, live PostgreSQL
third oracle, structured baselines, and the exact-flip policy-index prototype.
Consult `docs/m2_implementation_status.md` and `docs/roadmap.md` before
starting M3 or expanding scope.

M3 is complete. It includes the static real-model pipeline, a fine-tuned and
temperature-calibrated verifier, atomic PostgreSQL publication, exact replay,
and the top-level CLI. Read `docs/m3_implementation_status.md` before starting
M4. The M3 lane prompts and ownership protocol remain historical execution
evidence, not authorization to reopen frozen M3 contracts silently.

M4 audit and contract correction are complete. The authoritative contract is
`docs/m4_design_freeze.md`; the earlier plan is retained for phasing and audit
history. Do not implement an older proposal where it conflicts with the
freeze.

The original M4 audit and Wave 1/2 work used three path-exclusive lanes under
`docs/m4_multiagent_execution_plan.md`. That plan is historical evidence, not
an active ownership grant. No future lane may cross the audit/contract barrier
or edit another active lane's paths without a new coordinator-approved
manifest.

M4 is closed. The M4 implementation gates through M4.11 integrated PostgreSQL
persistence, application and CLI composition, durable model provenance, exact fresh
fallback, a point/CAS measured runtime, signed evaluation counters,
affected-key state patches, sparse publication, adversarial physical-history
tests, one pinned-model insert/delete/replace history and the controlled
evaluation harness. M4.10 then executed the naturally versioned real-history
pilot; M4.12 measured the frozen verifier on a revision-sensitive public
diagnostic; M4.13 executed the preregistered adaptation and returned `NO_GO`.
Read `docs/m4_implementation_status.md` and the M4.10/M4.12/M4.13 result docs
before proposing work or making performance or AI-quality claims.

Do not describe the original linear-looking whole-kernel expression in
`docs/m4_design_freeze.md` Section 13 as proved. The explicit 2026-07-20
decision-log amendment and
`docs/workstreams/m4_7_complexity_proof/README.md` reject it for the composed
implementation. The supported bound includes affected-accumulator/witness
work `G`, ordered score-index work, canonical sorting, bytes and PostgreSQL
index/I/O/WAL/lock costs. No superiority over DBSP, F-IVM, CROWN or another
named system follows.

The M4.9 controlled study remains reproducible evaluation plumbing, not
real-model quality or latency evidence. M4.10 executed the real-history path,
but its three histories, ten fixture-author claims and fourteen exhaustive
pairs have no independent human adjudication; its poor non-exhaustive recall
is a bounded negative result, not a population estimate. M4.12 confirmed that
the frozen M3 verifier was weak on fine-grained revisions. M4.13 improved the
held-out VitaminC revision metrics but failed preregistered retention gate G8,
so it is not promoted: the frozen M3 checkpoint remains the default.

M5 is unblocked but not started. A larger, independently adjudicated natural-
history evaluation remains mandatory before dissertation-level selective-
maintenance or end-to-end utility claims; schedule that work in pre-
dissertation/M6 rather than silently treating the M4 pilot as conclusive.

The older M4 and M4.1 multi-agent plans are historical ownership records.
Future parallel work requires a new explicit disjoint-path manifest; it does
not inherit permission to reopen completed contracts or edit another active
lane's files.
