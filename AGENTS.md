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
