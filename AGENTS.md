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
35. `docs/m5_design_freeze.md` (authoritative M5 semantics and theorems)
36. `docs/m5_implementation_plan.md` (M5.1--M5.6 stage gates)
37. `docs/m5_multiagent_execution_plan.md` (active path-exclusive ownership)
38. `docs/m5_acceptance_matrix.md` (contract and executable falsifiers)
39. `docs/m5_implementation_status.md` (current M5 evidence boundary)
40. `docs/workstreams/m5_runtime_contract/RECOVERY_WORK_AMENDMENT.md`
    (authoritative M5-D24 recovery, accounting, and migration-016 contract)
41. `docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT.md`
    (authoritative M5-D25 persisted-matching and migration-017 contract)
42. `docs/workstreams/m5_runtime_implementation/D25_CONTRACT_FREEZE_HANDOFF.md`
    (accepted D25 bytes, review evidence, and next implementation boundary)
43. `docs/workstreams/m5_runtime_contract/CHANGED_STATE_ABSENCE_AMENDMENT.md`
    (authoritative M5-D26 retired-state absence-reference contract)
44. `docs/workstreams/m5_runtime_implementation/D26_CONTRACT_FREEZE_HANDOFF.md`
    (accepted D26 bytes, review evidence, and next implementation boundary)
45. `docs/workstreams/m5_runtime_contract/REQUIREMENT_STATE_COUNTER_ERRATUM.md`
    (authoritative M5-D27 requirement-state counter-ownership correction)
46. `docs/workstreams/m5_runtime_implementation/D27_CONTRACT_FREEZE_HANDOFF.md`
    (accepted D27 bytes, review evidence, and next implementation boundary)
47. `docs/workstreams/m5_runtime_contract/PHASED_PERSISTED_MATCHING_COMPOSITION_AMENDMENT.md`
    (authoritative M5-D28 phased persisted-matching composition and replay
    projection correction)
48. `docs/workstreams/m5_runtime_implementation/D28_CONTRACT_FREEZE_HANDOFF.md`
    (accepted D28 bytes, review evidence, and next implementation boundary)
49. `docs/workstreams/m5_runtime_contract/BOUNDED_DOCUMENT_WITHDRAWAL_AMENDMENT.md`
    (authoritative M5-D29 persisted-source and bounded document-withdrawal
    correction)
50. `docs/workstreams/m5_runtime_implementation/D29_CONTRACT_FREEZE_HANDOFF.md`
    (accepted D29 bytes, review evidence, and next implementation boundary)
51. `docs/workstreams/m5_runtime_contract/DIRECT_M4_PROVENANCE_CLOSURE_AMENDMENT.md`
    (authoritative M5-D30 total claim-currency and direct-M4/M3 provenance
    closure correction)
52. `docs/workstreams/m5_runtime_implementation/D30_CONTRACT_FREEZE_HANDOFF.md`
    (accepted D30 bytes, review evidence, and next implementation boundary)
53. `docs/workstreams/m5_runtime_contract/PRETERMINAL_CONTEXT_ACCESS_AMENDMENT.md`
    (authoritative M5-D31 trusted preterminal context-access correction)
54. `docs/workstreams/m5_runtime_implementation/D31_CONTRACT_FREEZE_HANDOFF.md`
    (accepted D31 bytes, review evidence, and sequential implementation
    boundary)

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

M5.0 through M5.3 are complete and M5.4 is active. The authoritative M5
contract replaces the
old global-union/group-count sketch with exact bounded covering matching,
historical currency, immutable certificate artifacts, independent Python/SQL
oracles, typed v2 runtime identities, and a controlled WiCE mapping. Do not
implement from the older technical-design pseudocode where it conflicts with
`docs/m5_design_freeze.md`. M5-D24 is frozen and authorizes migration 016 plus
recoverable dispatch and durable work/timing accounting; its implementation
evidence remains pending. M5-D25 persisted matching is now a frozen contract at
`docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT.md`, exact
accepted SHA-256
`bac12ab5e74632c04f1bd70d0ef0d00522ba9d268eb8b73d11845bbf3b873aae`.
Its contract gate is `PASS` and its implementation evidence is `PENDING`;
migration 017 and every source/test change require a new path-exclusive
activation. M5-D26 is the frozen narrow correction for changed-state
references whose exact structural `REPLACE`/`RETIRE` result is absence. Its
authoritative amendment SHA-256 is
`85372d4c2f9108810bd75c3e5611de541d0f31c8a096421f30e68fad84676721`;
M5-D26 and M5.0-26 are contract-`PASS` / implementation-`PENDING`. It adds no
reference kind, tombstone, nullable hash, present-state recipe, or deployment
claim. Migration 017 may replace the one migration-015 child validator only
under the separate D26 implementation activation. M5-D27 is the frozen
wording-only correction that confirms there is no dedicated requirement-state
physical-row coordinate in D24 `M5RuntimeWork`. Its authoritative SHA-256 is
`7a51afc1f1b6c249572222a023814de8b22220058e00f09564de64f552bd411c`;
M5-D27 and M5.0-27 are contract-`PASS` / implementation-`PENDING`. It adds no
counter, DTO, digest, schema, migration, alias, replay field, or measurement.
Requirement-state changes remain exact D25 patch/bijection evidence, while any
transaction-local write count is diagnostic only. Runtime remains `v1_only`
outside isolated fixtures. M5-D28 is the frozen narrow correction for phased
composition of D24 accounting with D25 persisted matching, including the
direct path whose immutable M4 source is inserted at tier 15c, and for exact
historical replay from the retained changed-key projection. Its authoritative
SHA-256 is
`8a2bafd3478cf2cac6ac7c8de7ca7779a6d9ace7fbbe98a7dc3ff08afdb67eae`;
M5-D28 and M5.0-28 are contract-`PASS` / implementation-`PENDING`. It adds no
public API, DTO, digest, schema, migration, source/reference kind, counter,
measured value, or runtime-mode change. M5-D29 is the frozen narrow correction
for persisted legacy-document source identity, bounded reverse/current
withdrawal enumeration, retained declaration completeness, the checked
hydration terminal cutoff, and supported same-policy withdrawal. Its
authoritative SHA-256 is
`e05f159f98d5f282335a90d2e9db1a26f8d85560030314060d918df59ccc82fb`;
M5-D29 and M5.0-29 are contract-`PASS` / implementation-`PENDING`. At the D29
contract checkpoint it authorized only the future migration-018 contract with
exactly two locator indexes; no migration, installer, source, or test bytes had
yet been accepted, and runtime-addendum revision 10 was authoritative. Cross-
policy withdrawal remains `PENDING`. The same-policy
fail-closed rule is D29's sole semantic-admissibility supersession;
activation-bootstrap observations remain supported, typed rootless
`ObserveRequirementEvent` remains `PENDING`, and terminal D24-valid unresolved
dispatch history remains late-audit ambiguity. The later D29 activation and
migration-018 work are historical implementation evidence; held Lane-P bytes
remain non-authority until a new activation. M5-D30 is the frozen narrow
correction that makes changed-chunk current-observation enumeration total
across requirement and claim subjects and validates claim holders through
either exact dynamic direct-M4 closure or exact activation-base M3 closure. Its
authoritative
SHA-256 is
`db2568affc02cf1ca6f17a549029f31089cecd857debdedf2651e6aac6898fe4`;
M5-D30 and M5.0-30 are contract-`PASS` / implementation-`PENDING`. It uses the
persisted working delta as the dynamic observation-to-child link, preserves
optional M4 execution and arbitrary retained identities, and does not
reconstruct unavailable root hit sets or classic artifacts. It adds no public
API, DTO, digest, counter, schema object, migration 019, model/provider change,
or runtime-mode change; migrations 001--018 remain exact. Runtime-addendum
revision 11 is authoritative. Public store/runtime composition requires a new
path-exclusive D30 activation from the pushed D30 authority barrier; every
earlier activation is historical and insufficient for this corrected scope.
M5-D24 through M5-D30, Task 2, M5.4 and later gates,
deployment, and AI-quality claims remain `PENDING`; runtime remains `v1_only`.
M5-D31 is the frozen narrow correction for the genuine migration-017
preterminal promotion context when the supported runtime role is not the
trusted temporary-table owner. Its authoritative SHA-256 is
`6331c8149e38031c51cb22b6a9dc2d49d30d27df67d25b5fae24b00c52058dc9`;
M5-D31 and M5.0-31 are contract-`PASS` / implementation-`PENDING`, and
runtime-addendum revision 12 is authoritative. D31 supersedes only D30's
negative migration-019/schema-function boundary: migrations 001--018 remain
exact, while one future additive migration 019 may create exactly the trusted,
read-only
`groundloop_m5_matching_read_preterminal_seal_context(bigint,bigint,bigint)`
function and explicit `PUBLIC EXECUTE`, with no other schema object or
privilege. Acceptance implements neither the migration nor the accessor. Only
after the pushed D31 authority barrier may a separate path-exclusive
activation allocate the required sequential schema-019, revised C1-R, and
resumed C1 lanes; the current C1-R candidate remains read-only evidence until
the schema lane integrates. M5-D24 through M5-D31, Task 2, M5.4 and later
gates, deployment, security, utility, and AI-quality claims remain `PENDING`;
runtime remains `v1_only`.
M5 is not complete until every remaining M5.4--M5.6 executable gate is
recorded.

A larger, independently adjudicated natural-history evaluation remains
mandatory before dissertation-level selective-maintenance or end-to-end
utility claims; schedule that work in pre-dissertation/M6 rather than silently
treating the M4 pilot or retrospective WiCE mapping as conclusive.

The older M4 and M4.1 multi-agent plans are historical ownership records.
Future parallel work requires a new explicit disjoint-path manifest; it does
not inherit permission to reopen completed contracts or edit another active
lane's files.
