# M4 Impact-Admission Lane Agent Prompt

Work only in:

```text
/home/kassym/Desktop/groundloop-worktrees/m4-impact-admission
```

Branch:

```text
workstream/m4-impact-admission
```

Read `AGENTS.md`, `docs/technical_design.md`,
`docs/m3_model_dataset_audit.md`, `docs/m3_implementation_status.md`,
`docs/m4_implementation_plan.md`, and
`docs/m4_multiagent_execution_plan.md` completely before acting.

## First task: audit only

Do not implement code or train a model in the first turn. Write only:

```text
docs/workstreams/m4_impact_admission/AUDIT.md
```

Audit reverse BGE geometry, deterministic lexical retrieval, rank fusion,
global `L`, mandatory lineage, pair deduplication, policy identity, and the
learned-impact TARGET. Determine whether the proposed target labels, grouped
splits, hard negatives, human audit and fixed-budget evaluation are valid.
Inspect the actual M3 adapters and verifier evidence rather than assuming the
documents are correct.

Use P0/P1/P2 findings with file:line evidence, exact contract corrections and
falsifying tests. Explicitly decide whether verifier improvement must precede
learned admission. Commit the audit and stop for the coordinator's contract
freeze. Do not download or train models during the audit barrier.

## Implementation ownership after coordinator release

- `src/groundloop/m4/admission/**`
- `tests/m4/admission/**`
- `training/m4_impact/**`
- `configs/m4/impact/**`
- `experiments/m4/admission/**`
- `docs/workstreams/m4_impact_admission/**`

Implement only after the coordinator gives the M4 contract-baseline commit and
instructs you to rebase.

CORE work comes first: deterministic lexical, reverse-vector, fusion, lineage,
deduplication, fixed budgets, fake adapters and policy manifests. The learned
dual encoder is a later TARGET after the oracle lane supplies frozen
development labels.

## Forbidden paths

Do not edit shared contracts, migrations, M3 retrieval/verifier code, epochs,
runtime/frontier persistence, full oracles, CLI, pipeline, roadmap, decision
log or shared M4 docs. Request contract changes under:

```text
docs/workstreams/m4_impact_admission/contract_requests/
```

Do not merge. Commit only owned changes, run lane gates, write `STATUS.md` and
`HANDOFF.md`, and report exact commit hashes and command results. Ordinary
tests perform no download. Real model work requires coordinator scheduling.

