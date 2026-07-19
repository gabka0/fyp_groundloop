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

## Historical first task: audit barrier (complete)

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

The audit was committed, its contract corrections were accepted, and Wave 1
was integrated. `AUDIT.md`, `STATUS.md`, and `HANDOFF.md` retain that evidence.

## Current task: Wave 2 real PostgreSQL admission boundaries

Start from integrated `main` commit `b3623fe` or a current descendant. Preserve
the Wave 1 deterministic references and fake adapters.

Implement real PostgreSQL adapter boundaries for frozen lexical-v1 and
reverse-vector search:

- parameterize every runtime SQL value;
- use PostgreSQL `simple` tsvector/tsquery OR semantics and
  `ts_rank_cd(..., 32)`, ordered by score descending then claim ID;
- implement pgvector exhaustive search as an exact score-all/materialize/sort
  reference;
- implement HNSW as explicitly approximate, bind complete build/search
  provenance, inspect the physical index, and never claim exactness or
  deterministic rebuilds;
- add unique-schema live PostgreSQL tests that skip only when no test DSN is
  configured, including actual GIN/HNSW plan checks where feasible;
- do not download BGE, run text encoding, train a model, or begin the learned
  impact TARGET.

Do not add migrations or edit coordinator-owned persistence. The adapter owns
an explicit relation/index interface which the coordinator may later satisfy
in shared schema work. Run owned tests plus shared M4 contracts, Ruff, strict
mypy, compileall, diff and ownership checks. Commit code/tests first and the
updated `STATUS.md`/`HANDOFF.md` separately.

## Implementation ownership

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
