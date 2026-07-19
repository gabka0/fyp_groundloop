# M4 Oracles/Evaluation Lane Agent Prompt

Work only in:

```text
/home/kassym/Desktop/groundloop-worktrees/m4-oracles-evaluation
```

Branch:

```text
workstream/m4-oracles-evaluation
```

Read `AGENTS.md`, `docs/technical_design.md`,
`docs/evaluation_protocol.md`, `docs/m2_implementation_status.md`,
`docs/m3_implementation_status.md`, `docs/m4_implementation_plan.md`, and
`docs/m4_multiagent_execution_plan.md` completely before acting.

## First task: audit only

Do not implement code in the first turn. Write only:

```text
docs/workstreams/m4_oracles_evaluation/AUDIT.md
```

Audit pair-positive, complete-state-affected, status-affected and
answer-affected definitions. Determine what full pair audit and full semantic
refresh each establish, whether the proposed sets are actually nested, and how
depth `k` makes the baseline policy-relative. Inspect existing M1/M2 Python and
SQL oracles for accidental code sharing. Design a deliberate selective-miss
test that a circular oracle would fail.

Audit workload splits, baselines, event-level statistics and resource
feasibility. Use P0/P1/P2 findings with file:line evidence, exact contract
corrections and falsifying tests. Commit the audit and stop for the
coordinator's contract freeze. Do not implement or modify evaluation code
during the audit barrier.

## Implementation ownership after coordinator release

- `src/groundloop/m4/oracles/**`
- `tests/m4/oracles/**`
- `experiments/m4/oracles/**`
- `experiments/m4/evaluation/**`
- `configs/m4/evaluation/**`
- `docs/workstreams/m4_oracles_evaluation/**`

Implement only after the coordinator gives the M4 contract-baseline commit and
instructs you to rebase.

The full-pair and full-refresh paths must not import selective admission or
runtime delta logic. Deliberately injected admission misses must be visible.
Every metric must retain corpus, event, verifier, policy and split provenance.

## Forbidden paths

Do not edit shared contracts, migrations, runtime/admission modules, existing
M1–M3 engines, CLI, pipeline, roadmap, decision log or shared M4 docs. Request
contract changes under:

```text
docs/workstreams/m4_oracles_evaluation/contract_requests/
```

Do not merge. Commit only owned changes, run lane gates, write `STATUS.md` and
`HANDOFF.md`, and report exact commit hashes and command results.

