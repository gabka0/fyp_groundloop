# M4 Epoch/Runtime Lane Agent Prompt

Work only in:

```text
/home/kassym/Desktop/groundloop-worktrees/m4-epoch-runtime
```

Branch:

```text
workstream/m4-epoch-runtime
```

Read `AGENTS.md`, `docs/technical_design.md`,
`docs/m4_implementation_plan.md`, and
`docs/m4_multiagent_execution_plan.md` completely before acting.

## First task: audit only

Do not implement code in the first turn. Inspect the M1–M3 epoch,
repository, event, incremental and PostgreSQL implementations. Write only:

```text
docs/workstreams/m4_epoch_runtime/AUDIT.md
```

Determine whether the proposed dynamic job DAG, replacement epoch, exact
withdrawal and frontier semantics preserve D-19/D-20, immutability, replay and
failure atomicity. Focus on the existing fixed `required_job_ids` contract.
Try to construct failures involving child expansion, crash after inference,
duplicate completion, inactive chunks, empty frontier, and half replacement.

Use P0/P1/P2 findings with file:line evidence, exact contract corrections and
falsifying tests. Commit the audit and stop for the coordinator's contract
freeze. Do not change implementation during the audit barrier.

## Implementation ownership after coordinator release

- `src/groundloop/m4/runtime/**`
- `tests/m4/runtime/**`
- `sql/m4/runtime/**`
- `docs/workstreams/m4_epoch_runtime/**`

Implement only after the coordinator gives the M4 contract-baseline commit and
instructs you to rebase.

Expected later work:

- pure job-DAG transition engine;
- atomic parent-completion/child-declaration plans;
- stable job IDs and child-set closure;
- exact reverse-dependency withdrawal planner;
- frontier transitions and repair planner;
- PENDING, retry, conflict and late-inactive behavior;
- property/state-machine/crash tests;
- SQL fragments and index requirements, not migration edits.

## Forbidden paths

Do not edit shared contracts, `migrations/**`, CLI, pipeline, persistence,
existing M1–M3 modules, admission/oracle code, `AGENTS.md`, roadmap, decision
log or shared M4 docs. Request contract changes under:

```text
docs/workstreams/m4_epoch_runtime/contract_requests/
```

Do not merge. Commit only owned changes, run lane gates, write `STATUS.md` and
`HANDOFF.md`, and report exact commit hashes and command results.

