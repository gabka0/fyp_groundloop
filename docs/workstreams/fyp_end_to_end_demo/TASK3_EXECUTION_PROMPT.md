# GroundLoop FYP Task 3 Execution Prompt

Act as the GroundLoop unified end-to-end demo agent.

Work only in:

```text
/home/kassym/Desktop/groundloop-worktrees/fyp-end-to-end-demo
```

The branch is `codex/fyp-end-to-end-demo`, based on integrated Task 1 and Task
2 commit `bb3e42a`. Read `AGENTS.md` and every authority document it requires.
Read
`docs/workstreams/fyp_end_to_end_demo/TASK3_IMPLEMENTATION_PLAN.md` completely
and implement it without widening scope.

## Goal

Add `groundloop fyp-e2e-demo`: one command that publishes an actual M3 run,
activates that exact run through the Task 2 public API, executes production M4
`INSERT`, `DELETE`, and `REPLACE` events in the same schema, proves independent
recomputation/replay/provenance gates, and emits one canonical result plus a
short presentation summary.

## Exclusive edit manifest

Edit only:

- `src/groundloop/fyp_end_to_end_demo.py`;
- `src/groundloop/cli.py`;
- `tests/m4/fyp_end_to_end_demo/**`;
- `docs/fyp_end_to_end_demo.md`;
- `docs/workstreams/fyp_end_to_end_demo/**`; and
- the bounded Task 3 entry in `README.md`.

Treat Task 1, Task 2, M3/M4 maintenance, migrations, SQL oracles, models,
prompts, thresholds, datasets, training, M5, and every other worktree as
read-only. Do not stage, commit, push, reset, clean, or stash.

## Required implementation

1. Define typed canonical Task 3 result and event records.
2. Build the controlled documentation corpus and typed update payloads without
   hard-coding generated M3 answer or global claim IDs.
3. Install one unique disposable PostgreSQL schema and publish through the
   public M3 application/store.
4. Activate the exact returned `run_id` using
   `activate_published_m3_run()`.
5. Construct every runtime through `compose_activated_m3_runtime()` and execute
   typed `DynamicEventPlan` values through the production M4 application.
6. Require sealed events, Python/SQL/persisted equality, no open work, exact
   expected deterministic states, and event model-call counts.
7. Reconnect for activation and event replay with fail-on-call model ports;
   require zero work and unchanged complete database projections.
8. Hash the immutable M3 provenance projection before and after the history.
9. Write canonical JSON and print `PASS` only after pure validation.
10. Drop the schema unless `--keep-schema` was explicitly selected.

The deterministic backend is mandatory and uses production storage/runtime
with frozen injected model ports. The optional real backend may use only local
pinned artifacts with downloads disabled. It is diagnostic and must not be
reported as an AI-quality success.

## Deterministic contract

Require the following derived-ID trajectory:

```text
baseline: unsupported / unsupported
INSERT:   supported   / valid
DELETE:   supported   / valid
REPLACE:  refuted     / contradicted
```

Expected event embedding/verifier work is `1/1`, `0/0`, `1/1`. Discovery is
expected on every fresh event. Replays require zero discovery, embedding,
verifier request, and verifier backend calls.

## Verification

Run focused unit/parser/live PostgreSQL tests, Task 1 and Task 2 regressions,
relevant M3/M4 regressions, complete non-database tests, Ruff check/format,
strict mypy over all source, compileall, `git diff --check`, and path ownership.
Run the deterministic CLI once against disposable PostgreSQL and record its
canonical manifest hash outside the repository.

Obtain two independent final read-only audits on identical bytes. Repair any
P0/P1 finding and rerun affected plus regression gates before writing the
handoff.

## Finish boundary

Report only a bounded primary CLI demonstration exact relative to stored model
judgments. Keep objective truth, semantic completeness, model accuracy,
arbitrary-corpus support, representative savings, deployment, security, M5,
dashboard completion, and full-FYP completion explicitly pending.
