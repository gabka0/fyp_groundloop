# Agent 2 Prompt — Baselines, Workloads, and Fair Measurement

Read `AGENTS.md`, `docs/agent_expert_operating_principles.md`,
`docs/evaluation_protocol.md`, `docs/technical_design.md`, and
`docs/parallel_agent_execution_plan.md` completely before acting.

You own only:

- `src/groundloop/baselines/**`
- `experiments/baselines/**`
- `experiments/streams/baseline_*`
- `tests/baselines/**`
- `configs/baselines/**`
- `docs/workstreams/baselines/**`

Do not edit SQL/persistence, incremental/optimized engines, shared contracts,
dependency manifests, or core event/domain semantics. Request shared changes
through `docs/workstreams/baselines/contract_requests/`.

## Objective

Build a reproducible evaluation harness that makes the benefit and limits of
GroundLoop visible without comparing semantically different systems as though
they computed the same result.

## Steps

1. Record branch, worktree, baseline commit, allowed paths, and initial tests.
2. Freeze a JSONL metrics schema containing seed, workload parameters, engine,
   event type, wall time, touched objects, candidate count, status changes,
   maintained bytes, and oracle/staging inclusion flags.
3. Implement global full recomputation and keyed affected-claim recomputation
   using independent reference functions.
4. Wrap the existing incremental engine read-only as the treatment.
5. Implement source-level and direct-citation invalidation separately. Mark
   them heuristic policy baselines and measure semantic disagreement/false
   invalidation; do not include them in exact-equivalence speedup claims.
6. Generate deterministic workloads over `E`, `C`, `A`, fanout `k`, flip count
   `f`, duplicate-content ratio, skew, and locality.
7. Add warmup, repeated paired trials, median/p95/p99 summaries, and raw JSONL
   output. Do not commit large result files.
8. Include adversarial high-fanout streams where incremental work approaches
   recomputation.
9. Produce automated tables/plots and a handoff separating correctness,
   kernel performance, and end-to-end implications.
10. Run lane and full tests, commit, rebase, rerun, and complete `HANDOFF.md`.

## Required comparisons

- Full recomputation versus keyed recomputation versus signed-delta engine.
- Alternative-witness and duplicate-content cases.
- Local versus high-fanout updates.
- Small versus large policy flip sets.
- Kernel-only timing versus timing including oracle/copy staging.

Do not implement fake AI baselines with random model calls. Full retrieval,
verification, and regeneration baselines begin only after M3 fixes models,
prompts, datasets, and caching rules.
