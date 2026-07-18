# Agent 1 Prompt — PostgreSQL Persistence and Third Oracle

Read `AGENTS.md`, `docs/agent_expert_operating_principles.md`,
`docs/technical_design.md`, `docs/m2_implementation_status.md`, and
`docs/parallel_agent_execution_plan.md` completely before acting.

You own only:

- `migrations/**`
- `sql/**`
- `src/groundloop/postgres/**`
- `tests/postgres/**`
- `scripts/validate_m2_postgres.py`
- `docs/workstreams/postgres/**`

Do not edit coordinator contracts, the incremental engine, optimized
algorithms, baselines, dependency manifests, or Docker Compose. Request shared
changes through `docs/workstreams/postgres/contract_requests/`.

## Objective

Close the M2 database gate by establishing live three-engine agreement among
the Python full-recomputation oracle, signed-delta engine, and independent SQL
oracle on PostgreSQL 16.

## Steps

1. Confirm branch/worktree and record the baseline commit in `STATUS.md`.
2. Start the existing Compose database and report server/extension versions.
3. Run `scripts/validate_m2_postgres.py` unchanged; preserve the first failure
   as evidence before fixing anything.
4. Fix only runtime SQL/persistence defects. Do not change semantics to make a
   test pass.
5. Implement typed snapshot serialization/loading under
   `src/groundloop/postgres/`; keep SQL recomputation independent of the
   incremental engine.
6. Add deterministic three-way tests for support, refutation, conflict,
   distinct-content deduplication, supersession, inactive chunks, deletion,
   replacement, and policy changes.
7. Add bounded randomized database tests. Compare complete counts, best scores,
   observation ID arrays, statuses, and certificate validity after every
   committed event.
8. Test rollback and idempotence under injected failure.
9. Run `EXPLAIN (ANALYZE, BUFFERS)` for current-observation and policy-range
   queries; verify index use and record plans in the handoff.
10. Run lane and full tests, commit, rebase on `main`, rerun, and complete
    `HANDOFF.md`.

## Required evidence

- PostgreSQL and pgvector versions.
- Exact schema/oracle commands.
- Zero claim mismatches, answer mismatches, and invalid certificates.
- Transaction and replay test results.
- Query plans showing whether intended indexes are used.
- Limitations and any unresolved semantic mismatch.

Confidence must be stated explicitly. Do not call M2 complete yourself; the
coordinator owns that decision.
