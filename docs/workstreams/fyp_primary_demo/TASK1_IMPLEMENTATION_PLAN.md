# FYP Primary Demo Task 1 Implementation Plan

Status: implemented and verified on 2026-09-26

Base commit: `f76a0f6249c4563cc346e41582055387ff44773d`

Branch: `codex/fyp-primary-demo`

## Objective

Package the already accepted M4.8 real dynamic history as one clear,
submission-oriented command. The command must demonstrate a bounded,
pre-registered claim/answer moving through real-model `INSERT`, exact
withdrawal `DELETE`, and real-model `REPLACE` events while preserving the
existing M4 evidence and claim boundary.

This task does not create a new maintenance engine or a new M3-to-M4 data
bridge. It makes the strongest existing vertical slice easy to run, inspect,
and explain.

## Authority and evidence boundary

- Reuse `run_m4_real_dynamic_history` without changing its semantics.
- Treat M4.8 as the production-coordinator dynamic demo spine.
- Keep M3 static generation as a separate optional scene; do not imply that an
  arbitrary `m3-register` result currently feeds the M4 route.
- State exactness only over the same stored, versioned semantic observations.
- Keep model quality, objective truth, semantic completeness, representative
  speedup, population recall/call-savings, deployment, and M5 completion out
  of scope.
- Keep the frozen M3 verifier as the default. Do not edit model, prompt,
  threshold, migration, M5, provider, or runtime-mode contracts.

## Owned paths

- `src/groundloop/fyp_demo.py` (new)
- `src/groundloop/cli.py`
- `tests/m4/fyp_demo/**` (new)
- `docs/fyp_primary_demo.md` (new)
- `docs/workstreams/fyp_primary_demo/**` (new)
- `README.md`

Every other path is read-only. The protected checkout and every held M5
worktree are outside this task.

## Deliverables

1. A reusable execution prompt recording the exact scope and acceptance gate.
2. A pure fail-closed formatter that validates the M4.8 manifest before
   presenting its trajectory, model work, oracle checks, replay checks, and
   limitations.
3. A `groundloop fyp-demo` command that invokes the unchanged M4.8 route,
   writes its canonical JSON manifest, and prints a short human-readable
   report.
4. Unit/CLI tests that require the expected three-event contract and reject a
   malformed or non-passing manifest without needing models or PostgreSQL.
5. User-facing reproduction documentation, including the separate optional M3
   static scene and explicit non-claims.

## Acceptance gates

- Expected event order is exactly `insert`, `delete`, `replace`.
- Every event is sealed and equals both Python and SQL full recomputation.
- Every reconnect replay makes zero discovery, embedding, verifier-request,
  and verifier-backend calls and leaves the database projection unchanged.
- The report exposes the claim/answer status trajectory and event-level model
  call counts.
- Tests prove malformed, mismatching, or weakened manifests fail closed.
- Focused pytest, Ruff, Ruff format, strict mypy, compileall, and diff checks
  pass.
- If local pinned artifacts and PostgreSQL are available, one live
  `groundloop fyp-demo` execution must pass and its output must remain ignored
  rather than committed.

## Work order

1. Freeze this plan and the reusable task prompt.
2. Add the pure manifest validation/summary module.
3. Add the CLI route without changing M4.8.
4. Add deterministic unit and parser tests.
5. Add the demo guide and README entry.
6. Run focused static/unit gates.
7. Run the bounded live demo when the local environment permits it.
8. Re-audit the diff and report exact results and remaining boundaries.

The completed evidence record is
[`TASK1_IMPLEMENTATION_RESULT.md`](TASK1_IMPLEMENTATION_RESULT.md).
