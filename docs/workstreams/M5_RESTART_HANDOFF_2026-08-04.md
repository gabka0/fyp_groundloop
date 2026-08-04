# GroundLoop M5 restart handoff

Status: safe checkpoint for a fresh Codex session started from the GroundLoop
repository root. This is a work-in-progress handoff, not an M5 completion
claim.

## Start the next session

```bash
cd /home/kassym/Desktop/groundloop
codex
```

The new agent must first read `AGENTS.md`,
`docs/agent_expert_operating_principles.md`, this handoff,
`docs/m5_design_freeze.md`, `docs/m5_implementation_plan.md`, and
`docs/m5_acceptance_matrix.md`. It must inspect all three Git worktrees and
must not recreate or discard any checkpoint.

## Integrated main

```text
branch: main
head: ea841f4 Harden M5 evaluation matching gate
```

Integrated in this wave:

- `6b2506b Implement controlled M5 evaluation config`
- `ea841f4 Harden M5 evaluation matching gate`

The pure M5.5 adapter/protocol slice is integrated, but M5.5 is not complete.
It still uses a clearly labelled functional Hall-recomputation scaffold for
baseline 5. The real maintained overlay and SQL oracle must later consume the
same histories before target-runtime or final M5.5 claims are allowed.

Independent evidence recorded before this handoff:

- evaluation tests: 38 passed;
- evaluation Ruff: passed;
- strict mypy for the evaluation package: passed;
- exhaustive independent evaluation-matching differential: every 74,954
  simple bipartite graph with `1 <= r <= 4` and `1 <= H <= 4` passed;
- `pip check`: no broken requirements;
- virtual-environment NumPy: 2.4.6;
- project dependency is now `numpy>=2,<2.5`, preserving the declared Python
  3.11 typing contract.

The full ordinary repository suite passed immediately before evaluation
integration. It has not yet been rerun after `6b2506b` and `ea841f4`.

## User-owned dirt to preserve

Main deliberately remains dirty only for the user's presentation work:

```text
 M pyproject.toml
?? docs/presentations/
```

The remaining unstaged `pyproject.toml` change is only the presentation-renderer
Ruff ignore/comment. The NumPy constraint is already committed. Do not stage,
rewrite, remove, or commit the presentation hunk or files unless the user asks.

## PostgreSQL candidate checkpoint

```text
worktree: /home/kassym/Desktop/groundloop-worktrees/m5-postgres-oracle
branch: workstream/m5-postgres-oracle
head: abca7e3 WIP checkpoint corrected M5 PostgreSQL oracle
```

This checkpoint fixes the three independently reproduced P1 defects:

1. newly sealed registration/replacement groups disappearing at their own
   epoch;
2. published certificate fallback being suppressed before the first working
   binding, while preserving tombstone semantics after a working interval
   closes; and
3. migration 014 accepting a migration-013 schema with a missing/disabled
   critical trigger.

Completed evidence on the checkpoint's immediate predecessor/final saved
content:

- five mandatory live defect regressions passed;
- populated install/replay/hash-conflict test passed;
- nine non-live static-contract tests passed;
- `git diff --check` passed before the checkpoint.

One new coexistence test initially failed because its M4 registry snapshot had
no member. The fixture correction is included in `abca7e3`, but its rerun was
interrupted. Therefore `abca7e3` is WIP and must not be cherry-picked yet.

Next PostgreSQL steps:

1. rerun the corrected coexistence test;
2. run the full owned M5 PostgreSQL suite and relevant live M4 regression
   suites;
3. run Ruff, strict mypy, compileall, and diff checks;
4. have a different agent reproduce the three original P1 cases and audit the
   trigger-manifest/lock protocol;
5. only then create a clean candidate commit and integrate it.

The trigger preflight deliberately verifies trigger wiring/catalog integrity,
not function-body or historical byte provenance. The frozen seven writer
relations retain `ACCESS EXCLUSIVE` locks; additional trigger-catalog surfaces
use deterministic `ROW EXCLUSIVE` locks to block trigger-changing DDL without
blocking ordinary DML.

## Incremental overlay checkpoint

```text
worktree: /home/kassym/Desktop/groundloop-worktrees/m5-full-overlay
branch: workstream/m5-full-overlay
head: ac6859f WIP checkpoint M5 incremental overlay
```

This commit preserves the full overlay implementation but is explicitly not
integration-ready. Its attempted checkpoint-A refactor was aborted before a
write, so the following reviewed blockers remain:

1. measured event paths still globally sort touched/output identifiers and
   charge `canonical_sort_items`; replace these with deterministic
   insertion-ordered propagation and prove hash-seed-stable logical output;
2. claim-state dirtiness and claim-certificate dirtiness are conflated; a
   certificate-only repair in an unselected alternative group can scan all
   complete groups, violating the sparse M5-T2 bound;
3. epoch carry-forward or binding-history-only transitions are omitted from
   changed-ID and certificate-only classifications;
4. focused regressions are still required for forced two-child AVL deletion,
   long O(1) history append, no measured `export_snapshot`, many-group policy
   updates with zero stable-path sort charge, nonselected-certificate repair,
   binding-only carry-forward, and cross-hash-seed output digests;
5. after those fixes, run full per-event differential tests against the Python
   reference, failure/replay tests, complexity guards, and the frozen 100,000
   event gate before integration.

Continue this work as small tested commits. Do not cherry-pick `ac6859f` to
main.

## Required integration order

1. finish and independently audit the PostgreSQL candidate;
2. finish and independently audit the incremental overlay;
3. integrate one candidate at a time and rerun full pure/static/live M4+M5
   coexistence and three-oracle gates after each;
4. implement M5.4 migration 015, typed runtime DTOs, fake-port application,
   crash/replay/reconnect, controlled dynamic history, and pinned-model
   diagnostic in non-overlapping worktrees;
5. replace evaluation baseline 5 with the real maintained runtime and connect
   the same histories to Python and SQL oracles;
6. run the frozen 100,000-event gate and complete the M5.6 closure audit.

No current result supports a final M5-complete, production-ready, real-world
semantic-validation, or novel-database-algorithm claim.
