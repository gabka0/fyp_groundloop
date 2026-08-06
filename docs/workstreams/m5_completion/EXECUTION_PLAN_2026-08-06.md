# M5 Completion Restart Execution Plan

Status: active coordinator-approved path-exclusive manifest

Date: 2026-08-06

Integration base: `d0f4bdc` (`Reconcile M5 post-integration status`)

Authority: `docs/m5_design_freeze.md`, `docs/m5_implementation_plan.md`,
`docs/m5_multiagent_execution_plan.md`, `docs/m5_acceptance_matrix.md`, and
`docs/workstreams/m5_runtime_contract/CANDIDATE_RUNTIME_ADDENDUM.md`. This
manifest changes implementation ownership and sequencing only. It does not
amend M5-D1 through M5-D21, M5-T1/M5-T2, or any v1 identity. M5-D21 is the
separately recorded contract amendment that resolves the migration-014 typed
direct-open conflict.

## 1. Restart checkpoint and protected state

The accepted integration checkpoint is:

- M5.2 complete;
- M5.3-01 through M5.3-06 and M5.3-08 through M5.3-09 PASS;
- M5.3-07 PENDING;
- every M5.4 through M5.6 gate PENDING.

The PostgreSQL and incremental-overlay candidate worktrees are already
integrated patch-identically where accepted. They remain historical WIP
checkpoints and MUST NOT be reset, rebased, deleted, recommitted, or treated
as independent integration candidates:

```text
/home/kassym/Desktop/groundloop-worktrees/m5-postgres-oracle
  branch workstream/m5-postgres-oracle, checkpoint 529b1ae

/home/kassym/Desktop/groundloop-worktrees/m5-full-overlay
  branch workstream/m5-full-overlay, checkpoint bcc8b76
```

The docs-only runtime-contract worktree at checkpoint `1d0b732` and the
controlled-evaluation worktree at checkpoint `174ef4a` are likewise preserved.
Their accepted content is already present on main through patch-identical
integration commits `56bd5b9`, `6b2506b`, and `ea841f4`; neither worktree is a
new merge source.

The main checkout contains user-owned presentation work:

```text
M  pyproject.toml
?? docs/presentations/
```

No M5 lane owns those paths. No M5 commit may stage them. Their recorded
hashes and dirty state are rechecked before every integration and at closure.

The existing Compose PostgreSQL container was stopped with exit code 255 at
restart and has since been restarted non-destructively. Its volume is
preserved; no volume recreation or database deletion is authorized.

## 2. Audit verdict and first production slice

The current M5.3-07 tests are rollback-isolated SQL fixtures. They do not
exercise a production typed open, terminal failure, durable event result,
reconnect replay, conflicting replay, or complete publication-surface
immutability. M5.3-07 therefore remains PENDING.

The first accepted production slice MUST use migration 015 and the frozen
typed runtime architecture. It consists of:

1. byte-total runtime contracts and digests;
2. atomic 015 installation and immutable runtime-bundle ledgering;
3. production `open_typed_event_atomically`;
4. production `fail_typed_epoch_atomically`;
5. production `read_typed_event_result`;
6. live rejected-declaration, durable-failure, exact-replay, and
   conflicting-replay tests across reconnects.

An ad hoc result stored in `groundloop_m5_update.manifest`, a manual SQL state
flip, or another rollback-only fixture cannot close M5.3-07.

## 3. Active Wave A ownership

All Wave A branches start from the commit containing this manifest. A lane may
read any path but may edit only its listed paths. Needed cross-lane changes are
written in its handoff and left for the coordinator.

### Lane R1 -- runtime contracts and pure frontier

```text
branch:   workstream/m5-runtime-contracts-v2
worktree: /home/kassym/Desktop/groundloop-worktrees/m5-runtime-contracts-v2
```

Owned paths:

```text
src/groundloop/m5/runtime/contracts.py
src/groundloop/m5/runtime/digests.py
src/groundloop/m5/runtime/frontier.py
tests/m5/runtime/test_contracts.py
tests/m5/runtime/test_digests.py
tests/m5/runtime/test_frontier.py
docs/workstreams/m5_runtime_implementation/CONTRACTS_HANDOFF.md
```

The lane implements immutable enums/DTOs, all pure runtime digest recipes,
normalizer provenance, kind/nullability validation, candidate/snapshot/pair/
scope/job/completion/activation/work/result identities, and pure fallback,
deduplication, and frontier planning. It does not add persistence, migrations,
models, application orchestration, public exports, or M4 changes.

### Lane R2 -- runtime schema bundle

```text
branch:   workstream/m5-runtime-schema-015
worktree: /home/kassym/Desktop/groundloop-worktrees/m5-runtime-schema-015
```

Owned paths:

```text
migrations/015_m5_runtime.sql
src/groundloop/postgres/migrations.py
tests/m5/postgres_runtime/test_migration_015.py
docs/workstreams/m5_runtime_implementation/SCHEMA_HANDOFF.md
```

The lane implements exactly the 26 runtime relations enumerated in runtime
addendum Section 16 and the
`m5-runtime-schema-bundle-v2` install/rerun/conflict/rollback contract. It may
not edit migration 014, its SQL oracle, the core bundle identity, M4 tables, or
semantic-core relations owned by 014. M5-D21 authorizes only a 015
`CREATE OR REPLACE FUNCTION groundloop_m5_guard_v1_open()` body replacement
plus the runtime-header deferred validation; the trigger must remain installed
and every other 014/M4 object remains out of scope. The migration must leave
mode unchanged and must not activate M5.

R2 closed and was integrated as `05b975b`. A production root-declaration
smoke test then exposed a deferred-trigger record-field defect not covered by
the lane matrix. Because the R2 lane is no longer active, the coordinator owns
the narrow correction to `migrations/015_m5_runtime.sql`, its dedicated
`test_migration_015.py` regression, and the schema handoff. This does not
reopen any migration-014 object or grant either active lane those paths.

### Lane E1 -- controlled-adapter evidence hardening

```text
branch:   workstream/m5-evaluation-adapter-gate
worktree: /home/kassym/Desktop/groundloop-worktrees/m5-evaluation-adapter-gate
```

Owned paths:

```text
tests/m5/evaluation/test_wice_reject_matrix.py
docs/workstreams/m5_evaluation/ADAPTER_GATE_RESULT_2026-08-06.md
```

This lane extends only the pinned WiCE rejection/exclusion matrix and records
the already reproducible adapter evidence. It must not promote the scaffold
Hall recomputation to maintained-runtime evidence, change evaluation policy,
edit source/config files, or write generated reports into Git.

### Coordinator lane

The integration checkout exclusively owns:

```text
src/groundloop/m5/runtime/__init__.py
src/groundloop/m5/runtime/application.py
src/groundloop/m5/runtime/persistence.py
src/groundloop/m5/__init__.py
src/groundloop/m4/evaluation_overlay.py
src/groundloop/m4/pipeline.py
tests/m5/postgres_runtime/test_typed_runtime.py
tests/m5/postgres_runtime/test_runtime_races.py
tests/m5/postgres_runtime/test_runtime_crash_reconnect.py
all top-level status/contract/roadmap files
docs/workstreams/m5_runtime_contract/CANDIDATE_RUNTIME_ADDENDUM.md
docs/workstreams/m5_completion/
docs/workstreams/m5_integration/
```

The coordinator owns integration, public exports, transaction-local M4
extraction, mode barriers, activation, persistence/application composition,
full validation, live execution, evidence classification, and final claims.
No lane edits these paths.

### Lane R3 -- production failure/replay acceptance tests

Authorized after Barrier A integration at coordinator checkpoint `51a2bac`:

```text
branch:   workstream/m5-failure-replay-tests
worktree: /home/kassym/Desktop/groundloop-worktrees/m5-failure-replay-tests
```

Owned paths:

```text
tests/m5/postgres_runtime/conftest.py
tests/m5/postgres_runtime/test_open_failure_replay.py
docs/workstreams/m5_runtime_implementation/FAILURE_REPLAY_TEST_HANDOFF.md
```

The lane supplies adversarial live-PostgreSQL tests for the coordinator-owned
production persistence slice. It may read but must not edit persistence,
migrations, contracts, other tests, status documents, or user-owned paths.
Its evidence remains a test handoff until the coordinator inspects and
integrates it; a failing test is reported, never weakened to fit the code.

## 4. Wave barriers and later ownership

### Barrier A -- pure contracts and schema

R1 and R2 are inspected for exact owned-path scope and integrated separately.
The following must pass before production persistence work can claim evidence:

- all runtime digest, DTO, nullability, F64, ordering, and frontier tests;
- frozen M4 identity regressions after importing the runtime package;
- fresh/populated 015 install, exact rerun, hash conflict, missing/wrong 014
  prerequisite, and mid-DDL rollback;
- M5-D21 current-transaction guard/deferred-validation matrix, original guard
  restoration on failed 015 install, and byte-identical `v1_only` behavior;
- `git diff --check`, focused Ruff, strict mypy, and compileall.

### Barrier B -- M5.3-07 durable failure/replay

The coordinator implements the minimal production slice and runs it against
live PostgreSQL. PASS requires:

- rejected declaration consumes no event ID/epoch and leaves every table
  projection unchanged;
- injected open failures roll back completely across reconnect;
- terminal semantic failure retains the event/epoch, failed staged rows,
  overlays, failure reason, audit rows, and durable logical result;
- both publication heads, strict group validity, published/current currency,
  published states/certificates, and public deltas remain unchanged;
- exact failed replay returns stored work/deltas/state references with zero
  writes/calls/revision change;
- conflicting replay raises the domain conflict and changes no row.

Only then may M5.3-07 become PASS.

### Wave B -- full M5.4 runtime

Barrier A passed at coordinator checkpoint `64352dc`. Lane A1 is authorized:

```text
branch:   workstream/m5-typed-application
worktree: /home/kassym/Desktop/groundloop-worktrees/m5-typed-application
```

It may own only:

```text
src/groundloop/m5/runtime/application.py
tests/m5/runtime/test_typed_history.py
tests/m5/runtime/fake_ports.py
docs/workstreams/m5_runtime_implementation/APPLICATION_HANDOFF.md
```

The coordinator retains persistence, direct-M4 composition, activation,
PostgreSQL runtime tests, race/crash/reconnect matrices, publication, and all
shared files. Any new lane requires a recorded branch/worktree assignment in
this document or a successor manifest before editing.

M5.4 runs in this order:

1. fake-port typed history;
2. live typed runtime, activation, the M5-D21 same-transaction bridge and
   negative matrix, public-M4 terminal-path rejection, sparse seal, failure,
   and reconnect replay;
3. race and crash matrices;
4. measured history with inline full oracles disabled and out-of-band
   incremental/Python/SQL equality after every seal;
5. bounded pinned-model diagnostic with complete provenance.

No skipped, no-match, environment-failed, or scaffold-only run becomes PASS.

### Lane D1 -- typed direct-M4 bridge and public barriers

Authorized after the staged-failure checkpoint `516d0ae`:

```text
branch:   workstream/m5-direct-m4-bridge
worktree: /home/kassym/Desktop/groundloop-worktrees/m5-direct-m4-bridge
```

Owned paths:

```text
src/groundloop/m5/runtime/direct_m4.py
src/groundloop/m4/persistence.py
tests/m5/postgres_runtime/test_m4_typed_barrier.py
docs/workstreams/m5_runtime_implementation/DIRECT_M4_HANDOFF.md
```

D1 may extract cursor-local M4 structural helpers without changing frozen v1
identities or `v1_only` behavior, and must make every public M4 resume,
completion, failure, and seal mutation reject an epoch carrying a typed M5
runtime header before changing a row. It must not edit the typed coordinator,
migrations, M4 application contracts, other tests, or shared status files.

### Wave C -- maintained evaluation bridge

Only after the public M5.4 contracts and runtime are frozen may one exclusive
lane own:

```text
src/groundloop/m5/evaluation/
tests/m5/evaluation/
scripts/m5/
configs/m5/
docs/workstreams/m5_evaluation/
```

It must run the existing hash-bound controlled histories through actual M5
events, REQUIREMENT-only observation currency, maintained Hall state, sparse
publication, and the independent Python/SQL oracles. The final report records
maintained work counters, seal latency, state/certificate bytes, baseline
availability, exact event identities, and the controlled/retrospective claim
boundary. The current pure report remains scaffold evidence only.

The official WiCE source and frozen M3 checkpoint are already available
locally. No fresh human cohort is required for implementation closure, but
its absence remains explicit M6 debt and prohibits real-world or population
utility claims.

### Wave D -- M5.6 closure

After code and evaluation freeze, the coordinator executes and records:

- every focused and full test/static/database gate;
- the accepted 100,000-event artifact revalidation;
- crash/reconnect/replay and migration/backfill evidence;
- deterministic evaluation reproduction and artifact hashes;
- a generated-artifact/secrets/user-WIP audit;
- cross-stage mapping for M5-D1 through M5-D21;
- final status, acceptance matrix, roadmap, architecture, evaluation,
  literature, README, AGENTS, and decision-log reconciliation.

M5 closes only if every CORE row is executable PASS. Negative controlled or
model-relative results are retained. Missing independent adjudication narrows
the verdict to controlled/retrospective evidence and remains M6 debt.

## 5. Integration and preservation protocol

Before integrating any lane, the coordinator records branch/base/head,
`git status --short`, changed paths, and patch identity. A forbidden-path
change rejects the handoff. Integration uses an inspected commit or an
explicitly reconstructed patch; uncommitted WIP is never merged implicitly.

After each integration:

1. rerun the lane's focused tests;
2. run the applicable cross-lane contract tests;
3. verify user presentation hashes and dirty state;
4. verify both historical candidate worktrees remain unchanged;
5. commit only named M5 paths.

No command in this plan authorizes resetting main, deleting a worktree,
removing a database volume, committing generated data/model weights/caches,
or staging the user's `pyproject.toml` or `docs/presentations/` work.
