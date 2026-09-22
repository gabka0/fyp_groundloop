# M5-D30 Task-2 Store/Runtime Activation

Status: docs-only activation candidate; no implementation ownership exists
until this exact one-file change receives two independent same-byte `GO`,
`P0=0`, `P1=0` reviews, is committed, and that reviewed commit is pushed to
both its branch and `origin/main`

Date: 2026-09-22

## 1. Exact authority barrier

```text
required_activation_parent = c65a6fc1d13e2955a2e21d5f8a18e7223c9fd095
required_activation_parent_tree = 7e22df437547877799df0ecfa28b037514d32d08
required_origin_main = c65a6fc1d13e2955a2e21d5f8a18e7223c9fd095

d30_candidate_commit = 36998d1acf4d3f4248b7d1fdcad7ff939272dd21
d30_candidate_tree = 048d55b49647e641735cb9c6a738c5a3c4a7290b
d30_candidate_sha256 = db2568affc02cf1ca6f17a549029f31089cecd857debdedf2651e6aac6898fe4
d30_candidate_lines = 647
d30_candidate_bytes = 35791

d30_freeze_commit = c65a6fc1d13e2955a2e21d5f8a18e7223c9fd095
d30_freeze_tree = 7e22df437547877799df0ecfa28b037514d32d08
d30_freeze_handoff_sha256 = 546bf38719e1e3e3391739c3484e541b25468349204f60847e39ddf5771b90ee
runtime_addendum_revision = 11
runtime_addendum_sha256 = 50b3b78e97d5d24846eebd16cc1b3c1e23c73cdf9f59e58434dcc3365d23ce76
migration_018_sha256 = 941bba975c12e9fb5ba4b4f75a82e59fa518b23eac34468ed2f8b15d1cd9ed90

activation_branch = workstream/m5-d30-task2-store-runtime-activation
activation_worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d30-task2-store-runtime-activation
activation_changed_path_count = 1
runtime_mode = v1_only
```

The sole owned path is this file. Its parent is the exact pushed D30 authority
barrier. The activation commit may have only that parent and only this added
mode-`100644` path. Any intervening commit or byte edit restarts both reviews.
The resulting commit hash and tree are recorded by the two post-commit
identity checks and by the one-time custody record before implementation.

This activation is subordinate, in order, to the M5 design freeze, runtime
addendum revision 11, M5-D24 through M5-D30, the acceptance matrix, the D29
schema/application handoffs, and the D30 contract-freeze handoff. Frozen
authority wins over this ownership plan.

## 2. Authorized outcome and narrow scope

This activation authorizes sequential implementation of the still-pending
Task-2 PostgreSQL store/runtime composition only:

```text
matching-planner repair
  -> structural composition
  -> requirement composition
  -> direct-M4 store composition
  -> direct application composition
  -> public composition
  -> clean integration and evidence reconciliation
```

The implementation must preserve the already integrated D29 migration-018
and package-private application terminal-cutoff bytes. D30 changes claim
withdrawal provenance only as frozen:

1. one total changed-chunk locator returns full current-currency rows for both
   `requirement` and `claim`, then partitions without omission or conversion;
2. dynamic claim holders validate through immutable working delta, bounded
   predecessor, unique completed-active child, cited admission, complete owner
   topology, optional M4 execution, and current/published currency;
3. activation-base claim holders validate exact M3 epoch/run/execution/
   candidate/artifact/artifact-use closure with installed revision zero and
   NULL M3 reuse, without a negative-M4-provenance test;
4. typed and legacy owners remain exact and exclusive, including arbitrary
   D24 task/IDs/producer, optional execution, non-NULL M4 reuse, and accepted
   legacy empty task text;
5. direct-root hit/admission aggregates, reason-hit sets, classic artifacts,
   judgments, pair inputs, and unavailable raw preimages are not reconstructed;
6. the predecessor is one exact-full-key backward primary-key `LIMIT 1`
   probe, optional point lock, interval-coverage check, and guarded identical
   rerun; and
7. PostgreSQL must already be exact `READ COMMITTED` before the first D30
   locator, with the frozen tier 8 -> 9 -> 10 -> 11a ordering and global
   model/prompt deduplication and lock-once order.

No lane may add or alter a table, column, constraint, trigger, function,
backfill, index, installer route, migration, public DTO, public protocol,
digest recipe, counter, reference kind, present-state recipe, result byte, or
runtime-mode default. Migration 019 is forbidden; migrations 001--018 are
read-only and must remain byte-identical.

## 3. Held Lane-P custody

The pre-activation implementation evidence is physically held at:

```text
held_worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d29-matching-planner
held_branch = workstream/m5-d29-matching-planner
held_head = 8734162f8578ac3105119789a8729fc662da575d
held_tree = c821f388ec77fb5415d99bb2f39af378ddb985e2
historical_activation_base = 28a1392ccba9592ef41b1e51969c094f1f4b922a
held_index = exactly HEAD
held_path_count = 15
held_raw_porcelain_v2_z_sha256 = b5c1b18d82215e4879abb729d4e7149c132319f49c1dd169a27795cc961d29ff
destination_branch = workstream/m5-d30-matching-planner
destination_branch_local = absent
destination_branch_remote = absent
```

The complete held-entry ledger is:

| Path | XY/kind | Base blob | Mode | Bytes | SHA-256 |
|---|---|---|---:|---:|---|
| `src/groundloop/m5/runtime/postgres_matching.py` | `.M` tracked | `833a3b2f1e8d9b1117197c2ace04262cf2aa9706` | `100644` | 737360 | `180ebdb2cf227aebfa90dd18f93a296da9983d50639695065df2264256b12411` |
| `src/groundloop/m5/runtime/postgres_withdrawal.py` | `??` untracked | absent | `100644` | 316495 | `67b644aed3de336fb241813f782cbd1b3a50ea708eb84cf1446849b7446dc404` |
| `tests/m5/postgres_runtime/d25_store_core/conftest.py` | `.M` tracked | `048c00778ce2bfc2af1974010db7d7a3ed6ea6a6` | `100644` | 118081 | `9b0fd41cfb87500ebf8761cd6d6b6773bc8109fb6776cd807cd2b0903ad07e9e` |
| `tests/m5/postgres_runtime/d25_store_core/test_d27_counter_ownership.py` | `??` untracked | absent | `100644` | 13233 | `7a8e28c27a9b890bddf1e0df13378e522744efb771b238dffe654bfc19b14dad` |
| `tests/m5/postgres_runtime/d25_store_core/test_d28_phased_composition.py` | `??` untracked | absent | `100644` | 45034 | `043782e9b1bc6ec7bdc7757ddf52a5e62ec0ce3a2dd04d3ae984853ac9ad78a5` |
| `tests/m5/postgres_runtime/d25_store_core/test_nonempty_planner.py` | `??` untracked | absent | `100644` | 49425 | `36d1529d0980de994983dc9d5b8c4bb98a2375293be878665c1fe09474af6a07` |
| `tests/m5/postgres_runtime/d25_store_core/test_points.py` | `.M` tracked | `c21c7f786f49b90afa9278b8d47965b4a9ed65d0` | `100644` | 22988 | `09ff7bb34172d90d65c851b56eeeaee7ab3a26b4a42f0a1c6c8a2d4fcb8f19ad` |
| `tests/m5/postgres_runtime/d25_store_core/test_replay_work.py` | `.M` tracked | `e4a83e5c72aed207e641df58ea26729b98aa2b1a` | `100644` | 28705 | `556847445c6b8219c541f47418ffddaae72c3c60972d9710c683bc35210b98bc` |
| `tests/m5/postgres_runtime/d25_store_core/test_transition_apply.py` | `.M` tracked | `1a3d3a26e3bfeffb432a047439c0b290955a953e` | `100644` | 14627 | `0157c4523be46c9428bf71e9a971a975ff3c483d94f295e080a6614d39aa21bf` |
| `tests/m5/postgres_runtime/d29_store/conftest.py` | `??` untracked | absent | `100644` | 31912 | `cbb017fd86b5e3593b42c4210bfcb6742d1c88e63873ac41b87af859147a7507` |
| `tests/m5/postgres_runtime/d29_store/test_bounded_withdrawal.py` | `??` untracked | absent | `100644` | 80929 | `a1877ba7c144f80943ce3c555cb8ba20fe5344f40a505ef16d5bd4f5a4dec782` |
| `tests/m5/postgres_runtime/d29_store/test_query_plans.py` | `??` untracked | absent | `100644` | 38126 | `be99e7a59cfa475d16a85ddf163407b3bc150dd4b676def0b9dd28c48b6a4482` |
| `tests/m5/postgres_runtime/d29_store/test_replay_and_reservation.py` | `??` untracked | absent | `100644` | 17709 | `944fcc7b51b08620790be4dd65fc0e85ad7629d501b2aae2db4e289b86c05bc4` |
| `tests/m5/postgres_runtime/d29_store/test_retained_declarations.py` | `??` untracked | absent | `100644` | 23276 | `21c869eef6b4143e8015a5720ffe2f283596ae405bf849891c7fc10c0fbf1cc2` |
| `tests/m5/postgres_runtime/d29_store/test_source_identity.py` | `??` untracked | absent | `100644` | 22415 | `aca5d3ccc3a39b70cb6ad5f7c3e7fd9cd51be5f81c9ff110a62996929ed09b50` |

All held files parse, and the eleven held test modules collect 206 tests. That
is inventory evidence only: no held test has been accepted as D30 evidence.
The historical D29 matching-planner handoff was never created.

## 4. Base-to-activation disjointness and one-time branch move

The complete name/status/mode ledger from held HEAD `8734162f` to the required
activation parent is eleven docs-only paths:

| Status | Old mode | New mode | Path |
|---|---:|---:|---|
| `M` | `100644` | `100644` | `AGENTS.md` |
| `M` | `100644` | `100644` | `docs/decision_log.md` |
| `M` | `100644` | `100644` | `docs/m5_acceptance_matrix.md` |
| `M` | `100644` | `100644` | `docs/m5_design_freeze.md` |
| `M` | `100644` | `100644` | `docs/m5_implementation_plan.md` |
| `M` | `100644` | `100644` | `docs/m5_implementation_status.md` |
| `M` | `100644` | `100644` | `docs/m5_multiagent_execution_plan.md` |
| `M` | `100644` | `100644` | `docs/roadmap.md` |
| `M` | `100644` | `100644` | `docs/workstreams/m5_runtime_contract/CANDIDATE_RUNTIME_ADDENDUM.md` |
| `A` | `000000` | `100644` | `docs/workstreams/m5_runtime_contract/DIRECT_M4_PROVENANCE_CLOSURE_AMENDMENT.md` |
| `A` | `000000` | `100644` | `docs/workstreams/m5_runtime_implementation/D30_CONTRACT_FREEZE_HANDOFF.md` |

This activation adds only this docs path at mode `100644`. None of those
twelve paths intersects the fifteen held entries. Immediately before the
move, the coordinator must recompute the full held-to-reviewed-activation
name/status/mode ledger, not merely trust this candidate-time proof.

After the activation commit has passed both audits and post-commit identity
checks and is pushed to `origin/main`, the coordinator must stop/check every
writer and record, in one custody transcript:

1. physical held worktree, old branch, HEAD/tree and historical base;
2. exact pushed D30 freeze and reviewed activation commit/tree;
3. local and remote absence of `workstream/m5-d30-matching-planner`;
4. index exactly equal to HEAD;
5. the raw porcelain-v2-z bytes and SHA-256;
6. every held entry's XY/kind, base/index mode or absence, filesystem mode,
   byte count and SHA-256;
7. the complete held-HEAD-to-activation name/status/mode ledger and empty
   intersection; and
8. protected-main identity and absence of any non-audit writer.

Only after all eight checks may the same physical held worktree execute:

```text
git switch -c workstream/m5-d30-matching-planner <exact-pushed-activation-tip>
```

That is the only allowed branch movement. Immediately afterward the custody
transcript must prove HEAD and branch tip equal the pushed activation tip,
index still equals HEAD, and all fifteen WIP entry kinds, modes, byte counts,
hashes and raw porcelain bytes are identical. Any difference aborts. Before
that moment no held byte may be edited, staged, committed, copied,
cherry-picked, rebased, merged, stashed, or deleted.

## 5. Frozen paths and global exclusions

Every lane may read but may not edit an unlisted path. In particular, all
contracts, migrations, migration installers/tests, D29 application cutoff
source/tests, accepted D25 publication helpers, public types, model/provider
configuration, deployment files, and protected local-main paths are frozen.

Implementation must not introduce repository hydration, a retained engine,
full-state/oracle scans, provider/model calls inside a database transaction,
unbounded SQL, hidden caches, alternate digest code, JSON/`repr` hashing,
tombstones, nullable hashes, a seventh reference kind, cross-policy rebasing,
or a second timing anchor. A needed unlisted path is a hard stop for a new
one-file docs-only activation amendment with two same-byte audits and push.

Each lane starts only at the exact pushed predecessor barrier, uses a fresh
branch/worktree except for the one-time Lane-P custody move, and ends with two
independent whole-byte precommit audits, a commit, two postcommit identity
checks, and push before the next lane begins. Tests from different commits
cannot be pooled as one acceptance result.

## 6. Sequential path-exclusive lanes

### 6.1 Lane P -- matching-planner repair

```text
branch = workstream/m5-d30-matching-planner
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d29-matching-planner
base = exact pushed D30 activation commit through the Section-4 custody move
```

Owned paths are the fifteen held paths in Section 3 plus only:

1. `tests/m5/postgres_runtime/d30_store/conftest.py` (new);
2. `tests/m5/postgres_runtime/d30_store/test_total_claim_currency.py` (new);
3. `tests/m5/postgres_runtime/d30_store/test_dynamic_claim_provenance.py`
   (new);
4. `tests/m5/postgres_runtime/d30_store/test_activation_m3_provenance.py`
   (new);
5. `tests/m5/postgres_runtime/d30_store/test_owner_topology.py` (new);
6. `tests/m5/postgres_runtime/d30_store/test_predecessor_probe.py` (new);
7. `tests/m5/postgres_runtime/d30_store/test_query_plans_and_races.py` (new);
8. `tests/m5/postgres_runtime/d30_store/test_replay_nonchange.py` (new); and
9. `docs/workstreams/m5_runtime_implementation/D30_MATCHING_PLANNER_REPAIR_HANDOFF.md`
   (new).

Lane P preserves the D25/D27/D28 prepare-stage-finalize, replay, counter and
absence-artifact machinery. It replaces the held D29 claim classifier and
forbidden reconstruction with D30's two positive provenance branches, total
full-row locator, exact working-delta link, complete source-epoch topology,
typed/legacy terminal closure, optional execution, bounded predecessor route,
shared-artifact order and isolation guard. It updates any pre-D30 test that
positively expected root aggregate or classic-artifact reconstruction.

Every M5-D30 falsifier 1--15 must have an executable positive/negative case.
Default-planner `EXPLAIN (ANALYZE, FORMAT JSON)` and executed-query traces must
prove the frozen bounded routes and forbid the superseded scans. Retained
D24--D29, D25 differential, replay, work/counter, sparse-write and result-byte
regressions must pass. The handoff records the custody transcript before any
implementation edit and the exact final test/environment evidence.

### 6.2 Lane C1 -- structural composition

```text
branch = workstream/m5-d30-structural-composition
base = exact pushed Lane-P repair commit
```

Exact owned paths:

1. `src/groundloop/m5/runtime/persistence.py`;
2. `src/groundloop/m5/runtime/postgres_recovery.py`;
3. `tests/m5/postgres_runtime/d30_application/conftest.py` (new);
4. `tests/m5/postgres_runtime/d30_application/test_store_composition.py`
   (new);
5. `tests/m5/postgres_runtime/d30_application/test_structural_order.py`
   (new);
6. `tests/m5/postgres_runtime/d30_application/test_seal_atomicity.py` (new);
7. `tests/m5/postgres_runtime/d30_application/test_store_races.py` (new);
8. `tests/m5/postgres_runtime/d30_application/test_structural_open.py` (new);
9. `docs/workstreams/m5_runtime_implementation/D30_STRUCTURAL_COMPOSITION_HANDOFF.md`
   (new).

Lane C1 composes activation, migration-018 route validation, document/group
structural open, recovery, replay and seal around the accepted Lane-P API. It
does not reimplement provenance, matching, migration, direct completion or
public facades. Absent/exact-existing first-open branches, revision-one
structural state, atomic rollback and terminal-cut behavior remain exact.

### 6.3 Lane C2 -- requirement composition

```text
branch = workstream/m5-d30-requirement-composition
base = exact pushed Lane-C1 commit
```

Exact owned paths:

1. `src/groundloop/m5/runtime/postgres_verifier.py`;
2. `tests/m5/postgres_runtime/d30_application/test_requirement_completion.py`
   (new);
3. `tests/m5/postgres_runtime/d30_application/test_requirement_order.py`
   (new);
4. `tests/m5/postgres_runtime/d30_application/test_requirement_races.py`
   (new); and
5. `docs/workstreams/m5_runtime_implementation/D30_REQUIREMENT_COMPOSITION_HANDOFF.md`
   (new).

Lane C2 replaces only the active requirement-completion persisted-matching
hard stop. It derives once, prepares before header mutation, stages
observation/currency then remaining lower tiers, terminalizes the held
job/attempt, accounts D24, finalizes D25, writes exact status deltas and forces
validation. Inactive, late, cancelled, failed, loser and audit-only paths stay
D25-inert.

### 6.4 Lane M -- direct-M4 store composition

```text
branch = workstream/m5-d30-m4-store-composition
base = exact pushed Lane-C2 commit
```

Exact owned paths:

1. `src/groundloop/m4/pipeline.py`;
2. `tests/m4/integration/test_m5_typed_store_derived_direct.py` (new);
3. `tests/m4/integration/test_m5_d30_direct_stage_evidence.py` (new); and
4. `docs/workstreams/m5_runtime_implementation/D30_M4_STORE_COMPOSITION_HANDOFF.md`
   (new).

Lane M adds only the package-private typed store-derived construction/stage
route and exact lexical evidence for lower-tier/job, 15a, 15b and all 15c
families. It performs no repository/engine hydration, full scan, public M4-v1
change, provider call, reservation mutation or hidden caching.

### 6.5 Lane C3 -- direct application composition

```text
branch = workstream/m5-d30-direct-application-composition
base = exact pushed Lane-M commit
```

Exact owned paths:

1. `src/groundloop/m5/runtime/postgres_direct_recovery.py`;
2. `src/groundloop/m5/runtime/direct_m4.py`;
3. `tests/m5/postgres_runtime/d30_application/test_direct_transition.py`
   (new);
4. `tests/m5/postgres_runtime/d30_application/test_direct_order.py` (new);
5. `tests/m5/postgres_runtime/d30_application/test_direct_races.py` (new);
6. `tests/m5/postgres_runtime/d30_application/test_direct_store_reconnect.py`
   (new); and
7. `docs/workstreams/m5_runtime_implementation/D30_DIRECT_APPLICATION_COMPOSITION_HANDOFF.md`
   (new).

Lane C3 composes reservation, Lane-M lexical evidence, source completion,
header advance, D25 stage, D24 accounting, D25 finalization, status delta and
validation in frozen order. It owns no public preview/runner. Late, inactive,
replay, serialized-loser and combined-failure paths remain D25-inert.

### 6.6 Lane D -- public composition

```text
branch = workstream/m5-d30-public-composition
base = exact pushed Lane-C3 commit
```

Exact owned paths:

1. `src/groundloop/m5/runtime/postgres_application.py`;
2. `src/groundloop/m5/runtime/postgres_direct_application.py`;
3. `src/groundloop/m5/runtime/postgres_matching_postseal.py` (new);
4. `tests/m5/postgres_runtime/d30_application/test_group_public_composition.py`
   (new);
5. `tests/m5/postgres_runtime/d30_application/test_direct_public_composition.py`
   (new);
6. `tests/m5/postgres_runtime/d30_application/test_reconnect.py` (new);
7. `tests/m5/postgres_runtime/d30_application/test_postseal_audit.py` (new);
8. `tests/m5/postgres_runtime/d30_application/test_continuation.py` (new);
9. `tests/m5/postgres_runtime/d30_application/test_terminal_cutoff.py` (new);
10. `tests/m5/postgres_runtime/d30_application/test_route_barrier.py` (new);
11. `docs/workstreams/m5_runtime_implementation/D30_PUBLIC_COMPOSITION_HANDOFF.md`
    (new).

Lane D is the first lane allowed to wire production-capable public PostgreSQL
facades. It may raise the already integrated exact private D29 cutoff only
from final validated declaration checks. Direct continuation performs barrier,
supplied-revision validation, retained declaration hydration/terminal check,
acquisition and then external work; it never uses first-open planning.
Reconnect, zero-call terminal replay, one timing anchor and DTO bytes remain
exact. Post-seal Python/SQL/physical/provenance audits occur after commit and
outside measured latency with a final publication-head recheck.

### 6.7 Lane I -- clean integration and evidence reconciliation

```text
branch = integration/m5-d30-task2-store-runtime
base = exact pushed Lane-D commit
```

Exact owned paths:

1. `docs/workstreams/m5_runtime_implementation/D30_TASK2_STORE_RUNTIME_INTEGRATION_HANDOFF.md`
   (new);
2. `AGENTS.md`;
3. `docs/decision_log.md`;
4. `docs/m5_acceptance_matrix.md`;
5. `docs/m5_implementation_plan.md`;
6. `docs/m5_implementation_status.md`;
7. `docs/m5_multiagent_execution_plan.md`; and
8. `docs/roadmap.md`.

Lane I makes no source or test edit. Because every implementation lane is a
linear child of the pushed predecessor, it verifies a clean linear ancestry
rather than creating a merge commit. It runs the isolated combined serial
PostgreSQL regression, full unit suite, compile/lint/type/package checks,
fresh-process reconnect/replay checks, query-plan/trace assertions, and two
independent final whole-tree audits. Only evidence actually reproduced at the
exact Lane-D commit may be reconciled. Any failure returns ownership to the
lane that owns the failing path under a new audited repair activation; Lane I
must not patch source.

## 7. Mandatory per-lane evidence

Every handoff records exact base/commit/tree, parent, name-status/mode ledger,
whole-file SHA-256 values, branch/worktree, tool and PostgreSQL versions,
database identity, migration ledger, runtime mode, environment variables by
name without secrets, exact commands, collected/executed/skipped counts,
duration, query plans/traces where applicable, protected-main hashes, and all
negative/rollback/race results. Generated databases, caches, secrets, model
weights and reports stay untracked.

At minimum the combined sequence must prove:

- all fifteen D30 falsifier families, including both positive provenance
  branches, typed/legacy exclusivity and exact failure cuts;
- mixed requirement/claim total-location, long predecessor history and
  at-most-one-row physical execution;
- complete owner job/dependency/scope topology and arbitrary retained IDs;
- present/absent optional execution, NULL/non-NULL M4 reuse and NULL M3 reuse;
- no forbidden root/classic reconstruction and no migration 019;
- `READ COMMITTED` rejection before locator and both-order race outcomes;
- D24--D29 counter, work, timing, cancellation, recovery and cutoff behavior;
- D25 Python/SQL differential equality, sparse writes and immutable
  certificate/reference identities;
- historical replay with zero current-provenance reconstruction or semantic
  DML;
- unchanged M4-v1 and public DTO/result bytes; and
- exact `v1_only` behavior outside isolated activated fixtures.

## 8. Activation audit and protected main

Before the custody move or any implementation edit:

1. this file is the only changed path and the activation commit has sole
   parent `c65a6fc1d13e2955a2e21d5f8a18e7223c9fd095`;
2. the D30 candidate, freeze, addendum, migration-018 and origin-main
   identities in Section 1 are exact;
3. both reviewers audit identical candidate bytes and return `GO`, `P0=0`,
   `P1=0`; any byte edit restarts both reviews;
4. both reviewers confirm the held ledger, base-to-activation disjointness,
   destination-branch absence, path exclusivity, no schema lane and all claim
   ceilings;
5. after commit, both reviewers confirm committed bytes equal audited bytes;
6. the reviewed activation branch and exact commit are pushed, then
   `origin/main` fast-forwards to that commit without an intervening merge;
7. the Section-4 custody transcript and branch move complete exactly; and
8. only then does Lane P gain write authority.

The dirty local-main checkout is user-owned and excluded:

```text
local_main_head = 14598ae51562006eaf67850b19e8212f38997903
local_main_tree = 36af2be4c58572c5adde68279d1a3aacedd0beff
local_main_index = empty
local_main_raw_porcelain_v2_z_sha256 = 6fe14ac65352b34c1625cc8d50854f7b82a3ae74073a5e9995a39a505f7a88cc
2af4b19962dc8a7d22e377be17f342530a06ee6395bbbf2a092eab36599c8fc2  pyproject.toml
45c20ca46e9ad5bcd86b22c0d8882d1d611497f57ca8c45d3dea149260c110cd  docs/presentations/groundloop_fyp_professor_feedback.pdf
59a13cd8d4bbb017e712c0f39e70f2eba136557e945f64f1b1fc3891742a79f0  docs/presentations/groundloop_fyp_professor_feedback_v2.pdf
c12929c349a5c0be9793159143b09da40ea2a0b27df37b92d61d9ed6483d8c2a  docs/presentations/render_groundloop_fyp_professor_deck.py
167d1e7df5a720041fe0ff51879d08357f0dfbe3a7781ffaa08c0d958a47aa94  docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT_DRAFT.md
```

Never reset, clean, stash, reformat, stage, commit, copy or delete those paths.
The checkout may remain behind `origin/main`; that is not drift. Any listed
path/hash/status/index drift stops activation or integration.

## 9. Acceptance ceiling

This file grants path ownership; it accepts no implementation byte and proves
no executable gate. Until each lane and final integration pass:

```text
M5-D24 through M5-D30 = implementation-PENDING
M5.0-24 through M5.0-30 = implementation-PENDING
Task 2 = PENDING
M5.4-05 through M5.4-09 = PENDING
M5.5 and M5.6 = PENDING
runtime_mode = v1_only outside isolated fixtures
deployment/performance/utility/AI-quality = PENDING
```

No security, objective-truth, novelty, maintained-history, population-quality,
latency, scalability, or named-system-superiority claim follows from this
activation. Public composition is downstream work, not an already delivered
runtime.
