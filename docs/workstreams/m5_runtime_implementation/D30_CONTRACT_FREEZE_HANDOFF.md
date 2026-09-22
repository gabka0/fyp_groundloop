# M5-D30 Contract Authority-Freeze Handoff

Status: authority-only freeze candidate; no migration, installer, source,
test, database, provider, deployment, runtime-mode, performance, or AI-quality
change is authorized until this exact tranche receives two independent same-
byte `GO`, `P0=0`, `P1=0` reviews and is integrated and pushed

Date: 2026-09-22

## 1. Exact activation point

```text
authority_base_ref = origin/main
authority_base_commit = 8734162f8578ac3105119789a8729fc662da575d
authority_base_tree = c821f388ec77fb5415d99bb2f39af378ddb985e2

freeze_parent = 36998d1acf4d3f4248b7d1fdcad7ff939272dd21
freeze_parent_tree = 048d55b49647e641735cb9c6a738c5a3c4a7290b

reviewed_candidate_commit = 36998d1acf4d3f4248b7d1fdcad7ff939272dd21
reviewed_candidate_tree = 048d55b49647e641735cb9c6a738c5a3c4a7290b
reviewed_candidate_parent = 8734162f8578ac3105119789a8729fc662da575d
reviewed_candidate_parent_tree = c821f388ec77fb5415d99bb2f39af378ddb985e2
reviewed_candidate_path = docs/workstreams/m5_runtime_contract/DIRECT_M4_PROVENANCE_CLOSURE_AMENDMENT.md
reviewed_candidate_change = A
reviewed_candidate_mode = 100644
reviewed_candidate_blob = 60b2ca6609b31befb9caeb90a337fbbf0ff3a412
reviewed_candidate_sha256 = db2568affc02cf1ca6f17a549029f31089cecd857debdedf2651e6aac6898fe4
reviewed_candidate_lines = 647
reviewed_candidate_bytes = 35791
reviewed_candidate_changed_path_count = 1

d29_sha256 = e05f159f98d5f282335a90d2e9db1a26f8d85560030314060d918df59ccc82fb
authority_runtime_addendum_revision = 10
authority_runtime_addendum_sha256 = bbf215fd0d51af41f7e5e3ec7c0558c993dd1150e461283507cb9b0f044d4bff
d29_freeze_handoff_sha256 = 51ed98d60970d9458ae840ed6612baa0f9cf0d4c4dfeee326eb86713adc0d29a
migration_018_sha256 = 941bba975c12e9fb5ba4b4f75a82e59fa518b23eac34468ed2f8b15d1cd9ed90

branch = workstream/m5-d30-contract-freeze
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d30-contract-freeze
runtime_mode = v1_only
```

The reviewed file is
`docs/workstreams/m5_runtime_contract/DIRECT_M4_PROVENANCE_CLOSURE_AMENDMENT.md`.
It is immutable throughout this authority tranche. Commit `36998d1` is an
exact one-file direct child of the pushed authority base. Its committed bytes
equal the audited bytes; candidate worktree and index were clean after both
post-commit confirmations. The candidate is pushed at
`origin/workstream/m5-d30-frontier-hit-contract-candidate`; this handoff does
not claim that the candidate alone was already on `origin/main`.

## 2. Acceptance evidence

The first candidate revision was intentionally rejected. Independent reviews
identified an unbounded predecessor interval prefix, unprovable activation-
base negative M4 provenance, a missing producing M3 epoch, incorrect optional
M3 reuse, incomplete M3 relation ordering, and an undisclosed legacy empty-
task narrowing. The accepted bytes correct all of those findings.

Two independent reviewers then audited exact SHA-256
`db2568affc02cf1ca6f17a549029f31089cecd857debdedf2651e6aac6898fe4`,
647 lines and 35,791 bytes:

1. the semantic/contract audit returned `GO`, `P0=0`, `P1=0`, `P2=0`; and
2. the PostgreSQL/schema/concurrency audit returned `GO`, `P0=0`, `P1=0`,
   `P2=1`.

The nonblocking P2 requested more explicit implementation wording for the
already-inherited global model/prompt order: union dynamic and activation-M3
keys separately per relation, deduplicate, C-sort, and lock each row once.
D29's inherited global order already fixes that behavior, so it did not block
acceptance or require a candidate-byte change.

After commit, both reviewers independently confirmed the exact commit, tree,
sole parent, one added path, mode, blob, SHA-256, line/byte counts, clean index
and worktree, and exact equality between audited and committed bytes.

The accepted resolution is:

1. one changed-chunk currency locator returns both subject kinds and partitions
   them without omission or conversion;
2. activation-base claim holders use exact M3 epoch/run/execution/candidate/
   artifact closure, require M3 reuse NULL, and ignore unrelated M4 history;
3. dynamic claim holders use the immutable working delta, unique completed-
   active child, exact admission, root parent, complete owner topology, and
   sealed publication;
4. D24's arbitrary exact task, IDs, producer and optional execution, including
   non-NULL M4 reuse, remain valid;
5. accepted legacy empty task text remains valid;
6. unavailable root-local hit/admission aggregates and classic artifact,
   judgment, citation, or pair-input preimages are not invented;
7. the predecessor lookup is a physically bounded backward primary-key
   `LIMIT 1` probe with point authority and guarded rerun; and
8. lock ordering, `READ COMMITTED`, concurrency, replay, and output non-change
   remain exact.

## 3. Exact path ownership

This coordinator tranche owns only:

1. `AGENTS.md`;
2. `docs/m5_design_freeze.md`;
3. `docs/workstreams/m5_runtime_contract/CANDIDATE_RUNTIME_ADDENDUM.md`;
4. `docs/m5_acceptance_matrix.md`;
5. `docs/m5_implementation_plan.md`;
6. `docs/m5_multiagent_execution_plan.md`;
7. `docs/decision_log.md`;
8. `docs/m5_implementation_status.md`;
9. `docs/roadmap.md`; and
10. `docs/workstreams/m5_runtime_implementation/D30_CONTRACT_FREEZE_HANDOFF.md`
    (this file).

The accepted D30 amendment, every D24--D29 amendment and handoff, migrations
001--018, every source/test path, databases, providers, deployment/runtime
configuration, all implementation worktrees including
`workstream/m5-d29-matching-planner`, and protected local-main files are
outside ownership.

When an audit hashes this ten-path set, it must use C-sorted path order and
record the exact framing convention together with the manifest hash; the
numbered list above is narrative ownership order, not hash order.

## 4. Required authority result

The owned documents freeze one decision only:

```text
M5-D30 = document withdrawal enumerates every current holder with one total
          changed-chunk locator; claim holders are authorized only by exact
          activation-base M3 publication closure or exact generic dynamic-M4
          working-delta/child/owner closure; unavailable root-local and classic
          verifier preimages are not reconstructed; no schema or output
          identity changes
```

The result advances the M5.4 runtime addendum to revision 11 and adds
acceptance row `M5.0-30` as contract-`PASS` /
implementation-`PENDING`. Its six precedence cuts are exact:

1. replace only D29's requirement-only changed-chunk locator with total
   enumeration and subject-kind partition;
2. narrow D29 complete-source wording so it does not demand unavailable root
   hit/admission sets, aggregate/reason-hit preimages, classic artifacts,
   judgments, or pair inputs;
3. extend the already-installed migration-018 job/dependency routes once per
   distinct dynamic claim source epoch;
4. add working-delta point authority at tier 11a after observations and before
   currency, without creating a tier;
5. replace D29's held-WIP prohibition only through D30's later one-time custody
   protocol after the freeze and activation are pushed; and
6. require exact PostgreSQL `READ COMMITTED` before the first D30 locator.

Every other D24--D29 identity, semantic rule, counter, timing coordinate,
replay rule and migration remains unchanged. D30 authorizes no migration 019.
Migrations 001--018 remain byte-identical. No new index, table, column,
constraint, trigger, function, backfill, DTO, digest, counter, reference kind,
or public API is authorized. Migration 018 is inherited installed authority;
D30 neither recreates nor widens it.

The D29 activation `8ed44a6`, migration-018 tranche `9c855b5`, and application-
routing tranche `8734162` are partial historical evidence. They do not accept
Task 2 or D30 implementation. After this freeze is audited, integrated, and
pushed, implementation may resume only under a separately committed and
audited path-exclusive D30 activation whose exact parent is that pushed
authority barrier.

## 5. Non-change and status ceiling

This tranche does not:

- edit the reviewed D30 amendment;
- change public APIs, DTOs, digest recipes, counters, result bytes, work/timing
  or replay identities, reference kinds, present-state recipes, or M4-v1 bytes;
- add tombstones, nullable hashes, JSON/`repr` hashes, hidden caches, retained
  engines, full hydration, oracle scans, provider calls inside transactions,
  cross-policy rebasing, or migration 019;
- implement source/tests or execute database/runtime activation; or
- establish deployment, performance, utility, objective-truth, security,
  novelty, maintained-history, AI/model-quality, or named-system-superiority
  claims.

```text
runtime_mode = v1_only
M5-D24 through M5-D30 = implementation-PENDING
M5.0-24 through M5.0-30 = implementation-PENDING
Task 2 = PENDING
M5.4-05 through M5.4-09 = PENDING
all M5.5 and M5.6 gates = PENDING
deployment/performance/utility/AI-quality = PENDING
```

D30 adds `M5.0-30`; it does not invent an `M5.4-10` row.

## 6. Protected state and held-WIP custody

The dirty local-main checkout is user-owned and excluded from this tranche:

```text
local_main_head = 14598ae51562006eaf67850b19e8212f38997903
local_main_tree = 36af2be4c58572c5adde68279d1a3aacedd0beff
2af4b19962dc8a7d22e377be17f342530a06ee6395bbbf2a092eab36599c8fc2  pyproject.toml
45c20ca46e9ad5bcd86b22c0d8882d1d611497f57ca8c45d3dea149260c110cd  docs/presentations/groundloop_fyp_professor_feedback.pdf
59a13cd8d4bbb017e712c0f39e70f2eba136557e945f64f1b1fc3891742a79f0  docs/presentations/groundloop_fyp_professor_feedback_v2.pdf
c12929c349a5c0be9793159143b09da40ea2a0b27df37b92d61d9ed6483d8c2a  docs/presentations/render_groundloop_fyp_professor_deck.py
167d1e7df5a720041fe0ff51879d08357f0dfbe3a7781ffaa08c0d958a47aa94  docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT_DRAFT.md
```

Its exact porcelain is one unstaged modified `pyproject.toml` plus the four
displayed untracked files, with an empty index. Never reset, clean, stash,
reformat, stage, commit, copy, or delete those paths. Upstream distance may
change when authority commits are pushed; any protected path status or hash
drift stops the tranche.

The held implementation state is:

```text
held_worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d29-matching-planner
held_branch = workstream/m5-d29-matching-planner
current_head = 8734162f8578ac3105119789a8729fc662da575d
historical_activation_base = 28a1392ccba9592ef41b1e51969c094f1f4b922a
staged_delta = empty
held_path_count = 15
raw_porcelain_v2_z_sha256 = b5c1b18d82215e4879abb729d4e7149c132319f49c1dd169a27795cc961d29ff
authority = read-only nonauthority
```

Before the later one-time continuation, every writer process must be stopped.
The activation record must bind the physical worktree, old branch, before/
after HEAD and base, exact pushed D30 authority and activation tips,
destination-branch absence, an index exactly matching HEAD, raw
`git status --porcelain=v2 -z --untracked-files=all` bytes and SHA-256, and
every held entry's XY state, path kind, modes, byte count, and SHA-256.
Deletions require explicit absence plus base/index blob evidence. A complete
base-to-activation name/status/mode ledger must prove path disjointness.

Only then may that same physical worktree execute the one recorded branch
creation at the exact pushed activation tip. Immediately afterward it must
prove exact HEAD/tip equality, an empty index, and identical WIP path kinds,
modes, byte counts, and hashes. Before activation no WIP byte may be edited,
committed, copied, cherry-picked, rebased, stashed, merged, staged, or deleted.

## 7. Freeze verification and next activation gate

Before integration the coordinator must verify:

1. every candidate identity in Section 1;
2. this freeze commit changes exactly the ten Section-3 paths and never changes
   the reviewed amendment bytes;
3. the authority set collectively names M5-D30, M5.0-30, and runtime-addendum
   revision 11 consistently; it collectively preserves both positive
   claim-provenance branches and all six precedence cuts, with their complete
   wording in the runtime addendum and this handoff, while every summary
   remains consistent and noncontradictory; migration 018 remains unchanged
   and no migration 019 is authorized;
4. no implementation, runtime, deployment, performance, utility, or AI-quality
   claim is promoted;
5. protected local-main and held-WIP identities remain exact; and
6. two independent reviewers return `GO`, `P0=0`, `P1=0` on identical freeze
   commit/tree bytes before integration and push.

Any byte edit restarts both freeze reviews. Integration and push establish
contract authority only.

The next implementation gate must be a separately committed, reviewed, and
pushed path-exclusive D30/Task-2 activation whose exact parent is the pushed
D30 authority-freeze barrier. It must allocate disjoint ownership for:

- matching-planner repair;
- structural composition;
- requirement composition;
- direct-M4 store composition;
- direct application composition;
- public composition; and
- clean integration.

It may authorize no schema or migration-019 lane. Every implementation tranche
requires fresh whole-byte precommit and postcommit audits; earlier tests and
held WIP remain evidence, not acceptance.
