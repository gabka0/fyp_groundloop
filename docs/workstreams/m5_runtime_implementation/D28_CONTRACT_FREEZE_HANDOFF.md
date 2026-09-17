# M5-D28 Contract Authority-Freeze Handoff

Status: authority-only freeze candidate; no migration, source, test, database,
provider, deployment, runtime-mode, or AI-quality change is authorized until
this exact tranche receives two independent same-byte `GO`, `P0=0`, `P1=0`
reviews and is integrated and pushed

Date: 2026-09-18

## 1. Exact activation point

```text
freeze_parent = cc0b9cf25110992320dc0b7f81499a3930d1a565
freeze_parent_tree = 760107f9b6cbd70d37839144980cf23b8f5859ea
reviewed_candidate_commit = cc0b9cf25110992320dc0b7f81499a3930d1a565
reviewed_candidate_tree = 760107f9b6cbd70d37839144980cf23b8f5859ea
reviewed_candidate_parent = 8ecdc8e361cf4a8c055fa00e9916e1704cbb736d
reviewed_candidate_sha256 = 8a2bafd3478cf2cac6ac7c8de7ca7779a6d9ace7fbbe98a7dc3ff08afdb67eae
reviewed_candidate_lines = 810
reviewed_candidate_bytes = 48053
branch = workstream/m5-d28-contract-freeze
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d28-contract-freeze
runtime_mode = v1_only
```

The reviewed file is
`docs/workstreams/m5_runtime_contract/PHASED_PERSISTED_MATCHING_COMPOSITION_AMENDMENT.md`.
It is immutable throughout this authority tranche. The candidate commit is an
exact one-file direct child of the pushed Lane-A/Lane-B integration barrier.

## 2. Acceptance evidence

The D28 candidate was intentionally not accepted at its earlier draft hashes.
Independent reviews exposed and then rechecked the complete set of sequencing
defects: revision/header guards, tier-16 status deltas, representative
discovery ownership, stage/finalizer journals, replay intent authority,
read-only replay context, D24/runtime-addendum precedence, requirement tier-
11a DML, requirement terminalization, direct 15a/15b, all three direct 15c
families, complete plan-before-header placement, and terminal job/attempt
replay authority.

The final bytes resolve those findings with:

1. one explicit cursor-bound prepare, complete-header, lower-tier stage, D24
   15d--15h, and D25 15i--15k first-application sequence;
2. one direct-only complete tier-11a--15c reservation, lexical
   `_DirectM4StageResult`, source validation, full plan derivation before both
   header updates, and post-header D25 stage;
3. exact direct 15c evaluation-epoch, ordered override-counter, and immutable
   transition mutations plus old/new/count evidence;
4. requirement observation/currency DML before state/job terminalization and
   exact 15a/15b accounting;
5. a canonical retained changed-key replay projection derived only from the
   existing patch preimages, with explicit exclusion of nonpersisted no-op
   first-application keys; and
6. source-specific zero-DML replay validation, terminal job/attempt closure,
   historical contribution identity, and the validated current retained D25
   accumulator.

Three reviewers rehashed the final uncommitted bytes and independently returned
`GO`, `P0=0`, `P1=0`, `P2=0`. After commit, the authority/semantics reviewer
and direct-15c/PostgreSQL reviewer independently verified the exact commit,
tree, sole parent, one added path, file hash, line/byte counts, and clean
worktree and again returned `GO`, `P0=0`, `P1=0`, `P2=0`. The replay specialist
also returned the same post-commit identity verdict.

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
10. `docs/workstreams/m5_runtime_implementation/D28_CONTRACT_FREEZE_HANDOFF.md`
    (this file).

The reviewed D28 amendment, every D24--D27 amendment, migrations 013--017,
source/test paths, implementation worktrees, protected local-main files,
database, provider, deployment setting, and runtime mode are outside ownership.

## 4. Required authority result

The owned documents freeze one decision only:

```text
M5-D28 = first application uses explicit cursor-bound phased composition;
          direct_transition uses a complete pre-15c reservation and lexical
          stage evidence; historical apply validates the canonical retained
          changed-key projection and returns the current retained accumulator
```

The result advances the M5.4 runtime addendum to revision 9 and adds acceptance
row `M5.0-28` as contract-`PASS` / implementation-`PENDING`. It records D28's
narrow precedence over only the contradictory D24, D25, and runtime-addendum
relative-DML sentences named by the accepted amendment. It edits neither those
accepted amendment bytes nor migrations 013--017.

After this freeze is integrated and pushed, implementation may resume only
under a new separately committed and audited path-exclusive Task-2 activation
whose exact parent is that pushed D28 authority barrier. The held uncommitted
`D27_TASK2_STORE_RUNTIME_CONTINUATION_ACTIVATION.md` draft and the original
unstarted C1/C2/C3/D grants do not supply ownership. Lane-A/Lane-B accepted
bytes remain scoped foundation/helper evidence and are read-only unless the new
activation explicitly reopens a path.

## 5. Non-change and status ceiling

This tranche does not:

- change a public API, DTO, digest, counter, schema, migration, source kind,
  reference kind, present/absence recipe, M4-v1 byte, result, or measured value;
- accept any source/test implementation or executable Task-2 evidence;
- create a process cache, full repository hydration, oracle scan, new timing
  anchor, or external call inside a transaction;
- activate the M5 public runtime, a real database, deployment, or provider; or
- establish performance, utility, model/AI quality, objective truth, security,
  novelty, or named-system-superiority claims.

Runtime remains `v1_only` outside isolated fixtures. M5-D24 through M5-D28 and
M5.0-24 through M5.0-28 remain implementation-`PENDING`; Task 2, M5.4-05
through M5.4-09, every M5.5/M5.6 gate, deployment, and AI-quality claims remain
`PENDING`.

## 6. Protected local main

The dirty local-main checkout is user-owned and excluded from this tranche:

```text
2af4b19962dc8a7d22e377be17f342530a06ee6395bbbf2a092eab36599c8fc2  pyproject.toml
45c20ca46e9ad5bcd86b22c0d8882d1d611497f57ca8c45d3dea149260c110cd  docs/presentations/groundloop_fyp_professor_feedback.pdf
59a13cd8d4bbb017e712c0f39e70f2eba136557e945f64f1b1fc3891742a79f0  docs/presentations/groundloop_fyp_professor_feedback_v2.pdf
c12929c349a5c0be9793159143b09da40ea2a0b27df37b92d61d9ed6483d8c2a  docs/presentations/render_groundloop_fyp_professor_deck.py
167d1e7df5a720041fe0ff51879d08357f0dfbe3a7781ffaa08c0d958a47aa94  docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT_DRAFT.md
```

Never reset, clean, stash, reformat, stage, commit, copy, or delete those
paths. Any protected hash or porcelain drift stops the tranche.

## 7. Freeze verification

Before integration the coordinator must verify:

1. the exact candidate identity and content hash in Section 1;
2. this freeze commit changes exactly the ten Section-3 paths and never changes
   the reviewed amendment bytes;
3. every authority surface names M5-D28, M5.0-28, and runtime-addendum revision
   9 consistently;
4. no implementation, runtime activation, deployment, measured, or AI-quality
   claim is promoted;
5. the protected local-main hashes and porcelain remain exact; and
6. two independent reviewers return `GO`, `P0=0`, `P1=0` on identical freeze
   commit/tree bytes before integration and push.

Any byte edit restarts both freeze reviews. Integration creates authority only;
it does not activate a Task-2 implementation lane.
