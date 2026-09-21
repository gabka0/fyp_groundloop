# M5-D29 Contract Authority-Freeze Handoff

Status: authority-only freeze candidate; no migration, installer, source,
test, database, provider, deployment, runtime-mode, performance, or AI-quality
change is authorized until this exact tranche receives two independent same-
byte `GO`, `P0=0`, `P1=0` reviews and is integrated and pushed

Date: 2026-09-22

## 1. Exact activation point

```text
freeze_parent = 4667da0e230d2b52b3147d0ee771396b406ed1e9
freeze_parent_tree = 5f7e69e6f2f503fc3a36691fee16afa40bc8a479
reviewed_candidate_commit = 4667da0e230d2b52b3147d0ee771396b406ed1e9
reviewed_candidate_tree = 5f7e69e6f2f503fc3a36691fee16afa40bc8a479
reviewed_candidate_parent = 28a1392ccba9592ef41b1e51969c094f1f4b922a
reviewed_candidate_parent_tree = d96723faeee2bd3cce2098df77fb6dbdf8f3f561
reviewed_candidate_sha256 = e05f159f98d5f282335a90d2e9db1a26f8d85560030314060d918df59ccc82fb
reviewed_candidate_lines = 1322
reviewed_candidate_bytes = 81878
branch = workstream/m5-d29-contract-freeze
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d29-contract-freeze
runtime_mode = v1_only
```

The reviewed file is
`docs/workstreams/m5_runtime_contract/BOUNDED_DOCUMENT_WITHDRAWAL_AMENDMENT.md`.
It is immutable throughout this authority tranche. The candidate commit is an
exact one-file direct child of the pushed D28 Task-2 activation barrier.

## 2. Acceptance evidence

The D29 candidate was intentionally not accepted at its earlier draft hashes.
Independent reviews exposed and rechecked the complete set of persisted-source
and continuation defects: reconstructed legacy payload identity, missing
reverse-candidate and all-state direct-job locators, circular root
completeness, incomplete dependency/scope/provenance ranges, migration-017
tuple-width inheritance, cross-policy schema impossibility, direct revision/
hydration order, missing terminal-cut provenance, and the D24-valid unresolved
terminal dispatch boundary.

The final bytes resolve those findings with:

1. locked epoch event/payload identity plus independent exact validation of
   every normalized M4/M5 sidecar;
2. one existing-manifest typed declaration containing exactly one top-level
   `m5_d29_document_declaration_v1` key and exactly three sorted-unique text
   arrays, later compared with store-recomputed authority;
3. independent bounded candidate and observation projections, all-state direct
   job/dependency/scope enumeration, and all-state M5 job/scope/provenance
   enumeration;
4. an explicit same-policy supported form as D29's sole semantic-admissibility
   supersession, with cross-policy and typed rootless requirement history
   remaining fail-closed;
5. ordinary pre-opener terminal replay plus one exact package-private post-open
   checked cutoff using the held receipt, canonical result, and zero call work;
6. exact preservation of activation-bootstrap observations and D24-valid
   unresolved dispatch/attempt ambiguity attached only to terminal authority;
   and
7. one future migration-018 contract with exactly two locator indexes, one
   atomic ledger-first installer protocol, and one exact route barrier.

Three reviewers rehashed the final candidate bytes and independently returned
`GO`, `P0=0`, `P1=0`, `P2=0`. After commit, the contract/runtime and
PostgreSQL/schema reviewers independently verified the exact commit, tree,
sole parent, one added path, file hash, line/byte counts, and clean worktree;
each confirmed its `GO`, `P0=0`, `P1=0` verdict on the identical committed
bytes. The candidate commit was then pushed to `origin/main` unchanged.

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
10. `docs/workstreams/m5_runtime_implementation/D29_CONTRACT_FREEZE_HANDOFF.md`
    (this file).

The reviewed D29 amendment, every D24--D28 amendment, migrations 013--017,
the D28 activation, every source/test path, Lane-P and other implementation
worktrees, protected local-main files, database, provider, deployment setting,
and runtime mode are outside ownership.

## 4. Required authority result

The owned documents freeze one decision only:

```text
M5-D29 = persisted legacy document source uses locked epoch identity;
          reverse/current withdrawal and retained direct/M5 declarations use
          exact bounded locators; supported withdrawal is same-policy; exact
          terminal cuts retain canonical result authority; migration 018 may
          add only the two frozen locator indexes behind its exact barrier
```

The result advances the M5.4 runtime addendum to revision 10 and adds
acceptance row `M5.0-29` as contract-`PASS` /
implementation-`PENDING`. It records D29's narrow precedence over only the
D25, D28, revision-9, C5/C6/C7, and D28-activation sentences explicitly named
by the accepted amendment. Every other frozen identity, migration, phase,
counter, replay, work, timing, and semantic rule remains exact.

The five precedence cuts remain exact:

1. D25 Section 8 and Sections 9.1/9.3, D28 Section 4, and revision-9 Sections
   15/17.2 only for persisted source and bounded withdrawal/declaration
   enumeration;
2. revision-9 Section 11.1 only for same-policy supported withdrawal;
3. the exact C5/C6/C7 checked-origin exclusions only for D29 Section 4.1;
4. D28/revision-9 no-migration and frozen-schema sentences only for the exact
   migration-018 contract; and
5. the D28 activation's migration/index prohibition only after this freeze and
   a new D29 reactivation explicitly replace that implementation boundary.

The contract authorizes a future
`migrations/018_m5_bounded_document_withdrawal.sql` with only:

```text
groundloop_m5_admitted_pair_by_chunk_edge
  ON groundloop_m5_requirement_admitted_pair(chunk_version_id COLLATE "C")
groundloop_m4_job_by_epoch
  ON groundloop_semantic_job(epoch_id)
```

The freeze does not create or accept those bytes, their literal migration or
bundle hashes, installer/constants/tests, or any runtime route. Those become
eligible only under a new activation and their own executable acceptance
evidence.

After this freeze is integrated and pushed, implementation may resume only
under a new separately committed and audited path-exclusive Task-2 activation
whose exact parent is that pushed D29 authority barrier. The accepted D28
activation remains historical authority but is insufficient for corrected
D29 implementation. Its held Lane-P work is read-only non-authority and must
not be committed, merged, rebased, cherry-picked, copied, or represented as
accepted. The new activation must define disjoint ownership, sequential
integration barriers, and two same-byte `GO`, `P0=0`, `P1=0` audits per lane.

## 5. Non-change and status ceiling

This tranche does not:

- change the reviewed D29 candidate, a public API, frozen DTO, semantic/runtime
  digest, counter, source/reference kind, present/absence recipe, result, work,
  timing coordinate, or M4-v1 byte;
- add a tombstone, nullable hash, JSON/`repr` hash, seventh reference kind,
  process cache, retained engine, full hydration, oracle scan, hidden reserve,
  external call inside a transaction, or cross-policy rebase;
- create or accept migration 018, an installer, a schema object, source/test
  implementation, database execution, or executable Task-2 evidence;
- activate the public runtime, a real database, deployment, or provider; or
- establish performance, utility, objective truth, model/AI quality, security,
  novelty, maintained-history, or named-system-superiority claims.

Runtime remains `v1_only` outside isolated fixtures. M5-D24 through M5-D29 and
M5.0-24 through M5.0-29 remain implementation-`PENDING`; Task 2, M5.4-05
through M5.4-09, every M5.5/M5.6 gate, deployment, performance, utility, and
AI-quality claims remain `PENDING`.

## 6. Protected local main

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
displayed untracked files. It has no staged change. Never reset, clean, stash,
reformat, stage, commit, copy, or delete those paths. Upstream distance may
change when authority commits are pushed; any protected path status or hash
drift stops the tranche.

## 7. Freeze verification

Before integration the coordinator must verify:

1. the exact candidate identity and content hash in Section 1;
2. this freeze commit changes exactly the ten Section-3 paths and never changes
   the reviewed amendment bytes;
3. every authority surface names M5-D29, M5.0-29, runtime-addendum revision 10,
   the sole same-policy semantic narrowing, and migration 018 consistently;
4. no migration/installer/source/test implementation, runtime activation,
   deployment, measured, performance, utility, or AI-quality claim is
   promoted;
5. the protected local-main hashes and porcelain remain exact; and
6. two independent reviewers return `GO`, `P0=0`, `P1=0` on identical freeze
   commit/tree bytes before integration and push.

Any byte edit restarts both freeze reviews. Integration creates authority only;
it does not activate a Task-2 implementation lane.
