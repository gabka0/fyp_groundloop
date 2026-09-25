# M5-D31 Contract Authority-Freeze Handoff

Status: authority-only freeze candidate; contract candidate is accepted but
this ten-path freeze is not authority until its independent reviews, commit,
integration, and push pass; no migration, installer, source, test, database,
provider, deployment, runtime-mode, performance, security, utility, or
AI-quality result is implemented or promoted by this tranche

Date: 2026-09-25

## 1. Exact accepted candidate

```text
reviewed_candidate_commit = abce709d25e00c5774ac1b49cf046f2783e5fb2c
reviewed_candidate_tree = 2ab0a97206a486cd07f0308116ef8e1059eec514
reviewed_candidate_parent = ee14d697dd2dbf68b33e9a6c0e3d76791afc5520
reviewed_candidate_path = docs/workstreams/m5_runtime_contract/PRETERMINAL_CONTEXT_ACCESS_AMENDMENT.md
reviewed_candidate_change = A
reviewed_candidate_mode = 100644
reviewed_candidate_blob = 8ed4e8e451f21fce5685efc07eb272a4df680885
reviewed_candidate_sha256 = 6331c8149e38031c51cb22b6a9dc2d49d30d27df67d25b5fae24b00c52058dc9
reviewed_candidate_lines = 424
reviewed_candidate_bytes = 21523
reviewed_candidate_changed_path_count = 1

runtime_addendum_revision = 12
runtime_mode = v1_only
```

The accepted candidate is the sole added path in commit `abce709`. It is an
immutable input to this authority freeze and is outside this tranche's edit
ownership. Its exact content hash, Git blob, line count, byte count, commit,
tree, and sole-parent identity are recorded above.

## 2. Acceptance evidence

Two independent reviewers audited the identical candidate bytes at SHA-256
`6331c8149e38031c51cb22b6a9dc2d49d30d27df67d25b5fae24b00c52058dc9`:

1. the authority/semantic audit returned `GO`, `P0=0`, `P1=0`, `P2=0`; and
2. the PostgreSQL/runtime-boundary audit returned `GO`, `P0=0`, `P1=0`,
   `P2=0`.

The accepted correction is narrow. Migration 017 creates the genuine
promotion context triplet inside trusted `SECURITY DEFINER` execution, so a
supported non-owner runtime session cannot directly read its temporary
context and must not pretend to own it. The accepted D31 contract authorizes
one trusted, read-only, argument-bound accessor rather than weakening
temporary-table ownership, granting raw access, trusting GUCs alone, or
starting deferred validation early.

## 3. Exact authority-freeze path ownership

This docs-only authority tranche owns exactly these ten paths:

1. `AGENTS.md`;
2. `docs/m5_design_freeze.md`;
3. `docs/workstreams/m5_runtime_contract/CANDIDATE_RUNTIME_ADDENDUM.md`;
4. `docs/m5_acceptance_matrix.md`;
5. `docs/m5_implementation_plan.md`;
6. `docs/m5_multiagent_execution_plan.md`;
7. `docs/decision_log.md`;
8. `docs/m5_implementation_status.md`;
9. `docs/roadmap.md`; and
10. `docs/workstreams/m5_runtime_implementation/D31_CONTRACT_FREEZE_HANDOFF.md`
    (this file).

The accepted D31 candidate, every D24--D30 amendment and handoff, migrations
001--018, migration 019, all source and test paths, databases, providers,
deployment/runtime configuration, and every implementation worktree are
outside this freeze's ownership. Historical D30 statements remain checkpoint
evidence and are not rewritten as though D31 had existed at that time.

## 4. Required authority result

The ten owned documents freeze one decision:

```text
M5-D31 = a legitimate non-owner runtime obtains the unique genuine
          migration-017 preterminal promotion-context coordinates only
          through one exact trusted read-only accessor installed by one
          additive migration 019; migrations 001--018 remain exact and no
          other schema object, privilege, API, DTO, digest, counter, output,
          provider, or runtime-mode change is authorized
```

The result records M5-D31 and M5.0-31 as contract-`PASS` /
implementation-`PENDING` and advances the M5 runtime addendum from revision 11
to revision 12. It supersedes only the D30/M5.0-30 negative clauses that
forbade migration 019 and any new SQL function. The two corresponding
M5.0-30 evidence cuts become exact:

1. the negative falsifier is that migration 019 is missing, differs from exact
   M5-D31, or is accompanied by another migration; and
2. the positive evidence is frozen migrations 001--018 plus exact M5-D31
   migration 019.

Every other D30/M5.0-30 semantic, provenance, migration-018, identity,
runtime, replay, and claim boundary remains exact.

Migration 019 may add exactly:

```text
migrations/019_m5_preterminal_seal_context.sql

groundloop_m5_matching_read_preterminal_seal_context(
    bigint,
    bigint,
    bigint
)
```

The function is the sole new schema object. Its exact eight-column return
shape, `STABLE`, `CALLED ON NULL INPUT`, `SECURITY DEFINER`, pinned trusted
search path, fail-closed context proof, read-only behavior, owner equality,
and explicit `PUBLIC EXECUTE` are governed by the accepted amendment. No raw
temporary-table privilege or existing private-function privilege is widened.

This freeze does not install migration 019 or accept any implementation byte.
The exact migration and bundle hashes remain to be pinned by the later
implementation activation after review of final schema-lane bytes.

## 5. Sequential implementation barrier

Implementation may begin only after this ten-path freeze is independently
reviewed, committed, integrated, and pushed. A new docs-only path-exclusive
D31 implementation activation must then allocate three sequential lanes:

1. **schema 019:** migration, ledger-first installer, exact catalog and
   privilege checks, owner/non-owner live falsifiers, and schema handoff;
2. **revised C1-R:** package-private runtime preparation consumes the installed
   accessor exactly once, retains all independent envelope checks, and proves
   owner/non-owner behavior plus preterminal/result-bound equality; and
3. **resumed C1:** public store/runtime structural composition continues only
   after the revised C1-R barrier is accepted and integrated.

The schema lane must integrate before revised C1-R, and revised C1-R must
integrate before C1. The current uncommitted C1-R candidate is read-only
evidence until schema 019 integrates. Its valid distinct-mask-group counter
work may be retained under the later activation, but its owner-only
preterminal helper is not accepted. Existing D30 C1-R/C1 activations are
historical and insufficient for this corrected scope. No reset, clean, stash,
rebase, cherry-pick, manual copy, or patch transplant may bypass lineage,
dirty-state, and path-overlap checks.

## 6. Non-change and claim ceiling

This authority freeze does not:

- edit the accepted D31 amendment or historical D30 authority records;
- implement migration 019, its installer, the trusted accessor, revised C1-R,
  C1 composition, or any source/test change;
- change public APIs, DTOs, digest recipes, counters, reference kinds,
  present-state recipes, result bytes, work/timing identities, M4-v1 bytes,
  model/provider configuration, or runtime mode;
- grant direct access to promotion temporary tables or private migration-017
  helpers; or
- establish deployment, performance, scalability, utility, security, privacy,
  novelty, objective-truth, maintained-history, named-system-superiority, or
  AI/model-quality claims.

```text
M5-D31 contract = PASS
M5.0-31 contract = PASS
M5-D31 implementation = PENDING
M5.0-31 implementation = PENDING
preterminal_context_accessor = NOT_IMPLEMENTED
migration_019 = NOT_IMPLEMENTED
C1-R prerequisite repair = PENDING
C1 structural composition = PENDING
M5-D24 through M5-D31 implementation = PENDING
Task 2 = PENDING
M5.4-05 through M5.4-09 = PENDING
M5.5 and M5.6 = PENDING
runtime_mode = v1_only
deployment/security/utility/AI-quality = PENDING
```

D31 adds M5.0-31. It does not create an M5.4-10 row or close any existing
implementation/evaluation gate. The larger independently adjudicated
natural-history evaluation and AI-quality work remain separate mandatory
work.

## 7. Protected checkout and implementation custody

The user-owned checkout remains outside this tranche and is unchanged from its
protected checkpoint:

```text
protected_checkout = /home/kassym/Desktop/groundloop
protected_head = 14598ae51562006eaf67850b19e8212f38997903
protected_tree = 36af2be4c58572c5adde68279d1a3aacedd0beff
protected_index = empty
2af4b19962dc8a7d22e377be17f342530a06ee6395bbbf2a092eab36599c8fc2  pyproject.toml
45c20ca46e9ad5bcd86b22c0d8882d1d611497f57ca8c45d3dea149260c110cd  docs/presentations/groundloop_fyp_professor_feedback.pdf
59a13cd8d4bbb017e712c0f39e70f2eba136557e945f64f1b1fc3891742a79f0  docs/presentations/groundloop_fyp_professor_feedback_v2.pdf
c12929c349a5c0be9793159143b09da40ea2a0b27df37b92d61d9ed6483d8c2a  docs/presentations/render_groundloop_fyp_professor_deck.py
167d1e7df5a720041fe0ff51879d08357f0dfbe3a7781ffaa08c0d958a47aa94  docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT_DRAFT.md
```

Its exact porcelain is one unstaged modified `pyproject.toml`, the untracked
`docs/presentations/` tree containing the three listed files, and the untracked
persisted-matching draft. No freeze or later implementation lane may reset,
clean, stash, reformat, stage, commit, copy or delete those paths.

The held C1-R candidate is:

```text
c1_r_worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d30-c1-prerequisite-blocker-repair
c1_r_branch = workstream/m5-d30-c1-prerequisite-blocker-repair
c1_r_head = ee14d697dd2dbf68b33e9a6c0e3d76791afc5520
c1_r_index = empty
c1_r_raw_porcelain_v2_z_sha256 = da234ab487e21d296e6699792ecdbf9608607afdffca83d39b9fd483c548d1bb
2b18ca48d5276bc95961d2cbe84bca25244df49a72dba0e728f9e7bc681ba6e9  src/groundloop/m5/runtime/postgres_matching.py
80e9ab187a04cac2a95f71dc4a378997ebc4e2bcb4b0662e2cfe750276268563  src/groundloop/m5/runtime/postgres_matching_publication.py
ed63c156fd742bb32ff78ea0485d5626e270848cbfc9f9cff20b01d7c7052577  tests/m5/postgres_runtime/d25_publication/test_changed_state_references.py
b2ae3e149ea72aa0ed6f6e3e23553fdad350afb4190dea5a23d499134bed687b  tests/m5/postgres_runtime/d25_publication/test_seal_promotion.py
5bad539816933f2bae3eb9f461aac60d9dc1f3182e4772ba13cc605111ed36fb  tests/m5/postgres_runtime/d25_store_core/test_retirement_work_counter.py
```

The held C1 structural-composition candidate is:

```text
c1_worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d30-structural-composition
c1_branch = workstream/m5-d30-structural-composition
c1_head = 81a642e9e60f2e33e2dbe4cb62ef699a380cc26b
c1_index = empty
c1_raw_porcelain_v2_z_sha256 = 736f23f62b8ae744763219ab592266318662636dec4bcbc8a7be93e56ed0d58d
d697ff7b561f638d5148151413553e0595d8689345e04ed578f1f6de8a6f0d78  src/groundloop/m5/runtime/persistence.py
532df5887c20f8d40291b45a5d359d32752803b9784e5e160351aeddd83e8032  src/groundloop/m5/runtime/postgres_recovery.py
bcc3d6ff96cd68d071fb3bb28932edab1279e621488bd78d1eb7743fcd522762  docs/workstreams/m5_runtime_implementation/D30_STRUCTURAL_COMPOSITION_HANDOFF.md
2aaee9c9a483a52392ded87d0b0771b17f037b94df3a95eb6d79c23a05c6581f  tests/m5/postgres_runtime/d30_application/conftest.py
f068e534dd33f6a2751d4dcb17ccec505dd46a58328404587ea1fa329bfaa6ee  tests/m5/postgres_runtime/d30_application/test_seal_atomicity.py
63ed0caf131324e7aa52a887378ba9b929e626a3f05e431039189e70ae0835cb  tests/m5/postgres_runtime/d30_application/test_store_composition.py
0f14fde2fe8c3ebe78b933de9fae040012cbf2cab8200239ead45dbc60803179  tests/m5/postgres_runtime/d30_application/test_store_races.py
6dd8f98998524b3d7dda9acc2b4f5ee7c54eb74581bc85727c6fec965d9125e1  tests/m5/postgres_runtime/d30_application/test_structural_open.py
9c135c6250d1b572a611d2c57f84db1646b8e742a4642601728cda003fc0b30c  tests/m5/postgres_runtime/d30_application/test_structural_order.py
```

Both held worktrees are read-only non-authority during the D31 freeze and the
schema-019 lane. The later activation must recheck every entry, the empty
indexes, raw porcelain hashes, path overlap and exact lineage before any
fast-forward. No reset, checkout, clean, stash, rebase, cherry-pick, copy,
patch transplant or manual reconstruction may substitute for that check.

## 8. Freeze verification and next handoff

Before integration, reviewers must verify:

1. only the ten paths in Section 3 changed relative to candidate commit
   `abce709`;
2. the accepted candidate bytes and identity in Section 1 are unchanged;
3. M5-D31, M5.0-31, revision 12, the exact one-function migration-019
   exception, and the three-lane sequential barrier agree across all ten
   documents;
4. D30 historical wording remains preserved except the two explicit current
   M5.0-30 acceptance-matrix cuts authorized by D31;
5. no migration, source, test, database, provider, deployment, or runtime
   artifact changed; and
6. two independent whole-byte authority-freeze audits return `GO`, `P0=0`,
   `P1=0` before commit, followed by exact postcommit identity confirmation.

The eventual implementation activation must cite the pushed D31 authority
commit and tree as its exact parent barrier and publish disjoint ownership for
schema 019, revised C1-R, and resumed C1. Until that activation exists, all
three remain blocked by design rather than silently inherited from D30.
