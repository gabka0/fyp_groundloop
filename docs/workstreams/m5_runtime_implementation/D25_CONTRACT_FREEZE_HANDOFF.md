# M5-D25 Contract Authority-Freeze Handoff

Status: activated authority-only freeze tranche; no migration, source, test,
database, provider, deployment, or runtime-mode change is authorized

Date: 2026-09-03

## 1. Exact activation point

```text
integrated_main_base = 8b3c006fa959e9c0d13f12282853006b4dfbbd78
reviewed_candidate_commit = 002dcace2f89e71ef3a56955647b4d8077e9c91f
reviewed_candidate_sha256 = bac12ab5e74632c04f1bd70d0ef0d00522ba9d268eb8b73d11845bbf3b873aae
reviewed_candidate_lines = 2404
reviewed_candidate_bytes = 115141
branch = workstream/m5-d25-contract-freeze
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d25-contract-freeze
```

The candidate commit has the sole-parent ancestry
`8b3c006 -> a3c7dfc -> f2faf25 -> 24b8829 -> d3262e7 -> 7925999 ->
002dcac`. The authority tranche starts from those reviewed bytes. The candidate
amendment MUST remain byte-identical throughout this tranche.

## 2. Acceptance evidence

Two independent decisive reviews read exact commit `002dcac` and candidate
SHA-256 `bac12ab5e74632c04f1bd70d0ef0d00522ba9d268eb8b73d11845bbf3b873aae`.
The semantic/digest/counter/oracle review and the PostgreSQL/migration/lock/
replay review each returned `GO` with `P0=0`, `P1=0`, and `P2=0`.

Both reviews independently verified:

- the accepted migration-016 five-field prerequisite tuple;
- a clean sole-parent candidate lineage and unchanged protected draft;
- 112/112 runtime contract/digest tests;
- 14/14 public-M4/legacy compatibility tests;
- the exact 71-byte empty logical output with SHA-256
  `b4e641b66a06cb7d204377c37cfe031d958ce6d959832620fc2e9441339581c3`;
- all 37 matching/overlay work counters and all 40 D25 falsifiers; and
- the final structural-open authorization and keyed-malformed-current audit
  precedence rules.

The accepted prerequisite tuple is:

```text
accepted_016_bundle_id = "m5-runtime-recovery-schema-bundle-v1"
accepted_016_migration_sha256 = a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7
accepted_016_bundle_sha256 = 28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565
accepted_016_oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
accepted_016_prerequisite_sha256 = b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd
```

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
10. `docs/workstreams/m5_runtime_implementation/D25_CONTRACT_FREEZE_HANDOFF.md`
    (this file).

The reviewed
`docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT.md` is an
immutable input, not an editable authority-freeze path. The protected untracked
`PERSISTED_MATCHING_AMENDMENT_DRAFT.md`, user `pyproject.toml`, presentation
files, migrations, source, tests, databases, providers, deployment state, and
runtime mode are outside ownership.

## 4. Required authority result

The exact ten-path freeze must record only:

- M5-D25 contract `PASS` / implementation `PENDING`;
- runtime-addendum revision 6, with the reviewed candidate as authoritative;
- migration 017 as the next separately activated implementation barrier;
- M5.0-24 implementation `PENDING`;
- M5.4-05 through M5.4-09 `PENDING`;
- all M5.5/M5.6 gates `PENDING`; and
- runtime mode `v1_only` outside isolated fixtures.

It MUST NOT claim implementation, PostgreSQL/live evidence, activation,
deployment, model-quality improvement, maintained-history results, M5.4
completion, M5 completion, security, novelty, or named-system superiority.

## 5. Freeze and integration gates

1. Edit only the ten paths above and keep the candidate bytes at the reviewed
   SHA-256.
2. Run document consistency, stale-status, exact-path, digest, diff-check, and
   focused non-database regression gates.
3. Commit the exact authority bytes.
4. Obtain two independent same-byte reviews: one contract/status/claim audit
   and one authority/ownership/integration audit. Any P0/P1 or byte edit returns
   this tranche to `HOLD`.
5. Fast-forward main only from its unchanged base after both reviews return
   `GO`; recheck every protected hash before and after integration.
6. Push the exact integrated main commit to `origin/main`.
7. Only then may a separate docs-only migration-017 activation name disjoint
   implementation paths. This handoff grants no implementation ownership.

## 6. Independent reviewer prompt

```text
Audit the exact M5-D25 authority-freeze commit read-only. Verify that the
reviewed candidate SHA remains bac12ab5e74632c04f1bd70d0ef0d00522ba9d268eb8b73d11845bbf3b873aae,
that all ten authority paths consistently record D25 contract PASS and
implementation PENDING, runtime-addendum revision 6, migration 017 as the next
separate barrier, runtime v1_only, M5.0-24 implementation PENDING, and
M5.4-05..09/M5.5/M5.6 PENDING. Reject any implementation, live-PostgreSQL,
deployment, AI-quality, security, novelty, or superiority overclaim. Verify
exact path scope, ancestry, protected hashes, status-table consistency, and
that no migration/source/test/database/runtime change occurred. Return GO only
with zero unresolved P0/P1 and report exact commit/tree/path SHA evidence. Do
not edit files or authorize migration 017 implementation.
```

## 7. Protected main-worktree ledger

The following main-worktree inputs were verified immediately before activation
and remain outside this branch:

```text
pyproject.toml = 2af4b19962dc8a7d22e377be17f342530a06ee6395bbbf2a092eab36599c8fc2
groundloop_fyp_professor_feedback.pdf = 45c20ca46e9ad5bcd86b22c0d8882d1d611497f57ca8c45d3dea149260c110cd
groundloop_fyp_professor_feedback_v2.pdf = 59a13cd8d4bbb017e712c0f39e70f2eba136557e945f64f1b1fc3891742a79f0
render_groundloop_fyp_professor_deck.py = c12929c349a5c0be9793159143b09da40ea2a0b27df37b92d61d9ed6483d8c2a
PERSISTED_MATCHING_AMENDMENT_DRAFT.md = 167d1e7df5a720041fe0ff51879d08357f0dfbe3a7781ffaa08c0d958a47aa94
```
