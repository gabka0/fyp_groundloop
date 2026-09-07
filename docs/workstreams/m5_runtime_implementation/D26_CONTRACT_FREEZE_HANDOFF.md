# M5-D26 Contract Authority-Freeze Handoff

Status: activated authority-only freeze tranche; no migration, source, test,
database, provider, deployment, runtime-mode, or AI-quality change is
authorized

Date: 2026-09-07

## 1. Exact activation point

```text
integrated_main_base = 691e3d174e059ac041d3a46678fcb630e15478d5
reviewed_candidate_commit = ad04a372cd106f1702cddffcbafad56e826c8bc4
reviewed_candidate_tree = 2dba444399920262f386631f5b2cb578701b06c9
reviewed_candidate_sha256 = 85372d4c2f9108810bd75c3e5611de541d0f31c8a096421f30e68fad84676721
reviewed_candidate_lines = 422
reviewed_candidate_bytes = 19924
branch = workstream/m5-d26-contract-freeze
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d26-contract-freeze
```

The candidate ancestry is exactly
`691e3d1 -> fcd708b -> 4f93afd -> ad04a37`. This authority tranche starts
from the final reviewed commit. Both
`docs/workstreams/m5_runtime_contract/CHANGED_STATE_ABSENCE_AMENDMENT.md` and
`docs/workstreams/m5_runtime_implementation/D26_CONTRACT_REVIEW_HANDOFF.md`
are immutable inputs throughout the tranche.

## 2. Acceptance evidence

The review process retained every blocking result instead of pooling it with a
later pass:

1. At exact commit `fcd708b`, tree `ece3743`, and candidate SHA-256
   `5ad87a8b43b869911d7092038b9eda1bbda6c42b33b934c8b125ff1c2f88753c`,
   both reviews returned `HOLD`. Each found the missing independent derivation
   of the frozen structural payload; the semantic review also requested
   explicit requirement-state positives for both structural actions.
2. At exact commit `4f93afd`, tree `94a0b96`, and candidate SHA-256
   `401ab5863c3869bb1fbcb8bc96e9adb3e01d6568bf9d13aab791587096a63a0d`,
   the semantic review returned `GO`, but the PostgreSQL review returned
   `HOLD`. Mandatory falsifier 16 overclaimed the D26 replacement inventory
   relative to separately accepted D25 migration-014 authority.
3. Both audits restarted after the second byte change. On exact final commit
   `ad04a372cd106f1702cddffcbafad56e826c8bc4`, tree
   `2dba444399920262f386631f5b2cb578701b06c9`, and candidate SHA-256
   `85372d4c2f9108810bd75c3e5611de541d0f31c8a096421f30e68fad84676721`,
   the independent semantic/digest and PostgreSQL/enforceability reviewers
   each returned `GO` with `P0=0`, `P1=0`, and `P2=0`.

Only the third-cycle same-byte verdicts are decisive. Both independently
verified the exact typed absence digest, the three allowed existing kinds,
unchanged six-kind/outer/set/present recipes, canonical D25 present-to-None
bijection, present predecessor and exact closure, same-object successor
absence, independently derived REPLACE/RETIRE payload, one deactivation,
exact seal coordinates, zero-write replay, and one-function D26 migration
authority. Each also reproduced 112/112 runtime contract/digest tests and
14/14 public-M4/legacy tests. Neither used a database or changed files or Git.

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
10. `docs/workstreams/m5_runtime_implementation/D26_CONTRACT_FREEZE_HANDOFF.md`
    (this file).

The reviewed candidate/review paths, protected main-worktree files, retained
D25 draft, D25 implementation branches, migrations, source, tests, databases,
providers, deployment state, and runtime mode are outside ownership.

## 4. Required authority result

The exact ten-path freeze records only:

- M5-D26 and M5.0-26 contract `PASS` / implementation `PENDING`;
- runtime-addendum revision 7 and the reviewed candidate as authoritative;
- the exact existing six kinds, outer reference/set, and present recipes;
- migration 017 authority to replace only
  `groundloop_m5_validate_event_result_children()` among migration-015
  enforcement objects and as D26's only additional pre-017 replacement;
- separately accepted D25 authority unchanged;
- M5-D25/M5.0-25 implementation `PENDING`;
- M5.4-05 through M5.4-09 and all M5.5/M5.6 gates `PENDING`; and
- runtime mode `v1_only` outside isolated fixtures.

It must not claim implementation, live PostgreSQL evidence, runtime
activation, deployment, model/AI-quality improvement, maintained-history or
utility results, M5.4/M5 completion, security, novelty, or named-system
superiority.

## 5. Freeze and integration gates

1. Edit only the ten paths above and retain both candidate inputs byte-exact.
2. Run document consistency, stale-status, decision-row, stage-row,
   exact-path, digest, diff-check, protected-hash, and focused non-database
   regression gates.
3. Commit the exact authority bytes without self-referencing that commit.
4. Obtain two independent same-byte reviews: one contract/status/claim audit
   and one authority/ownership/integration audit. Any P0/P1 or byte edit
   returns the tranche to `HOLD` and restarts both reviews.
5. Fast-forward main only from unchanged base `691e3d1` after both reviews
   return `GO`; verify every protected hash before and after integration.
6. Push the exact integrated main commit to `origin/main`.
7. Only then create a separate docs-only path-exclusive migration-017
   activation that pins the freeze commit/tree. This handoff grants no
   implementation ownership.

## 6. Contract/status reviewer prompt

```text
Audit the exact M5-D26 authority-freeze commit read-only. Verify that the
accepted candidate remains SHA-256 85372d4c2f9108810bd75c3e5611de541d0f31c8a096421f30e68fad84676721
and both candidate inputs are unchanged. Verify all ten authority paths
consistently record D26/M5.0-26 contract PASS and implementation PENDING,
runtime-addendum revision 7, the exact three-of-six absence boundary,
unchanged outer/set/present recipes, the one migration-015 validator
replacement, separately accepted D25 authority unchanged, D25 implementation
PENDING, runtime v1_only, and all remaining M5.4/M5.5/M5.6 gates PENDING.
Reject implementation, live-PostgreSQL, deployment, AI-quality, utility,
security, novelty, or superiority overclaim. Return GO only with P0=0 and
P1=0; report exact commit/tree/path hashes and P2. Do not edit anything or
authorize implementation.
```

## 7. Authority/ownership reviewer prompt

```text
Audit the exact M5-D26 authority-freeze commit read-only. Verify sole-parent
ancestry from reviewed candidate ad04a372, exactly ten changed authority paths,
no candidate/migration/source/test/database/runtime-mode change, exact
candidate and protected hashes, 26 unique M5.0 decision rows ending at
M5.0-26, one D26 design heading/decision row, runtime revision 7, and unchanged
M5.4-01..04 PASS with M5.4-05..09/M5.5/M5.6 PENDING. Verify the freeze grants
no implementation ownership and requires a later commit/tree-pinned
path-exclusive plan. Return GO only with P0=0 and P1=0; report exact
commit/tree/path hashes and P2. Do not edit Git/files/database or integrate.
```

## 8. Protected main-worktree ledger

The following user-owned main-worktree inputs were reverified before the
authority edit and remain outside this branch:

```text
pyproject.toml = 2af4b19962dc8a7d22e377be17f342530a06ee6395bbbf2a092eab36599c8fc2
groundloop_fyp_professor_feedback.pdf = 45c20ca46e9ad5bcd86b22c0d8882d1d611497f57ca8c45d3dea149260c110cd
groundloop_fyp_professor_feedback_v2.pdf = 59a13cd8d4bbb017e712c0f39e70f2eba136557e945f64f1b1fc3891742a79f0
render_groundloop_fyp_professor_deck.py = c12929c349a5c0be9793159143b09da40ea2a0b27df37b92d61d9ed6483d8c2a
PERSISTED_MATCHING_AMENDMENT_DRAFT.md = 167d1e7df5a720041fe0ff51879d08357f0dfbe3a7781ffaa08c0d958a47aa94
```

## 9. Executed authority-freeze checkpoint

The exact candidate remains unchanged at SHA-256
`85372d4c2f9108810bd75c3e5611de541d0f31c8a096421f30e68fad84676721`.
The nine other authority paths consistently record M5-D26/M5.0-26 contract
`PASS` / implementation `PENDING`, runtime-addendum revision 7, the narrow
absence-reference and one-validator boundary, D25 implementation `PENDING`,
runtime `v1_only`, and every remaining stage/evaluation gate `PENDING`.

The immutable candidate inputs are:

```text
CHANGED_STATE_ABSENCE_AMENDMENT.md = 85372d4c2f9108810bd75c3e5611de541d0f31c8a096421f30e68fad84676721
D26_CONTRACT_REVIEW_HANDOFF.md = 5a8b06de8271117e25874b59ea6850324b7951901d5dc430d1f05e24d738d020
```

Pre-commit authority SHA ledger:

```text
AGENTS.md = 07c2f3f8516924eedd7d7bf49a4f270dae9bf5e39ccdacd6a7352e8137812460
docs/m5_design_freeze.md = c398b97fba58336797db283c1313699ea7de321cd40483293198fba88ab48a0e
docs/workstreams/m5_runtime_contract/CANDIDATE_RUNTIME_ADDENDUM.md = c475802e4920b2f804096bc034f1b7026f5d13d5e9f93f91b5f35bd6d21fe48c
docs/m5_acceptance_matrix.md = 620d7d2d216777f9b4c6b2a405fa604775769a8045b4bfa25223e9d528722f0f
docs/m5_implementation_plan.md = aacd84b622d167416f9c86cef3b3dd27d32db65919b47252efffaaf1cf4ef89b
docs/m5_multiagent_execution_plan.md = c6fdd55b3ca14f9dacf05867b618fc013f9ddba0c446ab51aaafe4eacdedc741
docs/decision_log.md = c9762e8e8bea4f27655bcfd3a3539e14c0a40ab6e23b21ee48dd31ce629c4cb5
docs/m5_implementation_status.md = d5d795da8c47d975c5516dd433c71f39f09dcea2d947a1fe64edc8cba30f6f00
docs/roadmap.md = 12a0fdde0b244d19c121d1b608812c5c7de2eb297e92d64ce1e0054df8f366f2
```

Pre-commit gates on these bytes:

- exact ten-path authority ownership: pass;
- `git diff --check`: pass;
- 26 unique decision rows ending at M5.0-26: pass;
- exactly one D26 design heading and one frozen-decision row: pass;
- runtime-addendum revision 7 with D26 precedence at Section 23 and final
  decision at Section 24: pass;
- M5.4 has nine rows with 01--04 `PASS` and 05--09 `PENDING`: pass;
- all nine M5.5 and all ten M5.6 rows remain `PENDING`: pass;
- migration 017 remains absent from this branch; migration/source/test/runtime
  changes: none;
- runtime contract/digest tests: 112/112 passed;
- public-M4/legacy compatibility tests: 14/14 passed;
- both held D25 implementation worktrees: clean and read-only;
- all five protected main-worktree hashes: unchanged; and
- database, network, provider, deployment, runtime-mode, and AI-model
  operations: none.

The containing authority commit/tree are intentionally recorded only by the
post-commit reviewers and later activation, not inside these self-containing
bytes.
