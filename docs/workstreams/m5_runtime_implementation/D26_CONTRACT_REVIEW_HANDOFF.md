# M5-D26 Changed-State Absence Contract Review Activation

Status: candidate-review lane; no authority freeze, migration, source, test,
database, runtime-mode, deployment, or milestone change is authorized

Date: 2026-09-06

## 1. Exact activation point

```text
base_commit = 691e3d174e059ac041d3a46678fcb630e15478d5
branch = workstream/m5-d26-contract-candidate
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d26-contract-candidate
candidate_path = docs/workstreams/m5_runtime_contract/CHANGED_STATE_ABSENCE_AMENDMENT.md
read_only_schema_evidence_branch = workstream/m5-d25-schema-017
read_only_schema_evidence_commit = fc6129d3ea374c3d280e5b1744d26aa135865fd5
```

The branch starts from the current pushed main commit. The implementation
branch above supplied read-only contradiction and feasibility evidence only;
none of its migration/source/test bytes are copied or accepted here.

## 2. Exact candidate byte pin

```text
candidate_sha256 = 5ad87a8b43b869911d7092038b9eda1bbda6c42b33b934c8b125ff1c2f88753c
candidate_lines = 384
candidate_bytes = 18032
candidate_owned_paths = 2
```

Both reviews must hash the candidate independently and stop if any value
differs. The candidate commit and tree are recorded after the two owned paths
are committed; recording them outside the committed candidate avoids a
self-referential commit identity.

Pre-commit gates on these bytes:

- `git diff --check`: pass;
- complete runtime contract/digest tests: 112/112 passed;
- public-M4/legacy compatibility selection: 14/14 passed;
- protected main `pyproject.toml`, two professor PDFs, renderer, and retained
  D25 draft hashes: unchanged from the lane-entry ledger;
- held schema-evidence branch: clean at exact commit `fc6129d3`; and
- database, network, migration, source, test, provider, deployment, and
  runtime-mode operations: none.

## 3. Candidate purpose

The candidate resolves only this joint contradiction:

- D25 represents structural state/binding removal as `after=None`;
- complete full-state/certificate-only changes must enter the changed-state
  set;
- migration 015 accepts only a reference matching a newly published row or
  binding at the event coordinates; and
- replacement/retirement intentionally publishes no successor row for the
  retired immutable predecessor object ID.

The proposed correction adds one typed absence-artifact domain for the
existing `requirement_state`, `group_state`, and `group_certificate` kinds
under exact group `REPLACE`/`RETIRE`. It retains the existing outer reference,
six-kind enum, present recipes, schema columns, and immutable certificate
artifacts.

## 4. Exact candidate ownership

This candidate lane owns only:

1. `docs/workstreams/m5_runtime_contract/CHANGED_STATE_ABSENCE_AMENDMENT.md`
   (new); and
2. `docs/workstreams/m5_runtime_implementation/D26_CONTRACT_REVIEW_HANDOFF.md`
   (this file).

It must not edit `AGENTS.md`, an existing authority/status document, D25,
migration 015/016/017, source, tests, the read-only D25 implementation branch,
the protected main worktree, a database, provider, deployment, or runtime
mode.

## 5. Review and stop protocol

1. Freeze and commit the two exact candidate paths.
2. Record the candidate file SHA-256, line count, byte count, commit, tree, and
   exact two-path diff without editing the committed bytes.
3. Give both independent reviewers that identical commit and candidate
   SHA-256. One reviews semantic/digest/lifecycle/replay correctness; the
   other reviews PostgreSQL validator/migration/interval/binding correctness.
4. Each reviewer classifies findings as P0, P1, or P2 and returns `GO` only
   with `P0=0` and `P1=0`.
5. Any P0/P1, reviewer disagreement, or candidate byte edit returns this lane
   to `HOLD`; after remediation both reviews restart on the new identical
   bytes.
6. Only two same-byte GOs permit a separate authority-freeze commit. No
   authority/status/implementation-plan file is edited before that gate.

## 6. Semantic/digest reviewer prompt

```text
Audit GroundLoop M5-D26 read-only at the exact commit and candidate SHA given
by the coordinator. Read AGENTS.md, M5 design freeze, runtime addendum, D22 and
D25 decisions, acceptance matrix, D25 amendment, migration-017 activation,
and the held D25 schema handoff. Read the entire D26 candidate. Verify the
absence digest uses exactly stable_m5_digest(
"m5-changed-state-absence-artifact-v1", ENUM(kind), TEXT(object_id)); permits
only requirement_state, group_state, and group_certificate; preserves the six
kinds, outer reference/set recipes, every present recipe, and certificate
digest rule; and requires a bijection with an exact canonical D25
before-present/after-None logical change. Falsify lifecycle ownership,
predecessor hash/interval, REPLACE/RETIRE mapping, successor absence, exact
seal coordinates, set completeness, replay, activation/failure exclusion, and
all nonclaims. Return GO only with P0=0 and P1=0. Report the exact commit,
candidate SHA, files read, checks, and any P2. Do not edit Git/files/database
or authorize implementation.
```

## 7. PostgreSQL reviewer prompt

```text
Audit GroundLoop M5-D26 read-only at the exact commit and candidate SHA given
by the coordinator. Read AGENTS.md and the complete M5/D22/D25 authority
chain, then inspect migration 014 lifecycle/published-state/binding rules,
migration 015 groundloop_m5_validate_event_result_children() and its three
constraint triggers, accepted D25 migration rules, the migration-017
activation, and the held schema-017 handoff/bytes as non-authoritative
evidence. Verify the candidate gives migration 017 authority to CREATE OR
REPLACE only groundloop_m5_validate_event_result_children(), leaves migration
015 bytes/ledger/triggers and all present branches unchanged, and defines an
enforceable database proof of the canonical D25 after=None change, exact
event/update/deactivation identity, present predecessor, interval/binding
closure, same-object successor absence, and exact sealed head/revision. Reject
tombstone tables, nullable hashes, JSON/repr hashing, a seventh kind, or an
application-only bypass. Return GO only with P0=0 and P1=0. Report exact
commit/SHA/files/checks and P2. Do not edit files/Git/database or authorize
migration 017.
```

## 8. Nonclaim boundary

Until the review gate and later authority freeze complete, M5-D26 is not an
accepted decision. D25 and M5.0-25 remain implementation-`PENDING`; M5.4-05
through M5.6 remain `PENDING`; migration 017 cannot integrate; runtime stays
`v1_only`; and no deployment or AI-quality result follows.
