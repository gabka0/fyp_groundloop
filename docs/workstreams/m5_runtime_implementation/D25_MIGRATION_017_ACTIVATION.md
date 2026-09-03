# M5-D25 Migration-017 Wave 1 Activation

Status: docs-only activation for two disjoint implementation lanes; no other
D25 path, runtime activation, database deployment, or milestone promotion is
authorized

Date: 2026-09-03

## 1. Exact authority and starting point

```text
integrated_and_pushed_base = 1064514c8722fd1569bb66fec3eecbf01968e893
integrated_tree = 2ebc0832f2f89d9802bba6ec7b128c0180ec8d1c
accepted_d25_candidate_commit = 002dcace2f89e71ef3a56955647b4d8077e9c91f
accepted_d25_candidate_sha256 = bac12ab5e74632c04f1bd70d0ef0d00522ba9d268eb8b73d11845bbf3b873aae
accepted_d25_candidate_lines = 2404
accepted_d25_candidate_bytes = 115141
runtime_addendum_revision = 6
runtime_mode = v1_only
```

`origin/main` and local main both named the integrated base when this activation
opened. M5-D25 and M5.0-25 are contract-`PASS` / implementation-`PENDING`.
M5.0-24 remains implementation-`PENDING`; M5.4-05 through M5.4-09 and every
M5.5/M5.6 gate remain `PENDING`.

The implementation authority order for this wave is:

1. `AGENTS.md`;
2. `docs/m5_design_freeze.md`;
3. `docs/workstreams/m5_runtime_contract/CANDIDATE_RUNTIME_ADDENDUM.md`;
4. `docs/workstreams/m5_runtime_contract/RECOVERY_WORK_AMENDMENT.md` and its
   accepted C1--C7 corrections;
5. `docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT.md`;
6. `docs/m5_acceptance_matrix.md`;
7. this activation.

The D25 amendment is authoritative wherever an older document left persisted
matching or migration 017 unspecified. No lane may reinterpret its exact enum,
field, sequence, relation, lock, transition, work, audit, or digest order.

## 2. Accepted migration-016 prerequisite

Both lanes must fail closed unless this exact tuple is reproduced from current
repository bytes:

```text
accepted_016_bundle_id = "m5-runtime-recovery-schema-bundle-v1"
accepted_016_migration_sha256 = a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7
accepted_016_bundle_sha256 = 28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565
accepted_016_oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
accepted_016_prerequisite_sha256 = b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd
```

A mismatch is a stop, not an invitation to update the D25 literal.

## 3. Wave 1 ownership manifest

### Lane A — pure contracts and digests

```text
branch = workstream/m5-d25-contracts-digests
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d25-contracts-digests
```

Lane A owns only:

1. `src/groundloop/m5/runtime/contracts.py`;
2. `src/groundloop/m5/runtime/digests.py`;
3. `tests/m5/runtime/test_contracts.py`;
4. `tests/m5/runtime/test_digests.py`; and
5. `docs/workstreams/m5_runtime_implementation/D25_CONTRACTS_DIGESTS_HANDOFF.md`
   (new).

It implements the immutable D25 DTO/enum/validation and pure digest surface
needed by later persistence work. It must cover exact current/working point
unions and tombstones; image point; transition intent; physical/logical patch,
contribution and 37-counter work identities; output-kind/order recipes;
receipts; audit/mismatch/provenance DTOs; optional malformed-current branch;
and every exact domain/field/`OPTION`/`SEQ` rule assigned to these two modules.

Lane A must not import PostgreSQL, implement a store, edit migrations, change
an existing M4-v1 byte, or invent reflection/JSON/`repr` serialization. Existing
public DTO/API snapshots and all legacy digest vectors must remain unchanged.

### Lane B — migration 017 schema and installer

```text
branch = workstream/m5-d25-schema-017
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d25-schema-017
```

Lane B owns only:

1. `migrations/017_m5_persisted_matching.sql` (new);
2. `src/groundloop/postgres/migrations.py`;
3. `tests/m5/postgres_runtime/test_migration_017.py` (new); and
4. `docs/workstreams/m5_runtime_implementation/D25_SCHEMA_017_HANDOFF.md`
   (new).

It implements only the D25 relation/index/constraint/trigger/function schema
and the transactional migration-017 installer/bundle identity. The exact
41-relation `ACCESS EXCLUSIVE MODE NOWAIT` order, singleton order, top-level
read-write `READ COMMITTED` transaction, partial-prefix rollback, retry only in
a fresh transaction, post-lock ledger reread, exact migration-016 prerequisite,
fresh/upgrade/activated-without-history paths, rerun/conflict behavior, raw-DML
guards, and ledger-last commit rule are mandatory.

Lane B must not implement the application/store adapter, edit runtime
contracts/digests, activate runtime mode, run a model/provider, deploy a
database, or weaken existing migration-014/015/016 behavior. Live tests may use
only the repository's configured disposable local test database protocol and
must prove exact pre/post inventory restoration.

## 4. Shared barriers and non-ownership

Both lanes may read any repository path but may edit only their manifested
paths. They must not edit each other, main, this activation, the accepted D25
amendment, frozen authority/status documents, the protected `_DRAFT.md`, user
`pyproject.toml`, presentations, provider configuration, or deployment state.

This wave explicitly does not own:

- `src/groundloop/m5/runtime/persistence.py` or any PostgreSQL D25 store;
- completion, structural-root, direct-M4, failure, seal, or publication
  composition;
- provider/discovery/verifier/measurement adapters;
- physical-audit orchestration or evaluation reports beyond pure DTO/digests;
- runtime-mode activation or production data; or
- M5.0-24, M5.4-05..09, M5.5, M5.6, README, architecture, or status promotion.

If an owned implementation cannot be correct without another path, the lane
must stop and write a proposal in its own handoff. It may not cross the barrier.

## 5. Lane A implementation prompt

```text
Implement GroundLoop M5-D25 Wave 1 Lane A from the exact activation commit.
Read AGENTS.md and the full authority order in D25_MIGRATION_017_ACTIVATION.md,
then read the entire accepted PERSISTED_MATCHING_AMENDMENT.md. Edit only the
five Lane A paths. Add immutable, byte-total contracts and pure digest helpers
for every D25 surface assigned by the activation; preserve all existing names,
M4-v1 bytes, and public snapshots. Use explicit enums, dataclasses, validation,
and existing typed digest primitives—never ambient JSON, repr, reflection, or
implicit field order. Test golden vectors, one-field mutation, invalid mixed
option/error shapes, empty sequences, tombstone versus absence, the 37-counter
shape, logical-output order, malformed-current skipped provenance, and legacy
regression. Run focused pytest, Ruff, strict mypy for owned source, compileall,
diff-check, exact path check, and protected-hash check. Commit only owned paths
and write a handoff with exact commit/tree/file hashes, commands/counts, limits,
and remaining work. Do not use a database or network and do not claim D25
implementation PASS.
```

## 6. Lane B implementation prompt

```text
Implement GroundLoop M5-D25 Wave 1 Lane B from the exact activation commit.
Read AGENTS.md and the full authority order in D25_MIGRATION_017_ACTIVATION.md,
then read the entire accepted PERSISTED_MATCHING_AMENDMENT.md and current
migration installer. Edit only the four Lane B paths. Implement migration 017
and its installer exactly: all required relations, keys, checks, foreign keys,
indexes, deferred validators, scoped transition/seal/activation DML guards,
ledger identity, exact migration-016 prerequisite, 41-relation NOWAIT lock
order, READ COMMITTED top-level transaction, partial-prefix abort/fresh retry,
post-lock ledger reread, backfill/activation branches, deferred-immediate
validation, ledger-last commit, exact rerun and conflict. Do not implement a
runtime store or activate mode. Add isolated tests for SQL literals and live
fresh/upgrade/rerun/conflict/rollback/guard/concurrency paths, using only the
configured disposable local database and proving exact inventory restoration.
Run focused migration-017, migration-016/015 regression, Ruff, strict mypy,
compileall, diff-check, exact path, accepted-prerequisite, and protected-hash
gates. Commit only owned paths and write a handoff with exact commit/tree/file
hashes, commands/counts, inventory evidence, limits, and remaining work. Stop
with a proposal if any additional path is required; do not claim D25 or M5.4
implementation PASS.
```

## 7. Lane acceptance gates

Each candidate requires:

1. clean sole-parent ancestry from the exact committed activation;
2. exact manifested path scope and a clean worktree;
3. `git diff --check`, Ruff, strict mypy, compileall, and focused tests;
4. unchanged candidate/authority/protected hashes;
5. no skipped, xfailed, silently deselected, or pooled partial result unless
   explicitly preregistered and reported;
6. one independent same-byte audit returning zero unresolved P0/P1; and
7. no status, runtime mode, database deployment, or claim promotion.

Lane B additionally requires exact pre/post local database inventory equality
for every live gate. A missing local PostgreSQL prerequisite is a reported
environment blocker, never permission to fake live evidence.

## 8. Integration order and stop point

After both independent lane audits return `GO`, the coordinator creates one
integration branch from the exact activation commit and cherry-picks Lane A,
then Lane B. It verifies no overlap, reruns combined pure/static gates, then
runs the complete migration-017 plus applicable migration-016/015 PostgreSQL
regression under a serialized database preguard and exact inventory ledger.
Any failure restarts the affected gate from test one after a bounded fix.

Two independent same-byte audits are required on the combined integration
commit before main may fast-forward. Wave 1 integration still leaves D25 and
M5.0-25 implementation `PENDING`; persisted-store/runtime composition and the
remaining 40-falsifier matrix require later explicit waves. Runtime must remain
`v1_only` and no deployment is authorized.

## 9. Protected main-worktree hashes

```text
pyproject.toml = 2af4b19962dc8a7d22e377be17f342530a06ee6395bbbf2a092eab36599c8fc2
groundloop_fyp_professor_feedback.pdf = 45c20ca46e9ad5bcd86b22c0d8882d1d611497f57ca8c45d3dea149260c110cd
groundloop_fyp_professor_feedback_v2.pdf = 59a13cd8d4bbb017e712c0f39e70f2eba136557e945f64f1b1fc3891742a79f0
render_groundloop_fyp_professor_deck.py = c12929c349a5c0be9793159143b09da40ea2a0b27df37b92d61d9ed6483d8c2a
PERSISTED_MATCHING_AMENDMENT_DRAFT.md = 167d1e7df5a720041fe0ff51879d08357f0dfbe3a7781ffaa08c0d958a47aa94
```
