# M5-D32 / H32 Migration-020 Exact-Byte Hash Freeze

Status: docs-only H32 candidate. Requires two independent identical-byte
GO/P0=0/P1=0 reviews, sole-path commit, two exact postcommit checks and atomic
branch/main push before the new production installer may first install 020.

Date: 2026-10-09

## 1. Authority and sole ownership

Authority is D32 (amendment SHA-256
0e9a4bbe8da34d8248ce138c4bc21cb8076ddb4a578ea6a36cd6a4e100811435),
runtime-addendum revision 13 and the accepted sequential activation (SHA-256
2e35e3d049d5793ddb0f5ad7af008ac20176ad18d8dca325b84e13e5f53e5740).
This document pins reviewed SQL bytes; it does not change either contract,
authorize a different object inventory, or accept runtime implementation.

```text
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d32-schema-020-hash-freeze
branch = workstream/m5-d32-schema-020-hash-freeze
required_parent = eccad6b00242f5f7fc584b76121564ffafe3a1c8
required_parent_tree = 336d0dc723d8ce5073e55fd443cd09ec72534926
required_origin_main = eccad6b00242f5f7fc584b76121564ffafe3a1c8
sole_owned_path = docs/workstreams/m5_runtime_implementation/D32_SCHEMA_020_HASH_FREEZE.md
sql_evidence_worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d32-schema-020
sql_evidence_head = eccad6b00242f5f7fc584b76121564ffafe3a1c8
```

Every implementation source, test and SQL path remains outside H32 ownership.
The evidence worktree's drafted source remains uncommitted and is not copied,
staged, transplanted or accepted by this docs-only tranche.

## 2. Exact identities

```text
migration_label = migrations/020_m5_semantic_readiness.sql
migration_sha256 = f6bed0be8de1d4a7d1a1ce3caec36f89f60944d37d2025d42533223c6a4cacb6
migration_line_count = 1288
migration_byte_count = 68224
bundle_id = m5-semantic-readiness-schema-bundle-v1
bundle_sha256 = c5afd2b094e3be1e1a29f380a7ae8da0494969357764423ef26d8abb2d0c51a7
oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
prerequisite_bundle_sha256 = e12d4abd95a9b2ef49010a43d6b708824462a80dfed2548107e175920dabe481
prerequisite_migration_019_sha256 = f92c02a365ac43a26f3291718866436b19f69924eb7a8c2b77af0312f82b536e
```

The canonical bundle recipe is unchanged additive-bundle framing:

```text
stable_m5_digest(
  "m5-semantic-readiness-schema-bundle-v1",
  TEXT("migrations/020_m5_semantic_readiness.sql"),
  HASH(migration_sha256), HASH(prerequisite_bundle_sha256)
)
```

After H32 acceptance, the installer must use these literal accepted identities.
Computing a descriptive identity from requested bytes does not grant those
bytes authority. Changed first-install bytes fail closed. Any SQL edit after
this pin requires a fresh independently reviewed H32 hash-freeze correction
before installation, even if it appears cosmetic or preserves behavior.

## 3. Reviewed surface and diagnostic boundary

The SQL's eleven statement barriers are constraint preflight; three separate
DROP/ADD CHECK pairs; the new pinned invoker predecessor function; its BEFORE
INSERT trigger; the work validator replacement; and the timing validator
replacement. Only D32's exact three kind-bearing CHECK extensions, one new
function/trigger and two enumerated migration-016 replacements are permitted.
There are no tables, columns, indexes, types, views, sequences, backfills or
ACL changes. Migrations 000--019 remain byte-identical.

The new guard proves the real locked predecessor and requires terminal
job/scope coordinates no later than it. Cancelled roots bind job/scope
completion and revision to the current event/epoch and genuine cancellation
contribution; completed roots bind them to genuine closure/barrier evidence.
It validates Python-compatible typed source/key framing and exact S+K work.
All old validator branches remain exact outside the enumerated additions.

Both independent final SQL reviews returned GO/P0=0/P1=0/P2=0 on the exact
migration SHA above. Both independently reproduced the exact bundle digest.
Audit B's first bundle calculation omitted migrations/ from the label; its
incorrect 8b94930f... identity was withdrawn and replaced by a corrected
receipt using the complete frozen label. There is no identity disagreement.
Earlier preliminary reviews of
52a80fd8496c96d01a6b2c7c1092d3557d3368e3ac1de211107b2c81b085b80a
and 1368eed26f922554e7a2d89f003a84902525f7daff1ab78ab0cd8a07ddcd142c
and 698567e6dc671b74c07979f0c1682fbf6a7b6b381e256231580a7aa6830a93cd
do not approve these bytes. Cancellation-proof findings, explicit NULL-reason
rejection, exact reverse plan membership and all-terminal-job cutoff tightening
were addressed before the final SQL freeze. SQL approval does not substitute
for the separate identical-byte H32 documentation audits.

On the final SQL bytes, the isolated raw-SQL schema and retained digest/DTO
run returned 182 passed in 30.41s (21 schema tests plus 161 pure tests; earlier
focused runs overlap). The schema tests include actual accepted root closure,
actual accepted cancellation and a mismatched cancelled-scope digest injected
through the real cancellation flow, with no disabled trigger. It also covers
cross-reason cancellation artifact rejection, real verifier-child NULL/future
coordinate faults, the exact complete catalog delta and retained old flags.
The old recovery
fixture's missing direct-v1 baseline was derived by the accepted independent
reference helper before installing 017, not bypassed. Read-only new-installer
preflight also passed; the production installer has not been executed.

These are schema/identity diagnostics only. Owner/non-owner private readiness,
the complete installer matrix, genuine held-reader pending histories and C1
production composition remain their later independent acceptance gates.

## 4. Custody and permitted advance

Immediately before H32 drafting all four protected/held indexes were empty;
HEADs, exact raw porcelain-v2 status digests and all thirteen held content hashes
matched activation Section 7. Primary and AI worktrees are not advanced or
edited. Held C1 and R-T remain untouched and uncommitted.

```text
primary_head = 14598ae51562006eaf67850b19e8212f38997903
primary_raw_status = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
ai_head = a1dab3e5ebfe3bb65c521acd84758297a589f11a
ai_raw_status = 279b7ebf78bc4b19c71be49031ecc1a3415d1033864869344af98f07e6a438a7
c1_head = 8510470176f0396472d2f125fb8bd010dc3a61c4
c1_raw_status = 736f23f62b8ae744763219ab592266318662636dec4bcbc8a7be93e56ed0d58d
rt_head = d7db09c146795d6e6bef562a0ca266234b397639
rt_raw_status = 8c5c47d44900bba354618d13f0a2afc554ca0483d8c8ec1d6a6fe118eb4f9822
```

After exact H32 postcommit checks and atomic push, S32 may fast-forward its
dirty worktree only after live HEAD/index/raw-status and complete owned-byte
custody checks, ancestor proof, and proof that the upstream sole H32 path is
disjoint from all eight S32 paths and all held C1/R-T paths. Record the resulting
H32 commit as S32's exact sole parent. Never reset, clean, stash, rebase,
cherry-pick or transplant WIP to bypass this lineage barrier.

## 5. Claim ceiling

H32 pins SQL/bundle authority only. S32 implementation, D32 implementation,
Task 17/C1/Task 2, M5.4--M5.6, public workflows, deployment, AI quality and
end-to-end utility remain PENDING. This pin adds no public route or provider
call and does not change runtime mode; v1_only remains the default outside
isolated fixtures. Acceptance of later lanes remains strictly sequential.
