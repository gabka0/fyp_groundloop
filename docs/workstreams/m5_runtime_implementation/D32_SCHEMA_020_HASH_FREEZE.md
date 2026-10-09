# M5-D32 / H32 Migration-020 Exact-Byte Hash Freeze

Status: docs-only H32 correction candidate. Requires two independent
identical-byte GO/P0=0/P1=0 reviews, sole-path commit, two exact postcommit
checks and atomic branch/main push before the corrected production installer
may install 020. Initial H32 fd3895793fe772f4bca723119fd88897bb352c2c is
historical; its SQL pin is superseded by Section 6 only after this barrier.

Date: 2026-10-09

## 1. Authority and sole ownership

Authority is D32 (amendment SHA-256
0e9a4bbe8da34d8248ce138c4bc21cb8076ddb4a578ea6a36cd6a4e100811435),
runtime-addendum revision 13 and the accepted sequential activation (SHA-256
2e35e3d049d5793ddb0f5ad7af008ac20176ad18d8dca325b84e13e5f53e5740).
This document pins reviewed SQL bytes; it does not change either contract,
authorize a different object inventory, or accept runtime implementation.

```text
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d32-schema-020-hash-correction
branch = workstream/m5-d32-schema-020-hash-correction
required_parent = fd3895793fe772f4bca723119fd88897bb352c2c
required_parent_tree = b8e45b04c6701ad3a61f24fae5b79e01cac3dc3c
required_origin_main = fd3895793fe772f4bca723119fd88897bb352c2c
sole_owned_path = docs/workstreams/m5_runtime_implementation/D32_SCHEMA_020_HASH_FREEZE.md
sql_evidence_worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d32-schema-020
sql_evidence_head = fd3895793fe772f4bca723119fd88897bb352c2c
```

Every implementation source, test and SQL path remains outside H32 ownership.
The evidence worktree's drafted source remains uncommitted and is not copied,
staged, transplanted or accepted by this docs-only tranche.

## 2. Historical initial H32 identities

The following initial pin was accepted and pushed in fd3895793fe772f4bca723119fd88897bb352c2c.
It is retained without rewriting its review history. Section 6 supplies the
corrected active pin after the new commit/postcommit/push barrier.

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

After initial H32 acceptance, the installer used these literal identities;
the correction barrier supersedes them with Section 6's exact pin.
Computing a descriptive identity from requested bytes does not grant those
bytes authority. Changed first-install bytes fail closed. Any SQL edit after
this pin requires a fresh independently reviewed H32 hash-freeze correction
before installation, even if it appears cosmetic or preserves behavior.

## 3. Initial-pin reviewed surface and diagnostic boundary

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
preflight also passed; the production installer had not been executed at the
initial H32 freeze. Subsequent installer diagnostics are recorded in Section 6.

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

## 6. Narrow schema-bounded catalog-introspection correction

After the initial pin was pushed and S32 fast-forwarded with all seven existing
owned-file hashes unchanged, the new installer was exercised in owned isolated
schemas. A focused first-install/read-only-replay diagnostic passed. An earlier
combined 203-test run passed before final signature checks were added; neither
run accepts S32 or the corrected bytes.

Running the 44-case schema suite alongside retained migration-016--019 tests
then produced 40 passes and four PostgreSQL InternalError failures (72.18s).
The installer CHECK query's WHERE clause combined namespace/relation filters
with pg_get_constraintdef(oid). PostgreSQL could reorder those predicates and
introspect a constraint from an unrelated schema being dropped concurrently:
could not open relation with OID. The initial SQL preflight had the same query
shape. Preserve these failures; no skip, retry-as-proof or prior-pin pass may
erase them. The four errors occurred in Python catalog validation, not a
failure of an accepted readiness runtime (which remains unimplemented).

The sole SQL change is a MATERIALIZED selected_constraints CTE in the
constraint-preflight statement. It first selects the exact pinned installation
schema and target relation without pg_get_constraintdef, then runs the original
uniqueness, expression, validation, nondeferrability, permanence and ambiguity
checks on that bounded set. The installer catalog checker uses the same
execution barrier. No readiness predicate, event/update identity, work recipe,
guard/validator body, trigger flags, object inventory or privilege changes.
Same-target corruption still fails closed; other-schema catalog churn is not
authority. This fits the activation's explicit post-pin correction procedure.

```text
migration_label = migrations/020_m5_semantic_readiness.sql
migration_sha256 = df0a3c0c9b228a4a22903479896326d27fbd6f98f5878e34d73182ba007bf837
migration_line_count = 1292
migration_byte_count = 68419
bundle_id = m5-semantic-readiness-schema-bundle-v1
bundle_sha256 = b7706feb7d54fcf9fdb4f9f38350a32967e0b460229b6f264493fb428be8ddfc
oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
prerequisite_bundle_sha256 = e12d4abd95a9b2ef49010a43d6b708824462a80dfed2548107e175920dabe481
prerequisite_migration_019_sha256 = f92c02a365ac43a26f3291718866436b19f69924eb7a8c2b77af0312f82b536e
```

The Section-2 canonical recipe uses the same complete migration label and
prerequisite identity, now with this new SQL hash. Both independent corrected
SQL audits returned GO/P0=0/P1=0/P2=0 on these identical bytes and independently
reproduced this bundle digest. Both reversed only this query change and recovered
the exact prior f6bed0... SQL hash and 68,224 bytes, proving the rest unchanged.
The separate identical-byte correction-doc audits are still required; the
old GO receipts cannot approve this pin.

An initial corrected diagnostic run returned 28 passed / 17 deselected in
42.64s. Its report is
/tmp/groundloop-d32-sql-diagnostics.v9QrLA/schema-correction-raw.xml;
SHA-256 54d3f0d925906e908e030fe2f300ccf2e852fd4f640f860811f78390cc8a3a48.
Inspection of every selected node revealed a process error: pytest -k matched
parameter IDs as well as function names. Besides 22 intended schema-only
cases, it selected four parametrized replay fixtures that installed corrected
020 through the new production installer, plus two negative installer cases.
Those four isolated fixture installations occurred before this hash correction
was pushed, contrary to the required execution sequence. This is disclosed,
not retroactively authorized; the mixed run is not corrected-pin installer
acceptance evidence. No public/non-test schema, protected worktree or default
runtime mode was changed. A fresh post-barrier full installer run is mandatory.

The first correction-doc candidate incorrectly described that run as raw-only
and unexecuted-by-installer. Its provisional unpushed commit
ddc4d1fcef4685c50c6d379a9801d9cc624722c4 and associated GO/postcommit receipts
are superseded, not pushed or used as acceptance. Only this docs-owned path is
corrected; its replacement commit retains the same exact pushed fd389... sole
parent. New identical-byte audits and postcommit checks are required.

Before the replacement barrier, schema-only diagnostics use explicit function
node IDs, not -k. The raw schema-bounded foreign-constraint-churn regression
performs 40 preflight checks followed by raw install/catalog verification,
with intact triggers. That exact explicit-node run returned 22 passed,
zero failures/errors/skips in 34.36s; its report is
/tmp/groundloop-d32-sql-diagnostics.v9QrLA/schema-correction-raw-only.xml,
SHA-256 272c848b0e94560cf7524df1dcbd42f8e5cb31850cd3b03218e1742a3e6f7bbe.
Every retained node was inspected: no installer, replay or unledgered-install
case is selected. This is schema-only candidate evidence, not runtime evidence.
The pure, compatibility, installer, static and package
acceptance gates must run again on final S32 bytes after the new barrier.

H32 still owns exactly this one documentation path, which is disjoint from
all eight S32 and every held C1/R-T path. The correction's exact pushed commit
becomes S32's sole parent only after a fresh external owned-byte manifest,
HEAD/index/raw-status checks, ancestor/disjointness proof and merge --ff-only
without copying, resetting, cleaning, stashing or transplanting WIP.
Protected/held custody and the Section-5 PENDING ceiling remain unchanged.
