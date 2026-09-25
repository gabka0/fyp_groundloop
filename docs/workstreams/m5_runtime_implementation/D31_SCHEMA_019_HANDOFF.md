# M5-D31 Schema-019 SQL-Byte Handoff

Status: `SQL_BYTES_CANDIDATE / NOT_INSTALLED`; this S0 tranche freezes a
candidate migration file for review only, creates no database object, edits no
installer or executable test, and cannot authorize a first installation until
the separate P hash-pin amendment is independently audited, committed,
integrated and pushed

Date: 2026-09-25

## 1. Exact activation and custody

```text
activation_commit = 98cdf3ac856f693d7c852f5df5a3325924a2f6f5
activation_tree = 11927e38513037179404b8b320a66f65e8bbb4b7
activation_parent = d5253e4a9b320a139205bd0ba7eaefde1c1f6141
activation_path = docs/workstreams/m5_runtime_implementation/D31_IMPLEMENTATION_ACTIVATION.md
activation_sha256 = 8c4d1eecb2b6a139fe3f36c7d8626b79257080a62493146fa892ae7aa94e93ed
activation_blob = 1606e6526ba59d77dce4ba12bf7b382e7d281855

lane = S0 -- migration-019 SQL-byte freeze
branch = workstream/m5-d31-schema-019
base = 98cdf3ac856f693d7c852f5df5a3325924a2f6f5
runtime_mode = v1_only
```

The user-owned checkout and the held C1-R/C1 dirty worktrees remain outside
this lane. Their exact heads, indexes, raw porcelain hashes and file hashes are
the ledgers in the activation. S0 performed no reset, checkout, clean, stash,
rebase, cherry-pick, patch transplant or manual copy from either held
worktree.

## 2. Exact S0 path boundary

Relative to the activation commit, S0 owns exactly:

1. `migrations/019_m5_preterminal_seal_context.sql` (new); and
2. `docs/workstreams/m5_runtime_implementation/D31_SCHEMA_019_HANDOFF.md`
   (this new file).

Every other path is read-only. In particular, S0 did not edit
`src/groundloop/postgres/migrations.py`, create
`tests/m5/postgres_runtime/test_migration_019.py`, execute SQL against a
database, invoke a migration installer, or run a fixture that installs the
candidate.

## 3. Candidate SQL identity

```text
candidate_path = migrations/019_m5_preterminal_seal_context.sql
candidate_git_blob = 827430b976cf971c3194d8d0673710cf877ace08
candidate_sha256 = f92c02a365ac43a26f3291718866436b19f69924eb7a8c2b77af0312f82b536e
candidate_bytes = 8459
candidate_lines = 236
candidate_bundle_sha256 = e12d4abd95a9b2ef49010a43d6b708824462a80dfed2548107e175920dabe481
candidate_bundle_status = DERIVED_NOT_ACCEPTED
```

The candidate bundle value above was derived without database access by the
accepted typed recipe:

```text
stable_m5_digest(
  "m5-preterminal-seal-context-schema-bundle-v1",
  *TEXT("migrations/019_m5_preterminal_seal_context.sql"),
  *HASH(f92c02a365ac43a26f3291718866436b19f69924eb7a8c2b77af0312f82b536e),
  *HASH(9c45e58fb5c61156d4d07aa0c9b767112bf285452664f39731d893445e7a9e4f)
)
```

Neither candidate hash is accepted authority in S0. The later P amendment must
pin the committed SQL identity after two identical-byte S0 audits and two
postcommit identity checks. Any SQL-byte change invalidates both values and
restarts S0 review.

## 4. Exact object and privilege surface

The SQL has exactly three top-level statements:

1. `CREATE FUNCTION` for
   `groundloop_m5_matching_read_preterminal_seal_context(bigint,bigint,bigint)`;
2. `REVOKE ALL ... FROM PUBLIC`; and
3. `GRANT EXECUTE ... TO PUBLIC`.

The sole created object is the function. It has exactly three named `bigint`
inputs and the frozen eight-column `RETURNS TABLE` order:

```text
policy_version text
anchor_m4_epoch_id bigint
anchor_m5_epoch_id bigint
anchor_m5_revision bigint
anchor_activation_count integer
anchor_predecessor_revision bigint
anchor_predecessor_sealed_at timestamptz
anchor_current_policy text
```

The declaration is `plpgsql`, `STABLE`, `CALLED ON NULL INPUT`, `SECURITY
DEFINER`, `PARALLEL UNSAFE`, `NOT LEAKPROOF`, and `SET search_path FROM
CURRENT`. There is no overload, default, variadic argument, companion object,
replacement, trigger, table, type, index, view, sequence, backfill or earlier
migration edit.

## 5. Fail-closed behavior encoded in the candidate

The body:

1. rejects SQL `NULL`, nonpositive and nonadjacent coordinates using an
   overflow-safe numeric difference;
2. captures the checked-transition, seal mode, epoch, revision, policy and
   three OID GUCs once, requires exact coordinates and a nonempty policy, and
   lets malformed numeric GUCs raise;
3. rejects any transition triplet;
4. resolves the three promotion relations only through `pg_temp`, requires
   three distinct ordinary temporary-table OIDs in `pg_my_temp_schema()`,
   calls the existing revoked trusted-owner triplet checker, and binds each
   canonical OID text to its transaction-local GUC;
5. reads the promotion context dynamically in one query image, with
   `INTO STRICT` plus a window count, so zero, duplicate or ambiguous rows
   raise;
6. requires exact backend PID, transaction ID, `session_user`, seal mode,
   epoch/revisions, policy and both validation flags false; and
7. returns the policy and seven anchors verbatim from that same unique row.

It reads no persistent GroundLoop application/envelope relation and does not
read promotion-journal or expected-set contents. It performs no DML, DDL,
explicit or advisory lock, GUC mutation, constraint-mode change, transaction
control, validation start or validator call. Its only catalog read proves the
three temporary relations, and its only data row read is the unique temporary
context row.

## 6. Static evidence and deliberate non-evidence

Recorded on the exact candidate bytes:

```text
git_diff_check = PASS
changed_paths = exactly the two S0 paths
utf8_lf_final_newline = PASS
pglast_top_level_parse = PASS (CreateFunctionStmt, GrantStmt, GrantStmt)
typed_bundle_vector = REPRODUCED
database_installation = NOT_RUN_BY_CONTRACT
live_owner_positive = NOT_RUN_BY_CONTRACT
live_non_owner_positive = NOT_RUN_BY_CONTRACT
catalog_acl_check = NOT_RUN_BY_CONTRACT
installer_replay_rollback_concurrency = NOT_RUN_BY_CONTRACT
```

S0 static evidence is not implementation acceptance. The future S1 installer
and executable test tranche remains responsible for exact catalog/ACL/owner
proof, owner and genuine non-owner positives, raw-access denial, NULL/spoof/
flag/nonmutation matrices, ledger-first replay/conflict, rollback cuts and
concurrency. Lane R remains responsible for captured-schema persistent
envelope validation and result-bound equality.

## 7. Review and integration barrier

Before S0 commit, two independent reviewers must audit the identical SQL and
handoff bytes and return `GO`, `P0=0`, `P1=0`. Review must cover:

1. the exact two-path boundary and activation ancestry;
2. the one-object/signature/return/security/search-path/ACL surface;
3. every M5-D31 fail-closed context condition, NULL-safe comparison and
   single-query-row provenance;
4. absence of persistent envelope reads, journal/expected contents and every
   forbidden mutation;
5. exact candidate SHA/blob/size/line and typed candidate bundle identity; and
6. the explicit `NOT_INSTALLED` claim ceiling.

After both reviews, S0 receives one exact commit and two postcommit identity
checks, then is pushed to the lane branch and `origin/main`. Only then may the
fresh docs-only P worktree pin those committed SQL bytes. P must integrate and
the clean Lane-S worktree must fast-forward to P before any installer/test edit
or any database execution.

## 8. Claim ceiling

```text
M5-D31 contract = PASS
M5-D31 implementation = PENDING
migration_019_sql_bytes = CANDIDATE_NOT_ACCEPTED
migration_019_hash_pin = PENDING_S0_AUDIT_AND_COMMIT
migration_019_installer = NOT_AUTHORIZED_BEFORE_PIN
preterminal_context_accessor = NOT_INSTALLED
C1-R prerequisite repair = PENDING
C1 structural composition = PENDING
Task 2 = PENDING
M5.4-05 through M5.4-09 = PENDING
M5.5 and M5.6 = PENDING
runtime_mode = v1_only
deployment/performance/scalability/utility/security/privacy/novelty/
objective-truth/maintained-history/named-system-superiority/AI-quality = PENDING
```

No implementation, deployment, security, utility, performance or model-quality
claim follows from authoring candidate SQL bytes.
