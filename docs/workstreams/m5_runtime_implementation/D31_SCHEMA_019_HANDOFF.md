# M5-D31 Schema-019 Implementation Handoff

Status: `S1 IMPLEMENTATION CANDIDATE / LIVE GATES PASS / NOT YET AUDITED OR
COMMITTED`; S0 and the P hash-pin amendment are accepted and pushed, the pinned
migration has now been installed only in isolated test schemas through the S1
installer, and the exact S1 bytes still require two independent identical-byte
audits, one commit, two postcommit identity checks and push before Lane R may
advance

Date: 2026-09-25

## 1. Exact activation, pin and custody

```text
original_activation_commit = 98cdf3ac856f693d7c852f5df5a3325924a2f6f5
hash_pin_commit = bdb733b561af0079d757a176c20e3a7b2657a074
hash_pin_tree = 286dd7a92833fa5d2948b8b34d8a6c09dd26b165
hash_pin_parent = c27baef9ae1971619e54c5bb8da34e5a32ae0f8d
activation_path = docs/workstreams/m5_runtime_implementation/D31_IMPLEMENTATION_ACTIVATION.md
activation_sha256 = 7e5614692169e57454760e7d68c34bdb0a27b9ac22d8f8cfe72da0e85f6c3808
activation_blob = b0e13db388bac1701d42b9123a0f987852733843

lane = S1 -- migration-019 installer and executable evidence
branch = workstream/m5-d31-schema-019
base = bdb733b561af0079d757a176c20e3a7b2657a074
runtime_mode = v1_only
```

The user-owned checkout and the held C1-R/C1 dirty worktrees remain outside
this lane. Their exact heads, indexes, raw porcelain hashes and file hashes are
the ledgers in the activation. S0, P and S1 performed no reset, checkout,
clean, stash, rebase, cherry-pick, patch transplant or manual copy from either
held worktree. Before S1 edits or database execution, this Lane-S worktree was
fast-forwarded from the exact S0 commit to the pushed P commit and the SQL blob
was reverified unchanged.

## 2. Exact S1 path boundary

Relative to the hash-pin commit, S1 has exactly four path responsibilities:

1. `migrations/019_m5_preterminal_seal_context.sql`, pinned and read-only;
2. `src/groundloop/postgres/migrations.py`, modified;
3. `tests/m5/postgres_runtime/test_migration_019.py`, new; and
4. `docs/workstreams/m5_runtime_implementation/D31_SCHEMA_019_HANDOFF.md`,
   this modified handoff.

Every other path is read-only. The migration file remains exact blob
`827430b976cf971c3194d8d0673710cf877ace08`; S1 did not edit an earlier
migration, runtime consumer, public store, C1-R/C1 path, packaging metadata or
runtime-mode configuration.

## 3. Accepted SQL and bundle identity

```text
accepted_path = migrations/019_m5_preterminal_seal_context.sql
accepted_git_blob = 827430b976cf971c3194d8d0673710cf877ace08
accepted_sha256 = f92c02a365ac43a26f3291718866436b19f69924eb7a8c2b77af0312f82b536e
accepted_bytes = 8459
accepted_lines = 236
accepted_bundle_sha256 = e12d4abd95a9b2ef49010a43d6b708824462a80dfed2548107e175920dabe481
accepted_bundle_status = PINNED_BY_P
```

The accepted bundle value was pinned by P from the reviewed S0 blob through
the typed recipe:

```text
stable_m5_digest(
  "m5-preterminal-seal-context-schema-bundle-v1",
  *TEXT("migrations/019_m5_preterminal_seal_context.sql"),
  *HASH(f92c02a365ac43a26f3291718866436b19f69924eb7a8c2b77af0312f82b536e),
  *HASH(9c45e58fb5c61156d4d07aa0c9b767112bf285452664f39731d893445e7a9e4f)
)
```

Neither value is inferred from mutable checkout bytes during first install.
Any SQL-byte change invalidates both values and restarts S0/P/S1 review.

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

## 5. Fail-closed behavior encoded in the accepted SQL

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

## 6. Installer semantics and exact candidate identities

The S1 installer adds the frozen public constants, identity/result DTOs,
typed identity function and `install_m5_preterminal_seal_context_bundle`. It:

1. accepts only an idle read-write `READ COMMITTED` connection and owns one
   top-level transaction;
2. captures `current_schema()` once and schema-qualifies every ledger read,
   lock, insert and catalog decision;
3. reads the migration bytes exactly once, computes caller-byte identity from
   those same bytes before the initial ledger decision, returns exact replay
   without the install lock and reports a mismatching row as a ledger conflict
   before content validation;
4. on an absent row only, checks the pinned migration/bundle hashes, UTF-8 and
   the exact three-statement surface, then requires the exact migration-018
   five-field prerequisite;
5. takes the sole schema-qualified `SHARE ROW EXCLUSIVE MODE NOWAIT` ledger
   lock, rereads the ledger, rejects any pre-existing accessor, and snapshots
   the accepted private-triplet checker and public seal authorizer;
6. sets the transaction-local path to the selected schema and `pg_catalog`,
   executes the accepted function/revoke/grant statements without splitting
   the PL/pgSQL body on semicolons, and proves exact catalog/owner/ACL bytes;
7. proves both trusted migration-017 routines are unchanged; and
8. inserts the exact ledger row last, so every injected failure rolls back the
   function, privileges and ledger together.

The exact pre-audit S1 candidate identities are:

```text
migration_sql_sha256 = f92c02a365ac43a26f3291718866436b19f69924eb7a8c2b77af0312f82b536e
migration_sql_blob = 827430b976cf971c3194d8d0673710cf877ace08
installer_sha256 = e7638a8f0e9764aebbd2393a836092641596aa2c67012e6cb0962c667bbeb079
installer_blob = f7ca1601ebf584cd2394376d50705b196a920fc9
installer_bytes = 151994
installer_lines = 3716
test_sha256 = e949752ef15a7a04c352ba1a118c9afd89d721c3962dbdba6cad650adc14daa5
test_blob = e8e4fe4880f825cd57cab8727a37269228ac61b0
test_bytes = 63282
test_lines = 1532
```

The handoff hash/blob are intentionally recorded only after its final evidence
update and before the identical-byte audits.

## 7. Executable evidence

The exact focused live invocation used a disposable Python test container on
the already-running isolated D29 PostgreSQL network with this worktree mounted
read-only:

```text
python -m pytest -p no:cacheprovider -q \
  tests/m5/postgres_runtime/test_migration_019.py
result = 12 passed
```

The retained compatibility gates on the same bytes were:

```text
migration_015_complete = 26 passed
migration_016_installer_ledger_catalog_subset = 22 passed
migration_017_installer_ledger_catalog_subset = 14 passed
migration_018_plus_migration_019_complete = 46 passed
```

An exploratory invocation of the entire 519-case migration-015--019 matrix
was deliberately interrupted after 55 percent with no failures because its
remaining exhaustive D25/D26 data-law cases were outside the changed installer
surface and dominated by intentional lock waits. It is not recorded as a
passing gate and is not used in any acceptance claim.

The 12 tests cover:

1. exact migration/bundle vectors, accepted migrations 001--018 bytes and
   retained prerequisite ledgers;
2. fresh install with a regression assertion for one migration-byte read,
   exact lock-free replay, conflicting replay, every migration-018 prerequisite
   field, unauthorized bytes, partial object and unsupported connection states;
3. the one exact function, return row, namespace, owner, language, volatility,
   strictness, security-definer, search path, leak/parallel flags and explicit
   owner/PUBLIC execute ACL;
4. all nine injected rollback cutpoints and both concurrent-installer orders;
5. `current_schema() IS NULL`, earlier-unmigrated/later-valid no-fallback and
   exact-selected-schema replay;
6. owner and fresh genuine `LOGIN` non-owner identical output, while raw
   context and the private triplet helper remain denied to the non-owner;
7. a caller-owned structurally identical triplet with absent, invented and
   exact actual OID GUCs; permanent wrong-persistence/wrong-namespace tables;
   wrong-kind, missing-relation and each of the three transition-table
   coexistence cases;
8. all NULL/coordinate/GUC/OID, backend/xid/session-role/policy/flag and
   zero/duplicate-row falsifiers; and
9. repeated-read determinism plus before/after proof of no GUC, temporary-row,
   journal, expected-set, validation, persistent-row or lock-state mutation,
   followed by behavioral proof that deferred constraint mode remains
   deferred.

On the same candidate, `git diff --check`, Ruff check, Ruff format check,
strict mypy, compileall, `pip wheel --no-deps` and wheel archive integrity all
pass. A disposable Compose start first failed only because host port 5432 was
already owned by the healthy D29 database; the empty stopped container,
network and volume created by that attempt were removed, and no project data
were deleted. Live evidence then used the existing isolated database network.
Root migration files remain outside the current wheel contents, a pre-existing
packaging limitation outside Lane-S authority; S1 therefore makes no
installed-wheel migration-path claim.

## 8. Review and integration barrier

Before commit, two independent reviewers must audit identical final bytes and
return `GO`, `P0=0`, `P1=0`. Review must cover:

1. exact four-path boundary, hash-pin ancestry and unchanged SQL blob;
2. ledger-first transaction, selected-schema binding, one lock, replay,
   prerequisite, content and concurrency ordering;
3. exact new catalog/owner/ACL verification and unchanged trusted functions;
4. all D31 accessor privilege, context, relation, identity, cardinality and
   nonmutation falsifiers;
5. exact file identities and reproducible live/static results; and
6. the explicit claim ceiling below.

After both reviews, S1 receives one exact commit and two postcommit identity
checks, then is atomically pushed to the lane branch and `origin/main`. Only
that exact pushed commit may become Lane R's disjoint fast-forward base.

## 9. Claim ceiling

```text
M5-D31 contract = PASS
M5-D31 SQL/hash pin = PASS
M5-D31 schema/installer executable gate = CANDIDATE_PASS_PENDING_AUDIT_COMMIT
M5-D31 runtime consumption = PENDING_LANE_R
M5-D31 implementation = PENDING
preterminal_context_accessor = TEST_INSTALLED_ONLY
C1-R prerequisite repair = PENDING
C1 structural composition = PENDING
Task 2 = PENDING
M5.4-05 through M5.4-09 = PENDING
M5.5 and M5.6 = PENDING
runtime_mode = v1_only
deployment/performance/scalability/utility/security/privacy/novelty/
objective-truth/maintained-history/named-system-superiority/AI-quality = PENDING
```

S1 establishes the isolated schema/accessor boundary only. It does not prove
runtime consumption, C1 composition, a public M5 route, model quality or the
FYP's end-to-end utility.
