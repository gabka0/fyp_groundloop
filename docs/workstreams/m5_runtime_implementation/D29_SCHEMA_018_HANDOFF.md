# M5-D29 Schema-018 Lane S Handoff

Status: **LANE-S CANDIDATE — exact four-path commit, external postcommit
confirmation, and pushed-barrier evidence remain coordinator work; Task 2 and
M5.4 remain `PENDING`**

Date: 2026-09-22

## 1. Scope and claim boundary

This lane implements only the migration-018 schema/installer boundary allocated
by the accepted D29 Task-2 activation:

1. the two frozen ordinary transactional locator indexes, in their exact order;
2. the pinned migration/bundle identity bound to the exact accepted
   migration-017 five-field ledger row;
3. the ledger-first, top-level read-write `READ COMMITTED` installer;
4. the package-private, exact-current-schema migration-018 route-authority
   reader; and
5. the isolated static and live PostgreSQL falsifiers for that surface.

The lane does not expose a D29 document route, implement withdrawal planning,
open or resume an event, compose D24--D28 persistence, change runtime mode, or
claim Task-2/M5.4 completion. There is no production caller of the private
route-authority reader in this lane. Runtime remains `v1_only` outside isolated
fixtures.

Deployment, latency, performance, end-to-end utility, objective truth,
security, novelty, maintained-history, named-system superiority, and AI/model
quality claims remain unsupported and `PENDING`.

## 2. Authority, ancestry, and ownership

```text
branch = workstream/m5-d29-schema-018
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d29-schema-018
base_commit = 8ed44a6492ffff3582b7cdaa844000d8c2124bd1
base_tree = 893ee21f75781be8985d023c566e0a128566f402
base_parent = 7ae653e78b57fb9960020b64604b2b30e197f4ce
D29_amendment_sha256 = e05f159f98d5f282335a90d2e9db1a26f8d85560030314060d918df59ccc82fb
D29_activation_sha256 = 109426f10bd0f21febd64710b529f7298fc6a7b2c68964bd21a7757011aed73c
```

The exact Lane-S manifest is:

1. `migrations/018_m5_bounded_document_withdrawal.sql` (new);
2. `src/groundloop/postgres/migrations.py`;
3. `tests/m5/postgres_runtime/test_migration_018.py` (new); and
4. this handoff (new).

No other tracked path differs from the pushed activation base. Migrations 001,
003, and 013--017 are unchanged. No historical branch or protected dirty
worktree supplied implementation bytes.

The containing handoff cannot embed its own final SHA-256, Git blob, commit, or
tree without self-reference. The coordinator and the two exact-commit
reviewers must record those identities externally after the four bytes are
committed.

## 3. Implemented schema and installer surface

Migration 018 contains exactly:

```sql
CREATE INDEX groundloop_m5_admitted_pair_by_chunk_edge
    ON groundloop_m5_requirement_admitted_pair (
        chunk_version_id COLLATE "C"
    );

CREATE INDEX groundloop_m4_job_by_epoch
    ON groundloop_semantic_job (epoch_id);
```

It creates no table, column, constraint, function, trigger, type, privilege,
backfill, unique index, included column, predicate, expression, concurrent
index, or third locator.

The installer:

- rejects ambient/nested transactions, read-only connections, and isolation
  levels other than `READ COMMITTED`;
- performs the migration-018 ledger decision before target-table locking;
- treats an exact initial ledger row as a lock-free no-op and a differing row
  as an immutable-content conflict;
- pins both accepted migration-018 hash literals on first installation;
- validates all five accepted migration-017 ledger fields and the required
  UTF-8/deterministic `pg_catalog."C"` environment;
- executes one exact canonical statement:

  ```sql
  LOCK TABLE groundloop_m5_requirement_admitted_pair,
             groundloop_semantic_job
      IN SHARE ROW EXCLUSIVE MODE NOWAIT;
  ```

- rereads the migration-018 ledger after both locks and before DDL;
- executes the two DDL statements separately in frozen order;
- inserts the migration-018 ledger row last in the same transaction; and
- performs no internal retry, commit, rollback, savepoint, or nested
  transaction.

The package-private route reader captures `pg_catalog.current_schema()` once,
requires a permanent ordinary `groundloop_m5_schema_bundle` table in exactly
that namespace through `pg_catalog.pg_class`/`pg_namespace`, and performs the
five-field read through a safely quoted schema-qualified identifier. A valid
ledger in a later `search_path` schema cannot authorize an unmigrated current
schema. The reader performs no transaction control or explicit lock.

## 4. Frozen identity and candidate byte ledger

```text
bundle_id = m5-bounded-document-withdrawal-schema-bundle-v1
migration_sha256 = 941bba975c12e9fb5ba4b4f75a82e59fa518b23eac34468ed2f8b15d1cd9ed90
bundle_sha256 = 9c45e58fb5c61156d4d07aa0c9b767112bf285452664f39731d893445e7a9e4f
oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
prerequisite_sha256 = 52240e19968926d0c051fe6146b3c7d877cf582014341efbfcc78637f3ff5761
```

The prerequisite is the exact accepted migration-017 row:

```text
bundle_id = m5-persisted-matching-schema-bundle-v1
bundle_sha256 = 52240e19968926d0c051fe6146b3c7d877cf582014341efbfcc78637f3ff5761
migration_sha256 = e387b01fa80145273ba40d2d83581bc54762d3a4a2edd34669c095076c52154c
oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
prerequisite_sha256 = 28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565
```

| Path | SHA-256 | Git blob | Lines | Bytes |
|---|---|---|---:|---:|
| `migrations/018_m5_bounded_document_withdrawal.sql` | `941bba975c12e9fb5ba4b4f75a82e59fa518b23eac34468ed2f8b15d1cd9ed90` | `fab74680a415afd135e86df5b95ccf2351915130` | 7 | 232 |
| `src/groundloop/postgres/migrations.py` | `1eb0cd5d2af0738d8d70f3b4a57bddd2fa6a86b288d3ef255e222aaa8d955140` | `daff250dc7cac7e68b99d398d898b09359546333` | 2,967 | 124,858 |
| `tests/m5/postgres_runtime/test_migration_018.py` | `2c7edf5eb5af8332ef120c3c53a17904bc4a11a0be1739e3632112a49788739b` | `c2351fc697a8caa779a8f75473c30ae634c83d61` | 2,554 | 99,490 |

The identity was independently recomputed from the exact SQL bytes with the
accepted `stable_m5_digest` recipe; both pinned literals match.

Frozen prerequisite migration hashes remain:

```text
migration 001 = ddbfdc1cedec89d9ffd852b66ece4192763099ee34429d5fbb85e54a3a196e37
migration 003 = fcbe9eaa2ef281cfdc03ddaf25fec3b5dbef73ce6ee957a76b7a3e68b729862c
migration 013 = ffe1a403039b78006aef6d8da005f77d9d8ca103f52822d81e9242b669a328fb
migration 014 = 4c37626f24524316c7990b2f3d213573bbe0f805a2c0f884585bf6aa325f0330
migration 015 = 85cb7f8e6a33273ce67fc6b4160e74a3aff314cd084df7ac3647cadae930185c
migration 016 = a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7
migration 017 = e387b01fa80145273ba40d2d83581bc54762d3a4a2edd34669c095076c52154c
```

## 5. Executable falsifier coverage

The final migration-018 module contains 34 selected cases and covers:

- exact two-statement bytes, order, object names, and forbidden SQL;
- literal migration/bundle identity and all five accepted-017 fields;
- byte-identical migrations 001, 003, and 013--017;
- fresh installation, exact lock-free rerun, arbitrary-byte first-install
  rejection, and same-ID differing-ledger conflict;
- each one-field accepted-017 mismatch before target locks or DDL;
- ambient/savepoint, read-only, wrong-isolation, invalid encoding, and invalid
  C-order rejection;
- the one exact two-table `SHARE ROW EXCLUSIVE NOWAIT` statement and live
  `pg_locks` proof of exactly the two granted target relation locks;
- all eight injected installer cuts:
  `after_initial_ledger`, `after_prerequisite`, `after_install_locks`,
  `before_first_index`, `after_admitted_pair_index`, `after_m4_job_index`,
  `before_ledger`, and `after_ledger`;
- actual execution-time failure of each `CREATE INDEX`, complete transaction
  rollback, and retry only in a fresh transaction;
- first-target and second-target `ACCESS EXCLUSIVE` conflicts, release of an
  acquired lock prefix, and fresh-transaction retry;
- two same-byte installers, commit-between-ledger-reads, and a conflicting
  concurrent installer observed by the post-lock reread;
- a missing pre-M5 ledger table and an exact accepted ledger only in a later
  fallback schema, both normalized to the D29 domain conflict;
- exact catalog shape for both new indexes, including one key, no `INCLUDE`,
  predicate, or expression;
- populated, non-sequential plans through both new locators, the existing
  current-currency chunk locator, dependency epoch prefix, M5 job epoch/state
  prefix, M4/M5 scope points, and requirement-root-provenance point;
- complete admitted-pair row point reads following changed-chunk locators;
- full M4 job rows and full M5 job rows, exact-epoch enumeration, explicit C
  ordering, and M5 rows spanning all seven frozen states;
- low-compressibility, long-but-schema-valid requirement/subject, claim/target,
  chunk, M4 job, scope-root, and registry identifiers during index creation and
  on post-install inserts; and
- exact migration-017 replay after migration 018.

PostgreSQL has three valid unique M5 discovery-scope indexes with the same
leading `root_job_id` operator class/collation. For an exact primary-key
coordinate, PostgreSQL 16.14 deterministically selected the equivalent valid
`UNIQUE(root_job_id, scope_contract_digest)` point path on the populated
fixture. The test therefore proves a non-sequential unique point plan and
independently proves that the required one-column primary key is primary,
unique, valid, ready, and has exactly `root_job_id` as its sole key. It does
not alter schema, statistics, planner internals, or index order to manufacture
a name. Both final independent reviewers accepted this as the truthful bounded
point-read evidence; the fetched full row, not index order, is authority.

This lane does not claim the later planner/store semantic validation of every
scope, provenance, declaration, withdrawal, or cancellation field. Those are
owned by the later path-exclusive runtime lanes.

## 6. Final static and live evidence

Static gates on the final technical hashes:

```text
git diff --check: PASS
ruff check: PASS
ruff format --check: PASS (2 files already formatted)
mypy --strict src/groundloop/postgres/migrations.py: PASS
python -m compileall -q src tests: PASS
exact migration/bundle identity recomputation: PASS
```

The final serial live matrix used the identical in-runner hashes listed in
Section 4 and produced:

| Suite | Selected | Passed | Failed | Skipped/xfail/deselected | Duration |
|---|---:|---:|---:|---:|---:|
| `test_migration_018.py` | 34 | 34 | 0 | 0 | 39.96 s |
| `test_migration_015.py` | 26 | 26 | 0 | 0 | 31.17 s |
| `test_migration_016.py` | 200 | 200 | 0 | 0 | 272.58 s |
| `test_migration_017.py` | 248 | 248 | 0 | 0 | 326.08 s |
| `tests/m5/postgres` | 57 | 57 | 0 | 0 | 19.20 s |
| **Total** | **565** | **565** | **0** | **0** | **688.99 s** |

The focused test implementer separately obtained 34/34 in 39.82 seconds on
the same hashes. That duplicate run is diagnostic corroboration and is not
pooled into the 565-case final matrix.

The qualified command shape was:

```text
docker exec \
  -e GROUNDLOOP_TEST_DATABASE_URL=postgresql://groundloop:groundloop@db:5432/groundloop \
  -e PGOPTIONS=-c jit=off \
  -e PYTHONDONTWRITEBYTECODE=1 \
  -e PYTHONPATH=src:. \
  -w /tmp/d29repo d29-pytest-runner \
  python -m pytest -ra -p no:cacheprovider --import-mode=importlib <suite>
```

One attempted host-published-port run was explicitly disqualified: its first
three non-database cases passed and the database cases then received a Docker
port-path `OperationalError` while the server remained healthy. No result from
that run is acceptance evidence. The accepted matrix used the isolated Docker
network and the `db` service name as required by the activation.

## 7. Qualified environment and clean inventories

```text
Docker project/network = m5-d29-schema-018 / m5-d29-schema-018_default
database container = m5-d29-schema-018-db-1 (3c9b47aa7f0d)
database image = pgvector/pgvector:pg16
image digest = sha256:1d533553fefe4f12e5d80c7b80622ba0c382abb5758856f52983d8789179f0fb
PostgreSQL = 16.14 (Debian 16.14-1.pgdg12+1)
runner = d29-pytest-runner
Python = 3.12.14
pytest = 9.1.1
psycopg = 3.3.4
server encoding = UTF8
database collation / ctype = en_US.utf8 / en_US.utf8
server-default jit = on
qualified test-session jit = off
extensions = btree_gist 1.7, pgcrypto 1.3, plpgsql 1.0, vector 0.8.5
```

The exact pre/post inventory was equal:

```text
connectable databases = groundloop, postgres, template1
non-temporary schemas = information_schema, pg_catalog, pg_toast, public
D25--D29/M5 test schemas = 0
other groundloop-database clients = 0
D25--D29/M5 test roles = 0
roles = groundloop,
        pg_checkpoint, pg_create_subscription, pg_database_owner,
        pg_execute_server_program, pg_monitor, pg_read_all_data,
        pg_read_all_settings, pg_read_all_stats, pg_read_server_files,
        pg_signal_backend, pg_stat_scan_tables, pg_use_reserved_connections,
        pg_write_all_data, pg_write_server_files
```

## 8. Audit history and final technical-byte verdicts

The first read-only review rejected the earlier bytes with `P0=0, P1=3,
P2=1`: populated plans were missing, actual DDL execution failures and the
first-target conflict were absent, and pre-M5 route failure leaked a raw
`UndefinedTable`. Those findings were fixed.

A second review rejected the next bytes because the route reader could resolve
a fallback-schema ledger, the current-currency test targeted the wrong
relation, the M5 hydration query was partial/single-state, and the width matrix
did not cover all required identifier families. Those findings were fixed.

Two independent read-only final reviews then inspected the identical three
technical hashes in Section 4:

```text
authority/contract review = GO, P0=0, P1=0, P2=0
PostgreSQL/locking review = GO, P0=0, P1=0, P2=0
```

Both confirmed the current-schema route boundary and accepted the honest
optimizer-tie evidence described in Section 5. These are technical-byte
reviews. The required four-byte precommit review, including this handoff, and
the exact-commit postcommit confirmations remain external coordinator gates;
any byte change restarts them.

## 9. Protected-worktree preservation

The protected dirty local main remains exactly:

```text
head = 14598ae51562006eaf67850b19e8212f38997903
tree = 36af2be4c58572c5adde68279d1a3aacedd0beff
status = one unstaged modified pyproject.toml plus four untracked files
pyproject.toml = 2af4b19962dc8a7d22e377be17f342530a06ee6395bbbf2a092eab36599c8fc2
groundloop_fyp_professor_feedback.pdf = 45c20ca46e9ad5bcd86b22c0d8882d1d611497f57ca8c45d3dea149260c110cd
groundloop_fyp_professor_feedback_v2.pdf = 59a13cd8d4bbb017e712c0f39e70f2eba136557e945f64f1b1fc3891742a79f0
render_groundloop_fyp_professor_deck.py = c12929c349a5c0be9793159143b09da40ea2a0b27df37b92d61d9ed6483d8c2a
PERSISTED_MATCHING_AMENDMENT_DRAFT.md = 167d1e7df5a720041fe0ff51879d08357f0dfbe3a7781ffaa08c0d958a47aa94
```

The held D28 Lane-P worktree remains exactly:

```text
head = 28a1392ccba9592ef41b1e51969c094f1f4b922a
tree = d96723faeee2bd3cce2098df77fb6dbdf8f3f561
staged = 0
modified tracked = 4
untracked = 3
```

Its seven activation-ledger file hashes remain unchanged. It was not reset,
cleaned, stashed, reformatted, copied, committed, rebased, cherry-picked, or
used as implementation evidence.

## 10. Remaining boundary

After the exact four-path commit receives two postcommit confirmations and the
reviewed barrier is pushed, the next and only authorized continuation is Lane
A on a new worktree based on that pushed commit:

```text
branch = workstream/m5-d29-application-routing
owned paths = src/groundloop/m5/runtime/application.py
              tests/m5/runtime/test_d29_hydration_terminal_cutoff.py
              docs/workstreams/m5_runtime_implementation/
                D29_APPLICATION_ROUTING_HANDOFF.md
```

Lane P, C1, C2, M, C3, and D remain unopened and cannot develop ahead of their
pushed predecessors. D24--D29 implementation, Task 2, M5.4--M5.6, runtime
activation, deployment, and all performance/utility/AI-quality claims remain
`PENDING`.
