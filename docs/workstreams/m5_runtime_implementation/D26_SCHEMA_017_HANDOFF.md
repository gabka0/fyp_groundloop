# M5-D26 Schema-017 Lane B Handoff

Status: **LANE CANDIDATE — final commit and exact-commit independent audit
pending; Wave R6 integration has not started**

Date: 2026-09-16

## 1. Scope and claim boundary

This Lane B recovery tranche completes the migration-017 schema, installer,
database transition validator, promotion/seal enforcement, and the narrow
M5-D26 changed-state absence-reference branch. It does not implement or prove
the application/store path that emits those references or composes D24
recovery with the persisted-matching transaction.

This handoff therefore retains:

- M5-D25/M5.0-25 contract `PASS`, implementation `PENDING`;
- M5-D26/M5.0-26 contract `PASS`, implementation `PENDING`;
- M5.4-01 through M5.4-04 `PASS` and M5.4-05 through M5.4-09 `PENDING`;
- every M5.5 and M5.6 gate `PENDING`;
- runtime mode `v1_only` outside isolated fixtures; and
- deployment, production activation, AI/model quality, maintained-history
  utility, latency, security, novelty, and named-system superiority claims
  unsupported.

## 2. Authority, ancestry, and isolated ownership

```text
branch = workstream/m5-d26-schema-017
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d26-schema-017
activation_commit = 14598ae51562006eaf67850b19e8212f38997903
activation_tree = 36af2be4c58572c5adde68279d1a3aacedd0beff
activation_parent = b9bbf9b9124bf4297f3d7394069088906352dd25
post_replay_checkpoint = 0e95fffc67227f408c61929f8172c0c151e2e7ab
post_replay_tree = d363d9469232013fe64eeb935fc0654d1307bdf5
```

The activation commit has the required sole parent and changes only
`D26_MIGRATION_017_ACTIVATION.md`. The held D25 Lane B evidence remains
read-only at commit `fc6129d3ea374c3d280e5b1744d26aa135865fd5`, tree
`055e2a80b878f3779265a85a1efaa6bde709cce1`.

The three held commits were replayed without conflict and reproduced the held
blobs exactly:

```text
0193cf2193a5696c7c51e753d2bdcae603f942f8 -> ec04600
5a189b47980fba91b8aca6a6425c2069c48fb9a0 -> 8a4f4c3
fc6129d3ea374c3d280e5b1744d26aa135865fd5 -> 0e95fff
stable patch IDs = 30d67c82..., 5e2e4a15..., da9823f3...
```

The imported checkpoint ledger was:

```text
migrations/017_m5_persisted_matching.sql
  sha256 = 8ca8812d90522b67f0b1b59123e7c41648dfb22f70d80d8e9ffbefed40e00f2a
src/groundloop/postgres/migrations.py
  sha256 = 29d6ee2802228b26afb69199b596f1780b32209ba501f12cc2e697a7bcecf723
tests/m5/postgres_runtime/test_migration_017.py
  sha256 = 19b75fd04e2e0ac795b41d6f6f757f60cd8d2cd9b0c5411d05a869235fa7a200
docs/workstreams/m5_runtime_implementation/D25_SCHEMA_017_HANDOFF.md
  sha256 = 0cbe30a5106f676e7eac0a2cb192498ffc64e2fcf1c84d3d5c23b10bffacca20
```

The old D25 handoff remains byte-identical. New work is confined to the three
editable technical paths and this new handoff, within the exact five-path
Lane B manifest.

## 3. Implemented database surface

The candidate closes the four retained B3b blockers and the remaining B3
transaction work:

1. activation rejects every other structurally committed, semantically
   pending/complete live epoch regardless of evaluation outcome and binds the
   complete committed/sealed/complete/strict head tuple, non-null `sealed_at`,
   and exact revision;
2. activation requires the exact accepted final migration-017 migration and
   bundle identities;
3. activation locks the M5 publication-head and activation singleton tables
   strongly enough to protect absent keys, then proves their row absence;
4. seal binds complete predecessor/live-epoch/runtime/head/current-image
   coordinates and the working-image policy to the typed update policy;
5. every current physical family and the image header uses symmetric expected-
   set equality, including missing, extra, wrong-operation, wrong-coordinate,
   and wrong tombstone/upsert rejection;
6. deferred validation is forced before promotion completes, every captured
   relation is rooted, no DML is accepted after validation starts, and final
   seal/promotion/heads/result/reference writes are one transaction;
7. installer replay is ledger-first and exact-byte idempotent; valid fresh,
   preactivation-upgrade, and activated-without-history installs succeed,
   while conflicts and injected failures fail closed atomically; and
8. non-owner transition and promotion contexts require three ordinary
   temporary relations owned by the trusted `SECURITY DEFINER` owner. Caller-
   created same-name relations remain rejected even when the caller supplies
   their exact OIDs through every custom setting.

Migration 017 retains exactly the two D25-authorized migration-014
replacements:

```text
groundloop_m5_validate_working_state_mutation()
groundloop_m5_validate_revision_interval_mutation()
```

It adds only the D26-authorized migration-015 replacement:

```text
groundloop_m5_validate_event_result_children()
```

The migration-015 file is unchanged, and all three original constraint-trigger
identities remain installed, enabled, and pointed at that function. Replacement
preserves the function OID and every catalog attribute except its definition:
owner, ACL, language, return type, volatility, security-definer flag, parallel
classification, and configuration remain equal in the captured catalog image.

The installer rejects an ambient transaction, requires one top-level
read-write `READ COMMITTED` transaction, acquires `ACCESS EXCLUSIVE NOWAIT`
locks on the exact 41-relation ordered tuple, validates the accepted 016
prerequisite before those locks, and rereads the 017 ledger after them. It
commits only after deferred promotion validation and exact final verification;
the 017 ledger row is written last.

For `requirement_state`, `group_state`, and `group_certificate` only, the D26
branch derives:

```text
groundloop_m5_digest_text_fields(ARRAY[
  'm5-changed-state-absence-artifact-v1',
  'enum', kind,
  'text', object_id
])
```

It accepts absence only for exact structural `replace_group`/`REPLACE` or
`retire_group`/`RETIRE`, and independently proves event/update/deactivation
identity, structural payload, the present predecessor at the prior published
head, exact interval/binding closure at the event epoch, no same-object
successor, the canonical `before`-present/`after=None` logical-change
bijection, unchanged outer/set recipes, and exact seal/head/reference
coordinates. All six present-reference kinds retain their existing recipes and
positive/negative paths. There is no seventh kind, tombstone table, nullable
hash, JSON/`repr` hashing, or present-state recipe change.

The database-only half of D26 falsifier 18 is covered: exact replay preserves
the stored changed-state rows and reference bytes, their `xmin` values, and
all relevant row counts, with zero writes. The public store/model-call half is
not owned or claimed here.

Some seal-path fixtures directly seed zero-valued D24 work/timing rows solely
to make the migration-015/016 constraints reachable while testing 017. Those
rows are test scaffolding, not evidence of public D24 composition, execution
or work/timing accounting, M5.4 completion, or a production recovery path.

## 4. Candidate technical byte ledger

| Path | SHA-256 | Git blob | Lines | Bytes |
|---|---|---|---:|---:|
| `migrations/017_m5_persisted_matching.sql` | `e387b01fa80145273ba40d2d83581bc54762d3a4a2edd34669c095076c52154c` | `a78397b34e1aad29b766d5cf1cfac635b9136968` | 6,142 | 351,772 |
| `src/groundloop/postgres/migrations.py` | `37ccb37f4d8760599b6065e59595c1f42bfe799c21a54d0306e598c86cfec421` | `0941a890688478d6cb65fb169b902c918704f431` | 2,583 | 110,023 |
| `tests/m5/postgres_runtime/test_migration_017.py` | `2bc96e8472c25355319281f6f6867a6a528fb1768907bd2dd3a37f250a71ee4e` | `ecf35a3dca7f7af9c3ddd58bb4a8bf79101e0b34` | 11,008 | 423,618 |
| immutable D25 handoff | `0cbe30a5106f676e7eac0a2cb192498ffc64e2fcf1c84d3d5c23b10bffacca20` | `8d5ddf94b466280497203a91403e39259a680935` | 440 | 26,960 |

```text
bundle_id = m5-persisted-matching-schema-bundle-v1
migration_sha256 = e387b01fa80145273ba40d2d83581bc54762d3a4a2edd34669c095076c52154c
bundle_sha256 = 52240e19968926d0c051fe6146b3c7d877cf582014341efbfcc78637f3ff5761
oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
prerequisite_sha256 = 28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565
```

Protected authority/prerequisite bytes remain:

```text
D25 amendment = bac12ab5e74632c04f1bd70d0ef0d00522ba9d268eb8b73d11845bbf3b873aae
D26 amendment = 85372d4c2f9108810bd75c3e5611de541d0f31c8a096421f30e68fad84676721
migration 014 = 4c37626f24524316c7990b2f3d213573bbe0f805a2c0f884585bf6aa325f0330
migration-014 bundle = 9bce4ae68a434aefbc196b6367b77454f4ec65d531f4ff206f37fb4d69e9167a
migration-014 oracle = 00c533e1789a1415a5571ee5bfe9df4f36577089754ca8be00ad6c7172b11b7b
migration-014 prerequisite = 187f2b0ca5ac10fa9e1d745d061ab7adad11e9c45db76d5f0147198174332179
migration 015 = 85cb7f8e6a33273ce67fc6b4160e74a3aff314cd084df7ac3647cadae930185c
migration-015 bundle = b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd
migration-015 empty oracle = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
migration-015 prerequisite = 9bce4ae68a434aefbc196b6367b77454f4ec65d531f4ff206f37fb4d69e9167a
migration 016 = a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7
migration-016 bundle = 28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565
migration-016 empty oracle = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
migration-016 prerequisite = b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd
```

The protected dirty main-worktree bytes were rechecked after the database
runs and still equal the activation ledger:

```text
pyproject.toml = 2af4b19962dc8a7d22e377be17f342530a06ee6395bbbf2a092eab36599c8fc2
groundloop_fyp_professor_feedback.pdf = 45c20ca46e9ad5bcd86b22c0d8882d1d611497f57ca8c45d3dea149260c110cd
groundloop_fyp_professor_feedback_v2.pdf = 59a13cd8d4bbb017e712c0f39e70f2eba136557e945f64f1b1fc3891742a79f0
render_groundloop_fyp_professor_deck.py = c12929c349a5c0be9793159143b09da40ea2a0b27df37b92d61d9ed6483d8c2a
PERSISTED_MATCHING_AMENDMENT_DRAFT.md = 167d1e7df5a720041fe0ff51879d08357f0dfbe3a7781ffaa08c0d958a47aa94
```

The containing handoff cannot embed its own final blob, commit, or tree without
self-reference. Those identities must be recorded externally by the
coordinator and same-byte reviewer after commit.

## 5. Verification evidence

The exact snapshot `/tmp/d26-prov-2bc` was copied into isolated runner
`d26-pytest-runner`. It used database container
`m5-d26-schema-017-db-1`, image digest
`sha256:1d533553fefe4f12e5d80c7b80622ba0c382abb5758856f52983d8789179f0fb`,
PostgreSQL `16.14 (Debian 16.14-1.pgdg12+1)` on x86-64, pgvector `0.8.5`,
pgcrypto `1.3`, serialized execution, and explicit process timeouts. The final
semantic gates set session-local `PGOPTIONS='-c jit=off'`; they did not change
server configuration or source bytes. No result from a superseded byte
snapshot is pooled into the final gate.

Static checks on the exact technical bytes passed:

```text
ruff format --check: PASS
ruff check: PASS
mypy --strict src/groundloop/postgres/migrations.py: PASS
python -m compileall -q src tests: PASS
git diff --check: PASS
accepted migration/bundle identity recomputation: PASS
collection: 248 tests
```

The two earlier focused runs below were useful diagnostics on the same
technical bytes, but are not acceptance evidence because their exact pre/post
database inventories were not captured:

```text
non-owner forged transition/promotion contexts + public continuation,
activation pending/failed and complete/degraded negatives:
  3 passed in 4.23s

B2 structural/later-transition matrix, activated-no-history rich backfill,
and activation live-epoch negatives:
  28 passed, 220 deselected in 67.35s
```

The exact acceptance commands were:

```text
cd /tmp/d26-prov-2bc

timeout 14400s env PGOPTIONS='-c jit=off' \
  GROUNDLOOP_TEST_DATABASE_URL='postgresql://groundloop:groundloop@localhost:5432/groundloop' \
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/tmp/d26-prov-2bc \
  pytest -q -p no:cacheprovider --import-mode=importlib \
  --junitxml=/tmp/d26-full-jitoff-2bc.xml \
  tests/m5/postgres_runtime/test_migration_017.py

timeout 3600s env PGOPTIONS='-c jit=off' \
  GROUNDLOOP_TEST_DATABASE_URL='postgresql://groundloop:groundloop@localhost:5432/groundloop' \
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/tmp/d26-prov-2bc \
  pytest -q -p no:cacheprovider --import-mode=importlib \
  --junitxml=/tmp/d26-regression-014-2bc.xml tests/m5/postgres

timeout 3600s env PGOPTIONS='-c jit=off' \
  GROUNDLOOP_TEST_DATABASE_URL='postgresql://groundloop:groundloop@localhost:5432/groundloop' \
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/tmp/d26-prov-2bc \
  pytest -q -p no:cacheprovider --import-mode=importlib \
  --junitxml=/tmp/d26-regression-015-2bc.xml \
  tests/m5/postgres_runtime/test_migration_015.py

timeout 7200s env PGOPTIONS='-c jit=off' \
  GROUNDLOOP_TEST_DATABASE_URL='postgresql://groundloop:groundloop@localhost:5432/groundloop' \
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/tmp/d26-prov-2bc \
  pytest -q -p no:cacheprovider --import-mode=importlib \
  --junitxml=/tmp/d26-regression-016-2bc.xml \
  tests/m5/postgres_runtime/test_migration_016.py
```

All four exact-snapshot commands passed without skip, xfail, deselection,
timeout, or retry pooling:

```text
migration-017 complete module: 248 passed in 322.31s
migration-014 applicable regression: 57 passed in 17.481s
migration-015 applicable regression: 26 passed in 28.619s
migration-016 applicable regression: 200 passed in 256.284s
```

Immediately before and after every command, the complete non-temporary schema
and role inventories were captured. Every boundary was exactly:

```text
schemas = information_schema, pg_catalog, pg_toast, public
roles = groundloop plus the standard pg_* roles
other client backends = empty
disposable schemas = empty
d25_runtime_* roles = empty
```

The disposable-schema scan included every
`d25_migration_017_*`, `groundloop_m5_postgres_*`,
`groundloop_m5_bundle_*`, `groundloop_m5_runtime_*`,
and `groundloop_m5_recovery_*` name. No fixture cleanup was needed after any
passing acceptance command.

## 6. D26-era failure chronology

The superseded snapshot names below identify the ordered technical tuple
`(017 SQL, migrations.py, test_migration_017.py)` by full SHA-256:

```text
aa10 snapshot =
  6a037769cd622c4622c707838872f914b98ce63df21413617a8465038148db11
  f8263cec288eaa66d1b76d47376d806001b633c88fe7e6b7103df188a11aa672
  aa10a494ccfc4504b313f4451791fa546d2f787887d9ffeafd0381a3470bb974
e534 snapshot =
  6a037769cd622c4622c707838872f914b98ce63df21413617a8465038148db11
  f8263cec288eaa66d1b76d47376d806001b633c88fe7e6b7103df188a11aa672
  e5344f8f8bbd01d7a67a46bed9e1096763035f3f4e204b3f44a861cc1d8401e2
ddb0 snapshot =
  46427319cc022775a48203d5b7d50332401ed39a898faaa1992080d8d2361902
  239037f1d69567ac43dc19093a6dc75de9d5e57bfee51ab38182d5ab2f8f3d1c
  ddb05ebf74e783592b9ae958bf135b01d326ed130f23fef7f0b8f36c6dcfdc7d
current 2bc snapshot =
  e387b01fa80145273ba40d2d83581bc54762d3a4a2edd34669c095076c52154c
  37ccb37f4d8760599b6065e59595c1f42bfe799c21a54d0306e598c86cfec421
  2bc96e8472c25355319281f6f6867a6a528fb1768907bd2dd3a37f250a71ee4e
```

| Snapshot/run | Observation | Classification | Correction and restart |
|---|---|---|---|
| Early R6 F31 work | Concurrency tests used direct setup on paths that needed public API reachability; several installer-first cases omitted sequence-state equality. | Test-coverage gap, not a demonstrated production failure. | Reworked cases through public M4/M5/typed-event APIs, added both orderings and exact `(last_value,is_called)` checks. |
| `aa10` snapshot | Full migration-017 run stopped at 127 passed when the empty B2 fixture eagerly indexed an empty logical-change list. | Test-helper defect. | Made `wrong_before` lookup conditional; focused empty and 22-case structural runs passed; restarted from test one. |
| `e534` snapshot | A same-byte audit found ordinary `<>` allowed unset OID settings to yield SQL `NULL` in a private-context comparison. The obsolete full run was stopped after 8 passes. | Database fail-closed defect. | Changed comparisons to NULL-safe checks and added a non-owner regression; restarted applicable gates. |
| `ddb0` snapshot | Two independent reviews showed that callers could set the exact OID GUCs for their own same-name temp tables; promotion was symmetric. One review also found activation incorrectly filtered live epochs by evaluation state. | Database authority and contract-predicate defects. | Added the revoked trusted-owner/ordinary/temp triplet proof to every transition and promotion entrypoint; forged actual OID GUCs in both negative paths; removed the evaluation filter and added both semantic-live negatives. Focused 3- and 28-test diagnostics passed on the current bytes. |
| Current `2bc` snapshot, default JIT | The complete 017 run reached 172 passes, then PostgreSQL PID 224400 terminated with signal 11 while executing `SELECT * FROM groundloop_m5_oracle_mismatch_counts`. Recovery caused the remaining cascade: `76 failed, 172 passed, 1 error in 2752.29s`. Container OOM was false and restart count stayed zero. | PostgreSQL 16.14 backend crash, not 76 independent GroundLoop assertion failures. Static diagnosis found no SQL condition that can legitimately terminate a backend; the large composite oracle is a plausible planner/JIT/executor trigger. | Preserved the server log, proved the first affected node passed alone with default JIT in 33.84s, proved the owning process gone, removed only its exact leftover schema `d25_migration_017_b4b6f7c1adab4a73b6e72fceef0523f2`, and restarted from test one with session-local JIT disabled. |
| Current `2bc` snapshot, JIT off | The complete 017 module and all 014/015/016 regressions passed with exact clean inventories. | Valid database semantic, rollback, trigger, and concurrency evidence under the recorded environment; not proof that PostgreSQL 16.14 default-JIT execution is reliable. | Retain the crash as an environment limitation; do not hide it by changing the oracle or claim default-JIT qualification. |

Interrupted and superseded runs above are chronology only. Their counts are not
combined with current-byte evidence.

## 7. Independent review

On 2026-09-16, two independent Codex reviewer agents statically audited the
same exact three technical hashes in Section 4. Both returned `GO`, `P0=0`,
`P1=0`, `P2=0`. Their scope covered D25/D26 structural semantics, typed digest
framing, installer/transaction/lock behavior, transition and promotion
authority, activation, seal, and test coverage. They did not run tests, access
PostgreSQL, audit this handoff, or review a final commit, so they are supporting
technical reviews rather than the required final candidate audit.

After all gates pass, the lane must be committed with exactly the five-path
activation manifest and a four-path post-replay delta. One independent reviewer
must then inspect the exact clean commit/tree and return `GO` with
`P0=0/P1=0`. Any subsequent byte edit invalidates that audit.

## 8. Remaining gates and later work

### 8.1 Lane B acceptance

The next action is to commit exactly the five Lane B manifest paths, prove a
clean worktree and both required path comparisons, and obtain one independent
same-byte final commit/tree audit with `P0=0/P1=0`. Passing that gate accepts
Lane B only; it does not integrate Wave R6 or change any M5 claim status.

### 8.2 Combined Wave R6 integration

Only after Lane A and Lane B are separately accepted may the coordinator
create the frozen integration worktree, import Lane A and then Lane B without
editing their bytes, prove the exact eleven-path combined delta, and rerun the
combined static, runtime-contract/digest, public-M4/legacy, migration-017, and
014/015/016 database gates. Two independent audits on identical integration
bytes are then required. Only that audited integration commit may fast-forward
and push `origin/main`.

### 8.3 Later store/runtime activation

Successful Wave R6 integration will prove only the pure contract surface plus
migration-017 schema, installer, and database enforcement. A new
path-exclusive activation is still required to:

1. emit the complete changed-state set, including D26 absence references;
2. compose persisted matching, seal, promotion, heads, event results, and
   references atomically in the public store path;
3. integrate D24 recoverable dispatch and durable work/timing accounting;
4. prove reconnect/replay performs zero reference regeneration, zero model
   calls, and zero writes, completing the application half of D26 falsifier
   18;
5. complete the separate remaining D25 store/runtime falsifiers, including its
   next-epoch isolation falsifier 18; and
6. rerun rollback, concurrency, public-M4/typed cross-version, and combined
   integration gates.

No store, runtime-mode, provider, model, production database, or deployment
surface is owned by this lane.
