# M5-D25/D26 publication and audit helper handoff

Date: 2026-09-17

Lane: B (`workstream/m5-d25-publication-helpers`)

```text
activation_commit = e3d83e36570efd65703472e3022c252c0d0fc008
activation_tree = 9f98d9c84b01a21be8fd727eaca2930f26388aab
lane_base = e3d83e36570efd65703472e3022c252c0d0fc008
rejected_checkpoint = 5855a0c51f02f5ba1bcc54fd8c405f72ef183064
rejected_checkpoint_tree = 5f2d50bd0c8bac6151bf00adcac1b3c3939ecb6b
corrective_parent = ff7b611d72bd081a76997ef9fa41f5d69a919384
corrective_parent_tree = 6ec75edb166ddc88d018405f7428a7b30e379336
accepted_d25_sha256 = bac12ab5e74632c04f1bd70d0ef0d00522ba9d268eb8b73d11845bbf3b873aae
accepted_d26_sha256 = 85372d4c2f9108810bd75c3e5611de541d0f31c8a096421f30e68fad84676721
```

This handoff records only the path-exclusive Lane-B correction authorized by
Section 4.2 of `D25_D26_STORE_RUNTIME_COMPOSITION_ACTIVATION.md`. It does not
activate the public runtime, change `v1_only`, or make D25, D26, M5.4,
deployment, performance, or AI-quality claims.

## 1. Audit response and owned result

The original checkpoint received PostgreSQL/schema `NO-GO`, `P0=0`, `P1=7`,
and semantic `NO-GO`, `P0=0`, `P1=4`. Its first correction at the exact
`corrective_parent` above received PostgreSQL/race and semantic `NO-GO`, each
with `P0=0`, `P1=3`. The remaining findings were:

1. the result-bound builder trusted the immutable parent's work digest without
   independently point-reading and validating the exact event work row;
2. the handoff incorrectly reported zero deselections for two filtered test
   commands; and
3. evidence custody lacked concrete commands, complete counts/durations,
   pre/post inventories for each database tranche, and an explicit
   falsifier-to-node map.

This response closes those three finding groups. Before returning publication
children, `build_matching_publication_children(...)` now point-reads the exact
`groundloop_m5_runtime_work` primary-key row for `(structural_event_id,
work_kind='event')`. It constructs the frozen `M5RuntimeWork` DTO from all 32
persisted counters, thereby recomputing the work digest, and independently
requires the exact event, epoch, `event` kind, parent work digest, and
`public_delta_count == len(combined_deltas)`. Missing authority, arbitrary
self-consistent parent work, wrong event/epoch/kind, bad counter/digest bytes,
or a wrong public-delta count fail closed.

The rest of the owned surface remains narrow:

- `install_matching_activation_projection(...)` installs the one permitted
  full SQL-oracle activation image;
- `promote_matching_overlay(...)` calls the accepted checked-transition
  authorizer before the migration-017 seal authorizer and promotes one exact
  epoch's observation, edge, mask, and Hall working rows/tombstones;
- `prepare_matching_publication_children(...)` derives deterministic children
  after terminal state/heads exist but before the immutable result parent is
  inserted; and
- `build_matching_publication_children(...)` re-derives those children after
  parent insertion and validates the complete result and work authority.

Neither child path accepts caller-authored changed keys. Present changes come
only from the six migration-014 published interval/binding relations at the
event epoch and exact seal revision. D26 absence candidates come only from
requirement-state, group-state, and group-certificate rows/bindings closed at
that epoch with no same-object successor. The absence branch point-reads the
unique contribution/patch and performs no aggregate contribution scan.

Before an absence is emitted, the helper validates the canonical patch and
contribution coordinates, all matching-work counters and both work digests,
contribution and outer patch identities, group-shape and physical-child bytes,
logical before-to-`None` records, exact `replace_group`/`retire_group`
update/deactivation/payload identity, dense predecessor intervals, certificate
state/binding/artifact rows, historical selected-observation currency and
policy authority, the complete absence set, and absence of a successor.
Present-state recipes are unchanged. No nullable hash, tombstone table,
seventh kind, `repr`/JSON identity, or caller-authored patch API was added.

The read-only audit adapter derives epoch status, terminal revision,
predecessor coordinates, and policy independently from runtime, epoch, update,
and predecessor authority. It accepts strictly ordered unique D25 revisions
with legal coordination gaps, requires a later seal revision, and binds patch
and contribution work digests to the independently summed counter vector.

Promotion owns no runtime/epoch header, publication head, immutable result,
transaction, commit, or rollback operation. The two-phase child API grants no
authority to insert prepared children; Lane C1 must insert the result parent
and call the result-bound builder in the same outer transaction.

## 2. Exact path manifest and blob custody

`git diff --name-status e3d83e36570efd65703472e3022c252c0d0fc008 --`
contains exactly these eight granted additions and no other path:

```text
A docs/workstreams/m5_runtime_implementation/D25_PUBLICATION_AUDIT_HANDOFF.md
A src/groundloop/m5/runtime/postgres_matching_audit.py
A src/groundloop/m5/runtime/postgres_matching_publication.py
A tests/m5/postgres_runtime/d25_publication/conftest.py
A tests/m5/postgres_runtime/d25_publication/test_activation_projection.py
A tests/m5/postgres_runtime/d25_publication/test_changed_state_references.py
A tests/m5/postgres_runtime/d25_publication/test_physical_audit.py
A tests/m5/postgres_runtime/d25_publication/test_seal_promotion.py
```

Final technical-path SHA-256 values:

```text
a4614b122ae19e7456e754421346fc5f63daf255fd2626c82f8f658b1768bbad  src/groundloop/m5/runtime/postgres_matching_publication.py
9a837764f79a2851a710d652bfa95ee3d0e096cd672b70fd4f9606721de4e36b  src/groundloop/m5/runtime/postgres_matching_audit.py
2e8dc638dde14530e14309163519cb9c5485e442f3a0c6e1eea2ae41d32f9f56  tests/m5/postgres_runtime/d25_publication/conftest.py
0c3cfff92ae3aeb964d73c354126856b18d91379493e6fdff7ef7e3bca0998cd  tests/m5/postgres_runtime/d25_publication/test_activation_projection.py
e48e9c5f75495948c1c501d0044d70831eb8b1f531eef3b4e99662743a2268e1  tests/m5/postgres_runtime/d25_publication/test_seal_promotion.py
4252f34d44ad7a9933cd042c4b6353090ce8bca98bc4c0af672ac1579895a0b4  tests/m5/postgres_runtime/d25_publication/test_changed_state_references.py
725f944729a682578fb6d9d97b252857d9a15f46976c06cfa971f479e3c38d37  tests/m5/postgres_runtime/d25_publication/test_physical_audit.py
```

The handoff cannot contain its own final blob, containing commit, or containing
tree without changing those identities. The external custody record delivered
with the committed candidate must therefore record: the handoff SHA-256, the
direct-child commit and tree, its sole parent `ff7b611d72bd081a76997ef9fa41f5d69a919384`,
and the final clean index/worktree. A missing or mismatched external record is
a hard audit failure, not permission to infer an identity.

## 3. Static and pure verification

All Python commands ran inside `d26-pytest-runner` from the exact candidate
copy `/tmp/d25-lane-b-fix.FGdRyf`. The runner used Python 3.12.14, Psycopg
3.3.4, pytest 9.1.1, Ruff 0.15.22, and mypy 2.3.0. Its image was
`python@sha256:78387bc3881b8273120a12ebe6c1ab22b018ccc2c9adf565ae1ac9b536e184ea`.

Exact static commands:

```bash
ruff format --check \
  src/groundloop/m5/runtime/postgres_matching_publication.py \
  src/groundloop/m5/runtime/postgres_matching_audit.py \
  tests/m5/postgres_runtime/d25_publication/conftest.py \
  tests/m5/postgres_runtime/d25_publication/test_activation_projection.py \
  tests/m5/postgres_runtime/d25_publication/test_seal_promotion.py \
  tests/m5/postgres_runtime/d25_publication/test_changed_state_references.py \
  tests/m5/postgres_runtime/d25_publication/test_physical_audit.py

ruff check \
  src/groundloop/m5/runtime/postgres_matching_publication.py \
  src/groundloop/m5/runtime/postgres_matching_audit.py \
  tests/m5/postgres_runtime/d25_publication/conftest.py \
  tests/m5/postgres_runtime/d25_publication/test_activation_projection.py \
  tests/m5/postgres_runtime/d25_publication/test_seal_promotion.py \
  tests/m5/postgres_runtime/d25_publication/test_changed_state_references.py \
  tests/m5/postgres_runtime/d25_publication/test_physical_audit.py

MYPYPATH=src python -m mypy --strict --explicit-package-bases \
  src/groundloop/m5/runtime/postgres_matching_publication.py \
  src/groundloop/m5/runtime/postgres_matching_audit.py \
  tests/m5/postgres_runtime/d25_publication/conftest.py \
  tests/m5/postgres_runtime/d25_publication/test_activation_projection.py \
  tests/m5/postgres_runtime/d25_publication/test_seal_promotion.py \
  tests/m5/postgres_runtime/d25_publication/test_changed_state_references.py \
  tests/m5/postgres_runtime/d25_publication/test_physical_audit.py

PYTHONDONTWRITEBYTECODE=1 python -m compileall -q \
  src/groundloop/m5/runtime/postgres_matching_publication.py \
  src/groundloop/m5/runtime/postgres_matching_audit.py \
  tests/m5/postgres_runtime/d25_publication/conftest.py \
  tests/m5/postgres_runtime/d25_publication/test_activation_projection.py \
  tests/m5/postgres_runtime/d25_publication/test_seal_promotion.py \
  tests/m5/postgres_runtime/d25_publication/test_changed_state_references.py \
  tests/m5/postgres_runtime/d25_publication/test_physical_audit.py

PYTHONDONTWRITEBYTECODE=1 \
PYTHONPATH=/tmp/d25-lane-b-fix.FGdRyf/src \
python -c 'from groundloop.m5.runtime.postgres_matching_publication import build_matching_publication_children; from groundloop.m5.runtime.postgres_matching_audit import audit_matching_actual_image; print(build_matching_publication_children.__name__, audit_matching_actual_image.__name__)'

git diff --check
```

Results: format `7 files already formatted`; Ruff `All checks passed!`;
strict mypy `Success: no issues found in 7 source files`; compileall and import
smoke passed; the import smoke printed
`build_matching_publication_children audit_matching_actual_image`; final diff
check passed.

Exact pure command:

```bash
env -u GROUNDLOOP_TEST_DATABASE_URL -u GROUNDLOOP_DATABASE_URL \
  PYTHONDONTWRITEBYTECODE=1 \
  PYTHONPATH=/tmp/d25-lane-b-fix.FGdRyf/src \
  python -m pytest -o addopts='' -q -p no:cacheprovider \
  --import-mode=importlib --junitxml=/tmp/d25-lane-b-pure-final.xml \
  tests/m5/postgres_runtime/d25_publication/test_seal_promotion.py \
  tests/m5/postgres_runtime/d25_publication/test_changed_state_references.py \
  tests/m5/postgres_runtime/d25_publication/test_physical_audit.py \
  -k 'not live' --tb=short
```

Result: 22 collected, 18 selected, 18 passed, 4 deselected, 0 failed,
0 errors, 0 skipped, 0 xfailed, in 0.08 seconds. JUnit recorded 18 tests,
0 failures/errors/skips, and 0.083 seconds.

## 4. Qualified PostgreSQL verification and inventory

The qualified server was PostgreSQL 16.14 x86-64 in
`pgvector/pgvector@sha256:1d533553fefe4f12e5d80c7b80622ba0c382abb5758856f52983d8789179f0fb`.
The runner shared the database container's network namespace. Both database
tranches ran serially with `PGOPTIONS='-c jit=off'`; an independent runner
connection printed:

```text
16.14 (Debian 16.14-1.pgdg12+1)
jit=off
```

The exact inventory command ran immediately before and immediately after each
database tranche:

```bash
docker exec m5-d26-schema-017-db-1 \
  psql -X -v ON_ERROR_STOP=1 -U groundloop -d groundloop -Atc \
  "SELECT 'database|' || datname FROM pg_database ORDER BY datname COLLATE \"C\"; SELECT 'schema|' || nspname FROM pg_namespace WHERE nspname !~ '^pg_temp_' AND nspname !~ '^pg_toast_temp_' ORDER BY nspname COLLATE \"C\"; SELECT 'test_role_count|' || count(*)::text FROM pg_roles WHERE rolname LIKE 'd25_runtime_%' OR rolname LIKE 'groundloop_m5_%'; SELECT 'other_client_count|' || count(*)::text FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND backend_type='client backend';"
```

The four boundary inventories were identical:

| Tranche | Boundary | Databases | Non-temporary schemas | Test roles | Other clients |
| --- | --- | --- | --- | ---: | ---: |
| Lane-B directory | pre | `groundloop`, `postgres`, `template0`, `template1` | `information_schema`, `pg_catalog`, `pg_toast`, `public` | 0 | 0 |
| Lane-B directory | post | `groundloop`, `postgres`, `template0`, `template1` | `information_schema`, `pg_catalog`, `pg_toast`, `public` | 0 | 0 |
| migration-017 D26 | pre | `groundloop`, `postgres`, `template0`, `template1` | `information_schema`, `pg_catalog`, `pg_toast`, `public` | 0 | 0 |
| migration-017 D26 | post | `groundloop`, `postgres`, `template0`, `template1` | `information_schema`, `pg_catalog`, `pg_toast`, `public` | 0 | 0 |

Exact full Lane-B command:

```bash
timeout 3600s env PGOPTIONS='-c jit=off' \
  GROUNDLOOP_TEST_DATABASE_URL='postgresql://groundloop:groundloop@localhost:5432/groundloop' \
  PYTHONDONTWRITEBYTECODE=1 \
  PYTHONPATH=/tmp/d25-lane-b-fix.FGdRyf/src \
  python -m pytest -o addopts='' -q -p no:cacheprovider \
  --import-mode=importlib --junitxml=/tmp/d25-lane-b-live-final.xml \
  tests/m5/postgres_runtime/d25_publication --tb=short
```

Result: 24 collected/selected, 24 passed, 0 deselected, 0 failed, 0 errors,
0 skipped, 0 xfailed, in 14.29 seconds. JUnit recorded 24 tests,
0 failures/errors/skips, and 14.294 seconds.

Exact retained migration-017 D26 command:

```bash
timeout 3600s env PGOPTIONS='-c jit=off' \
  GROUNDLOOP_TEST_DATABASE_URL='postgresql://groundloop:groundloop@localhost:5432/groundloop' \
  PYTHONDONTWRITEBYTECODE=1 \
  PYTHONPATH=/tmp/d25-lane-b-fix.FGdRyf/src \
  python -m pytest -o addopts='' -q -p no:cacheprovider \
  --import-mode=importlib --junitxml=/tmp/d25-lane-b-d26-final.xml \
  tests/m5/postgres_runtime/test_migration_017.py -k 'd26_' --tb=short
```

Result: 248 collected, 79 selected, 79 passed, 169 deselected, 0 failed,
0 errors, 0 skipped, 0 xfailed, in 164.91 seconds (`0:02:44`). JUnit
recorded 79 tests, 0 failures/errors/skips, and 164.912 seconds.

One earlier Lane-B directory attempt overlapped another lane's schema fixture.
It produced 22 passes and two fixture-inventory failures because foreign UUID
schemas appeared/disappeared between that fixture's before/after snapshots.
That run is invalid environmental evidence and is not pooled above. The
coordinator then reserved the database exclusively; the authoritative rerun
and both of its boundary inventories are the ones reported here. An older
host-port server-closed-connection attempt is likewise excluded. Default-JIT
evidence was not run and is not a product pass.

## 5. Frozen falsifier-to-node map

| Lane-B claim/falsifier | Exact test node or retained selection |
| --- | --- |
| Empty activation projection is exact and cursor-local | `test_activation_projection.py::test_empty_activation_projection_is_cursor_local_and_exact` |
| Committed activation image passes actual-image/provenance audit | `test_activation_projection.py::test_committed_empty_activation_passes_actual_image_and_provenance_audit` |
| Checked authorizer precedes seal authorization; no raw checked GUC; one-epoch ranges; no outer transaction/head/result ownership | `test_seal_promotion.py::test_seal_promotion_is_one_epoch_and_does_not_own_outer_transaction` |
| Nonadjacent seal revision fails before SQL | `test_seal_promotion.py::test_seal_promotion_rejects_nonadjacent_seal_before_sql` |
| Live nonempty observation/edge/mask/Hall tombstone promotion, competing lock timeout, and rollback restoration | `test_seal_promotion.py::test_live_nonempty_tombstone_promotion_uses_checked_authority` |
| Two-phase preparation/build is deterministic and exact-result-bound | `test_changed_state_references.py::test_empty_preparation_then_exact_result_binding_is_deterministic` |
| Wrong result publication identity fails closed | `test_changed_state_references.py::test_result_binding_rejects_wrong_publication_identity` |
| Missing exact event work row fails closed | `test_changed_state_references.py::test_result_binding_rejects_missing_exact_event_work` |
| Arbitrary self-consistent parent work digest fails closed | `test_changed_state_references.py::test_result_binding_rejects_arbitrary_parent_event_work_digest` |
| Wrong event/epoch/kind in work authority fails closed | `test_changed_state_references.py::test_result_binding_rejects_mismatched_event_work_authority[event]`, `[epoch]`, `[kind]` |
| Exact work row with wrong `public_delta_count` fails closed | `test_changed_state_references.py::test_result_binding_rejects_wrong_event_work_public_delta_count` |
| Absence is restricted to exact structural replace/retire | `test_changed_state_references.py::test_absence_candidate_still_requires_replace_or_retire` |
| Full six-kind D25/D26 child set is re-derived for structural replace | `test_changed_state_references.py::test_live_d26_children_rederive_exact_sealed_result[REPLACE]` |
| Full six-kind D25/D26 child set is re-derived for structural retire | `test_changed_state_references.py::test_live_d26_children_rederive_exact_sealed_result[RETIRE]` |
| Projection digest is stable and payload changes are keyed | `test_physical_audit.py::test_projection_digest_is_stable_and_payload_changes_are_keyed` |
| Duplicate/unsorted outer keys fail closed | `test_physical_audit.py::test_expected_projection_rejects_duplicate_or_unsorted_outer_keys` |
| Canonical equal actual image and provenance build a pass artifact | `test_physical_audit.py::test_canonical_equal_projection_and_provenance_build_pass_artifact` |
| Malformed keyed current image stops before provenance | `test_physical_audit.py::test_keyed_malformed_current_skips_provenance` |
| Independent runtime authority accepts legal coordination gaps | `test_physical_audit.py::test_runtime_authority_accepts_coordination_gaps_and_binds_update_policy` |
| Update policy overrides self-consistent wrong patch/header policy | `test_physical_audit.py::test_structural_patch_policy_comes_from_update_not_patch_or_header` |
| Patch/contribution work-digest disagreement fails closed | `test_physical_audit.py::test_patch_and_contribution_work_digests_must_match` |
| Live nonempty provenance replay passes; contribution-work corruption fails | `test_physical_audit.py::test_live_nonempty_provenance_replay_and_work_digest_corruption` |
| D26 closure, predecessor, payload/event/update/deactivation, certificate, currency, point-read contribution/patch, byte-total structural identity, absence completeness, and no-successor matrix | the 79 selected nodes in `test_migration_017.py -k 'd26_'` |

The live promotion race order is: retain the first transaction's checked seal
authorization and promoted rows uncommitted; attempt the same seal from a
second connection with a 500 ms lock timeout; require `LockNotAvailable` and
rollback the contender; rollback the first transaction; then require the
original installed epoch/revision. No reconnect claim is made by Lane B;
zero-write reconnect/replay belongs to Lane C1.

## 6. Honest uncovered rows and integration requirements

The following remain outside this lane and `PENDING`:

1. Lane-C1 ownership of the one outer seal transaction, result/child insertion,
   publication heads, terminal headers, rollback/crash cuts, exact replay, and
   zero-write reconnect;
2. shared exported-snapshot orchestration, independent Python/SQL expected
   readers, import failure, overlapping later-seal ambiguity, and the final
   post-collection head recheck;
3. application-side F18 reconnect with zero writes, model calls, or child
   regeneration;
4. every database-corruption variant beyond those mapped above; and
5. policy-change, rootless requirement-observation, and standalone
   claim-observation rows explicitly left uncovered by the activation.

Unsupported or incomplete authority fails closed. Lane-local green evidence
does not accept Task 2 or any status row.

## 7. Review state and nonclaims

The corrected response has not received new independent same-byte acceptance.
The rejected audits are diagnostic evidence only. A fresh semantic/correctness
reviewer and a fresh PostgreSQL/race reviewer must audit the identical committed
bytes and both return `GO`, `P0=0`, `P1=0`; any byte edit restarts both reviews.
No lane self-accepts.

No source outside the grant, migration, frozen DTO, digest recipe, runtime
mode, deployment setting, dataset, model weight, secret, database volume, or
generated cache is part of the response. D25, D26, M5.4--M5.6, deployment,
performance, and AI-quality claims remain `PENDING`.
