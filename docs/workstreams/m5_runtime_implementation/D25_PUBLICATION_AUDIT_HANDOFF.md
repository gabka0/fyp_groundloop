# M5-D25/D26 publication and audit helper handoff

Date: 2026-09-17

Lane: B (`workstream/m5-d25-publication-helpers`)

Activation/base commit: `e3d83e36570efd65703472e3022c252c0d0fc008`

Activation/base tree: `9f98d9c84b01a21be8fd727eaca2930f26388aab`

Rejected checkpoint commit: `5855a0c51f02f5ba1bcc54fd8c405f72ef183064`

Rejected checkpoint tree: `5f2d50bd0c8bac6151bf00adcac1b3c3939ecb6b`

This handoff records only the path-exclusive Lane-B correction authorized by
Section 4.2 of `D25_D26_STORE_RUNTIME_COMPOSITION_ACTIVATION.md`. It does not
activate the public runtime, change `v1_only`, or make D25, D26, M5.4,
deployment, performance, or AI-quality claims.

## 1. Audit response and owned result

The rejected checkpoint received PostgreSQL/schema `NO-GO`, `P0=0`, `P1=7`,
and semantic `NO-GO`, `P0=0`, `P1=4`. This response addresses those findings
without changing any path outside the eight-path Lane-B grant.

`postgres_matching_publication.py` now exposes four cursor-local operations:

- `install_matching_activation_projection(...)` installs the one permitted
  full SQL-oracle activation image;
- `promote_matching_overlay(...)` calls the accepted checked-transition
  authorizer and the migration-017 seal authorizer before promoting one exact
  epoch's observation, edge, mask, and Hall working rows/tombstones;
- `prepare_matching_publication_children(...)` derives deterministic children
  after terminal state/heads exist but before the immutable result parent is
  inserted; and
- `build_matching_publication_children(...)` re-derives identical children
  after parent insertion and requires the exact sealed event-result event,
  payload, epoch, publication, child hashes/counts, receipt bindings, work
  identity, and logical-result identity before returning insertable children.

Neither child path accepts caller-authored changed keys. Present changes come
only from the six migration-014 published interval/binding relations at
`valid_from_epoch = K` and the exact seal revision. D26 absence candidates come
only from requirement-state, group-state, and group-certificate rows/bindings
closed at `K` with no same-object successor. The D26 branch then point-reads
the unique `(K, structural_open, E)` contribution/patch; it performs no
measured-seal aggregate contribution scan.

Before an absence is emitted, the helper validates the complete canonical
patch/contribution envelope: coordinates, all 37 work counters and both work
digests, contribution digest, group-shape bytes, every physical child digest,
preimage, point, shape, and order, logical patch/output bytes, outer patch
digest/preimage, and exact logical before-to-`None` records. It also requires
the exact `replace_group`/`retire_group` update/deactivation/payload mapping,
the closed dense predecessor validity and state intervals, exact certificate
state/binding/artifact rows, historical selected-observation currency and
policy authority, complete absence set, and no same-object successor.
Present-state digest recipes are unchanged. No nullable hash, tombstone table,
seventh reference kind, `repr`/JSON identity, or caller-authored patch API was
introduced.

`postgres_matching_audit.py` retains its read-only actual-image/provenance
adapter and now derives expected epoch status, terminal revision, predecessor
coordinates, and policy independently from joined runtime, epoch, update, and
sealed-predecessor authority. It accepts strictly ordered unique D25 revisions
with coordination gaps, requires the terminal seal revision to be later than
the last D25 contribution, and binds both patch and contribution
`matching_work_digest` values to the independently summed counter vector.
Working/current replay, failed-artifact construction, snapshot ownership, and
the final head recheck boundaries remain unchanged.

Promotion still owns no runtime/epoch header, publication head, immutable
result, transaction, commit, or rollback operation.

## 2. Exact path manifest and technical blobs

Only the following eight granted paths differ from the activation base:

```text
6f6b49460c8b30a395e0380faf0acce327e0d4efd701f8ef0e2e9895831c18c0  src/groundloop/m5/runtime/postgres_matching_publication.py
9a837764f79a2851a710d652bfa95ee3d0e096cd672b70fd4f9606721de4e36b  src/groundloop/m5/runtime/postgres_matching_audit.py
2e8dc638dde14530e14309163519cb9c5485e442f3a0c6e1eea2ae41d32f9f56  tests/m5/postgres_runtime/d25_publication/conftest.py
0c3cfff92ae3aeb964d73c354126856b18d91379493e6fdff7ef7e3bca0998cd  tests/m5/postgres_runtime/d25_publication/test_activation_projection.py
e48e9c5f75495948c1c501d0044d70831eb8b1f531eef3b4e99662743a2268e1  tests/m5/postgres_runtime/d25_publication/test_seal_promotion.py
a851aa81805b0e728c3dff5c5de68539e1b7dae3e322ee3e85161e427153b51f  tests/m5/postgres_runtime/d25_publication/test_changed_state_references.py
725f944729a682578fb6d9d97b252857d9a15f46976c06cfa971f479e3c38d37  tests/m5/postgres_runtime/d25_publication/test_physical_audit.py
```

The handoff cannot contain its own final SHA-256 without self-reference. The
coordinator must record this file's blob and the response commit/tree after
the lane commit.

## 3. Executed verification

Static gates on the response technical bytes:

```text
python -m ruff format --check <two owned source files> tests/m5/postgres_runtime/d25_publication
python -m ruff check <all seven owned Python files>
MYPYPATH=src python -m mypy --strict --explicit-package-bases <all seven owned Python files>
PYTHONDONTWRITEBYTECODE=1 python -m compileall -q <all seven owned Python files>
git diff --check
```

Result: Ruff format/check `PASS`; strict mypy `PASS` for 7 files; compileall
`PASS`; diff check `PASS`.

Pure focused command:

```text
env -u GROUNDLOOP_TEST_DATABASE_URL -u GROUNDLOOP_DATABASE_URL \
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:. \
  python -m pytest -q -p no:cacheprovider --import-mode=importlib \
  tests/m5/postgres_runtime/d25_publication/test_seal_promotion.py \
  tests/m5/postgres_runtime/d25_publication/test_changed_state_references.py \
  tests/m5/postgres_runtime/d25_publication/test_physical_audit.py \
  -k 'not live'
```

Result: `12 passed`, zero failed/skipped/xfail/deselected.

The serial live gate ran from the exact candidate copy inside
`d26-pytest-runner`, sharing the database-container network with
`m5-d26-schema-017-db-1`:

```text
env PGOPTIONS='-c jit=off' \
  GROUNDLOOP_TEST_DATABASE_URL='postgresql://groundloop:groundloop@localhost:5432/groundloop' \
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=<exact-candidate-copy> \
  python -m pytest -q \
  tests/m5/postgres_runtime/d25_publication --tb=short
```

Result: `18 passed`, zero failed/skipped/xfail/deselected, 15.9 seconds
observed wall time.

The same isolated path then ran the retained migration-017 D26 regression
selection:

```text
env PGOPTIONS='-c jit=off' \
  GROUNDLOOP_TEST_DATABASE_URL='postgresql://groundloop:groundloop@localhost:5432/groundloop' \
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=<exact-candidate-copy> \
  python -m pytest -q -p no:cacheprovider --import-mode=importlib \
  tests/m5/postgres_runtime/test_migration_017.py -k 'd26_' --tb=short
```

Result: `79 passed`, zero failed/skipped/xfail/deselected.

The qualified server was PostgreSQL 16.14 x86-64 in pgvector image digest
`sha256:1d533553fefe4f12e5d80c7b80622ba0c382abb5758856f52983d8789179f0fb`,
with session-local `jit=off`. The final non-temporary schema inventory was:

```text
information_schema
pg_catalog
pg_toast
public
```

An earlier host-port attempt ended with a server-closed-connection failure. It
is excluded from evidence. Default-JIT evidence was not run and is not pooled.

## 4. Covered falsifiers

The response adds executable evidence for:

- accepted checked-transition authorization with no raw checked GUC write;
- live nonempty promotion and deletion of observation, edge, mask, and Hall
  tombstones, plus transaction rollback and a lock-timeout competing seal;
- `REPLACE` and `RETIRE` D26 positive paths that re-derive the exact stored
  six-kind child set, and a corrupted result-publication identity negative for
  each action;
- pre-parent deterministic preparation and post-parent exact identity binding;
- canonical structural patch/contribution, physical array, logical output,
  predecessor, certificate, and historical-observation validation through the
  retained 79-case migration-017 D26 matrix;
- live nonempty current-image/tombstone provenance replay and fail-closed
  corruption of contribution work identity;
- coordination gaps in semantic contribution revisions with a later seal;
- independent update-policy authority despite a self-consistent wrong
  patch/header policy; and
- explicit patch-versus-contribution work-digest disagreement.

The existing activation, canonical projection, keyed mismatch, and
`malformed_payload` tests remain green.

## 5. Honest uncovered rows and integration requirements

The following remain outside this lane and must not be converted into a
`PASS` claim:

1. Lane-C1 ownership of the one outer seal transaction, result/child insertion,
   publication heads, terminal headers, rollback/crash cuts, and exact replay;
2. shared exported-snapshot orchestration, independent Python/SQL expected
   readers, import failure, overlapping later-seal ambiguity, and the final
   post-collection head recheck;
3. application-side F18 reconnect with zero writes, model calls, or child
   regeneration;
4. every individual database-corruption variant beyond the exercised event
   publication identity and contribution work mismatch; and
5. policy-change, rootless requirement-observation, and standalone
   claim-observation rows explicitly left `PENDING` by the activation.

Unsupported or incomplete authority fails closed. The two-phase child API is
not permission to insert children from the prepare result; Lane C1 must insert
the parent and call the result-bound builder in the same outer transaction.

## 6. Review and nonclaims

The corrected response has not yet received a new independent same-byte
`P0=0`/`P1=0` audit. The rejected checkpoint audits are diagnostic evidence,
not acceptance of these changed bytes. No source outside the grant was edited.
No migration, frozen DTO, digest recipe, runtime mode, deployment setting,
dataset, model weight, secret, database volume, or generated cache is part of
the response.

D25, D26, M5.4--M5.6, deployment, performance, and AI-quality claims remain
`PENDING`.
