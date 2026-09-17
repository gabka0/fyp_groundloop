# M5-D25/D26 publication and audit helper handoff

Date: 2026-09-17

Lane: B (`workstream/m5-d25-publication-helpers`)

Activation/base commit: `e3d83e36570efd65703472e3022c252c0d0fc008`

Activation/base tree: `9f98d9c84b01a21be8fd727eaca2930f26388aab`

This handoff records only the path-exclusive Lane-B checkpoint authorized by
Section 4.2 of
`D25_D26_STORE_RUNTIME_COMPOSITION_ACTIVATION.md`. It does not activate the
public runtime, change `v1_only`, or make D25, D26, M5.4, deployment, or
AI-quality claims.

## 1. Owned result

`postgres_matching_publication.py` adds three cursor-local APIs:

- `install_matching_activation_projection(...)` authorizes and installs the
  one permitted full SQL-oracle bootstrap into the D25 current image;
- `promote_matching_overlay(...)` authorizes and promotes exactly one epoch's
  retained observation/edge/mask/Hall overlay, including tombstones, without
  owning semantic publication, heads, results, commit, or rollback; and
- `build_matching_publication_children(...)` derives net public status deltas
  and all six existing changed-state reference kinds from retained patch and
  published-state authority.

The D26 branch admits absence only for `requirement_state`, `group_state`, and
`group_certificate`. It independently requires an exact sealed
`replace_group`/`retire_group` event; event/update/deactivation identity;
payload derivation; predecessor group and requirement validity/closure;
applicable exact certificate artifact/binding; exactly one revision-1
`structural_open` before-to-`None` change at the predecessor seal coordinates;
the complete absence set; and no same-object successor. Present references use
the pre-existing D22/D25 artifact recipes. No nullable hash, tombstone table,
seventh reference kind, `repr`/JSON identity, or present-state recipe was
introduced.

`postgres_matching_audit.py` adds:

- `M5MatchingAuditProjection`, the canonical, key-sorted semantic projection
  DTO shared only at the comparator boundary;
- `audit_matching_actual_image(...)`, a read-only actual-image/provenance
  adapter for a caller-owned imported snapshot; and
- `M5MatchingAuditInvalidError`, the no-artifact result for invalid outer
  keys, duplicate keys, header drift, undecodable provenance, or incomplete
  retained authority.

The audit requires byte-identical Python/SQL expected projections, reads every
current row, implements the keyed `malformed_payload` failed-artifact branch,
rehashes canonical patch/child/logical/contribution evidence, independently
sums all 37 contribution counters, reconstructs retained working rows, applies
only sealed promotions, checks current installed coordinates, and emits the
frozen projection, row, provenance-pair, replay, mismatch, and outer audit
digests. Snapshot export/import and the final post-collection head recheck
remain coordinator-owned; the helper opens no transaction.

## 2. Exact path manifest and technical blobs

Only the following eight granted paths are present in the candidate:

```text
df569f6f67b74177a6b0337fd35290853ce3d18e3f18b7600b5d598f55994a69  src/groundloop/m5/runtime/postgres_matching_publication.py
8337e89b3b4b1842cb41a289dd12a02c01d09e7f3ca6c930d7552e38e845e1b7  src/groundloop/m5/runtime/postgres_matching_audit.py
2e8dc638dde14530e14309163519cb9c5485e442f3a0c6e1eea2ae41d32f9f56  tests/m5/postgres_runtime/d25_publication/conftest.py
0c3cfff92ae3aeb964d73c354126856b18d91379493e6fdff7ef7e3bca0998cd  tests/m5/postgres_runtime/d25_publication/test_activation_projection.py
20e917fea85411fb901e2a686bf3da87cc54d7df4f773ea49a7710e9fe8d7df2  tests/m5/postgres_runtime/d25_publication/test_seal_promotion.py
d9ecca0d463d0f72f8c70da0ebf1a476e3c3b9c113b3cbc7354a31e851d8e156  tests/m5/postgres_runtime/d25_publication/test_changed_state_references.py
ea5150aaaa84302e3186997be9aa9cbd3e7ff493ad11fc7cc0a9752502939f22  tests/m5/postgres_runtime/d25_publication/test_physical_audit.py
```

The handoff cannot contain its own final SHA-256 without self-reference. The
coordinator must record this file's final blob plus the candidate commit/tree
after the lane commit. The hashes above are the pre-handoff technical blobs;
the final static gate below rechecks their bytes together with this file.

## 3. Executed verification

Static gates on the final technical bytes:

```text
python -m ruff format --check <two owned source files> tests/m5/postgres_runtime/d25_publication
python -m ruff check <two owned source files> tests/m5/postgres_runtime/d25_publication
MYPYPATH=src python -m mypy --strict --explicit-package-bases <all seven owned Python files>
PYTHONDONTWRITEBYTECODE=1 python -m compileall -q <two owned source files> tests/m5/postgres_runtime/d25_publication
git diff --check
```

Result: Ruff format/check PASS; strict mypy PASS for 7 files; compileall PASS;
diff check PASS.

Pure focused command:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:. pytest -q -p no:cacheprovider \
  --import-mode=importlib \
  tests/m5/postgres_runtime/d25_publication/test_seal_promotion.py \
  tests/m5/postgres_runtime/d25_publication/test_changed_state_references.py \
  tests/m5/postgres_runtime/d25_publication/test_physical_audit.py
```

Result: `9 passed`, zero failed/skipped/xfail/deselected, 1.02 seconds.

The serial live gate ran from the exact candidate copy inside
`d26-pytest-runner`, sharing the database container network with
`m5-d26-schema-017-db-1`:

```text
timeout 1800s env PGOPTIONS='-c jit=off' \
  GROUNDLOOP_TEST_DATABASE_URL='postgresql://groundloop:groundloop@localhost:5432/groundloop' \
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=<exact-candidate-copy> \
  pytest -q -p no:cacheprovider --import-mode=importlib \
  tests/m5/postgres_runtime/d25_publication
```

Result: `11 passed`, zero failed/skipped/xfail/deselected, 3.5 seconds observed
wall time. The qualified server was PostgreSQL 16.14 x86-64, pgvector image
digest
`sha256:1d533553fefe4f12e5d80c7b80622ba0c382abb5758856f52983d8789179f0fb`,
with session-local `jit=off`. The pre/post non-temporary schema inventory was
identical:

```text
('information_schema', 'pg_catalog', 'pg_toast', 'public')
```

The candidate copy was removed from the runner after the gate. No host-port or
default-JIT result is pooled into this evidence.

## 4. Covered falsifiers

Executable coverage in this lane proves:

- an empty migration-017 activation projection authorizes and writes the exact
  current header without claiming public activation ownership;
- the correct coordinator order (semantic bootstrap, D25 projection, heads,
  activation, mode CAS, deferred checks) commits, and an empty canonical
  actual-image plus zero-history provenance audit passes;
- promotion selects one epoch, checks adjacent seal coordinates, applies every
  family/tombstone branch, and contains no head/result/commit/rollback SQL;
- empty net change emits empty children deterministically;
- claim-state absence is rejected and an allowed D26 kind is still rejected
  outside exact `REPLACE`/`RETIRE` authority;
- projection ordering/digest changes and keyed physical mismatches are exact;
  and
- the canonical equal and keyed-malformed artifact shapes satisfy the frozen
  DTO/digest invariants, with malformed current taking precedence and skipping
  provenance.

## 5. Honest uncovered rows and integration requirements

The following remain uncovered by Lane-B executable evidence and must not be
converted into a PASS claim:

1. a live nonempty structural/revision patch history through the public Lane-A
   installer, seal authorizer, promotion deferred guards, and provenance
   replay;
2. live positive/tombstone promotion for all four physical families under
   concurrent or failure-injected seal composition;
3. live `REPLACE` and `RETIRE` D26 happy paths, plus each predecessor,
   certificate, successor, payload, event, coordinate, and absence-set
   negative as a separately seeded PostgreSQL falsifier;
4. a database-corruption fixture for keyed malformed current payload and
   malformed retained provenance (the artifact branch itself has pure tests);
5. shared exported-snapshot orchestration, import failure, overlapping later
   seal ambiguity, and the final head recheck; and
6. composition with result insertion and rollback/crash cuts, which belongs to
   the later path-exclusive store/application lanes.

Accordingly this candidate is a scoped Lane-B implementation checkpoint, not
aggregate D25/D26 or M5.4 acceptance. Unsupported/incomplete provenance shapes
fail closed with no artifact rather than being hashed or treated as absence.

## 6. Review and nonclaims

Self-review corrected the actual-header policy query so it requires exactly one
active strict policy, separated malformed semantic payload from invalid
installed-coordinate handling, required canonical Hall arithmetic through the
frozen point DTO, and preserved the explicit zero-row sequence frames in all
audit/provenance digests.

No independent same-byte audit was performed in this lane. No source outside
the grant was edited. No migration, contract, present-state digest recipe,
runtime mode, deployment setting, dataset, model weight, secret, database
volume, or generated cache is part of the candidate. D25, D26, M5.4--M5.6,
deployment, performance, and AI-quality claims remain `PENDING`.
