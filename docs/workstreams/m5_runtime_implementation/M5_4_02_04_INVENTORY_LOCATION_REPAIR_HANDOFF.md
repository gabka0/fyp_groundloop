# M5.4-02/-03/-04 Inventory Location Repair Handoff

Status: **repair-lane candidate PASS** for the mechanical M4 shared-surface
inventory location correction. The corrected complete 2,219-test suite has
only been collected, not executed; no repository-wide pass is claimed here.
Integration, the guarded complete-suite rerun, compatibility, independent
integrated audit, main fast-forward, and status reconciliation remain separate
gates.

Date: 2026-09-02

## 1. Authority, provenance, and immutable boundary

This lane implements only the committed inventory-repair activation:

```text
late-result activation: 3200b39cfe01a4f41fdd1cd1492e85afc468cc2e
Lane A:                b9d251bfdcc4e617240b820f1da9ad289f0d6a23
Lane B:                a41386fcf18a96536c0be23aef99cac1ff9d96e3
repair activation/base: eb8a314ee22b566fb586f4a95165711e01e1282d
repair-activation tree: 7d3ae0e96776aaab97dbcc0f761ae8b2c2fe320e
branch:                  workstream/m5-4-02-04-inventory-location-repair
worktree:                /home/kassym/Desktop/groundloop-worktrees/m5-4-02-04-inventory-location-repair
expected commit parent:  eb8a314ee22b566fb586f4a95165711e01e1282d
```

The base has sole parent `a41386f...` and changes only
`M5_4_02_04_INVENTORY_LOCATION_REPAIR_ACTIVATION.md`. That activation is
SHA-256 `b56d719100384d436e6bac3bbbbf80490f6e5948ee36135ee02895a63fce577f`
(209 lines, 9,110 bytes). The required ancestry is present without merge,
rebase, squash, amend, or rewrite:

```text
3200b39 -> b9d251b -> a41386f -> eb8a314 -> this repair
```

The lane began with an empty index and clean worktree at the exact base. It
opened no database connection, made no network or model/provider call, and
edited no source, test, migration, schema, package export, status, roadmap,
decision-log, prior handoff, activation, dataset, artifact, secret, volume,
cache, or protected user-owned path.

This handoff cannot contain its own final SHA-256 or the commit/tree that
contains itself without a self-reference problem. The coordinator and
auditors must pin those identities externally after the two-path commit is
created.

## 2. Exact owned manifest and mechanical diff

Relative to the repair activation, this lane owns and changes exactly:

1. `docs/workstreams/m5_postgres_oracle/m4_shared_surface_inventory.json`;
2. `docs/workstreams/m5_runtime_implementation/M5_4_02_04_INVENTORY_LOCATION_REPAIR_HANDOFF.md`
   (this new file; final identity is externally pinned).

The inventory changes only these ten ordered integer scalars:

| Finding | Old `line` | New `line` |
|---|---:|---:|
| `M4-RUNTIME-READ-001` | 668 | 768 |
| `M4-RUNTIME-READ-002` | 685 | 785 |
| `M4-RUNTIME-READ-003` | 765 | 863 |
| `M4-RUNTIME-READ-004` | 943 | 1149 |
| `M4-RUNTIME-READ-005` | 977 | 1181 |
| `M4-RUNTIME-READ-006` | 1619 | 2026 |
| `M4-RUNTIME-READ-007` | 2908 | 4282 |
| `M4-RUNTIME-READ-008` | 4214 | 5644 |
| `M4-RUNTIME-WRITE-001` | 4245 | 5675 |
| `M4-RUNTIME-WRITE-002` | 4254 | 5684 |

The inventory-only diff is exactly 10 insertions and 10 deletions. No table,
kind, path, context, evidence string, array membership, status, or other JSON
value changes. Its identities are:

```text
base SHA-256: 55850dd2647f5f4d0e21cbf8fb95f8610f2232470cf9bad5a549b9980312470c
base Git blob: 2e36fc31dfbb53066ba8cfe33c0c1fdb32ce9adb
base size:     206 lines, 9,959 bytes
new SHA-256:  75bd97b53521b779e5de67591e3dee6a39be49b16ac9a9958d0cadbd8e6af3ef
new Git blob: 4511c0538076f0e9493bb3e072ccea0ec86d000c
new size:     206 lines, 9,961 bytes
```

The evidence source and checking test remain unchanged at every commit from
`3200b39` through `eb8a314`:

```text
src/groundloop/m4/pipeline.py
  Git blob 62628474139552c5e06c0538a3206215954713a6
  SHA-256 22c035139fdc57160980c9ddf387ae31dff5b4fbe965d9b48e392db8d63e5b84
  5,955 lines, 239,567 bytes
tests/m5/postgres/test_static_contract.py
  Git blob ea96de0c6d0c68c5fa014cf37459541ef9c924ca
  SHA-256 99617252ccfa79a001357cc25a7a9854aeb8009d40a677c7daeda6e77ea2e3fd
  289 lines, 10,829 bytes
```

The referenced SQL itself did not change. Later M4 source additions moved the
same ten checked windows below their stale recorded locations; this repair
only realigns the mechanical evidence anchors.

## 3. No-database repair gates

All repair gates ran from the dedicated worktree with Python 3.12.3, pytest
9.1.1, Git 2.43.0, `PYTHONDONTWRITEBYTECODE=1`, and the repository virtual
environment. No database URL was supplied and no database or network
connection was opened.

The base, sole parent, activation-only base manifest, clean start, and exact
two-path repair manifest were checked with `git status`, `git show`,
`git diff-tree`, `git merge-base --is-ancestor`, and `git diff --name-status`.
The final whitespace check was:

```bash
git diff --check eb8a314ee22b566fb586f4a95165711e01e1282d -- \
  docs/workstreams/m5_postgres_oracle/m4_shared_surface_inventory.json \
  docs/workstreams/m5_runtime_implementation/M5_4_02_04_INVENTORY_LOCATION_REPAIR_HANDOFF.md
```

Result: **PASS**.

An inline Python proof parsed both the base JSON from `git show` and the
working JSON, asserted the exact ordered IDs, old and new line vectors,
replaced only the base copy's ten `line` values, and required whole-object
equality. It then loaded each named source path and asserted the recorded
table on the new anchor plus the evidence string in the anchor's eight-line
window:

```text
JSON parse: PASS
machine diff proof: PASS; exactly 10 named line scalars changed
eight-line window proof: PASS; 10/10 table anchors and evidence strings
SHA-256: 75bd97b53521b779e5de67591e3dee6a39be49b16ac9a9958d0cadbd8e6af3ef
lines/bytes: 206 / 9,961
```

All static-contract inventory nodes passed:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:. \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider --import-mode=importlib -q -ra \
  tests/m5/postgres/test_static_contract.py -k 'inventory'
```

Result: **3 passed, 7 deselected in 0.14s** (wrapper 0.66s, exit 0).

The previously failing node also passed alone:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:. \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider --import-mode=importlib -q -ra \
  tests/m5/postgres/test_static_contract.py::test_resolved_runtime_inventory_has_location_checked_mechanical_evidence
```

Result: **1 passed in 0.03s** (wrapper 0.55s, exit 0).

## 4. Corrected complete-suite collection and command-only overlays

The complete selection was collected with the independently approved nested
M4 harness import-path additions and process-only provenance remote overlay:

```bash
GIT_CONFIG_COUNT=1 \
GIT_CONFIG_KEY_0=remote.groundloop-pinned.url \
GIT_CONFIG_VALUE_0=https://github.com/gabka0/dynagox.git \
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPATH=src:.:tests/m4/crash_matrix:tests/m4/incrementality:tests/m4/physical_runtime_gate \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider --import-mode=importlib \
  --collect-only -q -ra
```

Result: **2,219 tests collected in 7.96s** (wrapper 8.86s, exit 0). The
persisted Dynagox local Git configuration had the same ordered-output SHA-256
before and after the command:
`c24f9900025754a9bba461e51a2c14a70143cf052dfaac0a39ef3ef7cfd1a5eb`.
The overlay did not write or fetch the Dynagox checkout. The import-path
addition and Git setting exist only in the child process environment; no
repository or checkout configuration changed.

Collection is not execution. This result proves only that the corrected
complete command discovers the required 2,219 tests without collection error.

## 5. Carried-forward live dependencies from `a41386f`

The repair lane did **not** rerun PostgreSQL. The activation explicitly carries
forward these five completed gates from exact Lane B commit `a41386f` because
their source, test, migration, and configuration inputs are byte-identical and
none reads the repaired inventory JSON:

| Carried gate | Exact result on `a41386f` |
|---|---:|
| focused activity | 40 passed, 82 deselected, 77.17s |
| complete recovery module | 122 passed, 205.68s |
| D24 requirement directory | 171 passed, 303.01s |
| migration 016 | 200 passed, 270.42s |
| all PostgreSQL runtime | 797 passed, 1,224.13s |

Every carried gate had a fresh exact pre/post schema inventory match and zero
remaining sessions, locks, or orphan schema. These are dependency results on
`a41386f`, not reruns or new live evidence on the repair commit.

## 6. Recorded non-results and failures

All three prior complete-suite attempts remain non-results and are not pooled
with this repair or a future retry:

1. The original `PYTHONPATH=src:.` / importlib command stopped during
   collection with four missing nested flat-harness imports after discovering
   2,162 tests. It is a **failed command preflight / non-result**.
2. The import-corrected run was stopped after 643 passes and eight skips when
   local Dynagox persisted-remote drift tripped provenance validation. It is
   not a suite pass. The later accepted process-only Git overlay changes no
   persisted checkout state.
3. The next corrected run passed provenance, then failed
   `test_resolved_runtime_inventory_has_location_checked_mechanical_evidence`
   and was interrupted after 843 passes and nine skips. It is not a suite
   pass. That precise node now passes under the scalar-only repair, but the
   whole suite must restart from test 1.

Each stopped database window restored the exact pre-gate database/schema
inventory with zero remaining sessions, locks, or orphan schema. The repair
lane itself had no failed command, skip, xfail, timeout, or superseded edit.

The earlier Lane B handoff remains authoritative for its separately disclosed
host-interrupted PostgreSQL non-result, recovered disposable orphan, and
superseded test-only failures. This repair neither hides nor reclassifies
those events.

## 7. Limitations and non-claims

- No complete-suite execution or final complete-suite pass is claimed.
- No PostgreSQL gate was rerun on the repair bytes; the five live dependencies
  above remain explicitly carried from `a41386f`.
- No production source or runtime behavior changed. This is evidence-metadata
  maintenance, not a semantic fix, performance result, model-quality result,
  security result, or deployment.
- No M5-D25 acceptance, migration 017, persisted matching, active verifier,
  production seal/publication, provider composition, runtime activation,
  maintained-history result, model diagnostic, or status-row promotion is
  claimed.
- The full-suite Git overlay is local process configuration, not proof that
  any remote was contacted or that persisted remote drift was changed.
- The handoff's own final file identity and its containing commit/tree must be
  supplied externally after freeze.

## 8. Requested integration action

Freeze and commit exactly the two owned paths with sole parent `eb8a314...`,
then externally pin the commit, tree, both file identities, diff, and clean
state. Obtain two independent same-byte audits with no unresolved P0/P1.

Only after both repair audits are GO may the coordinator fast-forward the
integration branch through the repair activation and repair commit. It must
then start the guarded complete 2,219-test suite again from test 1 using the
approved command-only import and Git overlays. Record its exact result,
duration, skips/xfails, database inventory restoration, and persisted Dynagox
configuration equality. A passing complete suite is followed by the literal
14-node public-M4/legacy compatibility selection and two integrated audits.

Main fast-forward, focused main reruns, any GitHub push, and the separate
six-document status reconciliation remain coordinator-owned actions.
