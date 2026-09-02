# M5.4-02/-03/-04 Full-Suite Inventory Location Repair Activation

Status: coordinator correction activation candidate. This document grants no
repair ownership until it is committed as the sole change on top of the exact
accepted Lane B candidate and independently audited.

Date: 2026-09-02

Required parent:
`a41386fcf18a96536c0be23aef99cac1ff9d96e3`

## 1. Authority and purpose

Read `AGENTS.md`, the required GroundLoop authority sequence, and
`M5_4_02_04_LATE_RESULT_ACTIVITY_ACTIVATION.md` before acting. The frozen M5
design and accepted M5-D24 corrections continue to control semantics. This
document adds one mechanical evidence-maintenance tranche after the accepted
five-path Lane A/Lane B candidate; it does not rewrite or enlarge that
candidate's manifest.

The complete-repository gate exposed a pre-existing defect in
`docs/workstreams/m5_postgres_oracle/m4_shared_surface_inventory.json`: all ten
entries in `resolved_runtime_findings` still point to the line locations from
commit `fd13cca`, although later M4 commits inserted code above the same SQL
windows. The referenced SQL, table names, predicates, and explicit column
lists remain unchanged. Only their line anchors moved.

The relevant Git blobs are identical at activation `3200b39`, Lane A
`b9d251b`, and Lane B `a41386f`:

- `src/groundloop/m4/pipeline.py`;
- `docs/workstreams/m5_postgres_oracle/m4_shared_surface_inventory.json`; and
- `tests/m5/postgres/test_static_contract.py`.

Therefore this is not a Lane A/Lane B regression and is not a production
semantic failure. It is stale mechanical evidence metadata that must still be
repaired because the complete suite may not be waived, skipped, or reported
as passing while its location check fails.

## 2. Recorded full-suite non-results

The first complete-suite attempt used the activation's existing
`PYTHONPATH=src:.` command with `--import-mode=importlib`. Collection stopped
with four missing nested M4 flat harness modules after discovering 2,162
tests. It is a `FAILED COMMAND PREFLIGHT / NON-RESULT`; no suite result follows
from it. An independently adjudicated command-only import-path correction
adds exactly:

```text
tests/m4/crash_matrix
tests/m4/incrementality
tests/m4/physical_runtime_gate
```

That environment collects the complete 2,219-test selection.

The first corrected run then found local Dynagox provenance-remote drift after
643 passes and eight skips. The pinned commits, parent, blobs, content, diff,
and extractions were present and exact, but the persisted checkout no longer
listed the frozen historical HTTPS remote. The run was stopped and is not a
pass. Independent review accepted a process-only, zero-write, no-network Git
configuration overlay for the complete-suite command:

```text
GIT_CONFIG_COUNT=1
GIT_CONFIG_KEY_0=remote.groundloop-pinned.url
GIT_CONFIG_VALUE_0=https://github.com/gabka0/dynagox.git
```

The overlay must not mutate or fetch the Dynagox checkout, and its persisted
remote configuration must be identical before and after the gate.

The next complete run passed the provenance node and then failed
`test_resolved_runtime_inventory_has_location_checked_mechanical_evidence`.
It was interrupted after 843 passes and nine skips. That is also a non-pass;
partial counts from either attempt may not be pooled with a retry.

All stopped database windows restored the exact pre-gate database and schema
inventories, with zero remaining competing sessions, locks, or orphan schema.

## 3. Exact repair

The repair changes only the ten integer `line` fields below. The checked
eight-line windows and their evidence locations are:

| Finding | New anchor | Checked window | Evidence line |
|---|---:|---:|---:|
| `M4-RUNTIME-READ-001` | 768 | 768-775 | 771 |
| `M4-RUNTIME-READ-002` | 785 | 785-792 | 791 |
| `M4-RUNTIME-READ-003` | 863 | 863-870 | 864 |
| `M4-RUNTIME-READ-004` | 1149 | 1149-1156 | 1151 |
| `M4-RUNTIME-READ-005` | 1181 | 1181-1188 | 1184 |
| `M4-RUNTIME-READ-006` | 2026 | 2026-2033 | 2027 |
| `M4-RUNTIME-READ-007` | 4282 | 4282-4289 | 4283 |
| `M4-RUNTIME-READ-008` | 5644 | 5644-5651 | 5645 |
| `M4-RUNTIME-WRITE-001` | 5675 | 5675-5682 | 5676 |
| `M4-RUNTIME-WRITE-002` | 5684 | 5684-5691 | 5686 |

No table, kind, context, evidence string, array membership, status, or other
value may change. The expected inventory identity after exactly this edit is:

```text
SHA-256: 75bd97b53521b779e5de67591e3dee6a39be49b16ac9a9958d0cadbd8e6af3ef
Git blob: 4511c0538076f0e9493bb3e072ccea0ec86d000c
lines: 206
bytes: 9961
```

## 4. Ownership and topology

Commit this activation first as one new path on the exact required parent.
Obtain an independent read-only audit of its parent, tree, sole-path delta,
scope, and wording. Only then create:

```text
branch:   workstream/m5-4-02-04-inventory-location-repair
worktree: /home/kassym/Desktop/groundloop-worktrees/m5-4-02-04-inventory-location-repair
```

from that audited activation commit.

The repair lane owns exactly two paths:

1. `docs/workstreams/m5_postgres_oracle/m4_shared_surface_inventory.json`;
2. `docs/workstreams/m5_runtime_implementation/M5_4_02_04_INVENTORY_LOCATION_REPAIR_HANDOFF.md`.

The JSON may contain only the ten integer replacements in Section 3. The new
handoff records provenance, exact hashes and diff, executed commands and
results, inherited-evidence boundaries, failures, limitations, and requested
fast-forward. It cannot embed its own final hash or containing commit.

Forbidden changes include all source and test files, migrations, schemas,
package exports, the earlier activation and lane handoffs, M4 patch-plan and
handoff documents, status/roadmap/decision documents, model artifacts,
datasets, secrets, database volumes, and protected user-owned files.

Required ancestry is:

```text
3200b39 activation -> b9d251b Lane A -> a41386f Lane B
    -> correction activation -> inventory repair
```

Do not rebase, squash, amend, or rewrite the three accepted commits.

## 5. Repair gates

Before committing the repair:

1. verify exact parent/manifest and an empty index;
2. run `git diff --check` on the two owned paths;
3. parse the JSON;
4. mechanically prove that only the ten named `line` scalar values changed;
5. prove every table occurs on its new anchor line and every evidence string
   occurs within the corresponding eight-line window;
6. run all inventory tests in
   `tests/m5/postgres/test_static_contract.py` and the exact previously failing
   node; and
7. collect the corrected complete suite and require exactly 2,219 tests.

These are no-database gates. Freeze and commit the exact two-path repair, then
obtain two independent same-byte audits with no unresolved P0/P1.

## 6. Carried-forward and remaining integration evidence

The following completed gates remain evidence on exact `a41386f`; do not
mislabel them as rerun on the repair commit:

- focused activity: 40 passed, 82 deselected, 77.17 seconds;
- complete recovery module: 122 passed, 205.68 seconds;
- D24 requirement directory: 171 passed, 303.01 seconds;
- migration 016: 200 passed, 270.42 seconds; and
- all PostgreSQL runtime: 797 passed, 1,224.13 seconds.

Every gate had fresh exact pre/post inventory equality and zero remaining
sessions, locks, or orphan schema. Those selections do not read the repaired
JSON, and all their source, test, migration, and configuration inputs remain
byte-identical. This is carried-forward dependency evidence, not a new run.

After the repair commit and its audits, fast-forward the integration branch
through both new commits and restart the complete 2,219-test suite from test 1
under one fail-closed guarded database window. Use the approved import-path
correction and process-only Git provenance overlay. Record exact result,
duration, skips, xfails, database inventories, and persisted Dynagox config
equality. No partial prior result may be reused.

If the complete suite passes, release the database window and run the literal
14-node public-M4/legacy compatibility selection. Then obtain two independent
same-byte integrated audits covering ancestry, all manifests, carried-forward
dependencies, the fresh complete-suite result, compatibility, failures, and
claim boundaries.

Only after both audits are GO may the coordinator fast-forward `main`, while
preserving all five protected local paths, and rerun the focused pure and
focused PostgreSQL selections on `main`. Status reconciliation remains a
separate six-document tranche based on the final technical `main` head.

## 7. Stop conditions

Stop immediately on any need to change a source/test file, any additional JSON
field or path, any semantic mismatch in an eight-line window, any candidate
byte drift, a non-local or ambiguous database target, a competing database
session or lock, an orphan not proven to belong to the current run, a test
failure, a changed persisted Dynagox remote/configuration, or any request to
promote an additional M5 row.

This activation grants no production deployment, provider/model execution,
M5-D25 acceptance, migration 017, runtime-mode change, status-row promotion,
or publication claim.
