# M5-D30 Task-2 Custody-Mode Amendment

Status: docs-only activation correction candidate; the held implementation
worktree remains read-only and unswitched until this exact file receives two
independent same-byte `GO`, `P0=0`, `P1=0` reviews, is committed, receives two
post-commit identity confirmations, and is pushed to its branch and
`origin/main`

Date: 2026-09-22

## 1. Exact correction barrier

```text
required_parent = 0dc87437475047cfad01e927af013338b60bdce1
required_parent_tree = af82fea3afa9e0e63e40bb2466c1f7bb57dff627
required_origin_main = 0dc87437475047cfad01e927af013338b60bdce1

activation_path = docs/workstreams/m5_runtime_implementation/D30_TASK2_STORE_RUNTIME_ACTIVATION.md
activation_sha256 = a39b8aace948ab06e70f8bd2adfe5a6251ae282a87bcc49fff421081be0e6be4
activation_lines = 490
activation_bytes = 25038

correction_branch = workstream/m5-d30-task2-custody-mode-amendment
correction_worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d30-task2-custody-mode-amendment
correction_changed_path_count = 1
runtime_mode = v1_only
```

This correction changes no D30 semantic, lane, path grant, dependency,
implementation byte, test, schema, migration, API, result, counter, or claim
ceiling. It corrects only the interpretation and recording of held-worktree
file modes before the one-time branch move.

The sole owned path is this file. Its commit must be a one-file linear child
of the reviewed activation commit. Any other edit or intervening commit
restarts both reviews.

## 2. Confirmed custody-record ambiguity

Section 3 of the accepted activation records one `Mode` value of `100644` for
each held path. For a tracked path, that value is the exact Git index/base blob
mode. For an untracked path it is the prospective Git non-executable blob mode
if that path is later accepted and added. It is not the live filesystem
permission mode.

The final pre-switch custody check correctly inspected the physical worktree
and found that all fifteen live files have filesystem permission mode `0664`
(`-rw-rw-r--`). No content, path kind, index entry, branch, or Git executable
bit changed. Git's raw porcelain does not encode the group-write permission,
so the recorded raw-status SHA-256 remains exact while the separate
filesystem-mode field was missing.

The branch move did not run. The held worktree remains:

```text
held_worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d29-matching-planner
held_branch = workstream/m5-d29-matching-planner
held_head = 8734162f8578ac3105119789a8729fc662da575d
held_tree = c821f388ec77fb5415d99bb2f39af378ddb985e2
held_index = exactly HEAD
held_path_count = 15
held_raw_porcelain_v2_z_sha256 = b5c1b18d82215e4879abb729d4e7149c132319f49c1dd169a27795cc961d29ff
destination_branch = workstream/m5-d30-matching-planner
destination_branch_local = absent
destination_branch_remote = absent
```

## 3. Exact mode ledger

The accepted activation's path kind, base blob, byte count and SHA-256 remain
exact. This amendment supplies the missing distinction:

| Path | Kind | Base/index Git mode | Prospective Git mode if untracked | Live filesystem mode |
|---|---|---:|---:|---:|
| `src/groundloop/m5/runtime/postgres_matching.py` | tracked `.M` | `100644` | n/a | `0664` |
| `src/groundloop/m5/runtime/postgres_withdrawal.py` | untracked `??` | absent | `100644` | `0664` |
| `tests/m5/postgres_runtime/d25_store_core/conftest.py` | tracked `.M` | `100644` | n/a | `0664` |
| `tests/m5/postgres_runtime/d25_store_core/test_d27_counter_ownership.py` | untracked `??` | absent | `100644` | `0664` |
| `tests/m5/postgres_runtime/d25_store_core/test_d28_phased_composition.py` | untracked `??` | absent | `100644` | `0664` |
| `tests/m5/postgres_runtime/d25_store_core/test_nonempty_planner.py` | untracked `??` | absent | `100644` | `0664` |
| `tests/m5/postgres_runtime/d25_store_core/test_points.py` | tracked `.M` | `100644` | n/a | `0664` |
| `tests/m5/postgres_runtime/d25_store_core/test_replay_work.py` | tracked `.M` | `100644` | n/a | `0664` |
| `tests/m5/postgres_runtime/d25_store_core/test_transition_apply.py` | tracked `.M` | `100644` | n/a | `0664` |
| `tests/m5/postgres_runtime/d29_store/conftest.py` | untracked `??` | absent | `100644` | `0664` |
| `tests/m5/postgres_runtime/d29_store/test_bounded_withdrawal.py` | untracked `??` | absent | `100644` | `0664` |
| `tests/m5/postgres_runtime/d29_store/test_query_plans.py` | untracked `??` | absent | `100644` | `0664` |
| `tests/m5/postgres_runtime/d29_store/test_replay_and_reservation.py` | untracked `??` | absent | `100644` | `0664` |
| `tests/m5/postgres_runtime/d29_store/test_retained_declarations.py` | untracked `??` | absent | `100644` | `0664` |
| `tests/m5/postgres_runtime/d29_store/test_source_identity.py` | untracked `??` | absent | `100644` | `0664` |

`100644` and `0664` describe different layers. Git records regular,
non-executable blob mode `100644`; the shared filesystem currently grants
group write and reports permission bits `0664`. Neither value authorizes a
permission change. The one-time move must preserve both the tracked Git modes
and every live filesystem mode exactly.

## 4. Corrected one-time custody gate

This amendment narrowly supersedes the accepted activation's Section 4
switch-target and post-switch HEAD/tip clauses, and Section 8 items 6--8 only
where they name the earlier activation commit as the terminal branch-move
target. The exact reviewed and pushed correction commit becomes the required
switch target and post-switch HEAD/tip. The coordinator must still record and
validate the pushed D30 freeze commit, original activation commit, and this
correction commit separately. Every other original custody, audit, path,
ordering, and push requirement remains in force.

The accepted activation remains authoritative except that every phrase
requiring a held-entry `mode` must now record and compare both applicable
fields from Section 3:

1. tracked base/index Git mode, or explicit absence for an untracked path;
2. prospective Git mode for an untracked path, recorded as expectation only;
3. live filesystem permission mode for every present path; and
4. after the branch move, the same three fields with no unexplained change.

Before the switch, the coordinator must revalidate the original activation's
complete 15-path kind/blob/size/hash ledger, this exact mode ledger, the raw
porcelain bytes/hash, the empty index, the complete held-HEAD-to-corrected-
activation name/status/mode ledger, protected local-main identity, absence of
the destination branch locally/remotely, exact pushed freeze/activation/
correction tips, and absence of writer processes.

Only then may the same physical held worktree run exactly:

```text
git switch -c workstream/m5-d30-matching-planner <exact-pushed-correction-tip>
```

Immediately afterward it must prove exact HEAD/tip equality, an empty staged
delta, identical raw porcelain bytes/hash, identical path kinds, base/index
Git modes or absence, prospective Git modes, live filesystem modes, byte
counts and SHA-256 values. Any mismatch aborts before an implementation edit.

## 5. Audit and non-change ceiling

Two independent reviewers must verify identical candidate bytes, all fifteen
live filesystem modes, the original activation ledger, branch absence,
base-to-tip disjointness and all frozen claim ceilings. After commit, both
must confirm committed bytes equal reviewed bytes. The correction branch and
exact reviewed commit must be pushed, and `origin/main` must fast-forward to
that commit before the custody move.

This amendment accepts no implementation. M5-D24 through M5-D30,
M5.0-24 through M5.0-30, Task 2, M5.4-05 through M5.4-09, M5.5 and M5.6 remain
implementation-`PENDING`; runtime remains `v1_only` outside isolated
fixtures. Deployment, performance, utility, security, objective-truth,
novelty, maintained-history, named-system-superiority and AI/model-quality
claims remain `PENDING`.
