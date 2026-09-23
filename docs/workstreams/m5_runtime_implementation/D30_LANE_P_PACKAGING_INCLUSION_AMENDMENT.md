# M5-D30 Lane-P Packaging-Inclusion Amendment

Status: docs-only path-authority candidate; it authorizes no repository edit,
implementation verdict, next lane, runtime-mode change, deployment claim, or
AI-quality claim until these exact bytes receive two independent same-byte
`GO`, `P0=0`, `P1=0` reviews, are committed as the sole changed path, receive
two independent post-commit identity confirmations, and are pushed to this
branch and `origin/main`

Date: 2026-09-23

## 1. Exact authority barrier

```text
required_parent = dc8601707629dd313a5d1d41d0adb84c5acc810e
required_parent_tree = 80c256a038458f90a2ade5afbf83c452b1fd9bc6
required_origin_main = dc8601707629dd313a5d1d41d0adb84c5acc810e

d30_amendment_sha256 = db2568affc02cf1ca6f17a549029f31089cecd857debdedf2651e6aac6898fe4
d30_freeze_handoff_sha256 = 546bf38719e1e3e3391739c3484e541b25468349204f60847e39ddf5771b90ee
d30_activation_sha256 = a39b8aace948ab06e70f8bd2adfe5a6251ae282a87bcc49fff421081be0e6be4
d30_custody_amendment_sha256 = 0b65eeaca63a708a5a3d67ec543704d1c7b09ffa93b55d5b9e4c6d634c0d1fd0
d30_sequencing_amendment_sha256 = df06b98fc876d20dd66d94ac61db051d85a96867f7d4c8d542b5b915d6d79512
base_gitignore_sha256 = a2042ba8faca1313fd4b8d17473ac2aa6ef8c0e2b5ae27c209e8229714168911
base_pyproject_sha256 = f5dc5e99a74731eafa97a365e8e3c40f40c08dd960a7b7d520a9582c2c459010
migration_018_sha256 = 941bba975c12e9fb5ba4b4f75a82e59fa518b23eac34468ed2f8b15d1cd9ed90

candidate_branch = workstream/m5-d30-lanep-packaging-authority
candidate_worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d30-lanep-packaging-authority
candidate_changed_path_count = 1
runtime_mode = v1_only
```

The sole owned path before acceptance is this file. Its commit must be a
one-file linear child of the exact pushed sequencing amendment. Any other
edit, parent, or intervening commit restarts both reviews. The active Lane-P
worktree, protected local main, source, tests, `.gitignore`, `pyproject.toml`,
migrations and generated artifacts remain read-only during this docs gate.

This amendment corrects one build-inclusion boundary. It does not amend an M5
semantic, SQL route, schema, migration, digest, result byte, counter, timing
coordinate, public API, provider, model, prompt, calibration, or claim ceiling.

## 2. Confirmed packaging defect

The repository root `.gitignore` has, since the initial baseline, contained:

```text
models/
```

Hatchling 1.32.4 applies that unanchored directory rule while selecting wheel
and sdist members. It therefore omits these six tracked package files even
though `pyproject.toml` declares `packages = ["src/groundloop"]`:

```text
src/groundloop/m4/models/__init__.py
src/groundloop/m4/models/config.py
src/groundloop/m4/models/contracts.py
src/groundloop/m4/models/embedding.py
src/groundloop/m4/models/ports.py
src/groundloop/m4/models/verification.py
```

Their base-byte manifest is:

```text
920b2c67aa4d6d2cf079e32a31217c45ffe94e2f0dc65b1fedff678fc1b577c9  src/groundloop/m4/models/__init__.py
afdffe8faf7e1d96f7d85d6fd786c2d7245f93dda604e5d71693e309936fa91f  src/groundloop/m4/models/config.py
00ec5ad0c6ebe2845a6ddd936f2ad478a9765609eb0d53ba026cba9aab117eeb  src/groundloop/m4/models/contracts.py
8339fd23857a7c5ea44a13dca1ca0623c1bcc6558040f3cac6231682d6f10d92  src/groundloop/m4/models/embedding.py
b1f43a603972935230afbcc416898c605932ebb90f690e7bf00f92821f0cd339  src/groundloop/m4/models/ports.py
5e5c93091b5abd972828fa6fdebe8b48a166d51efb433b7edac5262c34201e08  src/groundloop/m4/models/verification.py
```

The six LF-terminated rows above have SHA-256
`72cbf9f51e75654cb5ff497cca57d4204b38da000c89971b8545002ad09751b3`.
No file in that set is authorized to change.

The current Lane-P planner imports `PairVerificationArtifact`,
`PairVerificationInput`, `decision_policy_hash`, `derive_operational_label`,
and `verification_artifact_payload_hash` from the tracked contracts and ports
modules. A repository-configured wheel contains both Lane-P modules but only
145 members; after installation with declared runtime dependencies, importing
either module fails exactly with:

```text
ModuleNotFoundError: No module named 'groundloop.m4.models'
```

The base wheel also omitted those six files, but the base planner did not
import them. Lane P therefore cannot honestly pass the activation's packaging
regression on its present path authority. A temporary build override, source
overlay, copied contract, lazy failure, or qualified `PASS` would conceal the
defect rather than repair it.

## 3. Exact narrow repair

After this amendment is accepted and enters Lane-P ancestry, Lane P gains
write authority for exactly one additional implementation path:

```text
.gitignore
```

The only authorized byte change in that file is to add these eight lines
immediately after the existing `models/` line:

```text
!/src/groundloop/m4/models/
/src/groundloop/m4/models/*
!/src/groundloop/m4/models/__init__.py
!/src/groundloop/m4/models/config.py
!/src/groundloop/m4/models/contracts.py
!/src/groundloop/m4/models/embedding.py
!/src/groundloop/m4/models/ports.py
!/src/groundloop/m4/models/verification.py
```

The existing unanchored `models/` rule remains present. This is deliberately
narrower than replacing it with `/models/` or unignoring the complete source
directory: top-level local model data, other nested `models/` directories and
every non-Python member of this source directory remain ignored, while the
six exact tracked Python files needed by the installed planner are re-included.
An arbitrary additional Python file remains ignored. No
`pyproject.toml`, source, test, migration, installer, contract, or public
configuration change is authorized.

The Lane-P candidate path count becomes exactly 25: the previously authorized
24 paths plus `.gitignore`. Its final handoff must record `.gitignore` base,
index and candidate mode/blob/byte/SHA identities separately; the frozen
23-file technical source/test manifest remains unchanged.

## 4. Mandatory repair falsifiers

The repaired final Lane-P bytes must prove all of the following on one clean
snapshot using the unchanged `pyproject.toml` and build backend plus only the
authorized `.gitignore` repair:

1. `.gitignore` differs from the base by only the eight exact added lines;
2. the root/local `models/` exclusion and ignored weight extensions remain
   effective;
3. the wheel and sdist model-member sets equal exactly the six tracked
   `src/groundloop/m4/models/*.py` files in Section 2, each with byte identity
   equal to that section, and contain no seventh model member;
4. the wheel contains both
   `groundloop/m5/runtime/postgres_matching.py` and
   `groundloop/m5/runtime/postgres_withdrawal.py`;
5. arbitrary Python and non-Python sentinels under
   `src/groundloop/m4/models/` are neither wheel nor sdist members, and no
   test, cache, weight, report, database volume or secret is a wheel member;
6. an isolated environment installs the wheel with its declared runtime
   dependencies and imports both Lane-P modules from `site-packages` with no
   source-tree or `PYTHONPATH` overlay;
7. a second isolated process repeats both imports;
8. wheel metadata, console entry point and package version remain unchanged;
9. wheel and sdist builds complete without creating a new tracked or
   non-ignored artifact or cache in either protected checkout or Lane P; and
10. all existing Lane-P static, complete-suite, migration, database-inventory,
    protected-state and claim-ceiling gates remain exact.

A disposable proof of the eight-line repair produced a 151-member wheel rather
than the defective 145-member wheel and made both isolated imports pass. That
probe is diagnostic only: its artifact hashes do not qualify the final
candidate. The final wheel identity may be recorded in the handoff. Because
the final sdist contains the handoff that describes the build, its archive
SHA-256 must be recorded externally after handoff byte freeze; member counts
and required-member checks remain mandatory before acceptance.

Any missing model-package member, extra `.gitignore` edit, import from the
source checkout, optional-ML download used to mask a base dependency error, or
build from altered `pyproject.toml` fails this gate.

## 5. One-time authority fast-forward for the active dirty lane

This authority file is disjoint from every active Lane-P path. After these
exact bytes are reviewed, committed, post-commit checked, and pushed to this
branch and `origin/main`, the coordinator must stop every Lane-P writer and
record:

1. the exact physical worktree, branch, current HEAD/tree, empty staged delta,
   and exact pushed amendment commit/tree;
2. raw porcelain-v2-z SHA-256 and record count;
3. all 24 current dirty/untracked paths' kinds, base/index modes or absence,
   prospective modes, live modes, byte counts and SHA-256 values;
4. a complete current-HEAD-to-amendment name/status/mode ledger proving that
   this file is the sole intervening path and is disjoint from the 24 paths;
5. exact protected-main identity; and
6. absence of test, formatter, editor, Git or other writer processes touching
   either checkout.

Only then may the active worktree execute exactly:

```text
git merge --ff-only <exact-pushed-packaging-amendment-tip>
```

Immediately afterward it must prove branch/HEAD/tip equality, an empty staged
delta, unchanged raw status identity, and unchanged kinds, modes, sizes and
SHA-256 values for all 24 pre-existing paths. Any mismatch aborts. No merge
commit, rebase, stash, reset, checkout overwrite, copy or path edit is
authorized by this section. Only after that custody check may Lane P add the
eight authorized `.gitignore` lines.

## 6. Audits and non-change ceiling

Two independent reviewers must audit identical candidate bytes for defect
reproduction, minimum repair, package executability, path exclusivity, custody
safety and claim limits. After commit, two independent checks must confirm the
exact parent, sole path, mode, blob, SHA-256, tree, clean docs worktree and
equality with the reviewed bytes. Only then may the commit be pushed and the
Section-5 fast-forward occur.

This amendment does not accept Lane P or any implementation. After this repair
and every other Lane-P-owned gate pass, the Lane-P handoff labels remain
exactly:

```text
matching_planner_repair = PASS
cross_layer_publication_races = DEFERRED_TO_LANE_D_AND_LANE_I
whole_route_output_nonchange = DEFERRED_TO_LANE_D_AND_LANE_I
M5-D30 implementation = PENDING
Task 2 = PENDING
runtime_mode = v1_only
```

It authorizes no migration 019 and preserves migrations 001--018 exactly.
M5-D24 through M5-D30 and M5.0-24 through M5.0-30 remain
`implementation-PENDING`. Task 2, M5.4-05 through M5.4-09, M5.5, M5.6,
deployment, performance, utility, security, objective-truth, novelty,
maintained-history, named-system-superiority and AI-quality claims remain
`PENDING`.
