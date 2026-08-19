# M5-D24 R2e PostgreSQL Typed-Direct Bridge Handoff

Status: **R2e scoped typed-direct pre-seal bridge candidate PASS**;
M5.0-24 remains contract-`PASS` / implementation-`PENDING`, M5.4 remains
partial, and integration, status reconciliation, deployment and human approval
remain separate gates

Date: 2026-08-19

## 1. Frozen authority, branch and worktree

This candidate implements only the path-exclusive R2e tranche authorized by
the accepted C7 correction and its committed activation:

```text
branch: workstream/m5-d24-r2e-postgres-typed-direct-bridge
worktree: /home/kassym/Desktop/groundloop-worktrees/m5-d24-r2e-postgres-typed-direct-bridge
activation base/current pre-candidate HEAD: 3630f444ed4ff5b3ffe312926aa7aaa77c801d02
activation commit sole parent: a44be2a13e688638ea9ff9183e530073f007b2f5
accepted C7 commit sole parent: 254e9c27b0dfc74df1e02ba2d8cd04c7fa9a2c6a
```

The authoritative bytes are:

- accepted C7,
  `docs/workstreams/m5_runtime_contract/DIRECT_ACQUISITION_TERMINAL_CUTOFF_CORRECTION.md`:
  `e76b36d15092a47ef531968cdd6689d6adaf9cf10862e8e997759b59e613e15c`;
- activation,
  `docs/workstreams/m5_runtime_implementation/D24_R2E_POSTGRES_TYPED_DIRECT_BRIDGE_ACTIVATION.md`:
  `d19b19d5db6471c6483dbd2bddc089f5862bbe6ae01a3a5a893ec20f610f43c1`;
  and
- the activation commit:
  `3630f444ed4ff5b3ffe312926aa7aaa77c801d02`.

The branch and persistent worktree were created at that exact activation
commit. The lane did not move, rebase, merge later main state, use a temporary
worktree, or expand beyond the exact 21-path manifest.

## 2. Exact candidate manifest and SHA-256 pins

Relative to the activation base, the candidate changes exactly the following
21 paths. Paths 1--20 are internally pinned at the final same bytes audited and
tested:

1. `src/groundloop/m4/persistence.py`
   `3e7acb64dd4d86c9ae33e73a23d0ae0827c075e541d03accf55e088358ed6730`
2. `src/groundloop/m4/pipeline.py`
   `22c035139fdc57160980c9ddf387ae31dff5b4fbe965d9b48e392db8d63e5b84`
3. `src/groundloop/m5/runtime/contracts.py`
   `64a2b66adc4bd3d5a63eb1d48e454a4559fc5667caeb7cbfebe111dd5825d0b8`
4. `src/groundloop/m5/runtime/application.py`
   `83db643bb7cb748e6492b3f80f828ae75910a2f909c05d902052cda03f2daebb`
5. `src/groundloop/m5/runtime/direct_m4.py`
   `e018fec8e99c53233087f7c16ac36e23d959f538ccaa7e251b920b7d8b029651`
6. `src/groundloop/m5/runtime/persistence.py`
   `dcd1c1723c798260281b2cd1d933fd8e8c4febad78fe7ea48760b0069a16e480`
7. `src/groundloop/m5/runtime/postgres_direct_recovery.py`
   `0b6a065d76e1f4b304e59c179151b506843d6467ea758998d951c011078e9b4e`
8. `src/groundloop/m5/runtime/postgres_recovery.py`
   `c30b320edbd63986d0f9cc179b807626349a7ac82438eb9634e8af022030d7b3`
9. `src/groundloop/m5/runtime/postgres_direct_application.py` (new)
   `e321f2ef0cf5a40663169b44eace00457be612c39a86f3a819be29820d8c21e5`
10. `src/groundloop/m5/runtime/postgres_application.py`
    `b40808940bf5dd769cc0a35230d3177ae2fe1050873ece9022e8bd94d7c09c5d`
11. `src/groundloop/m5/runtime/postgres_roots.py`
    `bb6a3d2fa2ce54f2a95ae849ad0d00d631fb9dbe4eeacbf90eaa1db5db832ba7`
12. `tests/m5/runtime/fake_ports.py`
    `2b3893318312a5698e16d2cf1b7320a33ef2bd7fae251fe028509e527a020145`
13. `tests/m5/runtime/test_contracts.py`
    `7e048222cc5361fe8356fc2206d59d9c108f1352ae7741cb7c4d65d51143c11f`
14. `tests/m5/runtime/test_d24_application_composition.py`
    `b8a2e617fec10dabb9750c26ea57834ccd9899e0bf0ddcd54effc4ea602c5361`
15. `tests/m5/postgres_runtime/d24_application/test_group_requirement_composition.py`
    `2477f5f42dced2b645e6f8240b20eafc2194db9a57657f3b8dadcc609c6c1231`
16. `tests/m5/postgres_runtime/d24_application/test_group_requirement_races.py`
    `559c7a0ee229b9be1be5a0667143a140cbad581cf016fc04b2d687de96f090f9`
17. `tests/m5/postgres_runtime/d24_direct_application/conftest.py` (new)
    `af1614b6738154065a3d719c00ab6dd05f85cdb92754f10245730f6de63bee02`
18. `tests/m5/postgres_runtime/d24_direct_application/test_typed_direct_composition.py`
    (new)
    `13ff3cb62197441fcbc44b1e5e65d26c685722568e71bb3f5ea105dcd566e825`
19. `tests/m5/postgres_runtime/d24_direct_application/test_typed_direct_races.py`
    (new)
    `56b5b56d41e0e51dc232041c56f7fba43925147b5850f99852da6e3854c382c2`
20. `tests/m5/postgres_runtime/test_direct_m4_composition.py`
    `9f1b885d911b1441c31001de222bb4295e6e64505928db6bc50f482e3c6bc5a8`
21. `docs/workstreams/m5_runtime_implementation/D24_R2E_POSTGRES_TYPED_DIRECT_BRIDGE_HANDOFF.md`
    (this new handoff)

The 20 internally pinned paths total 63,425 lines and 2,450,665 bytes. Their
manifest-ordered SHA ledger has SHA-256
`42c50e79c54e72c521abafb60a30a838206afb29d78c43eb9aacc1d45e4f7cf9`.
This ledger is the second GNU `sha256sum` over the exact first `sha256sum`
output for paths 1--20 in manifest order, where each inner line is
`<64hex><two spaces><relative path><LF>`.

Path 21 deliberately does not contain a placeholder, stale value, or claim to
pin its own final SHA-256. A file cannot contain its own content hash without
a self-reference problem. Its final SHA-256 and line count are measured
externally after the document is frozen, reported to the coordinator, and then
made immutable by the candidate commit/tree and its post-commit audit.

The exact activation-base-to-candidate name-status is:

```text
M  src/groundloop/m4/persistence.py
M  src/groundloop/m4/pipeline.py
M  src/groundloop/m5/runtime/contracts.py
M  src/groundloop/m5/runtime/application.py
M  src/groundloop/m5/runtime/direct_m4.py
M  src/groundloop/m5/runtime/persistence.py
M  src/groundloop/m5/runtime/postgres_direct_recovery.py
M  src/groundloop/m5/runtime/postgres_recovery.py
A  src/groundloop/m5/runtime/postgres_direct_application.py
M  src/groundloop/m5/runtime/postgres_application.py
M  src/groundloop/m5/runtime/postgres_roots.py
M  tests/m5/runtime/fake_ports.py
M  tests/m5/runtime/test_contracts.py
M  tests/m5/runtime/test_d24_application_composition.py
M  tests/m5/postgres_runtime/d24_application/test_group_requirement_composition.py
M  tests/m5/postgres_runtime/d24_application/test_group_requirement_races.py
A  tests/m5/postgres_runtime/d24_direct_application/conftest.py
A  tests/m5/postgres_runtime/d24_direct_application/test_typed_direct_composition.py
A  tests/m5/postgres_runtime/d24_direct_application/test_typed_direct_races.py
M  tests/m5/postgres_runtime/test_direct_m4_composition.py
A  docs/workstreams/m5_runtime_implementation/D24_R2E_POSTGRES_TYPED_DIRECT_BRIDGE_HANDOFF.md
```

No owned file was staged while this handoff was authored. `git diff --check`
was clean and the index was empty. Ignored test artifacts were inventoried as
21 cache directories and 131 files: 98 `.pyc`, 19 mypy-cache, 5 pytest-cache
and 9 Ruff-cache files. None was staged or nonignored candidate content.

## 3. Implemented scoped semantics

### 3.1 Exact contracts and held application authority

The implementation adds and recursively validates the exact immutable
`M5TypedDirectAcquisitionReceipt` and
`M5CheckedDirectTerminalFailureReceipt`. It extends the application-local
`M5DirectExecutionReceipt` with the two mutually exclusive C7 provenance
branches and admits `WORK_IN_PROGRESS` only as the required nonterminal
blocked outcome. Exact concrete types, primitive types, nested DTOs,
deterministic M4 attempt/token recipes, epoch/job/attempt/revision/work
bindings, and illegal mixed shapes are all checked before authority is used.

`run_pending_direct` now requires the caller-held, exact nonterminal
`OpenEventReceipt`. The pure application, the production typed-direct adapter,
and the group-only facade preserve its actual fresh/resumed flag. There is no
default, reconstruction, cached receipt, no-receipt production fallback, or
M4 terminal-state/reason conversion into an M5 failure reason. Active-cutoff
projection requires the accepted canonical same-event, same-payload,
same-epoch `FAILED` result and one exact C5/C6/C7 provenance origin.

Application transition-timing append is bound to a snapshot of the exact
transition anchor, a separately copied callback input, callback immutability,
the returned timing observation, and the actual append receipt/revision. The
four nonterminal timing families are covered: new/takeover direct acquisition,
first retryable direct execution, normal direct transition, and preterminal
late return. Live/terminal acquisition, exact retryable replay, postterminal
return and the combined terminal epoch-failure path do not invent a second
anchor.

### 3.2 Transaction-owned direct acquisition and provider boundary

The new PostgreSQL typed-direct facade owns tokenless acquisition from the
transaction that locks the job, samples database time, chooses the exact
dense attempt ordinal, and creates or reads the deterministic M4 attempt and
lease token. It returns the exact job, lease and persisted attempt or `None`.
The existing caller-token cursor method remains bounded checked compatibility;
the production path does not pre-read an ordinal, guess or retry a token, or
parse an exception to discover the selected attempt.

New/takeover acquisition advances exactly once; live and terminal acquisition
are zero-write and return the durable current revision. Exact `LIVE_LEASE` and
`expired_preterminal` stop the invocation as
`BLOCKED/WORK_IN_PROGRESS` without provider redispatch or seal, preserving
earlier invocation work and adding no fabricated acquisition work/timing.

Provider calls occur outside PostgreSQL transactions. Before each discovery
or verifier call, the adapter reconstructs a trusted operational acquisition,
document event, direct plan, requirement registry snapshot and active-chunk
snapshot, then passes separate recursively reconstructed callback copies.
After return it proves that the callback inputs still equal the trusted
authority. Returned execution, work, timing, envelope and verification values
are also reconstructed before later providers or settlement, closing nested
mutation and retained-object time-of-check/time-of-use routes.

Successful normal and late returns are bound to their exact
epoch/job/attempt/envelope, artifact/completion identity, requested verifier
effectiveness, and deterministic attempt-execution evidence. Receipt
revisions must follow the exact input/current/one-advance laws before timing,
wrapper construction, another mutation, or return.

### 3.3 Checked combined failure, cooperative locking and total closure

Every nonretryable production direct failure uses the checked combined outer
operation. The private M4 two-phase cursor-local helpers first lock the
complete C-ordered tier-9 direct-job set, then cooperate with the split M5
job/detail planning phases through the frozen tier order. Exact concrete,
cursor-local plans are revalidated and consumed once. No target or
cancellation write occurs before the complete lock set, no M4 state SQL is
copied into M5, and no nested independently committing operation is opened.

For a first checked failure at revision `N`, the target attempt/job,
execution evidence, work/timing contribution, every other open direct-job
cancellation and its `CANCELLED/epoch_failed` projection, typed epoch failure,
event result, cache decision and accumulators commit atomically at exactly
`N+1`. There is exactly one existing `m4-evaluation-failure-v1` `FAIL`
transition and only the `EPOCH_FAILURE` timing anchor; no target terminal-
failure DELTA or independently visible direct-terminal/nonterminal-epoch
revision exists.

The target-absent exact-held generic failure path cancels every open direct
job under the same cooperative closure. The fused timing finalizer applies an
optional direct-attempt observation, the prior pending-anchor missing point,
the terminal `EPOCH_FAILURE` missing point and anchor clear in one update. It
does not perform a second direct accumulator CAS or double-account direct
attempt work.

Exact checked replay validates the complete durable direct settlement,
requested typed reason, cancellation image and canonical failed result and
performs zero writes. The serialized loser is admitted only as exact
zero-write `CANCELLED/epoch_failed` terminal acquisition with the latest
unchanged leased attempt/token/dispatch, absent execution evidence and the
canonical failed result. Its requested direct reason and requested M5 reason
are not required to match the winning failure. Ambiguous, evidence-bearing,
stale, replaced, expired, nonlatest, malformed or mixed loser/replay images
conflict.

Standalone committed direct terminal failure is rejected. Ordinary terminal
reconnect remains canonical and zero-work. Successful discovery/verifier
normal and late paths retain the R2d active-cutoff rules, and a terminal
measurement/telemetry append occurs only after complete envelope validation.

## 4. Final executable evidence

All PostgreSQL commands ran serially from the persistent worktree, with
`GROUNDLOOP_TEST_DATABASE_URL` set to the local Compose test database and each
test using a unique disposable schema. Credential values were not printed or
recorded. The persistent production namespace `groundloop` was not a test
schema.

After the final path-20 compatibility repin, the exact focused selection was
rerun and passed **105/105** in **160.01s**, with zero
skip/xfail/failure:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider --import-mode=importlib \
  -q -ra --tb=short \
  tests/m5/postgres_runtime/d24_direct_application/test_typed_direct_composition.py \
  tests/m5/postgres_runtime/d24_direct_application/test_typed_direct_races.py::test_elapsed_current_lease_can_commit_checked_first_failure \
  tests/m5/postgres_runtime/test_direct_m4_composition.py::test_d24_atomic_retryable_failure_replay_has_no_second_anchor
```

The complete pure runtime suite passed **390/390** in **2.19s**:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider -q -ra --tb=short \
  tests/m5/runtime
```

The complete typed-direct application suite passed **172/172** in
**280.05s**:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider --import-mode=importlib \
  -q -ra --tb=short \
  tests/m5/postgres_runtime/d24_direct_application
```

The complete group application and D24 requirement suites passed **104/104**
in **160.28s** and **163/163** in **300.05s**, respectively:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider --import-mode=importlib \
  -q -ra --tb=short \
  tests/m5/postgres_runtime/d24_application

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider --import-mode=importlib \
  -q -ra --tb=short \
  tests/m5/postgres_runtime/d24_requirement
```

The ordered direct-M4/migration-016 compatibility selection passed
**218/218** in **297.24s** after the bounded compatibility-helper correction
described in Section 5:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider --import-mode=importlib \
  -q -ra --tb=short \
  tests/m5/postgres_runtime/test_direct_m4_composition.py \
  tests/m5/postgres_runtime/test_migration_016.py
```

The corrected full PostgreSQL runtime suite then passed **789/789** in
**1250.68s** in one serial import-isolated run:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider --import-mode=importlib \
  -q -ra --tb=short \
  tests/m5/postgres_runtime
```

The typed M4 barrier, direct-M4 composition and legacy D24-direct route
selection passed **53/53** in **80.40s**:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider -q -ra --tb=short \
  tests/m5/postgres_runtime/test_m4_typed_barrier.py \
  tests/m5/postgres_runtime/test_direct_m4_composition.py \
  tests/m5/postgres_runtime/d24_direct
```

The public-M4 DTO/API signature snapshot, legacy reference and M4 contract
selection passed **14/14** in **0.15s**:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider -q -rs --tb=short \
  tests/m5/runtime/test_contracts.py::test_d24_public_m4_dto_and_api_signature_snapshot_is_unchanged \
  tests/m5/reference/test_legacy_regression.py \
  tests/m4/test_m4_contracts.py
```

The migration-015/016 exact install, ledger, replay and rollback regression
selection passed **226/226** in **303.33s**:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider --import-mode=importlib \
  -q -ra --tb=short \
  tests/m5/postgres_runtime/test_migration_015.py \
  tests/m5/postgres_runtime/test_migration_016.py
```

All final passing selections above reported zero skipped, zero xfailed and
zero failed tests.

## 5. Disclosed intermediate full-suite failure and bounded fix

The first full PostgreSQL run after the final focused changes did **not** pass:
it completed with **684 passed and 105 failed**. The 105 failures shared one
import-order compatibility defect: path 20's `_OpenedDirectEpoch` helper
already had a newly required eighth `open_receipt` field, while migration-016's
legacy seven-positional-argument construction omitted it under full-suite
module reuse. Every failure therefore raised the same missing-argument
`TypeError`.

The correction was bounded to manifested path 20. It made the existing field
`open_receipt: OpenEventReceipt | None = None`, preserving the legacy
seven-positional-argument construction. The sole R2e terminal consumer now
asserts that the receipt is non-`None` immediately before using it. The
existing R2e factory already supplied the real receipt with explicit keywords
and continues to do so. Path 20 was repinned to
`9f1b885d911b1441c31001de222bb4295e6e64505928db6bc50f482e3c6bc5a8`.
No production source, contract, schema, migration or test expectation was
weakened.

The ordered path-20/migration-016 run then passed **218/218**, and the complete
PostgreSQL suite was rerun from scratch and passed **789/789**. The earlier
684/105 result is retained here as failure evidence; it is not represented as
a passing gate.

## 6. Static, collection and repository hygiene

The exact static path groups were:

```bash
owned_source=(
  src/groundloop/m4/persistence.py
  src/groundloop/m4/pipeline.py
  src/groundloop/m5/runtime/contracts.py
  src/groundloop/m5/runtime/application.py
  src/groundloop/m5/runtime/direct_m4.py
  src/groundloop/m5/runtime/persistence.py
  src/groundloop/m5/runtime/postgres_direct_recovery.py
  src/groundloop/m5/runtime/postgres_recovery.py
  src/groundloop/m5/runtime/postgres_direct_application.py
  src/groundloop/m5/runtime/postgres_application.py
  src/groundloop/m5/runtime/postgres_roots.py
)
owned_pure=(
  tests/m5/runtime/fake_ports.py
  tests/m5/runtime/test_contracts.py
  tests/m5/runtime/test_d24_application_composition.py
)
owned_postgres=(
  tests/m5/postgres_runtime/d24_application/test_group_requirement_composition.py
  tests/m5/postgres_runtime/d24_application/test_group_requirement_races.py
  tests/m5/postgres_runtime/d24_direct_application/conftest.py
  tests/m5/postgres_runtime/d24_direct_application/test_typed_direct_composition.py
  tests/m5/postgres_runtime/d24_direct_application/test_typed_direct_races.py
  tests/m5/postgres_runtime/test_direct_m4_composition.py
)
owned=("${owned_source[@]}" "${owned_pure[@]}" "${owned_postgres[@]}")
```

The 20 owned Python source/test paths passed Ruff format-check and Ruff check:
**20/20 files** in each command:

```bash
/home/kassym/Desktop/groundloop/.venv/bin/python -m ruff format --check "${owned[@]}"
/home/kassym/Desktop/groundloop/.venv/bin/python -m ruff check --no-cache "${owned[@]}"
```

Strict mypy with explicit package bases passed all three exact commands:

```bash
MYPY_CACHE_DIR=/tmp/r2e_final_mypy_src MYPYPATH=src:. PYTHONPATH=src \
/home/kassym/Desktop/groundloop/.venv/bin/mypy \
  --strict --explicit-package-bases "${owned_source[@]}"

MYPY_CACHE_DIR=/tmp/r2e_final_mypy_pure_correct MYPYPATH=src:tests PYTHONPATH=src \
/home/kassym/Desktop/groundloop/.venv/bin/mypy \
  --strict --explicit-package-bases "${owned_pure[@]}"

MYPY_CACHE_DIR=/tmp/r2e_final_mypy_pg MYPYPATH=src:. PYTHONPATH=src \
/home/kassym/Desktop/groundloop/.venv/bin/mypy \
  --strict --explicit-package-bases "${owned_postgres[@]}"
```

The results were:

- **11/11** owned production source modules with `MYPYPATH=src:.`;
- **3/3** owned pure test modules with `MYPYPATH=src:tests`; and
- **6/6** owned PostgreSQL test modules with `MYPYPATH=src:.`.

Each group used `PYTHONPATH=src`, `--strict`, `--explicit-package-bases` and a
separate external `/tmp` mypy cache. No suppression was added.

An initial coordinator-only pure-test mypy invocation used the wrong module
root, `MYPYPATH=src:.`, and exited nonzero with import-resolution `Any` errors.
That was a command-layout error, not a passing gate. The repository's correct
`MYPYPATH=src:tests` command shown above was then run and passed **3/3**; the
two other exact strict commands also passed as reported.

Cache-isolated compilation passed **20/20**:

```bash
PYTHONPYCACHEPREFIX=/tmp/r2e_final_pycache PYTHONPATH=src \
/home/kassym/Desktop/groundloop/.venv/bin/python -m compileall -q \
  "${owned[@]}"
```

Exact collection of the seven owned test modules collected **606** tests:

```text
test_contracts.py                              85
test_d24_application_composition.py           227
test_group_requirement_composition.py          73
test_group_requirement_races.py                31
test_typed_direct_composition.py               103
test_typed_direct_races.py                      69
test_direct_m4_composition.py                   18
total                                           606
```

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:. \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider --import-mode=importlib \
  --collect-only -q \
  tests/m5/runtime/test_contracts.py \
  tests/m5/runtime/test_d24_application_composition.py \
  tests/m5/postgres_runtime/d24_application/test_group_requirement_composition.py \
  tests/m5/postgres_runtime/d24_application/test_group_requirement_races.py \
  tests/m5/postgres_runtime/d24_direct_application/test_typed_direct_composition.py \
  tests/m5/postgres_runtime/d24_direct_application/test_typed_direct_races.py \
  tests/m5/postgres_runtime/test_direct_m4_composition.py
```

Added-line scans found no `skip`, `xfail`, `TODO`, `FIXME`, credential value,
DSN value, secret value, generated-cache or bytecode content. Exact
name-status confirmed no
migration, schema-definition, package-export, status or other unowned path.
`git diff --check` passed for tracked paths, the new-file whitespace checks
passed, and `git diff --cached --name-status` was empty.

## 7. PostgreSQL cleanup and post-gate production guard

The coordinator, not either semantic auditor, performed the serial PostgreSQL
runs and cleanup. Two newly orphaned disposable test schemas left by the
interrupted/failing test history were identified and removed, and only these
two were removed:

```text
groundloop_d24_requirement_45b46229ed114fcebd9dc1da549f6fbe
groundloop_d24_requirement_61e2f37bee334c049cbec06838a5dc0c
```

The final disposable-schema inventory was restored to exactly the three
pre-existing activation-listed schemas:

```text
groundloop_d24_requirement_6ee04557df8e4c42adfaceae86e99635
groundloop_d24_requirement_7d74a56d26404e158ac326bda62a609f
groundloop_d24_requirement_b70684d1bec743ff9fa8410cbd58f18d
```

This cleanup did not mutate the persistent production namespace `groundloop`.
It is cleanup of disposable local test namespaces, not migration, backfill,
repair, runtime activation or production data mutation.

After the gates and cleanup, the coordinator reran the accepted C7 production
history guard against `groundloop` in a separate `REPEATABLE READ`, `READ
ONLY` transaction and explicitly rolled it back. The exact guard SQL retained
SHA-256
`a407107b89b89272a01984ed903a03fdd93a0312449ee34e0449d054b2b634b3`.
The rechecked identity was exactly:

```text
database: groundloop
schema OID: 65280744
runtime relation OID: 65283657
PostgreSQL server_version_num: 160014
```

The accepted migration-016 ledger row remained exact:

```text
bundle_id = m5-runtime-recovery-schema-bundle-v1
bundle_sha256 = 28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565
migration_sha256 = a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7
oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
prerequisite_sha256 = b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd
```

The guard returned exact row count `0`, canonical JSON `[]`, and result
SHA-256
`4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945`.
It therefore remained `PASS`; the guard itself made no write.

## 8. Protected state and explicit exclusions

The following coordinator-main protected bytes remained outside the candidate
and retained their exact hashes:

- `pyproject.toml`:
  `2af4b19962dc8a7d22e377be17f342530a06ee6395bbbf2a092eab36599c8fc2`;
- `docs/presentations/groundloop_fyp_professor_feedback.pdf`:
  `45c20ca46e9ad5bcd86b22c0d8882d1d611497f57ca8c45d3dea149260c110cd`;
- `docs/presentations/groundloop_fyp_professor_feedback_v2.pdf`:
  `59a13cd8d4bbb017e712c0f39e70f2eba136557e945f64f1b1fc3891742a79f0`;
- `docs/presentations/render_groundloop_fyp_professor_deck.py`:
  `c12929c349a5c0be9793159143b09da40ea2a0b27df37b92d61d9ed6483d8c2a`;
  and
- `docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT_DRAFT.md`:
  `167d1e7df5a720041fe0ff51879d08357f0dfbe3a7781ffaa08c0d958a47aa94`.

R2e changes no migration, schema, frozen digest, public M4-v1 DTO/API/digest,
package export, accepted contract, activation, implementation-status,
acceptance, roadmap or decision-log path. The two M4 source edits are only the
accepted private activated-typed-M5 composite helpers; public and standalone
v1 behavior remains covered by the 14/14 and 53/53 gates.

Runtime mode remains `v1_only` outside isolated test fixtures. This candidate
does not enable typed M5 in a deployed database and does not implement or
claim:

- production seal/publication, lifecycle-head advancement, a deployed
  discovery/verifier/measurement provider, or remote/shared database work;
- exactly-once external provider execution, end-to-end production recovery,
  or production enablement;
- M5-D25, persisted matching, migration 017, or a new schema/digest/ledger;
- performance or call-savings evidence, model quality, representative utility,
  security, novelty, publication readiness or human approval; or
- completion/promotion of M5.0-24, M5.4, M5.5, M5.6 or M5 as a whole.

All activation exclusions and stop conditions remain authoritative.

## 9. Two independent final same-byte audits

Independent auditor `/root/r2e_p1_authority` returned final `GO` on the exact
same 20 path bytes pinned in Section 2. It independently verified the exact
branch/base/worktree, paths 1--20 only with path 21 still absent, all 20
SHA-256 values, empty index and whitespace-clean diff, protected hashes and
exclusions, marker scans, and coherence of the supplied final gates. It found
no remaining P0/P1 and authorized authorship of path 21.

Independent auditor `/root/r2e_final_semantic_audit2` separately returned
final semantic `GO` on the same exact 20 path bytes and base. It independently
recollected the **606** owned tests and no forbidden markers; verified the
public-M4, frozen-digest, migration, status and protected-state boundaries;
and found the final gate evidence coherent. Its review explicitly included
the disclosed intermediate **684 passed / 105 failed** import-order defect,
the bounded path-20 correction and repin, and the successful **218/218** and
**789/789** reruns. It found no remaining P0/P1 and authorized path 21.

Both auditors were read-only and ran no PostgreSQL mutation or cleanup. Their
GO decisions apply to the exact paths 1--20 hashes above. Path 21 still
requires the coordinator's external final hash/line-count measurement and the
normal immutable candidate-commit audit.

## 10. Claim and next gate

The exact scoped conclusion is: **R2e scoped typed-direct pre-seal bridge
candidate PASS**.

This is a candidate handoff, not an integration claim. The coordinator must
externally pin this handoff, commit exactly the 21 manifested paths, verify the
immutable candidate tree and parent, perform the required post-integration
focused live rerun on the integrated bytes, and only then decide any later
status reconciliation. Until those separate gates complete, M5.0-24 remains
implementation-`PENDING` and no M5.4 row is promoted.
