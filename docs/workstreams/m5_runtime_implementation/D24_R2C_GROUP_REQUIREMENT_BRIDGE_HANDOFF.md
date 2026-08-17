# M5-D24 R2c Group/Requirement PostgreSQL Bridge Handoff

Status: integrated five-path R2c group/requirement PostgreSQL pre-seal evidence
at `0e0ff4385b4f5e5145788f59cc55411b39d659c1`; scoped gate `PASS`, M5.0-24
implementation `PENDING`, and no current edit ownership

Date: 2026-08-17

Branch: `workstream/m5-d24-r2c-group-requirement-bridge`

Integration commit:
`0e0ff4385b4f5e5145788f59cc55411b39d659c1`

Activation commit/base:
`abe22e6d1cf844ca7bc63697f089cc77fcb4397f`

The branch was created at that exact commit and the implementation worktree
was clean before the first owned-path edit. The activation's original `/tmp`
worktree disappeared during an environment restart before candidate commit.
The user then explicitly directed the coordinator to stop using ephemeral or
permission-fragmented locations. The same branch at the same activation base
was recreated without rebase or merge at the persistent path:

`/home/kassym/Desktop/groundloop-worktrees/m5-d24-r2c-group-requirement-bridge`

The four in-flight source/test files were recovered from the session journal,
their exact checkpoint hashes were reproduced, and the candidate gates below
ran from the persistent worktree. The exact integrated-main rerun is identified
separately. This operational relocation changed no Git base, owned byte,
contract, or implementation authority.

## Exact five-path candidate

Relative to the activation base, the candidate adds exactly these five paths:

1. `src/groundloop/m5/runtime/postgres_application.py`
2. `tests/m5/postgres_runtime/d24_application/conftest.py`
3. `tests/m5/postgres_runtime/d24_application/test_group_requirement_composition.py`
4. `tests/m5/postgres_runtime/d24_application/test_group_requirement_races.py`
5. this handoff

Frozen source/test SHA-256 pins:

- `postgres_application.py`:
  `ac93da6cb0c14386d9c4c0cf7ae926790e144c7c3ab6ea1b2b0d92a81dfd7d45`
- `conftest.py`:
  `d48f007f395b1bf8370bac2c30efd50f9c0b06515bfa9f86436db684eccdd844`
- `test_group_requirement_composition.py`:
  `881ffb450dc11cff5409ffbbb3bf1c902936f5615c46ea714137c680d77c422c`
- `test_group_requirement_races.py`:
  `5fea25b5fca180d10db212ece06aaaec35a8607103f199c6cf7070aa8e1a4c95`

The candidate handoff bytes at
`0e0ff43:docs/workstreams/m5_runtime_implementation/D24_R2C_GROUP_REQUIREMENT_BRIDGE_HANDOFF.md`
had pre-conversion SHA-256
`302921a289203670d1619c74eff2ea33f52ddb52de743d7bda5cd2184253998a`.
This later status conversion intentionally changes this document; the
integration commit plus that pre-conversion hash pins the candidate without
pretending that a file can self-pin its own final hash.

No existing source, test, fixture, migration, package export, contract,
digest, status document, accepted C5/C6 document, or protected user path is
part of the candidate.

## Implemented boundary

`PostgresM5GroupRequirementPreSealPorts` is an internal facade over one
caller-supplied `PostgresM5RuntimeStore`, exact candidate-policy manifest, and
explicit runtime operational config. The assembly helper also requires
explicit discovery, verifier, measurement, and post-seal-audit inputs; it
creates no provider or configuration default.

The facade implements the unchanged application policy, structural,
group-only direct-noop, runtime-persistence, and runtime-read protocols. Every
method has exact current parameter, annotation, keyword-only, return, and
default parity with its consumed protocol.

Before the structural store call, the facade recursively validates:

- the exact group lifecycle event and `direct_plan=None` boundary;
- group, requirement, requirement-registry, and active-chunk snapshot DTOs;
- the canonical self-digested empty group-only withdrawal plan;
- absent direct payload/withdrawal and exact empty direct root/scope tuples;
- deterministic forward requirement roots, scope/job/execution identity,
  ordering, uniqueness, target, snapshots, and policy binding;
- the exact root-set hash; and
- per-root fallback provenance derived by exact fallback-key membership.

Only the existing store owns SQL, transactions, ledger checks, locks,
structural accounting, timing state, replay, settlement, failure, and
telemetry. The facade contains no SQL, private persistence helper, transaction,
retry loop, environment lookup, or returned-receipt reinterpretation.

Direct planning always rejects because typed-direct composition is outside
R2c. Checked direct execution for an admitted group event is a zero-write,
zero-work revision-preserving noop. It does not call the concrete direct-M4
adapter.

The production store still has no accepted typed seal. The facade validates
the unsupported seal call and raises before any store call, transaction,
telemetry, lifecycle promotion, result, or publication mutation. This makes
the coordinator constructible for concrete `BLOCKED`, `FAILED`, reconnect,
and pre-seal evidence without fabricating successful production completion.

## Live evidence

All live tests used unique PostgreSQL schemas, migrations through accepted
016, the literal accepted five-field recovery ledger, explicit runtime config,
and fresh connections where reconnect behavior was claimed. The server was
PostgreSQL `16.14 (Debian 16.14-1.pgdg12+1)` from the local
`pgvector/pgvector:pg16` service. No DSN or credential is recorded here.

The new suite collects exactly **81** cases:

- composition: **50**;
- races: **31**.

The final serial directory gate passed **81/81**:

```bash
set -a
source /home/kassym/Desktop/groundloop/.env
set +a
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPYCACHEPREFIX=/tmp/groundloop-r2c-live-evidence-pycache \
PYTHONPATH=src:. \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -p no:cacheprovider -q -rs --tb=short \
  tests/m5/postgres_runtime/d24_application
```

The same 81-case command passed again from the exact integrated main revision
`0e0ff4385b4f5e5145788f59cc55411b39d659c1`.

The suite proves exact register, replace, and retire planning/open behavior;
the full zero-write rejection matrix; retryable discovery/verifier `BLOCKED`;
nonretryable discovery/verifier production `FAILED`; successful pre-seal
fail-closed behavior; fresh terminal replay; structural-open loss; live lease;
database-clock takeover; retry reacquisition; and the applicable concrete C5
and C6 terminal-cutoff orders and corruptions.

Every first-written structural open, acquisition, attempt settlement,
successful root result, and root barrier is matched one-to-one against its
durable work-contribution and transition-timing rows. Live-lease, terminal,
and ordinary reconnect observations prove no new transition callback or row.
Fresh terminal reconnect snapshots exact semantic-job, attempt, dispatch, and
execution-evidence identity rows before and after and proves byte-equality.

Zero-write snapshots cover every current-schema table row, each row's `xmin`,
and every sequence's `last_value` and `is_called`, so rejection cannot hide a
nontransactional epoch-ID allocation.

## Regression and static gates

The unchanged pure M5 runtime suite passed **237/237**:

```bash
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPYCACHEPREFIX=/tmp/groundloop-r2c-pure-pycache \
PYTHONPATH=src:. \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -p no:cacheprovider -q -rs --tb=short tests/m5/runtime
```

The complete existing D24 requirement suite passed **163/163**:

```bash
set -a
source /home/kassym/Desktop/groundloop/.env
set +a
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPYCACHEPREFIX=/tmp/groundloop-r2c-d24req-pycache \
PYTHONPATH=src:. \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -p no:cacheprovider -q -rs --tb=short \
  tests/m5/postgres_runtime/d24_requirement
```

The complete existing PostgreSQL runtime suite passed **589/589** in one
fresh guarded run with pytest import isolation:

```bash
set -a
source /home/kassym/Desktop/groundloop/.env
set +a
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPYCACHEPREFIX=/tmp/groundloop-r2c-fullpg-importlib-pycache \
PYTHONPATH=src:. \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -p no:cacheprovider --import-mode=importlib \
  -q -rs --tb=short tests/m5/postgres_runtime
```

An initial invocation without `--import-mode=importlib` stopped during
collection, before any test ran, because the pre-existing
`d24_direct/test_races.py` and `d24_requirement/test_races.py` (and their
`test_recovery.py` peers) share top-level module basenames. Importlib mode is
pytest's standard isolation for that existing layout; it changes no
repository byte or test selection. The successful run collected and executed
the complete directory.

The focused live public-M4 route/direct compatibility gate passed **48/48**:

```bash
set -a
source /home/kassym/Desktop/groundloop/.env
set +a
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPYCACHEPREFIX=/tmp/groundloop-r2c-direct-route-pycache \
PYTHONPATH=src:. \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -p no:cacheprovider --import-mode=importlib \
  -q -rs --tb=short \
  tests/m5/postgres_runtime/test_m4_typed_barrier.py \
  tests/m5/postgres_runtime/test_direct_m4_composition.py \
  tests/m5/postgres_runtime/d24_direct
```

The non-database public-M4 DTO/signature, legacy/M4-v1, direct-only, and M4
contract selection collected and passed **14/14**:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m pytest \
  -o addopts='' -p no:cacheprovider -q \
  tests/m5/runtime/test_contracts.py::test_d24_public_m4_dto_and_api_signature_snapshot_is_unchanged \
  tests/m5/reference/test_legacy_regression.py \
  tests/m4/test_m4_contracts.py
```

On the four new Python paths, all of the following passed:

- Ruff check;
- Ruff format check;
- strict mypy with `MYPYPATH=src`, `PYTHONPATH=src:.`, explicit package bases,
  and no compatibility suppression;
- cache-isolated compile;
- tracked and per-untracked-file whitespace checks; and
- scan for added `TODO`, `FIXME`, or `xfail` markers.

No test was skipped or expected-failed in the recorded passing runs. No
credential, DSN, database dump, cache, bytecode, or generated output is in the
candidate.

## Independent audits

The source/contract audit returned `GO` with no P0/P1 at the frozen source
hash. It independently verified activation ancestry, protocol parity,
recursive validation-before-write, deterministic root/fallback derivation,
store-only mutation, fail-closed seal, direct exclusion, and the absence of a
public export or expanded claim.

The test/evidence audit initially found one P1: the tests did not yet assert
every required transition timing kind or immutable reconnect identity row.
That gap was corrected only in the two new test files. The independent
re-audit returned `GO` with no remaining P0/P1 at the frozen test hashes and
confirmed the collection remains 81.

The final five-path exact-byte and immutable-commit audit returned `GO` with no
unresolved P0/P1. It verified sole parent `abe22e6`, immutable tree
`021ba1d5c58434c9b8129dce257af302a810e0e9`, exactly five added paths, all
frozen source/test hashes, clean candidate worktree state, and unchanged
protected user bytes. On the integrated revision the Ruff, format, strict-mypy,
cache-isolated compile, diff, and exact-hash gates also passed.

## Explicit remaining boundary

R2c proves only a group/requirement PostgreSQL **pre-seal bridge**. It does
not provide or claim:

- typed-direct outer-settlement composition or cursor-local direct failure;
- production typed seal, lifecycle promotion, combined publication, either
  publication-head advance, or successful terminal application completion;
- production requirement discovery, verifier, or measurement adapters;
- M5-D25 active verifier persistence or migration 017;
- deployment readiness, exactly-once external provider execution,
  performance/call-savings evidence, model quality, representative utility,
  human approval, security, novelty, or publishing potential; or
- implementation completion of M5-D24, M5.4, M5.5, M5.6, or M5 as a whole.

M5.0-24 therefore remains contract-`PASS` / implementation-`PENDING`, and
M5.4 remains partial.

## Integration closure

The scoped R2c group/requirement PostgreSQL pre-seal bridge is integrated at
`0e0ff43` and its scoped tranche gate is `PASS`. It is not a M5.0-24
implementation promotion or a M5.4 row promotion.

Integration closed the five-path grant. This handoff and its activation are
historical evidence only and authorize no further edit. The persistent
worktree may remain for audit but grants no implementation authority. Every
later typed-direct, production-seal/publication, production-provider, or D25
tranche requires a fresh committed path-exclusive activation from the then-
current integrated barrier.
