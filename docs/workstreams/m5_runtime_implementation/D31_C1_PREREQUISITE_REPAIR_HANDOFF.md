# Task 16 / M5-D31 Revised C1 Prerequisite Repair

Status: final tested implementation candidate; local Lane-R executable gate
PASS. Identical-byte review and commit receipts are separate external gates.
Integration is not implied by this immutable candidate-time handoff;
acceptance requires both audits and the exact Git commit/push checks below.

Date: 2026-10-09

## Plan and ownership

Task 16 closes Lane R of the accepted D31 sequential implementation activation.
It does not silently activate C1, public wiring, deployment or AI-quality work.

1. Audit live main, the accepted schema-019 bytes and held Lane-R custody.
2. Advance the held lane only by the authorized disjoint-path fast-forward.
3. Replace the rejected owner-only reader with the exact D31 accessor; bind
   the ledger, accessor, independent envelope and shared child reads to one
   captured schema. Preserve terminal builder bytes and distinct-mask counter.
4. Prove owner/non-owner REPLACE/RETIRE, exact children, schema isolation,
   accounting order, rollback/replay and retained compatibility in isolated
   PostgreSQL schemas. Run compile, lint, format, typing and package checks.
5. Freeze identical candidate bytes for two independent audits. No C1 mutation
   occurs before Lane R is accepted, integrated and pushed.

Allowed paths are exactly the six Lane-R paths in
`D31_IMPLEMENTATION_ACTIVATION.md` Section 5: the two matching source modules,
the retirement-counter test, the changed-state-reference and seal-promotion
tests, and this handoff. Every other path is read-only.

## Verified start

- Live `origin/main`: `f76a0f6249c4563cc346e41582055387ff44773d`.
- Held Lane-R head: `ee14d697dd2dbf68b33e9a6c0e3d76791afc5520`.
- Empty index and all five held file hashes matched D31's custody ledger.
- Raw porcelain SHA-256 matched
  `da234ab487e21d296e6699792ecdbf9608607afdffca83d39b9fd483c548d1bb`.
- All 16 upstream changed paths were disjoint from all six owned paths.
- Exact ancestor check and `git merge --ff-only origin/main` succeeded; no
  reset, stash, rebase, cherry-pick, copy or patch transplant was used.
- The user checkout, AI-study worktree and held C1 worktree remain untouched.

## Implementation

The package-private preparation signature is unchanged. It captures
`pg_catalog.current_schema()` once; proves the named ledger's exact namespace,
ordinary relation kind and permanent persistence; compares all five pinned
migration-019 ledger coordinates; and invokes the qualified trusted accessor
once. No promotion temporary relation is read by the Python helper, and no
owner predicate or raw privilege substitutes for D31.

A lexical, frozen, package-private reader safely quotes GroundLoop identifiers
in this module's closed parameterized SELECT templates. The independent
half-terminal envelope and every shared D25/D26 relation/function read use the
same captured namespace, including the dynamically selected closed-constant
certificate and status tables. It does not mutate search path, own a
transaction, perform DML, construct an anchor or accept caller-authored SQL.

Only context attestation is replaced. The independent base/runtime/update,
predecessor, heads, current/working matching image, matching/work/timing
accumulators, policy, deactivation and absent-result/child checks remain.
The shared present/certificate/absence child body is exact, and the retained
terminal envelope, public helpers and immutable result/work-bound builder are
unchanged. Only terminal-builder children are inserted by the seal fixtures.

The valid held retirement repair is retained: predecessor-local matching work
is the count of distinct mask-change group IDs, with the existing successor
initialization still additive. Requirement-state writes acquire no new work
coordinate; no DTO, digest, reference kind, present-state recipe, migration,
counter, public API, provider or mode was changed.

## Executable coverage and environment

The Task-16 database is a new, owned PostgreSQL 16.14/pgvector fixture. The
project's existing database remains stopped and unchanged. Final live tests
run serially in `groundloop_task16_final` through the private socket
`/tmp/groundloop-task16-postgres.1zFwTy`. No production schema was installed,
no provider request was made and no AI-study artifact was rewritten.

The revised tests prove:

- all five ledger-field mismatches, absent ledger/current schema, wrong
  relation kind/persistence, safe quoted names, one schema capture and one
  accessor invocation before any child derivation;
- each nullable attested coordinate and the independent envelope's event,
  epoch, publication, runtime, predecessor, heads, images, accumulators,
  pending-timing, policy, deactivation and child/result drift checks;
- genuine owner and separately connected distinct non-owner LOGIN REPLACE and
  RETIRE seals; persistent table/schema privileges are granted for the fixture,
  but raw promotion-context SELECT and private-checker EXECUTE remain denied;
- contribution while runtime and accumulators are nonterminal, rejection of
  preparation and contribution after runtime terminalization, exact terminal
  builder equality, D26 absence and REPLACE present-state children, validation
  last and commit;
- all seven nonempty ledger/accessor/envelope missing-component mixtures with
  a usable later namespace; earlier unmigrated and decoy namespaces fail rather
  than falling through; restoring the selected schema restores exact children;
- a real injected preparation failure rolls back REPLACE/RETIRE seal changes
  to the already-committed semantic-complete revision, restores both heads,
  current image and predecessor interval, and leaves no seal contribution or
  immutable result; and
- empty/nonempty predecessor retirement counters, exact stored patch/work
  evidence and zero-write retained transition replay.

The certificate-only equality fixture is deterministic supplemental evidence,
not a live certificate-only history or a model-quality measurement. Broader
public-route crash/race/coexistence gates remain later C1/D/I work.

## Commands and results

The environment variable `GROUNDLOOP_TEST_DATABASE_URL` selects only the owned
acceptance database above. The executable is
`/home/kassym/Desktop/groundloop/.venv/bin/python`, with `PYTHONPATH=src:.`.
Final live test connections use `PGOPTIONS='-c jit=off'`; this is a test-process
setting only, not a runtime/default/schema change or performance result.

```text
python -m pytest -o addopts= -q \
  tests/m5/postgres_runtime/d25_publication \
  tests/m5/postgres_runtime/d25_store_core \
  tests/m5/postgres_runtime/d29_store \
  tests/m5/postgres_runtime/d30_store \
  tests/m5/postgres_runtime/test_migration_019.py --maxfail=1
result = 630 passed, 0 failed, 0 skipped in 369.49 seconds

python -m pytest -o addopts= -q tests/m5/runtime tests/m5/matching \
  tests/m5/reference tests/m5/incremental tests/m5/evaluation
result = 795 passed, 2 skipped in 93.40 seconds

python -m pytest -o addopts= -q \
  tests/m5/postgres_runtime/d25_publication/test_changed_state_references.py \
  -k 'not live'
result = 93 passed, 2 deselected

ruff check src tests = PASS
ruff format --check <five owned source/test paths> = PASS
mypy src/groundloop = PASS, 147 source files
python -m compileall -q src tests = PASS
git diff --check = PASS
pip wheel --no-deps --wheel-dir /tmp/groundloop-task16-wheel-20261009 . = PASS
wheel CRC and both exact runtime-module payloads = PASS
```

The two pure skips are the explicitly opt-in full 100,000-event gate and the
pinned local WiCE source gate. Neither was executed or promoted here. The
wheel SHA-256 is
`10438af3a0726ab0736356e93f26567656fac6fa4c703c81c38c93a22040c5ee`.
Root migration files remain absent from the wheel, an existing packaging
limitation outside this lane; no installed-wheel migration-path claim follows.

An out-of-scope whole-repository diagnostic used no test database or API key:
`--import-mode=importlib`, with legacy helper directories
`tests/m4/crash_matrix`, `tests/m4/incrementality` and
`tests/m4/physical_runtime_gate` added to `PYTHONPATH`. It returned **1 failed,
1738 passed, 1511 skipped** in 217.36 seconds. The failure is the unchanged
M4.10 local Git-history fixture: `/home/kassym/dynagox` has no remote matching
the pinned `https://github.com/gabka0/dynagox.git` source identity. It is not a
passing full-repository gate. No remote, source pin or fixture was weakened.
The same individual test was checked on the accepted clean schema-019 base.

Default pytest import mode first failed collection on four duplicate module
names; importlib mode without the legacy helper paths then failed collection
on four missing helper imports. Neither diagnostic is a pass. Whole-repository
format checking requests changes to 80 existing files on both this candidate
and clean accepted base; those unowned files were not reformatted. The first
wheel attempt without build isolation lacked `hatchling`; the normal isolated
build above passed.

Earlier live exploratory attempts were not pooled with final acceptance:
the first TCP fixture connection failed; the socket fixture initially required
a restart and database creation after its entrypoint assumed the default
socket. An interrupted broad run recorded 64 passed plus a cleanup failure
caused by overlapping schema inventories, and a prematurely started rerun
recorded 95 passed plus the complementary inventory failure. Both were
discarded as acceptance evidence. The final serial run uses a fresh database;
no validator, guard, trigger, hash or schema-law check was weakened to pass it.
The first clean serial attempt with default PostgreSQL JIT was interrupted
after 147 passes in 1167.98 seconds because repeated bootstrap-oracle
compilation dominated setup. A separate fixture diagnostic with JIT disabled
successfully installed the same accepted migration-017 bundle in 0.583 seconds.
The final complete matrix therefore restarts in a new database with JIT off on
all its connections. The partial run is not acceptance evidence, and fixture
timings are not GroundLoop runtime performance measurements. A standalone
diagnostic initially lacked the legacy `tests` import root; that invocation
failed before installation and the corrected fixture diagnostic passed.

## Exact candidate identities and review barrier

```text
base = f76a0f6249c4563cc346e41582055387ff44773d
branch = workstream/m5-d30-c1-prerequisite-blocker-repair
owned_changed_path_count = 6
2b18ca48d5276bc95961d2cbe84bca25244df49a72dba0e728f9e7bc681ba6e9  src/groundloop/m5/runtime/postgres_matching.py
cdae8248d391e51d6df615237d51ac3dc95e5b0eb28a3e6ce53e74b58ac6bf07  src/groundloop/m5/runtime/postgres_matching_publication.py
5bad539816933f2bae3eb9f461aac60d9dc1f3182e4772ba13cc605111ed36fb  tests/m5/postgres_runtime/d25_store_core/test_retirement_work_counter.py
456fe8d9cab1969cd81ead654d1b1bd324ca67920bd1df34688136931fc8366b  tests/m5/postgres_runtime/d25_publication/test_changed_state_references.py
899bd9993b8fe9f144f09fad876235c12d78010cb44dd9932ec6112261f4bcfa  tests/m5/postgres_runtime/d25_publication/test_seal_promotion.py
```

The final hash of this handoff is recorded externally after its evidence is
complete, avoiding a self-hash. Two independent reviewers must review the same
six hashes and return `GO`, `P0=0`, `P1=0`; provisional source-only review is
not acceptance. The exact six-path commit then requires two independent
postcommit parent/tree/path/mode/blob/hash checks and atomic branch/main push.
This handoff's candidate status is not a substitute for that Git receipt.

## Next boundary

Only after that exact Lane-R commit is accepted and pushed may held C1 advance
by the separately prescribed disjoint-path fast-forward. Its nine files,
empty index and raw porcelain digest must still match D31's custody ledger.
C1 then resumes the already-frozen structural transaction composition and
preterminal/contribution/terminal/result-bound seal order. Public facades,
real direct-M4 producers and final cross-layer integration remain separately
scoped work; no new lane or path is activated by this handoff.

## Claim ceiling

Before the external review/commit/push receipt, Lane-R acceptance is PENDING.
Even after that narrowly scoped acceptance, C1 composition, Task 2,
M5-D24--D31 implementation, M5.4-05--09, M5.5, M5.6 and deployment remain
PENDING. Runtime defaults remain `v1_only`. This repair establishes no
AI-quality, latency, utility, security, privacy, scalability, novelty or
objective-truth result.
