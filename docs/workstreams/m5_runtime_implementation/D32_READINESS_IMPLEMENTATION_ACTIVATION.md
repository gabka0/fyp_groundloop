# Task 17 / M5-D32 Sequential Readiness Implementation Activation

Status: docs-only activation candidate. No lane starts until these exact bytes
receive two independent same-byte GO/P0=0/P1=0 reviews, an exact sole-path
commit, two independent postcommit identity checks and atomic branch/main push.

Date: 2026-10-09

## 1. Exact authority and parent

The user approved the narrow readiness/accounting correction and subsequent
Task-17 implementation. Authority is AGENTS.md, runtime addendum revision 13,
D24--D32 and the acceptance matrix. This manifest grants paths and gates only;
it cannot change a frozen contract. Public routing, deployment and AI work are
not activated. Default runtime remains v1_only outside isolated fixtures.

```text
activation_worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d32-readiness-activation
activation_branch = workstream/m5-d32-readiness-activation
required_parent = f51a0a28a118fbb63159764bbccc8c756c02a785
required_parent_tree = 85d4d9b90602372690826d7e26534aff183c81e2
required_origin_main = f51a0a28a118fbb63159764bbccc8c756c02a785
sole_owned_activation_path = docs/workstreams/m5_runtime_implementation/D32_READINESS_IMPLEMENTATION_ACTIVATION.md
accepted_contract_commit = 5ae0a6a18441a0a47f350cea9487129793882524
accepted_contract_sha256 = 0e9a4bbe8da34d8248ce138c4bc21cb8076ddb4a578ea6a36cd6a4e100811435
authority_freeze_manifest_sha256 = 78204a5349b623a2991bc64707a46cafccb5bb39284b03ca11e27a3ab87082a9
```

Both final candidate reviews and both final ten-path freeze reviews returned
GO/P0=0/P1=0/P2=0. Both exact postcommit checks for each commit returned
GO/P0=0/P1=0. Candidate and freeze branches/main were atomically pushed. The
candidate and all ten freeze paths are read-only in implementation lanes.

## 2. Strict sequence and integration discipline

```text
this activation -> S32 schema/shared primitives
                    (H32 docs-only exact SQL hash pin before first installer)
                -> R32 private readiness
                -> resumed R-T pending reader
                -> resumed C1 atomic composition
```

Only one source lane runs at once. Two independent read-only audits may run in
parallel and own no edit paths. Every source lane starts from the exact pushed
prior barrier, freezes an external complete path/hash manifest, passes two
same-byte GO/P0=0/P1=0 audits, commits only its owned paths with one exact base
parent, passes two postcommit checks and atomically pushes its branch/main.
No partial source candidate is accepted or pushed to bypass an unmet gate.

S32 may draft SQL/shared primitives and run isolated diagnostics before H32.
Before installing through the new production installer, freeze reviewed final
SQL and the canonical bundle identity in H32; installer constants must use
those literal identities, not hashes inferred from mutable local authority.
H32 is a separate docs-only tranche from then-current pushed main. It owns
exactly the new path
docs/workstreams/m5_runtime_implementation/D32_SCHEMA_020_HASH_FREEZE.md.
Two same-byte audits, sole-path commit/postcommit checks and atomic push are
required. S32 may then fast-forward its dirty worktree only after proving that
H32's sole path is disjoint from all eight S32 paths and all custody matches.
The S32 final commit's sole parent is that exact pushed H32 commit. Any SQL
change after H32 requires a fresh reviewed hash-freeze correction before the
installer can accept it. Do not edit H32 opportunistically from a source lane.

No reset, clean, stash, rebase, cherry-pick, checkout replacement or manual WIP
copy/patch transplant is allowed. A held worktree may advance via merge --ff-only
only after live HEAD/index/raw-status and complete file-hash verification,
ancestor proof and upstream/owned-path disjointness. A mismatch or new necessary
path/object stops that lane for a written manifest/contract correction.

## 3. S32 exact eight-path schema/shared ownership

```text
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d32-schema-020
branch = workstream/m5-d32-schema-020
initial_base = exact pushed commit of this activation
final_base = exact pushed H32 commit
```

Owned paths, exactly:

1. migrations/020_m5_semantic_readiness.sql (new);
2. src/groundloop/postgres/migrations.py;
3. src/groundloop/m5/runtime/contracts.py;
4. src/groundloop/m5/runtime/digests.py;
5. tests/m5/runtime/test_contracts.py;
6. tests/m5/runtime/test_digests.py;
7. tests/m5/postgres_runtime/test_migration_020.py (new); and
8. docs/workstreams/m5_runtime_implementation/D32_SCHEMA_020_HANDOFF.md (new).

No other source, test, doc, dependency, public facade, package configuration or
legacy migration path may change. In particular both held recovery/persistence
files and every R-T file are outside ownership.

Implement only D32's three CHECK extensions, one predecessor function/trigger
and two 016 function replacements. Append SEMANTIC_READINESS to the work enum
without changing the existing member order/wires. Add source-identity/preimage
helpers for the exact D32 recipe with strict state/type/revision validation;
retain every old digest/DTO/vector. Existing anchor/append DTOs remain exact.
The opt-in ledger-first installer adds no default route or legacy globbing.

The new guard uses schema-qualified permanent relation reads, an installation
schema plus pg_catalog pinned search path, and no PUBLIC/pg_temp lookup for
persistent authority. Avoid unqualified public.digest dependencies in that
guard; built-in canonical SHA-256/framing can preserve exact identities without
adding a helper object, ACL or schema fallback. Preserve complete old function
bodies except D32's enumerated additions; compare prior/after catalog images.

Before acceptance prove the complete Section-7 schema/type/identity matrix:
Python/SQL golden vectors, exact S+K work, true predecessor and insert-after-
advance rejection, valid/failing source/event/declaration/phase coordinates,
all prior 000--019 hashes, exact catalog delta, function flags/search paths,
constraint closure, ledger-first rerun/conflict/partial/wrong-schema cases,
every DDL/ledger rollback cut and concurrent installer orders. Manual isolated
SQL fixtures may exercise guard/edge/deferred boundaries; they do not prove
production readiness. Report that evidence distinction.

Run owned tests, retained migration-019 and relevant migration-016/017/018
compatibility, all applicable pure M5 tests, compile, strict source typing,
owned lint/format, diff check and wheel archive/module checks. Root migrations
are outside the current wheel, an unchanged packaging boundary; do not claim
installed-wheel migration support or silently edit pyproject.toml to expand
this task. Report exact commands, pass/fail/skip counts and overlapping runs.

## 4. R32 exact four-path private-runtime ownership

```text
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d32-readiness-runtime
branch = workstream/m5-d32-readiness-runtime
base = exact pushed accepted S32 commit
```

Owned paths, exactly:

1. src/groundloop/m5/runtime/postgres_readiness.py (new);
2. tests/m5/runtime/test_semantic_readiness.py (new);
3. tests/m5/postgres_runtime/test_semantic_readiness.py (new); and
4. docs/workstreams/m5_runtime_implementation/D32_READINESS_HANDOFF.md (new).

No existing source/test, contract, installer, migration, public API or held
file may change. Reuse accepted cursor-local/accounting primitives where
appropriate; private new code must not import unaccepted held C1 source.
Private receipt/anchor use follows D32; no public facade, exported DTO, model
call, active runtime enablement or terminal accounting is added.

Implement both separate outer readiness commits with idle/read-write READ
COMMITTED gates, one captured trusted schema/exact pinned 020 authority,
established base/runtime/owner/answer/work/timing lock order, independent
readiness checks, guarded contribution-before-header advance, exactly one
revision, exact S+K work and one final work/timing CAS. Replay first validates
immutable original coordinates and returns the original anchor with zero
writes, including after later legal progress. Keep D25 semantic images and
matching contributions unchanged.

For genuine readiness-only histories, accepted
PostgresM5RuntimeStore.open_typed_event_atomically already supplies structural
open, roots and D24 recovery accounting. Use it and real acquisition/staging/
closure or verifier/cancellation transitions; do not author readiness headers,
fake direct jobs, pending points, hashes or post-hoc zero work. Owner and
distinct separately connected non-owner RETIRE and REPLACE histories, all
open/failed/direct/positive-counter blockers, prior-missing/observed timing,
exact replay/conflict, rollback after every mutation cut and both race orders
are required. These histories prove readiness coordination only: that opener
does not compose D25 matching state, and this lane must not claim integrated
matching/seal or public-C1 acceptance.

Run full owned pure/live tests plus schema/readiness and retained runtime
compatibility, static and wheel-module gates on final identical bytes. Capture
SQL traces/point plans to exclude global history scans and schema fallback.
Keep invocation/replay work and historical event work distinct.

## 5. Resumed R-T exact four-path grant and evidence boundary

Resume only after R32 acceptance/push and the Section-7 custody/disjointness
checks. Existing R-T activation/handoff remain applicable except this explicit
addition: add semantic_readiness to its exact nonterminal pending-kind allowlist
and cover it with the same immutable work/timing point checks. No reader DML,
timing clear, early validation, privilege/API/digest change is allowed.

R-T still owns exactly:

1. src/groundloop/m5/runtime/postgres_matching_publication.py;
2. tests/m5/postgres_runtime/d25_publication/test_changed_state_references.py;
3. tests/m5/postgres_runtime/d25_publication/test_seal_promotion.py; and
4. docs/workstreams/m5_runtime_implementation/D31_PENDING_TIMING_REPAIR_HANDOFF.md.

The genuine pending regression must use accepted matching primitives to create
the D25 structural foundation in the structural-open transaction, before its
commit—not bolt a patch onto an already committed revision-1 open. The accepted
registration fixture at tests/m5/postgres_runtime/d25_store_core/conftest.py
demonstrates this composition and may be read/reused; it is not an owned path.
Adapt local owned fixtures for REPLACE/RETIRE without importing or transplanting
held C1 code. Genuine D32 then supplies readiness and pending accounting.
This is an accepted-helper-composed fixture, not production store-first-open
evidence. Open public_delta_count stays zero because seal owns public deltas;
record real state-write counts and byte instrumentation, not planned outputs.

An owned SQL terminal-boundary adapter may test the reader/publication boundary,
but not impersonate production C1. It must preserve genuine readiness, lock
the exact D24 work-accumulator point at N, add only measured seal work once,
freeze that resulting point, insert seal contribution before terminal runtime,
retain and resolve the genuine prior anchor, perform exactly one fused terminal
timing CAS, derive children through the unchanged authoritative builder, force
constraints last and commit intact validators/privileges. No authored 1->2->3
readiness, zero overwrite, trigger bypass or fabricated positive pending hash.
Owner/non-owner RETIRE/REPLACE, missing timing identity/counts, replay, rollback,
terminal/half-terminal failures and retained compatibility remain required.
Any history SUM of immutable contributions is an independent test-oracle
assertion only, outside the simulated terminalizer, never its work authority.
Label this reader/SQL-boundary evidence; production terminalizer/C1 evidence
belongs exclusively to the later C1 lane.

The four-path repair requires final complete-byte manifests/audits/postcommit
checks and atomic push. Historical 165 passes are overlapping diagnostics,
not this new-kind or genuine pending-history acceptance.

## 6. Resumed C1 exact nine-path grant

After accepted R-T push, C1 may fast-forward with proven ancestor/disjointness
and unchanged held custody. Its existing D31 sequential activation still
governs except the newly accepted D32 helper may now be composed. It owns:

1. src/groundloop/m5/runtime/persistence.py;
2. src/groundloop/m5/runtime/postgres_recovery.py;
3. tests/m5/postgres_runtime/d30_application/conftest.py;
4. tests/m5/postgres_runtime/d30_application/test_seal_atomicity.py;
5. tests/m5/postgres_runtime/d30_application/test_store_composition.py;
6. tests/m5/postgres_runtime/d30_application/test_store_races.py;
7. tests/m5/postgres_runtime/d30_application/test_structural_open.py;
8. tests/m5/postgres_runtime/d30_application/test_structural_order.py; and
9. docs/workstreams/m5_runtime_implementation/D30_STRUCTURAL_COMPOSITION_HANDOFF.md.

Finish the private outer structural-open and seal composition, real readiness
integration, real D24 contribution/work/timing finalizers, owner/non-owner
histories, exact C1 lock/write/child ordering, rollback at every boundary,
retained replay and concurrent serial orders. Correct idle fixture boundaries
with genuine commits, not by removing the idle contract. Preserve previous
declaration/root/matching/counter/provenance corrections and all source bytes
outside ownership. Separate readiness edges retain separate outer commits.

Prove genuine production composition, not the R-T SQL adapter: complete held
C1 focused suite, relevant full pure/live M4/M5 coexistence and differential
gates, D26 present/absence/certificate-only equality, bounded-history plans,
work/timing identities and exact publication delta/reference/result replay.
Run compile/lint/format/typing/package and a full-repository diagnostic with
unchanged baseline failures/skips reported. The prior whole-repository
1738-pass/1511-skip/one pinned-Dynagox-fixture failure was diagnostic, not PASS.
Final byte manifests, independent audits, exact commits and pushes are required.

## 7. Immutable restart custody

All indexes were empty at the approved checkpoint. Recheck protected and still-
held work before each advance, candidate commit and push. A lane's own authorized
edits are checked against its final frozen manifest, not its pre-edit restart
hashes. After an accepted advance record the new HEAD/custody point while
proving every held file unchanged. Raw status hashes cover exact porcelain-v2
-uall -z bytes and do not substitute for file content hashes.

```text
primary = /home/kassym/Desktop/groundloop
primary_head = 14598ae51562006eaf67850b19e8212f38997903
primary_raw_status = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
ai = /home/kassym/Desktop/groundloop-worktrees/fyp-fresh-baseline-execution
ai_head = a1dab3e5ebfe3bb65c521acd84758297a589f11a
ai_raw_status = 279b7ebf78bc4b19c71be49031ecc1a3415d1033864869344af98f07e6a438a7
c1 = /home/kassym/Desktop/groundloop-worktrees/m5-d30-structural-composition
c1_head = 8510470176f0396472d2f125fb8bd010dc3a61c4
c1_raw_status = 736f23f62b8ae744763219ab592266318662636dec4bcbc8a7be93e56ed0d58d
rt = /home/kassym/Desktop/groundloop-worktrees/m5-d31-pending-timing-repair
rt_head = d7db09c146795d6e6bef562a0ca266234b397639
rt_raw_status = 8c5c47d44900bba354618d13f0a2afc554ca0483d8c8ec1d6a6fe118eb4f9822
c3b9d9de6b226a05d61ccf026710650c5a44df805e2a26f1bb0a0570437681c8  src/groundloop/m5/runtime/postgres_matching_publication.py
942c6a34ebeefc88a02323da3fddf549596937ee87cda0e4a4789f0c4240af50  tests/m5/postgres_runtime/d25_publication/test_changed_state_references.py
899bd9993b8fe9f144f09fad876235c12d78010cb44dd9932ec6112261f4bcfa  tests/m5/postgres_runtime/d25_publication/test_seal_promotion.py
528ad85620f0f30df8253c0eda271841cdd118a3b0f60bfe772bc9fe600ab823  docs/workstreams/m5_runtime_implementation/D31_PENDING_TIMING_REPAIR_HANDOFF.md
```

C1's nine-file restart content ledger is exactly
D31_PENDING_TIMING_REPAIR_ACTIVATION.md Section 5; all nine must match live
until C1's own edit grant begins, then its authorized final manifest governs.
No protected primary/AI edit, staging, branch advance or reset is authorized.
Never copy held C1/reader WIP into a new implementation lane to bypass lineage.

## 8. Isolated execution and terminal boundary

Use only owned temporary test databases/schemas in the stopped owned container
groundloop-task16-runtime-socket-20261009, host socket
/tmp/groundloop-task16-postgres.1zFwTy (internal /socket), or an equally explicit
isolated test instance. Inspect ownership/state first. The user's stopped
groundloop-db-1 and any non-test database are outside scope. No provider/model
calls, credential output, runtime enablement or deployment are authorized.

Report every failure/skip and any fixture/schema error; never skip/xfail or
swallow the new positive regression. Contract/source unit passes do not replace
live owner/non-owner, race, replay or transaction-commit evidence. Preserve
temporary reports and do not delete material user data.

This activation does not itself promote implementation. Task 17/C1/Task 2,
M5-D24--D32 implementation and remaining M5.4--M5.6 stay PENDING until their
specific evidence mapping; scoped tranche acceptance is not milestone closure.
Deployment, AI quality, independently adjudicated histories and end-to-end
utility remain PENDING. Stop after scoped Task-17/C1 acceptance and reassess
the next public vertical slice; no later lane is activated here.
