# M5-D32 / S32 Schema and Shared-Primitives Handoff

Status: final implementation candidate. Exact-byte audits, commit/postcommit
checks and atomic push are mandatory for acceptance and before R32 starts.

Date: 2026-10-10

## Authority and exact ownership

Authority is D32 amendment SHA-256
0e9a4bbe8da34d8248ce138c4bc21cb8076ddb4a578ea6a36cd6a4e100811435,
runtime addendum revision 13, acceptance matrix and activation SHA-256
2e35e3d049d5793ddb0f5ad7af008ac20176ad18d8dca325b84e13e5f53e5740.
The corrected H32 hash-freeze barrier was atomically pushed as
24cd57c1c0eb624ae25a9723c7c9d7ad83903727, tree
6c2a8559138f6f5541ed1d30fc212bbd31464e69. That exact commit is this
candidate's sole parent. R32, held R-T and
held C1 remain separate subsequent lanes; no source or WIP is transplanted.

Worktree: /home/kassym/Desktop/groundloop-worktrees/m5-d32-schema-020.
Branch: workstream/m5-d32-schema-020.
The eight owned paths are exactly:

1. migrations/020_m5_semantic_readiness.sql;
2. src/groundloop/postgres/migrations.py;
3. src/groundloop/m5/runtime/contracts.py;
4. src/groundloop/m5/runtime/digests.py;
5. tests/m5/runtime/test_contracts.py;
6. tests/m5/runtime/test_digests.py;
7. tests/m5/postgres_runtime/test_migration_020.py; and
8. this handoff.

No existing runtime/accounting/matching/public API, dependency, package
configuration, legacy migration, frozen contract or other test path changes.
Protected main remains 14598ae51562006eaf67850b19e8212f38997903; protected AI
and all thirteen held C1/R-T files remain at activation custody identities.

## Implemented surface

The work enum appends semantic_readiness after seal without changing prior
member order/wires. It is a D24 contribution/timing kind, not an additional
changed-state reference kind; the existing six reference kinds remain exact.
Strict typed source preimage/digest helpers implement the D32 two-edge recipe;
Python/SQL Unicode goldens and illegal type/state/revision cases are covered.
There is no repr/JSON hashing, nullable hash, tombstone or present-state recipe
change. Producers' work is exactly source-preimage plus existing key-preimage
bytes S+K in bytes_hashed/bytes_serialized, with all other 30 counters zero.

020 adds exactly the three closed CHECK allowlist extensions, one pinned
SECURITY INVOKER predecessor function and BEFORE INSERT trigger, and the two
authorized 016 validator replacements. The unchanged 015 phase-edge guard
remains in force. All other old bodies/configuration/privileges and 000--019
bytes are unchanged. The schema-qualified predecessor proof binds structural
event/update/declaration/root identity, no M4/direct surfaces, actual locked
header revision/state, all owner/answer/runtime zero counters and exact terminal
job/scope coordinates. Completed roots require real closure/barrier evidence;
cancelled roots require exact job/scope completion and reverse cancellation
plan membership at the current event/epoch. Explicit NULL reasons, future child
coordinates and cross-reason cancellation gaps fail closed.

The new opt-in administration installer captures one permanent schema, checks
the ledger first, verifies all exact five-field 014--019 prerequisite ledgers
and literal source identities, locks ledger/base/runtime/accounting tables in
fixed order, rechecks under lock, executes the eleven pinned statement barriers
and inserts the exact 020 ledger last. Replay performs no DDL/DML/install locks.
Changed requested bytes conflict before DDL. The read-only authority helper
supports an explicit captured schema and checks the pinned ledger/catalog.
Static prior/new function bodies, ownership, flags, pinned settings, execute
ACLs and complete declared signatures are checked; retained static trigger
attachments/flags and the new exact predecessor attachment are checked.
Constraint introspection first materializes the exact schema/relation set,
avoiding unrelated catalog-drop races without weakening ambiguity rejection.
No installer or helper enables runtime mode or becomes a public app route.

Independent preliminary audits rejected the original catalog verifier: it
could miss retained CHECK/PK/FK/column/index changes, extra triggers and a
dropped ledger PK with conflicting duplicate authority rows. A second review
also found differently named attached indexes and external-name inherited
children. The final verifier includes every attached index, inbound/outbound
qualified inheritance edge, and internal/user trigger, as well as the closed
GroundLoop relation/column/constraint/type/enum/sequence/view/function inventory.
It retains function source/signature/configuration/ACL checks separately.
Logical identities replace unstable OIDs; sequence current values, data/history
and legitimate table ACL grants are excluded. Actual expression dependencies
include function argument and operator operand type namespaces/identities.

The administrative structural checksums below were reproduced in two pristine
schemas built by accepted 000--019 installers and the pushed raw 020 SQL, with
ordinary and pinned search paths. They are not new semantic/state digest
recipes or additional reference kinds. SQL qualification normalization is
token-based: quoted data stays exact; allowed deparser qualifiers/operator
wrappers normalize only with separate actual-binding metadata. Static catalog
queries qualify builtins/types/operators without changing caller search_path;
quoted SQL data/identifiers and placeholders are preserved. This compiler is
never applied to migration SQL. An explicit-schema read-only check is tested
under real foreign left(text,integer) and =(text,text) shadows. No general
search-path security or cross-PostgreSQL-version portability claim follows.
Final hardening qualifies the ledger timestamp as pg_catalog.now() and compares
trigger return types by native namespace/type identity, not regtype display.
An unrelated same-schema application now() that raises is a positive first-
install/replay regression. Other noncanonical deparser renderings conservatively
fail closed; arbitrary caller-created type-shadow portability is not promised.

```text
pristine_019_catalog_sha256 = ffe7652cda9193348340660a9c42b2ec767ec95809d89cb21918305302108252
pristine_020_catalog_sha256 = 27a02a8910e1a4b671b879cea1bdc0a5a35d9f693a870183355088990e207f62
catalog_baseline = PostgreSQL 16.14 / owned test instance
```

## Exact SQL authority

```text
migration_sha256 = df0a3c0c9b228a4a22903479896326d27fbd6f98f5878e34d73182ba007bf837
migration_lines = 1292
migration_bytes = 68419
bundle_id = m5-semantic-readiness-schema-bundle-v1
bundle_sha256 = b7706feb7d54fcf9fdb4f9f38350a32967e0b460229b6f264493fb428be8ddfc
oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
prerequisite_019_bundle_sha256 = e12d4abd95a9b2ef49010a43d6b708824462a80dfed2548107e175920dabe481
```

The bundle recipe is stable_m5_digest(bundle_id,
TEXT("migrations/020_m5_semantic_readiness.sql"), HASH(migration_sha256),
HASH(prerequisite_019_bundle_sha256)). Literal acceptance is separate from
describing requested bytes. Initial f6bed0... / c5afd2... authority and its
diagnostics are historical, not approval of this corrected pin.

## Evidence distinction and execution history

Manual SQL header/edge fixtures test intact predecessor/015/deferred boundaries
and real S+K/work/timing persistence; they are schema-only evidence, not the
new private readiness runtime. Actual accepted acquisition, staging, closure
and cancellation helpers supply additional genuine predecessors, but their
readiness edge is still a schema adapter. Positive histories retain intact
triggers. A deliberately disabled internal FK trigger is one negative catalog
tampering fixture, alongside dropped constraints/PK/FK, renamed columns,
dropped/extra indexes, extra triggers/functions/relations and inherited-child
fixtures. Tampering occurs only in uniquely owned test schemas and is not a
positive runtime acceptance shortcut.

Earlier runs overlap: 161 pure digest/contract cases; 182 schema/digest passes
on the initial SQL; and 203 combined passes before final signature validation.
The first installer diagnostic rejected an incomplete expected catalog image;
accepted 017 installer-only settings and multiline private-function revocations
were added to its exact verifier, after which focused install/replay passed.
A race assertion initially expected a first-install error after a competitor
had already committed; it was corrected to the legitimate ledger-first conflict.

The initial parallel final-schema diagnostic returned 40 passes / four failures
in 72.18s: a broad pg_get_constraintdef predicate could inspect another schema
while it was dropped. The corrected SQL materializes schema/relation candidates
first. Both independent SQL reviews approve that narrow correction and reproduce
the new bundle hash; reversing it recovers the exact former SQL hash.

A -k selection intended for corrected raw-only diagnostics also matched parameter
IDs. The 28-pass report therefore included four positive installer/replay
fixtures and two negative installer cases before the new H32 correction was
pushed. That sequence violation is disclosed in H32; those executions are not
corrected-pin installer acceptance evidence. The provisional unpushed correction
doc commit ddc4d1f... and its false raw-only claim/receipts are superseded.
An explicit-node raw-only rerun returned 22 passes in 34.36s with no
installer/replay/unledgered case selected. All required installer acceptance
was rerun after the final corrected H32 push and verified dirty-worktree FF.

The first retained 016--019 run shared the schema-test database and returned
472 passes / 22 failures in 651.25s. Every failure was the retained 017 fixture's
whole-database before/after schema-inventory assertion during another suite's
concurrent schema creation/removal. Do not weaken that fixture or erase the
failures. A dedicated owned compatibility database isolates the final rerun.
An initial dedicated-database setup command omitted the container's /socket
flag, so creation failed and the premature pytest command saw missing-database
errors; that own process was terminated (exit 143, no accepted full result).
Setup was corrected to the explicit internal /socket and fresh dedicated
runs followed. An initially requested pure path tests/m5/test_digests.py did
not exist (exit 4, no tests); the corrected pure directory command ran normally.
Direct hatchling execution was unavailable; pip wheel's isolated build backend
builds the same unchanged package configuration without changing the protected
virtual environment. Retain these setup/selection errors as diagnostic history.

Post-corrected-H32 overlapping runs returned 49 schema passes and 816 pure
passes / two skips. The expanded first catalog-repair diagnostic returned 75
passes, but independent audits still found the attached-index/inheritance gaps;
that report does not validate final closure. The next 80-case diagnostic
returned 79 passes / one caller-shadow failure (230.59s); a separate two-case
shadow diagnostic returned one pass / one failure. PostgreSQL printed the same
stored operator as OPERATOR(pg_catalog.=) when shadowed. Canonicalization was
corrected while retaining actual dependency identities; the focused corrected
run returned two passes. A final identical-byte full rerun is recorded below.

The full dedicated retained 016--019 suite returned 494 passes after H32
(656.505s), then 494 passes again on the first structural repair (677.114s).
Both overlap; the second loaded pre-final closure code and is not a final-byte
new-validator claim. Final relevant 016/017, full 018 and full 019 compatibility
is rerun below. An initial explicit-node command misspelled the retained 015
hash test suffix as remain_exact rather than remain_unchanged (exit 4, no
tests); the corrected command is retained with final results.

## Final verification gates

Final complete eight-path manifest:
/tmp/groundloop-d32-s32-catalog-audit.wnTNk8/s32-final.sha256.
Both final same-byte audits and postcommit checks must refer to that complete
manifest, not earlier withdrawn preliminary manifests. Final executable source
SHA-256 is 6394a3700ba4b2dc9f925d841ddd070ff091da470438b6f986c752004c4f7b81;
owned schema test SHA-256 is
baaa68daca0ee9b943e5ae3dafa019db84c8e386cca18ebaf8ba6945ffac3460.

Final executable-byte verification:

- complete owned schema suite: 84 passed, zero failures/errors/skips, 210.785s;
- all applicable pure M5 tests: 816 passed / two skipped, 106.825s;
- retained compatibility: 63 passed, zero failures/errors/skips, 97.053s;
- compileall src/tests: PASS;
- strict mypy on the three changed source files: PASS;
- Ruff lint/format on six owned Python source/test files: PASS;
- git diff --check: PASS;
- wheel build and archive/module check: PASS, 151 entries, exact changed
  module bytes, no root migrations, secrets or bytecode;
- installed isolated wheel modules, enum/six-reference identity, independently
  framed Unicode readiness preimage/digest and supplied-byte 020 identity: PASS;
  missing default root migration assets reproduced.

Pure skips are the explicitly opt-in 100,000-event randomized gate and the
pinned WiCE-source reproduction gate (GROUNDLOOP_WICE_SOURCE_ROOT absent).
No new skip/xfail hides a schema/readiness failure.

```text
schema_report = schema-s32-final.xml
schema_report_sha256 = 47b8cb5d74c35bf271e215b56bba6bc205afbc2b0c9be85c1ed7ebfb17a49bf5
pure_report = pure-m5-s32-final.xml
pure_report_sha256 = 1a1eb020bcb6f109e5ed021f21559e844e6fd3295784a864048c36dc8783599a
retained_report = retained-relevant-s32-final.xml
retained_report_sha256 = b101440cce9d893a2e1211953d06fb43e8ebd0c8dce11fd654c811acd062aee6
wheel_sha256 = 447332f3d0eebead111d4e38af6da8dbc5579445c6f3143e437d02c132517dfc
```

Final commands use the protected environment interpreter with PYTHONPATH=src:.
and PYTHONDONTWRITEBYTECODE=1, the schema database URL below and PGOPTIONS=-c
jit=off. Schema: python -m pytest
tests/m5/postgres_runtime/test_migration_020.py -q --junitxml=<schema-report>.
Pure: python -m pytest tests/m5 --ignore=tests/m5/postgres
--ignore=tests/m5/postgres_runtime -q --junitxml=<pure-report>.
Static: python -m compileall -q src tests (external pycache prefix), mypy
--strict <three changed sources>, ruff check/format --check <six owned Python
paths>, git diff --check, and python -m pip wheel --no-deps --wheel-dir
/tmp/groundloop-d32-s32-package.sRkxwi/wheel-s32-final .
The final installed-module target is installed-s32-final in the same external
package directory. Earlier wheel
hashes/imports are overlapping diagnostics, not final-byte checks.

Retained final command uses the dedicated compatibility database and pytest
test_migration_018.py and test_migration_019.py in full, plus these exact nodes
(all under tests/m5/postgres_runtime):

```text
test_migration_016.py::test_root_result_contribution_accepts_exact_persisted_counts
test_migration_016.py::test_root_barrier_contribution_accepts_exact_completed_root_set
test_migration_016.py::test_verifier_completion_contribution_accepts_exact_result_artifact
test_migration_016.py::test_transition_timing_rejects_composite_anchor_mix_and_match
test_migration_016.py::test_timing_accumulator_insert_defers_exact_pending_anchor_closure
test_migration_016.py::test_timing_accumulator_rejects_count_or_pending_shape_mismatch
test_migration_016.py::test_migration_015_bytes_and_ledger_row_remain_unchanged
test_migration_017.py::test_identity_is_bound_to_exact_accepted_016
test_migration_017.py::test_d26_static_replacement_and_retained_trigger_inventory
test_migration_017.py::test_d26_partial_replace_seals_absent_predecessor_and_present_successor
test_migration_017.py::test_d26_complete_predecessor_closes_binding_and_retains_artifact
test_migration_017.py::test_d26_partial_retire_seals_from_reachable_bootstrapped_image
test_migration_017.py::test_d26_validator_catalog_is_atomic_and_replay_stable
```

Final breakdown: 016 selected 10 passes, 017 selected seven passes, 018 full
34 passes and 019 full 12 passes. This is a relevant compatibility gate, not
a claim that the final source reran all 494 broader cases.

The 63-pass final suite and broader 494-pass overlap are not summed as unique
tests. All new catalog positives/negatives and every 020 statement/ledger
rollback cut are included in the final 84-case owned schema suite. The final
byte manifests/audits, exact commit and postcommit checks are acceptance
receipts; no preliminary audit or earlier failure report is erased.

Test database use is restricted to the owned
groundloop-task16-runtime-socket-20261009 container, host socket
/tmp/groundloop-task16-postgres.1zFwTy, databases groundloop_task17_readiness
and groundloop_task17_readiness_compat. The user's groundloop-db-1 remains
stopped/untouched. No provider/model/API call or non-test migration occurs.
Use PYTHONPATH=src:. to avoid importing the protected primary checkout's old
editable modules; retain reports externally under
/tmp/groundloop-d32-sql-diagnostics.v9QrLA.

Root migrations are not included in the unchanged wheel configuration.
Archive/module checks cannot claim installed-wheel migration support; neither
pyproject.toml nor package assets are expanded by this activation.

## Next gate and claim ceiling

Only after scoped S32 acceptance and atomic push may R32 create its exact
four-path private-runtime lane from that pushed commit. It must implement
separate outer readiness commits, genuine owner/non-owner histories, original-
coordinate replay, rollback and races with one final work/timing CAS. Then the
held reader repair and C1 composition proceed strictly sequentially through
their independent barriers. No shared WIP is copied or silently integrated.

S32 candidate/schema diagnostics do not close D32 implementation, D24--D31
implementation evidence, Task 17/C1/Task 2 or M5.4--M5.6. Public workflows,
deployment, AI quality, independently adjudicated histories and end-to-end
utility remain PENDING. Default runtime remains v1_only outside isolated
fixtures. No security, novelty, model-quality, latency, cost or utility claim
follows from these schema tests.
