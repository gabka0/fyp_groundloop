# M5-D35 Sequential Transition-Access Implementation Activation

Date: 2026-10-15. Status: docs-only activation candidate. Source authority starts
only after two independent identical-byte GO/P0=0/P1=0 audits, exact sole-parent
commit, two fresh postcommit identity/custody checks, atomic no-force branch/main
push and verified live refs. This tranche executes no source/test/schema/DB/
container/provider/model work and advances no held checkout. Stop this turn
after accepted activation; implementation begins in a later continuation.

## 1. Literal accepted authority and docs ownership

```text
accepted_candidate = f9da529259c4983279b37e839416a3145b9b1f74
accepted_candidate_parent = a83e8c3085d3f496c83a11e0cab9ab5902223d3d
accepted_candidate_tree = a84ffcd04c2cffdf3d77590f98c2d26658c41d5b
accepted_freeze = 151fcd86c198d8ad516be6f55d45f9779f0e3494
accepted_freeze_parent = f9da529259c4983279b37e839416a3145b9b1f74
accepted_freeze_tree = 9d86e8d9ac8671a0581cdb128679cd589eac2c7f
amendment_sha256 = 8251885227c4a712c06d11ba48bcd2153764da28bbbba8b089f7b8c2ede560c8
runtime_addendum_revision = 16
original_c2_activation_sha256 = 0c07d7e0fa74d223a60e5f03f67aa1cc6f137ebc3f0d0a4585623f39ddffb179
status_delta_ownership_sha256 = 74963dc6e6879f71d49b481f30bc5cdc260406f08f12c5a7e83e60383b8baadc
docs_branch = workstream/m5-d35-transition-access-activation
docs_worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d35-transition-access-activation
sole_docs_path = docs/workstreams/m5_runtime_implementation/D35_TRANSITION_ACCESS_IMPLEMENTATION_ACTIVATION.md
```

Root alone owns this one NEW path. Independent A/B auditors own zero paths.
Candidate and ten-doc freeze each passed two complete same-byte and two fresh
postcommit GO/P0=0/P1=0 reviews before atomic publication/live-ref proof.
Freeze manifest SHA256
9d4150ce6d4878c8e00ec59a5a1eca596c2908af12b054eb53ce4c0ad1e4af7e;
receipts are in `/tmp/groundloop-d35-accept-activate.dH8PXcPv/` and the immutable
D35_CONTRACT_FREEZE_HANDOFF.md. Missing evidence stops work; never fabricate.
Accepted D24--D35 contracts/current AGENTS govern. This activation changes no
contract, matching semantics, persisted runtime identity, counter or public API.

## 2. Strict sequential ownership and barriers

```text
accepted activation -> S35 schema/admin/tests draft
                        -> H35 separate literal preinstall hash-freeze
                        -> same S35 checked advance/install/qualification
                    -> accepted/pushed S35 schema integration
                    -> SAME held C2 checked advance, root-only reader/glue repair
                    -> COMPLETE original C2 + D35 qualification/acceptance
                    -> SEPARATE future literal B33 parent-pin task
                    -> existing B33 -> P33 -> separately reconciled C1 barriers
```

Only ONE source lane/editor runs at a time. Root is that sole editor; A/B are
independent read-only reviewers, never matching/schema co-editors. S35/H35 are
disjoint from EVERY thirteen C2/five B33/nine C1 path in Sections5/7, not merely
the two runtime modules. No source lane inherits historical overlapping grants.
Each tranche freezes its complete external path/length/hash/blob/mode/index/raw
manifest, passes two full same-byte GO/P0=0/P1=0 audits, exact sole-parent commit,
two fresh postcommit checks, atomic no-force branch/main push and live-ref proof.
Any edit invalidates BOTH content verdicts. No partial source GO substitutes
for the complete tranche; no new N/A/xfail/skip/deselection waiver.

Every failed prerequisite stops BEFORE edit/install/advance/stage/push. No
reset/clean/stash/rebase/cherry-pick, overlapping merge, WIP copy/transplant or
checkout replacement. Unknown future commit/hash pins are later literal
receipt gates, never moving refs or invented values.

## 3. S35 exact five-path schema/admin/test lane

Only after this activation is accepted/pushed, record its literal commit/tree
from the fresh postcommit/publication receipt. Create NEW worktree
`/home/kassym/Desktop/groundloop-worktrees/m5-d35-schema-022`, branch
`workstream/m5-d35-schema-022`, initially from THAT literal activation commit,
not freeze/main/held C2. Root owns exactly:

1. `migrations/022_m5_transition_context_access.sql` (new)
2. `src/groundloop/postgres/migrations.py`
3. `tests/m5/runtime/test_transition_context_access_bundle.py` (new)
4. `tests/m5/postgres_runtime/test_migration_022.py` (new)
5. `docs/workstreams/m5_runtime_implementation/D35_SCHEMA_022_HANDOFF.md` (new)

No directory-wide grant. Migrations000--021, every held path, independent
oracles, matching/verifier/runtime/application/CLI, configuration/dependencies,
frozen docs and every other test are read-only. Do not import/copy B33's held
test_migration_021.py or any held C2/C1 fixture. Accepted-parent helpers may be
reused from this S35 checkout, never another WIP checkout/PYTHONPATH.

### 3.1 Exact SQL and existing-function limit

Implement exactly the six D35 SQL statements: the two eight-argument functions
groundloop_m5_matching_read_transition_context and
groundloop_m5_matching_read_transition_stage_journal, plus each REVOKE ALL and
GRANT EXECUTE TO PUBLIC. Exact attributes, ten result columns, trusted owners/
pinned search paths/ACLs/explicit pg_catalog binding and pg_temp data names
are mandatory. No third function/overload/alias, old body replacement, raw temp
or helper privilege, DML/backfill, trigger/table/index/type/column/default change.

Both accessors attest the complete common nonnull coordinate/OID/GUC/temp/
owner/backend/already-assigned full-XID/session/source/cardinality/flag closure.
Structural-open context expected=resulting1, even when intent predecessor>1;
completion/direct contexts N/N+1. Return actual complete fourteen-relation
journal values/multiplicity, including genuine empty projection. No caller key
filter/LIMIT/DISTINCT/repair/expected-set substitution, XID assignment, explicit
bridge locks or validation. Honest implicit read-lock and O(J+B) accounting.

In migrations.py, the ONLY existing function body grant is
`require_m5_document_update_completion_bundle`, signature unchanged. Add exact
022 successor dispatch: absent ledger AND both new names wholly absent -> old
strict021; exact022 -> full174 including exact021/transitive prerequisites;
malformed/conflicting/orphan/partial/extra -> fail without fallback/recursion.
All other existing function/class bodies, especially
`_m5_document_completion_verify_catalog` and all strict020 functions/installers,
remain byte-identical. Necessary imports and ONLY new administrative exports
in existing `__all__` are allowed; existing export entries remain unchanged.

New administrative public symbols are limited to
`M5TransitionContextAccessBundleError`, `M5TransitionContextAccessBundleIdentity`,
`M5TransitionContextAccessBundleInstallResult`,
`m5_transition_context_access_bundle_identity`,
`install_m5_transition_context_access_bundle`, and
`require_m5_transition_context_access_bundle`. New constants use
M5_TRANSITION_CONTEXT_ACCESS_* / M5_ACCEPTED_TRANSITION_CONTEXT_ACCESS_*;
new private helpers/types use `_m5_transition_access_*`. They implement only
the D35 bundle, source/statement validation, prerequisite, captured-schema,
closed catalog, ledger and installer surfaces. No new runtime/application port.

### 3.2 Exact installer, catalog and S35 qualification

Use D35's exact five-field021 prerequisite and new administrative bundle recipe.
Idle outer read-write READ COMMITTED only; ledger-first caller identity/replay
decision BEFORE content/prerequisite/catalog rejection or install locks. Absent
ledger validates reviewed bytes/prerequisites, then SHARE ROW EXCLUSIVE NOWAIT
ledger -> EXCLUSIVE NOWAIT runtime_epoch, rereads ledger, checks exact pre022,
executes six statements, verifies exact post022 and inserts ledger LAST. Native
ROW SHARE excludes first install during a matching phase. No mode activation.

New closed174 verification proves all old172 bodies/signatures/owners/flags/
search paths/ACLs and every nonfunction structural/dependency projection exact,
omitting ONLY the two exact new identities from the old function-identity
projection. Full174 proof cannot hide third objects, overloads or dependencies.
Keep old172 source digest8e8cfe6cbcf544d0e843bfa8189cb229573fa2b0cc597f84ff0a6d39139fb059
and structural27a02a8910e1a4b671b879cea1bdc0a5a35d9f693a870183355088990e207f62
under their existing framings. Never weaken the old strict checker to admit022.

S35 must individually map D35 Section8 groups1--8, applicable legacy/catalog/
dispatch/install aspects of10 and static/audit aspects of11: exact typed vectors,
all prerequisite fields; ledger-first invalid UTF8/hash/content cases; first/
exact/conflicting replay, orphan/partial/extra object, every rollback cut; both
installer/runtime race orders; exact owner/signature/config/default-ACL/catalog
tampering; all eight nullable inputs and coordinate/source/OID/GUC matrices;
genuine trusted owner AND distinct LOGIN context/journal positives and raw
table/private-helper denial; untrusted forged triplets including exact trueOID
settings; backend/full-XID/session/cardinality/flag/promotion/mixed-schema
negatives; repeatability/nonmutation/unassigned-XID rejection; all fourteen
native journal relations/fields/multiplicities and empty projection. Compare
actual native stage journal, never authored expected rows as actual evidence.

Use genuine native authorizer/context primitives; an isolated raw-SQL guard
fixture is labelled schema/guard evidence, NOT production C2 completion. Fresh
C2 end-to-end and its repaired native reader qualification remain Section6,
not a requirement to import/edit held C2 before schema integration. Record that
explicit deferred mapping, never promote M5.0-35 implementation for S35 alone.
Run complete owned/affected modules, retained021/020/019/017/016 and SQL-oracle
compatibility, pure M5/offline diagnostics, Ruff/format/mypy/compile/diff and
bounded package checks. Exact commands/counts/failures/skips stay separate;
known Dynagox provenance/packaging failures are not erased or repaired unowned.

## 4. H35 separate preinstall literal hash-freeze

S35 may author/review SQL and run labelled isolated raw-SQL diagnostic fixtures
before H35. The NEW production installer must NOT authoritatively accept022
until reviewed literal pins are independently frozen. Runtime acceptance may
not learn authority from caller bytes/current catalog. No invented future hash.

After exact SQL authoring/review and full five-path S35 draft checkpoint, create
NEW docs-only worktree `m5-d35-schema-022-hash-freeze`, branch
`workstream/m5-d35-schema-022-hash-freeze`, sole parent the literal accepted
activation commit (verify still the published main; otherwise stop for new
ancestry authority). Root owns ONLY NEW
`docs/workstreams/m5_runtime_implementation/D35_SCHEMA_022_HASH_FREEZE.md`.
S35 source edits pause during this docs tranche. Pin reviewed exact022 bytes/
SHA256, canonical bundle/five fields, exact021/transitive prerequisite pins,
two new bodies/signatures/attributes/config/ACLs, old172 preserved projection
and full174 structural/dependency catalog identities under explicit framing.
Fixture-derived diagnostics must be independently checked against reviewed
SQL/contracts before pinning, never automatically promoted to accepted truth.

H35 requires its own two complete same-byte GO/P0=0/P1=0 audits, sole-path
commit, two fresh postcommit checks, atomic push/live refs. Its sole path must
be disjoint from all five S35/thirteen C2/five B33/nine C1 paths. Record literal
accepted H35 commit/tree. Verify the single linear incoming H35 commit, exact
endpoint/touched-path set and full S35 draft pins/index/raw/protected custody;
ONLY THEN same S35 may `git -c merge.autostash=false merge --ff-only <H35>`.
Recheck complete pins/index/raw afterward. S35's final source sole parent is
that accepted H35 commit. Add literal accepted constants within owned S35;
first authoritative installation/qualification may now proceed. Changed SQL/
catalog pins require a separately audited docs-only correction BEFORE renewed
authoritative installation, with identical checked-advance/custody barriers.
This grants neither an early installer success nor a held C2 advance.

## 5. Same-held-C2 conditional resumption and exact thirteen paths

Only AFTER S35's COMPLETE five-path candidate/qualification has both source
GO/P0=0/P1=0 audits, exact sole-parent commit, two fresh checks and atomic push/
live proof may root record that literal accepted schema-integration commit/tree.
Do not create another C2 checkout. Existing C2 remains:

```text
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d34-requirement-completion
branch = workstream/m5-d34-requirement-completion
held_HEAD = a83e8c3085d3f496c83a11e0cab9ab5902223d3d
held_tree = acf012014329aa2fcc60b1f17722dea5034ceea4
full_manifest = /tmp/groundloop-d34-c2-source.20lCffBL/FINISH_HELD_CHECKPOINT.json
manifest_sha256 = c64f9e7630bb1a249cb5214fba7d9c3cc02034c823f062ede2d60a22ad9d0df8
raw_v2_z_sha256 = 5db0d87bc0773130afa45f11b86f637ec35a632ab7591fbae2cee206c1e66d4b
index = EMPTY
status = 4 modified / 9 new; UNACCEPTED_P1_STOP
```

Before any advance, check ALL thirteen full bytes/lengths/lines/hashes/blobs/
modes, HEAD/branch/index/raw and protected custody. Verify the ENTIRE incoming
ancestry from a83e8c3: linear sole-parent commits only, endpoint diff AND union
of paths touched by EVERY commit disjoint from ALL thirteen C2/five B33/nine
C1 paths. Stop on any unexpected merge/overlap/receipt loss. ONLY THEN perform
`git -c merge.autostash=false merge --ff-only <literal accepted S35 commit>`
in the SAME C2 worktree. Verify every full held pin/index/raw unchanged after;
record a NEW advance receipt with changed HEAD/tree, preserving prior immutable
checkpoint, not silently changing its expected HEAD to make an old check pass.
Unowned files must equal the new accepted schema parent. C2's eventual source
sole parent becomes that literal accepted S35 commit; never moving main/H35.

Root then owns exactly the original thirteen paths, enumerated anew:

1. `src/groundloop/m5/runtime/postgres_verifier.py`
2. `src/groundloop/m5/runtime/postgres_matching.py`
3. `tests/m5/postgres_runtime/d25_store_core/conftest.py`
4. `tests/m5/postgres_runtime/d25_store_core/test_d28_phased_composition.py`
5. `tests/m5/runtime/test_d34_archive_contract.py`
6. `tests/m5/postgres_runtime/d34_requirement_completion/conftest.py`
7. `tests/m5/postgres_runtime/d34_requirement_completion/test_active_completion.py`
8. `tests/m5/postgres_runtime/d34_requirement_completion/test_archive_order.py`
9. `tests/m5/postgres_runtime/d34_requirement_completion/test_replay.py`
10. `tests/m5/postgres_runtime/d34_requirement_completion/test_races.py`
11. `tests/m5/postgres_runtime/d34_requirement_completion/test_accounting.py`
12. `tests/m5/postgres_runtime/d34_requirement_completion/test_history.py`
13. `docs/workstreams/m5_runtime_implementation/D34_REQUIREMENT_COMPLETION_HANDOFF.md`

### 5.1 Existing-function grants and narrow D35 extension

Original verifier grants remain limited to `_persist_verifier_closure`,
`_insert_attempt_result`, `_validate_verifier_closure_replay`,
`_derive_verifier_completion_work`, `_validate_applied_replay`, and
`complete_m5_verifier` (same signature/legacy all-active blocker). Original
matching grants remain `_requirement_completion_source`,
`_capture_requirement_currency_before_image`, `_requirement_header_after_images`,
`_lock_requirement_replay_source`, and narrow factoring of `_lock_replay_envelope`
and `_read_matching_transition_replay`. Existing `_RequirementCompletionSource`
and `_requirement_logical_transition` retain ONLY the accepted status-delta
amendment's one private structural_event_id field/two event-ID selections,
unchanged attempts/reasons/retained bytes; no further semantic rewrite.
Original NEW `_d34_*` helpers/types/constants and imports remain owned solely
within the original archive/source/replay/accounting/outer-owner scope.

D35 extends existing matching-body ownership ONLY by `_capture_cursor_binding`
and `_validate_matching_stage_journal`. Replace their inaccessible SELECTs
with exact schema-qualified eight-argument bridges only on exact022; keep all
native prechecks, cardinality/ten-field binding and full native journal planned
key/multiplicity/before/final/op/count/flag comparison unchanged in meaning.
NEW matching `_d35_*` private reader/schema-ledger helpers and necessary imports
may provide bounded exact ledger/two-bridge metadata checks and safe calls.
Do not change `_CursorBinding`/prepared DTO fields, derive/apply bodies or public
exports, native prepare/stage/finalizer algorithms, lock order or source hash.

In the existing owned NEW verifier helper `_d34_complete_verifier_cursor`, add
ONLY the fresh-active exact022 prerequisite before its sole fresh authorizer
and any source mutation. Retained winner classification remains first; missing/
bad022 must fail even owner fresh active, not inactive/late/native replay.
Necessary owned private helper/import glue may call the checked same-schema
prerequisite. Full174 proof belongs to admin/strict prerequisite boundaries;
matching phases use bounded points, not whole-catalog/history scans. Legacy
owner020/021 readers work only when both022 ledger/names are absent; no privilege
exception fallback, cached token, cursor attachment or mixed-schema selection.

Store-core conftest edits stay `_lock_requirement_completion_source_prefix`,
necessary `authorize_and_derive_requirement_completion` glue/imports/affected
requirement fixture plumbing; D28 phase test edits stay
`test_requirement_source_and_phase_trace_obeys_the_d28_order` assertions only.
Retain all other cases/complete modules. C2-owned new conftest `c2_db` may add
exact022 to its native install chain; its other fixture builders/classes and
new owned tests may receive only D35 qualification/prerequisite glue and
remaining original C2 falsifiers. No imported held B33/C1 WIP or fabricated
source/ready header. Handoff maps all exact final commands/evidence.

Every other existing function/class body remains byte-identical to its accepted
parent. In particular `_capture_direct_context_binding`, public derive/apply,
`persistence.py:complete_m5_verifier_atomically` and its all-active blocker,
recovery/readiness/publication/application/CLI and all migrations are unowned.
An indispensable extra existing function/path stops for a written separately
audited activation amendment BEFORE editing; never broaden by helper renaming.

## 6. Complete C2/D35 qualification, then its own acceptance

Run the retained normal fresh-LOGIN failure unchanged: owner fresh A and LOGIN
retained A are not sufficient; genuine LOGIN fresh B must now commit/reconnect/
replay without raw grants/SET ROLE/forged context/early constraints. The current
rollback assertion remains, no xfail/skip/error swallow or owner-only substitute.
Add missing/malformed022 owner AND LOGIN fresh-active negatives proving failure
before authorization/source mutation; preserve inactive/late/retained behavior.
Exercise bridge route/schema phase stability and installer races using actual
held runtime lock; test genuine native structural-open predecessor>1 and all
context shapes without claiming direct pre-source caller repair.

Complete ALL original D34/C2 gates and D35 Section8 groups9--11 plus applicable
integrated1--8 regressions: SUPPORT/REFUTE/NEUTRAL, nonzero actual structural-
event deltas, all15 source fields/global aliases/physical collision order/P-only
reuse, no source relock, common active/inactive prefix, complete planned headers,
exact actual producer byte work/two contributions/one CAS/timing anchors,
guards-last, every archive/later mutation rollback/reconnect, latest/older
zero-write replay/current accumulator, both serializer orders/locked loser/
representative immediate-FK race, hostile schema/temp/OID/role/full-XID/flags,
actual full native14-relation journal planned-key comparisons and nonmutation.

Before seal independently derive expected currency from genuine before-state
plus validated completion source, THEN compare full working matching/state
with independent Python/SQL recomputation. After commit/reconnect compare full
images/receipts; no shared wrong-currency oracle shortcut. Keep controlled
REGISTER/new-positive baseline labels and all original carry/absence/new-positive/
independent group-image gates. No oracle/full hydration/aggregate enters measured
production transactions. Deterministic stamped judgments test structure, not AI.

Run COMPLETE affected native/C2 modules, D25 store-core/publication, D24 recovery/
race/terminalization, migrations/SQL-oracle/live compatibility, pure M5, whole
offline diagnostics and static/typing/compile/package checks. Exact overlapping
counts/failures/skips are reported separately, not summed into a milestone PASS.
Known provenance/packaging limitations stay visible; no unowned repair or hidden
deselection. Scoped S35 success is prerequisite evidence, not final C2 success.

Live implementation tests only: fresh inventory of the approved test PostgreSQL
instance, specifically owned isolated fixture DB/roles/schemas with explicit
name/OID custody and reversible narrow cleanup. Preserve all pre-existing DBs/
roles/clients/schemas and user-stopped container; no user DB writes/reset/copy,
broad deletion, dependency installation, external provider spend/model run.
Accepted022 may be installed only after H35 inside owned fixtures. Old020/021
compatibility tests remain separate exact old-schema fixtures.

Freeze the COMPLETE thirteen-path final C2 candidate, obtain two full identical-
byte GO/P0=0/P1=0 source audits, exact sole-parent commit, two fresh identity/
custody checks, atomic no-force source-branch/main push and live proof. No partial
exploratory pass or owner/replay-only review accepts C2. Keep D25/D34/D35
cross-stage decision mapping/public C1 acceptance pending until their real gates.

## 7. Protected downstream paths and separate B33 authority

B33 remains at4f50dc6701b462024e125a45505eb7be17bd4112, three modified/two new,
empty index, full attempt2 manifest SHA256
0699d07c659a80802e623af59ab55bbdf62c1d89aa8f36fb977526751b992552,
raw SHA256fda319f391dd9aaf65d38791440c2d6e9737fa4da2c7dba0894b0b7dc36422e1.
Its five held paths (unowned by S35/H35/C2) are:

- `src/groundloop/m5/runtime/postgres_matching_publication.py`
- `tests/m5/postgres_runtime/d25_publication/test_seal_promotion.py`
- `tests/m5/postgres_runtime/d25_publication/test_positive_promotion_journal.py`
- `tests/m5/postgres_runtime/test_migration_021.py`
- `docs/workstreams/m5_runtime_implementation/D33_MATCHING_PROMOTION_REPAIR_HANDOFF.md`

C1 remains at8290d47cba71350cc550cf18cb12c32b47cb7b2a, empty index, raw SHA256
736f23f62b8ae744763219ab592266318662636dec4bcbc8a7be93e56ed0d58d.
All nine current full pins in accepted D33 handoff SHA256
50caf608e3d1ab059ae9217d615b25a3c3f4768875eaaf8722a348e6397cfa48 govern:

- `src/groundloop/m5/runtime/persistence.py`
- `src/groundloop/m5/runtime/postgres_recovery.py`
- `tests/m5/postgres_runtime/d30_application/conftest.py`
- `tests/m5/postgres_runtime/d30_application/test_seal_atomicity.py`
- `tests/m5/postgres_runtime/d30_application/test_store_composition.py`
- `tests/m5/postgres_runtime/d30_application/test_store_races.py`
- `tests/m5/postgres_runtime/d30_application/test_structural_open.py`
- `tests/m5/postgres_runtime/d30_application/test_structural_order.py`
- `docs/workstreams/m5_runtime_implementation/D30_STRUCTURAL_COMPOSITION_HANDOFF.md`

Check all full contents/modes/blobs/HEAD/branch/index/raw, five protected
inventories, nine accepted prerequisites, AI WIP/primary local main/R32/R-T
and old C2 draft at each barrier. No advancing B33/C1 under this activation.
Only AFTER accepted/pushed C2 may the still-SEPARATE docs-only task allocate
`docs/workstreams/m5_runtime_implementation/D34_C2_B33_PARENT_PIN.md` in a NEW
worktree with the literal accepted C2 commit as sole parent. This activation
prescribes that barrier but grants neither its future write nor B33 advance.
That task requires its own complete audits/commit/fresh checks/push/live proof
and literal B33-final-parent pin. Then reapply original D34 Section6 whole-
history disjointness/custody/checked-FF and full B33 qualification. No mandatory
carry/absence/new-positive or independent full group postseal gate is waived.
P33 follows accepted full B33; C1 needs NEW separately audited overlap/
reconciliation authority, including its earlier persistence.py overlap.

## 8. Claim ceiling

This activation implements nothing and records no new numerical test result.
D35/M5.0-35 contract-PASS / implementation-PENDING; additive022/two trusted
bridges/closed174 remain NOT IMPLEMENTED at this docs boundary. Held C2 is
UNACCEPTED_P1_STOP until subsequent qualified repair/acceptance. D25/D34/D35
implementation mapping, B33/P33/C1/C2, Task2/Task17, M5.4--M5.6, deployment,
security, AI quality and end-to-end utility/speed/cost remain PENDING; runtime
stays v1_only outside isolated fixtures. Independent adjudicated natural
histories and full end-to-end evaluation remain mandatory separate work.
