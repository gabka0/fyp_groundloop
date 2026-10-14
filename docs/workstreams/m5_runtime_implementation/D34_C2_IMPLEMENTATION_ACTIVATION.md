# M5-D34 C2 Private Requirement-Completion Activation

Date: 2026-10-14. Status: **docs-only activation candidate**. No source work
starts until two independent identical-byte GO/P0=0/P1=0 audits, exact
sole-parent commit, two fresh postcommit identity checks, atomic no-force
branch/main push, and verified live refs. This docs tranche executes no tests,
DB/container/provider operation or implementation.

## 1. Exact authority and ownership

```text
accepted_freeze = c7bd59d143991ce62d88693d422ac59175d7a0a0
accepted_freeze_parent = f372f2bf94ac5527609a2595deaed50b9607e22a
accepted_freeze_tree = d95b8d682f3108c98e654acd1c5dd74a64e6b5c9
amendment_sha256 = 1fade6fd70d7bce5a4548bf1f0d37ed0682ce185f0c36089df6dd0dd8977ef6b
runtime_addendum_revision = 15
docs_branch = workstream/m5-d34-c2-implementation-activation
docs_worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d34-c2-implementation-activation
sole_docs_path = docs/workstreams/m5_runtime_implementation/D34_C2_IMPLEMENTATION_ACTIVATION.md
future_source_branch = workstream/m5-d34-requirement-completion
future_source_worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d34-requirement-completion
```

The freeze had two complete same-byte and two fresh postcommit GO/P0=0/P1=0
reviews; root atomically pushed freeze branch/main and verified both exact refs
before this activation worktree was created. D24--D34 and current AGENTS govern.
This activation allocates implementation only; it changes no contract, public
API/DTO, persisted identity, schema, counter definition, provider or default.

Root owns only this new document during the docs tranche. Two independent
read-only auditors own zero paths. After acceptance/push, pin this activation's
literal commit/tree from its audited postcommit receipt as the future source
lane's sole parent BEFORE creating that NEW worktree. Do not use the freeze
parent directly, a moving main, a historical C2 grant, or a held checkout.
Root is the sole source editor; at most these two zero-path auditors accompany
it. No parallel source lanes, source edits during docs acceptance, or inherited
ownership. Source implementation begins in the subsequent implementation lane.

## 2. Qualified surface and explicit public ceiling

Implement a package-private connection-owning requirement-completion entrypoint
in `postgres_verifier.py`, not a new public method/export/protocol. Reject a
non-IDLE/closed/errored/busy/ambient/nested/savepoint invocation before business
work, own one top-level read-write READ COMMITTED transaction, verify actual
server settings and permanent-schema authority, and bind private phases to the
exact cursor/connection/backend/full transaction ID/epoch/revision. No internal
nested transaction, provider call, implicit cursor attachment or global token
registry. Caller-authored skip/ownership booleans are not authority.

Reject configured incompatible isolation/read-only settings rather than
silently overriding caller defaults. Capture one verified permanent schema and
ensure every persistent query/function resolves there using accepted trusted
schema primitives and a transaction-local checked namespace; temp relations
or a hostile/multiple-schema search path cannot supply authority. Preserve the
genuine native temp journal context and its OID/role/backend checks. No schema
object, ACL or global session setting is changed; no security claim follows.

The protected `persistence.py:complete_m5_verifier_atomically` wrapper stays
unchanged, with its legacy all-active blocker intact. Existing cursor entry
signature and valid non-active/late behavior remain compatible. The private
owned route must not be advertised as public store/application wiring; C1
integration remains separately held. Any need to edit that wrapper, recovery,
application/CLI, exports, or another path stops for new ownership authority.

## 3. Exact future thirteen-path manifest

Root may edit/create only these paths after the accepted activation barrier:

1. `src/groundloop/m5/runtime/postgres_verifier.py`
2. `src/groundloop/m5/runtime/postgres_matching.py`
3. `tests/m5/postgres_runtime/d25_store_core/conftest.py`
4. `tests/m5/postgres_runtime/d25_store_core/test_d28_phased_composition.py`
5. `tests/m5/runtime/test_d34_archive_contract.py` (new)
6. `tests/m5/postgres_runtime/d34_requirement_completion/conftest.py` (new)
7. `tests/m5/postgres_runtime/d34_requirement_completion/test_active_completion.py` (new)
8. `tests/m5/postgres_runtime/d34_requirement_completion/test_archive_order.py` (new)
9. `tests/m5/postgres_runtime/d34_requirement_completion/test_replay.py` (new)
10. `tests/m5/postgres_runtime/d34_requirement_completion/test_races.py` (new)
11. `tests/m5/postgres_runtime/d34_requirement_completion/test_accounting.py` (new)
12. `tests/m5/postgres_runtime/d34_requirement_completion/test_history.py` (new)
13. `docs/workstreams/m5_runtime_implementation/D34_REQUIREMENT_COMPLETION_HANDOFF.md` (new)

All thirteen are disjoint from every held B33 and C1 path. No directory-wide
grant. Configuration/dependencies, migrations000--021, frozen contracts,
independent oracles, `postgres_recovery.py`, `persistence.py`, publication/
readiness/withdrawal modules, model/AI worktrees and all other tests are read-only.

### 3.1 Bounded source functions

In verifier, existing-function edits are limited to `_persist_verifier_closure`,
`_insert_attempt_result`, `_validate_verifier_closure_replay`,
`_derive_verifier_completion_work`, `_validate_applied_replay`, and
`complete_m5_verifier` for common-source/inactive rearrangement while preserving
its signature and legacy all-active blocker. Necessary imports/local types and
NEW `_d34_*` private helpers may implement only the enumerated outer ownership,
schema capture, archive mapping/preflight/persistence, active completion,
planned-work, ordered accounting/header/job/delta phases and retained routing.
Existing contribution writers may be reused without changing their recipes.

In matching, existing-function edits are limited to
`_requirement_completion_source`, `_capture_requirement_currency_before_image`,
`_requirement_header_after_images`, `_lock_requirement_replay_source`, and
narrow private factoring of `_lock_replay_envelope` /
`_read_matching_transition_replay`. NEW `_d34_*` private helpers may supply
one canonical archive-alias/map/reservation checker, complete source validation
and explicit held-prefix replay continuation. No general matching algorithm,
representative rule, structural/direct path or artifact recipe is reopened.
Keep public `derive_matching_transition_intent` and `apply_matching_transition`
signatures/bodies and `__all__` unchanged; reuse native prepare/stage/finalize
and shared retained-byte validators/receipt construction.

Preserve every unowned existing function byte-for-byte. Necessary module
imports and NEW `_d34_*` private types/constants may be added; existing public
types are unchanged. Any indispensable extra function/path becomes a
written activation amendment and two independent audits BEFORE editing it.

### 3.2 Retained-test correction, not silent deselection

In the existing store-core conftest, own only
`_lock_requirement_completion_source_prefix`, narrowly necessary
`authorize_and_derive_requirement_completion` call glue/imports and directly
affected requirement fixture plumbing. In the existing D28 phase test, own
only requirement-source/order assertions. Replace its obsolete early-plus-
repeated observation locking with genuine D34 source-prefix/no-relock evidence.
Retain all other test cases/assertions and complete module execution. No legacy
production flag, weaker fixture, fake source or deselection may preserve the old
trace. Other modules inheriting the fixture must run unchanged.

## 4. Required source sequence

1. Acquire real tier5/6 serialization and use the AFTER-WAIT revision. Under
   the checked read envelope, classify the complete retained D24/D25 tuple
   before fresh authorizer/tier7+ acquisition; stale pre-lock revision hints
   are not CAS authority. A committed winner takes native retained replay.
2. For fresh active work, invoke the single D25 transition authorizer at6,
   then validate/hold the complete frozen activity/snapshot/job/attempt/output/
   dispatch/model/provenance prefix. No archive/accumulator mutation before its
   applicable authorization and complete key validation.
3. Implement D34's complete global R/P/V/O/E alias set, separate D34A
   namespace1144206401, accepted private framing/hash map, sorted/deduplicated
   ACTUAL physical IDs and complete preflight before row locks. R/V/O/E absent,
   P-only exact reuse; result -> pair -> artifact -> observation -> execution
   with immediate FKs. Foreign/partial occupancy after fresh prefix conflicts,
   never repairs, salted identities or late replay. Both new active/inactive
   source writers use this common prefix before later accounting.
4. Matching independently reconstructs native aliases, checks actual current-
   transaction/reservation authority and all15 source-observation fields, then
   excludes ONLY that exact new source from physical11a reacquisition. Test the
   native full-XID/row freshness logic; raw xmin equality alone is not a general
   commit/history oracle. Missing prefix or caller flags fail closed. Old
   holders/currency remain at11a; frozen intent/patch fields stay unchanged.
5. Sole public derive, native private recomputation/prepare, complete planned
   work and headers precede one base-then-runtime N->N+1 advance. Stage native
   currency/state/certificates, then terminalize held job/attempt. Replace the
   aggregate header helper with checked point counters and the exact one-job
   decrement; no job/scope scans or revision-only intermediate header.
6. Execute15a/b/c then both sorted15d execution/completion contributions,
   one15e sum/CAS,15f attempt timing,15g prior-missing anchor, one15h timing CAS,
   native15i/j/k finalizer, exact16 deltas and accepted guards last. Reuse
   immutable recovery builders/readers and suitable private producers only.
   Its bundled start/persist/finish wrappers cross subtiers and are NOT active
   phases. No recovery source edit or double anchor/count is authorized.
7. Active pre-header work must come from genuinely produced known/planned
   typed bytes, not guessed future timestamps/postterminal JSON. Count actual
   new alias serialization/hash work once as verifier_completion, including
   distinct logical hashes whose physical IDs collide. Keep D24/D25 ownership
   and contribution versus current-accumulator distinction exact.

Retained replay uses shared native terminal source9/10 -> image11b ->15i/j/k,
ordinary immutable pair/observation reads and zero DML/constraint forcing/
calls/work. A private held-prefix continuation may accept ONLY internally
issued sealed exact-cursor/connection/backend/full-XID/epoch/current-revision
authority, not a skip flag or GUC-only claim. Do not reacquire earlier keys or
create a fresh transition context after acquiring later replay keys. The public
apply path remains unchanged. Preserve inactive/late/cancelled/expired/failure/
loser dispositions and cutoff accounting without claiming failure coordination.

## 5. Mandatory qualification and fixtures

All D34 Section7 falsifiers must execute and be individually mapped in the
handoff. Suggested allocation, not permission to omit a case:

| Owned test | Mandatory coverage |
| --- | --- |
| Pure archive contract | All global aliases, native typed encoding/physical-ID sorting/hash collisions, namespace separation,15-field mutations, private authority/cross-cursor/transaction rejection |
| Active completion | Genuine SUPPORT/REFUTE/NEUTRAL, no forced SUPPORT key, full source/working state, complete headers, zero parent refutation from requirement REFUTE |
| Archive order | Actual complete SQL/implicit FK order, R/V/O/E collisions and P reuse, missing prefix/FK/partial archive, schema/temp/search-path and read-only/isolation negatives, no source relock or aggregate scan, corrected retained D28 trace |
| Replay | Latest/older/reconnect receipts and current retained accumulator, zero durable writes/calls/accounting, no observation reservation/lock, no archive repair |
| Races | Separate connections, both fresh active/inactive orders, locked-loser AFTER-WAIT classification, cross-epoch aliases, artifact/execution inversion, representative-certificate immediate-FK scenario |
| Accounting | Exact separate D24/D25 work, actual producer bytes, both contributions/one CAS, observed/missing timing and prior anchor, every archive/later mutation rollback cut |
| History | Genuine closure/currency baseline, independently expected currency plus full working-image Python/SQL comparisons after commits; new-only controlled baseline and later document updates |

Use deterministic stamped test results for structural correctness, not live
provider requests or AI-quality claims. Do not import/copy held B33/C1 modules,
fixtures or WIP; imports must resolve to this accepted-parent source worktree.

The new fixture folder may build a labelled guarded controlled REGISTER
boundary from accepted native primitives, NOT public C1/P33 composition. For
baseline publication with the accepted old publisher, prove every positive key
in all four matching families is absent from current, and inspect actual
first_old=NULL/INSERT journal and physical receipts. Publish genuine C2
currency/history plus lifecycle/state, M4/M5 heads, children, work/timing and
results; force accepted guards last, commit and reconnect. Existing controlled
registration helper alone omits new C2 currency; REPLACE/RETIRE boundary is not
a registration helper. Never author completed headers, bypass privileges or
call a primitive publisher proof public completion.

Before seal, independently compare expected currency from before-state plus
the validated completion source, THEN full working matching/state against
independent Python/SQL recomputation. Two oracles using the same wrong stored
currency are insufficient. Audits remain outside measured transactions; no
full hydration/oracle/aggregate enters production. Distinguish this bounded
source-fold check from raw-history folding and neural truth.

Later B33 equal-value carry needs distinct A/B pairs sharing one normalized
hash: genuine baseline A SUPPORT/B NEUTRAL; withdraw neutral/candidate chunk C
to generate the real fallback, then A->NEUTRAL and B->SUPPORT. Keep another
requirement unsupported to avoid P33 certificate dependence. Baseline requires
real preceding C2 executions/artifacts/currency, not bootstrap observations.
This is a pending fixture/history gate, not a claim it passes today. No repeated
same-pair child/retry/replay can fabricate the carry.

Run complete affected modules, D25 store-core/publication and D24 recovery/
race/terminalization compatibility, relevant migrations/SQL oracle/live gates,
pure M5, whole offline diagnostics, Ruff/format/mypy/compile/diff and bounded
package checks. Record commands, exact counts, failures/skips separately.
Known Dynagox provenance failure and packaging limitations remain explicit;
do not repair unrelated paths or claim all-green results. No test deselection
to hide a newly introduced incompatibility.

Live tests, only in the later source lane, may use a specifically owned isolated
fixture DB/roles on the approved test PostgreSQL instance after fresh inventory.
Preserve all pre-existing DB name/OIDs, roles, clients, schemas and user-stopped
container. No user DB write/reset/copy, broad deletion, schema/migration content
edit, new dependency, external provider/API spend or model run. Accepted
migrations may be installed only inside the owned fixture. Exact021 is required
for new document histories; preserve old020 compatibility and strict checks.

## 6. C2 acceptance then a SEPARATE literal-parent-pin task

Freeze the COMPLETE thirteen-path source candidate only after applicable gates
pass; two independent same-byte GO/P0=0/P1=0 reviews, exact sole-parent commit,
two fresh postcommit checks and atomic no-force source-branch/main push are
required. No partial-module GO or reused exploratory review is acceptance.
Report private owned-completion evidence only; keep public/C1 and decision-row
cross-stage implementation mapping pending. Stop for any unowned prerequisite.

Only after accepted/pushed C2 may a SEPARATE docs-only task allocate the sole
new path `docs/workstreams/m5_runtime_implementation/D34_C2_B33_PARENT_PIN.md`
in its own clean worktree, sole parent the literal accepted C2 commit. That
task must record literal C2 commit/tree/qualification/audits and complete B33/
C1 custody; get two complete-byte audits, commit, two fresh identity checks,
atomic branch/main push and live-ref proof. THIS activation prescribes the
barrier but grants neither that future write nor a B33 advance automatically.
No unknowable future commit hash is supplied here.

The separately accepted/pushed parent-pin commit, recorded literally by its
postcommit receipt, is B33's final sole parent. Before advancing the EXISTING
B33 worktree, verify the complete linear ancestry from4f50dc6 to that exact pin,
the endpoint diff AND union of paths touched by EVERY upstream commit against
ALL five B33 and ALL nine C1 paths. No unexpected merge or overlap is allowed.
Recheck full five-file bytes/modes/blobs, HEAD/branch/empty index/raw inventory
before and after a checked `git -c merge.autostash=false merge --ff-only <pin>`.
No stash/reset/clean/rebase/cherry-pick, other-lane merge or WIP transplant.
Every failed prerequisite stops before further action.

Only then may the existing B33 five-path diagnostic lane resume its original
qualification with genuine C2 histories and unchanged mandatory gates. Its
later edited candidate still needs a new complete checkpoint, two final byte
audits, exact sole-parent commit, fresh checks and atomic push. No carry/absence/
new-positive or independent full group postseal image gate is waived. Additional
N/A requires both final auditors' exhaustive producer/guard reachability proof,
not a pending fixture or a repeated-pair fabrication.

Comparing this NEW upstream delta with C1 pins is preservation only. O33 already
overlapped C1's `persistence.py`; this does not repair C1's entire8290->current
disjointness or grant its resume. P33 follows full B33; C1 still requires a
NEW separate audited overlap/reconciliation activation afterward.

## 7. Protected checkpoints and evidence ceiling

B33 stays at4f50dc6701b462024e125a45505eb7be17bd4112, empty index, three
tracked changes/two untracked; full attempt2 manifest SHA256
0699d07c659a80802e623af59ab55bbdf62c1d89aa8f36fb977526751b992552,
raw-v2-z fda319f391dd9aaf65d38791440c2d6e9737fa4da2c7dba0894b0b7dc36422e1.
All five full path/byte/hash/mode pins and the immutable nine C1 pin table are
in the accepted D34 handoff. Verify them, five protected inventories and nine
accepted prerequisites before every mutation/acceptance/advance. Old C2 draft
SHA64be6f28b586743047063771de0026f9774bc83470fb938d55407c76bc94c720
remains unchanged/uncommitted; original D34/freeze bytes remain immutable.
Unavailable October11 receipts are not recreated. Current temporary evidence
availability is rechecked, never assumed durable.

This activation itself supplies no source/test/database evidence. D34/D25 full
implementation mapping, C2/public routing, B33/P33/C1, Task2/Task17,
M5.4--M5.6, deployment, AI quality, independent natural histories and end-to-end
utility/speed/cost remain PENDING. Runtime stays v1_only outside isolated
fixtures. No objective-truth/security/novelty/superiority claim follows.
