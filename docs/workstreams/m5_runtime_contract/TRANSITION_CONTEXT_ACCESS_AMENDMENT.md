# M5-D35 Transition Context Access Amendment

Status: **docs-only contract candidate; not frozen authority or implementation
permission**. Date: 2026-10-15. Proposed decision: M5-D35 / M5.0-35.

User approval covers drafting and two independent audits of identical complete
bytes. This tranche does not authorize stage, commit, push, authority-freeze
updates, migration installation, source/test edits, or held-worktree advancement.
All implementation and existing acceptance gates remain PENDING.

## 1. Exact candidate and protected evidence

```text
sole_parent = a83e8c3085d3f496c83a11e0cab9ab5902223d3d
parent_tree = acf012014329aa2fcc60b1f17722dea5034ceea4
branch = workstream/m5-d35-transition-context-contract-candidate
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d35-transition-context-contract-candidate
sole_owned_path = docs/workstreams/m5_runtime_contract/TRANSITION_CONTEXT_ACCESS_AMENDMENT.md
runtime_addendum_revision = 15
d31_sha256 = 6331c8149e38031c51cb22b6a9dc2d49d30d27df67d25b5fae24b00c52058dc9
d33_sha256 = 055ef0d6f971a23be31cc23d2b9f03b4b7cd17fa9f9bc7c505d1f565d6726b32
d34_sha256 = 1fade6fd70d7bce5a4548bf1f0d37ed0682ce185f0c36089df6dd0dd8977ef6b
c2_activation_sha256 = 0c07d7e0fa74d223a60e5f03f67aa1cc6f137ebc3f0d0a4585623f39ddffb179
c2_event_id_ownership_sha256 = 74963dc6e6879f71d49b481f30bc5cdc260406f08f12c5a7e83e60383b8baadc
```

Root owns only this new document. Existing independent auditors A/B own zero
paths. All other docs, source, tests, migrations000--021, held checkouts,
databases, containers, providers and model runs remain read-only. Current main
was checked live at the literal parent; a moving ref is not a substitute.

The held C2 source worktree is
`/home/kassym/Desktop/groundloop-worktrees/m5-d34-requirement-completion`, branch
`workstream/m5-d34-requirement-completion`, at that same parent/tree, empty
index, four tracked modifications and nine untracked files. All thirteen full
byte/length/hash/blob/mode pins and exact raw status are in:

```text
/tmp/groundloop-d34-c2-source.20lCffBL/FINISH_HELD_CHECKPOINT.json
sha256 = c64f9e7630bb1a249cb5214fba7d9c3cc02034c823f062ede2d60a22ad9d0df8
raw_v2_z_sha256 = 5db0d87bc0773130afa45f11b86f637ec35a632ab7591fbae2cee206c1e66d4b
handoff_sha256 = 125c0e2f23afd23d899281ec84077a74f755e9f116ab4419d998c9638fdb7a65
```

Preserve B33's complete five-file attempt2 checkpoint, SHA256
`0699d07c659a80802e623af59ab55bbdf62c1d89aa8f36fb977526751b992552`,
HEAD `4f50dc6701b462024e125a45505eb7be17bd4112`, empty index and raw SHA256
`fda319f391dd9aaf65d38791440c2d6e9737fa4da2c7dba0894b0b7dc36422e1`.
The current nine C1 pins are those in the accepted D33 handoff, not older
restart hashes. Preserve all five protected inventories, nine accepted
prerequisites and old C2 draft. Verify full contents and temporary evidence
availability at each barrier; missing evidence stops work and is not recreated.

## 2. Confirmed contradiction and narrow precedence

Migration017 creates the transition-context, change-journal and expected-change
temporary tables inside `groundloop_m5_matching_begin_transition_context`, a
trusted SECURITY DEFINER function. They belong to the trusted definer, not a
distinct runtime LOGIN. Keeping raw access private rejects caller-created or
mutable substitute authority.

Native `postgres_matching._capture_cursor_binding` must read the unique context
row before native preparation and again during binding revalidation. Native
`_validate_matching_stage_journal` must read the stage journal before native
finalization. Both currently perform direct SELECTs from these private tables.
Permanent-schema table grants do not grant those subsequently created private
temporary-table reads. Both functions are outside the accepted C2 grant.

Root's retained normal positive test proves owner fresh A and distinct-LOGIN
zero-write retained A replay, then fails fresh B with InsufficientPrivilege on
the transition context. An added assertion proves complete durable rollback
and re-raises the original error. Two independent source/authority audits
confirmed the dependency, including the second inaccessible journal read.
These are retained diagnostic findings, not fresh executable evidence here.

D34 Section7.8 preserves owner/LOGIN gates without literally specifying a fresh
C2 LOGIN positive. No accepted C2 owner-only exception exists; O33's exception
is expressly structural OPEN. This candidate explicitly requires fresh owner
and distinct-LOGIN C2 qualification. It neither asserts an earlier literal
mandate nor silently waives the discovered failure.

D31's migration019 accessor is seal-only and rejects a transition triplet. Its
function, body, ACL and no-journal-read rule remain unchanged. No conforming
source-only workaround was found: raw grants, SET ROLE, owner impersonation,
GUC-only trust, manufactured binding, copied preparation or skipped/early
validation would weaken the accepted checks.

Only these conflicting boundaries may be superseded after authority freeze:

1. D34 Section8, its activation's no-new-schema/migration/privilege statements,
   and earlier complete-inventory/extra-migration prohibitions, solely for the
   additive022 and two guarded SQL functions specified below.
2. D33's exact021-only requirement interpretation, solely to admit the exact022
   successor described in Section6. The old strict021 checker stays unchanged.
3. Existing-function ownership, only through a later separate activation for
   the two native reads and one prerequisite dispatch named in Sections6--7.
   This candidate itself grants no source ownership.

No other D24--D34 rule changes. No migration000--021 byte/ledger, old function,
table, column, index, type, view, sequence, CHECK, FK, trigger, schema default,
raw-table ACL, private-helper ACL, model, prompt, provider or runtime-mode change
is authorized. The two new function EXECUTE ACLs are the sole SQL privilege
addition. Existing grounding/state/certificate/absence/result/work/timing/
replay identities and recipes remain exact; only one new administrative schema
bundle identity is added. There is no seventh reference or fourth source kind.

## 3. Exact two-function SQL surface

The future additive file is exactly:

```text
migrations/022_m5_transition_context_access.sql
```

It may create exactly the following two functions, each with eight required
inputs in this order and no defaults, variadic form, overload, companion type,
procedure or alias:

```sql
groundloop_m5_matching_read_transition_context(
    selected_epoch_id bigint,
    selected_expected_revision bigint,
    selected_resulting_revision bigint,
    selected_source_kind text,
    selected_source_id text,
    selected_context_oid oid,
    selected_journal_oid oid,
    selected_expected_oid oid
)
RETURNS TABLE (
    backend_pid integer,
    transaction_id text,
    session_role text,
    epoch_id bigint,
    expected_revision bigint,
    resulting_revision bigint,
    source_kind text,
    source_id text,
    validation_started boolean,
    validation_done boolean
)
LANGUAGE plpgsql STABLE CALLED ON NULL INPUT SECURITY DEFINER
PARALLEL UNSAFE NOT LEAKPROOF SET search_path FROM CURRENT;

groundloop_m5_matching_read_transition_stage_journal(
    selected_epoch_id bigint,
    selected_expected_revision bigint,
    selected_resulting_revision bigint,
    selected_source_kind text,
    selected_source_id text,
    selected_context_oid oid,
    selected_journal_oid oid,
    selected_expected_oid oid
)
RETURNS TABLE (
    relation_name text,
    key_preimage bytea,
    first_old jsonb,
    final_new jsonb,
    first_operation text,
    last_operation text,
    mutation_count integer,
    saw_insert boolean,
    saw_update boolean,
    saw_delete boolean
)
LANGUAGE plpgsql STABLE CALLED ON NULL INPUT SECURITY DEFINER
PARALLEL UNSAFE NOT LEAKPROOF SET search_path FROM CURRENT;
```

These are exact signature/attribute specifications, not executable function
definitions. Each function is an ordinary set-returning function, not a trigger.
NULL input must raise: proisstrict=false is exact, not a zero-row shortcut.
The first returns exactly one row on success. The second returns the complete
possibly empty stage projection after the same successful context attestation;
zero rows is valid only for an attested empty projection, never for bad input.

Install with the selected trusted permanent schema followed by pg_catalog, so
SET search_path FROM CURRENT pins exactly that path. Both function owners equal
the existing private-triplet checker, begin-context helper, public transition
authorizer and permanent schema-bundle ledger owner. No superuser/row-security
bypass assumption is introduced. Explicitly REVOKE ALL then GRANT EXECUTE to
PUBLIC on each exact signature, with no PUBLIC grant option or third-party
grantee; normal implicit/explicit owner privileges remain. An unexpected
default-privilege grantee fails catalog validation rather than authorizing a
broader ACL rewrite. Schema USAGE remains separate. No runtime
raw-temp/private-helper privilege is granted.

Catalog functions/types/operators must be explicitly pg_catalog-bound; the
trusted private-triplet call is safely installation-schema-qualified with
exact regclass arguments. Data reads name pg_temp explicitly. A caller search
path, decoy function, implicit overload or another schema cannot supply a part
of the bridge. The existing trusted-schema-owner boundary remains explicit:
this is not protection against a malicious installation owner.

## 4. Common fail-closed attestation

Before returning any data, both bridges must establish all of the following
within their trusted execution. The journal bridge may call the context bridge
once through the same pinned schema/exact signature rather than duplicate it.

1. All eight arguments are non-NULL; E and both revisions are positive; all
   three OIDs are nonzero and distinct; source_id satisfies the native
   btrim(source_id) <> '' test and is compared/returned in full without trimming,
   normalization or replacement.
2. Source kind is exactly structural_open, requirement_completion or
   direct_transition. Structural OPEN requires expected=resulting=1, matching
   the native context, not the intent's sealed-predecessor before_revision.
   Both other kinds
   require resulting-expected=1 with overflow-safe arithmetic.
3. Checked-transition GUC is exactly on and matching-mode GUC is transition.
   Each epoch/revision/source/OID GUC exists and equals its argument exactly;
   integer/OID text must equal the native canonical argument::text. NULL,
   empty, malformed, noncanonical or mismatching values fail closed.
4. None of the promotion-context, promotion-journal or promotion-expected
   temporary relations exists. All three transition relations exist under
   their exact names in pg_my_temp_schema(), resolve to the three argument
   OIDs, and are distinct ordinary temporary tables.
5. The unchanged revoked private-triplet checker accepts these exact OIDs,
   proving trusted-definer ownership, kind and persistence. Distinct untrusted
   runtime-caller-owned same-name/same-shape tables are not authority even with
   their true OID GUCs. Malicious trusted-owner forgery is outside this boundary.
6. The transition context contains exactly one total row. It binds actual
   pg_backend_pid(), an already assigned full transaction ID and session_user,
   not current_user under definer execution. Use
   pg_current_xact_id_if_assigned() and reject NULL; the bridge must not assign
   a transaction ID to manufacture authority on a bad invocation.
7. That row's E/expected/resulting/source-kind/source-ID equal the arguments
   and GUCs exactly; validation_started and validation_done are exactly false.
   Preserve full-XID equality, not xmin-only or truncated identity.
8. The context result's ten fields come from that same unique validated row.
   transaction_id is the native bigint column's exact text projection. No
   default, reconstructed anchor, caller-authored row or second query image
   supplies a returned field.

Every absent/wrong setting, missing/replaced/duplicate relation, wrong
owner/kind/persistence/namespace/OID, extra context row, backend/XID/session/
coordinate/source mismatch, or true/NULL validation flag raises before output.
NULL-safe proof is mandatory; SQL three-valued comparison cannot admit NULL.

No permanent business state, header, publication head, observation, result,
matching image, contribution or accumulator is read or locked by these SQL
bridges. No DML, DDL, explicit GUC mutation, transaction control, explicit row/table/
advisory lock, validation start/completion or SET CONSTRAINTS is performed.
They never read expected-set contents. Genuine contexts may already have stage
or header writes; false validation flags do not mean an empty transaction.
Repeated unchanged calls preserve all persistent/temp rows, GUCs, flags,
transaction identity and constraint mode. Only declarative function-local
search_path entry/restoration occurs. Ordinary implicit catalog/relation
read locks are not advertised as absent or a lock-free guarantee.

## 5. Exact journal projection, unchanged comparison

The second bridge may expose only the ten native journal fields above for
exactly these fourteen native _STAGE_JOURNAL_RELATIONS:

```text
groundloop_m5_matching_image_working
groundloop_m5_matching_observation_working
groundloop_m5_matching_edge_working
groundloop_m5_matching_hash_mask_working
groundloop_m5_matching_hall_working
groundloop_m5_working_requirement_state
groundloop_m5_working_group_state
groundloop_m5_working_claim_state
groundloop_m5_working_answer_state
groundloop_m5_group_certificate_artifact
groundloop_m5_group_certificate_artifact_row
groundloop_m5_claim_certificate_artifact
groundloop_m5_working_group_certificate_binding
groundloop_m5_working_claim_certificate_binding
```

Return ALL rows matching that fixed relation filter in the caller's attested
private journal, with exact original values/multiplicity. No caller relation
list/key filter, LIMIT, pagination, truncation, DISTINCT, normalization,
aggregation, repair, expected-row substitution or correctness-dependent sort.
Journal rows for other native phases remain outside this same existing filter;
their existing deferred validation is unchanged. No other-session/backend
journal, promotion journal or expected-set content is exposed.

This explicitly permits a bounded current-transaction stage-journal projection,
not general raw journal access. Existing jsonb before/final row data is returned
as data, never serialized or hashed into a new semantic identity. Existing
nullable before images are not new nullable state hashes or tombstones.

Python still computes _expected_matching_stage_rows and
_matching_stage_before_map natively, requires equal expected before/final key
sets, then compares every returned key and multiplicity against the exact plan.
Preserve first_old/final_new, INSERT-vs-UPDATE choice, first/last operation,
mutation_count=1 and all insert/update/delete flags byte-for-byte in meaning.
Extra/missing/wrong rows must still conflict. Returning genuine rows is not
equality, staging or finalization authority by itself.

Let J be the actual current transaction's journal size and B its projected
bytes. The complete filter/transfer is O(J+B), not constant and not independent
of affected stage size. It adds no corpus/history scan. Existing work-coordinate
definitions/ownership remain unchanged; include actual additional measured
SQL/helper time and any genuinely new producer bytes in their existing owner,
never pretend a read is another physical state write. No measured result or
end-to-end/neural speed claim follows.

## 6. Additive022 ledger, installation and exact successor catalog

The exact prerequisite is the accepted five-field021 row, not one matching
hash, a computed checkout substitute or merely existing functions:

```text
bundle_id = m5-document-update-completion-schema-bundle-v1
bundle_sha256 = 893baf9d50cbb84c27b356cbc329f97bc714ca8c280778ece920b5a4dc728eb5
migration_sha256 = 198646f9a4b2f965d9ca89df684eb4f4287f3753aeff1d255c848bcb0352681c
oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
prerequisite_sha256 = b7706feb7d54fcf9fdb4f9f38350a32967e0b460229b6f264493fb428be8ddfc
```

Migration022's administrative identity is exactly:

```text
bundle_id = m5-transition-context-access-schema-bundle-v1
bundle_sha256 = stable_m5_digest(
  "m5-transition-context-access-schema-bundle-v1",
  TEXT("migrations/022_m5_transition_context_access.sql"),
  HASH(SHA256(exact_migration_022_bytes)),
  HASH("893baf9d50cbb84c27b356cbc329f97bc714ca8c280778ece920b5a4dc728eb5")
)
migration_sha256 = SHA256(exact_migration_022_bytes)
oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
prerequisite_sha256 = 893baf9d50cbb84c27b356cbc329f97bc714ca8c280778ece920b5a4dc728eb5
```

TEXT/HASH denote the existing typed M5 fields, not stringification. Literal
reviewed migration/bundle/body/catalog pins must be recorded in a separately
audited preinstallation hash-freeze after exact SQL bytes are authored and
reviewed, before first authoritative installation. No future hash is invented
here, and the installer never learns accepted authority from caller bytes.

### 6.1 Installer

The opt-in schema-admin installer owns one idle read-write READ COMMITTED outer
transaction, never a savepoint or ambient transaction. Capture/validate one
permanent schema and safely qualify ledger/catalog/DDL operations there.
Before the initial ledger decision, perform only connection/schema-envelope
checks and caller-byte identity calculation, not UTF-8/content/prerequisite/
catalog rejection. Read022's exact five-field ledger before target-schema
install locks or content validation. A conflicting caller identity is a ledger
conflict; exact replay uses the successor catalog check with no install lock,
DDL or ledger write. An installed malformed object is not replay success.

Only on absent ledger, validate the reviewed migration/bundle/SQL statements,
exact021 and transitive000--020 prerequisites. Take locks in this exact admin
order: SHARE ROW EXCLUSIVE NOWAIT on the permanent schema-bundle ledger, then
EXCLUSIVE NOWAIT on the permanent groundloop_m5_runtime_epoch table. The second
is the installation-only transition barrier: native checked transitions hold
ROW SHARE on that table through their outer transaction, so a new022 installer
must fail NOWAIT instead of changing authority between their phases. It is not
a new matching-phase business-row lock or state write. Reread022's ledger after
both locks and classify exact replay/conflict again. Require the exact pre022
catalog;
reject any pre-existing new-name function/overload or partial bridge without
its ledger, even if schema-owner-owned. Execute exactly two CREATE FUNCTION
statements plus their four REVOKE/GRANT statements under the pinned path.
Verify the exact post022 catalog/owners/config/ACL, then insert the exact022
ledger last. Every cut rolls back both functions, ACLs and ledger atomically.
No backfill, data scan, runtime-mode activation or old object replacement.

### 6.2 Closed catalog and021 compatibility

Keep _m5_document_completion_verify_catalog and every strict020 checker
byte-identical: they still reject catalogs beyond their own accepted revisions.
022 has a NEW exact closed174-function check: the original172 functions plus
only the two exact signatures above. Count/name checks alone are insufficient.

Require all old function bodies, signatures, owners, flags, search paths and
ACLs unchanged, all old nonfunction structural/dependency catalog images exact,
and all new function metadata/bodies/ACLs exact. The old172 body-source digest
remains 8e8cfe6cbcf544d0e843bfa8189cb229573fa2b0cc597f84ff0a6d39139fb059
under its existing administrative framing. When proving the old structural
fingerprint (27a02a8910e1a4b671b879cea1bdc0a5a35d9f693a870183355088990e207f62
under its existing framing), omit only the two new exact identities from its function-identity
projection; omit nothing from any other projection. Separately prove the full
174 inventory so the projection cannot hide a third function, overload,
different namespace, changed dependency or other extra object. Use reviewed
literal pins, not an observed hash promoted to authority at runtime.

The ONLY existing prerequisite-dispatch body needing a new grant is
src/groundloop/postgres/migrations.py:require_m5_document_update_completion_bundle.
Its public signature stays unchanged. In its same captured schema, an absent
022 ledger AND both new names wholly absent take the exact old021 check.
An exact022 ledger takes the new full successor check, including exact021 and
all transitive prerequisites. A malformed/conflicting022 row, partial/missing/
extra bridge or an orphan new-name object fails, with NO old021 fallback.
New successor validation must not call this dispatcher recursively.

This explicit successor dispatch lets unchanged native021 consumers continue
after an authorized022 upgrade. It does not weaken old021/020 checkers, allow
arbitrary functions or change the old installers' defaults. New schema-admin
identity/install/require utilities and their explicit administrative exports may
be allocated later; no public store/application API is added.

## 7. Runtime consumption and limited existing-function grant

Only after a separately audited implementation activation may root adapt:

1. _capture_cursor_binding: replace its inaccessible ten-column context SELECT
   with the exact context bridge on an exact022 schema. Retain native GUC/OID
   prechecks, E/expected mapping, row cardinality/ten-field comparisons and
   complete _CursorBinding construction unchanged in meaning.
2. _validate_matching_stage_journal: replace only its inaccessible filtered
   journal SELECT with the exact journal bridge. Arguments come from the
   already validated prepared binding/official intent, never caller key lists.
   Keep its complete existing native comparison from Section5.

Necessary NEW private _d35_* reader/schema-ledger helpers and imports may be
allocated in that same matching file. No _CursorBinding/_Prepared DTO field,
public derive/apply signature/body/export, native prepare/stage/finalizer
algorithm or lock order changes. Revalidation still recaptures through the
same native caller, comparing exact cursor/backend/full-XID/session/context/
source coordinates, including source_identity_hash in the unchanged Python
binding. The SQL context does not invent a missing source hash field.

Capture the checked outer permanent schema, resolve the exact permanent ledger
there and use safely quoted schema-qualified bridge calls with exact argument
casts. Both functions must be from that same schema, with their accepted
owner/signature/config/ACL. No mixed-schema ledger/function/envelope fallback.
Do not claim the017 temporary row stores an installation-schema label that it
does not contain: attestation is not a replacement for the checked outer
namespace/envelope or independent native source/header validation.

For retained legacy020/021 owner routes, absence of022 and BOTH new function
names preserves the existing direct-read behavior; do not catch privilege
failure and retry as owner or via another schema. Presence of any022 authority
or object requires the exact successor, never a partial fallback. The same
branch must be used consistently within one checked outer call: its genuine
held runtime-table ROW SHARE and the installer EXCLUSIVE/NOWAIT barrier prevent
a conforming022 first install between phases. Unexpected authority/namespace
drift fails, not a silent route switch; no cache, cursor attachment or new
binding field is authorized. Full174 integrity is checked at the admin/strict
prerequisite boundary; the runtime reader performs only bounded exact022 ledger
and two-bridge metadata checks, not a claimed whole-catalog scan per phase.

Fresh active private C2 explicitly requires exact022 before its sole fresh
authorizer/source mutation. Allocate this narrow new prerequisite in the held
owned verifier helper and fixture; it may not silently succeed owner-only
without022. Retained replay stays native zero-write and need not open a new
transition or use these bridges. Inactive/late/replay semantics remain exact.
Do not add a whole-catalog/history scan inside a matching phase: bounded point
ledger/bridge metadata checks suffice there; full catalog checks are admin gates.

_capture_direct_context_binding has another existing direct temp read but is
NOT needed by this C2 repair and stays unowned/unchanged. This amendment does
not qualify all direct/C1 nonowner paths or reopen their separate prerequisites.
The protected persistence wrapper and legacy all-active blocker remain intact.

## 8. Mandatory executable falsifiers — all PENDING

Later activation must allocate and map all of these on identical final bytes:

1. Exact typed schema-bundle vectors; all five prerequisite fields; ledger-first
   invalid-UTF-8/hash/content conflicts; first install, exact replay, conflicting
   replay, orphan/partial objects, every DDL/ACL/ledger rollback cut and both
   concurrent installer orders with actual wait/NOWAIT outcomes; a real held
   checked transition prevents first installation via the runtime-table NOWAIT
   barrier, with no authority switch or partial object.
2. Exact two-function/six-statement surface; complete174 catalog/172 projection;
   all old migrations000--021/ledgers unchanged; old checker still strict;
   changed old/new body, signature/default/overload, owner, flags, ACL, proconfig,
   extra function/type/trigger/index or dependency each rejected.
3. Genuine owner and actual distinct LOGIN context/journal positives, covering
   native structural_open1/1 with a sealed predecessor revision greater than1,
   and completion/direct N/N+1 context shapes without
   claiming the separate direct pre-source caller is repaired. Context fields
   and complete journal values agree for the same native logical fixture.
4. Distinct LOGIN still cannot SELECT any private transition table or EXECUTE
   private begin/triplet/journal/validator helpers. No SET ROLE, ownership/ACL
   alteration, forged binding or manufactured context may qualify a positive.
5. Every nullable input position, invalid/nonadjacent/overflow coordinate,
   unknown kind, blank/mismatched source, zero/equal/wrong OID and every absent/
   wrong/noncanonical GUC fails with no output. No assigned full-XID bad-call
   shortcut or SQL-NULL bypass.
6. Distinct untrusted runtime-owned same-name/same-shape triplets with absent/invented/exact true
   OID settings; missing/replaced/temp-namespace/kind/persistence/owner relations;
   wrong backend/full-XID/session/cardinality; promotion coexistence; either
   validation flag true; and schema/temp/search-path/overload/decoy/mixed-schema
   substitutions all fail closed at the appropriate bridge/outer boundary.
7. Repeatability and nonmutation of all context/journal/expected/persistent
   data, flags/GUCs/XID/constraint mode; no explicit locks, early validation,
   expected-set content read or business-table scan. Include a rejected call
   without an assigned XID and record ordinary implicit read locks honestly.
8. Actual native stage-journal projection equality for all14relations, genuine
   empty projection, extra/missing keys, duplicate/mutated returned projection,
   wrong before/final image, operation/mutation count/flag cases. Prove no
   LIMIT/key-filter/row substitution can conceal a planned-key mismatch.
9. Fresh owner and distinct-LOGIN C2 commit/reconnect/replay, SUPPORT/REFUTE/
   NEUTRAL, nonzero actual-event deltas, byte-total work/timing, guards-last and
   full rollback; original normal failing LOGIN positive becomes PASS without
   weakening/xfail/skip. Missing or bad022 must reject fresh active C2 before
   source mutation, including owner; inactive/late/retained paths stay exact.
10. Legacy020/021 native owner compatibility, exact022 prerequisite dispatch,
    no bad022 fallback, phase-stable schema/authority selection and unchanged
    public wrapper blocker. Independent full Python/SQL postcommit images,
    all original D34 aliases/producer/race/representative-FK/rollback/replay
    gates and complete affected native modules remain mandatory.
11. Pure/static/typing/compile/package diagnostics with exact counts/failures,
    no concealed deselection, and two full identical-byte GO/P0=0/P1=0 audits,
    sole-parent commit/postcommit checks and push barriers per later tranche.

Owner-only evidence, a passing retained replay, authored expected journal rows
used as actual rows, raw-table grants, trigger/guard monkeypatches or skipped
cases cannot satisfy a positive gate. Known Dynagox provenance and packaging
failures remain explicit, not repaired in unrelated paths.

## 9. Separate authority and implementation barriers

First freeze this complete sole-file candidate externally and obtain two
independent identical-byte GO/P0=0/P1=0 contract audits. Any byte change
invalidates BOTH verdicts; retain withdrawn drafts and review final bytes anew.
No stage/commit/push occurs under this drafting approval.

A separately authorized docs-only authority-freeze tranche must then record
M5-D35/M5.0-35 contract-PASS / implementation-PENDING, runtime-addendum revision16,
exact accepted candidate identity, decision/acceptance/status/design/roadmap/
AGENTS consistency and the limited supersessions in Section2. Clarify older
M5.0-30 inventory/falsifier wording only to admit exact019--022 under their
respective amendments; all other old falsifiers remain mandatory. That freeze
requires its own full audits, exact commit, fresh checks and published barrier.

Only afterward may a separately audited path-exclusive activation allocate:

1. A sequential schema022/source-admin/test/handoff lane, disjoint from EVERY
   one of the thirteen C2/five B33/nine C1 paths; narrow prerequisite-dispatch
   ownership from Section6, exact preinstall hash-freeze and closed catalog.
2. After accepted/pushed schema integration, a checked advance of the SAME
   held C2 worktree with all thirteen complete pins preserved, followed by root
   alone editing the two reader bodies and narrow owned C2 prerequisite/test
   glue. Enumerate every existing function and test/handoff path anew; no
   parallel matching-file lane or WIP transplant.
3. Complete original C2 qualification and its own two final source audits,
   exact commit/postcommit checks/push. Only then the still-separate literal
   B33 parent-pin task and existing B33/P33/C1 barriers may be considered.

Before any advance, verify every incoming commit is linear, its endpoint diff
AND all-commit touched-path union are disjoint from all held paths, and full
bytes/modes/blobs/HEAD/branch/index/raw/protected custody match before/after a
permitted no-autostash fast-forward. No reset/clean/stash/rebase/cherry-pick,
overlapping merge, manual copy or patch transplant. This candidate supplies
neither a future literal commit hash nor permission to advance any checkout.

## 10. Claim ceiling

```text
M5-D35 / M5.0-35 contract = PENDING_AUDIT_AND_AUTHORITY_FREEZE
migration022 / trusted transition bridges = NOT_IMPLEMENTED_NOT_ACTIVATED
held C2 = UNACCEPTED_P1_STOP
D25/D34 implementation mapping = PENDING
B33/P33/C1, Task2/Task17, M5.4--M5.6 = PENDING
deployment/security/AI quality/end-to-end utility/speed/cost = PENDING
runtime_mode = v1_only outside isolated fixtures
```

This docs-only candidate executes no tests, SQL, DB/container/provider/model
operation and claims no new measured result. It preserves a privilege boundary
without establishing system security, objective truth, novelty, superiority or
AI accuracy. Independently adjudicated natural histories and full end-to-end
evaluation remain separate mandatory work.

PostgreSQL mechanism checks (official documentation, read2026-10-15):
[table-lock modes](https://www.postgresql.org/docs/current/explicit-locking.html),
[function attributes and trusted search paths](https://www.postgresql.org/docs/current/sql-createfunction.html),
and [already-assigned full transaction IDs](https://www.postgresql.org/docs/current/functions-info.html#FUNCTIONS-PG-SNAPSHOT).
These support the mechanism design, not executed022 or C2 qualification.
