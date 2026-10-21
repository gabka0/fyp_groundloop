# M5-D37 Document-Clock Reverse-Validator Privilege Amendment

Status: DOCS-ONLY CANDIDATE, not frozen or implementation authority. Two
independent COMPLETE audits of identical bytes must each return
GO / P0=0 / P1=0 before authority-freeze updates and separate source
continuation activation. No SQL, source, test, ACL or default changes occur here.

Date: 2026-10-21 (Asia/Hong_Kong)

## 1. Confirmed contradiction and sole correction

D36 is frozen at
[DOCUMENT_DIRECT_CLOCK_AMENDMENT.md](DOCUMENT_DIRECT_CLOCK_AMENDMENT.md),
SHA-256 f72d47070cf3aca94cfc18aca0b6fc5b30a19b1ff2861f87c005f7c12692e847.
Its Sections8/9 require owner-only new proof tables except explicitly named
runtime-role SELECT/INSERT, two SECURITY INVOKER trigger validators, and an
exactly inert nonorigin branch on existing legacy tables. Those requirements
cannot all hold for a legitimate legacy caller lacking new-table SELECT:
even a negative origin-membership lookup needs that permission. Skipping
validation when access fails would also skip real origin-bound mutations.

The narrow correction is to make ONLY
`groundloop_m5_validate_document_clock_binding()` a constrained,
validation-only SECURITY DEFINER trigger function. Its first origin-membership
read runs with the existing trusted schema-bundle owner's privileges; absent
membership returns inertly. Present membership still requires explicit
original-session authorization and every D36 causal/source/clock check.
Owner execution is not caller authorization.

`groundloop_m5_validate_document_clock_origin()` stays SECURITY INVOKER,
owner-only OPEN, with its complete D36 proof unchanged. There is no third
function, new accessor, new caller privilege, or permission-error escape.

The PostgreSQL16 distinction between invoker and owner execution is documented
in [CREATE FUNCTION](https://www.postgresql.org/docs/16/sql-createfunction.html).
This is a feasibility correction to the local contract, not a security claim.

## 2. Exact precedence and unchanged boundaries

Supersede ONLY D36 Section9 item2's invoker requirement for the binding/reverse
function and the assertion that its trigger execution adds no validation read
privilege. Supersede Section8's no-expansion wording ONLY for the internal
owner-executed validation reads specified here, not for ordinary caller access.
Other authority documents must refer explicitly to this narrow override; do
not silently rewrite D36's accepted bytes or interpret its historical wording
as permission to change any other function.

Section4.1 also makes the membership-selection rule explicit inside the same
FOUR already-authorized existing body replacements. It changes none of their
security flags or privileges and grants no additional existing body. This is
the only clarification to their D36-versus-legacy routing; their actual legacy
validation and all D36 causal requirements remain mandatory.

The future additive023 inventory remains EXACTLY two proof tables, two new
trigger functions, nine new triggers, four existing validator BODY replacements
and its five-field administrative bundle registration. No old signature,
owner, ACL, search_path setting or trigger definition changes. In particular:

- Migrations000--022 and strict020/021/022 checker bodies remain exact.
- D31/D35 accessors, their projections and private-temp/helper ACLs remain exact.
- D36 origin eligibility, source/XID/OLD/NEW/closure checks, clocks N and C,
  lock order, indexed lookup bounds, recipes and replay rules remain exact.
- No public store, producer, context reader, runtime-mode or model change is
  activated. No new table/index/column, kind, counter or digest recipe is added.
- New table privileges remain owner-only except fixed, explicitly approved
  existing runtime LOGIN roles with SELECT/INSERT, never PUBLIC or inferred
  UPDATE/DELETE/TRUNCATE/REFERENCES. No grant on an old object is authorized.

This correction adds no ability to mutate state through a definer function:
the reverse validator may read and raise, but never repair or write anything.

## 3. Closed execution envelope

The binding function retains no SQL arguments, RETURNS trigger, LANGUAGE
plpgsql, VOLATILE, CALLED ON NULL INPUT, PARALLEL UNSAFE and NOT LEAKPROOF.
Only its security flag changes to SECURITY DEFINER. Its owner is the same
existing trusted installation/schema-bundle owner, not a new role. There is
no PUBLIC or nonowner direct EXECUTE, no caller-selectable schema/epoch API,
and no ordinary result containing private state. Trigger invocation is the
only supported nonowner entry. Missing privileges or invalid proof reject the
enclosing transaction; exceptions never become successful legacy returns.

Its sole proconfig is a safely quoted captured trusted installation schema
followed by pg_catalog. No $user or explicit pg_temp appears. All sensitive
relations, types, operators/helper functions used for authority are explicitly
qualified; identifiers from the trusted schema are quoted. A caller's path,
temporary lookalike, arbitrary object-name argument or custom GUC is not
authority. Do not read private pg_temp directly or make any extra bridge.

Before ANY owner-privileged origin or runtime read, validate the exact permanent
trigger attachment against the closed installation projection: trusted schema
OID/name, target OID/name, ordinary permanent relation, TG_RELID,
TG_TABLE_SCHEMA, AFTER/ROW, exact trigger name, and event. Only the six target
relations and six binding-function trigger identities already named in D36
Section9 are legal: origin_final, binding_guard, epoch_counter_guard,
transition_guard, override_guard and runtime_guard, using their full
`groundloop_m5_document_clock_` names. Their allowed events remain respectively
INSERT, INSERT, INSERT/UPDATE, INSERT, INSERT/UPDATE/DELETE and INSERT/UPDATE.
Catalog/namespace inspection for this attachment proof is allowed; no origin
membership may be inferred from a kind, counter inequality, catalog/rowtype
side channel, session flag or caller-supplied boolean.

For UPDATE, OLD and NEW must identify the same epoch; reject a changed epoch
before using either as membership authority. INSERT uses NEW.epoch_id;
DELETE uses OLD.epoch_id. A malformed/missing native epoch key rejects, not
falls back. This ordering must also cover direct raw DML, not only producers.

## 4. Origin lookup and exact nonorigin compatibility

After attachment/key validation, perform exactly the schema-qualified
origin primary-key point lookup for that native epoch. This lookup alone
may run before any D36-only original-session privilege restriction.

If no origin exists, return the native inert AFTER-trigger result immediately:
no caller role allowlist check, new-table privilege requirement, new role/session
restriction, new source query, lock, work/counter change, or D36-only GUC check.
The old validators still execute normally under their unchanged definitions.
This is how an authorized old-table-only legacy LOGIN continues to work without
SELECT on either proof table. The compatibility guarantee concerns otherwise
valid original DML; it does not admit a forged attachment or changed epoch.

If an origin exists, never return through that legacy branch because a caller
lacks permission, a binding/source is missing, a table is inaccessible, a
session setting is wrong, or the caller claims nonorigin mode. The function
must reject unless Sections5/6 and all original D36 validation succeed.
It may not catch insufficient_privilege/undefined_object and pretend absence.

No origin/binding source can be retrofitted by this lookup. Fresh OPEN remains
owner-only, N=C=1, with exact origin digest, assigned outer transaction,
source freshness and deferred structural_open closure. Immutable proof rows
cannot be deleted to manufacture a nonorigin branch.

### 4.1 Existing invoker-body routing, not proof of absence

The017 matching validator already executes as SECURITY DEFINER; its existing
flag stays exact. The021 readiness, work and result-children validators execute
as SECURITY INVOKER and must retain that execution mode. An unconditional
new-table membership read inside those invoker bodies would reproduce the
legacy permission failure even after correcting the independent reverse guard.

Inside ONLY these four body replacements, first use native privilege checks
for the EFFECTIVE SQL caller (current_user), exact trusted schema USAGE and
table-level SELECT on BOTH new proof-relation OIDs to decide whether the body
can perform its D36 membership lookup. Resolve only the closed permanent OIDs;
no user-selected name, private-temp object, exception catch or grant. This is
query-feasibility selection, never original-session authorization. The already
definer017 body normally satisfies this gate as its unchanged owner.

If this read gate is false, execute the exact original legacy validation branch
without querying either proof relation. Do NOT call the epoch nonorigin, skip
old validation, treat failed SELECT as evidence, approve a D36-only transition
or early-return success from the entire guard. If true, read actual membership;
absent membership selects that same exact legacy branch, and present membership
selects the complete D36 branch. Existing D36-only role requirements may not
be imposed on a legacy operation selected by this gate.

This gate is safe ONLY because the separate SECURITY DEFINER reverse validator
never uses it: that function ALWAYS checks real persisted membership first and
explicit original-session authorization for present origins. The future023
source/H36 review must prove closed coverage for EVERY origin-sensitive change
through all four old-body routes: it either fails an unchanged native guard
or necessarily queues a real enabled reverse-guard event on one of the four
old targets or two new proof targets, forced before successful commit. Runtime
advances and direct counter/transition/override mutations cannot avoid that
independent guard. A write without its required advance/source is still
rejected by the unchanged native closure. Neither a permission-false gate,
missing new SELECT/INSERT, an early SET CONSTRAINTS nor a legacy branch may
produce an origin-bound commit that escapes true-membership authorization.
Prove origin immutability/FKs and OPEN final closure prevent late origin
insertion after an earlier absent-membership deferred check.

Add branch-by-branch source/attachment/commit-closure evidence and live
negatives for this invariant, including raw DML without a new binding or
runtime advance. Earlier rejection in an old branch is a valid negative, not
a successful D36 lifecycle positive. An implementation that cannot establish
this closed proof must HOLD; no further flag/accessor/ACL change is inferred.

## 5. Original-session authorization for PRESENT origins

Inside SECURITY DEFINER, current_user is the function owner and therefore
MUST NOT serve as caller identity or permission evidence. Use the native
session_user as the original session role, under D36's existing separately
connected LOGIN/no-impersonation qualification. Owner positives use a native
owner session; distinct-role positives use a fresh connection authenticated
as that exact fixed existing LOGIN, never SET ROLE or superuser impersonation.
Native `role` must be `none` in a present-origin invocation; check it only
after the origin point lookup. A custom caller GUC cannot replace this check.
Trusted owner/superuser control of authentication/catalogs is outside this
validation boundary; no resistance to that control is claimed.

The original session must be either the exact trusted installation owner or
one of the exact existing runtime LOGIN roles named in the separately audited
installation manifest. Resolve and compare their real catalog identities;
role membership, caller text, current_user, PUBLIC and a discoverable table
ACL alone do not extend this fixed role set. Runtime-role identities and the
existing owner are installation-specific literals pinned before installation,
not an unrestricted runtime allowlist or new database object. A fixed role
must actually have LOGIN; no implicit creation or alteration is allowed.

For EVERY present-origin entry, explicitly check original-session ordinary
schema USAGE and SELECT/INSERT on BOTH new proof relations with the native
privilege machinery, against exact permanent object OIDs. No column-only ACL
substitution is supported. Runtime-role actual table ACLs still have precisely
the D36 allowed grant projection, no extra privileges. Owner's native owner
rights qualify only when session_user actually is that owner.

Additionally check all original-session permissions that owner execution would
otherwise mask on this entry's existing relations and helpers:

1. Require the corresponding ordinary INSERT/UPDATE/DELETE on the REAL trigger
   target for TG_OP, plus ordinary SELECT on OLD/NEW columns actually used.
   A privileged intermediary's write is not evidence of session permission.
2. Require ordinary SELECT on every permanent source relation/column read on
   the selected validation path, including transitive reads by SECURITY INVOKER
   helpers, and ordinary EXECUTE on each helper invoked there. If a path needs
   an additional ordinary privilege, check it for session_user explicitly.
3. Preserve the pre-existing guarded semantics/projections of any already
   accepted SECURITY DEFINER helper; do not convert its guarded projection into
   a general underlying-table grant requirement or widen its output. Preserve
   all its own session/XID/OID/GUC checks and entry-point EXECUTE requirement.

The future H36 preinstall source tranche must enumerate a finite, exact,
operation/branch-specific projection of these relation/function OIDs and
required privilege types from the reviewed SQL and its unchanged helper
dependencies. It must audit each actual query/call against that projection
before installation. The validator uses explicit checks from that reviewed
projection; it must not learn an allowlist from live grants, rely on a Python
precheck, grant privileges, or assert generic 'runtime permissions present'.
Checks precede the owner-executed read/call they authorize. An unconditional
superset of the needed read checks is allowed only if explicitly enumerated
and already supported by both fixed owner/LOGIN manifest positives; it may
never restrict the absent-origin compatibility branch.

Missing new or old access, removed schema USAGE/helper EXECUTE, wrong native
session role, SET ROLE, mismatched fixed role identity or unsupported column
ACLs fails closed for a present origin, including raw reverse-trigger DML.
No owner/superuser fallback may be reported as distinct-LOGIN success.

## 6. Owner validation is not state-changing authority

After membership and explicit original-session authorization, execute the
complete D36 reverse proof. Real immutable origin/binding identities, exact
N/C edges and indexed C_at(N), outer-XID freshness, current versus queued
OLD/NEW state, canonical transition payload, genuine dispatch/attempt/source,
D24/D25 contributions and final SEAL/FAIL closure are still mandatory.

Do not approve a direct counter, override, transition or runtime mutation merely
because the caller owns a new-table grant, has origin membership, can call an
old bridge, or supplied matching-looking coordinates. Reject forged or missing
binding, transition, work/source, override deletion/rollback and terminal proof
exactly as D36 requires. Do not introduce locks, broaden the allowed DML, move
the guards-last boundary, change counter authority or sum the transition
history. All reads remain bounded as in D36.

The function performs no INSERT, UPDATE, DELETE, TRUNCATE, DDL, grant/revoke,
session identity change, repair, counter/source write, network/provider call
or private-temp mutation, directly or via helpers. Rejections must not expose
private source contents as an accessor; use bounded generic error messages,
not arbitrary row values or query results. Do not declare LEAKPROOF or claim
timing/error confidentiality or database security from this contract.

## 7. Closed catalog and future execution gates

The corrected023/H36 closed catalog must pin the binding function's exact
SECURITY DEFINER flag, origin function's exact SECURITY INVOKER flag, both
complete bodies, owners, ACLs, proconfig and all other flags/dependencies.
Pin the exact trigger attachment/event/deferral projections, fixed roles,
new-table grants and explicit original-session privilege projection. Derive
them independently from the verified accepted022 catalog plus reviewed023;
never bless a live023 target, weaken an old checker or invent future hashes.
Administrative identity still uses exact023 bytes and the unchanged D36
five-field recipe. Future installer and replay must reject any differing flag,
owner, ACL, configuration, dependency or checked projection.

Mandatory additional falsifiers, not evidence already obtained:

1. Otherwise-valid nonorigin legacy operations from both native owner and an
   independently connected old-table-authorized LOGIN lacking ALL new-table
   privileges retain the same result on the FOUR OLD reverse targets:
   evaluation_epoch_counter INSERT/UPDATE, evaluation_counter_transition
   INSERT, evaluation_override_counter INSERT/UPDATE/DELETE and runtime_epoch
   INSERT/UPDATE, each with its full groundloop_m4_ or groundloop_m5_ name.
   Exercise all three old invoker bodies' read-gate-false legacy routes and the
   unchanged017 definer route. The TWO NEW origin/binding targets instead need
   separate authorized present-origin paths and missing-origin/permission
   negatives: there is no claimed old-table-only nonorigin INSERT positive
   on a new proof table. Wrong attachments still reject before lookup.
2. That old-table-only LOGIN cannot bypass validation by raw mutation of an
   origin-bound counter/transition/override/runtime row: it rejects and rolls
   back, rather than skipping because new proof SELECT/INSERT is absent.
   Also cover each old body's read-gate-false path, raw changes without an
   expected advance/binding and attempted early deferred validation/late origin
   insertion. Prove Section4.1's independent guard/immutable origin/FK/OPEN
   closure; do not confuse legacy selection with authenticated absence.
3. Fixed runtime LOGIN present-origin positives pass only with the exact approved
   new grants AND required original old permissions. Individually revoke each
   applicable permission class (old/new SELECT, new INSERT, target DML, schema
   USAGE, helper EXECUTE); prove fail-closed or an earlier native permission
   rejection. Record which layer rejected. Never grant old-object access here.
4. A valid grant/session cannot rescue missing/forged/misordered causal proof,
   changed OLD/NEW epoch, wrong target OID/name/schema/trigger event, a temporary
   lookalike, SET ROLE, custom-GUC 'nonorigin' or spoofed search_path. Both
   origin-present and origin-absent attachment cases are required.
5. Exact catalog negatives cover owner/security flags, direct EXECUTE grants,
   relation and column ACLs, proconfig/dependency changes and runtime-role
   identity changes; installation/replay remains atomic and fail-closed.
6. All original D36 eleven groups plus D25/D26/D33/P33/C1 positives, differential
   oracles, conflict/replay and actual byte/work/timing checks remain mandatory
   at their respective activated stages. Both nonempty execution orders and
   owner/separate-LOGIN lifecycle positives are NOT supplied by a docs freeze.

No skip, xfail, deselection, toy public ancestor, authored terminal fixture,
temporary privilege bypass, weakened source guard or reused receipt can turn
a failure into a pass. An earlier native privilege rejection is an explicitly
reported negative, not a successful validator/lifecycle positive.

## 8. Ownership, acceptance and pending work

This tranche owns ONLY this amendment path, from published main
db2ff2249ad88606297580c92f5cfbec9ea6b082, tree
476f143847999e7c5023723631043ca1e3b5809e, in its new isolated docs worktree.
Root is sole editor. Two independent full auditors own ZERO edit paths and
review the identical frozen bytes/custody; no subset or advisory substitutes
for complete GO/P0=0/P1=0. Two fresh postcommit complete checks must precede
atomic no-force publication of branch/main.

Preserve all153 pre-existing worktrees, their HEAD/tree/index/dirty-file pins,
all15 containers, and the original S36 twelve-path/two-dispatcher-body grant.
S36's current seven WIP files, five absent live-test paths and incomplete,
never-executed023 remain read-only evidence during this docs tranche.
The older two HOLD privilege advisories diagnose the contradiction only;
they are not audits of this candidate or source acceptance.

After the identical-byte audits, publish the candidate and a separately audited
authority freeze with a new decision-log entry and runtime-addendum revision18.
Then accept a NEW path-exclusive S36 continuation/ancestry amendment before
editing source: preserve the SAME held S36 worktree/WIP, original twelve paths
and two dispatcher bodies, and authorize only the necessary D37 definer/check
delta plus checked docs-only fast-forward ancestry. Its H36 parent must be the
actual accepted continuation activation, replacing the earlier literal
packaging-prerequisite parent only by explicit audited authority. Check both
endpoint and every-commit path union against all S36/P33/C1 protected paths;
no reset, stash, transplant or broad rebase. No023 execution before the complete
source audits and separate literal H36 preinstall freeze.

Candidate acceptance and later freeze mean contract-PASS / implementation-PENDING
for M5-D37 and M5.0-37, not a SQL/runtime/source gate. D25 integration, P33/C1,
Task2/Task17, M5.4--M5.6, deployment, security, AI quality and end-to-end utility
remain PENDING. Runtime remains v1_only outside isolated fixtures; the default
frozen M3 model is unchanged. Provider calls and API spend remain zero here.
