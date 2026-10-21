# M5-D38 Document-Clock Birth and Parent-Visibility Amendment

Date: 2026-10-21 (Asia/Hong_Kong).

Status: DOCS-ONLY PROPOSAL. Not frozen, source/SQL authority or an executed
counterexample. The revised user decision must explicitly cover BOTH the birth
order and extra BEFORE guard/raw-SQL compatibility exception below; the earlier
ordering-only question does not cover this complete correction. Two independent
COMPLETE identical-byte GO/P0=0/P1=0 audits, separate authority freeze and a new
source continuation are required. Root alone edits; auditors own ZERO paths.

## 1. Accepted prerequisites and confirmed gap

Accepted D36 DOCUMENT_DIRECT_CLOCK_AMENDMENT.md SHA256
f72d47070cf3aca94cfc18aca0b6fc5b30a19b1ff2861f87c005f7c12692e847
places origin insertion AFTER native counter declaration creation and requires
the actual fresh counter1 in the origin BEFORE proof. Accepted D37
DOCUMENT_CLOCK_VALIDATOR_PRIVILEGE_AMENDMENT.md SHA256
72b22368fa3a38c1e1c8024ae25fdd43463f04cc3b2c00646cc14ea6ea514136
requires a genuine origin-absence lookup to return inertly from each of the six
AFTER reverse attachments, and requires closed late-origin coverage.

Those rules do not establish the claimed coverage. Both independent native-
source advisories confirmed this STATIC sequence; no023 SQL was executed:

1. Begin an otherwise lawful fresh OPEN and create the genuine counter1,
   headers and declarations, as frozen D36 requires before origin insertion.
2. Make ONLY groundloop_m5_document_clock_override_guard IMMEDIATE, not ALL.
3. Insert a valid positive override at E and delete it before origin insertion.
   Both invocations see real origin absence and D37 requires an inert return.
4. Continue the genuine origin/D24/D25 OPEN. The erased cycle leaves no
   surviving override/transition/binding/work/header or017 context-flag evidence.

Unchanged013 has no override mutation trigger beyond the future023 attachment;
it supplies key/value checks and the native counter FK. Unchanged017 contains
no reference to this override relation in its transition/promotion journals or
trigger targets. Forcing only the named new constraint does not force017 or
alter its pristine context flags. Native constraint timing is per constraint,
not implicitly ALL: [PostgreSQL16 SET CONSTRAINTS](https://www.postgresql.org/docs/16/sql-set-constraints.html)
and [CREATE TRIGGER](https://www.postgresql.org/docs/16/sql-createtrigger.html).

Fresh1/1 plus current override absence therefore does not prove that no earlier
override mutation occurred. This is not a failure of the retained pure test
suite, nor a live measurement; it is a counterexample to a proposed static proof.

### 1.1 Counter absence ALONE is also insufficient

An already-executed counter INSERT is visible to a VOLATILE PL/pgSQL guard's
read-write SPI query, including within an outer command. Ordinary sibling-CTE
snapshot wording must not be used to hide a physically earlier counter. See
[SPI visibility](https://www.postgresql.org/docs/16/spi-visibility.html).

However, a native immediate FK is an AFTER/end-statement check, not a BEFORE-
insertion prerequisite. PostgreSQL16 queues INSERT AFTER events before RETURNING.
A volatile RETURNING callback can DELETE the newly inserted orphan through
nested SPI before the outer FK event runs. RI_FKey_check skips a child already
deleted under SnapshotSelf. The outer statement can end with neither counter
nor origin nor override; the caller can then force only the named override
guard and later create an origin whose counter-absence lookup passes.

Source: PostgreSQL16 [nodeModifyTable.c](https://github.com/postgres/postgres/blob/REL_16_STABLE/src/backend/executor/nodeModifyTable.c),
[trigger.c](https://github.com/postgres/postgres/blob/REL_16_STABLE/src/backend/commands/trigger.c)
and [ri_triggers.c](https://github.com/postgres/postgres/blob/REL_16_STABLE/src/backend/utils/adt/ri_triggers.c).
This second sequence is also source-derived, not an executed diagnostic.

Thus do NOT freeze the inference that every successful override INSERT already
established a parent before origin. Do not substitute caller ordering promises,
statistics or an unproved cmin comparison: an outer executor may retain its
saved output command ID while nested SPI advances the current command counter.

## 2. Sole semantic and inventory correction

Correct this birth boundary with BOTH parts together:

1. Insert the immutable origin BEFORE native counter creation, proving actual
   counter absence at its BEFORE cut. Defer actual counter1/declaration/P proof
   to the existing origin-final closure within the SAME outer OPEN transaction.
2. Add exactly ONE ordinary BEFORE INSERT ROW guard on the existing override
   relation, requiring its actual parent counter to be visible BEFORE physical
   override insertion. Reuse the existing validation-only binding function.

Neither part alone is the complete correction. The second is an explicit raw-
SQL admissibility restriction, including nonorigin epochs: otherwise-legal
child-first statements/callbacks or not-yet-visible-parent inserts no longer
qualify. Normal native M4 parent-first producers retain their semantics and
must pass byte-exact regressions. Do not label raw-SQL compatibility unchanged.

Supersede ONLY:

- D36 Section3.1's origin-after-counter order and counter-dependent BEFORE proof;
  the counter is temporarily ABSENT at birth, not a new persisted clock value.
  The completed OPEN still establishes exactly N=C=1.
- D36 Section9's nine-trigger inventory, adding the one trigger in Section4;
  the total becomes TEN. No other old trigger is replaced or retimed.
- D37 Section3's six-AFTER-only function attachment envelope, adding only the
  exact BEFORE branch; Section4's first-origin-lookup/no-extra-read rule remains
  exact for the original six AFTER paths, not this new parent-only entry.
- Earlier blanket nonorigin raw-SQL preservation ONLY for the visible-parent
  prerequisite on override INSERT. No new caller role/session/grant requirement
  is imposed by that parent-only branch.

The corrected023 inventory is exactly TWO new proof tables, TWO new trigger
functions, TEN new triggers, the SAME FOUR old function-BODY replacements,
the existing five-field023 registration and already authorized new-object ACLs.
No old signature/security flag/owner/ACL/proconfig/trigger definition changes.
000--022 and strict020/021/022 checker bodies remain byte-identical.

## 3. Fresh origin BEFORE cut and deferred completion

Keep the genuine matching-aware, metadata-preserving nonempty document_replace
scope. Preserve owner-only OPEN, exact permanent attachment/schema/OIDs, native
owner/session/no-impersonation qualification, actual source/declarations/policies/
snapshots, outer-XID freshness, genuine committed typed predecessor and all
original D29/D30/nonempty-root checks at their lawful source cuts.

At the origin BEFORE INSERT cut:

- Actual base, typed update, direct update and runtime source tuples are fresh
  in the assigned outer transaction, with shared N=1/SC and unsealed state.
- The exact permanent counter PK lookup for E must return ABSENT. Existing
  counter(E), even with no remaining overrides or with matching-looking values,
  rejects origin creation. Do not adopt, reset, delete, rekey or repair it.
- No native transition, binding or prior D24 work exists. Preserve the genuine
  pristine017/022 context proof and complete retained declarations/metadata.
- Compute the origin's unchanged m4_declaration_hash from the real retained
  initial direct declarations using the UNCHANGED native M4 canonical recipe:
  E, revision1, confirmed P and the actual initial scope count. It is not an
  arbitrary caller hash, placeholder, nullable hash or new serialization.

After the origin has been physically inserted, create the genuine native
counter1/declaration under the already lawful evaluation-family reservations.
Do not move a counter lock to an earlier tier or acquire lower-tier source keys
after15a/b/c. Preserve D29 source/scopes/jobs, D25 families12--14, ledger families
and the single accounting/finalization order. Counter absence uses a bounded
plain point lookup, not a new gap lock or new reservation object.

The existing origin-final AFTER INSERT deferred attachment retains its binding-
validator entry. Before OPEN may commit, it must prove:

- Actual counter1 was created in the SAME assigned outer transaction, is active,
  confirmed P, and has the exact real declaration/default/scope values derived
  from the actual initial direct declarations. Its declaration hash equals the
  unchanged origin field and independently reconstructed native declaration.
- Actual base/runtime remain at the same fresh N=1/SC source identity; the exact
  queued native initial NEW rows agree with final native points. Counter0->1
  UPDATE or another counter advance is not a substitute for initial creation.
- No transition/binding/override exists, and complete genuine structural_open
  D24 and D25 contribution/patch closures are fresh and exactly E/1/P bound.
- Every original nonclock OPEN check remains mandatory. An expected declaration
  hash before materialization does not itself authorize completed OPEN.

Prematurely forcing origin-final or the initial counter/runtime guards before
this closure rejects and rolls back. There is no committed half-origin mode,
new state flag, retry repair, backfill or relaxed source identity. The temporary
uncommitted counter absence is not a new public N/C edge. Public/default/v1
contracts and all non-origin routes outside the explicit INSERT rule stay exact.

## 4. One new parent-visible BEFORE INSERT attachment

```text
trigger = groundloop_m5_document_clock_override_parent_guard
target = groundloop_m4_evaluation_override_counter
timing = BEFORE INSERT
level = FOR EACH ROW
constraint = ordinary trigger, NOT DEFERRABLE
function = groundloop_m5_validate_document_clock_binding()
enabled = origin/default O
```

The same binding function remains no-argument RETURNS trigger, plpgsql VOLATILE,
CALLED ON NULL INPUT, SECURITY DEFINER, PARALLEL UNSAFE, NOT LEAKPROOF, owned by
the accepted trusted bundle owner, with only the safely captured trusted schema
then pg_catalog path. Direct EXECUTE remains owner-only. This adds a trigger
attachment, NOT a seventh changed-state reference kind or new SQL accessor.

Before ANY privileged parent read, the new branch validates its exact permanent
schema/namespace/target/function OIDs and names, trigger identity, enabled flag,
BEFORE/ROW/INSERT, no arguments/predicate and nondeferrable catalog projection.
INSERT uses the native nonnull NEW.epoch_id. Preserve legal nonorigin epoch0;
do not impose D36 positivity before a genuine origin lookup elsewhere.

Then perform exactly one qualified parent-counter PK existence lookup at E,
using the VOLATILE read-write SPI visibility of changes actually made so far.
Require the exact permanent owner-controlled counter relation. Missing parent
raises a bounded generic error BEFORE insertion/RETURNING/queued FK execution;
present parent returns native NEW unchanged. No member lookup is performed in
this branch: parent presence is NOT inferred origin membership or absence.

This is internal validation read privilege only. It grants no caller SELECT,
INSERT, helper EXECUTE or accessor result; does not learn an allowlist from ACLs;
and imposes no new-table SELECT/INSERT, role allowlist, SET ROLE or D36-only GUC
restriction on an otherwise-qualified nonorigin parent-first caller. The native
FK remains unchanged and still runs. No source/counter/row repair, lock, DML,
DDL, grant, session mutation or private-temp access occurs in the function.

The original override AFTER constraint remains installed, enabled and initially
deferred. It still proves genuine membership then original-session authorization
and complete D36 source/clock/OLD/NEW semantics. The other five AFTER branches
also remain exact. BEFORE success is ONLY parent visibility, never a successful
D36 transition or substitute for deferred validation. New supported parent-first
compatibility does not waive any original-session or causal requirement after
actual origin presence. Both branches need closed per-query privilege/call
projections in future full source/H36 review; do not widen old helper grants.

## 5. Native exclusion argument and limits

Under the exact enabled/catalog-qualified new BEFORE guard and ordinary native
INSERT/UPDATE/DELETE boundary:

1. Any override INSERT physically performed before origin has a parent already
   visible at its BEFORE cut. The row cannot be inserted first and erased in a
   RETURNING callback while bypassing that prerequisite.
2. Unchanged013 forbids counter DELETE and epoch/declaration rekeying. Removing
   or changing the override cannot erase the parent established by the BEFORE
   prerequisite. Ordinary counter UPDATE preserves parent existence/identity.
3. A later VOLATILE origin BEFORE lookup sees that counter and rejects. If a
   sibling counter operation has not executed yet, the BEFORE override lookup
   fails instead; textual CTE ordering is not authority.
4. After origin insertion, all later override events see the immutable real
   membership through the unchanged AFTER lookup and cannot take an absent
   branch because access/source/binding is missing. Early named forcing then
   either validates the complete causal proof or rejects.

For an uncommitted parent from another transaction that is not visible, the
new BEFORE guard rejects rather than relying on a future FK wait/success. This
is explicitly within the new raw-SQL restriction. Full source/race qualification
must also retain same-outer-XID counter creation and original epoch serialization;
no counter from another transaction can be adopted into origin-final closure.

This argument does NOT establish the rest of the full validator. Exact operation
reconstruction, complete override affected-key/before-image induction, physical
rowcounts, canonical M4 JSON payload, real source/dispatch/attempt/terminal proof
and D24/D25 accounting remain mandatory. No stats, command-ID ordering or new
journal supplies a missing proof. Counter DELETE/rekey exclusion concerns
ordinary native DML, not trusted owner/superuser DDL/TRUNCATE/catalog control;
that existing D37 boundary remains and no security claim is added.

## 6. Mandatory additional falsifiers — all executable evidence PENDING

1. Reproduce BOTH source-derived counterexamples separately in the approved
   future isolated fixture: parent-present pre-origin INSERT->DELETE with ONLY
   the named AFTER guard immediate; and orphan INSERT/volatile RETURNING delete
   before end-statement FK checking. Record actual rejecting guard/layer.
2. New BEFORE rejects INSERT without a visible parent before physical row and
   callback effects; existing parent plus erased overrides cannot rescue late
   origin insertion. Counter DELETE/rekey attempts reject. Also test existing
   counter0,1 and later/terminal states; none is absence for fresh origin.
3. Counter already physically written via sibling CTE/volatile SPI is visible
   to origin and rejects. Counter not yet executed cannot qualify BEFORE
   override insertion. Do not assume SQL textual order, ALL constraints or cmin.
4. Both concurrency orders: a genuinely visible parent-first legacy operation
   retains its result; a not-yet-visible concurrent parent is the explicitly
   rejected new raw-SQL case. No lock/order reversal or foreign-counter adoption.
5. Genuine fresh owner OPEN passes origin-before-counter, canonical expected
   declaration then actual fresh counter1 and full D24/D25 final closure.
   Missing/wrong/stale counter/hash/P/scope/XID, UPDATE-masked initial counter,
   early forced origin-final and subtransaction/caller-flag forgeries reject.
6. Exact new trigger negatives: target/schema/temp/OID/name/event/timing/ROW/
   predicate/arguments/enabled/deferral/function drift. Native epoch0 parent-
   first legacy positives remain legal; malformed native keys reject.
7. Owner and separate old-table-authorized LOGIN lacking BOTH new-table grants
   pass otherwise-valid parent-first nonorigin override INSERT. Preserve D37's
   four-old-target compatibility and six-AFTER true-absence behavior, except
   the explicitly changed child-first/not-yet-visible-parent INSERT rule.
8. No PUBLIC/nonowner direct EXECUTE, new caller grant, old function flag/ACL/
   proconfig change, missing/extra object or partial/orphan023 catalog passes.
   Closed successor catalog now pins TEN new triggers, not an old nine count.
9. Every original D36 group1--11, D37 raw-write/session/feasibility/coverage gate,
   D25/D26/D33/P33/C1 falsifier, independent oracle, real producer byte/time,
   installed-wheel, rollback/race and whole-repository diagnostic remains mapped
   and mandatory at its own stage. No blanket legacy/speed/AI/runtime PASS.

No deselection, xfail/skip waiver, fake READY/terminal/history, inline measured
oracle, undercharge, temporary privilege bypass or old-source transplantation.
Schema-only negatives are not production OPEN/lifecycle positives.

## 7. Candidate custody and separate authority/source barriers

This NEW isolated docs-only proposal starts from published literal main:

```text
parent = ca98e1ce9fac9bc48d4eae19ab49616a683e7806
parent_tree = c5e95435f1c248cabfd4ea590e0260cea9f29395
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d38-clock-birth-order
branch = workstream/m5-d38-clock-birth-order
sole_owned_path = docs/workstreams/m5_runtime_contract/DOCUMENT_CLOCK_BIRTH_ORDER_AMENDMENT.md
```

External R2 evidence:
/home/kassym/Desktop/groundloop-d38-clock-birth-order.fBtV4BMD/.
PRECREATION.json SHA256
63bcf6a4c93188792b1308579a3817cc748dbd2bce7a0846091d984a34bfdab2
captures all156 preexisting worktrees/complete dirty-file modes/blobs,780 parent
blobs, P33/C1 and15 container identities/states before this157th checkout.
The earlier preparation failure remains in its separate D0Es8HVi evidence root;
it created no worktree/branch/SQL and is not passing execution evidence.

SAME S36 stays at ca98 with EMPTY index, seven current WIP files/five live-test
paths ABSENT. Its1610-line023 SHA256
06f4bbe589382ca06d3cf197bd0e038790e3cb04c89683527736174d92fbdbd6
and1044-line bundle-test SHA256
d513e4ccaa03782f20412b6a646612d4fbde6f080d2336517dd402a2de0a1064
are preserved. R9 actual200 focused/1233 complete pure-runtime passes overlap
earlier runs and prove no SQL/birth behavior. PRESENT-origin execution still
unconditionally raises; H36 pins stay empty/disabled. No source birth-order,
trigger inventory, runtime default, DB/role/grant or provider change occurs here.

Freeze the COMPLETE candidate path/SHA/blob/bytes/lines/mode/parent/tree/custody;
obtain two actual independent COMPLETE same-byte GO/P0=0/P1=0 audits. Any edit
revokes BOTH. Candidate publication and authority freeze require the explicit
revised user decision above; never turn the ordering-only answer into authority
for the tenth trigger or raw-SQL exception. Normal exact commit, two fresh full
postcommit checks, atomic no-force branch/main publication and live refs apply.

After the approved candidate gate, prepare a SEPARATELY audited authority freeze
with a new decision-log entry, runtime-addendum revision19, exact precedence,
acceptance/ownership/status/roadmap and a literal handoff. No original D36/D37
amendment bytes are rewritten. Then accept a NEW exact twelve-path/two-dispatcher
body S36 continuation: preserve all seven current WIP/five absences and P33/C1,
allocate ONLY this complete birth/parent delta alongside unfinished D36/D37,
and explicitly authorize checked docs-only SAME-S36 fast-forward ancestry.
Check BOTH endpoint and every incoming commit union against all S36/P33/C1 paths.
No reset/stash/rebase/transplant or automatic moving-main adoption.

Future H36 must use the ACTUAL accepted new-continuation literal parent, replacing
the old ca98 pin only by that audited ancestry authority. COMPLETE twelve-path
source plus TWO complete source GOs precede the separately audited literal
H36 preinstall freeze. Exact SQL/five-field bundle/full six bodies/catalog/
flags/owners/ACLs/config/dependencies/roles/finite permissions now include the
tenth BEFORE trigger. No023 execution, fixture bypass or live-derived expected
pin before that accepted/published barrier and checked same-S36 advance.

Later private producers/receipts/context/complete predecessor/both execution
orders/P33 and separate C1 overlap reconciliation require their own activations.
This candidate grants none of those paths or any held-checkout advance.

## 8. Claim ceiling

Not frozen or implemented. D38 and M5.0-38 remain PROPOSED/PENDING. D25
integration, D36/D37 implementation, S36/P33/C1, Task2/Task17, M5.4--M5.6,
deployment/security, AI quality, independent natural-history validation and
end-to-end utility/speed/cost remain PENDING. Runtime stays v1_only outside
isolated fixtures; frozen M3 remains default. No semantic kind, work counter,
new digest recipe, tombstone, nullable semantic hash or repr/JSON clock hashing
is added. Native M4 JSON recipe is retained, not a new proof recipe.
New provider calls/spend/reserved remain0 under aggregateUS$5. Task14 labels
retain self-reported AI-assisted project-author review, not independent/blinded
human adjudication. The four-deliverable completion campaign is not complete.
