# M5-D25 Schema-017 Lane B Handoff

Status: **HOLD — B1 and executable B2 checkpoints complete; B3 pending; not
an accepted migration-017 candidate and not D25 implementation evidence**

Date: 2026-09-04

## Exact base and ownership

The lane started from activation commit
`691e3d174e059ac041d3a46678fcb630e15478d5` on branch
`workstream/m5-d25-schema-017`. It touched only the four authorized Lane B
paths: migration 017, the migration installer, its new test module, and this
handoff. This bounded B1 checkpoint may be committed for audit continuity, but
no Lane B result may integrate until the deferred-validation contract is
complete.

## Implemented investigation surface

The current uncommitted bytes provide the 13 D25 current/working/image,
patch/contribution and accumulator relations, representative indexes, local
Hall checks, the exact 41-table `ACCESS EXCLUSIVE MODE NOWAIT` installer lock
tuple, pinned migration-016 prerequisite, content-ledger identity, top-level
read-write `READ COMMITTED` and ambient-transaction rejection, post-lock
ledger reread, first-install history guards, grouped failure injection,
ledger-last commit, empty `v1_only` installation, activated-no-history
reconstruction skeleton, and scoped transition/seal/activation DML guards.

Bounded subphase B1 now implements a recursive canonical decoder/re-encoder
for D1 typed fields and the retained logical-value grammar. It checks every
current/working physical point family, group shapes, bounded masks, Hall
arithmetic, immutable edge coordinates, exact source/result points,
component-wise family order/uniqueness, the nine logical-output blocks and
allowlisted nested artifacts, decoder-derivable after hashes, certificate
bindings, Python-compatible strip-empty identifier checks pinned to the exact
29 accepted code points, all four paired physical arrays, the outer patch, 37-counter work,
and contribution identity. Frozen self-contained vectors were generated from
accepted Lane A commit `6591d4779cd59bec4bcb9aa15c1b44901e0049f7`;
Lane B does not import or edit Lane A-only paths. The installer now compares
psycopg's concrete isolation enum and exposes
`after_initial_ledger` / `after_install_locks` failure-injection boundaries.

The seal guard was corrected so deletion checks the matching working
tombstone for the sealing epoch rather than comparing the predecessor
current-row installation point with the new seal point.

## B2 transaction-bijection checkpoint

The accepted D25 Sections 4.7, 5 and 6 require a deferred checked validator
that decodes every retained typed preimage and enforces one exact transition
bijection across the runtime revision, working image, every D25 working row,
migration-014 semantic state and certificate rows, patch artifact,
contribution, and independently summed accumulator. The current B2 SQL adds
xid/backend/session-bound `pg_temp` context, change-journal and
expected-key tables with `ON COMMIT DROP`; first OLD/final NEW plus an
insert/update/delete operation mask; preserved migration-014 working-state
and revision-interval checks with conditional journaling; artifact INSERT
journals; and deferred roots on the runtime revision plus every journaled
in-lock relation. It has canonical
length-framed keys, byte comparisons for all four physical families and
migration-014 logical states/bindings, symmetric `EXCEPT ALL` key checks,
exact one-operation shapes, certificate insert/reuse/removal checks,
status-delta derivation, and component-wise accumulator OLD-plus-contribution
checks. The accumulator requires exactly one structural INSERT with no OLD or
one later UPDATE whose retained OLD revision and 37-counter digest are exact;
all NEW counters must equal OLD plus the contribution. Post-install `migration` mode is
explicitly closed. `groundloop_status_delta` is outside the frozen 41-lock
tuple, so B2 intentionally adds no trigger or lock there; the validator
compares exact final epoch/revision rows against the logical output and derives
their statuses from journaled claim/answer OLD/NEW state.

B1 passed its full live restart and two independent same-byte audits. B2 now
has executable empty and nonempty structural success, exact later-update,
source/context/operation/counter/journal/privilege falsifiers, rollback-safe
fixture isolation, and a complete 84-test restart. Complete seal-promotion
and activated-upgrade behavior remain B3 work.

Separately, B3 retains a P1: the activated-without-history Hall backfill uses
`count(*)` over a left join and therefore reports one absent mask. It must
count a nullable matched D25 key before final Lane B freeze.

The validator binds and sums all 37 counters but does not claim migration 017
independently observes SELECT-side probe/representative/augmenting operations;
later store call sites remain responsible for producing those observed
values. The remaining items are P1 correctness requirements, not optional
later store checks.
Application validation cannot replace the frozen database constraint. The
overall lane therefore remains HOLD without a migration-017 PASS claim.

## Smallest safe continuation

After both final B2 same-byte auditors return GO, commit the four-path B2
checkpoint on the isolated branch. Then keep the same four-path ownership for
the B3 backfill fix and full seal/activation gates. Restart all static/live
gates and obtain an independent exact-byte final Lane B audit before
integration.

## B2b independent-review HOLD

Two independent reviews of the B2a snapshot reported `P0=0` and required a
stricter root on every journaled relation, exact one-operation shapes,
working-image and runtime OLD/NEW proofs, private-helper privileges, derivable
counter checks, certificate-removal handling, and exact currency equality.
The current unexecuted B2b WIP addresses the rooted relation surface and exact
runtime, image, patch, contribution, accumulator, physical, semantic-state,
binding and certificate operation shapes. It accepts certificate `after=None`
without deleting immutable history and revokes public execution from the two
internal direct-entry helpers without claiming protection against the schema
owner. The bounded structural positive/no-anchor/multi-mutation fixture has
not yet been added, and no B2b PostgreSQL execution has occurred. It remains
HOLD pending same-byte review and executable tests.

The coordinator resolved an apparent currency conflict by applying the exact
textual scope: Section 4.7 roots migration-017 equality on D25 rows and
migration-014 semantic/certificate rows, while Sections 8 and 9.2 assign
affected-key/currency derivation to the later cursor-local store and separate
upstream currency coalescing from the D25 physical/logical patch. The D25
journal therefore excludes `groundloop_m5_working_currency_history`, adds no
trigger or lock there, and preserves the migration-014 interval guard
semantics. This avoids both an invented patch field and circular
self-authorization. Exact upstream currency derivation remains a later store
responsibility under the existing migration-014 constraints.

## B2c semantic checkpoint

The next same-byte review found no P0 and retained HOLD. B2c closes the mode
escape by treating any surviving private context or pinned OID with a cleared
or changed transition mode as an error. Structural runtime authority is now
order-independent: the exact runtime row must have revision 1, matching
epoch/event identity, a nonterminal state, and an `xmin` from the current
transaction. Later sources retain the exact one-UPDATE OLD/NEW proof. The
public authorizer locks and compares both the base epoch revision and runtime
revision after the checked-transition Boolean is present.

Patch source identity now dispatches to the exact structural event, active
requirement-completion attempt/result/verifier chain, or delta-only M4 direct
transition with exact before/result revisions. The private context and journal
helpers use the installation schema through `SECURITY DEFINER SET search_path
FROM CURRENT` while retaining the public revoke, session-user, xid and pinned
temporary-table OID checks; this is not a defense against the schema owner.

The validator independently checks retained-byte derivable output bytes,
observation contribution additions/removals, unequal edge/mask key counts,
mask-XOR bit crossings, initialized masks, Hall zeta/subset/neighbour/
deficiency formulas, status counts by type, touched group/claim/answer unions,
state-only classifications from exact effective OLD/final NEW, and
certificate-or-binding-only classifications before completing the transition.
`requirement_observation_changes_processed`; policy candidates/probes and
ordered index/sort work; representative reads; augmenting searches/visits; and
certificate repair/reconstruction/rebinding/digest-input operation counts
remain nonnegative, digest-bound store evidence because retained bytes
do not independently prove their execution paths. No B2d PostgreSQL execution
or new live fixture has occurred; the lane remains HOLD pending same-byte
review and executable tests.

## B2e initialization, mode and captured-path checkpoint

B2e separates newly initialized Hall groups from ordinary mask transitions.
Only positive masks belonging to a Hall `before=None`/present-after image count
as `hash_masks_initialized`; they are excluded from transition, XOR, subset and
neighbour work, while an absent-to-positive mask in an existing Hall group is
an ordinary transition. The final initialized positive-mask cardinality must
still equal the retained Hall `distinct_hash_count`. The independently derived
`group_local_state_operations` is the sum of distinct ordinary-mask groups and
distinct group-binding output groups, independent of artifact insertion.

All seal, activation and migration DML/helper entry paths now reject a surviving
transition temp context or any pinned transition OID GUC. A transition still
requires the exact context. Before executing migration-017 DDL, the installer
sets the transaction-local search path to exactly the quoted `current_schema()`
and `pg_catalog`, so trusted security-definer trigger/authorization paths can
invoke the two public-revoked internal helpers without a public/writable
fallback. `public.digest` remains explicitly qualified. The schema owner
remains trusted and can bypass this privilege boundary; no malicious-owner
security claim is made.

This checkpoint passed PostgreSQL parsing, Ruff, project-environment strict
mypy, collection of 70 tests and `git diff --check`. No B2e PostgreSQL test was
executed, so B2 remains HOLD pending same-byte review and live fixtures.

## B2 executable-fixture checkpoint

The B2 fixture tranche now contains a same-transaction empty
`structural_open` success, an exact later `direct_transition` revision
`1 -> 2` path, prior-transaction and forged-source runtime authority
falsifiers, cleared-mode and duplicate-operation failures, independently
re-digested `output_bytes` and accumulator mismatches, and a valid immutable
claim-certificate insertion that must be rejected by the global unconsumed
journal check. The privilege fixture uses a genuine fresh login session: it
checks `has_function_privilege` for all four private helpers and both public
authorizers, directly proves begin-context and journal-helper denial with
rollback between errors, and then exercises the public checked/D25
authorization path. Its broad table/sequence grants apply only to the
disposable isolated test schema and are not a production grant policy.

The latest static review also closed two working-image/header P1s. A working
image keeps its sealed-current `base_epoch_id`/`base_revision` and policy for
the entire epoch; later transitions may advance only `updated_revision`.
Structural source authority now binds the M5 update, typed candidate policy,
both publication heads, sealed predecessor coordinates/state, and current
image coordinates. The current image policy is independently required to be
the policy interval covering the predecessor; it is deliberately not required
to equal the new patch policy, so a real `policy_change` remains admissible.
Later base- and policy-mutation falsifiers are collected, and the source
predicate has a focused old-policy/new-policy distinction assertion.

Live investigation on the preceding fixture bytes retained the migration DDL
compile failures and then proved the first-install smoke and structural
positive independently. It also retained and repaired only adjudicated SQL
defects: SQL-NULL mode exits, begin-context argument arity, private trigger
function security-definer execution, and captured-search-path digest lookup.
The D25-local digest helper is byte-identical to the accepted length-framed
algorithm and calls `public.digest` explicitly. The independently coherent
`output_bytes` negative subsequently passed. The forged-source, later-update,
valid unconsumed-journal, and real non-owner fixtures have not yet been run.

On that intermediate static snapshot, PostgreSQL parsing accepted 88
statements over 213332 SQL bytes; Ruff, strict mypy, compileall and
`git diff --check` passed, and the module collected 83 tests. The subsequent
history below preserves the failed runs that preceded the final 84-test B2
result. B3 and seal/activation work remain outside this checkpoint.

The first complete 83-test live restart on these bytes retained `66 passed / 17
failed` in 20.12 seconds and restored the exact database guard. Inspection of
all failures found three roots: a transaction-control assertion confused `ON
COMMIT DROP` with a transaction statement; seal-delete code referenced OLD
fields that do not exist on every current relation; and the full nonempty byte
vector attempted to reuse the deliberately closed post-ledger migration mode,
poisoning the shared fixture and cascading cross-mode cases. The assertion now
rejects only anchored SQL transaction statements while retaining `ON COMMIT
DROP`. Seal deletion reads relation-specific fields through the generic JSONB
row image, so `image_current` deletion rejects cleanly and the four physical
families retain their exact tombstone tests. The nonempty vector now runs on a
fresh pre-ledger DDL fixture; the sealed production ledger is never bypassed.
Raw hand-set transition probes were also aligned with the stronger private
context prerequisite and rollback independently after each expected error.

Focused remediation evidence retained an initial `2 passed / 1 failed` regex
sequence, followed by the corrected transaction assertion pass. The exact
root cluster then passed `7/7`: transaction assertion, first install/rerun,
structural image bootstrap, full nonempty retained vector, and all three
cross-mode/context cases. The focused fixtures restored zero active other
clients and zero `d25_migration_017_*` schemas. At that boundary a complete
restart and independent exact inventory-hash confirmation still remained;
both were later completed as recorded below.

The pre-ledger full nonempty fixture is now named and documented strictly as
Lane-A constructor/trigger coverage and asserts that the migration-017 ledger
row is absent; it is not B2 deferred-validation evidence. A separate fully
authorized B2 structural-open fixture seeds an authoritative one-requirement
group, persists a nonempty Hall initialization with zero hashes, supplies the
exact Hall zeta/subset/deficiency work counters plus logical output bytes,
executes `SET CONSTRAINTS ALL IMMEDIATE`, observes
`validation_done = true`, commits, and re-reads the retained Hall row.

The nonempty fixture retained two setup failures before acceptance: the first
used placeholder group digests and failed whole-group integrity; the second
placed the Hall child array in the edge slot and failed the typed domain check.
After changing only those fixture roots, the focused authorized Hall test
passed `1/1` in 1.68 seconds. The immediate postguard again observed zero
other active clients and zero `d25_migration_017_*` schemas. The top-level
transaction detector now removes dollar-quoted PL/pgSQL bodies before rejecting
anchored `BEGIN`, `START TRANSACTION`, `END`, `COMMIT`, `ROLLBACK`, or `ABORT`
statements, while explicitly retaining legitimate `ON COMMIT DROP`.

The next complete 84-test restart retained `79 passed / 5 failed` in 24.22
seconds with exact inventory and role restoration. All five were fixture
roots. The three later-transition cases now insert the M4 direct update only
after the exact current-transaction epoch, typed update and runtime sidecars,
matching the accepted bridge guard without a raw GUC or production weakening.
The unconsumed valid claim-certificate row expects the validator's exact
`certificate journal has an extra row` rejection. The disposable login role
uses a safely composed `sql.Literal` password rather than an illegal utility
statement bind parameter.

The first focused five-node attempt retained `3/5`: the policy-mutation setup
tried to create a second open policy, and the non-owner session omitted
`public` from its ordinary search path, causing a legacy migration-015 digest
lookup to fail before D25 validation. The alternate policy now has an exact
closed interval, and the ordinary runtime path is
`<isolated schema>, public, pg_catalog`; D25 security-definer functions retain
their separately pinned safe path. After only those fixture corrections, all
five nodes passed. The postguard was `0` other active clients, `0`
`d25_migration_017_*` schemas and `0` `d25_runtime_*` roles.

The coordinator then restarted the exact complete module from test one:
`84/84` passed in 23.94 seconds (`24.69` seconds wrapper time, exit `0`). The
independent postguard was exactly `0` other active clients, `0`
`d25_migration_017_*` schemas, namespace inventory SHA-256
`9521c8ef7907a31bafccd0ca2f416a964ea505a156e2e8ca82503f92e31fcdf5`,
and `0` `d25_runtime_*` roles. This completes the executable B2 gate on the
recorded bytes while preserving B3 and final Lane-B acceptance as pending.

## Non-acceptance evidence

The first live run was retained as `5 passed / 4 failed`; failures were caused
by an invalid immutable-ledger conflict setup that left the shared connection
aborted. After repairing only that harness, the module passed `9/9`. An
expanded run passed `14/14`, including mid-group rollback/fresh retry, NOWAIT
partial-prefix release/retry, cross-mode/cross-epoch rejection, rerun,
conflict, raw-DML rejection, and exact fixture inventory restoration.

After the resumed decoder/validator work, complete restarts retained these
non-acceptance sequences: `8/18` (function-result array-subscript SQL),
`8/18` (multi-record `SELECT INTO` SQL), `16/18` (two overbroad harness
assertions), and `17/18` (one stale trigger-function name). After repairing
only each observed fault and restarting from test one, the focused module
passed `18/18`. Fresh Ruff, mypy and compileall checks passed, and PostgreSQL
parsing accepted all 37 migration statements. This surface still lacks the
activated-without-history target comparison, post-lock ledger race, full
typed-vector, transition set-equality, and complete seal-promotion falsifiers;
the focused pass is not acceptance evidence.

The first expanded B1 restart retained `13/24`; the next retained `20/25`
(two decoder roots and three transaction-abort cascades). After correcting
only those roots, the full module passed `25/25` in 4.60 seconds and exact
inventory restoration was independently confirmed. Subsequent completeness
remediation expanded the self-contained collection to 68 tests, including a
full four-family artifact-trigger vector. The first live restart on those
bytes retained `60/68` in 5.42 seconds: two psycopg JSONB adaptation faults,
the logical validator's `STRICT`/default-NULL trap, an invalid unsealed
predecessor fixture, and three transaction-abort cascades. Only those observed
roots were repaired. The next complete restart retained `57/68`: one JSON-null
versus SQL-NULL shape predicate rejected a valid absent mask before-image, the
decoded change omitted its top-level requirement identifier needed by exact
observation/group-shape validation, and nine failures cascaded from those two
aborted transactions. Only those observed roots were repaired. The resulting
bytes then remained HOLD after an independent static audit found that the
patch-point helper confused JSON `null` with SQL NULL: it could admit an
absent after-image and incorrectly enter before-image laws for an absent
before-image. The predicate now rejects JSON-null after-images explicitly and
runs before laws only for a non-JSON-null point; retained-artifact coverage
recomputes the affected child and outer digests to reach that exact law, while
the valid absent-before case is asserted explicitly. The following restart
retained `63/68`: one
new direct JSON call lacked its psycopg `Jsonb` adapter, one paired-array
mutation correctly failed the table cardinality constraint before the later
trigger message expected by the harness, and three failures cascaded from
those aborted transactions. The same missing adapter shape was corrected at
all four direct point-law calls, and the exact expected constraint was
corrected. The next restart retained `61/68`: the structural-open before-layer
falsifier supplied an after revision that failed the earlier mandatory result
point law, and six failures cascaded from that aborted transaction. Its after
point was corrected to the declared structural result `(epoch_id, 1)` so the
test reaches the intended non-current-before rejection. The final complete
restart passed `68/68` in 5.15 seconds. An independent post-run guard observed
zero other active clients, zero `d25_migration_017_*` schemas, and the exact
baseline non-temporary namespace hash
`9521c8ef7907a31bafccd0ca2f416a964ea505a156e2e8ca82503f92e31fcdf5`.
This closes B1 only; the B2/B3 contract gaps above keep the lane on HOLD.

Every live fixture used a unique repository-local Compose schema and asserted
the exact before/after schema tuple. After the final live run there were zero
`d25_migration_017_*` schemas. These are investigation results only; they do
not cover or waive the blocking validator requirements.

No database deployment, store/runtime implementation, runtime-mode change,
provider execution, status promotion, or D25/M5.4 implementation PASS is
claimed.
