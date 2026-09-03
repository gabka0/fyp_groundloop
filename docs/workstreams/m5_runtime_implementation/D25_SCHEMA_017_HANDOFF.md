# M5-D25 Schema-017 Lane B Handoff

Status: **HOLD — B1 decoder checkpoint complete; B2/B3 pending; not an
accepted migration-017 candidate and not D25 implementation evidence**

Date: 2026-09-03

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

## Blocking contract gap

The accepted D25 Sections 4.7, 5 and 6 require a deferred checked validator
that decodes every retained typed preimage and enforces one exact transition
bijection across the runtime revision, working image, every D25 working row,
migration-014 semantic state and certificate rows, patch artifact,
contribution, and independently summed accumulator. The current SQL proves
the source-keyed patch/contribution identity, final working-image/runtime
revision, and independently summed 37-counter accumulator. B1 passed its full
live restart and two independent same-byte audits. B2 does not yet:

- prove the exact set of current-transaction physical and semantic/certificate
  writes equals the decoded patch;
- journal typed OLD/NEW rows, including deletes, across every required D25 and
  migration-014 relation;
- bind declared counters without falsely claiming migration 017 observes the
  store's SELECT-side algorithm operations; or
- enforce the complete seal-promotion bijection.

Separately, B3 retains a P1: the activated-without-history Hall backfill uses
`count(*)` over a left join and therefore reports one absent mask. It must
count a nullable matched D25 key before final Lane B freeze.

Those are P1 correctness requirements, not optional later store checks.
Application validation cannot replace the frozen database constraint. The
overall lane therefore remains HOLD without a migration-017 PASS claim.

## Smallest safe continuation

Commit the bounded B1 checkpoint on the isolated branch, then keep the same
four-path ownership while implementing B2 journaling and set equality,
followed by the B3 backfill fix and full seal/activation gates. Restart all
static/live gates and obtain an independent exact-byte final Lane B audit
before integration.

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
