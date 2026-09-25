# M5-D31 Sequential Implementation Activation

Status: docs-only path-exclusive activation candidate; it grants no database
installation or runtime acceptance until the gates below pass, and no
implementation lane may start until these exact bytes receive two independent
same-byte `GO`, `P0=0`, `P1=0` reviews, are committed as the sole path change,
fast-forwarded to `main`, and pushed

Date: 2026-09-25

## 1. Exact authority barrier

```text
required_activation_parent = d5253e4a9b320a139205bd0ba7eaefde1c1f6141
required_activation_parent_tree = b3735ffe9ac1b83ad27c68f3bc209b30e5eb371e
accepted_d31_candidate_commit = abce709d25e00c5774ac1b49cf046f2783e5fb2c
accepted_d31_candidate_tree = 2ab0a97206a486cd07f0308116ef8e1059eec514
accepted_d31_candidate_sha256 = 6331c8149e38031c51cb22b6a9dc2d49d30d27df67d25b5fae24b00c52058dc9
accepted_d31_freeze_commit = d5253e4a9b320a139205bd0ba7eaefde1c1f6141
accepted_d31_freeze_tree = b3735ffe9ac1b83ad27c68f3bc209b30e5eb371e
runtime_addendum_revision = 12
runtime_mode = v1_only
```

The sole path owned by the initial activation commit is this file. That
initial commit must have the exact sole parent above and may change no other
path. The later P hash-pin amendment has the separately constrained S0 parent
and ownership in Section 4.2; it must not claim `d5253e4` as its parent. The authority
order is `AGENTS.md`, the M5 design freeze, runtime addendum revision 12, the
accepted D24--D31 amendments, the acceptance matrix, the D31 freeze handoff,
and this activation. Frozen contract authority wins over this ownership plan.

M5-D31 and M5.0-31 are contract-`PASS` / implementation-`PENDING`. This file
implements no migration, function, installer, runtime helper, seal route,
test, database state, provider, model, public API, runtime-mode change or
claim. It replaces the historical D30 implementation activations only for the
three corrected lanes and exact ordering named here; all unaffected D30
semantics and ceilings remain exact.

## 2. Authorized outcome and hard sequence

This activation authorizes one narrow prerequisite chain:

```text
S -- migration-019 schema/accessor lane
  -> R -- revised C1 prerequisite repair consuming the accessor
  -> C1 -- resumed structural store/runtime composition
```

The lanes are strictly sequential. Lane R may not mutate until Lane S is
accepted, integrated and pushed. Lane C1 may not mutate until Lane R is
accepted, integrated and pushed. There is no parallel implementation in this
wave and no lane inherits another lane's paths.

Lane S has a mandatory pre-install byte-freeze sub-barrier because accepted
M5-D31 requires the exact migration and bundle hashes to be reviewed and
pinned before the first installation:

```text
S0 = author and audit final migration-019 SQL bytes without installing them
  -> integrate the exact S0 SQL-byte commit
  -> P = docs-only hash-pin amendment to this activation
  -> S1 = installer, executable tests and final schema handoff
```

S0 and S1 are subphases of the single schema lane, not independent semantic
lanes. Before P integrates, no command, fixture or test may execute migration
019 against any database. The P amendment must record the exact S0 commit,
blob, byte count, line count, migration SHA-256 and derived bundle SHA-256,
receive two independent same-byte audits, and be pushed. This satisfies the
accepted requirement to pin reviewed final SQL bytes before first
installation without embedding a circular hash in the SQL file.

No later phase may alter the pinned SQL bytes. If S1 exposes a required SQL
change, the schema lane stops and repeats S0 and P on new reviewed bytes; it
must not update a literal opportunistically inside a test run.

## 3. Exact migration-018 prerequisite

Every schema and runtime phase fails closed unless the selected schema carries
this exact accepted five-field migration-018 ledger row:

```text
bundle_id = m5-bounded-document-withdrawal-schema-bundle-v1
bundle_sha256 = 9c45e58fb5c61156d4d07aa0c9b767112bf285452664f39731d893445e7a9e4f
migration_sha256 = 941bba975c12e9fb5ba4b4f75a82e59fa518b23eac34468ed2f8b15d1cd9ed90
oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
prerequisite_sha256 = 52240e19968926d0c051fe6146b3c7d877cf582014341efbfcc78637f3ff5761
```

Migrations 001--018 and all their ledger identities remain byte-identical.
Migration 019 may create exactly the one D31 function and explicit `PUBLIC
EXECUTE`; no table, column, type, sequence, index, trigger, view, backfill,
replacement of an earlier function, or other privilege change is authorized.

## 4. Lane S -- migration 019 and trusted accessor

```text
branch = workstream/m5-d31-schema-019
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d31-schema-019
base = exact pushed commit of this activation
```

The complete technical Lane-S delta relative to the pushed initial activation
is limited to exactly these four paths:

1. `migrations/019_m5_preterminal_seal_context.sql` (new);
2. `src/groundloop/postgres/migrations.py`;
3. `tests/m5/postgres_runtime/test_migration_019.py` (new); and
4. `docs/workstreams/m5_runtime_implementation/D31_SCHEMA_019_HANDOFF.md`
   (new).

Every other path is read-only. Discovery of a fifth required path is a hard
stop for a docs-only manifest amendment.

The coordinator-owned P amendment changes this activation file outside the
technical lane. Exact cumulative accounting is:

```text
initial activation .. S0 = migration 019 plus schema handoff
S0 .. P = this activation file only
P .. S1 = migrations.py, test_migration_019.py and the schema handoff;
          migration 019 is byte-identical
initial activation .. S1 = this activation file plus the four technical
                           Lane-S paths above
```

### 4.1 S0 SQL-byte freeze

Before the P hash-pin amendment, Lane S may edit only paths 1 and 4. The
handoff must say `SQL_BYTES_CANDIDATE / NOT_INSTALLED`; it may contain static
review evidence but no database result. `migrations.py`, the executable test
module, every database and every fixture remain untouched. The final S0 SQL
must define exactly:

```sql
groundloop_m5_matching_read_preterminal_seal_context(
    bigint,
    bigint,
    bigint
)
```

with this exact return order and types:

```text
policy_version text
anchor_m4_epoch_id bigint
anchor_m5_epoch_id bigint
anchor_m5_revision bigint
anchor_activation_count integer
anchor_predecessor_revision bigint
anchor_predecessor_sealed_at timestamptz
anchor_current_policy text
```

It must
be `LANGUAGE plpgsql`, `STABLE`, `CALLED ON NULL INPUT`, `SECURITY DEFINER`,
non-leakproof, parallel-unsafe, and carry `SET search_path FROM CURRENT` from
the installer-selected `<schema>, pg_catalog` path. It must revoke implicit
function execution and then explicitly grant `EXECUTE` to `PUBLIC` so the ACL
is intentional and catalog-verifiable.

The function must raise on NULL, nonpositive or nonadjacent coordinates and
must prove the exact checked-transition/promotion GUCs, absence of every
transition triplet, genuine promotion triplet OIDs and relation properties,
owner equality, current backend/xid/session role, unique context row, exact
seal coordinates and policy, and both validation flags still false. It returns
the policy and all seven anchors verbatim from that same unique context row;
it does not reconstruct or validate them against persistent state. It returns
exactly one row only after every check succeeds. It reads no persistent
GroundLoop application or envelope-state relation: only `pg_catalog` metadata
needed for the triplet proof and the one temporary context row may be read,
never promotion-journal or expected-set contents. It performs no DML, DDL,
lock statement, GUC mutation, constraint forcing or validation start.

Two independent reviewers must audit identical S0 SQL and handoff bytes for
contract semantics and PostgreSQL privilege/catalog behavior. After `GO`,
`P0=0`, `P1=0`, S0 receives one exact commit and two postcommit identity
checks, then is pushed to its branch and `origin/main` as an inert byte
candidate. M5-D31 implementation remains `PENDING` and first installation
remains forbidden.

### 4.2 P hash-pin amendment

After S0 integration, use:

```text
branch = workstream/m5-d31-schema-019-hash-pin
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d31-schema-019-hash-pin
base = exact pushed S0 commit
owned_path = docs/workstreams/m5_runtime_implementation/D31_IMPLEMENTATION_ACTIVATION.md
```

This activation file alone is amended in that fresh docs-only worktree from
the exact pushed S0 commit to replace the following ledger with final values:

```text
reviewed_migration_019_commit = PENDING_S0
reviewed_migration_019_blob = PENDING_S0
reviewed_migration_019_bytes = PENDING_S0
reviewed_migration_019_lines = PENDING_S0
accepted_migration_019_sha256 = PENDING_S0
accepted_migration_019_bundle_sha256 = PENDING_S0
```

The bundle digest is derived only by the accepted typed recipe:

```text
stable_m5_digest(
  "m5-preterminal-seal-context-schema-bundle-v1",
  *TEXT("migrations/019_m5_preterminal_seal_context.sql"),
  *HASH(accepted_migration_019_sha256),
  *HASH(9c45e58fb5c61156d4d07aa0c9b767112bf285452664f39731d893445e7a9e4f)
)
```

The P amendment receives two independent same-byte audits, one exact commit,
two postcommit identity checks, integration and push before S1 or any first
install. A `PENDING_S0` field is therefore an executable stop, not a value the
installer may infer.

Within Section 10, P must change only the phase-state lines from
`NOT_AUTHORED`/`PENDING_S0`/`NOT_AUTHORIZED_BEFORE_PIN` to exact
`PINNED_NOT_INSTALLED`/the accepted hashes/`AUTHORIZED_NOT_RUN`; all broader
implementation and claim rows remain pending.

After P is pushed, and before any S1 edit or database execution, the clean
Lane-S worktree advances from its integrated S0 commit to the exact P commit
by fast-forward only and rechecks the pinned SQL blob. Its migration and
handoff history must remain ancestors of the resulting branch; no rebase,
cherry-pick or reconstruction is permitted.

### 4.3 S1 installer and executable evidence

Only after P is pushed may Lane S edit paths 2--4 and execute migration 019.
Path 1 must equal the pinned S0 blob exactly. The installer must implement the
ledger-first, one-top-level-read-write-`READ COMMITTED` transaction from D31:
initial ledger decision before lock/content/catalog mutation; exact replay
without the install lock; content conflict; exact migration-018 prerequisite;
one canonical `SHARE ROW EXCLUSIVE ... NOWAIT` ledger-table lock and ledger
reread; absent-ledger same-signature rejection; trusted local search path;
accepted statements only; complete catalog/owner/ACL verification; unchanged
private helper and authorizer; ledger write last; atomic rollback at every
injected cut; and two-order concurrent install behavior.

The executable schema test path must cover every schema/accessor-applicable
D31 falsifier group (installer/accessor groups 1--10 and the
installer-selected-schema portion of group 11), including:

1. exact identities, fresh install, exact replay, conflicting replay, wrong
   prerequisite, partial object, rollback cuts and concurrent installers;
2. the sole new object and exact signature, return row, language, volatility,
   strictness, security, search-path, leak/parallel flags, owner and ACL;
3. unchanged migration 001--018 bytes and ledgers;
4. genuine owner and distinct non-owner positives with identical eight-field
   output;
5. non-owner denial of raw context and private helper while the public
   accessor succeeds;
6. spoofed/same-name triplets, all three SQL-NULL inputs and every wrong
   GUC/OID/relation/backend/xid/role/cardinality/coordinate/policy/flag;
7. repeated-read determinism and before/after proof of no GUC, row, journal,
   expected-set, validation, persistent, lock or deferred-state mutation; and
8. installer selected-schema binding, exact function `proconfig`, rejection
   when `current_schema()` is NULL or the selected schema is nonconforming,
   and proof that later search-path entries are never a fallback.

The installed function owner must equal the unchanged owner of both the
accepted migration-017 private-triplet checker and public seal authorizer;
catalog evidence must prove all three, their unchanged definitions/security
flags and their unchanged private/public ACL boundaries.

Lane S ends only after focused and retained migration gates, compile,
format/lint/type/package checks, two independent identical-byte audits, one
exact final schema commit, two postcommit identity checks and push to its
branch and `origin/main`. Its handoff records exact final path/blob/hash/test
evidence and retains `M5-D31 implementation = PENDING` because runtime
consumption and C1 are not yet complete.

Both final Lane-S audits must return `GO`, `P0=0`, `P1=0`.

## 5. Lane R -- revised C1 prerequisite repair

```text
branch = workstream/m5-d30-c1-prerequisite-blocker-repair
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d30-c1-prerequisite-blocker-repair
base_after_advance = exact pushed Lane-S final commit
```

The held dirty worktree is preserved read-only through S. After Lane S
integrates, it may advance only by an exact disjoint-path fast-forward. The
coordinator must first recheck its exact head, empty index, raw porcelain hash
and five file hashes from the D31 freeze handoff, prove that Lane S touched
none of them, and prove that the upstream path set touches none of Lane R's
six owned paths. No reset, checkout, clean, stash, rebase, cherry-pick, patch
transplant, manual copy or reconstruction is allowed.

Lane R owns exactly:

1. `src/groundloop/m5/runtime/postgres_matching.py`;
2. `src/groundloop/m5/runtime/postgres_matching_publication.py`;
3. `tests/m5/postgres_runtime/d25_store_core/test_retirement_work_counter.py`
   (new);
4. `tests/m5/postgres_runtime/d25_publication/test_changed_state_references.py`;
5. `tests/m5/postgres_runtime/d25_publication/test_seal_promotion.py`; and
6. `docs/workstreams/m5_runtime_implementation/D31_C1_PREREQUISITE_REPAIR_HANDOFF.md`
   (new).

The valid distinct-mask-group retirement-counter change and its live test may
be retained. The held owner-only preterminal context read is rejected and must
be replaced, not accepted as evidence.

The revised package-private helper retains its frozen signature. It captures
`pg_catalog.current_schema()` exactly once, proves the exact permanent ledger
table and pinned migration-019 row in that namespace, calls the safely
schema-qualified accessor exactly once, and schema-qualifies every persistent
relation read to the same captured namespace. It may not read a promotion
temporary table, trust a later search-path schema, duplicate a weaker owner
predicate, accept caller-authored child material, or weaken any independent
half-terminal envelope check. The accessor's eight fields replace only the
inaccessible context-row read. The shared D25/D26 derivation and unchanged
terminal result-bound builder remain exact, and only terminal builder bytes
are eligible for insertion.

Owned tests must install exact migration 018 and pinned migration 019 where
the positive needs them and must prove owner and genuine distinct non-owner
preterminal preparation, one-schema binding, exact REPLACE/RETIRE and
present/certificate/absence child equality, contribution before terminal,
rejection after terminal, final result-bound equality, deferred validation,
rollback and replay. No raw privilege grant, ownership change, trigger
disablement, monkeypatch, authored anchor/hash or early constraint forcing is
positive evidence.

The one-schema matrix belongs here, not to Lane S: capture
`pg_catalog.current_schema()` once; reject an earlier unmigrated schema before
a later valid schema; reject an earlier decoy ledger/function before a valid
schema; reject every mixed-schema ledger/function/persistent-envelope
combination; and prove the exact captured-schema positive never falls through
to another namespace.

Lane R ends with focused and retained D25/D26/D29/D30/D31 PostgreSQL tests,
compile/format/lint/type/package gates, two independent same-byte audits, one
exact commit, two postcommit identity checks, and push to its branch and
`origin/main`.

Both final Lane-R audits must return `GO`, `P0=0`, `P1=0`.

## 6. Lane C1 -- resumed structural composition

```text
branch = workstream/m5-d30-structural-composition
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d30-structural-composition
base_after_advance = exact pushed Lane-R commit
```

The held C1 worktree remains read-only through S and R. After Lane R
integrates, it may advance only by an exact disjoint-path fast-forward after
the coordinator rechecks its exact head, empty index, raw porcelain hash and
nine file hashes from the D31 freeze handoff and proves no overlap with the
integrated upstream delta. The same no-reset/no-copy custody rule applies.

C1 retains exactly its historical nine paths:

1. `src/groundloop/m5/runtime/persistence.py`;
2. `src/groundloop/m5/runtime/postgres_recovery.py`;
3. `tests/m5/postgres_runtime/d30_application/conftest.py`;
4. `tests/m5/postgres_runtime/d30_application/test_store_composition.py`;
5. `tests/m5/postgres_runtime/d30_application/test_structural_order.py`;
6. `tests/m5/postgres_runtime/d30_application/test_seal_atomicity.py`;
7. `tests/m5/postgres_runtime/d30_application/test_store_races.py`;
8. `tests/m5/postgres_runtime/d30_application/test_structural_open.py`; and
9. `docs/workstreams/m5_runtime_implementation/D30_STRUCTURAL_COMPOSITION_HANDOFF.md`.

C1 composes the already-frozen structural route; it does not invent a new
algorithm or AI behavior. It must finish activation/recovery/replay,
document/group structural open, post-declaration D25 preparation, exact tier-16
status-delta persistence, promotion, the D31-backed preterminal comparison,
seal-owned contribution and public-delta accounting, terminalization,
immutable result creation, unchanged result-bound child comparison/insertion,
deferred validation and one-transaction rollback.

The corrected seal order remains: promote and publish; advance base/heads/current
image while runtime and accumulators remain nonterminal; prepare children
through D31 once; insert the sole seal contribution; terminalize runtime and
both accumulators; insert immutable work/timing/result; rederive through the
unchanged terminal builder; require exact equality; insert its children; force
constraints last; commit. The half-terminal state must never become externally
visible.

C1 must rerun the focused register/replace/retire path, real document
insert/delete/replace ordering, a nonzero-delta open/seal accounting case,
atomic seal cutpoints, replay, races, the complete C1-owned suite and retained
D25/D31 publication/store gates. It ends with final whole-byte audits, an
exact commit, two postcommit identity checks and push to its branch and
`origin/main`. Both final whole-byte audits must be on identical bytes and
return `GO`, `P0=0`, `P1=0`. It passes only this package-private structural
composition lane; it does not by itself complete a public M5 route.

Its owned `d30_application/conftest.py` must install the exact accepted
migration-018 and pinned migration-019 bundles for every positive route that
consumes D31; no unledgered SQL execution or fixture-only privilege bypass is
allowed.

## 7. Protected and held-work custody ledger

The user-owned checkout remains outside every lane:

```text
protected_checkout = /home/kassym/Desktop/groundloop
protected_head = 14598ae51562006eaf67850b19e8212f38997903
protected_tree = 36af2be4c58572c5adde68279d1a3aacedd0beff
protected_index = empty
protected_porcelain = one unstaged pyproject.toml plus untracked
                      docs/presentations/ and
                      docs/workstreams/m5_runtime_contract/
                      PERSISTED_MATCHING_AMENDMENT_DRAFT.md
2af4b19962dc8a7d22e377be17f342530a06ee6395bbbf2a092eab36599c8fc2  pyproject.toml
45c20ca46e9ad5bcd86b22c0d8882d1d611497f57ca8c45d3dea149260c110cd  docs/presentations/groundloop_fyp_professor_feedback.pdf
59a13cd8d4bbb017e712c0f39e70f2eba136557e945f64f1b1fc3891742a79f0  docs/presentations/groundloop_fyp_professor_feedback_v2.pdf
c12929c349a5c0be9793159143b09da40ea2a0b27df37b92d61d9ed6483d8c2a  docs/presentations/render_groundloop_fyp_professor_deck.py
167d1e7df5a720041fe0ff51879d08357f0dfbe3a7781ffaa08c0d958a47aa94  docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT_DRAFT.md
```

The exact held C1-R checkpoint is:

```text
c1_r_head = ee14d697dd2dbf68b33e9a6c0e3d76791afc5520
c1_r_index = empty
c1_r_raw_porcelain_v2_z_sha256 = da234ab487e21d296e6699792ecdbf9608607afdffca83d39b9fd483c548d1bb
2b18ca48d5276bc95961d2cbe84bca25244df49a72dba0e728f9e7bc681ba6e9  src/groundloop/m5/runtime/postgres_matching.py
80e9ab187a04cac2a95f71dc4a378997ebc4e2bcb4b0662e2cfe750276268563  src/groundloop/m5/runtime/postgres_matching_publication.py
ed63c156fd742bb32ff78ea0485d5626e270848cbfc9f9cff20b01d7c7052577  tests/m5/postgres_runtime/d25_publication/test_changed_state_references.py
b2ae3e149ea72aa0ed6f6e3e23553fdad350afb4190dea5a23d499134bed687b  tests/m5/postgres_runtime/d25_publication/test_seal_promotion.py
5bad539816933f2bae3eb9f461aac60d9dc1f3182e4772ba13cc605111ed36fb  tests/m5/postgres_runtime/d25_store_core/test_retirement_work_counter.py
```

The exact held C1 checkpoint is:

```text
c1_head = 81a642e9e60f2e33e2dbe4cb62ef699a380cc26b
c1_index = empty
c1_raw_porcelain_v2_z_sha256 = 736f23f62b8ae744763219ab592266318662636dec4bcbc8a7be93e56ed0d58d
d697ff7b561f638d5148151413553e0595d8689345e04ed578f1f6de8a6f0d78  src/groundloop/m5/runtime/persistence.py
532df5887c20f8d40291b45a5d359d32752803b9784e5e160351aeddd83e8032  src/groundloop/m5/runtime/postgres_recovery.py
bcc3d6ff96cd68d071fb3bb28932edab1279e621488bd78d1eb7743fcd522762  docs/workstreams/m5_runtime_implementation/D30_STRUCTURAL_COMPOSITION_HANDOFF.md
2aaee9c9a483a52392ded87d0b0771b17f037b94df3a95eb6d79c23a05c6581f  tests/m5/postgres_runtime/d30_application/conftest.py
f068e534dd33f6a2751d4dcb17ccec505dd46a58328404587ea1fa329bfaa6ee  tests/m5/postgres_runtime/d30_application/test_seal_atomicity.py
63ed0caf131324e7aa52a887378ba9b929e626a3f05e431039189e70ae0835cb  tests/m5/postgres_runtime/d30_application/test_store_composition.py
0f14fde2fe8c3ebe78b933de9fae040012cbf2cab8200239ead45dbc60803179  tests/m5/postgres_runtime/d30_application/test_store_races.py
6dd8f98998524b3d7dda9acc2b4f5ee7c54eb74581bc85727c6fec965d9125e1  tests/m5/postgres_runtime/d30_application/test_structural_open.py
9c135c6250d1b572a611d2c57f84db1646b8e742a4642601728cda003fc0b30c  tests/m5/postgres_runtime/d30_application/test_structural_order.py
```

Any custody mismatch stops the affected advance. All unowned edits remain the
user's work and may not be staged, committed, reformatted, copied or deleted.
Every raw custody digest is over exact
`git status --porcelain=v2 -uall -z` bytes.

## 8. Explicit exclusions and scope stop

This activation does not authorize Lane C2 requirement composition, Lane M
direct-M4 store composition, Lane C3 direct application composition, Lane D
public facade, Lane I integration/reconciliation, deployment, a runtime-mode
flip, provider/model/prompt changes, performance tuning or evaluation. Those
remain outside the three-lane repair chain.

After C1 is accepted, the coordinator must stop and reassess the smallest
public vertical slice before activating more Task-2 lanes. C1 is necessary for
the currently frozen end-to-end PostgreSQL design, but it is integration
engineering rather than the FYP's novelty and it does not improve neural
accuracy. No later lane is silently implied by finishing C1.

## 9. Audit and integration gates

Before this activation is committed, reviewers must verify:

1. parent `d5253e4`, tree `b3735f...`, and the accepted D31 identities;
2. this file is the only changed path and the protected checkout plus both
   held dirty worktrees retain their exact freeze ledgers;
3. S0 -> P -> S1 -> R -> C1 is strict and first installation is impossible
   while any hash-pin field is `PENDING_S0`;
4. Lane-S, Lane-R and Lane-C1 path manifests are exact and pairwise disjoint;
5. the schema/accessor/installer, current-schema and non-owner boundaries match
   M5-D31 without weakening migrations 017/018;
6. the owner-only held helper is explicitly rejected while its independent
   valid counter work is retainable;
7. runtime mode, public APIs, result/digest/counter semantics and every claim
   ceiling remain unchanged; and
8. two independent whole-byte reviews return `GO`, `P0=0`, `P1=0`, followed
   by one exact commit, two postcommit identity checks and atomic push of the
   branch and `origin/main`.

Every implementation integration repeats its lane-specific ancestry, exact
path, mode/blob/hash, empty-index, test and claim checks. Pre-D31, owner-only,
pre-pin or different-byte evidence cannot be pooled with final acceptance.

## 10. Claim ceiling

This activation changes no current result:

```text
M5-D31 contract = PASS
M5.0-31 contract = PASS
M5-D31 implementation = PENDING
M5.0-31 implementation = PENDING
migration_019_sql_bytes = NOT_AUTHORED
migration_019_hash_pin = PENDING_S0
migration_019_installer = NOT_AUTHORIZED_BEFORE_PIN
preterminal_context_accessor = NOT_IMPLEMENTED
C1-R prerequisite repair = PENDING
C1 structural composition = PENDING
Task 2 = PENDING
M5-D24 through M5-D31 implementation = PENDING
M5.4-05 through M5.4-09 = PENDING
M5.5 and M5.6 = PENDING
runtime_mode = v1_only
deployment/performance/scalability/utility/security/privacy/novelty/
objective-truth/maintained-history/named-system-superiority/AI-quality = PENDING
```

The trusted accessor preserves the accepted schema-owner boundary; it does not
establish system security. C1 establishes no model-quality result. The larger
independently adjudicated natural-history evaluation and AI-quality work remain
separate mandatory work.
