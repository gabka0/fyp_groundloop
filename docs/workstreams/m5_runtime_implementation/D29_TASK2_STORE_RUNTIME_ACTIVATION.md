# M5-D29 Task-2 Store/Runtime Reactivation

Status: docs-only activation candidate; it grants no implementation ownership
until this exact file is committed as the sole change on the exact pushed
parent below, two independent reviewers audit identical commit/tree bytes and
each returns `GO`, `P0=0`, `P1=0`, and that reviewed activation commit is
fast-forwarded and pushed to `origin/main`

Date: 2026-09-22

## 1. Exact activation barrier

```text
required_activation_parent = 7ae653e78b57fb9960020b64604b2b30e197f4ce
required_activation_parent_tree = c0952268dd9cb0b3f4a13b05af92f1a875003f8e

d29_candidate_commit = 4667da0e230d2b52b3147d0ee771396b406ed1e9
d29_candidate_tree = 5f7e69e6f2f503fc3a36691fee16afa40bc8a479
d29_candidate_parent = 28a1392ccba9592ef41b1e51969c094f1f4b922a
d29_amendment_sha256 = e05f159f98d5f282335a90d2e9db1a26f8d85560030314060d918df59ccc82fb
d29_amendment_lines = 1322
d29_amendment_bytes = 81878
d29_freeze_commit = 7ae653e78b57fb9960020b64604b2b30e197f4ce
d29_freeze_tree = c0952268dd9cb0b3f4a13b05af92f1a875003f8e
d29_freeze_handoff_sha256 = 51ed98d60970d9458ae840ed6612baa0f9cf0d4c4dfeee326eb86713adc0d29a

d24_amendment_sha256 = 3fe36a38c3c789143542f8b8cf6614a1df5e7807b9dffa99f0d2cc5118dc1209
d25_amendment_sha256 = bac12ab5e74632c04f1bd70d0ef0d00522ba9d268eb8b73d11845bbf3b873aae
d26_amendment_sha256 = 85372d4c2f9108810bd75c3e5611de541d0f31c8a096421f30e68fad84676721
d27_erratum_sha256 = 7a51afc1f1b6c249572222a023814de8b22220058e00f09564de64f552bd411c
d28_amendment_sha256 = 8a2bafd3478cf2cac6ac7c8de7ca7779a6d9ace7fbbe98a7dc3ff08afdb67eae
d28_activation_commit = 28a1392ccba9592ef41b1e51969c094f1f4b922a
d28_activation_tree = d96723faeee2bd3cce2098df77fb6dbdf8f3f561
d28_activation_sha256 = eab9f23f267e783b318d9a488556bc3ba3e26bb678fa3e5e15ad71a4979e04a0

runtime_addendum_revision = 10
runtime_addendum_sha256 = bbf215fd0d51af41f7e5e3ec7c0558c993dd1150e461283507cb9b0f044d4bff
migration_001_sha256 = ddbfdc1cedec89d9ffd852b66ece4192763099ee34429d5fbb85e54a3a196e37
migration_003_sha256 = fcbe9eaa2ef281cfdc03ddaf25fec3b5dbef73ce6ee957a76b7a3e68b729862c
migration_013_sha256 = ffe1a403039b78006aef6d8da005f77d9d8ca103f52822d81e9242b669a328fb
migration_014_sha256 = 4c37626f24524316c7990b2f3d213573bbe0f805a2c0f884585bf6aa325f0330
migration_015_sha256 = 85cb7f8e6a33273ce67fc6b4160e74a3aff314cd084df7ac3647cadae930185c
migration_016_sha256 = a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7
migration_017_sha256 = e387b01fa80145273ba40d2d83581bc54762d3a4a2edd34669c095076c52154c
runtime_mode = v1_only
```

The required parent is the pushed D29 authority-freeze barrier. The D29
candidate and freeze received independent same-byte precommit and postcommit
`GO` reviews with `P0=0`, `P1=0`, `P2=0`. This activation changes exactly this
file. A commit cannot name its own identity, so two external reviews must pin
its commit, tree, sole parent, file SHA-256, line/byte counts, and clean
worktree before push.

No branch or worktree named in Section 8 may exist until the reviewed
activation commit is present on `origin/main`. The historical D28 Lane-P
worktree is the sole explicit exception and remains protected read-only
non-authority under Section 3.

## 2. Controlling authority and narrow supersession

The controlling reading order is `AGENTS.md`, the accepted D24 through D29
contracts/corrections, runtime-addendum revision 10, the acceptance matrix,
migration-017 activation/schema handoffs, the accepted Lane-A/Lane-B handoffs,
the D28 activation, the D29 freeze handoff, and finally this activation.
Frozen authority always overrides this path plan.

The accepted D28 activation at
`28a1392ccba9592ef41b1e51969c094f1f4b922a`, file SHA-256
`eab9f23f267e783b318d9a488556bc3ba3e26bb678fa3e5e15ad71a4979e04a0`,
remains immutable historical authority for every D28 composition rule not
expressly superseded by M5-D29. This activation replaces only D28's prospective
branch/worktree/path grants, dependency graph, and implementation boundary. It
reopens D28's migration/index prohibition and frozen read-only inventory only
for the exact paths granted below. No prior implementation byte is accepted or
imported.

In particular, the D28 migration/index prohibition is replaced only after this
activation is audited and pushed, and only for Lane S. The D28 read-only rule
for `src/groundloop/m5/runtime/application.py` is replaced only for Lane A
because D29 requires literal pre-opener and post-open catch sites. Every other
D28 exclusion remains exact.

## 3. Held Lane-P preservation ledger

The historical D28 Lane-P worktree remains at:

```text
branch = workstream/m5-d28-matching-planner
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d28-matching-planner
head = 28a1392ccba9592ef41b1e51969c094f1f4b922a
tree = d96723faeee2bd3cce2098df77fb6dbdf8f3f561
staged_paths = 0
modified_tracked_paths = 4
untracked_paths = 3
```

Its exact preservation-only hashes are:

```text
9a9cf676cb3961c09de90457fdd38e701ccbbd18e60783c525b107141639924e  src/groundloop/m5/runtime/postgres_matching.py
fcbbab94e1454bdb549e9f206bd3242d96af7a49cf22ec13eeda031c9d2043e3  tests/m5/postgres_runtime/d25_store_core/conftest.py
73aebe37b02f797fb0b52396e8d9739bb652b580f7844b2ccc0126cb700459dd  tests/m5/postgres_runtime/d25_store_core/test_points.py
27eaa7766e413dad9c3368c50a5e68f840ae5aa8828d4324f33422d6a82dc473  tests/m5/postgres_runtime/d25_store_core/test_transition_apply.py
7c99552c66634c48a468f7641d6eec0f3dccedfce01333ae0eb76659805758e8  tests/m5/postgres_runtime/d25_store_core/test_d27_counter_ownership.py
873c17aaf6923bfc9a24cab362f7f0b69115c1fdccbf0d8e139e4f586fec0133  tests/m5/postgres_runtime/d25_store_core/test_d28_phased_composition.py
08e04191f54e76a81c1c89397f42cf251609b48b1ee566565a9404a6c4a60ab8  tests/m5/postgres_runtime/d25_store_core/test_nonempty_planner.py
```

These bytes are not a checkpoint. They reconstruct legacy payload identity
from normalized rows and therefore violate D29. No lane may commit, merge,
rebase, cherry-pick, copy, patch-forward, mechanically reproduce, or cite them
as implementation evidence. New work starts from the pushed D29 barrier and is
implemented independently from authority and current accepted source.

Any status or hash drift in this protected worktree stops activation or later
integration until the coordinator records and audits the cause. No process may
clean, stash, reset, reformat, or delete it.

## 4. Authorized outcome

This activation authorizes only the supported Task-2 PostgreSQL store/runtime
composition already frozen by D24--D29:

1. exact migration-018 installation and route authority;
2. locked-epoch persisted legacy source identity and independent exact sidecar
   validation;
3. store-derived, bounded same-policy document withdrawal with independent
   admitted-pair and current-observation projections;
4. one exact typed-update manifest declaration commitment and complete
   existing-event M4/M5 declaration hydration;
5. D28 cursor-local prepare/stage/account/finalize composition and retained
   replay for group structural, supported document structural, active
   requirement-completion, direct expansion, and direct verifier-completion
   transitions;
6. exact ordinary pre-opener and checked post-open terminal-cut routing;
7. production-capable PostgreSQL group and typed-direct facades, seal,
   reconnect, terminal replay, and post-seal independent audits; and
8. complete executable evidence without changing runtime mode or claims.

The store, never a caller, derives source closure, affected keys, declarations,
before/after images, patch, certificates, references, artifacts, work, counts,
status deltas, contributions, headers, and accumulators. All caller plans and
declarations are compare-only. Provider/model/retrieval calls remain outside
PostgreSQL transactions and lock scope.

### 4.1 Supported and unsupported forms

This tranche supports:

- group `REGISTER`, `REPLACE`, and `RETIRE` structural events;
- same-policy typed document `INSERT`, `DELETE`, and `REPLACE`;
- active requirement-verifier completion;
- typed direct expansion and verifier completion;
- activation-bootstrap current observations under D29's exact provenance
  rule; and
- exact replay/late audit of D24-valid unresolved dispatch or attempt history
  attached only to terminal predecessor authority.

The following remain fail-closed and `PENDING`:

- cross-policy document withdrawal, with no policy rebase;
- typed rootless `ObserveRequirementEvent`;
- policy change; and
- standalone claim `ObserveEvent`.

No lane may map an unsupported form to another update kind, infer missing
provenance, recancel terminal historical work, or count its falsifier as
passed.

## 5. Frozen D29 runtime order

The 1,322-line accepted D29 amendment is authoritative; this section allocates
work and does not restate or weaken it.

### 5.1 Preview, open, and continuation

For a D29 typed document event, the application order is exact:

1. initial durable-result lookup;
2. migration-018-guarded requirement preview;
3. migration-018-guarded direct preview when applicable;
4. one typed open whose absent-event branch independently recomputes every
   source, withdrawal, declaration, scope, root, and manifest value;
5. for a nonterminal `replayed=true` receipt, one persisted requirement
   hydration before the first current-revision read;
6. current-revision point read;
7. direct runner entry barrier, supplied-revision validation, retained direct
   declaration hydration/terminal check, then acquisition/external work; and
8. the remaining D24/D28 runtime, seal, replay, and audit protocol.

Both preview calls retain no lock, context, token, or authority. The opener
ignores a stale absent-event preview when the event appeared after its point
check. The existing manifest for a D29 document has exactly one top-level
`m5_d29_document_declaration_v1` key and exactly the three frozen sorted-unique
text arrays. Production has no update/delete route for that commitment.

### 5.2 Terminal cuts

Only on a typed legacy document `INSERT`, `DELETE`, or `REPLACE`, the package-
private `_M5D29HydrationTerminalCutoff` has exact type, empty `args`, no
payload, and no public export. A pre-opener catch is valid only around either
fully validated D29 document planner call and returns the ordinary canonical
terminal-known-at-entry envelope without opening. After a nonterminal D29
document receipt, the sole checked origin is the literal requirement-hydration
or direct-hydration catch site with that held receipt, canonical-zero
accumulated work, and a newly validated same-epoch canonical result. It then
uses the existing active-terminal projection and unchanged D24 timing/
telemetry rules. Group routes retain the exact D28 flow and never perform D29
hydration or cutoff projection.

A signal from any other site, before complete declaration validation, with a
subclass/nonempty `args`, after nonzero work, or without an exact canonical
result conflicts. The signal is never authority. No generic result read gains
an active-cutoff origin.

### 5.3 Migration-018 barrier

Every future runtime activation that exposes D29 typed document routes must
validate the exact accepted migration-018 ledger row before exposure. Within a
D29 typed document route, only an initial exact durable terminal-result replay
may proceed without migration 018. Every D29 legacy-document preview, every
D29 document first typed open (absent or exact-existing), every D29 document
nonterminal existing-event hydration, and every D29 document direct runner/
resume must independently validate the exact accepted 018 ledger row at D29's
frozen position. The opener repeats the check before event-ID/epoch
consumption; a planner check is not opener authority. Missing or one-field-
different authority conflicts; no route falls back to a sequential scan.
Group routes retain their frozen D28 authority and do not depend on migration
018.

## 6. Global exclusions

No lane may:

- edit migrations 001, 003, or 013--017, their ledgers, accepted functions,
  triggers, constraints, privileges, indexes, or tests;
- change a public signature/protocol, frozen DTO, enum, semantic/runtime
  digest, source/reference kind, result shape, counter, measured value, or
  runtime mode;
- introduce a tombstone, nullable hash, JSON/`repr` hash, seventh reference
  kind, requirement-state counter alias, policy rebase, or third/wider 018
  index;
- accept caller-authored authoritative keys, declaration, plan, patch, output,
  certificate, reference, artifact, work, count, status delta, or header;
- add `M5IncrementalOverlay`, a full oracle/repository hydration, retained
  engine/cache, aggregate relation scan, hidden transaction registry/state, or
  model/provider/retrieval call inside a transaction;
- add a nested transaction, internal commit/rollback, in-transaction retry,
  second timing anchor, or work outside frozen counters;
- import any historical implementation byte or edit a historical worktree;
- expand dashboard, HTTP API, CLI, provider, model, deployment, or unrelated
  evaluation scope;
- transition a real database from `v1_only` to `m5_active`; or
- claim Task 2/M5.4 completion, deployment, latency, performance, utility,
  objective truth, model/AI quality, security, novelty, maintained-history, or
  named-system superiority.

## 7. Frozen read-only paths

Every lane may read but must not edit any unlisted path. In particular these
remain read-only throughout:

```text
AGENTS.md
docs/m5_design_freeze.md
docs/m5_acceptance_matrix.md
docs/m5_implementation_plan.md
docs/m5_implementation_status.md
docs/m5_multiagent_execution_plan.md
docs/decision_log.md
docs/roadmap.md
docs/workstreams/m5_runtime_contract/CANDIDATE_RUNTIME_ADDENDUM.md
docs/workstreams/m5_runtime_contract/RECOVERY_WORK_AMENDMENT.md
docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT.md
docs/workstreams/m5_runtime_contract/CHANGED_STATE_ABSENCE_AMENDMENT.md
docs/workstreams/m5_runtime_contract/REQUIREMENT_STATE_COUNTER_ERRATUM.md
docs/workstreams/m5_runtime_contract/PHASED_PERSISTED_MATCHING_COMPOSITION_AMENDMENT.md
docs/workstreams/m5_runtime_contract/BOUNDED_DOCUMENT_WITHDRAWAL_AMENDMENT.md
docs/workstreams/m5_runtime_implementation/D28_TASK2_STORE_RUNTIME_ACTIVATION.md
docs/workstreams/m5_runtime_implementation/D29_CONTRACT_FREEZE_HANDOFF.md
migrations/001_m2_base.sql
migrations/003_m4_dynamic_impact.sql
migrations/013_m4_evaluation_overlay.sql
migrations/014_m5_evidence_groups.sql
migrations/015_m5_runtime.sql
migrations/016_m5_runtime_recovery.sql
migrations/017_m5_persisted_matching.sql
src/groundloop/m5/runtime/contracts.py
src/groundloop/m5/runtime/digests.py
src/groundloop/m5/incremental_overlay.py
src/groundloop/m5/runtime/postgres_matching_publication.py
src/groundloop/m5/runtime/postgres_matching_audit.py
src/groundloop/postgres/m5.py
tests/m5/postgres/test_static_contract.py
tests/m5/postgres_runtime/test_migration_015.py
tests/m5/postgres_runtime/test_migration_016.py
tests/m5/postgres_runtime/test_migration_017.py
```

Existing D25 publication/audit source and tests remain read-only. The exact
lane grants below are the only exceptions to the unlisted-path rule.

## 8. Sequential dependency barriers and path-exclusive lanes

Every lane begins only after the immediately preceding reviewed integration is
pushed. A lane may edit fewer granted paths but no unlisted path. Discovering a
necessary path is a hard stop for a one-file docs-only activation amendment,
two same-byte audits, integration, and push.

```text
audited and pushed D29 activation
  -> Lane S  migration 018
  -> Lane A  application terminal routing
  -> Lane P  matching/withdrawal planner, phases, and replay
  -> Lane C1 activation, structural open, recovery, and seal
  -> Lane C2 active requirement completion
  -> Lane M  store-derived private M4 stage
  -> Lane C3 typed-direct persistence composition
  -> Lane D  public/reconnect/post-seal composition
  -> combined serial regression and two final same-byte audits
```

No implementation lane develops ahead of its barrier. This keeps private
interfaces reviewable on the exact pushed predecessor and prevents a
historical branch from supplying hidden authority.

### 8.1 Lane S -- migration 018

```text
branch = workstream/m5-d29-schema-018
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d29-schema-018
base = exact pushed D29 activation commit
```

Exact owned paths:

1. `migrations/018_m5_bounded_document_withdrawal.sql` (new);
2. `src/groundloop/postgres/migrations.py`;
3. `tests/m5/postgres_runtime/test_migration_018.py` (new); and
4. `docs/workstreams/m5_runtime_implementation/D29_SCHEMA_018_HANDOFF.md`
   (new).

Lane S implements exactly the two frozen ordinary transactional indexes in
their frozen order and the dedicated identity/installer/route-ledger reader.
The installer owns one top-level read-write `READ COMMITTED` transaction,
performs the ledger-first decision, validates all five pinned accepted-017
literals, validates UTF-8/C collation, takes the one exact two-table `SHARE ROW
EXCLUSIVE NOWAIT` statement, rereads the ledger, executes the two statements
separately in order, and inserts the ledger last. Exact rerun takes no target
lock; any retry begins in a new transaction.

The final migration and bundle SHA-256 literals may be computed only from the
reviewed final SQL bytes. They must be pinned in code and recorded in the Lane-
S handoff before acceptance. No later lane may edit migration 018,
`migrations.py`, its tests, or those literals.

### 8.2 Lane A -- application terminal routing

```text
branch = workstream/m5-d29-application-routing
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d29-application-routing
base = exact pushed Lane-S integration
```

Exact owned paths:

1. `src/groundloop/m5/runtime/application.py`;
2. `tests/m5/runtime/test_d29_hydration_terminal_cutoff.py` (new); and
3. `docs/workstreams/m5_runtime_implementation/D29_APPLICATION_ROUTING_HANDOFF.md`
   (new).

Only for typed legacy document `INSERT`, `DELETE`, and `REPLACE`, Lane A
defines the exact package-private `_M5D29HydrationTerminalCutoff`, with empty
`args`, no payload, and no package export or public protocol/signature/DTO
change. Under that exact document guard it installs the two literal pre-opener
planner catches, the replayed-nonterminal requirement hydration before the
first post-open revision read, and the literal post-open catches around
requirement hydration and direct execution. A pre-opener catch rereads and
validates only the ordinary canonical terminal result. A post-open catch
requires the held exact nonterminal receipt, canonical-zero accumulated work,
and a newly reread same-epoch canonical result before using the existing
active-terminal projection. Every wrong type, subclass, nonempty argument,
wrong call site, malformed result, terminal-looking held receipt, or nonzero-
work origin conflicts.

The lane uses only test doubles to prove routing. It adds no PostgreSQL,
planner, provider, retrieval, verifier, transition, or seal implementation.
Its tests prove group `REGISTER`, `REPLACE`, and `RETIRE` retain the exact D28
planner-call, replay, revision-read, timing, and cutoff flow, with no D29
rehydration or signal route.
The signal definition becomes read-only after Lane A; Lane P and Lane D may
import and raise that exact type but may not redefine, wrap, translate, or
export it.

### 8.3 Lane P -- matching, bounded withdrawal, phases, and replay

```text
branch = workstream/m5-d29-matching-planner
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d29-matching-planner
base = exact pushed Lane-A integration
```

Exact owned paths:

1. `src/groundloop/m5/runtime/postgres_matching.py`;
2. `src/groundloop/m5/runtime/postgres_withdrawal.py` (new);
3. `tests/m5/postgres_runtime/d25_store_core/conftest.py`;
4. `tests/m5/postgres_runtime/d25_store_core/test_points.py`;
5. `tests/m5/postgres_runtime/d25_store_core/test_transition_apply.py`;
6. `tests/m5/postgres_runtime/d25_store_core/test_replay_work.py`;
7. `tests/m5/postgres_runtime/d25_store_core/test_nonempty_planner.py` (new);
8. `tests/m5/postgres_runtime/d25_store_core/test_d27_counter_ownership.py`
   (new);
9. `tests/m5/postgres_runtime/d25_store_core/test_d28_phased_composition.py`
   (new);
10. `tests/m5/postgres_runtime/d29_store/conftest.py` (new);
11. `tests/m5/postgres_runtime/d29_store/test_source_identity.py` (new);
12. `tests/m5/postgres_runtime/d29_store/test_bounded_withdrawal.py` (new);
13. `tests/m5/postgres_runtime/d29_store/test_retained_declarations.py` (new);
14. `tests/m5/postgres_runtime/d29_store/test_replay_and_reservation.py`
    (new);
15. `tests/m5/postgres_runtime/d29_store/test_query_plans.py` (new); and
16. `docs/workstreams/m5_runtime_implementation/D29_MATCHING_PLANNER_HANDOFF.md`
    (new).

Lane P independently implements D28's nonempty intent/prepare/stage/finalize
and retained replay from accepted source, never from Lane-P WIP. The new
private PostgreSQL withdrawal module owns the migration-018 ledger barrier,
locked epoch payload identity, bounded admitted/current projections, lineage/
policy/activity/dedup validation, prospective coordinate gathering, exact one-
key manifest value helpers, all-state direct/M5 declaration readers, and final
event-local terminal check. It returns only frozen plan/declaration types or
the exact private cutoff signal; it retains no cursor/lock/cache/token across
calls.

Prepare derives complete patch/plan/work/count/header authority before writes.
Stage writes only prelocked lower-tier D25 rows. Finalize writes only 15i--15k.
Direct reservation/completion retains explicit lexical D28 stage evidence.
Replay validates retained changed-key projection, historical contribution, and
current accumulator with zero semantic/event/history DML or regeneration.

### 8.4 Lane C1 -- activation, structural open, recovery, and seal

```text
branch = workstream/m5-d29-structural-composition
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d29-structural-composition
base = exact pushed Lane-P integration
```

Exact owned paths:

1. `src/groundloop/m5/runtime/persistence.py`;
2. `src/groundloop/m5/runtime/postgres_recovery.py`;
3. `tests/m5/postgres_runtime/d29_application/conftest.py` (new);
4. `tests/m5/postgres_runtime/d29_application/test_store_composition.py`
   (new);
5. `tests/m5/postgres_runtime/d29_application/test_structural_order.py` (new);
6. `tests/m5/postgres_runtime/d29_application/test_seal_atomicity.py` (new);
7. `tests/m5/postgres_runtime/d29_application/test_store_races.py` (new);
8. `tests/m5/postgres_runtime/d29_application/test_d29_structural_open.py`
   (new); and
9. `docs/workstreams/m5_runtime_implementation/D29_STRUCTURAL_COMPOSITION_HANDOFF.md`
   (new).

Lane C1 installs the accepted D25 activation projection and requires the exact
accepted migration-018 ledger before any future activation exposes D29
document routes. It inserts the exact D29 document manifest value and composes
group/document structural open through Lane P. Every D29 document first typed
open independently repeats the 018 check before event-ID/epoch consumption,
whether its locked classification becomes absent or exact-existing; group open
retains its D28 route without that dependency. The absent branch recomputes
every compare-only input under frozen locks before any tier-8/9/D25 DML. The
exact-existing branch ignores stale previews, validates retained source/
declarations, performs no first-application work, and exposes only frozen
replay authority. Structural open remains at revision 1; later source-present
transitions use D28 phase order. Seal performs no D25 transition or second
timing anchor and uses the accepted publication helper without editing it.

### 8.5 Lane C2 -- active requirement completion

```text
branch = workstream/m5-d29-requirement-composition
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d29-requirement-composition
base = exact pushed Lane-C1 integration
```

Exact owned paths:

1. `src/groundloop/m5/runtime/postgres_verifier.py`;
2. `tests/m5/postgres_runtime/d29_application/test_requirement_completion.py`
   (new);
3. `tests/m5/postgres_runtime/d29_application/test_requirement_order.py`
   (new);
4. `tests/m5/postgres_runtime/d29_application/test_requirement_races.py`
   (new); and
5. `docs/workstreams/m5_runtime_implementation/D29_REQUIREMENT_COMPOSITION_HANDOFF.md`
   (new).

Lane C2 replaces only the active-completion persisted-matching hard stop. It
keeps job/attempt preterminal until exact source closure is persisted/locked,
derives once, prepares before header updates, stages observation/currency then
remaining D25 lower tiers, terminalizes the held job/attempt, accounts D24,
finalizes D25, writes exact status deltas, and forces validation. Inactive,
expired, cancelled, failed, loser, and audit-only paths remain D25-inert.

### 8.6 Lane M -- store-derived private M4 stage

```text
branch = workstream/m5-d29-m4-store-derived-direct
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d29-m4-store-derived-direct
base = exact pushed Lane-C2 integration
```

Exact owned paths:

1. `src/groundloop/m4/pipeline.py`;
2. `tests/m4/integration/test_m5_typed_store_derived_direct.py` (new);
3. `tests/m4/integration/test_m5_d29_direct_stage_evidence.py` (new); and
4. `docs/workstreams/m5_runtime_implementation/D29_M4_DIRECT_STORE_DERIVED_HANDOFF.md`
   (new).

Lane M adds only the package-private typed store-derived construction/stage
route. It performs no retained repository/engine hydration or full scan,
changes no public M4-v1 entrypoint/receipt/digest/projection, and returns exact
lexical evidence for lower-tier/job, 15a, 15b, and all three 15c families. It
cannot mutate, cache, or hide the Lane-P reservation.

### 8.7 Lane C3 -- typed-direct persistence composition

```text
branch = workstream/m5-d29-direct-composition
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d29-direct-composition
base = exact pushed Lane-M integration
```

Exact owned paths:

1. `src/groundloop/m5/runtime/postgres_direct_recovery.py`;
2. `src/groundloop/m5/runtime/direct_m4.py`;
3. `tests/m5/postgres_runtime/d29_application/test_direct_transition.py`
   (new);
4. `tests/m5/postgres_runtime/d29_application/test_direct_order.py` (new);
5. `tests/m5/postgres_runtime/d29_application/test_direct_races.py` (new);
6. `tests/m5/postgres_runtime/d29_application/test_direct_store_reconnect.py`
   (new); and
7. `docs/workstreams/m5_runtime_implementation/D29_DIRECT_COMPOSITION_HANDOFF.md`
   (new).

Lane C3 composes the exact reservation, lexical M4 stage evidence, source
completion, header advance, D25 stage, D24 accounting, D25 finalization,
status-delta, and validation order. It owns no public preview or runner. Late,
inactive, replay, serialized-loser, and combined-failure paths remain D25-
inert; C5/C6/C7 work/timing/cutoff authority remains exact.

### 8.8 Lane D -- public facades, reconnect, and post-seal audit

```text
branch = workstream/m5-d29-public-composition
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d29-public-composition
base = exact pushed Lane-C3 integration
```

Exact owned paths:

1. `src/groundloop/m5/runtime/postgres_application.py`;
2. `src/groundloop/m5/runtime/postgres_direct_application.py`;
3. `src/groundloop/m5/runtime/postgres_matching_postseal.py` (new);
4. `tests/m5/postgres_runtime/d29_application/test_group_public_composition.py`
   (new);
5. `tests/m5/postgres_runtime/d29_application/test_direct_public_composition.py`
   (new);
6. `tests/m5/postgres_runtime/d29_application/test_reconnect.py` (new);
7. `tests/m5/postgres_runtime/d29_application/test_postseal_audit.py` (new);
8. `tests/m5/postgres_runtime/d29_application/test_d29_continuation.py`
    (new);
9. `tests/m5/postgres_runtime/d29_application/test_d29_terminal_cutoff.py`
    (new);
10. `tests/m5/postgres_runtime/d29_application/test_d29_route_barrier.py`
    (new); and
11. `docs/workstreams/m5_runtime_implementation/D29_PUBLIC_COMPOSITION_HANDOFF.md`
    (new).

Lane D makes the public PostgreSQL composition raise Lane A's exact private
cutoff only from D29's final validated declaration checks. The direct runner
performs barrier, supplied-revision validation, retained declaration
hydration/terminal check, acquisition, then external work; it never calls
`plan_direct_open` for continuation. Both historical pre-seal facades remain
fail-closed regression surfaces.

The production-capable facades preserve fresh-process reconnect, exact
terminal zero-call replay, one timing anchor, canonical work, and unchanged
DTOs. Post-seal Python, SQL, and physical/provenance audits execute after
commit and outside measured latency, using accepted read-only helpers and a
final publication-head recheck.

## 9. Per-lane acceptance and integration

Every lane handoff must record exact base/commit/tree, name-status subset,
blob SHA-256 values, clean state, commands, tool versions, selected/passed/
failed/skipped/xfail/deselected counts, duration, PostgreSQL inventories,
falsifier mapping, race orders, crash cuts, reconnect behavior, findings,
fixes, nonclaims, and remaining acceptance rows.

Before each lane integration, two independent reviewers audit identical
committed bytes:

1. semantic/authority/API/DTO/digest/counter/oracle/M4-v1 compatibility; and
2. PostgreSQL schema/lock/order/race/replay/environment correctness.

Both must return `GO`, `P0=0`, `P1=0`. Any byte change restarts both audits.
The coordinator integrates the exact reviewed commit without cherry-picking or
rebasing, reruns applicable gates on integrated ancestry, verifies protected
local-main and held Lane-P hashes, and pushes the accepted barrier before
creating the next lane worktree. No lane self-accepts.

Lane S must record accepted literal migration-018 and bundle hashes before it
can pass. Later lanes validate those literals but may not change them. A green
individual lane is scoped evidence only.

## 10. Mandatory evidence

The combined tranche must map exact tests to all applicable:

- all 24 D29 falsifier groups, including retained source/sidecars, independent
  projections, same-policy/activity/lineage/dedup, activation-bootstrap and
  typed-rootless boundaries, D24 terminal ambiguity, exact event forms,
  two-index catalog/plan shapes, declaration completeness, ledger/install,
  crash/race/replay/lock order, bounded work, two-call authority, every
  terminal cut, reservations, timing ownership, and route barrier;
- all 15 D28 phase/replay groups, 8 D27 counter groups, 18 D26 absence groups,
  40 D25 persisted-matching groups, and every applicable D24-C1--C7 case;
- exact migration-018 SQL/identity/catalog/install/rerun/conflict/NOWAIT/
  rollback/concurrency/width and populated `EXPLAIN` evidence, plus independent
  route checks at activation exposure, both document planners, every document
  first open (absent and exact-existing), post-open requirement hydration, and
  direct runner/resume;
- byte-identical migrations 001, 003, and 013--017 plus their applicable
  fresh/rerun/conflict/rollback/ledger regressions;
- crash cuts after every authorization, locator, reservation, lower-tier plan/
  write, both header updates, job/attempt closure, 15a, 15b, every 15c family,
  15d--15h, 15i/15j/15k, status delta/later row, and forced validation;
- both serial orders for planner/open/hydration terminalization, same-edge/
  different-edge completion, replay, seal, failure, and reconnect;
- static/runtime rejection of payload reconstruction, caller authority,
  registries, full hydration/oracle/cache, aggregate/sequential scans, and in-
  transaction external calls;
- end-to-end group and same-policy typed document insert/delete/replace
  histories through seal, fresh-process reconnect, later-head replay, and
  exact terminal replay;
- independent Python, SQL, and D25 physical/provenance audit after each
  measured successful seal; and
- never-activated/public M4-v1 byte, receipt, projection, route, and migration
  compatibility.

No skipped, xfailed, unavailable, environment-failed, timeout-without-
diagnosis, no-match, or silently deselected mandatory case is a pass.

## 11. Environment qualification

Use repository source and isolated caches:

```text
MYPYPATH=src
PYTHONPATH=src:.
PYTHONDONTWRITEBYTECODE=1
```

PostgreSQL suites run serially through an isolated runner sharing the database
container network. Current qualified PostgreSQL 16.14 evidence requires:

```text
PGOPTIONS=-c jit=off
```

Record exact pre/post database, non-temporary-schema, client, test-role, and
container inventories for every tranche. The default-JIT SIGSEGV remains
unresolved and is not a product pass. Host-published-port cascades and
overlapping schema fixtures are invalid evidence and cannot be pooled.

## 12. Acceptance ceiling

Task 2 is accepted only after the complete supported vertical slice and final
combined regression matrix receive two independent same-byte `GO`, `P0=0`,
`P1=0` reviews. Lane evidence cannot be pooled as final acceptance without
that exact integrated rerun and audit.

Even after technical Task-2 acceptance, runtime remains `v1_only` outside
isolated fixtures. M5-D24 through M5-D29 implementation rows and M5.4-05,
M5.4-06, and M5.4-09 may change only in a separate audited status-
reconciliation tranche. M5.4-07, M5.4-08, M5.5, M5.6, deployment, latency,
performance, utility, objective truth, security, novelty, maintained-history,
named-system superiority, and model/AI quality remain `PENDING`.

## 13. Activation audit and protected local main

Before any D29 implementation lane exists:

1. this file is the only changed path and its commit has sole parent
   `7ae653e78b57fb9960020b64604b2b30e197f4ce`;
2. exact parent/tree, authority hashes, migration hashes, branch/worktree
   collision inventory, diff check, status/nonclaim boundaries, protected
   local-main hashes, and held Lane-P hashes pass;
3. one authority/ownership reviewer and one D24--D29/PostgreSQL reviewer audit
   identical commit/tree bytes;
4. both return `GO`, `P0=0`, `P1=0` and confirm no Section-8 D29 branch or
   worktree existed during review;
5. any byte change restarts both reviews; and
6. `origin/main` is fast-forwarded to the exact reviewed activation commit
   before Lane S is created; no unaudited merge commit may intervene.

The dirty local-main checkout is user-owned and excluded:

```text
local_main_head = 14598ae51562006eaf67850b19e8212f38997903
local_main_tree = 36af2be4c58572c5adde68279d1a3aacedd0beff
2af4b19962dc8a7d22e377be17f342530a06ee6395bbbf2a092eab36599c8fc2  pyproject.toml
45c20ca46e9ad5bcd86b22c0d8882d1d611497f57ca8c45d3dea149260c110cd  docs/presentations/groundloop_fyp_professor_feedback.pdf
59a13cd8d4bbb017e712c0f39e70f2eba136557e945f64f1b1fc3891742a79f0  docs/presentations/groundloop_fyp_professor_feedback_v2.pdf
c12929c349a5c0be9793159143b09da40ea2a0b27df37b92d61d9ed6483d8c2a  docs/presentations/render_groundloop_fyp_professor_deck.py
167d1e7df5a720041fe0ff51879d08357f0dfbe3a7781ffaa08c0d958a47aa94  docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT_DRAFT.md
```

Its exact porcelain is one unstaged modified `pyproject.toml` plus the four
displayed untracked files and no staged change. Never reset, clean, stash,
reformat, stage, commit, copy, or delete those paths. Any protected hash or
porcelain drift stops activation or integration.

All other pre-existing implementation branches/worktrees are historical read-
only evidence. No lane may merge, rebase, cherry-pick, copy source from, or
revive them.
