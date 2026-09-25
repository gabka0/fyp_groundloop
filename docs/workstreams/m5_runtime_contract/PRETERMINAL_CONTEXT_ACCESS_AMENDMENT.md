# M5-D31 Preterminal Context Access Amendment

Status: docs-only contract candidate; no authority to edit a migration,
installer, source, test, schema, runtime, status, acceptance, or deployment path
exists until these exact bytes receive two independent same-byte `GO`,
`P0=0`, `P1=0` reviews and the separately reviewed authority-freeze update is
committed and pushed

Date: 2026-09-25

## 1. Scope and precedence

This amendment resolves one permission contradiction exposed while composing
the accepted M5-D25/D26 persisted-matching seal with the M5-D24 accounting
envelope under M5-D30 C1. It is subordinate to every unchanged M5 decision and
supersedes only the following M5-D30 statements:

1. migrations 001--018 are the complete frozen migration inventory;
2. migration 019 is a falsifier; and
3. no new SQL function or schema object may be introduced.

The supersession is limited to the one additive migration and one read-only
SQL function defined below. Migrations 001--018 remain byte-identical. M5-D30's
claim-current locator, direct-M4/M3 provenance closure, locks, queries, outputs,
digests, DTOs, counters, and public application behavior remain unchanged.

M5-D31 changes no evidence-group, matching, state, certificate, delta,
changed-state reference, absence-artifact, event-result, work, timing, replay,
or failure semantic. It adds no table, column, index, constraint, trigger, type,
sequence, view, materialized view, backfill, provider, model, prompt, public
Python API, or runtime mode. The new SQL function is an internal database
attestation bridge for the existing package-private preparation helper.

## 2. Confirmed contradiction

Migration 017 deliberately creates the promotion context, journal, and
expected-set temporary tables inside
`groundloop_m5_matching_begin_promotion_context(...)`, a trusted
`SECURITY DEFINER` function. Their owner is therefore the trusted function
owner, not a distinct non-owner runtime session. This is necessary: the
accepted D26 security boundary rejects caller-created same-name temporary
relations even when a caller supplies their real OIDs through every custom
setting.

The accepted C1 prerequisite repair requires the package-private preterminal
publication helper to validate, before deriving any child:

1. the genuine migration-017 promotion triplet and transaction-local OID/GUC
   bindings;
2. the unique context row for the current backend, transaction and
   `session_user`;
3. its exact seal mode, epoch, expected/resulting revisions and policy;
4. all captured predecessor/head/current-image anchors; and
5. `validation_started=false` and `validation_done=false`.

An owner-session implementation can directly read the temporary context. A
legitimate non-owner runtime cannot: caller-side `relowner = current_user`
rejects the genuine trusted-owner tables, and a direct `SELECT` lacks table
privilege. Two independent audits of the otherwise passing C1 repair candidate
reported this same `P1`. Owner-only tests cannot establish the supported
non-owner public-authorizer route.

No conforming source-only workaround exists. The existing public migration-017
functions authorize, create, journal, or deferred-validate the context, but do
not expose the captured row. The existing raw readers and private-triplet
checker are intentionally revoked. A no-op write/savepoint probe would make a
compare-only helper perform guarded DML; early `SET CONSTRAINTS` would start and
complete validation before the required seal contribution; trusting only
caller-visible GUCs would omit the captured anchors and validation flags; and
granting raw temporary-table access would expose mutable implementation
authority. Each alternative violates an accepted contract.

Therefore the smallest complete correction is one trusted, read-only,
argument-bound SQL accessor installed additively after migration 018.

## 3. Exact SQL surface

Migration 019 may create exactly this function signature:

```sql
groundloop_m5_matching_read_preterminal_seal_context(
    selected_epoch_id bigint,
    selected_expected_revision bigint,
    selected_sealed_revision bigint
)
RETURNS TABLE (
    policy_version text,
    anchor_m4_epoch_id bigint,
    anchor_m5_epoch_id bigint,
    anchor_m5_revision bigint,
    anchor_activation_count integer,
    anchor_predecessor_revision bigint,
    anchor_predecessor_sealed_at timestamptz,
    anchor_current_policy text
)
LANGUAGE plpgsql
STABLE
CALLED ON NULL INPUT
SECURITY DEFINER
SET search_path FROM CURRENT
```

The function has exactly three input arguments and eight output columns in the
order and PostgreSQL types above. It has no default argument, variadic form,
overload, OUT-only alias, companion procedure, or alternate name. It returns
exactly one row on success and raises on every invalid or ambiguous input.
`CALLED ON NULL INPUT` is exact: `proisstrict=false`, and the function body
must raise rather than silently return zero rows when any argument is `NULL`.

The installer creates it while the local search path is exactly the
installation schema followed by `pg_catalog`, so `SET search_path FROM
CURRENT` pins that trusted path. The function owner must be the same role that
owns the accepted migration-017 private-triplet checker and public seal
authorizer. The function is not leakproof, is not parallel-safe, and is not a
trigger. No row-security bypass or superuser assumption is authorized.

Raw access remains private. The migration must not grant a runtime role direct
access to any promotion temporary table or EXECUTE on
`groundloop_m5_matching_private_temp_triplet(...)`, the begin-context helper,
journal helper, promotion guard, or deferred validator. The new function has
explicit `PUBLIC EXECUTE`; schema `USAGE` remains an independent deployment
grant. This is safe only because success is confined to the caller's genuine
current transaction context and the function returns bounded, non-secret
coordinates rather than journal or expected-set contents.

## 4. Fail-closed accessor contract

Before returning a row, the trusted function must prove all of the following
inside its own `SECURITY DEFINER` execution:

1. each supplied value has the exact PostgreSQL type from the signature;
   `selected_epoch_id > 0`, `selected_expected_revision > 0`,
   `selected_sealed_revision > 0`, and
   `selected_sealed_revision = selected_expected_revision + 1` without an
   unchecked-overflow path;
2. `groundloop.m5_checked_transition` is exactly `on`;
3. `groundloop.m5_matching_mode` is exactly `seal`;
4. the epoch, expected-revision and resulting-revision GUCs exist and equal the
   three arguments exactly, and the policy GUC exists and is nonempty;
5. none of the migration-017 transition-context, change-journal, or
   expected-change temporary relations exists;
6. all three promotion-context, promotion-journal and promotion-expected
   temporary relations exist, resolve in `pg_temp`, have three distinct OIDs,
   and are ordinary temporary tables in `pg_my_temp_schema()`;
7. the existing revoked
   `groundloop_m5_matching_private_temp_triplet(...)` accepts those exact OIDs,
   thereby binding them to the trusted definer owner rather than the caller;
8. each of the three OIDs, rendered canonically as text, equals its respective
   transaction-local context/journal/expected OID GUC;
9. the promotion context contains exactly one total row;
10. that row has `backend_pid = pg_backend_pid()`,
    `transaction_id = pg_current_xact_id()`, and
    `session_role = session_user`;
11. its mode, epoch, expected revision, resulting revision and policy equal the
    exact GUC/argument coordinates above;
12. `validation_started` and `validation_done` are both exactly false; and
13. every one of the eight returned values comes from that same unique row,
    with no default, reconstruction, coercive fallback, second query image, or
    caller-supplied replacement.

SQL `NULL`, a missing setting, malformed numeric setting, missing relation,
duplicate/replaced relation, wrong owner/kind/persistence/namespace, duplicate
context row, wrong backend/xid/role, wrong coordinate/policy, or either true
validation flag must raise and return no row. Ordinary `=`/`<>` comparisons
must not permit a SQL-`NULL` result to bypass rejection; checks must be
NULL-safe or preceded by exact non-null proof.

The function performs no `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `TRUNCATE`,
DDL, `LOCK`, advisory lock, constraint-mode change, transaction control, GUC
mutation, journal read, expected-set read, validation start, or validation
completion. It neither calls the promotion deferred validator nor consumes a
pending trigger. Repeated calls in the same unchanged context return the same
single row and leave all persistent and temporary rows, flags, GUCs and
transaction coordinates unchanged.

## 5. Migration-019 boundary

The only new migration is:

```text
migrations/019_m5_preterminal_seal_context.sql
```

It creates the function from Section 3 and pins the explicit EXECUTE privilege
from that section. It creates or changes nothing else. In particular it may
not replace migrations 015--018 or any existing function, add a helper type,
persist a context snapshot, expose journal/expected rows, or grant raw
temporary-table privilege.

Its prerequisite is the exact accepted migration-018 five-field row:

```text
bundle_id = "m5-bounded-document-withdrawal-schema-bundle-v1"
bundle_sha256 = "9c45e58fb5c61156d4d07aa0c9b767112bf285452664f39731d893445e7a9e4f"
migration_sha256 = "941bba975c12e9fb5ba4b4f75a82e59fa518b23eac34468ed2f8b15d1cd9ed90"
oracle_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
prerequisite_sha256 = "52240e19968926d0c051fe6146b3c7d877cf582014341efbfcc78637f3ff5761"
```

No individual field, inferred row, current-checkout substitute, or merely
matching migration-file hash satisfies that prerequisite. Migration 019's
five-field ledger identity uses the existing canonical byte-total bundle
recipe. Let `migration_019_sha256 = SHA256(exact_migration_019_bytes)` and
`accepted_migration_018_bundle_sha256` equal the exact `9c45...e4f` literal
above:

```text
stable_m5_digest(
  "m5-preterminal-seal-context-schema-bundle-v1",
  *TEXT("migrations/019_m5_preterminal_seal_context.sql"),
  *HASH(migration_019_sha256),
  *HASH(accepted_migration_018_bundle_sha256)
)
```

The ledger row is:

```text
bundle_id = "m5-preterminal-seal-context-schema-bundle-v1"
bundle_sha256 = the digest above
migration_sha256 = SHA256(exact_migration_019_bytes)
oracle_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
prerequisite_sha256 = "9c45e58fb5c61156d4d07aa0c9b767112bf285452664f39731d893445e7a9e4f"
```

The implementation activation must pin the exact migration SHA-256 and bundle
SHA-256 before first installation. Neither value may be inferred from
unreviewed checkout bytes. Migration 018 remains exact at accepted SHA-256
`941bba975c12e9fb5ba4b4f75a82e59fa518b23eac34468ed2f8b15d1cd9ed90`.

### 5.1 Installer

The migration-019 installer is ledger-first and owns one top-level read-write
`READ COMMITTED` transaction. It must:

1. before the initial ledger decision, perform only connection-state checks
   for an idle, read-write, supported `READ COMMITTED` installation route and
   compute the caller-byte identity needed for ledger comparison; it must not
   reject UTF-8, accepted-hash, prerequisite, catalog, or DDL content yet;
2. inside the owned transaction, read the exact five-field migration-019
   ledger row before any target-schema lock, content validation or catalog
   mutation; an exact caller-byte replay returns without taking the install
   lock, while a conflicting caller-byte identity fails as a ledger conflict;
3. only when the ledger is absent, reject non-UTF-8 bytes, a wrong accepted
   migration hash, a wrong accepted bundle hash, or malformed/nonexact SQL;
4. require the exact accepted migration-018 ledger row above and, transitively, leave
   the exact accepted migration-017 row unchanged;
5. acquire one canonical `SHARE ROW EXCLUSIVE ... NOWAIT` lock on
   `groundloop_m5_schema_bundle`, then reread the migration-019 ledger;
6. reject a pre-existing same-signature function when the exact migration-019
   ledger is absent, including a same-name function owned by the schema owner;
7. set the local installation search path to the exact selected schema and
   `pg_catalog`, then execute only the accepted migration-019 statements;
8. verify from `pg_catalog` the exact namespace, name, identity arguments, result row
   shape, language, volatility, security-definer flag, non-leakproof and
   parallel-unsafe flags, `CALLED ON NULL INPUT`/`proisstrict=false`, owner
   equality, pinned search path and ACL;
9. prove the accepted private-triplet checker and public seal authorizer still
   have their pre-019 owner, signatures, definitions, security flags and ACLs;
10. write the exact five-field migration-019 ledger row last; and
11. roll back the function, privilege and ledger atomically at every injected
    failure boundary.

The installer performs no data backfill. Concurrent installers serialize on
the one ledger-table lock; the winner installs once and a loser either observes
exact replay or fails without a partial object. Schema-owner trust remains the
same explicit boundary as migrations 017 and 018; M5-D31 makes no claim against
a malicious schema owner.

## 6. Runtime consumption

The package-private Python helper retains the accepted signature:

```python
def _prepare_preterminal_matching_publication_children(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    expected_revision: int,
    sealed_revision: int,
) -> M5MatchingPublicationChildren: ...
```

It must capture `pg_catalog.current_schema()` exactly once and fail if it is
`NULL`. In that exact namespace it must prove that
`groundloop_m5_schema_bundle` is the expected permanent ordinary ledger table
and require the exact five-field migration-019 row. It must invoke the
accessor exactly once through safely quoted schema qualification in that same
namespace. Every persistent relation read by the helper must remain bound to
that captured schema through safe qualification; no earlier or later
`search_path` schema may supply a ledger, function, or subset of the envelope.
The helper may not directly query any promotion temporary table or duplicate a
weaker trusted-owner predicate. The one returned policy/anchor row replaces
only the caller-inaccessible context-row read. The helper must still
independently validate every accepted C1 half-terminal base epoch, runtime,
update, predecessor, M4/M5 head, matching current/working image, work/timing
accumulator, deactivation and absent-result coordinate. It still derives
through the exact shared D25/D26 child body.

The accessor is not publication authority. Its output contains no state hash,
certificate hash, status delta, changed-state reference, set hash, work value,
timing value, event result, or child bytes. The unchanged result-bound
`build_matching_publication_children(...)` remains the sole source eligible
for child insertion after runtime/accounting terminalization and immutable
result creation, and its DTO/bytes must equal the preterminal preparation.

The corrected atomic seal order remains:

1. authorize, promote and publish the bounded prepared state;
2. advance base epoch, heads and matching current image while runtime and both
   accumulators remain at the prior nonterminal revision;
3. call the trusted accessor through the package-private helper and derive the
   preterminal child DTO;
4. insert the exact seal contribution while runtime is nonterminal;
5. terminalize runtime and both accumulators;
6. insert immutable event work, timing and result;
7. call the unchanged result-bound builder, require exact equality, insert its
   children, force all deferred constraints and commit.

The new SQL function does not alter this order and cannot make the
half-terminal image externally visible.

## 7. Mandatory falsifiers

The implementation evidence must include all of the following on identical
final bytes:

1. exact bundle/hash/ledger vectors plus install, exact replay, conflicting
   replay, partial-object, wrong-prerequisite, injected rollback and two-order
   concurrent-install tests;
2. catalog proof of the exact sole function, signature, return row, owner,
   `SECURITY DEFINER`, pinned search path, volatility, parallel/leak flags and
   explicit privilege; no unexpected migration-019 object;
3. retained byte/hash proof that migrations 001--018 and their ledgers are
   unchanged;
4. a genuine owner-session positive and a genuine distinct non-owner runtime
   positive returning identical eight fields for the same logical fixture;
5. in the non-owner session, direct `SELECT` from the promotion context remains
   `InsufficientPrivilege`, private helper EXECUTE remains denied, and the
   trusted accessor plus package-private preparation succeeds;
6. caller-created same-name triplets with absent, invented, or exact actual OID
   GUCs fail, including a caller-owned structurally identical triplet;
7. each of the three nullable input positions is tested as SQL `NULL` and must
   raise; no `STRICT`/zero-row shortcut is accepted;
8. absent/wrong checked-transition mode, promotion mode, epoch, expected/
   sealed revision, adjacency, policy, table, OID, backend, xid, role,
   cardinality, owner, relation kind/persistence/namespace, and transition-
   triplet coexistence each fail closed;
9. both `validation_started=true` and `validation_done=true` fail, while two
   unchanged prevalidation reads are deterministic;
10. before/after snapshots prove the accessor changes no GUC, temporary row,
   journal, expected set, validation flag, persistent row, lock inventory or
   deferred-constraint state;
11. one-schema binding rejects an earlier unmigrated schema followed by a later
    valid schema, an earlier decoy ledger/function followed by a valid schema,
    and every mixed-schema ledger/function/envelope combination; the exact
    captured schema positive must not fall through to another namespace;
12. real REPLACE and RETIRE seals prove preterminal derivation, contribution
    before terminalization, rejection after terminalization, exact terminal
    builder equality, D26 absence references, deferred validation and commit;
13. present-state, certificate-only, absence, malformed child, replay and
    rollback regressions remain exact; and
14. compile, formatting, lint, strict relevant type checks, package-content
    checks, two independent whole-byte audits, exact commit identity and two
    postcommit identity checks pass for every tranche.

An owner-only positive is insufficient. A test that grants raw temp-table
access, changes table ownership, disables a trigger, monkeypatches a guard,
authors returned anchors, manually authors child hashes, starts deferred
validation early, or rewrites a derived row is not success evidence.

## 8. Activation and ownership barrier

Acceptance of this document implements nothing. After two independent
same-byte contract audits, a separate docs-only authority-freeze tranche must
update the decision log, M5 design freeze, acceptance matrix, execution plan,
implementation status, roadmap and agent instructions. That tranche must
record M5-D31 and M5.0-31 as contract-`PASS` /
implementation-`PENDING`, advance the runtime addendum by one revision, and
make exactly two narrow replacements in M5.0-30: its negative `add migration
019` falsifier becomes `migration 019 is missing, differs from exact M5-D31,
or is accompanied by another migration`; and its positive `frozen migrations
001--018 and no migration 019` evidence becomes `frozen migrations 001--018
plus exact M5-D31 migration 019`. M5.0-30 otherwise remains exact.

Only after that freeze is independently reviewed, committed and pushed may a
docs-only path-exclusive implementation activation allocate sequential lanes:

1. a migration-019 schema/installer/test/handoff lane;
2. a revised C1 prerequisite runtime/test/handoff lane consuming the installed
   accessor; and
3. the resumed C1 structural-composition lane.

The activation must preserve the current uncommitted C1-R candidate as
read-only evidence until the schema lane integrates. It may retain the valid
distinct-mask-group counter change, but it must not accept or commit the
owner-only preterminal helper. Dirty C1 and C1-R worktrees may advance only by
exact disjoint-path fast-forward after lineage and overlap checks. No reset,
checkout, clean, stash, rebase, cherry-pick, manual copy, or patch transplant
may bypass those checks.

## 9. Claim ceiling

This candidate changes no current result:

```text
M5-D31 contract = PENDING_AUDIT
M5.0-31 contract / implementation = PENDING / PENDING
preterminal_context_accessor = NOT_IMPLEMENTED
migration_019 = NOT_AUTHORIZED
C1-R prerequisite repair = PENDING
C1 structural composition = PENDING
M5-D24 through M5-D30 implementation = PENDING
Task 2 = PENDING
M5.4-05 through M5.4-09 = PENDING
M5.5 and M5.6 = PENDING
runtime_mode = v1_only
```

No deployment, performance, scalability, utility, security, privacy, novelty,
objective-truth, maintained-history, named-system-superiority, or AI/model-
quality claim follows. The accessor preserves an existing privilege boundary;
it does not establish system security. The larger independently adjudicated
natural-history evaluation and AI-quality work remain separate mandatory work.
