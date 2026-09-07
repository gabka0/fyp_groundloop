# M5-D26 Migration-017 Recovery Wave Activation

Status: docs-only activation contingent on the gates in Section 11 for two
fresh, disjoint implementation lanes; no implementation branch may start until
this exact plan is independently audited, integrated to main, and pushed

Date: 2026-09-07

## 1. Exact authority and frozen starting point

```text
required_activation_parent = b9bbf9b9124bf4297f3d7394069088906352dd25
required_activation_parent_tree = 2a02065a08e1f0aacf920520bd40274bcbe4a419
accepted_d25_candidate_commit = 002dcace2f89e71ef3a56955647b4d8077e9c91f
accepted_d25_candidate_sha256 = bac12ab5e74632c04f1bd70d0ef0d00522ba9d268eb8b73d11845bbf3b873aae
accepted_d26_candidate_commit = ad04a372cd106f1702cddffcbafad56e826c8bc4
accepted_d26_candidate_tree = 2dba444399920262f386631f5b2cb578701b06c9
accepted_d26_candidate_sha256 = 85372d4c2f9108810bd75c3e5611de541d0f31c8a096421f30e68fad84676721
accepted_d26_candidate_lines = 422
accepted_d26_candidate_bytes = 19924
runtime_addendum_revision = 7
runtime_mode = v1_only
```

Local main and `origin/main` both named the required parent when this activation
was opened. That commit is the independently audited and pushed M5-D26
authority freeze. M5-D25/M5.0-25 and M5-D26/M5.0-26 are contract-`PASS` /
implementation-`PENDING`. M5.4-01 through M5.4-04 remain `PASS`; M5.4-05
through M5.4-09 and every M5.5/M5.6 gate remain `PENDING`.

This file must be the only path changed by its activation commit, whose sole
parent must be the exact commit above. Because a commit cannot contain its own
identity without circularity, the two independent plan reviews record the
containing commit and tree. Only after that exact commit is fast-forwarded to
main and pushed may the coordinator fork either implementation lane. Both
lanes must start from that integrated activation commit, not merely from the
parent recorded above.

The implementation authority order is:

1. `AGENTS.md`;
2. `docs/m5_design_freeze.md`, including M5-D22 and M5-D26;
3. `docs/workstreams/m5_runtime_contract/CANDIDATE_RUNTIME_ADDENDUM.md`
   revision 7;
4. `docs/workstreams/m5_runtime_contract/RECOVERY_WORK_AMENDMENT.md` and its
   accepted C1--C7 corrections;
5. `docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT.md`;
6. `docs/workstreams/m5_runtime_contract/CHANGED_STATE_ABSENCE_AMENDMENT.md`;
7. `docs/m5_acceptance_matrix.md`;
8. `docs/workstreams/m5_runtime_implementation/D25_MIGRATION_017_ACTIVATION.md`
   as the historical first-wave boundary;
9. `docs/workstreams/m5_runtime_implementation/D26_CONTRACT_FREEZE_HANDOFF.md`;
   and
10. this activation.

The D26 amendment is authoritative wherever D22/D25 and migration 015 cannot
represent the intentional absence of a retired predecessor object. It changes
only the narrow absence identity and its validation branch. No lane may
reinterpret D25 physical matching, D24 accounting, present-state identities,
the six-kind enum, or migration-014/015/016 behavior outside the exact accepted
D25/D26 replacement authority.

## 2. Accepted migration-016 prerequisite

Both lanes must fail closed unless current repository bytes reproduce the
accepted migration-016 prerequisite exactly:

```text
accepted_016_bundle_id = "m5-runtime-recovery-schema-bundle-v1"
accepted_016_migration_sha256 = a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7
accepted_016_bundle_sha256 = 28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565
accepted_016_oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
accepted_016_prerequisite_sha256 = b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd
```

A mismatch stops the wave. It does not authorize changing a D25 literal or a
previous migration ledger row.

## 3. Read-only held evidence and derivative rule

The former D25 branches remain immutable evidence. They are not active lanes,
and no commit may be added to, amended on, rebased in, or force-pushed from
either branch.

### 3.1 Held Lane A

```text
branch = workstream/m5-d25-contracts-digests
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d25-contracts-digests
commit = 6591d4779cd59bec4bcb9aa15c1b44901e0049f7
tree = 35709442636bbbe33830867616e642d34dade185
source_base = 691e3d174e059ac041d3a46678fcb630e15478d5
source_commits = 079d3898741978e146eaec06aedc9ade881217d8,
                 40317ee5d8c77f1a939822112f268a9fa393841f,
                 6a95c22ae95a8d54f6e502a1fcc9245b68f40685,
                 6591d4779cd59bec4bcb9aa15c1b44901e0049f7
status = scoped candidate; independent final audit still required
```

Its exact five-path checkpoint ledger is:

```text
src/groundloop/m5/runtime/contracts.py = f441789a18f9381ffc9951e17788d7df7a728dc63de18cd4cc540fe2c8395d8c
src/groundloop/m5/runtime/digests.py = 34f55999b77a9e96c831b4f664321bc8f558232d3bdf61237bf4ac4e038571f9
tests/m5/runtime/test_contracts.py = d0d244f55bc870f29d31afe9e29c6746b91712a8735e542633ee3ade6c599542
tests/m5/runtime/test_digests.py = b35785893462fcabeba10eee5486c24bd0ed03cfda64741ec3173f877bd799ea
docs/workstreams/m5_runtime_implementation/D25_CONTRACTS_DIGESTS_HANDOFF.md = 3b77deaaa968b39f1115d17ee0eacd8bdc7aa39725487d885f8b105c2578e66f
```

### 3.2 Held Lane B

```text
branch = workstream/m5-d25-schema-017
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d25-schema-017
commit = fc6129d3ea374c3d280e5b1744d26aa135865fd5
tree = 055e2a80b878f3779265a85a1efaa6bde709cce1
source_base = 691e3d174e059ac041d3a46678fcb630e15478d5
source_commits = 0193cf2193a5696c7c51e753d2bdcae603f942f8,
                 5a189b47980fba91b8aca6a6425c2069c48fb9a0,
                 fc6129d3ea374c3d280e5b1744d26aa135865fd5
status = HOLD; not an accepted migration-017 candidate
```

Its exact four-path checkpoint ledger is:

```text
migrations/017_m5_persisted_matching.sql = 8ca8812d90522b67f0b1b59123e7c41648dfb22f70d80d8e9ffbefed40e00f2a
src/groundloop/postgres/migrations.py = 29d6ee2802228b26afb69199b596f1780b32209ba501f12cc2e697a7bcecf723
tests/m5/postgres_runtime/test_migration_017.py = 19b75fd04e2e0ac795b41d6f6f757f60cd8d2cd9b0c5411d05a869235fa7a200
docs/workstreams/m5_runtime_implementation/D25_SCHEMA_017_HANDOFF.md = 0cbe30a5106f676e7eac0a2cb192498ffc64e2fcf1c84d3d5c23b10bffacca20
```

The held SQL happens to derive bundle SHA-256
`9ecd340745504ab2896df1ea5cb5b8f749c32ff24be06975ce0dbf58cdbad818`.
That value and the held migration SHA above are import-checkpoint values only.
Both must change after a D26 SQL edit and must never be presented as the final
accepted migration-017 identity.

The recorded `151/151` PostgreSQL result and bounded `18/18` structural result
belong only to those held bytes. They are useful investigation evidence, not a
pass that may be pooled with a later candidate.

### 3.3 Exact derivative procedure

For each fresh lane, the coordinator must:

1. verify that the integrated activation commit has sole parent `b9bbf9b` and
   changes only this file;
2. create the named fresh branch and worktree from that integrated activation
   commit;
3. replay the lane's exact source commits above, in order, into the fresh
   branch without editing the held branch;
4. stop on any replay conflict rather than resolving it implicitly;
5. before new edits, prove the imported path set and every imported blob equal
   the held ledger above; and
6. record both the activation commit/tree and imported checkpoint in the new
   D26 handoff.

Replayed commits will have new commit identities because they have a new
parent. Equality is therefore established by exact path and blob hashes, not
by pretending the new commits retain their old IDs. After the import check,
new work may occur only in the editable subset assigned below.

## 4. Fresh Wave R6 ownership manifest

### Lane A — D25 contracts plus D26 pure absence digest

```text
branch = workstream/m5-d26-contracts-digests
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d26-contracts-digests
```

The exact allowed branch delta relative to the integrated activation commit is
limited to these six paths:

1. `src/groundloop/m5/runtime/contracts.py`;
2. `src/groundloop/m5/runtime/digests.py`;
3. `tests/m5/runtime/test_contracts.py`;
4. `tests/m5/runtime/test_digests.py`;
5. `docs/workstreams/m5_runtime_implementation/D25_CONTRACTS_DIGESTS_HANDOFF.md`;
   and
6. `docs/workstreams/m5_runtime_implementation/D26_CONTRACTS_DIGESTS_HANDOFF.md`
   (new).

Paths 1, 3, and 5 must remain byte-identical to the held ledger. After exact
import, Lane A may edit only paths 2, 4, and 6. D26 freezes digest bytes, not a
new Python public-helper name, and adds no DTO or contract shape. Any required
change to `contracts.py`, `test_contracts.py`, the old handoff, or a seventh
path is a coordinator stop and manifest-amendment proposal, not implied
ownership.

Lane A adds the smallest typed pure helper for:

```text
stable_m5_digest(
  "m5-changed-state-absence-artifact-v1",
  *ENUM(kind),
  *TEXT(object_id)
)
```

It must accept exactly the existing `requirement_state`, `group_state`, and
`group_certificate` values, reject the other three existing kinds, keep the
enum at six values, keep `state_artifact_hash` non-null, and leave the outer
`m5-changed-state-reference-v2`, set recipe/order/uniqueness, all present-state
recipes, and direct certificate-digest behavior byte-identical. It must use
the existing typed digest primitives and exact nonempty object ID; it must not
use JSON, `repr`, reflection, delimiter joining, normalization, trimming,
tombstones, a nullable hash, or a seventh kind.

Lane A tests must include Python golden vectors for all three legal kinds; the
same non-ASCII object ID under each kind; empty/whitespace-invalid IDs under
the existing identifier rule; prefix and framing boundaries; kind, object,
domain, and one-byte mutations; rejection of all three excluded kinds even
with a correctly computed domain hash; unchanged construction of the outer
reference and set; and regression vectors for all six present-reference kinds,
existing D25 contracts, and legacy M4-v1 bytes.

The final audit must cover the complete carried D25 pure checkpoint together
with the D26 digest delta; replay does not inherit an audit or a `PASS`. Lane A
owns no PostgreSQL, SQL, migration, store, emission, seal, replay, runtime-mode,
provider, or deployment behavior. Its result is pure helper and contract
evidence only; it cannot mark D25 or D26 implementation `PASS`.

### Lane B — completed migration 017 and D26 database validator

```text
branch = workstream/m5-d26-schema-017
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d26-schema-017
```

The exact allowed branch delta relative to the integrated activation commit is
limited to these five paths:

1. `migrations/017_m5_persisted_matching.sql`;
2. `src/groundloop/postgres/migrations.py`;
3. `tests/m5/postgres_runtime/test_migration_017.py`;
4. `docs/workstreams/m5_runtime_implementation/D25_SCHEMA_017_HANDOFF.md`; and
5. `docs/workstreams/m5_runtime_implementation/D26_SCHEMA_017_HANDOFF.md`
   (new).

Path 4 must remain byte-identical to the held ledger. After exact import, Lane
B may edit only paths 1--3 and 5. Any needed sixth path or change to the old
HOLD handoff is a coordinator stop.

Lane B must retain and complete the D25 migration-017 candidate, close every
retained B3 blocker, and add D26's exact database branch. The four retained
B3b P1s are mandatory:

1. activation must reject any existing committed preactivation M4/v1 epoch in
   `pending` or `complete` evaluation state and bind the complete structurally
   committed, semantically sealed, evaluation-complete, strictly published
   head tuple plus non-null exact `sealed_at` and exact revision;
2. activation must require the exact final migration-017 bundle and migration
   hashes, never arbitrary hexadecimal strings;
3. the authorizer must explicitly lock and prove M5 publication-head state and
   activation-row absence with a lock sufficient for an absent singleton key;
   an empty `SELECT ... FOR UPDATE` observation is not sufficient;
4. seal must bind the complete predecessor and live-epoch status/revision
   tuples, equal M4/M5 heads, current-image coordinates, and require the
   working-image policy to equal the typed update policy.

Lane B must also complete symmetric expected-set equality for the image header
and all four current physical families; reject every missing, extra,
wrong-operation, wrong-coordinate, or wrong tombstone/upsert row; execute
deferred promotion validation; prove that no DML can occur after validation
starts; and complete final seal, promotion, heads, result/reference validation,
rollback, and exact replay in the one transaction. It must restart the full
D25 B1--B3 static/live/rollback/concurrency matrix on final bytes; no earlier
partial count may be pooled with that run.

All accepted D25 installer invariants remain mandatory: the exact 41-relation
`ACCESS EXCLUSIVE MODE NOWAIT` order and singleton order; one top-level
read-write `READ COMMITTED` transaction; rejection of an ambient transaction;
post-lock ledger reread; the exact migration-016 prerequisite; fresh,
preactivation-upgrade, activated-without-history, and exact-rerun branches;
partial-prefix rollback followed by retry only in a fresh transaction; raw-DML
guards; deferred-to-immediate validation; and ledger-last commit. D26 does not
weaken, reorder, or bypass any of them.

D26 grants migration 017 one and only one additional replacement among
migration-015 objects:

```text
CREATE OR REPLACE FUNCTION groundloop_m5_validate_event_result_children()
```

Only that body may change, and only to validate the accepted qualifying
absence branch. The three migration-015 constraint-trigger identities must
remain installed, enabled, targeted at the same function, and otherwise
byte-identical. The function signature, return type, language, volatility,
security/search-path properties, ownership, and privileges must retain their
migration-015 behavior. Migration-015 file bytes and ledger identity must not
change.
Migration 017 already contains two separately accepted D25 replacements of
migration-014 functions:

```text
groundloop_m5_validate_working_state_mutation()
groundloop_m5_validate_revision_interval_mutation()
```

The final static pre-017 replacement inventory must therefore be exactly those
two D25-authorized functions plus the one D26-authorized migration-015
function. D26 must add no fourth replacement and must not relabel either D25
replacement as D26 authority.

Migration 015 must remain byte-identical at SHA-256
`85cb7f8e6a33273ce67fc6b4160e74a3aff314cd084df7ac3647cadae930185c`.
The PostgreSQL absence digest must use the existing canonical helper with the
exact typed field stream:

```text
groundloop_m5_digest_text_fields(ARRAY[
  'm5-changed-state-absence-artifact-v1',
  'enum', kind,
  'text', object_id
])
```

The D26 SQL branch must independently prove all Section 3 conditions of the
accepted amendment: exact sealed event/update/deactivation identity; only
`replace_group`/`REPLACE` with the exact non-null successor or
`retire_group`/`RETIRE` with NULL; exactly one deactivation; independently
derived structural payload and, for replacement, successor record payload;
the canonical D25 `before`-present/`after=None` logical-change bijection;
present predecessor state or applicable certificate binding at the prior
published head; exact closure at the event epoch; no same-object successor;
and agreement among event, durable epoch, runtime epoch, both heads, reference
epoch, and exact sealed revision.

It must derive the typed absence hash inside PostgreSQL, compare the unchanged
outer/set/logical-result identities, preserve every present-reference path,
and reject absence for activation, failed/nonsealed or nonstructural events,
excluded kinds, incomplete predecessors without a binding, wrong/missing/
duplicate logical changes, closure and successor mutations, and every one-field
identity/payload/head mutation required by D26 falsifiers 1--17. A database
reconnect/replay fixture must prove stable persisted references and zero writes;
the later application/store lane remains responsible for proving zero model
call and no reference regeneration end to end under falsifier 18.

The replacement must be installed inside the single top-level migration-017
transaction before the ledger insert and roll back atomically with every other
017 object. Fresh install, upgrade, exact rerun, content conflict, every
failure-injection boundary, and partial-prefix retry must prove one complete
before-or-after image. Exact rerun is ledger-first and performs no replacement
or DDL. After final SQL bytes exist, Lane B must recompute and pin the exact 017
migration/bundle ledger identity; tests must reject any arbitrary or stale
hash. The final bundle hash is derived exactly as:

```text
stable_m5_digest(
  "m5-persisted-matching-schema-bundle-v1",
  *TEXT("migrations/017_m5_persisted_matching.sql"),
  *HASH(final_migration_017_sha256),
  *HASH(28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565)
)
```

Its ledger must also bind the exact empty oracle SHA-256 and accepted
migration-016 prerequisite from Section 2.

Lane B owns no application/store adapter, runtime-mode activation, provider,
model call, production database, or deployment. It cannot mark D25 or D26
implementation `PASS`.

## 5. Shared barriers and non-ownership

The lanes may run in parallel because their six-path and five-path deltas are
disjoint. They may read each other's committed candidate for comparison, but
must not merge, edit, or cherry-pick the other lane. Neither lane may edit:

- this activation or any frozen authority/decision/status document;
- either accepted D25/D26 amendment or review/freeze handoff;
- the old D25 handoff that its own manifest marks immutable;
- `src/groundloop/m5/runtime/persistence.py` or another store/composition path;
- migration 014, 015, or 016 files or their existing ledger rows;
- provider/discovery/verifier/measurement/evaluation code;
- user `pyproject.toml`, presentations, or the protected D25 draft;
- main, the other lane, deployment state, production data, or runtime mode.

This wave does not own application emission of changed-state absence
references, runtime reconnect without regeneration/model calls, persisted
matching store composition, D24 recovery integration, maintained-history
evaluation, or M5.4/M5.5/M5.6 status promotion. If correctness requires one of
those surfaces or any unlisted path, the lane must stop and write a proposal in
its new D26 handoff.

## 6. Lane A execution prompt

```text
Implement GroundLoop M5-D26 recovery Wave R6 Lane A from the exact integrated
D26_MIGRATION_017_ACTIVATION.md commit. Read the full authority order. Verify
the frozen parent/candidate/prerequisite hashes and the held Lane A commit,
tree, path, and blob ledger. In the fresh named branch/worktree, replay the
four held source commits in order and prove exact imported blobs before making
new edits. Never modify the held branch. Keep contracts.py, test_contracts.py,
and the old D25 handoff byte-identical; edit only digests.py, test_digests.py,
and the new D26 handoff. Implement the smallest typed absence-artifact digest
for exactly the three authorized existing kinds without inventing a frozen
public-helper name. Preserve the six-kind enum, non-null field,
outer/set/present recipes, D25 bytes, and M4-v1 bytes. Add all positive,
excluded-kind, non-ASCII, framing, mutation, outer/set, present-path, and
legacy regressions.
Use no JSON, repr, reflection, normalization, tombstone, nullable hash, SQL,
database, or network. Run focused and complete applicable pure tests, Ruff,
strict mypy, compileall, diff-check, exact-path/imported-handoff/protected-hash
checks. Commit only the manifested paths and record exact commit/tree/file
hashes, commands, counts, audit history, limits, and pending work. Obtain one
independent same-byte final audit covering both the carried D25 checkpoint and
the D26 delta with P0=0/P1=0. Do not claim D25/D26 PASS.
```

## 7. Lane B execution prompt

```text
Implement GroundLoop M5-D26 recovery Wave R6 Lane B from the exact integrated
D26_MIGRATION_017_ACTIVATION.md commit. Read the full authority order. Verify
the frozen parent/candidate/prerequisite hashes and the held Lane B commit,
tree, path, and blob ledger. In the fresh named branch/worktree, replay the
three held source commits in order and prove exact imported blobs before new
edits. Never modify the held branch. Keep D25_SCHEMA_017_HANDOFF.md immutable;
edit only migration 017, migrations.py, its test module, and the new D26
handoff. Close all four retained B3b P1s, expected-set equality, deferred
promotion validation, and final atomic seal. Add only D26's authorized
groundloop_m5_validate_event_result_children() migration-015 replacement;
retain exactly the two D25 migration-014 replacements and all three
migration-015 trigger identities. Implement the complete structural,
logical-change, predecessor-closure, successor-absence, payload, seal-head,
typed-digest, present-reference, rollback, and rerun falsifiers. Recompute and
pin exact final 017 ledger hashes; reject arbitrary/stale identities. Run a
fresh complete static/live/rollback/concurrency and migration-014/015/016
regression under the disposable local PostgreSQL protocol with bounded
timeouts and exact pre/post inventory equality. Do not pool held counts, edit
another migration, implement a store, activate runtime mode, deploy, or use a
model/provider. Commit only manifested paths, record exact bytes/counts/
failures/inventory/limits, and obtain one independent same-byte final audit
with P0=0/P1=0. Do not claim D25/D26 or M5.4 PASS.
```

## 8. Lane acceptance gates

Each lane candidate requires all of the following:

1. a fresh branch/worktree from the exact integrated activation commit;
2. exact held-commit replay, imported path/blob equality, and no change to the
   held evidence branch;
3. a full diff from the activation commit equal to its six- or five-path
   manifest, a post-replay diff contained by its narrower editable subset, all
   immutable imported blobs unchanged, and a clean worktree;
4. `git diff --check`, Ruff, strict mypy for owned source, compileall, and all
   focused plus applicable regression tests;
5. unchanged frozen D25/D26 candidates, authority, migration-016 prerequisite,
   migration-014/015/016 files and ledgers, and protected hashes;
6. retained failure chronology, with no skipped, xfailed, silently deselected,
   or pooled partial result unless explicitly preregistered and reported;
7. an exact final commit/tree/file SHA ledger; and
8. one independent same-byte audit returning `GO` with no unresolved P0/P1.

Any final-byte edit invalidates that lane's audit and restarts its complete
applicable gate. Lane B additionally requires a serialized live-database
preguard, explicit bounded timeouts, and exact pre/post schema/role/client
inventory equality for every PostgreSQL run. Missing local PostgreSQL is an
environment blocker, never permission to substitute static evidence. The
inventory must explicitly report other active clients plus every disposable
`d25_migration_017_*` schema and `d25_runtime_*` role before and after.

For each lane the reviewer must report both comparisons explicitly:

```text
git diff --name-only <integrated-activation-commit>..HEAD
    = exact full six-path or five-path manifest
git diff --name-only <post-replay-checkpoint>..HEAD
    = subset of the exact editable manifest
```

The expected combined technical delta relative to the activation commit is
exactly eleven disjoint paths. The complete history relative to `b9bbf9b`
contains exactly twelve paths: this activation plus those eleven technical and
handoff paths.

## 9. Integration order and combined gates

After both lane audits return `GO`, the coordinator creates:

```text
branch = integration/m5-d26-migration-017-wave1
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d26-migration-017-wave1-integration
base = exact integrated activation commit containing this file
```

The coordinator must verify the base and protected hashes, import the final
Lane A candidate, then the final Lane B candidate, and prove that the combined
delta is exactly the disjoint union of six Lane A paths and five Lane B paths.
It must not resolve an unanticipated conflict or edit implementation bytes
during import; any required edit returns ownership to the affected lane and
invalidates its audit.

On the exact combined bytes, restart:

1. exact-path, imported-handoff, authority/candidate/prerequisite/legacy-hash,
   static replacement-inventory, trigger-identity, and `git diff --check`
   gates;
2. Ruff, strict mypy, compileall, all runtime contract/digest tests, and the
   public-M4/legacy regression selection;
3. Python/PostgreSQL cross-oracle golden hashes for every legal D26 kind,
   non-ASCII/framing adversaries, excluded kinds, and one-field mutations;
4. the complete migration-017 static and live module from test one, not just
   failed or newly added nodes;
5. applicable migration-014/015/016 fresh/upgrade/rerun/conflict/rollback/
   trigger/concurrency regressions;
6. the complete D25 final-seal/activation and D26 structural-absence matrix;
7. atomic before-or-after image and exact ledger-first no-op rerun checks; and
8. exact pre/post disposable PostgreSQL inventory equality.

The combined commit then requires two independent audits on identical bytes:
one D25/D26 semantic/digest/status/nonclaim audit and one PostgreSQL/schema/
transaction/rollback audit. Each must return `GO` with `P0=0` and `P1=0` and
report the exact commit, tree, path hashes, commands, and test counts. Any P0,
P1, byte edit, inventory leak, timeout without diagnosis, or unexplained test
deselection returns the integration to `HOLD` and restarts both audits after a
lane-owned fix.

Main may fast-forward only from the unchanged integrated activation commit,
only after both same-byte integration audits pass, and only after the
coordinator rechecks the protected ledger before and after. The exact main
commit may then be pushed to `origin/main`.

## 10. Integration stop and remaining implementation

Even successful Wave R6 integration proves only the pure D25/D26 contract
surface and the migration-017 schema/installer/database-enforcement tranche.
It does not complete application/store generation and seal composition,
end-to-end reconnect/replay without regeneration or model calls, D24 recovery
composition, or the rest of the accepted D25/D26 falsifier matrix.

Therefore integration must retain:

- M5-D25/M5.0-25 contract `PASS` / implementation `PENDING`;
- M5-D26/M5.0-26 contract `PASS` / implementation `PENDING`;
- M5.4-01..04 `PASS`, M5.4-05..09 `PENDING`;
- every M5.5 and M5.6 gate `PENDING`;
- runtime mode `v1_only` outside isolated fixtures; and
- deployment, production activation, AI/model-quality, maintained-history,
  utility, latency, security, novelty, and named-system superiority claims as
  unsupported.

A later coordinator-owned, path-exclusive wave must activate the store/runtime
composition paths and remaining end-to-end D25/D26 falsifiers. This activation
does not pre-authorize that wave.

## 11. Activation-plan acceptance

Before this plan may grant implementation ownership:

1. its commit must have sole parent `b9bbf9b9124bf4297f3d7394069088906352dd25`
   and change exactly this one documentation path;
2. all held branches must remain clean, at their pinned commits, and read-only;
3. `git diff --check`, exact-path, branch-name/worktree-name collision,
   candidate/prerequisite/held-ledger, status/nonclaim, and protected-hash
   checks must pass;
4. one independent authority/ownership reviewer and one independent
   D25/D26/PostgreSQL-gate reviewer must audit identical commit/tree bytes;
5. both must return `GO` with `P0=0` and `P1=0`; any byte change restarts both;
6. main must remain at the required activation parent until both reviews pass;
   and
7. the exact plan commit must then fast-forward main and be pushed before any
   new implementation worktree is created.

## 12. Protected main-worktree hashes

```text
pyproject.toml = 2af4b19962dc8a7d22e377be17f342530a06ee6395bbbf2a092eab36599c8fc2
groundloop_fyp_professor_feedback.pdf = 45c20ca46e9ad5bcd86b22c0d8882d1d611497f57ca8c45d3dea149260c110cd
groundloop_fyp_professor_feedback_v2.pdf = 59a13cd8d4bbb017e712c0f39e70f2eba136557e945f64f1b1fc3891742a79f0
render_groundloop_fyp_professor_deck.py = c12929c349a5c0be9793159143b09da40ea2a0b27df37b92d61d9ed6483d8c2a
PERSISTED_MATCHING_AMENDMENT_DRAFT.md = 167d1e7df5a720041fe0ff51879d08357f0dfbe3a7781ffaa08c0d958a47aa94
```

These files remain user-owned and outside every activation, lane, review, and
integration path above.
