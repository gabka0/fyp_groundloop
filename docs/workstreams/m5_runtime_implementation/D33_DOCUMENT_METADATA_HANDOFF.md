# M5-D33 M33 replacement-metadata implementation

Date: 2026-10-12. Status: complete candidate; acceptance receipt required.

## Scope and authority

Exact sole parent: `960acc33d5eb5623bf1434443dffaa9499bcd69c` (accepted S33).
Worktree `/home/kassym/Desktop/groundloop-worktrees/m5-d33-document-metadata`,
branch `workstream/m5-d33-document-metadata`. Authority is the accepted D33
amendment Section3, original implementation activation Section4 and accepted
readiness-before-promotion order correction. Root is the sole source editor.

Exactly four owned paths:

1. `src/groundloop/m5/runtime/postgres_withdrawal.py`;
2. `tests/m5/runtime/test_d33_document_metadata.py`;
3. `tests/m5/postgres_runtime/d33_document_completion/test_metadata.py`;
4. this handoff.

All other source/test/schema/contract/default/dependency files remain read-only.
Protected primary/AI and held C1/B33 work, accepted R32/R-T are preserved. Three
Sol6.1/xhigh read-only agents own zero edit paths: independent contract audit,
independent acceptance/test audit, downstream/result inventory. Only the first
two supply final complete-candidate/postcommit acceptance reviews.

## Detailed execution plan

1. Audit every actual metadata path and record the accepted baseline. Keep the
   legacy replacement event/payload hash and the exact D29 declaration intact.
2. Read replacement metadata from the exact old-version owning public document;
   preview is nonlocking, while preparation derives it at the existing locked
   source tier. Require same parent and valid predecessor source. Preserve
   NULL/Unicode/nondefault authority exactly, never default over existing data.
3. Carry the exact pair through private payload, immutable prepared image,
   proposal comparison, manifest/overlay validation and stored-direct replay.
   Replay derives from event-local sidecars and compares owning public metadata,
   never from a newer current version. Preserve INSERT and DELETE behavior.
4. Add pure and isolated live source-helper tests for genuine replacements,
   metadata/type/parent/source conflicts, changed previews and each independent
   public/manifest/overlay mismatch. Test real source locking, rollback cuts,
   reconnect/later legal progress and zero-write replay. Do not import held C1
   code or fabricate production composition. Source-helper scope is explicit.
5. Run full owned suites, retained withdrawal/provenance compatibility, applicable
   pure/differential/static/strict typing/package and repository diagnostics.
   Record exact commands/XMLs/hashes and all failures/skips/overlaps. DB suites
   run serially in owned socket test instance only. No provider/model/download.
6. Freeze all four complete file bytes/modes and required parent. Two independent
   complete identical-byte GO/P0=0/P1=0 reviews; any edit invalidates both. Stage
   only exact owned blobs, sole-parent commit, two fresh postcommit checks,
   atomic no-force branch/main push, live-ref and protected-custody verification.

External detailed orchestration plan/receipts:
`/tmp/groundloop-d33-next-steps.uu0yfevO/PLAN.md` and `RECEIPT.md`.
No test/acceptance result is claimed yet.

## Implementation and executable evidence — 2026-10-12

The initial paragraph/plan above records the pre-edit state. This is now a
complete implementation candidate, not yet independently accepted or pushed.
Exactly the four owned paths changed. Source SHA256:
`d88e15ffaf31f2306e7b7333f0e43d971706ed3b31153f488605d27245824737`.

The new private `_document_structural_payload` reads the exact old-version
owner at P, preserves exact URI/authority, and rejects malformed, missing,
inactive or wrong-parent source. It adds no locks. Existing source-tier locks
precede authoritative rederivation. Retained reads require the exact event-local
overlay and complete M4 event manifest plus owning-public equality. Private
prepared authority deep-binds the full payload; continuation rederives before
tier8 and compares the actual full proposal. Preview, prospective declarations,
source validation and stored-direct reconstruction all carry the same pair.

Historical replay permits a common exact closure L>K for REPLACE successor
version/chunks only under event-local comparison. Mixed/malformed intervals
reject. INSERT retains its previous rejection and exact defaults; DELETE still
has no inserted payload. Legacy event hashes, D29 three-array declaration,
present-state/hash recipes, M4 bytes/promotion rejection, schema/defaults and
public/export signatures are unchanged.

### Evidence boundaries and test coverage

Pure tests exercise exact NULL/empty/space/Unicode/nondefault preservation,
legacy identity, INSERT/DELETE compatibility, every independent metadata field,
coherent manifest+overlay lies, exact manifest/overlay closure, wrong/missing
parent, prepared substitution and actual continuation comparison/order. SQL
tiers in continuation unit tests are explicitly mocked: not live source proof.

Live tests install accepted000--018 in a unique schema, remain v1_only, seed
source/provenance before bootstrap, and use genuine measured M4Application
publication with empty scripted discovery and ZERO verifier/model requests.
They exercise actual absent-event preview, existing source-tier locks,
revalidation and retained source/stored-direct reconstruction. Two actual
same-document replacements and a new connection establish historical replay
with both original old and inserted versions closed through real M4 promotion.
SQL trace rejects all DML/current-head reads; all rows/xmins/sequences remain
equal on read replay. This is NOT a fabricated persisted M5 runtime closure.

Two-connection tests commit changed public source after preview, independently
rederive under locks and establish full-proposal inequality. Separate genuine
M4 promotion negatives reject caller metadata lies. Six live SELECT-result fault
cases alter manifest/overlay/public URI/authority independently without editing
immutable rows; an actual overlay UPDATE separately proves the immutable guard.
No guard/trigger/constraint is disabled or weakened.

Rollback coverage is seven named source/open hooks PLUS all16 actual mutating
executions in a genuine two-chunk M4 OPEN, including the interior chunk INSERT
loop. A successful rollback-only trace defines the cuts; each failing prefix
must match it exactly and restore every table row/xmin. PostgreSQL sequences
are nontransactional and excluded ONLY from rollback equality, never from
read-only replay equality. A fresh genuine M4 application then succeeds on retry.
Neither per-DML M4 source coverage nor a passing metadata helper accepts C1.

### Complete commands and receipts

All XMLs below are in `/tmp/groundloop-d33-next-steps.uu0yfevO`. Executable is
`/home/kassym/Desktop/groundloop/.venv/bin/python`; worktree is the M33 worktree
above. No provider, dataset/source download, model or deployment action occurred.

| Check | Executed result | Receipt |
|---|---|---|
| Complete pure M5 |898PASS/0FAIL/0ERROR/2SKIP,126.085s|m33-pure-m5-final.xml|
| Whole-repository non-live diagnostic |1915PASS/1FAIL/0ERROR/1740SKIP,283.812s; NOT GREEN|m33-full-repository-final.xml|
| Qualified owned+retained serial live selection |624PASS/0FAIL/0ERROR/0SKIP,483.591s; includes48owned pure+26owned live|m33-qualified-final.xml|

The qualified selection uses a fresh task-owned socket database
`groundloop_m33_metadata_20261012`, PostgreSQL16.14, session-only
`PGOPTIONS='-c jit=off'`, and `PYTHONPATH=src:tests`. Observed session setting is
off; server default remains on. This follows D29 activation Section11 and D31
repair's qualified functional-test profile, not a runtime/performance/default
change. Run command:

```text
PGOPTIONS='-c jit=off' \
GROUNDLOOP_TEST_DATABASE_URL='postgresql://groundloop@/groundloop_m33_metadata_20261012?host=/tmp/groundloop-task16-postgres.1zFwTy' \
PYTHONPATH=src:tests /home/kassym/Desktop/groundloop/.venv/bin/python \
 -m pytest --import-mode=importlib -q \
 tests/m5/runtime/test_d33_document_metadata.py \
 tests/m5/postgres_runtime/d33_document_completion/test_metadata.py \
 tests/m5/postgres_runtime/d29_store \
 tests/m5/postgres_runtime/d30_store \
 tests/m5/postgres_runtime/d25_store_core \
 tests/m5/postgres_runtime/test_direct_m4_composition.py \
 tests/m4/integration/test_postgres_pipeline.py \
 --junitxml=/tmp/groundloop-d33-next-steps.uu0yfevO/m33-qualified-final.xml
```

The complete624-case selection ran with no deselection or xfail; all74owned
cases passed. Its JUnit suite property `m33_actual_open_dml_cuts=16` records
the actual per-DML rollback cuts, not sixteen independent test cases. Test
wall times above are functional-check receipts, NOT end-to-end/model latency.

The non-live commands are below. The environment had no configured
GROUNDLOOP_TEST_DATABASE_URL, GROUNDLOOP_DATABASE_URL or DATABASE_URL;
the explicit unset arguments shown are the actual executed commands.

```text
env -u GROUNDLOOP_TEST_DATABASE_URL -u DATABASE_URL PYTHONPATH=src:tests \
 /home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
 --import-mode=importlib tests/m5 --ignore=tests/m5/postgres \
 --ignore=tests/m5/postgres_runtime -q \
 --junitxml=/tmp/groundloop-d33-next-steps.uu0yfevO/m33-pure-m5-final.xml

env -u GROUNDLOOP_TEST_DATABASE_URL -u DATABASE_URL \
 PYTHONPATH=src:tests:tests/m4/crash_matrix:tests/m4/incrementality:tests/m4/physical_runtime_gate \
 /home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
 --import-mode=importlib -q \
 --junitxml=/tmp/groundloop-d33-next-steps.uu0yfevO/m33-full-repository-final.xml
```

No test was removed or guard predicate relaxed. The sole executing diagnostic
failure is the unchanged real-history pinned Git source check: no local remote
matches frozen `https://github.com/gabka0/dynagox.git`. Its exact exception
message matches the accepted S33 parent-reproduction receipt; both relevant
source/test files are unchanged from parent. No external repository was edited.

Whole-repository skips:1731DB opt-ins (including26owned live cases separately
executed above),7explicit real-model/source profiles,1pinned WiCE reproduction,
1default-disabled100K gate. The two pure skips are WiCE and100K. None is PASS,
N/A or waived. This lane does not rerun the100K gate or reproduce WiCE; prior
S33 evidence is historical and M5.5 remains PENDING. Counts above overlap and
must not be summed as independent cases or new AI-quality evidence.

Owned Ruff/format, `git diff --check`, compileall src/tests and full strict
mypy src (148source files) PASS. Actual no-isolation wheel build and temporary
no-deps/no-network installation PASS; installed metadata module exactly equals
current source and imports/compiles. Wheel152entries SHA256:
`f30feb3420a787a6ab48a0c0c24455ed88e1a60314ffcd81cb89586a0e8b8955`.
Root migrations/sql remain absent as before: default installed bundle paths are
not certified and no unowned packaging expansion was made.

Complete final-source XML SHA256s in table order:

```text
1cb0c2f9514ec5db55e028da50e1e403459154b1a2b7cecd0ede7b4875ed99d5
f5738ff4bdb667409c4249165b6f5d93e06f3ba7467a3956a770781681f7ff58
349ff12dd646288942313006a57bb221719496b766a7c07959f499763e493959
```

Post-run database inventory:17databases, exactly the prior16 plus this task's
fresh `groundloop_m33_metadata_20261012`; all prior names/owners unchanged.
Fresh DB has only public/information_schema non-system schemas, only the
pre-existing groundloop LOGIN role and zero other client backends. No fixture
schema/role/client leak. Container inventory is unchanged: only the owned
groundloop-task16-runtime-socket-20261009 is running; user groundloop-db-1 stays
stopped and no other container was started/stopped. Server JIT remains on;
separately observed qualified-session JIT is off. No whole-instance purge,
default change or deployment action occurred. The task-owned test DB remains
available for diagnostics; no user DB or material file was deleted.

### Failed/incomplete attempts retained, not pooled

External RECEIPT.md and separately named draft XMLs preserve initial missing
digest-import collection failure, six test-author key/exception mistakes and
the immutable-overlay UPDATE rejection. Initial static formatting/unused-import
failures were corrected only in owned paths. Initial direct zip-wheel runtime
import failed because its implicit namespace folders are not zip-importable;
actual installed-wheel import is the passing package check, not that failed
probe. Worktree-local missing-venv/path lookups were corrected to absolute tools.

Independent advisory A caught an INSERT scope expansion in the first interval
draft; root narrowed it to REPLACE and added unchanged-INSERT rejection.
Advisory B caught missing actual source-change race/final comparison and per-DML
coverage; all were added. Old-byte and default-JIT broader runs were interrupted
with exit2 and retained XMLs; no partial run counts as acceptance. Default-JIT
fixture bootstrap compilation is not a product pass or performance result.
No teardown error was observed; fresh qualified DB avoids pooling inventories.

The first complete qualified run (m33-qualified-complete.xml) recorded
621PASS/3FAIL/0ERROR/0SKIP,454.300s. All three unchanged DELETE locator fixtures
failed because the new prospective argument eagerly read closure.epoch_id.
Root narrowed evaluation to exact REPLACE; INSERT/DELETE pass None and perform
no metadata SQL. The three actual live regressions then passed in8.517s
(m33-delete-compatibility-corrected.xml). No old fixture changed; the complete
624-case selection is rerun above, never replaced by that focused check.
Previous74owned/default-JIT,898pure/2skip and1915pass/1fail/1740skip full-repo
receipts use pre-correction source and remain historical, not final acceptance.

## Newly verified next-lane authority blocker

Two independent read-only source audits and root agree that the planned R33
genuine document positives have an unactivated prerequisite, unaffected by M33.
Accepted `persistence.py`4652--4665 is the sole production M5 update INSERT and
hardcodes `{}` for both group/direct paths. Accepted direct opener4358 delegates
there; the pre-seal facade/adapter stages only M4 work. No accepted source writes
the D29 declaration. M33's builder is read/compare-only. Exact021 requires the
one-key/three-empty-array declaration and independently validates its source.

R33 activation Section5 requires a genuine accepted document opener but owns no
opener source paths. C1 owns that production OPEN composition and starts AFTER
P33, which depends on R33/J33/B33. This creates a source-backed order cycle, not
a new claimed live failure. Existing accepted direct-M4 and S33 SQL fixtures
author M5 headers/declarations or have nonempty direct history; neither is an
R33 positive. The order correction permits controlled terminal SQL matching
publication, NOT authoring an OPEN declaration or importing held C1.

After scoped M33 acceptance: STOP affected R33 acceptance and request a new
audited opener prerequisite/split/order activation, with exact path ownership
and held-C1 custody. Do not edit persistence, transplant held C1, fabricate a
ready header or waive genuine positives. The separate later C2/live-carry hold
also remains mandatory. M33 itself can be accepted independently.

Two complete same-byte M33 audits, exact sole-parent commit, two postcommit
checks and atomic push/live refs remain PENDING until separately recorded.

## Explicit ceiling

M33 is metadata-source-helper evidence only. It cannot accept C1/public OPEN,
readiness/seal composition, active C2/failure coordination, certificate reader,
M5.4--M5.6 closure, deployment, AI accuracy or end-to-end utility. The retained
failing C1 REPLACE positive stays untouched and must later pass in its own lane.
R33/J33 run only after preceding accepted/pushed barriers; final B33/P33/C1
remain held for new C2/order authority and mandatory genuine live carry gates.
Runtime stays `v1_only` outside isolated fixtures.
