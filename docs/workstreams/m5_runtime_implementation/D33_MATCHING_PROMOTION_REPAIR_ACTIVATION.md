# M5-D33 Narrow Matching-Promotion Repair Activation

Date: 2026-10-11

Status: docs-only activation candidate. Acceptance requires two independent
reviews of identical complete bytes, each GO/P0=0/P1=0, sole-path commit, two
fresh postcommit checks and atomic branch/main push. This docs-only tranche
stops after acceptance; production helper edits start in a later continuation.

## 1. Authority, exact parent and sole docs ownership

The user approved preparing this narrow repair activation and then resuming
schema freeze/installer work. Authority remains AGENTS.md, frozen M5-D24--D33,
runtime-addendum revision14 and the acceptance matrix. This is an execution
order/path correction, NOT a semantic amendment, schema authorization or
acceptance of the uncommitted S33 draft.

```text
activation_worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d33-matching-promotion-repair-activation
activation_branch = workstream/m5-d33-matching-promotion-repair-activation
sole_owned_activation_path = docs/workstreams/m5_runtime_implementation/D33_MATCHING_PROMOTION_REPAIR_ACTIVATION.md
required_parent = 52ae94cea2a3918c24b95a73ecb8f66f71b7cafe
required_parent_tree = cbcea4ae7298fc93a44cb9557baaf82eae341c0c
required_origin_main = 52ae94cea2a3918c24b95a73ecb8f66f71b7cafe
original_activation_path = docs/workstreams/m5_runtime_implementation/D33_DOCUMENT_COMPLETION_IMPLEMENTATION_ACTIVATION.md
original_activation_sha256 = 320170693177b6e939b98ab42382d96b0dd1e55271d123d9aa97ee505ddf591b
frozen_D33_sha256 = 055ef0d6f971a23be31cc23d2b9f03b4b7cd17fa9f9bc7c505d1f565d6726b32
```

Root owns ONLY this new document in the activation tranche. Two independent
read-only auditors own zero edit paths and perform no DB/provider actions.
Original activation, contracts, status/decision/roadmap files, protected
checkouts, held S33/C1 and ALL source/test/migration paths remain read-only.
No DB operation occurs during this docs-only tranche.

## 2. Confirmed defect and evidence ceiling

At the exact accepted parent, promote_matching_overlay uses INSERT ON CONFLICT
DO UPDATE for positive observation, edge, hash-mask and Hall current rows.
Migration017 BEFORE guards journal an attempted INSERT as well as an existing
row's actual UPDATE. Its deferred validator requires exactly ONE UPDATE with
a present first_old for an existing positive key, or ONE INSERT with absent
first_old for a new positive key. The validator is correct and stays unchanged.

The genuine complete-group document-loss S33 diagnostic reached the existing
Hall path and failed with "persisted matching seal current upsert mismatch".
The journal probe recorded ('INSERT','UPDATE',2,true,true), not the required
('UPDATE','UPDATE',1,false,true). Both exploratory auditors independently
confirmed the helper defect and the four analogous positive paths. Accepted
source identities are:

```text
src/groundloop/m5/runtime/postgres_matching_publication.py
  aeb2d084a2acf3ece5fc0f995c23d25c62563f658d0dce4ce2674e8727397bcc
migrations/017_m5_persisted_matching.sql
  e387b01fa80145273ba40d2d83581bc54762d3a4a2edd34669c095076c52154c
```

Read-only local evidence: D33_SCHEMA_021_HANDOFF.md and
/tmp/groundloop-d33-schema-audit.TPhExi/receipt.md, including retained initial
seal failure and failed journal probe/teardown. The final raw schema diagnostic
was31 PASS/0 fail/error/skip (875.254s), including an explicitly named expected
helper-defect rejection; its XML SHA256 is
5e2658c6fe55fb285915f2563d8bad791fa6f053628e2cea1c979cbae6846fea.
The separate retained020 baseline was84 PASS/0 fail/error/skip (394.921s), XML
SHA256 ec40a11430bba09a7d135d3565237d20579be7d364f36852ed7db6ec72c9b634.
Counts overlap other targeted runs and must NOT be pooled. Temporary receipts
are local diagnostic evidence, not durable acceptance artifacts.

Controlled SQL promotion uses genuine D25 working values/authorizers and one
guarded UPDATE or INSERT per key, with all triggers/journals/checks intact.
Its passing schema-boundary tests are NOT production helper/J33/P33/C1 GO.
S33 SQL's focused exploratory zero-findings checks are NOT acceptance audits.

## 3. Narrow precedence and sequential ownership

This manifest supersedes ONLY the original activation's execution order as
needed to insert the existing-helper prerequisite; its other paths, contracts,
gates and ceilings stay exact. No source lane starts before this activation's
accepted push. Thereafter the strict sequence is:

```text
held S33 (all draft bytes preserved)
 -> B33 promotion repair from this exact accepted/pushed activation
 -> checked disjoint S33 fast-forward to accepted/pushed B33
 -> resume S33 raw diagnostics
 -> separate H33 literal SQL/bundle/catalog hash freeze
 -> checked disjoint S33 fast-forward to H33; opt-in installer; S33 acceptance
 -> M33 metadata -> R33 readiness -> J33 private job-free seal
 -> P33 absence reader -> checked resumed held C1
```

Only ONE source lane runs at once. Root is the sole source editor; the two
auditors may work independently in parallel with zero edit paths. B33 is
complete and pushed BEFORE any S33 source resumes. H33 remains a separate
docs-only sole-path tranche under the original activation. Its accepted parent
must descend from accepted B33; S33's final sole parent remains accepted H33.
No H33/installer acceptance may be inferred from a mutable SQL hash.

B33 and future P33 intentionally share the publication module and existing
seal-promotion test file, but NEVER concurrently. P33 is not activated early:
it later starts from its accepted J33 parent containing B33, preserves the
repair and adds ONLY its original D33 absence-reader branch. B33 has NO reader
ownership. S33's four paths and C1's nine paths are disjoint from B33's four.
Any additional required path or contract change stops for NEW written authority.

## 4. B33 exact four-path implementation grant

```text
repair_worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d33-matching-promotion-repair
repair_branch = workstream/m5-d33-matching-promotion-repair
repair_initial_and_final_parent = exact accepted/pushed commit of THIS activation
```

Root may edit exactly:

1. src/groundloop/m5/runtime/postgres_matching_publication.py;
2. tests/m5/postgres_runtime/d25_publication/test_seal_promotion.py;
3. tests/m5/postgres_runtime/d25_publication/test_positive_promotion_journal.py (new);
4. docs/workstreams/m5_runtime_implementation/D33_MATCHING_PROMOTION_REPAIR_HANDOFF.md (new).

In path1 edit ONLY promote_matching_overlay's four positive-row write paths
and, if essential, a small private SQL helper used solely by that function.
Keep the activation-image initializer and all publication/absence/preterminal
readers byte-identical. Public signatures, DTOs, exports, digests, counters,
schema-admin installer and runtime defaults do not change. Paths2/3 own only
focused repair coverage and necessary adjustment of existing promotion
recording expectations, not unrelated rewrites or easier replacement fixtures.

For each positive working family, UPDATE keys already present in current,
then INSERT ONLY keys genuinely absent in current under the existing locked
image/epoch authority. Do not use a conflict-handling INSERT for an existing
key, even if its values are equal. Preserve every complete primary-key join,
positive predicate, epoch prefix, installed epoch/revision value and existing
deterministic insertion order. UPDATE all positive working/current overlaps
once, including value-equal carry rows; untouched nonworking keys stay untouched.
The locked authority serializes compliant writers; an unexpected unique-key
conflict must fail/roll back, not retry as an extra journal mutation.

Preserve checked-transition then persisted-seal authorization, exact successor
revision validation, one current-image header update, original family/delete
order and all four tombstone DELETE paths. Do not acquire new late-tier locks
or release/reacquire caller locks. No broad current-image/full-history scan,
unbounded Python materialization or full-oracle production authority. Keep the
indexed epoch/key access pattern. The caller owns the outer transaction:
no BEGIN/COMMIT/ROLLBACK, forced deferred checks, result/header/head publication
or work/timing finalization is added inside the helper.

Each receipt *_writes is the sum of ACTUAL UPDATE and INSERT rowcounts for
that family; *_deletes stays actual DELETE rowcount. Validate independently,
not via authored work or rowcounts fabricated from working-set cardinality.
No synthetic counter, zero-work override or double charging. Migration017
guard/journal/one-operation law and every D24--D33 identity/present/absence
recipe stay unchanged. No schema file, validator, ACL, trigger, private-journal
DML, disabled guard, wrapper monkeypatch or held-C1 import/copy is authorized.

## 5. Required repair gates on final identical bytes

Use accepted schema000--020 with its exact installer/ledger/catalog identities
for B33 production-helper acceptance. Mutable raw021 may provide a later S33
diagnostic but cannot be a B33 prerequisite or acceptance authority. Fixtures
may reuse existing accepted test foundations read-only; new code is confined
to the four paths above. No fabricated positive headers/working patches, guard
disabling, context forgery, journal rewriting or test-only publisher may stand
in for this production helper. Label controlled outer-envelope fixture scope.

Required evidence includes:

1. Reproduce the existing-positive double-operation failure against the
   accepted pre-repair helper as a retained diagnostic; after repair require
   genuine helper success, never a permanent expected-failure positive.
2. Cover BOTH positive SQL branches in ALL FOUR families with explicit unit/
   recording coverage of complete joins, predicates, parameters, ordering and
   unequal UPDATE/INSERT rowcounts. This is branch coverage, NOT a live positive.
   Separately record a producer-reachability matrix before live acceptance.
   Accepted producers reject result observations already resolved in current/
   working matching and emit new SUPPORT observations with before=None; prior
   support/withdrawal rows become tombstones. Existing-positive observation
   overlap/carry is therefore NOT APPLICABLE to genuine live histories, not a
   skipped test or production pass. Retain its defensive UPDATE branch/unit
   coverage without fabricating a patch. For every other source-proven
   unreachable combination, give the exact exhaustive producer/guard argument
   in the handoff and require BOTH final auditors to independently accept it;
   difficulty or a failing fixture is never nonapplicability.
   Genuine checked D25 histories must exercise all reachable combinations of
   existing/new positives, absent/present tombstones, mixed insert/update/delete,
   value-equal carries and untouched keys. Genuine existing-positive Hall,
   edge and mask cases remain MANDATORY. Prove complete current-image equality
   against an independent expected image/full oracle in those live tests.
3. For every reachable live combination, inspect authorized OWNER journals
   without altering them: exactly one UPDATE with actual first_old for an
   existing positive, one INSERT with absent first_old for new positive, one
   DELETE for present tombstones and zero for absent tombstones/untouched keys.
   Assert first/last operation, mutation_count, saw_* flags, complete before/
   final values and exact seal coordinates. Distinct LOGIN must pass genuine
   helper promotion too, without direct private-table access/privilege expansion;
   its journal law is proved by unchanged guards. Nonapplicable/unit cases are
   reported separately, never included in live-positive counts.
4. Run actual unchanged authorizers and production helper through a valid
   full sealed outer envelope, force ALL deferred constraints last, commit,
   reconnect and verify persisted image/result. Complete scope-limited fixture
   acceptance is not held C1/public store-first-open acceptance. Independently
   tally actual physical writes and compare all receipt fields and retained
   applicable work accounting, including nonzero update cases.
5. Failure injection after EVERY helper mutation boundary (header; each delete,
   positive update and insert), caller rollback, caller-controlled commit and
   both forced serialization race orders. Exact winning outer replay is zero
   DML; stale/wrong-revision or unauthorized second promotion rejects without
   extra durable journal/image changes. Do not invent a new public replay API.
6. Retain preterminal-context, R-T pending-anchor/missing/observed timing,
   D26 group REPLACE/RETIRE/present/certificate-only and six-kind recipe tests.
   Run the focused publication module suite, retained017/019/020 applicable
   live compatibility and applicable pure M5 differential tests. Record exact
   commands, environment, pass/fail/error/skip and overlaps; a skip/xfail or
   deselected genuine required positive cannot satisfy its gate.
7. Source strict typing, lint/format, compile, whitespace diff and wheel/module
   payload checks; preserve the root-migrations-outside-wheel limitation.
   No dependency/pyproject/package expansion, model download or provider call.

If any required positive needs an unowned helper/contract/schema change, STOP
that acceptance for a new activation rather than weakening a fixture or guard.
No repair GO until applicable required gates pass on the full final bytes.

## 6. Complete-byte and integration barriers

Before edits/advance/commit/push, all prerequisites must exit0; stop on nonzero.
Freeze an external complete path/mode/SHA256 manifest, including every owned
new/tracked file, and check exact tracked/untracked/index sets. Two independent
fresh full reviews must each return GO/P0=0/P1=0 on the SAME complete bytes.
Any edit invalidates BOTH reviews; regenerate whole manifest and reissue both.
For both this sole-path activation and B33's four-path candidate: stage ONLY
the exact owned paths after reviews, verify staged blobs against the manifest,
commit with the exact sole accepted parent, then obtain two fresh postcommit
identity checks (parent/tree/path/mode/blob/content/clean index/worktree).
Only then atomic no-force branch/main push and live verification of both refs.
If live main has moved, STOP before push/advance; do not overwrite, merge or
reset around it. Keep exact external audit/acceptance receipts and failures.

S33 may advance only by merge --ff-only after B33 acceptance, proving current
HEAD/index/raw status AND all three content pins below, exact ancestry and
the ENTIRE upstream changed-path set disjoint from ALL four S33 owned paths.
Do the same again for H33. Preserve held bytes across advance and verify after;
no reset/clean/stash/rebase/cherry-pick/checkout replacement/WIP transplant.
Original pre-edit pins are not the later authorized final edit manifest.
S33 must then change its expected helper-defect rejection to a genuine helper
promotion positive and rerun the complete applicable diagnostics on accepted
B33; retain historical failing XML. Its controlled schema-only tests stay
honestly labelled. Nothing here accepts current021 SQL or installer bytes.

## 7. Exact held/protected custody

All indexes empty. Primary/AI/accepted R32/R-T and held C1 remain read-only;
their pins are unchanged from original activation Section9 and D33 freeze
handoff Section4 (ALL nine C1 hashes, not historical earlier pins).

```text
primary_head = 14598ae51562006eaf67850b19e8212f38997903
primary_raw_v2_z_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
ai_head = a1dab3e5ebfe3bb65c521acd84758297a589f11a
ai_raw_v2_z_sha256 = 279b7ebf78bc4b19c71be49031ecc1a3415d1033864869344af98f07e6a438a7
held_c1_head = 8290d47cba71350cc550cf18cb12c32b47cb7b2a
held_c1_raw_v2_z_sha256 = 736f23f62b8ae744763219ab592266318662636dec4bcbc8a7be93e56ed0d58d
accepted_r32_head = d716ebb15a07fe703be21d48bf6b037f9253993e
accepted_rt_head = 8290d47cba71350cc550cf18cb12c32b47cb7b2a
accepted_r32_rt_raw = empty
held_s33_worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d33-schema-021
held_s33_branch = workstream/m5-d33-schema-021
held_s33_head = 52ae94cea2a3918c24b95a73ecb8f66f71b7cafe
held_s33_raw_v2_z_sha256 = 4529238c7947f216c73d766a41818e660367ea4d158af887ce3d7bc668dd415b
held_s33_index = empty; no tracked changes; exactly three untracked paths below
```

```text
467922f4f8b2735e1d19c318460fa35bb1e747a5a2ff675c0c77dab692739daa  migrations/021_m5_document_update_completion.sql
4b4ec23f87aa55b3bc5e1719407a5758556565a6ce3c2e15a816f4eaa287687a  tests/m5/postgres_runtime/test_migration_021.py
44788b20a40cc63326c1cc117c082a87389456447721cc488b9da7bb548150a2  docs/workstreams/m5_runtime_implementation/D33_SCHEMA_021_HANDOFF.md
```

S33's fourth owned path src/groundloop/postgres/migrations.py is unchanged
from held HEAD. Recheck live byte identity, not raw status alone. Preserve the
earlier C1 failed-precheck advance disclosure; later proofs do not pass it
retroactively. S33/C1 cannot advance during this docs-only tranche or B33.

## 8. Isolated execution and claim ceiling

B33 live tests may use ONLY explicitly owned temporary DBs/schemas in
groundloop-task16-runtime-socket-20261009, host socket
/tmp/groundloop-task16-postgres.1zFwTy (internal /socket), or an equally explicit
isolated instance after state inspection. Run live DB suites serially; do not
touch the user's stopped groundloop-db-1 or non-test data. No material receipt
deletion, model/provider/network calls or deployment/runtime enablement.

Activation acceptance implements nothing. B33 acceptance repairs only the
four-family physical promotion helper; it is not S33/H33/J33/P33/C1 acceptance.
Task17, Task2, M5-D24--D33 full implementation mapping, M5.4--M5.6, failure/C2,
deployment, AI quality, independently adjudicated natural histories and full
end-to-end speed/cost/utility remain PENDING. Defaults remain v1_only outside
isolated fixtures. No security, objective-truth, novelty or superiority claim.
