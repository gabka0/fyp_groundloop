# M5-D33 S33 schema lane — acceptance candidate

Date: 2026-10-12.

## Corrected current scoped acceptance candidate

This section supersedes ALL running/drafting/rejected checkpoint statuses below.
It is a complete S33 schema/admin candidate, NOT yet accepted. Two fresh
independent complete identical-byte GO/P0=0/P1=0 audits, the exact sole-H33-parent
four-path commit, two fresh postcommit checks and atomic branch/main push with
live verification remain required. No earlier advisory is a final GO.

Current fixed source:

- SQL198646f9a4b2f965d9ca89df684eb4f4287f3753aeff1d255c848bcb0352681c,
  4670lines/274945bytes; exact accepted H33 SQL, three bodies only.
- Installer6a98b0b5303e45a75661e23a2562d3cff36a89ca2b7c38a16a0ba0861f032ab2,
  5415lines/223246bytes;406 additive lines, all old source/defaults unchanged.
- Tests2cc08d5e19d271c7a3cc7e09c8e4364bbdfe9e387ac099364bd051a8c6d17248,
  3631lines/146638bytes; complete fresh run on these exact bytes.
- Required sole parent6e1fe1e6092c2b8f6e18f00d186f856d2b07c44e,
  treeab5199d9c20f252ab9070dcc9b3db2ae40a3cf3e; all four Git modes100644.

Fresh complete owned suite:89PASS/0FAIL/0ERROR/0SKIP,2414.270s, exit0.
Receipt `/tmp/groundloop-d33-schema-resume-audit.Jromux/owned-89-final.xml`,
SHA256bea952dcb01ad1d300a3939f6e7fab9883030991602f43b46e1dd47dd977fa1f.
This is the final complete suite, not a sum of focused/earlier runs.

Both rejected76 P1 coverage gaps are now exercised live in isolated PostgreSQL:
11 old-group negative/shadow/deferred wrappers on actual raw021, and two
complete-to-complete certificate cases. The latter use two genuine preactivation
supports, accepted D25 structural withdrawal, independent raw-input Python/full
SQL states/artifact/reference/set equality, empty status deltas, old binding
closure and present successor coordinates, retained immutable certificates,
and rejection of self-consistently rehashed false absence. All-present artifact
keyset equality is explicit. These are controlled schema boundaries, not C1.
Focused14 passed before the additional keyset self-check; its overlapping XML
is retained, while this full89 run includes the self-check.

Unchanged-implementation compatibility/build receipts below remain exact:
544 retained016/017/019/020 passes;850 pure M5 passes with two disclosed opt-in
skips; separate100K opt-in1PASS/100000events/statechecks/0mismatches; actual
isolated wheel build/import/payload PASS. The100K resolves only its own skip.
External pinned WiCE replay is still SKIPPED/NOTEXECUTED, never PASS/N/A/waived.
Final owned lint/format, src/tests compile, full strict mypy src148files and
diff checks PASS. SQL/module/wheel pins are unchanged by the test corrections.
Every failed/overlapping historical XML and both complete HOLDs remain retained.

Acceptance is ONLY the exact three-body/schema-admin implementation. The
unchanged production matching-helper rejection is an expected diagnostic, not
a production positive. Future-interval tests are rollback-only021 child probes,
not legal C2 restoration or P33 historical replay. Root/job/counter fixtures are
controlled boundaries, not full C1 accounting or end-to-end latency evidence.
Genuine C2 restoration/recurrent/later-only histories, mandatory live carry,
M33/R33/J33/final B33/P33/C1, D24--D33 implementation closure, M5.4--M5.6,
deployment, AI quality and end-to-end utility remain PENDING. Runtime remains
v1_only outside isolated fixtures. No live provider/real-model runs or dataset
downloads.

## Full-repository diagnostic — completed, NOT GREEN

The first corrected package (mode/path/hash manifest2475748012c739e25f0f0afefaa4e7ddbbc10bfa1ef714978020ffc02ae348f6,
handoff08b544047b735754aef76510b44ad249e5109932ccfcaec1aa3a07f2a090243d)
received two fresh complete HOLD/P0=0/P1=1/P2=0 verdicts: missing the D33§6(10)
full-repository diagnostic receipt. Both earlier coverage P1s were closed; no
additional source defect was found. Neither S33 scope nor C2 deferral waives
this diagnostic. The following execution/classification closes the evidence
omission, subject to TWO NEW complete four-byte audits; no earlier GO exists.

Full unfiltered collection/execution, no deselection/xfail override, ordinary
non-live defaults: both DB DSNs and all real-model/data/100K RUN opt-ins were
already unset. Explicit `env -u GROUNDLOOP_TEST_DATABASE_URL -u
GROUNDLOOP_DATABASE_URL` prevents accidental shared-DB use. All receipts below
are in `/tmp/groundloop-d33-schema-resume-audit.Jromux/`.

| Invocation / receipt | Observed result (XML time) |
|---|---|
| Plain `PYTHONPATH=src:tests python -m pytest -ra`; full-repository-default-diagnostic.xml | exit2;5 collection errors;11.240s; no tests executed |
| Identical plain command on CLEAN accepted H33 parent6e; full-repository-parent-default-diagnostic.xml | exit2; exact same5 errors;18.324s |
| Add supported `--import-mode=importlib`; full-repository-importlib-diagnostic.xml | exit2;4 helper-import collection errors;4.593s |
| Identical importlib command on CLEAN H33; full-repository-parent-importlib-diagnostic.xml | exit2; exact same4 errors;4.862s |
| Importlib + explicit legacy helper paths; full-repository-importlib-paths-diagnostic.xml | exit1;3582collected,1867PASS/1FAIL/0ERROR/1714SKIP;276.095s |
| Exact failing node on CLEAN H33, same profile; full-repository-parent-source-failure.xml | exit1;1FAIL/0ERROR/0SKIP;0.909s |

The executing invocation changes ONLY test import lookup, not source/config:

```bash
env -u GROUNDLOOP_TEST_DATABASE_URL -u GROUNDLOOP_DATABASE_URL \
  PYTHONPATH=src:tests:tests/m4/crash_matrix:tests/m4/incrementality:tests/m4/physical_runtime_gate \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  --import-mode=importlib -ra --junitxml=FULL_RECEIPT_PATH
```

Installed pytest help confirms this import mode. The three explicit directories
restore legacy absolute helper lookup; they do not omit tests or change guard
predicates. No cache deletion, unowned package initializer or dependency edit.
H33 HEAD/hash/index/raw clean state passed before/after baseline execution.

All collection failures are independently parent-reproduced: XML error images
match EXACTLY after normalizing only the two worktree prefixes. Plain mode has
duplicate-basename import-file mismatches for d24_requirement/test_races.py,
test_recovery.py and m5/runtime/test_contracts.py, test_frontier.py,
test_semantic_readiness.py. Importlib alone lacks crash_harness, harness,
physical_gate_harness and history_gate_harness (four affected nodes). Both
failed attempts remain failures, not PASS; no broad test-discovery fix is owned.

The sole executing-run failure is
`tests/m4/real_history_study/test_real_history_contract.py::test_pinned_git_blobs_diffs_and_extractions_verify_locally`.
It raises ValidationError at unchanged real_history_study.py567 because no local
remote matches the frozen source identity https://github.com/gabka0/dynagox.git.
The same node AND exact exception message reproduce on accepted H33. This is
pre-existing external source-identity configuration, not an observed S33
regression. It is NOT repaired, suppressed or converted into PASS. No external
repository remote/history was changed. Full-repository green is NOT claimed.

All1714 skipped cases are NOT EXECUTED in this profile; full per-node reasons
remain in the pinned XML. Exact profile classification:

| Skipped profile | Count |
|---|---:|
| PostgreSQL DSN opt-ins |1705|
| Explicit real-model/source-preparation opt-ins |7|
| Pinned WiCE source reproduction |1|
| Default-disabled100K randomized gate |1|

The1705 DB reasons are1retrieval,5event-audit,1incrementality,11evaluation-
overlay,5fresh-frontier,30M4pipeline,5physical-gate,600test-DB-unconfigured and
1047generic-DSN. The7 real profiles are M4.12(1), M4.13prepare(1), dynamic-
history(1), local-model-smoke(2), real-PG-model-smoke(1), real-history-study(1).
No skip is PASS/N/A/waived. Separate actually executed owned89/retained544 and
100K receipts above remain necessary; this diagnostic does not replace them
or authorize/complete broader live M4/M5, M5.5, providers or C1 gates.

Exact diagnostic XML SHA256s, in table order:

```text
a3eaf1d071d1e4d290a795d9c7f973d4633171880d6e84a064ae3fb114a2687d
320a81b8a81d2a3639666d3952473cad275b66bdd2ecb77173ca457428d251d7
1199bff4486dbf7eb746d89dc8e6ae79a3101b3554d46e2262257964c83023bf
92456764f037d1f5883525f3843be92acd37761258972d5c5377ee786848bec8
809dd81a6ed31de4b7138c0cede78ceb690cf9dc22c82df42bbdf11a355a64f1
4c66f26a99a8556bc8168d1ae8e057f0a41b8d4c09fe4c502d91366af4366126
```

Only this owned handoff changed after both HOLDs; SQL/module/tests and their
actual live/pure/static/wheel receipts remain byte-identical. New complete
freeze/audits, exact commit/postcommit checks and atomic push remain PENDING.

## Rejected76 audit HOLD and owned correction history — 2026-10-12

The76-pass checkpoint below is NOT accepted. Complete independent audits A/B
on identical four-file manifest ec7ee35a6dd911c821a9b5eef1b595f424515583f72dfa08a128f1df5f58677a
returned HOLD: A P0=0/P1=1 and B P0=0/P1=2. Both found missing reachable
complete-to-complete structural certificate-change coverage; B also found old
group negatives were exercised on020 but not021. Neither reported a SQL or
installer defect. The exact rejected manifest/XMLs remain retained separately.

Correction plan, owned tests/handoff only; SQL/module bytes remain frozen:

1. Seed a second genuine preactivation support on a surviving document. Delete
   the selected witness through accepted D25 structural recomputation, without
   C2; test present successor certificate, old-binding closure, immutable
   artifacts, independent Python/SQL/full reference bytes and rehashed false
   absence rejection. Keep empty status deltas distinct from certificate changes.
2. Reuse accepted020 negative assertions with the owned raw021 fixture for
   source/key/exact S+K/foreign counter/revision/phase/after-advance/wrong-kind/
   half-terminal, temp-shadow and deferred-result boundaries. Do not edit020.
3. Run focused new cases, then the complete owned suite and static gates;
   preserve all failed/overlapping receipts. Retained544, pure and actual wheel
   results remain evidence on unchanged SQL/module, not substitutes for021.
4. Freeze four NEW complete bytes, obtain two fresh complete GO/P0=0/P1=0
   audits, exact sole-H33-parent commit, two postcommit checks and atomic push/
   live refs. Stop after S33; all held/runtime/quality claims remain PENDING.

## Rejected76 acceptance checkpoint — retained history

The following76-case checkpoint passed its then-owned suite, but both complete
audits above returned HOLD for missing reachable coverage. It is NOT accepted
and its test hash is NOT the corrected current candidate. No earlier advisory
or partial pass carries forward as a final GO. C2 deferral is not permission to
omit reachable branches. Final acceptance still requires a fresh full owned
run, two identical-byte complete GOs, exact sole-H33-parent/four-path commit,
two postcommit checks and atomic branch/main push/live verification.

Rejected source checkpoint:

- SQL:198646f9a4b2f965d9ca89df684eb4f4287f3753aeff1d255c848bcb0352681c,
  4670lines/274945bytes; H33-frozen and unchanged.
- Installer module:6a98b0b5303e45a75661e23a2562d3cff36a89ca2b7c38a16a0ba0861f032ab2,
  5415lines/223246bytes;406 additive lines, no old000--020/default changes.
- Owned tests:c5a2c72c2956b72a81d7d79db08b9ab2df728affb9f63263aa0f0cbdb0124ae8,
  3345lines/135369bytes; final complete run below, not a sum of exploratory runs.
- Required sole parent:6e1fe1e6092c2b8f6e18f00d186f856d2b07c44e (accepted H33).

Rejected owned checkpoint and unchanged compatibility/build receipts in
`/tmp/groundloop-d33-schema-resume-audit.Jromux/`:

| Gate | Observed result | Receipt |
|---|---|---|
| Rejected owned021 checkpoint | 76PASS/0FAIL/0ERROR/0SKIP,2147.977s; audit HOLD | owned-76-final.xml |
| Retained016/017/019/020 | 544PASS/0FAIL/0ERROR/0SKIP,3935.047s | retained-016-017-019-020.xml |
| Broader pure M5 | 850PASS/0FAIL/0ERROR/2explicitoptinSKIP,102.297s | pure-m5.xml |
| Separate100K opt-in | 1PASS/0FAIL/0ERROR/0SKIP,1094.216s;100000events/statechecks,0mismatches | pure-m5-100k.xml |
| Owned lint/format, src/tests compile, full strict source typing, diff | PASS;148sourcefiles typed | Repeated after final test edits |
| Approved isolated actual wheel build/payload/import | PASS;152entries, explicit SQL-byte identity | Temporary build root documented below |

Rejected76 owned XML SHA256:
29831f92cd3b250655a73e7107bff39c3ee3f30ca4ba5c48b0fde11252cdbd31.
Compatibility XML SHA256:
ea52c7b2329b5327edeaf4708e0985f62fb927250942b608fc48f63b963ccd41.
The pure100K run resolves only that original opt-in skip, not the unchanged
external WiCE source-reproduction skip. Its exact node/source availability and
mandatory/PENDING M5.5 boundary are recorded below. No skipped test is PASS/N/A.
All raw45/installer11 and focused diagnostic counts below overlap the complete
owned76 run; they are retained history, never added to current totals.

Acceptance scope is ONLY the exact three-body/schema-admin implementation and
honestly labelled controlled SQL boundaries. The unchanged production matching
helper's expected double-journal rejection remains a diagnostic, not a helper
positive. Nonempty jobs, full Python/SQL comparison and reference bytes are
tested in the controlled document fixture, not C1 production. The future-
interval test is rollback-only021 child validation, not a legal restored L
publication, full deferred-envelope acceptance or historical reader replay.
Initial corruption negatives may be rejected by retained earlier guards.

Genuine C2 restoration/recurrent/later-only production histories and later P33
zero-write reader replay remain mandatory/PENDING under separate authority;
reachable S33 requirements are not waived by that dependency. M33/R33/J33,
final B33/P33/C1, D24--D33 implementation closure, M5.4--M5.6, deployment,
AI quality and end-to-end utility claims remain PENDING. Runtime stays v1_only
outside isolated fixtures. No provider/model calls or dataset downloads occurred.

## Resumption plan — 2026-10-11

Current authority is the accepted/pushed execution-order correction68eb4bc,
`D33_READINESS_PROMOTION_ORDER_ACTIVATION.md`, SHA256
`0ddfe719262e88c48ea6ae7bad288bbb2d6ac75a33a969a8f62f585ca7d32199`.
The earlier repair-first order/proposed next steps below are historical.
S33 was checked-fast-forwarded from52ae94c to68eb4bc: full protected/S33/B33
custody, ancestry and ENTIRE upstream disjointness passed before the advance;
all three held file hashes and empty index passed afterward. B33/C1 did not
advance. Root remains sole editor of the same four S33 paths.

Plan (follow in order; stop each acceptance on failed prerequisites):

1. Audit complete fixed021 SQL against D33 and retained000--020; two independent
   read-only body reviews. Complete missing raw source/identity/interval/as-of-K/
   restoration and self-consistently rehashed negative coverage. Derive exact
   three-body catalog images and canonical five-field bundle identity.
2. Pause source editing; prepare separate sole-path H33 literal hash freeze
   from current accepted main. Two complete same-byte reviews, exact commit,
   two postcommit checks, atomic branch/main push and live verification.
3. Recheck S33's NEW complete pre-advance checkpoint, protected/B33/C1 custody,
   ancestry and whole upstream/H33 four-path disjointness; fast-forward to H33
   preserving every WIP byte. Then implement only opt-in ledger-first021
   identity/install/require helpers. Old020 strictness/default installation and
   all000--020 files/ledgers stay exact.
4. Test all installer prerequisite/schema/catalog/lookup/temp-shadow/partial/
   caller-byte conflict/replay/tamper/DDL+ledger rollback and both race orders.
   Run full owned suite, retained applicable020/019/017/016 compatibility and
   pure differential/static/wheel-module gates; record failures/skips/overlaps.
5. Freeze ALL four final owned files; two fresh complete same-byte acceptance
   GOs, sole-H33-parent commit, two postcommit checks, atomic branch/main push
   and live verification. Stop after scoped schema acceptance, not M33/runtime.

The existing unchanged-helper expected rejection stays explicitly diagnostic
until B33's separately activated fifth-path conversion. No helper/production
readiness/seal/reader/C1 GO follows from S33 SQL boundaries. Final B33/P33/C1
remains held for new C2/order authority and mandatory live carry evidence.
Current execution receipts: `/tmp/groundloop-d33-schema-resume-audit.Jromux/`;
older failed diagnostics remain in the original receipt location below.

## Approved isolated build and remaining schema gates — 2026-10-12

User explicitly approved a temporary isolated environment containing build and
hatchling, with project dependencies unchanged. Root rechecked live main/H33 at
6e1fe1e, S33 HEAD/index/raw status/full four paused content pins and all protected/
held custody before resuming. The active source lane remains only S33's four
paths. No default/shared venv/package/project dependency change is authorized.
Temporary environment: `/tmp/groundloop-d33-build.722IjcPO/venv`.

Resumed plan:

1. Build the actual wheel with the approved temporary tooling; inspect payload,
   import/compile the packaged migration module and preserve the known omitted
   root-migrations limitation. Retain previous missing-tool failures separately.
2. Complete read-only audit B's six missing source/history/digest/oracle matrices
   in the owned021 test file, using genuine immutable source and accepted
   primitives. Controlled C2-dependent fixtures remain validator-only evidence.
3. Run all owned and retained compatibility gates serially on the owned socket
   database; resolve applicable pure opt-ins, full static/source typing and
   wheel checks. Record every failure/skip/overlap, no acceptance extrapolation.
4. Only after all prerequisites pass, freeze all four owned files for two fresh
   complete same-byte audits, sole-H33-parent commit, two postcommit checks and
   atomic push/live refs. Stop before M33/runtime/B33/C1 activation.

Observed100K exit0 and XML:1PASS/0FAIL/0ERROR/0SKIP,1094.216s,
SHA2562988af594fc430dcbfafe612f9ab97289277089a23ef40945ec43b82c11ba2a8.
The frozen test asserts100000 committed events/state checks and0mismatches.
This is deterministic in-memory systems evidence, NOT model quality or latency.

Approved isolated build gate PASS: build1.6.1/hatchling1.32.4 installed only in
the temporary venv; actual wheel build --no-isolation exited0. Wheel path
`/tmp/groundloop-d33-build.722IjcPO/wheel/groundloop-0.1.0-py3-none-any.whl`,
SHA256dbb545f336bbb8bd5705b1bad8ecc4a2e00b94b4af1645f0c72ca056972d70fe.
152payload entries; packaged migrations.py exact6a98b0b source, compilation,
actual zip-wheel import/export and explicit SQL-byte bundle identity PASS.
Root migrations/sql remain absent, as before; no default path/package expansion
claim. The earlier missing-build/hatchling diagnostics below are historical,
now resolved without altering project/shared environment dependencies.

Retained016/017/019/020 compatibility completed exit0:544PASS/0FAIL/0ERROR/
0SKIP,3935.047s, XML `retained-016-017-019-020.xml`, SHA256
ea52c7b2329b5327edeaf4708e0985f62fb927250942b608fc48f63b963ccd41.
The additional20 source/readiness/root cases are now running serially on the
owned socket, stop-on-first-failure, not yet live-verified. Lint found unused
imports before their test body and four long strings; corrected, lint/format
PASS. No acceptance advance occurred during those diagnostics.
S33 remains NOT ACCEPTED.

### Newly drafted coverage (not yet a live PASS)

The owned021 suite now includes initial malformed policy/registry source cases,
unchanged source/key and exact byte-work vectors, genuine nonempty-root job
progress/cancellation with independent1/P M4 counter, complete four-family
Python/full-SQL state equality and all six reference/artifact/set bytes, first-
close binding interval falsifiers, and initial self-rehashed result/receipt/
work/child/source negatives. Two independent read-only fixture advisories found
no obvious root/oracle construction blocker; B identified the wrong-close
regex and root corrected it to014's actual close-once rejection before any live
run. These advisories are NOT final identical-byte acceptance reviews.

The Python oracle hydrates raw domain inputs only, never SQL materialized/
certificate/patch outputs. It is deliberately limited to the one-group loss
fixture. The typed-P corruption cases fault initial insertion: older immutable
source/result guards may preempt021, so they do NOT independently establish
every021 predicate against an existing corrupted predecessor. Equal-to-start
binding closure is likewise a retained014 rejection, not new D33 evidence.
The nonempty-root history is controlled-open/schema/helper evidence: real
acquire/stage/barrier or cancellation, but zero fixture OPEN work and no claim
of post-barrier matching recomposition/full C1 accounting.

Still required for scoped acceptance: live validation of all added boundary
cases, complete fresh owned run after all changes, compatibility result,
applicable source/static/package gates and both fresh complete four-path
acceptance audits/postcommit/push checks. Independent published claim currency,
retained frontier missing-creator/wrong-policy, incomplete-before genuine D25
no-loss foundation and as-of-K future-interval probes are now drafted, not PASS.
The frontier negatives do not isolate policy/creator from fallback mismatch.

Scope refinement, not gate deletion: BOTH independent read-only authority
checks cite frozen D33 Section6(7), original activation Section3/7 and accepted
order-correction Section3--5. C2-dependent active restoration/recurrent/later-
only production histories explicitly WAIT for separately activated C2; original
permission for labelled controlled fixtures is not a mandate to bypass its
zero-write active-completion blocker. The earlier blanket remaining-history
checklists below stay historical. Those genuine gates are PENDING, never PASS
or N/A; scoped S33 still owes its reachable source/absence/interval/reference
checks. Both authority readings are advisory, not final acceptance GOs.

The as-of-K probe appends an adversarial UNCOMMITTED future binding interval
at L>K (open/closed), forces only the real021 children INSERT validator, then
rolls the ENTIRE image back. Future epochs are pending, not invented ready or
sealed; the old artifact is NOT asserted valid at L. This proves only that the
K validator does not apply a timeless exclusion to unrelated later intervals.
It is NOT a legal restored publication, all-guard terminal positive, complete
history or historical reader replay. The unchanged same-K successor negative
tests the converse boundary. All other guards stay installed/intact.

Pre-run source checks also caught/corrected an incorrect counter table name and
invalid negative anchor DTO construction: bad contribution-key bytes now fault
only the initial SQL INSERT wire, preserving the self-validating DTO. No live
result, guard weakening or acceptance advance was inferred from these drafts.
Do not simulate restoration by reopening a closed binding, authoring readiness
headers, disabling guards, advancing/reverting heads, or two terminal judgments
for one epoch/pair/policy. Current-head021 INSERT validation is NOT historical
zero-write replay after L>K: that later reader gate remains separately P33.

Full strict source typing now PASS:148 source files. Owned lint/format, all src/
tests compilation and diff checks PASS at this drafting checkpoint. SQL198646
and installer module6a98b0b remain unchanged. No S33 acceptance commit/push.

Both independent read-only scope checks confirm the unchanged external WiCE
source reproduction test is not a new S33 dataset-acquisition prerequisite:
original activation116--120 requires applicable pure M5 and honest skip
reporting; accepted order121--134 limits this lane to schema/admin correctness.
The actual skipped node is `tests/m5/evaluation/test_wice_reject_matrix.py::`
`test_pinned_wice_bytes_reproduce_the_frozen_adapter_audit` (marker216--222,
test223). No pinned local source/train.jsonl is available. It remains explicitly
SKIPPED / NOT EXECUTED, not PASS, N/A or waived. M5.5-01/02/03 source provenance
and independent adapter reproduction remain mandatory/PENDING (acceptance
matrix304--306; implementation plan930--955). No download was authorized.

The as-of-K probe now additionally verifies all future relational rows absent,
exact prior binding rows and both publication heads restored after rollback.
Nontransactional sequence advancement is deliberately not undone. Twenty new
cases collect; final drafting lint/format PASS. No live PASS is inferred yet.

First additional-case run stopped at5PASS/1FAIL/0ERROR/0SKIP,228.316s:
`added-20-first.xml`, SHA256
b4822a57edb3cbe4a23ab2de930d76ba454d515491cb3a43f429db103a1e8164.
The closed-future probe accidentally authored two committed/pending structural
epochs; unchanged004's one-open-epoch index correctly rejected the second.
Corrected ONLY the adversarial initial fixture rows to received/pending/0:
they are not committed, ready or sealed, and the entire image still rolls back.
No constraint/guard/SQL pin was changed. Full20-case retry is running; no
acceptance advance follows the retained failed run.

That retry stopped at6PASS/1FAIL/0ERROR/0SKIP,260.390s:
`added-20-retry-1.xml`, SHA256
5e44affc09f1a3432bbdb03882cc651384d28e2b5629cf807170207e41e27065.
The payload corruption was correctly rejected by retained017's exact
"persisted matching seal final envelope mismatch"; test regex omitted that
earlier guard message. Added that precise alternative without weakening any
guard. Both future-interval probes passed. Remaining14-case diagnostic is
running; these overlapping exploratory counts are NOT summed or acceptance.
The complete76-case owned suite is still required after all corrections.

Remaining14 first attempt stopped at3PASS/1FAIL/0ERROR/0SKIP,130.644s:
`added-remaining-14.xml`, SHA256
399cf596f7c5d6c8d93a7122372ef74db9c57703cf5ebe139c7c7c7bee219738.
Changing event-work counters with dataclasses.replace retained the old digest;
the immutable DTO correctly rejected it BEFORE SQL. Reset only the fixture's
work_digest input to its canonical constructor-computed value; corrected the
same mistake in five exact-byte-work cases before execution. This is not a
relaxed DTO/hash recipe. Remaining14 diagnostic is rerunning without -x to
collect any other fixture failures; no acceptance advance is authorized.

Remaining14 retry completed12PASS/2FAIL/0ERROR/0SKIP,463.462s:
`added-remaining-14-retry.xml`, SHA256
a71156006894f699ae80fcf031c98fa40a01109237b54d456ba2f1f7a07e0107.
All six typed-result corruptions, incomplete-before and both real nonempty-job
paths passed. The two remaining fixture errors were the bad-key assertion's
regex (021 correctly rejected its exact S+K identity/key), and an alternate
frontier policy with duplicate registered content. Corrected only the regex
to the actual exact message and built genuinely distinct canonical policy
content (frontier_depth+1) through the unchanged builder/registration API.
Those two cases are rerunning serially. SQL/installer remain unchanged.

Final two-case retry completed2PASS/0FAIL/0ERROR/0SKIP,67.380s:
`added-final-two-retry.xml`, SHA256
4d295c25dd1b1ab23cc888795b771e61684e1310ccbff93399cc512bd504df48.
The fresh complete76-case owned suite is now running on test SHA256
c5a2c72c2956b72a81d7d79db08b9ab2df728affb9f63263aa0f0cbdb0124ae8.
Earlier focused/exploratory counts overlap and are not acceptance totals.

## Historical H33/installer checkpoint — 2026-10-12

H33 is COMPLETE: sole-path commit6e1fe1e6092c2b8f6e18f00d186f856d2b07c44e,
treeab5199d9c20f252ab9070dcc9b3db2ae40a3cf3e, authoritative document SHA256
404f3b98a30aa8621fe03b5b905d8c72b99169fb3e6f70721a7c15c3cd73de6d.
Both independent complete same-byte and both fresh postcommit reviews returned
GO/P0=0/P1=0. Atomic H33 branch/main push and both live refs verified6e.
Root then rechecked ALL protected/C1/B33 and new S33 pause pins, full68..6e
ancestry/changed-path disjointness and --ff-only advanced S33. All four WIP
bytes, exact raw status and empty index survived. No protected tree advanced.
Accepted H33 is now S33's required final sole parent. Only four activated paths
are owned; migration021 remains byte-exact198646 and is no longer mutable here.

The complete pre-installer raw suite finished **45PASS /0FAIL /0ERROR /0SKIP**,
1390.083s, final198646 SQL and original307837 test checkpoint. XML
`raw021-full-198646.xml`, SHA256
de289b6ff4430a7cf857a26a62c3d1965c5a26a950bccf09a2629f3485cae67bee.
This is raw schema/controlled-SQL evidence only, including the unchanged-helper
expected-rejection diagnostic; NOT production readiness/seal/C1 acceptance.

The opt-in021 installer/checker is now drafted append-only in migrations.py.
It retains old020/default behavior; verifies all172 source bodies, structural
catalog, existing167 flags/signatures and the additional five oracle functions'
flags/ACLs/signatures. It retains exact five-field transitive000--020 authority,
captured permanent schema, ledger-first identity, fixed lock/reread, atomic
three-body replacement and ledger-last publication. No application/runtime
mode or default installation change is made.

Exploratory installer audit found two P1s in the first draft: unbound text
concatenation at path capture and unbound LIKE operator resolution. Both fixed
in the new021 block only: explicit pg_catalog concatenation and textlike.
Precision: literal groundloop_ extra functions were independently structural-
catalog protected; the LIKE wildcard additionally matches groundloopX names.
Canary tests cover real hostile operators/side effects and the broader LIKE
inventory. No change to frozen020 source. First lint attempt had three line
length errors; test additions had loop-binding/line-length errors, corrected.
Strict source typing and subsequent owned lint/format pass. No acceptance
advance follows a failing diagnostic. Two installer smoke tests PASS; full
installer matrix and remaining required raw/compatibility/pure/package/audit
gates were still PENDING at that checkpoint. Counts stay separate/overlapping.

Fresh complete installer suite now **11PASS /0FAIL /0ERROR /0SKIP**,93.833s,
`installer-full-final.xml`, SHA256
6b0505e9a8edb3694c5623337d97136ad2889f953439dabf1da571fd049c837f.
It covers every rollback cut, both forced installer race orders, exact three-
body production-installer delta, ledger-first conflict/replay/readonly require,
all40five-field ledger mutations, partial raw installs, schema/temp/fallback,
pre/post signatures/flags/oracle-five metadata and actual hostile concat/LIKE
canaries. The broader wildcard attack uses groundloopX (not literal underscore)
so the full172-source check, not just structural prefix inventory, rejects it.

Earlier installer matrix8PASS/2FAIL retained: a missing own021 id in a ledger-
only negative correctly reaches missing runtime tables; ADD/DROP column cannot
restore exact attnum/attisdropped catalog. First retry1PASS/1FAIL revealed that
REVOKE/GRANT also cannot restore original NULL proacl; predecessor-ACL negatives
now have independent fresh catalogs. Second retry2PASS. No guard loosened and
no positive uses tampered catalog. Final source module SHA256
6a98b0b5303e45a75661e23a2562d3cff36a89ca2b7c38a16a0ba0861f032ab2;
test SHA2566fa8ff7d99bbedb96d9732154546b092048b4a490d1bef4413ec0cde7074a193.
Strict typing, owned lint/format, compile and diff checks PASS.

Broader pure suite:852collected,850PASS/0FAIL/0ERROR/2explicitoptinSKIP,
102.297s. Frozen100K differential is running separately with opt-in enabled;
the pinnedWiCE source audit has no configured/local checkout. Neither skip is
a pass/waiver; no AI or independently adjudicated evaluation claim follows.

Packaging BLOCKED on environment prerequisites: python -m build lacks build;
pipwheel--no-deps--no-build-isolation lacks hatchling. Existing known venv/
system/cache probes found no builder. No package/dependency install is made
under the active prohibition. No schema/source acceptance commit/push occurs.

Remaining S33 checklist (all PENDING, no silent gate removal):

1. Explicit policy/hash/version/spec, registry density/count/hash and independent
   predecessor-family/frontier-policy/sealed-creator source falsifiers.
2. Typed-P result/receipt/work/children corruption with self-consistent rehashes;
   explicitly prove true nonempty-root document readiness or record its boundary.
3. Binding interval/as-of-K source histories: open/wrong close, same-K successor,
   legitimate later L>K restoration, transient restore-at-K PRESENT, recurrent
   loss and incomplete-before/no-loss. C2-dependent fixtures are controlled
   validator evidence ONLY, never active production/B33/C1.
4. Complete final owned suite (raw and installer together), retained applicable
   020/019/017/016 compatibility, applicable pure opt-in resolution, full source
   typing/static checks and actual wheel/module/payload gates after authorized
   build-environment resolution. Root migrations stay outside the wheel.
5. Both complete final four-path same-byte audits; exact sole-H33-parent commit,
   two fresh postcommit checks, atomic push/live refs. Until then S33 PENDING.

These source-coverage gaps were independently identified in read-only audit B;
they are not fixed by installer11PASS or raw45PASS. Root is stopping at this
honest checkpoint while requesting build-environment direction. All protected
and held B33/C1 pins remain exact; no held lane, M33 or runtime activation.

## Historical raw-SQL pause checkpoint — 2026-10-12

Root remains sole four-path S33 editor. No installer edit, H33 freeze, source
commit/push or held B33/C1 advance has occurred. New receipt directory above
retains every failure and retry; historical diagnostics below are not pooled.

Full SQL review of467922 returned two independent HOLDs: incomplete canonical
old-source validation and an unkeyed closed-binding-history search. The next
2d98c3 draft fixed both; B returned SQL-only GO but A found canonical inserted-
source equality insufficient. These earlier receipts cannot accept that draft.
All three source copies now use existing017 canonical identity/hash validators,
positive old interval bounds and accepted optional inserted provenance semantics.
Every validated affected shape drives group-keyed binding lookup; the independent
loss/binding/reference bijection remains intact. No new index/object/recipe,
input_hash==content_hash or dense chunk-index rule is added.

Current SQL SHA256 is
`198646f9a4b2f965d9ca89df684eb4f4287f3753aeff1d255c848bcb0352681c`
(4670lines/274945bytes). Both independent fresh complete SQL-only audits return
GO/P0=0/P1=0/P2=0 on these same bytes. This is NOT H33 or full S33 acceptance.
The fresh owned live catalog probe confirms172 functions, exactly three body
changes, unchanged structural fingerprint/flags/signatures/owners/ACLs/triggers/
CHECKs and all ledgers. Catalog source identity is independently reproduced:

```text
pre/post_structural = 27a02a8910e1a4b671b879cea1bdc0a5a35d9f693a870183355088990e207f62
pre_function_sources = 95de1ba328b2f3de1cf99f3810d7f992e2bf9d9d01b4b08c85940efec2e65cfa
post_function_sources = 8e8cfe6cbcf544d0e843bfa8189cb229573fa2b0cc597f84ff0a6d39139fb059
bundle_sha256 = 893baf9d50cbb84c27b356cbc329f97bc714ca8c280778ece920b5a4dc728eb5
```

Added intact-guard preactivation old-source/provenance falsifiers, independent
direct-eligibility/deferred-work/result-source matrices, all prohibited absence
kinds, coherent row-plus-manifest REPLACE attacks and fully coherent omission of
all11 affected families. The latter calls accepted preparation against the real
withdrawal source and rejects before immutable patch insertion. It is distinct
from the weaker child-to-shape attack, which remains covered.

Targeted runs:7PASS on the old-source/bounded fixes; a later7-case run1PASS/6FAIL
due to incorrect guard-layer/regex expectations, then its six retries6PASS.
On final198646, catalog/REPLACE/co-omission run1PASS/2FAIL: exact outer xmin guard
rejected the test's SAVEPOINT OPEN, and coherent omission correctly raised the
different EventConflictError. Complete-outer rollback and the exact domain error
expectation fixed these; both retries2PASS. No guard was bypassed. These runs
overlap and are NOT a45-case acceptance result. The current complete raw suite
collects45 cases; a fresh serial full run has STARTED, result PENDING. Matrix
subcases are not counted as additional pytest passes. Lint/format pass.

S33 source/test/SQL/handoff editing now PAUSES for the separately owned H33
literal-pin tranche. The installer and all remaining source/interval/as-of-K/
restoration/compatibility/static/package/final-acceptance gates remain PENDING.
H33 may pin reviewed SQL only; it cannot accept those gates or production runtime.

## Historical diagnostic checkpoint

This is an unfinished implementation handoff, not an authority freeze, a
completion receipt or permission to activate runtime behavior. SQL remains
mutable. H33, the opt-in production installer, complete acceptance gates,
two same-byte acceptance audits, commit and push are **PENDING**.
Source work stops at this diagnostic checkpoint pending direction on the
new narrow promotion-helper repair activation described below. No production
source ownership is inferred from a successful controlled SQL fixture.

## Scope and custody

Accepted activation: commit52ae94cea2a3918c24b95a73ecb8f66f71b7cafe,
`D33_DOCUMENT_COMPLETION_IMPLEMENTATION_ACTIVATION.md`, SHA256
`320170693177b6e939b98ab42382d96b0dd1e55271d123d9aa97ee505ddf591b`.
Worktree: `/home/kassym/Desktop/groundloop-worktrees/m5-d33-schema-021`;
branch: `workstream/m5-d33-schema-021`. Root is the only editor of its four
activated paths. Existing auditors A/B have zero edit paths.

The draft replaces only the three authorized function bodies. It adds no
schema object, reference kind, digest recipe, privilege, CHECK or trigger.
Migrations000--020 and the old strict020 installer remain untouched. The NEW
production installer must not accept021 until separate H33 literal authority.
Primary, AI, held C1, accepted R32 and R-T worktrees remain read-only/protected.
Held C1 is not copied/imported. External receipts retain exact custody pins.

## Current diagnostic evidence

Receipts are in `/tmp/groundloop-d33-schema-audit.TPhExi/receipt.md` and its
XML files. This temporary location is a local diagnostic record, not durable
repository acceptance evidence. Tests use the explicitly owned isolated
database `groundloop_d33_schema_20261010`, PostgreSQL16.14 in
`groundloop-task16-runtime-socket-20261009`; no user database/provider/model
is touched. DB tests run serially.

- Retained020 baseline: 84 passed, zero failed/errored/skipped.
- Raw catalog comparison: only three function `prosrc` values change; owner,
  ACL, signature, security/path flags, trigger/CHECK catalog and ledgers stay
  unchanged. Old strict020 correctly rejects the altered body catalog.
- Controlled old-group readiness remains valid under raw021.
- Controlled job-free DELETE takes both separate readiness edges without
  changing the genuine M4 evaluation counter, including its `xmin`.
- A genuinely complete group is seeded BEFORE activation. The actual accepted
  D25 structural foundation produces certificate loss and real logical deltas;
  both readiness edges pass. No positive disables triggers or rewrites journals.
- Controlled guarded matching publication and logical seal pass with intact
  guards. This SQL-authored fixture is NOT J33/C1 or production promotion,
  work, latency or end-to-end evidence.
- Rehashed omitted/wrong-kind/object/hash/coordinate reference diagnostics
  reject. An initial eight-case run had seven passes and one assertion-regex
  failure; SQL correctly rejected that case with a subtype error. Corrected
  expectation and explicit co-omitted publication/binding/reference case pass.
- Final current raw suite: **31 passed / 0 failures / 0 errors / 0 skips**,
  875.254s; two static source/inventory cases and29 isolated live cases. All
  collected cases ran, including the explicitly named unchanged-helper defect
  rejection diagnostic, which is NOT a production-positive gate. These counts
  are not pooled with earlier overlapping targeted or baseline runs.
- The two-document history accepts a real guarded controlled typed terminal
  predecessor and takes both readiness edges for the second document. No
  trigger disabling, authored old result hash or held C1 source is used.

Final raw XML: `raw-current-full-diagnostic.xml`, SHA256
`5e2658c6fe55fb285915f2563d8bad791fa6f053628e2cea1c979cbae6846fea`.
Test-file SHA256:
`4b4ec23f87aa55b3bc5e1719407a5758556565a6ce3c2e15a816f4eaa287687a`.
Retained020 XML SHA256:
`ec40a11430bba09a7d135d3565237d20579be7d364f36852ed7db6ec72c9b634`.
Owned test lint/format and compile checks pass. Full strict typing, package,
installer/race/compatibility/remaining source matrices are NOT yet accepted.

Earlier syntax, nested-format, fixture alias/seed/transaction, missing actual
delta and native digest-path failures are retained in the external receipt.
They are not erased, xfailed or silently relabelled acceptance passes.

## Exploratory review findings — not acceptance

Both exploratory reviews of SQL
`c80e4bbb3dd5fd2756ded85fabb031166ebbd900e666b98c348e5e33e63c79a6`
returned HOLD/P0=0/P1=2 each (one overlapping finding). Draft corrections:

1. Check withdrawn frontier policy and sealed creator authority before
   reducing its dependency to a fallback claim.
2. Independently qualify initial certificate losses against the effective
   final working state; require exact equality with closed/no-successor-at-K
   binding keys and absence references. Co-omission must reject.
3. Re-derive predecessor open/publication bindings and canonical publication
   ID, durable work digests, dense/sorted child sets and their hashes rather
   than accepting a self-consistently rehashed result envelope.
4. Use the accepted017 identifier validator for all29 Python strip-empty
   codepoints rather than SQL `btrim` alone.

Revised SQL SHA256
`467922f4f8b2735e1d19c318460fa35bb1e747a5a2ff675c0c77dab692739daa`
(4591lines/268652bytes) received two fresh focused exploratory fix checks.
A confirmed both its P1s closed with no new mismatch; B reported
P0=0/P1=0/P2=0. These are NOT full acceptance reviews. Targeted coverage and
complete lane gates remain pending; no prior review is an acceptance GO.

## OPEN production-helper blocker (outside S33)

Unchanged `postgres_matching_publication.py` uses `INSERT ... ON CONFLICT DO
UPDATE` for an existing positive Hall key. Migration017's BEFORE guard
journals the attempted INSERT and the resulting UPDATE. Live diagnostics
record `('INSERT','UPDATE',2,true,true)`; the unchanged deferred validator
correctly rejects this as a current upsert mismatch. Similar helper patterns
exist for overlapping observation/edge/mask keys.

Original failed seal XML and the failed journal-probe XML (one failure plus
one teardown error from attempted unfinished-envelope commit) are retained.
The unchanged helper source SHA256 is
`aeb2d084a2acf3ece5fc0f995c23d25c62563f658d0dce4ce2674e8727397bcc`;
unchanged017 SHA256 is
`e387b01fa80145273ba40d2d83581bc54762d3a4a2edd34669c095076c52154c`.
Both match accepted S33 parent52ae94c; this is a pre-existing helper defect,
not a rationale to weaken a validator or change its required journal law.
The passing defect-reproduction diagnostic means ONLY that the unchanged
helper failure is reproducible and rejected, not that production works.

Both independent auditors confirmed S33 activation section3 allows an
explicitly labelled controlled SQL publisher: genuine D25 working values,
unchanged checked/seal authorizers, one guarded UPDATE for an existing key,
INSERT only for an absent key, all journals/constraints intact and checks
forced last. It is not a production helper repair or production positive.

Actual helper repair needs a NEW narrow written path-exclusive activation:
S33 owns no publication source, and current P33 owns ONLY its absence-reader
extension. No migration017 relaxation, fourth021 function, private journal
rewrite or wrapper monkeypatch is permitted. D33 section6.6 and eventual
J33/P33/C1 production gates remain PENDING on this defect.

Proposed next authority request (not an active grant): a docs-only activation
that explicitly holds S33 and allocates a separate repair lane for ONLY
`promote_matching_overlay`, its existing seal-promotion tests, a new focused
positive-journal live test, and a repair handoff. It must reconcile future
P33's overlapping paths and require custody/disjointness before resuming S33.
Implementation should UPDATE existing positive keys and INSERT genuinely
absent keys separately for observation/edge/mask/Hall; preserve checked/seal
authorization, deletes, actual rowcount receipts, transaction ownership,
every017 guard and all present/absence recipes. Test each family, mixed
insert/update/delete, exact one-operation journals, independent actual-write
counts, rollback and both race orders on accepted schema. No SQL-validator or
runtime-default change is needed. Two identical-byte audits and exact
commit/postcommit/push barriers apply to both activation and repair. S33 must
then rerun its unchanged-helper regression as a genuine promotion positive;
the historical failing XML remains evidence, not a permanent expected defect.

## Remaining ordered work

1. Finish rawSQL source/identity/interval/as-of-K/restoration and malformed,
   rehashed counterexamples; fresh same-byte review of the final three bodies.
2. Separate docs-only H33 exact SQL/bundle/five-field/pre/post-catalog pin,
   two audits, commit, postcommit checks and atomic push. Then prove custody,
   ancestry and sole-path disjointness before `--ff-only` S33 advancement.
3. Only then implement the opt-in ledger-first021 installer/checker, preserving
   old020 strictness and default `v1_only`; exercise all prerequisite/catalog/
   lookup/shadowing/partial/replay/conflict/rollback/both-race-order cases.
4. Run complete owned/retained compatibility/pure/static/package gates; retain
   every failure/skip and overlapping count. Freeze the four-path candidate,
   obtain two acceptance audits, commit, postcommit checks and atomic push.
5. Request a separate narrow promotion-helper repair activation before any
   production source fix or affected production acceptance. Later M33/R33/
   J33/P33/C1 lanes are not activated by this draft.

Task17/C1/Task2, M5-D24--D33 implementation closure, M5.4--M5.6, failure/C2,
deployment, AI quality, independently adjudicated histories and end-to-end
speed/cost/utility remain **PENDING**. No full-system completion claim follows.
