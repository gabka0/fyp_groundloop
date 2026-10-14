# J33 private job-free document seal adapter

Date: 2026-10-14. Status: final qualification candidate; acceptance requires the
external complete-byte freeze, BOTH independent GO/P0=0/P1=0 audits, exact
sole-parent commit, two fresh postcommit checks and verified atomic push.
This immutable handoff records the pre-audit snapshot; subsequent acceptance
is bound externally, never by editing reviewed bytes. This is not a public
runtime, full M5, deployment, AI-quality or end-to-end utility result.

## Authority and ownership

Sole accepted parent987ff9ded5ff92f2e559e3865a2d369ea12b72b8 (R33).
Worktree m5-d33-job-free-document-seal, branch
workstream/m5-d33-job-free-document-seal. Root sole editor; two independent
read-only auditors own zero paths and perform no tests, application execution,
DB/provider/network/install actions or Git mutations.

Exactly four NEW owned paths:

1. src/groundloop/m5/runtime/postgres_document_seal.py
2. tests/m5/runtime/test_document_seal.py
3. tests/m5/postgres_runtime/d33_document_completion/test_document_seal.py
4. this handoff

Authority is D33 section2.3, original D33 implementation activation section6,
accepted readiness-before-promotion order correction and accepted R33. Old
M4, all migrations/installers/DTOs/digests/defaults/dependencies, public
coordinators and held C1/B33/AI work remain unchanged. No held WIP is imported.

## Detailed execution plan

Recorded BEFORE source/test edits at
/tmp/groundloop-j33-implementation.zhCeU3Q1/PLAN.md. Sequence: verify authority,
remote/custody; read native M4/R33 prerequisites and map gates; isolated DB and
unchanged baseline; implement only the private cursor-local adapter; real
owner/distinct-LOGIN adapter staging and rejection/rollback/retry matrices;
retained/pure/static/package/whole-repository diagnostics; complete-byte freeze,
two independent audits, exact commit/postcommit checks and atomic push.

## Implemented private boundary

The adapter retains exact021 and the native legacy lookup guard, complete
persisted document/declaration/source/job closure, all zero counters and the
genuine M4 active1/P declaration. It requires the actual retained complete
readiness contribution at N, validates its original N-1->N bytes/work, and
rejects completion authored in the current transaction. Native
validate_seal_point_local runs at actual N>=3. Only the legacy equality of
M4 evaluation revision and M5 revision is replaced by the frozen two-clock
proof. No old M4 wrapper or assertion is modified or called as the new route.

The cache must be the measured ports' adopted working cache for K. Bounded
consumed values are compared with validated persisted source; native OPEN's
inactive retained objects and reconnect hydration's omitted inactive objects
are both legal. No whole-cache reconstruction, inline oracle, constructor-
origin registry or cache adoption occurs inside the adapter.

Native staging order is structural promotion -> observation currency ->
grounding publication at N+1 with direct deltas suppressed -> genuine canonical
M4 evaluation SEAL expected_revision1, resulting revision2/K. The adapter owns
no outer transaction, base/runtime/head advance, result/children, contribution,
work/timing finalizer or cache adoption. Future C1 charges native writes/producer
boundaries once; no synthetic work or extra identity is introduced here.

Private function: `_stage_job_free_document_seal_local(ports,cursor,K,N,update)`.
It returns the unchanged M4 PublicationReceipt. There is no public export or
caller integration. Existing M4 source/DTO/SEAL/declaration/payload recipes,
021 migration/installer, privileges and runtime defaults remain byte-identical.

### Nonrevision validation map

1. Genuine exact measured M4 ports and same cursor/store connections; caller's
   active read-write READ COMMITTED transaction. No adapter-owned transaction.
2. Captured schema and exact021 catalog/bundle plus unchanged legacy lookup/
   temporary-shadow guard; lock base then M5 runtime in accepted order.
3. Exact persisted M5/base point and caller event/payload/update-kind/P/policy/
   revision equality; provisional complete N>=3, no terminal marker and all
   open-work/open-scope/blocking-failure counters zero.
4. Accepted R33/D29/D30 declaration/source closure, active M4 counter1/P,
   canonical declaration hash and source/policy/registry snapshots. All THREE
   direct arrays empty; no direct jobs, scopes, projections, overrides or
   evaluation transition/history. Independent root/job/scope closure and
   cancellation/provenance checks still run, not merely trusted counters.
5. Retained semantic_complete contribution at N, exact original N-1->N
   source/key/work artifact replay, plus prior-committed transaction proof.
6. Unchanged native M4 validate_seal_point_local at actual N and its nonreplayed
   coordination checks. Its failure injection point is retained.
7. Adopted working cache for K, exact native repository/engine classes and
   current policy. Compare only consumed source/claim/answer values against
   persisted bounded projections; do not invent cache-origin authority.
8. After native promotion, independently require exact canonical counter2/K,
   sole SEAL1->2 ID/payload/zero-override ledger and unchanged native receipt.

The older `_assert_incremental_evaluation` equality cannot be used at M5 N:
its other requirements (active complete counter, zero scopes, no overrides)
are retained and strengthened by the job-free1/P proof. No equality/guard in
the old wrapper is relaxed. Two live cases reproduce its incompatibility.

### Prior-commit proof and bounded availability ceiling

Raw xmin inequality is insufficient: an own subtransaction has a different
xmin. The final adapter reads the actual assigned full top XID and snapshot
xmax, lifts the visible xmin by PostgreSQL's nearby signed32 rule, accepts
visible older/permanent IDs, rejects candidates at/above xmax, and otherwise
requires native pg_xact_status to report committed. Own current and released/
subcommitted children remain in-progress. Snapshot xmax is latestCompleted+1,
NOT next allocated; an attempted future-XID acceptance shortcut was rejected
during review and never entered source. age() is not used because its cached
reference can predate the adapter's actual assigned top XID.

PostgreSQL freezing can preserve raw xmin while marking commit status in an
unexposed tuple flag. An ancient genuinely committed frozen row whose raw32
bits alias the current/future nearby XID may therefore conservatively fail
this proof. That is a fail-closed validation/availability ceiling, NOT an age
eligibility rule, a claim that such readiness is uncommitted, or arbitrary-age
frozen-row liveness. Refusal occurs before promotion/DML and does not fail the
epoch, alter lifecycle, clear timing, repair metadata or create an attestation.
Guaranteed arbitrary-age progress would need separately reviewed authority.
No schema/object/privilege/recipe expansion is made for this bounded proof.

Primary provenance: PostgreSQL16 [transaction information functions](https://www.postgresql.org/docs/16/functions-info.html),
[xid8 status implementation](https://github.com/postgres/postgres/blob/REL_16_STABLE/src/backend/utils/adt/xid8funcs.c),
[snapshot/nearby-XID implementation](https://github.com/postgres/postgres/blob/REL_16_STABLE/src/backend/storage/ipc/procarray.c),
[own/subtransaction handling](https://github.com/postgres/postgres/blob/REL_16_STABLE/src/backend/access/transam/xact.c),
and [freezing behavior](https://www.postgresql.org/docs/16/routine-vacuuming.html#VACUUM-FOR-WRAPAROUND).

## Executable evidence and exact ceilings

Detailed attempts/failures/commands/XMLs are retained in
/tmp/groundloop-j33-implementation.zhCeU3Q1/RECEIPT.md. The unchanged-source
baseline and initial failed draft fixtures are not candidate acceptance.

Genuine adapter positives run accepted O33 activation/OPEN and separately
committed R33 readiness, then invoke the real adapter/native SQL inside an
explicitly controlled outer transaction which is intentionally rolled back.
They are cursor-local ADAPTER evidence, not a committed combined publication.
No authored complete headers, fake jobs, test seal replacement, counter
overwrite, guard bypass or premature partial terminal commit is permitted.

Owned suite:86pure cases plus23live cases. Four genuine positive branches are
rootless/nonempty M5 roots x owner/separately connected distinct LOGIN. LOGIN
cache is built by unchanged native hydration OUTSIDE the adapter transaction;
owner cache comes from accepted native OPEN. Complete working/published cache
snapshots, engine revision/stats/object aliases and active epoch stay unchanged.

Actual native call order/arguments are observed without replacing primitives.
The reachable job-free DELETE closes one document row and two chunk rows:
native structural promotion physically rewrites their already-closed K->K
intervals, which are counted as writes, not erased as zero work. Direct
currency and grounding sets are genuinely empty in these histories; their
zero-loop native calls are valid, not fabricated nonzero paths. The17pure
consumed-cache vectors are explicitly CONTROLLED predicate checks, not genuine
nonempty job-free SQL histories. They include two mis-keyed cached states whose
embedded claim/answer ID differs despite equal counts/witness/status; the native
publisher consumes that embedded ID, so exact identity is explicitly checked.
Retained M4 tests cover nonzero direct paths.

Every actual client-side adapter DML position is cut:10positions x4branches =
40cuts. Five native named cuts x4branches =20cuts. Each verifies full SQL
rows/xmins/sequences and working/published cache rollback, then genuine retry;
deferred constraints are forced before intentional final rollback. Native
point-check failures, caller/cache/context conflicts (17x2), retained-source
wire faults (35+40), and header/counter/history wire faults (18x2) all reject
before adapter DML. Wire negatives modify read projections only; immutable
guards are never relaxed. Native catalog, lookup/temporary shadow, wrong
connection/read-only/isolation, stale/missing K/N, and original SEAL coordinate
checks are retained. Public-first lookup can raise native UndefinedTable; that
is recorded as rejection, not a weakened schema fallback.

Duplicate adapter staging rejects before repeated promotions. The unchanged
canonical native evaluation transition replay is zero-DML; wrong ID/kind/
revision conflicts. All positive adapter images are intentionally ROLLED BACK:
no partial M4-sealed/M5-complete state is committed to manufacture replay.
Full combined document seal, result-bound replay and losing outer coordinator
behavior remain B33/C1 evidence, NOT claimed by these adapter tests.

Native XID probe separately tests own top/current child/released child and a
later OTHER genuinely committed transaction. Its fixture-owned probe tables
are not GroundLoop authority, authored readiness or production schema changes.
Pure vectors cover wrap and conservative frozen-alias refusal.

## Final qualification snapshot

Owned final attempt2:109PASS/0FAIL/0ERROR/0SKIP,287.229s XML time;
owned-final-attempt-2.xml SHA256
5b092a168efdd2ba0bc84b80e609eceb433ce3bec9480cc320d8d018f7001888.
First owned107PASS and pure1060PASS/2SKIP receipts are OLD-PIN evidence after
the exploratory embedded-ID correction, not final acceptance. Corrected pure-M5:
1062PASS/2SKIP,1064total,101.825s XML time;
pure-m5-final-attempt-2.xml SHA256
c498c46f17f6ebe3d7a3d0fcc3cb030918f032e55cd6115339294cd1a6f78731.
The2skips are pinned local WiCE source and explicit full frozen100k opt-in gate,
neither claimed executed here. These counts overlap; do not pool them.

Retained live compatibility:484PASS/1SKIP/0FAIL/0ERROR,485total,1656.548s XML
time; retained-live-final-attempt-1.xml SHA256
b9737ab52122534f2fae4efce2fa252bd51a7e0c66c2b542d2bc2c08d203fc87.
The1skip is explicit opt-in real-model PostgreSQL smoke, NOT enabled.
Selected scope: all retained R33 readiness and O33 opener prerequisites,
D33metadata, direct M4 composition, typed M4 barrier, entire M5 PostgreSQL
oracle/coexistence directory, M4 evaluation overlay/integration and original
PostgreSQL suites. This is NOT a claim to rerun the entire2403case R33 live
suite or every unrelated migration/runtime suite. Exact command is external.

Actual whole-repository offline diagnostic has NO ignored directories:
2079PASS/1FAIL/1923SKIP,4003total,224.691s XML time;
repository-offline-final-attempt-2.xml SHA256
8d1e9758347e86d1785e91e973916e9f044ba8a39588153fd5ad147ec5b6010f.
First attempt2077PASS/1FAIL/1923SKIP is retained OLD-PIN evidence after the
embedded-ID correction.
It is NOT green. The unchanged pinned natural-history test rejects local
Dynagox's missing matching remote identity. No unrelated repo/remote is changed.
Both DB URLs are unset; provider keys empty and HuggingFace/transformer offline
flags set. Every skip/failure and initial draft failure remains in the receipt.
Only one live command has DB access at a time; offline commands may run together
with it. Their wall times are functional qualification, not latency/speed evidence.
The selected retained live run excludes ALL3J33 Python files and does not
import/call the unexported adapter; its actual source/test/helper inputs remain
unchanged through the narrow correction. This input-byte boundary is explicit,
not a claim that its in-flight interpreter reloaded changed J33 source. The
new owned live run starts only after retained compatibility exits.

Full src/tests Ruff, owned Python format, strict mypy149sourcefiles, compileall
src/tests, diffcheck and pipcheck PASS. Actual offline wheel build and no-deps
target install plus isolated installed module source/signature identity PASS.
Wheel153entries SHA256
4d688fbd2f2a3367d19c535c52a4bf1f0fad5f18ad41f2bac3d2a708ef893d69.
Previously cached build ZIPs were read; no download/dependency installation.
Root migration assets remain absent; installed021identity actually raises
FileNotFoundError. Packaging/deployment is therefore NOT complete.

Final source/test SHA256, fixed after embedded-ID correction before attempt2:

```text
0455683688283c8c15a9dfc6182e85c59aa6f0998cec298e642a6107e5201a10 src/groundloop/m5/runtime/postgres_document_seal.py
50a732d0874f1dd5bd3f6ea013ec115fe68c120e4418029b41f3f0949cdec968 tests/m5/runtime/test_document_seal.py
9fc683ed462e245323a4aa05a00232780f3ee7dd41ff55bef2210519821decec tests/m5/postgres_runtime/d33_document_completion/test_document_seal.py
```

## Reproduction and acceptance discipline

All final commands and XML counts/hashes/properties are under
/tmp/groundloop-j33-implementation.zhCeU3Q1/FINAL_COMMANDS.md and FINAL_RESULTS.json.
Owned database:groundloop_j33_document_seal_20261014 OID45831677, only in owned
groundloop-task16-runtime-socket-20261009 via
/tmp/groundloop-task16-postgres.1zFwTy; PG16.14, session-local jit=off only.
FINAL_INVENTORY.json verifies all23prior database names/OIDs unchanged. Total25
is those23plus owned J33DB and ONE intentionally retained fresh public-catalog
diagnostic DB from the unchanged readiness test:
groundloop_d32_public_64f98fef44c8411983ff58df0c0dcf6d OID46294616.
No database is deleted/reset. Owned J33DB has only permanent public schema;
no fixture roles/other clients remain, roles match baseline, serverjitON
unchanged; only owned socket container runs and user's groundloop-db-1 stays
STOPPED. Six protected HEAD/index/raw anchors and all13held content/modepins
remainPASS. Accepted original/order/O33 activations, H33/S33/M33/O33/R33
worktrees remain clean and unadvanced; their exact pins are in the manifest.

Before stage/commit/push recheck all6protected HEAD/index/raw anchors and13held
C1/B33 complete content/modepins plus clean accepted prerequisite worktrees.
Freeze all4COMPLETE content/mode/byte/Gitblob identities and exact sole parent
987ff9d in external FROZEN_MANIFEST.json. BOTH independent auditors must read
ALL4identical files and return GO/P0=0/P1=0. ANY edit revokes BOTH GOs and needs
relevant reruns/refreeze/full reviews. StageONLYthose4blobs, one sole-parent
commit, TWOfresh postcommit checks, atomic no-force branch/main push ONLYwhile
live main remains987ff9d, then live refs/custody/inventory confirmation. External
ACCEPTANCE.json binds those events; this freeze snapshot does not claim them.

## Ceilings and next boundary

J33 does not implement B33 matching publication, C2/live-value-equal carry, P33
absence reader, C1 overlap/reconciliation/public composition, failure or
documents with direct-job history. All remain PENDING/HELD under their explicit
activations; runtime defaults stay v1_only outside isolated fixtures.
Task17/Task2/full D24--D33 mapping/M5.4--M5.6, packaging/deployment, AI quality,
independent natural histories and end-to-end speed/cost/utility remain PENDING.
Next scoped action after accepted/pushed J33 is B33 diagnostic resumption;
final B33/P33/C1 acceptance still needs the separate C2 dependency authority.
