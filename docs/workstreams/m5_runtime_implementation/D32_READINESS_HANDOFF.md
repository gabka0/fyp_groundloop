# M5-D32 / R32 Private Readiness Handoff

Status: corrected R32 implementation candidate after withdrawn first freeze.
Final corrected-byte executable gates below passed. Acceptance remains pending
two identical-four-byte audits, exact commit, two postcommit checks and atomic
push. Those external receipts determine the disposition of these frozen bytes;
no R-T/C1 advance may bypass that barrier.

Date: 2026-10-10

## Authority, parent and exclusive paths

Follow D32 amendment SHA-256
0e9a4bbe8da34d8248ce138c4bc21cb8076ddb4a578ea6a36cd6a4e100811435,
runtime addendum revision 13 and the accepted sequential activation SHA-256
2e35e3d049d5793ddb0f5ad7af008ac20176ad18d8dca325b84e13e5f53e5740.
S32 was accepted by two identical-eight-byte GO/P0=0/P1=0/P2=0 audits, committed,
independently postchecked twice and atomically pushed to its branch and main.

```text
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d32-readiness-runtime
branch = workstream/m5-d32-readiness-runtime
sole_parent = 14f8def0f5117ab377f3e91af9c74de07737fb1d
parent_tree = 29b810b76f439cefaaf1a74e9304a78d4975233a
accepted_s32_manifest_sha256 = 3a837a65b6a6050d8c581bbb80d253f22fda679399525fa72553808103bbcd93
```

Exactly four new owned paths:

1. src/groundloop/m5/runtime/postgres_readiness.py;
2. tests/m5/runtime/test_semantic_readiness.py;
3. tests/m5/postgres_runtime/test_semantic_readiness.py; and
4. this handoff.

All existing source, tests, migrations, contracts, installers, public APIs,
protected primary/AI and thirteen held C1/R-T files remain read-only.

## Implementation prompt and execution plan

Act as the private D32 readiness agent. Implement only the group-event edges
structural_committed/1 -> semantic_pending/2 and semantic_pending/N ->
semantic_complete/N+1 (N>=2), each owning its separate idle read-write READ
COMMITTED transaction. Derive source identity only from locked persisted
event/declaration values. Return an existing timing anchor in a private receipt
with replay status; add no public method, DTO, default route or terminalizer.

1. Capture one permanent schema and require literal pinned 020 authority.
   Schema-qualify every persistent relation/function; retain base/runtime,
   C-ordered owner/answer, work/timing lock order and exact independent point
   validation. Reject direct/M4 surfaces and unsupported update kinds.
2. Independently check exact zero counters, durable jobs/scopes, terminal
   coordinates, genuine root closures/cancellation membership and true root-set
   identity. First edge additionally requires a genuinely job-free empty root
   set. Preserve declaration, publication/lifecycle/results and all D25 images.
3. Encode/hash source preimage S and existing key preimage K once each as
   producer boundaries. Only bytes_hashed/bytes_serialized are S+K. Resolve
   any older pending anchor as an immutable missing point without an
   intermediate accumulator update; insert readiness contribution before
   header advance; advance exactly one revision and perform one final work CAS
   plus one final timing CAS installing the new sole pending anchor. Force
   deferred checks only after the whole sequence.
4. Replay first validates immutable original contribution/work/key coordinates
   and current event consistency, returns the original anchor without writes
   after later/terminal progress, and rejects conflicting original revision.
5. Add pure identity/work/type tests and genuine owner/separately connected
   non-owner RETIRE/REPLACE histories using accepted structural-open, acquisition,
   staging, closure/verifier/cancellation APIs. No authored readiness header,
   fake direct job, trigger bypass, fabricated positive hash or zero overwrite.
   Trace schema/point/lock/write order; test pending/observed timing, every
   mutation rollback cut, idempotency/conflicts and both concurrent orders.
6. Run final pure/live owned, schema/readiness and retained compatibility,
   typing/lint/format/compile/diff and wheel-module gates. Record every failure,
   skip and evidence limitation. Freeze four exact bytes, obtain two independent
   GO/P0=0/P1=0 audits, make the sole-parent commit, obtain two postchecks and
   atomically push. Only then may the held reader lane resume under its grant.

## Evidence boundary

Readiness-only production histories use accepted
PostgresM5RuntimeStore.open_typed_event_atomically. It does not compose D25
matching state, so this tranche cannot prove integrated matching/seal or public
C1. Terminal-boundary adapters, if needed for replay/races, are labeled exactly
and are not production C1 evidence. No unaccepted held implementation is
imported, copied or transplanted.

## Implemented private boundaries

The package-private coordinator adds no export or public store method. Its
receipt contains the unchanged transition-timing anchor and replay flag. Each
new edge owns an idle read-write READ COMMITTED outer transaction. It captures
one permanent schema and requires the exact accepted 020 bundle before reads
or writes. Literal authority is unchanged:

```text
sql_020_sha256 = df0a3c0c9b228a4a22903479896326d27fbd6f98f5878e34d73182ba007bf837
bundle_020_sha256 = b7706feb7d54fcf9fdb4f9f38350a32967e0b460229b6f264493fb428be8ddfc
hash_freeze_commit = 24cd57c1c0eb624ae25a9723c7c9d7ad83903727
```

Every owned persistent relation/function is captured-schema qualified, native
operators are explicit, and the clock is pg_catalog qualified. Point reads
independently validate returned event/epoch/revision/phase coordinates.
The replay header pair is read in one statement snapshot: separate READ
COMMITTED statements cannot splice pre-seal and post-seal headers. Replay
validates the original contribution key, source, work vector and revision and
returns the original nonterminal anchor with zero DML, including after failure
or sealing. A competing fresh call rechecks after the base/runtime locks.

Fresh edges lock base, runtime, C-ordered owner/answer counters, work and timing
in that order. Exact zero multiplicities, true durable jobs/scopes, terminal
job coordinates, root-set identity, exact root closure/barrier artifacts and
canonical cancellation membership are checked independently. A first edge
requires genuinely empty job and root sets; REGISTER/REPLACE cannot skip their
accepted staging/closure path. Direct/M4 surfaces are rejected.

The producer encodes/hashes source S and unchanged D24 key K once each. Only
bytes_hashed/bytes_serialized are len(S)+len(K); every other counter is zero.
Validation/DTO rehashes are not charged. Prior pending timing is resolved by
one immutable missing sample, without an interim timing-accumulator update.
Readiness contribution precedes every header advance. Exactly one work CAS
and one fused timing CAS preserve cumulative history, advance one revision,
and install the new sole pending anchor. Constraints are forced last. No
D25 image, matching contribution, lifecycle, published result, head or mode
is changed by production readiness.

### Preliminary P1 and narrow legacy lookup boundary

Independent preliminary review identified caller-resolved operators in the
new SQL. A foreign bigint equality operator could select a different existing
epoch and produce a wrong receipt. The final code explicitly qualifies owned
equality/addition operators and independently checks returned coordinates.

Live probes also exposed frozen migration-015 authorizer and migration-016
terminal-insert invokers whose internal lookup uses the caller context. This
lane may not rewrite those functions. Before DML it therefore requires the
effective permanent lookup order (pg_catalog, captured schema, public),
deduplicated if the captured schema is public. Public is only a retained
pgcrypto dependency when distinct, never a fallback GroundLoop authority.
The check rejects
temporary relation/type names colliding with captured authority or native
types. Noncolliding private matching temporary objects remain permitted.
There is no SET search_path, foreign-schema fallback or frozen-function edit.
Unsafe contexts reject replay too. This is narrow fail-closed compatibility,
not arbitrary hostile-search-path availability or a general security proof.

A subsequent actual public-schema 000--019 installation probe failed the
accepted S32 production installer's structural catalog checksum before
readiness. The pin distinguishes extension bindings in public from the
installation namespace; when public is also the installation, those bindings
have a different normalized catalog identity. Accepting a permanent schema
name is not proof that its catalog is an accepted 020 baseline. The context
predicate now deduplicates public and has isolated pure coverage, while the
actual public installer/readiness probe is retained as a fail-closed negative
with unchanged table/xmin/ledger and structural-catalog snapshots. No valid
public-schema end-to-end history is claimed. Supporting it requires separately
authorized S32/catalog work, not a new hash, fallback or bypass in this lane.

## Executable coverage and exact claim boundary

The 34 pure cases independently frame Unicode S/K bytes, verify all 32 work
counters, reject invalid identities/types/revisions, check real terminal
shapes and replay coordinate conflicts, and observe both producer encodes and
hashes exactly once and cover the deduplicated context predicate. The 36 live
cases include the public catalog negative and genuine accepted-store RETIRE
and REPLACE histories for the owner and a distinct separately connected login
non-owner, real acquisition/staging/closure/cancellation, open/failed/direct
blockers, prior missing versus observed timing, 15 rollback cuts for each
readiness edge, conflicting/repeated revisions and concurrent edge orders.
Rollback loops are not counted as extra distinct pytest cases.

Full-schema snapshots include xmin and timestamps, so same-valued rewrites do
not masquerade as zero-write replay or rollback. Snapshot JSON is display-only,
never an identity/hashing recipe. Trace assertions cover schema qualification,
native operators, point reads, C-ordered locks, contribution-before-header,
six final point updates, and one work/timing CAS. EXPLAIN with seqscan disabled
checks two accumulator PK-index point plans only; it is not a natural planner
choice, whole-system boundedness or performance result.

Six supplemental owner/non-owner seal-boundary cases use the accepted D25
registration foundation inside the actual revision-1 open transaction, real
cancellation and D32 completion, and an actual separate accepted timing append
before sealing. The owned test-only SQL adapter starts from locked cumulative
D24 points, adds measured frozen seal S/K bytes once, inserts the contribution
before terminal runtime advance, performs one fused terminal timing CAS,
derives children with unchanged preterminal/result-bound builders, and forces
intact constraints last. It preserves genuine published images and replay.

These are SQL reader/terminal-boundary diagnostics, NOT a production C1
terminalizer, public registration workflow or full physical producer-accounting
gate. Seal S/K regions are explicitly partial byte instrumentation; helper
validation/oracle work is not silently promoted to measured seal work.
Both conflicting fresh completion/seal lock orders are tested: seal-first
rejects pending readiness before DML; readiness-first exposes the genuine new
unreported anchor, rejects seal, then reports it and retries legally. Two
separate concurrent historical replay cases read either before or after the
seal commit and return their immutable original anchors with zero writes.
Fast replay does not take the base lock and is not described as a lock-order
test. After-seal replay is independently exercised for owner/non-owner.

No held C1/R-T source is imported or transplanted. Genuine accepted-store
readiness histories prove this private coordinator only, since that accepted
opener does not compose D25 matching state. The later C1 lane must prove the
real production structural-open/readiness/seal composition independently.

## Final identical-executable-byte validation

Final reports are external under /tmp/groundloop-d32-sql-diagnostics.v9QrLA.
These runs use the same three executable file bytes frozen for this candidate:

```text
a81080c3d56bbd20daaa11d92bfe4df3d9ba2bd14cc1e4c34a7875efc9ca62a4  src/groundloop/m5/runtime/postgres_readiness.py
db36c0a15a7726a4cb19b2f54cb95d457f7cd3686ef11ee92aa03d37f5359c8a  tests/m5/runtime/test_semantic_readiness.py
11e3dce620043d43ab4ec39d15a0ba688d8fa1388cc69a44a74ed79a7bc6aa19  tests/m5/postgres_runtime/test_semantic_readiness.py
owned_report = r32-owned-final-v2.xml
owned_report_sha256 = 0a522588044b31d139e3b434183bb31627b792b3fae6fb525420f8160ba8fd2e
pure_report = r32-pure-final-v2.xml
pure_report_sha256 = c85c6774aaa613d5e30318eb4abef20533e1f8195772818b9faed833b9e53b30
compat_report = r32-schema-runtime-compat-final-v2.xml
compat_report_sha256 = f18a692324d2c8c95d1352ff5ba8839259e218d17aba7af732494ce584bbd45c
```

- Owned pure/live: 70 passed, zero failures/errors/skips, 189.885s.
- All applicable pure M5: 850 passed / two skipped, 124.641s.
- Retained schema/runtime compatibility: 187 passed, zero failures/errors/skips,
  390.340s: 020=84, 018=34, 019=12, root transitions=15, job lifecycle=5,
  open/failure/replay=37.
- Strict mypy on the new source, Ruff lint/format on all three owned Python
  paths, compileall src/tests with external bytecode, and diff check: PASS.
- Wheel archive/module and separately installed isolated-module checks: PASS.
  Wheel has 152 entries, exact new-module bytes, no root migrations, secrets or
  bytecode. Isolated imports resolve inside the installed target; Unicode
  artifact, six reference kinds and byte-only work checks pass.

Pure skips remain the explicit opt-in frozen 100,000-event randomized gate and
the absent pinned WiCE source checkout. No new skip/xfail hides a readiness
failure. Runs overlap (including pure owned cases); do not add them as unique
test totals. This compatibility run is not all PostgreSQL/M4/C1 integration.

The wheel is /tmp/groundloop-d32-r32-package-v2.9vVQ8V/wheel/
groundloop-0.1.0-py3-none-any.whl, SHA-256
387eb7ba815ea84bf8defcdf2bec7a188527cca3d3ee7dbb7545c6897e9d81e0.
Its isolated installed target is installed/ in the same directory. Missing
default root migration assets remain the unchanged packaging boundary; no
installed-wheel migration support or package-configuration expansion is claimed.

### Reproduction commands and environment

Interpreter is /home/kassym/Desktop/groundloop/.venv/bin/python, with
PYTHONPATH=src:., PYTHONDONTWRITEBYTECODE=1, PGOPTIONS='-c jit=off' and the
owned host socket /tmp/groundloop-task16-postgres.1zFwTy. Only owned container
groundloop-task16-runtime-socket-20261009 and test databases
groundloop_task17_readiness / groundloop_task17_readiness_compat plus fresh
owned groundloop_d32_public_<uuid> catalog-diagnostic databases were used.
The latter are retained as isolated diagnostic artifacts, not reused or moved
to alter an existing installation.
The user's groundloop-db-1 remains stopped and untouched. No provider/model
call, deployment or non-test migration was performed.

```text
python -m pytest --import-mode=importlib tests/m5/runtime/test_semantic_readiness.py tests/m5/postgres_runtime/test_semantic_readiness.py -q --junitxml=<owned-report>
python -m pytest tests/m5/matching tests/m5/reference tests/m5/incremental tests/m5/evaluation tests/m5/runtime -q --junitxml=<pure-report>
python -m pytest --import-mode=importlib tests/m5/postgres_runtime/test_migration_020.py tests/m5/postgres_runtime/test_migration_018.py tests/m5/postgres_runtime/test_migration_019.py tests/m5/postgres_runtime/test_root_transitions.py tests/m5/postgres_runtime/test_job_lifecycle.py tests/m5/postgres_runtime/test_open_failure_replay.py -q --junitxml=<compat-report>
python -m mypy --strict src/groundloop/m5/runtime/postgres_readiness.py
python -m ruff check <three owned Python paths>
python -m ruff format --check <three owned Python paths>
python -m compileall -q src tests
git diff --check
python -m pip wheel --no-deps --wheel-dir /tmp/groundloop-d32-r32-package-v2.9vVQ8V/wheel .
```

Compile uses a task-specific external PYTHONPYCACHEPREFIX. The owned pytest
paths have the required identical basename; importlib mode prevents pytest's
default import-file mismatch without changing package configuration.

## Retained failures and correction history

Preliminary runs are diagnostic overlaps, not acceptance evidence:

1. First combined collection failed (exit 2) on identical test basenames. The
   explicit importlib collection mode fixed invocation; no test was removed.
2. A first runtime expansion had three failures from a nonexistent runtime
   updated_at column. Only the new owned SQL was corrected; the basic rerun
   passed 23 tests. A later first expanded report passed 16 live cases.
3. r32-expanded-2.xml retained 19 passes / three failures: missing test Cursor
   and ValidationError imports and legacy authorizer temporary-table lookup.
4. r32-focused-hardening.xml retained seven passes / three failures: a trace
   assertion's function-call filter, the temporary authorizer collision, and
   foreign operator resolution inside the retained terminal-insert guard.
   Owned operator qualification, independent point checks and the pre-DML
   canonical-context rejection fixed the source issue; focused context rerun
   passed four tests. No frozen function, GUC or accepted installer was changed.
5. Initial direct-surface fixture used the wrong event attribute; the genuine
   accepted adapter's update.event_id fixed it. Initial SQL seal adapter runs
   retained half-terminal ordering and DTO call-coverage failures. The adapter
   was corrected to use the unchanged builder's exact predecessor/result
   ordering and joint immutable event/call coverage; the six-case final
   supplemental suite then passed.
6. Interim typing/lint/format errors were corrected only in owned Python
   paths. The final static gates pass; no ignored diagnostic was suppressed.
7. A read-only custody check initially hashed porcelain-v1 bytes instead of
   the authority's exact porcelain-v2 -uall -z bytes and reported a raw-status
   mismatch. The correct format rerun verified every HEAD, empty index, raw
   status and all thirteen full file hashes. It was a checker-format error,
   not a protected/held file change.

All diagnostic reports remain external; no failure is erased or laundered
into a positive gate. The historical S32 hash-freeze ordering incident remains
disclosed in H32/S32 handoffs and is not reinterpreted by this candidate.

The first complete R32 freeze was
/tmp/groundloop-d32-r32-audit.zRREEi/r32-final.sha256, SHA-256
a8ba9fe15b7354cbcc726276ebc53097e12ff8de1690129cd94b69e57d217188.
It is WITHDRAWN, never committed/pushed. Both independent reviews returned
NO-GO, not acceptance. Review A identified the duplicated-public context
predicate; the live probe subsequently bounded that concern to the upstream
unaccepted catalog limitation above. Review B identified a genuine P1 coverage
mismatch: the non-owner seal case replayed on the owner after its session
ended. The corrected test seals AND replays inside the parametrized session,
proves the principals' equality/difference using actual current_user values,
and checks the complete zero-write snapshot there. No accepted receipt is
reused for these changed bytes.

Its earlier 67-pass owned, 848-pass/two-skip pure and 187-pass compatibility
reports remain r32-owned-final-preaudit.xml, r32-pure-final-preaudit.xml and
r32-schema-runtime-compat-final.xml. The old package remains under
/tmp/groundloop-d32-r32-package.4rdH3z. These are withdrawn-candidate overlaps,
not the corrected final-byte acceptance gates and are never pooled with them.

The first new public-schema probe retained its production-installer catalog
failure in r32-public-context-first.xml; it was not a positive history. The
first corrected focused run retained four passes/one failure because its test
assertion forgot the installer's noncolliding temporary schema in
current_schemas(true). Filtering only temporary namespaces in that assertion
matches the actual permanent-context predicate, without permitting a foreign
permanent fallback. The next focused run passed five cases (two pure context,
one catalog negative, two same-principal after-seal replay). Reports remain
r32-review-corrections-focused.xml and r32-review-corrections-focused-2.xml.
Final corrected-byte full gates below supersede, not erase, the old reports.

## Custody, final-byte barrier and next lane

Before this candidate freeze, root rechecked the exact protected/held HEADs,
empty indexes and porcelain-v2 -uall -z hashes from the accepted activation,
plus all nine C1 and four R-T full file hashes. All matched. The primary remains
14598ae51562006eaf67850b19e8212f38997903 and is clean; the protected AI lane and
held C1/R-T are not staged, committed, pushed, reset or advanced. Origin main
and S32 branch both point to the accepted 14f8def parent at this barrier.

The final complete four-path external manifest is
/tmp/groundloop-d32-r32-audit.zRREEi/r32-final-v2.sha256. Its own hash and both
candidate verdicts, exact commit/tree/parent/path identity, both postcommit
checks and atomic push receipt are external acceptance records. These bytes
cannot declare their own future acceptance. Any source/test/handoff change
requires a fresh complete manifest and two identical-byte audits.

Only after R32 scoped acceptance and push may R-T resume. It must first prove
all held custody, ancestry and changed-path disjointness before fast-forwarding
without reset/clean/stash/transplant. It must add semantic_readiness to its
exact reader allowlist and independently prove genuine owner/non-owner pending
histories. Then, after a separate R-T acceptance/push, C1 may resume similarly.

Task 17/C1/Task 2, D24--D32 implementation closure, M5.4--M5.6, public workflows,
deployment, AI quality and independently adjudicated end-to-end utility remain
PENDING. Defaults remain v1_only outside isolated fixtures. No security,
novelty, cost, model-quality or latency claim follows from this private tranche.
