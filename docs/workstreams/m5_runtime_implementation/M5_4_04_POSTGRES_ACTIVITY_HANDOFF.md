# M5.4-04 PostgreSQL Activity Handoff

Status: **final Lane B candidate after the corrected focused live gate**. The
prior `d6a7a97...` freeze received a commit-audit HOLD for two
bounded test-evidence gaps. The required Lane A -> Lane B topology remains
present, and the gaps are now addressed on stricter test bytes, but the
earlier live results do not certify those changed bytes. The first authorized
focused run on the audit-remediation candidate exposed one test-only pending-
timing projection shape error after 39 tests passed; Section 8 records it
without claiming a pass. The corrected focused gate then passed 40/40 with
exact pre/post inventory equality. Final commit freeze, ordered integration, the complete-
suite gate, status reconciliation, M5-D25, production sealing/providers,
deployment, and human approval remain separate gates.

Date: 2026-09-02

## 1. Authority, branch, worktree, and post-rebase boundary

This candidate implements only Lane B of the committed activation:

```text
activation commit/base: 3200b39cfe01a4f41fdd1cd1492e85afc468cc2e
activation tree:        b19b4895e03ec9904fb6aab15e9482e51ef8935f
Lane A commit/parent:   b9d251bfdcc4e617240b820f1da9ad289f0d6a23
Lane A tree:            21faf2330e99cd42abff331ea48b583cd7a19c70
branch:                 workstream/m5-4-04-postgres-activity
worktree:               /home/kassym/Desktop/groundloop-worktrees/m5-4-04-postgres-activity
audit-held HEAD:        d6a7a97e9c1ef53320a808bbcbb05fa27dd22f09
audit-held tree:        31fd987a55a7358cf1c292d8c24f1c2042b71ce1
expected final parent:  b9d251bfdcc4e617240b820f1da9ad289f0d6a23
```

The authoritative activation is
`M5_4_02_04_LATE_RESULT_ACTIVITY_ACTIVATION.md`, SHA-256
`ddacf6f068ecdd6c9fdc2dbd129621c92a1c49b7201403c3321d3c5a0ec8d4d4`
(775 lines, 37,205 bytes). Lane B began at the exact activation commit. Its
provisional `c3784e2aa0ee96e95b4f09fdd8cda23566954154` commit had the activation
as sole parent and tree `a7af989a2a6839f615faed16ad970db54501b9d2`.
The coordinator then rebased that disjoint two-path commit onto the exact
audited Lane A commit, producing carrier `677a5f0...`; the first post-rebase
freeze was the audit-held identity above, with Lane A as its sole parent and
the same exact two-path delta. No merge commit was created. The remediation
reopens only those two paths. Lane B made no model/network-provider call and
edited no production source or other worktree.

This handoff cannot contain its own final SHA-256 or the final amended
commit/tree that contains itself without a self-reference problem. There is no
placeholder or claimed fixed point here. The rebased carrier identity above is
the immutable input to this handoff update, not a claim that it contains these
later bytes. After the focused live rerun and final two-path commit are frozen,
the coordinator must externally pin that containing commit, sole Lane A
parent, tree, this handoff's SHA-256/lines/bytes, both Lane B path identities,
the combined five-path identities, and the ordered ledger.

## 2. Exact owned manifest and non-self pin

Relative to the activation base, Lane B owns and changes exactly:

1. `tests/m5/postgres_runtime/d24_requirement/test_recovery.py`
   `1fb910ec806727e33cecd2b27406e503a653b14a48fa19fb98f3ccd5689bb86a`
   (8,493 lines, 315,282 bytes)
2. `docs/workstreams/m5_runtime_implementation/M5_4_04_POSTGRES_ACTIVITY_HANDOFF.md`
   (this new handoff; final identity is externally pinned)

The test-file Lane A-parent diff is 2,031 insertions and 151 deletions. No
production source, fake-history path, activation/prompt, contract, migration,
schema, package export, status, roadmap, decision log, D25 draft, generated
dataset, model weight, secret, database volume, build cache, or protected
user-owned path changed.

After rebase, the activation-to-carrier manifest is exactly the committed five
paths. Its four non-self identities are:

1. `tests/m5/runtime/fake_ports.py`
   `a5ff263ca1545d5cf68e053c4509175ef10ca95909ec060ea9dc92826334b2e7`
   (3,009 lines, 121,568 bytes)
2. `tests/m5/runtime/test_typed_history.py`
   `4397723687cb361c8aee91fdfe83de63cd2cd814ae6c08bb4c1f832daa872e8c`
   (1,859 lines, 69,335 bytes)
3. `docs/workstreams/m5_runtime_implementation/M5_4_02_03_FAKE_HISTORY_HANDOFF.md`
   `b49fdd10835cc36ea4d5b001b3349230dc28091da5e204192e7978534e9760fc`
   (301 lines, 15,255 bytes)
4. `tests/m5/postgres_runtime/d24_requirement/test_recovery.py`
   `1fb910ec806727e33cecd2b27406e503a653b14a48fa19fb98f3ccd5689bb86a`
   (8,493 lines, 315,282 bytes)

Path 5 is this Lane B handoff and remains externally pinned after its final
containing commit is frozen. Relative to Lane A, the rebased carrier changes
only Lane B paths 4 and 5.

## 3. Implemented PostgreSQL evidence

### 3.1 Accepted live eight-row activity table

`test_m54_04_live_activity_matrix_is_exact_and_reconnects_without_dispatch`
realizes the accepted 2 x 2 x 2 table over:

```text
epoch_active
subject_active = requirement_active AND group_active
chunk_active
```

The eight rows independently include active/failed epochs, active/inactive
chunks, and requirement/group inactivity. Lane B realizes the subject-
inactive fixtures by changing exact staged lifecycle rows from `STAGED` to
`FAILED`; that schema-valid staged-lifecycle failure is not a structural
retirement or successor transition. Requirement and group inactivity are
separately established and checked. A `FAILED` group-family lifecycle is
independently schema-valid; `chunk_active=false` is paired with it only to
populate the `subject_active=false, chunk_active=false` cube cell. Lane A
separately owns the deterministic structural successor/replacement history
evidence.

For every applicable row, the test binds the serialized activity snapshot and
revision, exact precedence-selected archive reason, job/attempt/scope/runtime
state, cancellation attribution, result disposition, durable row shape,
confirmed work/timing, and owner/answer PENDING multiplicity. It installs a
predecessor currency image and proves both that image and the returned pair's
currency projection remain exact. It also snapshots the available semantic,
edge, certificate, claim/answer, publication, and head surfaces before and
after the return.

The precedence proved by the live matrix is:

```text
EPOCH_FAILED > SUBJECT_INACTIVE > CHUNK_INACTIVE > JOB_ALREADY_TERMINAL
```

Failed-epoch rows use a genuinely cancelled job and terminalized failed epoch;
they retain the original `cancelled_by_event_id`, `cancelled_by_epoch_id`, and
`cancellation_reason=epoch_failed`. Later serialized activity flags do not
rewrite that cancellation identity. A fresh connection and fresh
`PostgresM5RuntimeStore` then observe a terminal, non-executing acquisition
with `should_execute=False`, exact replay, no redispatch, and no state change.

The active/all-active verifier row deliberately remains fail-closed with
`M5-D25 persisted matching` and a complete zero-write snapshot. It is repeated
by `test_m54_04_verifier_all_active_is_d25_blocked_without_mutation`. This lane
does not manufacture persisted matching or claim active verifier completion.

### 3.2 Inactive root: two exact CAS transitions

`test_m54_04_inactive_root_uses_two_cas_and_reconnects_terminal` proves the
root write law as two separate compare-and-swap transitions:

1. result staging validates and persists the channel hit, selection,
   discovery result, attempt result/evidence, work, and timing; moves the scope
   from open to result-staged; keeps the root running; advances the revision
   once; and leaves open work and PENDING multiplicity unchanged; then
2. the event-wide barrier contributes the canonical empty inactive selection,
   completes the root as `COMPLETED_INACTIVE/subject_inactive`, closes its
   scope inactive, decrements open work/PENDING exactly once, and advances the
   revision once.

The inactive root creates no semantic observation, admitted pair, verifier
child, or frontier head attributable to itself. The test preserves every
unrelated semantic surface, rolls back at the barrier-contribution cutoff,
reconnects for the successful closure, proves exact replay after both closure
and a later revision, rejects future-revision and changed-root-set inputs, and
ends with a terminal non-executing acquisition.

The stricter audit-remediation form uses named nonzero retrieval/model work
and attempt timing. It independently frames the exact six-row inactive root-
stage byte projection; pins the full attempt-result activity, snapshot, job,
and NULL cancellation fields; binds execution evidence and its work/timing
digests; verifies the attempt-owned and root-stage work contributions, keys,
and accumulator; and records the inner attempt and outer root-stage timing
rows exactly. The canonical-empty barrier is independently framed as a
nonzero byte contribution with zero admitted pairs and no foreign counters;
its work key/identity/accumulator, pending timing anchor, outer timing row, and
settled timing accumulator are all exact.

### 3.3 Four exact audit-only archive shapes

The maintained live nodes exercise all four durable attempt-currency and event
terminality branches:

- `test_m54_04_nonexpired_preterminal_return_has_one_exact_anchor` proves the
  current-attempt preterminal archive, its one exact transition anchor, event
  work/timing accumulation at unchanged revision, a late-accounting cutoff's
  full rollback, retry through a fresh connection, exact replay before and
  after later terminalization, coherent changed-output conflict, independent
  disposition/work/timing conflicts, and cancellation attribution;
- `test_m54_04_expired_preterminal_return_is_exact_and_replays_after_terminal`
  proves archival only while the dense successor remains running, the expired
  attempt's required NULL output digest plus immutable expired-return binding,
  preterminal accounting at unchanged revision, replay after the successor
  later becomes terminal, and zero-write rejection of a future revision;
- `test_m54_04_nonexpired_postterminal_audit_is_isolated` proves returned and
  reused-artifact postterminal archives with execution evidence, ordinary
  terminal-audit result, timing, and general audit sidecar but no event
  contribution or accumulator mutation; and
- `test_m54_04_expired_postterminal_return_uses_exact_five_row_archive` proves
  the exact execution-evidence, expired-return, attempt-result, postterminal-
  timing, and general-audit five-row archive, with the terminal successor state
  bound into the immutable result.

`test_m54_04_expired_postterminal_cutoffs_roll_back_and_reconnect` injects
failure after each of seven postterminal archive/accounting/constraint
boundaries, proves full rollback, then succeeds through a fresh connection.
The retained recovery tests also prove that an expired-attempt/terminal-
successor call conflicts with a full zero-write snapshot until the legal
postterminal branch is available.

Every legal audit-only branch preserves event revision, job/scope/open-work
state, PENDING, currency, semantic observations, candidate/admitted edges,
the available matching/reference image, group/claim certificates,
claim/answer state, publication/result identities, and heads. Exact replay is
zero-write. Changed output, evidence, work, timing, disposition, activity/job
image, or cancellation attribution conflicts atomically.

### 3.4 Inactive verifier closure and accounting

`test_m54_04_inactive_verifier_persists_exact_closure_and_replays` covers both
fresh `RETURNED` evidence with timing and `REUSED_ARTIFACT` evidence without
new attempt timing. In an active epoch with an inactive subject it proves:

- `RUNNING -> COMPLETED_INACTIVE/subject_inactive`;
- one immutable pair input, verifier artifact/execution, ineligible semantic
  observation, attempt-result artifact, execution evidence, and verifier-
  completion contribution;
- one observation-artifact and inactive-completion increment but zero
  effective-observation increment;
- exact work, timing, digest joins, and one revision advance;
- exactly one open-work and owner/answer PENDING decrement; and
- no currency, edge, matching, certificate, semantic state, publication, or
  head change.

Immediate and fresh-connection replay write nothing. Changed disposition,
work, timing, output, or immutable verifier envelope conflicts without
mutation. `test_m54_04_inactive_verifier_cutoffs_roll_back_every_owned_row`
injects failures across all 16 owned write, revision, constraint, work, and
timing boundaries, requires complete rollback at each boundary, and then
reconnects for successful closure.

### 3.5 Maintained public and historical surfaces

The test-only changes preserve the existing runtime APIs and exercise the
accepted PostgreSQL implementation as a black-box persistence boundary. The
full recovery and broader PostgreSQL gates retain the earlier acquisition,
expiry/takeover, retryable/terminal failure, root closure, verifier audit,
work/timing, replay, recovery, and migration-016 coverage. No source defect
was exposed on the final test pin, so the activation's source-edit stop rule
was not invoked.

## 4. Interface assumptions and non-claims

- The accepted production classifier and persistence paths already implement
  the frozen activity precedence and inactive/late write laws; this lane adds
  maintained falsifiers and found no final production-source defect.
- Fixture SQL establishes only schema-valid activity preconditions and an
  independently inspectable predecessor currency image. It does not write the
  result, observation, edge, certificate, PENDING, publication, or head
  outcome under test.
- There are no authorized persisted matching rows before M5-D25/migration
  017. The all-active verifier path remains an intentional zero-write error.
- A fresh store acquisition with `should_execute=False` is store-level durable
  reconnect evidence. It is not an end-to-end claim that an outer provider
  callback was suppressed.
- These tests make no claim about model accuracy, objective truth, retrieval
  quality, benchmark performance, production seal, provider composition,
  deployment, security, or end-to-end utility.
- No M4-v1 contract/digest/public route, migration, schema, package export, or
  status document changed. M5.4-02, M5.4-03, and M5.4-04 remain `PENDING`
  unless a separately authorized reconciliation changes them after final
  integration and audit.

## 5. Post-rebase static and collection evidence

The following gates were independently rerun from the dedicated Lane B
worktree on exact-final executable bytes after the audit corrections. The
Lane B test remained exact SHA-256 `1fb910ec...`; the two Lane A executable
files remained the exact pins in Section 2. External temporary/no caches and
`PYTHONDONTWRITEBYTECODE=1` were used. No database connection was opened.

Environment:

```text
Python 3.12.3
ruff 0.15.22
mypy 2.3.0 (compiled: yes)
git 2.43.0
```

Base, ancestry, five-path manifest, Lane B two-path parent delta, and
whitespace checks passed. The carrier has exactly one parent, the exact Lane A
commit; the activation is its grandparent. The Python gates used the
activation's exact combined-path forms:

```bash
RUFF_CACHE_DIR="$M54_RUFF_CACHE" \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m ruff format --check \
  tests/m5/runtime/fake_ports.py \
  tests/m5/runtime/test_typed_history.py \
  tests/m5/postgres_runtime/d24_requirement/test_recovery.py

RUFF_CACHE_DIR="$M54_RUFF_CACHE" \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m ruff check --no-cache \
  tests/m5/runtime/fake_ports.py \
  tests/m5/runtime/test_typed_history.py \
  tests/m5/postgres_runtime/d24_requirement/test_recovery.py

MYPY_CACHE_DIR="$M54_PURE_MYPY_CACHE" MYPYPATH=src:tests PYTHONPATH=src \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m mypy \
  --strict --explicit-package-bases \
  tests/m5/runtime/fake_ports.py \
  tests/m5/runtime/test_typed_history.py

MYPY_CACHE_DIR="$M54_PG_MYPY_CACHE" MYPYPATH=src:. PYTHONPATH=src \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m mypy \
  --strict --explicit-package-bases \
  tests/m5/postgres_runtime/d24_requirement/test_recovery.py

PYTHONDONTWRITEBYTECODE=1 PYTHONPYCACHEPREFIX="$M54_PYCACHE_PREFIX" \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m compileall -q \
  tests/m5/runtime/fake_ports.py \
  tests/m5/runtime/test_typed_history.py \
  tests/m5/postgres_runtime/d24_requirement/test_recovery.py
```

Results:

```text
ruff format --check: 3 files already formatted
ruff check:          All checks passed
mypy pure --strict:  Success: no issues found in 2 source files
mypy PG --strict:    Success: no issues found in 1 source file
compileall:          PASS, exit 0
pure focused tests:  39 passed in 1.60s
all runtime tests:   392 passed in 3.72s
```

Cache-isolated post-rebase collection reported:

| Selection | Exact collection result | Duration |
|---|---:|---:|
| `test_frontier.py` + `test_typed_history.py` | 39 | 0.46s |
| all `tests/m5/runtime` | 392 | 0.88s |
| `test_recovery.py -k 'm54_04_'` | 40/122 selected; 82 deselected | 0.55s |
| all `test_recovery.py` | 122 | 0.55s |
| all `d24_requirement` | 171 | 0.68s |
| `test_migration_016.py` | 200 | 0.52s |
| all `tests/m5/postgres_runtime` | 797 | 2.16s |

Every collection process exited 0. No test was executed during collection.

## 6. PostgreSQL target, baseline, and interrupted-run recovery

All live gates targeted only the repository-local Compose `db` service through
`unix:///var/run/docker.sock`, published on loopback/local port 5432. The
sanitized test target resolved to `localhost:5432/groundloop`; credentials and
the DSN value were never printed. Generic GroundLoop and target-changing
libpq variables were cleared for the guard and every test command.

The database identity was PostgreSQL server number `160014`, database
`groundloop`, OID `16384`. Every inventory was read in a separate
`REPEATABLE READ`, `READ ONLY` transaction and rolled back. The complete
non-temporary ordered baseline was:

```json
[["groundloop",65280744,"groundloop"],
 ["groundloop_d24_requirement_6ee04557df8e4c42adfaceae86e99635",46562354,"groundloop"],
 ["groundloop_d24_requirement_7d74a56d26404e158ac326bda62a609f",46568432,"groundloop"],
 ["groundloop_d24_requirement_b70684d1bec743ff9fa8410cbd58f18d",46573737,"groundloop"],
 ["groundloop_m3_demo",440382,"groundloop"],
 ["groundloop_m4_incrementality_f8b9895166924e06a8a33773c58d9933",1880804,"groundloop"],
 ["information_schema",13212,"groundloop"],
 ["pg_catalog",11,"groundloop"],
 ["pg_toast",99,"groundloop"],
 ["public",2200,"pg_database_owner"]]
```

Its compact ordered-JSON SHA-256 is
`9521c8ef7907a31bafccd0ca2f416a964ea505a156e2e8ca82503f92e31fcdf5`.
The corresponding seven-row user-visible filter was:

```json
[["groundloop",65280744,"groundloop"],
 ["groundloop_d24_requirement_6ee04557df8e4c42adfaceae86e99635",46562354,"groundloop"],
 ["groundloop_d24_requirement_7d74a56d26404e158ac326bda62a609f",46568432,"groundloop"],
 ["groundloop_d24_requirement_b70684d1bec743ff9fa8410cbd58f18d",46573737,"groundloop"],
 ["groundloop_m3_demo",440382,"groundloop"],
 ["groundloop_m4_incrementality_f8b9895166924e06a8a33773c58d9933",1880804,"groundloop"],
 ["public",2200,"pg_database_owner"]]
```

Its compact ordered-JSON SHA-256 is
`a525454fe2fb3bff215ea9795627186c2683ec499149a88a27a2f1c314082f2e`.
The complete ten-row inventory, not only the seven-row filter, controls
cleanup and baseline equality.

An earlier long PostgreSQL runner ended across a host/session interruption and
produced no final pytest report. **No pass is claimed for that interrupted
process.** On recovery the pinned local Compose container was stopped with
exit 255 (`OOM=false`, `dead=false`, restart count zero); no pytest/PostgreSQL
client process or port-5432 listener remained. After the explicitly authorized
local service start, the complete inventory contained one absent-from-baseline
delta:

```text
name:  groundloop_m5_recovery_da24409d153646b7ac4c05b8080d0e67
OID:   105106952
owner: groundloop
```

The exact name matched the UUID-disposable migration-016 fixture namespace.
It contained 62 tables, 119 indexes, 3 sequences, 6 views, 18 functions, and
142 types, with no session and no lock. After explicit coordinator
authorization, a name/OID/owner/session/lock guard dropped only that exact
schema. It was disposable test residue and is not recoverable. No pre-existing
schema was removed or changed. A fresh read-only audit then matched both
baseline row sets and hashes exactly, with no session, lock, or unsafe orphan,
before the recovered test DB window started.

## 7. Exact live executable evidence

Every live command used the guarded local target, import isolation,
`PYTHONDONTWRITEBYTECODE=1`, `PYTHONPATH=src:.`, `-o addopts=''`,
`-p no:cacheprovider`, `--import-mode=importlib`, `-q`, and `-ra`. Gates ran
serially with a fresh pre/post read-only inventory and exact baseline equality.

Earlier completed gates on the superseded `e3b6333f...` test-file pin are
retained as historical evidence only; they do not certify the stricter
`00f39086...` bytes:

| Gate | Exact result | Duration | Post-gate inventory |
|---|---:|---:|---|
| `test_recovery.py -k 'm54_04_'` | 40/40 passed | 74.70s | complete SHA `9521c8ef...`; exact rows |
| all `test_recovery.py` | 122/122 passed | 209.34s | complete SHA `9521c8ef...`; exact rows |
| all `d24_requirement` | 171/171 passed | 302.65s | complete SHA `9521c8ef...`; exact rows |
| `test_migration_016.py` | 200/200 passed | 273.93s | complete SHA `9521c8ef...`; exact rows |

After the interrupted process was discarded as a non-result and its exact
orphan was safely removed, the broad PostgreSQL gate was rerun from the
beginning on that same superseded pin:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:. \
python -m pytest \
  -o addopts='' -p no:cacheprovider --import-mode=importlib -q -ra \
  tests/m5/postgres_runtime
```

Result: **797/797 passed in 1,254.49s (20:54)**; wrapper wall time was 1,256s
and the process exited 0. Its complete pre- and post-command inventories both
matched the exact ten-row baseline and SHA `9521c8ef...`; the seven-row filter
also matched SHA `a525454f...`. No session, lock, or new orphan remained. The
sole guarded DB window was explicitly ended and released after that audit.
The local Compose service remained running after that earlier broad gate. Its
identity and baseline were freshly guarded again for the post-rebase focused
gate below.

### 7.1 Prior post-rebase focused gate on superseded bytes

After the combined static and collection gates passed, the coordinator
authorized exactly one fresh focused DB window. An initial preflight stopped
before any database connection because `GROUNDLOOP_TEST_DATABASE_URL` was not
already exported. The lane then explicitly derived that test-only variable
from the repository-local `.env` GroundLoop URL, cleared the generic URL and
all target-changing libpq variables for the child processes, and reran the
complete target guard without printing credentials.

The successful pre-gate guard revalidated:

```text
Docker socket:    unix:///var/run/docker.sock
Compose container c9edea136042f3a032725af4c80049ee99e56012c33b62ab0a7044b0266b9def
container state:  running, OOM=false, dead=false, restart count 0
published port:   0.0.0.0:5432
sanitized target: localhost:5432/groundloop
database identity: groundloop, OID 16384, PostgreSQL server 160014
other sessions:   0
other-session locks: 0
```

The complete ten-row and filtered seven-row inventories matched the exact rows
in Section 6 and SHA-256 values `9521c8ef...` and `a525454f...`. With the DB
window explicitly announced, the lane ran only:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:. \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider --import-mode=importlib -q -ra \
  tests/m5/postgres_runtime/d24_requirement/test_recovery.py \
  -k 'm54_04_'
```

Historical result on `e3b6333f...`: **40/40 passed, 82 deselected in 72.84s
(1:12)**; wrapper wall time was 73.53s and the process exited 0. The post-gate
read-only audit matched the
same database identity, exact complete and filtered inventories, and both
hashes. It found zero other sessions, zero other-session locks, and no orphan
delta, so no cleanup was required. The DB window was explicitly ended and
released. The later audit HOLD means this result is not the final focused gate.

### 7.2 Completed corrected audit-remediation rerun

On exact final `1fb910ec...` bytes, the inactive-root node closes the detailed
stage/barrier work and inner/outer timing evidence described in Section 3.2.
The nonexpired-preterminal node now proves full rollback at
`late_return_accounting_inserted`, retries through a fresh connection, checks
self-consistent changed output plus independent disposition/work/timing
conflicts, and replays unchanged after later terminalization. Production
source remains untouched.

The combined static/diff/collection gates in Section 5 pass on these bytes.
After two fresh read-only audits approved the corrected pin, the sole
authorized focused live rerun passed **40/40, with 82 deselected, in 72.56s**
(wrapper 73.24s, exit 0). Its pre- and post-command identities were both
`groundloop`, OID `16384`, PostgreSQL server `160014`; both exact complete and
filtered inventories matched Section 6 and SHA-256 values `9521c8ef...` and
`a525454f...`. Zero other sessions, zero other-session locks, and no orphan
delta remained. The DB window was explicitly ended. No broader DB suite was
run on the corrected bytes.

The superseded passing live gates did not skip, xfail, time out, require a
network or model provider, or depend on execution order. The audit-remediation
non-pass is disclosed in Section 8 and is not evidence for the corrected
bytes. The repository-wide complete suite is intentionally not claimed; it
remains an integration gate under the activation.

Benchmark/data commands: **not applicable**. This lane adds test evidence only;
it downloaded no dataset, loaded no model, and ran no performance benchmark.
The measured pytest durations above are validation wall times, not runtime
performance evidence.

## 8. Superseded test-only failures and corrections

Before the final pin and final successful runs, one focused attempt reported
**36 passed / 4 failed in 76.03s**. One failure came from an ambiguous column
in a diagnostic assertion query. Three came from test expectations that read
the inactive attempt-result's post-write job state as `RUNNING`; the durable
contract requires `COMPLETED_INACTIVE`. The query and expectations were fixed
in this owned test file only.

Earlier root-specific oracle checks also exposed two test defects: the PENDING
projection compared the wrong revision slot, and one frontier diagnostic used
the wrong `epoch_id` column. Both oracles were corrected in this owned test
file only. None of these superseded assertion/query defects required or
authorized a production-source change.

No historical test failure is hidden by a skip, xfail, retry waiver, relaxed
assertion, or source edit. The earlier focused and broad results above share
the superseded `e3b6333f...` pin and remain historical only. The final
`1fb910ec...` focused 40/40 result is recorded in Section 7.2.

The first authorized focused run on the audit-remediation candidate produced
**39 passed / 1 failed / 82 deselected in 71.97s** (wrapper 72.67s, exit 1).
The sole failure was an exact test-oracle shape mismatch: the pending timing
assertion expected the barrier contribution-key digest, while
`_m54_04_timing_accumulator` did not select
`pending_contribution_key_digest`. Production source was not implicated.
Independent adjudication required retaining the exact digest assertion and
adding that missing projection column, plus matching settled-tail `NULL`s and
the exact verifier-completion pending key. The failed run's post-audit restored
both exact inventory hashes, with zero other sessions, zero other-session
locks, and no orphan delta. It is a recorded non-pass, not evidence for the
corrected bytes.

## 9. Protected dirt and forbidden-path verification

The coordinator-protected main-checkout paths remained byte-identical to the
activation pins when rechecked from this lane:

```text
2af4b19962dc8a7d22e377be17f342530a06ee6395bbbf2a092eab36599c8fc2  pyproject.toml
45c20ca46e9ad5bcd86b22c0d8882d1d611497f57ca8c45d3dea149260c110cd  docs/presentations/groundloop_fyp_professor_feedback.pdf
59a13cd8d4bbb017e712c0f39e70f2eba136557e945f64f1b1fc3891742a79f0  docs/presentations/groundloop_fyp_professor_feedback_v2.pdf
c12929c349a5c0be9793159143b09da40ea2a0b27df37b92d61d9ed6483d8c2a  docs/presentations/render_groundloop_fyp_professor_deck.py
167d1e7df5a720041fe0ff51879d08357f0dfbe3a7781ffaa08c0d958a47aa94  docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT_DRAFT.md
```

The lane did not read from or write to a production/shared database. It added
no TODO, FIXME, credential, literal DSN, generated data, model weight, cache,
or build artifact. At the audit-remediation static/collection checkpoint, the
index was empty and the worktree changes were exactly the two Lane B paths.
The resulting Lane A-parent delta remains exactly those two paths, while its
activation-base delta remains exactly the combined five paths.

## 10. Requested finalization and integration action

The ordered rebase onto exact Lane A commit `b9d251...` and the combined
static/diff/collection gates are complete. Do not rebase, cherry-pick, squash,
merge, or rewrite Lane A again.

The corrected focused `test_recovery.py -k 'm54_04_'` database window has
passed with complete and filtered inventory equality. The two-path commit may
now be amended. Then externally pin the final containing commit,
its sole Lane A parent, tree, both Lane B file identities, combined five-path
ledger, clean index/worktree, and exact parent/activation manifests. Then
obtain both required independent same-byte audits. Only after both audits have
no unresolved P0/P1 may the coordinator fast-forward the integration branch
to final Lane B and execute the remaining combined/repository-wide/main gates
in activation order.

The required final ancestry is activation -> Lane A -> Lane B. This handoff
grants no production-source edit, status-document edit, M5-D25 promotion,
acceptance-row promotion, main fast-forward, deployment, or publication claim.
