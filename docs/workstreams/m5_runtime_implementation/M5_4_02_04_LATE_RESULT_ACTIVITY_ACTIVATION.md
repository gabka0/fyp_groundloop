# M5.4-02/-03/-04 Late-Result Activity Activation

Status: coordinator activation candidate; no implementation ownership or
stage-row promotion is granted until this document and its non-authoritative
launch wrapper are committed on `main` and the resulting full activation
commit is independently audited

Date: 2026-08-28

Planning parent: `2513253f99dd5a0e5a8750b8ba4ebbe7d9a3b8cc`

Planning-parent tree: `112d771d20e83458f3580144174437d0e4cc305c`

## Authority and supersession

Before acting, read `AGENTS.md` and its complete required document sequence.
For this tranche, reread these current authorities completely and in order:

1. `docs/m5_design_freeze.md`
2. `docs/m5_implementation_plan.md`
3. `docs/m5_multiagent_execution_plan.md`
4. `docs/m5_acceptance_matrix.md`
5. `docs/m5_implementation_status.md`
6. `docs/workstreams/m5_runtime_contract/CANDIDATE_RUNTIME_ADDENDUM.md`
7. `docs/workstreams/m5_runtime_contract/RECOVERY_WORK_AMENDMENT.md`
8. `docs/workstreams/m5_runtime_contract/EXECUTION_DISPOSITION_RECEIPT_CORRECTION.md`
9. `docs/workstreams/m5_runtime_contract/LEGACY_TERMINAL_COVERAGE_CORRECTION.md`
10. `docs/workstreams/m5_runtime_contract/CANCELLATION_EXPIRED_OUTPUT_CORRECTION.md`
11. `docs/workstreams/m5_runtime_contract/TERMINAL_SUCCESSOR_EXPIRED_OUTPUT_CORRECTION.md`
12. `docs/workstreams/m5_runtime_contract/TERMINAL_RACE_INVOCATION_WORK_CORRECTION.md`
13. `docs/workstreams/m5_runtime_contract/ACTIVE_TERMINAL_CUTOFF_INVOCATION_WORK_COMPLETION_CORRECTION.md`
14. `docs/workstreams/m5_runtime_contract/DIRECT_ACQUISITION_TERMINAL_CUTOFF_CORRECTION.md`
15. `docs/workstreams/m5_runtime_implementation/D24_R2E_POSTGRES_TYPED_DIRECT_BRIDGE_HANDOFF.md`
16. this activation

The design freeze controls frozen semantics. The runtime addendum controls the
M5.4 byte-total runtime and falsifier matrix. Accepted D24 corrections control
the operational surfaces they supersede. Historical activations/handoffs are
evidence only. The protected persisted-matching draft is not authority.

## 1. Goal

Finish the **primary GroundLoop M5 runtime** before starting dissertation
packaging, a dashboard, or another model-training study.

The next immediate gate is deliberately smaller than all of M5.4. It will:

1. qualify the existing deterministic evidence for M5.4-02;
2. harden and qualify the no-parent-refutation evidence for M5.4-03; and
3. implement the missing maintained late-activity falsifiers required for
   M5.4-04.

This is the next acceptance-ordered deterministic evidence tranche. It does
not authorize M5-D25, migration 017, production sealing/publication, deployed
providers, or runtime activation.

The larger primary-runtime sequence is:

```text
M5.4-02/-03 evidence hardening and M5.4-04 late-activity closure
    -> M5-D25 contract acceptance
    -> migration 017 persisted matching and active verifier completion
    -> production combined seal/publication and lifecycle heads
    -> provider adapters and guarded production application composition
    -> separately authorized `v1_only -> m5_active` activation/race gate
    -> reconnect/crash/three-oracle maintained PostgreSQL history
    -> bounded pinned-model diagnostic
```

Each arrow is a contract and integration barrier. A later phase cannot borrow
the earlier phase's path ownership.

## 2. Current evidence and the actual gap

The current status is intentionally conservative:

- M5.4-01 is `PASS`;
- M5.4-02 through M5.4-09 remain `PENDING`;
- M5.0-24 remains contract-`PASS` / implementation-`PENDING`;
- the bounded R2e PostgreSQL typed-direct pre-seal bridge is integrated; and
- runtime mode remains `v1_only` outside isolated tests.

Current pure tests already substantively exercise much of M5.4-02 and
M5.4-03:

- forward and reverse requirement retrieval;
- overlapping-pair deduplication;
- verifier execution and observation creation;
- exact withdrawal and fallback;
- empty and short scopes;
- retry, terminal failure, replay, and fake sealing; and
- REFUTE and NEUTRAL outcomes with parent `refute_count == 0`.

Those cases were not accepted as row-closing evidence. They are spread across
isolated histories, the fake post-seal audit compares structured states but
not the complete certificate image, discovery emits only vector-channel hits,
and the REFUTE/NEUTRAL test could pass without proving that both observations
were retained in the fake observation repository.

M5.4-04 has a genuine falsifier gap. The existing pure classifier proves the
activity precedence, and the PostgreSQL recovery suite proves several late and
inactive fragments. The current fake late helper, however, only appends an
audit tuple. There is no one maintained history proving all of the following
together:

- activity is classified from epoch, requirement, group, and chunk state at
  the serialized return boundary;
- precedence is
  `EPOCH_FAILED > SUBJECT_INACTIVE > CHUNK_INACTIVE > JOB_ALREADY_TERMINAL`;
- a still-running inactive job becomes `COMPLETED_INACTIVE` exactly once;
- an already-cancelled job receives only an inert late-attempt archive;
- prior currency, edges, reference matching state, and certificates do not
  change; PENDING changes only by the required one-time decrement for a still-
  running inactive completion and remains unchanged for an already-cancelled
  late return;
- exact replay is a zero-write no-op; and
- a conflicting replay rolls back.

## 3. Immediate gate: M5.4-02/-03/-04 late-result activity evidence

### 3.1 Required outcome for M5.4-02

First map every runtime-addendum Section 18.2 clause to the existing test nodes
and helpers. Then run one deterministic, sequential fake-provider history
through `M5TypedApplication.run_event`. It must cover the complete accepted
fake-port history, not merely call lower-level helpers:

1. new-requirement forward top-k plus inserted-chunk reverse discovery, with an
   overlapping pair deduplicated before closure;
2. requirement SUPPORT creating a complete group without direct support;
3. requirement REFUTE and NEUTRAL creating no parent refutation;
4. alternative-witness repair;
5. nonfinal matching-edge loss with no requirement zero crossing;
6. final assignment loss and a surviving alternative group;
7. direct support surviving group loss and direct refutation conflicting with
   group support;
8. deletion fallback with empty success, short success, temporary
   unavailability, retry, and terminal failure;
9. optional-owner PENDING and multiple-owner multiplicity;
10. cancellation followed by a stale attempt after a later replacement;
11. failed-epoch late completion; and
12. reconnect replay with zero external calls.

The history must also preserve exact external-call order with no
provider/model work inside a transaction. For item 12, reconnect means a fresh
`M5TypedApplication` and fresh port facade over the same retained fake
world/store; invoking the same application object twice is not reconnect
evidence. The existing frontier suite remains the vector/lexical fusion,
fixed-budget, permutation, and least-root evidence.

At every successful seal, compare the complete structured state **and** the
group/claim certificate image with an independently recomputed Python oracle.
Compose the current scenarios and add only missing certificate,
observation-retention, and checkpoint assertions. Do not copy or reimplement
the fake coordinator or present deterministic retrieval as production quality.
Name the consolidated acceptance node
`test_m54_02_03_sequential_fake_history_is_exact`.

### 3.2 Required outcome for M5.4-03

In the maintained fake history, commit exact requirement-subject REFUTE and
NEUTRAL observations and prove all of the following:

- both observations exist with the expected task, subject, chunk, scores, and
  currency eligibility;
- neither becomes a SUPPORT witness;
- neither increments the owner claim's direct `refute_count`;
- no parent-refutation edge, certificate input, or status delta is created;
- direct claim-subject REFUTE still refutes normally in the same independent
  oracle; and
- exact replay performs zero provider calls and changes no state or
  certificate.

This is a grounding-relative claim only. It does not establish objective
truth or neural quality.

### 3.3 Required outcome for M5.4-04

Add a maintained pure/live matrix for stale and inactive requirement returns.
Retain the existing 16-case pure classifier over the four activity booleans.
Define the accepted live eight-row table over:

```text
epoch_active
subject_active = requirement_active AND group_active
chunk_active
```

Realize `subject_active=false` with at least one requirement-inactive case and
one independently checked group-inactive case. Map every row to its applicable
root/verifier return, exact disposition, archive reason, and allowed write set.
The all-active verifier row retains the current D25 fail-closed, zero-write
rejection; this tranche must not demand successful live active completion.
Name new live nodes with the stable prefix `test_m54_04_` so the focused gate
is replayable before the complete suite.

The matrix must include:

- active epoch and subject with inactive chunk;
- inactive requirement and inactive group, independently where the schema
  permits and jointly where lifecycle constraints require it;
- a stale worker from a failed prior epoch;
- a stale worker from a cancelled prior structural epoch/job after a later
  group successor or retirement, not merely an expired attempt with a retry
  successor;
- a still-running job completing inactive; and
- an already-cancelled job returning late.

For each applicable case, assert the exact activity snapshot, archive reason,
job/scope state, attempt-result disposition, event revision, confirmed work,
timing, and PENDING multiplicity. Snapshot before and after all semantic
surfaces: observation currency, candidate/admitted edges, the available
reference/derived matching image, group and claim certificates, claim/answer
state, publication identity, and head rows. There are no authorized persisted
matching rows before D25/migration 017.

The three job-shape write laws are distinct:

- An inactive verifier first write is permitted only in an active epoch with
  an inactive applicable requirement, group, or chunk. It performs
  `RUNNING -> COMPLETED_INACTIVE`, persists the exact ineligible verifier
  execution/observation and attempt-result/closure artifacts, accounts work
  and timing, advances the revision once, and decrements open work/PENDING
  once. It changes no currency, edge, matching, certificate, semantic state,
  publication, or head.
- An inactive forward/reverse root uses two exact CAS steps. First,
  `ROOT_RESULT_STAGED` persists the validated channel hits, selections,
  discovery/result and attempt artifacts, accounts work/timing, moves the
  scope `OPEN -> RESULT_STAGED`, keeps the root `RUNNING`, advances the
  revision once, and leaves PENDING unchanged. The later event-wide barrier
  contributes that root's canonical empty inactive selection, installs its
  closure/completion and PENDING projection, and advances once. It creates no
  semantic observation and no admitted pair, verifier child, or frontier head
  attributable to that inactive root. Active roots in the same mixed barrier
  may still make their independently authorized admitted/child/head writes,
  so attribution—not a blanket whole-transaction absence check—is required.
- Audit-only returns use four exact archive shapes, selected by durable attempt
  currency and event terminality:
  - a nonexpired preterminal current attempt inserts execution evidence, its
    ordinary `terminal_audit_only` attempt-result artifact, and exact
    preterminal work/timing contributions; it updates the event accumulators
    at the unchanged revision. A cancellation with `reason=epoch_failed` must
    instead wait for outer terminalization;
  - an expired preterminal attempt is archivable only while its dense
    successor remains `RUNNING`. It inserts execution evidence, the
    expired-return sidecar, its matching
    `terminal_audit_only/attempt_expired` `RUNNING -> RUNNING` artifact, and
    preterminal work/timing contributions at the unchanged revision. The
    expired attempt column retains its required NULL output digest and the
    immutable expired-return sidecar supplies the output binding. If the
    nonterminal successor is `RETRYABLE_FAILED` or terminal (including
    `CANCELLED`), the call conflicts with zero writes;
  - a nonexpired postterminal return inserts execution evidence, its ordinary
    `terminal_audit_only` attempt-result, postterminal timing, and the general
    audit sidecar, with no event contribution or accumulator change; and
  - an expired postterminal return inserts the exact five-row archive:
    execution evidence, expired-return, matching
    `terminal_audit_only/attempt_expired` attempt-result with the exact
    terminal successor state, postterminal timing, and general audit. It also
    makes no event contribution or accumulator change.
  Every receipt disposition and artifact identity is bound to the selected
  branch. None creates a normal discovery/verifier semantic artifact. Every
  legal branch preserves revision, job, scope, open work, PENDING, currency,
  edges, matching, certificates, semantic state, result/publication, and
  heads. Exact replay is zero-write; any changed output, evidence, work,
  timing, disposition, job image, or attribution conflicts atomically.

A failed prior epoch must already expose the cancelled terminal audit-only
branch with `EPOCH_FAILED`; fixture setup must never manufacture a failed epoch
that still has a running job. `JOB_ALREADY_TERMINAL` is the lowest-precedence
reason only when the epoch and every applicable subject/chunk predicate remain
active. Higher inactive reasons still win for a terminal return.

For every cancelled late return, assert that `cancelled_by_event_id`,
`cancelled_by_epoch_id`, and `cancellation_reason` are jointly present and bind
the original cancelling event, epoch, and reason. Those identities must remain
stable after a later structural group successor, replacement, or retirement,
while the activity flags come from the later serialized snapshot.

Fixture SQL may establish a schema-valid inactive precondition when no
accepted public transition exists. It must not manufacture the attempt-result,
observation, currency, edge, matching, certificate, PENDING, publication, or
head outcome under test.

First write, exact replay, changed-input conflict, transaction cutoff rollback,
and reconnect must all be explicit. The test must distinguish:

- verifier `RUNNING -> COMPLETED_INACTIVE`, which decrements open work once;
- root result staging, which leaves the root/PENDING open until the separate
  canonical inactive barrier closure decrements them once;
- all four nonexpired/expired and preterminal/postterminal audit shapes, with
  their exact artifact sets and accumulator boundaries; and
- an expired-attempt/terminal-successor preterminal call, which conflicts with
  a complete zero-write snapshot until legal postterminal archival.

Live reconnect evidence means a fresh database connection and a new
`PostgresM5RuntimeStore` over the same durable projection, followed by a
terminal/non-executing acquisition (`should_execute=False`) with no redispatch.
A store-level test must not claim that an outer provider callback was
suppressed; that end-to-end claim belongs to Lane A or a later application
gate.

Lane A retains deterministic fake sealing for oracle comparison. No active
PostgreSQL verifier completion, persisted matching publication, or production
PostgreSQL seal is authorized; those remain behind M5-D25.

## 4. Exact technical path manifest

The coordinator activation commit must have planning parent
`2513253f99dd5a0e5a8750b8ba4ebbe7d9a3b8cc` as its sole parent and change
exactly
`docs/workstreams/m5_runtime_implementation/M5_4_02_04_LATE_RESULT_ACTIVITY_ACTIVATION.md`
and
`docs/workstreams/m5_runtime_implementation/M5_4_02_04_LATE_RESULT_ACTIVITY_AGENT_PROMPT.md`.
If `main` moves before that commit, stop and repin/re-audit the planning parent
rather than silently updating the claim.

This file cannot embed the commit ID or its own final SHA-256. The coordinator
and independent activation auditor must externally report the full activation
commit, parent, tree, both file hashes, and exact two-path delta before any lane
starts.

After that full activation commit is independently audited, the coordinator
creates both fresh path-exclusive implementation branches/worktrees and the
fresh clean integration branch/worktree from it. All three branch names and
worktree paths below must be absent before creation; the integration worktree
remains clean until Lane A integration.
The combined technical candidate may change exactly five paths:

1. `tests/m5/runtime/fake_ports.py`
2. `tests/m5/runtime/test_typed_history.py`
3. `docs/workstreams/m5_runtime_implementation/M5_4_02_03_FAKE_HISTORY_HANDOFF.md`
   (new)
4. `tests/m5/postgres_runtime/d24_requirement/test_recovery.py`
5. `docs/workstreams/m5_runtime_implementation/M5_4_04_POSTGRES_ACTIVITY_HANDOFF.md`
   (new)

Suggested disjoint ownership:

- **Lane A — deterministic history and pure late-activity half:** paths 1
  through 3;
- **Lane B — PostgreSQL activity matrix:** paths 4 and 5;
- **Coordinator — activation-base ownership, integration, gate execution, and
  later status reconciliation:** no candidate path; and
- **independent audit:** obtain two read-only same-byte audits, sequentially if
  concurrency is unavailable.

Required identities:

```text
Lane A branch:   workstream/m5-4-02-03-fake-history
Lane A worktree: /home/kassym/Desktop/groundloop-worktrees/m5-4-02-03-fake-history
Lane B branch:   workstream/m5-4-04-postgres-activity
Lane B worktree: /home/kassym/Desktop/groundloop-worktrees/m5-4-04-postgres-activity
Integration branch: integration/m5-4-02-04-late-result-activity
Integration worktree: /home/kassym/Desktop/groundloop-worktrees/m5-4-02-04-late-result-activity
```

Both implementation lanes begin from the exact activation commit. They may
author disjoint paths concurrently, but integration is ordered. Lane B's
pre-rebase commit and handoff are explicitly provisional. After Lane A passes
its pure gate and same-byte audit, fast-forward the integration branch to Lane
A. Rebase Lane B's disjoint work onto that exact integration head, update its
handoff with the post-rebase parent and evidence, rerun its static/focused live
gates, create its final two-path commit, and only then freeze and audit Lane B.
Fast-forward the integration branch to that final Lane B commit. No merge
commit, squash, or cherry-pick is allowed. No history rewrite is allowed after
a lane's final audited pin.

The mechanically verified final ancestry must be exactly:

```text
audited activation commit
    -> Lane A commit (sole parent activation; exactly paths 1-3)
    -> Lane B commit (sole parent Lane A; exactly paths 4-5)
```

The two literal activation files named above are coordinator-owned base
history and are read-only to every technical lane.

No production source path is pre-authorized because current source inspection
indicates the production activity classifier and inactive/late persistence
paths already implement the frozen precedence. If a new falsifier exposes a
source defect, the lane must stop with:

- the exact failing node and durable before/after snapshot;
- the authoritative contract anchor;
- the minimal proposed source/test manifest; and
- an independent P0/P1 adjudication.

Only a new committed correction activation may then authorize source edits.
Do not opportunistically edit `application.py`, `frontier.py`,
`postgres_roots.py`, `postgres_verifier.py`, contracts, digests, migrations,
or schema SQL.

## 5. Validation gates

All commands run from the fresh candidate worktree with external/no caches.
Exact counts are collected and recorded from the frozen candidate rather than
predicted in advance.

### 5.1 Static and pure gates

```bash
test -n "${ACTIVATION_BASE:-}" || {
  echo "ACTIVATION_BASE must be the externally audited full activation commit" >&2
  exit 1
}
test "$(git rev-parse "${ACTIVATION_BASE}^{commit}")" = "$ACTIVATION_BASE"
git merge-base --is-ancestor "$ACTIVATION_BASE" HEAD

git diff --check "$ACTIVATION_BASE" -- \
  tests/m5/runtime/fake_ports.py \
  tests/m5/runtime/test_typed_history.py \
  docs/workstreams/m5_runtime_implementation/M5_4_02_03_FAKE_HISTORY_HANDOFF.md \
  tests/m5/postgres_runtime/d24_requirement/test_recovery.py \
  docs/workstreams/m5_runtime_implementation/M5_4_04_POSTGRES_ACTIVITY_HANDOFF.md

M54_RUFF_CACHE=$(mktemp -d)
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

M54_PURE_MYPY_CACHE=$(mktemp -d)
MYPY_CACHE_DIR="$M54_PURE_MYPY_CACHE" MYPYPATH=src:tests PYTHONPATH=src \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m mypy \
  --strict --explicit-package-bases \
  tests/m5/runtime/fake_ports.py \
  tests/m5/runtime/test_typed_history.py
M54_PG_MYPY_CACHE=$(mktemp -d)
MYPY_CACHE_DIR="$M54_PG_MYPY_CACHE" MYPYPATH=src:. PYTHONPATH=src \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m mypy \
  --strict --explicit-package-bases \
  tests/m5/postgres_runtime/d24_requirement/test_recovery.py

M54_PYCACHE_PREFIX=$(mktemp -d)
PYTHONDONTWRITEBYTECODE=1 PYTHONPYCACHEPREFIX="$M54_PYCACHE_PREFIX" \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m compileall -q \
  tests/m5/runtime/fake_ports.py \
  tests/m5/runtime/test_typed_history.py \
  tests/m5/postgres_runtime/d24_requirement/test_recovery.py

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:. \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider -q \
  tests/m5/runtime/test_frontier.py \
  tests/m5/runtime/test_typed_history.py
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:. \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider -q tests/m5/runtime
```

The current pre-gate baseline for `test_frontier.py + test_typed_history.py` is
37/37. That number is context only and must increase if new parametrized
falsifiers are added. Ruff 0.15.22 currently proposes one mechanical one-line
format change in the owned `test_typed_history.py`; Lane A may apply it but must
record that formatting delta separately from semantic evidence.

### 5.2 Serialized PostgreSQL gates

Use only the repository's local test DSN and UUID disposable schemas. One
runner announces `DB window START` and `DB window END`; no second runner uses
PostgreSQL concurrently. Do not target the retained `groundloop` schema or any
remote/shared database. The guard must require an explicit
`GROUNDLOOP_TEST_DATABASE_URL`, verify without printing credentials that it
names the local Compose database `groundloop` on loopback port 5432, and clear
the generic GroundLoop URL and target-changing libpq environment variables for
every guard/test command so no fallback target can be used. The Compose check
must unset Docker context/TLS overrides and use the pinned local
`unix:///var/run/docker.sock`; a remote daemon is not acceptable evidence for
the local target.

Before announcing the DB window, use a separate `REPEATABLE READ`, `READ ONLY`
transaction to record the sanitized database name/OID, server identity, and
complete schema name/OID/owner inventory, then explicitly roll it back. Take a
fresh read-only pre- and post-command inventory for every PostgreSQL gate; the
first pre-command inventory is the baseline and every clean post-command
inventory must restore it. Register every remaining new/orphan schema delta by
exact name/OID/owner; do not claim an exhaustive registry of transient fixture
schemas that already tore down successfully. The immutable baseline and
observed current-run deltas control cleanup.

Run, in order:

1. the new activity/replay/conflict nodes only;
2. all of `tests/m5/postgres_runtime/d24_requirement/test_recovery.py`;
3. all of `tests/m5/postgres_runtime/d24_requirement`;
4. all of `tests/m5/postgres_runtime`; and
5. the repository's complete test suite on the final frozen bytes.

The existing broad commands are:

```bash
test -n "${GROUNDLOOP_TEST_DATABASE_URL:-}" || {
  echo "GROUNDLOOP_TEST_DATABASE_URL is required" >&2
  exit 1
}

M54_COMPOSE=(
  env
  -u DOCKER_HOST
  -u DOCKER_CONTEXT
  -u DOCKER_TLS_VERIFY
  -u DOCKER_CERT_PATH
  docker --host unix:///var/run/docker.sock compose -p groundloop
  -f /home/kassym/Desktop/groundloop/docker-compose.yml
)
M54_DB_CONTAINER=$("${M54_COMPOSE[@]}" ps --status running -q db)
test -n "$M54_DB_CONTAINER" || {
  echo "the repository-local Compose db service must be running" >&2
  exit 1
}
M54_COMPOSE_PORT=$("${M54_COMPOSE[@]}" port db 5432)
case "$M54_COMPOSE_PORT" in
  0.0.0.0:5432 | 127.0.0.1:5432 | '[::]:5432' | ':::5432') ;;
  *)
    echo "the pinned Compose db service must publish local port 5432" >&2
    exit 1
    ;;
esac
printf 'sanitized Docker target: %s container=%s port=%s\n' \
  'unix:///var/run/docker.sock' "$M54_DB_CONTAINER" "$M54_COMPOSE_PORT"

m54_clean_env() {
  env \
    -u GROUNDLOOP_DATABASE_URL \
    -u GROUNDLOOP_TEST_DATABASE_URL \
    -u PGHOST \
    -u PGHOSTADDR \
    -u PGPORT \
    -u PGDATABASE \
    -u PGSERVICE \
    -u PGSERVICEFILE \
    -u PGOPTIONS \
    "$@"
}

M54_TEST_TARGET=$(
  m54_clean_env \
  GROUNDLOOP_TEST_DATABASE_URL="$GROUNDLOOP_TEST_DATABASE_URL" \
  /home/kassym/Desktop/groundloop/.venv/bin/python - <<'PY'
import os
from psycopg.conninfo import conninfo_to_dict

value = os.environ["GROUNDLOOP_TEST_DATABASE_URL"]
normalized = value.replace("postgresql+psycopg://", "postgresql://", 1)
resolved = conninfo_to_dict(normalized)
forbidden = {"hostaddr", "service", "servicefile", "options"}
present = sorted(forbidden.intersection(resolved))
if present:
    raise SystemExit(f"forbidden test DSN options: {','.join(present)}")
host = resolved.get("host", "")
port = resolved.get("port", "5432")
database = resolved.get("dbname", "")
if "," in host or "," in port:
    raise SystemExit("multi-host test DSNs are forbidden")
if host not in {"localhost", "127.0.0.1", "::1"}:
    raise SystemExit("test DSN must target loopback")
if port != "5432" or database != "groundloop":
    raise SystemExit("test DSN must target local Compose groundloop:5432")
print(f"{host}:{port}/{database}")
PY
) || exit 1
printf 'sanitized test target: %s\n' "$M54_TEST_TARGET"

m54_clean_env \
GROUNDLOOP_TEST_DATABASE_URL="$GROUNDLOOP_TEST_DATABASE_URL" \
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:. \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider --import-mode=importlib -q -ra \
  tests/m5/postgres_runtime/d24_requirement/test_recovery.py \
  -k 'm54_04_'
m54_clean_env \
GROUNDLOOP_TEST_DATABASE_URL="$GROUNDLOOP_TEST_DATABASE_URL" \
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:. \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider --import-mode=importlib -q -ra \
  tests/m5/postgres_runtime/d24_requirement/test_recovery.py
m54_clean_env \
GROUNDLOOP_TEST_DATABASE_URL="$GROUNDLOOP_TEST_DATABASE_URL" \
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:. \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider --import-mode=importlib -q -ra \
  tests/m5/postgres_runtime/d24_requirement
m54_clean_env \
GROUNDLOOP_TEST_DATABASE_URL="$GROUNDLOOP_TEST_DATABASE_URL" \
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:. \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider --import-mode=importlib -q -ra \
  tests/m5/postgres_runtime/test_migration_016.py
m54_clean_env \
GROUNDLOOP_TEST_DATABASE_URL="$GROUNDLOOP_TEST_DATABASE_URL" \
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:. \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider --import-mode=importlib -q -ra \
  tests/m5/postgres_runtime
m54_clean_env \
GROUNDLOOP_TEST_DATABASE_URL="$GROUNDLOOP_TEST_DATABASE_URL" \
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:. \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider --import-mode=importlib -q -ra

m54_clean_env \
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:. \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider -q -rs --tb=short \
  tests/m5/runtime/test_contracts.py::test_d24_public_m4_dto_and_api_signature_snapshot_is_unchanged \
  tests/m5/reference/test_legacy_regression.py \
  tests/m4/test_m4_contracts.py
```

Recollect and record the final count/duration for every command, including the
literal public-M4/legacy compatibility selection above.

Every current-run schema remaining after a gate must be torn down. Compare each
read-only schema name/OID/owner inventory with the baseline and drop only an
exact observed current-run UUID delta that was absent from the baseline, after
its identity/scope check. Never remove or alter any pre-existing schema,
including a retained disposable-looking fixture schema. Final baseline
equality proves cleanup. Record the complete before/after inventories,
observed orphan deltas, cleanup, and explicit DB-window release.

### 5.3 Compatibility and evidence gates

The final candidate must additionally prove:

- the exact five-path manifest and an empty index;
- no new skip, xfail, TODO, FIXME, credential value, or hard-coded DSN value;
- no M4-v1 contract/digest/public route change;
- no migration/schema/package-export/status-document change;
- no model or provider network call;
- unchanged protected user-owned dirt; and
- two independent same-byte audits with no unresolved P0/P1.

Each lane handoff pins every non-self owned file. A file cannot embed its own
final SHA-256 or the commit/tree that contains itself. Each handoff must state
that limitation. After freeze and commit, the coordinator and both auditors
externally report both handoff hashes, all five path hashes/lines/bytes, the
aggregate ledger, and the candidate commit, parent, and tree. Stale
placeholders and claimed self-hash fixed points are forbidden.

Each handoff must also record the lane branch, worktree, exact base and head
(with self/commit pins explicitly external where impossible), owned files,
interface assumptions, exact test/static commands and results, benchmark/data
commands or an explicit not-applicable statement, limitations, failed
attempts, skips, environment dependencies, forbidden-path verification, and
the requested merge action. Lane B updates these fields after its pre-audit
rebase; a pre-rebase handoff is not final evidence.

After fast-forwarding `main` to the final two-lane candidate ancestry, rerun
the focused new selection on `main`. This technical activation ends after that
rerun and grants no status-document ownership. The branch/worktree/manifest
below is routing only for a separately authorized, separately audited
coordinator reconciliation; it is not a conditional grant in this activation.
Only that new authority may propose changing M5.4-02, M5.4-03, or M5.4-04
from `PENDING` to `PASS`. It should create branch
`workstream/m5-4-02-04-status-reconciliation` and worktree
`/home/kassym/Desktop/groundloop-worktrees/m5-4-02-04-status-reconciliation`
from the exact post-integration technical head. Its committed manifest is:

1. `docs/decision_log.md`;
2. `docs/m5_acceptance_matrix.md`;
3. `docs/m5_implementation_plan.md`;
4. `docs/m5_implementation_status.md`;
5. `docs/m5_multiagent_execution_plan.md`; and
6. `docs/roadmap.md`.

Audit that separate commit before a fast-forward to `main`; do not edit the
protected-dirty main checkout directly. No other acceptance row may move:
every M5.0 implementation half, M5.4-01, M5.4-05 through M5.4-09, and every
M5.5/M5.6 row stays byte-semantically unchanged.

## 6. Stop conditions

Stop without widening scope if any of the following occurs:

- an accepted contract is ambiguous or internally inconsistent for a tested
  durable state;
- a production source change is required;
- an active PostgreSQL verifier completion or production PostgreSQL seal
  reaches the M5-D25 barrier;
- a required case can only pass by mutating the non-authoritative D25 draft;
- a replay performs a write or provider call;
- a late return changes currency, edges, reference matching, certificates,
  publication, or heads, or changes job/revision/work/timing/PENDING beyond the
  exact allowed write set in Section 3.3;
- fixture SQL manufactures an attempt-result, observation, currency, edge,
  matching, certificate, PENDING, publication, or head outcome under test;
- a required test skips, xfails, times out, or depends on test order;
- the protected dirt set or any out-of-manifest path changes;
- any additional test, fixture, handoff, documentation, source, migration, or
  schema path is required but is not in the committed manifest; or
- an auditor reports an unresolved P0/P1.

## 7. Later primary-system phases

### Phase B — M5-D25 contract acceptance

M5-D25 is a separate contract gate. The current untracked draft is protected,
non-authoritative user work and must not be edited or adopted without explicit
permission. A future coordinator must pin the accepted migration-016 ledger
tuple literally, remove obsolete placeholders, obtain independent semantic
GO, and commit a path-exclusive contract manifest before any migration-017
implementation begins.

### Phase C — migration 017 and persisted matching

After D25 acceptance, implement and audit the cacheless persisted Hall-state
contract, current/working separation, active verifier completion, rollback,
replay, upgrade, and compatibility. This phase owns its own migration,
persistence, oracle, and test manifest.

### Phase D — production seal and publication

Implement combined sparse publication, activation/lifecycle heads, measured
seal, replay, and failure atomicity. This phase supplies the production
publication boundary needed by later maintained-runtime gates.

### Phase E — providers and application composition

Add explicitly versioned discovery, verifier, and measurement provider
implementations, then compose them through the guarded production application
route. Prove provider execution is outside database locks and follows the
accepted exactly-once/recovery boundary. This phase does not itself activate a
database or authorize deployment.

### Phase F — guarded runtime enablement and maintained history

Use the already implemented public activation/bootstrap contract; do not
reimplement it. First prove activation-vs-v1-open and typed-open races in
disposable schemas. Any real `v1_only -> m5_active` transition then requires a
separate coordinator activation plus explicit database-owner/user authority,
target identity/zero-history guard, and rollback/reconnect evidence. This
document grants no database activation or production-data mutation.

After that boundary is authorized and green, run strict reconnect, crash
recovery, and post-seal three-oracle maintained PostgreSQL histories. These
phases can close M5.4-05 through M5.4-07 and the M5.0-24 implementation half
only if every dependency passes.

### Phase G — bounded model diagnostic

Run the frozen-model diagnostic for M5.4-08 only after the deterministic
maintained runtime is green. Model output remains a versioned semantic
observation, not truth. M5.4-09 compatibility remains mandatory in every
phase.

Dashboard/API work, dissertation packaging, and a new model-accuracy study
come after the primary runtime is complete or are deliberately scheduled as
separate non-overlapping work.

## 8. Protected coordinator state

Before the two literal coordinator-owned activation files named in Section 4
were authored, `main` had an empty index and exactly five protected user-owned
dirt paths. Those two files are the only additional coordinator-owned paths;
the five paths below remain outside every gate in this activation:

```text
2af4b19962dc8a7d22e377be17f342530a06ee6395bbbf2a092eab36599c8fc2  pyproject.toml
45c20ca46e9ad5bcd86b22c0d8882d1d611497f57ca8c45d3dea149260c110cd  docs/presentations/groundloop_fyp_professor_feedback.pdf
59a13cd8d4bbb017e712c0f39e70f2eba136557e945f64f1b1fc3891742a79f0  docs/presentations/groundloop_fyp_professor_feedback_v2.pdf
c12929c349a5c0be9793159143b09da40ea2a0b27df37b92d61d9ed6483d8c2a  docs/presentations/render_groundloop_fyp_professor_deck.py
167d1e7df5a720041fe0ff51879d08357f0dfbe3a7781ffaa08c0d958a47aa94  docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT_DRAFT.md
```

Never reset, clean, stash, reformat, stage, commit, copy into a lane, or delete
those paths as part of this activation.
