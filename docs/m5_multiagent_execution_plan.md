# GroundLoop M5 Multi-Agent Execution Plan

Status: frozen M5 ownership contract; Wave 1 active

Date: 2026-08-02

Authority: `docs/m5_design_freeze.md` defines semantics and
`docs/m5_implementation_plan.md` defines gates. This document defines only
ownership, integration order, and evidence required from parallel lanes.

## 1. Non-negotiable execution rule

M5 uses at most three active implementation lanes, including the coordinator.
Parallel work begins only after M5.0 is frozen and only where paths and input
contracts are disjoint. An agent may read any path but may edit only its owned
paths. A needed cross-lane change becomes a written proposal to the
coordinator; it is not implemented opportunistically.

Every implementation lane uses a dedicated Git branch and worktree rooted
outside the main checkout. The main checkout is the integration worktree and
contains pre-existing user presentation changes that no M5 lane owns.

No lane may claim completion from its local tests alone. The coordinator owns
merge order, conflict resolution, full-suite validation, live PostgreSQL
validation, research claims, and milestone status.

## 2. Shared contract barrier

The coordinator exclusively owns these shared contracts for all of M5:

- `AGENTS.md`
- `README.md`
- `pyproject.toml`
- `src/groundloop/domain.py`
- `src/groundloop/repository.py`
- `src/groundloop/reference.py`
- `src/groundloop/events.py`
- `src/groundloop/incremental.py`
- `src/groundloop/differential.py`
- `src/groundloop/cli.py`
- `src/groundloop/postgres/snapshot.py`
- every file under `migrations/`
- `docs/technical_design.md`
- `docs/decision_log.md`
- `docs/roadmap.md`
- all top-level M5 contract/status documents

The coordinator also owns creation of `src/groundloop/m5/__init__.py` and any
shared public protocol imported by multiple lanes. Lane-local modules depend
on those interfaces; they do not redefine them.

## 3. Lane A -- bounded matching kernel and proof artifacts

Branch/worktree:

```text
branch:   workstream/m5-matching
worktree: /home/kassym/Desktop/groundloop-worktrees/m5-matching
```

Owned paths:

- `src/groundloop/m5/matching.py`
- `tests/m5/matching/`
- `docs/workstreams/m5_matching/`

Forbidden edits include all shared contracts, PostgreSQL code, M4 code,
evaluation code, and dependency configuration.

Inputs frozen by the coordinator:

- requirements use dense ordinals `0..r-1`, with `1 <= r <= 8`;
- an active distinct edge is `(requirement_ordinal, text_hash)`;
- edge multiplicity is maintained outside the kernel;
- one coalesced kernel update is a text hash moving from `old_mask` to
  `new_mask`;
- completeness means a matching that covers every requirement;
- certificate inputs expose ordered concrete hashes per mask, ordered active
  observation IDs per edge, the prior immutable certificate artifact, exact
  epoch/revision binding, and policy identity.

Deliverables:

1. deterministic affected-group augmenting-path baseline;
2. exact Hall-mask state with initialization and mask transitions;
3. deficiency-derived maximum matching size and completeness;
4. deterministic stateful certificate-artifact construction/validation, local
   observation repair, zero-flip policy rebinding, selected-edge rebuild, and
   revision-binding transition results;
5. exhaustive small-graph comparison and randomized transition tests;
6. executable work counters and a proof handoff covering M5-T1/M5-T2.

Lane A must not import the Python full-state oracle or SQL oracle. Its baseline
may cross-check the optimized kernel but is not an independent GroundLoop
oracle.

## 4. Lane B -- PostgreSQL M5 adapter and independent SQL oracle

Branch/worktree:

```text
branch:   workstream/m5-postgres-oracle
worktree: /home/kassym/Desktop/groundloop-worktrees/m5-postgres-oracle
```

Owned paths after the coordinator lands migration 014 and the shared Python
contracts:

- `src/groundloop/postgres/m5.py`
- `sql/m5/` (including the bundle member
  `sql/m5/full_recompute_oracle.sql`)
- `tests/m5/postgres/`
- `docs/workstreams/m5_postgres_oracle/`

Forbidden edits include migrations, the generic snapshot adapter, core domain
and event files, matching code, M4 persistence, and dependency configuration.

Inputs frozen by the coordinator:

- names and columns of group, requirement, typed-subject, observation,
  working, published, and certificate relations;
- the ordered, atomically ledgered schema-plus-oracle bundle contract;
- revision-interval working currency, `eligible_for_currency`, group-only
  validity, durable retirement, and temporal duplicate constraints;
- Python snapshot records and loader order;
- one-to-eight requirement bound and lifecycle constraints;
- canonical policy and task-type semantics;
- expected result records for requirement, group, claim, answer, and
  certificate state.

Deliverables:

1. explicit-column M5 snapshot loading and state reading;
2. base-edge Hall-subset SQL oracle plus the capped unmatched-branch recursive
   `UNION` assignment cross-check with `H<=16`, `E<=128`, the frozen 100,000-
   state preflight, and explicit cap-exceeded output;
3. independent SQL policy, witness-edge, group, claim, and answer derivation;
4. certificate-validity and mismatch queries;
5. migration/backfill, rollback, and live-database tests;
6. query-plan and index evidence with limitations.

Lane B must not read Hall-mask materialized state to decide oracle truth and
must not port the Python backtracking or matching-kernel control flow into
SQL. SQL may be exponential in the fixed bound because it is an independent
correctness oracle.

## 5. Lane C -- controlled data adapter and evaluation harness

Branch/worktree:

```text
branch:   workstream/m5-controlled-evaluation
worktree: /home/kassym/Desktop/groundloop-worktrees/m5-controlled-evaluation
```

Owned paths after the coordinator freezes the evaluation-record protocol:

- `src/groundloop/m5/evaluation/`
- `tests/m5/evaluation/`
- `scripts/m5/`
- `configs/m5/`
- `docs/workstreams/m5_evaluation/`

Forbidden edits include core contracts, matching, PostgreSQL, M4, migrations,
dependency configuration, and all raw/generated dataset directories.

Inputs frozen by the coordinator:

- event-stream and state-result records;
- baseline interface and signed-work-counter names;
- official WiCE source revision, expected file hashes, and license rules;
- exact sentence-set-to-evidence-unit mapping;
- split/tuning prohibition and report schema.

Deliverables:

1. pinned, hash-checked WiCE controlled adapter;
2. eligibility, rejection, overlap, and SDR-distribution audit;
3. deterministic controlled dynamic histories;
4. the seven frozen baselines with their semantic-policy versus systems-
   comparator roles and cohort-specific UNAVAILABLE states preserved;
5. raw-count, denominator, clustered-interval, work, latency, and state-size
   reports;
6. separate source/human labels, controlled score projections, exact SDR
   states, and frozen-model diagnostic outputs;
7. a handoff that states every exclusion and unsupported claim.

Lane C may consume the public matching protocol but may not use M5 test labels
to select policies, thresholds, prompts, models, or implementation variants.
It does not commit downloaded data, generated event corpora, model weights, or
large result artifacts.

## 6. Coordinator lane

The coordinator performs work that crosses semantic boundaries:

- M5.0 freeze, decision log, and acceptance matrix;
- shared Python domain, history, events, reference oracle, and composition;
- migration 014 and typed-subject integrity;
- M4-compatible typed v2 runtime/job/publication integration;
- public imports and CLI composition;
- lane contract publication and merge order;
- three-oracle differential and failure-injection harnesses;
- full validation, final evaluation execution, and closure report.

The coordinator does not duplicate a lane's implementation while that lane is
active. If a lane blocks, it is stopped and its committed state is inspected
before ownership is reassigned.

## 7. Contract handoffs and waves

### Wave 0 -- M5.0 freeze

Only the coordinator edits. Independent agents perform read-only theory,
schema/runtime, and data audits. Exit requires no unresolved P0/P1 and a
falsifying acceptance row for every M5-D decision.

### Wave 1 -- pure semantics and matching

- Coordinator: M5.1 shared types, history, events, and independent Python
  oracle.
- Lane A: matching baseline, Hall-mask kernel, certificates, and proof tests.
- Lane B/C: read-only contract preparation; no implementation until their
  inputs are frozen.

Barrier: the Python oracle and structural-event suite pass, and Lane A passes
its exhaustive kernel gate. Coordinator integrates Lane A and runs differential
tests before Wave 2.

### Wave 2 -- persistence and controlled-data plumbing

- Coordinator first lands the migration-014 relational schema contract and M5
  combined engine/protocol, but does not claim or execute the atomic bundle yet.
- Lane B: snapshot adapter plus `sql/m5/full_recompute_oracle.sql` against that
  landed schema contract.
- Lane C: pinned adapter and evaluation mechanics against the landed event and
  report protocols.

Barrier: Lane B and Lane C rebase onto the same integration commit. SQL and
data audits pass independently. After Lane B integration, the coordinator
computes the exact two-file bundle digest, lands the transactional upgrader and
bundle-ledger tests, and only then may the schema bundle execute or M5.3 PASS.

### Wave 3 -- runtime integration

- Coordinator: typed v2 jobs, M4 composition, PENDING, sealing, replay, and
  failure atomicity.
- Lane A: only focused performance/proof falsification if requested.
- Lane B: only SQL/live-DB defect fixes within owned paths.
- Lane C: deterministic controlled histories and reports, not model tuning.

Barrier: deterministic fake-port end-to-end history passes, followed by an
out-of-band three-oracle audit, before any frozen-model diagnostic.

### Wave 4 -- evaluation and closure

No independent lane changes semantics. The coordinator freezes code, executes
the registered gates, records negative results, and updates status documents.
Defects reopen their owning wave; failed scientific results do not trigger
post-hoc threshold or dataset tuning.

## 8. Required lane handoff

Each lane writes a handoff under its owned workstream directory containing:

- branch, worktree, base and head commits;
- files changed;
- interface assumptions consumed;
- tests and exact results;
- lint/type/compile results for owned code;
- benchmark or data commands with seeds and hashes;
- limitations, failed attempts, skips, and environment dependencies;
- explicit statement that forbidden paths were not changed;
- recommended merge order and any coordinator action.

The lane then commits all intended source/test/doc changes and stops. Untracked
generated artifacts are removed or documented, never silently merged.

## 9. Merge order and collision checks

Default integration order:

1. coordinator shared M5.1 contracts;
2. Lane A matching;
3. coordinator group overlay and migration-014 relational schema contract;
4. Lane B PostgreSQL adapter and SQL oracle;
5. coordinator atomic schema-plus-oracle upgrader, frozen bundle digest, and
   ledger tests;
6. Lane C controlled adapter/evaluation;
7. coordinator M5.4 runtime composition and closure docs.

Before every merge, the coordinator checks `git diff --name-only` against the
lane manifest. A forbidden-path edit rejects the handoff until split. After
every merge, focused tests run first, then the full non-model suite at the
wave barrier. Database and long randomized gates run only where specified.

## 10. Current execution note

Three explicitly configured `gpt-5.6-sol` ultra lanes completed the read-only
theory, pure-reference/runtime/evaluation, and PostgreSQL architecture audits.
After an initial NO-GO correction cycle, all three returned final GO with high
confidence. Wave 1 is now active. Implementation ownership begins only after
the coordinator creates the exact path-exclusive worktree/branch named in this
plan; completed audit membership alone grants no edit ownership.
