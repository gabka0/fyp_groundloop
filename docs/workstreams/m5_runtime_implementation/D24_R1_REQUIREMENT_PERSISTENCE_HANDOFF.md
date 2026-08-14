# M5-D24 R1 Requirement Persistence Handoff

Status: final implementation candidate; independently auditable and not yet
integrated

Date: 2026-08-14

Branch: `workstream/m5-d24-requirement-persistence`

Activation base: `3897ee4e4d128f3832ca96e5d747a1a7bb8b1b82`

The branch must be reviewed as one eight-path candidate. It owns exactly:

- `src/groundloop/m5/runtime/persistence.py`
- `src/groundloop/m5/runtime/postgres_recovery.py`
- `src/groundloop/m5/runtime/postgres_roots.py`
- `src/groundloop/m5/runtime/postgres_verifier.py`
- `tests/m5/postgres_runtime/d24_requirement/conftest.py`
- `tests/m5/postgres_runtime/d24_requirement/test_recovery.py`
- `tests/m5/postgres_runtime/d24_requirement/test_races.py`
- this handoff

No other path belongs to this lane. The candidate commit is the commit that
contains this handoff and the seven pinned Python files below; its immutable
Git identity is recorded by the post-commit audit rather than self-referenced
inside this file.

Final Python SHA-256 pins:

- `persistence.py`:
  `64045d743daf062e3ce29821f6f3cdc7df967b3e25dafba999e29d91bf2667c5`
- `postgres_recovery.py`:
  `7ac948d47a1b056e93901e1effa2fb5f07c036970b239469752338e7f76def8a`
- `postgres_roots.py`:
  `7a687569da3a5e3ab46b736d506a64b43346153371ca6f6796060ab18f6be0fd`
- `postgres_verifier.py`:
  `801131075b61d8a748036b0f800601f1db9c69c7aaaa5536681a398a9a18a2b2`
- nested `conftest.py`:
  `e1f3d8865ee39d10e8b36c4627f69a81fe641b8966696b10ad3aa418947d94e3`
- nested `test_recovery.py`:
  `e0384b0b29268ce48b260e47655b1f41d563df6596bd9401879c3e630ba16738`
- nested `test_races.py`:
  `0c42499de30a1cdd799272eda7fbfeb5c59100d6d88987108826778cdb0f0b33`

## Implemented surface

The lane adds checked PostgreSQL requirement-runtime procedures behind
`PostgresM5RuntimeStore` for:

- requirement acquisition, live-lease replay and dense expired-attempt
  takeover;
- retryable and terminal attempt failure;
- discovery/root-result staging, root barrier settlement and cancellation;
- inactive verifier completion, with active maintained-matching completion
  rejected pending M5-D25;
- exact work contribution and point-accumulator maintenance;
- transition-call timing append/read, terminal timing coverage hydration and
  terminal invocation telemetry; and
- preterminal expired output plus the narrowly corrected post-terminal audit
  path.

`postgres_recovery.py` owns migration-016 identity checks, dispatch and
execution accounting, work/timing maintenance, replay reads and terminal
telemetry. `postgres_roots.py` owns checked discovery, barrier, cancellation
and late-return settlement. `postgres_verifier.py` owns checked verifier
closure and its inactive-result accounting. The store remains the public
transaction boundary.

## Preserved semantics

- Every mutation verifies the accepted migration-016 ledger identity and uses
  PostgreSQL lock/time state for authorization.
- Acquisition returns an exact live result without writes; equality at the
  lease deadline permits one dense successor attempt.
- Immutable evidence and exact replay are checked before latest-attempt
  rejection. Replays do not advance revision or duplicate work, timing or
  semantic rows.
- Dispatch is not execution evidence. `RETURNED` and `REUSED` retain distinct
  execution-accounting meanings, including zero-work results.
- Work counters follow `M5RuntimeWork.counter_names()` and are maintained by
  contributions plus locked point accumulators rather than transaction-time
  history scans.
- Pending timing is resolved before the first nonterminal transition and the
  outer transaction creates the next pending anchor. Terminal coverage and
  telemetry are immutable, identity-checked records.
- Discovery results are contextually revalidated, including the explicit
  `eligible_snapshot_exhausted` input.
- An expired old worker may write preterminal evidence only while its dense
  successor is `running`. A nonrunning successor with a nonterminal event is a
  zero-write conflict. Once the event and job are terminal, the first legal
  post-terminal return writes only the exact five audit/accounting rows; it
  does not populate normal discovery/verifier semantic tables or change the
  frozen event totals.

## Final candidate evidence

Live tests use a unique PostgreSQL schema installed through accepted migration
016; the credential-bearing DSN is omitted.

- Complete nested R1-P live PostgreSQL run: **127/127 passed** with process
  exit code zero.
- Exhaustive root-stage, root-barrier and cancellation crash matrices:
  **29/29 passed**. The inactive-verifier matrix covers all **16/16** route
  cutoffs, and recovery open covers its introduced initialization cutoff.
  Every one of the **37** R1-P-introduced hook names is now referenced by
  executable rollback or serialization evidence; rollback cases reconnect to
  a byte-identical image and permit a fresh checked retry.
- C4 evidence covers running, retryable-failed, completed-active,
  completed-inactive, terminal-failed and cancelled successor states; the
  discovery and verifier paths both prove every applicable branch. The
  SQL-only fixture proves retryable successor resolution, the first legal
  post-terminal five-row archive and exact replay without normal
  semantic/currency writes.
- Pure runtime contracts/digests/C1 suite: **84/84 passed**.
- Collection: **127** nested cases (`114` recovery plus `13` races).
- Ruff check and format check: **passed on all seven Python paths**.
- Strict mypy with explicit package bases: **passed on all seven Python
  paths**.
- Cache-isolated compileall and `git diff --check`: **passed**.

The 127/127 result is focused lane evidence. It is not a claim that shared
PostgreSQL runtime tests or application composition are compatible yet.

## Explicit remaining boundary

This lane does not edit or validate `application.py`, fake ports/history,
package exports, or shared PostgreSQL tests. Those surfaces remain pre-D24 and
are R2-owned, except for the explicit R1-C shared-test compatibility pass.

In particular, the current application protocol/caller does not yet thread
the checked persistence disposition, work, timing and explicit
`eligible_snapshot_exhausted` inputs. Calling the concrete R1-P store through
that pre-D24 application route is an explicit R2 composition blocker, not a
reason to add defaults or cross this lane's manifest.

The following remain outside this candidate:

- production open/fail/seal orchestration and terminalization composition;
- shared test-fixture/signature adaptation and broader regression evidence
  (R1-C);
- active verifier persistence, maintained matching and migration 017 (M5-D25);
- production proof for the terminal event/job state used by post-terminal
  return tests—the nested lane uses an explicit SQL-only terminal fixture; and
- exactly-once provider execution, representative utility, performance
  superiority, or closure of M5.4--M5.6.

Integration must preserve the accepted terminal-successor correction and the
separately accepted R1-D direct-persistence bytes. This handoff is not an
authorization to cross either lane's path ownership or to promote the
unaccepted M5-D25 draft.
