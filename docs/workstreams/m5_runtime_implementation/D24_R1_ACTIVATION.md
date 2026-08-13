# M5-D24 R1 Checked-Persistence Activation

Status: active path-exclusive manifest; R1-D is integrated and R1-P is resumed
from its live-green cancellation checkpoint under accepted M5-D24-C3 and
M5-D24-C4

Date: 2026-08-08; accepted M5-D24-C3 amendment 2026-08-10; accepted
M5-D24-C4 amendment 2026-08-12

Accepted R0 implementation commit:
`61875894172c8e0b36866d6b215ecab7a57b76ec`

Exact activation base before this manifest:
`15415edadb70201ef26f1e5e50b7fe0d63279210`

Each lane must resolve and record the commit containing this note as its actual
branch base before editing. The literal parent above identifies the accepted
code/status barrier; it is not permission to branch from an older workstream.

## 1. Accepted migration-016 barrier

The accepted five-field ledger identity is:

```text
bundle_id = m5-runtime-recovery-schema-bundle-v1
bundle_sha256 = 28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565
migration_sha256 = a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7
oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
prerequisite_sha256 = b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd
```

On the exact accepted R0 implementation commit, the main-line gates passed:

- migration 016: 200/200;
- migration 015 regression: 26/26;
- broader live PostgreSQL runtime: 292/292;
- Ruff format/check, strict mypy, compileall, bundle-identity recomputation,
  and `git diff --check`: PASS.

The local-compose server was PostgreSQL 16.14 with pgvector 0.8.5. The DSN is
intentionally omitted. These results accept the R0 schema/installer boundary,
not R1 checked operations or complete D24 recovery.

## 2. Lane R1-P — requirement persistence

```text
branch:   workstream/m5-d24-requirement-persistence
worktree: /home/kassym/Desktop/groundloop-worktrees/m5-d24-requirement-persistence
```

R1-P owns exactly:

- `src/groundloop/m5/runtime/persistence.py`
- `src/groundloop/m5/runtime/postgres_recovery.py` (new)
- `src/groundloop/m5/runtime/postgres_roots.py`
- `src/groundloop/m5/runtime/postgres_verifier.py` (new)
- `tests/m5/postgres_runtime/d24_requirement/conftest.py` (new)
- `tests/m5/postgres_runtime/d24_requirement/test_recovery.py` (new)
- `tests/m5/postgres_runtime/d24_requirement/test_races.py` (new)
- `docs/workstreams/m5_runtime_implementation/D24_R1_REQUIREMENT_PERSISTENCE_HANDOFF.md`
  (new)

R1-P implements checked requirement acquisition, dense takeover, exact replay,
explicit successful disposition, retryable/terminal failure, root-result and
barrier settlement, verifier settlement, work/timing point maintenance,
preterminal late return, post-terminal audit, and transition-timing append.
It must leave active maintained-matching completion blocked for M5-D25 rather
than trusting a process-local overlay after reconnect.

## 3. Lane R1-D — typed-direct persistence

```text
branch:   workstream/m5-d24-direct-persistence
worktree: /home/kassym/Desktop/groundloop-worktrees/m5-d24-direct-persistence
```

R1-D owns exactly:

- `src/groundloop/m5/runtime/direct_m4.py`
- `src/groundloop/m5/runtime/postgres_direct_recovery.py` (new)
- `src/groundloop/m4/persistence.py`
- `src/groundloop/m4/pipeline.py`
- `tests/m5/postgres_runtime/test_direct_m4_composition.py`
- `tests/m5/postgres_runtime/d24_direct/conftest.py` (new)
- `tests/m5/postgres_runtime/d24_direct/test_recovery.py` (new)
- `tests/m5/postgres_runtime/d24_direct/test_races.py` (new)
- `docs/workstreams/m5_runtime_implementation/D24_DIRECT_PERSISTENCE_HANDOFF.md`
  (new)

R1-D implements M5-owned cursor-local direct acquisition/takeover and direct
result/failure/late-return settlement while preserving every public M4-v1 DTO,
digest, row identity, route and replay byte. It may make only the narrowly
required cursor-local changes in M4 persistence/pipeline; production
application orchestration remains R2.

## 4. Shared operational pins

The lanes are path-disjoint but must implement the same frozen operational
contract:

1. mutating and resume routes verify the literal accepted five-field tuple;
2. acquisition samples the PostgreSQL clock only after the frozen locks and
   computes the deadline in PostgreSQL;
3. a live lease returns the total live result without writes, equality at the
   deadline permits takeover, and takeover creates the dense successor;
4. immutable evidence/replay lookup precedes latest-attempt enforcement;
5. dispatch is not confirmed execution evidence, and provider execution is
   at-least-once with an explicit unresolved-call ambiguity bound;
6. work counters use exactly `M5RuntimeWork.counter_names()` order and point
   maintenance—neither lane may scan or sum contribution history inline;
7. the prior pending timing anchor is resolved canonically before a first
   nonterminal write, and exactly one new pending anchor belongs to the outer
   transaction;
8. exact replay performs zero semantic, work, timing, revision or model-call
   writes;
9. post-terminal evidence changes only the dedicated audit/timing surfaces and
   cannot change terminal event totals; and
10. result/takeover/failure/cancellation races are classified under the same
    locked transaction, never by a preflight TOCTOU read.

Lane-specific recovery helpers may not import or edit the other lane's helper.
If either lane discovers that a shared contract or owned path is insufficient,
it must stop and propose a coordinator amendment instead of crossing paths.

## 5. Required focused evidence

Each nested `conftest.py` independently creates a unique live schema, installs
through accepted migration 016 before attempts exist, asserts the literal
tuple, and supplies reconnect and two-connection helpers. Neither lane edits
or imports private helpers from the shared PostgreSQL `conftest.py` or the
migration-016 test module.

At minimum, each applicable lane covers:

- every total acquisition outcome and equality-at-deadline takeover;
- two acquirers and both serial orders of takeover versus result/failure;
- exact replay plus token/error/work/timing/disposition conflicts;
- returned versus reused evidence, including equal zero-work cases;
- crash cutoffs between durable row groups followed by reconnect;
- transition timing observed/missing/replay/late-conflict;
- preterminal expired and post-terminal returned/reused audit paths;
- dispatch-without-evidence ambiguity bounds;
- unchanged terminal event work/timing under post-terminal evidence; and
- lane-specific unchanged M4-v1/direct-only regression evidence.

R1-P additionally covers root-result/barrier/cancellation/verifier races and
exact counts. R1-D additionally covers byte-total direct envelopes, both
discovery and verifier branches, and frozen public-M4 behavior.

Accepted M5-D24-C3 governs the affected cancellation race without changing
this path manifest. Output-first must archive `EXPIRED_PRETERMINAL` before
cancellation, while cancellation-first must reject the preterminal expired
output with zero writes. R1-P owns those two orders and fixture-backed
postterminal persistence-shape evidence. R2 owns production event
terminalization and the end-to-end `EXPIRED_POSTTERMINAL` continuation.

Accepted M5-D24-C4 also leaves this manifest unchanged. R1-P owns zero-write
coverage when the dense successor is `retryable_failed`, `completed_active`,
`completed_inactive`, `terminal_failed`, or `cancelled` but the event remains
nonterminal. It also owns checked-reacquisition coverage from
`retryable_failed` back to `running` and fixture-backed proof that the first
legal postterminal five-row archive starts only after its SQL-only fixture
resolves the test-local job to one of migration 016's four exact terminal
states; the fixture carries forward the zero-write rejection while the job is
`retryable_failed`. For every audit-only requirement discovery/verifier return,
R1-P must validate the complete resupplied context/self-digested DTO,
discovery exhaustion and nested context, and verifier pair-input immutable
core, then prove that no normal semantic artifact table is populated.
Snapshot-exhaustion evidence is true and revalidated for
`snapshot_exhausted`; the `budget_filled` boolean is non-material and not
replay-bound. R2 continues to own production event
terminalization, including proof of production resolution of
`retryable_failed`, and the end-to-end continuation. R1-P resumes under the
accepted correction, and neither lane may change migration 016/017 or a public
contract/digest to implement it.

## 6. Integration and forbidden paths

Both lanes require independent code/handoff audit before integration. The
coordinator validates the full branch range, exact name-status, focused live
gates, broader PostgreSQL runtime, and M4-v1 regressions. Cross-lane wrappers
are deferred to R2.

Neither R1 lane owns:

- `src/groundloop/m5/runtime/application.py` or fake ports/history;
- any package `__init__.py`, public contract/digest file or shared test fixture;
- open/fail/seal application composition or shared reconnect tests;
- migration 016, migration 017, top-level status/design documents;
- `pyproject.toml`, `docs/presentations/`, or any other user-owned dirt; or
- `docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT_DRAFT.md`.

M5-D25 and migration 017 remain blocked. This activation does not close
M5.0-24's implementation half, M5.4-02 through M5.4-09, M5.5, M5.6, or M5 as
a whole and does not establish exactly-once provider execution, objective
truth, performance superiority, or representative utility.
