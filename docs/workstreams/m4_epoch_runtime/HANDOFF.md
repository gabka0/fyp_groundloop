# M4 Epoch/Runtime Handoff

## Wave 2 exact-withdrawal empirical gate

Code/test commit:

```text
7b955a60889044cbba290df9be604f9af9d1d4d3
```

The production planner now exposes `WithdrawalPlan.indexed_operation_count`,
defined as chunk-index lookups plus enumerated observation and candidate
edges. It performs no new work and exists to make the claimed bound directly
testable.

`tests/m4/runtime/withdrawal_reference.py` is an intentionally naive,
independent full dependency-scan oracle and deterministic measurement helper.
It is not exported from `groundloop.m4.runtime` and must not be called by the
pipeline. It scans every stored observation and candidate edge, validates
immutable edge-ID uniqueness, and records global scan and matched-edge counts.

The differential gate proves set equality between indexed and full-scan
outputs over:

- 60 seeded insert/delete/replace-shaped cases;
- multiple distinct dependencies for one pair;
- duplicate immutable dependency IDs;
- empty and absent/inactive deactivation sets;
- cold deletion under highly skewed fanout;
- deletion of the dense hot key.

The event-work assertion is exact under the logical hash-index model:

```text
indexed operations = |D| + |E_obs(D)| + |E_cand(D)|
```

The skew test records 2 indexed operations versus 12,002 full-scan operations
for a cold deletion. The dense test records 10,001 operations for both paths.
Consequently the evidence supports output sensitivity, not a sublinear
worst-case claim. These are deterministic logical counts and say nothing about
latency or PostgreSQL query-plan quality.

## Baseline and ownership

- Integrated Wave 2 baseline: `b3623feb36f719ef8715e6011e92c1a865530d7d`
- Original contract baseline: `a1059ff63883fa388def0e39986fa4b8393ef709`
- Branch: `workstream/m4-epoch-runtime`
- All changes are confined to the lane-owned source, tests and workstream docs.

## Integration entry points

Import public runtime primitives from `groundloop.m4.runtime`.

The coordinator should persist `RuntimeBook` semantics rather than the Python
container itself:

1. `open_epoch` is the short structural transaction and enforces the serial
   writer plus exact event replay.
2. `start_attempt`, retry/failure transitions and `apply_completion` are
   revision-CAS microtransactions outside model execution.
3. `apply_completion` is the indivisible SQL contract: parent completion,
   child closure, all child rows and discovery-scope closure must commit in one
   transaction with exactly one epoch-revision increment.
4. `seal_epoch` is only the runtime half of sealing. The coordinator must also
   require the independent structured-state oracles, certificates and policy
   manifest before publishing working state.
5. `plan_withdrawal` consumes a prebuilt exact reverse index. PostgreSQL should
   realize its two logical lookups with indexes beginning in
   `chunk_version_id`; retrieval services must not appear on this call path.
6. `plan_frontier_refill` returns verifier pairs and an explicit mandatory
   fresh-retrieval flag. The latter must become a `FRONTIER_RETRIEVE` root job,
   so an unfilled frontier cannot seal silently.

## Important integration constraints

- Supply `CompletionPlan.child_jobs` sorted by `job_id` and deactivated chunk
  IDs sorted and unique. Canonical inputs are part of deterministic replay and
  the linear withdrawal-event bound.
- Supply the current active chunk set at completion time. The runtime rejects
  a claimed active/inactive terminal state that disagrees with it.
- Keep model inference outside the completion transaction. Persist the model
  artifact first; then replay the same completion plan until its CAS commits.
- Do not map `TERMINAL_FAILED` or `CANCELLED` to completion. They intentionally
  block strict sealing until the whole epoch is explicitly failed.
- A failed epoch can coexist with a newer active epoch while an older running
  worker returns. Only a matching late inactive result is accepted on the old
  epoch, and the newer active epoch pointer must remain unchanged.

## Suggested Wave 2 tests at the PostgreSQL boundary

- kill the process after artifact persistence but before completion commit;
- kill it between every SQL statement in expandable completion and prove that
  no partial parent/child/scope state is visible;
- race two exact completions and then race exact versus conflicting child
  sets;
- run a late failed-epoch completion while a newer epoch is active;
- differential-test indexed deletion against a full SQL edge scan under
  zero-degree, Zipfian and dense fanout;
- prove the query plan uses reverse chunk indexes and records actual edge
  visits;
- prove empty and below-floor frontiers install a retrieval job and block seal.

## Validation commands

```bash
.venv/bin/pytest tests/m4/runtime
.venv/bin/ruff check src/groundloop/m4/runtime tests/m4/runtime
.venv/bin/mypy --strict src/groundloop/m4/runtime \
  tests/m4/runtime/withdrawal_reference.py
python3 -m compileall -q src/groundloop/m4/runtime \
  tests/m4/runtime/withdrawal_reference.py
.venv/bin/pytest
```
