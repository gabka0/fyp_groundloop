# M4 Epoch/Runtime Wave 1 Handoff

## Baseline and ownership

- Contract baseline: `a1059ff63883fa388def0e39986fa4b8393ef709`
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
.venv/bin/mypy --strict src/groundloop/m4/runtime
python3 -m compileall -q src/groundloop/m4/runtime
.venv/bin/pytest
```
