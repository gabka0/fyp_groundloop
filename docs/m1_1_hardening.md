# M1.1 Reference Atomicity and Invariant Hardening

Status: complete (2026-07-17)

## Reason

M1's 43 tests and static checks passed, but independent review found that the
event layer advanced the live revision counter before operations that could
raise. Rejected observations, policies, or deletions therefore consumed a
revision without being recorded. Batch-local duplicate IDs and caller-forged
content hashes were also accepted. Finally, the synchronous M1 revision counter
needed to be distinguished from M2's asynchronous semantic epoch.

## Implemented corrections

- `apply_event` now deep-copies the repository, applies and recomputes on the
  staged copy, records the event there, and replaces live fields only after all
  steps succeed.
- Rejected events leave all live repository state unchanged and do not consume
  an event ID or revision.
- Claim and chunk IDs are checked for duplicates within their incoming batch as
  well as against stored history.
- `ChunkVersion` verifies supplied normalization-v1 hashes and rejects negative
  chunk indexes.
- D-19 freezes failure atomicity. D-20 distinguishes M1 snapshot revisions from
  M2 semantic epochs and publication coordination.

## Regression evidence

`tests/integration/test_event_atomicity.py` covers:

- rejected late/dangling observation;
- rejected deletion of a missing version;
- rejected duplicate policy version;
- duplicate chunk IDs in one insert;
- replacement failure after the staged copy deactivates the old version;
- reuse of a failed event ID with a corrected payload.

Unit tests additionally cover duplicate claim IDs, forged normalized hashes,
and negative chunk indexes.

## Validation

```text
python3 -m compileall src tests scripts    PASS
python3 -m pytest                          PASS (50 tests)
python3 -m ruff check .                    PASS
python3 -m mypy --strict src/groundloop    PASS (7 source files)
```

PostgreSQL remains unavailable because Docker/local PostgreSQL is not installed.
No PostgreSQL, AI model, evidence-group, or optimized-IVM implementation was
introduced in M1.1.
