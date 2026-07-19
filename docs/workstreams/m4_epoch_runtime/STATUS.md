# M4 Epoch/Runtime Lane Status

## Current state

Wave 1 pure runtime implementation is complete on top of replacement contract
baseline `a1059ff63883fa388def0e39986fa4b8393ef709`.

Implemented in owned paths:

- immutable serialized epoch book with stable epoch IDs and revision CAS;
- job attempts, retryable failure, terminal failure, cancellation and strict
  seal transitions;
- atomic expandable-parent completion, result-bound child closure, child
  declaration and discovery-scope closure;
- exact replay and conflicting replay behavior;
- late inactive completion on failed epochs without resurrection;
- claim/answer `PENDING` and `FAILED` projection from discovery scopes and
  explicitly targeted jobs;
- exact indexed reverse-dependency withdrawal planning;
- deterministic frontier refill, reserve selection and mandatory-fresh-
  retrieval signaling;
- executable invariant, crash/replay, randomized withdrawal and frontier edge
  tests;
- explicit safety and cost arguments in `ALGORITHM_NOTES.md`.

## Validation

Focused lane result:

```text
pytest tests/m4/runtime: 21 passed
ruff owned source/tests: all checks passed
mypy --strict owned source: no issues in 4 source files
```

Repository regression result before final documentation-only changes:

```text
pytest: 199 passed, 18 skipped
```

## Deliberately not implemented in Wave 1

- PostgreSQL tables, migrations, triggers or transaction adapter;
- working/published structured-state mutation;
- candidate admission, semantic oracle or real model execution;
- CLI/pipeline wiring;
- degraded sealing;
- synthetic frontier entries for M3 state.

Those boundaries are coordinator-owned or belong to later waves.
