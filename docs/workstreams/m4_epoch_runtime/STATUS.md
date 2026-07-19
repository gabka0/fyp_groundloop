# M4 Epoch/Runtime Lane Status

## Current state

Wave 2 exact-withdrawal empirical gate is complete on integrated main baseline
`b3623feb36f719ef8715e6011e92c1a865530d7d`.

Wave 2 code/test commit:

```text
7b955a60889044cbba290df9be604f9af9d1d4d3
```

Wave 2 adds:

- an independent naive full dependency-scan oracle under tests only;
- deterministic operation counters for indexed probes/enumeration and global
  full-scan work;
- 60 seeded insert/delete/replace-shaped differential cases;
- explicit duplicate-pair, duplicate-ID, empty, absent/inactive, cold-skew and
  dense-hot cases;
- an exact executable assertion that indexed logical work equals
  `|D| + |E_obs(D)| + |E_cand(D)|`;
- a dense-fanout counterexample preventing a false universal sublinear claim.

Wave 1 pure runtime remains integrated and unchanged apart from the derived
indexed operation-count property.

## Wave 1 implementation

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
pytest tests/m4/runtime: 86 passed
ruff owned source/tests: all checks passed
mypy --strict runtime source plus test oracle: no issues in 5 source files
```

Repository regression result before final documentation-only changes:

```text
pytest: 309 passed, 22 skipped
```

Deterministic counter checks include:

```text
cold skew: indexed 2 operations; full scan 12,002 operations
dense hot: indexed 10,001 operations; full scan 10,001 operations
```

These are logical operation counts, not wall-clock benchmark claims.

## Deliberately not implemented

- PostgreSQL tables, migrations, triggers or transaction adapter;
- working/published structured-state mutation;
- candidate admission, semantic oracle or real model execution;
- CLI/pipeline wiring;
- degraded sealing;
- synthetic frontier entries for M3 state.

Those boundaries are coordinator-owned or belong to later waves.
