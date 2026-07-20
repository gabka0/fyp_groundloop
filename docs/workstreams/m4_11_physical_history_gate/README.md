# M4.11 Physical-History Gate

Status: complete on 2026-07-20

Owned paths:

- `tests/m4/physical_runtime_gate/test_history_gate.py`
- `tests/m4/physical_runtime_gate/history_gate_harness.py`
- `docs/workstreams/m4_11_physical_history_gate/`

Forbidden paths: production code, migrations, shared status/roadmap/design
documents, and every other workstream directory.

This lane extends the existing one-event physical gate with adversarial event
histories.  It is regression evidence, not an asymptotic proof.  Successful
measured kernels forbid deep copies, full repository hydration, full runtime
book/epoch reads, and inline Python/SQL full recomputation.  Full grounding,
SQL, evaluation, and runtime reconstruction run only after the kernel.

Recovery is reported separately.  Failure cleanup and process restart are
allowed to hydrate the published/working snapshot because the formal M4.7
event-time bound explicitly excludes recovery.  Pending-event resume is not
claimed point-bounded.

The lane initially found that the composed M4 application exposed no
retryable-failure port capable of updating runtime and evaluation state
atomically.  The coordinator added that transition in `e7d2236`; this lane now
tests the composed retry, exact failure replay, second attempt, seal, exact
event replay, and conflicting replay with all full reads forbidden.

## Coverage map

| Required case | Executable evidence |
|---|---|
| Insert with zero admitted pairs | One closed impact root, zero children, zero verifier calls, sealed publication |
| Supporting deletion | Exact withdrawal changes `SUPPORTED/VALID` to `UNSUPPORTED/UNSUPPORTED`; one required frontier root closes with an explicit empty result |
| Neutral-to-refute replacement | Neutral insertion leaves the required claim unsupported; replacement withdraws it and installs a REFUTE observation, yielding `REFUTED/CONTRADICTED` |
| Retry and replay | First attempt becomes retryable, exact failure replay is a no-op, the second attempt completes, the event seals, exact sealed replay makes zero model calls, and conflicting replay changes none of the 62 GroundLoop table projections |
| Failed epoch plus late completion | Failure leaves all nine publication/current-state projections unchanged; late result is archived `COMPLETED_INACTIVE` and cannot mutate publication or failed evaluation state |
| Optional beside required | One event creates both children; required completion writes claim+answer evaluation overrides while optional completion writes only its claim override |
| Transaction rollback | Failure after verifier state installation restores complete projections of 61 transactional GroundLoop tables; the excluded execution-accounting row changes only by the separately committed one-row active-chunk examination |

The successful history runs at `(8 claims, 2 unrelated historical jobs)` and
`(256 claims, 256 unrelated historical jobs)`.  Per-event client SQL
fingerprint sequences, event job projections, and explicit execution-accounting
rows are identical between scales.  SQL checks also reject policy-time registry
construction and full epoch/book statement shapes in the measured kernel.

## Validation

Executed against PostgreSQL using `GROUNDLOOP_TEST_DATABASE_URL`:

```bash
PYTHONPATH=src .venv/bin/pytest -q tests/m4/physical_runtime_gate
.venv/bin/ruff check \
  tests/m4/physical_runtime_gate/history_gate_harness.py \
  tests/m4/physical_runtime_gate/test_history_gate.py
.venv/bin/python -m compileall -q \
  tests/m4/physical_runtime_gate/history_gate_harness.py \
  tests/m4/physical_runtime_gate/test_history_gate.py
```

Result: 6 tests passed (the two pre-existing physical-gate tests plus four new
history tests); Ruff and compileall passed.

## Claim boundary

This is adversarial regression evidence for the concrete PostgreSQL plan.  It
does not prove an asymptotic bound, and equal client SQL text does not by itself
prove equal server-side page work.  Failure cleanup is observed to perform one
published-snapshot rehydration and is reported as recovery work outside the
successful event-time theorem.  Pending-process resume is likewise outside
this gate's point-bounded claim.
