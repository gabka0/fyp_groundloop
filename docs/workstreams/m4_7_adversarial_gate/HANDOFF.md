# M4.7 Adversarial Physical-Runtime Gate Handoff

Status: test lane implemented; production wiring remains coordinator-owned.

## Ownership

This lane owns only:

- `tests/m4/physical_runtime_gate/`
- `docs/workstreams/m4_7_adversarial_gate/`

It does not edit the pipeline, persistence, migrations, existing tests or
shared design/status documents.

## Acceptance question

Can one measured multi-child insertion execute with the same client-visible
SQL statement sequence when the frozen registry and sealed predecessor runtime
grow from `(8 claims, 2 unrelated jobs)` to
`(256 claims, 256 unrelated jobs)`?

The test makes the question falsifiable by:

1. prebuilding the immutable claim-registry snapshot outside the event;
2. passing only the registry identity and an empty member tuple in the event;
3. generating one impact root and three verifier children;
4. patching both `copy.deepcopy` entry points used by the pipeline and the
   runtime store's `read_epoch`/`read_book` methods to raise during the measured
   kernel;
5. recording every client-issued SQL execution through a Psycopg cursor
   subclass;
6. requiring the exact statement fingerprint sequence and durable accounting
   row to be identical at both unrelated scales; and
7. running grounding, evaluation and full runtime-book audits only after the
   measured kernel, while proving those reads do not change measured
   accounting.

No test is marked `xfail`. On integration baseline `84df134`, the live gate
fails before model work because the measured pipeline still validates the
empty compact event tuple as if it were the complete prebuilt registry. After
that binding is corrected, the guards will reject the remaining `deepcopy` and
full-runtime reconstruction paths until coordinator wiring routes measured
execution through point-CAS runtime operations, signed evaluation counters and
affected-key state patches.

## Run

```bash
set -a
source .env
set +a
GROUNDLOOP_TEST_DATABASE_URL="$GROUNDLOOP_DATABASE_URL" \
  .venv/bin/pytest -q tests/m4/physical_runtime_gate
```

Static checks for this lane:

```bash
.venv/bin/ruff check tests/m4/physical_runtime_gate
.venv/bin/python -m compileall -q tests/m4/physical_runtime_gate
```

## What this gate does not prove

- It does not measure server CPU, buffer misses, WAL, network bytes or query
  plan complexity.
- Equal client SQL traces do not imply equal latency.
- The fixed event has three admitted pairs; dense output and reverse-edge
  fanout remain linear in enumerated work.
- Registry construction, process hydration and explicit audits are outside the
  measured kernel by design.
- The gate supports only the claimed absence of hidden registry/runtime-size
  scans for this adversarial history. It is not evidence of superiority over
  another IVM system.
