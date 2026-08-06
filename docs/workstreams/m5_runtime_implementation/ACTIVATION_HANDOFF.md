# M5 Public Activation Handoff

Status: integrated implementation checkpoint; live local gate PASS

Date: 2026-08-06

Authority: M5-D14, M5-D21, M5-D22, and runtime-addendum revision 3.

## Implemented boundary

`PostgresM5RuntimeStore` now prepares and executes public M5 activation. The
write transaction acquires runtime mode, M4 head, M5 head, activation, and
base-epoch locks in the frozen order; rejects any live mutation epoch; verifies
the immutable core/runtime bundle ledgers; independently rebuilds the SQL
oracle bootstrap projection; and compares the request's complete six-kind
changed-state-set hash before writing.

One transaction writes all materialized/published M5 state and certificate
bindings at the existing M4 head, creates the M5 head, inserts the immutable
activation record, and advances mode revision exactly once. It creates no
synthetic epoch. Same-ID/same-payload replay uses a read-only transaction,
returns the original semantic receipt with `replayed=true`, and performs no
write or mode revision change. Reused IDs/payloads conflict.

M5-D22 supplies byte-total state artifact recipes. Activation includes every
requirement/group/claim/answer state plus every present group/claim
certificate reference. Python and SQL implementations agree across all six
kinds in the live acceptance fixture.

## Evidence

```bash
GROUNDLOOP_TEST_DATABASE_URL='postgresql://groundloop:groundloop@localhost:5432/groundloop' \
PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/m5/postgres_runtime/test_typed_runtime.py
```

Result: PASS, 8/8 live tests. Coverage includes a populated complete group,
all six changed-state kinds, exact receipt replay, conflicting replay, wrong
bootstrap rejection, no synthetic epoch, and injected failure after each of
six activation statement groups with byte-stable rollback snapshots.

The composed live gate with migration 015, the existing bundle/race suite,
and M5.3-07 failure/replay passed all 75 collected tests.

## Remaining boundary

This checkpoint does not claim typed document composition, job execution,
sparse seal/publication, crash reconnect of nonterminal work, measured
history, or pinned-model execution. Those remain M5.4 work.
