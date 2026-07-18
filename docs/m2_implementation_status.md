# M2 Implementation Status: Relational IVM and Differential Testing

Status: **complete**. The in-memory algorithms, randomized differential gate,
epoch coordinator, PostgreSQL schema, independent SQL oracle, live database
adapter, and differential validation harness all pass their M2 gates.

Date: 2026-07-18

## Delivered

### Independent incremental engine

`src/groundloop/incremental.py` maintains the M2 direct-witness CORE without
calling any full-recomputation function:

- signed activation and withdrawal of current observations;
- exact supersession as old inverse delta plus new forward delta;
- per-claim support/refute text-hash reference counts;
- distinct-content 0-to-1 and 1-to-0 crossings;
- counted max-score multisets with lazy heap deletion;
- claim truth-table and required-claim answer aggregation;
- propagation only when a claim status boundary changes;
- compact support/refutation certificates with local repair;
- inspectable per-event maintenance-work statistics;
- monotone threshold-policy candidate discovery using score intervals.

The Python score index uses sorted lists. Range discovery is `O(log E + m)`;
point insertion/removal is `O(E)`. This is a transparent correctness prototype,
not the final physical-index claim. The PostgreSQL schema supplies B-tree score
indexes.

Evidence groups are deliberately absent; they remain M5 scope.

### Differential publication

`src/groundloop/differential.py` stages the repository and incremental engine,
applies an event to both independent paths, compares complete claim and answer
states, validates certificate membership, and publishes the pair only if every
check passes. Exact event replay does not reapply a signed delta. Failure
injection covers failure after the reference step and after the incremental
step.

### Semantic epoch coordinator

`src/groundloop/epochs.py` implements D-20 independently from M1 snapshot
revisions:

- one stable EpochId per structurally committed corpus event;
- idempotent job completion microtransactions that advance only `revision`;
- pending, complete, degraded, failed, and sealed transitions;
- sealing preconditions;
- strict and provisional visibility with `confirmed_as_of_epoch`;
- single-writer enforcement and event payload conflicts.

This module is a coordinator oracle, not yet a database service or async worker
system.

### Generated update streams

`experiments/streams/run_m2_differential.py` generates reproducible mixtures of
insert, delete, replace, observe/supersede, and policy-change events. Histories
are sharded because the M1 oracle deliberately copy-stages immutable history;
this prevents a correctness run from becoming an accidental quadratic-history
benchmark.

Recorded full gate:

```text
seed:               20260718
events:             100000
streams:            1000
events per stream:  100
elapsed:            298.034632 seconds
throughput:         335.531 events/second
result:             no differential mismatch
```

The throughput includes full recomputation and deep-copy staging. It is not an
incremental-engine performance result.

### PostgreSQL artifacts

- `migrations/000_extensions.sql`: explicit pgvector extension installation.
- `migrations/001_m2_base.sql`: epochs, immutable/versioned base relations,
  active-version uniqueness, claim-subject observations, observation currency,
  score indexes, materialized state tables, certificates, and status deltas.
- `sql/m2_full_recompute_oracle.sql`: current decisions, full claim/answer
  state recomputation, certificate validity, and zero-row mismatch views.
- `src/groundloop/postgres/snapshot.py`: typed immutable snapshot capture,
  atomic loading, typed SQL-oracle reads, isolated temporary schemas, and
  idempotent event recording.
- `scripts/validate_m2_postgres.py`: applies the SQL in a unique temporary
  schema, compares Python reference, signed-delta, and SQL state, validates
  certificates, records server metadata and query plans, and removes the
  fixture transactionally.

Static validation with `pglast` 7.17 parsed the extension statement, all 29
base-migration statements, and all 6 oracle statements. Live execution also
passes on PostgreSQL 16.14 with pgvector 0.8.5.

### Additional evaluation lanes

- `src/groundloop/baselines/` supplies an independent global recomputation
  baseline, exact keyed affected-claim recomputation, the signed-delta
  treatment, and explicitly non-equivalent invalidation policies.
- `src/groundloop/optimized/` supplies an independent exact-flip candidate
  engine backed by deterministic AVL indexes. For frozen tie rule v1, a
  threshold-only change has expected `O(log E + f + p)` maintenance time under
  the stated hash-table model. This is a known ordered-range mechanism
  specialized to GroundLoop, not a new general IVM result. See
  `docs/theory/exact_flip_theorem.md`.

## Correctness scenarios

The automated suite covers:

- alternative witness survives one deletion and final-witness zero crossing;
- identical-content witnesses use reference counts rather than inflated counts;
- supersession changes labels, best scores, and certificates exactly once;
- policy range changes touch only threshold-crossing candidates;
- answer propagation stops when a claim status does not change;
- exact replay is a no-op for the delta engine;
- reference/incremental publication is failure-atomic;
- stable semantic EpochId across multiple job completions;
- strict/provisional publication and sealing rules;
- 1,000 randomized events in the normal pytest suite;
- 100,000 randomized events in the explicit M2 stress gate.

## Final integrated validation evidence

Executed successfully on 2026-07-18:

```text
GROUNDLOOP_TEST_DATABASE_URL=... .venv/bin/python -m pytest -q
100 passed

.venv/bin/python -m ruff check .
All checks passed!

.venv/bin/python -m mypy --strict src
Success: no issues found in 23 source files

GROUNDLOOP_DATABASE_URL=... .venv/bin/python scripts/validate_m2_postgres.py
PostgreSQL 16.14; pgvector 0.8.5
0 claim mismatches; 0 answer mismatches; 0 invalid certificates
current-observation and policy-score indexes usable

PYTHONPATH=src python3 experiments/streams/run_m2_differential.py \
  --events 100000 --events-per-stream 100 --seed 20260718
100000 events, 1000 streams, no mismatch

PYTHONPATH=src .venv/bin/python \
  experiments/streams/algorithm_randomized_differential.py \
  --events 10000 --events-per-stream 100 --seed 20260718
10000 events, 100 streams, no mismatch
```

The live randomized PostgreSQL suite covers 20 committed events; the
100,000-event gate is in-memory and must not be described as a database
throughput result. Likewise, the structured benchmark is a smoke harness, not
a dissertation-grade performance result.
