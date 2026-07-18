# M2 Implementation Status: Relational IVM and Differential Testing

Status: **in progress**. The in-memory algorithms, randomized differential
gate, epoch coordinator, PostgreSQL schema, SQL oracle, and DB validation
harness are implemented. M2 is not marked complete until the PostgreSQL
harness runs successfully on a live PostgreSQL 16 instance.

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

- `migrations/001_m2_base.sql`: epochs, immutable/versioned base relations,
  active-version uniqueness, claim-subject observations, observation currency,
  score indexes, materialized state tables, certificates, and status deltas.
- `sql/m2_full_recompute_oracle.sql`: current decisions, full claim/answer
  state recomputation, certificate validity, and zero-row mismatch views.
- `scripts/validate_m2_postgres.py`: applies both files in a unique temporary
  schema, loads a deterministic fixture, compares materialized and oracle
  state, validates its certificate, and rolls back the entire transaction.

Static validation with `pglast` 7.17 parsed 29 migration statements and 6
oracle statements. Static parsing is not equivalent to PostgreSQL execution.

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

## Validation evidence

Executed successfully on 2026-07-18:

```text
python3 -m pytest -q
62 passed

python3 -m ruff check src tests experiments/streams
All checks passed!

python3 -m mypy --strict src
Success: no issues found in 10 source files

PYTHONPATH=src python3 experiments/streams/run_m2_differential.py \
  --events 100000 --events-per-stream 100 --seed 20260718
100000 events, 1000 streams, no mismatch
```

## Remaining gate

This host has no `docker`, `psql`, or PostgreSQL server, so the SQL was not
executed against a database. Psycopg is now installed in `.venv`. After running
the privileged prerequisite installer and starting the Compose service:

```bash
cp .env.example .env
docker compose up -d db
set -a
source .env
set +a
make validate-postgres
```

M2 can be marked complete only if that command reports zero claim mismatches,
zero answer mismatches, and zero invalid certificates. Do not begin claiming
three-engine equality before this gate passes.
