# PostgreSQL Persistence and Third-Oracle Handoff

## Outcome

The M2 PostgreSQL runtime gate passes on the project Compose database. The
owned schema and independent SQL oracle execute on PostgreSQL 16.14 with
pgvector 0.8.5 installed. The live validator reports:

```text
claim_mismatches:       0
answer_mismatches:      0
invalid_certificates:   0
current_index_usable:   true
policy_index_usable:    true
```

Confidence: **high** for the tested M2 direct-witness snapshot semantics and
transaction/constraint behavior. This lane does not declare M2 complete; the
coordinator owns the integrated milestone decision. Evidence groups remain M5
scope.

## Delivered files

- `migrations/000_extensions.sql` installs pgvector explicitly.
- `src/groundloop/postgres/snapshot.py` provides typed capture, atomic loading,
  typed SQL-oracle reads, mismatch reads, server metadata, isolated schemas,
  and idempotent epoch recording with payload-conflict detection.
- `scripts/validate_m2_postgres.py` now exercises the typed adapter and all
  three engines, reports versions/mismatches, and records query plans.
- `tests/postgres/` adds dependency-free capture coverage plus explicitly
  DSN-gated live database tests.

## Semantics exercised live

- support, refutation, and conflict;
- distinct-content deduplication;
- observation supersession/currency;
- inactive late observations;
- document deletion and replacement;
- threshold policy changes;
- complete claim/answer counts, best scores, observation-ID arrays, statuses,
  and certificate validity;
- 20 seeded randomized committed events, with three-way comparison after each;
- injected snapshot-load rollback and successful retry;
- exact event replay and different-payload conflict;
- one-active-document-version uniqueness;
- deferred at-least-one-required-claim enforcement.

## Runtime and exact commands

Database URL was passed only through the environment and was not written to
Git.

```bash
PYTHONPATH=src \
GROUNDLOOP_TEST_DATABASE_URL='postgresql+psycopg://.../groundloop' \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest
# 71 passed in 5.19s

PYTHONPATH=src \
GROUNDLOOP_DATABASE_URL='postgresql+psycopg://.../groundloop' \
/home/kassym/Desktop/groundloop/.venv/bin/python \
  scripts/validate_m2_postgres.py
# PostgreSQL 16.14; pgvector available/installed 0.8.5
# 0 claim mismatches; 0 answer mismatches; 0 invalid certificates

env -u GROUNDLOOP_DATABASE_URL -u GROUNDLOOP_TEST_DATABASE_URL \
  PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  -m pytest tests/postgres
# 1 passed, 8 skipped in 0.04s (explicit no-DSN gate)

PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  -m compileall -q src tests scripts

PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  -m ruff check .
# All checks passed!

PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  -m mypy --strict src
# Success: no issues found in 12 source files
```

Static PostgreSQL parsing also passed:

```text
migrations/000_extensions.sql:             1 statement
migrations/001_m2_base.sql:               29 statements
sql/m2_full_recompute_oracle.sql:           6 statements
```

## Query-plan evidence

Both natural `EXPLAIN (ANALYZE, BUFFERS)` plans used the intended indexes on
the live fixture; the validator also repeats the check with sequential scans
disabled to make index eligibility explicit.

Current-observation lookup:

```text
Bitmap Index Scan on groundloop_current_observations_by_chunk
  Index Cond: (chunk_version_id = 'p'::text)
Execution Time: 0.020 ms
```

Support-threshold range lookup:

```text
Index Only Scan using groundloop_observation_support_scores
  Index Cond: support_score >= 0.8 AND support_score < 0.95
Execution Time: 0.050 ms
```

These tiny-fixture timings are smoke evidence, not performance claims.

## Interface assumptions and limitations

- M1 does not retain creation revisions for static questions/answers or
  production revisions for observations. Snapshot import assigns static
  registry rows to the first captured revision and observations to the current
  captured revision. This preserves current snapshot semantics but is not an
  event-history reconstruction.
- The repository lacks a public bulk snapshot contract. The adapter currently
  reads private historical collections without mutation. A nonblocking
  coordinator request is in
  `contract_requests/repository_snapshot_export.md`.
- Database randomized coverage is intentionally bounded (20 events) for the
  normal live suite. The existing 100,000-event in-memory differential result
  is separate evidence; no 100,000-event PostgreSQL claim is made.
- pgvector is installed and versioned but unused by the M2 direct-witness SQL
  schema; vector storage/search belongs to later AI-pipeline work.
- The plans come from a one-row fixture and establish correctness/index use,
  not break-even behavior or production latency.

## Integration notes

The lane started at commit `5e5181b3920a5ed548f12880e280fe4b869c05aa`
on `workstream/postgres`. Only paths assigned to the PostgreSQL lane were
changed. Rebase on the coordinator's current `main`, rerun the live commands,
and merge this lane before updating shared roadmap/decision documents or
headline M2 claims.
