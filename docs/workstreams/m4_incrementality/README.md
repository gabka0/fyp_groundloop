# M4.1d Physical-Incrementality Acceptance Lane

Status: implementation handoff

Owner: `workstream/m4-incrementality`

## Path ownership

This lane owns only:

- `tests/m4/incrementality/**`
- `docs/workstreams/m4_incrementality/**`

It does not edit production source, migrations, existing tests, configuration,
or shared project documents.

## Acceptance question

The existing M4 correctness suite proves logical equality.  It does not by
itself prove that a production event avoids full recomputation and
snapshot-wide writes.  M4.1d adds a black-box PostgreSQL gate for that distinct
claim.

The fixture contains 64 registered one-claim answers and 128 unrelated active
chunks.  One inserted chunk affects exactly one claim and one answer.  The gate
requires:

1. measured event execution makes zero inline Python/SQL grounding-oracle
   calls;
2. the working overlay writes only the changed claim and answer;
3. global discovery PENDING is represented by one default row and zero
   per-object rows at the external-worker boundary;
4. publication versions only the changed claim and answer, leaving every
   unchanged validity interval open from the original epoch;
5. active-chunk work is bounded by logical jobs, not active-corpus size; and
6. a separate explicit full audit still proves incremental/Python/SQL/effective
   equality without changing measured-event accounting.

## Production contract consumed

The coordinator owns and supplies:

```python
PostgresM4ApplicationPorts(
    ...,
    execution_mode=M4ExecutionMode.MEASURED,
)

ports.audit_grounding_exactness(epoch_id) -> None
```

The durable counter row is keyed by `epoch_id` in
`groundloop_m4_execution_accounting` and exposes:

- `execution_mode`;
- `inline_grounding_oracle_calls`;
- `working_claim_rows_written`;
- `working_answer_rows_written`;
- `evaluation_default_rows_written`;
- `evaluation_override_rows_written`;
- `active_chunk_rows_examined`;
- `published_claim_versions_written`; and
- `published_answer_versions_written`.

Compact PENDING uses one row in `groundloop_m4_evaluation_default`.  Only
exceptions to that default may appear in `groundloop_object_evaluation`.
Working state uses sparse overlay tables plus
`groundloop_m4_effective_working_claim_state` and
`groundloop_m4_effective_working_answer_state`.

If the measured-mode API has not yet landed, only the API-presence case is
expected to xfail.  A missing database URL is an environment skip, not a
correctness success.

## Run

```bash
set -a
source .env
set +a
GROUNDLOOP_TEST_DATABASE_URL="$GROUNDLOOP_DATABASE_URL" \
  .venv/bin/pytest -q tests/m4/incrementality
```

The lane makes no latency, verifier-quality, ANN, or asymptotic novelty claim.
It accepts only the narrow physical statement observable in the counters and
validity intervals above.
