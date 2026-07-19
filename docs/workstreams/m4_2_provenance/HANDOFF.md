# M4.2 Durable Execution Schema Handoff

Status: durable schema and lease/replay correctness follow-up complete;
coordinator integration pending

## Lease-attempt and completion-replay follow-up

The coordinator explicitly expanded this lane's ownership for one correctness
follow-up over the M4 application, PostgreSQL runtime, composition pipeline and
their lane-local/integration tests. The implementation now enforces:

- every executable `JobLease` carries the exact attempt ID, hashed lease token
  and acquisition revision;
- the acquisition revision is a lower bound, while the transaction's current
  epoch revision remains the compare-and-swap revision, because other jobs may
  legitimately advance the same serialized epoch after a lease is issued;
- a fresh completion must name the latest persisted `leased` attempt and exact
  token, and updates only that row to `completed`;
- an older attempt is rejected after a retry rather than being attributed to
  the newest attempt;
- failed epochs can still archive the result of their exact already-running
  attempt as the frozen `COMPLETED_INACTIVE` path requires;
- exact completion replay requires the bound attempt already to be
  `completed`; expansion replay performs no evaluation rewrite;
- active verifier replay read-validates the complete immutable observation,
  historical-base working delta, installed completion revision, optional real
  model provenance and structured-state equality without reinserting the
  immutable delta or advancing epoch state;
- replay returns `ObservationCompletionReceipt(False, False)` because it
  stores and activates nothing in the replay transaction.

The follow-up deliberately does not change structural overlay/failure
semantics, discovery/admission atomicity, persisted-hit replay, or Surface-C
sealing. Those remain coordinator-owned defects outside this task.

## Ownership observed

This lane changed only:

- `migrations/005_m4_durable_execution.sql`;
- `tests/postgres/test_m4_durable_execution_schema.py`;
- `docs/workstreams/m4_2_provenance/**`.

It did not edit Python source, an earlier migration, the CLI, shared domain
contracts, admission algorithms or another lane's tests.

## Delivered relations

### Complete working structured state

`groundloop_m4_working_claim_state` and
`groundloop_m4_working_answer_state` contain the complete per-object M2 state
for one M4 epoch. The claim row additionally stores the full certificate
digest. Their primary keys are `(epoch_id, object_id)` and the epoch foreign
key targets `groundloop_m4_update`, not merely any historical epoch.

Working rows can change while the event runs. A trigger prevents key changes,
revision regression, changed content at the same revision, and every mutation
after the epoch fails or seals. This is intentionally different from the
immutable observation overlay: structured state is a maintained view, while
observations and execution artifacts are history.

### Production claim-admission population

`groundloop_m4_claim_admission_index` is the unqualified relation required by
the existing PostgreSQL admission adapters. It stores exactly the frozen
registry, claim, embedding model, claim-role template, role-input, 384-vector
and lexical identities. The vector must be L2-normalized and the lexical value
must equal PostgreSQL `to_tsvector('simple', claim.text)`.

The physical indexes are:

```text
groundloop_m4_claim_admission_lexical_gin
groundloop_m4_claim_admission_hnsw
  vector_cosine_ops, m=16, ef_construction=64
```

Rows are immutable. The HNSW adapter remains responsible for its existing
whole-population check that exactly one registry/model/role population is
loaded. The coordinator must build the candidate-policy manifest with the
same physical HNSW options or deliberately rebuild the index and register a
different physical policy; it must not pretend a different manifest matches
this index.

### Dynamic verifier execution provenance

`groundloop_m4_verification_execution` resolves the real-model lane's contract
request without inventing an M3 static pipeline run or retrieval candidate.
Each immutable row binds one semantic observation to one `VERIFY_PAIR` job,
one admitted pair, registered verifier model and prompt artifacts, exact
execution-spec and pair-input hashes, calibration identity and file hash,
positive finite temperature, exactly three finite raw logits, raw-output
hash, and an optional reused observation.

The insert validator additionally requires:

- the job kind is `verify_pair`;
- job and admitted pair agree on epoch, policy, claim and chunk;
- the execution-spec hash equals the logical job's frozen hash;
- observation and admitted pair agree on claim and chunk;
- the observation's raw-output hash equals the execution row;
- referenced model and prompt artifacts have task `verification`.

The pair-input hash is deliberately not equated to
`groundloop_semantic_observation.input_hash`. The former hashes the complete M4
pair record; the latter is the reused M3 verifier's own model-input hash. Both
are required for complete provenance and they have different semantics.

## Coordinator integration assumptions

1. Install migration 005 after 004; the existing migration loader's sorted
   order already does this.
2. Persist the semantic observation, M4 verification execution row, working
   observation delta, working structured state and terminal verifier-job
   transition in one transaction.
3. On exact replay, read and content-validate the immutable execution row or
   use `ON CONFLICT DO NOTHING` followed by that comparison. Do not issue an
   `ON CONFLICT DO UPDATE`: the immutability trigger correctly rejects it.
4. Use the M4 pair-verification artifact ID/hash as the semantic job's generic
   result identity. Migration 005 does not duplicate that generic runtime
   field.
5. Before seal, compare both working-state tables to the independent Python
   and SQL full-recomputation outputs. The schema enforces row integrity, not
   global completeness of the claim/answer population.
6. A reuse source is an immutable semantic-observation FK. The coordinator
   must content-validate reusable execution provenance before setting it; the
   FK alone does not assert semantic equivalence.

## Validation evidence

Executed against live PostgreSQL in unique temporary schemas:

```bash
GROUNDLOOP_TEST_DATABASE_URL="$GROUNDLOOP_DATABASE_URL" \
  .venv/bin/pytest -o addopts='' -q \
  tests/postgres/test_m4_durable_execution_schema.py
# 4 passed

GROUNDLOOP_TEST_DATABASE_URL="$GROUNDLOOP_DATABASE_URL" \
  .venv/bin/pytest -o addopts='' -q tests/postgres
# 29 passed

.venv/bin/ruff check tests/postgres/test_m4_durable_execution_schema.py
# All checks passed

.venv/bin/ruff format --check \
  tests/postgres/test_m4_durable_execution_schema.py
# 1 file already formatted

git diff --check
# clean
```

The tests inspect exact column order, all admission index access methods and
HNSW reloptions, verifier foreign-key count, working-state checks and terminal
guards, role-safe immutable admission rows, finite three-logit enforcement,
job/pair/execution binding and immutable verifier provenance.

Follow-up validation from the rebased `workstream/m4_2-provenance` branch:

```bash
.venv/bin/pytest -q tests/m4/application/test_application.py
# 15 passed

set -a; source .env; set +a
GROUNDLOOP_TEST_DATABASE_URL="$GROUNDLOOP_DATABASE_URL" \
  .venv/bin/pytest -q \
  tests/m4/persistence/test_runtime_store.py \
  tests/m4/integration/test_postgres_pipeline.py
# 21 passed

.venv/bin/ruff check \
  src/groundloop/m4/application.py \
  src/groundloop/m4/persistence.py \
  src/groundloop/m4/pipeline.py \
  tests/m4/application tests/m4/persistence tests/m4/integration
# All checks passed

.venv/bin/mypy --strict src
# Success: no issues found in 101 source files

.venv/bin/python -m compileall -q \
  src/groundloop/m4 tests/m4/application tests/m4/persistence \
  tests/m4/integration
# passed
```

## Limitation

This migration supplies the durable relational prerequisites; it is not the
coordinator transaction implementation. It makes no neural-quality, HNSW
recall, call-saving or latency claim.
