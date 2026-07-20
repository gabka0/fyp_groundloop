# M4.5 Real-Model PostgreSQL Smoke Handoff

Status: implemented in an isolated worktree; coordinator integration pending

## Owned paths

- `src/groundloop/m4/smoke.py`
- `scripts/run_m4_real_postgres_smoke.py`
- `tests/m4/integration/test_real_postgres_models.py`
- `docs/workstreams/m4_real_pg_smoke/HANDOFF.md`

No existing source, migration, configuration, shared test, or shared document
was changed.

## What the smoke proves

The opt-in harness creates a unique disposable PostgreSQL schema and executes
one bounded M4 insertion through the production artifact, exact pgvector,
PostgreSQL lexical-v1, admission, verifier, application, equality-gate and
publication implementations. It checks:

- one real BGE passage embedding and one calibrated MiniLM verifier call;
- a SEALED epoch with a closed discovery root and terminal verifier child;
- Surface A equality between incremental/Python (inside the application), SQL
  full recomputation, working materialization and strict publication;
- Surface B sealed runtime state, exact job attempts and closed child scope;
- Surface C complete/confirmed claim and answer evaluation rows;
- durable claim-query and chunk-passage role artifacts;
- durable channel hits, admitted pair, discovery header, raw-logit verifier
  execution and typed model judgment;
- a distinct PostgreSQL connection plus newly constructed adapter/port graph
  replaying the same sealed event with zero admission embedding requests, zero
  verifier requests, zero backend pair calls, unchanged epoch revision and
  unchanged durable row and artifact counts.

The output is a small JSON manifest containing exact artifact/execution IDs,
server versions, state labels and integer counters. The manifest explicitly
labels the run as a bounded integration smoke, not a model-quality result.

## Reproduction

From the repository root, with the local M3 artifacts already installed:

```bash
set -a
source .env
set +a
export PYTHONPATH=src
export GROUNDLOOP_M3_ARTIFACT_ROOT=/home/kassym/Desktop/groundloop

.venv/bin/python scripts/run_m4_real_postgres_smoke.py \
  --output /tmp/groundloop-m4-real-postgres-smoke.json
```

The test is deliberately opt-in:

```bash
GROUNDLOOP_RUN_M4_REAL_POSTGRES_SMOKE=1 \
GROUNDLOOP_ARTIFACT_ROOT=/home/kassym/Desktop/groundloop \
.venv/bin/pytest -q \
  tests/m4/integration/test_real_postgres_models.py
```

Without the opt-in flag, the expensive integration test skips. Once requested,
a missing DSN or missing hash-valid local artifact skips because the required
external fixture is absent; artifact drift after startup, schema failure,
semantic mismatch and replay work are hard failures.

## Interface note

`PostgresHybridAdmissionPort` returns complete role artifacts through its
embedding service but does not own their durable registry. The smoke therefore
composes a narrow persistence wrapper around `M4AdmissionEmbeddingService`.
It stores each immutable role artifact after inference and before discovery
closure while leaving channel hits, admitted pairs, jobs and semantic state to
the production application. If the coordinator later adds a general
production role-artifact writer, this wrapper should be replaced rather than
duplicated.

`build_candidate_policy_manifest()` currently hashes role templates as a
namespaced `stable_m4_digest`, while durable role artifacts and
`PostgresM4ArtifactRegistry.seed_claim_admission_index()` require the raw
`sha256_text(template)`. This runner constructs the otherwise frozen manifest
directly from `ExactPgvectorConfig` and the durable registry identities. The
coordinator should fix the generic builder before making it the sole production
manifest factory; this lane did not edit the coordinator-owned implementation.

## Non-claims

- This does not measure retrieval recall, verifier accuracy, calibration
  transfer, latency, throughput or cost.
- It does not show that the neural judgment is objectively correct.
- It exercises insertion and sealed replay only. Delete/replacement histories
  remain covered by deterministic M4 tests and need a separate bounded
  real-history evaluation if promoted to an empirical claim.
- Exact pgvector is used intentionally; no HNSW recall claim follows.

## Validation evidence

Executed in this isolated worktree against PostgreSQL 16.14 and pgvector
0.8.5:

```text
opt-in real-model PostgreSQL test: 1 passed
tests/m4: 236 collected, 233 passed, 3 explicitly gated real-model tests skipped
focused pipeline + role-artifact + smoke set: 26 passed, 1 opt-in skip
ruff check .: passed
mypy --strict src: passed over 103 source files
compileall src tests scripts experiments training: passed
pip check: no broken requirements
```

The direct CLI also completed and wrote a JSON manifest. Its bounded run used
one admission embedding request and one verifier backend pair call; the
distinct-session replay used zero of each. It stored two role embeddings, two
channel hits, one admitted pair, one discovery result, one verification
execution and one typed pair judgment, then left every artifact count
unchanged on replay.
