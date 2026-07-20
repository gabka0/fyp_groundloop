# M4 Fresh Frontier Retrieval Handoff

Status: isolated production component complete; coordinator integration pending

Branch: `workstream/m4-fresh-frontier`

Baseline: `fd5c0610c5c38c897f24945a43dd08840ebf2f0e`

## Ownership observed

This lane edited only:

- `src/groundloop/m4/admission/fresh.py`
- `tests/m4/admission/test_postgres_fresh.py`
- `docs/workstreams/m4_fresh_frontier/HANDOFF.md`

It did not edit the admission service, application, pipeline, persistence,
migrations, shared contracts, or exports.

## Delivered interface

`FreshFrontierRetriever` is the typed service boundary. The production
implementation is `PostgresExactFreshFrontierRetriever`, which accepts:

- one M4 epoch and claim;
- the complete `CandidatePolicyManifest`;
- a requested positive limit;
- a set or sequence of excluded chunk-version IDs.

It returns `FreshFrontierRetrieval`, containing exact ranked candidates,
canonical exclusions, the selected claim artifact, policy/model/role-template
identities, active and compatible artifact counts, missing active chunks, an
explicit artifact-completeness flag, and a replay hash.

The query is brute-force cosine distance over
`groundloop_m4_role_embedding_artifact` joined to
`groundloop_m4_effective_chunk_version`. Final order is `(distance ASC,
chunk_version_id ASC)`. It does not use or claim ANN behavior.

## Correctness behavior

- The retriever requires a psycopg autocommit connection and opens one explicit
  repeatable-read, read-only transaction for a consistent database snapshot.
- It read-backs the registered candidate policy and the epoch's policy before
  reading artifacts, and requires the claim to belong to the policy's frozen
  claim-registry snapshot.
- A claim must have exactly one query-role artifact compatible with the
  policy's embedding model and claim-role template. Missing or ambiguous claim
  artifacts fail.
- Every active chunk is audited, including excluded chunks. The result is
  incomplete if any active chunk lacks a compatible passage-role artifact.
- Multiple compatible passage artifacts for one active chunk fail rather than
  creating duplicate or arbitrarily selected candidates.
- Excluded IDs and missing IDs are canonical sorted unique tuples and enter the
  result hash.
- The artifact hash also binds the epoch, policy/hash, model, role templates,
  claim artifact, limit, completeness counts, missing IDs, exact ordered
  candidates, distances, scores, and passage artifact hashes.

`complete` means embedding-artifact coverage of the active epoch, not semantic
completeness or retrieval quality.

## Coordinator integration assumptions

The existing frontier service can import the new module directly; this lane
did not modify `admission/__init__.py` under its path restriction. Integration
should:

1. call reserve planning first;
2. invoke `FreshFrontierRetriever.retrieve` only when mandatory fresh retrieval
   is required;
3. pass all current verified, queued, and otherwise disallowed chunks in
   `excluded_chunk_ids`;
4. refuse to close the fallback or seal when `complete` is false;
5. translate returned candidates to frontier channel hits/admitted pairs in
   deterministic result order;
6. persist the result hash and exact candidate declaration atomically with
   expandable-parent completion.

This component intentionally does not persist results or mutate frontier state;
those actions belong to the coordinator's atomic discovery-completion work.

## Validation

Executed from this worktree with the main repository virtual environment:

```bash
/home/kassym/Desktop/groundloop/.venv/bin/ruff check \
  src/groundloop/m4/admission/fresh.py \
  tests/m4/admission/test_postgres_fresh.py
```

Result: all checks passed.

```bash
/home/kassym/Desktop/groundloop/.venv/bin/mypy --strict \
  src/groundloop/m4/admission/fresh.py
```

Result: success, no issues in one source file.

```bash
set -a
source /home/kassym/Desktop/groundloop/.env
set +a
GROUNDLOOP_TEST_DATABASE_URL="$GROUNDLOOP_DATABASE_URL" \
  /home/kassym/Desktop/groundloop/.venv/bin/pytest -q \
  tests/m4/admission/test_postgres_fresh.py
```

Result: 4 passed against four separate unique live PostgreSQL schemas. The
tests cover deterministic tie order, exclusions, incomplete-to-complete
artifact coverage, exact replay hashes, ambiguous claim artifacts, ambiguous
chunk artifacts, and persisted policy drift.

The complete admission test directory was then run with the same live database
configuration: 32 tests collected, 32 passed in 1.55 seconds.

Repository-wide static gates on this branch also passed:

- `ruff check .`: all checks passed;
- `mypy --strict src`: no issues in 103 source files;
- `python -m compileall -q src tests`: passed;
- `pip check`: no broken requirements.

## Limitations

- This is an exact bounded fallback suitable for correctness and small CORE
  histories, not a claim of scalable exhaustive retrieval.
- It assumes M4's serialized epoch rule; it does not arbitrate concurrent
  structural writers.
- It requires passage embeddings to have been materialized by another
  component. It reports missing coverage and never invokes an embedding model.
- No semantic recall, verifier quality, latency, or speedup conclusion follows
  from these wiring tests.
