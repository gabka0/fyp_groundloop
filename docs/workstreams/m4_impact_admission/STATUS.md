# M4 Impact-Admission Lane Status

Status: **Wave 2 PostgreSQL admission boundaries complete; ready for integration**

Updated: 2026-07-19

Wave 2 base: integrated `main` commit
`b3623feb36f719ef8715e6011e92c1a865530d7d`

## Wave 2 delivered

- A runtime identity inspection binding the exact PostgreSQL version,
  pgvector extension version and resolved `simple` regconfig.
- A real PostgreSQL lexeme analyzer using parameterized
  `to_tsvector(%s::regconfig, %s)` and `tsvector_to_array`.
- A real lexical backend that constructs an OR `tsquery` inside PostgreSQL
  from a parameterized lexeme array, searches a GIN-indexable `tsvector`,
  evaluates `ts_rank_cd(..., 32)`, and returns deterministic
  `(score DESC, claim_id ASC)` order.
- Full lexical-registry validation against the frozen in-memory IDF snapshot:
  claim IDs and actual server lexemes must match, not merely the row count.
- A pgvector exact reference that computes every filtered cosine distance in
  a materialized CTE, then sorts by `(distance, claim_id)`. The live plan does
  not use the HNSW index.
- An explicitly approximate pgvector HNSW adapter. It validates the physical
  access method, cosine operator class, dimension, explicit `m` and
  `ef_construction`, and hashes `pg_get_indexdef` into query provenance.
- Complete recorded HNSW search settings: `ef_search`, iterative-scan mode,
  maximum scan tuples, scan-memory multiplier, candidate-pool rule, distance
  and tie rule. Settings are installed transaction-locally with parameterized
  `set_config` calls.
- An HNSW population-scope guard: the indexed relation must contain one sealed
  claim-registry/model/role population. This prevents unrelated snapshots from
  silently changing filtered-ANN behavior.
- A two-stage HNSW query: the inner operator-order/limit is indexable; the
  outer result applies deterministic distance/claim ordering to the returned
  approximate set. Rebuild identity is not asserted; replay must use persisted
  channel hits.
- Unique-schema live tests with real GIN and HNSW indexes, exact versus
  in-memory pair-order comparison, HNSW recall measurement, malicious-text
  parameterization coverage, registry-drift rejection, physical-index
  mismatch rejection, and population-isolation rejection.

Live server exercised:

```text
PostgreSQL: 16.14 (Debian 16.14-1.pgdg12+1)
pgvector:   0.8.5
regconfig:  simple
```

The tiny 64-claim, three-dimensional live fixture measured HNSW recall@8 of
`1.0` against the brute-force role-vector reference. This is a wiring and
measurement result, not a semantic-quality, scale or general recall claim.

## Wave 1 retained

- Role-specific normalized claim and inserted-chunk vector DTOs.
- A brute-force exact reverse-vector reference ordered by cosine distance and
  claim ID, with content-addressed query and hit provenance.
- An explicit approximate-index protocol and pair-set recall-at-depth
  measurement hook. Approximate recall is measured against the exact
  reference; it is never inferred from HNSW parameters.
- Strict loading and content addressing of the frozen `lexical_v1.json`
  policy: PostgreSQL `simple`, no stop-word removal, at most 32 distinct query
  lexemes selected by IDF descending then lexeme ascending, OR semantics, and
  `ts_rank_cd(..., 32)` as the real-backend contract.
- A versioned claim-lexeme registry snapshot with canonical sorted/unique
  validation and exact frozen IDF computation.
- Injected lexical analyzer and search-backend protocols. The included fake
  analyzer and overlap backend are deterministic test doubles and explicitly
  do not claim PostgreSQL parsing or ranking equivalence.
- Deterministic `rank-interleave-v1` fusion: VECTOR then LEXICAL round robin,
  pair deduplication at `(claim_id, chunk_version_id)`, one global approximate
  cap per inserted chunk, and all mandatory lineage pairs appended outside
  that cap.
- Preservation of all channel-hit rows and all reasons for an admitted pair,
  while producing one admitted pair and therefore one logical verifier job per
  event-pair key.
- Exact accounting for approximate pairs, mandatory lineage pairs, lineage
  excess, admitted pairs, and the frozen verifier-call upper bound
  `inserted_chunks * L + lineage_excess`.
- Candidate-policy manifest construction binding embedding artifact, role
  templates, exact/approximate index identity and build/search configuration,
  lexical/PostgreSQL identity, claim-registry snapshot/count, fusion version,
  cap, frontier, verifier execution spec, decision policy, and lineage mode.

## Combined test coverage

The admission suite now has 28 test cases, including the parameterized vector
validation cases and four PostgreSQL-adapter tests. It covers:

- vector tie ordering, exact dot-product scores, replay, invalid normalization,
  duplicate claim IDs, and dimensional mismatch;
- explicit ANN recall, empty exact denominators, and mixed-query rejection;
- frozen lexical-config drift, IDF selection, the 32-lexeme cap, empty text,
  no fake stop-word special case, replay, backend-order normalization,
  manifest/registry mismatch, and canonical registry rows;
- rank interleaving, cross-channel pair deduplication, event-pair rather than
  claim-only identity, lineage excess and bound accounting, empty channels,
  conflicting persisted-hit payloads, mixed epochs, invalid ranks, policy
  mismatch, disabled lineage override, and manifest provenance sensitivity.

## Validation evidence

Executed in this worktree with its `.venv` link:

```text
set -a; source /home/kassym/Desktop/groundloop/.env; set +a
.venv/bin/pytest -q tests/m4/admission
28 passed

.venv/bin/pytest -q tests/m4/test_m4_contracts.py tests/m4/admission
37 passed

.venv/bin/ruff check src/groundloop/m4/admission tests/m4/admission
All checks passed!

.venv/bin/mypy --strict src/groundloop/m4/admission
Success: no issues found in 9 source files

.venv/bin/python -m compileall -q \
  src/groundloop/m4/admission tests/m4/admission
PASS: exit 0, no diagnostics
```

Without either database environment variable, exactly the two unique-schema
live tests skip; missing DSN is their only skip path. With the project DSN,
both execute. No test downloads a model or contacts a model hub.

## Deferred work and limits

- No real BGE embedding run or reverse-geometry quality result is claimed.
- No HNSW scale, latency, index-size, rebuild or cross-parameter recall sweep
  has run. One tiny live recall check cannot choose production parameters.
- No real BGE vectors were used. The live vector fixture is three-dimensional
  synthetic geometry and establishes database/query mechanics only.
- No shared migration or coordinator persistence was added. Integration must
  supply the documented claim-admission relation and indexes transactionally.
- No learned impact model was trained. The learned dual-encoder TARGET remains
  blocked on coordinator/oracle delivery of typed teacher and human judgments,
  leakage-safe history-component splits, and fixed-budget evaluation data.
- This lane does not implement persistence, epoch scheduling, logical job
  execution, frontier propagation, semantic oracles, or end-to-end evaluation.

Confidence is **high** for the deterministic Wave 1 contracts and the tested
PostgreSQL query/provenance boundaries; **moderate** for production physical
integration until coordinator migrations exist; **unknown** for reverse-BGE,
large-index HNSW and learned-admission quality until their empirical gates run.
