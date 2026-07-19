# M4 Impact-Admission Lane Status

Status: **Wave 1 deterministic CORE complete; ready for coordinator review**

Updated: 2026-07-19

Contract baseline: `a1059ff63883fa388def0e39986fa4b8393ef709`
(`m4-contract-baseline-2026-07-19`)

## Delivered

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

## Test coverage

The 22 deterministic tests cover:

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
.venv/bin/pytest -q tests/m4/admission
22 passed

.venv/bin/ruff check src/groundloop/m4/admission tests/m4/admission
All checks passed!

.venv/bin/mypy --strict src/groundloop/m4/admission
Success: no issues found in 6 source files

.venv/bin/python -m compileall -q \
  src/groundloop/m4/admission tests/m4/admission
PASS: exit 0, no diagnostics
```

No ordinary test downloaded a model, contacted a model hub, or required a
database.

## Deferred work and limits

- No real BGE embedding run or reverse-geometry quality result is claimed.
- No HNSW adapter, index build, rebuild/replay experiment, scale benchmark, or
  recall measurement has run. The interface and exact comparison hook are now
  available for that experiment.
- No live PostgreSQL lexical analyzer or `ts_rank_cd` adapter has run. Fake
  lexical results test policy plumbing only and must not be reported as
  PostgreSQL equivalence or retrieval quality.
- No learned impact model was trained. The learned dual-encoder TARGET remains
  blocked on coordinator/oracle delivery of typed teacher and human judgments,
  leakage-safe history-component splits, and fixed-budget evaluation data.
- This lane does not implement persistence, epoch scheduling, logical job
  execution, frontier propagation, semantic oracles, or end-to-end evaluation.

Confidence is **high** for the deterministic Wave 1 contracts, ordering,
deduplication and accounting; **unknown** for reverse-BGE, HNSW, PostgreSQL
lexical quality, and learned-admission quality until their explicit empirical
gates run.
