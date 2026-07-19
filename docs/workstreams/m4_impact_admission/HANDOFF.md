# M4 Impact-Admission Lane Handoff

## Public interface

The coordinator can import the Wave 1 surface from:

```python
from groundloop.m4.admission import (
    ChunkRoleVector,
    ClaimRoleVector,
    ExactReverseVectorIndex,
    LexicalRegistrySnapshot,
    LexicalV1Policy,
    build_candidate_policy_manifest,
    fuse_admission_channels,
    lineage_channel_hits,
    load_frozen_lexical_v1,
    measure_ann_recall,
)
```

`ClaimRoleVector` must contain the frozen claim/query-role embedding.
`ChunkRoleVector` must contain the inserted chunk/passage-role embedding. Both
must already be finite, nonempty and L2-normalized, and each carries the
SHA-256 of the exact role-transformed input. This lane intentionally does not
duplicate or alter the M3 model adapter.

`ExactReverseVectorIndex.search(...)` is the brute-force oracle for one
inserted chunk against the registered claim-vector snapshot. It ranks by
`(1 - dot_product, claim_id)` and returns content-addressed `ChannelHit` rows.
An HNSW implementation should satisfy `ApproximateReverseVectorIndex`, then be
compared with `measure_ann_recall` on the same epoch, policy and chunk.

## Lexical integration boundary

Load the frozen checked-in policy with:

```python
lexical_config = load_frozen_lexical_v1(repo_root)
```

Build a canonical `LexicalRegistrySnapshot` from the sealed claim registry.
Rows must be ordered by claim ID; each claim's distinct lexemes must be ordered
lexicographically. The snapshot identity and row count must match the candidate
policy manifest.

`LexicalV1Policy` delegates two database-specific operations:

- `LexemeAnalyzer.analyze(text)` corresponds to
  `to_tsvector('simple', text)` lexeme extraction;
- `LexicalSearchBackend.search(...)` corresponds to a parameterized OR query
  ranked by `ts_rank_cd(..., 32)`.

The production adapter must publish a nonempty artifact identity and preserve
the frozen PostgreSQL version and regconfig in the policy manifest. The
included `DeterministicFakeLexemeAnalyzer` and
`DeterministicFakeLexicalBackend` are ordinary-test doubles only. Their token
boundaries and scores are deliberately not described as PostgreSQL results.

## Fusion semantics

Call `fuse_admission_channels(...)` with sorted unique inserted chunk IDs and
the VECTOR, LEXICAL and optional LINEAGE hits for one event epoch and one
candidate policy.

For each inserted chunk, `rank-interleave-v1` considers VECTOR rank 1, then
LEXICAL rank 1, then VECTOR rank 2, then LEXICAL rank 2, and so on. A pair seen
by both channels occupies one approximate slot. Approximate selection stops at
the manifest's global per-chunk cap `L`; this is not `L` per channel.

All mandatory lineage pairs are then admitted. A lineage pair already selected
approximately does not create another job, but LINEAGE remains in its reasons.
Lineage pairs outside the approximate set are appended deterministically by
claim ID and counted as `lineage_excess`.

The result retains canonical channel hits separately from admitted pairs. The
runtime should persist every channel-hit reason but schedule at most one
logical verifier job for each admitted `(event, claim, chunk)` target. This
lane's `PairKey` is the claim/chunk portion; the coordinator-owned runtime binds
the event identity.

The accounting contract is:

```text
actual approximate pairs <= inserted_chunk_count * L
actual verifier jobs       = admitted_pair_count
verifier-call upper bound  = inserted_chunk_count * L + lineage_excess
```

The upper bound intentionally need not equal the actual call count when a
channel produces fewer than `L` distinct pairs.

## Policy identity

Use `build_candidate_policy_manifest(...)`; do not manually invent a partial
policy ID. Build and search configuration pairs must be sorted by unique key.
The returned shared `CandidatePolicyManifest` content-addresses all frozen
admission provenance, including model and role hashes, index kind/config,
lexical and PostgreSQL identities, registry snapshot/count, fusion/cap,
frontier, verifier execution, decision policy, and lineage override.

Consequently, a change to HNSW parameters, retrieval depth, role template,
PostgreSQL/regconfig, registry snapshot, verifier or threshold policy creates a
different policy hash and cannot silently replay as the old policy.

## Coordinator integration sequence

1. Persist or load the sealed claim registry and its role-specific vectors and
   lexical snapshot under the same manifest identity.
2. For each inserted chunk, obtain its passage-role vector and lexical text.
3. Run vector and lexical channels independently and persist their `ChannelHit`
   rows, including hits that later deduplicate.
4. Construct exact mandatory-lineage hits from coordinator-owned history.
5. Fuse once per event, persist the admitted pairs and accounting, and create
   one logical verifier job per admitted event-pair target.
6. Feed completion results to the coordinator-owned epoch/frontier runtime.
7. Compare approximate vector results to the exact index on a sampled or
   exhaustive frozen evaluation slice; report measured recall rather than an
   algorithmic guarantee.

## Next empirical gates

Wave 2 should add the production PostgreSQL lexical adapter and a reverse HNSW
adapter without changing the deterministic fusion contract. It must record
PostgreSQL/pgvector versions, exact index build/search settings, claim snapshot,
replay/rebuild hashes, ANN recall at the actual candidate depth, latency and
index size. Both channels require comparison against their exact/reference
counterpart.

The learned impact TARGET comes later. It must keep model-teacher and human
judgments typed and separate, split by connected history/lineage components,
exclude final human-test judgments from training/mining/tuning, and compare
methods at equal *actual verifier calls*. It should be abandoned if it does
not improve the fixed-call admission Pareto frontier over deterministic fusion.

## Validation

Targeted Wave 1 result: 22 tests passed; Ruff clean; strict mypy clean for all
6 admission source files; compileall clean. Exact commands and limitations are
recorded in `STATUS.md`.
