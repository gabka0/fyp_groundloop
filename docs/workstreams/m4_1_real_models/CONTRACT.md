# M4.1 Real-Model Adapter Contract

## Owned boundary

This lane implements empirical M4 adapters only:

- `M4BgeRoleAdapter` converts immutable claims to prefixed BGE query vectors
  and inserted chunks to unprefixed BGE passage vectors;
- `M4CalibratedVerifierAdapter` converts canonical `PairKey` inputs to the
  existing M3 evidence-first verifier and returns calibrated score artifacts;
- `PinnedM3ReuseConfig` and `build_pinned_m3_adapters` validate and construct
  the real local-only adapters.

The lane does not issue SQL, open or close epochs, expand jobs, publish state,
regenerate answers, retrain the verifier, train an impact retriever or
download artifacts.

## Embedding interface

Input:

- `ClaimEmbeddingInput(claim_id, text)` for registered claims;
- existing immutable M3 `ChunkDraft` records for inserted chunks.

Output:

- `ClaimVectorArtifact`, containing a `ClaimRoleVector` and complete immutable
  role provenance;
- `ChunkVectorArtifact`, containing a `ChunkRoleVector` and complete immutable
  role provenance.

Claim inputs are exactly:

```text
Represent this sentence for searching relevant passages: {claim text}
```

Chunk inputs are exactly the chunk text without a prefix. Every artifact binds
the model artifact, model revision, tokenizer revision, role-template hash,
exact input hash, exact vector hash, adapter-spec hash and truncation audit.
The real spec additionally binds the inspected local BGE snapshot tree hash.

Outputs are sorted by subject ID. Exact repeats are served from the adapter
cache. Reusing an immutable subject ID with different role input is an
`ArtifactConflictError`.

## Verification interface

Input is `PairVerificationInput`, which binds a shared M4 `PairKey` to exact
claim text, claim metadata, document/chunk identity, chunk text hash and
chunker artifact. `to_m3_pair()` constructs the existing M3 `AtomicClaim` and
`ChunkDraft`; the M3 verifier continues to receive evidence as premise and
claim as hypothesis.

Output is `PairVerificationArtifact`, containing:

- the unmodified M3 `VerificationResult`;
- raw base-order logits when supplied by the M3 adapter;
- calibrated GroundLoop-order support/refute/neutral scores;
- exact pair-input and execution-spec hashes;
- the frozen decision-policy identity and threshold-derived operational label;
- a content-derived artifact ID.

When raw logits are present, the adapter recomputes temperature-scaled scores
and rejects a mismatch. The operational label uses `groundloop.policy.decide`;
it is never raw argmax. The adapter can produce an M4 `PairJudgment` and a
score-only `SemanticObservation` without storing a permanent label in the
observation.

Inputs are sorted by `PairKey` and partitioned into deterministic batches.
Exact repeated pairs are reused without another model call. Changed immutable
input under the same `PairKey` and execution spec is rejected.

## Integration assumptions

1. The application supplies immutable claim and chunk records already
   validated against the M3/M4 registries.
2. The coordinator persists returned artifacts transactionally with the
   corresponding verifier job completion and observation.
3. Process-local cache reuse is an optimization, not the durable reuse
   authority. PostgreSQL content validation remains authoritative across
   processes and restarts.
4. Candidate rank/channel provenance remains admission-owned. This adapter
   receives only the already admitted pair.
5. The adapter does not decide whether a late result is active. The runtime
   owns `COMPLETED_ACTIVE` versus `COMPLETED_INACTIVE`.
6. The adapter does not claim that a neural score or operational label is
   objectively correct.
