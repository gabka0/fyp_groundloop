# M4.5 Model Application-Port Contract

## Owned boundary

This lane connects the already frozen M4 model adapters to persistence-neutral
application interfaces. It owns no SQL, schema, epoch transition, admission
policy, publication rule, or model download.

## Verification input resolution

`PairVerificationInputResolver.resolve_pair_input(epoch_id, pair)` is the
canonical boundary for resolving one persisted `PairKey` to the immutable
claim/chunk text and metadata required by `PairVerificationInput`.

The verification port calls the resolver on every application invocation,
including exact replay. This is intentional: a service-level result cache must
not conceal text or registry drift. The lower `M4CalibratedVerifierAdapter`
still reuses an exact pair artifact without another neural call.

## Verification application port

`M4VerificationApplicationPort`:

1. accepts only a positive epoch and a non-expandable `VERIFY_PAIR` job;
2. requires the job execution hash to equal the adapter's full pinned
   execution-spec hash;
3. binds the epoch/job identity, including the job payload hash;
4. rejects a resolver result whose `PairKey` differs from the job;
5. invokes the calibrated M4 adapter and validates the returned pair and
   execution identity;
6. converts the exact artifact to a score-only `SemanticObservation` using the
   pinned model revision and prompt version;
7. returns `application.VerificationResult` with the exact
   `PairVerificationArtifact.artifact_id` and a separate domain-separated hash
   covering every persisted artifact/result field.

`artifact_by_id` exposes the full immutable pair artifact after successful
execution so coordinator-owned persistence can archive raw logits and complete
execution provenance without reconstructing data or reaching into adapter
internals.

The port does not decide whether the observation is active. The application
and runtime retain ownership of active versus inactive completion.

Counters have explicit meanings:

- `request_count`: calls reaching logical-job validation/binding;
- `resolver_call_count`: actual resolver calls;
- `successful_result_count`: successfully returned application results;
- `new_artifact_count` / `reused_artifact_count`: successful process-local
  content-addressed artifact outcomes;
- `backend_pair_calls`: pairs actually sent through the adapter's neural
  backend.

## Admission embedding service

`M4AdmissionEmbeddingService.embed_for_admission` accepts claim inputs and
chunk drafts in arbitrary order. It returns `AdmissionEmbeddingArtifacts` in
sorted, unique subject order. The result retains complete role provenance and
also exposes `claim_vectors` and `chunk_vectors` directly for exact, HNSW, or
lexical/vector-fusion admission implementations.

Exact replay invokes the role adapter but reuses its cached immutable
artifacts. Reusing a claim or chunk ID with changed text remains an
`ArtifactConflictError`. Service counters distinguish new and reused claim and
chunk artifacts; durable reuse remains a persistence responsibility.

## Scientific boundary

The result hash makes model execution auditable and replayable. It does not
make neural scores exact or objectively true. Exact GroundLoop maintenance
begins only after the returned immutable observation is transactionally
accepted by the observation-application port.
