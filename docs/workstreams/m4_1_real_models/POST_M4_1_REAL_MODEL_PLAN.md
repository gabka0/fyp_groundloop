# Post-M4.1 Real-Model Execution Plan

## Entry gate

Do not start the real dynamic run merely because this lane's local smoke
passes. Entry requires the coordinator's deterministic M4.1 history to pass
all three exact surfaces under insert, delete and replacement, including
injected failure, replay and late inactive completion. The M4 verification
persistence request must also be resolved.

## Stage RM-0: immutable preflight

1. Load `configs/m4/models/m3_reuse_v1.json`.
2. Validate BGE snapshot, verifier checkpoint, training manifest, calibration
   file, prompt role and decision policy against frozen hashes.
3. Require local-only model loading; network access remains disabled.
4. Select one already published M3 answer with all registered claims and its
   last sealed grounding snapshot.
5. Freeze the claim-registry snapshot, candidate-policy manifest, lexical-v1
   manifest, event history and exact exhaustive-audit caps.

Exit: the run manifest can name every artifact before model inference begins.

## Stage RM-1: bounded real insertion

1. Embed every registered claim once using the prefixed claim role. Persist or
   exact-reuse each role artifact.
2. Structurally insert one immutable document version and chunk in an M4 epoch.
3. Confirm strict reads still resolve the previous sealed snapshot and
   provisional reads expose global discovery PENDING.
4. Embed the inserted chunk without a prefix.
5. Run exact reverse-vector reference, frozen HNSW vector search and
   PostgreSQL lexical-v1. Persist raw channel hits before fusion.
6. Fuse and deduplicate admitted pairs using the frozen cap and lineage rule.
7. Atomically close discovery with the complete sorted verifier-child set.
8. Execute the admitted verifier pairs through this adapter outside SQL
   transactions. Complete one child per microtransaction, preserving raw
   logits, calibrated scores and exact execution provenance.
9. After every completion compare grounding, coordination and evaluation
   surfaces with their independent references.
10. Seal once, publishing only the net previous-sealed to new-sealed delta.

Run the exhaustive inserted-chunk by registered-claim audit on the same
bounded event. Any omitted operational SUPPORT/REFUTE pair is an inspected
admission miss, not an IVM correctness failure.

## Stage RM-2: deletion without semantic retrieval

1. Delete the inserted chunk in a new serialized epoch.
2. Assert that neither ANN, lexical search nor the verifier is called for exact
   withdrawal.
3. Enumerate stored reverse candidate/observation edges, invert active
   contributions and repair the frontier as required.
4. Preserve strict reads on the previous snapshot until all required work
   closes and the new snapshot seals.
5. Compare indexed edge visits with a naive full scan, stating dense-fanout
   limitations explicitly.

Exit: exact withdrawal, publication and replay pass with zero new semantic
model artifacts unless mandatory frontier retrieval legitimately fires.

## Stage RM-3: replacement and supersession

1. Replace an old document version with a changed immutable version in one
   structural transaction.
2. Withdraw old-chunk contributions exactly and admit new-chunk pairs through
   the real channels.
3. Verify that a repeated logical subject/chunk/task execution either exact
   reuses its artifact or creates a new immutable execution identity; it never
   overwrites history.
4. Exercise a supporting-to-neutral or supporting-to-refuting score change and
   confirm observation currency, claim state and answer status against all
   exact oracles.
5. Inject failure before expansion, after expansion and during verifier
   completion. The previous sealed snapshot must remain unchanged.
6. Archive a late result whose target became inactive and prove that it emits
   no active-view delta.

## Stage RM-4: exact replay and reproducibility

Replay the identical three-event history from the same sealed M3 seed.
Require:

- identical event, job, role-embedding, verification, observation and
  published-snapshot IDs;
- zero new neural artifacts and zero new verifier calls when the durable cache
  is populated;
- identical raw channel hits and selective pair set from persisted replay
  artifacts, without assuming a rebuilt HNSW index has identical behavior;
- zero mismatches on exact grounding, coordination and evaluation surfaces.

Publish one manifest-linked command and a machine-readable artifact inventory.

## Stage RM-5: M4 CORE evaluation closure

1. Run the frozen controlled histories first.
2. Pilot exhaustive cost and freeze pair/event caps plus timeout accounting.
3. Run vector-only, lexical-only, deterministic union, lineage and frontier
   ablations on identical event IDs.
4. Persist event-level integer numerators and denominators for positive-pair,
   positive-claim, status and answer agreement. Empty denominators remain N/A.
5. Record actual unique verifier calls, tokens, embedding/index work, component
   latency, PENDING exposure, withdrawal edge visits and every miss.
6. Aggregate only after event records are complete and use history-cluster
   bootstrap intervals.

Only this stage can support an empirical recall/work or call-saving claim. One
real smoke cannot.

## Conditional work after CORE

A learned impact retriever is optional and must not delay CORE. It starts only
after exhaustive development judgments and history-component splits are
frozen. Verifier retraining remains a separate conditional experiment and is
justified only if M4 error analysis shows verifier quality, rather than impact
admission, dominates the measured failure budget.
