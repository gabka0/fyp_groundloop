# Contract Request: Durable M4 Verification Execution Provenance

Status: coordinator decision required before real M4 publication

## Blocking example

Migration 003 persists an admitted dynamic pair in
`groundloop_admitted_pair` and can put a generic result ID/hash on
`groundloop_semantic_job`. The only detailed verifier-execution table is the
M3 `groundloop_verification_execution` from migration 002. That table requires:

- an M3 `groundloop_pipeline_run.run_id`; and
- an M3 static `groundloop_retrieval_candidate.candidate_id`.

An M4 `VERIFY_PAIR` job originates from `groundloop_admitted_pair`, not from a
new static M3 pipeline run or static top-k retrieval candidate. Creating fake
M3 runs/candidates solely to satisfy those foreign keys would corrupt the
meaning of both schemas.

The generic job result columns are insufficient by themselves: they cannot
independently recover the model, tokenizer/checkpoint identity, prompt role,
calibration identity, temperature, raw logits, calibrated score triple,
pair-input hash or reuse source needed for an auditable semantic observation.

## Requested coordinator-owned change

Add a migration-owned M4 verifier-execution relation, or generalize the M3
relation without weakening M3 invariants. A dedicated relation is less risky.
Its minimum semantic fields should be:

```text
observation_id                 PK/FK semantic observation
job_id                         UNIQUE/FK M4 VERIFY_PAIR job
admitted_pair_id               FK M4 admitted pair
model_artifact_id              FK model registry
prompt_artifact_id             FK prompt registry
execution_spec_hash            frozen M4 execution identity
pair_input_hash                exact M4 PairVerificationInput hash
calibration_version
calibration_artifact_sha256
temperature
raw_logits[3]
raw_output_hash
reused_from_observation_id     nullable FK semantic observation
```

The score triple remains in the immutable semantic observation. The
operational label should not be stored as permanent observation truth; if an
evaluation label is required, `groundloop_pair_judgment` already binds it to a
decision-policy identity.

The coordinator should content-validate exact replay and reject the same
observation/job identity with different execution provenance. Publication of
the observation, verifier-execution row and terminal job result must be one
transaction.

## Non-request

No change to `PairKey`, `CandidatePolicyManifest`, the M3 verifier, M3 model
weights, admission semantics or the exact structured-maintenance boundary is
requested.
