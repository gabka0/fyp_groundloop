# M4.1 Real-Model Adapter Handoff

Status: lane implementation complete; coordinator integration pending

Code commit: `8b24384d4215edbe42c31799061265d1b22458ba`

## Delivered

- Strict frozen config for reuse of M3 BGE and the fine-tuned calibrated M3
  verifier.
- Role-correct claim-query and chunk-passage adapters with complete immutable
  provenance.
- Canonical `PairKey` to M3 verifier adaptation, deterministic batching,
  score/logit calibration conformance, threshold-derived labels and exact
  process-local reuse.
- Conflict detection for model, tokenizer, prompt, calibration, policy and
  immutable input drift.
- Download-free fake/conformance tests and an opt-in real local-artifact
  smoke.
- Exact artifact availability audit and post-M4.1 real execution plan.
- One coordinator-owned persistence contract request; no shared file was
  changed by this lane.

## Validation

```bash
.venv/bin/pytest -o addopts='' -q tests/m4/models
# 16 passed, 1 skipped

.venv/bin/pytest -o addopts='' -q \
  tests/ai tests/m4/admission tests/m4/models tests/m4/test_m4_contracts.py
# 128 passed, 4 skipped

.venv/bin/ruff check src/groundloop/m4/models tests/m4/models
# All checks passed

.venv/bin/mypy --strict src/groundloop/m4/models
# Success: no issues found in 5 source files

.venv/bin/python -m compileall -q \
  src/groundloop/m4/models tests/m4/models
# exit 0
```

The real opt-in smoke also passed with the current ignored M3 artifacts. See
`ARTIFACT_AUDIT.md` for the exact command, paths and hashes.

## Integration sequence

1. Resolve
   `contract_requests/M4_VERIFICATION_PERSISTENCE.md` in a coordinator-owned
   migration/persistence implementation.
2. Merge this lane after persistence and deterministic application lanes, per
   the M4.1 merge plan.
3. Map the application claim/chunk records into `ClaimEmbeddingInput`,
   `ChunkDraft` and `PairVerificationInput`; do not move epoch logic into this
   module.
4. Persist role artifacts and pair verification atomically with the matching
   job/observation state.
5. Run the deterministic integration history before the real dynamic history.

## Limitations

- Cache reuse is process-local in this lane. Durable cross-process reuse is a
  persistence responsibility.
- The adapter does not provide HNSW, lexical fusion, epoch orchestration,
  publication or exact withdrawal.
- The successful real smoke is not a neural quality evaluation.
- No answer was regenerated and no model was retrained.
