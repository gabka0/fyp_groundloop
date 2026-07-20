# M4.12 Lane Handoff

## Ownership respected

This lane adds only:

- `src/groundloop/m4/public_ai_gate.py`
- `experiments/m4_public_ai_gate/`
- `tests/m4/public_ai_gate/`
- `docs/workstreams/m4_12_public_ai_gate/`
- `configs/m4/public_ai/`

No shared contract, migration, CLI, M3 artifact, model weight, dataset, or
generated report was modified or committed.

## Interface assumptions

- The M3 checkpoint tree and calibration artifacts remain exactly the hashes
  in `vitaminc_revision_gate_v1.json`.
- The BGE cache resolves the exact frozen revision and snapshot tree hash.
- The caller supplies a detached official VitaminC repository, the exact
  content-addressed archive, and its extracted `vitaminc/` directory.
- Ordinary tests are download-free and model-free. Real execution is explicit.
- The output directory is external/generated and may be deleted without
  changing repository state.

## Coordinator integration

Cherry-pick the lane commit, then run:

```bash
.venv/bin/pytest -q tests/m4/public_ai_gate
.venv/bin/ruff check .
.venv/bin/mypy --strict src
.venv/bin/python -m compileall -q src tests experiments
```

The exact real command and source-acquisition commands are in `README.md`.
The coordinator should describe this as a change-sensitive component-quality
gate, not M4 end-to-end closure. The main actionable conclusion is to evaluate
a bounded verifier adaptation before claiming strong neural quality.

## Consumed audit-set rule

The 128-page M4.12 sample has now been scored. It must not be used for neural
model selection. Any adaptation stage must freeze a different page-disjoint
reserve before the first adapted model is evaluated.
