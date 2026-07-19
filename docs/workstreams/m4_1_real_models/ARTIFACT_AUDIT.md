# M4.1 Reused-M3 Artifact Audit

Audit date: 2026-07-19

## Verdict

The pinned M3 embedding and calibrated-verifier artifacts required by this
lane are present in the main GroundLoop worktree and passed a local-only M4
adapter smoke. They are not copied into this Git worktree and are not tracked
by Git. No model or dataset was downloaded, generated, trained or modified by
this lane.

## Exact availability

Artifact root inspected:

`/home/kassym/Desktop/groundloop`

| Artifact | Inspected path | Required identity | Result |
|---|---|---|---|
| BGE cache | `models/m3/huggingface-cache` | `BAAI/bge-small-en-v1.5` at `5c38ec7c405ec4b44b94cc5a9bb96e735b38267a` | available |
| BGE sentence-transformers snapshot | `models/m3/huggingface-cache/models--BAAI--bge-small-en-v1.5/snapshots/5c38ec7c405ec4b44b94cc5a9bb96e735b38267a` | tree SHA-256 `22ad3b4f1d45d362fed00289e1d5ae2910e566a044767adb33865a9e98863b40` | exact match |
| Adapted verifier checkpoint | `models/m3/verifier-run-20260718/checkpoints/minilm2-m3-bounded-v1` | tree SHA-256 `81870b683cec57eff82665103fcff3a35f45b9c9be0e18b53dcd40f485bfa4cf` | exact match |
| Verifier weights | checkpoint `model.safetensors` | SHA-256 `81c49c30048dcbcc9fb2895b56622f702b4aa9894e7d055f28bb520c09f74e3e` | exact match |
| Temperature calibration | `models/m3/verifier-run-20260718/reports/temperature_calibration.json` | file SHA-256 `d0ec9d23ded61fbcfaccc550e0486ce89845685abb4f27a0ca20e22d4934b873` | exact match |

The calibration file records development-only fitting, temperature
`1.1037657679769346`, and calibration version
`temperature-v1:6ae200db8d75477da143bd6d8d6c8927cfdfdbd8995932bbc1c59ce67e090727`.
The checkpoint training manifest records the frozen base model
`cross-encoder/nli-MiniLM2-L6-H768` at revision
`b95119ce93d3e065de6214e38cd4a97b0f2f2c6d`.

## Smoke evidence

Executed from this isolated worktree:

```bash
GROUNDLOOP_RUN_M4_REAL_MODEL_SMOKE=1 \
GROUNDLOOP_M3_ARTIFACT_ROOT=/home/kassym/Desktop/groundloop \
.venv/bin/pytest -q \
  tests/m4/models/test_config_and_real_smoke.py::test_pinned_local_artifact_smoke
```

Result: one test passed in 9.4 seconds on the observed run. It loaded only
local artifacts, produced a 384-dimensional prefixed claim vector, a
384-dimensional unprefixed passage vector and one calibrated three-way
verification artifact. Repeating the identical pair reused the exact adapter
artifact and did not issue a second verifier call.

This establishes artifact availability and adapter wiring only. It is not an
admission-recall, verifier-quality, calibration-transfer or latency result.

## Drift policy

`configs/m4/models/m3_reuse_v1.json` freezes all identities above plus the
M3 decision thresholds and premise/hypothesis role template. The production
factory rejects:

- model, checkpoint, base revision or tokenizer drift;
- BGE query-prefix, dimension or local snapshot-tree drift;
- verifier prompt or evidence/claim role drift;
- checkpoint-tree or model-file drift;
- calibration file, version, fit split or temperature drift;
- decision-policy version, threshold or tie-rule drift;
- changed text under an already bound immutable claim, chunk or `PairKey`.

The opt-in smoke skips when the explicit opt-in flag is absent. Once opted in,
missing named local artifacts are the only expected skip condition; identity
or payload drift fails the test.
