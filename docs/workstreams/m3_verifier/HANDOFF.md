# M3 Verifier Lane Handoff

## Interface assumptions

- Evidence is the premise; claim is the hypothesis.
- Base logits are contradiction, entailment, neutral.
- Stored scores are support, refute, neutral.
- Verifier outputs never store a permanent label; the current decision policy
  derives it.
- A local fine-tuned checkpoint requires a logical model ID and tree SHA-256.
  Runtime path relocation cannot change semantic artifact identity.
- Calibration version is content-derived. Temperature and calibration identity
  participate in result provenance and output hashes.
- `VerificationCompletion.should_emit_delta` is false for a result completing
  after chunk deactivation; the result remains auditable.

## Reproduction commands

Use `/home/kassym/Desktop/groundloop/.venv` and `PYTHONPATH=src`. The following
commands are explicit opt-ins; ordinary pytest never runs them.

```bash
ARTIFACT_ROOT=/tmp/groundloop-m3-verifier-20260718

PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  training/m3_verifier/prepare.py \
  --artifact-root "$ARTIFACT_ROOT" --download --seed 20260718

OMP_NUM_THREADS=8 MKL_NUM_THREADS=8 TOKENIZERS_PARALLELISM=false \
PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  training/m3_verifier/train.py \
  --artifact-root "$ARTIFACT_ROOT" \
  --config configs/m3/verifier/bounded_cpu.json --allow-download

OMP_NUM_THREADS=8 MKL_NUM_THREADS=8 TOKENIZERS_PARALLELISM=false \
PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  training/m3_verifier/calibrate.py \
  --artifact-root "$ARTIFACT_ROOT" \
  --config configs/m3/verifier/bounded_cpu.json

OMP_NUM_THREADS=8 MKL_NUM_THREADS=8 TOKENIZERS_PARALLELISM=false \
PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  experiments/m3/verifier/evaluate.py \
  --artifact-root "$ARTIFACT_ROOT" \
  --config configs/m3/verifier/bounded_cpu.json \
  --transfer-fixture \
    experiments/m3/verifier/fixtures/software_docs_transfer.jsonl \
  --allow-download

PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  experiments/m3/verifier/real_smoke.py \
  --artifact-root "$ARTIFACT_ROOT"
```

## Dataset and artifact checksums

- SciFact source archive:
  `11c621288d41ac144d29b13b0f8503b3820b7d6e8b1f6ff24dff335c196d76be`
- Prepared train:
  `1b76ed557b43955980e57a0f049666496335b649caa6e406c2e7b0245e7e3f4e`
- Prepared development:
  `a9cd74df8df6e6f7a825ee446d222adc87a7bcf61c9394f45efaacc1cb479882`
- Prepared public test:
  `3ccd2c761bed3101f65dadd75a597afd831d3d95041113c2f0236b968cec6a2f`
- Prepared manifest:
  `1d1ef303a8a7d5b560b6e3a558573953d52a02aa227a96aff5b90f18b14aec7c`
- Authored transfer fixture:
  `2aee9007bdbe1fd33098c3f3d83e8f956835d71e87f1ed94ceb8e0b3abbfc4af`
- Model weights:
  `81c49c30048dcbcc9fb2895b56622f702b4aa9894e7d055f28bb520c09f74e3e`
- Checkpoint including training manifest:
  `81870b683cec57eff82665103fcff3a35f45b9c9be0e18b53dcd40f485bfa4cf`

## Exact real-run evidence

- Prepare: 4,367 examples; two cross-split claim-derived removals and two
  within-split pair removals; 1.79s; 134,416 KiB peak RSS.
- Train: exit 0; 27m38.38s; 3,347,492 KiB peak RSS; 3,022 examples;
  95 optimizer steps; mean loss 0.6611415.
- Calibrate: exit 0; 2m19.25s; 969,304 KiB peak RSS; temperature
  1.1037657679769346; development NLL 0.5039963 to 0.5016337.
- Evaluate: exit 0; 2m18.92s; 1,284,184 KiB peak RSS. Exact metrics and CIs
  are in `CALIBRATION_REPORT.md`.

## Limitations and integration notes

- The public test cannot estimate REFUTE performance and every public-test pair
  truncates at 256 tokens.
- The transfer fixture is authored, independent, balanced, and only 18 rows.
- Checkpoint distribution requires a separate license review.
- Dynamic impact discovery and M4 verifier-call savings are out of lane scope.
- Coordinator-owned contracts, persistence, pipeline, migrations, CLI, and M2
  engines were not modified in this lane.

