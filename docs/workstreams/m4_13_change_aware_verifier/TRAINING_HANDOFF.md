# M4.13 training and calibration handoff

Status: implementation complete; no real optimizer run has been started

## Verdict

Lane B implements the frozen continuation experiment and its calibration
contract. It does not provide a model-quality result. Until the serialized
V1/V2/V3/A1 runs, development selection, calibration and terminal evaluation
finish, whether either continuation objective improves revision response is
**unknown**.

The code starts only from the exact M3 checkpoint tree
`81870b683cec57eff82665103fcff3a35f45b9c9be0e18b53dcd40f485bfa4cf`
and exact weights
`81c49c30048dcbcc9fb2895b56622f702b4aa9894e7d055f28bb520c09f74e3e`.
It does not download or silently reconstruct the upstream base model.

## Owned paths

This lane adds only:

- `training/m4_13_verifier/__init__.py`
- `training/m4_13_verifier/losses.py`
- `training/m4_13_verifier/train.py`
- `training/m4_13_verifier/calibrate.py`
- `tests/m4/change_aware_verifier/training/**`
- this handoff

Generated logits, checkpoints, schedules and results remain outside Git.

## Implemented training contract

The stored label order is `(support, refute, neutral)`. MiniLM raw logits stay
in `(contradiction, entailment, neutral)` order, so stored SUPPORT, REFUTE and
NEUTRAL map to base indices 1, 0 and 2 respectively. VitaminC `NOT ENOUGH
INFO` maps to NEUTRAL, never REFUTE. Both the prepared-row label and original
VitaminC label are checked before training.

The endpoint loss implements the literal frozen formula

```text
mean_i -w[y_i] log_softmax(z_i)[y_i]
```

It does not use PyTorch's weighted-cross-entropy mean, whose denominator is
the sum of selected weights and is therefore a different objective. On each
VitaminC transition, V3 and A1 add

```text
0.5 * [softplus(0.5 - (q_a[y_a] - q_b[y_a]))
     + softplus(0.5 - (q_b[y_b] - q_a[y_b]))]
```

with weight 0.25. M3 microbatches always use endpoint CE only.

The deterministic schedule implements all four trainable variants:

| Variant | Frozen source schedule | Paired loss | Seeds |
|---|---|---|---|
| V1-replay-only | complete M3 replay, then batch-cycle to 890 | no | 20260720 |
| V2-ce-mix | 512 VitaminC plus 378 M3 microbatches | no | 20260720/21/22 |
| V3-margin-mix | byte-identical V2 schedule | VitaminC only | 20260720/21/22 |
| A1-margin-no-replay | 512 VitaminC batches, then case-batch cycle to 890 | yes | 20260720 |

VitaminC is shuffled as atomic four-row cases and batched as two complete
cases. M3 claim-group IDs are shuffled deterministically, their rows are
concatenated in stable order, and all 3,022 rows occur exactly once in the
mixed epoch. The two source batch streams are merged with integer arithmetic,
not floating-point or nondeterministic sampling. Every run has 890
microbatches, gradient accumulation 4 and 223 optimizer steps. The batch
manifest marks repeated control rows explicitly.

For each seed, V2 and V3 independently materialize and compare the same batch
order hash, class weights, optimizer boundaries and complete schedule hash
before loading model weights. The objective variant is deliberately excluded
from that schedule identity.

Against Lane A's final deterministic artifact root
`/tmp/groundloop-m4-13-data-real-f`, the pre-training audit loaded 4,096
VitaminC rows in 1,024 cases and all 3,022 M3 rows in 2,067 claim groups. The
real schedule identities are:

| Seed | V2/V3 batch order | Optimizer schedule | Complete schedule |
|---:|---|---|---|
| 20260720 | `5af9e12db06c49816c2241f44473ff22a0e8e354aec273b6dcda8b8b00fb21b7` | `c3435e897dc18605a021a2b92116fffdc409a21403b8030680acd289526519df` | `e72933a24a02b24d7b25d7244dafc97139d4c329d4544797d4f771ef9a7ef2f2` |
| 20260721 | `a5796597ca7b90994db8c770117258399feafca2f0d918d99663f201e68006eb` | `c3435e897dc18605a021a2b92116fffdc409a21403b8030680acd289526519df` | `8a5a3559b46ec5f0cc5f93405da3bb851ee25a28368425ac254ae26e4dc117b5` |
| 20260722 | `00691999e027a0e5f2ac3a2648cceb9c7db34a8c3fe58951b174bb7f73184759` | `c3435e897dc18605a021a2b92116fffdc409a21403b8030680acd289526519df` | `87cc71a2fbb0867c52766d3e322258b22ab20d1cbc49f004c81747541347b131` |

These are schedule identities, not training measurements or quality results.

## Input and output schemas

Training consumes Lane A's `groundloop-m4-13-dataset-manifest-v1`, validates
its config digest, six train/development artifact digests, exact label mapping,
and `terminal_paths_exposed=false`. A recursive guard permits terminal counts
and hashes but rejects terminal path, row ID, case ID, page, label, claim or
evidence content. The train and calibration call signatures expose no test or
terminal path parameter.

Each completed external run directory contains:

```text
runs/<variant>/<seed>/
  checkpoint/
  checkpoint_identity.json
  batch_schedule.json
  training_manifest.json
  runtime.json
  run_complete.json
```

The checkpoint tree, weights, deterministic manifest and schedule hashes are
bound by `run_complete.json`. Timing and host telemetry are isolated in
`runtime.json` and separately hashed, so they do not alter the deterministic
training-manifest identity. Existing incomplete or conflicting run directories
fail closed. A new run is constructed under a sibling temporary directory,
self-validated, and exposed by one same-filesystem directory rename. Injected
failures before checkpoint write, manifest write and final sealing leave no
visible run.

Lane C should emit development logits with schema
`groundloop-m4-13-development-logit-v1`. Required fields are:

```text
split=development, domain=m3|vitaminc, row_id, group_id,
label, claim_sha256, evidence_sha256, base_logit_order, logits[3],
checkpoint_tree_sha256, variant, seed
```

For M3, `group_id` is the claim-group ID. For VitaminC, it is the case ID.
Calibration requires both complete development domains and checks the frozen
987-row/649-group M3 and 1,024-row/256-case VitaminC dimensions.
It also reconstructs the complete expected row/group/label/content-hash map
from Lane A's hash-bound development files, preventing a same-count row
substitution from passing calibration.

The sealed selection schema is `groundloop-m4-13-selection-v1`. Calibration
requires `sealed=true`, the development-selected V2 or V3 variant, primary
seed 20260720, and a unique checkpoint allow-list entry containing exact
variant, seed, checkpoint-tree digest and checkpoint-identity-file digest. It
binds the hash of the entire selection file, so Lane C's additional
development-only and rule-verdict fields are covered without creating a
second selection format.

Calibration output schema is
`groundloop-m4-13-group-balanced-temperature-v1`. The objective gives M3 and
VitaminC weight 0.5 each, averages rows within each M3 claim group or VitaminC
case, then averages groups. It uses 96-step positive log-temperature golden
search over `[0.05, 20]`. The candidate is accepted only when combined NLL
decreases and neither domain NLL rises by more than 0.01; otherwise the sealed
deployment temperature is exactly 1.0. The artifact reports uncalibrated,
old-M3-temperature, candidate and deployed NLL surfaces. Its atomic temporary
file is never accepted without a complete schema/invocation seal.

## Commands

Use the repository virtual environment but add the standalone training
package root explicitly:

```bash
export PYTHONPATH=src:training

.venv/bin/python -m m4_13_verifier.train \
  --artifact-root /tmp/groundloop-m4-13-data-real-f \
  --config configs/m4/verifier/change_aware_v1.json \
  --m3-checkpoint \
    models/m3/verifier-run-20260718/checkpoints/minilm2-m3-bounded-v1 \
  --variant V2-ce-mix \
  --seed 20260720
```

Run only one real training process at a time. Repeat with the frozen variants
and seeds; do not run a model sweep. After Lane C seals development selection
and writes the selected checkpoint's combined development logits:

```bash
.venv/bin/python -m m4_13_verifier.calibrate \
  --artifact-root /tmp/groundloop-m4-13-data-real-f \
  --config configs/m4/verifier/change_aware_v1.json \
  --run-directory \
    /tmp/groundloop-m4-13-data-real-f/runs/V2-ce-mix/20260720 \
  --selection \
    /tmp/groundloop-m4-13-data-real-f/selection.json \
  --development-logits \
    /tmp/groundloop-m4-13-data-real-f/runs/V2-ce-mix/20260720/development_logits.jsonl
```

The variant in the second command is illustrative. The calibrator rejects it
unless that exact variant/checkpoint is in the sealed development selection.

## Validation

No real model training was run. Fixture-sized validation completed:

```text
pytest -q tests/m4/change_aware_verifier/training
  21 passed

ruff check training/m4_13_verifier \
  tests/m4/change_aware_verifier/training
  All checks passed

mypy --strict --python-version 3.12 --explicit-package-bases \
  training/m4_13_verifier
  Success: no issues found in 4 source files

python -m compileall -q training/m4_13_verifier \
  tests/m4/change_aware_verifier/training
  passed
```

Tests cover hand-computed weighted CE and paired loss, finite gradients,
margin monotonicity, shift invariance, label/logit order, V2/V3 schedule
identity, complete M3 replay, control cycling, dataset-manifest and checkpoint
drift, terminal-input rejection, failure injection, exact replay, artifact
tampering and incomplete-run rejection.

The final real Lane A training surface and the local M3 checkpoint were also
validated without optimizer work. The observed config, dataset-manifest,
checkpoint-tree and weights hashes were respectively `d50c2af5...`,
`1a5ed4b7...`, `81870b68...` and `81c49c30...`.

## Limits and integration notes

- Real training will take hours and remains coordinator-controlled.
- The code proves schedule equivalence, not numerical equality of V2/V3
  gradients; their objectives intentionally differ.
- Deterministic PyTorch mode and identical seeds reduce implementation noise,
  but bitwise checkpoint reproducibility is still platform/dependency scoped.
- The scalar calibrator cannot change argmax transition metrics.
- Calibration cannot precede development selection and cannot inspect the
  terminal reserve, M3 public test or Git pilot.
- A passed software gate says nothing about VitaminC, M3-retention or Git
  transfer quality.
- Lane A and Lane B both created `training/m4_13_verifier/__init__.py`; during
  integration retain the Lane B exports plus the package docstring.
