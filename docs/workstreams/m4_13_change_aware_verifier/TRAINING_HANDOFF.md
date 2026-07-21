# M4.13 training and calibration handoff

Status: implementation and all eight real optimizer runs complete

Canonical post-execution result: [`RESULTS.md`](RESULTS.md). The implementation
and command details below remain the historical pre-execution handoff; where
they discuss unknown or pending outcomes, `RESULTS.md` supersedes them.

## Pre-execution verdict (historical)

Lane B implements the frozen continuation experiment and its calibration
contract. It does not provide a model-quality result. Until the serialized
V1/V2/V3/A1 runs, development selection, calibration and terminal evaluation
finish, whether either continuation objective improves revision response is
**unknown**.

One V2 primary-seed process was deliberately interrupted after the later
provenance audit found that trainer code identity was not yet bound. It
published no run directory or completion seal and is not a result. Real runs
must start again from the final clean integrated commit.

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
manifest marks repeated control rows explicitly. Each microbatch loss is
divided by the actual number of microbatches in its optimizer group: 4 for the
first 222 groups and 2 for the final partial group. The divisor is part of the
optimizer schedule identity, preventing the last step from being silently
half-scaled or reinterpreted.

For each seed, V2 and V3 independently materialize and compare the same batch
order hash, class weights, optimizer boundaries and complete schedule hash
before loading model weights. The objective variant is deliberately excluded
from that schedule identity.

Against Lane A's final deterministic artifact root
`artifacts/m4_13_change_aware_verifier`, the pre-training audit loaded 4,096
VitaminC rows in 1,024 cases and all 3,022 M3 rows in 2,067 claim groups. The
real schedule identities are:

| Seed | V2/V3 batch order | Optimizer schedule | Complete schedule |
|---:|---|---|---|
| 20260720 | `5af9e12db06c49816c2241f44473ff22a0e8e354aec273b6dcda8b8b00fb21b7` | `4137d4849d84f8a2ed962e6e4715cd8eab4765c463c02c1ce9acab9f66e37c9c` | `559fffe4713cf3bc6de5e2503a74f11813bbdd7323de6d9dcfdd0d7298798071` |
| 20260721 | `a5796597ca7b90994db8c770117258399feafca2f0d918d99663f201e68006eb` | `4137d4849d84f8a2ed962e6e4715cd8eab4765c463c02c1ce9acab9f66e37c9c` | `959ba3e4bae8697865ab73293510ce0e0f6bb46744a8b2bfd466685132bf92cd` |
| 20260722 | `00691999e027a0e5f2ac3a2648cceb9c7db34a8c3fe58951b174bb7f73184759` | `4137d4849d84f8a2ed962e6e4715cd8eab4765c463c02c1ce9acab9f66e37c9c` | `7aec74b35c09f02d6242695929773b2881ad201eddbdd4657ab99ac69a23b3f2` |

These are schedule identities, not training measurements or quality results.

## Input and output schemas

Training consumes Lane A's `groundloop-m4-13-dataset-manifest-v1`, validates
its config digest, six train/development artifact digests, exact label mapping,
and `terminal_paths_exposed=false`. A recursive guard permits terminal counts
and hashes but rejects terminal path, row ID, case ID, page, label, claim or
evidence content. The train and calibration call signatures expose no test or
terminal path parameter.

This is an application-interface leakage guard, not an operating-system
sandbox: Lane A's artifact root physically has evaluator-owned `sealed/` and
`prepared/test_m3.jsonl` siblings. Lane B never resolves or reads them, but a
separately compromised process with the same filesystem permissions could.

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
visible run. `KeyboardInterrupt` and other direct `BaseException` exits also
remove the staging directory.

Before importing or loading PyTorch/model weights, every real invocation also
records and validates:

```text
repository.git_head
repository.dirty = false
trainer_implementation.schema_version =
  groundloop-m4-13-trainer-implementation-v1
trainer_implementation.files_sha256
trainer_implementation.sha256
```

The repository root must be the exact Git worktree root. `git status
--porcelain=v1 --untracked-files=all` must be empty, so staged, modified,
deleted and nonignored untracked files all stop the run; ignored external model
artifacts do not. The implementation file map binds `train.py`, `losses.py`,
the package `__init__.py`, GroundLoop's artifact hashing module and its error
module. The aggregate SHA-256 is over canonical JSON containing the schema and
sorted relative-path/file-hash map. Both the Git HEAD and aggregate
implementation identity enter the invocation hash, so a code or commit change
cannot reuse an earlier training run.

The trainer repeats the same repository/provenance validation after checkpoint
serialization and immediately before writing the completion seal. A source edit
or clean commit created during the long optimizer run therefore aborts
publication instead of producing a checkpoint that claims the earlier state.
AdamW betas, epsilon and all execution flags are explicit (`foreach=false`,
`fused=false`), and the linear scheduler binds 13 warmup steps out of 223.
Runtime provenance records Python, PyTorch, Transformers, Tokenizers,
Safetensors and NumPy versions plus the frozen CPU thread settings.

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
variant, seed, checkpoint-tree digest, checkpoint-identity-file digest,
development-logits file digest and development-source-alignment digest. The
calibrator independently hashes the supplied logits and reconstructs Lane C's
alignment hash from the hash-bound prepared files. Therefore a logits file or
source set substituted after selection fails closed. It also binds the hash of
the entire selection file, so Lane C's additional development-only and
rule-verdict fields are covered without creating a second selection format.
On the final Lane A data root, Lane B and Lane C independently reconstructed
the same source-alignment identity:
`5ef17da9a02a03d9746b2578bbf48d4f1bb3c920f2f627902aa484cd2782b1ac`.

Calibration output schema is
`groundloop-m4-13-group-balanced-temperature-v1`. The objective gives M3 and
VitaminC weight 0.5 each, averages rows within each M3 claim group or VitaminC
case, then averages groups. It uses 96-step positive log-temperature golden
search over `[0.05, 20]`. The candidate is accepted only when combined NLL
decreases and neither domain NLL rises by more than 0.01; otherwise the sealed
deployment temperature is exactly 1.0. The artifact reports uncalibrated,
old-M3-temperature, candidate and deployed NLL surfaces. Its atomic temporary
file is never accepted without a complete schema/invocation seal.

Calibration also fails closed unless `--repository-root` is the exact clean
Git worktree root. The calibration invocation binds:

```text
repository.git_head
repository.dirty = false
calibrator_implementation.schema_version =
  groundloop-m4-13-calibrator-implementation-v1
calibrator_implementation.files_sha256
calibrator_implementation.sha256
```

The file map covers `calibrate.py`, `losses.py`, `train.py`, package
`__init__.py`, GroundLoop's artifact hashing module and its error module.
These are all local modules on the calibrator's semantic import path. The
aggregate digest uses canonical JSON over the schema and sorted relative-path
map. The repository is checked before any calibration input artifact is loaded
and again after fitting, immediately before the result write; a tracked,
staged, deleted or nonignored untracked change stops publication. Both
provenance objects are copied into the sealed artifact and covered by
`invocation_sha256`. Exact replay compares the complete canonical result, not
just the invocation digest, so a stale or corrupted same-invocation
temperature, NLL or semantic version fails closed.

## Commands

Use the repository virtual environment but add the standalone training
package root explicitly:

```bash
export PYTHONPATH=src:training

.venv/bin/python -m m4_13_verifier.train \
  --artifact-root artifacts/m4_13_change_aware_verifier \
  --config configs/m4/verifier/change_aware_v1.json \
  --m3-checkpoint \
    models/m3/verifier-run-20260718/checkpoints/minilm2-m3-bounded-v1 \
  --repository-root "$PWD" \
  --variant V2-ce-mix \
  --seed 20260720
```

Run only one real training process at a time. Repeat with the frozen variants
and seeds; do not run a model sweep. After Lane C seals development selection
and writes the selected checkpoint's combined development logits:

```bash
.venv/bin/python -m m4_13_verifier.calibrate \
  --artifact-root artifacts/m4_13_change_aware_verifier \
  --config configs/m4/verifier/change_aware_v1.json \
  --run-directory \
    artifacts/m4_13_change_aware_verifier/runs/V2-ce-mix/20260720 \
  --selection \
    artifacts/m4_13_change_aware_verifier/development_bundle/selection.json \
  --development-logits \
    'artifacts/m4_13_change_aware_verifier/development_bundle/development/V2-ce-mix:seed-20260720/development_logits.jsonl' \
  --repository-root "$PWD"
```

The variant in the second command is illustrative. The calibrator rejects it
unless that exact variant/checkpoint is in the sealed development selection.
Run the command once for each of the selected variant's three frozen seeds,
changing both the run directory and the model-key component of the development
logits path. Calibration output remains inside each corresponding run
directory.

## Pre-execution fixture validation (historical)

At the time of this lane handoff, no completed real model result had been
produced. Fixture-sized validation completed:

```text
pytest -q tests/m4/change_aware_verifier/training
  39 passed

ruff check training/m4_13_verifier \
  tests/m4/change_aware_verifier/training
  All checks passed

mypy --strict --python-version 3.12 --explicit-package-bases \
  training/m4_13_verifier
  Success: no issues found in 5 source files

python -m compileall -q training/m4_13_verifier \
  tests/m4/change_aware_verifier/training
  passed
```

Tests cover hand-computed weighted CE and paired loss, finite gradients,
margin monotonicity, shift invariance, label/logit order, V2/V3 schedule
identity, complete M3 replay, control cycling, dataset-manifest and checkpoint
drift, terminal-input rejection, failure injection, exact replay, artifact
tampering, incomplete-run rejection, exact trainer and calibrator
Git/implementation provenance, dependency hash drift, invocation-hash binding
and tracked/untracked dirty-worktree rejection. Direct interruption and
pre-publication source-change tests prove that no partial run is published.
Calibration replay tests also reject a same-invocation payload with drifted
semantic results.

The final real Lane A training surface and the local M3 checkpoint were also
validated without optimizer work. The observed config, dataset-manifest,
checkpoint-tree and weights hashes were respectively `d50c2af5...`,
`1a5ed4b7...`, `81870b68...` and `81c49c30...`.

## Pre-execution limits and integration notes (historical)

The executed outcomes and current limits are in [`RESULTS.md`](RESULTS.md).
Before execution, the lane recorded the following constraints:

- Real training was expected to take hours and remain coordinator-controlled.
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
