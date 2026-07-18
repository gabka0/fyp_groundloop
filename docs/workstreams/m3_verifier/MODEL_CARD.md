# GroundLoop M3 MiniLM2 Verifier Model Card

Status: one real bounded CPU adaptation completed on 2026-07-18. The checkpoint
is an external academic artifact and is not distributed in Git.

## Intended use and semantic boundary

This model scores whether an evidence passage, supplied as the NLI premise,
supports, refutes, or is neutral toward a claim supplied as the hypothesis.
Raw checkpoint logits use `(contradiction, entailment, neutral)` order.
GroundLoop stores probabilities in `(support, refute, neutral)` order. A
versioned `DecisionPolicy`, not this adapter, derives operational labels.

These scores are empirical model judgments. They are not truth labels and do
not extend GroundLoop's exact relational-maintenance guarantee beyond stored
observations.

## Identity

- Base: `cross-encoder/nli-MiniLM2-L6-H768`
- Frozen revision: `b95119ce93d3e065de6214e38cd4a97b0f2f2c6d`
- Base license: Apache-2.0
- Fine-tuned logical ID: `groundloop/minilm2-m3-bounded-v1`
- Model weights (`model.safetensors`) SHA-256:
  `81c49c30048dcbcc9fb2895b56622f702b4aa9894e7d055f28bb520c09f74e3e`
- Checkpoint tree digest before the training manifest:
  `c555021dfe05585db68f75e2d09de1e201ac750d92fb85c6c2a9f78ffad33872`
- Checkpoint tree digest including the training manifest:
  `81870b683cec57eff82665103fcff3a35f45b9c9be0e18b53dcd40f485bfa4cf`

Runtime file paths are not part of semantic identity. The lane adapter keys a
fine-tuned artifact by the logical ID, immutable revision, and checkpoint tree
digest, so relocating identical files does not create a new model artifact.

## Training data

The external prepared dataset contains 4,367 claim/evidence pairs across 3,072
normalized claim groups:

| Split | Support | Refute | Neutral | Total |
|---|---:|---:|---:|---:|
| Train | 1,074 | 341 | 1,607 | 3,022 |
| Development | 331 | 122 | 534 | 987 |
| Public test | 111 | 0 | 247 | 358 |

Sources and frozen revisions:

- [SciFact](https://huggingface.co/datasets/allenai/scifact), revision
  `1fe54665deee011033b2dd98db5752e0d586fdfb`, CC BY-NC 2.0.
- [WiCE](https://github.com/ryokamoi/wice), revision
  `ddeb6c183665e2a20c5f03c5aa07f03888b9870f`; annotations are ODC-BY and
  underlying Wikipedia/Common Crawl text retains its terms.

Frozen mapping: SciFact SUPPORT to support, CONTRADICT to refute, and one
split-local sampled non-evidence document per claim to noisy neutral. WiCE
`supported` maps to support; `partially_supported` and `not_supported` both map
to neutral. WiCE absence of support is never manufactured into contradiction.

Claim-derived examples remain in one official partition. Normalized duplicate
control removed two lower-priority cross-split claim-derived examples and two
within-split duplicate pairs. SciFact's 300 official test claims have no public
labels and were excluded from scored evaluation. Prepared checksums are in the
lane handoff; raw data is not committed.

## Training procedure and resources

- Seed: `20260718`
- Epochs: 1
- Maximum sequence length: 256 wordpiece tokens
- Batch size: 8
- Gradient accumulation: 4
- Optimizer steps: 95; microbatches: 378
- Learning rate: `2e-5`; weight decay: `0.01`; warmup ratio: `0.06`
- Inverse-frequency class-weighted cross entropy in base-logit order
- Mean training loss: `0.6611415183654538`
- Hardware: AMD Ryzen 7 5700U host, CPU only, 8 Torch threads
- Wall time: 1,655.82 seconds (external `/usr/bin/time`: 27m38.38s)
- Peak RSS: 3,347,492 KiB
- PyTorch 2.13.0+cpu; Transformers 4.57.6; Datasets 4.8.5;
  Accelerate 1.14.0; scikit-learn 1.9.0

## Evaluation summary

Public test macro-F1 is the unweighted mean over classes with nonzero
ground-truth support. REFUTE recall/F1 are undefined, not zero, because the
public test contains no contradiction labels.

| Fixture/model | Accuracy | Macro-F1 | Brier | ECE (10 bins) |
|---|---:|---:|---:|---:|
| Public test, zero-shot | 0.6006 | 0.4001 | 0.6609 | 0.2597 |
| Public test, fine-tuned uncalibrated | 0.6173 | 0.5298 | 0.4888 | 0.0970 |
| Public test, fine-tuned calibrated | 0.6173 | 0.5298 | 0.4829 | 0.0709 |
| Transfer, zero-shot | 0.6111 | 0.6083 | 0.5884 | 0.2422 |
| Transfer, fine-tuned uncalibrated | 0.6667 | 0.6646 | 0.4739 | 0.2698 |
| Transfer, fine-tuned calibrated | 0.6667 | 0.6646 | 0.4533 | 0.2834 |

Fine-tuning improves the point estimate for public-test present-class macro-F1
and for transfer macro-F1. Development-fitted calibration improves public-test
Brier and ECE, but transfer ECE worsens from 0.2698 to 0.2834. Therefore this
run does not establish an across-domain ECE improvement.

## Prominent limitations

- All 358 public-test WiCE evidence documents exceed the 256-token pair limit
  and are truncated. The result evaluates a bounded-input verifier, not
  full-document WiCE classification.
- The public test has zero REFUTE examples. Public-test contradiction recall and
  F1 are not estimable.
- The independent software/API transfer fixture has only 18 authored examples,
  six per class; confidence intervals are wide and it is a transfer smoke, not
  a benchmark substitute.
- SciFact neutral examples are split-local sampled non-evidence and are noisy;
  non-evidence is not proof of neutrality.
- Temperature scaling changes confidence but not argmax predictions.
- A single seed and one bounded CPU run do not estimate training variance or
  support model-selection claims.
- The SciFact CC BY-NC license and WiCE source terms mean the checkpoint is not
  advertised as commercially reusable. Redistribution requires a separate
  license review.
- The model does not guarantee claim-extraction quality, retrieval
  completeness, objective truth, freshness, security, or privacy.

