# M3 Verifier, Fine-Tuning, and Calibration Lane Agent Prompt

You own the three-way verifier and the D-5 empirical deliverable. Work only in
`/home/kassym/Desktop/groundloop-worktrees/m3-verifier` on branch
`workstream/m3-verifier`.

Read `AGENTS.md`, `docs/m3_model_dataset_audit.md`,
`docs/m3_design_freeze.md`, `docs/m3_multiagent_execution_plan.md`, and the
coordinator-owned AI contracts completely before editing.

## Owned paths

- `src/groundloop/ai/verification/**`
- `tests/ai/verification/**`
- `training/m3_verifier/**`, `experiments/m3/verifier/**`
- `configs/m3/verifier/**`
- `docs/workstreams/m3_verifier/**`

Do not edit contracts, migration, pyproject, persistence, pipeline, CLI, M1/M2
engines, shared docs, or another lane. Put shared-contract proposals in
`docs/workstreams/m3_verifier/contract_requests/<slug>.md`.

## Required implementation

Implement a deterministic fake and a pinned real cross-encoder adapter for
`cross-encoder/nli-MiniLM2-L6-H768` revision
`b95119ce93d3e065de6214e38cd4a97b0f2f2c6d`. Input is evidence as premise and
claim as hypothesis. Base indices are contradiction=0, entailment=1,
neutral=2; stored order is support, refute, neutral. Temperature-scaled
softmax must yield finite normalized triples. Preserve auditable logits and
hashes; the decision policy derives labels.

Build reproducible dataset preparation from pinned SciFact and WiCE revisions.
Map SciFact SUPPORT to support, CONTRADICT to refute, and split-local sampled
non-evidence to neutral. Map WiCE supported to support and both
partially_supported/not_supported to neutral. Mapping WiCE not_supported to
refute is forbidden. Preserve official splits, group derived examples by claim,
deduplicate normalized cross-split duplicates, record counts/checksums/seeds,
and never commit raw data.

Fine-tune one bounded CPU-feasible run. Fit temperature on development data
only. Evaluate zero-shot, fine-tuned uncalibrated, and fine-tuned calibrated
models on untouched test and an independent software/API-doc transfer fixture.
Report per-class precision/recall/F1, macro-F1, confusion matrix, multiclass
Brier score, ECE with fixed documented bins, reliability rows, class counts,
appropriate confidence intervals, dependency/model/dataset revisions,
hyperparameters, seed, hardware, wall time, peak RSS, and checkpoint SHA-256.
Weights and raw datasets stay out of Git.

Tests cover label order, ties, malformed/empty evidence, token truncation,
batching, deterministic inference, calibration leakage guards, probability
validation, dataset mapping, and inactive-chunk late-result semantics at the
adapter boundary. Ordinary tests must never download data/models. Add explicit
prepare/train/calibrate/evaluate/real-smoke commands that fail clearly when an
artifact is absent.

The host has no GPU. Try bounded data, sequence length, gradient accumulation,
and one seeded run before declaring hardware impossible. Do not weaken D-5 or
substitute zero-shot. Write `MODEL_CARD.md`, `CALIBRATION_REPORT.md`,
`STATUS.md`, and `HANDOFF.md` with exact evidence and confidence. Commit owned
changes and report the hash. Do not merge.
