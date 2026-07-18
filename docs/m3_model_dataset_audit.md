# M3 Model, Dataset, License, and Hardware Audit

Status: frozen input to the M3 contract baseline

Audit date: 2026-07-18

## Host constraint

The development host has an AMD Ryzen 7 5700U (8 cores, 16 threads), 14 GiB
RAM, 4 GiB swap, and 92 GiB free disk. No NVIDIA device, CUDA driver, or
`nvidia-smi` is present. M3 therefore uses compact local models and CPU-first
execution. Sustained training belongs only to the verifier lane; real-model
integration runs are serialized.

This is sufficient for a small cross-encoder adaptation and smoke-quality
local generation. It is not sufficient evidence that a full WiCE-scale
hyperparameter search is practical. M3 may not be declared complete unless a
real adapted checkpoint is produced and evaluated.

## Selected model artifacts

| Task | Selected artifact and immutable revision | License | Rationale |
|---|---|---|---|
| Embedding | [`BAAI/bge-small-en-v1.5`](https://huggingface.co/BAAI/bge-small-en-v1.5), `5c38ec7c405ec4b44b94cc5a9bb96e735b38267a` | MIT | 33.4M parameters, 384 dimensions, explicit short-query retrieval instruction, normalized cosine retrieval, CPU-feasible |
| Generation | [`Qwen/Qwen2.5-0.5B-Instruct`](https://huggingface.co/Qwen/Qwen2.5-0.5B-Instruct), `7ae557604adf67be50417f59c2c2f167def9a775` | Apache-2.0 | 0.49B parameters, explicit chat template, model card emphasizes structured/JSON output, small enough for local CPU smoke |
| Claim extraction | same Qwen checkpoint, separately registered task and prompt | Apache-2.0 | avoids a second decoder model while preserving independent task and prompt provenance |
| Verifier base | [`cross-encoder/nli-MiniLM2-L6-H768`](https://huggingface.co/cross-encoder/nli-MiniLM2-L6-H768), `b95119ce93d3e065de6214e38cd4a97b0f2f2c6d` | Apache-2.0 | NLI-pretrained cross-encoder; labels are contradiction, entailment, neutral; smaller on-disk footprint than the inspected DeBERTa alternative |

All Hub artifacts must be loaded with the exact revision above. `main`,
`latest`, or an unpinned model alias is prohibited in recorded runs.

### Alternatives rejected for the first vertical slice

- `Qwen/Qwen3-0.6B` is newer and Apache-2.0, but its additional reasoning-mode
  behavior adds output-control complexity without solving an M3 research
  question. Qwen2.5 is the more stable integration choice.
- `sentence-transformers/all-MiniLM-L6-v2` is smaller and Apache-2.0, but BGE
  v1.5 has an explicit retrieval setup and stronger model-card retrieval
  results at the same 384-dimensional storage shape.
- `cross-encoder/nli-deberta-v3-small` reports stronger SNLI/MNLI results, but
  its 568 MB checkpoint is materially heavier than the 329 MB MiniLM2
  checkpoint on a CPU-only, 14 GiB host. It remains a later evaluation
  alternative, not an M3 dependency.

## Frozen embedding behavior

- Passage input: normalized chunk text without an instruction prefix.
- Question/claim query input: prefix exactly
  `Represent this sentence for searching relevant passages: `.
- Output: L2-normalized 384-dimensional vector.
- Similarity: cosine distance in pgvector; deterministic fake embeddings are
  used by ordinary tests.
- Maximum model input length: 512 tokens; truncation counts must be recorded by
  real-model experiments.

The BGE model card explicitly recommends the query prefix for short-query to
long-passage retrieval and no prefix for passages.

## Verification datasets

### SciFact

Source: [`allenai/scifact`](https://huggingface.co/datasets/allenai/scifact),
revision `1fe54665deee011033b2dd98db5752e0d586fdfb`.

- 1.4K expert-written scientific claims with evidence abstracts and
  rationales.
- License: CC BY-NC 2.0.
- Use: SUPPORT and REFUTE supervision, plus split-local neutral sampling.
- Restriction: academic/non-commercial use only. Do not advertise the
  resulting checkpoint as commercially reusable. Do not redistribute raw
  dataset files in this repository.

### WiCE

Source: [`ryokamoi/wice`](https://github.com/ryokamoi/wice), revision
`ddeb6c183665e2a20c5f03c5aa07f03888b9870f`.

- Fine-grained entailment over Wikipedia claims and cited web evidence.
- Labels: `supported`, `partially_supported`, `not_supported`.
- Wikipedia/Common Crawl text retains its underlying terms; annotations are
  ODC-BY; code and released model outputs are MIT.
- Use: SUPPORT/NEUTRAL supervision and transfer evaluation. WiCE supplies no
  contradiction label.
- Raw data is downloaded from the authors at run time and never committed.

## Frozen three-way label mapping

| Source label/construction | GroundLoop label | Reason |
|---|---|---|
| SciFact `SUPPORT` | SUPPORT | direct entailment |
| SciFact `CONTRADICT` | REFUTE | explicit contradiction |
| SciFact split-local non-evidence pair | NEUTRAL | no annotated support/refutation; recorded as sampled/noisy |
| WiCE `supported` | SUPPORT | complete entailment |
| WiCE `partially_supported` | NEUTRAL | insufficient for a complete direct witness |
| WiCE `not_supported` | NEUTRAL | absence of support is not contradiction |

Mapping WiCE `not_supported` to REFUTE is forbidden. It would manufacture a
contradiction class that the dataset does not annotate.

## Leakage and evaluation rules

1. Preserve official train/development/test partitions.
2. Group all examples derived from one claim in one partition.
3. Sample neutral pairs only from documents within the same source split.
4. Remove exact normalized claim/evidence duplicates across splits and report
   removals.
5. Fit model parameters on train only.
6. Fit temperature on development/calibration only.
7. Tune decision thresholds on neither the test nor transfer set.
8. Evaluate once on the untouched test set and separately on the authored
   software-documentation transfer fixture.
9. Record dataset revisions, seeds, mappings, counts, and checksum manifests.

## Checkpoint distribution decision

Because SciFact is CC BY-NC and WiCE combines several underlying terms, M3
will commit training code, configuration, evaluation reports, and artifact
checksums but not the fine-tuned weights. Redistribution of the trained
checkpoint requires a separate license review. Local academic training and
evaluation remain allowed under the recorded source terms.

## Confidence

- Hardware audit: **high**.
- Model identity/license metadata: **high**, taken from current official model
  cards and Hub API records.
- CPU feasibility for one compact adaptation: **moderate** until measured.
- Dataset mapping correctness: **high** for the stated GroundLoop semantics.
- Checkpoint redistribution rights: **low/unknown** without formal license
  review; therefore weights remain uncommitted.
