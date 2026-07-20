# M4.12 VitaminC Revision-Sensitive AI Gate

Status: implemented and executed with the frozen real BGE and calibrated M3
verifier on 2026-07-20.

## Verdict

The frozen verifier is not good enough on fine-grained evidence changes. The
result supports a bounded neural adaptation stage; it does not support a claim
that M4 AI quality is solved.

The 512 endpoint judgments have accuracy `0.5059`, macro-F1 `0.4655`, and ECE
`0.2779`. More importantly for GroundLoop, the verifier changes its argmax on
only `0.3281` of 256 human-label-changing evidence pairs, and gets both
endpoints right on only `0.1992`. The true-label scores move in both correct
directions on `0.6836` of pairs, so the checkpoint contains useful signal but
its decision behavior is weak.

BGE performs materially better on the deliberately narrow two-version task:
it ranks the human-SUPPORT version first for `0.7891` of claims. This is not
global retrieval or affected-claim recall. It only tests whether BGE can rank a
supporting evidence sentence above its near-identical REFUTE or NEUTRAL
counterpart.

Confidence in the executed measurements and provenance: **high**. Confidence
that verifier adaptation is the highest-value next neural task: **high**.
Confidence that the proposed adaptation will improve natural-history M4
utility: **moderate** until tested on a new disjoint reserve and real histories.

## Why this gate is new

The existing M3 report already scored the frozen verifier on 358 WiCE public
test pairs. Repeating those rows under an M4 name would add no scientific
evidence. M4.12 instead uses VitaminC, which was absent from M3 training,
calibration, and public testing and was designed around contrastive Wikipedia
revisions.

The gate evaluates three distinct empirical questions:

1. Three-class endpoint classification on SUPPORT, REFUTE, and NEUTRAL rows.
2. Whether the verifier reacts correctly when only a near-identical evidence
   version changes the human label.
3. Whether BGE ranks the supporting evidence version over the paired
   counterfactual or insufficient version.

It does not evaluate exact IVM correctness, objective truth, generation,
claim extraction, global reverse retrieval, or end-to-end affected-claim
recall.

## Primary source and immutable data

Official code repository:
`https://github.com/TalSchuster/VitaminC.git`, commit
`eb532922b88b199df68ed26afeb58dca5501b52f`.

The official README identifies the NAACL 2021 paper *Get Your Vitamin C!
Robust Fact Verification with Contrastive Evidence* and links the fact
verification archive. The archive is pinned through the hosting repository
commit rather than the mutable `master` URL:

`https://raw.githubusercontent.com/TalSchuster/talschuster.github.io/152c9639f1e33ca295d0776b618c17884088f72f/static/vitaminc.zip`

The primary paper (`arXiv:2103.08541`, NAACL 2021) reports that trained human
annotators first identified factual changes, then 70 native English speakers
wrote and reviewed symmetric claims. It also reports Fleiss kappa `0.7065` on
an independently re-annotated 2,000-pair sample. The labels are therefore
human-annotated evaluation targets derived under the dataset construction;
they are not GroundLoop model judgments or an assertion of timeless truth.

Key hashes:

| Artifact | SHA-256 |
|---|---|
| Official archive | `49d82dc1690cbee420d18e2c26f687a7937710bb211845d2571430dfd4dc0337` |
| `train.jsonl` | `446163d81ec9b654d2514e8ba5b8f2e4bd8938342e34c98e7660fe05a8a3ca39` |
| `dev.jsonl` | `a3258bc959754c84bade150d3bf447fd9c91a529d93bb676f034fdafda5f26e5` |
| `test.jsonl` | `7f799e076dec184e31533d46704fef2022d08ef68a0db2c44f138d2b4e552f36` |
| Official `DATA_LICENSE` | `153a128e86c7d0e978243c8c8994dbb5fba9081a36f8d380cd1f5994699e369e` |

The data license applies the applicable Wikipedia article terms, or CC BY-SA
3.0 where those terms are unavailable. Synthetic annotations also derive from
FEVER. M4.12 excludes every `revision_type=synthetic` row.

The full test archive contains 55,197 rows, 16,487 cases, and 3,068 pages.
Although the paper says assignment was random by article, the downloaded
official files have small normalized page-name intersections: train/development 18
pages, train/test 22, and development/test 2. M4.12 does not guess why. It
rejects every test page present in either train or development before sampling.

Exact normalized overlap between the entire VitaminC test split and all M3
prepared train/development/test pairs is zero on claims, evidence, and complete
claim/evidence pairs. This is an exact-string audit, not a semantic or
pretraining-contamination guarantee.

## Frozen sample design

The sampler uses no model output. It selects real-revision cases satisfying
all of these conditions:

- exactly four rows;
- exactly two claims and two evidence versions;
- the complete 2-by-2 claim/evidence cross-product;
- one SUPPORT endpoint per claim;
- page absent from VitaminC train and development;
- no page used by another selected case.

It deterministically selects 64 SUPPORT/REFUTE cases followed by 64
SUPPORT/NEUTRAL cases using SHA-256 ordering over the seed, stratum, page, and
case ID. The result is 128 cases on 128 pages and 512 endpoint rows: 256
SUPPORT, 128 REFUTE, and 128 NEUTRAL. Its manifest SHA-256 is
`214885784d13912ce603cc30eaed5904fbb588e493405767d66bbb3a490787d6`.

This is a label-stratified diagnostic sample. It is not an unbiased estimate
of the full VitaminC test distribution. All confidence intervals use 1,000
seeded percentile-bootstrap resamples at the Wikipedia-page unit.

## Real results

Endpoint classification:

| Metric | Point estimate | 95% page-bootstrap interval |
|---|---:|---:|
| Accuracy | 0.5059 | [0.4589, 0.5508] |
| Macro-F1 | 0.4655 | [0.4164, 0.5110] |
| Multiclass Brier | 0.7211 | [0.6581, 0.7891] |
| NLL | 1.3593 | [1.2242, 1.5014] |
| ECE, 10 bins | 0.2779 | [0.2376, 0.3253] |

Per-class recall is SUPPORT `0.5977`, REFUTE `0.2734`, and NEUTRAL `0.5547`.
The confusion matrix, rows=true and columns=predicted in
`(support, refute, neutral)` order, is:

```text
[[153, 27, 76],
 [ 47, 35, 46],
 [ 37, 20, 71]]
```

Contrastive change behavior:

| Metric | Point estimate | 95% page-bootstrap interval |
|---|---:|---:|
| Model argmax changes when human label changes | 0.3281 | [0.2617, 0.3906] |
| Both endpoints correct | 0.1992 | [0.1445, 0.2578] |
| Both true-label probability margins move correctly | 0.6836 | [0.6211, 0.7383] |

The SUPPORT/REFUTE cases are harder than SUPPORT/NEUTRAL: both-endpoint
accuracy is `0.1172` versus `0.2812`.

BGE two-version ranking:

| Metric | Point estimate | 95% page-bootstrap interval |
|---|---:|---:|
| SUPPORT-version recall@1 | 0.7891 | [0.7344, 0.8359] |
| MRR | 0.8945 | [0.8672, 0.9199] |

All 512 verifier pairs and all BGE inputs fit their 256- and 512-token limits.
The verifier took 16.76 seconds including model load; BGE took 6.10 seconds
including model load. The whole run took 37.89 seconds externally with
1,665,300 KiB peak RSS and zero recorded failures or timeouts. The local
fail-fast runner enforces no per-batch timeout; the zero timeout count must not
be read as a timeout-resilience experiment.

Derived output hashes from the executed run:

| Output | SHA-256 |
|---|---|
| `report.json` | `201f90dd74a6842f5e15605ac333b89073f3254e48e1ca52329ab2063932b2f6` |
| `predictions.jsonl` | `0cd315438ff94d54923b8fdefc16ac1c20d9f9dc385ae2963251c31e04c4bc0e` |
| `manifest.json` | `29aa9112429dc2bcf2a3b65eb1a326226751f871c541b8f0e3e83172abeb67cf` |

Generated data, raw VitaminC text, model weights, and reports remain outside
Git. `predictions.jsonl` contains IDs, labels, logits, probabilities, and text
hashes but no raw claim or evidence text.

## Reproduction

Acquire the two official sources at their frozen revisions, verify the archive
hash, and extract it outside the repository:

```bash
git clone https://github.com/TalSchuster/VitaminC.git /tmp/vitaminc-official
git -C /tmp/vitaminc-official checkout --detach \
  eb532922b88b199df68ed26afeb58dca5501b52f

curl -L --fail \
  -o /tmp/vitaminc.zip \
  https://raw.githubusercontent.com/TalSchuster/talschuster.github.io/152c9639f1e33ca295d0776b618c17884088f72f/static/vitaminc.zip
sha256sum /tmp/vitaminc.zip
mkdir -p /tmp/vitaminc-data
unzip -q /tmp/vitaminc.zip -d /tmp/vitaminc-data
```

Run the frozen gate:

```bash
PYTHONPATH=src .venv/bin/python \
  experiments/m4_public_ai_gate/run_public_ai_gate.py \
  --official-repository /tmp/vitaminc-official \
  --vitaminc-archive /tmp/vitaminc.zip \
  --vitaminc-root /tmp/vitaminc-data/vitaminc \
  --m3-artifact-root models/m3/verifier-run-20260718 \
  --embedding-cache models/m3/huggingface-cache \
  --config configs/m4/public_ai/vitaminc_revision_gate_v1.json \
  --output-directory /tmp/groundloop-m4-12-vitaminc
```

Ordinary tests never load models or download data. The opt-in integration test
requires `GROUNDLOOP_RUN_M4_PUBLIC_AI_REAL=1` and the five explicit paths in
`tests/m4/public_ai_gate/test_real_gate_opt_in.py`.

## Adaptation-stage decision

Prioritize the verifier, not BGE, in the first neural-improvement experiment.
A defensible bounded stage is:

1. freeze the current M4.12 sample as a consumed audit set and never use it to
   select checkpoints, thresholds, prompts, losses, or epochs;
2. train only on VitaminC train plus an explicitly versioned mixture of the M3
   training data, filtering page overlaps before fitting;
3. calibrate and select on development data only;
4. compare ordinary three-way cross-entropy with one predeclared
   change-aware objective that rewards correct paired endpoint margins;
5. before training, freeze a new page-disjoint reserve from the remaining
   VitaminC test pages, and evaluate it exactly once after the model and
   calibration procedure are fixed;
6. separately test regression on the original WiCE/SciFact and authored
   software-documentation fixtures;
7. only after component quality improves, rerun natural version histories to
   measure affected-claim recall, status effects, calls, and latency.

The M4.12 result justifies that experiment; it does not pre-authorize replacing
the current checkpoint or modifying exact database semantics.
