# GroundLoop M4.13 Change-Aware Verifier Plan

Status: implementation-ready design; training is not authorized by this file

Plan date: 2026-07-20

Depends on: committed M4.12 provenance and its deterministic semantic-result
artifact, and a corrected M4.10 rerun. M4.12's exact scientific identities are
bound below; corrected M4.10 hashes remain required inputs and are deliberately
not guessed.

## 1. Decision and confidence

Proceed with a bounded continuation experiment on the existing MiniLM2
verifier. Compare ordinary endpoint cross-entropy continuation against the same
continuation plus a paired revision-margin loss. Both variants must use the
same article-disjoint VitaminC cases, complete M3 replay data, optimizer budget,
three seeds, calibration protocol and terminal tests.

Do **not** change the online verifier interface. At inference time the model
still receives one evidence passage as premise and one claim as hypothesis and
emits three logits. “Change-aware” means that training and evaluation exploit
paired versions; it does not mean that the deployed model receives both
versions. This preserves the immutable-observation boundary and lets M4 reuse
the existing verifier adapter and provenance contracts.

The strongest counterargument is that this is not a novel model architecture
or an IVM contribution. VitaminC was explicitly created for contrastive
evidence training, and its paper already demonstrated increased revision
sensitivity. The value here is narrower: diagnose and repair a measured neural
failure in GroundLoop, preserve the old domains, and connect the improved
versioned judgments to an exact dynamic database system without corrupting the
evaluation. That is a credible FYP and portfolio result. It is not a new NLP
method by itself.

Confidence:

- The current verifier needs revision-sensitive adaptation: **high**, subject
  to the M4.12 semantic-result hash matching the reported metrics.
- The proposed experiment can determine whether paired continuation helps on
  this bounded sample: **high**.
- It will improve the Git software-documentation pilot: **moderate**; Wikipedia
  transfer is not guaranteed.
- It can support a broad model-quality or state-of-the-art claim: **low**.

## 2. Scientific and systems boundaries

M4.13 changes the empirical semantic-observation producer. It does not change
the exact structured state maintained after observations are stored.

The claims remain separate:

1. **Neural claim:** on held-out, version-paired evidence, the adapted model may
   become more likely to change its verdict in the correct direction.
2. **Systems claim:** given any stored score triples from any frozen model
   version, GroundLoop's incremental state equals its independent full
   recomputation oracles.
3. **End-to-end claim:** a better verifier may improve useful reactions to
   corpus revisions, but only a separately evaluated dynamic history can show
   that.

No neural metric can strengthen the exact IVM theorem. No exact IVM test can
establish that the neural judgments are true.

## 3. Blocking inputs and baseline-first gate

Real training must not begin until the coordinator records the M4.12 scientific
identities below and the sealed M4.13 reserve identity from Section 5.4 in the
new configuration. Corrected M4.10 hashes may remain pending until the terminal
Git transfer gate; no result may be declared complete without them.

### 3.1 M4.12 required inputs

The M4.12 handoff must be committed and must provide:

- full SHA-256 of its committed configuration;
- full SHA-256 of its deterministic `semantic_result.json`;
- full SHA-256 of every emitted per-row logits file used as the baseline;
- the exact M3 checkpoint, calibration and decision-policy identities;
- zero model failures/timeouts and complete alignment for all 512 endpoints;
- the source and consumed-sample identities in Sections 4 and 5.4; and
- a statement that the result came from the frozen real local checkpoint, not
  a table fixture or synthetic telemetry.

Bind these cross-run scientific identities:

- M4.12 committed config SHA-256:
  `d69b9abacc2e550006ab0fe71c2478e8bfbb089439195a2d83f08413352fac9e`;
- M4.12 canonical semantic-result SHA-256:
  `017fd20fb810652725846e214c884114c5afdc06cb009206e4bdaf899f9ecef7`;
- M4.12 predictions JSONL SHA-256:
  `0cd315438ff94d54923b8fdefc16ac1c20d9f9dc385ae2963251c31e04c4bc0e`;
  and
- M4.12 consumed-sample manifest SHA-256:
  `214885784d13912ce603cc30eaed5904fbb588e493405767d66bbb3a490787d6`.

Do not bind the final `report.json` or `manifest.json` hashes as scientific
identity. They include timing and environment telemetry and legitimately differ
between equivalent executions. M4.13 must recompute the canonical semantic
hash from M4.12's deterministic payload and reject a mismatch.

The hash-bound M4.12 consumed-diagnostic baseline is:

| Measure | Frozen M3 verifier |
|---|---:|
| Endpoints | 512 |
| Accuracy | 0.5059 |
| Macro-F1 | 0.4655 |
| ECE | 0.2779 |
| SUPPORT recall | 0.5977 |
| REFUTE recall | 0.2734 |
| NEUTRAL recall | 0.5547 |
| Contrastive transitions | 256 |
| Detected argmax change | 0.3281, 95% page CI [0.2617, 0.3906] |
| Both endpoints correct | 0.1992, 95% page CI [0.1445, 0.2578] |
| True-label bidirectional margin | 0.6836, 95% page CI [0.6211, 0.7383], under the deployed old-M3 temperature |

The same gate reported BGE two-version SUPPORT ranking recall@1 of 0.7891 and
MRR of 0.8945. Those are retrieval diagnostics, not verifier results, and must
not be attributed to M4.13.

This baseline passes the **adaptation-start criterion** because REFUTE recall
is below 0.30, fewer than one third of true transitions cause any argmax
change, and fewer than one fifth have both endpoints correct. These thresholds
were checked before candidate training. They are not a promise that the
proposed method will pass the terminal gate.

One audit correction matters for comparison: recomputing the same hash-bound
M4.12 raw logits at common `T=1` gives bidirectional margin `0.6796875`, not
`0.68359375`. The published M4.12 interval above belongs to its deployed
temperature surface and is not silently relabeled as a `T=1` interval.

### 3.2 Corrected M4.10 required inputs

An independent audit found that the first M4.10 implementation had an uncapped
UNION treatment, a mismatched vector-policy identity and synthetic work
telemetry. The old structural hash
`8d79324b8c3ecccefa42c42e811ccaf3fcde72a20633d324a03ccd88f7b20bef`
must therefore not be used as the M4.13 transfer reference.

M4.13 may retain only the qualitative observation that the frozen M3 verifier
failed obvious numeric, API and version changes. Before terminal transfer
evaluation, bind the corrected M4.10 configuration, source manifest, model
manifest, raw-logit manifest and result hashes. If the corrected rerun does not
reproduce the qualitative failure, report the discrepancy and do not silently
reuse the earlier narrative.

## 4. Primary-source and data provenance

The primary paper is [Get Your Vitamin C! Robust Fact Verification with
Contrastive Evidence](https://aclanthology.org/2021.naacl-main.52/) by Tal
Schuster, Adam Fisch and Regina Barzilay, NAACL 2021. The paper's relevant
premise is established, not ours: nearly identical evidence versions can
require different verdicts, and contrastive training can increase sensitivity
to those changes. The official code/data entrypoint is the pinned
[VitaminC repository](https://github.com/TalSchuster/VitaminC/tree/eb532922b88b199df68ed26afeb58dca5501b52f).

The preparation command must fail closed unless all of these values match:

| Artifact | Frozen identity |
|---|---|
| Official code repository | `https://github.com/TalSchuster/VitaminC.git` |
| Official code commit | `eb532922b88b199df68ed26afeb58dca5501b52f` |
| Dataset-hosting commit | `152c9639f1e33ca295d0776b618c17884088f72f` |
| Downloaded archive SHA-256 | `49d82dc1690cbee420d18e2c26f687a7937710bb211845d2571430dfd4dc0337` |
| Train JSONL SHA-256 | `446163d81ec9b654d2514e8ba5b8f2e4bd8938342e34c98e7660fe05a8a3ca39` |
| Development JSONL SHA-256 | `a3258bc959754c84bade150d3bf447fd9c91a529d93bb676f034fdafda5f26e5` |
| Test JSONL SHA-256 | `7f799e076dec184e31533d46704fef2022d08ef68a0db2c44f138d2b4e552f36` |
| Test rows/cases/pages | 55,197 / 16,487 / 3,068 |
| Test real/synthetic rows | 34,481 / 20,716 |

The downloaded files currently audit to the following complete dimensions;
the final preparer must re-derive rather than trust these literals:

| Official split | Rows | `case_id`s | Raw page strings |
|---|---:|---:|---:|
| Train | 370,653 | 112,426 | 22,198 |
| Development | 63,054 | 18,836 | 2,975 |
| Test | 55,197 | 16,487 | 3,068 |

Page identity is `normalize_pair_text(page)`, not the raw page string. Under
that exact normalization, official intersections are train-development 18,
train-test 22 and development-test 2, with one normalized page in all three and
40 unique normalized identities appearing in more than one split. The
train-development count differs from a raw-string audit because the `XXx`/`XXX`
near-duplicate collapses to one normalized identity. Quarantine all 40 from
M4.13 train and development construction. Do not merely group rows by
`case_id`: cases from the same normalized page can otherwise leak
article-specific language across partitions.

The data license states that annotations incorporate Wikipedia material under
the applicable article terms, or CC BY-SA 3.0 where those terms are
unavailable; synthetic annotations also derive from FEVER. M4.13 uses only
`revision_type=real`. Raw data and trained weights stay outside Git. Weight
redistribution requires a separate review because the mixed replay also uses
SciFact CC BY-NC 2.0 and WiCE material with its recorded underlying terms.

## 5. Case semantics and split construction

### 5.1 Eligible VitaminC case

An eligible case is an atomic four-row object, never four independently
sampled rows. It must satisfy all of the following:

- one `case_id`, one page and one `revision_type=real`;
- suffixes exactly `{1,2,3,4}` parsed from `unique_id`;
- rows 1 and 2 have the same claim;
- rows 3 and 4 have the same claim;
- rows 1 and 3 have the same evidence version A;
- rows 2 and 4 have the same evidence version B;
- exactly two distinct claims, two distinct evidence strings and four distinct
  claim/evidence pairs;
- each transition `(1,2)` and `(3,4)` contains SUPPORT plus one different
  label; and
- its page is legal for the split after article quarantine.

This yields two same-claim, cross-version transitions per case. Preserve the
case and page IDs in every later artifact.

### 5.2 Label mapping

Use the existing GroundLoop semantics without reinterpretation:

| VitaminC label | GroundLoop label | Base-logit index |
|---|---|---:|
| `SUPPORTS` | SUPPORT | entailment, 1 |
| `REFUTES` | REFUTE | contradiction, 0 |
| `NOT ENOUGH INFO` | NEUTRAL | neutral, 2 |

Do not map NOT ENOUGH INFO to REFUTE. The stored probability order remains
`(support, refute, neutral)` while raw MiniLM logits remain
`(contradiction, entailment, neutral)`.

### 5.3 Deterministic train and development samples

Use seed `20260720` for data identity. Rank an eligible case within each
stratum by

```text
sha256(seed || NUL || split || NUL || stratum || NUL || page || NUL || case_id)
```

Take at most one case per normalized page across both strata. Select strata in
the fixed order SUPPORT-REFUTE then SUPPORT-NEUTRAL, skipping a normalized page
already selected. The resulting frozen budgets are:

| Split | SUPPORT-REFUTE cases | SUPPORT-NEUTRAL cases | Rows | Unique pages |
|---|---:|---:|---:|---:|
| M4.13 train | 512 | 512 | 4,096 | 1,024 |
| M4.13 development | 128 | 128 | 1,024 | 256 |

Expected row labels in train are 2,048 SUPPORT, 1,024 REFUTE and 1,024
NEUTRAL. Development is 512/256/256. The preparer must assert these counts and
write every selected `unique_id`, `case_id`, page, revision ID, label,
claim SHA-256 and evidence SHA-256 to a canonical manifest. Selection may use
source labels to balance strata but must not use any model output.

The available audited pools after quarantining all 40 cross-split normalized
pages are large enough: train has 35,346 eligible SUPPORT-REFUTE and 7,416
eligible SUPPORT-NEUTRAL cases; development has 5,945 and 1,416. The preparer must
re-derive these counts and stop on drift.

### 5.4 Consumed M4.12 diagnostic and sealed terminal reserve

The M4.12 sample is consumed. Its 128 pages and manifest
`214885784d13912ce603cc30eaed5904fbb588e493405767d66bbb3a490787d6`
remain an adaptation-start diagnostic and provenance-regression input only.
No adapted checkpoint may be evaluated on it, and it must not participate in
M4.13 selection, calibration or terminal candidate comparison.

Before the first optimizer update, the data lane must freeze a different
terminal reserve from the remaining official VitaminC test cases. Use reserve
seed `20260721`, fixed stratum order SUPPORT-REFUTE then SUPPORT-NEUTRAL, and
selection key:

```text
sha256(seed || NUL || stratum || NUL || raw_page || NUL || case_id)
```

Eligibility is Section 5.1 plus all of these exclusions:

- normalized page occurs in official VitaminC train or development;
- normalized page occurs in the consumed M4.12 diagnostic; or
- normalized page was already selected for another reserve case.

Select 64 SUPPORT-REFUTE cases followed by 64 SUPPORT-NEUTRAL cases. The
pre-training freeze audit yields:

- 128 real cases on 128 distinct normalized pages;
- zero overlap with the 128 consumed M4.12 normalized pages;
- 512 endpoints: 256 SUPPORT, 128 REFUTE and 128 NEUTRAL;
- eligible post-exclusion pools of 3,310 SUPPORT-REFUTE and 626
  SUPPORT-NEUTRAL cases; and
- terminal-reserve manifest SHA-256
  `3dcfcba0b809e3bcfcf2f9c036c4946f484d706faa61eec8c3fa489fbcb11dc5`.

The terminal manifest uses the same canonical row schema as M4.12: stratum,
selection key, row/case/page/revision identities, source label, claim hash and
evidence hash. The preparer must re-derive the hash and counts from the pinned
source. The full official test has zero exact normalized claim, evidence or
pair overlap with the M3 prepared splits, but the reserve audit must reassert
that property rather than inherit it silently.

Split reserve preparation from model development. Training and calibration
commands receive only the train/development manifests and the digest of
normalized page identities they must exclude. They must have no terminal path
argument and emit no terminal text, label, ID or metric. Store the complete
reserve externally under the evaluation lane's sealed artifact root; only the
terminal evaluator may resolve its rows after `selection.json` is sealed.

This reserve is held out from M4.13 model training and selection, but the
overall experiment is not blind in the strongest scientific sense: M4.12 used
a sibling label-stratified sample from the same official test split to motivate
the objective and absolute floors. Describe the reserve as a **pre-frozen,
page-disjoint terminal diagnostic**, not an untouched representative benchmark.
A publication-quality claim would need an independently locked evaluation.

### 5.5 M3 mixed replay

Reuse the complete prepared M3 splits and fail on checksum drift:

| Artifact | SHA-256 / count |
|---|---|
| Prepared manifest | `1d1ef303a8a7d5b560b6e3a558573953d52a02aa227a96aff5b90f18b14aec7c` |
| Train JSONL | `1b76ed557b43955980e57a0f049666496335b649caa6e406c2e7b0245e7e3f4e`, 3,022 rows, 2,067 claim groups |
| Development JSONL | `a9cd74df8df6e6f7a825ee446d222adc87a7bcf61c9394f45efaacc1cb479882`, 987 rows, 649 claim groups |
| Public test JSONL | `3ccd2c761bed3101f65dadd75a597afd831d3d95041113c2f0236b968cec6a2f`, 358 WiCE rows/groups |

Replay all 3,022 M3 training rows once per mixed epoch. Do not sample a
convenient subset: preserving the old SciFact/WiCE behavior is part of the
experiment. Keep all examples from an M3 `claim_group_id` in the same official
split. Use the complete M3 development set for selection/calibration and the
complete original M3 public test only in the terminal evaluation.

The M3 public test has 111 SUPPORT, zero REFUTE and 247 NEUTRAL rows, and all
358 pairs truncate at 256 tokens. It cannot guard REFUTE forgetting. Add a
development-only per-class forgetting guard and retain this limitation in every
report.

## 6. Frozen model and experimental variants

All variants start from, or compare with, the existing M3 artifact:

- base family: `cross-encoder/nli-MiniLM2-L6-H768` at
  `b95119ce93d3e065de6214e38cd4a97b0f2f2c6d`;
- continuation checkpoint tree:
  `81870b683cec57eff82665103fcff3a35f45b9c9be0e18b53dcd40f485bfa4cf`;
- weights SHA-256:
  `81c49c30048dcbcc9fb2895b56622f702b4aa9894e7d055f28bb520c09f74e3e`;
- old calibration version:
  `temperature-v1:6ae200db8d75477da143bd6d8d6c8927cfdfdbd8995932bbc1c59ce67e090727`;
  and
- old temperature: `1.1037657679769346`.

Do not add a larger transformer sweep. On this CPU host, the scientifically
useful comparison is the training objective under an identical compact model
and budget.

### 6.1 Required variants

| ID | Training data | Objective | Seeds | Role |
|---|---|---|---|---|
| `V0-frozen-m3` | none | none | n/a | frozen checkpoint control, scored on the new reserve only at terminal evaluation |
| `V1-replay-only` | M3 replay, deterministically repeated to the mixed step budget | endpoint CE | primary only | controls for extra optimizer updates |
| `V2-ce-mix` | VitaminC train + complete M3 replay | endpoint CE | 20260720, 20260721, 20260722 | ordinary continuation baseline |
| `V3-margin-mix` | identical to V2 | endpoint CE + paired revision margin | same three | proposed candidate |
| `A1-margin-no-replay` | VitaminC only, cycled to the same step budget | endpoint CE + paired margin | primary only | exposes catastrophic forgetting |

V2 and V3 are the only candidates eligible to become the new verifier. V1 and
A1 are mandatory ablations, not deployment candidates.

### 6.2 Exact training schedule

- Maximum sequence length: 256 wordpieces.
- Microbatch size: 8.
- Gradient accumulation: 4.
- Epochs: 1.
- Learning rate: `1e-5`.
- Weight decay: `0.01`.
- Warmup ratio: `0.06`.
- Gradient norm clip: `1.0`.
- Torch threads: 8; inter-op threads: 1.
- Deterministic algorithms: enabled.
- VitaminC microbatch: two complete four-row cases.
- M3 microbatch: eight rows after deterministic claim-group-level shuffle.
- Mixed epoch: 512 VitaminC microbatches and 378 M3 microbatches, merged by a
  deterministic proportional schedule; 890 microbatches and 223 optimizer
  steps after accumulation.
- Divide every accumulated loss by the actual number of microbatches in its
  optimizer group. The first 222 groups have size four and the final group has
  size two; the final group is therefore divided by two, not four. Bind these
  per-step divisors into the optimizer-schedule identity.

Compute inverse-frequency class weights from the exact materialized mixed
epoch in base-logit order and record them. V2 and V3 must have byte-identical
row order, batch boundaries, class weights, scheduler steps and random seed for
each corresponding seed. Only the extra loss term may differ. If that identity
check fails, the ablation is invalid.

V1 and A1 deterministically cycle their own source at the claim-group or case
boundary to exactly 890 microbatches. Mark the repeated examples in the
training manifest; they are controls, not independent observations.

## 7. Candidate objective

Let `z_i` be the three raw logits in base order and
`q_i = log_softmax(z_i)`. Let `y_i` be the mapped base-order class index.

The weighted endpoint loss over a microbatch is the existing three-way
cross-entropy:

```text
L_CE = mean_i -w[y_i] * q_i[y_i]
```

For each same-claim, two-version transition `(a,b)` whose correct labels differ,
define the shift-invariant paired margin on log probabilities:

```text
L_pair(a,b) = 0.5 * [
    softplus(m - (q_a[y_a] - q_b[y_a]))
  + softplus(m - (q_b[y_b] - q_a[y_b]))
]
```

The term asks version A to assign more log probability to A's correct label
than version B does, and symmetrically for B's correct label. It cannot be
satisfied merely by changing an example-wide logit offset. Each eligible
VitaminC case contributes transitions `(1,2)` and `(3,4)`.

Freeze:

```text
m = 0.5 log-probability units
lambda = 0.25
L_V3 = L_CE + lambda * mean_transition(L_pair)
```

On M3 replay microbatches, `L_V3 = L_CE`. V2 uses `L_CE` everywhere. Do not
tune `m` or `lambda` further against the consumed M4.12 diagnostic, sealed
terminal reserve or Git pilot. They are a single pre-registered design choice.
If V3 loses to V2 on development, that is a negative result; do not search a
grid after seeing terminal behavior.

This paired loss is an ablation/design choice inspired by VitaminC's
contrastive structure. Do not call it novel without a separate current
literature review.

## 8. Development selection and catastrophic-forgetting guard

Evaluate V0, V2 and V3 on the frozen M4.13 VitaminC development cases and M3
development groups. V1 and A1 are diagnostic controls.

Select between V2 and V3 using only development data:

1. Reject a variant if its M3-development macro-F1 falls by more than 0.03
   absolute from V0 or any M3-development per-class F1 falls by more than 0.05.
2. Among surviving variants, choose the larger median three-seed VitaminC
   development joint-transition accuracy defined in Section 10.
3. If the medians differ by less than 0.01 absolute, choose V2 because it is the
   simpler objective.
4. Designate seed `20260720` as the deployable primary before test evaluation.
   Seeds 20260721 and 20260722 estimate replication sensitivity; never choose
   the best test seed.

Only a variant that passes every forgetting guard is eligible. If V2 is
ineligible and V3 passes the guards but does not satisfy the Section 12.1
paired-term usefulness rule, seal `selection.json` with no eligible selection
and stop before terminal access. Never fall back to a known-unsafe V2. If both
variants are ineligible, apply the same no-selection outcome.

Write `selection.json` containing all development metrics, checkpoint hashes,
the exact rule above and the selected variant **before** any candidate is run
on VitaminC test, M3 test or corrected M4.10. The selection must also bind the
hash of the complete immutable development report, including mandatory V1 and
A1 ablation metrics; terminal validation must reject a missing or changed
report even though those ablations are not terminal candidates.

Publish raw development logits, the complete development report and selection
as one failure-atomic staged bundle. Before writing anything, an existing
sealed bundle must either validate as an exact replay of the same invocation
or cause a collision failure. Never overwrite logits or the report and only
then discover that an older selection disagrees; that would destroy the
evidence required to reproduce the sealed decision.

## 9. Calibration

Fit one scalar temperature per selected checkpoint after selection, using only
the two development sources. Frozen operational decision thresholds remain
unchanged.

To prevent the larger VitaminC development sample from dominating, minimize
the equal-domain, group-balanced objective:

```text
NLL(T) = 0.5 * mean over M3 claim groups(
                    mean row NLL within group)
       + 0.5 * mean over VitaminC cases(
                    mean of four endpoint NLLs)
```

Use the existing deterministic positive log-temperature golden search. The
calibration artifact identity must additionally include:

- checkpoint tree digest;
- M3 development JSONL hash;
- VitaminC development sample-manifest hash;
- development-logits file hash;
- group weighting formula and domain weights;
- example/group/case counts;
- temperature, NLL before/after by domain and combined; and
- method/version identifier.

Bind the exact clean repository commit and calibrator implementation-file hash
used to produce each artifact. Before terminal scoring, the independent
evaluation path must reconstruct the bound development rows and rerun the
frozen objective, search, NLL surfaces and acceptance rule, rejecting any
semantic mismatch. A self-consistent calibration JSON is not sufficient
evidence by itself.

Accept the new temperature only if combined development NLL decreases and
neither domain NLL increases by more than 0.01. Otherwise deploy the
uncalibrated `T=1` scores for this candidate and record calibration failure.
Scalar temperature cannot change argmax transition metrics, but it can change
GroundLoop's threshold-derived operational labels.

Report uncalibrated, old-M3-temperature and newly calibrated results as a
calibration ablation. Never choose among them on terminal test.

All endpoint and GroundLoop-policy calibration surfaces must identify the
temperature used. Transition `flip_detected`, `joint_correct`, and
`bidirectional_margin` comparisons use one common uncalibrated surface,
`T=1`, for every model. In particular, do not compare bidirectional margins
computed under different fitted temperatures; that would confound model
adaptation with calibration.

## 10. Metrics and uncertainty

### 10.1 Endpoint metrics

For each dataset and model, report:

- accuracy and three-class macro-F1;
- precision, recall and F1 per present class;
- confusion matrix in stored `(support, refute, neutral)` order;
- NLL, multiclass Brier score, ECE with ten fixed equal-width bins;
- truncation count, failures, batches, wall time and peak RSS; and
- both raw argmax and frozen-policy operational label counts.

Absent-class recall/F1 is null, not zero.

### 10.2 Transition metrics

For each transition `(a,b)` with different true endpoint labels, define:

```text
flip_detected = 1[argmax(p_a) != argmax(p_b)]

joint_correct = 1[argmax(p_a) == y_a and argmax(p_b) == y_b]

bidirectional_margin = 1[
    p_a[y_a] > p_b[y_a] and p_b[y_b] > p_a[y_b]
]
```

`joint_correct` is the primary **flip-consistency accuracy**. A raw prediction
flip can be wrong at both endpoints, so `flip_detected` is never the primary
success measure. Also report case-complete accuracy: all four endpoints in one
VitaminC case correct.

Report each metric overall and separately for SUPPORT-REFUTE and
SUPPORT-NEUTRAL. The REFUTE stratum is essential because M4.10 exposed a
contradiction failure.

### 10.3 Confidence intervals

Use exactly 1,000 95% percentile bootstrap resamples with seed `20260720` and
preserve dependence:

- primary VitaminC intervals resample Wikipedia pages and include all cases and
  transitions from the sampled page;
- case-level intervals resample complete `case_id`s as a sensitivity analysis;
- M3 intervals resample complete `claim_group_id`s; and
- baseline-candidate deltas use a paired bootstrap with the same sampled units
  for both models.

The production terminal path must reject any other seed or resample count.
Reduced-count overrides are permitted only in an explicitly synthetic,
non-scientific test path that cannot emit a promotion verdict.

The frozen M4.13 terminal reserve has one case per normalized page, so page and
case point estimates and intervals should coincide. Still emit both labeled
surfaces so a later multi-case-per-page evaluation cannot silently
row-bootstrap. Do not pool rows across the three training seeds. Report each
seed, their arithmetic mean and standard deviation; three seeds do not justify
a confidence interval over training randomness.

The corrected Git pilot is too small for a meaningful bootstrap interval.
Report exact per-pair decisions and counts.

## 11. Terminal evaluation protocol

After `selection.json` and all calibration artifacts are sealed:

1. Verify the persisted M4.12 predictions and semantic-result hashes without
   running an adapted checkpoint on the consumed diagnostic. This is a
   provenance regression, not a terminal comparison.
2. Unlock the terminal reserve identified by
   `3dcfcba0b809e3bcfcf2f9c036c4946f484d706faa61eec8c3fa489fbcb11dc5`.
3. Score V0 and the selected variant's three seeds on exactly those 512 reserve
   endpoints. V0 and each candidate must use the same row order, tokenizer
   truncation contract and metric implementation.
4. Evaluate V0 and the same candidate checkpoints once on the original M3
   public test.
5. Evaluate V0 and the predesignated candidate primary seed once on the
   corrected, hash-bound M4.10 Git pilot with the frozen GroundLoop policy.
6. Write raw logits/probabilities before computing summaries, then derive all
   metrics from those immutable rows.

Training and development commands must not accept a test path. The terminal
command must require
`--final-test-manifest-sha256 3dcfcba0b809e3bcfcf2f9c036c4946f484d706faa61eec8c3fa489fbcb11dc5`,
reject every candidate checkpoint absent from `selection.json`, and allow V0
only by its exact frozen tree digest. Exact rerun of the same
checkpoint/configuration is allowed for reproducibility; introducing a new
checkpoint after terminal metrics creates a new milestone and requires a new
held-out reserve.

The Git pilot remains a transfer diagnostic unless its expected labels are
independently annotated and adjudicated. Fixture-author construction notes are
not objective gold.

## 12. Pre-registered stop/go criteria

### 12.1 Objective-design verdict: paired margin versus CE

Call the paired-margin term useful only if V3, compared with V2 under the
identical budget:

- improves median VitaminC development `joint_correct` by at least 0.01;
- passes every M3 development forgetting guard; and
- is not driven by one seed, meaning at least two of three V3 seeds match or
  exceed their corresponding V2 seed.

Otherwise select V2 and report that the paired term did not justify its added
complexity.

The sentence above applies only when V2 itself passes every Section 8
forgetting guard. If V2 is unsafe and V3 is not useful by all three criteria,
there is no eligible M4.13 selection and terminal evaluation remains locked.

### 12.2 Default-model go gate

Promote the development-selected variant as the new experimental GroundLoop
verifier only if all conditions hold for the predesignated primary seed, and at
least two of three seeds satisfy the point thresholds:

1. On the new reserve, VitaminC `joint_correct >= 0.30`. This floor was frozen
   from M4.12's consumed diagnostic value 0.1992 and upper CI 0.2578.
2. On the new reserve, VitaminC `flip_detected >= 0.50`. This floor was frozen
   from M4.12's 0.3281 and upper CI 0.3906.
3. On the new reserve, VitaminC `bidirectional_margin >= 0.75`. This remains a
   conservative pre-registered floor above M4.12's deployed-temperature upper
   CI 0.7383, but it is not claimed to be an apples-to-apples `T=1` CI-derived
   threshold; the common-`T=1` M4.12 point is 0.6796875.
4. The primary seed's paired page-bootstrap 95% lower bound for the
   new-reserve `joint_correct` delta over V0 is greater than zero, with point
   delta at least +0.10.
5. The primary seed's paired page-bootstrap 95% lower bound for the
   new-reserve `flip_detected` delta over V0 is greater than zero.
6. The paired new-reserve lower bound for the bidirectional-margin delta over
   V0 is at least -0.03; the model may not buy hard flips by destroying
   relative score sensitivity.
7. Original M3 public-test macro-F1 is at least 0.4998 and accuracy at least
   0.5873, no more than 0.03 below the frozen calibrated M3 values 0.5298 and
   0.6173.
8. The paired M3 claim-group bootstrap lower bound for both macro-F1 and
   accuracy deltas is at least -0.05.
9. Calibration passes Section 9 or is explicitly rejected in favor of `T=1`;
   a failed calibrator may not be hidden behind an improved argmax score.
10. All provenance, determinism, raw-output completeness and software gates
    pass with zero missing rows.

M4.12's consumed metrics set the pre-training absolute floors only. They are
never substituted for V0's predictions on the new reserve and never enter a
paired candidate delta.

If the point thresholds pass but the primary paired interval includes zero,
classify the result as **engineering-positive but statistically inconclusive**;
do not make it the headline model-improvement claim. If the VitaminC gate passes
but the M3 forgetting guard fails, do not promote the checkpoint.

### 12.3 Git transfer diagnostic

If independent/adjudicated labels are added before the sealed terminal run,
require at least four of the five previously identified obsolete claims
(Ubuntu 22.04, 16/16 precision, Boost 1.81, `sbitint.get_type`, hardware TEE) to
be REFUTE, none to be SUPPORT, and no more than one regression on an adjudicated
stable claim. Without independent labels, report these same exact counts as a
diagnostic but do not use them as a go/no-go decision.

Failure is a valid M4.13 result. Do not tune against terminal errors and rerun
the same test under the M4.13 name.

## 13. Artifact and audit contract

Generated artifacts remain outside Git. Every deterministic JSON/JSONL file is
canonicalized and hashed. Runtime timing gets a separate hash so wall-clock
variation does not change semantic identity.

Required external artifact tree:

```text
artifacts/m4_13_change_aware_verifier/
  source/source_manifest.json
  prepared/train_vitaminc.jsonl
  prepared/development_vitaminc.jsonl
  prepared/train_m3_replay.jsonl
  prepared/development_m3.jsonl
  prepared/test_m3.jsonl
  prepared/dataset_manifest.json
  sealed/terminal_reserve.jsonl
  sealed/terminal_reserve_identity.json
  runs/<variant>/<seed>/training_manifest.json
  runs/<variant>/<seed>/checkpoint_identity.json
  runs/<variant>/<seed>/development_logits.jsonl
  runs/<variant>/<seed>/calibration.json
  selection.json
  terminal/vitaminc_logits.jsonl
  terminal/m3_test_logits.jsonl
  terminal/git_pilot_logits.jsonl
  terminal/metrics.json
  terminal/bootstrap.json
  terminal/result_manifest.json
  runtime/timings.json
```

The top-level result manifest must bind:

- the exact clean repository commit used by training, the canonical aggregate
  trainer-implementation hash, and its per-file dependency hashes;
- the exact clean repository commit used by evaluation, the canonical
  evaluator-implementation hash, and its per-file dependency hashes;
- the exact clean repository commit and implementation hash used by
  calibration, plus the evaluator's independent calibration-replay verdict;
- primary paper DOI/Anthology ID and source URLs;
- source repository, hosting commit, archive/member/license hashes;
- exact row/case/page sampling manifests and split-intersection audit;
- M3 data/checkpoint/calibration hashes;
- M4.12 configuration, deterministic semantic result, consumed-sample and
  prediction-logit hashes, explicitly labeled adaptation-start only;
- M4.13 terminal-reserve seed, canonical manifest hash and normalized-page
  exclusion digest, plus the sealed reserve file hash and proof that its raw
  row order equals the canonical manifest order;
- corrected M4.10 configuration/source/model/result hashes;
- variant, seeds, batch order hash, loss formula, hyperparameters and optimizer
  step count;
- dependency versions, platform, CPU thread settings and deterministic flags;
- checkpoint tree and weights hashes;
- every raw-logit, calibration, metric and bootstrap file hash;
- truncations, failures and timeouts; and
- the exact stop/go verdict with failed clauses enumerated.

Raw-logit rows must include fixture, split, page, case/group ID, transition ID,
row ID, claim/evidence hashes, mapped label, input hash, logits, uncalibrated
probabilities, calibrated probabilities, truncation flag, model identity and
calibration identity. Do not commit raw licensed text or weights.

## 14. Implementation phases and parallel ownership

Parallelism begins only after the coordinator freezes the M4.12 scientific
identities and M4.13 terminal-reserve identity in the new config. Corrected
M4.10 hashes may remain pending during code development and training, but must
be frozen before the terminal Git transfer run. Actual model runs are
serialized on the CPU host even if code development is parallel.

### Lane A — provenance and preparation

Owned new paths:

- `configs/m4/verifier/change_aware_v1.json`
- `training/m4_13_verifier/prepare.py`
- `tests/m4/change_aware_verifier/data/**`
- `docs/workstreams/m4_13_change_aware_verifier/DATA_HANDOFF.md`

Deliver the fail-closed source validator, page-quarantine audit, four-row case
validator, deterministic train/dev manifests, sealed terminal-reserve identity
and fixture-sized unit tests. The training-facing output exposes only the
reserve exclusion digest, never reserve rows or labels. Do not read candidate
model outputs.

### Lane B — training and calibration

Owned new paths:

- `training/m4_13_verifier/losses.py`
- `training/m4_13_verifier/train.py`
- `training/m4_13_verifier/calibrate.py`
- `tests/m4/change_aware_verifier/training/**`
- `docs/workstreams/m4_13_change_aware_verifier/TRAINING_HANDOFF.md`

Consume only Lane A's manifest schema. Implement schedule identity checks,
the two exact objectives, deterministic manifests and group-balanced scalar
calibration. Unit-test the paired loss with hand-computed logits and gradients;
do not train real weights until Lane A is frozen.

### Lane C — evaluation and reporting

Owned new paths:

- `experiments/m4_change_aware_verifier/**`
- `tests/m4/change_aware_verifier/evaluation/**`
- `docs/workstreams/m4_13_change_aware_verifier/EVALUATION_HANDOFF.md`

Implement transition metrics, page/case/claim-group paired bootstraps, terminal
checkpoint allow-listing, M4.12 consumed-diagnostic provenance validation,
new-reserve V0/candidate scoring, corrected M4.10 transfer ingestion and
stop/go reporting. M4.12 is not the terminal baseline. Develop against small
synthetic logits only; do not unlock reserve rows or inspect candidate terminal
outputs before `selection.json` is sealed.

### Coordinator-only surfaces

The coordinator integrates the lanes, runs the real experiment, applies any
necessary edits to shared files, and writes the final status. Lanes must not
edit `pyproject.toml`, `AGENTS.md`, `src/groundloop/ai/contracts.py`, existing
M3 code, existing M4 runtime code, or shared milestone/decision documents.

Interface barrier: Lane A publishes a versioned dataset manifest schema; Lane B
publishes checkpoint/training/calibration schemas; Lane C consumes both. A
schema change is a coordinator decision, not three independent fixes.

## 15. Expected runtime and resources

Measured M3 reference on this host: 3,022 rows, 378 microbatches and 95 optimizer
steps took 27m38s, with 3,347,492 KiB peak RSS. M4.13 keeps the same model,
maximum length and microbatch size but raises each mixed run to 890
microbatches/223 steps.

Budget, not a performance claim:

| Work | Expected wall time | Peak memory / disk |
|---|---:|---:|
| Source validation and deterministic preparation | 2-10 min after local archive exists | about 207 MiB extracted source plus manifests |
| One mixed training run | 45-80 min | expected under 4 GiB RSS; abort above 8 GiB |
| Six V2/V3 three-seed runs | 4.5-8 h serialized | about 1.9 GiB checkpoints |
| V1 and A1 primary-seed ablations | 1.5-2.7 h serialized | about 0.7 GiB checkpoints |
| Development/calibration and terminal inference | 1-3 h total | expected under 2 GiB RSS |
| Full milestone | approximately 7-14 h CPU wall time | reserve 5 GiB artifact space |

These ranges are extrapolations, not measurements. Record actual model-load,
training, calibration and inference times separately. Never run two real model
processes concurrently on the 14 GiB host. Ordinary tests must use tiny logits
fixtures and perform no downloads.

## 16. Verification gates

Before a real run:

- unit-test source hash mismatch, row drift, malformed cases, cross-split page
  leakage, normalized duplicate detection and test-path rejection;
- unit-test label/logit order in both directions;
- unit-test paired loss against a hand calculation and verify it decreases when
  the correct version-specific log-probability gaps increase;
- assert V2/V3 batch-order and optimizer-schedule hash equality per seed;
- unit-test exact page-, case- and claim-group-level bootstrap membership;
- unit-test that paired deltas use identical resampled units;
- unit-test absent classes remain null;
- unit-test checkpoint allow-listing and exact terminal replay;
- failure-inject before checkpoint write, manifest write and result sealing; and
- verify partial runs cannot be mistaken for complete reports.

Repository gates:

```bash
.venv/bin/pytest -q tests/m4/change_aware_verifier
.venv/bin/ruff check training/m4_13_verifier \
  experiments/m4_change_aware_verifier tests/m4/change_aware_verifier
.venv/bin/mypy --strict training/m4_13_verifier \
  experiments/m4_change_aware_verifier
.venv/bin/python -m compileall -q training/m4_13_verifier \
  experiments/m4_change_aware_verifier tests/m4/change_aware_verifier
```

After integration, run the complete repository suite and the existing live
PostgreSQL validator. M4.13 should not need a schema migration because it
creates a new immutable model/calibration identity and reuses existing model
registries. The coordinator must stop if implementation discovers otherwise.

## 17. Exact non-claims

Even if every gate passes, do not claim:

- a novel contrastive-learning method;
- state-of-the-art fact verification or VitaminC performance;
- representative performance on the full VitaminC distribution;
- an untouched/blind test in the strongest sense;
- general transfer from Wikipedia to software documentation, Git histories or
  arbitrary RAG corpora;
- objective truth, guaranteed contradiction detection or hallucination
  elimination;
- improved retrieval, generation, claim extraction or admission recall;
- any stronger exactness, complexity or performance theorem for GroundLoop's
  IVM engine;
- that the online model compares two revisions—it scores one evidence/claim
  pair at a time;
- calibration improvement across domains unless each held-out domain supports
  it;
- commercial redistributability of the mixed-data checkpoint;
- security or privacy; or
- that the tiny Git pilot supplies statistically reliable model quality.

The strongest defensible positive statement, if the go gate passes, is:

> Under a pinned, article-disjoint, label-stratified VitaminC diagnostic and a
> fixed CPU budget, continued training of GroundLoop's compact verifier
> improved paired revision response relative to the frozen M3 checkpoint while
> staying within a pre-registered forgetting margin on the original M3 test.

That statement must be followed immediately by the sample, transfer,
calibration and non-blind-test limitations above.

## 18. Completion checklist

M4.13 is complete only when:

1. exact M4.12 config/semantic-result/predictions/consumed-sample identities,
   the M4.13 terminal-reserve identity and corrected M4.10 hashes are bound;
2. source, license and every split hash validate;
3. train, development and terminal reserve are normalized-page-disjoint, and
   the reserve excludes every consumed M4.12 page;
4. V0, V1, V2, V3 and A1 execute under the recorded budgets;
5. all V2/V3 seeds and ablations emit complete checkpoint manifests;
6. selection is sealed using development only;
7. calibration uses development only and passes or is explicitly rejected;
8. V0 plus selected candidates execute once on the new terminal reserve, and
   original M3 plus corrected Git evaluations execute under the frozen
   selection;
9. page/case/group uncertainty and paired deltas are emitted;
10. the exact stop/go clauses are machine-evaluated and reported;
11. no raw datasets, weights, caches or secrets enter Git; and
12. focused and full software gates pass with exact command evidence.

If these conditions are not met, the correct status is partial or negative,
not “M4 complete.”
