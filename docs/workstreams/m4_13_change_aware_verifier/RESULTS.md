# M4.13 change-aware verifier: final results

Status: **completed scientific execution; pre-registered verdict `NO_GO`**

Execution date: 2026-07-20; final repository validation: 2026-07-21

Canonical external artifact root:
`artifacts/m4_13_change_aware_verifier/`

The terminal bundle is sealed and complete. The development-selected model was
`V2-ce-mix`, but it is **not authorized for promotion** as GroundLoop's default
verifier. The only failed terminal clause was the pre-registered M3
non-inferiority interval in G8. No threshold, objective, checkpoint, seed or
terminal metric was changed after terminal access.

This document reports the executed experiment. The preregistration remains
unchanged in [`docs/m4_13_change_aware_verifier_plan.md`](../../m4_13_change_aware_verifier_plan.md).
The machine-readable artifacts, rather than rounded values in this document,
are authoritative.

## 1. Result in one paragraph

Ordinary mixed endpoint cross-entropy continuation (`V2`) materially improved
revision response over the frozen M3 verifier (`V0`) on the pre-frozen
VitaminC terminal diagnostic. For primary seed 20260720, joint correctness rose
from 0.250000 to 0.382812, detected flips from 0.394531 to 0.582031, and the
bidirectional margin criterion from 0.683594 to 0.816406. All three V2 seeds
passed the frozen point floors. The proposed paired-margin objective (`V3`) did
not earn its added complexity on development: its median joint-correct gain
over V2 was only 0.001953, below the pre-registered 0.01 requirement. V2 also
met the absolute M3 test floors, but its primary paired macro-F1 delta interval
was `[-0.057113, 0.043549]`; the lower endpoint crossed the permitted -0.05
loss by 0.007113. Consequently G8 failed and the frozen verdict is `NO_GO`.

Confidence in this bounded result: **high**. Confidence that it generalizes
from the label-stratified Wikipedia diagnostic to software documentation or
arbitrary RAG corpora: **low**.

## 2. Frozen experiment that executed

All eight required trainable runs completed under the same clean Git commit
and trainer implementation:

| Variant | Seeds | Role | Terminal eligibility |
|---|---|---|---|
| `V1-replay-only` | 20260720 | Extra-optimizer-step control using only M3 replay | Never eligible |
| `V2-ce-mix` | 20260720, 20260721, 20260722 | VitaminC plus complete M3 replay with endpoint CE | Eligible |
| `V3-margin-mix` | 20260720, 20260721, 20260722 | Same schedule as V2 plus the frozen paired-margin term | Eligible |
| `A1-margin-no-replay` | 20260720 | VitaminC-only forgetting ablation | Never eligible |

Every run used 890 microbatches, gradient accumulation four and 223 optimizer
steps. Corresponding V2/V3 seeds had identical source row order, batch
boundaries, class weights and optimizer schedule; only the objective differed.
The frozen M3 checkpoint, not an upstream reconstruction, initialized every
run.

The data surfaces were:

- training: 4,096 VitaminC endpoints in 1,024 four-row cases plus all 3,022
  M3 replay rows;
- development: 1,024 VitaminC endpoints in 256 cases and 987 M3 rows in 649
  claim groups;
- terminal VitaminC diagnostic: 512 endpoints in 128 cases on 128 distinct
  normalized pages, balanced as 64 SUPPORT-REFUTE and 64 SUPPORT-NEUTRAL
  cases; and
- original M3 public test: 358 rows/claim groups, with 111 SUPPORT, no REFUTE
  and 247 NEUTRAL rows.

The terminal reserve was page-disjoint from the training and development
surfaces and excluded all pages consumed by M4.12. It is nevertheless a
pre-frozen, label-stratified diagnostic from the same official VitaminC test
split that motivated M4.12, not an untouched representative benchmark.

## 3. Development selection

### 3.1 V2 versus V3

Development used all three frozen seeds and no terminal data:

| Variant and seed | VitaminC joint correct | M3 macro-F1 | Forgetting guard |
|---|---:|---:|---|
| V2 / 20260720 | 0.382812 | 0.777675 | pass |
| V2 / 20260721 | 0.382812 | 0.777327 | pass |
| V2 / 20260722 | 0.382812 | 0.776853 | pass |
| V3 / 20260720 | 0.380859 | 0.773762 | pass |
| V3 / 20260721 | 0.390625 | 0.771641 | pass |
| V3 / 20260722 | 0.384766 | 0.777925 | pass |

The V2 median was 0.382812 and the V3 median was 0.384766. V3 matched or beat
the corresponding V2 seed twice, but its median gain was only 0.001953 instead
of the required 0.01. The sealed objective-design verdict is therefore
`PAIRED_MARGIN_NOT_USEFUL`, and the simpler `V2-ce-mix` was selected before
terminal access.

This is a negative result for the specific frozen margin (`m=0.5`,
`lambda=0.25`) under this budget. It does not prove that all paired or
contrastive objectives are useless.

### 3.2 Diagnostic controls

`V1-replay-only` reached only 0.226562 development joint correctness, compared
with V0's 0.238281. Additional optimizer steps on M3 replay therefore do not
explain V2's revision gain.

`A1-margin-no-replay` reached 0.453125 development joint correctness, but its
M3 development macro-F1 was 0.606413 versus V0's 0.756065. It was deliberately
not eligible for selection. The ablation shows the expected trade-off: a
VitaminC-only objective can improve in-domain revision behavior while damaging
the old development domain. Complete M3 replay was not decorative.

## 4. Calibration

After selection, one group-balanced scalar temperature was fitted using only
M3 and VitaminC development data for each V2 checkpoint. All three fits passed
the frozen acceptance rule; no terminal score selected a temperature.

| Seed | Deployed temperature | Uncalibrated combined NLL | Deployed combined NLL | Accepted |
|---:|---:|---:|---:|---|
| 20260720 | 1.251319981 | 0.639864545 | 0.627590331 | yes |
| 20260721 | 1.261888216 | 0.642343365 | 0.629067886 | yes |
| 20260722 | 1.255728988 | 0.643832473 | 0.630858582 | yes |

The objective weighted M3 and VitaminC equally, averaging within 649 M3 claim
groups and 256 VitaminC cases. The evaluator independently replayed each
calibration before terminal access. Scalar calibration affects endpoint
probabilities and GroundLoop's thresholded operational labels; it cannot
change argmax-based revision transitions. All transition comparisons below
therefore use the common uncalibrated `T=1` surface.

## 5. Terminal result

### 5.1 Primary VitaminC result

Intervals are the pre-registered paired, normalized-page-clustered,
fixed-stratum percentile intervals: 1,000 resamples, seed 20260720. Because the
reserve has one case per page, page- and case-clustered point estimates
coincide. Endpoint and case metrics use each model's frozen deployed
temperature; transition metrics use common `T=1`.

| Metric | V0 point [95% CI] | V2 primary point [95% CI] | Paired delta [95% CI] |
|---|---:|---:|---:|
| Endpoint accuracy | 0.546875 [0.503857, 0.591797] | 0.636719 [0.587842, 0.689453] | +0.089844 [0.048828, 0.136719] |
| Endpoint macro-F1 | 0.508918 [0.458628, 0.557296] | 0.623702 [0.573524, 0.678835] | +0.114784 [0.069546, 0.166653] |
| Endpoint NLL | 1.110953 [0.985568, 1.234334] | 0.757710 [0.687941, 0.826625] | -0.353243 [-0.445410, -0.267072] |
| Endpoint multiclass Brier | 0.625125 [0.562616, 0.684781] | 0.445832 [0.400021, 0.490348] | -0.179293 [-0.223050, -0.138239] |
| Endpoint ECE | 0.240223 [0.197554, 0.281461] | 0.052391 [0.035248, 0.093311] | -0.187832 [-0.225925, -0.123503] |
| Four-endpoint case complete | 0.117188 [0.070312, 0.171875] | 0.250000 [0.179688, 0.328125] | +0.132812 [0.070312, 0.203125] |
| Transition joint correct | 0.250000 [0.195312, 0.312500] | 0.382812 [0.312500, 0.457129] | +0.132812 [0.078125, 0.199219] |
| Transition flip detected | 0.394531 [0.332031, 0.464844] | 0.582031 [0.507812, 0.652344] | +0.187500 [0.117188, 0.261719] |
| Bidirectional margin | 0.683594 [0.617188, 0.746094] | 0.816406 [0.761719, 0.871094] | +0.132812 [0.082031, 0.183594] |

The improvement is not merely more frequent flipping: joint correctness and
case-complete accuracy also rose, and every primary VitaminC paired interval
above excludes zero in the favorable direction.

### 5.2 Original M3 public test

These paired intervals resample complete M3 claim groups. All 358 rows are
truncated under the frozen 256-token inference contract, as anticipated by the
preregistration. There is no REFUTE class in this public test, so it cannot
measure REFUTE retention.

| Metric | V0 point [95% CI] | V2 primary point [95% CI] | Paired delta [95% CI] |
|---|---:|---:|---:|
| Accuracy | 0.617318 [0.567039, 0.667598] | 0.712291 [0.667598, 0.756983] | +0.094972 [0.053073, 0.139665] |
| Macro-F1 | 0.529838 [0.478019, 0.578666] | 0.524446 [0.472328, 0.573810] | -0.005392 [-0.057113, 0.043549] |
| NLL | 0.704987 [0.651603, 0.758887] | 0.678312 [0.608256, 0.749059] | -0.026675 [-0.064746, 0.010708] |
| Multiclass Brier | 0.482933 [0.440949, 0.525450] | 0.437026 [0.382792, 0.490105] | -0.045907 [-0.075065, -0.016645] |
| ECE | 0.070893 [0.050771, 0.131756] | 0.088410 [0.066045, 0.144704] | +0.017517 [-0.019803, 0.059650] |

The primary accuracy improved, and the point macro-F1 loss was small. The
promotion rule was deliberately stricter: the macro-F1 delta's lower bound had
to remain at or above -0.05. It was -0.057113, so G8 failed.

### 5.3 Three-seed replication surface

V0 is common across candidate seeds. The last row reports arithmetic mean and
sample standard deviation (`n-1`); three seeds are not treated as grounds for
a confidence interval over training randomness.

| Model | VitaminC accuracy | VitaminC macro-F1 | Case complete | Joint correct | Flip detected | Bidirectional margin | M3 accuracy | M3 macro-F1 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| V0 | 0.546875 | 0.508918 | 0.117188 | 0.250000 | 0.394531 | 0.683594 | 0.617318 | 0.529838 |
| V2 / 20260720 | 0.636719 | 0.623702 | 0.250000 | 0.382812 | 0.582031 | 0.816406 | 0.712291 | 0.524446 |
| V2 / 20260721 | 0.644531 | 0.631186 | 0.242188 | 0.378906 | 0.558594 | 0.824219 | 0.712291 | 0.529878 |
| V2 / 20260722 | 0.640625 | 0.627464 | 0.257812 | 0.371094 | 0.570312 | 0.789062 | 0.701117 | 0.517088 |
| V2 mean ± SD | 0.640625 ± 0.003906 | 0.627451 ± 0.003742 | 0.250000 ± 0.007812 | 0.377604 ± 0.005967 | 0.570312 ± 0.011719 | 0.809896 ± 0.018460 | 0.708566 ± 0.006451 | 0.523804 ± 0.006419 |

All three seeds passed the VitaminC point floors. Replication spread was small
for accuracy, macro-F1, joint correctness and detected flips; the largest of
these displayed standard deviations was 0.018460 for bidirectional margin.

## 6. Pre-registered G1-G10 verdict

| Gate | Observed primary result | Pass |
|---|---|---:|
| G1 | joint correct 0.382812; all three seeds passed all point floors | yes |
| G2 | flip detected 0.582031 | yes |
| G3 | bidirectional margin 0.816406 | yes |
| G4 | joint delta +0.132812, CI [0.078125, 0.199219] | yes |
| G5 | flip delta CI [0.117188, 0.261719] | yes |
| G6 | margin delta lower bound 0.082031 | yes |
| G7 | M3 macro-F1 0.524446 and accuracy 0.712291 | yes |
| G8 | M3 accuracy delta lower 0.053073; macro-F1 delta lower **-0.057113** | **no** |
| G9 | all three development calibrations accepted and independently replayed | yes |
| G10 | three complete selected seeds and complete provenance/raw outputs | yes |

Machine verdict: **`NO_GO`**. Promotion is false and terminal tuning is false.
The primary macro-F1 interval includes zero but also extends 0.007113 beyond
the frozen -0.05 non-inferiority boundary. Under the preregistration this is a
safety-gate failure, not permission to round the interval, change the bootstrap
or choose another seed. V2 remains an experimental checkpoint and is not the
new GroundLoop default.

## 7. Corrected Git-history transfer diagnostic

The transfer surface contains 14 new-version claim/excerpt pairs over ten
claims from three small Git histories. It has no independently adjudicated
labels and no old-version pairs, so it is neither a quality benchmark nor a
temporal-regression measurement and had no go/no-go effect.

| Diagnostic | V0 | V2 primary |
|---|---:|---:|
| Pair-level operational labels | 4 SUPPORT, 10 NEUTRAL, 0 REFUTE | 2 SUPPORT, 12 NEUTRAL, 0 REFUTE |
| Obsolete concept groups REFUTED | 0/5 | 0/5 |
| Obsolete claims SUPPORT/CONFLICTED | 2 | 0 |
| Stable negative warnings | 0 | 0 |
| Inserted-positive claims SUPPORTED | 0/2 | 0/2 |

V2 removed two obsolete SUPPORT states by making them UNSUPPORTED, but it did
not refute any obsolete concept and did not support either inserted-positive
claim. The only defensible interpretation is that V2 was more conservative on
this tiny transfer surface. There is no evidence here that it detects software
documentation changes correctly.

## 8. Runtime and resource evidence

Training ran serially on the 16-logical-CPU host with eight PyTorch threads and
one inter-op thread. Runtime JSON separates timing telemetry from semantic
identity.

| Run | Training wall time | Peak RSS KiB |
|---|---:|---:|
| V1 / 20260720 | 43m 01s | 3,511,368 |
| V2 / 20260720 | 23m 11s | 3,555,408 |
| V2 / 20260721 | 24m 21s | 3,582,880 |
| V2 / 20260722 | 25m 58s | 3,583,972 |
| V3 / 20260720 | 37m 36s | 3,525,244 |
| V3 / 20260721 | 32m 03s | 3,629,208 |
| V3 / 20260722 | 27m 09s | 3,595,964 |
| A1 / 20260720 | 13m 39s | 3,118,768 |

The eight recorded trainer intervals total 13,618 seconds, about 3h 47m.
Development scoring of all nine models took 1,231 seconds, about 20m 31s. The
terminal artifact reports 306 seconds of model scoring and derivation; the
complete guarded command took 5m 30s externally and peaked at 966,732 KiB.
All ten terminal score calls reported zero failures and zero timeouts.

These are measurements of this CPU execution, not a general throughput or
IVM-complexity result.

## 9. Recovery and execution audit

The first terminal command stopped during preflight because the expected
derived M4.12 `semantic_result.json` was absent from the otherwise frozen
M4.12 artifact directory. The guard stopped before terminal-reserve access and
published no partial terminal bundle. The missing derivative was reconstructed
from the already frozen M4.12 artifacts without rerunning or adapting a model.
It was accepted only after its canonical SHA-256 matched the preregistered
`017fd20fb810652725846e214c884114c5afdc06cb009206e4bdaf899f9ecef7`,
alongside the frozen configuration, prediction-logit and sample-manifest
hashes. The guarded production command then ran and sealed one terminal
bundle. This was prerequisite recovery, not terminal-driven model tuning.

The final validation supervisor also recorded two environment/invocation
retries. The original Codex process had not inherited the user's existing
`docker` supplementary-group membership, so validation was relaunched under
that group and used the Compose PostgreSQL container. One repository-wide
pytest invocation then lacked the required project `PYTHONPATH` and failed
collection before the corrected invocation ran. Neither retry changed source
code, model artifacts, selection, calibration or terminal results.

## 10. Software and database validation

Final validation was run after the terminal bundle sealed:

```text
Focused M4.13 pytest: 109 passed, 1 skipped
Focused Ruff: all checks passed
Focused strict mypy: no issues in 19 source files
Focused compileall: passed

Complete repository pytest: 680 passed, 7 skipped
Repository Ruff: all checks passed
Repository strict mypy: no issues in 110 source files
Repository compileall: passed

PostgreSQL: 16.14 (Debian 16.14-1.pgdg12+1)
pgvector: 0.8.5
Live validator: 0 claim mismatches, 0 answer mismatches,
                0 invalid certificates; current and policy indexes usable
```

The focused skipped test is the explicit real-prepare opt-in path. Skips in
the complete suite retain the repository's existing opt-in/environment
boundaries; they were not converted into passes.

## 11. Audit identities and artifact pointers

| Surface | Identity |
|---|---|
| Clean training/evaluation Git commit | `2bf686d70ba1be5a2b2ad7f3f6e960e338d36373` |
| M4.13 config SHA-256 | `d50c2af5b5461f74e31fc759c3ca5e0aa074556cf0163789a4fe9615c8b7a11d` |
| Dataset manifest SHA-256 | `1a5ed4b7933c79cba5a09487a24c7dcd576e3518e1a27808d5f4afcbc3007e60` |
| Terminal reserve manifest SHA-256 | `3dcfcba0b809e3bcfcf2f9c036c4946f484d706faa61eec8c3fa489fbcb11dc5` |
| Trainer implementation SHA-256 | `dd3a661b98864b8769d3811fa61aba03f5e833f2ae5733f7daf6584ad6f8ef85` |
| Calibrator implementation SHA-256 | `95f2700d7b76ac56a1f0a1890fb77b20d1a2bf36ed4df5ec74b263154d1e2cfc` |
| Evaluator implementation SHA-256 | `5415fc73159cd940b3fe9776b86ae39e56842d6a1a571fb7d550d0820702f988` |
| Sealed selection SHA-256 | `904c31fdf343539acbed8fcb7f594acd5a6b90f71e7bfef583179cb9c4ee7583` |
| Terminal semantic result SHA-256 | `196b1eab46eb93103c72fa6d0d73d016d5b36545ac2673e0cc4da41e6e92cb9c` |
| Terminal result-manifest file SHA-256 | `4220d8d13c9bc3d4c284f12a011a0255b8db79a7b8336d28c178ed5c1b693762` |

Primary external files:

- `development_bundle/selection.json`: frozen development selection;
- `development_bundle/development_report.json`: all nine development models;
- `runs/V2-ce-mix/<seed>/calibration.json`: the three accepted calibration
  artifacts;
- `terminal_bundle/terminal/metrics.json`: full metrics, confusion matrices,
  calibration ablations and Git decisions;
- `terminal_bundle/terminal/bootstrap.json`: all primary and sensitivity
  intervals plus the complete three-seed scalar surface;
- `terminal_bundle/terminal/semantic_result.json`: runtime-independent
  scientific identity;
- `terminal_bundle/terminal/result_manifest.json`: transitive provenance and
  file hash chain; and
- `terminal_bundle/runtime/timings.json`: isolated runtime telemetry.

Raw licensed text, model weights and generated artifacts remain outside Git.

## 12. Scientific conclusion and non-claims

M4.13 established three bounded facts:

1. Mixed VitaminC/M3 CE continuation produced a repeatable improvement in
   revision response over V0 on this terminal diagnostic.
2. The frozen paired-margin addition did not provide the pre-registered
   development gain over ordinary CE.
3. The selected model did not clear the complete promotion contract because
   M3 macro-F1 non-inferiority uncertainty narrowly exceeded the safety
   margin.

It did **not** establish a novel learning method, state-of-the-art fact
verification, representative VitaminC performance, general transfer to Git or
other RAG domains, or permission to replace GroundLoop's default verifier. It
also does not alter the database theorem: GroundLoop's IVM remains exact only
relative to the stored versioned neural observations. Model correctness
remains empirical.

Any further model work must be a new milestone with a newly locked evaluation
surface. M4.13's terminal reserve may be replayed only for exact artifact
validation; it is consumed for model development.
