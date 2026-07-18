# GroundLoop M3 Verifier Calibration Report

## Fit protocol

Scalar temperature was fitted once on all 987 development examples from 649
claim groups. Train, public test, and transfer examples are rejected by the
calibration API's leakage guard. The deterministic log-temperature golden
search produced:

- Method: `scalar-temperature-golden-v1`
- Calibration version:
  `temperature-v1:6ae200db8d75477da143bd6d8d6c8927cfdfdbd8995932bbc1c59ce67e090727`
- Temperature: `1.1037657679769346`
- Development NLL before: `0.5039963095039312`
- Development NLL after: `0.5016337471652763`
- Development truncation: 608/987 pairs at 256 tokens
- Wall time: 138.06 seconds (external time: 2m19.25s)
- Peak RSS: 969,304 KiB

The calibration version is content-derived from the method, temperature, fit
split, example count, and before/after NLL. That identity and temperature are
stored on every calibrated `VerificationResult`.

## Evaluation metrics and confidence intervals

ECE uses 10 fixed equal-width confidence bins `[0.0,0.1), ...,
[0.9,1.0]`. Brier is the mean sum of squared error across three classes.
Intervals are 95% percentile bootstrap intervals with 1,000 seeded resamples.
Public-test macro-F1 averages only SUPPORT and NEUTRAL because REFUTE support is
zero; REFUTE recall/F1 are null/not estimable.

| Fixture/model | Accuracy (95% CI) | Macro-F1 (95% CI) | Brier (95% CI) | ECE (95% CI) |
|---|---|---|---|---|
| Public zero-shot | 0.6006 [0.5503, 0.6508] | 0.4001 [0.3690, 0.4329] | 0.6609 [0.5758, 0.7390] | 0.2597 [0.2126, 0.3082] |
| Public fine-tuned, uncalibrated | 0.6173 [0.5670, 0.6676] | 0.5298 [0.4771, 0.5839] | 0.4888 [0.4437, 0.5337] | 0.0970 [0.0666, 0.1470] |
| Public fine-tuned, calibrated | 0.6173 [0.5670, 0.6648] | 0.5298 [0.4777, 0.5833] | 0.4829 [0.4412, 0.5261] | 0.0709 [0.0493, 0.1343] |
| Transfer zero-shot | 0.6111 [0.3889, 0.8333] | 0.6083 [0.3444, 0.8222] | 0.5884 [0.2813, 0.9764] | 0.2422 [0.1325, 0.4829] |
| Transfer fine-tuned, uncalibrated | 0.6667 [0.4444, 0.8889] | 0.6646 [0.4105, 0.8808] | 0.4739 [0.1988, 0.7949] | 0.2698 [0.1177, 0.4553] |
| Transfer fine-tuned, calibrated | 0.6667 [0.4444, 0.8889] | 0.6646 [0.4040, 0.8851] | 0.4533 [0.1917, 0.7428] | 0.2834 [0.1306, 0.4677] |

## Per-class results

Public test class counts are SUPPORT 111, REFUTE 0, NEUTRAL 247.

| Model/class | Precision | Recall | F1 |
|---|---:|---:|---:|
| Zero-shot SUPPORT | 1.0000 | 0.0180 | 0.0354 |
| Zero-shot REFUTE | 0.0000 | undefined | undefined |
| Zero-shot NEUTRAL | 0.6871 | 0.8623 | 0.7648 |
| Fine-tuned calibrated SUPPORT | 0.3626 | 0.2973 | 0.3267 |
| Fine-tuned calibrated REFUTE | 0.0000 | undefined | undefined |
| Fine-tuned calibrated NEUTRAL | 0.7068 | 0.7611 | 0.7329 |

Public confusion matrices use rows=true and columns=predicted in
`(support, refute, neutral)` order:

```text
zero-shot:              [[2, 12, 97], [0, 0, 0], [0, 34, 213]]
fine-tuned calibrated: [[33, 0, 78], [0, 0, 0], [58, 1, 188]]
```

The transfer fixture is balanced at six examples per class.

| Model/class | Precision | Recall | F1 |
|---|---:|---:|---:|
| Zero-shot SUPPORT | 0.7500 | 0.5000 | 0.6000 |
| Zero-shot REFUTE | 0.5000 | 0.8333 | 0.6250 |
| Zero-shot NEUTRAL | 0.7500 | 0.5000 | 0.6000 |
| Fine-tuned calibrated SUPPORT | 0.8000 | 0.6667 | 0.7273 |
| Fine-tuned calibrated REFUTE | 0.5556 | 0.8333 | 0.6667 |
| Fine-tuned calibrated NEUTRAL | 0.7500 | 0.5000 | 0.6000 |

Transfer confusion matrices:

```text
zero-shot:             [[3, 2, 1], [1, 5, 0], [0, 3, 3]]
fine-tuned calibrated: [[4, 1, 1], [1, 5, 0], [0, 3, 3]]
```

## Reliability rows

Rows list `bin: count, mean confidence, accuracy`; omitted bins have count 0.

Public test:

| Model | Nonempty reliability rows |
|---|---|
| Zero-shot | 0.4-0.5: 2, .4836, .0000; 0.5-0.6: 29, .5540, .3793; 0.6-0.7: 22, .6533, .4091; 0.7-0.8: 43, .7505, .5116; 0.8-0.9: 61, .8553, .5246; 0.9-1.0: 201, .9559, .7015 |
| Fine-tuned uncalibrated | 0.4-0.5: 6, .4910, .6667; 0.5-0.6: 94, .5500, .5106; 0.6-0.7: 87, .6463, .5517; 0.7-0.8: 84, .7530, .7738; 0.8-0.9: 60, .8519, .6167; 0.9-1.0: 27, .9218, .7037 |
| Fine-tuned calibrated | 0.4-0.5: 12, .4923, .5833; 0.5-0.6: 98, .5499, .5510; 0.6-0.7: 96, .6459, .5833; 0.7-0.8: 76, .7481, .7105; 0.8-0.9: 66, .8496, .6364; 0.9-1.0: 10, .9249, .8000 |

Software/API transfer:

| Model | Nonempty reliability rows |
|---|---|
| Zero-shot | 0.3-0.4: 1, .3908, .0000; 0.5-0.6: 1, .5344, .0000; 0.6-0.7: 3, .6524, .6667; 0.7-0.8: 2, .7517, .5000; 0.8-0.9: 1, .8787, 1.0000; 0.9-1.0: 10, .9767, .7000 |
| Fine-tuned uncalibrated | 0.5-0.6: 1, .5015, 1.0000; 0.6-0.7: 2, .6910, .0000; 0.7-0.8: 1, .7690, .0000; 0.8-0.9: 3, .8583, .3333; 0.9-1.0: 11, .9666, .9091 |
| Fine-tuned calibrated | 0.4-0.5: 1, .4902, 1.0000; 0.6-0.7: 2, .6655, .0000; 0.7-0.8: 1, .7449, .0000; 0.8-0.9: 5, .8495, .4000; 0.9-1.0: 9, .9702, 1.0000 |

## Interpretation

On the public test, temperature scaling improves Brier from 0.4888 to 0.4829
and ECE from 0.0970 to 0.0709 without changing predictions. This is evidence
for in-distribution calibration improvement under the stated truncation and
missing-class limitations. On the 18-example transfer fixture, Brier improves
but ECE worsens from 0.2698 to 0.2834 and intervals are wide. No across-domain
calibration-improvement claim is supported.
