# FYP Impact-Selection Pareto Checkpoint

Task 5B explains the frozen Task 4 misses, sweeps the complete Task 5A
held-out budget frontier, and freezes the exact hosted-verifier request
population. It performs no paid inference.

## Reproduction

```bash
groundloop fyp-impact-pareto \
  --source /absolute/path/to/development_vitaminc.jsonl \
  --output-dir /tmp/groundloop-fyp-impact-pareto
```

The source must be the prepared M4.13 VitaminC development artifact with
SHA-256
`1a306620c363d52c6abfcdf5f6d272e7767cfdc8cbd364768b27901870a3c882`.
The command writes the canonical report and CSV files plus request mappings,
the selected-new subset, and four upload-ready Batch JSONL parts. These files
contain no API key. The large generated request artifacts are deliberately not
committed.

## Task 4 error analysis

All eight oracle-positive misses are accounted for:

- four channel-only misses place the positive pair outside the one-pair
  vector or lexical budget;
- two vector-first union misses spend the shared one-pair cap on the vector
  decoy before the lexical result can enter; and
- two union-plus-lineage misses are not reached because lineage points to the
  other claim.

The frontier policy recovers those first-version positives only by performing
the same eight total pair attempts as exhaustive re-verification. This confirms
the Task 4 `NO_GO`; it is not evidence against selective maintenance in every
population.

## Retrospective held-out frontier

The complete frontier is explicitly
`retrospective_after_task5a_selection`: budget 8 had already been selected
before this sweep. The strongest policy is `old_new_rarity_coverage`.

| Budget | Recovered affected claims | Fully covered events | Selected pairs | Pair reduction |
|---:|---:|---:|---:|---:|
| 1 | 120/256 | 0/128 | 128/32,768 | 99.6% |
| 2 | 237/256 | 114/128 | 256/32,768 | 99.2% |
| 4 | 244/256 | 120/128 | 512/32,768 | 98.4% |
| 8 | 249/256 | 124/128 | 1,024/32,768 | 96.9% |
| 16 | 253/256 | 126/128 | 2,048/32,768 | 93.8% |
| 32 | 254/256 | 127/128 | 4,096/32,768 | 87.5% |
| 64 | 256/256 | 128/128 | 8,192/32,768 | 75.0% |
| 256 | 256/256 | 128/128 | 32,768/32,768 | 0.0% |

Budget 64 is the smallest measured point with full affected-claim and
full-event coverage on this constructed held-out partition. Budget 8 remains
the preregistered Task 5A point, with 97.3% affected-claim recall at a 96.9%
pair-work reduction.

## Hosted request freeze

The frozen evaluation population contains 65,536 requests: 32,768 old-side
and 32,768 new-side judgments. The selected Task 5A path contains 1,024 new
requests. The model is `gpt-6-luna`, reasoning is disabled, and the response is
a strict one-field `support|refute|neutral` JSON object. Gold labels, page
identity, stratum, and expected affected identities are excluded from request
construction.

- Estimated input tokens: 14,650,463
- Maximum possible output tokens: 2,097,152
- Standard-price upper estimate: USD 2.513623
- Batch-price upper estimate: USD 1.256812
- Frozen Task 5C cumulative spend cap: USD 4.00
- Request manifest hash:
  `ad962ba1ec4336fd24f1d255f511cc93f5baa4a157d85d3c0f16e31a41d49bc2`
- Task 5B report hash:
  `ef826f2a4872f43828d639f30d68a354d206a22720f72b6b431bf33ced142f6a`

## Claim boundary

This checkpoint demonstrates a reproducible selector work/coverage trade-off
and a bounded cost plan. It does not yet demonstrate hosted-model accuracy,
actual latency or cost, claim/status/answer-effect retention, end-to-end
speedup, or representative natural-history utility. Those claims require the
staged Task 5C execution frozen in
`docs/workstreams/fyp_impact_pareto/TASK5B_PLAN.md`.
