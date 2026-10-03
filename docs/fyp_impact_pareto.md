# FYP Impact-Selection Pareto and Hosted-Verifier Result

Task 5B explains the frozen Task 4 misses, sweeps the complete Task 5A
held-out budget frontier, and freezes the exact hosted-verifier request
population. Task 5C executes that population with OpenAI `gpt-6-luna` and
compares selective maintenance with exhaustive re-verification. The final
pre-registered verdict is **`NO_GO`**.

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

## Hosted request freeze and execution

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

The complete Task 5C execution returned a schema-valid response for every
request. A live 2,000,000-token queued-input limit rejected the first
exhaustive-new Batch job before any request executed. The exact same request
identities were therefore repartitioned into six smaller jobs under the
pre-output recovery rule. No prompt, request body, result metric, threshold,
or model changed.

## Task 5C result

The exhaustive old/new observation set found 336 model-relative pair and
claim-status changes and 138 answer-status changes. These denominators are
larger than Task 5A's 256 fixture-authored affected claims: the hosted model
also changed its judgment for some other registry claims. That is the main
reason the earlier deterministic coverage result did not translate into high
effect recall.

| Budget | Pair reduction | Pair/claim effect recall | Status effect recall | Answer effect recall |
|---:|---:|---:|---:|---:|
| 1 | 99.6% | 110/336 (32.7%) | 210/336 (62.5%) | 74/138 (53.6%) |
| 2 | 99.2% | 209/336 (62.2%) | 283/336 (84.2%) | 99/138 (71.7%) |
| 4 | 98.4% | 221/336 (65.8%) | 291/336 (86.6%) | 103/138 (74.6%) |
| 8 | 96.9% | 238/336 (70.8%) | 297/336 (88.4%) | 106/138 (76.8%) |
| 16 | 93.8% | 268/336 (79.8%) | 310/336 (92.3%) | 114/138 (82.6%) |
| 32 | 87.5% | 289/336 (86.0%) | 320/336 (95.2%) | 123/138 (89.1%) |
| 64 | 75.0% | 317/336 (94.3%) | 329/336 (97.9%) | 132/138 (95.7%) |
| 256 | 0.0% | 336/336 (100%) | 336/336 (100%) | 138/138 (100%) |

The frozen gate required at least 80% work reduction and at least 95% recall
for every effect metric. Budget 32 met the work threshold but missed three
recall thresholds. Budget 64 nearly reached the pair/claim threshold, but it
still missed it and reduced work by only 75%. No budget qualified.

On the 512 own-page annotated endpoints, hosted accuracy was 421/512 (82.2%):
205/256 on old evidence and 216/256 on new evidence. This is better than the
weak frozen MiniLM diagnostic, but it is not accurate enough to treat the API
as an objective-truth oracle. The effect gate is deliberately model-relative
and therefore remains valid as a selector diagnostic even when the model is
wrong against the annotations.

Actual Batch usage was:

| Role | Requests | Batch calls | Input/output tokens | Cost (USD) | Provider processing |
|---|---:|---:|---:|---:|---:|
| Selected new, budget 8 | 1,024 | 1 | 232,065 / 18,501 | 0.01622850 | 1,070 s |
| Baseline old | 32,768 | 2 | 6,430,976 / 590,563 | 0.46918955 | 9,105 s |
| Exhaustive new | 32,768 | 6 | 7,418,112 / 590,130 | 0.51843810 | 11,129 s |

The nine Batch calls cost USD 1.00385615; including the one allowed 32-request
standard-endpoint smoke, total Task 5C spend was USD 1.00486185. The observed
selected update took 1,135.406 seconds including selection, provider lifecycle,
and local projection; exhaustive new re-verification took 11,207.775 seconds.
That observed 89.9% difference is **not** a production-speedup claim: the runs
were separate asynchronous Batch jobs, exhaustive work was serially split by
the live queue limit, and OpenAI does not expose pure accelerator inference
time.

The complete local result bundle is retained at
`artifacts/fyp_impact_pareto/task5c/` and excluded from Git because it contains
generated model outputs. The authoritative execution summary and hashes are
in
`docs/workstreams/fyp_impact_pareto/TASK5C_IMPLEMENTATION_HANDOFF.md`.

## Claim boundary

Task 5 is complete as a bounded negative experiment. It demonstrates that the
current selector reduces verifier work, but it does not retain enough
hosted-model effects under the frozen simultaneous gate. It also measures API
usage, cost, and Batch lifecycle time for this run. It does not establish
objective grounding accuracy, online latency, a general end-to-end speedup,
or representative natural-history utility. It does not promote `gpt-6-luna`
or any selector into the GroundLoop runtime, and it changes no M4 or M5 claim.
