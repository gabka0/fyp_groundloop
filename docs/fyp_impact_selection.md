# FYP Task 5A Impact-Selection Diagnostic

## Question

Can GroundLoop avoid most claim--revision verifier pairs while still selecting
the claims whose grounding label changes?

The older frozen M4 policies did not answer this positively: reduced-work
policies lost effects, and the full-recall policy performed exhaustive pair
work. This command evaluates a new text-only selector on a larger bounded
public-data diagnostic.

This is the Task 5A selector checkpoint. It does not replace the remaining
Task 5 miss analysis, integrated effect Pareto, timing, cost, or hosted-model
evaluation.

## Input and split

The command accepts the exact M4.13 prepared VitaminC development JSONL with
SHA-256
`1a306620c363d52c6abfcdf5f6d272e7767cfdc8cbd364768b27901870a3c882`.
It fails closed for any other bytes or invalid four-row case topology.

The 256 cases are split deterministically inside each label stratum:

- 128 development cases select the policy and budget;
- 128 held-out cases evaluate the one selected candidate;
- each partition contains 256 distinct claims;
- exhaustive evaluation therefore represents `128 * 256 = 32,768`
  claim--revision pairs.

The source artifact was already used by M4.13. “Held-out” means held out from
this selector's policy choice, not a never-before-used external dataset.

## Selector

The selected policy, `old_new_rarity_coverage`, scores a claim against both
the old and new revision text. Claim tokens that occur in fewer registry
claims receive greater integer weight. The score is the greater of old/new
weighted token coverage, followed by the lesser coverage. All comparisons use
exact rational arithmetic and claim hashes break remaining ties.

Only claim text and old/new evidence text enter ranking. Page identifiers,
case identifiers, strata, gold labels, and known affected identities do not.

## Run

From an editable checkout:

```bash
python3 -m groundloop.cli fyp-impact-selection \
  --config configs/fyp/impact_selection_v1.json \
  --source /absolute/path/to/development_vitaminc.jsonl \
  --output-dir /tmp/groundloop-fyp-impact-selection
```

The command writes:

- `impact_selection_report.json`;
- `development_frontier.csv`; and
- `evaluation_events.csv`.

## Recorded result

The frozen development rule selected `old_new_rarity_coverage` with a budget
of 8 candidates per event. On the held-out half it recorded:

| Measure | Exact result |
| --- | ---: |
| Affected-claim recall | 249/256 (97.3%) |
| Events recovering both affected claims | 124/128 (96.9%) |
| Selected pairs | 1,024 |
| Exhaustive pairs | 32,768 |
| Avoided pairs | 31,744/32,768 (96.9%) |
| Verdict | `PASS` |

The canonical report manifest hash is
`dac99fd0ffff4749b6defe6ad3975e593f4615d6a291050ce0920da875bea9fa`.

## Interpretation

This is positive evidence for the core selection mechanism: on this bounded
registry, GroundLoop can route only 8 of 256 candidate claims per revision and
retain 249 of 256 known affected claims.

It is not yet evidence of wall-clock speedup. The diagnostic does not call a
verifier, so it does not measure API latency, batching, rate limits, monetary
cost, model accuracy, answer correctness, or database overhead. The registry
is also constructed from two revision-sensitive claims per source page rather
than sampled from a natural deployed workload. The next experiment must run a
provider-neutral hosted verifier over the selected and exhaustive pairs and
keep selection recall separate from verifier accuracy.
