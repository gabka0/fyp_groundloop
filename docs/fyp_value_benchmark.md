# GroundLoop FYP Value Benchmark

## Purpose

The value benchmark gives one honest, reproducible answer to a narrow question:

> In the frozen controlled workload, does any evaluated selective policy use
> fewer neural verifier-pair attempts than exhaustive re-verification while
> preserving every measured affected pair, claim, claim-status, and
> answer-status effect?

It is a presentation layer over the existing frozen M4 controlled experiment.
It does not modify the workload, policies, oracle, or results.

## Run it

From the repository root:

```bash
groundloop fyp-value-benchmark \
  --config configs/m4/evaluation/controlled_v1.json \
  --output-dir artifacts/fyp-value-benchmark
```

The command needs no database, model download, or API key. It exits successfully
when the experiment and report complete, even when the scientific verdict is
`NO_GO`.

## Decision rule

A policy qualifies only if all of these conditions hold:

1. Its measured verifier-pair attempts are strictly below the exhaustive
   claim-by-inserted-chunk baseline.
2. It has no failed verifier attempts.
3. Its pooled numerator equals its positive denominator for all four frozen
   metrics:
   - positive-pair recall;
   - positive-claim recall;
   - claim-status-effect recall; and
   - answer-status-effect recall.

The overall verdict is `GO` only when at least one policy qualifies. This rule
prevents a reduction in model work from being presented as useful when it also
misses relevant changes.

## Current controlled result

The frozen fixture contains two histories and six events. Exhaustive
re-verification performs eight pair attempts in four non-empty batches.

| Policy kind | Pair attempts | Pair recall | Claim recall | Status-effect recall | Answer-effect recall | Qualifies |
|---|---:|---:|---:|---:|---:|---|
| `lexical_only` | 4/8 | 2/4 | 2/4 | 4/6 | 4/6 | No |
| `vector_lexical_union` | 4/8 | 2/4 | 2/4 | 4/6 | 4/6 | No |
| `union_lineage_frontier` | 8/8 | 4/4 | 4/4 | 6/6 | 6/6 | No |
| `union_lineage` | 4/8 | 2/4 | 2/4 | 4/6 | 4/6 | No |
| `vector_only` | 4/8 | 2/4 | 2/4 | 4/6 | 4/6 | No |

The verdict is `NO_GO`: the low-work policies miss measured effects, and the
only full-recall policy performs the same pair work as exhaustive verification.
This is a useful negative result because it shows that the current controlled
evidence does not yet establish GroundLoop's proposed selective-maintenance
advantage.

For the checked-in config and code, the value report manifest is
`b13d03d161d9dc984ac3bb734015de9acb4328b67ad776bd5286284d236baf26`.
It is bound to controlled source report
`b30cb1cfa94ed1ad6e72bfa91926ba62312f7ea76f3527c980cde7ad62d73f17`.

## Artifacts

The output directory contains:

- `value_benchmark.json`: canonical derived result, rule, verdict, hashes, and
  limitations;
- `value_benchmark_policies.csv`: one compact row per policy;
- `controlled/controlled_evaluation_report.json`: complete source report;
- `controlled/controlled_event_metrics.csv`: event-level metric evidence; and
- `controlled/controlled_bootstrap_intervals.csv`: the frozen paired bootstrap
  results.

Repeated executions with the same code and config produce identical bytes.

## Claim boundary

This benchmark uses a deterministic judgment table. It measures actual
verifier-pair attempts within a tiny controlled fixture; it does not measure
embedding calls, answer generation, wall-clock neural latency, monetary cost,
real-model accuracy, false changes on unaffected objects, representative
workloads, or end-to-end user utility.

GroundLoop's exact relational maintenance after semantic observations have
been stored is a separate systems property. The `NO_GO` verdict concerns the
currently demonstrated trade-off between selective neural work and semantic
effect coverage.

## Research implication

The next value-focused experiment should evaluate a stronger impact-selection
policy on independently adjudicated, naturally changing histories. It must
keep the same exhaustive comparison and report both actual neural work and
effect recall. The success criterion remains simultaneous work reduction and
acceptable retained coverage, not model accuracy or work reduction in
isolation.
