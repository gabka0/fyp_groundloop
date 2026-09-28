# Task 4 Plan: FYP Value Benchmark

## Objective

Turn the frozen M4 controlled evaluation into one presentation-facing,
fail-closed answer to the immediate FYP question:

> Does any evaluated GroundLoop selection policy reduce neural verifier-pair
> work while preserving every measured effect found by exhaustive
> re-verification?

This task does not create a new selection algorithm and does not reinterpret
the deterministic fixture as real-model or population evidence.

## Path-exclusive ownership

Task 4 may change only:

- `src/groundloop/fyp_value_benchmark.py`
- the Task 4 command wiring in `src/groundloop/cli.py`
- `tests/m4/fyp_value_benchmark/**`
- `docs/fyp_value_benchmark.md`
- `docs/workstreams/fyp_value_benchmark/**`
- the Task 4 section of `README.md`

The frozen M4 experiment runner, fixture, policies, metrics, and existing
reports are read-only dependencies.

## Execution

1. Run the existing frozen controlled evaluation without changing its bytes.
2. Aggregate actual exhaustive and per-policy verifier work.
3. Preserve the exact integer numerator and denominator for all four frozen
   metrics: positive-pair, positive-claim, status-effect, and answer-effect
   recall.
4. Evaluate a strict usefulness gate for every policy:
   - verifier-pair attempts are strictly below the exhaustive baseline; and
   - all four pooled recall numerators equal their non-zero denominators; and
   - no verifier attempt failed.
5. Emit canonical JSON, a compact policy CSV, the complete source controlled
   report bundle, and a readable console summary.
6. Report `GO` only if at least one policy passes every condition. Otherwise
   report `NO_GO` without treating successful execution as positive evidence.

## Acceptance gates

- The new command is `groundloop fyp-value-benchmark`.
- Repeated runs over the same config produce byte-identical artifacts.
- The checked-in controlled fixture reports 8 exhaustive pair attempts.
- The four low-work policies report 4 attempts and incomplete recall.
- The frontier policy reports full recall but 8 attempts.
- The overall verdict is therefore `NO_GO` with no qualifying policy.
- Unit, CLI, serialization, mutation/fail-closed, lint, type, compile, and
  relevant regression tests pass.

## Claim boundary

The result measures a deterministic table-verifier fixture. It does not
measure embeddings, answer generation, wall-clock model latency, monetary
cost, real-model accuracy, false changes on unaffected objects, representative
workloads, or end-to-end utility. Relational exactness after observations are
stored remains a separate systems result.
