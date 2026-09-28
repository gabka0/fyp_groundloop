# Task 4 FYP Value Benchmark Handoff

Date: 2026-09-28
Branch: `codex/fyp-value-benchmark`
Worktree: `/home/kassym/Desktop/groundloop-worktrees/fyp-value-benchmark`

## Status

- Task 4 implementation gate: **PASS**
- Current controlled scientific value gate: **NO_GO**
- M4/M5 milestone status: unchanged
- Real-model accuracy or representative utility claim: not made

The implementation is complete because it faithfully executes and reports the
preregistered fail-closed comparison. The scientific verdict is negative
because none of the five frozen policies simultaneously reduces pair work and
preserves every measured effect.

## Implemented surface

- `groundloop fyp-value-benchmark` command;
- a typed derived value report bound to the frozen controlled report, config,
  fixture, and policy manifests;
- exact aggregation of measured batch calls, pair attempts, completions, and
  failures;
- exact integer numerator/denominator preservation for all four frozen recall
  metrics;
- a strict per-policy and overall `GO`/`NO_GO` gate;
- canonical JSON and compact policy CSV;
- the complete underlying controlled JSON/CSV report bundle;
- human-readable terminal output with the negative-result boundary; and
- fail-closed tests for inconsistent qualification and source-report binding.

No frozen M4 fixture, policy, oracle, metric, or report implementation was
changed.

## Reproduction

```bash
groundloop fyp-value-benchmark \
  --config configs/m4/evaluation/controlled_v1.json \
  --output-dir artifacts/fyp-value-benchmark
```

The command requires neither PostgreSQL nor a model download. A successful
process exit means the measurement completed; it does not convert `NO_GO` into
`GO`.

## Frozen result

The source fixture has two histories, six events, and eight exhaustive
verifier-pair attempts in four non-empty batches.

| Policy ID | Attempts | Pair recall | Claim recall | Status recall | Answer recall | Reduced work | Full effects | Qualifies |
|---|---:|---:|---:|---:|---:|---|---|---|
| `lexical-only-l1` | 4/8 | 2/4 | 2/4 | 4/6 | 4/6 | Yes | No | No |
| `union-l1` | 4/8 | 2/4 | 2/4 | 4/6 | 4/6 | Yes | No | No |
| `union-lineage-frontier-l1-f1` | 8/8 | 4/4 | 4/4 | 6/6 | 6/6 | No | Yes | No |
| `union-lineage-l1` | 4/8 | 2/4 | 2/4 | 4/6 | 4/6 | Yes | No | No |
| `vector-only-l1` | 4/8 | 2/4 | 2/4 | 4/6 | 4/6 | Yes | No | No |

Verdict: `NO_GO`.

- Value report manifest:
  `b13d03d161d9dc984ac3bb734015de9acb4328b67ad776bd5286284d236baf26`
- Bound controlled source report:
  `b30cb1cfa94ed1ad6e72bfa91926ba62312f7ea76f3527c980cde7ad62d73f17`

## Verification record

- Task 4 focused suite: `7 passed` in 17.49s.
- Complete repository run without database URLs, using importlib collection,
  the required test-harness paths, and the process-only Dynagox remote overlay:
  `1718 passed, 1573 skipped` in 226.26s.
- Ruff lint over the repository: pass.
- Ruff formatting check over the touched Python paths: pass (`5 files already
  formatted`).
- Strict mypy: pass over 151 source files.
- Compileall over `src`, `tests`, `scripts`, `experiments`, and `training`:
  pass.
- `git diff --check`: pass.
- CLI reproduction: pass with the exact hashes and `NO_GO` verdict above.

The full suite's skipped tests are live PostgreSQL, live-model, external-data,
or explicit scale/opt-in gates. Task 4 itself is deterministic and had no
skips.

## Evidence boundary

This result is a small deterministic-fixture measurement. It does not measure
embeddings, answer generation, real neural latency, provider cost, real-model
accuracy, false changes on unaffected objects, representative workloads, or
end-to-end user utility. It also does not weaken the separately established
conditional exactness of relational maintenance over stored observations.

The correct conclusion is narrower: the current controlled evidence does not
demonstrate that GroundLoop saves semantic verification work without losing
measured change coverage.

## Next research gate

Design and preregister a stronger impact-selection experiment over larger,
independently adjudicated natural histories. Keep exhaustive re-verification
as the oracle and retain the same joint success condition: measured work must
fall while effect coverage remains acceptable. Do not promote a policy from
work reduction alone.
