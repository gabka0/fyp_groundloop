# M4 Oracles/Evaluation Lane Status

Date: 2026-07-19

Branch: `workstream/m4-oracles-evaluation`

Current integration baseline:

```text
0b28df6 Merge M4 oracle evaluation lane
```

## Verdict

The independent Wave 2 evaluation-mechanics increment is complete and ready
for coordinator review. Wave 1 is already present on `main`; this increment
touches only lane-owned oracle source, tests, and workstream documentation.

## Wave 2 Mechanics Implemented

- Immutable event-level raw records containing all four frozen integer
  numerator/denominator metrics.
- Metric units derived from metric identity, preventing pair, claim,
  status-claim, and answer counts from being silently mixed.
- Explicit `None`/N/A for a zero denominator. Such events and failure-coded
  events remain in total raw-event counts and are never assigned fake zero or
  one values.
- Micro pooled ratios, eligible-event counts, total-event counts, and macro
  per-event ratios regenerated from validated raw rows.
- Complete provenance for dataset, split and split manifest, seed manifest,
  treatment policy, verifier, decision policy, oracle kind, named baseline,
  and oracle policy. The provenance has a content-derived manifest hash.
- Homogeneous-run validation rejecting mixed run, policy, split, seed,
  verifier, decision, oracle, or baseline identities, duplicate event IDs,
  and duplicate event positions within one history.
- Paired-policy alignment requiring distinct policy hashes and exact agreement
  on event IDs, history/index/type, before/after corpus snapshots, baseline
  manifest, common experimental provenance, and per-event denominators.
- Deterministic paired history-cluster bootstrap using SHA-256-derived draws,
  not process-global randomness or event-level resampling.
- A frozen bootstrap configuration binding seed `20260719`, 10,000
  replicates, 95% percentile interval, minimum two eligible histories, and the
  macro-history pooled-ratio-difference estimand.
- Descriptive-only output for no eligible history or a single eligible
  history; no unsupported confidence interval is fabricated.
- Controlled proof that duplicating events within one history neither
  increases independent cluster count nor changes the paired macro-history
  bootstrap result when the duplicated observations are identical.

## Validation

All commands ran after the implementation commit from the lane worktree:

```text
.venv/bin/pytest -q tests/m4/oracles tests/m4/test_m4_contracts.py
30 passed

.venv/bin/ruff check src/groundloop/m4/oracles tests/m4/oracles
All checks passed!

.venv/bin/mypy --strict src/groundloop/m4/oracles
Success: no issues found in 9 source files

.venv/bin/python -m compileall -q src/groundloop/m4/oracles tests/m4/oracles
exit 0

git diff --check
exit 0
```

## Scope Boundary

This increment contains pure deterministic evaluation mechanics. It does not
add model execution, admission or runtime imports, migrations, PostgreSQL
persistence, a top-level pipeline, ANN, workload generation, or neural TARGET
training. Raw append-only storage and bounded model runners remain separate
future integration work.
