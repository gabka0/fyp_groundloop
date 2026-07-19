# M4 Oracles/Evaluation Lane Status

Date: 2026-07-19

Branch: `workstream/m4-oracles-evaluation`

Contract baseline:

- commit `a1059ff63883fa388def0e39986fa4b8393ef709`
- tag `m4-contract-baseline-2026-07-19`

## Verdict

Wave 1 deterministic oracle work is complete and ready for coordinator review.
No shared contract defect was found after the two accepted contract fixes in
the baseline above. No lane-owned contract request remains open.

## Implemented

- Exact, canonical Cartesian enumeration of registered claims by inserted
  active chunks.
- Hard failure for missing, duplicate, reordered, or identity-changing
  verifier output.
- Immutable deterministic score-table adapter for fixtures.
- Judgment identities and result manifests that bind pair identity, source,
  execution/decision identity, input hash, split, label, and exact float scores.
- Independent direct-witness grounding recomputation over model judgments,
  including distinct normalized-content witness counts and full observation
  provenance.
- Explicit exhaustive additive `Bw -> Bx` recomputation that retains surviving
  working-snapshot judgments, adds every audited inserted pair, and computes
  affected sets.
- Separate materialized-state, decision-summary, claim-status, and
  answer-status affected sets, with only the frozen same-baseline containment.
- Exact brute-force cosine top-k retrieval per claim with deterministic
  `(distance, chunk_version_id)` ties.
- From-scratch `SnapshotRefresh_k` state recomputation.
- Mandatory selective-miss fixture: a neutral decoy is selected while an
  omitted support changes one required claim and answer. The exhaustive path
  detects the miss while selective admission is monkeypatched to raise.
- AST import-boundary test rejecting selective admission, runtime delta,
  pipeline, M1 incremental logic, and admitted-pair DTO imports.
- Small-domain Cartesian checks including empty claim/chunk domains, refresh
  candidate-churn separation, duplicate-content counting, and manifest
  provenance sensitivity.

## Validation

All commands ran from the lane worktree:

```text
.venv/bin/pytest -q tests/m4/oracles tests/m4/test_m4_contracts.py
22 passed

.venv/bin/ruff check src/groundloop/m4/oracles tests/m4/oracles
All checks passed!

.venv/bin/mypy --strict src/groundloop/m4/oracles
Success: no issues found in 8 source files

.venv/bin/python -m compileall -q src/groundloop/m4/oracles tests/m4/oracles
exit 0

git diff --check
exit 0
```

## Scope Boundary

This wave contains pure deterministic primitives and fixtures only. It does
not contain PostgreSQL persistence, a real embedding/verifier adapter, ANN,
batch checkpoint/replay, event-history statistics, full workload runners, or
coordinator pipeline/CLI integration. `compute_exhaustive_additive_delta`
starts from an already independently withdrawn `Bw`; exact structural
withdrawal remains the runtime lane's responsibility and must be supplied as
raw surviving judgments and working states during integration.

No model was downloaded and no network or database service was used.
