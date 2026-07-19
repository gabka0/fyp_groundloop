# M4 Oracles/Evaluation Wave 1 Handoff

## Integration Commit

Implementation commit:

```text
d6d98cdc4ab9b0ce6ec7a97acbc124b5adf243e0
```

Parent contract baseline:

```text
a1059ff63883fa388def0e39986fa4b8393ef709
```

The coordinator should cherry-pick the implementation commit and this handoff
commit. This lane did not edit contracts, migrations, runtime, admission,
pipeline, CLI, or M1-M3 code.

## Public Lane API

Exports are collected in `groundloop.m4.oracles`:

- `run_full_pair_audit(...) -> FullPairAuditResult`
- `compute_exhaustive_additive_delta(...) -> ExhaustiveAdditiveDelta`
- `compute_affected_sets(...) -> AffectedSets`
- `pair_positive_claim_ids(...) -> tuple[str, ...]`
- `exact_brute_force_pairs(...) -> tuple[PairKey, ...]`
- `run_snapshot_refresh(...) -> SnapshotRefreshResult`
- `recompute_grounding_states(...)`
- `RefreshClaim`, `RefreshChunk`, `PairJudge`, and
  `DeterministicJudgmentTable`

## Required Coordinator Wiring

### Exhaustive insertion comparator

1. Freeze the registered-claim and inserted-active-chunk IDs for the event.
2. Supply a `PairJudge` backed by the same frozen verifier execution and
   decision policy used by treatment.
3. Call `run_full_pair_audit`. Any missing attempt is a hard failure.
4. Independently obtain `Bw` after exact withdrawal, including surviving
   current judgments and complete claim/answer states.
5. Call `compute_exhaustive_additive_delta` with those `Bw` values and the
   complete active chunk-version-to-normalized-text-hash map.
6. Persist the audit manifest, fresh `Bx` states, and baseline-qualified
   affected sets. Do not relabel pair-positive claims as state-affected claims.

The function rejects an overlap between surviving `Bw` pairs and newly
audited inserted pairs. In the frozen event model, `Bw` precedes observations
for inserted chunk versions; overlap therefore indicates incorrect snapshot
construction or replay contamination.

### Snapshot refresh comparator

1. Build `RefreshClaim` and `RefreshChunk` records from one sealed corpus
   snapshot using normalized, role-specific, L2-normalized vectors.
2. Supply a SHA-256 corpus snapshot hash and a frozen refresh policy ID.
3. Call `run_snapshot_refresh` separately before and after the event.
4. Report candidate-set churn separately from exhaustive admission misses.

The Wave 1 implementation uses exact in-memory brute-force cosine ranking.
It must remain the reference when a later ANN adapter is evaluated.

## Exact Guarantees

- Full-pair attempted keys are exactly the Cartesian set represented by the
  frozen `FullPairAuditResult` contract.
- Input ordering does not affect pair order, manifests, or results.
- Exact top-k ranking is per claim and deterministic under ties.
- Grounding recomputation does not import or call M1/M2 incremental engines,
  M4 selective admission, runtime delta, or pipeline logic.
- Distinct normalized text hashes determine support/refute counts; all
  contributing observation IDs remain in complete state.
- Affected-set projections compare one named pair of complete snapshots and
  enforce only `status subset decision-summary subset materialized-state`.
- Manifests change when judgment score, label, source, input, policy, pair, or
  split provenance changes.

## Non-Guarantees

- The full-pair audit is verifier-relative, not human-semantic completeness.
- Snapshot refresh is policy- and depth-relative; it is not an exhaustive
  semantic oracle.
- Exact model inference reproducibility is not created by these pure helpers;
  the later adapter must pin model artifact, templates, execution settings,
  batching, and decision policy.
- This lane does not itself perform structural withdrawal from `B0` to `Bw`.
- This wave does not establish SQL equivalence, crash recovery, timeout
  accounting, resource feasibility, cluster bootstrap validity, or split
  leakage safety. Those remain Wave 2/integration tasks.

## Coordinator Acceptance Gate

Run:

```bash
.venv/bin/pytest -q tests/m4/oracles tests/m4/test_m4_contracts.py
.venv/bin/ruff check src/groundloop/m4/oracles tests/m4/oracles
.venv/bin/mypy --strict src/groundloop/m4/oracles
.venv/bin/python -m compileall -q src/groundloop/m4/oracles tests/m4/oracles
```

Expected result at handoff: 22 tests pass, Ruff clean, strict mypy clean over
eight oracle source modules, and compileall exits zero.

## Next Lane Work

Do not start real exhaustive model work yet. After coordinator integration,
the next bounded lane stage is Wave 2: persistence/result artifacts, bounded
full-pair and full-refresh runners, checkpoint/replay and timeout accounting,
then a resource pilot. Neural TARGET training remains blocked until frozen
development exhaustive-audit labels exist.
