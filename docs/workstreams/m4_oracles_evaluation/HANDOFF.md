# M4 Oracles/Evaluation Wave 2 Mechanics Handoff

## Integration

Baseline already containing Wave 1:

```text
0b28df6
```

Wave 2 implementation commit:

```text
c5f29ee2e2b3f3e63c7743ed58efaa2a26b574c8
```

The coordinator should cherry-pick the implementation commit and the separate
documentation commit containing this handoff. No shared contract, migration,
runtime, admission, CLI, pipeline, or M1-M3 path changed.

## API

The new API is exported from `groundloop.m4.oracles`:

- `MetricName`, `MetricUnit`, `MetricCount`
- `OracleKind`, `EvaluationProvenance`, `EventMetricRecord`
- `PolicyRun`, `validate_policy_run(...)`
- `PolicyMetricSummary`, `summarize_policy_metric(...)`
- `AlignedEventPair`, `PairedPolicyRun`,
  `align_paired_policy_records(...)`
- `BootstrapConfig`, `FROZEN_HISTORY_BOOTSTRAP_V1`
- `PairedBootstrapResult`, `paired_history_cluster_bootstrap(...)`

## Raw Record Contract

Every `EventMetricRecord` contains exactly one integer numerator/denominator
for each frozen metric:

| Metric | Unit |
|---|---|
| `positive_pair_recall` | pair |
| `positive_claim_recall` | claim |
| `status_effect_recall` | status-claim |
| `answer_effect_recall` | answer |

The metric name determines its unit; callers cannot provide a contradictory
unit. Numerators and denominators must be actual nonnegative integers, with
`numerator <= denominator`. A `0/0` record has value `None`, remains in the
raw event population, and is excluded only from that metric's eligible-value
mean.

Every event still carries all four metrics. Delete-only inserted-pair metrics
therefore use `0/0`; they are not omitted. Failure-coded events are also
retained.

## Provenance and Aggregation

`EvaluationProvenance` binds:

- schema and dataset version;
- split ID and split-manifest hash;
- seed-manifest hash;
- treatment-policy ID and hash;
- verifier artifact and execution-spec hash;
- decision-policy ID;
- oracle kind;
- named comparison baseline;
- oracle-policy ID and hash.

`validate_policy_run` fails closed if any of these values differ within a run.
It also rejects mixed run IDs, duplicate event IDs, and duplicate event-index
positions within a history. `summarize_policy_metric` can only aggregate a
validated homogeneous run and returns both total and eligible event counts,
the pooled integer counts, micro ratio, and macro event ratio.

## Paired Comparison

`align_paired_policy_records` accepts two distinct treatment-policy runs. It
rejects:

- missing or extra event IDs;
- duplicate event IDs or history positions;
- mixed split, verifier, decision, seed, oracle, baseline, or dataset identity;
- the same policy under aliases or one policy ID mapped to conflicting hashes;
- mismatched history ID, event index/type, corpus snapshots, or baseline
  manifest for an aligned event;
- different denominators for the same event and metric.

The result is canonically ordered by `(history_id, event_index, event_id)`.
Treatment outcomes and failure codes may differ; those are results, not
pairing identities.

## History-Cluster Bootstrap

The paired bootstrap first pools each metric's integer counts within each
history and computes `first_policy - second_policy`. It then resamples the
same independent history clusters for both policies and computes the macro
mean of within-history differences.

The implementation never resamples events as independent observations.
Deterministic draws are derived from SHA-256 over seed, replicate index, and
draw index. The returned result retains total and eligible cluster IDs, both
policy provenances, the bootstrap-config hash, the descriptive estimate, the
interval, and exact replicate values.

`FROZEN_HISTORY_BOOTSTRAP_V1` fixes:

```text
seed:                  20260719
replicates:            10000
confidence:            0.95
minimum clusters:      2
interval method:       percentile-v1
estimand:              macro-history-pooled-ratio-difference-v1
```

No interval is emitted for zero or one eligible history. This is deliberate:
one sequential history is not independent replication.

## Acceptance Commands

```bash
.venv/bin/pytest -q tests/m4/oracles tests/m4/test_m4_contracts.py
.venv/bin/ruff check src/groundloop/m4/oracles tests/m4/oracles
.venv/bin/mypy --strict src/groundloop/m4/oracles
.venv/bin/python -m compileall -q src/groundloop/m4/oracles tests/m4/oracles
```

Expected at handoff: 30 tests pass, Ruff clean, strict mypy clean across nine
oracle source modules, and compileall exits zero.

## Remaining Work

This handoff does not authorize neural or full-corpus model execution. The
next independent evaluation work should be append-only persistence and bounded
batch/checkpoint runners, followed by the resource pilot. It must reuse these
raw identities and must not silently filter timeouts or unaffordable events.
