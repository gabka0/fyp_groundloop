# Structured Baseline Metrics Schema v1

Schema identifier: `groundloop.baselines.metrics.v1`

Each JSONL line is one paired `(scenario, trial, event, engine, timing_scope)`
measurement. The code definition is `groundloop.baselines.models.MetricsRecord`.

Required fields:

- `run_id`, `scenario`, `seed`, `trial`, and `event_index` identify provenance.
- `workload` freezes `E` active current observations, `C` claims, `A` answers,
  maximum per-chunk observation fanout `k`, threshold-decision flip count `f`,
  duplicate-content ratio, skew exponent, and update locality.
- `engine`, `engine_class`, and `semantic_equivalent` prevent heuristic action
  policies from entering exact-state comparisons.
- `event_type` names the structured event.
- `wall_time_ns` and `timing_scope` record latency.
- `touched_objects` records claim, answer, and observation work.
- `candidate_count` records candidates examined by the engine. Its physical
  meaning is engine-specific: all scan candidates for full recomputation,
  affected-key discovery candidates for keyed recomputation, and score-index
  candidates for the treatment's policy path.
- `status_changes` counts exact externally visible claim and answer status
  transitions. Heuristic action policies do not produce GroundLoop statuses,
  so this field is zero for them.
- `maintained_bytes` is a recursive, deduplicated `sys.getsizeof` estimate of
  retained Python state. It is useful for within-runtime comparison, not a
  serialized or database storage claim.
- `oracle_included` and `copy_staging_included` state whether the timed region
  includes M1 event-oracle work and repository deep-copy staging.
- `semantic_disagreement_count`, `false_invalidation_count`, and
  `stale_state_exposure_count` compare heuristic invalidation action sets with
  exact status-change sets. Exact engines are equality-asserted and record
  zero in these fields.

Timing scopes:

- `kernel_only`: receives already constructed before/after snapshots; excludes
  event application, M1 oracle recomputation, and repository copy staging.
- `with_oracle_staging`: includes repository copies plus `apply_event`, which
  invokes the M1 oracle. It must not be presented as incremental-kernel
  throughput.

Semantic comparison classes:

- Exact: `global_full_recompute`, `keyed_affected_recompute`, and
  `signed_delta_treatment`. The runner compares complete claim and answer
  states after every event and fails on disagreement.
- Non-equivalent heuristic policies: `source_level_invalidation` and
  `direct_citation_invalidation`. They emit conservative refresh/invalidation
  action sets, not canonical grounding states. Their latency is reportable,
  but `speedup_vs_global_full` is always `n/a`.
