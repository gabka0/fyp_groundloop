# M4.9 selective-maintenance study

This directory contains a reproducible controlled study for the seven frozen
M4.9 ablations. It executes GroundLoop's independent full-pair audit and
`SnapshotRefresh_k` implementations over three held-out synthetic dynamic
histories. It does not load an embedding model or verifier model.

Run:

```bash
PYTHONPATH=src .venv/bin/python \
  experiments/m4_selective_study/run_controlled_study.py \
  --output-directory /tmp/groundloop-m4-9-controlled
```

The output directory contains:

- `study_manifest.json`: frozen split, policy, oracle and treatment inputs;
- `report.json`: event rows, aggregate metrics, uncertainty and work totals;
- `event_metrics.csv`: one metric row per policy and event;
- `summary_metrics.csv`: micro, event-macro and history-cluster estimates;
- `misses.csv`: positive-pair, claim-effect, answer-effect and timeout records;
- `bundle_manifest.json`: SHA-256 digest of every other artifact.

The fixture has six disconnected histories, with three assigned to development
and three to test. Only the test histories are evaluated. Each has insert,
replace, alternative-support insert and refutation-delete events. A single
deterministic timeout is injected to exercise failure accounting. Token and
latency values remain `null`: filling them with invented values would turn a
software acceptance fixture into a false model experiment.

These synthetic results validate mechanics, not semantic quality. The real
M4.9 study must bind each row to a persisted event audit, a pinned real-model
execution, a real `SnapshotRefresh_k` artifact and real timing/token telemetry.
