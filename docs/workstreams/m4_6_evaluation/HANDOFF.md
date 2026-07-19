# M4.6 Controlled Evaluation Handoff

## Owned paths

This lane changed only:

- `src/groundloop/m4/experiments/**`
- `tests/m4/experiments/**`
- `configs/m4/evaluation/**`
- `docs/workstreams/m4_6_evaluation/**`

It did not edit shared M4 contracts, oracles, models, SQL, migrations,
persistence, pipeline, CLI or milestone documents.

## Public API

Exports from `groundloop.m4.experiments`:

- `load_controlled_evaluation_config(...)`
- `build_frozen_controlled_fixture_v1()`
- `run_controlled_evaluation(...)`
- `ControlledEvaluationReport`
- `write_report_bundle(...)`
- immutable policy, event-work, miss and result records.

The runner consumes the established oracle contracts for full-pair audit,
metric derivation, policy-run alignment, summaries, grounding recomputation and
paired history bootstrap. It does not redefine an affected-claim metric.

## Policy semantics

The approximate budget applies per inserted chunk. Vector/lexical union is
deterministic VECTOR-then-LEXICAL round-robin with pair deduplication. Lineage
is a mandatory safety override and therefore deliberately has no numeric cap;
the exact excess beyond approximate selection is recorded. Frontier additions
have their own explicit per-inserted-chunk cap. The report separates all three
components from actual judged-pair work.

This controlled frontier channel is an ablation input on inserted-event pairs.
It is not a substitute for, or a test of, the live runtime's delete-time
frontier refill and sealing behavior.

## Coordinator integration

The production/real-history runner can replace the frozen candidate and judge
adapters while retaining:

1. `EvaluationPolicy` and its manifest;
2. `PolicyEventMeasurement` integer counts and observed work;
3. `OracleEventResult` from the exact full-pair audit;
4. `derive_event_metric_record(...)` for the four frozen recalls;
5. `paired_history_cluster_bootstrap(...)` with independent history units;
6. canonical JSON/CSV raw rows and deliberate-miss diagnostics.

Do not carry the controlled candidate rankings or table judgments into a real
evaluation. Do not compare policies without identical event/oracle/model/split
provenance. Real latency and token measurements remain coordinator work because
this lane intentionally makes no SQL or model calls.

## Validation commands

```bash
.venv/bin/pytest -q tests/m4/experiments
.venv/bin/ruff check src/groundloop/m4/experiments tests/m4/experiments
.venv/bin/mypy --strict src/groundloop/m4/experiments
.venv/bin/python -m compileall -q \
  src/groundloop/m4/experiments tests/m4/experiments
```

The repository requested `docs/agent_custom_instructions.md`, but that file is
absent in this worktree. `AGENTS.md` and
`docs/agent_expert_operating_principles.md` were followed.
