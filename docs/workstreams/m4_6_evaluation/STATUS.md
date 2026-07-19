# M4.6 Controlled Evaluation Lane Status

Date: 2026-07-19

Branch: `workstream/m4_6-evaluation`

## Verdict

The controlled, model-free M4.6 evaluation machinery is implemented. It is a
reproducibility and measurement-contract fixture, not an empirical AI-quality
or system-performance result.

## Implemented

- Five frozen treatment policies:
  - vector-only at approximate cap `L=1`;
  - lexical-only at approximate cap `L=1`;
  - vector/lexical rank-interleaved union at approximate cap `L=1`;
  - union plus every mandatory lineage candidate;
  - union plus every mandatory lineage candidate plus frontier cap `F=1`.
- Exact `registered claims x inserted chunks` audits through the existing
  `run_full_pair_audit` oracle contract.
- Existing four integer event metrics: positive-pair, positive-claim,
  claim-status-effect and answer-status-effect recall. Delete-only pair metrics
  remain explicit integer `0/0` and serialize as JSON `null`/empty CSV value.
- Actual controlled-verifier adapter work per event: nonempty batch calls,
  attempted unique pairs, completed pairs and failed pairs. These are observed
  calls in the table-driven runner, not a budget-derived proxy and not neural
  model inference.
- Treatment state recomputation from only the pairs that each treatment
  actually judges, including exact removal of judgments on deactivated chunks.
- Inspectable deliberate-miss rows for every policy/event, including the
  designated probe, whether the exhaustive oracle found it positive, and all
  missed positive pairs.
- Paired percentile bootstrap against the exhaustive treatment using the
  existing history-cluster implementation, fixed seed `20260719`, 10,000
  replicates and the two independent test histories.
- Canonical JSON containing raw records and every bootstrap replicate, plus
  event-metric CSV and compact bootstrap-interval CSV.
- A one-command module entrypoint that writes the complete bundle.

## Reproduction

From the repository root:

```bash
PYTHONPATH=src .venv/bin/python -m groundloop.m4.experiments \
  --output-dir /tmp/groundloop-m4-6-controlled
```

Outputs:

```text
controlled_evaluation_report.json
controlled_event_metrics.csv
controlled_bootstrap_intervals.csv
```

The report embeds config, fixture, workload, split, policy, verifier-table,
oracle, event and bootstrap identities. Reordering input records cannot change
the bytes.

## Scientific boundary

The frozen candidate ranks and pair judgments are deliberately table-driven.
No PostgreSQL query, embedding model, verifier model, M4 coordinator or live
pipeline executes in this lane. Consequently:

- the output tests recall/work accounting and reporting mechanics;
- the deliberately induced misses are test probes, not estimated miss rates;
- the fixed-seed intervals test clustered-bootstrap reproducibility, not
  population uncertainty from a real corpus;
- there is no latency, token, neural quality, verifier-saving or superiority
  claim.

Each selected history is already checked by the existing workload contract to
be independent in split-component, lineage, claim-family, content, claim,
answer and chunk identities. The bootstrap clusters on `history_id`; in this
fixture each history owns exactly one independent split component.

## Provenance identities

The controlled verifier execution hash is derived as:

```text
stable_m4_digest("m4-controlled-table-verifier-v1", "no-model-call")
```

The exhaustive oracle-policy hash is derived as:

```text
stable_m4_digest("m4-controlled-full-pair-policy-v1", "exact-cartesian")
```

They identify controlled executable semantics, not external model artifacts.

## Validation

Executed from this worktree:

```text
.venv/bin/pytest -q tests/m4/experiments
12 passed

.venv/bin/pytest -q
exit 0; 395 tests collected, with environment-specific skips

.venv/bin/ruff check .
All checks passed!

.venv/bin/mypy --strict src
Success: no issues found in 97 source files

.venv/bin/python -m compileall -q src tests scripts experiments training
exit 0

git diff --check
exit 0
```

The final one-command smoke produced report manifest:

```text
b30cb1cfa94ed1ad6e72bfa91926ba62312f7ea76f3527c980cde7ad62d73f17
```
