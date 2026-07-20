# M4.9 empirical selective-maintenance lane handoff

Status: implementation complete in the path-exclusive lane; coordinator
integration and real-history execution remain open.

## Ownership followed

This lane changed only:

- `src/groundloop/m4/empirical_eval.py`
- `experiments/m4_selective_study/`
- `tests/m4/empirical_eval/`
- `docs/workstreams/m4_9_empirical_study/`

It did not edit the M4 pipeline, migrations, CLI, roadmap, shared status docs,
existing tests or existing evaluation/oracle modules.

## What is implemented

`empirical_eval.py` is a frozen-output evaluation boundary. It consumes:

1. baseline-qualified positive pairs and status effects from the exhaustive
   event audit;
2. actual `SnapshotRefresh_k` pairs and artifact identity;
3. one immutable admission/runtime result for every policy and event;
4. actual verifier pair/call outcomes and optional token/latency telemetry.

It requires all seven treatments on exactly the same event IDs:

- exhaustive refresh;
- vector only;
- lexical only;
- vector plus lexical union;
- union plus lineage;
- union plus lineage and frontier;
- union plus lineage, frontier and mandatory fresh fallback.

The exhaustive treatment is rejected unless its pairs exactly equal the
supplied `SnapshotRefresh_k` pairs. For real persisted runs,
`FrozenOracleEvent.from_persisted_manifest` accepts no caller-supplied metric
targets: it parses exact positive-pair identities and exact post-status targets
from the immutable stored event-audit result, verifies the full JSON result
hash against `PersistedEventAudit.result_hash`, and binds the exact refresh
manifest and pairs. A second projection hash detects mutation after parsing.
The negative test changes a positive pair to a wrong identity with the same
cardinality, recomputes the untrusted JSON hashes, and confirms rejection
against the independently returned persisted result hash.

Per event, the harness records two explicitly qualified impact metrics
(positive-pair and positive-claim recall), claim-status-effect recall,
answer-status-effect recall, missed objects, failed events, failed pairs and
timeouts. Zero-denominator event metrics are `null`, never manufactured as
zero or one. Verifier attempts and calls are mandatory. Token and latency
fields are optional and carry a separate observation count, so missing
telemetry cannot be mistaken for zero work.

## Leakage and uncertainty

Every history declares all document-lineage, claim-family, normalized-content
and other connected-component identities relevant to split isolation. The
spec rejects any declared component appearing in more than one split.
Histories linked by a same-split component are unioned into one bootstrap
cluster. The percentile bootstrap resamples these connected clusters, not
individual events, using a content-derived deterministic draw and frozen seed.

This prevents leakage only for identities supplied by the dataset adapter. A
real-data adapter remains responsible for deriving the complete connected
components before the split is frozen.

## Controlled run evidence

Command:

```bash
PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  experiments/m4_selective_study/run_controlled_study.py \
  --output-directory /tmp/groundloop-m4-9-controlled-v5
```

Observed deterministic identities:

- study manifest: `58a1685e35573ed81fd59814af625148ea7e291a0ab873aeb2edcfcc21d2fffd`
- report: `1afe954ea7e205dfc28ee22c315d62f03ef39361a7eab004ee946aee477a4784`
- bundle manifest: `a6d6fed664313d212115bae2f94cf1be93df87e44b06ad71679c1c55dedaf3fa`

The selected test split has three independent history components and twelve
events. The study produces 84 policy-event rows, 336 event-metric rows and 28
metric summaries. It exercises positive candidate misses, claim-status misses,
answer-status misses, zero-denominator delete-event impact metrics, and one
explicit controlled timeout.

Controlled work totals were 45 verifier pairs for exhaustive refresh, 27 for
fresh fallback, 26 for frontier, 19 for lineage, 17 for union, and 9 each for
vector-only and lexical-only. The controlled fresh-fallback and exhaustive
rows achieve 1.0 on all four recalls; lower-cost policies intentionally miss
effects. These values say only that the fixture has the intended discriminating
shape. They are not model-quality or production-latency evidence.

## Validation

The lane validation commands and expected results are:

```bash
PYTHONPATH=src .venv/bin/pytest -q tests/m4/empirical_eval
# 7 passed

.venv/bin/ruff check \
  src/groundloop/m4/empirical_eval.py \
  experiments/m4_selective_study \
  tests/m4/empirical_eval
# All checks passed

.venv/bin/mypy --strict src/groundloop/m4/empirical_eval.py
# Success: no issues found in 1 source file

MYPYPATH=src .venv/bin/mypy --strict --explicit-package-bases \
  experiments/m4_selective_study/run_controlled_study.py
# Success: no issues found in 1 source file
```

The coordinator should rerun these commands after rebasing and then run the
repository-wide gate.

## Remaining work and hard limits

This lane does not complete M4.9. It deliberately does not fabricate:

- real-model token counts;
- embedding, lexical, verifier or end-to-end latency;
- quality results on a public claim-verification dataset;
- results on a naturally versioned software-document history;
- confidence intervals with more than three controlled history clusters.

Before an FYP result claim, run the same harness on frozen real histories with
persisted event-audit rows, pinned model/prompt/calibration identities, complete
component-level split construction, and actual telemetry. The three-cluster
controlled intervals are a reproducibility and aggregation test, not reliable
scientific uncertainty.
