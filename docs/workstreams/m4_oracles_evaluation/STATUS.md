# M4 Oracles/Evaluation Lane Status

Date: 2026-07-19

Branch: `workstream/m4-oracles-evaluation`

Wave 3 integration baseline:

```text
b3623fe Merge M4 evaluation mechanics lane
```

## Verdict

Wave 3 controlled dynamic workload and paired-report mechanics are complete
and ready for coordinator review. No empirical system-performance claim is
made: this increment uses deterministic controlled inputs and contains no
model, admission, runtime, database or pipeline execution.

## Implemented

- Immutable insert, replacement and delete event specifications.
- Continuous per-history corpus-snapshot chains and contiguous event indexes.
- Four deterministic independent histories: two development and two test,
  each containing insert, replacement and delete.
- Content-derived event, history, seed, split and workload manifests.
- Leakage rejection across split-component, lineage, claim-family, normalized
  content, claim, answer and chunk identities. Linked identifiers cannot be
  assigned to separate histories or cross development/test.
- Deliberate miss probes restricted to registered-claim by inserted-chunk
  pairs and required to be oracle-positive when metrics are derived.
- Immutable oracle and treatment event-result inputs with canonical pair and
  status ordering.
- Derivation of positive-pair, positive-claim, status-effect and answer-effect
  integer metrics. Admission and final-status agreement are deliberately
  separate.
- Delete-only positive-pair and positive-claim metrics remain explicit `0/0`
  records and serialize as JSON `null`, not fake zero or one values.
- Canonical paired JSON reports containing full workload, run, split, policy,
  verifier, decision, oracle, event, artifact and bootstrap provenance.
- Event diagnostics retain designated probes and actual missed positive pairs
  for both compared treatments.
- Report manifests bind the complete canonical payload, including raw event
  counts and bootstrap replicates.

## Frozen Controlled Workload Identities

```text
workload: 165c5e0999ad40593e222c6335144742c9e226a370a35c38025e53e21b075836
seeds:    7ef3b0cf803e32bae8809f403581752c8ca09326bbcca6948868dc943615d538
splits:   ca54bf4bd24ae721095737f515bb720039a8ed91f2e46b9dcb9f2328d317c1f6
```

These identify the deterministic fixture definition only. They are not
experimental measurements.

## Validation

```text
.venv/bin/pytest -q tests/m4/oracles tests/m4/test_m4_contracts.py
36 passed

.venv/bin/ruff check src/groundloop/m4/oracles tests/m4/oracles
All checks passed!

.venv/bin/mypy --strict src/groundloop/m4/oracles
Success: no issues found in 11 source files

.venv/bin/python -m compileall -q src/groundloop/m4/oracles tests/m4/oracles
exit 0

git diff --check
exit 0
```

## Scope Boundary

No selective admission or runtime delta module is imported. No shared
contract, migration, persistence, pipeline, CLI, roadmap or decision-log file
changed. No model was downloaded or run. Real workload ingestion, append-only
report persistence and bounded model runners remain later integration work.
