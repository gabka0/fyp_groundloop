# M4.13 evaluation and selection handoff

## Status

Lane C implementation and the production development, calibration and
terminal executions are complete. The canonical post-execution report is
[`RESULTS.md`](RESULTS.md). The implementation and command details below remain
the historical pre-execution handoff; where they discuss unknown or pending
outcomes, `RESULTS.md` supersedes them.

The implementation covers development scoring and selection, calibration and
training-artifact replay, the production terminal unlock boundary, raw-logit
publication, deterministic metrics and uncertainty, the Git transfer
diagnostic, machine-evaluated gates, and failure-atomic bundle sealing.

## Read order

1. `docs/m4_13_change_aware_verifier_plan.md`, especially Sections 8-13.
2. `docs/workstreams/m4_13_change_aware_verifier/DATA_HANDOFF.md`.
3. `docs/workstreams/m4_13_change_aware_verifier/TRAINING_HANDOFF.md`.
4. `experiments/m4_change_aware_verifier/artifacts.py`.
5. `experiments/m4_change_aware_verifier/development_runner.py`.
6. `experiments/m4_change_aware_verifier/terminal.py`.
7. `tests/m4/change_aware_verifier/evaluation/`.

## Output-root contract

Development and terminal outputs use dedicated bundle roots. Do not point
either command at the shared M4.13 artifact root. A dedicated root is required
because all files in a phase are written to a sibling temporary directory,
self-validated, and published with one same-filesystem rename.

The physical layout is:

```text
artifacts/m4_13_change_aware_verifier/
  prepared/...
  sealed/...
  runs/...
  development_bundle/
    DEVELOPMENT_COMPLETED.json
    development/<model-key>/development_logits.jsonl
    development_report.json
    development_runtime.json
    selection.json
  terminal_bundle/
    terminal/
      vitaminc_logits.jsonl
      m3_test_logits.jsonl
      git_pilot_logits.jsonl
      metrics.json
      bootstrap.json
      semantic_result.json
      result_manifest.json
      COMPLETED.json
    runtime/timings.json
```

This is the failure-atomic realization of the logical file list in Section 13
of the plan. The filenames are unchanged, but development and terminal files
live under independently sealed bundle roots. Partial output is never retained
at the final path. An existing completed bundle permits exact validated replay;
an invocation collision, orphan, altered file, expanded file surface, unsafe
path, or concurrent output appearance fails closed.

## Development command

Run real model processes serially on this host. The command accepts either a
prebuilt model-path manifest or the training artifact root plus V0 checkpoint.
The latter form creates its transient path manifest outside the final bundle.

```bash
PYTHONPATH=.:src:training .venv/bin/python -m \
  experiments.m4_change_aware_verifier.run_development_evaluation \
  --data-root artifacts/m4_13_change_aware_verifier \
  --training-artifact-root artifacts/m4_13_change_aware_verifier \
  --v0-checkpoint \
    models/m3/verifier-run-20260718/checkpoints/minilm2-m3-bounded-v1 \
  --output-directory \
    artifacts/m4_13_change_aware_verifier/development_bundle
```

Development scores all nine frozen models: V0; V1 and A1 at the primary seed;
and V2/V3 at all three seeds. Selection is derived from immutable reloaded raw
logits. The sealed decision includes the V1/A1 diagnostic checkpoints even
though only V2 or V3 can be selected. `NO_ELIGIBLE_SELECTION` is a valid sealed
result and keeps terminal evaluation locked.

If and only if selection is eligible, calibrate all three selected-variant
checkpoints before terminal evaluation. The selection and development logits
belong to the dedicated development bundle; the training run directories do
not move:

```bash
SELECTION=artifacts/m4_13_change_aware_verifier/development_bundle/selection.json
SELECTED_VARIANT="$(.venv/bin/python -c \
  'import json,sys; v=json.load(open(sys.argv[1]))["selected_variant"]; assert v in {"V2-ce-mix", "V3-margin-mix"}; print(v)' \
  "$SELECTION")"
for SEED in 20260720 20260721 20260722; do
  PYTHONPATH=training .venv/bin/python -m m4_13_verifier.calibrate \
    --artifact-root artifacts/m4_13_change_aware_verifier \
    --config configs/m4/verifier/change_aware_v1.json \
    --run-directory \
      "artifacts/m4_13_change_aware_verifier/runs/$SELECTED_VARIANT/$SEED" \
    --selection "$SELECTION" \
    --development-logits \
      "artifacts/m4_13_change_aware_verifier/development_bundle/development/$SELECTED_VARIANT:seed-$SEED/development_logits.jsonl" \
    --repository-root "$PWD"
done
```

Each command writes and seals `calibration.json` inside its corresponding run
directory. Do not calibrate V1, A1, or the unselected V2/V3 variant.

## Production terminal command

```bash
PYTHONPATH=.:src:training .venv/bin/python -m \
  experiments.m4_change_aware_verifier.run_terminal_evaluation \
  --selection \
    artifacts/m4_13_change_aware_verifier/development_bundle/selection.json \
  --data-root artifacts/m4_13_change_aware_verifier \
  --candidate-artifact-root artifacts/m4_13_change_aware_verifier \
  --m4-12-artifact-root \
    /tmp/groundloop-m4-12-vitaminc-real-final-20260720 \
  --corrected-m4-10-root \
    /tmp/groundloop-m4-10-real-history-root-corrected \
  --v0-checkpoint \
    models/m3/verifier-run-20260718/checkpoints/minilm2-m3-bounded-v1 \
  --v0-calibration \
    models/m3/verifier-run-20260718/reports/temperature_calibration.json \
  --output-directory artifacts/m4_13_change_aware_verifier/terminal_bundle \
  --final-test-manifest-sha256 \
    3dcfcba0b809e3bcfcf2f9c036c4946f484d706faa61eec8c3fa489fbcb11dc5
```

The M4.12 path above is the exact final run containing semantic result
`017fd20f...` and predictions `0cd31543...`; do not substitute the stale
non-`final` run. The concrete M3, M4.10 and V0 paths must still be checked
against their local artifact handoffs before executing this example. The V0
paths shown are the exact repository paths expected by the frozen identities.

Production cannot call the row-injection API. The path-owned core performs the
following before any reserve file is opened:

1. reject symlinked/special prerequisite entries recursively, reject any
   non-reserve path or output under `sealed/`, and require the canonical exact
   development-bundle file surface without opening file contents;
2. replay the complete positive or negative development decision from raw
   logits and verify the report/runtime/completion hash chain;
3. independently validate all eight completed candidate runs, detailed
   890-microbatch schedules, checkpoints, runtime/dependency parity, and the
   V2/V3 paired schedule;
4. independently replay each selected calibration and verify its clean Git and
   implementation identity;
5. verify exact M4.12 consumed-diagnostic, original M3-test, and corrected
   M4.10 provenance;
6. bind a clean evaluator Git/code/dependency identity; and
7. validate or reject an existing terminal bundle by the row-free preflight
   hash.

Only after all seven checks pass can the core call `verify_terminal_reserve`.
Exact completed replay returns before that call. The evaluator revalidates the
non-reserve preflight, model/calibration artifacts, evaluator identity, reserve
file, reserve proof, and ordered reserve-row identity immediately before
publication.

## Metric contract

- Stored probability order is `(support, refute, neutral)` and base-logit order
  is `(contradiction, entailment, neutral)`.
- Endpoint and case metrics use the deployed-temperature probabilities and
  state the exact probability surface and temperature.
- Revision-transition flip, joint-correct, and bidirectional-margin metrics use
  the common uncalibrated `T=1` surface.
- Every bootstrap surface uses exactly seed `20260720`; production uses exactly
  1,000 percentile resamples.
- Primary VitaminC uncertainty is paired, page-clustered, and stratified within
  SUPPORT-REFUTE and SUPPORT-NEUTRAL. The unstratified page bootstrap is a
  labeled sensitivity surface. Complete-case and M3 claim-group surfaces are
  also emitted.
- Three-seed standard deviation uses the sample denominator `n-1`. The summary
  contains every frozen scalar point metric but does not average counts,
  confusion matrices, or confidence intervals.
- For a present true class with zero predicted positives, precision is `0.0`.
  For an absent true class, recall and F1 remain null; precision is `0.0` when
  false-positive predictions exist and otherwise null.
- Calibration ablations independently recompute NLL and operational policy
  counts at `T=1`, old M3 temperature, and deployed temperature.

The engineering-positive verdict is possible only when all point and safety
gates pass, including the `+0.10` primary joint-correct delta, and the only
remaining required-interval failure is an interval containing zero. An
entirely negative interval is `NO_GO`. Synthetic execution can never emit GO,
NO_GO, or promotion authorization.

## Git transfer diagnostic

The evaluator requires exactly 14 new-version pairs over the ten frozen claim
IDs. It aggregates distinct evidence witnesses with the GroundLoop truth
table: support only is SUPPORTED, refute only is REFUTED, both is CONFLICTED,
and neither is UNSUPPORTED. It applies the exact five obsolete concept groups,
reports stable negative warnings separately from stable UNSUPPORTED outcomes,
and reports inserted-positive states separately. It never applies a majority
vote, calls the surface temporal regression, or gives it go/no-go effect
without independent adjudication.

## Validation evidence

Validated in the shared project virtual environment without model downloads or
real inference:

```bash
PYTHONPATH=.:src:training .venv/bin/pytest -q \
  tests/m4/change_aware_verifier
# 110 collected; 109 passed and 1 expected skip

.venv/bin/ruff check training/m4_13_verifier \
  experiments/m4_change_aware_verifier tests/m4/change_aware_verifier
# all checks passed

MYPYPATH=src:training:. .venv/bin/mypy --strict --python-version 3.12 \
  training/m4_13_verifier experiments/m4_change_aware_verifier
# success

.venv/bin/python -m compileall -q training/m4_13_verifier \
  experiments/m4_change_aware_verifier tests/m4/change_aware_verifier
# success
```

The explicit mypy target matches this repository's active Python 3.12 virtual
environment. The repository-wide default remains 3.11, while the installed
NumPy stubs use Python 3.12 type-statement syntax.

Lane C adds 59 focused evaluator tests. They cover exact schedule and artifact
replay, development selection derivation, label/logit mapping, calibration
policy threshold crossings, atomic four-row cases in both directions, exact
cluster membership, paired deltas, the complete three-seed scalar surface,
sample standard deviation, stop/go edge cases, the Git truth table, preflight
before reserve access, exact replay before reserve reopening, raw-logit
reloading, positive and negative decision rederivation, resealed-decision
tampering, recursive prerequisite aliases into the sealed reserve, unsafe
output placement, unsafe/colliding artifacts, and RuntimeError/KeyboardInterrupt
failure atomicity.

## Next action

Execution is closed. Read [`RESULTS.md`](RESULTS.md) for the sealed `NO_GO`
verdict, artifact identities, limitations and the rule that any further model
work requires a new milestone and newly locked evaluation surface.
