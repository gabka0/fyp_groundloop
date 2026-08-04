# M5.5 Lane C handoff

Date: 2026-08-03

## Owned deliverables

- `configs/m5/wice_source_manifest_v1.json`
- `configs/m5/controlled_evaluation_v1.json`
- `scripts/m5/run_controlled_evaluation.py`
- `src/groundloop/m5/evaluation/`
- `tests/m5/evaluation/`
- `docs/workstreams/m5_evaluation/README.md`
- `docs/workstreams/m5_evaluation/SOURCE_AUDIT.md`
- this handoff

`COORDINATOR_SOURCE_PREFLIGHT.md` was read but not edited. No shared/core,
matching, PostgreSQL, runtime, migration, dependency, top-level status, raw
data, generated report, cache, or large artifact is included.

## Public interfaces for integration

- `load_controlled_evaluation_config(path)` and
  `evaluation_config_identity_dict(config)`
- `load_source_manifest(path)` and `verify_source_files(root, manifest)`
- `adapt_wice(root, manifest)` / `load_and_adapt_wice(root, manifest)`
- `build_wice_primary_histories(adapter)`
- `build_authored_controlled_histories()`
- `run_baselines(histories, measure_latency=False)`
- `build_metric_report(run)`
- `evaluation_report_dict(...)` and `write_json_report(...)`

`WiceAdapterResult` exposes independent tuples for source claims/requirements,
external evidence-unit members, every valid original source annotation,
primary controlled groups, least-ordinal controlled observations, source
semantic states, exact SDR states, rejects/exclusions, and optional frozen
model diagnostics.

## Gate verdict

- **GO:** merge the pure adapter, protocol, baseline semantics, report
  mechanics, configs, and tests.
- **NO_GO:** do not mark M5.5 complete or report target-algorithm performance
  until the maintained Hall overlay/runtime and independent SQL oracle consume
  the same histories at coordinator integration.
- **NO_GO:** do not make a fresh semantic-validation/population claim; no new
  independently adjudicated human cohort exists.

Hall-failing rows remain source-supported and are never negative gold.
Serialized logical sizes are never described as process/database memory.
Pure baseline-5 latency is structurally disabled.

The config loader enforces frozen revision metadata and the adapter verifies
exact consumed bytes against manifest SHA-256 before parsing. Reports name the
consumed config/manifest paths and canonical config hash. This is not a Git
HEAD inspection claim.

The primary gate additionally requires config hash
`4ae7e27b9ada58965cbb72037739006d4caa728f955c27caf0bcc93bbee7532f`
and manifest hash
`c4a186f42252c280b5cafdc26b182e88c5aec14072866af15dd86d4b4c4722f2`;
other structurally valid bundles are explicitly `non_primary_config_bundle`.

## Validation evidence

The portable focused test command is:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src \
python3 -m pytest -q tests/m5/evaluation -p no:cacheprovider
```

Final lane validation on 2026-08-04:

- focused pytest: `37 passed`;
- focused Ruff: `All checks passed!`;
- focused compileall: exit 0;
- cached diff check: exit 0;
- strict mypy was blocked before project checking by the shared NumPy 2.5.1
  stub at `numpy/__init__.pyi:737` (`Type statement is only supported in Python
  3.12 and greater`) under the project's Python-3.11 target; no dependency or
  `pyproject.toml` change was made in this lane; and
- the supplemental strict diagnostic with site packages disabled reported
  `Success: no issues found in 11 source files`; this does not replace the
  required strict mypy rerun after the shared environment is corrected.

The source reproduction command and independent counts are in `README.md` and
`SOURCE_AUDIT.md`.
