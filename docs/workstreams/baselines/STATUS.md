# Baselines Workstream Status

## Scope record

- Branch: `workstream/baselines`
- Worktree: `/home/kassym/Desktop/groundloop-worktrees/baselines`
- Baseline commit: `5e5181b3920a5ed548f12880e280fe4b869c05aa`
- Writable paths: `src/groundloop/baselines/**`, `experiments/baselines/**`,
  `experiments/streams/baseline_*`, `tests/baselines/**`,
  `configs/baselines/**`, `docs/workstreams/baselines/**`
- Forbidden paths: migrations/SQL, incremental or optimized engines, shared
  contracts, dependency manifests, and core event/domain semantics

## Current step

Implementation committed at `7719edf`; ready for coordinator review.

## Last validation

```text
PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python -m pytest -q
67 passed

PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python -m ruff check .
All checks passed!

PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python -m mypy --strict src
Success: no issues found in 17 source files

Structured smoke: 200 raw JSONL records, 20 summary rows
```

## Blocker

None. No shared-contract change was required.

## Next action

Coordinator reviews the ownership diff and integrates the lane. `main` remains
at the recorded baseline commit, so no rebase transformation was required.
