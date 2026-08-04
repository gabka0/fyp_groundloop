# M4 shared-surface inventory and coordinator patch plan

Status: production compatibility patch incorporated; residual audit/fixture debt remains.

The canonical machine-readable inventory is
`m4_shared_surface_inventory.json`. A static M5.3 test rescans every Python
file under `src/`, `tests/`, and `scripts/` and requires the positional-INSERT
inventory to remain exact. It also verifies that every resolved runtime finding
still points at its shared table and contains the recorded mechanical claim
predicate or explicit column list. No M4-owned file is changed by this
workstream.

## Resolved runtime findings

Coordinator commits `f7394f7` and `c6dd2a8` resolve all ten production
findings in `src/groundloop/m4/pipeline.py`: eight claim-only runtime reads now
mechanically constrain `subject_kind = 'claim'`, and both shared-currency
promotion writes name every column, including `installed_revision`. These
boundaries cover strict bootstrap, working reconnect, publication bootstrap,
reverse withdrawal, replay validation, and seal promotion.

The machine-readable inventory retains the original finding IDs as resolved
evidence. The M5 PostgreSQL lane adds live coexistence coverage so requirement
rows remain present but invisible to v1 bootstrap/reconnect/withdrawal,
publication, and replay paths.

## Nonblocking inventory findings

The remaining source findings are deliberately distinguished from runtime
blockers:

- `real_dynamic_history.py` uses broad epoch audit counts, not state reads;
- `smoke.py` has a job-bound provenance read but no explicit claim predicate;
- `ai/persistence.py` reads by immutable observation ID and validates the kind
  in Python, but should be reviewed for mechanical SQL filtering;
- `real_history_study.py` has a positional claim INSERT in benchmark seeding.

The JSON also records fixture/test positional INSERTs and broad assertion or
query-plan reads. They do not contaminate production state by themselves, but
they remain explicit-column/static-audit debt and should be converted
mechanically after the runtime patch.

## Remaining order

1. Retain the live activated v1/M5 coexistence regression and frozen v1 replay
   behavior.
2. Convert benchmark and fixture positional INSERTs mechanically.
3. Decide explicitly whether broad audit counts are cross-kind metrics or
   claim-only M4 metrics, then encode that choice in SQL and tests.

This resolved compatibility evidence does not by itself approve the integrated
M5.3 milestone; independent re-audit and the separate M5.2/M5.4 integration
gates remain required.
