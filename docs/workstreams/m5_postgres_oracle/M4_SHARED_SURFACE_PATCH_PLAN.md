# M4 shared-surface inventory and coordinator patch plan

Status: coordinator patch required before the M5.3 coexistence gate can pass.

The canonical machine-readable inventory is
`m4_shared_surface_inventory.json`. A static M5.3 test rescans every Python
file under `src/`, `tests/`, and `scripts/` and requires the positional-INSERT
inventory to remain exact. It also verifies that every classified source read
still points at the recorded table and line. No M4-owned file is changed by
this workstream.

## Blocking runtime findings

`src/groundloop/m4/pipeline.py` has eight claim-only runtime reads that do not
mechanically constrain `subject_kind = 'claim'` and two positional INSERTs on
shared currency tables. These are correctness/static-gate blockers because an
activated database can contain requirement observations and currency beside
v1 claim rows. They cover strict bootstrap, working reconnect, publication
bootstrap, reverse withdrawal, replay validation, and seal promotion.

The coordinator patch should add claim predicates at those v1 boundaries and
explicit column lists to both promotion INSERTs without changing v1 digest,
event, replay, or publication identities. It must then run an activated
coexistence fixture with live requirement currency through bootstrap,
reconnect, withdrawal, publication, and exact replay.

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

## Required order

1. Patch the ten production `m4/pipeline.py` findings.
2. Prove activated v1/M5 coexistence and frozen v1 replay bytes.
3. Convert benchmark and fixture positional INSERTs mechanically.
4. Decide explicitly whether broad audit counts are cross-kind metrics or
   claim-only M4 metrics, then encode that choice in SQL and tests.

Until step 2 passes, M5.3 is locally implemented and its owned tests may be
green, but the repository-wide M5.3 coexistence exit gate remains **NO-GO**.
