# Baselines Workstream Handoff

## Outcome

Implementation commit: `7719edf` (`add structured baseline evaluation harness`).
The current `main` tip is still the recorded baseline
`5e5181b3920a5ed548f12880e280fe4b869c05aa`, so the required rebase check found
no newer integration commits to apply.

The lane now provides a dependency-free structured evaluation framework with:

- an independent global full-recomputation baseline;
- an exact keyed affected-claim recomputation baseline;
- a read-only wrapper around the existing signed-delta engine as treatment;
- separately typed source-level and direct-citation invalidation policies;
- deterministic workloads over `E`, `C`, `A`, `k`, `f`, duplicate-content
  ratio, skew, and locality;
- paired warmup/repeated trials, raw versioned JSONL, median/p95/p99 summaries,
  CSV/Markdown tables, and an automated SVG plot;
- explicit kernel-only versus M1-oracle/copy-staged timing; and
- adversarial high-fanout and large policy-flip smoke scenarios.

No shared contract, SQL, persistence, incremental-engine, optimized-engine, or
dependency-manifest file was changed.

## Semantic contract

The following three paths are semantically equivalent for the currently
implemented direct-witness structured event model:

1. `global_full_recompute` evaluates every claim and answer from base records.
2. `keyed_affected_recompute` discovers an exact affected-key superset for
   observe, delete, replace, insert, and frozen-rule policy events, then
   recomputes only those claims and parent answers.
3. `signed_delta_treatment` imports the existing M2 engine without modifying
   it.

The benchmark fails if complete claim or answer states differ after an event.
The baseline reference functions live in
`src/groundloop/baselines/semantics.py` and do not import
`groundloop.reference` or `groundloop.incremental`.

`source_level_invalidation` and `direct_citation_invalidation` are policy
baselines with different outputs: they produce action sets saying which
objects should be invalidated/refreshed. They do not compute canonical
GroundLoop states. Alternative witnesses therefore create measurable false
invalidations. Policy changes can create stale exposure because these two
document-change policies do not react to policy events. They are never given
an exact-state speedup.

## Reproduction

```bash
PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  experiments/baselines/run_structured_baselines.py \
  --config configs/baselines/structured_smoke.json \
  --output-dir /tmp/groundloop-structured-baselines
```

Recorded smoke execution in this worktree:

```text
records:      200
summary rows: 20
raw JSONL:    200 lines
summary CSV:  21 lines including header
```

The smoke is a reproducibility/shape check, not a stable performance result.
On this execution, the local exact kernels showed 4.414x keyed and 5.334x
signed-delta median speedups over global recomputation; the high-fanout
scenario narrowed them to 0.925x and 1.383x. Copy/oracle-staged medians were
approximately equal across exact paths, showing that M1 staging dominates
these tiny workloads. These numbers are host- and run-specific and must be
regenerated for any report.

## Validation

Executed from `/home/kassym/Desktop/groundloop-worktrees/baselines`:

```text
PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python -m pytest -q
67 passed

PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python -m ruff check .
All checks passed!

PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python -m mypy --strict src
Success: no issues found in 17 source files

PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  -m compileall -q src tests experiments/baselines
PASS
```

Baseline-specific tests cover deterministic generation, exact three-path
equality after every event, alternative-witness false invalidation, schema and
speedup separation, and high-fanout degradation.

## Interface assumptions

- M2 currently implements direct claim-subject witnesses only; evidence groups
  remain M5 and are not simulated here.
- `apply_event` is the authoritative structured mutation path. Its copy and
  full-reference work is excluded from `kernel_only` and included in
  `with_oracle_staging`.
- `f` counts observation decision flips caused by the generated monotone
  threshold event, not necessarily downstream claim status changes when an
  alternative support witness survives.
- Maintained bytes are Python-object estimates and should not be compared with
  future PostgreSQL on-disk size without a new storage metric.
- Full retrieval, neural verification, answer regeneration, TTL, and
  freshness-risk baselines are intentionally absent until M3/M4 freeze models,
  prompts, datasets, candidate policy, and caching rules.

## Remaining limitations and risks

- Timing samples are intentionally small smoke measurements. Dissertation
  claims need isolated larger runs, confidence intervals or repeated process
  launches, and controlled host load.
- Global and keyed recomputation are independent from the production oracle
  and treatment, but share baseline-local claim/answer reference helpers. A
  later SQL/Feldera comparison would add a stronger physical baseline.
- Keyed policy candidate discovery scans active observations to establish an
  exact set; it does not claim the optimized range-index complexity of the
  treatment.
- Current heuristic disagreement treats any exact status transition as a
  refresh target. Later end-to-end evaluation should refine severity and
  stale-exposure duration once publication and semantic-job timing are fixed.

## Contract requests

None.
