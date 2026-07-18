# Algorithm Workstream Handoff

Date: 2026-07-18

## Outcome

The exact-flip hypothesis is proved for frozen tie rule v1 under an explicit
RAM/index model and implemented in an engine independent of the current
incremental engine. The mechanism is classified as **known mechanism
specialized to GroundLoop**, not a novel IVM theorem.

The implementation uses two deterministic AVL indexes:

```text
P_support = {o : s > r and s > n}, keyed by (s,id)
P_refute  = {o : r >= s and r >= n}, keyed by (r,id)
```

Everything outside those sets is threshold-invariant NEUTRAL. A changed
threshold reports exactly the half-open score interval
`[min(old,new), max(old,new))`. Two simultaneous threshold changes perform two
disjoint searches.

## Guarantee matched by code

Under worst-case balanced-tree operations and expected-constant Python hash
table operations:

- space: `O(E + C + A)`;
- one observation point update: `O(log E)` ordered-index work;
- threshold-only policy update: expected `O(log E + f + p)`;
- explicit-state lower bound: `Omega(f+p)` writes;
- document withdrawal: `O(k log E + k + p)` in this implementation.

The two-search form is `O(2 log E + f_s + f_r + p)` and is compressed because
two is constant. Full provenance snapshot materialization has separate output
cost and is not hidden in the maintenance theorem. Arbitrary calibration or a
different coupled tie rule does not receive the fast path.

Proofs and assumptions:

- `docs/theory/direct_witness_model.md`
- `docs/theory/exact_flip_theorem.md`

## Implementation map

- `src/groundloop/optimized/avl.py`: deterministic worst-case logarithmic
  ordered set with half-open interval enumeration and invariant validation.
- `src/groundloop/optimized/exact_flip.py`: static dominance partition and
  exact flip query.
- `src/groundloop/optimized/engine.py`: independent direct-witness engine for
  activity, supersession, distinct content, maxima, claim/answer propagation,
  and certificates.
- `experiments/analysis/ivm/run_exact_flip_profiles.py`: candidate-visit
  profiles.
- `experiments/streams/algorithm_randomized_differential.py`: independent
  optimized-vs-reference randomized harness.

The engine imports immutable domain/events/repository records and the frozen
`decide` function. It does not import `groundloop.incremental` or use reference
recomputation as maintenance logic. The differential experiment alone imports
the oracle to compare complete snapshots after every event.

## Tests and adversarial coverage

`tests/optimized/` covers:

- strict support and REFUTE-conservative equality/tie boundaries;
- lower-inclusive, upper-exclusive threshold endpoints;
- simultaneous support/refute threshold changes;
- adversarial sorted AVL insert/delete order;
- `f=0`, sparse `f`, and `f=Theta(E)`;
- 500 never-winning rows inside a raw support-score interval;
- 100 equal-content flips producing one distinct-hash crossing;
- 250 supersessions on one currency key;
- document withdrawal with `k=E=200`;
- exact replay;
- complete-state randomized differential equality and certificate checks.

## Recorded results

Complexity profile command:

```bash
PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  experiments/analysis/ivm/run_exact_flip_profiles.py \
  --size 10000 --sparse-flips 10
```

Result: all 10,000 rows were in the raw support-score interval; the exact
index visited `0`, `10`, and `10,000` candidates for `f=0`, `f=10`, and
`f=E`. The candidate count equalled actual flips in every profile.

Randomized gate:

```bash
PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  experiments/streams/algorithm_randomized_differential.py \
  --events 10000 --events-per-stream 100 --seed 20260718
```

Result: 10,000 events, 100 streams, no mismatch. The measured 27.59 seconds
include deep copies, full oracle recomputation, full provenance output, and
invariant checks; it is not a kernel benchmark.

Full validation:

```text
compileall: PASS
pytest:     92 passed, 8 skipped (live PostgreSQL integration unavailable)
ruff:       All checks passed!
mypy:       Success: no issues found in 23 source files
```

## Literature positioning

`docs/theory/literature_audit.md` records search terms, date, primary links,
and assumptions for DBSP, F-IVM, CROWN, classical counting IVM, dynamic
conjunctive queries, materialized-view redefinition, and one-dimensional range
reporting.

No inspected source states this exact neural-score tie partition, but absence
from a bounded audit is not novelty evidence. View adaptation plus ordered
range reporting and counting deltas are established ideas. Confidence is high
in the proof and moderate in the literature classification.

## Limitations and integration notes

- Direct witnesses only; evidence groups remain M5.
- Neural inference and candidate discovery are excluded.
- Python hash tables provide expected, not adversarial worst-case, constant
  lookup.
- Dense flips and high-fanout withdrawals are linear; no speedup is claimed.
- Repeated AVL initialization is `O(E log E)`; no bulk-load optimization was
  added.
- The optimized engine is a candidate component. The coordinator should not
  replace the current engine without selecting an integration contract and
  rerunning all shared differential/baseline gates.
- No shared contracts, dependency manifests, current engine, baseline lane,
  PostgreSQL lane, roadmap, or decision log were edited.

No unresolved contract request exists.
