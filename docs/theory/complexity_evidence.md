# Exact-Flip Complexity Evidence

The executable profile is
`experiments/analysis/ivm/run_exact_flip_profiles.py`. It constructs rows whose
support scores all fall in the same raw threshold interval, while varying how
many rows can actually win against refute and neutral.

Command run on 2026-07-18:

```bash
PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  experiments/analysis/ivm/run_exact_flip_profiles.py \
  --size 10000 --sparse-flips 10
```

Recorded result:

| profile | E | raw interval rows | visited candidates | f |
|---|---:|---:|---:|---:|
| `f_zero` | 10,000 | 10,000 | 0 | 0 |
| `f_sparse` | 10,000 | 10,000 | 10 | 10 |
| `f_dense` | 10,000 | 10,000 | 10,000 | 10,000 |

This falsifies the concern that the partitioned implementation merely scans
the raw threshold interval and filters afterward. Candidate visits equal
actual flips in all three regimes. It also exposes the honest worst case:
when `f = E`, the index returns every row and the maintenance work is linear.

Other adversarial automated tests cover:

- threshold equality and conservative ties;
- high duplicate-content multiplicity;
- 250 successive currency supersessions on one key;
- a document withdrawal with `k = E = 200` active observations;
- adversarial sorted insertion/deletion into the AVL index;
- certificate validity after every differential event.

The randomized command

```bash
PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  experiments/streams/algorithm_randomized_differential.py \
  --events 10000 --events-per-stream 100 --seed 20260718
```

completed 10,000 events across 100 independent bounded streams with no state
mismatch. Its 27.59-second elapsed time includes repository deep copies, full
oracle recomputation, full provenance materialization, and invariant checking;
it is correctness evidence, not an optimized-kernel throughput claim.
