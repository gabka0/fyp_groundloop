# Experiments

- `datasets/`: adapters and metadata, not large downloaded corpora.
- `streams/`: reproducible update-stream generators and manifests.
- `baselines/`: full recomputation and invalidation policies.
- `analysis/`: scripts that produce tables and figures from recorded results.

Do not commit raw datasets, model weights, secrets, or large result artifacts.

## M2 differential stream

The correctness workload compares the independent signed-delta engine with
full recomputation after every event. It shards immutable histories so the M1
copy-and-commit oracle remains a correctness tool rather than an accidental
quadratic benchmark.

```bash
PYTHONPATH=src python3 experiments/streams/run_m2_differential.py \
  --events 100000 --events-per-stream 100 --seed 20260718
```
