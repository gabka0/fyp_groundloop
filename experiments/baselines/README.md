# Structured baseline runner

Run the dependency-free smoke benchmark from the repository root:

```bash
PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  experiments/baselines/run_structured_baselines.py \
  --config configs/baselines/structured_smoke.json \
  --output-dir /tmp/groundloop-structured-baselines
```

The command writes raw versioned JSONL, CSV and Markdown summaries, and an SVG
plot. Result directories are intentionally not committed.

The exact comparison set is global full recomputation, keyed affected-claim
recomputation, and the existing signed-delta treatment. Source-level and
direct-citation invalidation emit action sets with different semantics. Their
latencies and false-invalidation/stale-exposure counts are reported, but they
are excluded from exact-equivalence speedup values.

`kernel_only` excludes event deep-copy and the M1 event oracle.
`with_oracle_staging` includes both and is labeled accordingly; it is useful
for end-to-end implementation accounting, not as an incremental-kernel claim.
