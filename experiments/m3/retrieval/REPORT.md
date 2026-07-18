# M3 Retrieval Fixture Report

Scope: deterministic offline wiring evaluation, not BGE semantic-quality
evidence. The fake hash embedder is deliberately model-free, so its retrieval
quality numbers are a regression baseline only.

Command executed on 2026-07-18:

```bash
PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  experiments/m3/retrieval/evaluate_fixture.py
```

Fixture: eight short annotated Nimbus software-document pages and four queries
(two questions at `k=6`, two claims at `k=4`).

| Metric | Result |
|---|---:|
| Mean recall@k | 0.500000 |
| MRR | 0.175000 |
| Mean nDCG@k | 0.254446 |
| Cold corpus embedding/index construction | 2.525714 ms |
| Cold retrieval median | 0.470781 ms |
| Warm replay retrieval median | 0.475765 ms |
| Exact in-memory vector payload | 24,576 bytes |

The latency values are one host-local smoke measurement and are not a
dissertation-grade benchmark. The committed fixture runner also verifies that
all five warm replays per query return byte-for-byte equal candidate tuples.
A real BGE report remains pending the coordinator-serialized real-model window.
