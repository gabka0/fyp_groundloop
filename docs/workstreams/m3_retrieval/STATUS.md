# M3 Retrieval Lane Status

Status: deterministic/offline implementation complete; live pgvector validated;
real BGE smoke pending the coordinator-serialized model window.

## Owned scope delivered

- `fixed-char-v1` line-ending normalization, outer trimming, paragraph
  packing, deterministic long-paragraph splitting, no overlap, and empty-doc
  handling.
- Stable directory identities from corpus namespace plus NFC-normalized POSIX
  relative path; document versions additionally bind raw-content SHA-256 and
  chunker artifact.
- Stable chunk IDs binding document version, zero-based index, chunker
  artifact, and normalization-v1 text hash.
- Deterministic offline fake embeddings and pinned local-only
  `BAAI/bge-small-en-v1.5` adapter at revision
  `5c38ec7c405ec4b44b94cc5a9bb96e735b38267a`.
- Exact frozen query prefix, no passage prefix, 384-dimensional finite unit
  vector validation, input hashing, and per-input truncation telemetry.
- Injected cosine-store boundary, exact in-memory store, and pgvector query
  ordered by `(distance, chunk_version_id)`.
- Question `top_k=6` and claim `top_k=4` defaults, stable candidate IDs, and
  explicit method/model/depth/rank execution provenance.
- Annotated eight-document software fixture, offline recall/ranking/latency/
  size evaluation, and a local-only real-model smoke command.

No coordinator-owned contract, migration, shared config, persistence,
pipeline, CLI, M1/M2, generation, or verifier file was changed.

## Test coverage

The owned tests cover empty documents, CRLF and bare-CR normalization, Unicode
text preservation, NFC path identity, whitespace/hard-limit long paragraphs,
repeated text, duplicate content at different paths, changed content, changed
chunker identity, invalid UTF-8, unsafe paths, vector dimension/nonfinite/zero
norm failures, exact query prefixing, real-adapter truncation telemetry via a
stub, missing-artifact failure, deterministic distance ties, default/top-k
boundaries, replay, artifact mismatch, pgvector SQL shape, live pgvector
execution, and required fixture metrics.

## Validation evidence

Executed in this worktree on 2026-07-18 with
`/home/kassym/Desktop/groundloop/.venv` and `PYTHONPATH=src`:

```text
pytest tests/ai/chunking tests/ai/retrieval (live DSN)  PASS: 26 passed
pytest (live DSN)                                      PASS: 139 passed
ruff check .                                           PASS: All checks passed!
mypy --strict src                                      PASS: 39 source files
python -m compileall -q src tests scripts experiments  PASS: no output
```

The live DSN came from `/home/kassym/Desktop/groundloop/.env`; the test used a
unique temporary schema and dropped it in `finally`.

Offline fixture command:

```bash
PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  experiments/m3/retrieval/evaluate_fixture.py
```

Recorded smoke results: eight documents/chunks, four annotated queries, mean
recall@k `0.500000`, MRR `0.175000`, mean nDCG@k `0.254446`, cold embedding
and index construction `2.525714 ms`, cold retrieval median `0.470781 ms`,
warm replay median `0.475765 ms`, and `24,576` exact vector bytes. These are
fake-embedding wiring results, not BGE semantic-quality or production latency
claims.

## Real artifact acquisition and smoke

Not executed: the verifier lane owns the serialized real-workload window, and
the coordinator explicitly asked this lane not to download or run BGE yet.
When released, acquire the exact revision outside ordinary tests:

```bash
export BGE_CACHE=/absolute/path/outside-git/hf-cache
/home/kassym/Desktop/groundloop/.venv/bin/hf download \
  BAAI/bge-small-en-v1.5 \
  --revision 5c38ec7c405ec4b44b94cc5a9bb96e735b38267a \
  --cache-dir "$BGE_CACHE"
```

Then run:

```bash
PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  experiments/m3/retrieval/real_model_smoke.py --cache-dir "$BGE_CACHE"
```

The adapter always uses `local_files_only=True`; an absent artifact exits with
the explicit prefix `REAL_MODEL_ARTIFACT_MISSING`. Ordinary tests monkeypatch
the loader and verify this path without importing or downloading a model.

## Limitations and confidence

- No real BGE quality/latency/truncation measurement has run yet.
- The offline fixture's hash embeddings intentionally have no semantic-quality
  interpretation.
- Live pgvector correctness is validated on a small exact fixture, not an HNSW
  scale/performance study.
- Integration into coordinator-owned persistence and pipeline remains the
  coordinator gate after merge.

Confidence is **high** for deterministic chunking/identity, offline embedding
contracts, retrieval ordering/provenance, and the tested pgvector boundary;
**unknown** for real-model host latency until the serialized smoke runs.
