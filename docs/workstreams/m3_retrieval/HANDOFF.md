# M3 Retrieval Lane Handoff

## Integration surface

Use these public imports:

```python
from groundloop.ai.chunking import FixedCharChunker, prepare_directory
from groundloop.ai.embeddings import BgeSmallEmbedder, DeterministicFakeEmbedder
from groundloop.ai.retrieval import (
    InMemoryCosineStore,
    PgvectorCosineStore,
    StaticCosineRetriever,
    claim_query,
    question_query,
)
```

`FixedCharChunker.chunk()` satisfies the coordinator `Chunker` protocol.
Both embedders satisfy `Embedder` and additionally expose
`embed_query(text, query_kind)`. `StaticCosineRetriever.retrieve()` satisfies
the coordinator `Retriever` protocol.

## Coordinator assumptions

1. The pipeline persists `PreparedDocument.identity.document_id`,
   `document_version_id`, raw content hash, and every returned `ChunkDraft`
   without rewriting IDs.
2. Passage embeddings are inserted into
   `groundloop_chunk_embedding` before static retrieval. Persistence remains
   coordinator-owned.
3. Construct `PgvectorCosineStore` with a connection whose search path exposes
   the frozen M3 tables. The store parameterizes the vector/model/limit and
   performs no writes.
4. Construct queries with `question_query()` or `claim_query()` so the frozen
   defaults and method version are used. Candidate identity binds requested
   depth; `StaticCosineRetriever.last_execution` exposes depth, input hash,
   and truncation provenance explicitly.
5. Store `candidate.score` as cosine similarity (`1 - cosine distance`). Rank
   order is always distance ascending then chunk ID ascending.
6. The BGE adapter is CPU-only, revision pinned, and local-only. Artifact
   acquisition must occur before constructing it in the integrated real path.

## Replay and immutability behavior

- Same namespace/path/raw bytes/chunker yields identical document, version,
  chunk, embedding, and retrieval candidate identities.
- A different relative path changes `document_id`, even with duplicate raw
  content.
- Changed raw content retains `document_id` but changes document-version and
  chunk identities.
- Changed chunker artifact changes document-version and chunk identities.
- Same embedding key with identical payload is reusable; a conflicting
  payload raises `ArtifactConflictError` in the offline store.
- Same retrieval query and store snapshot returns identical ordered candidate
  tuples.

## Validation and unresolved gate

Exact command/results and the fake-fixture report are in `STATUS.md` and
`experiments/m3/retrieval/REPORT.md`. The full live suite passes 139 tests;
Ruff, strict mypy, and compileall pass.

The only lane-local empirical gate not run is the real BGE smoke because the
coordinator assigned the serialized real-workload window to the verifier lane.
Do not interpret that skip as a pass. After the coordinator releases the
window, use the exact acquisition and smoke commands in `STATUS.md`, record
artifact hashes and measured truncation/latency, and then perform coordinator
pipeline integration.
