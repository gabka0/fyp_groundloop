"""Offline retrieval wiring evaluation over an annotated software-doc fixture."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import time
from pathlib import Path
from typing import Any

from groundloop.ai.chunking import FixedCharChunker, prepare_document
from groundloop.ai.embeddings import DeterministicFakeEmbedder
from groundloop.ai.retrieval import (
    InMemoryCosineStore,
    StaticCosineRetriever,
    claim_query,
    question_query,
)

HERE = Path(__file__).resolve().parent
DEFAULT_FIXTURE = HERE / "fixtures" / "software_docs.json"


def _ndcg(retrieved: tuple[str, ...], relevant: set[str]) -> float:
    dcg = sum(
        1.0 / math.log2(rank + 1)
        for rank, chunk_id in enumerate(retrieved, start=1)
        if chunk_id in relevant
    )
    ideal = sum(
        1.0 / math.log2(rank + 1)
        for rank in range(1, min(len(retrieved), len(relevant)) + 1)
    )
    return dcg / ideal if ideal else 0.0


def evaluate(fixture_path: Path) -> dict[str, Any]:
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    chunker = FixedCharChunker()
    documents = [
        prepare_document(
            corpus_namespace=str(fixture["corpus_namespace"]),
            relative_path=str(document["path"]),
            raw_content=str(document["text"]).encode("utf-8"),
            chunker=chunker,
        )
        for document in fixture["documents"]
    ]
    chunks = tuple(chunk for document in documents for chunk in document.chunks)
    chunk_by_path = {
        document.identity.relative_path: document.chunks[0].chunk_version_id
        for document in documents
    }
    embedder = DeterministicFakeEmbedder()
    store = InMemoryCosineStore()
    embed_start = time.perf_counter_ns()
    store.add(embedder.embed(chunks))
    embedding_ms = (time.perf_counter_ns() - embed_start) / 1_000_000
    retriever = StaticCosineRetriever(embedder, store)

    rows: list[dict[str, Any]] = []
    cold_latencies: list[float] = []
    warm_latencies: list[float] = []
    for query_data in fixture["queries"]:
        query_factory = (
            question_query if query_data["kind"] == "question" else claim_query
        )
        query = query_factory(
            query_id=str(query_data["id"]),
            text=str(query_data["text"]),
            embedding_model_artifact_id=embedder.model_artifact.artifact_id,
        )
        started = time.perf_counter_ns()
        candidates = retriever.retrieve(query)
        cold_latencies.append((time.perf_counter_ns() - started) / 1_000_000)
        warm_runs: list[float] = []
        for _ in range(5):
            started = time.perf_counter_ns()
            replay = retriever.retrieve(query)
            warm_runs.append((time.perf_counter_ns() - started) / 1_000_000)
            if replay != candidates:
                raise RuntimeError("deterministic retrieval replay diverged")
        warm_latencies.append(statistics.median(warm_runs))
        retrieved = tuple(candidate.chunk_version_id for candidate in candidates)
        relevant = {chunk_by_path[str(path)] for path in query_data["relevant_paths"]}
        relevant_ranks = [
            rank
            for rank, chunk_id in enumerate(retrieved, start=1)
            if chunk_id in relevant
        ]
        rows.append(
            {
                "query_id": query.query_id,
                "kind": query.query_kind.value,
                "top_k": query.top_k,
                "recall_at_k": len(relevant & set(retrieved)) / len(relevant),
                "reciprocal_rank": 1.0 / min(relevant_ranks) if relevant_ranks else 0.0,
                "ndcg_at_k": _ndcg(retrieved, relevant),
                "retrieved_chunk_version_ids": retrieved,
            }
        )
    return {
        "schema_version": "m3-retrieval-eval-v1",
        "fixture": fixture_path.name,
        "embedding_backend": "deterministic-fake-embedding-v1",
        "quality_scope": "offline wiring only; not BGE semantic quality",
        "document_count": len(documents),
        "chunk_count": len(chunks),
        "query_count": len(rows),
        "mean_recall_at_k": statistics.fmean(row["recall_at_k"] for row in rows),
        "mrr": statistics.fmean(row["reciprocal_rank"] for row in rows),
        "mean_ndcg_at_k": statistics.fmean(row["ndcg_at_k"] for row in rows),
        "embedding_cold_ms": embedding_ms,
        "retrieval_cold_median_ms": statistics.median(cold_latencies),
        "retrieval_warm_median_ms": statistics.median(warm_latencies),
        "exact_vector_bytes": store.index_size_bytes,
        "queries": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", type=Path, default=DEFAULT_FIXTURE)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = evaluate(args.fixture)
    serialized = json.dumps(report, indent=2, sort_keys=True)
    if args.output is not None:
        args.output.write_text(f"{serialized}\n", encoding="utf-8")
    print(serialized)


if __name__ == "__main__":
    main()
