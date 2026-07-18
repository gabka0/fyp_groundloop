"""Deterministic question/claim retrieval with complete provenance."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Protocol

from groundloop.ai.contracts import (
    ModelArtifact,
    QueryKind,
    RetrievalCandidate,
    RetrievalQuery,
    stable_digest,
)
from groundloop.ai.embeddings.common import EmbeddedQuery
from groundloop.ai.retrieval.store import EmbeddingStore
from groundloop.errors import ValidationError

QUESTION_TOP_K = 6
CLAIM_TOP_K = 4
RETRIEVAL_METHOD_VERSION = "bge-cosine-v1"


class QueryEmbedder(Protocol):
    model_artifact: ModelArtifact
    dimension: int

    def embed_query(self, text: str, query_kind: QueryKind) -> EmbeddedQuery: ...


@dataclass(frozen=True, slots=True)
class RetrievalExecution:
    query_kind: QueryKind
    query_id: str
    method_version: str
    top_k: int
    embedding_model_artifact_id: str
    input_hash: str
    truncated: bool


@dataclass(slots=True)
class StaticCosineRetriever:
    embedder: QueryEmbedder
    store: EmbeddingStore
    last_execution: RetrievalExecution | None = field(default=None, init=False)

    def retrieve(self, query: RetrievalQuery) -> tuple[RetrievalCandidate, ...]:
        artifact_id = self.embedder.model_artifact.artifact_id
        if query.embedding_model_artifact_id != artifact_id:
            raise ValidationError(
                "retrieval query embedding artifact does not match the embedder"
            )
        embedded = self.embedder.embed_query(query.text, query.query_kind)
        raw_hits = self.store.search_cosine(
            vector=embedded.vector,
            model_artifact_id=artifact_id,
            limit=query.top_k,
        )
        hits = sorted(raw_hits, key=lambda hit: (hit.distance, hit.chunk_version_id))[
            : query.top_k
        ]
        if any(not math.isfinite(hit.distance) for hit in hits):
            raise ValidationError("retrieval store returned a non-finite distance")
        candidates = tuple(
            RetrievalCandidate(
                candidate_id=stable_digest(
                    "retrieval-candidate-v1",
                    query.query_kind.value,
                    query.query_id,
                    hit.chunk_version_id,
                    artifact_id,
                    query.method_version,
                    str(query.top_k),
                    str(rank),
                ),
                query_kind=query.query_kind,
                query_id=query.query_id,
                chunk_version_id=hit.chunk_version_id,
                score=1.0 - hit.distance,
                rank=rank,
                embedding_model_artifact_id=artifact_id,
                method_version=query.method_version,
            )
            for rank, hit in enumerate(hits, start=1)
        )
        self.last_execution = RetrievalExecution(
            query_kind=query.query_kind,
            query_id=query.query_id,
            method_version=query.method_version,
            top_k=query.top_k,
            embedding_model_artifact_id=artifact_id,
            input_hash=embedded.input_hash,
            truncated=embedded.audit.truncated,
        )
        return candidates


def question_query(
    *,
    query_id: str,
    text: str,
    embedding_model_artifact_id: str,
    top_k: int = QUESTION_TOP_K,
) -> RetrievalQuery:
    return RetrievalQuery(
        query_kind=QueryKind.QUESTION,
        query_id=query_id,
        text=text,
        top_k=top_k,
        embedding_model_artifact_id=embedding_model_artifact_id,
        method_version=RETRIEVAL_METHOD_VERSION,
    )


def claim_query(
    *,
    query_id: str,
    text: str,
    embedding_model_artifact_id: str,
    top_k: int = CLAIM_TOP_K,
) -> RetrievalQuery:
    return RetrievalQuery(
        query_kind=QueryKind.CLAIM,
        query_id=query_id,
        text=text,
        top_k=top_k,
        embedding_model_artifact_id=embedding_model_artifact_id,
        method_version=RETRIEVAL_METHOD_VERSION,
    )
