"""Static cosine retrieval over an injected embedding store."""

from groundloop.ai.retrieval.retriever import (
    CLAIM_TOP_K,
    QUESTION_TOP_K,
    RETRIEVAL_METHOD_VERSION,
    RetrievalExecution,
    StaticCosineRetriever,
    claim_query,
    question_query,
)
from groundloop.ai.retrieval.store import (
    EmbeddingStore,
    InMemoryCosineStore,
    PgvectorCosineStore,
    StoredEmbeddingHit,
)

__all__ = [
    "CLAIM_TOP_K",
    "QUESTION_TOP_K",
    "RETRIEVAL_METHOD_VERSION",
    "EmbeddingStore",
    "InMemoryCosineStore",
    "PgvectorCosineStore",
    "RetrievalExecution",
    "StaticCosineRetriever",
    "StoredEmbeddingHit",
    "claim_query",
    "question_query",
]
