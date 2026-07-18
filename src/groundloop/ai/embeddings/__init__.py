"""Deterministic and pinned embedding implementations for M3 retrieval."""

from groundloop.ai.embeddings.bge import (
    BGE_MODEL_ID,
    BGE_QUERY_PREFIX,
    BGE_REVISION,
    BgeSmallEmbedder,
)
from groundloop.ai.embeddings.common import EmbeddedQuery, EmbeddingInputAudit
from groundloop.ai.embeddings.fake import DeterministicFakeEmbedder

__all__ = [
    "BGE_MODEL_ID",
    "BGE_QUERY_PREFIX",
    "BGE_REVISION",
    "BgeSmallEmbedder",
    "DeterministicFakeEmbedder",
    "EmbeddedQuery",
    "EmbeddingInputAudit",
]
