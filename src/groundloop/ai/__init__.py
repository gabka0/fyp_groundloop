"""Versioned AI boundary for the GroundLoop M3 static pipeline.

Neural components implement the protocols in :mod:`groundloop.ai.contracts`.
Their outputs are empirical observations; exact maintenance starts only after
those immutable outputs have been registered with the structured core.
"""

from groundloop.ai.contracts import (
    AnswerGenerator,
    ArtifactKind,
    ArtifactStore,
    AtomicClaim,
    ChunkDraft,
    Chunker,
    CitedAnswer,
    ClaimExtractionResult,
    Embedder,
    EmbeddingRecord,
    EvidencePassage,
    EvidenceVerifier,
    ModelArtifact,
    ModelRegistry,
    ModelTask,
    PipelineRunManifest,
    PromptArtifact,
    PromptRegistry,
    RetrievalCandidate,
    RetrievalQuery,
    Retriever,
    ScoreTriple,
    VerificationResult,
    stable_digest,
)

__all__ = [
    "AnswerGenerator",
    "ArtifactStore",
    "ArtifactKind",
    "AtomicClaim",
    "ChunkDraft",
    "Chunker",
    "ClaimExtractionResult",
    "CitedAnswer",
    "Embedder",
    "EmbeddingRecord",
    "EvidencePassage",
    "EvidenceVerifier",
    "ModelArtifact",
    "ModelRegistry",
    "ModelTask",
    "PipelineRunManifest",
    "PromptArtifact",
    "PromptRegistry",
    "RetrievalCandidate",
    "RetrievalQuery",
    "Retriever",
    "ScoreTriple",
    "VerificationResult",
    "stable_digest",
]
