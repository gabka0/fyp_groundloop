"""M4 adapters that reuse frozen M3 embedding and verification artifacts."""

from groundloop.m4.models.config import (
    LocalArtifactAvailability,
    PinnedM3AdapterBundle,
    PinnedM3ReuseConfig,
    build_pinned_m3_adapters,
    inspect_local_artifacts,
)
from groundloop.m4.models.contracts import (
    CHUNK_ROLE_TEMPLATE,
    CLAIM_ROLE_TEMPLATE,
    VERIFIER_ROLE_TEMPLATE,
    ChunkVectorArtifact,
    ClaimVectorArtifact,
    EmbeddingAdapterSpec,
    EmbeddingRole,
    PairVerificationArtifact,
    PairVerificationInput,
    RoleEmbeddingProvenance,
    VerificationAdapterSpec,
    decision_policy_hash,
)
from groundloop.m4.models.embedding import ClaimEmbeddingInput, M4BgeRoleAdapter
from groundloop.m4.models.verification import M4CalibratedVerifierAdapter

__all__ = [
    "CHUNK_ROLE_TEMPLATE",
    "CLAIM_ROLE_TEMPLATE",
    "VERIFIER_ROLE_TEMPLATE",
    "ChunkVectorArtifact",
    "ClaimEmbeddingInput",
    "ClaimVectorArtifact",
    "EmbeddingAdapterSpec",
    "EmbeddingRole",
    "LocalArtifactAvailability",
    "M4BgeRoleAdapter",
    "M4CalibratedVerifierAdapter",
    "PairVerificationArtifact",
    "PairVerificationInput",
    "PinnedM3AdapterBundle",
    "PinnedM3ReuseConfig",
    "RoleEmbeddingProvenance",
    "VerificationAdapterSpec",
    "build_pinned_m3_adapters",
    "decision_policy_hash",
    "inspect_local_artifacts",
]
