"""Deterministic M4 impact-admission CORE."""

from groundloop.m4.admission.fake import DeterministicFakeApproximateIndex
from groundloop.m4.admission.fresh import (
    FreshFrontierCandidate,
    FreshFrontierRetrieval,
    FreshFrontierRetriever,
    PostgresExactFreshFrontierRetriever,
)
from groundloop.m4.admission.fusion import (
    AdmissionAccounting,
    AdmissionResult,
    fuse_admission_channels,
    lineage_channel_hits,
)
from groundloop.m4.admission.lexical import (
    DeterministicFakeLexemeAnalyzer,
    DeterministicFakeLexicalBackend,
    LexicalRawHit,
    LexicalRegistrySnapshot,
    LexicalV1Config,
    LexicalV1Policy,
    PreparedLexicalQuery,
    load_frozen_lexical_v1,
)
from groundloop.m4.admission.manifest import (
    build_candidate_policy_manifest,
    hash_config_pairs,
)
from groundloop.m4.admission.postgres_common import (
    CLAIM_ADMISSION_HNSW_INDEX,
    CLAIM_ADMISSION_RELATION,
    PostgresAdmissionServerIdentity,
)
from groundloop.m4.admission.postgres_lexical import (
    PostgresLexicalSearchBackend,
    PostgresSimpleLexemeAnalyzer,
)
from groundloop.m4.admission.postgres_vector import (
    ExactPgvectorConfig,
    HnswPgvectorBuildConfig,
    HnswPgvectorSearchConfig,
    PhysicalHnswIndex,
    PostgresExactReverseVectorIndex,
    PostgresHnswReverseVectorIndex,
)
from groundloop.m4.admission.vector import (
    AnnRecallMeasurement,
    ApproximateReverseVectorIndex,
    ChunkRoleVector,
    ClaimRoleVector,
    ExactReverseVectorIndex,
    ReverseVectorSearch,
    measure_ann_recall,
)

__all__ = [
    "AdmissionAccounting",
    "AdmissionResult",
    "AnnRecallMeasurement",
    "ApproximateReverseVectorIndex",
    "ChunkRoleVector",
    "ClaimRoleVector",
    "CLAIM_ADMISSION_HNSW_INDEX",
    "CLAIM_ADMISSION_RELATION",
    "DeterministicFakeApproximateIndex",
    "DeterministicFakeLexemeAnalyzer",
    "DeterministicFakeLexicalBackend",
    "ExactReverseVectorIndex",
    "ExactPgvectorConfig",
    "FreshFrontierCandidate",
    "FreshFrontierRetrieval",
    "FreshFrontierRetriever",
    "HnswPgvectorBuildConfig",
    "HnswPgvectorSearchConfig",
    "LexicalRawHit",
    "LexicalRegistrySnapshot",
    "LexicalV1Config",
    "LexicalV1Policy",
    "PreparedLexicalQuery",
    "PhysicalHnswIndex",
    "PostgresAdmissionServerIdentity",
    "PostgresExactFreshFrontierRetriever",
    "PostgresExactReverseVectorIndex",
    "PostgresHnswReverseVectorIndex",
    "PostgresLexicalSearchBackend",
    "PostgresSimpleLexemeAnalyzer",
    "ReverseVectorSearch",
    "build_candidate_policy_manifest",
    "fuse_admission_channels",
    "hash_config_pairs",
    "lineage_channel_hits",
    "load_frozen_lexical_v1",
    "measure_ann_recall",
]
