"""Deterministic M4 impact-admission CORE."""

from groundloop.m4.admission.fake import DeterministicFakeApproximateIndex
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
from groundloop.m4.admission.manifest import build_candidate_policy_manifest
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
    "DeterministicFakeApproximateIndex",
    "DeterministicFakeLexemeAnalyzer",
    "DeterministicFakeLexicalBackend",
    "ExactReverseVectorIndex",
    "LexicalRawHit",
    "LexicalRegistrySnapshot",
    "LexicalV1Config",
    "LexicalV1Policy",
    "PreparedLexicalQuery",
    "ReverseVectorSearch",
    "build_candidate_policy_manifest",
    "fuse_admission_channels",
    "lineage_channel_hits",
    "load_frozen_lexical_v1",
    "measure_ann_recall",
]
