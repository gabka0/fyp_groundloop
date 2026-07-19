from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pytest

from groundloop.m4.admission import (
    LexicalV1Config,
    build_candidate_policy_manifest,
)
from groundloop.m4.contracts import CandidatePolicyManifest, VectorIndexKind

HASH = "a" * 64
REPO_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def lexical_config() -> LexicalV1Config:
    return LexicalV1Config.load(REPO_ROOT / "configs/m4/impact/lexical_v1.json")


def policy_manifest(
    lexical_config: LexicalV1Config,
    *,
    cap: int = 3,
    claim_count: int = 4,
    registry_snapshot_id: str = "registry-1",
    vector_index_kind: VectorIndexKind = VectorIndexKind.EXACT,
    vector_build: Sequence[tuple[str, str]] = (("algorithm", "brute-force"),),
    vector_search: Sequence[tuple[str, str]] = (("distance", "cosine"),),
    lineage: bool = True,
) -> CandidatePolicyManifest:
    return build_candidate_policy_manifest(
        policy_id="policy-1",
        embedding_model_artifact_id="embedding-1",
        claim_role_template=(
            "Represent this sentence for searching relevant passages: {claim}"
        ),
        chunk_role_template="{chunk}",
        vector_method_version="reverse-bge-v1",
        vector_index_kind=vector_index_kind,
        vector_index_build_config=vector_build,
        vector_search_config=vector_search,
        lexical_method_version="postgres-lexical-v1",
        lexical_config=lexical_config,
        lexical_postgres_version="fake-wave1",
        lexical_regconfig_identity="simple",
        claim_registry_snapshot_id=registry_snapshot_id,
        claim_count=claim_count,
        fusion_version="rank-interleave-v1",
        approximate_cap_per_inserted_chunk=cap,
        frontier_depth=4,
        verifier_execution_spec_hash=HASH,
        decision_policy_version="m3-policy-v1",
        lineage_safety_override=lineage,
    )
