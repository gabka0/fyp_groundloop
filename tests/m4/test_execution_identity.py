from __future__ import annotations

from dataclasses import replace

import pytest

from groundloop.errors import ValidationError
from groundloop.m4.contracts import CandidatePolicyManifest, VectorIndexKind
from groundloop.m4.execution import M4ExecutionIdentity


def _hash(character: str) -> str:
    return character * 64


def _manifest() -> CandidatePolicyManifest:
    return CandidatePolicyManifest.build(
        policy_id="candidate-v1",
        embedding_model_artifact_id="embedding-v1",
        claim_role_template_hash=_hash("a"),
        chunk_role_template_hash=_hash("b"),
        vector_method_version="exact-v1",
        vector_index_kind=VectorIndexKind.EXACT,
        vector_index_build_config_hash=_hash("c"),
        vector_search_config_hash=_hash("d"),
        lexical_method_version="lexical-v1",
        lexical_config_hash=_hash("e"),
        lexical_postgres_version="16.14",
        lexical_regconfig_identity="simple",
        claim_registry_snapshot_id="registry-v1",
        claim_count=1,
        fusion_version="interleave-v1",
        approximate_cap_per_inserted_chunk=1,
        frontier_depth=1,
        verifier_execution_spec_hash=_hash("f"),
        decision_policy_version="decision-v1",
    )


def _identity(manifest: CandidatePolicyManifest) -> M4ExecutionIdentity:
    return M4ExecutionIdentity.build(
        manifest=manifest,
        embedding_adapter_spec_hash=_hash("1"),
        vector_adapter_artifact_id="vector-adapter-v1",
        lexical_analyzer_artifact_id="analyzer-v1",
        lexical_backend_artifact_id="lexical-backend-v1",
        frontier_retriever_artifact_id="frontier-retriever-v1",
    )


def test_execution_hashes_bind_candidate_policy_and_physical_adapters() -> None:
    first = _identity(_manifest())
    changed_vector = M4ExecutionIdentity.build(
        manifest=_manifest(),
        embedding_adapter_spec_hash=_hash("1"),
        vector_adapter_artifact_id="vector-adapter-v2",
        lexical_analyzer_artifact_id="analyzer-v1",
        lexical_backend_artifact_id="lexical-backend-v1",
        frontier_retriever_artifact_id="frontier-retriever-v1",
    )
    changed_policy = _identity(
        replace(
            _manifest(),
            policy_id="candidate-v2",
            policy_hash=CandidatePolicyManifest.build(
                policy_id="candidate-v2",
                embedding_model_artifact_id="embedding-v1",
                claim_role_template_hash=_hash("a"),
                chunk_role_template_hash=_hash("b"),
                vector_method_version="exact-v1",
                vector_index_kind=VectorIndexKind.EXACT,
                vector_index_build_config_hash=_hash("c"),
                vector_search_config_hash=_hash("d"),
                lexical_method_version="lexical-v1",
                lexical_config_hash=_hash("e"),
                lexical_postgres_version="16.14",
                lexical_regconfig_identity="simple",
                claim_registry_snapshot_id="registry-v1",
                claim_count=1,
                fusion_version="interleave-v1",
                approximate_cap_per_inserted_chunk=1,
                frontier_depth=1,
                verifier_execution_spec_hash=_hash("f"),
                decision_policy_version="decision-v1",
            ).policy_hash,
        )
    )
    assert first.impact_discovery_hash != changed_vector.impact_discovery_hash
    assert first.impact_discovery_hash != changed_policy.impact_discovery_hash
    assert first.verifier_hash == _hash("f")


def test_execution_identity_rejects_unbound_adapter_identity() -> None:
    with pytest.raises(ValidationError, match="vector_adapter_artifact_id"):
        M4ExecutionIdentity.build(
            manifest=_manifest(),
            embedding_adapter_spec_hash=_hash("1"),
            vector_adapter_artifact_id="",
            lexical_analyzer_artifact_id="analyzer-v1",
            lexical_backend_artifact_id="lexical-v1",
            frontier_retriever_artifact_id="frontier-v1",
        )
