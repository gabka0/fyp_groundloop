"""Candidate-policy manifest construction from frozen admission artifacts."""

from __future__ import annotations

from collections.abc import Sequence

from groundloop.errors import ValidationError
from groundloop.m4.admission.lexical import LexicalV1Config
from groundloop.m4.contracts import (
    CandidatePolicyManifest,
    VectorIndexKind,
    stable_m4_digest,
)
from groundloop.m4.models.contracts import sha256_text


def hash_config_pairs(namespace: str, values: Sequence[tuple[str, str]]) -> str:
    """Hash canonical sorted config pairs for manifest/provenance validation."""
    keys = tuple(key for key, _value in values)
    if keys != tuple(sorted(set(keys))):
        raise ValidationError("config fields must be sorted with unique keys")
    return stable_m4_digest(
        namespace,
        *(value for key, item in values for value in (key, item)),
    )


def build_candidate_policy_manifest(
    *,
    policy_id: str,
    embedding_model_artifact_id: str,
    claim_role_template: str,
    chunk_role_template: str,
    vector_method_version: str,
    vector_index_kind: VectorIndexKind,
    vector_index_build_config: Sequence[tuple[str, str]],
    vector_search_config: Sequence[tuple[str, str]],
    lexical_method_version: str,
    lexical_config: LexicalV1Config,
    lexical_postgres_version: str,
    lexical_regconfig_identity: str,
    claim_registry_snapshot_id: str,
    claim_count: int,
    fusion_version: str,
    approximate_cap_per_inserted_chunk: int,
    frontier_depth: int,
    verifier_execution_spec_hash: str,
    decision_policy_version: str,
    lineage_safety_override: bool = True,
) -> CandidatePolicyManifest:
    if not claim_role_template.strip() or not chunk_role_template.strip():
        raise ValidationError("embedding role templates must be non-empty")
    return CandidatePolicyManifest.build(
        policy_id=policy_id,
        embedding_model_artifact_id=embedding_model_artifact_id,
        claim_role_template_hash=sha256_text(claim_role_template),
        chunk_role_template_hash=sha256_text(chunk_role_template),
        vector_method_version=vector_method_version,
        vector_index_kind=vector_index_kind,
        vector_index_build_config_hash=hash_config_pairs(
            "m4-vector-index-build-config-v1", vector_index_build_config
        ),
        vector_search_config_hash=hash_config_pairs(
            "m4-vector-search-config-v1", vector_search_config
        ),
        lexical_method_version=lexical_method_version,
        lexical_config_hash=lexical_config.config_hash,
        lexical_postgres_version=lexical_postgres_version,
        lexical_regconfig_identity=lexical_regconfig_identity,
        claim_registry_snapshot_id=claim_registry_snapshot_id,
        claim_count=claim_count,
        fusion_version=fusion_version,
        approximate_cap_per_inserted_chunk=approximate_cap_per_inserted_chunk,
        frontier_depth=frontier_depth,
        verifier_execution_spec_hash=verifier_execution_spec_hash,
        decision_policy_version=decision_policy_version,
        lineage_safety_override=lineage_safety_override,
    )
