"""Canonical execution identities for the three M4 semantic job classes."""

from __future__ import annotations

from dataclasses import dataclass

from groundloop.errors import ValidationError
from groundloop.m4.contracts import CandidatePolicyManifest, stable_m4_digest


def _identity(name: str, value: str) -> str:
    if not value.strip():
        raise ValidationError(f"{name} must be non-empty")
    return value


@dataclass(frozen=True, slots=True)
class M4ExecutionIdentity:
    """Content-derived identities consumed by :class:`M4Application`.

    The hashes bind logical jobs to the concrete admission/model boundaries.
    They are execution provenance, not claims that approximate discovery is
    deterministic across different physical indexes.
    """

    impact_discovery_hash: str
    frontier_retrieval_hash: str
    verifier_hash: str

    @classmethod
    def build(
        cls,
        *,
        manifest: CandidatePolicyManifest,
        embedding_adapter_spec_hash: str,
        vector_adapter_artifact_id: str,
        lexical_analyzer_artifact_id: str,
        lexical_backend_artifact_id: str,
        frontier_retriever_artifact_id: str,
        admission_service_version: str = "postgres-hybrid-admission-v1",
        frontier_planner_version: str = "candidate-frontier-refill-v1",
    ) -> M4ExecutionIdentity:
        values = tuple(
            _identity(name, value)
            for name, value in (
                ("embedding_adapter_spec_hash", embedding_adapter_spec_hash),
                ("vector_adapter_artifact_id", vector_adapter_artifact_id),
                ("lexical_analyzer_artifact_id", lexical_analyzer_artifact_id),
                ("lexical_backend_artifact_id", lexical_backend_artifact_id),
                ("frontier_retriever_artifact_id", frontier_retriever_artifact_id),
                ("admission_service_version", admission_service_version),
                ("frontier_planner_version", frontier_planner_version),
            )
        )
        (
            embedding_hash,
            vector_id,
            analyzer_id,
            lexical_id,
            frontier_id,
            service_version,
            planner_version,
        ) = values
        impact = stable_m4_digest(
            "m4-impact-discovery-execution-v1",
            manifest.policy_id,
            manifest.policy_hash,
            embedding_hash,
            vector_id,
            analyzer_id,
            lexical_id,
            service_version,
        )
        frontier = stable_m4_digest(
            "m4-frontier-retrieval-execution-v1",
            manifest.policy_id,
            manifest.policy_hash,
            embedding_hash,
            frontier_id,
            planner_version,
        )
        return cls(impact, frontier, manifest.verifier_execution_spec_hash)
