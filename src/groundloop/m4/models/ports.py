"""Persistence-neutral application ports for pinned M4 model adapters.

The classes in this module deliberately stop at the empirical/exact boundary:
they resolve immutable model inputs, execute or exactly reuse the pinned M3
adapters, and return content-addressed artifacts to the M4 application.  They
do not issue SQL, decide late-result activation, or coordinate epochs.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Protocol

from groundloop.ai.contracts import ChunkDraft
from groundloop.errors import ArtifactConflictError, ValidationError
from groundloop.m4.admission.vector import ChunkRoleVector, ClaimRoleVector
from groundloop.m4.application import (
    VerificationResult as ApplicationVerificationResult,
)
from groundloop.m4.contracts import JobKind, LogicalJobSpec, PairKey, stable_m4_digest
from groundloop.m4.models.contracts import (
    ChunkVectorArtifact,
    ClaimVectorArtifact,
    PairVerificationArtifact,
    PairVerificationInput,
)
from groundloop.m4.models.embedding import ClaimEmbeddingInput, M4BgeRoleAdapter
from groundloop.m4.models.verification import M4CalibratedVerifierAdapter


class PairVerificationInputResolver(Protocol):
    """Resolve one persisted logical pair to its canonical immutable texts."""

    def resolve_pair_input(
        self, *, epoch_id: int, pair: PairKey
    ) -> PairVerificationInput: ...


def verification_artifact_payload_hash(
    artifact: PairVerificationArtifact,
) -> str:
    """Hash every persisted field of an exact pair-verification artifact."""
    result = artifact.result
    raw_logits = result.raw_logits
    return stable_m4_digest(
        "m4-application-verification-artifact-payload-v1",
        artifact.artifact_id,
        artifact.pair.claim_id,
        artifact.pair.chunk_version_id,
        artifact.pair_input_hash,
        artifact.execution_spec_hash,
        artifact.decision_policy_version,
        artifact.decision_policy_hash,
        artifact.operational_label.value,
        result.claim_id,
        result.chunk_version_id,
        result.candidate_id,
        result.model_artifact_id,
        result.prompt_artifact_id,
        result.calibration_version,
        format(result.temperature, ".17g"),
        format(result.scores.support, ".17g"),
        format(result.scores.refute, ".17g"),
        format(result.scores.neutral, ".17g"),
        result.input_hash,
        result.raw_output_hash,
        "raw-logits-present" if raw_logits is not None else "raw-logits-absent",
        *(format(value, ".17g") for value in raw_logits or ()),
    )


def _logical_job_identity(job: LogicalJobSpec) -> str:
    pair = job.pair
    return stable_m4_digest(
        "m4-model-verification-port-job-v1",
        job.job_id,
        job.event_id,
        job.kind.value,
        job.candidate_policy_id,
        job.payload_hash,
        job.execution_spec_hash,
        job.parent_job_id or "",
        pair.claim_id if pair is not None else "",
        pair.chunk_version_id if pair is not None else "",
        "expandable" if job.expandable else "terminal",
    )


@dataclass(slots=True)
class M4VerificationApplicationPort:
    """Implement ``application.VerificationPort`` over the pinned M3 adapter.

    Resolver calls are repeated on application replay so immutable-text drift
    cannot hide behind a process-local result cache.  The lower adapter still
    ensures that an exact repeat performs no second neural call.
    """

    resolver: PairVerificationInputResolver
    adapter: M4CalibratedVerifierAdapter
    request_count: int = field(default=0, init=False)
    resolver_call_count: int = field(default=0, init=False)
    successful_result_count: int = field(default=0, init=False)
    new_artifact_count: int = field(default=0, init=False)
    reused_artifact_count: int = field(default=0, init=False)
    _job_bindings: dict[tuple[int, str], str] = field(
        default_factory=dict, init=False
    )
    _artifact_results: dict[str, ApplicationVerificationResult] = field(
        default_factory=dict, init=False
    )
    _artifacts: dict[str, PairVerificationArtifact] = field(
        default_factory=dict, init=False
    )

    @property
    def backend_pair_calls(self) -> int:
        """Number of pairs actually sent through the neural backend."""
        return self.adapter.backend_pair_calls

    def artifact_by_id(self, artifact_id: str) -> PairVerificationArtifact:
        """Return an exact produced artifact for coordinator-owned persistence."""
        try:
            return self._artifacts[artifact_id]
        except KeyError as error:
            raise ValidationError("unknown verification artifact_id") from error

    def verify(
        self, epoch_id: int, verifier_job: LogicalJobSpec
    ) -> ApplicationVerificationResult:
        if epoch_id <= 0:
            raise ValidationError("verification epoch_id must be positive")
        if verifier_job.kind is not JobKind.VERIFY_PAIR:
            raise ValidationError("model verification port accepts VERIFY_PAIR only")
        pair = verifier_job.pair
        if pair is None:  # LogicalJobSpec enforces this; keep the port defensive.
            raise ValidationError("VERIFY_PAIR job is missing its PairKey")
        if verifier_job.execution_spec_hash != self.adapter.spec.execution_spec_hash:
            raise ArtifactConflictError("verifier job execution identity drift")

        self.request_count += 1
        job_key = (epoch_id, verifier_job.job_id)
        job_identity = _logical_job_identity(verifier_job)
        previous_job_identity = self._job_bindings.get(job_key)
        if previous_job_identity is not None and previous_job_identity != job_identity:
            raise ArtifactConflictError("verifier logical job identity drift")
        self._job_bindings[job_key] = job_identity

        self.resolver_call_count += 1
        pair_input = self.resolver.resolve_pair_input(epoch_id=epoch_id, pair=pair)
        if pair_input.pair != pair:
            raise ArtifactConflictError("resolver returned another PairKey")

        artifact = self.adapter.verify_pairs((pair_input,))[0]
        if artifact.pair != pair:
            raise ArtifactConflictError("verifier artifact belongs to another PairKey")
        if artifact.execution_spec_hash != verifier_job.execution_spec_hash:
            raise ArtifactConflictError("verifier artifact execution identity drift")

        spec = self.adapter.spec
        observation = artifact.to_semantic_observation(
            model_id=spec.model_artifact.model_id,
            model_revision=spec.model_artifact.immutable_revision,
            prompt_version=spec.prompt_artifact.version,
        )
        application_result = ApplicationVerificationResult(
            result_artifact_id=artifact.artifact_id,
            result_artifact_hash=verification_artifact_payload_hash(artifact),
            observation=observation,
        )
        previous_result = self._artifact_results.get(artifact.artifact_id)
        if previous_result is not None and previous_result != application_result:
            raise ArtifactConflictError("verification application artifact drift")
        if previous_result is None:
            self._artifact_results[artifact.artifact_id] = application_result
            self._artifacts[artifact.artifact_id] = artifact
            self.new_artifact_count += 1
        else:
            if self._artifacts[artifact.artifact_id] != artifact:
                raise ArtifactConflictError("verification exact artifact drift")
            self.reused_artifact_count += 1
        self.successful_result_count += 1
        return application_result


@dataclass(frozen=True, slots=True)
class AdmissionEmbeddingArtifacts:
    """Canonical embedding artifacts and role vectors for admission indexes."""

    claims: tuple[ClaimVectorArtifact, ...]
    chunks: tuple[ChunkVectorArtifact, ...]

    def __post_init__(self) -> None:
        claim_ids = tuple(item.vector.claim_id for item in self.claims)
        chunk_ids = tuple(item.vector.chunk_version_id for item in self.chunks)
        if claim_ids != tuple(sorted(set(claim_ids))):
            raise ValidationError("claim embedding artifacts must be sorted and unique")
        if chunk_ids != tuple(sorted(set(chunk_ids))):
            raise ValidationError("chunk embedding artifacts must be sorted and unique")

    @property
    def claim_vectors(self) -> tuple[ClaimRoleVector, ...]:
        return tuple(item.vector for item in self.claims)

    @property
    def chunk_vectors(self) -> tuple[ChunkRoleVector, ...]:
        return tuple(item.vector for item in self.chunks)


@dataclass(slots=True)
class M4AdmissionEmbeddingService:
    """Produce canonical role artifacts for exact or approximate admission."""

    adapter: M4BgeRoleAdapter
    request_count: int = field(default=0, init=False)
    new_claim_artifact_count: int = field(default=0, init=False)
    new_chunk_artifact_count: int = field(default=0, init=False)
    reused_claim_artifact_count: int = field(default=0, init=False)
    reused_chunk_artifact_count: int = field(default=0, init=False)
    _claim_artifact_ids: dict[str, str] = field(default_factory=dict, init=False)
    _chunk_artifact_ids: dict[str, str] = field(default_factory=dict, init=False)

    @staticmethod
    def _record_artifacts(
        artifacts: Sequence[ClaimVectorArtifact] | Sequence[ChunkVectorArtifact],
        bindings: dict[str, str],
        *,
        claim_role: bool,
    ) -> tuple[int, int]:
        new_count = 0
        reused_count = 0
        for artifact in artifacts:
            subject_id = (
                artifact.vector.claim_id
                if isinstance(artifact, ClaimVectorArtifact)
                else artifact.vector.chunk_version_id
            )
            artifact_id = artifact.provenance.artifact_id
            previous = bindings.get(subject_id)
            if previous is not None and previous != artifact_id:
                role = "claim" if claim_role else "chunk"
                raise ArtifactConflictError(f"{role} embedding artifact drift")
            if previous is None:
                bindings[subject_id] = artifact_id
                new_count += 1
            else:
                reused_count += 1
        return new_count, reused_count

    def embed_for_admission(
        self,
        *,
        claims: Sequence[ClaimEmbeddingInput] = (),
        chunks: Sequence[ChunkDraft] = (),
    ) -> AdmissionEmbeddingArtifacts:
        claim_artifacts = self.adapter.embed_claims(claims)
        chunk_artifacts = self.adapter.embed_chunks(chunks)
        new_claims, reused_claims = self._record_artifacts(
            claim_artifacts, self._claim_artifact_ids, claim_role=True
        )
        new_chunks, reused_chunks = self._record_artifacts(
            chunk_artifacts, self._chunk_artifact_ids, claim_role=False
        )
        result = AdmissionEmbeddingArtifacts(claim_artifacts, chunk_artifacts)
        self.request_count += 1
        self.new_claim_artifact_count += new_claims
        self.new_chunk_artifact_count += new_chunks
        self.reused_claim_artifact_count += reused_claims
        self.reused_chunk_artifact_count += reused_chunks
        return result
