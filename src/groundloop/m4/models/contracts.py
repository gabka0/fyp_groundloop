"""Immutable M4 provenance records for reused M3 model executions.

These contracts live on the empirical side of GroundLoop's boundary.  They
bind model outputs to role templates and exact inputs; they do not make neural
predictions exact.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import StrEnum

from groundloop.ai.contracts import (
    AtomicClaim,
    ChunkDraft,
    ModelArtifact,
    PromptArtifact,
    VerificationResult,
)
from groundloop.domain import (
    DecisionPolicy,
    ModelStamp,
    SemanticObservation,
    SubjectKind,
    VerificationLabel,
    normalized_text_hash,
)
from groundloop.errors import ValidationError
from groundloop.m4.admission.vector import ChunkRoleVector, ClaimRoleVector
from groundloop.m4.contracts import (
    JudgmentSourceKind,
    PairJudgment,
    PairKey,
    stable_m4_digest,
)
from groundloop.policy import decide

CLAIM_ROLE_TEMPLATE = (
    "Represent this sentence for searching relevant passages: {text}"
)
CHUNK_ROLE_TEMPLATE = "{text}"
VERIFIER_ROLE_TEMPLATE = "premise={evidence}\nhypothesis={claim}"


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def vector_sha256(vector: tuple[float, ...]) -> str:
    """Hash exact IEEE-754 values without locale-sensitive formatting."""
    return sha256_text("\0".join(value.hex() for value in vector))


def decision_policy_hash(policy: DecisionPolicy) -> str:
    return stable_m4_digest(
        "m4-decision-policy-v1",
        policy.policy_version,
        format(policy.support_threshold, ".17g"),
        format(policy.refute_threshold, ".17g"),
        policy.tie_rule_version,
    )


class EmbeddingRole(StrEnum):
    CLAIM_QUERY = "claim_query"
    CHUNK_PASSAGE = "chunk_passage"


@dataclass(frozen=True, slots=True)
class EmbeddingAdapterSpec:
    model_artifact: ModelArtifact
    dimension: int
    local_artifact_sha256: str | None = None
    claim_role_template: str = CLAIM_ROLE_TEMPLATE
    chunk_role_template: str = CHUNK_ROLE_TEMPLATE
    adapter_version: str = "m4-m3-bge-role-adapter-v1"

    def __post_init__(self) -> None:
        if self.dimension <= 0:
            raise ValidationError("embedding dimension must be positive")
        if self.local_artifact_sha256 is not None and (
            len(self.local_artifact_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.local_artifact_sha256
            )
        ):
            raise ValidationError("local embedding artifact hash must be SHA-256")
        if self.claim_role_template != CLAIM_ROLE_TEMPLATE:
            raise ValidationError("claim role template drift")
        if self.chunk_role_template != CHUNK_ROLE_TEMPLATE:
            raise ValidationError("chunk role template drift")
        if not self.adapter_version.strip():
            raise ValidationError("embedding adapter version must be non-empty")

    @property
    def spec_hash(self) -> str:
        return stable_m4_digest(
            "m4-embedding-adapter-spec-v1",
            self.model_artifact.artifact_id,
            self.model_artifact.model_id,
            self.model_artifact.immutable_revision,
            self.model_artifact.tokenizer_revision,
            self.model_artifact.config_hash,
            self.local_artifact_sha256 or "hub-revision-only",
            str(self.dimension),
            sha256_text(self.claim_role_template),
            sha256_text(self.chunk_role_template),
            self.adapter_version,
        )


@dataclass(frozen=True, slots=True)
class RoleEmbeddingProvenance:
    artifact_id: str
    subject_id: str
    role: EmbeddingRole
    model_artifact_id: str
    model_id: str
    model_revision: str
    tokenizer_revision: str
    role_template_hash: str
    input_hash: str
    vector_hash: str
    adapter_spec_hash: str
    token_count: int | None
    max_tokens: int
    truncated: bool

    def __post_init__(self) -> None:
        if not self.subject_id.strip():
            raise ValidationError("embedding subject_id must be non-empty")
        if self.max_tokens <= 0:
            raise ValidationError("embedding max_tokens must be positive")
        if self.token_count is not None and self.token_count < 0:
            raise ValidationError("embedding token_count must be nonnegative")
        expected = self.build_artifact_id(
            subject_id=self.subject_id,
            role=self.role,
            model_artifact_id=self.model_artifact_id,
            model_id=self.model_id,
            model_revision=self.model_revision,
            tokenizer_revision=self.tokenizer_revision,
            role_template_hash=self.role_template_hash,
            input_hash=self.input_hash,
            vector_hash=self.vector_hash,
            adapter_spec_hash=self.adapter_spec_hash,
        )
        if self.artifact_id != expected:
            raise ValidationError("embedding artifact_id does not match provenance")

    @staticmethod
    def build_artifact_id(
        *,
        subject_id: str,
        role: EmbeddingRole,
        model_artifact_id: str,
        model_id: str,
        model_revision: str,
        tokenizer_revision: str,
        role_template_hash: str,
        input_hash: str,
        vector_hash: str,
        adapter_spec_hash: str,
    ) -> str:
        return stable_m4_digest(
            "m4-role-embedding-artifact-v1",
            subject_id,
            role.value,
            model_artifact_id,
            model_id,
            model_revision,
            tokenizer_revision,
            role_template_hash,
            input_hash,
            vector_hash,
            adapter_spec_hash,
        )


@dataclass(frozen=True, slots=True)
class ClaimVectorArtifact:
    vector: ClaimRoleVector
    provenance: RoleEmbeddingProvenance

    def __post_init__(self) -> None:
        if self.provenance.role is not EmbeddingRole.CLAIM_QUERY:
            raise ValidationError("claim vector has the wrong embedding role")
        if self.vector.claim_id != self.provenance.subject_id:
            raise ValidationError("claim vector subject/provenance mismatch")
        if self.vector.input_hash != self.provenance.input_hash:
            raise ValidationError("claim vector input/provenance mismatch")
        if vector_sha256(self.vector.vector) != self.provenance.vector_hash:
            raise ValidationError("claim vector output/provenance mismatch")


@dataclass(frozen=True, slots=True)
class ChunkVectorArtifact:
    vector: ChunkRoleVector
    provenance: RoleEmbeddingProvenance

    def __post_init__(self) -> None:
        if self.provenance.role is not EmbeddingRole.CHUNK_PASSAGE:
            raise ValidationError("chunk vector has the wrong embedding role")
        if self.vector.chunk_version_id != self.provenance.subject_id:
            raise ValidationError("chunk vector subject/provenance mismatch")
        if self.vector.input_hash != self.provenance.input_hash:
            raise ValidationError("chunk vector input/provenance mismatch")
        if vector_sha256(self.vector.vector) != self.provenance.vector_hash:
            raise ValidationError("chunk vector output/provenance mismatch")


@dataclass(frozen=True, slots=True)
class PairVerificationInput:
    """A PairKey plus all immutable text needed by the M3 verifier."""

    pair: PairKey
    claim_text: str
    claim_required: bool
    claim_cited_chunk_version_ids: tuple[str, ...]
    document_version_id: str
    chunk_index: int
    chunk_text: str
    chunk_text_hash: str
    chunker_artifact_id: str

    def __post_init__(self) -> None:
        if not self.claim_text.strip():
            raise ValidationError("verification claim text must be non-empty")
        if not self.document_version_id.strip():
            raise ValidationError("document_version_id must be non-empty")
        if self.chunk_index < 0:
            raise ValidationError("chunk_index must be nonnegative")
        if not self.chunk_text.strip():
            raise ValidationError("verification chunk text must be non-empty")
        if self.chunk_text_hash != normalized_text_hash(self.chunk_text):
            raise ValidationError("verification chunk text hash drift")
        if not self.chunker_artifact_id.strip():
            raise ValidationError("chunker_artifact_id must be non-empty")
        if len(set(self.claim_cited_chunk_version_ids)) != len(
            self.claim_cited_chunk_version_ids
        ):
            raise ValidationError("claim citations must be unique")

    @property
    def input_hash(self) -> str:
        return stable_m4_digest(
            "m4-verification-pair-input-v1",
            self.pair.claim_id,
            self.pair.chunk_version_id,
            self.claim_text,
            "required" if self.claim_required else "optional",
            str(len(self.claim_cited_chunk_version_ids)),
            *self.claim_cited_chunk_version_ids,
            self.document_version_id,
            str(self.chunk_index),
            self.chunk_text,
            self.chunk_text_hash,
            self.chunker_artifact_id,
        )

    def to_m3_pair(self) -> tuple[AtomicClaim, ChunkDraft]:
        return (
            AtomicClaim(
                local_claim_id=self.pair.claim_id,
                text=self.claim_text,
                required=self.claim_required,
                cited_chunk_version_ids=self.claim_cited_chunk_version_ids,
            ),
            ChunkDraft(
                chunk_version_id=self.pair.chunk_version_id,
                document_version_id=self.document_version_id,
                chunk_index=self.chunk_index,
                text=self.chunk_text,
                text_hash=self.chunk_text_hash,
                chunker_artifact_id=self.chunker_artifact_id,
            ),
        )


@dataclass(frozen=True, slots=True)
class VerificationAdapterSpec:
    model_artifact: ModelArtifact
    prompt_artifact: PromptArtifact
    calibration_version: str
    calibration_artifact_sha256: str | None
    temperature: float
    max_length: int
    decision_policy: DecisionPolicy
    adapter_version: str = "m4-m3-verifier-adapter-v1"

    def __post_init__(self) -> None:
        if not self.calibration_version.strip():
            raise ValidationError("calibration version must be non-empty")
        if self.calibration_artifact_sha256 is not None and (
            len(self.calibration_artifact_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.calibration_artifact_sha256
            )
        ):
            raise ValidationError("calibration artifact hash must be SHA-256")
        if self.temperature <= 0:
            raise ValidationError("temperature must be positive")
        if self.max_length <= 0:
            raise ValidationError("max_length must be positive")
        if self.prompt_artifact.template != VERIFIER_ROLE_TEMPLATE:
            raise ValidationError("verifier premise/hypothesis role template drift")

    @property
    def execution_spec_hash(self) -> str:
        return stable_m4_digest(
            "m4-verifier-execution-spec-v1",
            self.model_artifact.artifact_id,
            self.model_artifact.model_id,
            self.model_artifact.immutable_revision,
            self.model_artifact.tokenizer_revision,
            self.model_artifact.config_hash,
            self.model_artifact.artifact_sha256 or "no-tree-hash",
            self.prompt_artifact.artifact_id,
            self.prompt_artifact.template_hash,
            self.prompt_artifact.decoding_config_hash,
            self.calibration_version,
            self.calibration_artifact_sha256 or "unmaterialized-calibration",
            format(self.temperature, ".17g"),
            str(self.max_length),
            decision_policy_hash(self.decision_policy),
            self.adapter_version,
        )


@dataclass(frozen=True, slots=True)
class PairVerificationArtifact:
    artifact_id: str
    pair: PairKey
    pair_input_hash: str
    execution_spec_hash: str
    decision_policy_version: str
    decision_policy_hash: str
    operational_label: VerificationLabel
    result: VerificationResult

    def __post_init__(self) -> None:
        if self.result.claim_id != self.pair.claim_id:
            raise ValidationError("verifier result claim does not match PairKey")
        if self.result.chunk_version_id != self.pair.chunk_version_id:
            raise ValidationError("verifier result chunk does not match PairKey")
        expected = self.build_artifact_id(
            pair=self.pair,
            pair_input_hash=self.pair_input_hash,
            execution_spec_hash=self.execution_spec_hash,
            result=self.result,
            decision_policy_hash=self.decision_policy_hash,
            operational_label=self.operational_label,
        )
        if self.artifact_id != expected:
            raise ValidationError("verification artifact_id does not match payload")

    @staticmethod
    def build_artifact_id(
        *,
        pair: PairKey,
        pair_input_hash: str,
        execution_spec_hash: str,
        result: VerificationResult,
        decision_policy_hash: str,
        operational_label: VerificationLabel,
    ) -> str:
        return stable_m4_digest(
            "m4-pair-verification-artifact-v1",
            pair.claim_id,
            pair.chunk_version_id,
            pair_input_hash,
            execution_spec_hash,
            result.input_hash,
            result.raw_output_hash,
            format(result.scores.support, ".17g"),
            format(result.scores.refute, ".17g"),
            format(result.scores.neutral, ".17g"),
            *(format(value, ".17g") for value in result.raw_logits or ()),
            decision_policy_hash,
            operational_label.value,
        )

    def to_pair_judgment(self, *, split_id: str) -> PairJudgment:
        return PairJudgment(
            pair=self.pair,
            source_kind=JudgmentSourceKind.MODEL,
            source_artifact_id=self.artifact_id,
            decision_policy_or_guideline_id=self.decision_policy_version,
            derived_label=self.operational_label,
            input_hash=self.result.input_hash,
            split_id=split_id,
            support_score=self.result.scores.support,
            refute_score=self.result.scores.refute,
            neutral_score=self.result.scores.neutral,
        )

    def to_semantic_observation(
        self,
        *,
        model_id: str,
        model_revision: str,
        prompt_version: str,
        task_type: str = "verify",
    ) -> SemanticObservation:
        return SemanticObservation(
            observation_id=stable_m4_digest(
                "m4-semantic-observation-v1", self.artifact_id, task_type
            ),
            subject_kind=SubjectKind.CLAIM,
            subject_id=self.pair.claim_id,
            chunk_version_id=self.pair.chunk_version_id,
            task_type=task_type,
            support_score=self.result.scores.support,
            refute_score=self.result.scores.refute,
            neutral_score=self.result.scores.neutral,
            producer=ModelStamp(model_id, model_revision, prompt_version),
            input_hash=self.result.input_hash,
        )


def derive_operational_label(
    result: VerificationResult, policy: DecisionPolicy
) -> VerificationLabel:
    observation = SemanticObservation(
        observation_id="m4-transient-decision",
        subject_kind=SubjectKind.CLAIM,
        subject_id=result.claim_id,
        chunk_version_id=result.chunk_version_id,
        task_type="verify",
        support_score=result.scores.support,
        refute_score=result.scores.refute,
        neutral_score=result.scores.neutral,
        producer=ModelStamp(
            result.model_artifact_id,
            result.calibration_version,
            result.prompt_artifact_id,
        ),
        input_hash=result.input_hash,
    )
    return decide(observation, policy)
