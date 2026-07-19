"""M4 role-specific embedding adapter over the frozen M3 embedder API."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from groundloop.ai.contracts import (
    ChunkDraft,
    EmbeddingRecord,
    ModelArtifact,
    QueryKind,
)
from groundloop.ai.embeddings.bge import BGE_QUERY_PREFIX
from groundloop.ai.embeddings.common import EmbeddedQuery, EmbeddingInputAudit
from groundloop.errors import ArtifactConflictError, ValidationError
from groundloop.m4.admission.vector import ChunkRoleVector, ClaimRoleVector
from groundloop.m4.models.contracts import (
    CHUNK_ROLE_TEMPLATE,
    CLAIM_ROLE_TEMPLATE,
    ChunkVectorArtifact,
    ClaimVectorArtifact,
    EmbeddingAdapterSpec,
    EmbeddingRole,
    RoleEmbeddingProvenance,
    sha256_text,
    vector_sha256,
)


class M3RoleEmbedder(Protocol):
    model_artifact: ModelArtifact
    dimension: int
    last_audit: tuple[EmbeddingInputAudit, ...]

    def embed(self, chunks: tuple[ChunkDraft, ...]) -> tuple[EmbeddingRecord, ...]: ...

    def embed_query(self, text: str, query_kind: QueryKind) -> EmbeddedQuery: ...


@dataclass(frozen=True, slots=True)
class ClaimEmbeddingInput:
    claim_id: str
    text: str

    def __post_init__(self) -> None:
        if not self.claim_id.strip():
            raise ValidationError("claim_id must be non-empty")
        if not self.text.strip():
            raise ValidationError("claim text must be non-empty")


class M4BgeRoleAdapter:
    """Bind M3 BGE outputs to M4 query/passage roles and immutable IDs."""

    def __init__(self, backend: M3RoleEmbedder, spec: EmbeddingAdapterSpec) -> None:
        self._backend = backend
        self.spec = spec
        self._claim_bindings: dict[str, str] = {}
        self._chunk_bindings: dict[str, str] = {}
        self._claim_cache: dict[str, ClaimVectorArtifact] = {}
        self._chunk_cache: dict[str, ChunkVectorArtifact] = {}
        self._validate_backend()

    def _validate_backend(self) -> None:
        if self._backend.model_artifact != self.spec.model_artifact:
            raise ArtifactConflictError("embedding model artifact drift")
        if self._backend.dimension != self.spec.dimension:
            raise ArtifactConflictError("embedding dimension drift")
        if self.spec.claim_role_template != CLAIM_ROLE_TEMPLATE:
            raise ArtifactConflictError("claim role template drift")
        if self.spec.chunk_role_template != CHUNK_ROLE_TEMPLATE:
            raise ArtifactConflictError("chunk role template drift")

    @staticmethod
    def _reject_duplicate_ids(label: str, identifiers: tuple[str, ...]) -> None:
        if len(set(identifiers)) != len(identifiers):
            raise ValidationError(f"duplicate {label} identifiers in embedding batch")

    @staticmethod
    def _bind_input(
        bindings: dict[str, str], subject_id: str, input_hash: str, label: str
    ) -> None:
        previous = bindings.get(subject_id)
        if previous is not None and previous != input_hash:
            raise ArtifactConflictError(f"{label} immutable input drift")
        bindings[subject_id] = input_hash

    def _provenance(
        self,
        *,
        subject_id: str,
        role: EmbeddingRole,
        input_hash: str,
        vector: tuple[float, ...],
        audit: EmbeddingInputAudit,
    ) -> RoleEmbeddingProvenance:
        model = self.spec.model_artifact
        template = (
            self.spec.claim_role_template
            if role is EmbeddingRole.CLAIM_QUERY
            else self.spec.chunk_role_template
        )
        role_template_hash = sha256_text(template)
        output_hash = vector_sha256(vector)
        artifact_id = RoleEmbeddingProvenance.build_artifact_id(
            subject_id=subject_id,
            role=role,
            model_artifact_id=model.artifact_id,
            model_id=model.model_id,
            model_revision=model.immutable_revision,
            tokenizer_revision=model.tokenizer_revision,
            role_template_hash=role_template_hash,
            input_hash=input_hash,
            vector_hash=output_hash,
            adapter_spec_hash=self.spec.spec_hash,
        )
        return RoleEmbeddingProvenance(
            artifact_id=artifact_id,
            subject_id=subject_id,
            role=role,
            model_artifact_id=model.artifact_id,
            model_id=model.model_id,
            model_revision=model.immutable_revision,
            tokenizer_revision=model.tokenizer_revision,
            role_template_hash=role_template_hash,
            input_hash=input_hash,
            vector_hash=output_hash,
            adapter_spec_hash=self.spec.spec_hash,
            token_count=audit.token_count,
            max_tokens=audit.max_tokens,
            truncated=audit.truncated,
        )

    def embed_claims(
        self, claims: Sequence[ClaimEmbeddingInput]
    ) -> tuple[ClaimVectorArtifact, ...]:
        self._validate_backend()
        ordered = tuple(sorted(claims, key=lambda item: item.claim_id))
        self._reject_duplicate_ids("claim", tuple(item.claim_id for item in ordered))
        outputs: list[ClaimVectorArtifact] = []
        for item in ordered:
            expected_input_hash = sha256_text(f"{BGE_QUERY_PREFIX}{item.text}")
            self._bind_input(
                self._claim_bindings,
                item.claim_id,
                expected_input_hash,
                "claim",
            )
            cached = self._claim_cache.get(item.claim_id)
            if cached is not None:
                outputs.append(cached)
                continue
            embedded = self._backend.embed_query(item.text, QueryKind.CLAIM)
            if embedded.input_hash != expected_input_hash:
                raise ArtifactConflictError("claim query prefix/input hash drift")
            provenance = self._provenance(
                subject_id=item.claim_id,
                role=EmbeddingRole.CLAIM_QUERY,
                input_hash=embedded.input_hash,
                vector=embedded.vector,
                audit=embedded.audit,
            )
            artifact = ClaimVectorArtifact(
                ClaimRoleVector(item.claim_id, embedded.vector, embedded.input_hash),
                provenance,
            )
            self._claim_cache[item.claim_id] = artifact
            outputs.append(artifact)
        return tuple(outputs)

    def embed_chunks(
        self, chunks: Sequence[ChunkDraft]
    ) -> tuple[ChunkVectorArtifact, ...]:
        self._validate_backend()
        ordered = tuple(sorted(chunks, key=lambda item: item.chunk_version_id))
        self._reject_duplicate_ids(
            "chunk", tuple(item.chunk_version_id for item in ordered)
        )
        missing: list[ChunkDraft] = []
        for chunk in ordered:
            expected_input_hash = sha256_text(chunk.text)
            self._bind_input(
                self._chunk_bindings,
                chunk.chunk_version_id,
                expected_input_hash,
                "chunk",
            )
            if chunk.chunk_version_id not in self._chunk_cache:
                missing.append(chunk)

        if missing:
            records = self._backend.embed(tuple(missing))
            audits = self._backend.last_audit
            if len(records) != len(missing) or len(audits) != len(missing):
                raise ArtifactConflictError("embedding backend returned partial batch")
            for chunk, record, audit in zip(missing, records, audits, strict=True):
                if record.chunk_version_id != chunk.chunk_version_id:
                    raise ArtifactConflictError(
                        "passage embedding order/identity drift"
                    )
                if record.model_artifact_id != self.spec.model_artifact.artifact_id:
                    raise ArtifactConflictError("passage embedding model drift")
                expected_input_hash = sha256_text(chunk.text)
                if record.input_hash != expected_input_hash:
                    raise ArtifactConflictError("passage role/input hash drift")
                provenance = self._provenance(
                    subject_id=chunk.chunk_version_id,
                    role=EmbeddingRole.CHUNK_PASSAGE,
                    input_hash=record.input_hash,
                    vector=record.vector,
                    audit=audit,
                )
                self._chunk_cache[chunk.chunk_version_id] = ChunkVectorArtifact(
                    ChunkRoleVector(
                        chunk.chunk_version_id,
                        record.vector,
                        record.input_hash,
                    ),
                    provenance,
                )
        return tuple(self._chunk_cache[item.chunk_version_id] for item in ordered)
