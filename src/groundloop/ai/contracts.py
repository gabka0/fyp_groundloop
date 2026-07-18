"""Frozen typed contracts at GroundLoop's empirical/exact M3 boundary.

The records in this module describe versioned AI inputs and outputs. They do
not assert that a generated answer, extracted claim, retrieved passage, or
verification score is semantically correct. Exact GroundLoop claims begin
only after :class:`VerificationResult` values become immutable structured
observations.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from groundloop.domain import AnswerState, ClaimState, normalized_text_hash
from groundloop.errors import ValidationError

_HEX = frozenset("0123456789abcdef")


def stable_digest(*parts: str) -> str:
    """Return an unambiguous SHA-256 digest for ordered UTF-8 string parts."""
    digest = hashlib.sha256()
    for part in parts:
        encoded = part.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def _require_text(name: str, value: str) -> None:
    if not value.strip():
        raise ValidationError(f"{name} must be non-empty")


def _require_sha256(name: str, value: str) -> None:
    if len(value) != 64 or any(character not in _HEX for character in value):
        raise ValidationError(f"{name} must be a lowercase SHA-256 hex digest")


class ArtifactKind(StrEnum):
    MODEL = "model"
    PROMPT = "prompt"
    CHUNKER = "chunker"
    EMBEDDING = "embedding"
    RETRIEVAL = "retrieval"
    GENERATION = "generation"
    EXTRACTION = "extraction"
    VERIFICATION = "verification"


class ModelTask(StrEnum):
    EMBEDDING = "embedding"
    GENERATION = "generation"
    CLAIM_EXTRACTION = "claim_extraction"
    VERIFICATION = "verification"


class QueryKind(StrEnum):
    QUESTION = "question"
    CLAIM = "claim"


class PipelineRunStatus(StrEnum):
    STAGED = "staged"
    PUBLISHED = "published"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class ModelArtifact:
    artifact_id: str
    task: ModelTask
    provider: str
    model_id: str
    immutable_revision: str
    tokenizer_revision: str
    license_id: str
    config_hash: str
    artifact_sha256: str | None = None

    def __post_init__(self) -> None:
        for name, value in (
            ("artifact_id", self.artifact_id),
            ("provider", self.provider),
            ("model_id", self.model_id),
            ("immutable_revision", self.immutable_revision),
            ("tokenizer_revision", self.tokenizer_revision),
            ("license_id", self.license_id),
        ):
            _require_text(name, value)
        _require_sha256("config_hash", self.config_hash)
        if self.artifact_sha256 is not None:
            _require_sha256("artifact_sha256", self.artifact_sha256)


@dataclass(frozen=True, slots=True)
class PromptArtifact:
    artifact_id: str
    task: ModelTask
    version: str
    template: str
    template_hash: str
    decoding_config_hash: str

    def __post_init__(self) -> None:
        _require_text("artifact_id", self.artifact_id)
        _require_text("version", self.version)
        _require_text("template", self.template)
        _require_sha256("template_hash", self.template_hash)
        _require_sha256("decoding_config_hash", self.decoding_config_hash)
        expected = hashlib.sha256(self.template.encode("utf-8")).hexdigest()
        if self.template_hash != expected:
            raise ValidationError("template_hash does not match the prompt template")


@dataclass(frozen=True, slots=True)
class ChunkDraft:
    chunk_version_id: str
    document_version_id: str
    chunk_index: int
    text: str
    text_hash: str
    chunker_artifact_id: str

    def __post_init__(self) -> None:
        _require_text("chunk_version_id", self.chunk_version_id)
        _require_text("document_version_id", self.document_version_id)
        _require_text("chunker_artifact_id", self.chunker_artifact_id)
        if self.chunk_index < 0:
            raise ValidationError("chunk_index must be nonnegative")
        if not self.text.strip():
            raise ValidationError("empty chunks are forbidden")
        if self.text_hash != normalized_text_hash(self.text):
            raise ValidationError("chunk text_hash violates normalization v1")


@dataclass(frozen=True, slots=True)
class EmbeddingRecord:
    chunk_version_id: str
    model_artifact_id: str
    vector: tuple[float, ...]
    input_hash: str

    def __post_init__(self) -> None:
        _require_text("chunk_version_id", self.chunk_version_id)
        _require_text("model_artifact_id", self.model_artifact_id)
        _require_sha256("input_hash", self.input_hash)
        if not self.vector:
            raise ValidationError("embedding vector must be non-empty")
        if any(not math.isfinite(value) for value in self.vector):
            raise ValidationError("embedding vector values must be finite")


@dataclass(frozen=True, slots=True)
class RetrievalQuery:
    query_kind: QueryKind
    query_id: str
    text: str
    top_k: int
    embedding_model_artifact_id: str
    method_version: str

    def __post_init__(self) -> None:
        _require_text("query_id", self.query_id)
        _require_text("text", self.text)
        _require_text(
            "embedding_model_artifact_id", self.embedding_model_artifact_id
        )
        _require_text("method_version", self.method_version)
        if self.top_k <= 0:
            raise ValidationError("top_k must be positive")


@dataclass(frozen=True, slots=True)
class RetrievalCandidate:
    candidate_id: str
    query_kind: QueryKind
    query_id: str
    chunk_version_id: str
    score: float
    rank: int
    embedding_model_artifact_id: str
    method_version: str

    def __post_init__(self) -> None:
        for name, value in (
            ("candidate_id", self.candidate_id),
            ("query_id", self.query_id),
            ("chunk_version_id", self.chunk_version_id),
            ("embedding_model_artifact_id", self.embedding_model_artifact_id),
            ("method_version", self.method_version),
        ):
            _require_text(name, value)
        if not math.isfinite(self.score):
            raise ValidationError("retrieval score must be finite")
        if self.rank <= 0:
            raise ValidationError("retrieval rank must be one-based and positive")


@dataclass(frozen=True, slots=True)
class EvidencePassage:
    candidate: RetrievalCandidate
    chunk: ChunkDraft

    def __post_init__(self) -> None:
        if self.candidate.chunk_version_id != self.chunk.chunk_version_id:
            raise ValidationError("candidate and evidence chunk identifiers differ")


@dataclass(frozen=True, slots=True)
class CitedAnswer:
    text: str
    cited_chunk_version_ids: tuple[str, ...]
    input_hash: str
    raw_output_hash: str
    repair_count: int = 0

    def __post_init__(self) -> None:
        _require_text("answer text", self.text)
        _require_sha256("input_hash", self.input_hash)
        _require_sha256("raw_output_hash", self.raw_output_hash)
        if self.repair_count not in (0, 1):
            raise ValidationError("generation repair_count must be zero or one")
        if not self.cited_chunk_version_ids:
            raise ValidationError("a cited answer must resolve at least one chunk")
        if len(set(self.cited_chunk_version_ids)) != len(
            self.cited_chunk_version_ids
        ):
            raise ValidationError("answer citations must be unique and ordered")
        for chunk_id in self.cited_chunk_version_ids:
            _require_text("cited chunk id", chunk_id)


@dataclass(frozen=True, slots=True)
class AtomicClaim:
    local_claim_id: str
    text: str
    required: bool
    cited_chunk_version_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _require_text("local_claim_id", self.local_claim_id)
        _require_text("claim text", self.text)
        if len(set(self.cited_chunk_version_ids)) != len(
            self.cited_chunk_version_ids
        ):
            raise ValidationError("claim citations must be unique")


@dataclass(frozen=True, slots=True)
class ClaimExtractionResult:
    claims: tuple[AtomicClaim, ...]
    input_hash: str
    raw_output_hash: str
    repair_count: int = 0

    def __post_init__(self) -> None:
        _require_sha256("extraction input_hash", self.input_hash)
        _require_sha256("extraction raw_output_hash", self.raw_output_hash)
        if not self.claims:
            raise ValidationError("claim extraction must produce at least one claim")
        if not any(claim.required for claim in self.claims):
            raise ValidationError("claim extraction requires a required claim")
        local_ids = tuple(claim.local_claim_id for claim in self.claims)
        if len(set(local_ids)) != len(local_ids):
            raise ValidationError("extracted local claim identifiers must be unique")
        if self.repair_count not in (0, 1):
            raise ValidationError("extraction repair_count must be zero or one")


@dataclass(frozen=True, slots=True)
class ScoreTriple:
    support: float
    refute: float
    neutral: float

    def __post_init__(self) -> None:
        scores = (self.support, self.refute, self.neutral)
        if any(not math.isfinite(score) or not 0.0 <= score <= 1.0 for score in scores):
            raise ValidationError("verification scores must be finite in [0, 1]")
        if not math.isclose(sum(scores), 1.0, rel_tol=1e-6, abs_tol=1e-6):
            raise ValidationError("verification scores must form a normalized triple")


@dataclass(frozen=True, slots=True)
class VerificationResult:
    claim_id: str
    chunk_version_id: str
    candidate_id: str
    model_artifact_id: str
    prompt_artifact_id: str
    calibration_version: str
    temperature: float
    scores: ScoreTriple
    input_hash: str
    raw_output_hash: str
    raw_logits: tuple[float, float, float] | None = None

    def __post_init__(self) -> None:
        _require_text("claim_id", self.claim_id)
        _require_text("chunk_version_id", self.chunk_version_id)
        _require_text("candidate_id", self.candidate_id)
        _require_text("model_artifact_id", self.model_artifact_id)
        _require_text("prompt_artifact_id", self.prompt_artifact_id)
        _require_text("calibration_version", self.calibration_version)
        if not math.isfinite(self.temperature) or self.temperature <= 0:
            raise ValidationError("verifier temperature must be finite and positive")
        _require_sha256("input_hash", self.input_hash)
        _require_sha256("raw_output_hash", self.raw_output_hash)
        if self.raw_logits is not None and any(
            not math.isfinite(logit) for logit in self.raw_logits
        ):
            raise ValidationError("raw verifier logits must be finite")


@dataclass(frozen=True, slots=True)
class ComponentTiming:
    component: str
    elapsed_ms: float
    cold_start: bool

    def __post_init__(self) -> None:
        _require_text("component", self.component)
        if not math.isfinite(self.elapsed_ms) or self.elapsed_ms < 0:
            raise ValidationError("component timing must be finite and nonnegative")


@dataclass(frozen=True, slots=True)
class PipelineRunManifest:
    schema_version: str
    run_id: str
    status: PipelineRunStatus
    config_hash: str
    input_hash: str
    corpus_hash: str
    question_id: str
    decision_policy_version: str
    answer_version_id: str | None
    semantic_epoch_id: int | None
    confirmed_as_of_epoch: int | None
    model_artifact_ids: tuple[str, ...]
    prompt_artifact_ids: tuple[str, ...]
    chunk_version_ids: tuple[str, ...]
    chunk_text_hashes: tuple[tuple[str, str], ...]
    retrieval_candidates: tuple[RetrievalCandidate, ...]
    answer: CitedAnswer | None
    extraction: ClaimExtractionResult | None
    claims: tuple[AtomicClaim, ...]
    verifications: tuple[VerificationResult, ...]
    claim_states: tuple[ClaimState, ...]
    answer_states: tuple[AnswerState, ...]
    timings: tuple[ComponentTiming, ...]
    reused_artifact_ids: tuple[str, ...]
    new_artifact_ids: tuple[str, ...]
    failure_code: str | None = None

    def __post_init__(self) -> None:
        for name, value in (
            ("schema_version", self.schema_version),
            ("run_id", self.run_id),
            ("question_id", self.question_id),
            ("decision_policy_version", self.decision_policy_version),
        ):
            _require_text(name, value)
        for name, value in (
            ("config_hash", self.config_hash),
            ("input_hash", self.input_hash),
            ("corpus_hash", self.corpus_hash),
        ):
            _require_sha256(name, value)
        if self.status is PipelineRunStatus.PUBLISHED:
            if self.answer_version_id is None or self.answer is None:
                raise ValidationError("published runs require an answer")
            if not self.claims:
                raise ValidationError("published runs require at least one claim")
            if self.extraction is None or self.extraction.claims != self.claims:
                raise ValidationError(
                    "published runs require matching extraction provenance"
                )
            if not self.claim_states or not self.answer_states:
                raise ValidationError("published runs require structured states")
            if self.semantic_epoch_id is None or self.confirmed_as_of_epoch is None:
                raise ValidationError(
                    "published runs require confirmed epoch provenance"
                )
            if self.failure_code is not None:
                raise ValidationError("published runs cannot carry a failure code")
        if self.status is PipelineRunStatus.FAILED and not self.failure_code:
            raise ValidationError("failed runs require a failure code")
        if set(self.reused_artifact_ids) & set(self.new_artifact_ids):
            raise ValidationError("an artifact cannot be both reused and new")
        if tuple(chunk_id for chunk_id, _ in self.chunk_text_hashes) != (
            self.chunk_version_ids
        ):
            raise ValidationError("chunk hashes must align with chunk-version order")
        for _, text_hash in self.chunk_text_hashes:
            _require_sha256("chunk text_hash", text_hash)


class Chunker(Protocol):
    artifact_id: str

    def chunk(self, document_version_id: str, text: str) -> tuple[ChunkDraft, ...]: ...


class Embedder(Protocol):
    model_artifact: ModelArtifact
    dimension: int

    def embed(self, chunks: tuple[ChunkDraft, ...]) -> tuple[EmbeddingRecord, ...]: ...


class Retriever(Protocol):
    def retrieve(self, query: RetrievalQuery) -> tuple[RetrievalCandidate, ...]: ...


class AnswerGenerator(Protocol):
    model_artifact: ModelArtifact
    prompt_artifact: PromptArtifact

    def generate(
        self,
        question: str,
        evidence: tuple[EvidencePassage, ...],
    ) -> CitedAnswer: ...


class ClaimExtractor(Protocol):
    model_artifact: ModelArtifact
    prompt_artifact: PromptArtifact

    def extract(
        self,
        answer: CitedAnswer,
        evidence: tuple[EvidencePassage, ...],
    ) -> ClaimExtractionResult: ...


class EvidenceVerifier(Protocol):
    model_artifact: ModelArtifact
    prompt_artifact: PromptArtifact
    calibration_version: str
    temperature: float

    def verify(self, claim: AtomicClaim, chunk: ChunkDraft) -> VerificationResult: ...


class ModelRegistry(Protocol):
    def register(self, artifact: ModelArtifact) -> bool: ...

    def get(self, artifact_id: str) -> ModelArtifact: ...


class PromptRegistry(Protocol):
    def register(self, artifact: PromptArtifact) -> bool: ...

    def get(self, artifact_id: str) -> PromptArtifact: ...


class ArtifactStore(Protocol):
    """Publication boundary; implementations must make publish atomic."""

    def stage(self, manifest: PipelineRunManifest) -> None: ...

    def publish(self, manifest: PipelineRunManifest) -> None: ...

    def fail(self, manifest: PipelineRunManifest) -> None: ...
