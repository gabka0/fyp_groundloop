"""Coordinator-owned translation from empirical M3 outputs to exact M2 state."""

from __future__ import annotations

from dataclasses import dataclass

from groundloop.ai.contracts import (
    AtomicClaim,
    ChunkDraft,
    ModelArtifact,
    PromptArtifact,
    VerificationResult,
    stable_digest,
)
from groundloop.differential import DifferentialRunner
from groundloop.domain import (
    AnswerState,
    AnswerVersion,
    Claim,
    ClaimState,
    DecisionPolicy,
    ModelStamp,
    Question,
    SemanticObservation,
    SubjectKind,
)
from groundloop.errors import ValidationError
from groundloop.events import (
    ChunkInput,
    InsertDocumentEvent,
    ObserveEvent,
    PolicyChangeEvent,
    apply_event,
)
from groundloop.incremental import ClaimCertificate
from groundloop.reference import compute_all_states
from groundloop.repository import InMemoryRepository


@dataclass(frozen=True, slots=True)
class CorpusDocument:
    document_id: str
    document_version_id: str
    source_uri: str
    content_hash: str
    chunks: tuple[ChunkDraft, ...]

    def __post_init__(self) -> None:
        if not self.document_id or not self.document_version_id:
            raise ValidationError("document identities must be nonempty")
        if not self.source_uri or not self.content_hash:
            raise ValidationError("document source and content hash must be nonempty")
        if not self.chunks:
            raise ValidationError("M3 corpus documents must contain a chunk")
        for chunk in self.chunks:
            if chunk.document_version_id != self.document_version_id:
                raise ValidationError("chunk/document-version identity mismatch")


@dataclass(frozen=True, slots=True)
class StructuredGroundingResult:
    question: Question
    answer: AnswerVersion
    claims: tuple[Claim, ...]
    observations: tuple[SemanticObservation, ...]
    verification_publications: tuple[VerificationPublication, ...]
    claim_states: tuple[ClaimState, ...]
    answer_states: tuple[AnswerState, ...]
    certificates: tuple[ClaimCertificate, ...]
    repository: InMemoryRepository
    runner: DifferentialRunner


@dataclass(frozen=True, slots=True)
class VerificationPublication:
    observation: SemanticObservation
    result: VerificationResult


def _model_stamp(model: ModelArtifact, prompt: PromptArtifact) -> ModelStamp:
    return ModelStamp(
        model_id=model.model_id,
        model_version=model.immutable_revision,
        prompt_version=f"{prompt.version}:{prompt.template_hash}",
    )


def _claim_id(answer_version_id: str, claim: AtomicClaim) -> str:
    return "claim-" + stable_digest(
        "m3-claim-v1",
        answer_version_id,
        claim.local_claim_id,
        claim.text,
    )


def _observation_id(claim_id: str, result: VerificationResult) -> str:
    return "observation-" + stable_digest(
        "m3-observation-v1",
        claim_id,
        result.chunk_version_id,
        "direct_verification",
        result.model_artifact_id,
        result.prompt_artifact_id,
        result.calibration_version,
        result.input_hash,
    )


def build_structured_grounding(
    *,
    question: Question,
    answer_version_id: str,
    answer_text: str,
    atomic_claims: tuple[AtomicClaim, ...],
    documents: tuple[CorpusDocument, ...],
    verifications: tuple[VerificationResult, ...],
    generation_model: ModelArtifact,
    generation_prompt: PromptArtifact,
    extraction_model: ModelArtifact,
    extraction_prompt: PromptArtifact,
    verifier_models: dict[str, ModelArtifact],
    verifier_prompts: dict[str, PromptArtifact],
    policy: DecisionPolicy,
) -> StructuredGroundingResult:
    """Validate AI products and enter them into both exact Python engines.

    This function does not infer truth. It proves only that the incremental
    engine equals full recomputation over the supplied immutable scores.
    """
    if not atomic_claims or not any(claim.required for claim in atomic_claims):
        raise ValidationError("a structured answer requires a required claim")
    local_ids = tuple(claim.local_claim_id for claim in atomic_claims)
    if len(set(local_ids)) != len(local_ids):
        raise ValidationError("local claim identifiers must be unique")

    chunks = {
        chunk.chunk_version_id: chunk
        for document in documents
        for chunk in document.chunks
    }
    expected_chunk_count = sum(len(document.chunks) for document in documents)
    if len(chunks) != expected_chunk_count:
        raise ValidationError("chunk-version identifiers must be globally unique")

    answer = AnswerVersion(
        answer_version_id=answer_version_id,
        question_id=question.question_id,
        text=answer_text,
        producer=_model_stamp(generation_model, generation_prompt),
    )
    claim_by_local: dict[str, Claim] = {}
    claims: list[Claim] = []
    extraction_stamp = _model_stamp(extraction_model, extraction_prompt)
    for atomic in atomic_claims:
        for chunk_id in atomic.cited_chunk_version_ids:
            if chunk_id not in chunks:
                raise ValidationError(
                    f"claim {atomic.local_claim_id} cites unknown chunk {chunk_id}"
                )
        claim = Claim(
            claim_id=_claim_id(answer_version_id, atomic),
            answer_version_id=answer_version_id,
            text=atomic.text,
            extractor=extraction_stamp,
            required=atomic.required,
        )
        claim_by_local[atomic.local_claim_id] = claim
        claims.append(claim)

    verification_count = {local_id: 0 for local_id in local_ids}
    observations: list[SemanticObservation] = []
    verification_publications: list[VerificationPublication] = []
    seen_pairs: set[tuple[str, str]] = set()
    for result in verifications:
        resolved_claim = claim_by_local.get(result.claim_id)
        if resolved_claim is None:
            raise ValidationError(
                f"verification references unknown local claim {result.claim_id}"
            )
        if result.chunk_version_id not in chunks:
            raise ValidationError(
                f"verification references unknown chunk {result.chunk_version_id}"
            )
        pair = (result.claim_id, result.chunk_version_id)
        if pair in seen_pairs:
            raise ValidationError("duplicate claim/chunk verification result")
        seen_pairs.add(pair)
        verification_count[result.claim_id] += 1
        try:
            model = verifier_models[result.model_artifact_id]
            prompt = verifier_prompts[result.prompt_artifact_id]
        except KeyError as exc:
            raise ValidationError(
                "verification references an unregistered model or prompt"
            ) from exc
        observation = SemanticObservation(
            observation_id=_observation_id(resolved_claim.claim_id, result),
            subject_kind=SubjectKind.CLAIM,
            subject_id=resolved_claim.claim_id,
            chunk_version_id=result.chunk_version_id,
            task_type="direct_verification",
            support_score=result.scores.support,
            refute_score=result.scores.refute,
            neutral_score=result.scores.neutral,
            producer=_model_stamp(model, prompt),
            input_hash=result.input_hash,
        )
        observations.append(observation)
        verification_publications.append(VerificationPublication(observation, result))
    missing = sorted(
        local_id for local_id, count in verification_count.items() if count == 0
    )
    if missing:
        raise ValidationError(f"claims lack verifier results: {', '.join(missing)}")

    repository = InMemoryRepository()
    apply_event(
        repository,
        PolicyChangeEvent(
            event_id="policy-event-" + stable_digest(policy.policy_version),
            policy=policy,
        ),
    )
    repository.register_question(question)
    repository.register_answer(answer, tuple(claims))
    for document in sorted(documents, key=lambda item: item.document_version_id):
        apply_event(
            repository,
            InsertDocumentEvent(
                event_id="document-event-"
                + stable_digest(document.document_version_id),
                document_id=document.document_id,
                document_version_id=document.document_version_id,
                content_hash=document.content_hash,
                chunks=tuple(
                    ChunkInput(
                        chunk_version_id=chunk.chunk_version_id,
                        chunk_index=chunk.chunk_index,
                        text=chunk.text,
                    )
                    for chunk in document.chunks
                ),
            ),
        )
    runner = DifferentialRunner.from_repository(repository)
    for observation in sorted(observations, key=lambda item: item.observation_id):
        runner.apply(
            ObserveEvent(
                event_id="observation-event-"
                + stable_digest(observation.observation_id),
                observation=observation,
            )
        )
    runner.assert_equivalent()
    reference_claims, reference_answers = compute_all_states(runner.repository)
    return StructuredGroundingResult(
        question=question,
        answer=answer,
        claims=tuple(claims),
        observations=tuple(sorted(observations, key=lambda item: item.observation_id)),
        verification_publications=tuple(
            sorted(
                verification_publications,
                key=lambda item: item.observation.observation_id,
            )
        ),
        claim_states=tuple(reference_claims[key] for key in sorted(reference_claims)),
        answer_states=tuple(
            reference_answers[key] for key in sorted(reference_answers)
        ),
        certificates=tuple(
            runner.engine.certificates[key]
            for key in sorted(runner.engine.certificates)
        ),
        repository=runner.repository,
        runner=runner,
    )
