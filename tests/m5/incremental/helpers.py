from __future__ import annotations

import hashlib

from groundloop.domain import (
    AnswerVersion,
    Claim,
    DecisionPolicy,
    ModelStamp,
    Question,
    SemanticObservation,
    SubjectKind,
)
from groundloop.events import (
    ChunkInput,
    InsertDocumentEvent,
    PolicyChangeEvent,
    apply_event,
)
from groundloop.m5.domain import (
    ConstructionKind,
    EvidenceGroupVersion,
    EvidenceRequirementVersion,
)
from groundloop.m5.repository import M5Repository
from groundloop.repository import InMemoryRepository

STAMP = ModelStamp("overlay-fixture", "v1", "prompt-v1")


def sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def make_repository(*, claim_ids: tuple[str, ...] = ("claim-a",)) -> M5Repository:
    base = InMemoryRepository()
    apply_event(
        base,
        PolicyChangeEvent(
            event_id="base-policy",
            policy=DecisionPolicy("policy-v1", 0.8, 0.8),
        ),
    )
    base.register_question(Question("question-a", "Question?"))
    base.register_answer(
        AnswerVersion("answer-a", "question-a", "Answer.", STAMP),
        tuple(
            Claim(claim_id, "answer-a", f"Claim {claim_id}.", STAMP, True)
            for claim_id in claim_ids
        ),
    )
    apply_event(
        base,
        InsertDocumentEvent(
            event_id="base-document",
            document_id="document-a",
            document_version_id="document-version-a",
            content_hash=sha("document-version-a"),
            chunks=(
                ChunkInput("chunk-a", 0, "alpha evidence"),
                ChunkInput("chunk-b", 1, "beta evidence"),
                ChunkInput("chunk-c", 2, "gamma evidence"),
            ),
        ),
    )
    return M5Repository(base)


def make_group(
    *,
    group_id: str = "group-a",
    family_id: str = "family-a",
    claim_id: str = "claim-a",
    texts: tuple[str, ...] = ("requirement one",),
    requirement_ids: tuple[str, ...] | None = None,
    predecessors: tuple[str | None, ...] | None = None,
    supersedes_group_id: str | None = None,
) -> EvidenceGroupVersion:
    identifiers = requirement_ids or tuple(
        f"{group_id}-requirement-{index}" for index in range(len(texts))
    )
    prior = predecessors or (None,) * len(texts)
    if len(identifiers) != len(texts) or len(prior) != len(texts):
        raise ValueError("fixture requirement fields must align")
    requirements = tuple(
        EvidenceRequirementVersion(
            requirement_version_id=requirement_id,
            group_version_id=group_id,
            ordinal=ordinal,
            requirement_text=text,
            supersedes_requirement_version_id=predecessor,
        )
        for ordinal, (requirement_id, text, predecessor) in enumerate(
            zip(identifiers, texts, prior, strict=True)
        )
    )
    return EvidenceGroupVersion(
        group_version_id=group_id,
        group_family_id=family_id,
        owner_claim_id=claim_id,
        requirements=requirements,
        construction_kind=ConstructionKind.CONTROLLED,
        construction_source_id="overlay-fixture",
        supersedes_group_version_id=supersedes_group_id,
    )


def make_requirement_observation(
    *,
    observation_id: str,
    requirement_id: str,
    chunk_id: str = "chunk-a",
    scores: tuple[float, float, float] = (0.9, 0.05, 0.05),
) -> SemanticObservation:
    return SemanticObservation(
        observation_id=observation_id,
        subject_kind=SubjectKind.REQUIREMENT,
        subject_id=requirement_id,
        chunk_version_id=chunk_id,
        task_type="verify_requirement_v1",
        support_score=scores[0],
        refute_score=scores[1],
        neutral_score=scores[2],
        producer=STAMP,
        input_hash=sha(f"input:{observation_id}"),
    )


def make_claim_observation(
    *,
    observation_id: str,
    claim_id: str = "claim-a",
    chunk_id: str = "chunk-a",
    scores: tuple[float, float, float] = (0.9, 0.05, 0.05),
) -> SemanticObservation:
    return SemanticObservation(
        observation_id=observation_id,
        subject_kind=SubjectKind.CLAIM,
        subject_id=claim_id,
        chunk_version_id=chunk_id,
        task_type="verify_claim_v1",
        support_score=scores[0],
        refute_score=scores[1],
        neutral_score=scores[2],
        producer=STAMP,
        input_hash=sha(f"input:{observation_id}"),
    )
