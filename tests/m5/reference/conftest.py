"""Deterministic builders for the independent M5 reference tests."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable

import pytest

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

STAMP = ModelStamp(model_id="fixture", model_version="v1", prompt_version="p1")
SUPPORT = (0.9, 0.05, 0.05)
REFUTE = (0.05, 0.9, 0.05)
NEUTRAL = (0.05, 0.05, 0.9)


def sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def make_base(*, claim_ids: tuple[str, ...] = ("c1",)) -> InMemoryRepository:
    base = InMemoryRepository()
    apply_event(
        base,
        PolicyChangeEvent(
            event_id="base-policy",
            policy=DecisionPolicy(
                policy_version="policy-v1",
                support_threshold=0.8,
                refute_threshold=0.8,
            ),
        ),
    )
    base.register_question(Question(question_id="q1", text="question"))
    base.register_answer(
        AnswerVersion(
            answer_version_id="a1",
            question_id="q1",
            text="answer",
            producer=STAMP,
        ),
        tuple(
            Claim(
                claim_id=claim_id,
                answer_version_id="a1",
                text=f"claim {claim_id}",
                extractor=STAMP,
                required=True,
            )
            for claim_id in claim_ids
        ),
    )
    return base


def add_document(
    base: InMemoryRepository,
    *,
    event_id: str,
    document_id: str,
    version_id: str,
    chunks: Iterable[tuple[str, str]],
) -> None:
    apply_event(
        base,
        InsertDocumentEvent(
            event_id=event_id,
            document_id=document_id,
            document_version_id=version_id,
            content_hash=sha(f"content:{version_id}"),
            chunks=tuple(
                ChunkInput(chunk_version_id=chunk_id, chunk_index=index, text=text)
                for index, (chunk_id, text) in enumerate(chunks)
            ),
        ),
    )


def make_group(
    *,
    group_id: str = "g1",
    family_id: str = "f1",
    claim_id: str = "c1",
    texts: tuple[str, ...] = ("requirement one", "requirement two"),
    requirement_ids: tuple[str, ...] | None = None,
    predecessors: tuple[str | None, ...] | None = None,
    supersedes_group_id: str | None = None,
    construction_kind: ConstructionKind = ConstructionKind.CONTROLLED,
    source_id: str = "fixture-source",
    model_triple: tuple[str, str, str] | None = None,
) -> EvidenceGroupVersion:
    ids = requirement_ids or tuple(f"{group_id}-r{i}" for i in range(len(texts)))
    prior = predecessors or (None,) * len(texts)
    if len(ids) != len(texts) or len(prior) != len(texts):
        raise ValueError("fixture identifiers and predecessors must align with texts")
    model_id, model_version, prompt_version = (
        model_triple if model_triple is not None else (None, None, None)
    )
    requirements = tuple(
        EvidenceRequirementVersion(
            requirement_version_id=requirement_id,
            group_version_id=group_id,
            ordinal=ordinal,
            requirement_text=text,
            constructor_model_id=model_id,
            constructor_model_version=model_version,
            constructor_prompt_version=prompt_version,
            supersedes_requirement_version_id=predecessor,
        )
        for ordinal, (requirement_id, text, predecessor) in enumerate(
            zip(ids, texts, prior, strict=True)
        )
    )
    return EvidenceGroupVersion(
        group_version_id=group_id,
        group_family_id=family_id,
        owner_claim_id=claim_id,
        requirements=requirements,
        construction_kind=construction_kind,
        construction_source_id=source_id,
        constructor_model_id=model_id,
        constructor_model_version=model_version,
        constructor_prompt_version=prompt_version,
        supersedes_group_version_id=supersedes_group_id,
    )


def make_requirement_observation(
    *,
    observation_id: str,
    requirement_id: str,
    chunk_id: str,
    scores: tuple[float, float, float] = SUPPORT,
    task_type: str = "verify_requirement_v1",
) -> SemanticObservation:
    return SemanticObservation(
        observation_id=observation_id,
        subject_kind=SubjectKind.REQUIREMENT,
        subject_id=requirement_id,
        chunk_version_id=chunk_id,
        task_type=task_type,
        support_score=scores[0],
        refute_score=scores[1],
        neutral_score=scores[2],
        producer=STAMP,
        input_hash=sha(f"input:{observation_id}"),
    )


@pytest.fixture
def m5_repo() -> M5Repository:
    base = make_base()
    add_document(
        base,
        event_id="base-document",
        document_id="doc1",
        version_id="dv1",
        chunks=(("h1", "alpha"), ("h2", "beta"), ("h3", "gamma")),
    )
    return M5Repository(base)
