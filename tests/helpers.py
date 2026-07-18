"""Shared deterministic builders for M1 tests.

All corpus, observation, and policy mutations go through the event layer so
every test also exercises idempotence bookkeeping.
"""

from groundloop.domain import (
    AnswerVersion,
    Claim,
    DecisionPolicy,
    ModelStamp,
    Question,
    SemanticObservation,
    StatusDelta,
    SubjectKind,
)
from groundloop.events import (
    ChunkInput,
    InsertDocumentEvent,
    ObserveEvent,
    PolicyChangeEvent,
    apply_event,
)
from groundloop.repository import InMemoryRepository

STAMP = ModelStamp(model_id="manual", model_version="v1", prompt_version="p1")

SUPPORT_SCORES = (0.9, 0.05, 0.05)
REFUTE_SCORES = (0.05, 0.9, 0.05)
NEUTRAL_SCORES = (0.1, 0.1, 0.8)


def make_repo(
    support_threshold: float = 0.8, refute_threshold: float = 0.8
) -> InMemoryRepository:
    """Repository with an initial decision policy activated via an event."""
    repo = InMemoryRepository()
    apply_event(
        repo,
        PolicyChangeEvent(
            event_id="ev-policy-initial",
            policy=DecisionPolicy(
                policy_version="k1",
                support_threshold=support_threshold,
                refute_threshold=refute_threshold,
            ),
        ),
    )
    return repo


def register_answer(
    repo: InMemoryRepository,
    answer_id: str = "a1",
    claims: tuple[tuple[str, bool], ...] = (("c1", True),),
) -> None:
    question_id = f"q-{answer_id}"
    repo.register_question(Question(question_id=question_id, text="question"))
    claim_records = tuple(
        Claim(
            claim_id=claim_id,
            answer_version_id=answer_id,
            text=f"claim {claim_id}",
            extractor=STAMP,
            required=required,
        )
        for claim_id, required in claims
    )
    repo.register_answer(
        AnswerVersion(
            answer_version_id=answer_id,
            question_id=question_id,
            text="answer",
            producer=STAMP,
        ),
        claim_records,
    )


def insert_document(
    repo: InMemoryRepository,
    event_id: str,
    document_id: str,
    document_version_id: str,
    chunks: tuple[tuple[str, str], ...],
) -> tuple[StatusDelta, ...]:
    """Insert a document version; chunks are (chunk_version_id, text)."""
    return apply_event(
        repo,
        InsertDocumentEvent(
            event_id=event_id,
            document_id=document_id,
            document_version_id=document_version_id,
            content_hash=f"hash-{document_version_id}",
            chunks=tuple(
                ChunkInput(chunk_version_id=cid, chunk_index=i, text=text)
                for i, (cid, text) in enumerate(chunks)
            ),
        ),
    )


def make_observation(
    observation_id: str,
    claim_id: str,
    chunk_version_id: str,
    scores: tuple[float, float, float],
    task_type: str = "verify",
) -> SemanticObservation:
    return SemanticObservation(
        observation_id=observation_id,
        subject_kind=SubjectKind.CLAIM,
        subject_id=claim_id,
        chunk_version_id=chunk_version_id,
        task_type=task_type,
        support_score=scores[0],
        refute_score=scores[1],
        neutral_score=scores[2],
        producer=STAMP,
        input_hash=f"input-{observation_id}",
    )


def observe(
    repo: InMemoryRepository,
    event_id: str,
    observation_id: str,
    claim_id: str,
    chunk_version_id: str,
    scores: tuple[float, float, float],
    task_type: str = "verify",
) -> tuple[StatusDelta, ...]:
    return apply_event(
        repo,
        ObserveEvent(
            event_id=event_id,
            observation=make_observation(
                observation_id, claim_id, chunk_version_id, scores, task_type
            ),
        ),
    )
