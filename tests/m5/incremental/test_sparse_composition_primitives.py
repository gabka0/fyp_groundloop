from __future__ import annotations

import hashlib
from copy import deepcopy

import pytest

from groundloop.domain import (
    AnswerVersion,
    Claim,
    ClaimStatus,
    DecisionPolicy,
    ModelStamp,
    Question,
    SemanticObservation,
    SubjectKind,
)
from groundloop.events import (
    ChunkInput,
    InsertDocumentEvent,
    ObserveEvent,
    PolicyChangeEvent,
    apply_event,
)
from groundloop.incremental import IncrementalMaintenanceEngine
from groundloop.repository import InMemoryRepository

STAMP = ModelStamp("fixture-model", "v1", "prompt-v1")


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _repository() -> InMemoryRepository:
    repo = InMemoryRepository()
    apply_event(
        repo,
        PolicyChangeEvent(
            event_id="policy",
            policy=DecisionPolicy("policy-v1", 0.8, 0.8),
        ),
    )
    repo.register_question(Question("question", "Question?"))
    repo.register_answer(
        AnswerVersion("answer", "question", "Answer.", STAMP),
        (
            Claim("claim-a", "answer", "Claim A", STAMP, True),
            Claim("claim-b", "answer", "Claim B", STAMP, True),
        ),
    )
    apply_event(
        repo,
        InsertDocumentEvent(
            event_id="document",
            document_id="document",
            document_version_id="document-v1",
            content_hash=_sha("document-v1"),
            chunks=(ChunkInput("chunk", 0, "supporting text"),),
        ),
    )
    return repo


def _support_event() -> ObserveEvent:
    return ObserveEvent(
        event_id="observe-support",
        observation=SemanticObservation(
            observation_id="observation-support",
            subject_kind=SubjectKind.CLAIM,
            subject_id="claim-a",
            chunk_version_id="chunk",
            task_type="verify_claim_v1",
            support_score=0.95,
            refute_score=0.02,
            neutral_score=0.03,
            producer=STAMP,
            input_hash=_sha("observation-input"),
        ),
    )


def test_direct_patch_preview_and_m5_noop_are_sparse_and_atomic() -> None:
    before = _repository()
    engine = IncrementalMaintenanceEngine.from_repository(before)
    after = deepcopy(before)
    event = _support_event()
    apply_event(after, event)
    patch = engine.prepare_committed_event_patch(event, before, after)
    original_claims = engine.claim_states
    original_answers = engine.answer_states

    preview = engine.preview_claim_states_after_patch(
        patch,
        ("claim-a", "claim-b", "claim-a"),
    )

    assert tuple(preview) == ("claim-a", "claim-b")
    assert preview["claim-a"].status is ClaimStatus.SUPPORTED
    assert preview["claim-b"] == original_claims["claim-b"]
    assert engine.claim_states == original_claims
    assert engine.answer_states == original_answers

    engine.apply_state_patch(patch)
    assert engine.claim_state("claim-a") == preview["claim-a"]
    claims_after_direct = engine.claim_states
    answers_after_direct = engine.answer_states

    noop = engine.prepare_noop_event_patch("m5-only-event")
    assert noop.touched_claim_ids == ()
    assert noop.touched_answer_ids == ()
    assert (
        engine.preview_claim_states_after_patch(noop, ("claim-a", "claim-b"))
        == claims_after_direct
    )

    def reject_before_publish(phase: str) -> None:
        if phase == "published":
            raise RuntimeError("reject before global publish")

    with pytest.raises(RuntimeError, match="reject before global publish"):
        engine.apply_state_patch(
            noop,
            failure_injector=reject_before_publish,
        )
    assert engine.claim_states == claims_after_direct
    assert engine.answer_states == answers_after_direct

    # The failed application restored the exact revision precondition, so the
    # same prepared no-op patch can still be committed once.
    engine.apply_state_patch(noop)
    assert engine.claim_states == claims_after_direct
    assert engine.answer_states == answers_after_direct
    with pytest.raises(AssertionError, match="stale|already applied"):
        engine.apply_state_patch(noop)
