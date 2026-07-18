"""Repository invariants: identity, references, currency, history."""

import pytest
from helpers import (
    STAMP,
    SUPPORT_SCORES,
    insert_document,
    make_observation,
    make_repo,
    observe,
    register_answer,
)

from groundloop.domain import (
    AnswerVersion,
    Claim,
    DocumentVersion,
    Question,
    SemanticObservation,
    SubjectKind,
)
from groundloop.errors import (
    DanglingReferenceError,
    DuplicateIdentifierError,
    ValidationError,
)
from groundloop.reference import compute_all_states


def test_duplicate_identifiers_fail_explicitly() -> None:
    repo = make_repo()
    register_answer(repo)
    with pytest.raises(DuplicateIdentifierError):
        repo.register_question(Question(question_id="q-a1", text="again"))
    insert_document(repo, "ev-1", "doc", "dv1", (("p1", "a"),))
    with pytest.raises(DuplicateIdentifierError):
        repo.register_document_version(
            DocumentVersion(
                document_version_id="dv1", document_id="doc2", content_hash="h"
            ),
            (),
            repo.advance_epoch(),
        )
    observe(repo, "ev-2", "o1", "c1", "p1", SUPPORT_SCORES)
    with pytest.raises(DuplicateIdentifierError):
        repo.register_observation(make_observation("o1", "c1", "p1", SUPPORT_SCORES))


def test_dangling_references_fail_explicitly() -> None:
    repo = make_repo()
    register_answer(repo)
    insert_document(repo, "ev-1", "doc", "dv1", (("p1", "a"),))
    with pytest.raises(DanglingReferenceError):
        repo.register_observation(
            make_observation("o1", "missing-claim", "p1", SUPPORT_SCORES)
        )
    with pytest.raises(DanglingReferenceError):
        repo.register_observation(
            make_observation("o2", "c1", "missing-chunk", SUPPORT_SCORES)
        )
    with pytest.raises(DanglingReferenceError):
        repo.register_answer(
            AnswerVersion(
                answer_version_id="a2",
                question_id="missing-question",
                text="t",
                producer=STAMP,
            ),
            (
                Claim(
                    claim_id="c9",
                    answer_version_id="a2",
                    text="t",
                    extractor=STAMP,
                    required=True,
                ),
            ),
        )


def test_requirement_subject_observations_are_rejected_in_m1() -> None:
    repo = make_repo()
    register_answer(repo)
    insert_document(repo, "ev-1", "doc", "dv1", (("p1", "a"),))
    observation = SemanticObservation(
        observation_id="o1",
        subject_kind=SubjectKind.REQUIREMENT,
        subject_id="r1",
        chunk_version_id="p1",
        task_type="verify",
        support_score=0.9,
        refute_score=0.0,
        neutral_score=0.0,
        producer=STAMP,
        input_hash="ih",
    )
    with pytest.raises(DanglingReferenceError):
        repo.register_observation(observation)


def test_answer_with_zero_required_claims_is_rejected() -> None:
    repo = make_repo()
    repo.register_question(Question(question_id="q1", text="q"))
    with pytest.raises(ValidationError):
        repo.register_answer(
            AnswerVersion(
                answer_version_id="a1", question_id="q1", text="t", producer=STAMP
            ),
            (
                Claim(
                    claim_id="c1",
                    answer_version_id="a1",
                    text="t",
                    extractor=STAMP,
                    required=False,
                ),
            ),
        )


def test_duplicate_claim_ids_within_one_registration_are_rejected_atomically() -> None:
    repo = make_repo()
    repo.register_question(Question(question_id="q1", text="q"))
    answer = AnswerVersion(
        answer_version_id="a1", question_id="q1", text="t", producer=STAMP
    )
    duplicate_claims = (
        Claim(
            claim_id="c1",
            answer_version_id="a1",
            text="first",
            extractor=STAMP,
            required=True,
        ),
        Claim(
            claim_id="c1",
            answer_version_id="a1",
            text="second",
            extractor=STAMP,
            required=True,
        ),
    )

    with pytest.raises(DuplicateIdentifierError):
        repo.register_answer(answer, duplicate_claims)

    with pytest.raises(DanglingReferenceError):
        repo.answer("a1")
    with pytest.raises(DanglingReferenceError):
        repo.claim("c1")


def test_second_active_version_requires_replace() -> None:
    repo = make_repo()
    insert_document(repo, "ev-1", "doc", "dv1", (("p1", "a"),))
    with pytest.raises(ValidationError):
        repo.register_document_version(
            DocumentVersion(
                document_version_id="dv2", document_id="doc", content_hash="h"
            ),
            (),
            repo.advance_epoch(),
        )


def test_deactivating_inactive_version_fails() -> None:
    repo = make_repo()
    insert_document(repo, "ev-1", "doc", "dv1", (("p1", "a"),))
    repo.deactivate_document_version("dv1", repo.advance_epoch())
    with pytest.raises(ValidationError):
        repo.deactivate_document_version("dv1", repo.advance_epoch())


def test_currency_supersession_keeps_history() -> None:
    repo = make_repo()
    register_answer(repo)
    insert_document(repo, "ev-1", "doc", "dv1", (("p1", "a"),))
    observe(repo, "ev-2", "o1", "c1", "p1", SUPPORT_SCORES)
    observe(repo, "ev-3", "o2", "c1", "p1", SUPPORT_SCORES)

    assert repo.is_observation_superseded("o1")
    assert not repo.is_observation_superseded("o2")
    assert repo.observation("o1").observation_id == "o1"  # still queryable
    assert repo.observations_for_chunk("p1") == ("o1", "o2")
    current = [o.observation_id for o in repo.current_observations()]
    assert current == ["o2"]


def test_public_snapshot_export_is_deterministic_and_detached() -> None:
    repo = make_repo()
    register_answer(repo)
    insert_document(repo, "ev-1", "doc", "dv1", (("p1", "a"),))
    observe(repo, "ev-2", "o1", "c1", "p1", SUPPORT_SCORES)

    snapshot = repo.export_snapshot()
    assert snapshot.revision == repo.current_epoch
    assert tuple(item.claim_id for item in snapshot.claims) == ("c1",)
    assert tuple(item.observation_id for item in snapshot.observations) == ("o1",)
    assert snapshot.current_observation_ids == ("o1",)
    assert tuple(event_id for event_id, _, _ in snapshot.processed_events) == (
        "ev-policy-initial",
        "ev-1",
        "ev-2",
    )

    observe(repo, "ev-3", "o2", "c1", "p1", SUPPORT_SCORES)
    assert snapshot.revision == 3
    assert snapshot.current_observation_ids == ("o1",)
    assert tuple(item.observation_id for item in snapshot.observations) == ("o1",)


def test_observation_for_inactive_chunk_is_stored_but_inert() -> None:
    repo = make_repo()
    register_answer(repo)
    insert_document(repo, "ev-1", "doc", "dv1", (("p1", "a"),))
    repo.deactivate_document_version("dv1", repo.advance_epoch())
    assert not repo.is_chunk_active("p1")

    deltas = observe(repo, "ev-2", "o1", "c1", "p1", SUPPORT_SCORES)
    assert deltas == ()  # no state change
    assert repo.observation("o1").observation_id == "o1"
    claim_states, _ = compute_all_states(repo)
    assert claim_states["c1"].support_count == 0


def test_history_survives_deactivation() -> None:
    repo = make_repo()
    insert_document(repo, "ev-1", "doc", "dv1", (("p1", "old text"),))
    repo.deactivate_document_version("dv1", repo.advance_epoch())

    assert repo.document_version("dv1").content_hash == "hash-dv1"
    assert repo.chunk_version("p1").text == "old text"
    assert not repo.is_chunk_active("p1")
    assert repo.document_versions_of("doc") == ("dv1",)
    assert repo.active_document_version("doc") is None
