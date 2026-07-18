"""M1.1 regression tests for in-memory transactional event application."""

from copy import deepcopy

import pytest
from helpers import (
    SUPPORT_SCORES,
    insert_document,
    make_observation,
    make_repo,
    register_answer,
)

from groundloop.domain import DecisionPolicy
from groundloop.errors import (
    DanglingReferenceError,
    DuplicateIdentifierError,
)
from groundloop.events import (
    ChunkInput,
    DeleteDocumentVersionEvent,
    InsertDocumentEvent,
    ObserveEvent,
    PolicyChangeEvent,
    ReplaceDocumentVersionEvent,
    apply_event,
)
from groundloop.repository import InMemoryRepository


def _assert_unchanged(
    repo: InMemoryRepository, before: InMemoryRepository, failed_event_id: str
) -> None:
    assert repo == before
    assert repo.current_epoch == before.current_epoch
    assert repo.status_deltas == before.status_deltas
    assert repo.recorded_event(failed_event_id) is None


def test_rejected_observation_does_not_advance_epoch_or_mutate_indexes() -> None:
    repo = make_repo()
    insert_document(repo, "ev-insert", "doc", "dv1", (("p1", "evidence"),))
    original = make_observation("o1", "missing-claim", "p1", SUPPORT_SCORES)
    event = ObserveEvent(event_id="ev-failed-observe", observation=original)
    before = deepcopy(repo)

    with pytest.raises(DanglingReferenceError):
        apply_event(repo, event)

    _assert_unchanged(repo, before, event.event_id)

    # A failed ID was never consumed and can be retried with a corrected payload.
    # Register the missing answer/claim directly, as initial answer registration
    # is outside the event stream in M1.
    register_answer(repo)
    valid = ObserveEvent(
        event_id=event.event_id,
        observation=make_observation("o1", "c1", "p1", SUPPORT_SCORES),
    )
    epoch_before_retry = repo.current_epoch
    apply_event(repo, valid)
    assert repo.current_epoch == epoch_before_retry + 1
    assert repo.recorded_event(event.event_id) is not None


def test_rejected_delete_and_policy_change_leave_repository_unchanged() -> None:
    repo = make_repo()

    delete = DeleteDocumentVersionEvent(
        event_id="ev-failed-delete", document_version_id="missing"
    )
    before_delete = deepcopy(repo)
    with pytest.raises(DanglingReferenceError):
        apply_event(repo, delete)
    _assert_unchanged(repo, before_delete, delete.event_id)

    duplicate_policy = PolicyChangeEvent(
        event_id="ev-failed-policy",
        policy=DecisionPolicy(
            policy_version="k1", support_threshold=0.2, refute_threshold=0.2
        ),
    )
    before_policy = deepcopy(repo)
    with pytest.raises(DuplicateIdentifierError):
        apply_event(repo, duplicate_policy)
    _assert_unchanged(repo, before_policy, duplicate_policy.event_id)


def test_duplicate_chunk_ids_are_rejected_without_partial_insert() -> None:
    repo = make_repo()
    event = InsertDocumentEvent(
        event_id="ev-duplicate-chunks",
        document_id="doc",
        document_version_id="dv1",
        content_hash="hash",
        chunks=(
            ChunkInput(chunk_version_id="p1", chunk_index=0, text="first"),
            ChunkInput(chunk_version_id="p1", chunk_index=1, text="second"),
        ),
    )
    before = deepcopy(repo)

    with pytest.raises(DuplicateIdentifierError):
        apply_event(repo, event)

    _assert_unchanged(repo, before, event.event_id)
    assert repo.active_document_version("doc") is None


def test_failed_replace_rolls_back_old_version_deactivation() -> None:
    repo = make_repo()
    insert_document(repo, "ev-insert", "doc", "dv1", (("p1", "old"),))
    event = ReplaceDocumentVersionEvent(
        event_id="ev-failed-replace",
        document_id="doc",
        old_document_version_id="dv1",
        new_document_version_id="dv2",
        content_hash="new-hash",
        chunks=(
            ChunkInput(chunk_version_id="p2", chunk_index=0, text="first"),
            ChunkInput(chunk_version_id="p2", chunk_index=1, text="second"),
        ),
    )
    before = deepcopy(repo)

    with pytest.raises(DuplicateIdentifierError):
        apply_event(repo, event)

    _assert_unchanged(repo, before, event.event_id)
    assert repo.active_document_version("doc") == "dv1"
    assert repo.is_chunk_active("p1")
    assert not repo.has_document_version("dv2")
