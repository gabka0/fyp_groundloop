"""M2 semantic-epoch and publication-oracle tests (D-20)."""

import pytest

from groundloop.domain import EvaluationState
from groundloop.epochs import EpochCoordinator, PublicationMode, SemanticEpochStatus
from groundloop.errors import EventConflictError, ValidationError


def test_job_microtransactions_keep_epoch_stable_and_advance_revision() -> None:
    coordinator = EpochCoordinator()
    opened = coordinator.open_epoch(
        event_id="ev-1",
        payload_hash=coordinator.payload_hash("payload-1"),
        required_job_ids=frozenset({"j1", "j2"}),
    )
    assert (opened.epoch_id, opened.revision) == (1, 0)
    assert opened.semantic_status is SemanticEpochStatus.PENDING

    first = coordinator.complete_job(opened.epoch_id, "j1")
    assert (first.epoch_id, first.revision) == (1, 1)
    assert first.semantic_status is SemanticEpochStatus.PENDING

    # Exact completion replay is a no-op.
    assert coordinator.complete_job(opened.epoch_id, "j1") == first

    second = coordinator.complete_job(opened.epoch_id, "j2")
    assert (second.epoch_id, second.revision) == (1, 2)
    assert second.semantic_status is SemanticEpochStatus.COMPLETE
    sealed = coordinator.seal(opened.epoch_id)
    assert (sealed.epoch_id, sealed.revision) == (1, 3)
    assert sealed.semantic_status is SemanticEpochStatus.SEALED


def test_strict_and_provisional_publication_boundaries() -> None:
    coordinator = EpochCoordinator()
    first = coordinator.open_epoch(
        event_id="ev-1",
        payload_hash="hash-1",
        required_job_ids=frozenset(),
    )
    coordinator.seal(first.epoch_id)

    pending = coordinator.open_epoch(
        event_id="ev-2",
        payload_hash="hash-2",
        required_job_ids=frozenset({"j"}),
        publication_mode=PublicationMode.PROVISIONAL,
    )
    snapshot = coordinator.publication_snapshot()
    assert snapshot.visible_epoch_id == pending.epoch_id
    assert snapshot.evaluation_state is EvaluationState.PENDING
    assert snapshot.confirmed_as_of_epoch == first.epoch_id
    coordinator.mark_failed(pending.epoch_id)
    failed_snapshot = coordinator.publication_snapshot()
    assert failed_snapshot.visible_epoch_id == pending.epoch_id
    assert failed_snapshot.evaluation_state is EvaluationState.FAILED
    assert failed_snapshot.confirmed_as_of_epoch == first.epoch_id

    strict = coordinator.open_epoch(
        event_id="ev-3",
        payload_hash="hash-3",
        required_job_ids=frozenset({"j"}),
        publication_mode=PublicationMode.STRICT,
    )
    snapshot = coordinator.publication_snapshot()
    assert snapshot.visible_epoch_id == first.epoch_id
    assert snapshot.evaluation_state is EvaluationState.PENDING
    assert snapshot.confirmed_as_of_epoch == first.epoch_id
    coordinator.mark_failed(strict.epoch_id)


def test_degraded_evaluation_survives_sealing() -> None:
    coordinator = EpochCoordinator()
    opened = coordinator.open_epoch(
        event_id="ev",
        payload_hash="hash",
        required_job_ids=frozenset({"job"}),
    )
    coordinator.mark_degraded(opened.epoch_id)
    coordinator.seal(opened.epoch_id)
    snapshot = coordinator.publication_snapshot()
    assert snapshot.visible_epoch_id == opened.epoch_id
    assert snapshot.evaluation_state is EvaluationState.DEGRADED
    assert snapshot.confirmed_as_of_epoch == opened.epoch_id


def test_single_writer_idempotence_and_event_conflict() -> None:
    coordinator = EpochCoordinator()
    opened = coordinator.open_epoch(
        event_id="ev-1",
        payload_hash="hash-1",
        required_job_ids=frozenset({"j"}),
    )
    assert (
        coordinator.open_epoch(
            event_id="ev-1",
            payload_hash="hash-1",
            required_job_ids=frozenset({"j"}),
        )
        == opened
    )

    with pytest.raises(EventConflictError):
        coordinator.open_epoch(
            event_id="ev-1",
            payload_hash="different",
            required_job_ids=frozenset(),
        )
    with pytest.raises(ValidationError, match="single-writer"):
        coordinator.open_epoch(
            event_id="ev-2",
            payload_hash="hash-2",
            required_job_ids=frozenset(),
        )


def test_pending_epoch_cannot_seal_or_accept_unknown_job() -> None:
    coordinator = EpochCoordinator()
    opened = coordinator.open_epoch(
        event_id="ev",
        payload_hash="hash",
        required_job_ids=frozenset({"j"}),
    )
    with pytest.raises(ValidationError, match="cannot seal"):
        coordinator.seal(opened.epoch_id)
    with pytest.raises(ValidationError, match="not required"):
        coordinator.complete_job(opened.epoch_id, "unknown")
