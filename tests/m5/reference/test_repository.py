from __future__ import annotations

from dataclasses import replace

import pytest

from groundloop.errors import (
    DanglingReferenceError,
    DuplicateIdentifierError,
    ValidationError,
)
from groundloop.events import DeleteDocumentVersionEvent, apply_event
from groundloop.m5.domain import SnapshotPoint
from groundloop.m5.events import (
    ObserveRequirementEvent,
    RegisterGroupEvent,
    ReplaceGroupEvent,
    RetireGroupEvent,
    apply_m5_event,
)
from groundloop.m5.repository import M5Repository

from .conftest import (
    SUPPORT,
    add_document,
    make_base,
    make_group,
    make_requirement_observation,
)


def test_register_group_indexes_owner_and_derives_requirement_activity(
    m5_repo: M5Repository,
) -> None:
    assert m5_repo.is_known_snapshot_point(SnapshotPoint(0, 0))
    assert m5_repo.is_known_snapshot_point(SnapshotPoint(1, 0))
    group = make_group()
    result = apply_m5_event(
        m5_repo, RegisterGroupEvent(event_id="group-register", group=group)
    )
    assert not result.replayed
    assert result.deltas == ()
    assert m5_repo.family("f1").claim_id == "c1"
    assert m5_repo.group_ids_for_claim("c1") == ("g1",)
    assert m5_repo.requirement_ids_for_group("g1") == ("g1-r0", "g1-r1")
    assert m5_repo.owner_claim_id("g1-r0") == "c1"
    assert m5_repo.is_group_active("g1")
    assert m5_repo.is_requirement_active("g1-r0")


def test_typed_namespaces_allow_same_raw_claim_and_requirement_id() -> None:
    repo = M5Repository(make_base())
    group = make_group(texts=("one",), requirement_ids=("c1",))
    apply_m5_event(repo, RegisterGroupEvent(event_id="register", group=group))
    assert (
        repo.base.claim("c1").claim_id == repo.requirement("c1").requirement_version_id
    )


def test_active_semantic_duplicate_is_rejected_despite_reorder_or_provenance() -> None:
    repo = M5Repository(make_base())
    first = make_group(group_id="g1", family_id="f1", texts=("alpha", "beta"))
    apply_m5_event(repo, RegisterGroupEvent(event_id="register-1", group=first))
    duplicate = make_group(
        group_id="g2",
        family_id="f2",
        texts=("beta", "alpha"),
        source_id="different-source",
    )
    before = repo.export_snapshot()
    with pytest.raises(ValidationError, match="equivalent"):
        apply_m5_event(repo, RegisterGroupEvent(event_id="register-2", group=duplicate))
    assert repo.export_snapshot() == before


def test_atomic_successor_closes_whole_old_group_and_transfers_no_currency(
    m5_repo: M5Repository,
) -> None:
    old = make_group(group_id="g1", family_id="f1", texts=("alpha", "beta"))
    apply_m5_event(m5_repo, RegisterGroupEvent(event_id="register", group=old))
    observation = make_requirement_observation(
        observation_id="o-old", requirement_id="g1-r0", chunk_id="h1"
    )
    apply_m5_event(
        m5_repo,
        ObserveRequirementEvent(event_id="observe-old", observation=observation),
    )
    old_active_epoch = m5_repo.current_epoch

    successor = make_group(
        group_id="g2",
        family_id="f1",
        texts=("alpha", "beta"),
        predecessors=("g1-r0", "g1-r1"),
        supersedes_group_id="g1",
    )
    apply_m5_event(
        m5_repo,
        ReplaceGroupEvent(
            event_id="replace", old_group_version_id="g1", successor=successor
        ),
    )
    assert m5_repo.is_group_active("g1", old_active_epoch)
    assert not m5_repo.is_group_active("g1")
    assert not m5_repo.is_requirement_active("g1-r0")
    assert m5_repo.is_group_active("g2")
    assert m5_repo.is_requirement_active("g2-r0")
    assert m5_repo.current_requirement_observation_ids() == ("o-old",)
    assert (
        m5_repo.currency_observation_id_at(observation.key, m5_repo.current_point)
        == "o-old"
    )
    new_key = make_requirement_observation(
        observation_id="probe", requirement_id="g2-r0", chunk_id="h1"
    ).key
    assert m5_repo.currency_observation_id_at(new_key, m5_repo.current_point) is None


def test_successor_lineage_rejects_cross_family_owner_and_reused_predecessor() -> None:
    repo = M5Repository(make_base(claim_ids=("c1", "c2")))
    old = make_group()
    apply_m5_event(repo, RegisterGroupEvent(event_id="register", group=old))

    invalid_successors = (
        make_group(
            group_id="g2",
            family_id="other-family",
            predecessors=("g1-r0", "g1-r1"),
            supersedes_group_id="g1",
        ),
        make_group(
            group_id="g3",
            family_id="f1",
            claim_id="c2",
            predecessors=("g1-r0", "g1-r1"),
            supersedes_group_id="g1",
        ),
        make_group(
            group_id="g4",
            family_id="f1",
            predecessors=("g1-r0", "g1-r0"),
            supersedes_group_id="g1",
        ),
    )
    for index, successor in enumerate(invalid_successors):
        before = repo.export_snapshot()
        with pytest.raises(ValidationError):
            apply_m5_event(
                repo,
                ReplaceGroupEvent(
                    event_id=f"bad-{index}",
                    old_group_version_id="g1",
                    successor=successor,
                ),
            )
        assert repo.export_snapshot() == before


def test_replacement_must_follow_latest_active_version_and_retirement_is_final() -> (
    None
):
    repo = M5Repository(make_base())
    first = make_group(group_id="g1")
    apply_m5_event(repo, RegisterGroupEvent(event_id="register", group=first))
    second = make_group(
        group_id="g2",
        family_id="f1",
        predecessors=("g1-r0", "g1-r1"),
        supersedes_group_id="g1",
    )
    apply_m5_event(
        repo,
        ReplaceGroupEvent(
            event_id="replace", old_group_version_id="g1", successor=second
        ),
    )
    nonadjacent = make_group(
        group_id="g3",
        family_id="f1",
        predecessors=("g1-r0", "g1-r1"),
        supersedes_group_id="g1",
    )
    with pytest.raises(ValidationError, match="not active"):
        apply_m5_event(
            repo,
            ReplaceGroupEvent(
                event_id="nonadjacent",
                old_group_version_id="g1",
                successor=nonadjacent,
            ),
        )

    apply_m5_event(repo, RetireGroupEvent(event_id="retire", group_version_id="g2"))
    assert not repo.is_group_active("g2")
    with pytest.raises(ValidationError, match="not active"):
        apply_m5_event(
            repo, RetireGroupEvent(event_id="retire-again", group_version_id="g2")
        )


def test_currency_supersession_has_exact_as_of_intervals(m5_repo: M5Repository) -> None:
    apply_m5_event(
        m5_repo,
        RegisterGroupEvent(event_id="register", group=make_group(texts=("one",))),
    )
    first = make_requirement_observation(
        observation_id="o1", requirement_id="g1-r0", chunk_id="h1"
    )
    apply_m5_event(
        m5_repo, ObserveRequirementEvent(event_id="observe-1", observation=first)
    )
    first_point = m5_repo.current_point
    second = make_requirement_observation(
        observation_id="o2", requirement_id="g1-r0", chunk_id="h1"
    )
    apply_m5_event(
        m5_repo, ObserveRequirementEvent(event_id="observe-2", observation=second)
    )
    second_point = m5_repo.current_point

    history = m5_repo.currency_history(first.key)
    assert len(history) == 2
    assert history[0].observation_id == "o1"
    assert history[0].valid_from == first_point
    assert history[0].valid_to == second_point
    assert history[1].observation_id == "o2"
    assert history[1].valid_to is None
    assert m5_repo.currency_observation_id_at(first.key, first_point) == "o1"
    assert m5_repo.currency_observation_id_at(first.key, second_point) == "o2"
    with pytest.raises(ValidationError, match="does not exist"):
        m5_repo.currency_observation_id_at(
            first.key, SnapshotPoint(m5_repo.current_epoch + 1, 0)
        )


def test_same_epoch_revision_currency_is_monotonic(m5_repo: M5Repository) -> None:
    apply_m5_event(
        m5_repo,
        RegisterGroupEvent(event_id="register", group=make_group(texts=("one",))),
    )
    point_one = m5_repo.advance_semantic_revision()
    first = make_requirement_observation(
        observation_id="r1", requirement_id="g1-r0", chunk_id="h1"
    )
    m5_repo.register_requirement_observation(first, point_one)
    point_two = m5_repo.advance_semantic_revision()
    second = make_requirement_observation(
        observation_id="r2", requirement_id="g1-r0", chunk_id="h1"
    )
    m5_repo.register_requirement_observation(second, point_two)
    assert point_one.epoch_id == point_two.epoch_id
    assert point_two.revision == point_one.revision + 1
    assert m5_repo.currency_observation_id_at(first.key, point_one) == "r1"
    assert m5_repo.currency_observation_id_at(first.key, point_two) == "r2"


def test_same_point_currency_rejection_is_failure_atomic(
    m5_repo: M5Repository,
) -> None:
    apply_m5_event(
        m5_repo,
        RegisterGroupEvent(event_id="register", group=make_group(texts=("one",))),
    )
    point = m5_repo.advance_semantic_revision()
    first = make_requirement_observation(
        observation_id="same-point-first", requirement_id="g1-r0", chunk_id="h1"
    )
    m5_repo.register_requirement_observation(first, point)
    before = m5_repo.export_snapshot()
    second = make_requirement_observation(
        observation_id="same-point-second", requirement_id="g1-r0", chunk_id="h1"
    )
    with pytest.raises(ValidationError, match="advance monotonically"):
        m5_repo.register_requirement_observation(second, point)
    assert m5_repo.export_snapshot() == before
    assert not m5_repo.base.has_observation_id("same-point-second")

    # Rejection must not burn the globally reserved identifier.  The same
    # immutable observation becomes legal at the next semantic revision.
    next_point = m5_repo.advance_semantic_revision()
    m5_repo.register_requirement_observation(second, next_point)
    assert m5_repo.base.has_observation_id("same-point-second")
    assert m5_repo.currency_observation_id_at(second.key, next_point) == (
        "same-point-second"
    )


def test_direct_typed_observation_validation_precedes_all_mutation(
    m5_repo: M5Repository,
) -> None:
    apply_m5_event(
        m5_repo,
        RegisterGroupEvent(event_id="register", group=make_group(texts=("one",))),
    )
    point = m5_repo.advance_semantic_revision()
    valid = make_requirement_observation(
        observation_id="valid-shape", requirement_id="g1-r0", chunk_id="h1"
    )
    malformed = (
        replace(valid, observation_id="   "),
        replace(valid, task_type=""),
        replace(valid, input_hash="not-a-hash"),
        replace(valid, support_score=True),
    )
    for observation in malformed:
        before = m5_repo.export_snapshot()
        with pytest.raises(ValidationError):
            m5_repo.register_requirement_observation(observation, point)
        assert m5_repo.export_snapshot() == before
        assert not m5_repo.base.has_observation_id(observation.observation_id)


def test_sidecar_reconstructs_every_intermediate_committed_legacy_epoch(
    m5_repo: M5Repository,
) -> None:
    starting_epoch = m5_repo.current_epoch
    add_document(
        m5_repo.base,
        event_id="legacy-extra-1",
        document_id="extra-1",
        version_id="extra-v1",
        chunks=(("extra-h1", "one"),),
    )
    add_document(
        m5_repo.base,
        event_id="legacy-extra-2",
        document_id="extra-2",
        version_id="extra-v2",
        chunks=(("extra-h2", "two"),),
    )
    assert m5_repo.current_point == SnapshotPoint(starting_epoch + 2, 0)
    assert m5_repo.is_known_snapshot_point(SnapshotPoint(starting_epoch + 1, 0))
    assert m5_repo.is_known_snapshot_point(SnapshotPoint(starting_epoch + 2, 0))


def test_first_revision_after_external_legacy_epoch_starts_at_one(
    m5_repo: M5Repository,
) -> None:
    m5_repo.advance_semantic_revision()
    add_document(
        m5_repo.base,
        event_id="legacy-advance",
        document_id="external",
        version_id="external-v1",
        chunks=(("external-h1", "external"),),
    )
    external_epoch = m5_repo.base.current_epoch
    point = m5_repo.advance_semantic_revision()
    assert point == SnapshotPoint(external_epoch, 1)
    assert m5_repo.is_known_snapshot_point(SnapshotPoint(external_epoch, 0))
    assert m5_repo.is_known_snapshot_point(SnapshotPoint(external_epoch, 1))
    assert not m5_repo.is_known_snapshot_point(SnapshotPoint(external_epoch, 2))


def test_known_point_query_lazily_synchronizes_external_legacy_head(
    m5_repo: M5Repository,
) -> None:
    add_document(
        m5_repo.base,
        event_id="legacy-known",
        document_id="known",
        version_id="known-v1",
        chunks=(("known-h1", "known"),),
    )
    actual_head = SnapshotPoint(m5_repo.base.current_epoch, 0)
    assert m5_repo.is_known_snapshot_point(actual_head)
    m5_repo.require_snapshot_point(actual_head)


def test_inactive_group_or_chunk_observation_is_archived_and_inert(
    m5_repo: M5Repository,
) -> None:
    apply_m5_event(
        m5_repo,
        RegisterGroupEvent(event_id="register", group=make_group(texts=("one",))),
    )
    current = make_requirement_observation(
        observation_id="current", requirement_id="g1-r0", chunk_id="h1"
    )
    apply_m5_event(
        m5_repo,
        ObserveRequirementEvent(event_id="observe-current", observation=current),
    )
    apply_m5_event(m5_repo, RetireGroupEvent(event_id="retire", group_version_id="g1"))
    late = make_requirement_observation(
        observation_id="late", requirement_id="g1-r0", chunk_id="h1"
    )
    result = apply_m5_event(
        m5_repo, ObserveRequirementEvent(event_id="observe-late", observation=late)
    )
    assert result.deltas == ()
    assert not m5_repo.requirement_observation("late").eligible_for_currency
    assert m5_repo.currency_observation_id_at(current.key, m5_repo.current_point) == (
        "current"
    )

    # A distinct active group against an inactive chunk has the same archive-only rule.
    second = make_group(group_id="g2", family_id="f2", texts=("different",))
    apply_m5_event(m5_repo, RegisterGroupEvent(event_id="register-2", group=second))
    apply_event(
        m5_repo.base,
        DeleteDocumentVersionEvent(event_id="delete-doc", document_version_id="dv1"),
    )
    inactive_chunk = make_requirement_observation(
        observation_id="inactive-chunk", requirement_id="g2-r0", chunk_id="h2"
    )
    apply_m5_event(
        m5_repo,
        ObserveRequirementEvent(
            event_id="observe-inactive", observation=inactive_chunk
        ),
    )
    assert not m5_repo.requirement_observation("inactive-chunk").eligible_for_currency


def test_wrong_subject_missing_references_and_cross_store_duplicate_observation_reject(
    m5_repo: M5Repository,
) -> None:
    apply_m5_event(
        m5_repo,
        RegisterGroupEvent(event_id="register", group=make_group(texts=("one",))),
    )
    wrong_kind = make_requirement_observation(
        observation_id="wrong", requirement_id="g1-r0", chunk_id="h1"
    )
    wrong_kind = type(wrong_kind)(
        observation_id=wrong_kind.observation_id,
        subject_kind=wrong_kind.subject_kind.CLAIM,
        subject_id="c1",
        chunk_version_id=wrong_kind.chunk_version_id,
        task_type=wrong_kind.task_type,
        support_score=wrong_kind.support_score,
        refute_score=wrong_kind.refute_score,
        neutral_score=wrong_kind.neutral_score,
        producer=wrong_kind.producer,
        input_hash=wrong_kind.input_hash,
    )
    with pytest.raises(ValidationError, match="REQUIREMENT"):
        apply_m5_event(
            m5_repo,
            ObserveRequirementEvent(event_id="wrong-kind", observation=wrong_kind),
        )
    missing_chunk = make_requirement_observation(
        observation_id="missing", requirement_id="g1-r0", chunk_id="missing"
    )
    with pytest.raises(DanglingReferenceError, match="missing chunk"):
        apply_m5_event(
            m5_repo,
            ObserveRequirementEvent(
                event_id="missing-chunk", observation=missing_chunk
            ),
        )

    duplicate = make_requirement_observation(
        observation_id="shared-id", requirement_id="g1-r0", chunk_id="h1"
    )
    claim_observation = type(duplicate)(
        observation_id="shared-id",
        subject_kind=duplicate.subject_kind.CLAIM,
        subject_id="c1",
        chunk_version_id="h1",
        task_type="verify",
        support_score=SUPPORT[0],
        refute_score=SUPPORT[1],
        neutral_score=SUPPORT[2],
        producer=duplicate.producer,
        input_hash=duplicate.input_hash,
    )
    m5_repo.base.register_observation(claim_observation)
    with pytest.raises(DuplicateIdentifierError, match="already registered"):
        m5_repo.register_requirement_observation(duplicate, m5_repo.current_point)
