from __future__ import annotations

import hashlib

import pytest

from groundloop.errors import (
    DuplicateIdentifierError,
    EventConflictError,
    GroundLoopError,
    ValidationError,
)
from groundloop.events import DeleteDocumentVersionEvent, ObserveEvent, apply_event
from groundloop.m5.events import (
    ObserveRequirementEvent,
    RegisterGroupEvent,
    ReplaceGroupEvent,
    RetireGroupEvent,
    apply_m5_event,
    legacy_event_payload_digest,
    m5_event_payload_digest,
)
from groundloop.m5.repository import M5Repository

from .conftest import (
    REFUTE,
    make_group,
    make_requirement_observation,
)


def test_exact_replay_is_noop_and_conflicting_payload_rejects(
    m5_repo: M5Repository,
) -> None:
    event = RegisterGroupEvent(event_id="register", group=make_group(texts=("one",)))
    first = apply_m5_event(m5_repo, event)
    snapshot = m5_repo.export_snapshot()
    replay = apply_m5_event(m5_repo, event)
    assert replay.replayed
    assert replay.deltas == first.deltas
    assert m5_repo.export_snapshot() == snapshot

    conflict = RegisterGroupEvent(
        event_id="register",
        group=make_group(group_id="g2", family_id="f2", texts=("different",)),
    )
    with pytest.raises(EventConflictError):
        apply_m5_event(m5_repo, conflict)
    assert m5_repo.export_snapshot() == snapshot


def test_structural_payload_digest_excludes_only_outer_idempotency_key() -> None:
    group = make_group(texts=("one",))
    first = RegisterGroupEvent(event_id="outer-a", group=group)
    second = RegisterGroupEvent(event_id="outer-b", group=group)
    assert m5_event_payload_digest(first) == m5_event_payload_digest(second)


def test_legacy_and_m5_events_share_one_global_idempotency_namespace(
    m5_repo: M5Repository,
) -> None:
    before = m5_repo.export_snapshot()
    with pytest.raises(EventConflictError):
        apply_m5_event(
            m5_repo,
            RegisterGroupEvent(
                event_id="base-policy", group=make_group(texts=("one",))
            ),
        )
    assert m5_repo.export_snapshot() == before

    apply_m5_event(
        m5_repo,
        RegisterGroupEvent(event_id="m5-owned-id", group=make_group(texts=("one",))),
    )
    after = m5_repo.export_snapshot()
    with pytest.raises(EventConflictError):
        apply_event(
            m5_repo.base,
            DeleteDocumentVersionEvent(
                event_id="m5-owned-id", document_version_id="dv1"
            ),
        )
    assert m5_repo.export_snapshot() == after


def test_requirement_observation_id_is_reserved_against_legacy_alias(
    m5_repo: M5Repository,
) -> None:
    apply_m5_event(
        m5_repo,
        RegisterGroupEvent(event_id="register", group=make_group(texts=("one",))),
    )
    requirement_observation = make_requirement_observation(
        observation_id="globally-shared", requirement_id="g1-r0", chunk_id="h1"
    )
    apply_m5_event(
        m5_repo,
        ObserveRequirementEvent(
            event_id="requirement-observe", observation=requirement_observation
        ),
    )
    claim_observation = type(requirement_observation)(
        observation_id="globally-shared",
        subject_kind=requirement_observation.subject_kind.CLAIM,
        subject_id="c1",
        chunk_version_id="h1",
        task_type="verify",
        support_score=requirement_observation.support_score,
        refute_score=requirement_observation.refute_score,
        neutral_score=requirement_observation.neutral_score,
        producer=requirement_observation.producer,
        input_hash=requirement_observation.input_hash,
    )
    before = m5_repo.export_snapshot()
    with pytest.raises(DuplicateIdentifierError, match="already registered"):
        apply_event(
            m5_repo.base,
            ObserveEvent(event_id="legacy-collision", observation=claim_observation),
        )
    assert m5_repo.export_snapshot() == before


@pytest.mark.parametrize(
    "event",
    (
        RegisterGroupEvent(
            event_id="missing-owner",
            group=make_group(claim_id="absent", texts=("one",)),
        ),
        RetireGroupEvent(event_id="missing-group", group_version_id="absent"),
        RegisterGroupEvent(event_id="", group=make_group(texts=("one",))),
    ),
)
def test_rejected_events_preserve_complete_snapshot_and_epoch(
    m5_repo: M5Repository, event: object
) -> None:
    before = m5_repo.export_snapshot()
    with pytest.raises(Exception) as captured:
        apply_m5_event(m5_repo, event)  # type: ignore[arg-type]
    assert isinstance(captured.value, GroundLoopError)
    assert m5_repo.export_snapshot() == before


def test_base_alias_identity_is_preserved_across_copy_commit(
    m5_repo: M5Repository,
) -> None:
    base_alias = m5_repo.base
    apply_m5_event(
        m5_repo,
        RegisterGroupEvent(event_id="register", group=make_group(texts=("one",))),
    )
    assert m5_repo.base is base_alias
    assert base_alias.current_epoch == m5_repo.current_epoch
    assert base_alias.recorded_event("register") is not None


@pytest.mark.parametrize(
    "failure_point",
    (
        "after_epoch_open",
        "after_mutation",
        "after_recompute",
        "after_event_record",
        "before_commit",
    ),
)
def test_every_injected_failure_is_atomic(
    m5_repo: M5Repository, failure_point: str
) -> None:
    before = m5_repo.export_snapshot()

    def inject(point: str, _staged: M5Repository) -> None:
        if point == failure_point:
            raise RuntimeError(f"injected:{point}")

    with pytest.raises(RuntimeError, match=failure_point):
        apply_m5_event(
            m5_repo,
            RegisterGroupEvent(
                event_id=f"event-{failure_point}", group=make_group(texts=("one",))
            ),
            failure_injector=inject,
        )
    assert m5_repo.export_snapshot() == before


def test_old_complete_new_incomplete_replacement_is_atomic_and_no_transfer(
    m5_repo: M5Repository,
) -> None:
    old = make_group(group_id="g1", texts=("one",))
    apply_m5_event(m5_repo, RegisterGroupEvent(event_id="register", group=old))
    old_observation = make_requirement_observation(
        observation_id="old-observation", requirement_id="g1-r0", chunk_id="h1"
    )
    completion = apply_m5_event(
        m5_repo,
        ObserveRequirementEvent(event_id="complete-old", observation=old_observation),
    )
    assert [(delta.object_id, delta.new_status) for delta in completion.deltas] == [
        ("c1", "supported"),
        ("a1", "valid"),
    ]

    successor = make_group(
        group_id="g2",
        family_id="f1",
        texts=("one",),
        predecessors=("g1-r0",),
        supersedes_group_id="g1",
    )
    replacement = apply_m5_event(
        m5_repo,
        ReplaceGroupEvent(
            event_id="replace", old_group_version_id="g1", successor=successor
        ),
    )
    assert [(delta.object_id, delta.new_status) for delta in replacement.deltas] == [
        ("c1", "unsupported"),
        ("a1", "unsupported"),
    ]
    assert m5_repo.current_requirement_observation_ids() == ("old-observation",)

    new_observation = make_requirement_observation(
        observation_id="new-observation", requirement_id="g2-r0", chunk_id="h1"
    )
    completed_new = apply_m5_event(
        m5_repo,
        ObserveRequirementEvent(event_id="complete-new", observation=new_observation),
    )
    assert [(delta.object_id, delta.new_status) for delta in completed_new.deltas] == [
        ("c1", "supported"),
        ("a1", "valid"),
    ]


def test_failed_replacement_leaves_old_complete_group_active_for_retry(
    m5_repo: M5Repository,
) -> None:
    old = make_group(group_id="g1", texts=("one",))
    apply_m5_event(m5_repo, RegisterGroupEvent(event_id="register", group=old))
    apply_m5_event(
        m5_repo,
        ObserveRequirementEvent(
            event_id="complete",
            observation=make_requirement_observation(
                observation_id="o1", requirement_id="g1-r0", chunk_id="h1"
            ),
        ),
    )
    bad = make_group(
        group_id="g-bad",
        family_id="wrong-family",
        texts=("one",),
        predecessors=("g1-r0",),
        supersedes_group_id="g1",
    )
    before = m5_repo.export_snapshot()
    with pytest.raises(ValidationError):
        apply_m5_event(
            m5_repo,
            ReplaceGroupEvent(
                event_id="bad-replace", old_group_version_id="g1", successor=bad
            ),
        )
    assert m5_repo.export_snapshot() == before
    assert m5_repo.is_group_active("g1")

    good = make_group(
        group_id="g2",
        family_id="f1",
        texts=("one",),
        predecessors=("g1-r0",),
        supersedes_group_id="g1",
    )
    apply_m5_event(
        m5_repo,
        ReplaceGroupEvent(
            event_id="good-replace", old_group_version_id="g1", successor=good
        ),
    )
    assert m5_repo.is_group_active("g2")


def test_requirement_refute_and_noncanonical_support_are_stored_but_parent_inert(
    m5_repo: M5Repository,
) -> None:
    apply_m5_event(
        m5_repo,
        RegisterGroupEvent(event_id="register", group=make_group(texts=("one",))),
    )
    refute = make_requirement_observation(
        observation_id="refute",
        requirement_id="g1-r0",
        chunk_id="h1",
        scores=REFUTE,
    )
    refute_result = apply_m5_event(
        m5_repo, ObserveRequirementEvent(event_id="observe-refute", observation=refute)
    )
    assert refute_result.deltas == ()
    noncanonical = make_requirement_observation(
        observation_id="wrong-task",
        requirement_id="g1-r0",
        chunk_id="h1",
        task_type="verify_requirement_experimental",
    )
    wrong_task_result = apply_m5_event(
        m5_repo,
        ObserveRequirementEvent(
            event_id="observe-wrong-task", observation=noncanonical
        ),
    )
    assert wrong_task_result.deltas == ()
    assert set(m5_repo.current_requirement_observation_ids()) == {
        "refute",
        "wrong-task",
    }


def test_legacy_digest_recipe_is_frozen_repr_sha256() -> None:
    event = DeleteDocumentVersionEvent(
        event_id="legacy-vector", document_version_id="dv1"
    )
    expected = hashlib.sha256(repr(event).encode("utf-8")).hexdigest()
    assert legacy_event_payload_digest(event) == expected
    assert (
        expected == "4b1269ed55198abb8215d420cc17fa245e6cfbbd07f31679f12a50b74fde2f13"
    )
