"""Affected-key transaction tests for the in-memory exact IVM engine."""

from __future__ import annotations

import copy
from collections.abc import Iterator

import pytest
from helpers import (
    NEUTRAL_SCORES,
    REFUTE_SCORES,
    SUPPORT_SCORES,
    insert_document,
    make_observation,
    make_repo,
    register_answer,
)

from groundloop.domain import AnswerStatus, ClaimStatus, DecisionPolicy
from groundloop.events import (
    ChunkInput,
    DeleteDocumentVersionEvent,
    Event,
    ObserveEvent,
    PolicyChangeEvent,
    ReplaceDocumentVersionEvent,
    apply_event,
)
from groundloop.incremental import IncrementalMaintenanceEngine
from groundloop.reference import compute_all_states
from groundloop.repository import InMemoryRepository


def _stage(
    repository: InMemoryRepository, event: Event
) -> InMemoryRepository:
    after = copy.deepcopy(repository)
    apply_event(after, event)
    return after


def _apply_patch(
    engine: IncrementalMaintenanceEngine,
    repository: InMemoryRepository,
    event: Event,
) -> InMemoryRepository:
    after = _stage(repository, event)
    patch = engine.prepare_committed_event_patch(event, repository, after)
    engine.apply_state_patch(patch)
    return after


def _assert_equivalent(
    engine: IncrementalMaintenanceEngine, repository: InMemoryRepository
) -> None:
    expected_claims, expected_answers = compute_all_states(repository)
    assert engine.claim_states == expected_claims
    assert engine.answer_states == expected_answers
    engine.validate_certificates()


def _one_claim_engine() -> tuple[InMemoryRepository, IncrementalMaintenanceEngine]:
    repository = make_repo()
    register_answer(repository)
    insert_document(
        repository, "insert-v1", "doc", "dv-1", (("chunk-1", "evidence"),)
    )
    return repository, IncrementalMaintenanceEngine.from_repository(repository)


def test_insert_and_supersession_patch_only_the_affected_claim_and_answer() -> None:
    repository, engine = _one_claim_engine()
    support = ObserveEvent(
        event_id="observe-support",
        observation=make_observation(
            "observation-support", "c1", "chunk-1", SUPPORT_SCORES
        ),
    )
    after_support = _stage(repository, support)
    support_patch = engine.prepare_committed_event_patch(
        support, repository, after_support
    )
    assert support_patch.touched_claim_ids == ("c1",)
    assert support_patch.touched_answer_ids == ("a1",)
    assert support_patch.stats.score_index_point_updates == 2
    engine.apply_state_patch(support_patch)
    repository = after_support
    assert engine.claim_state("c1").status is ClaimStatus.SUPPORTED
    assert engine.answer_state("a1").status is AnswerStatus.VALID
    assert engine.certificate("c1").support_observation_id == "observation-support"

    refute = ObserveEvent(
        event_id="observe-refute",
        observation=make_observation(
            "observation-refute", "c1", "chunk-1", REFUTE_SCORES
        ),
    )
    after_refute = _stage(repository, refute)
    refute_patch = engine.prepare_committed_event_patch(
        refute, repository, after_refute
    )
    assert len(refute_patch.score_index_changes) == 2
    engine.apply_state_patch(refute_patch)
    repository = after_refute
    assert engine.claim_state("c1").status is ClaimStatus.REFUTED
    assert engine.answer_state("a1").status is AnswerStatus.CONTRADICTED
    assert engine.certificate("c1").refute_observation_id == "observation-refute"
    _assert_equivalent(engine, repository)


@pytest.mark.parametrize("operation", ["delete", "replace"])
def test_delete_and_replace_withdraw_active_witnesses(operation: str) -> None:
    repository, engine = _one_claim_engine()
    repository = _apply_patch(
        engine,
        repository,
        ObserveEvent(
            event_id="observe",
            observation=make_observation(
                "observation", "c1", "chunk-1", SUPPORT_SCORES
            ),
        ),
    )
    event: Event
    if operation == "delete":
        event = DeleteDocumentVersionEvent(
            event_id="delete", document_version_id="dv-1"
        )
    else:
        event = ReplaceDocumentVersionEvent(
            event_id="replace",
            document_id="doc",
            old_document_version_id="dv-1",
            new_document_version_id="dv-2",
            content_hash="hash-dv-2",
            chunks=(ChunkInput("chunk-2", 0, "replacement"),),
        )
    after = _stage(repository, event)
    patch = engine.prepare_committed_event_patch(event, repository, after)
    assert patch.touched_claim_ids == ("c1",)
    assert patch.stats.active_observations_removed == 1
    engine.apply_state_patch(patch)
    assert engine.claim_state("c1").status is ClaimStatus.UNSUPPORTED
    assert engine.answer_state("a1").status is AnswerStatus.UNSUPPORTED
    _assert_equivalent(engine, after)


def test_neutral_and_late_inactive_observations_do_not_touch_grounding() -> None:
    repository, engine = _one_claim_engine()
    neutral = ObserveEvent(
        event_id="neutral",
        observation=make_observation(
            "neutral-observation", "c1", "chunk-1", NEUTRAL_SCORES
        ),
    )
    after_neutral = _stage(repository, neutral)
    neutral_patch = engine.prepare_committed_event_patch(
        neutral, repository, after_neutral
    )
    assert neutral_patch.touched_claim_ids == ()
    assert neutral_patch.touched_answer_ids == ()
    assert neutral_patch.stats.active_observations_added == 1
    engine.apply_state_patch(neutral_patch)
    repository = after_neutral

    delete = DeleteDocumentVersionEvent(
        event_id="delete", document_version_id="dv-1"
    )
    repository = _apply_patch(engine, repository, delete)
    late = ObserveEvent(
        event_id="late",
        observation=make_observation(
            "late-observation", "c1", "chunk-1", SUPPORT_SCORES
        ),
    )
    after_late = _stage(repository, late)
    late_patch = engine.prepare_committed_event_patch(late, repository, after_late)
    assert late_patch.touched_claim_ids == ()
    assert late_patch.stats.active_observations_added == 0
    assert late_patch.stats.score_index_point_updates == 0
    engine.apply_state_patch(late_patch)
    _assert_equivalent(engine, after_late)


def test_optional_claim_patch_does_not_recompute_answer_state() -> None:
    repository = make_repo()
    register_answer(repository, claims=(("required", True), ("optional", False)))
    insert_document(
        repository, "insert", "doc", "dv", (("chunk", "optional evidence"),)
    )
    engine = IncrementalMaintenanceEngine.from_repository(repository)
    event = ObserveEvent(
        event_id="observe-optional",
        observation=make_observation(
            "optional-observation", "optional", "chunk", SUPPORT_SCORES
        ),
    )
    after = _stage(repository, event)
    patch = engine.prepare_committed_event_patch(event, repository, after)
    assert patch.touched_claim_ids == ("optional",)
    assert patch.touched_answer_ids == ()
    engine.apply_state_patch(patch)
    assert engine.claim_state("optional").status is ClaimStatus.SUPPORTED
    assert engine.answer_state("a1").status is AnswerStatus.UNSUPPORTED
    _assert_equivalent(engine, after)


def test_policy_patch_uses_threshold_candidates_without_score_index_updates() -> None:
    repository = make_repo(support_threshold=0.95)
    register_answer(repository)
    insert_document(repository, "insert", "doc", "dv", (("chunk", "evidence"),))
    apply_event(
        repository,
        ObserveEvent(
            event_id="observe",
            observation=make_observation(
                "observation", "c1", "chunk", SUPPORT_SCORES
            ),
        ),
    )
    engine = IncrementalMaintenanceEngine.from_repository(repository)
    event = PolicyChangeEvent(
        event_id="lower-threshold",
        policy=DecisionPolicy(
            policy_version="lower",
            support_threshold=0.8,
            refute_threshold=0.8,
        ),
    )
    after = _stage(repository, event)
    patch = engine.prepare_committed_event_patch(event, repository, after)
    assert patch.stats.policy_index_candidates == 1
    assert patch.stats.decision_flips == 1
    assert patch.stats.score_index_point_updates == 0
    engine.apply_state_patch(patch)
    assert engine.claim_state("c1").status is ClaimStatus.SUPPORTED
    _assert_equivalent(engine, after)


@pytest.mark.parametrize(
    "failure_stage",
    [
        "active_label:observation",
        "contribution:observation",
        "accumulator:c1",
        "claim_state:c1",
        "certificate:c1",
        "answer_counts:a1",
        "answer_state:a1",
        "score_index:observation",
        "published",
    ],
)
def test_patch_application_rolls_back_every_point_mutation(
    failure_stage: str,
) -> None:
    repository, engine = _one_claim_engine()
    event = ObserveEvent(
        event_id="observe",
        observation=make_observation("observation", "c1", "chunk-1", SUPPORT_SCORES),
    )
    after = _stage(repository, event)
    patch = engine.prepare_committed_event_patch(event, repository, after)
    before_engine = copy.deepcopy(engine)

    def fail(stage: str) -> None:
        if stage == failure_stage:
            raise RuntimeError("injected patch failure")

    with pytest.raises(RuntimeError, match="injected patch failure"):
        engine.apply_state_patch(patch, failure_injector=fail)

    assert engine == before_engine
    engine.apply_state_patch(patch)
    with pytest.raises(AssertionError, match="stale or was already applied"):
        engine.apply_state_patch(patch)
    _assert_equivalent(engine, after)


@pytest.mark.parametrize("claim_count", [8, 256])
def test_patch_path_avoids_whole_state_access_and_reports_linear_score_index(
    monkeypatch: pytest.MonkeyPatch, claim_count: int
) -> None:
    repository = make_repo()
    claims = tuple((f"claim-{index:04d}", True) for index in range(claim_count))
    register_answer(repository, claims=claims)
    chunks = tuple(
        (f"chunk-{index:04d}", f"evidence {index}")
        for index in range(claim_count)
    )
    insert_document(repository, "insert", "doc", "dv", chunks)
    for index in range(claim_count):
        apply_event(
            repository,
            ObserveEvent(
                event_id=f"seed-{index:04d}",
                observation=make_observation(
                    f"observation-{index:04d}",
                    f"claim-{index:04d}",
                    f"chunk-{index:04d}",
                    SUPPORT_SCORES,
                ),
            ),
        )
    engine = IncrementalMaintenanceEngine.from_repository(repository)
    event = ObserveEvent(
        event_id="supersede-one",
        observation=make_observation(
            "replacement-observation", "claim-0000", "chunk-0000", REFUTE_SCORES
        ),
    )
    after = _stage(repository, event)

    def forbidden(*_args: object, **_kwargs: object) -> Iterator[None]:
        raise AssertionError("whole-state/deepcopy path was used")

    monkeypatch.setattr(copy, "deepcopy", forbidden)
    monkeypatch.setattr(
        IncrementalMaintenanceEngine,
        "claim_states",
        property(forbidden),
    )
    monkeypatch.setattr(
        IncrementalMaintenanceEngine,
        "answer_states",
        property(forbidden),
    )
    monkeypatch.setattr(
        IncrementalMaintenanceEngine,
        "certificates",
        property(forbidden),
    )
    monkeypatch.setattr(InMemoryRepository, "all_claim_ids", forbidden)
    monkeypatch.setattr(InMemoryRepository, "all_answer_ids", forbidden)

    patch = engine.prepare_committed_event_patch(event, repository, after)
    engine.apply_state_patch(patch)
    assert patch.touched_claim_ids == ("claim-0000",)
    assert patch.touched_answer_ids == ("a1",)
    assert patch.stats.score_index_entries_before == claim_count
    assert patch.stats.score_index_point_updates == 4
    assert engine.last_stats.score_index_shift_work > 0
    assert (
        engine.last_stats.score_index_shift_work
        <= engine.last_stats.score_index_shift_upper_bound
    )
    assert engine.claim_state("claim-0000").status is ClaimStatus.REFUTED
    last_claim = engine.claim_state(f"claim-{claim_count - 1:04d}")
    assert last_claim.status is ClaimStatus.SUPPORTED
