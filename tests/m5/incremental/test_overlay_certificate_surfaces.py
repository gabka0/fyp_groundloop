from __future__ import annotations

from copy import deepcopy

from groundloop.incremental import MaintenanceStats
from groundloop.m5 import incremental_overlay as overlay_module
from groundloop.m5.events import (
    ObserveRequirementEvent,
    RegisterGroupEvent,
    apply_m5_event,
)
from groundloop.m5.repository import M5Repository

from .helpers import make_group, make_repository, make_requirement_observation


def _complete_group_repository() -> M5Repository:
    repository = make_repository()
    apply_m5_event(
        repository,
        RegisterGroupEvent(
            event_id="register-group",
            group=make_group(),
        ),
    )
    apply_m5_event(
        repository,
        ObserveRequirementEvent(
            event_id="observe-selected-support",
            observation=make_requirement_observation(
                observation_id="a-selected-support",
                requirement_id="group-a-requirement-0",
                chunk_id="chunk-a",
            ),
        ),
    )
    return repository


def _cross_epoch_retain_event() -> ObserveRequirementEvent:
    return ObserveRequirementEvent(
        event_id="observe-unselected-support",
        observation=make_requirement_observation(
            observation_id="z-unselected-support",
            requirement_id="group-a-requirement-0",
            chunk_id="chunk-b",
        ),
    )


def test_cross_epoch_group_retain_is_a_certificate_only_change() -> None:
    repository = _complete_group_repository()
    overlay = overlay_module.M5IncrementalOverlay.from_repository(repository)
    before = deepcopy(repository)
    after = deepcopy(repository)
    event = _cross_epoch_retain_event()
    apply_m5_event(after, event)

    patch = overlay.prepare_committed_event_patch(event, before, after)

    assert patch.group_state_changes == ()
    assert patch.group_artifact_changes == ()
    assert tuple(change.key for change in patch.group_binding_changes) == ("group-a",)
    assert tuple(change.key for change in patch.group_history_changes) == ("group-a",)
    assert len(patch.group_binding_rows) == 1
    assert patch.group_binding_rows[0].open
    assert patch.group_binding_rows[0].epoch_id == patch.point.epoch_id
    assert patch.result.changed_group_ids == ("group-a",)
    assert patch.result.certificate_only_group_ids == ("group-a",)
    assert patch.result.work.group_certificate_only_changes == 1
    assert patch.claim_state_changes == ()
    assert patch.claim_artifact_changes == ()
    assert patch.claim_binding_changes == ()
    assert patch.claim_history_changes == ()
    assert patch.result.changed_claim_ids == ()
    assert patch.result.work.claims == 0
    assert patch.result.deltas == ()


def test_exact_replay_returns_references_without_new_certificate_writes() -> None:
    repository = _complete_group_repository()
    overlay = overlay_module.M5IncrementalOverlay.from_repository(repository)
    before = deepcopy(repository)
    after = deepcopy(repository)
    event = _cross_epoch_retain_event()
    apply_m5_event(after, event)
    first = overlay.apply_committed_event(event, before, after)
    revision = overlay.state_revision
    point = overlay.point
    score_index = overlay._score_index
    group_history = overlay._group_binding_history["group-a"]
    processed_events = dict(overlay._processed_events)
    state_images = (
        overlay.requirement_states,
        overlay.group_states,
        overlay.claim_states,
        overlay.answer_states,
        overlay.group_certificates,
        overlay.claim_certificates,
    )

    replay_patch = overlay.prepare_committed_event_patch(event, after, after)

    assert replay_patch.matching_patches == ()
    assert replay_patch.direct_patch is None
    for changes in (
        replay_patch.index_changes,
        replay_patch.hall_changes,
        replay_patch.requirement_local_changes,
        replay_patch.edge_count_changes,
        replay_patch.requirement_state_changes,
        replay_patch.group_state_changes,
        replay_patch.complete_group_changes,
        replay_patch.claim_state_changes,
        replay_patch.answer_count_changes,
        replay_patch.answer_state_changes,
        replay_patch.group_artifact_changes,
        replay_patch.group_binding_changes,
        replay_patch.group_history_changes,
        replay_patch.claim_artifact_changes,
        replay_patch.claim_binding_changes,
        replay_patch.claim_history_changes,
        replay_patch.observation_changes,
        replay_patch.observations_by_requirement_changes,
        replay_patch.observations_by_chunk_changes,
    ):
        assert changes == ()
    assert replay_patch.group_binding_rows == ()
    assert replay_patch.claim_binding_rows == ()

    replay = overlay.apply_prepared_event(replay_patch)

    assert replay.replayed
    assert replay.deltas == first.deltas
    assert replay.changed_requirement_ids == first.changed_requirement_ids
    assert replay.changed_group_ids == first.changed_group_ids
    assert replay.changed_claim_ids == first.changed_claim_ids
    assert replay.changed_answer_ids == first.changed_answer_ids
    assert replay.logical_output_digest == first.logical_output_digest
    assert replay.published_group_bindings == ()
    assert replay.published_claim_bindings == ()
    assert replay.work == overlay_module.M5OverlayWork()
    assert replay.direct_stats == MaintenanceStats()
    assert overlay.state_revision == revision
    assert overlay.point == point
    assert overlay._score_index is score_index
    assert overlay._group_binding_history["group-a"] is group_history
    assert overlay._processed_events == processed_events
    assert (
        overlay.requirement_states,
        overlay.group_states,
        overlay.claim_states,
        overlay.answer_states,
        overlay.group_certificates,
        overlay.claim_certificates,
    ) == state_images
