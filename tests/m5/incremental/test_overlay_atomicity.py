from __future__ import annotations

from copy import deepcopy

import pytest

from groundloop.domain import DecisionPolicy
from groundloop.errors import EventConflictError
from groundloop.events import ObserveEvent, PolicyChangeEvent, apply_event
from groundloop.incremental import MaintenanceStats
from groundloop.m5.claim_certificates import validate_claim_binding_history
from groundloop.m5.events import (
    ObserveRequirementEvent,
    RegisterGroupEvent,
    ReplaceGroupEvent,
    apply_m5_event,
)
from groundloop.m5.incremental_overlay import M5IncrementalOverlay, M5OverlayWork
from groundloop.m5.reference import compute_reference_states
from groundloop.m5.repository import M5Repository

from .helpers import (
    make_claim_observation,
    make_group,
    make_repository,
    make_requirement_observation,
)


def _neutralizing_event_fixture() -> tuple[
    M5Repository,
    M5IncrementalOverlay,
    M5Repository,
    ObserveRequirementEvent,
]:
    repository = make_repository()
    apply_m5_event(
        repository,
        RegisterGroupEvent(event_id="register-group", group=make_group()),
    )
    apply_m5_event(
        repository,
        ObserveRequirementEvent(
            event_id="observe-initial-support",
            observation=make_requirement_observation(
                observation_id="initial-support",
                requirement_id="group-a-requirement-0",
                chunk_id="chunk-a",
            ),
        ),
    )
    overlay = M5IncrementalOverlay.from_repository(repository)
    after = deepcopy(repository)
    event = ObserveRequirementEvent(
        event_id="neutralize-selected-support",
        observation=make_requirement_observation(
            observation_id="neutral-selected-support",
            requirement_id="group-a-requirement-0",
            chunk_id="chunk-a",
            scores=(0.1, 0.1, 0.8),
        ),
    )
    point = after.advance_semantic_revision()
    after.register_requirement_observation(event.observation, point)
    return repository, overlay, after, event


def _direct_event_fixture() -> tuple[
    M5Repository,
    M5IncrementalOverlay,
    M5Repository,
    ObserveEvent,
]:
    repository = make_repository()
    overlay = M5IncrementalOverlay.from_repository(repository)
    after = deepcopy(repository)
    event = ObserveEvent(
        event_id="observe-direct-support",
        observation=make_claim_observation(observation_id="direct-support"),
    )
    apply_event(after.base, event)
    _ = after.current_point
    return repository, overlay, after, event


def _overlay_audit_image(overlay: M5IncrementalOverlay) -> tuple[object, ...]:
    return (
        overlay.point,
        overlay.state_revision,
        overlay._policy,
        overlay.last_work,
        overlay.requirement_states,
        overlay.group_states,
        overlay.claim_states,
        overlay.answer_states,
        overlay.group_certificates,
        overlay.claim_certificates,
        overlay.group_certificate_artifacts_by_digest,
        overlay.claim_certificate_artifacts_by_digest,
        dict(overlay._hall_states),
        dict(overlay._requirement_locals),
        dict(overlay._edge_counts),
        dict(overlay._complete_groups_by_claim),
        dict(overlay._group_bindings),
        dict(overlay._claim_bindings),
        {
            key: overlay.group_binding_history(key)
            for key in overlay._group_binding_history
        },
        {
            key: overlay.claim_binding_history(key)
            for key in overlay._claim_binding_history
        },
        dict(overlay._observations),
        dict(overlay._observations_by_requirement),
        dict(overlay._observations_by_chunk),
        overlay._score_index,
        {
            group_id: index.audit_snapshot(
                point=overlay.point,
                decision_policy_version=overlay._policy.policy_version,
            )
            for group_id, index in overlay._group_indexes.items()
        },
        dict(overlay._processed_events),
        deepcopy(overlay.direct_engine),
    )


def _persistent_roots(overlay: M5IncrementalOverlay) -> tuple[object, ...]:
    index = overlay._group_indexes["group-a"]
    return (
        index,
        *(value.root for value in index._edge_observations.values()),
        *(value.root for value in index._hashes_by_mask.values()),
        overlay._complete_groups_by_claim["claim-a"].root,
        overlay._group_binding_history["group-a"],
        overlay._claim_binding_history["claim-a"],
        overlay._score_index,
        overlay.direct_engine._score_index,
    )


def _assert_overlay_matches_repository(
    overlay: M5IncrementalOverlay,
    repository: M5Repository,
) -> None:
    reference = compute_reference_states(repository)
    assert overlay.requirement_states == reference.requirements
    assert overlay.group_states == reference.groups
    assert overlay.claim_states == reference.claims
    assert overlay.answer_states == reference.answers
    assert overlay.matching_audit_issues() == {}


@pytest.mark.parametrize(
    "failure_stage",
    (
        "matching:group-a",
        "overlay:requirement_local:'group-a-requirement-0'",
        "overlay:group_state:'group-a'",
        "overlay:complete_group:'claim-a'",
        "overlay:claim_state:'claim-a'",
        "overlay:answer_count:'answer-a'",
        "overlay:group_artifact:'group-a'",
        "overlay:group_binding:'group-a'",
        "overlay:group_history:'group-a'",
        "overlay:claim_artifact:'claim-a'",
        "overlay:claim_binding:'claim-a'",
        "overlay:claim_history:'claim-a'",
        "overlay:score_index",
        "before_direct_publish",
        "direct:published",
    ),
)
def test_failure_rolls_back_every_overlay_surface_and_fresh_retry_succeeds(
    failure_stage: str,
) -> None:
    before, overlay, after, event = _neutralizing_event_fixture()
    image = _overlay_audit_image(overlay)
    roots = _persistent_roots(overlay)
    patch = overlay.prepare_committed_event_patch(event, before, after)
    assert _overlay_audit_image(overlay) == image

    def inject(stage: str) -> None:
        ledger_prefix = f"overlay:{failure_stage}:"
        if stage == failure_stage or (
            failure_stage.endswith("artifact_ledger")
            and stage.startswith(ledger_prefix)
        ):
            raise RuntimeError(f"injected failure at {stage}")

    with pytest.raises(RuntimeError, match="injected failure"):
        overlay.apply_prepared_event(patch, failure_injector=inject)

    assert patch._state == "failed"
    assert _overlay_audit_image(overlay) == image
    assert all(
        current is original
        for current, original in zip(
            _persistent_roots(overlay),
            roots,
            strict=True,
        )
    )
    assert overlay.matching_audit_issues() == {}

    retry = overlay.prepare_committed_event_patch(event, before, after)
    result = overlay.apply_prepared_event(retry)
    reference = compute_reference_states(after)
    assert overlay.requirement_states == reference.requirements
    assert overlay.group_states == reference.groups
    assert overlay.claim_states == reference.claims
    assert overlay.answer_states == reference.answers
    assert result.changed_group_ids == ("group-a",)
    assert result.changed_claim_ids == ("claim-a",)
    assert result.changed_answer_ids == ("answer-a",)
    assert overlay.matching_audit_issues() == {}


def test_every_emitted_neutralization_checkpoint_is_failure_atomic() -> None:
    probe_before, probe_overlay, probe_after, probe_event = (
        _neutralizing_event_fixture()
    )
    probe_patch = probe_overlay.prepare_committed_event_patch(
        probe_event,
        probe_before,
        probe_after,
    )
    emitted_stages: list[str] = []
    probe_overlay.apply_prepared_event(
        probe_patch,
        failure_injector=emitted_stages.append,
    )
    assert len(emitted_stages) == len(set(emitted_stages))
    assert {
        "overlay:hall:'group-a'",
        "overlay:requirement_state:'group-a-requirement-0'",
        "overlay:answer_state:'answer-a'",
        "overlay:requirement_reverse:'group-a-requirement-0'",
        "overlay:chunk_reverse:'chunk-a'",
        "direct:published",
    }.issubset(emitted_stages)
    assert any(stage.startswith("overlay:edge_count:") for stage in emitted_stages)

    for failure_stage in emitted_stages:
        before, overlay, after, event = _neutralizing_event_fixture()
        image = _overlay_audit_image(overlay)
        roots = _persistent_roots(overlay)
        patch = overlay.prepare_committed_event_patch(event, before, after)

        def inject(stage: str, target: str = failure_stage) -> None:
            if stage == target:
                raise RuntimeError(f"injected failure at {stage}")

        with pytest.raises(RuntimeError, match="injected failure"):
            overlay.apply_prepared_event(patch, failure_injector=inject)

        assert patch._state == "failed"
        assert _overlay_audit_image(overlay) == image
        assert all(
            current is original
            for current, original in zip(
                _persistent_roots(overlay),
                roots,
                strict=True,
            )
        )
        retry = overlay.prepare_committed_event_patch(event, before, after)
        overlay.apply_prepared_event(retry)
        _assert_overlay_matches_repository(overlay, after)


def test_every_direct_engine_checkpoint_is_failure_atomic() -> None:
    probe_before, probe_overlay, probe_after, probe_event = _direct_event_fixture()
    probe_patch = probe_overlay.prepare_committed_event_patch(
        probe_event,
        probe_before,
        probe_after,
    )
    emitted_stages: list[str] = []
    probe_overlay.apply_prepared_event(
        probe_patch,
        failure_injector=emitted_stages.append,
    )
    direct_stages = tuple(
        stage for stage in emitted_stages if stage.startswith("direct:")
    )
    assert direct_stages == (
        "direct:active_label:direct-support",
        "direct:contribution:direct-support",
        "direct:accumulator:claim-a",
        "direct:claim_state:claim-a",
        "direct:certificate:claim-a",
        "direct:answer_counts:answer-a",
        "direct:answer_state:answer-a",
        "direct:score_index:direct-support",
        "direct:published",
    )

    for failure_stage in direct_stages:
        before, overlay, after, event = _direct_event_fixture()
        image = _overlay_audit_image(overlay)
        patch = overlay.prepare_committed_event_patch(event, before, after)

        def inject(stage: str, target: str = failure_stage) -> None:
            if stage == target:
                raise RuntimeError(f"injected failure at {stage}")

        with pytest.raises(RuntimeError, match="injected failure"):
            overlay.apply_prepared_event(patch, failure_injector=inject)

        assert patch._state == "failed"
        assert _overlay_audit_image(overlay) == image
        retry = overlay.prepare_committed_event_patch(event, before, after)
        overlay.apply_prepared_event(retry)
        _assert_overlay_matches_repository(overlay, after)


def test_structural_group_index_insert_rolls_back_before_fresh_retry() -> None:
    repository = make_repository()
    overlay = M5IncrementalOverlay.from_repository(repository)
    before = deepcopy(repository)
    after = deepcopy(repository)
    event = RegisterGroupEvent(event_id="register-group", group=make_group())
    apply_m5_event(after, event)
    image = _overlay_audit_image(overlay)
    patch = overlay.prepare_committed_event_patch(event, before, after)

    def inject(stage: str) -> None:
        if stage == "overlay:group_index:'group-a'":
            raise RuntimeError(f"injected failure at {stage}")

    with pytest.raises(RuntimeError, match="injected failure"):
        overlay.apply_prepared_event(patch, failure_injector=inject)

    assert patch._state == "failed"
    assert _overlay_audit_image(overlay) == image
    assert "group-a" not in overlay._group_indexes
    retry = overlay.prepare_committed_event_patch(event, before, after)
    overlay.apply_prepared_event(retry)
    _assert_overlay_matches_repository(overlay, after)


def test_failed_complete_group_replacement_preserves_old_group_until_retry() -> None:
    repository = make_repository()
    apply_m5_event(
        repository,
        RegisterGroupEvent(event_id="register-group", group=make_group()),
    )
    apply_m5_event(
        repository,
        ObserveRequirementEvent(
            event_id="observe-support",
            observation=make_requirement_observation(
                observation_id="selected-support",
                requirement_id="group-a-requirement-0",
            ),
        ),
    )
    overlay = M5IncrementalOverlay.from_repository(repository)
    before = deepcopy(repository)
    after = deepcopy(repository)
    event = ReplaceGroupEvent(
        event_id="replace-group",
        old_group_version_id="group-a",
        successor=make_group(
            group_id="group-a-v2",
            family_id="family-a",
            texts=("successor requirement",),
            requirement_ids=("group-a-v2-requirement-0",),
            predecessors=("group-a-requirement-0",),
            supersedes_group_id="group-a",
        ),
    )
    apply_m5_event(after, event)
    image = _overlay_audit_image(overlay)
    roots = _persistent_roots(overlay)
    old_artifact = overlay.group_certificates["group-a"]
    patch = overlay.prepare_committed_event_patch(event, before, after)

    def inject(stage: str) -> None:
        if stage == "overlay:group_index:'group-a-v2'":
            raise RuntimeError(f"injected failure at {stage}")

    with pytest.raises(RuntimeError, match="injected failure"):
        overlay.apply_prepared_event(patch, failure_injector=inject)

    assert patch._state == "failed"
    assert _overlay_audit_image(overlay) == image
    assert all(
        current is original
        for current, original in zip(
            _persistent_roots(overlay),
            roots,
            strict=True,
        )
    )
    assert overlay.group_state("group-a").complete
    assert overlay.group_certificates["group-a"] == old_artifact
    assert "group-a-v2" not in overlay.group_states

    retry = overlay.prepare_committed_event_patch(event, before, after)
    result = overlay.apply_prepared_event(retry)
    _assert_overlay_matches_repository(overlay, after)
    assert set(result.changed_group_ids) == {"group-a", "group-a-v2"}
    assert "group-a" not in overlay.group_states
    assert not overlay.group_state("group-a-v2").complete
    assert overlay.group_certificates == {}


def test_legacy_event_exact_and_conflicting_replay_are_atomic() -> None:
    repository = make_repository()
    overlay = M5IncrementalOverlay.from_repository(repository)
    before = deepcopy(repository)
    event = ObserveEvent(
        event_id="observe-direct-support",
        observation=make_claim_observation(observation_id="direct-support"),
    )
    apply_event(repository.base, event)
    _ = repository.current_point
    first = overlay.apply_committed_event(event, before, repository)
    image = _overlay_audit_image(overlay)

    replay_patch = overlay.prepare_committed_event_patch(
        event,
        repository,
        repository,
    )
    emitted_stages: list[str] = []
    replay = overlay.apply_prepared_event(
        replay_patch,
        failure_injector=emitted_stages.append,
    )

    assert replay.replayed
    assert replay_patch._state == "applied"
    assert replay.changed_claim_ids == first.changed_claim_ids
    assert replay.changed_answer_ids == first.changed_answer_ids
    assert replay.deltas == first.deltas
    assert replay.logical_output_digest == first.logical_output_digest
    assert replay.work == M5OverlayWork()
    assert replay.direct_stats == MaintenanceStats()
    assert emitted_stages == []
    assert _overlay_audit_image(overlay) == image

    conflict = ObserveEvent(
        event_id=event.event_id,
        observation=make_claim_observation(
            observation_id="conflicting-direct-support",
            chunk_id="chunk-b",
        ),
    )
    with pytest.raises(EventConflictError, match="another payload"):
        overlay.prepare_committed_event_patch(conflict, repository, repository)
    assert _overlay_audit_image(overlay) == image


@pytest.mark.parametrize(
    "ledger_name",
    ("group_artifact_ledger", "claim_artifact_ledger"),
)
def test_artifact_ledger_insert_is_failure_atomic(ledger_name: str) -> None:
    before, overlay, _, _ = _neutralizing_event_fixture()
    after = deepcopy(before)
    event = PolicyChangeEvent(
        event_id=f"policy-rebind-{ledger_name}",
        policy=DecisionPolicy("policy-v2", 0.8, 0.8),
    )
    apply_event(after.base, event)
    _ = after.current_point
    image = _overlay_audit_image(overlay)
    roots = _persistent_roots(overlay)
    patch = overlay.prepare_committed_event_patch(event, before, after)
    assert len(patch.group_artifact_ledger_changes) == 1
    assert len(patch.claim_artifact_ledger_changes) == 1
    group_change = patch.group_artifact_ledger_changes[0]
    claim_change = patch.claim_artifact_ledger_changes[0]
    assert group_change.before is None and group_change.after is not None
    assert claim_change.before is None and claim_change.after is not None
    assert group_change.key == group_change.after.certificate_digest
    assert claim_change.key == claim_change.after.certificate_digest

    def inject(stage: str) -> None:
        if stage.startswith(f"overlay:{ledger_name}:"):
            raise RuntimeError(f"injected failure at {stage}")

    with pytest.raises(RuntimeError, match="injected failure"):
        overlay.apply_prepared_event(patch, failure_injector=inject)

    assert patch._state == "failed"
    assert _overlay_audit_image(overlay) == image
    assert all(
        current is original
        for current, original in zip(
            _persistent_roots(overlay),
            roots,
            strict=True,
        )
    )
    retry = overlay.prepare_committed_event_patch(event, before, after)
    overlay.apply_prepared_event(retry)
    assert len(overlay.group_certificate_artifacts_by_digest) == 2
    assert len(overlay.claim_certificate_artifacts_by_digest) == 2
    assert (
        overlay.group_certificate_artifacts_by_digest[group_change.key]
        == group_change.after
    )
    assert (
        overlay.claim_certificate_artifacts_by_digest[claim_change.key]
        == claim_change.after
    )
    assert overlay._group_bindings["group-a"].certificate_digest == group_change.key
    assert overlay._claim_bindings["claim-a"].certificate_digest == claim_change.key
    assert (
        validate_claim_binding_history(
            overlay.claim_binding_history("claim-a"),
            overlay.claim_certificate_artifacts_by_digest,
        )
        == ()
    )
    assert all(
        binding.certificate_digest in overlay.group_certificate_artifacts_by_digest
        for binding in overlay.group_binding_history("group-a")
    )


def test_conflicting_replay_is_rejected_without_mutation() -> None:
    before, overlay, after, event = _neutralizing_event_fixture()
    overlay.apply_committed_event(event, before, after)
    image = _overlay_audit_image(overlay)
    conflict = ObserveRequirementEvent(
        event_id=event.event_id,
        observation=make_requirement_observation(
            observation_id="conflicting-observation",
            requirement_id="group-a-requirement-0",
            chunk_id="chunk-a",
        ),
    )

    with pytest.raises(EventConflictError, match="another payload"):
        overlay.prepare_committed_event_patch(conflict, after, after)

    assert _overlay_audit_image(overlay) == image
