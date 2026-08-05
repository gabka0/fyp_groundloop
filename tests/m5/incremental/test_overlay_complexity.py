from __future__ import annotations

import hashlib
from copy import deepcopy

from groundloop.domain import DecisionPolicy
from groundloop.events import PolicyChangeEvent, apply_event
from groundloop.m5 import incremental_overlay as overlay_module
from groundloop.m5.events import (
    ObserveRequirementEvent,
    RegisterGroupEvent,
    apply_m5_event,
)
from groundloop.m5.incremental_overlay import M5IncrementalOverlay

from .helpers import make_group, make_repository, make_requirement_observation


def _assert_exact_publication_accounting(
    patch: overlay_module.PreparedM5OverlayPatch,
) -> None:
    group_keys = {
        change.key
        for changes in (
            patch.group_state_changes,
            patch.group_artifact_changes,
            patch.group_binding_changes,
            patch.group_history_changes,
        )
        for change in changes
    }
    claim_keys = {
        change.key
        for changes in (
            patch.claim_state_changes,
            patch.claim_artifact_changes,
            patch.claim_binding_changes,
            patch.claim_history_changes,
        )
        for change in changes
    }
    answer_keys = {
        change.key
        for changes in (patch.answer_count_changes, patch.answer_state_changes)
        for change in changes
    }
    work = patch.result.work.matching
    assert work.groups_touched == len(group_keys)
    assert work.claims_touched == len(claim_keys)
    assert work.answers_touched == len(answer_keys)

    output_records: list[tuple[str, str, object]] = []
    for kind, changes in (
        ("requirement_state", patch.requirement_state_changes),
        ("group_state", patch.group_state_changes),
        ("claim_state", patch.claim_state_changes),
        ("answer_state", patch.answer_state_changes),
        ("group_certificate", patch.group_artifact_changes),
        ("claim_certificate", patch.claim_artifact_changes),
    ):
        output_records.extend((kind, change.key, change.after) for change in changes)
    output_records.extend(
        ("group_binding", binding.group_version_id, binding)
        for binding in patch.group_binding_rows
    )
    output_records.extend(
        ("claim_binding", binding.claim_id, binding)
        for binding in patch.claim_binding_rows
    )
    output_records.extend(
        ("status_delta", delta.object_id, delta) for delta in patch.result.deltas
    )
    output_image = overlay_module._logical_output_image(output_records)
    assert work.output_bytes == len(output_image)
    assert (
        patch.result.logical_output_digest == hashlib.sha256(output_image).hexdigest()
    )


def test_policy_flip_charges_u_as_well_as_candidate_probe_and_mask_work() -> None:
    repository = make_repository()
    apply_m5_event(
        repository,
        RegisterGroupEvent(event_id="register-group", group=make_group()),
    )
    apply_m5_event(
        repository,
        ObserveRequirementEvent(
            event_id="observe-neutral",
            observation=make_requirement_observation(
                observation_id="threshold-candidate",
                requirement_id="group-a-requirement-0",
                scores=(0.75, 0.1, 0.15),
            ),
        ),
    )
    overlay = M5IncrementalOverlay.from_repository(repository)
    before = deepcopy(repository)
    after = deepcopy(repository)
    event = PolicyChangeEvent(
        event_id="lower-support-threshold",
        policy=DecisionPolicy("policy-v2", 0.7, 0.8),
    )
    apply_event(after.base, event)
    _ = after.current_point

    patch = overlay.prepare_committed_event_patch(event, before, after)
    _assert_exact_publication_accounting(patch)
    result = overlay.apply_prepared_event(patch)
    work = result.work.matching

    assert work.requirement_observation_changes_processed == 1
    assert work.ordered_policy_range_probes == 1
    assert work.policy_candidate_observations == 1
    assert work.hash_mask_transitions == 1
    assert work.groups_touched == len(result.changed_group_ids) == 1
    assert work.claims_touched == len(result.changed_claim_ids) == 1
    assert work.answers_touched == len(result.changed_answer_ids) == 1


def test_refute_to_neutral_policy_flip_charges_u_without_group_work() -> None:
    repository = make_repository()
    apply_m5_event(
        repository,
        RegisterGroupEvent(event_id="register-group", group=make_group()),
    )
    apply_m5_event(
        repository,
        ObserveRequirementEvent(
            event_id="observe-refute",
            observation=make_requirement_observation(
                observation_id="refute-candidate",
                requirement_id="group-a-requirement-0",
                scores=(0.05, 0.85, 0.10),
            ),
        ),
    )
    overlay = M5IncrementalOverlay.from_repository(repository)
    before = deepcopy(repository)
    after = deepcopy(repository)
    event = PolicyChangeEvent(
        event_id="raise-refute-threshold",
        policy=DecisionPolicy("policy-v2", 0.8, 0.9),
    )
    apply_event(after.base, event)
    _ = after.current_point

    patch = overlay.prepare_committed_event_patch(event, before, after)
    _assert_exact_publication_accounting(patch)
    result = overlay.apply_prepared_event(patch)
    work = result.work.matching

    assert work.ordered_policy_range_probes == 1
    assert work.policy_candidate_observations == 1
    assert work.requirement_observation_changes_processed == 1
    assert work.hash_mask_transitions == 0
    assert work.groups_touched == 0
    assert result.changed_group_ids == ()
    assert work.claims_touched == 1


def test_two_policy_probes_deduplicate_one_overlapping_candidate() -> None:
    repository = make_repository()
    apply_m5_event(
        repository,
        RegisterGroupEvent(event_id="register-group", group=make_group()),
    )
    apply_m5_event(
        repository,
        ObserveRequirementEvent(
            event_id="observe-overlap",
            observation=make_requirement_observation(
                observation_id="overlapping-candidate",
                requirement_id="group-a-requirement-0",
                scores=(0.45, 0.44, 0.11),
            ),
        ),
    )
    overlay = M5IncrementalOverlay.from_repository(repository)
    before = deepcopy(repository)
    after = deepcopy(repository)
    event = PolicyChangeEvent(
        event_id="lower-both-thresholds",
        policy=DecisionPolicy("policy-v2", 0.4, 0.4),
    )
    apply_event(after.base, event)
    _ = after.current_point

    patch = overlay.prepare_committed_event_patch(event, before, after)
    _assert_exact_publication_accounting(patch)
    result = overlay.apply_prepared_event(patch)
    work = result.work.matching

    assert work.ordered_policy_range_probes == 2
    assert work.policy_candidate_observations == 1
    assert work.requirement_observation_changes_processed == 1
    assert work.hash_mask_transitions == 1


def test_zero_candidate_policy_rebind_does_not_touch_incomplete_groups() -> None:
    repository = make_repository()
    for index in range(5):
        group_id = f"group-{index}"
        apply_m5_event(
            repository,
            RegisterGroupEvent(
                event_id=f"register-{group_id}",
                group=make_group(
                    group_id=group_id,
                    family_id=f"family-{index}",
                    texts=(f"requirement {index}",),
                    requirement_ids=(f"requirement-{index}",),
                ),
            ),
        )
    overlay = M5IncrementalOverlay.from_repository(repository)
    before = deepcopy(repository)
    after = deepcopy(repository)
    event = PolicyChangeEvent(
        event_id="empty-policy-range",
        policy=DecisionPolicy("policy-v2", 0.81, 0.8),
    )
    apply_event(after.base, event)
    _ = after.current_point

    result = overlay.apply_committed_event(event, before, after)
    work = result.work.matching

    assert work.ordered_policy_range_probes == 1
    assert work.policy_candidate_observations == 0
    assert work.requirement_observation_changes_processed == 0
    assert work.groups_touched == 0
    assert result.changed_group_ids == ()
    assert result.certificate_only_group_ids == ()
    assert work.claims_touched == 1
    assert result.changed_claim_ids == ("claim-a",)
    assert result.certificate_only_claim_ids == ("claim-a",)
    assert work.canonical_sort_items == 0


def test_structural_group_registration_charges_hall_initialization() -> None:
    repository = make_repository()
    overlay = M5IncrementalOverlay.from_repository(repository)
    before = deepcopy(repository)
    after = deepcopy(repository)
    event = RegisterGroupEvent(
        event_id="register-r3-group",
        group=make_group(
            group_id="group-r3",
            family_id="family-r3",
            texts=("first", "second", "third"),
            requirement_ids=("r3-0", "r3-1", "r3-2"),
        ),
    )
    apply_m5_event(after, event)

    result = overlay.apply_committed_event(event, before, after)
    work = result.work.matching

    assert work.hall_zeta_additions == 12
    assert work.hall_subset_entries_examined == 7
    assert work.hall_deficiency_entries_examined == 7
    assert work.hash_masks_initialized == 0
    assert work.hash_mask_transitions == 0
    assert work.groups_touched == 1
    assert result.changed_group_ids == ("group-r3",)
    assert work.canonical_sort_items == 0


def test_zero_candidate_policy_rebind_charges_complete_group_only() -> None:
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
                observation_id="stable-support",
                requirement_id="group-a-requirement-0",
                scores=(0.9, 0.05, 0.05),
            ),
        ),
    )
    overlay = M5IncrementalOverlay.from_repository(repository)
    before = deepcopy(repository)
    after = deepcopy(repository)
    event = PolicyChangeEvent(
        event_id="zero-candidate-complete-rebind",
        policy=DecisionPolicy("policy-v2", 0.81, 0.8),
    )
    apply_event(after.base, event)
    _ = after.current_point

    result = overlay.apply_committed_event(event, before, after)
    work = result.work.matching

    assert work.ordered_policy_range_probes == 1
    assert work.policy_candidate_observations == 0
    assert work.requirement_observation_changes_processed == 0
    assert work.policy_rebindings == 1
    assert work.groups_touched == 1
    assert work.claims_touched == 1
    assert result.certificate_only_group_ids == ("group-a",)
    assert result.certificate_only_claim_ids == ("claim-a",)
    assert result.deltas == ()
    assert work.canonical_sort_items == 0


def test_completion_reconstruction_exposes_every_bounded_work_family() -> None:
    repository = make_repository()
    overlay = M5IncrementalOverlay.from_repository(repository)

    before_register = deepcopy(repository)
    group = make_group(
        group_id="group-r2",
        family_id="family-r2",
        texts=("first", "second"),
        requirement_ids=("r2-0", "r2-1"),
    )
    register = RegisterGroupEvent("register-r2", group)
    apply_m5_event(repository, register)
    register_result = overlay.apply_committed_event(
        register,
        before_register,
        repository,
    )

    before_first = deepcopy(repository)
    first = ObserveRequirementEvent(
        "observe-r2-first",
        make_requirement_observation(
            observation_id="r2-first-support",
            requirement_id="r2-0",
            chunk_id="chunk-a",
        ),
    )
    apply_m5_event(repository, first)
    first_result = overlay.apply_committed_event(first, before_first, repository)

    before_second = deepcopy(repository)
    second = ObserveRequirementEvent(
        "observe-r2-second",
        make_requirement_observation(
            observation_id="r2-second-support",
            requirement_id="r2-1",
            chunk_id="chunk-b",
        ),
    )
    apply_m5_event(repository, second)
    second_result = overlay.apply_committed_event(second, before_second, repository)

    assert second_result.work.matching.certificate_reconstructions == 1
    assert second_result.work.matching.certificate_repairs == 0

    aggregate = register_result.work + first_result.work + second_result.work
    work = aggregate.matching
    positive_terms = (
        work.contribution_additions,
        work.requirement_observation_changes_processed,
        work.ordered_index_operations,
        work.edge_refcount_keys_updated,
        work.distinct_edge_crossings,
        work.hash_mask_transitions,
        work.hall_zeta_additions,
        work.hall_subset_entries_examined,
        work.hall_neighbor_entries_changed,
        work.hall_deficiency_entries_examined,
        work.certificate_reconstructions,
        work.representative_hashes_read,
        work.representative_observations_read,
        work.augmenting_searches,
        work.augmenting_requirement_visits,
        work.augmenting_edge_visits,
        work.certificate_digest_input_bytes,
        work.group_local_state_operations,
        work.groups_touched,
        work.claims_touched,
        work.answers_touched,
        work.claim_status_changes,
        work.answer_status_changes,
        work.output_bytes,
    )
    assert all(value > 0 for value in positive_terms)
    assert work.canonical_sort_items > 0
    assert second_result.work.matching.groups_touched == len(
        second_result.changed_group_ids
    )
    assert second_result.work.matching.claims_touched == len(
        second_result.changed_claim_ids
    )
    assert second_result.work.matching.answers_touched == len(
        second_result.changed_answer_ids
    )
