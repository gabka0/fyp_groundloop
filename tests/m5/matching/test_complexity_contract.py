from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pytest

import groundloop.m5.matching as matching_module
from groundloop.errors import ValidationError
from groundloop.m5.matching import (
    MatchingWorkCounters,
    affected_group_matching,
    affected_group_matching_canonical,
    apply_hash_mask_transition,
    apply_hash_mask_transitions,
    initialize_hall_mask_state,
    policy_range_probe_work,
    requirement_observation_work,
    touched_state_work,
)

ROOT = Path(__file__).resolve().parents[3]
PROOF = ROOT / "docs/workstreams/m5_matching/PROOF_AND_COMPLEXITY.md"


def test_matching_module_does_not_import_either_full_state_oracle() -> None:
    tree = ast.parse(inspect.getsource(matching_module))
    imported_modules = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    }

    assert "groundloop.reference" not in imported_modules
    assert "groundloop.m5.reference" not in imported_modules
    assert not any(name.startswith("groundloop.postgres") for name in imported_modules)


@pytest.mark.parametrize("requirement_count", range(1, 9))
def test_one_mask_transition_has_the_frozen_exponential_cap(
    requirement_count: int,
) -> None:
    full_mask = (1 << requirement_count) - 1
    state = initialize_hall_mask_state(
        requirement_count,
        {"hash": full_mask},
    ).state

    result = apply_hash_mask_transition(
        state,
        old_mask=full_mask,
        new_mask=0,
    )

    assert result.work.hash_mask_transitions == 1
    assert result.work.hall_subset_entries_examined == full_mask
    assert result.work.hall_neighbor_entries_changed <= full_mask
    assert result.work.hall_deficiency_entries_examined == full_mask


def test_augmenting_baseline_counters_are_bounded_by_r_times_edges() -> None:
    requirement_count = 8
    masks = {f"h{ordinal}": 255 for ordinal in range(8)}
    edge_count = sum(mask.bit_count() for mask in masks.values())

    result = affected_group_matching(requirement_count, masks)

    assert result.complete
    assert result.work.augmenting_searches == requirement_count
    assert result.work.augmenting_requirement_visits >= requirement_count
    assert result.work.augmenting_edge_visits <= requirement_count * edge_count
    assert result.work.canonical_sort_items == len(masks)


def test_preordered_matching_kernel_has_no_sort_charge() -> None:
    result = affected_group_matching_canonical(
        2,
        (("a", 0b01), ("b", 0b11)),
    )

    assert result.complete
    assert result.work.canonical_sort_items == 0
    with pytest.raises(ValidationError, match="ordered"):
        affected_group_matching_canonical(
            2,
            (("b", 0b11), ("a", 0b01)),
        )


@pytest.mark.parametrize(
    "operation",
    (
        affected_group_matching_canonical,
        matching_module._affected_group_matching_from_canonical,
        apply_hash_mask_transitions,
    ),
)
def test_bounded_kernels_do_not_hide_a_sort(operation: object) -> None:
    tree = ast.parse(inspect.getsource(operation))
    called_names = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }

    assert "sorted" not in called_names


def test_zero_candidate_policy_probe_is_not_zero_work() -> None:
    work = policy_range_probe_work(
        changed_threshold_dimensions=1,
        candidate_observations=0,
    )

    assert work.ordered_policy_range_probes == 1
    assert work.ordered_index_operations == 1
    assert work.policy_candidate_observations == 0


def test_u_is_explicit_even_when_a_processed_flip_creates_no_edge() -> None:
    work = requirement_observation_work(
        changes_processed=1,
        ordered_index_operations=2,
    )

    assert work.requirement_observation_changes_processed == 1
    assert work.contribution_additions == 0
    assert work.contribution_removals == 0
    assert work.ordered_index_operations == 2


def test_work_counters_support_signed_differences_and_release_guard() -> None:
    before = MatchingWorkCounters(hash_mask_transitions=4, groups_touched=3)
    after = MatchingWorkCounters(hash_mask_transitions=1, groups_touched=2)

    delta = after - before

    assert delta.hash_mask_transitions == -3
    assert delta.groups_touched == -1
    assert before + delta == after
    with pytest.raises(ValidationError, match="hash_mask_transitions"):
        delta.assert_nonnegative()
    after.assert_nonnegative()


def test_transaction_level_touches_are_supplied_once_after_id_deduplication() -> None:
    two_local_operations = MatchingWorkCounters(group_local_state_operations=2)
    unique_touches = touched_state_work(
        groups_touched=1,
        claims_touched=1,
        answers_touched=0,
        output_bytes=128,
    )

    combined = two_local_operations + unique_touches

    assert combined.group_local_state_operations == 2
    assert combined.groups_touched == 1
    assert combined.claims_touched == 1
    assert combined.output_bytes == 128


def test_touched_status_and_output_terms_are_explicit_counters() -> None:
    fields = MatchingWorkCounters.__dataclass_fields__

    for term in (
        "ordered_policy_range_probes",
        "requirement_observation_changes_processed",
        "policy_candidate_observations",
        "hash_mask_transitions",
        "certificate_repairs",
        "certificate_reconstructions",
        "group_local_state_operations",
        "groups_touched",
        "claims_touched",
        "answers_touched",
        "claim_status_changes",
        "answer_status_changes",
        "output_bytes",
    ):
        assert term in fields


def test_proof_states_the_bound_and_rejects_the_main_overclaims() -> None:
    proof = PROOF.read_text(encoding="utf-8")

    for term in ("`U`", "`P`", "`Z`", "`R`", "`Y`", "`G_touched`"):
        assert term in proof
    assert "O(r^3*2^r)" in proof
    assert "does not establish superiority" in proof
    assert "not a measured stable-update operation" in proof
    assert "100,000-committed-event" in proof
