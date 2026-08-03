from __future__ import annotations

import random

import pytest

from groundloop.errors import ValidationError
from groundloop.m5.matching import (
    HallMaskState,
    HashMaskTransition,
    apply_hash_mask_transition,
    apply_hash_mask_transitions,
    initialize_hall_mask_state,
    validate_hall_mask_state,
)


def _base_masks(requirement_count: int) -> dict[str, int]:
    return {f"mask-{mask}": mask for mask in range(1, 1 << requirement_count)}


def test_every_old_new_mask_pair_through_r4_is_exact() -> None:
    checked = 0
    for requirement_count in range(1, 5):
        full_mask = (1 << requirement_count) - 1
        for old_mask in range(full_mask + 1):
            for new_mask in range(full_mask + 1):
                masks = _base_masks(requirement_count)
                if old_mask == 0:
                    moved_hash = "new-hash"
                else:
                    moved_hash = f"mask-{old_mask}"
                initial = initialize_hall_mask_state(
                    requirement_count,
                    masks,
                ).state
                result = apply_hash_mask_transition(
                    initial,
                    old_mask=old_mask,
                    new_mask=new_mask,
                )

                expected_masks = dict(masks)
                if old_mask != new_mask:
                    if old_mask != 0:
                        del expected_masks[moved_hash]
                    if new_mask != 0:
                        expected_masks[moved_hash] = new_mask
                expected = initialize_hall_mask_state(
                    requirement_count,
                    expected_masks,
                ).state

                assert result.state == expected
                assert not validate_hall_mask_state(result.state)
                if old_mask == new_mask:
                    assert result.state is initial
                    assert result.work.hash_mask_transitions == 0
                    assert result.work.hall_subset_entries_examined == 0
                else:
                    assert result.work.hash_mask_transitions == 1
                    assert (
                        result.work.distinct_edge_crossings
                        == (old_mask ^ new_mask).bit_count()
                    )
                    assert result.work.hall_subset_entries_examined == full_mask
                    assert result.work.hall_deficiency_entries_examined == full_mask
                    expected_changed = sum(
                        bool(subset & old_mask) != bool(subset & new_mask)
                        for subset in range(1, full_mask + 1)
                    )
                    assert result.work.hall_neighbor_entries_changed == expected_changed
                checked += 1

    assert checked == sum(
        (1 << requirement_count) ** 2 for requirement_count in range(1, 5)
    )


def test_seeded_random_transition_sequences_through_r8() -> None:
    rng = random.Random(20260802)
    for requirement_count in range(5, 9):
        masks = {
            f"h{ordinal}": rng.randrange(1, 1 << requirement_count)
            for ordinal in range(64)
        }
        state = initialize_hall_mask_state(requirement_count, masks).state
        for _ in range(1_000):
            text_hash = f"h{rng.randrange(80)}"
            old_mask = masks.get(text_hash, 0)
            new_mask = rng.randrange(1 << requirement_count)
            result = apply_hash_mask_transition(
                state,
                old_mask=old_mask,
                new_mask=new_mask,
            )
            if new_mask:
                masks[text_hash] = new_mask
            else:
                masks.pop(text_hash, None)
            state = result.state
            expected = initialize_hall_mask_state(requirement_count, masks).state
            assert state == expected
            assert not validate_hall_mask_state(state)


def test_batch_is_canonical_and_counts_one_group_touch() -> None:
    state = initialize_hall_mask_state(3, {"a": 0b001, "b": 0b010}).state
    transitions = (
        HashMaskTransition("b", 0b010, 0b110),
        HashMaskTransition("a", 0b001, 0b101),
        HashMaskTransition("c", 0, 0),
    )

    forward = apply_hash_mask_transitions(state, transitions)
    reverse = apply_hash_mask_transitions(state, reversed(transitions))

    assert forward == reverse
    assert (
        forward.state
        == initialize_hall_mask_state(
            3,
            {"a": 0b101, "b": 0b110},
        ).state
    )
    assert forward.work.hash_mask_transitions == 2
    assert forward.work.group_local_state_operations == 1
    assert forward.work.groups_touched == 0


def test_batch_rejects_duplicate_hash_and_leaves_input_unchanged() -> None:
    state = initialize_hall_mask_state(2, {"a": 0b01}).state
    before = state

    with pytest.raises(ValidationError, match="duplicate coalesced transition"):
        apply_hash_mask_transitions(
            state,
            (
                HashMaskTransition("a", 0b01, 0b10),
                HashMaskTransition("a", 0b10, 0b11),
            ),
        )

    assert state == before


def test_transition_rejects_absent_old_bucket() -> None:
    state = initialize_hall_mask_state(2, {"a": 0b01}).state
    with pytest.raises(ValidationError, match="empty mask bucket"):
        apply_hash_mask_transition(state, old_mask=0b10, new_mask=0)


def test_audit_validator_detects_histogram_neighbor_divergence() -> None:
    valid = initialize_hall_mask_state(2, {"a": 0b01}).state
    corrupted = HallMaskState(
        requirement_count=2,
        mask_histogram=(0, 0, 1, 0),
        # Internally algebraically consistent, but not consistent with C.
        neighbor_counts=valid.neighbor_counts,
        deficiencies=valid.deficiencies,
        maximum_deficiency=valid.maximum_deficiency,
        matching_size=valid.matching_size,
        distinct_hash_count=1,
    )

    issues = validate_hall_mask_state(corrupted)
    assert "neighbor_counts_do_not_match_histogram" in issues


def test_initialization_counter_is_exact() -> None:
    requirement_count = 8
    result = initialize_hall_mask_state(
        requirement_count,
        {"a": 1, "b": 255},
    )

    assert result.work.hash_masks_initialized == 2
    assert result.work.hall_zeta_additions == requirement_count * (1 << 7)
    assert result.work.hall_subset_entries_examined == 255
    assert result.work.hall_deficiency_entries_examined == 255


def test_matching_only_loss_is_detected_without_requirement_zero_crossing() -> None:
    masks = {
        "a": 0b0111,
        "b": 0b0111,
        "c": 0b1100,
        "d": 0b1000,
    }
    before = initialize_hall_mask_state(4, masks).state
    witness_counts_before = tuple(
        sum(bool(mask & (1 << ordinal)) for mask in masks.values())
        for ordinal in range(4)
    )

    result = apply_hash_mask_transition(
        before,
        old_mask=0b1100,
        new_mask=0b1000,
    )
    masks["c"] = 0b1000
    witness_counts_after = tuple(
        sum(bool(mask & (1 << ordinal)) for mask in masks.values())
        for ordinal in range(4)
    )

    assert before.complete
    assert not result.state.complete
    assert before.matching_size == 4
    assert result.state.matching_size == 3
    assert all(count > 0 for count in witness_counts_before)
    assert all(count > 0 for count in witness_counts_after)
    assert result.work.distinct_edge_crossings == 1
