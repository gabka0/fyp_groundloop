from __future__ import annotations

from itertools import permutations

import pytest

from groundloop.errors import ValidationError
from groundloop.m5.matching import (
    affected_group_matching,
    initialize_hall_mask_state,
    validate_hall_mask_state,
)


def _graph_masks(
    requirement_count: int,
    hash_count: int,
    edge_bits: int,
) -> dict[str, int]:
    masks: dict[str, int] = {}
    for hash_ordinal in range(hash_count):
        mask = 0
        for requirement_ordinal in range(requirement_count):
            bit_ordinal = hash_ordinal * requirement_count + requirement_ordinal
            if edge_bits & (1 << bit_ordinal):
                mask |= 1 << requirement_ordinal
        # A right-side vertex with no incident edge is absent from H_g and has
        # no effect on matching or Hall deficiency.
        if mask:
            masks[f"h{hash_ordinal}"] = mask
    return masks


def _assert_valid_matching(
    requirement_count: int,
    masks: dict[str, int],
) -> int:
    result = affected_group_matching(requirement_count, masks)
    ordinals = tuple(pair.requirement_ordinal for pair in result.pairs)
    selected_hashes = tuple(pair.text_hash for pair in result.pairs)
    assert ordinals == tuple(sorted(set(ordinals)))
    assert len(set(selected_hashes)) == len(selected_hashes)
    for pair in result.pairs:
        assert masks[pair.text_hash] & (1 << pair.requirement_ordinal)
    assert result.complete is (result.matching_size == requirement_count)
    return result.matching_size


def test_every_simple_graph_through_r4_h4_matches_hall_state() -> None:
    graph_count = 0
    for requirement_count in range(1, 5):
        for hash_count in range(5):
            edge_count = requirement_count * hash_count
            for edge_bits in range(1 << edge_count):
                masks = _graph_masks(
                    requirement_count,
                    hash_count,
                    edge_bits,
                )
                matching_size = _assert_valid_matching(requirement_count, masks)
                hall = initialize_hall_mask_state(requirement_count, masks)
                assert not validate_hall_mask_state(hall.state)
                assert hall.state.matching_size == matching_size
                assert hall.state.complete is (
                    matching_size == requirement_count
                )
                graph_count += 1

    # Sum_{r=1..4,H=0..4} 2^(rH): the exact finite frozen gate.
    assert graph_count == 74_958


@pytest.mark.parametrize(
    ("masks", "expected_size"),
    (
        ({}, 0),
        ({"a": 0b10}, 1),
        ({"a": 0b11}, 1),
        ({"a": 0b01, "b": 0b10}, 2),
    ),
)
def test_matching_includes_the_unmatched_branch_cases(
    masks: dict[str, int],
    expected_size: int,
) -> None:
    assert affected_group_matching(2, masks).matching_size == expected_size
    assert initialize_hall_mask_state(2, masks).state.matching_size == expected_size


def test_frozen_hall_counterexample_has_matching_size_three() -> None:
    masks = {
        "a": 0b0111,
        "b": 0b0111,
        "c": 0b1000,
        "d": 0b1000,
    }

    matching = affected_group_matching(4, masks)
    hall = initialize_hall_mask_state(4, masks)

    assert matching.matching_size == 3
    assert hall.state.matching_size == 3
    assert hall.state.maximum_deficiency == 1
    assert not hall.state.complete


def test_matching_exposes_every_size_from_zero_through_r() -> None:
    requirement_count = 4
    observed_sizes = {
        affected_group_matching(
            requirement_count,
            {
                f"h{ordinal}": 1 << ordinal
                for ordinal in range(target_size)
            },
        ).matching_size
        for target_size in range(requirement_count + 1)
    }

    assert observed_sizes == set(range(requirement_count + 1))


def test_matching_is_deterministic_under_mapping_insertion_order() -> None:
    entries = (("c", 0b111), ("a", 0b011), ("b", 0b110), ("d", 0b101))
    expected = affected_group_matching(3, dict(entries)).pairs

    for ordered in permutations(entries):
        assert affected_group_matching(3, dict(ordered)).pairs == expected


@pytest.mark.parametrize("requirement_count", (0, 9, True))
def test_requirement_bound_is_enforced(requirement_count: int) -> None:
    with pytest.raises(ValidationError):
        affected_group_matching(requirement_count, {})


def test_zero_hash_masks_are_not_active_right_vertices() -> None:
    with pytest.raises(ValidationError, match="at least 1"):
        affected_group_matching(2, {"isolated": 0})
