from __future__ import annotations

import hashlib
from itertools import permutations

import pytest

from groundloop.errors import ValidationError
from groundloop.m5.matching import (
    CertificateSnapshot,
    HallMaskState,
    affected_group_matching,
    initialize_hall_mask_state,
    reconstruct_certificate,
    validate_certificate_artifact,
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


def _assert_relational_hall_arrays(
    requirement_count: int,
    masks: dict[str, int],
    state: HallMaskState,
) -> None:
    full_mask = (1 << requirement_count) - 1
    histogram = [0] * (full_mask + 1)
    for mask in masks.values():
        histogram[mask] += 1
    neighbors = [0] * (full_mask + 1)
    deficiencies = [0] * (full_mask + 1)
    for subset in range(1, full_mask + 1):
        neighbors[subset] = sum(bool(mask & subset) for mask in masks.values())
        deficiencies[subset] = subset.bit_count() - neighbors[subset]

    assert state.mask_histogram == tuple(histogram)
    assert state.neighbor_counts == tuple(neighbors)
    assert state.deficiencies == tuple(deficiencies)
    assert state.maximum_deficiency == max(
        0,
        max(deficiencies[1:]),
    )


def _certificate_snapshot(
    requirement_count: int,
    masks: dict[str, int],
) -> CertificateSnapshot:
    digest_masks = {
        hashlib.sha256(text_hash.encode("utf-8")).hexdigest(): mask
        for text_hash, mask in masks.items()
    }
    observations = {
        (ordinal, text_hash): (f"obs-{ordinal}-{text_hash}",)
        for text_hash, mask in digest_masks.items()
        for ordinal in range(requirement_count)
        if mask & (1 << ordinal)
    }
    return CertificateSnapshot.from_primitives(
        epoch_id=1,
        revision=0,
        decision_policy_version="policy-v1",
        group_version_id="group-v1",
        requirement_version_ids=tuple(
            f"requirement-{ordinal}" for ordinal in range(requirement_count)
        ),
        hash_masks=digest_masks,
        observation_ids_by_edge=observations,
    )


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
                _assert_relational_hall_arrays(
                    requirement_count,
                    masks,
                    hall.state,
                )
                assert hall.state.matching_size == matching_size
                assert hall.state.complete is (matching_size == requirement_count)
                snapshot = _certificate_snapshot(requirement_count, masks)
                first_certificate = reconstruct_certificate(snapshot)
                second_certificate = reconstruct_certificate(snapshot)
                assert first_certificate.artifact == second_certificate.artifact
                assert first_certificate.matching == second_certificate.matching
                assert first_certificate.matching.matching_size == matching_size
                if matching_size == requirement_count:
                    assert first_certificate.artifact is not None
                    rows = first_certificate.artifact.rows
                    assert tuple(row.requirement_ordinal for row in rows) == tuple(
                        range(requirement_count)
                    )
                    assert len({row.text_hash for row in rows}) == requirement_count
                    snapshot_masks = dict(snapshot.hash_masks)
                    assert all(
                        snapshot_masks[row.text_hash] & (1 << row.requirement_ordinal)
                        and row.selected_observation_id
                        == f"obs-{row.requirement_ordinal}-{row.text_hash}"
                        for row in rows
                    )
                    assert validate_certificate_artifact(
                        first_certificate.artifact,
                        snapshot,
                    ).valid
                else:
                    assert first_certificate.artifact is None
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
            {f"h{ordinal}": 1 << ordinal for ordinal in range(target_size)},
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
