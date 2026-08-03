from __future__ import annotations

import pytest

from groundloop.errors import ValidationError
from groundloop.m5.matching import (
    EdgeMultiplicityDelta,
    EdgeRefcount,
    apply_edge_multiplicity_deltas,
    coalesce_edge_multiplicity_deltas,
    initialize_hall_mask_state,
)


def _refcount_dict(rows: tuple[EdgeRefcount, ...]) -> dict[tuple[int, str], int]:
    return {(row.requirement_ordinal, row.text_hash): row.count for row in rows}


def test_zero_positive_and_positive_positive_multiplicity_transitions() -> None:
    state = initialize_hall_mask_state(2, {}).state
    refcounts: tuple[EdgeRefcount, ...] = ()

    added = apply_edge_multiplicity_deltas(
        state,
        refcounts,
        (EdgeMultiplicityDelta(0, "a", 1),),
    )
    assert added.transitions[0].old_mask == 0
    assert added.transitions[0].new_mask == 0b01
    assert added.work.contribution_additions == 1
    assert added.work.distinct_edge_crossings == 1
    assert added.work.hash_mask_transitions == 1
    assert added.work.group_local_state_operations == 1
    assert added.work.groups_touched == 0

    duplicate = apply_edge_multiplicity_deltas(
        added.state,
        added.refcounts,
        (EdgeMultiplicityDelta(0, "a", 1),),
    )
    assert _refcount_dict(duplicate.refcounts) == {(0, "a"): 2}
    assert duplicate.state is added.state
    assert not duplicate.transitions
    assert duplicate.work.contribution_additions == 1
    assert duplicate.work.edge_refcount_keys_updated == 1
    assert duplicate.work.distinct_edge_crossings == 0
    assert duplicate.work.hash_mask_transitions == 0
    assert duplicate.work.group_local_state_operations == 0
    assert duplicate.work.groups_touched == 0

    still_positive = apply_edge_multiplicity_deltas(
        duplicate.state,
        duplicate.refcounts,
        (EdgeMultiplicityDelta(0, "a", -1),),
    )
    assert _refcount_dict(still_positive.refcounts) == {(0, "a"): 1}
    assert still_positive.state is duplicate.state
    assert not still_positive.transitions
    assert still_positive.work.contribution_removals == 1
    assert still_positive.work.hash_mask_transitions == 0

    removed = apply_edge_multiplicity_deltas(
        still_positive.state,
        still_positive.refcounts,
        (EdgeMultiplicityDelta(0, "a", -1),),
    )
    assert not removed.refcounts
    assert removed.transitions[0].old_mask == 0b01
    assert removed.transitions[0].new_mask == 0
    assert removed.work.distinct_edge_crossings == 1
    assert removed.work.hash_mask_transitions == 1
    assert removed.state == state


def test_multi_bit_changes_coalesce_to_one_hash_transition() -> None:
    state = initialize_hall_mask_state(3, {}).state
    result = apply_edge_multiplicity_deltas(
        state,
        (),
        (
            EdgeMultiplicityDelta(0, "shared", 1),
            EdgeMultiplicityDelta(2, "shared", 1),
            EdgeMultiplicityDelta(0, "shared", 1),
        ),
    )

    assert result.transitions == (result.transitions[0],)
    assert result.transitions[0].old_mask == 0
    assert result.transitions[0].new_mask == 0b101
    assert result.work.contribution_additions == 3
    assert result.work.edge_refcount_keys_updated == 2
    assert result.work.distinct_edge_crossings == 2
    assert result.work.hash_mask_transitions == 1
    assert result.state.mask_histogram[0b101] == 1


def test_net_equal_swap_has_zero_refcount_and_hall_work() -> None:
    state = initialize_hall_mask_state(2, {"a": 0b01}).state
    current = (EdgeRefcount(0, "a", 1),)
    result = apply_edge_multiplicity_deltas(
        state,
        current,
        (
            EdgeMultiplicityDelta(0, "a", -1),
            EdgeMultiplicityDelta(0, "a", 1),
        ),
    )

    assert result.state is state
    assert result.refcounts == current
    assert not result.transitions
    assert result.work.contribution_additions == 1
    assert result.work.contribution_removals == 1
    assert result.work.edge_refcount_keys_updated == 0
    assert result.work.hash_mask_transitions == 0
    assert result.work.hall_subset_entries_examined == 0


def test_supersession_between_hashes_is_two_transitions() -> None:
    state = initialize_hall_mask_state(2, {"old": 0b01}).state
    result = apply_edge_multiplicity_deltas(
        state,
        (EdgeRefcount(0, "old", 1),),
        (
            EdgeMultiplicityDelta(0, "old", -1),
            EdgeMultiplicityDelta(0, "new", 1),
        ),
    )

    assert tuple(item.text_hash for item in result.transitions) == ("new", "old")
    assert result.work.hash_mask_transitions == 2
    assert result.work.distinct_edge_crossings == 2
    assert result.state == initialize_hall_mask_state(2, {"new": 0b01}).state


def test_underflow_is_failure_atomic() -> None:
    state = initialize_hall_mask_state(2, {"a": 0b01}).state
    current = (EdgeRefcount(0, "a", 1),)

    with pytest.raises(ValidationError, match="underflow"):
        apply_edge_multiplicity_deltas(
            state,
            current,
            (EdgeMultiplicityDelta(0, "a", -2),),
        )

    assert state == initialize_hall_mask_state(2, {"a": 0b01}).state
    assert current == (EdgeRefcount(0, "a", 1),)


def test_application_rejects_refcount_state_mismatch() -> None:
    state = initialize_hall_mask_state(2, {"a": 0b01}).state

    with pytest.raises(ValidationError, match="do not match"):
        apply_edge_multiplicity_deltas(
            state,
            (),
            (EdgeMultiplicityDelta(0, "a", 1),),
        )


def test_coalescer_rejects_out_of_range_ordinals() -> None:
    with pytest.raises(ValidationError, match="at most 1"):
        coalesce_edge_multiplicity_deltas(
            2,
            (),
            (EdgeMultiplicityDelta(2, "a", 1),),
        )


def test_zero_delta_is_rejected_at_the_boundary() -> None:
    with pytest.raises(ValidationError, match="nonzero"):
        EdgeMultiplicityDelta(0, "a", 0)
