from __future__ import annotations

import random

import pytest
from m4.runtime.withdrawal_reference import (
    WithdrawalMeasurement,
    full_scan_withdrawal,
    measure_withdrawal,
)

from groundloop.errors import ValidationError
from groundloop.m4.contracts import PairKey
from groundloop.m4.runtime.withdrawal import (
    CandidateDependency,
    ObservationDependency,
    ReverseDependencyIndex,
)


def _random_edges(
    rng: random.Random,
    *,
    chunk_count: int,
    claim_count: int,
    observation_count: int,
    candidate_count: int,
) -> tuple[tuple[ObservationDependency, ...], tuple[CandidateDependency, ...]]:
    observations = tuple(
        ObservationDependency(
            observation_id=f"obs-{index}",
            pair=PairKey(
                claim_id=f"claim-{rng.randrange(claim_count)}",
                chunk_version_id=f"chunk-{rng.randrange(chunk_count)}",
            ),
        )
        for index in range(observation_count)
    )
    candidates = tuple(
        CandidateDependency(
            candidate_edge_id=f"candidate-{index}",
            pair=PairKey(
                claim_id=f"claim-{rng.randrange(claim_count)}",
                chunk_version_id=f"chunk-{rng.randrange(chunk_count)}",
            ),
        )
        for index in range(candidate_count)
    )
    return observations, candidates


def _assert_exact_bound(measurement: WithdrawalMeasurement) -> None:
    # Kept as a local structural assertion so the formula cannot share the
    # implementation of WithdrawalPlan.indexed_operation_count.
    indexed = measurement.indexed
    expected = (
        len(indexed.deactivated_chunk_ids)
        + len(indexed.observation_ids)
        + len(indexed.candidate_edge_ids)
    )
    assert indexed.indexed_operation_count == expected


@pytest.mark.parametrize("seed", range(20))
@pytest.mark.parametrize("event_shape", ["insert", "delete", "replace"])
def test_seeded_event_shapes_match_independent_full_scan(
    seed: int, event_shape: str
) -> None:
    rng = random.Random(100_003 * seed + 17)
    observations, candidates = _random_edges(
        rng,
        chunk_count=30,
        claim_count=40,
        observation_count=400,
        candidate_count=600,
    )
    if event_shape == "insert":
        deactivated: tuple[str, ...] = ()
    elif event_shape == "delete":
        deactivated = tuple(
            sorted({f"chunk-{rng.randrange(30)}" for _ in range(5)})
        )
    else:
        # Replacement withdraws old versions only. New-version edges remain.
        deactivated = tuple(
            sorted({f"chunk-{rng.randrange(15)}" for _ in range(4)})
        )

    measurement = measure_withdrawal(observations, candidates, deactivated)

    assert measurement.outputs_equal
    _assert_exact_bound(measurement)
    counters = measurement.full_scan.counters
    assert counters.observation_edge_scans == len(observations)
    assert counters.candidate_edge_scans == len(candidates)
    if event_shape == "replace":
        retained_new_chunks = {f"chunk-{index}" for index in range(15, 30)}
        withdrawn_pairs = set(measurement.indexed.affected_pairs)
        assert not any(
            pair.chunk_version_id in retained_new_chunks for pair in withdrawn_pairs
        )


def test_distinct_dependencies_for_the_same_pair_are_all_enumerated() -> None:
    pair = PairKey("claim-1", "chunk-1")
    observations = (
        ObservationDependency("obs-1", pair),
        ObservationDependency("obs-2", pair),
    )
    candidates = (
        CandidateDependency("candidate-1", pair),
        CandidateDependency("candidate-2", pair),
    )

    measurement = measure_withdrawal(observations, candidates, ("chunk-1",))

    assert measurement.outputs_equal
    assert set(measurement.indexed.observation_ids) == {"obs-1", "obs-2"}
    assert set(measurement.indexed.candidate_edge_ids) == {
        "candidate-1",
        "candidate-2",
    }
    assert measurement.indexed.affected_pairs == (pair,)
    _assert_exact_bound(measurement)


def test_duplicate_immutable_edge_ids_are_rejected_by_both_algorithms() -> None:
    observations = (
        ObservationDependency("duplicate", PairKey("claim-1", "chunk-1")),
        ObservationDependency("duplicate", PairKey("claim-2", "chunk-2")),
    )
    with pytest.raises(ValidationError, match="unique"):
        ReverseDependencyIndex.build(observations, ())
    with pytest.raises(ValidationError, match="unique"):
        full_scan_withdrawal(observations, (), ("chunk-1",))


def test_empty_and_no_dependency_deactivations_have_zero_indexed_edge_visits() -> None:
    observations = (
        ObservationDependency("obs-1", PairKey("claim-1", "active")),
    )
    candidates = (
        CandidateDependency("candidate-1", PairKey("claim-1", "active")),
    )
    empty = measure_withdrawal(observations, candidates, ())
    absent = measure_withdrawal(observations, candidates, ("inactive-absent",))

    assert empty.outputs_equal and absent.outputs_equal
    assert empty.indexed.indexed_operation_count == 0
    assert absent.indexed.indexed_operation_count == 1
    assert absent.indexed.observation_edge_visits == 0
    assert absent.indexed.candidate_edge_visits == 0
    assert absent.full_scan.counters.full_scan_operation_count == 3


def test_highly_skewed_cold_delete_is_output_sensitive() -> None:
    observations = tuple(
        ObservationDependency(f"hot-obs-{index}", PairKey(f"claim-{index}", "hot"))
        for index in range(8_000)
    ) + (ObservationDependency("cold-obs", PairKey("cold-claim", "cold")),)
    candidates = tuple(
        CandidateDependency(
            f"hot-candidate-{index}", PairKey(f"claim-{index}", "hot")
        )
        for index in range(4_000)
    )

    measurement = measure_withdrawal(observations, candidates, ("cold",))

    assert measurement.outputs_equal
    assert measurement.indexed.indexed_operation_count == 2
    assert measurement.full_scan.counters.full_scan_operation_count == 12_002
    _assert_exact_bound(measurement)


def test_dense_hot_delete_is_not_claimed_to_be_sublinear() -> None:
    observations = tuple(
        ObservationDependency(f"obs-{index}", PairKey(f"claim-{index}", "hot"))
        for index in range(5_000)
    )
    candidates = tuple(
        CandidateDependency(
            f"candidate-{index}", PairKey(f"claim-{index}", "hot")
        )
        for index in range(5_000)
    )

    measurement = measure_withdrawal(observations, candidates, ("hot",))

    assert measurement.outputs_equal
    assert measurement.indexed.indexed_operation_count == 10_001
    assert measurement.full_scan.counters.full_scan_operation_count == 10_001
    _assert_exact_bound(measurement)
