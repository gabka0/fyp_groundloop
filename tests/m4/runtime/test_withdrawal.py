from __future__ import annotations

import random

import pytest

from groundloop.errors import ValidationError
from groundloop.m4.contracts import PairKey
from groundloop.m4.runtime.withdrawal import (
    CandidateDependency,
    ObservationDependency,
    ReverseDependencyIndex,
    plan_withdrawal,
)


def test_indexed_withdrawal_matches_full_scan_on_randomized_skewed_data() -> None:
    rng = random.Random(20260719)
    observations = tuple(
        ObservationDependency(
            f"obs-{index}",
            PairKey(f"claim-{rng.randrange(20)}", f"chunk-{rng.randrange(8)}"),
        )
        for index in range(500)
    )
    candidates = tuple(
        CandidateDependency(
            f"candidate-{index}",
            PairKey(f"claim-{rng.randrange(20)}", f"chunk-{rng.randrange(8)}"),
        )
        for index in range(700)
    )
    index = ReverseDependencyIndex.build(observations, candidates)
    deleted = ("chunk-1", "chunk-3", "chunk-7")

    plan = plan_withdrawal(index, deleted)
    expected_observations = tuple(
        sorted(
            edge.observation_id
            for edge in observations
            if edge.pair.chunk_version_id in deleted
        )
    )
    expected_candidates = tuple(
        sorted(
            edge.candidate_edge_id
            for edge in candidates
            if edge.pair.chunk_version_id in deleted
        )
    )

    assert set(plan.observation_ids) == set(expected_observations)
    assert set(plan.candidate_edge_ids) == set(expected_candidates)
    assert plan.observation_edge_visits == len(expected_observations)
    assert plan.candidate_edge_visits == len(expected_candidates)
    assert plan.chunk_lookups == 3


def test_zero_degree_and_high_fanout_costs_are_output_sensitive() -> None:
    observations = tuple(
        ObservationDependency(f"obs-{i}", PairKey(f"claim-{i}", "hot"))
        for i in range(1_000)
    )
    index = ReverseDependencyIndex.build(observations, ())

    zero = plan_withdrawal(index, ("absent",))
    hot = plan_withdrawal(index, ("hot",))

    assert zero.chunk_lookups == 1
    assert zero.observation_edge_visits == 0
    assert hot.chunk_lookups == 1
    assert hot.observation_edge_visits == 1_000
    assert len(hot.affected_claim_ids) == 1_000


def test_duplicate_dependency_identity_is_rejected() -> None:
    duplicate = (
        ObservationDependency("obs", PairKey("claim-1", "chunk-1")),
        ObservationDependency("obs", PairKey("claim-2", "chunk-2")),
    )
    with pytest.raises(ValidationError, match="unique"):
        ReverseDependencyIndex.build(duplicate, ())


def test_deactivated_chunk_input_must_be_canonical_for_linear_event_work() -> None:
    index = ReverseDependencyIndex.build((), ())
    with pytest.raises(ValidationError, match="sorted and unique"):
        plan_withdrawal(index, ("chunk-2", "chunk-1"))
