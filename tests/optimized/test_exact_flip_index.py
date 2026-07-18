"""Boundary-complete tests for the exact-flip partition and AVL index."""

import pytest
from helpers import STAMP

from groundloop.domain import DecisionPolicy, SemanticObservation, SubjectKind
from groundloop.optimized.exact_flip import (
    ExactFlipIndex,
    PotentialPartition,
    potential_partition,
)
from groundloop.policy import decide


def _observation(
    observation_id: str, scores: tuple[float, float, float]
) -> SemanticObservation:
    return SemanticObservation(
        observation_id=observation_id,
        subject_kind=SubjectKind.CLAIM,
        subject_id="claim",
        chunk_version_id=f"chunk-{observation_id}",
        task_type="verify",
        support_score=scores[0],
        refute_score=scores[1],
        neutral_score=scores[2],
        producer=STAMP,
        input_hash=f"input-{observation_id}",
    )


@pytest.mark.parametrize(
    ("scores", "expected"),
    [
        ((0.7, 0.6, 0.5), PotentialPartition.SUPPORT),
        ((0.7, 0.7, 0.2), PotentialPartition.REFUTE),
        ((0.2, 0.7, 0.7), PotentialPartition.REFUTE),
        ((0.7, 0.2, 0.7), PotentialPartition.ALWAYS_NEUTRAL),
        ((0.2, 0.3, 0.7), PotentialPartition.ALWAYS_NEUTRAL),
        ((0.5, 0.5, 0.5), PotentialPartition.REFUTE),
    ],
)
def test_partition_exhausts_equality_and_tie_boundaries(
    scores: tuple[float, float, float], expected: PotentialPartition
) -> None:
    assert potential_partition(_observation("o", scores)) is expected


@pytest.mark.parametrize(
    ("old_threshold", "new_threshold", "score", "flips"),
    [
        (0.8, 0.6, 0.6, True),
        (0.8, 0.6, 0.799999, True),
        (0.8, 0.6, 0.8, False),
        (0.6, 0.8, 0.6, True),
        (0.6, 0.8, 0.8, False),
        # score 0 cannot be in P_support because the other scores are
        # nonnegative and support requires strict dominance.
        (0.0, 1.0, 0.0, False),
        (0.0, 1.0, 1.0, False),
    ],
)
def test_support_interval_is_lower_inclusive_upper_exclusive(
    old_threshold: float, new_threshold: float, score: float, flips: bool
) -> None:
    observation = _observation("o", (score, 0.0, 0.0))
    index = ExactFlipIndex()
    index.add(observation)
    old = DecisionPolicy("old", old_threshold, 0.8)
    new = DecisionPolicy("new", new_threshold, 0.8)
    query = index.exact_flips(old, new)
    assert ("o" in query.observation_ids) is flips
    assert (decide(observation, old) is not decide(observation, new)) is flips


def test_refute_zero_score_boundary_flips_under_conservative_tie_rule() -> None:
    observation = _observation("o", (0.0, 0.0, 0.0))
    index = ExactFlipIndex()
    index.add(observation)
    query = index.exact_flips(
        DecisionPolicy("old", 0.8, 0.0),
        DecisionPolicy("new", 0.8, 1.0),
    )
    assert query.observation_ids == ("o",)


def test_simultaneous_threshold_change_uses_two_disjoint_searches() -> None:
    observations = (
        _observation("support", (0.7, 0.1, 0.2)),
        _observation("refute", (0.1, 0.7, 0.2)),
        _observation("support-tied-neutral", (0.7, 0.1, 0.7)),
        _observation("refute-tie", (0.7, 0.7, 0.1)),
    )
    index = ExactFlipIndex()
    for observation in observations:
        index.add(observation)
    query = index.exact_flips(
        DecisionPolicy("old", 0.8, 0.8),
        DecisionPolicy("new", 0.6, 0.6),
    )
    assert query.searches == 2
    assert query.support_flips == 1
    assert query.refute_flips == 2
    assert set(query.observation_ids) == {"support", "refute", "refute-tie"}
    index.validate()


def test_avl_index_survives_adversarial_sorted_insert_delete_order() -> None:
    index = ExactFlipIndex()
    observations = tuple(
        _observation(f"o-{i:04d}", (i / 1000, 0.0, 0.0))
        for i in range(1, 900)
    )
    for observation in observations:
        index.add(observation)
    index.validate()
    for observation in observations[::2]:
        index.remove(observation)
    index.validate()
