"""Exact output-sensitive policy-flip index for frozen tie rule v1."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from groundloop.domain import DecisionPolicy, SemanticObservation
from groundloop.optimized.avl import ScoreOrderedSet


class PotentialPartition(StrEnum):
    SUPPORT = "support"
    REFUTE = "refute"
    ALWAYS_NEUTRAL = "always_neutral"


def potential_partition(observation: SemanticObservation) -> PotentialPartition:
    """Classify an observation independently of policy thresholds.

    The strict support inequalities and conservative refute inequalities are
    exactly the frozen v1 tie semantics.  The three sets are exhaustive and
    disjoint.
    """
    s = observation.support_score
    r = observation.refute_score
    n = observation.neutral_score
    if s > r and s > n:
        return PotentialPartition.SUPPORT
    if r >= s and r >= n:
        return PotentialPartition.REFUTE
    return PotentialPartition.ALWAYS_NEUTRAL


@dataclass(frozen=True, slots=True)
class FlipQuery:
    observation_ids: tuple[str, ...]
    searches: int
    support_flips: int
    refute_flips: int


@dataclass(slots=True)
class ExactFlipIndex:
    """Two disjoint AVL indexes containing only potential non-neutral rows."""

    _support: ScoreOrderedSet = field(default_factory=ScoreOrderedSet)
    _refute: ScoreOrderedSet = field(default_factory=ScoreOrderedSet)
    _members: dict[str, tuple[PotentialPartition, float]] = field(
        default_factory=dict
    )

    def __len__(self) -> int:
        return len(self._members)

    @property
    def support_size(self) -> int:
        return len(self._support)

    @property
    def refute_size(self) -> int:
        return len(self._refute)

    def add(self, observation: SemanticObservation) -> None:
        if observation.observation_id in self._members:
            raise KeyError(f"observation already indexed: {observation.observation_id}")
        partition = potential_partition(observation)
        if partition is PotentialPartition.SUPPORT:
            score = observation.support_score
            self._support.add((score, observation.observation_id))
        elif partition is PotentialPartition.REFUTE:
            score = observation.refute_score
            self._refute.add((score, observation.observation_id))
        else:
            return
        self._members[observation.observation_id] = (partition, score)

    def remove(self, observation: SemanticObservation) -> None:
        partition = potential_partition(observation)
        if partition is PotentialPartition.ALWAYS_NEUTRAL:
            return
        stored = self._members.pop(observation.observation_id, None)
        expected_score = (
            observation.support_score
            if partition is PotentialPartition.SUPPORT
            else observation.refute_score
        )
        if stored != (partition, expected_score):
            raise KeyError(f"observation not indexed: {observation.observation_id}")
        tree = (
            self._support
            if partition is PotentialPartition.SUPPORT
            else self._refute
        )
        tree.remove((expected_score, observation.observation_id))

    def exact_flips(self, old: DecisionPolicy, new: DecisionPolicy) -> FlipQuery:
        """Enumerate exactly the decisions that flip under threshold changes."""
        if old.tie_rule_version != "v1" or new.tie_rule_version != "v1":
            raise ValueError("exact-flip index is defined only for tie rule v1")
        searches = 0
        support_ids: tuple[str, ...] = ()
        refute_ids: tuple[str, ...] = ()
        if old.support_threshold != new.support_threshold:
            searches += 1
            lower, upper = sorted(
                (old.support_threshold, new.support_threshold)
            )
            support_ids = self._support.ids_between(lower, upper)
        if old.refute_threshold != new.refute_threshold:
            searches += 1
            lower, upper = sorted((old.refute_threshold, new.refute_threshold))
            refute_ids = self._refute.ids_between(lower, upper)
        return FlipQuery(
            observation_ids=support_ids + refute_ids,
            searches=searches,
            support_flips=len(support_ids),
            refute_flips=len(refute_ids),
        )

    def validate(self) -> None:
        self._support.validate()
        self._refute.validate()
        if len(self._members) != len(self._support) + len(self._refute):
            raise AssertionError("exact-flip membership/index size mismatch")
