"""Exact reverse-dependency withdrawal planning for M4."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from groundloop.errors import ValidationError
from groundloop.m4.contracts import PairKey


@dataclass(frozen=True, slots=True)
class ObservationDependency:
    observation_id: str
    pair: PairKey

    def __post_init__(self) -> None:
        if not self.observation_id.strip():
            raise ValidationError("observation_id must be non-empty")


@dataclass(frozen=True, slots=True)
class CandidateDependency:
    candidate_edge_id: str
    pair: PairKey

    def __post_init__(self) -> None:
        if not self.candidate_edge_id.strip():
            raise ValidationError("candidate_edge_id must be non-empty")


@dataclass(frozen=True, slots=True)
class ReverseDependencyIndex:
    """Immutable logical index used by deletion; construction is not event work."""

    observations_by_chunk: Mapping[str, tuple[ObservationDependency, ...]]
    candidates_by_chunk: Mapping[str, tuple[CandidateDependency, ...]]

    @classmethod
    def build(
        cls,
        observations: tuple[ObservationDependency, ...],
        candidates: tuple[CandidateDependency, ...],
    ) -> ReverseDependencyIndex:
        observation_ids: set[str] = set()
        candidate_ids: set[str] = set()
        obs_buckets: dict[str, list[ObservationDependency]] = {}
        candidate_buckets: dict[str, list[CandidateDependency]] = {}
        for observation in observations:
            if observation.observation_id in observation_ids:
                raise ValidationError("observation dependency IDs must be unique")
            observation_ids.add(observation.observation_id)
            obs_buckets.setdefault(observation.pair.chunk_version_id, []).append(
                observation
            )
        for candidate in candidates:
            if candidate.candidate_edge_id in candidate_ids:
                raise ValidationError("candidate dependency IDs must be unique")
            candidate_ids.add(candidate.candidate_edge_id)
            candidate_buckets.setdefault(candidate.pair.chunk_version_id, []).append(
                candidate
            )
        frozen_obs = {
            chunk_id: tuple(sorted(values, key=lambda item: item.observation_id))
            for chunk_id, values in obs_buckets.items()
        }
        frozen_candidates = {
            chunk_id: tuple(sorted(values, key=lambda item: item.candidate_edge_id))
            for chunk_id, values in candidate_buckets.items()
        }
        return cls(
            observations_by_chunk=MappingProxyType(frozen_obs),
            candidates_by_chunk=MappingProxyType(frozen_candidates),
        )


@dataclass(frozen=True, slots=True)
class WithdrawalPlan:
    deactivated_chunk_ids: tuple[str, ...]
    observation_ids: tuple[str, ...]
    candidate_edge_ids: tuple[str, ...]
    affected_pairs: tuple[PairKey, ...]
    affected_claim_ids: tuple[str, ...]
    chunk_lookups: int
    observation_edge_visits: int
    candidate_edge_visits: int


def plan_withdrawal(
    index: ReverseDependencyIndex, deactivated_chunk_ids: tuple[str, ...]
) -> WithdrawalPlan:
    """Enumerate exactly stored reverse edges, without any retrieval operation."""
    chunks = deactivated_chunk_ids
    if any(not chunk_id.strip() for chunk_id in chunks):
        raise ValidationError("deactivated chunk IDs must be non-empty")
    if chunks != tuple(sorted(set(chunks))):
        raise ValidationError("deactivated chunk IDs must be sorted and unique")
    observations: list[ObservationDependency] = []
    candidates: list[CandidateDependency] = []
    for chunk_id in chunks:
        observations.extend(index.observations_by_chunk.get(chunk_id, ()))
        candidates.extend(index.candidates_by_chunk.get(chunk_id, ()))
    pairs = tuple(
        dict.fromkeys(
            [observation.pair for observation in observations]
            + [candidate.pair for candidate in candidates]
        )
    )
    return WithdrawalPlan(
        deactivated_chunk_ids=chunks,
        observation_ids=tuple(item.observation_id for item in observations),
        candidate_edge_ids=tuple(item.candidate_edge_id for item in candidates),
        affected_pairs=pairs,
        affected_claim_ids=tuple(dict.fromkeys(pair.claim_id for pair in pairs)),
        chunk_lookups=len(chunks),
        observation_edge_visits=len(observations),
        candidate_edge_visits=len(candidates),
    )
