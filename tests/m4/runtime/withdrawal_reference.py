"""Independent full-scan withdrawal oracle used only by tests/measurement."""

from __future__ import annotations

from dataclasses import dataclass

from groundloop.errors import ValidationError
from groundloop.m4.contracts import PairKey
from groundloop.m4.runtime.withdrawal import (
    CandidateDependency,
    ObservationDependency,
    ReverseDependencyIndex,
    WithdrawalPlan,
    plan_withdrawal,
)


@dataclass(frozen=True, slots=True)
class FullScanCounters:
    deactivated_chunk_loads: int
    observation_edge_scans: int
    candidate_edge_scans: int
    matched_observation_edges: int
    matched_candidate_edges: int

    @property
    def full_scan_operation_count(self) -> int:
        return (
            self.deactivated_chunk_loads
            + self.observation_edge_scans
            + self.candidate_edge_scans
        )


@dataclass(frozen=True, slots=True)
class FullScanWithdrawalResult:
    deactivated_chunk_ids: tuple[str, ...]
    observation_ids: tuple[str, ...]
    candidate_edge_ids: tuple[str, ...]
    affected_pairs: tuple[PairKey, ...]
    affected_claim_ids: tuple[str, ...]
    counters: FullScanCounters


@dataclass(frozen=True, slots=True)
class WithdrawalMeasurement:
    indexed: WithdrawalPlan
    full_scan: FullScanWithdrawalResult

    @property
    def outputs_equal(self) -> bool:
        return (
            self.indexed.deactivated_chunk_ids
            == self.full_scan.deactivated_chunk_ids
            and set(self.indexed.observation_ids)
            == set(self.full_scan.observation_ids)
            and set(self.indexed.candidate_edge_ids)
            == set(self.full_scan.candidate_edge_ids)
            and set(self.indexed.affected_pairs)
            == set(self.full_scan.affected_pairs)
            and set(self.indexed.affected_claim_ids)
            == set(self.full_scan.affected_claim_ids)
        )


def full_scan_withdrawal(
    observations: tuple[ObservationDependency, ...],
    candidates: tuple[CandidateDependency, ...],
    deactivated_chunk_ids: tuple[str, ...],
) -> FullScanWithdrawalResult:
    """Scan every stored edge and independently derive the withdrawal set."""
    if deactivated_chunk_ids != tuple(sorted(set(deactivated_chunk_ids))):
        raise ValidationError("deactivated chunk IDs must be sorted and unique")
    if any(not chunk_id.strip() for chunk_id in deactivated_chunk_ids):
        raise ValidationError("deactivated chunk IDs must be non-empty")
    deleted = set(deactivated_chunk_ids)
    observation_ids: set[str] = set()
    candidate_ids: set[str] = set()
    matched_observations: list[ObservationDependency] = []
    matched_candidates: list[CandidateDependency] = []
    for observation in observations:
        if observation.observation_id in observation_ids:
            raise ValidationError("observation dependency IDs must be unique")
        observation_ids.add(observation.observation_id)
        if observation.pair.chunk_version_id in deleted:
            matched_observations.append(observation)
    for candidate in candidates:
        if candidate.candidate_edge_id in candidate_ids:
            raise ValidationError("candidate dependency IDs must be unique")
        candidate_ids.add(candidate.candidate_edge_id)
        if candidate.pair.chunk_version_id in deleted:
            matched_candidates.append(candidate)
    pairs = tuple(
        dict.fromkeys(
            [edge.pair for edge in matched_observations]
            + [edge.pair for edge in matched_candidates]
        )
    )
    return FullScanWithdrawalResult(
        deactivated_chunk_ids=deactivated_chunk_ids,
        observation_ids=tuple(edge.observation_id for edge in matched_observations),
        candidate_edge_ids=tuple(
            edge.candidate_edge_id for edge in matched_candidates
        ),
        affected_pairs=pairs,
        affected_claim_ids=tuple(dict.fromkeys(pair.claim_id for pair in pairs)),
        counters=FullScanCounters(
            deactivated_chunk_loads=len(deactivated_chunk_ids),
            observation_edge_scans=len(observations),
            candidate_edge_scans=len(candidates),
            matched_observation_edges=len(matched_observations),
            matched_candidate_edges=len(matched_candidates),
        ),
    )


def measure_withdrawal(
    observations: tuple[ObservationDependency, ...],
    candidates: tuple[CandidateDependency, ...],
    deactivated_chunk_ids: tuple[str, ...],
) -> WithdrawalMeasurement:
    """Run both independent algorithms over the same immutable edge snapshot."""
    index = ReverseDependencyIndex.build(observations, candidates)
    return WithdrawalMeasurement(
        indexed=plan_withdrawal(index, deactivated_chunk_ids),
        full_scan=full_scan_withdrawal(
            observations, candidates, deactivated_chunk_ids
        ),
    )
