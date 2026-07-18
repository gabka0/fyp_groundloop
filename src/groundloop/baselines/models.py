"""Versioned workload and raw JSONL metrics schemas for structured baselines."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any

METRICS_SCHEMA_VERSION = "groundloop.baselines.metrics.v1"


class Locality(StrEnum):
    LOCAL = "local"
    HIGH_FANOUT = "high_fanout"
    MIXED = "mixed"


class TimingScope(StrEnum):
    KERNEL_ONLY = "kernel_only"
    WITH_ORACLE_STAGING = "with_oracle_staging"


class EngineClass(StrEnum):
    EXACT_BASELINE = "exact_baseline"
    TREATMENT = "treatment"
    HEURISTIC_POLICY = "heuristic_policy"


@dataclass(frozen=True, slots=True)
class WorkloadParameters:
    """Controlled structured workload dimensions.

    ``E`` is the number of current observations at the initial snapshot, ``C``
    the number of claims, ``A`` the number of answers, ``k`` the maximum
    observations assigned to one chunk, and ``f`` the number of observations
    constructed to flip under the generated threshold event.
    """

    E: int
    C: int
    A: int
    k: int
    f: int
    duplicate_content_ratio: float
    skew: float
    locality: Locality

    def __post_init__(self) -> None:
        if self.E <= 0 or self.C <= 0 or self.A <= 0:
            raise ValueError("E, C, and A must be positive")
        if self.A > self.C:
            raise ValueError("A cannot exceed C: every answer needs a claim")
        if not 1 <= self.k <= self.C:
            raise ValueError("k must lie in [1, C]")
        if self.E > self.C * ((self.E + self.k - 1) // self.k):
            raise ValueError("E cannot be placed without duplicate currency keys")
        if not 0 <= self.f <= self.E:
            raise ValueError("f must lie in [0, E]")
        if not 0.0 <= self.duplicate_content_ratio < 1.0:
            raise ValueError("duplicate_content_ratio must lie in [0, 1)")
        if self.skew < 0.0:
            raise ValueError("skew must be nonnegative")


@dataclass(frozen=True, slots=True)
class TouchedObjects:
    claims: int
    answers: int
    observations: int


@dataclass(frozen=True, slots=True)
class MetricsRecord:
    """One engine/event/trial observation in the frozen JSONL schema."""

    run_id: str
    scenario: str
    seed: int
    trial: int
    event_index: int
    workload: WorkloadParameters
    engine: str
    engine_class: EngineClass
    semantic_equivalent: bool
    event_type: str
    timing_scope: TimingScope
    wall_time_ns: int
    touched_objects: TouchedObjects
    candidate_count: int
    status_changes: int
    maintained_bytes: int
    oracle_included: bool
    copy_staging_included: bool
    semantic_disagreement_count: int
    false_invalidation_count: int
    stale_state_exposure_count: int
    schema_version: str = METRICS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        nonnegative = (
            self.wall_time_ns,
            self.candidate_count,
            self.status_changes,
            self.maintained_bytes,
            self.semantic_disagreement_count,
            self.false_invalidation_count,
            self.stale_state_exposure_count,
        )
        if any(value < 0 for value in nonnegative):
            raise ValueError("metric counts and timings must be nonnegative")
        if (
            self.semantic_equivalent
            and self.engine_class is EngineClass.HEURISTIC_POLICY
        ):
            raise ValueError(
                "heuristic policy baselines cannot claim exact equivalence"
            )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))
