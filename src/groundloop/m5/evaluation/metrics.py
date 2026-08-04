"""Raw estimands and deterministic clustered bootstrap for M5.5."""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass

import numpy as np
from numpy.typing import NDArray

from groundloop.errors import ValidationError
from groundloop.m5.evaluation.baselines import (
    FROZEN_BASELINES,
    Availability,
    BaselineId,
    BaselinePoint,
    EvaluationRun,
)

BOOTSTRAP_SEED = 20260802
BOOTSTRAP_RESAMPLES = 10_000
BOOTSTRAP_CONFIDENCE_LEVEL = 0.95
BOOTSTRAP_CLUSTER_UNIT = "claim-history/source"
BOOTSTRAP_METHOD = "percentile"
BOOTSTRAP_RNG = "numpy-pcg64-v1"


@dataclass(frozen=True, slots=True)
class RawRate:
    numerator: int
    denominator: int
    value: float | None

    def __post_init__(self) -> None:
        if self.numerator < 0 or self.denominator < 0:
            raise ValidationError("rate counts must be nonnegative")
        if self.numerator > self.denominator:
            raise ValidationError("rate numerator cannot exceed denominator")
        expected = self.numerator / self.denominator if self.denominator else None
        if self.value != expected:
            raise ValidationError("rate value must preserve raw counts exactly")


@dataclass(frozen=True, slots=True)
class BootstrapInterval:
    lower: float | None
    upper: float | None
    confidence_level: float
    seed: int
    requested_resamples: int
    defined_resamples: int
    cluster_count: int
    cluster_unit: str = BOOTSTRAP_CLUSTER_UNIT


@dataclass(frozen=True, slots=True)
class WorkSummary:
    requirement_keys_touched: int
    group_keys_touched: int
    claim_keys_touched: int
    answer_keys_touched: int
    edge_checks: int
    hall_subset_masks: int
    groups_rematched: int
    source_checks: int
    citation_checks: int
    verifier_pairs: int
    model_calls: int
    model_tokens: int
    latency_observation_count: int
    latency_total_ns: int | None
    state_observation_count: int
    state_total_bytes: int
    certificate_observation_count: int
    certificate_total_bytes: int


@dataclass(frozen=True, slots=True)
class BaselineMetricSummary:
    baseline_id: BaselineId
    available_points: int
    unavailable_points: int
    unavailable_reasons: tuple[tuple[str, int], ...]
    false_invalidation: RawRate
    false_invalidation_interval: BootstrapInterval
    false_retention: RawRate
    false_retention_interval: BootstrapInterval
    exact_structured_agreement: RawRate
    exact_structured_agreement_interval: BootstrapInterval
    work: WorkSummary


@dataclass(frozen=True, slots=True)
class PairedDifference:
    comparator: BaselineId
    reference: BaselineId
    metric: str
    paired_points: int
    comparator_value: float | None
    reference_value: float | None
    difference: float | None
    interval: BootstrapInterval


@dataclass(frozen=True, slots=True)
class EvaluationMetricReport:
    schema: str
    event_identity_digest: str
    bootstrap_seed: int
    bootstrap_resamples: int
    bootstrap_rng: str
    bootstrap_method: str
    bootstrap_cluster_unit: str
    bootstrap_confidence_level: float
    summaries: tuple[BaselineMetricSummary, ...]
    paired_differences: tuple[PairedDifference, ...]


@dataclass(frozen=True, slots=True)
class _Counts:
    false_invalidation_numerator: int = 0
    false_invalidation_denominator: int = 0
    false_retention_numerator: int = 0
    false_retention_denominator: int = 0
    exact_agreement_numerator: int = 0
    exact_agreement_denominator: int = 0

    def __add__(self, other: _Counts) -> _Counts:
        return _Counts(
            self.false_invalidation_numerator + other.false_invalidation_numerator,
            self.false_invalidation_denominator + other.false_invalidation_denominator,
            self.false_retention_numerator + other.false_retention_numerator,
            self.false_retention_denominator + other.false_retention_denominator,
            self.exact_agreement_numerator + other.exact_agreement_numerator,
            self.exact_agreement_denominator + other.exact_agreement_denominator,
        )


def _point_counts(point: BaselinePoint) -> _Counts:
    if point.availability is Availability.UNAVAILABLE:
        return _Counts()
    assert point.y_hat is not None
    source = point.source_semantic_label
    assert point.predicted_status is not None
    return _Counts(
        false_invalidation_numerator=int(source and not point.y_hat),
        false_invalidation_denominator=int(source),
        false_retention_numerator=int(not source and point.y_hat),
        false_retention_denominator=int(not source),
        exact_agreement_numerator=int(point.predicted_status is point.exact_status),
        exact_agreement_denominator=1,
    )


def _raw_rate(numerator: int, denominator: int) -> RawRate:
    return RawRate(
        numerator=numerator,
        denominator=denominator,
        value=numerator / denominator if denominator else None,
    )


def _percentile(sorted_values: list[float], probability: float) -> float:
    if not sorted_values:
        raise ValidationError("cannot take a percentile of no values")
    position = probability * (len(sorted_values) - 1)
    lower_index = math.floor(position)
    upper_index = math.ceil(position)
    if lower_index == upper_index:
        return sorted_values[lower_index]
    fraction = position - lower_index
    return (
        sorted_values[lower_index] * (1.0 - fraction)
        + sorted_values[upper_index] * fraction
    )


def _interval(
    values: list[float],
    *,
    confidence_level: float,
    seed: int,
    requested_resamples: int,
    cluster_count: int,
) -> BootstrapInterval:
    if not values:
        return BootstrapInterval(
            lower=None,
            upper=None,
            confidence_level=confidence_level,
            seed=seed,
            requested_resamples=requested_resamples,
            defined_resamples=0,
            cluster_count=cluster_count,
        )
    values.sort()
    alpha = (1.0 - confidence_level) / 2.0
    return BootstrapInterval(
        lower=_percentile(values, alpha),
        upper=_percentile(values, 1.0 - alpha),
        confidence_level=confidence_level,
        seed=seed,
        requested_resamples=requested_resamples,
        defined_resamples=len(values),
        cluster_count=cluster_count,
    )


def _metric_pair(counts: _Counts, metric: str) -> tuple[int, int]:
    if metric == "false_invalidation":
        return (
            counts.false_invalidation_numerator,
            counts.false_invalidation_denominator,
        )
    if metric == "false_retention":
        return counts.false_retention_numerator, counts.false_retention_denominator
    if metric == "exact_structured_agreement":
        return counts.exact_agreement_numerator, counts.exact_agreement_denominator
    raise ValidationError(f"unknown evaluation metric {metric!r}")


def _cluster_counts(
    points: tuple[BaselinePoint, ...], baseline_id: BaselineId
) -> dict[str, _Counts]:
    result: dict[str, _Counts] = defaultdict(_Counts)
    for point in points:
        if point.baseline_id is baseline_id:
            result[point.cluster_id] = result[point.cluster_id] + _point_counts(point)
    return dict(result)


def _counts_matrix(
    cluster_counts: dict[str, _Counts],
    cluster_ids: tuple[str, ...],
) -> NDArray[np.int64]:
    return np.asarray(
        [
            (
                cluster_counts.get(cluster_id, _Counts()).false_invalidation_numerator,
                cluster_counts.get(
                    cluster_id, _Counts()
                ).false_invalidation_denominator,
                cluster_counts.get(cluster_id, _Counts()).false_retention_numerator,
                cluster_counts.get(cluster_id, _Counts()).false_retention_denominator,
                cluster_counts.get(cluster_id, _Counts()).exact_agreement_numerator,
                cluster_counts.get(cluster_id, _Counts()).exact_agreement_denominator,
            )
            for cluster_id in cluster_ids
        ],
        dtype=np.int64,
    ).reshape((len(cluster_ids), 6))


def _bootstrap_metric_values(
    cluster_counts: dict[str, _Counts],
    draws: NDArray[np.int32],
    cluster_ids: tuple[str, ...],
) -> dict[str, list[float]]:
    matrix = _counts_matrix(cluster_counts, cluster_ids)
    result: dict[str, list[float]] = {
        "false_invalidation": [],
        "false_retention": [],
        "exact_structured_agreement": [],
    }
    metric_columns = {
        "false_invalidation": (0, 1),
        "false_retention": (2, 3),
        "exact_structured_agreement": (4, 5),
    }
    batch_size = 256
    for start in range(0, len(draws), batch_size):
        totals = matrix[draws[start : start + batch_size]].sum(axis=1)
        for metric, (numerator_column, denominator_column) in metric_columns.items():
            numerators = totals[:, numerator_column]
            denominators = totals[:, denominator_column]
            defined = denominators != 0
            values = numerators[defined] / denominators[defined]
            result[metric].extend(float(value) for value in values)
    return result


def _paired_bootstrap_differences(
    comparator: dict[str, _Counts],
    reference: dict[str, _Counts],
    cluster_ids: tuple[str, ...],
    draws: NDArray[np.int32],
) -> dict[str, list[float]]:
    comparator_matrix = _counts_matrix(comparator, cluster_ids)
    reference_matrix = _counts_matrix(reference, cluster_ids)
    result: dict[str, list[float]] = {
        "false_invalidation": [],
        "false_retention": [],
    }
    metric_columns = {
        "false_invalidation": (0, 1),
        "false_retention": (2, 3),
    }
    batch_size = 256
    for start in range(0, len(draws), batch_size):
        selected = draws[start : start + batch_size]
        comparator_totals = comparator_matrix[selected].sum(axis=1)
        reference_totals = reference_matrix[selected].sum(axis=1)
        for metric, (numerator_column, denominator_column) in metric_columns.items():
            comparator_denominators = comparator_totals[:, denominator_column]
            reference_denominators = reference_totals[:, denominator_column]
            defined = (comparator_denominators != 0) & (reference_denominators != 0)
            values = (
                comparator_totals[defined, numerator_column]
                / comparator_denominators[defined]
                - reference_totals[defined, numerator_column]
                / reference_denominators[defined]
            )
            result[metric].extend(float(value) for value in values)
    return result


def _sum_work(
    points: tuple[BaselinePoint, ...], baseline_id: BaselineId
) -> WorkSummary:
    selected = tuple(
        point
        for point in points
        if point.baseline_id is baseline_id
        and point.availability is Availability.AVAILABLE
    )
    work_fields = (
        "requirement_keys_touched",
        "group_keys_touched",
        "claim_keys_touched",
        "answer_keys_touched",
        "edge_checks",
        "hall_subset_masks",
        "groups_rematched",
        "source_checks",
        "citation_checks",
        "verifier_pairs",
        "model_calls",
        "model_tokens",
    )
    totals = {
        name: sum(getattr(point.work, name) for point in selected)
        for name in work_fields
    }
    latencies = [point.latency_ns for point in selected if point.latency_ns is not None]
    state_sizes = [
        point.state_bytes for point in selected if point.state_bytes is not None
    ]
    certificate_sizes = [
        point.certificate_bytes
        for point in selected
        if point.certificate_bytes is not None
    ]
    return WorkSummary(
        **totals,
        latency_observation_count=len(latencies),
        latency_total_ns=sum(latencies) if latencies else None,
        state_observation_count=len(state_sizes),
        state_total_bytes=sum(state_sizes),
        certificate_observation_count=len(certificate_sizes),
        certificate_total_bytes=sum(certificate_sizes),
    )


def _paired_cluster_counts(
    points: tuple[BaselinePoint, ...],
    comparator: BaselineId,
    reference: BaselineId,
) -> tuple[dict[str, _Counts], dict[str, _Counts], int]:
    by_key: dict[tuple[str, int, BaselineId], BaselinePoint] = {
        (point.history_id, point.event_ordinal, point.baseline_id): point
        for point in points
    }
    comparator_clusters: dict[str, _Counts] = defaultdict(_Counts)
    reference_clusters: dict[str, _Counts] = defaultdict(_Counts)
    paired_points = 0
    for point in points:
        if (
            point.baseline_id is not comparator
            or point.availability is Availability.UNAVAILABLE
        ):
            continue
        other = by_key.get((point.history_id, point.event_ordinal, reference))
        if other is None or other.availability is Availability.UNAVAILABLE:
            continue
        comparator_clusters[point.cluster_id] = comparator_clusters[
            point.cluster_id
        ] + _point_counts(point)
        reference_clusters[point.cluster_id] = reference_clusters[
            point.cluster_id
        ] + _point_counts(other)
        paired_points += 1
    return dict(comparator_clusters), dict(reference_clusters), paired_points


def build_metric_report(
    run: EvaluationRun,
    *,
    seed: int = BOOTSTRAP_SEED,
    resamples: int = BOOTSTRAP_RESAMPLES,
    confidence_level: float = BOOTSTRAP_CONFIDENCE_LEVEL,
) -> EvaluationMetricReport:
    """Build raw rates and paired cluster-bootstrap intervals.

    One draw table is shared by all strategies and paired differences.  An
    undefined denominator stays undefined; such a replicate is counted but is
    not converted into zero.
    """

    if isinstance(resamples, bool) or resamples <= 0:
        raise ValidationError("bootstrap resamples must be positive")
    if not 0.0 < confidence_level < 1.0:
        raise ValidationError("confidence level must lie strictly between zero and one")
    cluster_ids = tuple(sorted({point.cluster_id for point in run.points}))
    if not cluster_ids:
        raise ValidationError("cannot evaluate an empty history set")
    generator = np.random.Generator(np.random.PCG64(seed))
    draws = generator.integers(
        0,
        len(cluster_ids),
        size=(resamples, len(cluster_ids)),
        dtype=np.int32,
    )
    metrics = (
        "false_invalidation",
        "false_retention",
        "exact_structured_agreement",
    )
    summaries: list[BaselineMetricSummary] = []
    for spec in FROZEN_BASELINES:
        selected = tuple(
            point for point in run.points if point.baseline_id is spec.baseline_id
        )
        available = tuple(
            point for point in selected if point.availability is Availability.AVAILABLE
        )
        unavailable = tuple(
            point
            for point in selected
            if point.availability is Availability.UNAVAILABLE
        )
        total = _Counts()
        for point in available:
            total = total + _point_counts(point)
        clusters = _cluster_counts(run.points, spec.baseline_id)
        bootstrap_values = _bootstrap_metric_values(clusters, draws, cluster_ids)
        intervals = {
            metric: _interval(
                bootstrap_values[metric],
                confidence_level=confidence_level,
                seed=seed,
                requested_resamples=resamples,
                cluster_count=len(cluster_ids),
            )
            for metric in metrics
        }
        summaries.append(
            BaselineMetricSummary(
                baseline_id=spec.baseline_id,
                available_points=len(available),
                unavailable_points=len(unavailable),
                unavailable_reasons=tuple(
                    sorted(
                        (reason, count)
                        for reason, count in defaultdict(
                            int,
                            {
                                reason: sum(
                                    point.unavailable_reason == reason
                                    for point in unavailable
                                )
                                for reason in {
                                    point.unavailable_reason
                                    for point in unavailable
                                    if point.unavailable_reason is not None
                                }
                            },
                        ).items()
                    )
                ),
                false_invalidation=_raw_rate(
                    total.false_invalidation_numerator,
                    total.false_invalidation_denominator,
                ),
                false_invalidation_interval=intervals["false_invalidation"],
                false_retention=_raw_rate(
                    total.false_retention_numerator,
                    total.false_retention_denominator,
                ),
                false_retention_interval=intervals["false_retention"],
                exact_structured_agreement=_raw_rate(
                    total.exact_agreement_numerator,
                    total.exact_agreement_denominator,
                ),
                exact_structured_agreement_interval=intervals[
                    "exact_structured_agreement"
                ],
                work=_sum_work(run.points, spec.baseline_id),
            )
        )

    paired: list[PairedDifference] = []
    reference = BaselineId.GROUNDLOOP_HALL_SDR
    for spec in FROZEN_BASELINES:
        if spec.baseline_id is reference:
            continue
        comparator_clusters, reference_clusters, paired_points = _paired_cluster_counts(
            run.points, spec.baseline_id, reference
        )
        pair_cluster_ids = tuple(
            sorted(set(comparator_clusters) | set(reference_clusters))
        )
        if pair_cluster_ids:
            pair_generator = np.random.Generator(np.random.PCG64(seed))
            pair_draws = pair_generator.integers(
                0,
                len(pair_cluster_ids),
                size=(resamples, len(pair_cluster_ids)),
                dtype=np.int32,
            )
        else:
            pair_draws = np.empty((resamples, 0), dtype=np.int32)
        comparator_ordered = {
            cluster_id: comparator_clusters.get(cluster_id, _Counts())
            for cluster_id in pair_cluster_ids
        }
        reference_ordered = {
            cluster_id: reference_clusters.get(cluster_id, _Counts())
            for cluster_id in pair_cluster_ids
        }
        difference_values = _paired_bootstrap_differences(
            comparator_ordered,
            reference_ordered,
            pair_cluster_ids,
            pair_draws,
        )
        for metric in ("false_invalidation", "false_retention"):
            comparator_total = _Counts()
            reference_total = _Counts()
            for value in comparator_ordered.values():
                comparator_total = comparator_total + value
            for value in reference_ordered.values():
                reference_total = reference_total + value
            comp_n, comp_d = _metric_pair(comparator_total, metric)
            ref_n, ref_d = _metric_pair(reference_total, metric)
            comparator_value = comp_n / comp_d if comp_d else None
            reference_value = ref_n / ref_d if ref_d else None
            paired.append(
                PairedDifference(
                    comparator=spec.baseline_id,
                    reference=reference,
                    metric=metric,
                    paired_points=paired_points,
                    comparator_value=comparator_value,
                    reference_value=reference_value,
                    difference=(
                        comparator_value - reference_value
                        if comparator_value is not None and reference_value is not None
                        else None
                    ),
                    interval=_interval(
                        difference_values[metric],
                        confidence_level=confidence_level,
                        seed=seed,
                        requested_resamples=resamples,
                        cluster_count=len(pair_cluster_ids),
                    ),
                )
            )
    return EvaluationMetricReport(
        schema="groundloop-m5-metric-report-v1",
        event_identity_digest=run.event_identity_digest,
        bootstrap_seed=seed,
        bootstrap_resamples=resamples,
        bootstrap_rng=BOOTSTRAP_RNG,
        bootstrap_method=BOOTSTRAP_METHOD,
        bootstrap_cluster_unit=BOOTSTRAP_CLUSTER_UNIT,
        bootstrap_confidence_level=confidence_level,
        summaries=tuple(summaries),
        paired_differences=tuple(paired),
    )


__all__ = [
    "BOOTSTRAP_CLUSTER_UNIT",
    "BOOTSTRAP_CONFIDENCE_LEVEL",
    "BOOTSTRAP_METHOD",
    "BOOTSTRAP_RNG",
    "BOOTSTRAP_RESAMPLES",
    "BOOTSTRAP_SEED",
    "BaselineMetricSummary",
    "BootstrapInterval",
    "EvaluationMetricReport",
    "PairedDifference",
    "RawRate",
    "WorkSummary",
    "build_metric_report",
]
