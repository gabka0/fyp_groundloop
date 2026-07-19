"""Deterministic, provenance-safe evaluation mechanics for M4."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum

from groundloop.errors import ValidationError
from groundloop.m4.contracts import stable_m4_digest

_HEX = frozenset("0123456789abcdef")


def _require_text(name: str, value: str) -> None:
    if not value.strip():
        raise ValidationError(f"{name} must be non-empty")


def _require_sha256(name: str, value: str) -> None:
    if len(value) != 64 or any(character not in _HEX for character in value):
        raise ValidationError(f"{name} must be a lowercase SHA-256 digest")


class MetricUnit(StrEnum):
    PAIR = "pair"
    CLAIM = "claim"
    STATUS_CLAIM = "status_claim"
    ANSWER = "answer"


class MetricName(StrEnum):
    POSITIVE_PAIR_RECALL = "positive_pair_recall"
    POSITIVE_CLAIM_RECALL = "positive_claim_recall"
    STATUS_EFFECT_RECALL = "status_effect_recall"
    ANSWER_EFFECT_RECALL = "answer_effect_recall"

    @property
    def unit(self) -> MetricUnit:
        return {
            MetricName.POSITIVE_PAIR_RECALL: MetricUnit.PAIR,
            MetricName.POSITIVE_CLAIM_RECALL: MetricUnit.CLAIM,
            MetricName.STATUS_EFFECT_RECALL: MetricUnit.STATUS_CLAIM,
            MetricName.ANSWER_EFFECT_RECALL: MetricUnit.ANSWER,
        }[self]


class OracleKind(StrEnum):
    EXHAUSTIVE_DELTA = "exhaustive_delta"
    SNAPSHOT_REFRESH = "snapshot_refresh"


@dataclass(frozen=True, slots=True)
class MetricCount:
    """One event-level integer numerator and denominator."""

    metric_name: MetricName
    numerator: int
    denominator: int

    def __post_init__(self) -> None:
        if not isinstance(self.metric_name, MetricName):
            raise ValidationError("metric_name must be a frozen M4 metric")
        if type(self.numerator) is not int or type(self.denominator) is not int:
            raise ValidationError("metric numerator and denominator must be integers")
        if self.numerator < 0 or self.denominator < 0:
            raise ValidationError("metric counts must be nonnegative integers")
        if self.numerator > self.denominator:
            raise ValidationError("metric numerator exceeds denominator")

    @property
    def unit(self) -> MetricUnit:
        return self.metric_name.unit

    @property
    def value(self) -> float | None:
        """Return N/A as None; never manufacture zero or one for 0/0."""
        if self.denominator == 0:
            return None
        return self.numerator / self.denominator


@dataclass(frozen=True, slots=True)
class EvaluationProvenance:
    """Identities that must be homogeneous within an aggregation."""

    schema_version: str
    dataset_version: str
    split_id: str
    split_manifest_hash: str
    seed_manifest_hash: str
    treatment_policy_id: str
    treatment_policy_hash: str
    verifier_model_artifact_id: str
    verifier_execution_spec_hash: str
    decision_policy_id: str
    oracle_kind: OracleKind
    baseline_id: str
    oracle_policy_id: str
    oracle_policy_hash: str

    def __post_init__(self) -> None:
        if self.schema_version != "m4-evaluation-v1":
            raise ValidationError("unsupported evaluation schema_version")
        if not isinstance(self.oracle_kind, OracleKind):
            raise ValidationError("oracle_kind must be a frozen M4 oracle kind")
        for name, value in (
            ("schema_version", self.schema_version),
            ("dataset_version", self.dataset_version),
            ("split_id", self.split_id),
            ("treatment_policy_id", self.treatment_policy_id),
            ("verifier_model_artifact_id", self.verifier_model_artifact_id),
            ("decision_policy_id", self.decision_policy_id),
            ("baseline_id", self.baseline_id),
            ("oracle_policy_id", self.oracle_policy_id),
        ):
            _require_text(name, value)
        for name, value in (
            ("split_manifest_hash", self.split_manifest_hash),
            ("seed_manifest_hash", self.seed_manifest_hash),
            ("treatment_policy_hash", self.treatment_policy_hash),
            ("verifier_execution_spec_hash", self.verifier_execution_spec_hash),
            ("oracle_policy_hash", self.oracle_policy_hash),
        ):
            _require_sha256(name, value)

    @property
    def treatment_identity(self) -> tuple[str, str]:
        return self.treatment_policy_id, self.treatment_policy_hash

    @property
    def manifest_hash(self) -> str:
        return stable_m4_digest(
            "m4-evaluation-provenance-v1",
            self.schema_version,
            self.dataset_version,
            self.split_id,
            self.split_manifest_hash,
            self.seed_manifest_hash,
            self.treatment_policy_id,
            self.treatment_policy_hash,
            self.verifier_model_artifact_id,
            self.verifier_execution_spec_hash,
            self.decision_policy_id,
            self.oracle_kind.value,
            self.baseline_id,
            self.oracle_policy_id,
            self.oracle_policy_hash,
        )

    @property
    def pairing_identity(self) -> tuple[object, ...]:
        """Common experimental identity, excluding the compared treatment."""
        return (
            self.schema_version,
            self.dataset_version,
            self.split_id,
            self.split_manifest_hash,
            self.seed_manifest_hash,
            self.verifier_model_artifact_id,
            self.verifier_execution_spec_hash,
            self.decision_policy_id,
            self.oracle_kind,
            self.baseline_id,
            self.oracle_policy_id,
            self.oracle_policy_hash,
        )


@dataclass(frozen=True, slots=True)
class EventMetricRecord:
    """Append-only raw metrics for one policy on one ordered event."""

    run_id: str
    provenance: EvaluationProvenance
    history_id: str
    event_id: str
    event_index: int
    event_type: str
    corpus_snapshot_before_hash: str
    corpus_snapshot_after_hash: str
    treatment_manifest_id: str
    baseline_manifest_id: str
    metrics: tuple[MetricCount, ...]
    failure_code: str | None = None

    def __post_init__(self) -> None:
        for name, value in (
            ("run_id", self.run_id),
            ("history_id", self.history_id),
            ("event_id", self.event_id),
            ("event_type", self.event_type),
            ("treatment_manifest_id", self.treatment_manifest_id),
            ("baseline_manifest_id", self.baseline_manifest_id),
        ):
            _require_text(name, value)
        if type(self.event_index) is not int or self.event_index < 0:
            raise ValidationError("event_index must be nonnegative")
        _require_sha256(
            "corpus_snapshot_before_hash", self.corpus_snapshot_before_hash
        )
        _require_sha256(
            "corpus_snapshot_after_hash", self.corpus_snapshot_after_hash
        )
        if self.failure_code is not None:
            _require_text("failure_code", self.failure_code)
        expected_names = frozenset(MetricName)
        actual_names = tuple(metric.metric_name for metric in self.metrics)
        if len(set(actual_names)) != len(actual_names):
            raise ValidationError("event metrics contain duplicate metric names")
        if set(actual_names) != expected_names:
            raise ValidationError("event record requires every frozen metric")
        expected_order = tuple(sorted(self.metrics, key=lambda item: item.metric_name))
        if self.metrics != expected_order:
            raise ValidationError("event metrics must use canonical name order")

    def metric(self, name: MetricName) -> MetricCount:
        return next(metric for metric in self.metrics if metric.metric_name is name)

    @property
    def event_identity(self) -> tuple[object, ...]:
        return (
            self.history_id,
            self.event_id,
            self.event_index,
            self.event_type,
            self.corpus_snapshot_before_hash,
            self.corpus_snapshot_after_hash,
            self.baseline_manifest_id,
        )


@dataclass(frozen=True, slots=True)
class PolicyRun:
    run_id: str
    provenance: EvaluationProvenance
    records: tuple[EventMetricRecord, ...]


def validate_policy_run(records: Iterable[EventMetricRecord]) -> PolicyRun:
    """Canonicalize one run and reject mixed policy/split/model provenance."""
    materialized = tuple(records)
    if not materialized:
        raise ValidationError("policy run must contain at least one event")
    run_id = materialized[0].run_id
    provenance = materialized[0].provenance
    event_ids: set[str] = set()
    event_positions: set[tuple[str, int]] = set()
    for record in materialized:
        if record.run_id != run_id:
            raise ValidationError("aggregation mixes run identities")
        if record.provenance != provenance:
            raise ValidationError("aggregation mixes evaluation provenance")
        if record.event_id in event_ids:
            raise ValidationError(f"duplicate event ID {record.event_id}")
        position = (record.history_id, record.event_index)
        if position in event_positions:
            raise ValidationError("duplicate event index within a history")
        event_ids.add(record.event_id)
        event_positions.add(position)
    ordered = tuple(
        sorted(
            materialized,
            key=lambda record: (
                record.history_id,
                record.event_index,
                record.event_id,
            ),
        )
    )
    return PolicyRun(run_id=run_id, provenance=provenance, records=ordered)


@dataclass(frozen=True, slots=True)
class PolicyMetricSummary:
    run_id: str
    provenance: EvaluationProvenance
    metric_name: MetricName
    total_event_count: int
    eligible_event_count: int
    pooled_numerator: int
    pooled_denominator: int
    micro_value: float | None
    macro_event_value: float | None


def summarize_policy_metric(
    records: Iterable[EventMetricRecord], metric_name: MetricName
) -> PolicyMetricSummary:
    run = validate_policy_run(records)
    counts = tuple(record.metric(metric_name) for record in run.records)
    eligible_values = tuple(
        count.value for count in counts if count.value is not None
    )
    numerator = sum(count.numerator for count in counts)
    denominator = sum(count.denominator for count in counts)
    return PolicyMetricSummary(
        run_id=run.run_id,
        provenance=run.provenance,
        metric_name=metric_name,
        total_event_count=len(counts),
        eligible_event_count=len(eligible_values),
        pooled_numerator=numerator,
        pooled_denominator=denominator,
        micro_value=None if denominator == 0 else numerator / denominator,
        macro_event_value=(
            None
            if not eligible_values
            else math.fsum(eligible_values) / len(eligible_values)
        ),
    )


@dataclass(frozen=True, slots=True)
class AlignedEventPair:
    first: EventMetricRecord
    second: EventMetricRecord


@dataclass(frozen=True, slots=True)
class PairedPolicyRun:
    first: PolicyRun
    second: PolicyRun
    event_pairs: tuple[AlignedEventPair, ...]


def align_paired_policy_records(
    first_records: Iterable[EventMetricRecord],
    second_records: Iterable[EventMetricRecord],
) -> PairedPolicyRun:
    """Align two distinct policies on an identical event/oracle population."""
    first = validate_policy_run(first_records)
    second = validate_policy_run(second_records)
    if first.provenance.pairing_identity != second.provenance.pairing_identity:
        raise ValidationError("paired runs have mismatched experimental identities")
    if first.provenance.treatment_identity == second.provenance.treatment_identity:
        raise ValidationError("paired comparison requires distinct treatment policies")
    if (
        first.provenance.treatment_policy_id
        == second.provenance.treatment_policy_id
    ):
        raise ValidationError("treatment policy ID maps to conflicting hashes")
    if (
        first.provenance.treatment_policy_hash
        == second.provenance.treatment_policy_hash
    ):
        raise ValidationError("paired comparison requires distinct policy hashes")

    first_by_id = {record.event_id: record for record in first.records}
    second_by_id = {record.event_id: record for record in second.records}
    if first_by_id.keys() != second_by_id.keys():
        missing_first = sorted(second_by_id.keys() - first_by_id.keys())
        missing_second = sorted(first_by_id.keys() - second_by_id.keys())
        raise ValidationError(
            "paired runs have missing event IDs: "
            f"first={missing_first}, second={missing_second}"
        )

    pairs: list[AlignedEventPair] = []
    for event_id in sorted(first_by_id):
        first_record = first_by_id[event_id]
        second_record = second_by_id[event_id]
        if first_record.event_identity != second_record.event_identity:
            raise ValidationError(f"event identity mismatch for {event_id}")
        for metric_name in MetricName:
            first_count = first_record.metric(metric_name)
            second_count = second_record.metric(metric_name)
            if first_count.denominator != second_count.denominator:
                raise ValidationError(
                    f"paired denominator mismatch for {event_id}/{metric_name.value}"
                )
        pairs.append(AlignedEventPair(first_record, second_record))
    pairs.sort(
        key=lambda pair: (
            pair.first.history_id,
            pair.first.event_index,
            pair.first.event_id,
        )
    )
    return PairedPolicyRun(first=first, second=second, event_pairs=tuple(pairs))


@dataclass(frozen=True, slots=True)
class BootstrapConfig:
    config_id: str
    seed: int
    replicate_count: int
    confidence_level: float = 0.95
    minimum_cluster_count: int = 2
    interval_method: str = "percentile-v1"
    estimand: str = "macro-history-pooled-ratio-difference-v1"

    def __post_init__(self) -> None:
        _require_text("config_id", self.config_id)
        if type(self.seed) is not int or self.seed < 0:
            raise ValidationError("bootstrap seed must be nonnegative")
        if type(self.replicate_count) is not int or self.replicate_count <= 0:
            raise ValidationError("bootstrap replicate_count must be positive")
        if (
            type(self.minimum_cluster_count) is not int
            or self.minimum_cluster_count < 2
        ):
            raise ValidationError("bootstrap requires at least two clusters")
        if type(self.confidence_level) is not float or not (
            0.0 < self.confidence_level < 1.0
        ):
            raise ValidationError("bootstrap confidence_level must lie in (0,1)")
        if self.interval_method != "percentile-v1":
            raise ValidationError("unsupported bootstrap interval method")
        if self.estimand != "macro-history-pooled-ratio-difference-v1":
            raise ValidationError("unsupported bootstrap estimand")

    @property
    def manifest_hash(self) -> str:
        return stable_m4_digest(
            "m4-history-bootstrap-config-v1",
            self.config_id,
            str(self.seed),
            str(self.replicate_count),
            self.confidence_level.hex(),
            str(self.minimum_cluster_count),
            self.interval_method,
            self.estimand,
        )


FROZEN_HISTORY_BOOTSTRAP_V1 = BootstrapConfig(
    config_id="m4-history-bootstrap-v1",
    seed=20260719,
    replicate_count=10_000,
    confidence_level=0.95,
    minimum_cluster_count=2,
)


@dataclass(frozen=True, slots=True)
class PairedBootstrapResult:
    metric_name: MetricName
    first_provenance: EvaluationProvenance
    second_provenance: EvaluationProvenance
    config_manifest_hash: str
    total_cluster_ids: tuple[str, ...]
    eligible_cluster_ids: tuple[str, ...]
    observed_difference: float | None
    confidence_lower: float | None
    confidence_upper: float | None
    replicate_differences: tuple[float, ...]

    @property
    def total_cluster_count(self) -> int:
        return len(self.total_cluster_ids)

    @property
    def eligible_cluster_count(self) -> int:
        return len(self.eligible_cluster_ids)


def _deterministic_draw(
    *, seed: int, replicate_index: int, draw_index: int, population_size: int
) -> int:
    digest = stable_m4_digest(
        "m4-history-bootstrap-draw-v1",
        str(seed),
        str(replicate_index),
        str(draw_index),
    )
    return int(digest[:16], 16) % population_size


def _quantile(values: tuple[float, ...], probability: float) -> float:
    ordered = tuple(sorted(values))
    rank = (len(ordered) - 1) * probability
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[lower]
    fraction = rank - lower
    return ordered[lower] + fraction * (ordered[upper] - ordered[lower])


def paired_history_cluster_bootstrap(
    paired: PairedPolicyRun,
    *,
    metric_name: MetricName,
    config: BootstrapConfig,
) -> PairedBootstrapResult:
    """Bootstrap paired macro differences by independent history clusters."""
    by_history: dict[str, list[AlignedEventPair]] = defaultdict(list)
    for pair in paired.event_pairs:
        by_history[pair.first.history_id].append(pair)
    total_cluster_ids = tuple(sorted(by_history))

    history_differences: dict[str, float] = {}
    for history_id in total_cluster_ids:
        event_pairs = by_history[history_id]
        first_counts = [pair.first.metric(metric_name) for pair in event_pairs]
        second_counts = [pair.second.metric(metric_name) for pair in event_pairs]
        first_denominator = sum(count.denominator for count in first_counts)
        second_denominator = sum(count.denominator for count in second_counts)
        if first_denominator != second_denominator:
            raise ValidationError("paired history denominators differ")
        if first_denominator == 0:
            continue
        first_value = sum(count.numerator for count in first_counts) / first_denominator
        second_value = (
            sum(count.numerator for count in second_counts) / second_denominator
        )
        history_differences[history_id] = first_value - second_value

    eligible_cluster_ids = tuple(sorted(history_differences))
    if not eligible_cluster_ids:
        return PairedBootstrapResult(
            metric_name=metric_name,
            first_provenance=paired.first.provenance,
            second_provenance=paired.second.provenance,
            config_manifest_hash=config.manifest_hash,
            total_cluster_ids=total_cluster_ids,
            eligible_cluster_ids=(),
            observed_difference=None,
            confidence_lower=None,
            confidence_upper=None,
            replicate_differences=(),
        )

    observed = math.fsum(history_differences.values()) / len(history_differences)
    cluster_count = len(eligible_cluster_ids)
    if cluster_count < config.minimum_cluster_count:
        return PairedBootstrapResult(
            metric_name=metric_name,
            first_provenance=paired.first.provenance,
            second_provenance=paired.second.provenance,
            config_manifest_hash=config.manifest_hash,
            total_cluster_ids=total_cluster_ids,
            eligible_cluster_ids=eligible_cluster_ids,
            observed_difference=observed,
            confidence_lower=None,
            confidence_upper=None,
            replicate_differences=(),
        )

    replicate_values: list[float] = []
    for replicate_index in range(config.replicate_count):
        sampled = tuple(
            eligible_cluster_ids[
                _deterministic_draw(
                    seed=config.seed,
                    replicate_index=replicate_index,
                    draw_index=draw_index,
                    population_size=cluster_count,
                )
            ]
            for draw_index in range(cluster_count)
        )
        replicate_values.append(
            math.fsum(history_differences[history_id] for history_id in sampled)
            / cluster_count
        )
    replicates = tuple(replicate_values)
    tail = (1.0 - config.confidence_level) / 2.0
    return PairedBootstrapResult(
        metric_name=metric_name,
        first_provenance=paired.first.provenance,
        second_provenance=paired.second.provenance,
        config_manifest_hash=config.manifest_hash,
        total_cluster_ids=total_cluster_ids,
        eligible_cluster_ids=eligible_cluster_ids,
        observed_difference=observed,
        confidence_lower=_quantile(replicates, tail),
        confidence_upper=_quantile(replicates, 1.0 - tail),
        replicate_differences=replicates,
    )
