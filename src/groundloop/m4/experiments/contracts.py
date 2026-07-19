"""Immutable contracts for controlled M4.6 policy experiments."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from groundloop.errors import ValidationError
from groundloop.m4.contracts import AdmissionChannel, PairKey, stable_m4_digest
from groundloop.m4.oracles import (
    EventMetricRecord,
    MetricName,
    PairedBootstrapResult,
    PolicyMetricSummary,
)

_HEX = frozenset("0123456789abcdef")


def _require_text(name: str, value: str) -> None:
    if not value.strip():
        raise ValidationError(f"{name} must be non-empty")


def _require_sha256(name: str, value: str) -> None:
    if len(value) != 64 or any(character not in _HEX for character in value):
        raise ValidationError(f"{name} must be a lowercase SHA-256 digest")


def _require_count(name: str, value: int) -> None:
    if type(value) is not int or value < 0:
        raise ValidationError(f"{name} must be a nonnegative integer")


class PolicyKind(StrEnum):
    """Frozen ablations over the four non-learned admission channels."""

    VECTOR_ONLY = "vector_only"
    LEXICAL_ONLY = "lexical_only"
    VECTOR_LEXICAL_UNION = "vector_lexical_union"
    UNION_LINEAGE = "union_lineage"
    UNION_LINEAGE_FRONTIER = "union_lineage_frontier"


@dataclass(frozen=True, slots=True)
class EvaluationPolicy:
    """One treatment with explicit approximate and frontier budgets."""

    policy_id: str
    kind: PolicyKind
    approximate_budget_per_inserted_chunk: int
    frontier_budget_per_inserted_chunk: int

    def __post_init__(self) -> None:
        _require_text("policy_id", self.policy_id)
        if not isinstance(self.kind, PolicyKind):
            raise ValidationError("policy kind must be a frozen M4.6 ablation")
        if (
            type(self.approximate_budget_per_inserted_chunk) is not int
            or self.approximate_budget_per_inserted_chunk <= 0
        ):
            raise ValidationError("approximate budget must be a positive integer")
        _require_count(
            "frontier_budget_per_inserted_chunk",
            self.frontier_budget_per_inserted_chunk,
        )
        frontier_enabled = self.kind is PolicyKind.UNION_LINEAGE_FRONTIER
        if frontier_enabled != (self.frontier_budget_per_inserted_chunk > 0):
            raise ValidationError(
                "frontier budget must be positive exactly for the frontier policy"
            )

    @property
    def manifest_hash(self) -> str:
        return stable_m4_digest(
            "m4-controlled-policy-v1",
            self.policy_id,
            self.kind.value,
            str(self.approximate_budget_per_inserted_chunk),
            str(self.frontier_budget_per_inserted_chunk),
        )


@dataclass(frozen=True, slots=True, order=True)
class RankedChannelCandidate:
    """One frozen treatment input; it is not an oracle judgment."""

    pair: PairKey
    channel: AdmissionChannel
    rank: int

    def __post_init__(self) -> None:
        if self.channel not in {
            AdmissionChannel.VECTOR,
            AdmissionChannel.LEXICAL,
            AdmissionChannel.LINEAGE,
            AdmissionChannel.FRONTIER,
        }:
            raise ValidationError("controlled fixture uses an unsupported channel")
        if type(self.rank) is not int or self.rank <= 0:
            raise ValidationError("candidate rank must be a positive integer")


@dataclass(frozen=True, slots=True, order=True)
class ChannelCount:
    channel: AdmissionChannel
    candidate_count: int

    def __post_init__(self) -> None:
        if not isinstance(self.channel, AdmissionChannel):
            raise ValidationError("channel count requires an admission channel")
        _require_count("candidate_count", self.candidate_count)


@dataclass(frozen=True, slots=True)
class VerifierWorkRecord:
    """Observed adapter work, not a bound inferred from the candidate budget."""

    batch_call_count: int
    attempted_pair_count: int
    completed_pair_count: int
    failed_pair_count: int

    def __post_init__(self) -> None:
        for name, value in (
            ("batch_call_count", self.batch_call_count),
            ("attempted_pair_count", self.attempted_pair_count),
            ("completed_pair_count", self.completed_pair_count),
            ("failed_pair_count", self.failed_pair_count),
        ):
            _require_count(name, value)
        if self.completed_pair_count + self.failed_pair_count != (
            self.attempted_pair_count
        ):
            raise ValidationError("verifier terminal pair counts do not match attempts")
        if self.attempted_pair_count == 0 and self.batch_call_count != 0:
            raise ValidationError("empty verifier work cannot contain a batch call")
        if self.attempted_pair_count > 0 and self.batch_call_count == 0:
            raise ValidationError("attempted verifier work requires a batch call")
        if self.batch_call_count > self.attempted_pair_count:
            raise ValidationError("verifier batches exceed attempted pairs")


@dataclass(frozen=True, slots=True)
class ExhaustiveEventMeasurement:
    """Actual work performed by the exact claim x inserted-chunk audit."""

    history_id: str
    event_id: str
    expected_cartesian_pair_count: int
    work: VerifierWorkRecord
    audit_manifest_id: str

    def __post_init__(self) -> None:
        _require_text("history_id", self.history_id)
        _require_text("event_id", self.event_id)
        _require_text("audit_manifest_id", self.audit_manifest_id)
        _require_count(
            "expected_cartesian_pair_count", self.expected_cartesian_pair_count
        )
        if self.work.attempted_pair_count != self.expected_cartesian_pair_count:
            raise ValidationError("exhaustive work does not equal the Cartesian count")
        if self.work.failed_pair_count:
            raise ValidationError(
                "failed exhaustive attempts need an explicit run failure"
            )


@dataclass(frozen=True, slots=True)
class PolicyEventMeasurement:
    """Raw append-only event record with exact metrics and measured work."""

    policy: EvaluationPolicy
    metric_record: EventMetricRecord
    admitted_pairs: tuple[PairKey, ...]
    channel_counts: tuple[ChannelCount, ...]
    inserted_chunk_count: int
    approximate_selected_pair_count: int
    mandatory_lineage_extra_pair_count: int
    frontier_extra_pair_count: int
    verifier_work: VerifierWorkRecord
    treatment_manifest_hash: str

    def __post_init__(self) -> None:
        if self.admitted_pairs != tuple(sorted(set(self.admitted_pairs))):
            raise ValidationError("admitted pairs must be sorted and unique")
        if self.channel_counts != tuple(sorted(self.channel_counts)):
            raise ValidationError("channel counts must use canonical channel order")
        channels = tuple(item.channel for item in self.channel_counts)
        if len(channels) != len(set(channels)):
            raise ValidationError("channel counts contain duplicate channels")
        for name, value in (
            ("inserted_chunk_count", self.inserted_chunk_count),
            (
                "approximate_selected_pair_count",
                self.approximate_selected_pair_count,
            ),
            (
                "mandatory_lineage_extra_pair_count",
                self.mandatory_lineage_extra_pair_count,
            ),
            ("frontier_extra_pair_count", self.frontier_extra_pair_count),
        ):
            _require_count(name, value)
        _require_sha256("treatment_manifest_hash", self.treatment_manifest_hash)
        if self.metric_record.provenance.treatment_policy_id != self.policy.policy_id:
            raise ValidationError("metric record and policy IDs differ")
        if (
            self.metric_record.provenance.treatment_policy_hash
            != self.policy.manifest_hash
        ):
            raise ValidationError("metric record and policy manifests differ")
        if self.metric_record.treatment_manifest_id != self.treatment_manifest_hash:
            raise ValidationError("metric treatment artifact is not content-bound")
        if self.verifier_work.attempted_pair_count != len(self.admitted_pairs):
            raise ValidationError("verifier attempts differ from admitted unique pairs")
        extras = (
            self.mandatory_lineage_extra_pair_count
            + self.frontier_extra_pair_count
        )
        if len(self.admitted_pairs) != self.approximate_selected_pair_count + extras:
            raise ValidationError(
                "admitted work does not decompose into budget and extras"
            )
        upper = (
            self.inserted_chunk_count
            * self.policy.approximate_budget_per_inserted_chunk
        )
        if self.approximate_selected_pair_count > upper:
            raise ValidationError("approximate selection exceeds its explicit budget")
        lineage_enabled = self.policy.kind in {
            PolicyKind.UNION_LINEAGE,
            PolicyKind.UNION_LINEAGE_FRONTIER,
        }
        if not lineage_enabled and self.mandatory_lineage_extra_pair_count:
            raise ValidationError("lineage extras appear in a policy without lineage")
        if (
            self.policy.kind is not PolicyKind.UNION_LINEAGE_FRONTIER
            and self.frontier_extra_pair_count
        ):
            raise ValidationError("frontier extras appear in a policy without frontier")


@dataclass(frozen=True, slots=True)
class DeliberateMissRecord:
    policy_id: str
    history_id: str
    event_id: str
    designated_pairs: tuple[PairKey, ...]
    oracle_positive_designated_pairs: tuple[PairKey, ...]
    missed_designated_pairs: tuple[PairKey, ...]
    all_missed_positive_pairs: tuple[PairKey, ...]

    def __post_init__(self) -> None:
        for name, value in (
            ("policy_id", self.policy_id),
            ("history_id", self.history_id),
            ("event_id", self.event_id),
        ):
            _require_text(name, value)
        for name, pairs in (
            ("designated_pairs", self.designated_pairs),
            (
                "oracle_positive_designated_pairs",
                self.oracle_positive_designated_pairs,
            ),
            ("missed_designated_pairs", self.missed_designated_pairs),
            ("all_missed_positive_pairs", self.all_missed_positive_pairs),
        ):
            if pairs != tuple(sorted(set(pairs))):
                raise ValidationError(f"{name} must be sorted and unique")
        if not set(self.oracle_positive_designated_pairs) <= set(
            self.designated_pairs
        ):
            raise ValidationError("oracle-positive probes are not designated probes")
        if not set(self.missed_designated_pairs) <= set(
            self.oracle_positive_designated_pairs
        ):
            raise ValidationError("missed probes must be oracle-positive probes")
        if not set(self.missed_designated_pairs) <= set(
            self.all_missed_positive_pairs
        ):
            raise ValidationError("missed probes are absent from all positive misses")


@dataclass(frozen=True, slots=True)
class PolicyEvaluation:
    policy: EvaluationPolicy
    event_measurements: tuple[PolicyEventMeasurement, ...]
    metric_summaries: tuple[PolicyMetricSummary, ...]
    versus_exhaustive_bootstrap: tuple[PairedBootstrapResult, ...]

    def __post_init__(self) -> None:
        if not self.event_measurements:
            raise ValidationError("policy evaluation requires event measurements")
        if any(item.policy != self.policy for item in self.event_measurements):
            raise ValidationError("policy evaluation mixes treatment policies")
        records = tuple(item.metric_record for item in self.event_measurements)
        if records != tuple(
            sorted(
                records,
                key=lambda item: (item.history_id, item.event_index, item.event_id),
            )
        ):
            raise ValidationError("policy event measurements are not canonical")
        event_ids = tuple(record.event_id for record in records)
        if len(event_ids) != len(set(event_ids)):
            raise ValidationError("policy evaluation contains duplicate events")
        expected_metrics = tuple(sorted(MetricName))
        if tuple(item.metric_name for item in self.metric_summaries) != (
            expected_metrics
        ):
            raise ValidationError("policy metric summaries are incomplete")
        if tuple(
            item.metric_name for item in self.versus_exhaustive_bootstrap
        ) != expected_metrics:
            raise ValidationError("policy bootstrap results are incomplete")
        provenance = records[0].provenance
        if any(
            summary.provenance != provenance for summary in self.metric_summaries
        ):
            raise ValidationError("policy summary provenance differs from events")
        if any(
            result.first_provenance != provenance
            for result in self.versus_exhaustive_bootstrap
        ):
            raise ValidationError("policy bootstrap provenance differs from events")
