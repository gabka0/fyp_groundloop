"""Frozen-history evaluation for M4 selective semantic maintenance.

The module consumes immutable outputs from the M4 event audit, snapshot
refresh and admission lanes.  It does not run retrieval or a verifier and it
does not treat empirical semantic quality as an exact IVM property.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import cast

from groundloop.errors import ValidationError
from groundloop.m4.contracts import PairKey, SnapshotRefreshResult, stable_m4_digest
from groundloop.m4.event_audit import PersistedEventAudit

_HEX = frozenset("0123456789abcdef")


def _require_text(name: str, value: str) -> None:
    if not value.strip():
        raise ValidationError(f"{name} must be non-empty")


def _require_hash(name: str, value: str) -> None:
    if len(value) != 64 or any(character not in _HEX for character in value):
        raise ValidationError(f"{name} must be a lowercase SHA-256 digest")


def _require_count(name: str, value: int) -> None:
    if type(value) is not int or value < 0:
        raise ValidationError(f"{name} must be a nonnegative integer")


def _canonical_json(payload: object) -> str:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _json_hash(payload: object) -> str:
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _manifest_object(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(
        not isinstance(key, str) for key in value
    ):
        raise ValidationError(f"{field} must be a JSON object")
    return cast(dict[str, object], value)


def _manifest_strings(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ValidationError(f"{field} must be a string array")
    result = tuple(cast(list[str], value))
    if result != tuple(sorted(set(result))):
        raise ValidationError(f"{field} must be sorted and unique")
    return result


def _manifest_pairs(value: object, field: str) -> tuple[PairKey, ...]:
    if not isinstance(value, list):
        raise ValidationError(f"{field} must be an array")
    result: list[PairKey] = []
    for raw_pair in value:
        pair = _manifest_object(raw_pair, field)
        if set(pair) != {"claim_id", "chunk_version_id"}:
            raise ValidationError(f"{field} contains an invalid pair")
        claim_id = pair["claim_id"]
        chunk_id = pair["chunk_version_id"]
        if not isinstance(claim_id, str) or not isinstance(chunk_id, str):
            raise ValidationError(f"{field} pair identities must be strings")
        result.append(PairKey(claim_id, chunk_id))
    pairs = tuple(result)
    if pairs != tuple(sorted(set(pairs))):
        raise ValidationError(f"{field} must be sorted and unique")
    return pairs


def _manifest_status_effects(
    *,
    state_rows: object,
    affected_ids: object,
    id_field: str,
    field: str,
) -> tuple[ObjectStatus, ...]:
    if not isinstance(state_rows, list):
        raise ValidationError(f"{field} must be an array")
    statuses: dict[str, str] = {}
    for raw_state in state_rows:
        state = _manifest_object(raw_state, field)
        object_id = state.get(id_field)
        status = state.get("status")
        if not isinstance(object_id, str) or not isinstance(status, str):
            raise ValidationError(f"{field} contains an invalid state")
        if object_id in statuses:
            raise ValidationError(f"{field} contains duplicate object IDs")
        statuses[object_id] = status
    selected_ids = _manifest_strings(affected_ids, f"{field}.affected_ids")
    if not set(selected_ids) <= set(statuses):
        raise ValidationError(f"{field} omits an affected object state")
    return tuple(
        ObjectStatus(object_id, statuses[object_id])
        for object_id in selected_ids
    )


class AblationKind(StrEnum):
    """The seven M4.9 treatments evaluated on identical event identities."""

    EXHAUSTIVE_REFRESH = "exhaustive_refresh"
    VECTOR_ONLY = "vector_only"
    LEXICAL_ONLY = "lexical_only"
    UNION = "union"
    LINEAGE = "lineage"
    FRONTIER = "frontier"
    FRESH_FALLBACK = "fresh_fallback"


class EmpiricalMetric(StrEnum):
    """Baseline-qualified recall metrics; zero denominators are N/A."""

    POSITIVE_PAIR_IMPACT_RECALL = "positive_pair_impact_recall"
    POSITIVE_CLAIM_IMPACT_RECALL = "positive_claim_impact_recall"
    CLAIM_STATUS_EFFECT_RECALL = "claim_status_effect_recall"
    ANSWER_STATUS_EFFECT_RECALL = "answer_status_effect_recall"


@dataclass(frozen=True, slots=True, order=True)
class ObjectStatus:
    object_id: str
    status: str

    def __post_init__(self) -> None:
        _require_text("object_id", self.object_id)
        _require_text("status", self.status)


@dataclass(frozen=True, slots=True)
class HistoryAssignment:
    """A history and every connected component used for split isolation."""

    history_id: str
    split_id: str
    component_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _require_text("history_id", self.history_id)
        _require_text("split_id", self.split_id)
        if not self.component_ids:
            raise ValidationError("history requires at least one component identity")
        if self.component_ids != tuple(sorted(set(self.component_ids))):
            raise ValidationError("component_ids must be sorted and unique")
        if any(not item.strip() for item in self.component_ids):
            raise ValidationError("component_ids must be non-empty")


@dataclass(frozen=True, slots=True)
class PolicySpec:
    policy_id: str
    kind: AblationKind
    policy_hash: str

    def __post_init__(self) -> None:
        _require_text("policy_id", self.policy_id)
        if not isinstance(self.kind, AblationKind):
            raise ValidationError("kind must be a frozen M4.9 ablation")
        _require_hash("policy_hash", self.policy_hash)


@dataclass(frozen=True, slots=True)
class FrozenOracleEvent:
    """Event-audit truth and the independently computed SnapshotRefresh_k."""

    history_id: str
    event_id: str
    event_index: int
    event_type: str
    event_manifest_hash: str
    event_audit_manifest_id: str
    event_audit_result_hash: str
    snapshot_refresh_manifest_id: str
    positive_pairs: tuple[PairKey, ...]
    claim_status_effects: tuple[ObjectStatus, ...]
    answer_status_effects: tuple[ObjectStatus, ...]
    snapshot_refresh_pairs: tuple[PairKey, ...]
    oracle_projection_hash: str = ""

    def __post_init__(self) -> None:
        for name, value in (
            ("history_id", self.history_id),
            ("event_id", self.event_id),
            ("event_type", self.event_type),
            ("event_audit_manifest_id", self.event_audit_manifest_id),
            ("snapshot_refresh_manifest_id", self.snapshot_refresh_manifest_id),
        ):
            _require_text(name, value)
        if type(self.event_index) is not int or self.event_index < 0:
            raise ValidationError("event_index must be nonnegative")
        _require_hash("event_manifest_hash", self.event_manifest_hash)
        _require_hash("event_audit_result_hash", self.event_audit_result_hash)
        pair_fields = (
            ("positive_pairs", self.positive_pairs),
            ("snapshot_refresh_pairs", self.snapshot_refresh_pairs),
        )
        for pair_name, pair_values in pair_fields:
            if pair_values != tuple(sorted(set(pair_values))):
                raise ValidationError(f"{pair_name} must be sorted and unique")
        status_fields = (
            ("claim_status_effects", self.claim_status_effects),
            ("answer_status_effects", self.answer_status_effects),
        )
        for status_name, status_values in status_fields:
            if status_values != tuple(sorted(status_values)):
                raise ValidationError(f"{status_name} must use canonical order")
            if len({item.object_id for item in status_values}) != len(
                status_values
            ):
                raise ValidationError(
                    f"{status_name} contains duplicate object IDs"
                )
        expected_projection_hash = self.expected_projection_hash()
        if not self.oracle_projection_hash:
            object.__setattr__(
                self, "oracle_projection_hash", expected_projection_hash
            )
        elif self.oracle_projection_hash != expected_projection_hash:
            raise ValidationError("oracle projection hash does not match payload")

    def expected_projection_hash(self) -> str:
        return stable_m4_digest(
            "m4-empirical-oracle-projection-v1",
            self.event_id,
            self.event_audit_manifest_id,
            self.event_audit_result_hash,
            self.snapshot_refresh_manifest_id,
            *(
                part
                for pair in self.positive_pairs
                for part in (pair.claim_id, pair.chunk_version_id)
            ),
            "claim-effects",
            *(
                part
                for item in self.claim_status_effects
                for part in (item.object_id, item.status)
            ),
            "answer-effects",
            *(
                part
                for item in self.answer_status_effects
                for part in (item.object_id, item.status)
            ),
            "refresh-pairs",
            *(
                part
                for pair in self.snapshot_refresh_pairs
                for part in (pair.claim_id, pair.chunk_version_id)
            ),
        )

    @classmethod
    def from_persisted_manifest(
        cls,
        *,
        audit: PersistedEventAudit,
        refresh: SnapshotRefreshResult,
        persisted_manifest: Mapping[str, object],
        history_id: str,
        event_index: int,
        event_type: str,
        event_manifest_hash: str,
    ) -> FrozenOracleEvent:
        """Derive exact metric targets from a content-validated stored manifest.

        The caller may not supply pair identities or status targets.  The
        projection is parsed from the immutable event-audit result whose full
        JSON hash must match ``PersistedEventAudit.result_hash``.
        """
        manifest = dict(persisted_manifest)
        manifest_hash = manifest.get("manifest_hash")
        if not isinstance(manifest_hash, str):
            raise ValidationError("persisted event-audit manifest hash is absent")
        unsigned = dict(manifest)
        del unsigned["manifest_hash"]
        if _json_hash(unsigned) != manifest_hash:
            raise ValidationError("persisted event-audit manifest hash mismatch")
        result = _manifest_object(manifest.get("result"), "result")
        result_hash = manifest.get("result_hash")
        if result_hash != audit.result_hash or _json_hash(result) != audit.result_hash:
            raise ValidationError("persisted event-audit result hash mismatch")
        event = _manifest_object(manifest.get("event"), "event")
        if event.get("event_id") != audit.event_id or event.get("epoch_id") != (
            audit.epoch_id
        ):
            raise ValidationError("persisted event-audit event identity mismatch")
        full_pair = _manifest_object(
            result.get("full_pair_audit"), "full_pair_audit"
        )
        if full_pair.get("manifest_id") != audit.audit_manifest_id:
            raise ValidationError("persisted full-pair audit identity mismatch")
        positive_pairs = _manifest_pairs(
            full_pair.get("positive_pairs"), "positive_pairs"
        )
        if len(positive_pairs) != audit.positive_pair_count:
            raise ValidationError("persisted positive pair count mismatch")
        refresh_payload = _manifest_object(
            result.get("snapshot_refresh"), "snapshot_refresh"
        )
        if (
            refresh_payload.get("manifest_id") != audit.refresh_manifest_id
            or refresh.manifest_id != audit.refresh_manifest_id
        ):
            raise ValidationError("event audit and SnapshotRefresh_k manifests differ")
        refresh_pairs = _manifest_pairs(
            refresh_payload.get("retrieved_pairs"), "snapshot_refresh.retrieved_pairs"
        )
        if refresh_pairs != refresh.retrieved_pairs:
            raise ValidationError("persisted and supplied refresh pairs differ")
        exhaustive = _manifest_object(
            result.get("exhaustive_additive"), "exhaustive_additive"
        )
        affected = _manifest_object(
            exhaustive.get("affected_from_working"),
            "exhaustive_additive.affected_from_working",
        )
        claim_status_effects = _manifest_status_effects(
            state_rows=exhaustive.get("claim_states"),
            affected_ids=affected.get("status_claim_ids"),
            id_field="claim_id",
            field="exhaustive_additive.claim_states",
        )
        answer_status_effects = _manifest_status_effects(
            state_rows=exhaustive.get("answer_states"),
            affected_ids=affected.get("answer_status_ids"),
            id_field="answer_version_id",
            field="exhaustive_additive.answer_states",
        )
        return cls(
            history_id=history_id,
            event_id=audit.event_id,
            event_index=event_index,
            event_type=event_type,
            event_manifest_hash=event_manifest_hash,
            event_audit_manifest_id=audit.audit_manifest_id,
            event_audit_result_hash=audit.result_hash,
            snapshot_refresh_manifest_id=refresh.manifest_id,
            positive_pairs=positive_pairs,
            claim_status_effects=claim_status_effects,
            answer_status_effects=answer_status_effects,
            snapshot_refresh_pairs=refresh.retrieved_pairs,
        )


@dataclass(frozen=True, slots=True)
class VerifierMeasurement:
    """Observed work. Optional fields remain missing rather than becoming zero."""

    attempted_pair_count: int
    completed_pair_count: int
    failed_pair_count: int
    timeout_pair_count: int
    call_count: int
    input_tokens: int | None = None
    output_tokens: int | None = None
    retrieval_latency_ms: float | None = None
    verifier_latency_ms: float | None = None
    end_to_end_latency_ms: float | None = None

    def __post_init__(self) -> None:
        integer_fields = (
            ("attempted_pair_count", self.attempted_pair_count),
            ("completed_pair_count", self.completed_pair_count),
            ("failed_pair_count", self.failed_pair_count),
            ("timeout_pair_count", self.timeout_pair_count),
            ("call_count", self.call_count),
        )
        for count_name, count_value in integer_fields:
            _require_count(count_name, count_value)
        terminal_count = (
            self.completed_pair_count
            + self.failed_pair_count
            + self.timeout_pair_count
        )
        if terminal_count != self.attempted_pair_count:
            raise ValidationError("verifier outcomes do not sum to attempted pairs")
        if self.attempted_pair_count == 0 and self.call_count != 0:
            raise ValidationError("zero attempted pairs cannot have verifier calls")
        if self.attempted_pair_count > 0 and self.call_count == 0:
            raise ValidationError("attempted verifier pairs require at least one call")
        if self.call_count > self.attempted_pair_count:
            raise ValidationError("verifier calls cannot exceed attempted pairs")
        optional_counts = (
            ("input_tokens", self.input_tokens),
            ("output_tokens", self.output_tokens),
        )
        for optional_count_name, optional_count in optional_counts:
            if optional_count is not None:
                _require_count(optional_count_name, optional_count)
        optional_latencies = (
            ("retrieval_latency_ms", self.retrieval_latency_ms),
            ("verifier_latency_ms", self.verifier_latency_ms),
            ("end_to_end_latency_ms", self.end_to_end_latency_ms),
        )
        for latency_name, latency in optional_latencies:
            if latency is not None and (
                not math.isfinite(latency) or latency < 0.0
            ):
                raise ValidationError(
                    f"{latency_name} must be finite and nonnegative"
                )


@dataclass(frozen=True, slots=True)
class TreatmentEvent:
    """One admission/runtime output for one policy and frozen event."""

    policy_id: str
    event_id: str
    treatment_manifest_hash: str
    admitted_pairs: tuple[PairKey, ...]
    claim_post_statuses: tuple[ObjectStatus, ...]
    answer_post_statuses: tuple[ObjectStatus, ...]
    work: VerifierMeasurement
    failure_code: str | None = None

    def __post_init__(self) -> None:
        _require_text("policy_id", self.policy_id)
        _require_text("event_id", self.event_id)
        _require_hash("treatment_manifest_hash", self.treatment_manifest_hash)
        if self.admitted_pairs != tuple(sorted(set(self.admitted_pairs))):
            raise ValidationError("admitted_pairs must be sorted and unique")
        if self.work.attempted_pair_count != len(self.admitted_pairs):
            raise ValidationError("verifier attempts differ from admitted pairs")
        for name, values in (
            ("claim_post_statuses", self.claim_post_statuses),
            ("answer_post_statuses", self.answer_post_statuses),
        ):
            if values != tuple(sorted(values)):
                raise ValidationError(f"{name} must use canonical order")
            if len({item.object_id for item in values}) != len(values):
                raise ValidationError(f"{name} contains duplicate object IDs")
        if self.failure_code is not None:
            _require_text("failure_code", self.failure_code)


@dataclass(frozen=True, slots=True)
class EmpiricalStudySpec:
    schema_version: str
    study_id: str
    dataset_version: str
    selected_split_id: str
    measurement_source: str
    verifier_identity: str
    decision_policy_id: str
    histories: tuple[HistoryAssignment, ...]
    policies: tuple[PolicySpec, ...]
    oracle_events: tuple[FrozenOracleEvent, ...]
    treatments: tuple[TreatmentEvent, ...]
    bootstrap_seed: int
    bootstrap_replicates: int
    confidence_level: float = 0.95

    def __post_init__(self) -> None:
        if self.schema_version != "m4-empirical-study-v1":
            raise ValidationError("unsupported empirical study schema")
        for name, value in (
            ("study_id", self.study_id),
            ("dataset_version", self.dataset_version),
            ("selected_split_id", self.selected_split_id),
            ("measurement_source", self.measurement_source),
            ("verifier_identity", self.verifier_identity),
            ("decision_policy_id", self.decision_policy_id),
        ):
            _require_text(name, value)
        if type(self.bootstrap_seed) is not int or self.bootstrap_seed < 0:
            raise ValidationError("bootstrap_seed must be nonnegative")
        if type(self.bootstrap_replicates) is not int or self.bootstrap_replicates <= 0:
            raise ValidationError("bootstrap_replicates must be positive")
        if type(self.confidence_level) is not float or not (
            0.0 < self.confidence_level < 1.0
        ):
            raise ValidationError("confidence_level must lie in (0, 1)")
        self._validate_histories()
        self._validate_policies_and_events()

    def _validate_histories(self) -> None:
        if not self.histories or self.histories != tuple(
            sorted(self.histories, key=lambda item: item.history_id)
        ):
            raise ValidationError("histories must be non-empty and canonical")
        if len({item.history_id for item in self.histories}) != len(self.histories):
            raise ValidationError("history IDs must be unique")
        component_split: dict[str, str] = {}
        for history in self.histories:
            for component_id in history.component_ids:
                prior = component_split.setdefault(component_id, history.split_id)
                if prior != history.split_id:
                    raise ValidationError(
                        f"history component {component_id} leaks across splits"
                    )
        if not any(
            history.split_id == self.selected_split_id for history in self.histories
        ):
            raise ValidationError("selected split contains no histories")

    def _validate_policies_and_events(self) -> None:
        canonical_policies = tuple(
            sorted(self.policies, key=lambda item: item.policy_id)
        )
        if self.policies != canonical_policies:
            raise ValidationError("policies must use canonical policy-ID order")
        if len({item.policy_id for item in self.policies}) != len(self.policies):
            raise ValidationError("policy IDs must be unique")
        if {item.kind for item in self.policies} != set(AblationKind):
            raise ValidationError("study requires all seven M4.9 ablations")
        if len({item.kind for item in self.policies}) != len(self.policies):
            raise ValidationError("each M4.9 ablation must appear exactly once")

        selected_histories = {
            item.history_id
            for item in self.histories
            if item.split_id == self.selected_split_id
        }
        if self.oracle_events != tuple(
            sorted(
                self.oracle_events,
                key=lambda item: (item.history_id, item.event_index, item.event_id),
            )
        ):
            raise ValidationError("oracle_events must use canonical history order")
        if any(
            item.history_id not in selected_histories
            for item in self.oracle_events
        ):
            raise ValidationError("oracle event lies outside the selected split")
        event_ids = tuple(item.event_id for item in self.oracle_events)
        if not event_ids or len(set(event_ids)) != len(event_ids):
            raise ValidationError("oracle event IDs must be non-empty and unique")
        indexes: defaultdict[str, list[int]] = defaultdict(list)
        for event in self.oracle_events:
            indexes[event.history_id].append(event.event_index)
        if any(values != list(range(len(values))) for values in indexes.values()):
            raise ValidationError("event indexes must be contiguous per history")

        policy_ids = {item.policy_id for item in self.policies}
        grouped: defaultdict[str, list[TreatmentEvent]] = defaultdict(list)
        for treatment in self.treatments:
            if treatment.policy_id not in policy_ids:
                raise ValidationError("treatment references an unknown policy")
            grouped[treatment.policy_id].append(treatment)
        expected = set(event_ids)
        if set(grouped) != policy_ids:
            raise ValidationError("every policy requires treatment outputs")
        for policy_id, rows in grouped.items():
            actual = [row.event_id for row in rows]
            if len(actual) != len(set(actual)) or set(actual) != expected:
                raise ValidationError(
                    f"policy {policy_id} is not aligned on identical event IDs"
                )

        exhaustive_id = next(
            item.policy_id
            for item in self.policies
            if item.kind is AblationKind.EXHAUSTIVE_REFRESH
        )
        oracle_by_id = {item.event_id: item for item in self.oracle_events}
        for row in grouped[exhaustive_id]:
            if row.admitted_pairs != oracle_by_id[row.event_id].snapshot_refresh_pairs:
                raise ValidationError(
                    "exhaustive-refresh treatment differs from SnapshotRefresh_k"
                )

    @property
    def manifest_hash(self) -> str:
        return stable_m4_digest(
            "m4-empirical-study-v1", _canonical_json(_spec_payload(self))
        )


@dataclass(frozen=True, slots=True)
class MetricCount:
    metric: EmpiricalMetric
    numerator: int
    denominator: int

    @property
    def value(self) -> float | None:
        return None if self.denominator == 0 else self.numerator / self.denominator


@dataclass(frozen=True, slots=True)
class EventEvaluation:
    policy_id: str
    ablation: AblationKind
    history_id: str
    history_cluster_id: str
    event_id: str
    event_index: int
    event_type: str
    metrics: tuple[MetricCount, ...]
    missed_positive_pairs: tuple[PairKey, ...]
    missed_claim_effect_ids: tuple[str, ...]
    missed_answer_effect_ids: tuple[str, ...]
    work: VerifierMeasurement
    failure_code: str | None


@dataclass(frozen=True, slots=True)
class MetricSummary:
    policy_id: str
    metric: EmpiricalMetric
    event_count: int
    eligible_event_count: int
    pooled_numerator: int
    pooled_denominator: int
    micro_value: float | None
    macro_event_value: float | None
    eligible_history_cluster_count: int
    macro_history_value: float | None
    confidence_lower: float | None
    confidence_upper: float | None


@dataclass(frozen=True, slots=True)
class WorkSummary:
    policy_id: str
    event_count: int
    verifier_pairs: int
    verifier_calls: int
    completed_pairs: int
    failed_pairs: int
    timeout_pairs: int
    failed_event_count: int
    input_tokens: int | None
    input_token_observation_count: int
    output_tokens: int | None
    output_token_observation_count: int
    retrieval_latency_ms: float | None
    retrieval_latency_observation_count: int
    verifier_latency_ms: float | None
    verifier_latency_observation_count: int
    end_to_end_latency_ms: float | None
    end_to_end_latency_observation_count: int


@dataclass(frozen=True, slots=True)
class EmpiricalStudyReport:
    spec: EmpiricalStudySpec
    events: tuple[EventEvaluation, ...]
    metric_summaries: tuple[MetricSummary, ...]
    work_summaries: tuple[WorkSummary, ...]

    @property
    def report_hash(self) -> str:
        return stable_m4_digest(
            "m4-empirical-study-report-v1", _canonical_json(_report_payload(self))
        )

    def to_canonical_json(self) -> str:
        payload = _report_payload(self)
        payload["report_hash"] = self.report_hash
        return _canonical_json(payload) + "\n"

    def to_event_csv(self) -> str:
        output = io.StringIO(newline="")
        fields = (
            "study_manifest_hash",
            "report_hash",
            "policy_id",
            "ablation",
            "history_id",
            "history_cluster_id",
            "event_id",
            "event_index",
            "event_type",
            "metric",
            "metric_scope",
            "oracle_baseline",
            "numerator",
            "denominator",
            "value",
            "verifier_pairs",
            "verifier_calls",
            "failed_pairs",
            "timeout_pairs",
            "input_tokens",
            "output_tokens",
            "retrieval_latency_ms",
            "verifier_latency_ms",
            "end_to_end_latency_ms",
            "failure_code",
        )
        writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for event in self.events:
            for metric in event.metrics:
                writer.writerow(
                    {
                        "study_manifest_hash": self.spec.manifest_hash,
                        "report_hash": self.report_hash,
                        "policy_id": event.policy_id,
                        "ablation": event.ablation.value,
                        "history_id": event.history_id,
                        "history_cluster_id": event.history_cluster_id,
                        "event_id": event.event_id,
                        "event_index": event.event_index,
                        "event_type": event.event_type,
                        "metric": metric.metric.value,
                        "metric_scope": "empirical_ai_quality",
                        "oracle_baseline": "frozen_exhaustive_event_audit",
                        "numerator": metric.numerator,
                        "denominator": metric.denominator,
                        "value": "" if metric.value is None else metric.value,
                        "verifier_pairs": event.work.attempted_pair_count,
                        "verifier_calls": event.work.call_count,
                        "failed_pairs": event.work.failed_pair_count,
                        "timeout_pairs": event.work.timeout_pair_count,
                        "input_tokens": _optional_csv(event.work.input_tokens),
                        "output_tokens": _optional_csv(event.work.output_tokens),
                        "retrieval_latency_ms": _optional_csv(
                            event.work.retrieval_latency_ms
                        ),
                        "verifier_latency_ms": _optional_csv(
                            event.work.verifier_latency_ms
                        ),
                        "end_to_end_latency_ms": _optional_csv(
                            event.work.end_to_end_latency_ms
                        ),
                        "failure_code": event.failure_code or "",
                    }
                )
        return output.getvalue()

    def to_summary_csv(self) -> str:
        output = io.StringIO(newline="")
        fields = (
            "study_manifest_hash",
            "report_hash",
            "policy_id",
            "metric",
            "metric_scope",
            "oracle_baseline",
            "event_count",
            "eligible_event_count",
            "pooled_numerator",
            "pooled_denominator",
            "micro_value",
            "macro_event_value",
            "eligible_history_cluster_count",
            "macro_history_value",
            "confidence_lower",
            "confidence_upper",
        )
        writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for summary in self.metric_summaries:
            writer.writerow(
                {
                    "study_manifest_hash": self.spec.manifest_hash,
                    "report_hash": self.report_hash,
                    "policy_id": summary.policy_id,
                    "metric": summary.metric.value,
                    "metric_scope": "empirical_ai_quality",
                    "oracle_baseline": "frozen_exhaustive_event_audit",
                    "event_count": summary.event_count,
                    "eligible_event_count": summary.eligible_event_count,
                    "pooled_numerator": summary.pooled_numerator,
                    "pooled_denominator": summary.pooled_denominator,
                    "micro_value": _optional_csv(summary.micro_value),
                    "macro_event_value": _optional_csv(summary.macro_event_value),
                    "eligible_history_cluster_count": (
                        summary.eligible_history_cluster_count
                    ),
                    "macro_history_value": _optional_csv(
                        summary.macro_history_value
                    ),
                    "confidence_lower": _optional_csv(summary.confidence_lower),
                    "confidence_upper": _optional_csv(summary.confidence_upper),
                }
            )
        return output.getvalue()


@dataclass(frozen=True, slots=True)
class WrittenEmpiricalBundle:
    output_directory: Path
    study_manifest_path: Path
    report_path: Path
    event_csv_path: Path
    summary_csv_path: Path
    miss_csv_path: Path
    bundle_manifest_path: Path
    study_manifest_hash: str
    report_hash: str
    bundle_manifest_hash: str


def _optional_csv(value: object | None) -> object:
    return "" if value is None else value


def _pair_payload(pair: PairKey) -> dict[str, str]:
    return {"claim_id": pair.claim_id, "chunk_version_id": pair.chunk_version_id}


def _status_payload(status: ObjectStatus) -> dict[str, str]:
    return {"object_id": status.object_id, "status": status.status}


def _work_payload(work: VerifierMeasurement) -> dict[str, object]:
    return {
        "attempted_pair_count": work.attempted_pair_count,
        "completed_pair_count": work.completed_pair_count,
        "failed_pair_count": work.failed_pair_count,
        "timeout_pair_count": work.timeout_pair_count,
        "call_count": work.call_count,
        "input_tokens": work.input_tokens,
        "output_tokens": work.output_tokens,
        "retrieval_latency_ms": work.retrieval_latency_ms,
        "verifier_latency_ms": work.verifier_latency_ms,
        "end_to_end_latency_ms": work.end_to_end_latency_ms,
    }


def _spec_payload(spec: EmpiricalStudySpec) -> dict[str, object]:
    return {
        "schema_version": spec.schema_version,
        "study_id": spec.study_id,
        "dataset_version": spec.dataset_version,
        "selected_split_id": spec.selected_split_id,
        "measurement_source": spec.measurement_source,
        "verifier_identity": spec.verifier_identity,
        "decision_policy_id": spec.decision_policy_id,
        "scientific_boundaries": {
            "structured_oracle_correctness": (
                "precondition_from_separate_python_sql_runtime_audits"
            ),
            "reported_recall_scope": (
                "empirical_ai_quality_relative_to_frozen_event_audit"
            ),
            "reported_work_scope": "observed_selective_pipeline_work",
            "objective_truth_claim": False,
        },
        "bootstrap": {
            "seed": spec.bootstrap_seed,
            "replicates": spec.bootstrap_replicates,
            "confidence_level": spec.confidence_level,
            "unit": "connected_history_component",
            "method": "deterministic_percentile_v1",
        },
        "histories": [
            {
                "history_id": item.history_id,
                "split_id": item.split_id,
                "component_ids": list(item.component_ids),
            }
            for item in spec.histories
        ],
        "policies": [
            {
                "policy_id": item.policy_id,
                "kind": item.kind.value,
                "policy_hash": item.policy_hash,
            }
            for item in spec.policies
        ],
        "oracle_events": [
            {
                "history_id": item.history_id,
                "event_id": item.event_id,
                "event_index": item.event_index,
                "event_type": item.event_type,
                "event_manifest_hash": item.event_manifest_hash,
                "event_audit_manifest_id": item.event_audit_manifest_id,
                "event_audit_result_hash": item.event_audit_result_hash,
                "snapshot_refresh_manifest_id": item.snapshot_refresh_manifest_id,
                "oracle_projection_hash": item.oracle_projection_hash,
                "positive_pairs": [_pair_payload(pair) for pair in item.positive_pairs],
                "claim_status_effects": [
                    _status_payload(value) for value in item.claim_status_effects
                ],
                "answer_status_effects": [
                    _status_payload(value) for value in item.answer_status_effects
                ],
                "snapshot_refresh_pairs": [
                    _pair_payload(pair) for pair in item.snapshot_refresh_pairs
                ],
            }
            for item in spec.oracle_events
        ],
        "treatments": [
            {
                "policy_id": item.policy_id,
                "event_id": item.event_id,
                "treatment_manifest_hash": item.treatment_manifest_hash,
                "admitted_pairs": [
                    _pair_payload(pair) for pair in item.admitted_pairs
                ],
                "claim_post_statuses": [
                    _status_payload(value) for value in item.claim_post_statuses
                ],
                "answer_post_statuses": [
                    _status_payload(value) for value in item.answer_post_statuses
                ],
                "work": _work_payload(item.work),
                "failure_code": item.failure_code,
            }
            for item in sorted(
                spec.treatments, key=lambda value: (value.policy_id, value.event_id)
            )
        ],
    }


def _metric_payload(metric: MetricCount) -> dict[str, object]:
    return {
        "metric": metric.metric.value,
        "metric_scope": "empirical_ai_quality",
        "oracle_baseline": "frozen_exhaustive_event_audit",
        "numerator": metric.numerator,
        "denominator": metric.denominator,
        "value": metric.value,
    }


def _report_payload(report: EmpiricalStudyReport) -> dict[str, object]:
    return {
        "schema_version": "m4-empirical-study-report-v1",
        "study_manifest_hash": report.spec.manifest_hash,
        "events": [
            {
                "policy_id": item.policy_id,
                "ablation": item.ablation.value,
                "history_id": item.history_id,
                "history_cluster_id": item.history_cluster_id,
                "event_id": item.event_id,
                "event_index": item.event_index,
                "event_type": item.event_type,
                "metrics": [_metric_payload(metric) for metric in item.metrics],
                "missed_positive_pairs": [
                    _pair_payload(pair) for pair in item.missed_positive_pairs
                ],
                "missed_claim_effect_ids": list(item.missed_claim_effect_ids),
                "missed_answer_effect_ids": list(item.missed_answer_effect_ids),
                "work": _work_payload(item.work),
                "failure_code": item.failure_code,
            }
            for item in report.events
        ],
        "metric_summaries": [
            {
                "policy_id": item.policy_id,
                "metric": item.metric.value,
                "metric_scope": "empirical_ai_quality",
                "oracle_baseline": "frozen_exhaustive_event_audit",
                "event_count": item.event_count,
                "eligible_event_count": item.eligible_event_count,
                "pooled_numerator": item.pooled_numerator,
                "pooled_denominator": item.pooled_denominator,
                "micro_value": item.micro_value,
                "macro_event_value": item.macro_event_value,
                "eligible_history_cluster_count": (
                    item.eligible_history_cluster_count
                ),
                "macro_history_value": item.macro_history_value,
                "confidence_lower": item.confidence_lower,
                "confidence_upper": item.confidence_upper,
            }
            for item in report.metric_summaries
        ],
        "work_summaries": [
            {
                field: getattr(item, field)
                for field in item.__dataclass_fields__
            }
            for item in report.work_summaries
        ],
    }


def _history_clusters(
    histories: tuple[HistoryAssignment, ...], selected_split: str
) -> dict[str, str]:
    selected = [item for item in histories if item.split_id == selected_split]
    parent = {item.history_id: item.history_id for item in selected}

    def find(history_id: str) -> str:
        while parent[history_id] != history_id:
            parent[history_id] = parent[parent[history_id]]
            history_id = parent[history_id]
        return history_id

    def union(first: str, second: str) -> None:
        left, right = find(first), find(second)
        if left != right:
            parent[max(left, right)] = min(left, right)

    owners: dict[str, str] = {}
    for history in selected:
        for component in history.component_ids:
            owner = owners.setdefault(component, history.history_id)
            union(owner, history.history_id)
    groups: defaultdict[str, list[str]] = defaultdict(list)
    for history in selected:
        groups[find(history.history_id)].append(history.history_id)
    result: dict[str, str] = {}
    for members in groups.values():
        cluster_id = "history-cluster-" + stable_m4_digest(*sorted(members))[:16]
        for history_id in members:
            result[history_id] = cluster_id
    return result


def _event_evaluation(
    *,
    policy: PolicySpec,
    oracle: FrozenOracleEvent,
    treatment: TreatmentEvent,
    cluster_id: str,
) -> EventEvaluation:
    positive = set(oracle.positive_pairs)
    admitted = set(treatment.admitted_pairs)
    positive_claims = {pair.claim_id for pair in positive}
    admitted_claims = {pair.claim_id for pair in admitted}
    actual_claims = {
        item.object_id: item.status for item in treatment.claim_post_statuses
    }
    actual_answers = {
        item.object_id: item.status for item in treatment.answer_post_statuses
    }
    expected_claims = {
        item.object_id: item.status for item in oracle.claim_status_effects
    }
    expected_answers = {
        item.object_id: item.status for item in oracle.answer_status_effects
    }
    counts = {
        EmpiricalMetric.POSITIVE_PAIR_IMPACT_RECALL: (
            len(positive & admitted),
            len(positive),
        ),
        EmpiricalMetric.POSITIVE_CLAIM_IMPACT_RECALL: (
            len(positive_claims & admitted_claims),
            len(positive_claims),
        ),
        EmpiricalMetric.CLAIM_STATUS_EFFECT_RECALL: (
            sum(
                actual_claims.get(key) == value
                for key, value in expected_claims.items()
            ),
            len(expected_claims),
        ),
        EmpiricalMetric.ANSWER_STATUS_EFFECT_RECALL: (
            sum(
                actual_answers.get(key) == value
                for key, value in expected_answers.items()
            ),
            len(expected_answers),
        ),
    }
    missed_claims = tuple(
        sorted(
            key
            for key, value in expected_claims.items()
            if actual_claims.get(key) != value
        )
    )
    missed_answers = tuple(
        sorted(
            key
            for key, value in expected_answers.items()
            if actual_answers.get(key) != value
        )
    )
    return EventEvaluation(
        policy_id=policy.policy_id,
        ablation=policy.kind,
        history_id=oracle.history_id,
        history_cluster_id=cluster_id,
        event_id=oracle.event_id,
        event_index=oracle.event_index,
        event_type=oracle.event_type,
        metrics=tuple(
            MetricCount(metric, *counts[metric]) for metric in sorted(EmpiricalMetric)
        ),
        missed_positive_pairs=tuple(sorted(positive - admitted)),
        missed_claim_effect_ids=missed_claims,
        missed_answer_effect_ids=missed_answers,
        work=treatment.work,
        failure_code=treatment.failure_code,
    )


def _quantile(values: tuple[float, ...], probability: float) -> float:
    ordered = tuple(sorted(values))
    rank = (len(ordered) - 1) * probability
    lower, upper = math.floor(rank), math.ceil(rank)
    if lower == upper:
        return ordered[lower]
    fraction = rank - lower
    return ordered[lower] + fraction * (ordered[upper] - ordered[lower])


def _summarize_metric(
    *,
    policy_id: str,
    metric: EmpiricalMetric,
    events: tuple[EventEvaluation, ...],
    seed: int,
    replicates: int,
    confidence_level: float,
) -> MetricSummary:
    values = [
        next(item for item in event.metrics if item.metric is metric)
        for event in events
    ]
    eligible = [item.value for item in values if item.value is not None]
    by_cluster: defaultdict[str, list[MetricCount]] = defaultdict(list)
    for event, value in zip(events, values, strict=True):
        by_cluster[event.history_cluster_id].append(value)
    cluster_values: list[tuple[str, float]] = []
    for cluster_id in sorted(by_cluster):
        numerator = sum(item.numerator for item in by_cluster[cluster_id])
        denominator = sum(item.denominator for item in by_cluster[cluster_id])
        if denominator:
            cluster_values.append((cluster_id, numerator / denominator))
    macro_history = (
        None
        if not cluster_values
        else math.fsum(value for _, value in cluster_values) / len(cluster_values)
    )
    lower: float | None = None
    upper: float | None = None
    if len(cluster_values) >= 2:
        samples: list[float] = []
        for replicate in range(replicates):
            drawn: list[float] = []
            for draw in range(len(cluster_values)):
                digest = stable_m4_digest(
                    "m4-empirical-cluster-bootstrap-v1",
                    str(seed),
                    policy_id,
                    metric.value,
                    str(replicate),
                    str(draw),
                )
                selected = int(digest[:16], 16) % len(cluster_values)
                drawn.append(cluster_values[selected][1])
            samples.append(math.fsum(drawn) / len(drawn))
        tail = (1.0 - confidence_level) / 2.0
        lower = _quantile(tuple(samples), tail)
        upper = _quantile(tuple(samples), 1.0 - tail)
    numerator = sum(item.numerator for item in values)
    denominator = sum(item.denominator for item in values)
    return MetricSummary(
        policy_id=policy_id,
        metric=metric,
        event_count=len(events),
        eligible_event_count=len(eligible),
        pooled_numerator=numerator,
        pooled_denominator=denominator,
        micro_value=None if denominator == 0 else numerator / denominator,
        macro_event_value=None if not eligible else math.fsum(eligible) / len(eligible),
        eligible_history_cluster_count=len(cluster_values),
        macro_history_value=macro_history,
        confidence_lower=lower,
        confidence_upper=upper,
    )


def _optional_sum(
    events: tuple[EventEvaluation, ...], attribute: str
) -> tuple[int | float | None, int]:
    values = [getattr(item.work, attribute) for item in events]
    observed = [value for value in values if value is not None]
    return (None if not observed else sum(observed), len(observed))


def _summarize_work(policy_id: str, events: tuple[EventEvaluation, ...]) -> WorkSummary:
    input_tokens, input_count = _optional_sum(events, "input_tokens")
    output_tokens, output_count = _optional_sum(events, "output_tokens")
    retrieval, retrieval_count = _optional_sum(events, "retrieval_latency_ms")
    verifier, verifier_count = _optional_sum(events, "verifier_latency_ms")
    end_to_end, end_to_end_count = _optional_sum(events, "end_to_end_latency_ms")
    return WorkSummary(
        policy_id=policy_id,
        event_count=len(events),
        verifier_pairs=sum(item.work.attempted_pair_count for item in events),
        verifier_calls=sum(item.work.call_count for item in events),
        completed_pairs=sum(item.work.completed_pair_count for item in events),
        failed_pairs=sum(item.work.failed_pair_count for item in events),
        timeout_pairs=sum(item.work.timeout_pair_count for item in events),
        failed_event_count=sum(item.failure_code is not None for item in events),
        input_tokens=None if input_tokens is None else int(input_tokens),
        input_token_observation_count=input_count,
        output_tokens=None if output_tokens is None else int(output_tokens),
        output_token_observation_count=output_count,
        retrieval_latency_ms=None if retrieval is None else float(retrieval),
        retrieval_latency_observation_count=retrieval_count,
        verifier_latency_ms=None if verifier is None else float(verifier),
        verifier_latency_observation_count=verifier_count,
        end_to_end_latency_ms=None if end_to_end is None else float(end_to_end),
        end_to_end_latency_observation_count=end_to_end_count,
    )


def evaluate_frozen_history(spec: EmpiricalStudySpec) -> EmpiricalStudyReport:
    """Evaluate every frozen treatment with identical event denominators."""
    policies = {item.policy_id: item for item in spec.policies}
    oracles = {item.event_id: item for item in spec.oracle_events}
    clusters = _history_clusters(spec.histories, spec.selected_split_id)
    events = tuple(
        sorted(
            (
                _event_evaluation(
                    policy=policies[treatment.policy_id],
                    oracle=oracles[treatment.event_id],
                    treatment=treatment,
                    cluster_id=clusters[oracles[treatment.event_id].history_id],
                )
                for treatment in spec.treatments
            ),
            key=lambda item: (
                item.policy_id,
                item.history_id,
                item.event_index,
                item.event_id,
            ),
        )
    )
    grouped: defaultdict[str, list[EventEvaluation]] = defaultdict(list)
    for event in events:
        grouped[event.policy_id].append(event)
    summaries: list[MetricSummary] = []
    work: list[WorkSummary] = []
    for policy_id in sorted(grouped):
        policy_events = tuple(grouped[policy_id])
        for metric in sorted(EmpiricalMetric):
            summaries.append(
                _summarize_metric(
                    policy_id=policy_id,
                    metric=metric,
                    events=policy_events,
                    seed=spec.bootstrap_seed,
                    replicates=spec.bootstrap_replicates,
                    confidence_level=spec.confidence_level,
                )
            )
        work.append(_summarize_work(policy_id, policy_events))
    return EmpiricalStudyReport(
        spec=spec,
        events=events,
        metric_summaries=tuple(summaries),
        work_summaries=tuple(work),
    )


def _miss_csv(report: EmpiricalStudyReport) -> str:
    output = io.StringIO(newline="")
    fields = (
        "study_manifest_hash",
        "report_hash",
        "policy_id",
        "history_id",
        "event_id",
        "kind",
        "object_id",
        "chunk_version_id",
    )
    writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    for event in report.events:
        common = {
            "study_manifest_hash": report.spec.manifest_hash,
            "report_hash": report.report_hash,
            "policy_id": event.policy_id,
            "history_id": event.history_id,
            "event_id": event.event_id,
        }
        for pair in event.missed_positive_pairs:
            writer.writerow(
                common
                | {
                    "kind": "positive_pair",
                    "object_id": pair.claim_id,
                    "chunk_version_id": pair.chunk_version_id,
                }
            )
        for object_id in event.missed_claim_effect_ids:
            writer.writerow(
                common
                | {"kind": "claim_status_effect", "object_id": object_id}
            )
        for object_id in event.missed_answer_effect_ids:
            writer.writerow(
                common
                | {"kind": "answer_status_effect", "object_id": object_id}
            )
        if event.work.timeout_pair_count:
            writer.writerow(
                common
                | {
                    "kind": "timeout_pairs",
                    "object_id": str(event.work.timeout_pair_count),
                }
            )
    return output.getvalue()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def write_empirical_bundle(
    report: EmpiricalStudyReport, output_directory: str | Path
) -> WrittenEmpiricalBundle:
    """Write deterministic JSON/CSV artifacts and their content hash manifest."""
    output = Path(output_directory)
    output.mkdir(parents=True, exist_ok=True)
    study_payload = _spec_payload(report.spec)
    study_payload["study_manifest_hash"] = report.spec.manifest_hash
    contents = {
        "study_manifest.json": (_canonical_json(study_payload) + "\n").encode(),
        "report.json": report.to_canonical_json().encode(),
        "event_metrics.csv": report.to_event_csv().encode(),
        "summary_metrics.csv": report.to_summary_csv().encode(),
        "misses.csv": _miss_csv(report).encode(),
    }
    hashes = {name: _sha256_bytes(value) for name, value in contents.items()}
    bundle_payload = {
        "schema_version": "m4-empirical-bundle-v1",
        "study_manifest_hash": report.spec.manifest_hash,
        "report_hash": report.report_hash,
        "files": [
            {"path": name, "sha256": hashes[name]} for name in sorted(hashes)
        ],
    }
    bundle_text = _canonical_json(bundle_payload) + "\n"
    for name, content in contents.items():
        (output / name).write_bytes(content)
    bundle_path = output / "bundle_manifest.json"
    bundle_path.write_text(bundle_text, encoding="utf-8")
    return WrittenEmpiricalBundle(
        output_directory=output,
        study_manifest_path=output / "study_manifest.json",
        report_path=output / "report.json",
        event_csv_path=output / "event_metrics.csv",
        summary_csv_path=output / "summary_metrics.csv",
        miss_csv_path=output / "misses.csv",
        bundle_manifest_path=bundle_path,
        study_manifest_hash=report.spec.manifest_hash,
        report_hash=report.report_hash,
        bundle_manifest_hash=_sha256_bytes(bundle_text.encode()),
    )
