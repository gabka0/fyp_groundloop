"""Metric derivation and canonical paired reports for controlled M4 data."""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass

from groundloop.domain import AnswerStatus, ClaimStatus
from groundloop.errors import ValidationError
from groundloop.m4.contracts import PairKey, stable_m4_digest
from groundloop.m4.oracles.evaluation import (
    BootstrapConfig,
    EvaluationProvenance,
    EventMetricRecord,
    MetricCount,
    MetricName,
    PairedBootstrapResult,
    PolicyMetricSummary,
    PolicyRun,
    align_paired_policy_records,
    paired_history_cluster_bootstrap,
    summarize_policy_metric,
)
from groundloop.m4.oracles.workload import (
    ControlledEventSpec,
    ControlledHistorySpec,
    ControlledWorkload,
    WorkloadSplit,
)

_HEX = frozenset("0123456789abcdef")


def _require_text(name: str, value: str) -> None:
    if not value.strip():
        raise ValidationError(f"{name} must be non-empty")


def _require_sha256(name: str, value: str) -> None:
    if len(value) != 64 or any(character not in _HEX for character in value):
        raise ValidationError(f"{name} must be a lowercase SHA-256 digest")


@dataclass(frozen=True, slots=True, order=True)
class ClaimStatusResult:
    claim_id: str
    status: ClaimStatus

    def __post_init__(self) -> None:
        _require_text("claim_id", self.claim_id)
        if not isinstance(self.status, ClaimStatus):
            raise ValidationError("claim status result requires ClaimStatus")


@dataclass(frozen=True, slots=True, order=True)
class AnswerStatusResult:
    answer_version_id: str
    status: AnswerStatus

    def __post_init__(self) -> None:
        _require_text("answer_version_id", self.answer_version_id)
        if not isinstance(self.status, AnswerStatus):
            raise ValidationError("answer status result requires AnswerStatus")


def _validate_sorted_unique_pairs(name: str, pairs: tuple[PairKey, ...]) -> None:
    if pairs != tuple(sorted(set(pairs))):
        raise ValidationError(f"{name} must be sorted and unique")


def _validate_status_results(
    name: str,
    results: tuple[ClaimStatusResult, ...] | tuple[AnswerStatusResult, ...],
) -> None:
    if results != tuple(sorted(results)):
        raise ValidationError(f"{name} must use canonical object-ID order")
    object_ids = tuple(
        result.claim_id
        if isinstance(result, ClaimStatusResult)
        else result.answer_version_id
        for result in results
    )
    if len(object_ids) != len(set(object_ids)):
        raise ValidationError(f"{name} contains duplicate object IDs")


@dataclass(frozen=True, slots=True)
class OracleEventResult:
    event_id: str
    manifest_id: str
    positive_pairs: tuple[PairKey, ...]
    affected_claim_statuses: tuple[ClaimStatusResult, ...]
    affected_answer_statuses: tuple[AnswerStatusResult, ...]

    def __post_init__(self) -> None:
        _require_text("event_id", self.event_id)
        _require_text("manifest_id", self.manifest_id)
        _validate_sorted_unique_pairs("positive_pairs", self.positive_pairs)
        _validate_status_results(
            "affected_claim_statuses", self.affected_claim_statuses
        )
        _validate_status_results(
            "affected_answer_statuses", self.affected_answer_statuses
        )


@dataclass(frozen=True, slots=True)
class TreatmentEventResult:
    event_id: str
    manifest_id: str
    admitted_pairs: tuple[PairKey, ...]
    claim_post_statuses: tuple[ClaimStatusResult, ...]
    answer_post_statuses: tuple[AnswerStatusResult, ...]
    failure_code: str | None = None

    def __post_init__(self) -> None:
        _require_text("event_id", self.event_id)
        _require_text("manifest_id", self.manifest_id)
        _validate_sorted_unique_pairs("admitted_pairs", self.admitted_pairs)
        _validate_status_results("claim_post_statuses", self.claim_post_statuses)
        _validate_status_results("answer_post_statuses", self.answer_post_statuses)
        if self.failure_code is not None:
            _require_text("failure_code", self.failure_code)


def _validate_result_domain(
    *,
    event: ControlledEventSpec,
    oracle: OracleEventResult,
    treatment: TreatmentEventResult,
) -> None:
    if oracle.event_id != event.event_id or treatment.event_id != event.event_id:
        raise ValidationError("result event ID differs from workload event")
    claim_ids = set(event.registered_claim_ids)
    answer_ids = set(event.registered_answer_ids)
    inserted_ids = set(event.inserted_chunk_version_ids)
    for name, pairs in (
        ("oracle positive", oracle.positive_pairs),
        ("treatment admitted", treatment.admitted_pairs),
    ):
        if any(
            pair.claim_id not in claim_ids
            or pair.chunk_version_id not in inserted_ids
            for pair in pairs
        ):
            raise ValidationError(f"{name} pair lies outside workload event domain")
    if not set(event.deliberate_miss_pairs) <= set(oracle.positive_pairs):
        raise ValidationError("deliberate miss probe is not oracle-positive")
    if any(
        result.claim_id not in claim_ids
        for result in (
            *oracle.affected_claim_statuses,
            *treatment.claim_post_statuses,
        )
    ):
        raise ValidationError("claim status result lies outside registered claims")
    if any(
        result.answer_version_id not in answer_ids
        for result in (
            *oracle.affected_answer_statuses,
            *treatment.answer_post_statuses,
        )
    ):
        raise ValidationError("answer status result lies outside registered answers")


def derive_event_metric_record(
    *,
    run_id: str,
    provenance: EvaluationProvenance,
    history: ControlledHistorySpec,
    event: ControlledEventSpec,
    oracle: OracleEventResult,
    treatment: TreatmentEventResult,
) -> EventMetricRecord:
    """Convert frozen oracle/treatment outputs into the four raw metrics."""
    if event not in history.events:
        raise ValidationError("event does not belong to the supplied history")
    if provenance.split_id != history.split.value:
        raise ValidationError("evaluation provenance split differs from history")
    _validate_result_domain(event=event, oracle=oracle, treatment=treatment)

    positive_pairs = set(oracle.positive_pairs)
    admitted_pairs = set(treatment.admitted_pairs)
    positive_claims = {pair.claim_id for pair in positive_pairs}
    admitted_claims = {pair.claim_id for pair in admitted_pairs}
    oracle_claim_statuses = {
        result.claim_id: result.status
        for result in oracle.affected_claim_statuses
    }
    treatment_claim_statuses = {
        result.claim_id: result.status for result in treatment.claim_post_statuses
    }
    oracle_answer_statuses = {
        result.answer_version_id: result.status
        for result in oracle.affected_answer_statuses
    }
    treatment_answer_statuses = {
        result.answer_version_id: result.status
        for result in treatment.answer_post_statuses
    }
    counts = {
        MetricName.POSITIVE_PAIR_RECALL: (
            len(positive_pairs & admitted_pairs),
            len(positive_pairs),
        ),
        MetricName.POSITIVE_CLAIM_RECALL: (
            len(positive_claims & admitted_claims),
            len(positive_claims),
        ),
        MetricName.STATUS_EFFECT_RECALL: (
            sum(
                treatment_claim_statuses.get(claim_id) is expected_status
                for claim_id, expected_status in oracle_claim_statuses.items()
            ),
            len(oracle_claim_statuses),
        ),
        MetricName.ANSWER_EFFECT_RECALL: (
            sum(
                treatment_answer_statuses.get(answer_id) is expected_status
                for answer_id, expected_status in oracle_answer_statuses.items()
            ),
            len(oracle_answer_statuses),
        ),
    }
    metrics = tuple(
        MetricCount(metric_name, *counts[metric_name])
        for metric_name in sorted(MetricName)
    )
    return EventMetricRecord(
        run_id=run_id,
        provenance=provenance,
        history_id=history.history_id,
        event_id=event.event_id,
        event_index=event.event_index,
        event_type=event.update_kind.value,
        corpus_snapshot_before_hash=event.corpus_snapshot_before_hash,
        corpus_snapshot_after_hash=event.corpus_snapshot_after_hash,
        treatment_manifest_id=treatment.manifest_id,
        baseline_manifest_id=oracle.manifest_id,
        metrics=metrics,
        failure_code=treatment.failure_code,
    )


@dataclass(frozen=True, slots=True)
class PairedEventDiagnostic:
    event_id: str
    event_manifest_hash: str
    deliberate_miss_pairs: tuple[PairKey, ...]
    first_missed_positive_pairs: tuple[PairKey, ...]
    second_missed_positive_pairs: tuple[PairKey, ...]

    def __post_init__(self) -> None:
        _require_text("event_id", self.event_id)
        _require_sha256("event_manifest_hash", self.event_manifest_hash)
        for name, pairs in (
            ("deliberate_miss_pairs", self.deliberate_miss_pairs),
            ("first_missed_positive_pairs", self.first_missed_positive_pairs),
            ("second_missed_positive_pairs", self.second_missed_positive_pairs),
        ):
            _validate_sorted_unique_pairs(name, pairs)


@dataclass(frozen=True, slots=True)
class PairedEvaluationReport:
    report_schema_version: str
    workload_schema_version: str
    workload_dataset_version: str
    workload_manifest_hash: str
    workload_seed_manifest_hash: str
    workload_split_manifest_hash: str
    split: WorkloadSplit
    first_run: PolicyRun
    second_run: PolicyRun
    first_summaries: tuple[PolicyMetricSummary, ...]
    second_summaries: tuple[PolicyMetricSummary, ...]
    bootstrap_config: BootstrapConfig
    bootstrap_results: tuple[PairedBootstrapResult, ...]
    event_diagnostics: tuple[PairedEventDiagnostic, ...]

    def __post_init__(self) -> None:
        if self.report_schema_version != "m4-paired-evaluation-report-v1":
            raise ValidationError("unsupported paired report schema")
        if self.workload_schema_version != "m4-controlled-workload-v1":
            raise ValidationError("unsupported report workload schema")
        _require_text("workload_dataset_version", self.workload_dataset_version)
        for name, value in (
            ("workload_manifest_hash", self.workload_manifest_hash),
            ("workload_seed_manifest_hash", self.workload_seed_manifest_hash),
            ("workload_split_manifest_hash", self.workload_split_manifest_hash),
        ):
            _require_sha256(name, value)
        if not isinstance(self.split, WorkloadSplit):
            raise ValidationError("report split must be development or test")
        expected_names = tuple(sorted(MetricName))
        for name, run, summaries in (
            ("first", self.first_run, self.first_summaries),
            ("second", self.second_run, self.second_summaries),
        ):
            provenance = run.provenance
            if (
                provenance.dataset_version != self.workload_dataset_version
                or provenance.split_id != self.split.value
                or provenance.split_manifest_hash
                != self.workload_split_manifest_hash
                or provenance.seed_manifest_hash
                != self.workload_seed_manifest_hash
            ):
                raise ValidationError(f"{name} run provenance conflicts with report")
            if tuple(summary.metric_name for summary in summaries) != expected_names:
                raise ValidationError(f"{name} summaries are incomplete or unordered")
            if any(
                summary.run_id != run.run_id
                or summary.provenance != provenance
                for summary in summaries
            ):
                raise ValidationError(f"{name} summary provenance conflicts with run")
        if tuple(result.metric_name for result in self.bootstrap_results) != (
            expected_names
        ):
            raise ValidationError("bootstrap results are incomplete or unordered")
        if any(
            result.first_provenance != self.first_run.provenance
            or result.second_provenance != self.second_run.provenance
            or result.config_manifest_hash != self.bootstrap_config.manifest_hash
            for result in self.bootstrap_results
        ):
            raise ValidationError("bootstrap provenance conflicts with report")
        diagnostic_ids = tuple(
            diagnostic.event_id for diagnostic in self.event_diagnostics
        )
        expected_event_ids = tuple(
            sorted(record.event_id for record in self.first_run.records)
        )
        if diagnostic_ids != expected_event_ids:
            raise ValidationError("event diagnostics do not match report events")

    @property
    def manifest_hash(self) -> str:
        return stable_m4_digest(
            "m4-paired-report-v1", self._canonical_payload_json()
        )

    def _canonical_payload_json(self) -> str:
        return json.dumps(
            self._machine_payload(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )

    def to_canonical_json(self) -> str:
        payload = self._machine_payload()
        payload["report_manifest_hash"] = self.manifest_hash
        return json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )

    def _machine_payload(self) -> dict[str, object]:
        return {
            "report_schema_version": self.report_schema_version,
            "workload": {
                "schema_version": self.workload_schema_version,
                "dataset_version": self.workload_dataset_version,
                "manifest_hash": self.workload_manifest_hash,
                "seed_manifest_hash": self.workload_seed_manifest_hash,
                "split_manifest_hash": self.workload_split_manifest_hash,
                "selected_split": self.split.value,
            },
            "first_run": _run_payload(self.first_run),
            "second_run": _run_payload(self.second_run),
            "first_summaries": [
                _summary_payload(summary) for summary in self.first_summaries
            ],
            "second_summaries": [
                _summary_payload(summary) for summary in self.second_summaries
            ],
            "bootstrap_config": {
                "config_id": self.bootstrap_config.config_id,
                "manifest_hash": self.bootstrap_config.manifest_hash,
                "seed": self.bootstrap_config.seed,
                "replicate_count": self.bootstrap_config.replicate_count,
                "confidence_level": self.bootstrap_config.confidence_level,
                "minimum_cluster_count": (
                    self.bootstrap_config.minimum_cluster_count
                ),
                "interval_method": self.bootstrap_config.interval_method,
                "estimand": self.bootstrap_config.estimand,
            },
            "bootstrap_results": [
                _bootstrap_payload(result) for result in self.bootstrap_results
            ],
            "event_diagnostics": [
                {
                    "event_id": diagnostic.event_id,
                    "event_manifest_hash": diagnostic.event_manifest_hash,
                    "deliberate_miss_pairs": _pairs_payload(
                        diagnostic.deliberate_miss_pairs
                    ),
                    "first_missed_positive_pairs": _pairs_payload(
                        diagnostic.first_missed_positive_pairs
                    ),
                    "second_missed_positive_pairs": _pairs_payload(
                        diagnostic.second_missed_positive_pairs
                    ),
                }
                for diagnostic in self.event_diagnostics
            ],
        }


def _pairs_payload(pairs: tuple[PairKey, ...]) -> list[dict[str, str]]:
    return [
        {
            "claim_id": pair.claim_id,
            "chunk_version_id": pair.chunk_version_id,
        }
        for pair in pairs
    ]


def _provenance_payload(provenance: EvaluationProvenance) -> dict[str, object]:
    return {
        "schema_version": provenance.schema_version,
        "dataset_version": provenance.dataset_version,
        "split_id": provenance.split_id,
        "split_manifest_hash": provenance.split_manifest_hash,
        "seed_manifest_hash": provenance.seed_manifest_hash,
        "treatment_policy_id": provenance.treatment_policy_id,
        "treatment_policy_hash": provenance.treatment_policy_hash,
        "verifier_model_artifact_id": provenance.verifier_model_artifact_id,
        "verifier_execution_spec_hash": (
            provenance.verifier_execution_spec_hash
        ),
        "decision_policy_id": provenance.decision_policy_id,
        "oracle_kind": provenance.oracle_kind.value,
        "baseline_id": provenance.baseline_id,
        "oracle_policy_id": provenance.oracle_policy_id,
        "oracle_policy_hash": provenance.oracle_policy_hash,
        "manifest_hash": provenance.manifest_hash,
    }


def _metric_payload(metric: MetricCount) -> dict[str, object]:
    return {
        "metric_name": metric.metric_name.value,
        "unit": metric.unit.value,
        "numerator": metric.numerator,
        "denominator": metric.denominator,
        "value": metric.value,
    }


def _run_payload(run: PolicyRun) -> dict[str, object]:
    return {
        "run_id": run.run_id,
        "provenance": _provenance_payload(run.provenance),
        "events": [
            {
                "history_id": record.history_id,
                "event_id": record.event_id,
                "event_index": record.event_index,
                "event_type": record.event_type,
                "corpus_snapshot_before_hash": (
                    record.corpus_snapshot_before_hash
                ),
                "corpus_snapshot_after_hash": record.corpus_snapshot_after_hash,
                "treatment_manifest_id": record.treatment_manifest_id,
                "baseline_manifest_id": record.baseline_manifest_id,
                "failure_code": record.failure_code,
                "metrics": [_metric_payload(metric) for metric in record.metrics],
            }
            for record in run.records
        ],
    }


def _summary_payload(summary: PolicyMetricSummary) -> dict[str, object]:
    return {
        "run_id": summary.run_id,
        "provenance_manifest_hash": summary.provenance.manifest_hash,
        "metric_name": summary.metric_name.value,
        "unit": summary.metric_name.unit.value,
        "total_event_count": summary.total_event_count,
        "eligible_event_count": summary.eligible_event_count,
        "pooled_numerator": summary.pooled_numerator,
        "pooled_denominator": summary.pooled_denominator,
        "micro_value": summary.micro_value,
        "macro_event_value": summary.macro_event_value,
    }


def _bootstrap_payload(result: PairedBootstrapResult) -> dict[str, object]:
    return {
        "metric_name": result.metric_name.value,
        "unit": result.metric_name.unit.value,
        "first_provenance_manifest_hash": result.first_provenance.manifest_hash,
        "second_provenance_manifest_hash": result.second_provenance.manifest_hash,
        "config_manifest_hash": result.config_manifest_hash,
        "total_cluster_ids": list(result.total_cluster_ids),
        "eligible_cluster_ids": list(result.eligible_cluster_ids),
        "observed_difference": result.observed_difference,
        "confidence_lower": result.confidence_lower,
        "confidence_upper": result.confidence_upper,
        "replicate_differences": list(result.replicate_differences),
    }


def _unique_by_event_id(
    name: str,
    results: Iterable[OracleEventResult] | Iterable[TreatmentEventResult],
) -> dict[str, OracleEventResult | TreatmentEventResult]:
    mapped: dict[str, OracleEventResult | TreatmentEventResult] = {}
    for result in results:
        if result.event_id in mapped:
            raise ValidationError(f"{name} contains duplicate event IDs")
        mapped[result.event_id] = result
    return mapped


def _validate_report_provenance(
    *,
    workload: ControlledWorkload,
    split: WorkloadSplit,
    provenance: EvaluationProvenance,
) -> None:
    if provenance.dataset_version != workload.dataset_version:
        raise ValidationError("report provenance dataset differs from workload")
    if provenance.split_id != split.value:
        raise ValidationError("report provenance split differs from selected split")
    if provenance.split_manifest_hash != workload.split_manifest_hash:
        raise ValidationError("report split manifest differs from workload")
    if provenance.seed_manifest_hash != workload.seed_manifest_hash:
        raise ValidationError("report seed manifest differs from workload")


def build_paired_evaluation_report(
    *,
    workload: ControlledWorkload,
    split: WorkloadSplit,
    first_run_id: str,
    second_run_id: str,
    first_provenance: EvaluationProvenance,
    second_provenance: EvaluationProvenance,
    oracle_results: tuple[OracleEventResult, ...],
    first_results: tuple[TreatmentEventResult, ...],
    second_results: tuple[TreatmentEventResult, ...],
    bootstrap_config: BootstrapConfig,
) -> PairedEvaluationReport:
    """Build a complete deterministic report without treatment implementation."""
    _validate_report_provenance(
        workload=workload, split=split, provenance=first_provenance
    )
    _validate_report_provenance(
        workload=workload, split=split, provenance=second_provenance
    )
    histories = workload.histories_for_split(split)
    if not histories:
        raise ValidationError("workload contains no histories for selected split")
    expected_events = {
        event.event_id: (history, event)
        for history in histories
        for event in history.events
    }
    oracle_by_id = _unique_by_event_id("oracle_results", oracle_results)
    first_by_id = _unique_by_event_id("first_results", first_results)
    second_by_id = _unique_by_event_id("second_results", second_results)
    expected_ids = set(expected_events)
    for name, mapped in (
        ("oracle_results", oracle_by_id),
        ("first_results", first_by_id),
        ("second_results", second_by_id),
    ):
        if set(mapped) != expected_ids:
            raise ValidationError(f"{name} does not match selected workload events")

    first_records: list[EventMetricRecord] = []
    second_records: list[EventMetricRecord] = []
    diagnostics: list[PairedEventDiagnostic] = []
    for event_id in sorted(expected_events):
        history, event = expected_events[event_id]
        oracle = oracle_by_id[event_id]
        first = first_by_id[event_id]
        second = second_by_id[event_id]
        if not isinstance(oracle, OracleEventResult):
            raise ValidationError("oracle result map contains a treatment result")
        if not isinstance(first, TreatmentEventResult) or not isinstance(
            second, TreatmentEventResult
        ):
            raise ValidationError("treatment result map contains an oracle result")
        first_records.append(
            derive_event_metric_record(
                run_id=first_run_id,
                provenance=first_provenance,
                history=history,
                event=event,
                oracle=oracle,
                treatment=first,
            )
        )
        second_records.append(
            derive_event_metric_record(
                run_id=second_run_id,
                provenance=second_provenance,
                history=history,
                event=event,
                oracle=oracle,
                treatment=second,
            )
        )
        positive = set(oracle.positive_pairs)
        diagnostics.append(
            PairedEventDiagnostic(
                event_id=event_id,
                event_manifest_hash=event.manifest_hash,
                deliberate_miss_pairs=event.deliberate_miss_pairs,
                first_missed_positive_pairs=tuple(
                    sorted(positive - set(first.admitted_pairs))
                ),
                second_missed_positive_pairs=tuple(
                    sorted(positive - set(second.admitted_pairs))
                ),
            )
        )

    paired = align_paired_policy_records(first_records, second_records)
    first_summaries = tuple(
        summarize_policy_metric(paired.first.records, metric_name)
        for metric_name in sorted(MetricName)
    )
    second_summaries = tuple(
        summarize_policy_metric(paired.second.records, metric_name)
        for metric_name in sorted(MetricName)
    )
    bootstrap_results = tuple(
        paired_history_cluster_bootstrap(
            paired,
            metric_name=metric_name,
            config=bootstrap_config,
        )
        for metric_name in sorted(MetricName)
    )
    return PairedEvaluationReport(
        report_schema_version="m4-paired-evaluation-report-v1",
        workload_schema_version=workload.schema_version,
        workload_dataset_version=workload.dataset_version,
        workload_manifest_hash=workload.manifest_hash,
        workload_seed_manifest_hash=workload.seed_manifest_hash,
        workload_split_manifest_hash=workload.split_manifest_hash,
        split=split,
        first_run=paired.first,
        second_run=paired.second,
        first_summaries=first_summaries,
        second_summaries=second_summaries,
        bootstrap_config=bootstrap_config,
        bootstrap_results=bootstrap_results,
        event_diagnostics=tuple(
            sorted(diagnostics, key=lambda diagnostic: diagnostic.event_id)
        ),
    )
