"""Canonical JSON/CSV serialization for controlled M4.6 measurements."""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass
from pathlib import Path

from groundloop.m4.contracts import PairKey, stable_m4_digest
from groundloop.m4.experiments.contracts import (
    DeliberateMissRecord,
    PolicyEventMeasurement,
)
from groundloop.m4.experiments.runner import ControlledEvaluationResult
from groundloop.m4.oracles import (
    MetricCount,
    PairedBootstrapResult,
    PolicyMetricSummary,
)


def _pairs(pairs: tuple[PairKey, ...]) -> list[dict[str, str]]:
    return [
        {"claim_id": pair.claim_id, "chunk_version_id": pair.chunk_version_id}
        for pair in pairs
    ]


def _metric(metric: MetricCount) -> dict[str, object]:
    return {
        "metric_name": metric.metric_name.value,
        "unit": metric.unit.value,
        "numerator": metric.numerator,
        "denominator": metric.denominator,
        "value": metric.value,
    }


def _summary(summary: PolicyMetricSummary) -> dict[str, object]:
    return {
        "metric_name": summary.metric_name.value,
        "unit": summary.metric_name.unit.value,
        "total_event_count": summary.total_event_count,
        "eligible_event_count": summary.eligible_event_count,
        "pooled_numerator": summary.pooled_numerator,
        "pooled_denominator": summary.pooled_denominator,
        "micro_value": summary.micro_value,
        "macro_event_value": summary.macro_event_value,
    }


def _bootstrap(result: PairedBootstrapResult) -> dict[str, object]:
    return {
        "metric_name": result.metric_name.value,
        "unit": result.metric_name.unit.value,
        "first_policy_manifest_hash": result.first_provenance.manifest_hash,
        "exhaustive_policy_manifest_hash": result.second_provenance.manifest_hash,
        "config_manifest_hash": result.config_manifest_hash,
        "total_history_component_ids": list(result.total_cluster_ids),
        "eligible_history_component_ids": list(result.eligible_cluster_ids),
        "observed_policy_minus_exhaustive": result.observed_difference,
        "confidence_lower": result.confidence_lower,
        "confidence_upper": result.confidence_upper,
        "replicate_differences": list(result.replicate_differences),
    }


def _event_measurement(item: PolicyEventMeasurement) -> dict[str, object]:
    record = item.metric_record
    return {
        "history_id": record.history_id,
        "event_id": record.event_id,
        "event_index": record.event_index,
        "event_type": record.event_type,
        "corpus_snapshot_before_hash": record.corpus_snapshot_before_hash,
        "corpus_snapshot_after_hash": record.corpus_snapshot_after_hash,
        "baseline_manifest_id": record.baseline_manifest_id,
        "treatment_manifest_hash": item.treatment_manifest_hash,
        "failure_code": record.failure_code,
        "inserted_chunk_count": item.inserted_chunk_count,
        "approximate_selected_pair_count": item.approximate_selected_pair_count,
        "mandatory_lineage_extra_pair_count": (
            item.mandatory_lineage_extra_pair_count
        ),
        "frontier_extra_pair_count": item.frontier_extra_pair_count,
        "admitted_pairs": _pairs(item.admitted_pairs),
        "channel_candidate_counts": {
            count.channel.value: count.candidate_count
            for count in item.channel_counts
        },
        "verifier_work": {
            "batch_call_count": item.verifier_work.batch_call_count,
            "attempted_pair_count": item.verifier_work.attempted_pair_count,
            "completed_pair_count": item.verifier_work.completed_pair_count,
            "failed_pair_count": item.verifier_work.failed_pair_count,
        },
        "metrics": [_metric(metric) for metric in record.metrics],
    }


def _miss(item: DeliberateMissRecord) -> dict[str, object]:
    return {
        "policy_id": item.policy_id,
        "history_id": item.history_id,
        "event_id": item.event_id,
        "designated_pairs": _pairs(item.designated_pairs),
        "oracle_positive_designated_pairs": _pairs(
            item.oracle_positive_designated_pairs
        ),
        "missed_designated_pairs": _pairs(item.missed_designated_pairs),
        "all_missed_positive_pairs": _pairs(item.all_missed_positive_pairs),
    }


@dataclass(frozen=True, slots=True)
class ControlledEvaluationReport:
    """A report over a deterministic fixture, never an empirical quality claim."""

    result: ControlledEvaluationResult

    def _payload(self) -> dict[str, object]:
        config = self.result.config
        fixture = self.result.fixture
        selected_histories = fixture.workload.histories_for_split(config.split)
        return {
            "report_schema_version": "m4-controlled-evaluation-report-v1",
            "result_scope": "deterministic_fixture_measurement_not_empirical_claim",
            "config": {
                "config_id": config.config_id,
                "manifest_hash": config.manifest_hash,
                "split": config.split.value,
                "verifier_model_artifact_id": config.verifier_model_artifact_id,
                "verifier_execution_spec_hash": (
                    config.verifier_execution_spec_hash
                ),
                "decision_policy_id": config.decision_policy_id,
                "oracle_policy_id": config.oracle_policy_id,
                "oracle_policy_hash": config.oracle_policy_hash,
                "bootstrap": {
                    "config_id": config.bootstrap.config_id,
                    "manifest_hash": config.bootstrap.manifest_hash,
                    "seed": config.bootstrap.seed,
                    "replicate_count": config.bootstrap.replicate_count,
                    "confidence_level": config.bootstrap.confidence_level,
                    "minimum_cluster_count": config.bootstrap.minimum_cluster_count,
                    "interval_method": config.bootstrap.interval_method,
                    "estimand": config.bootstrap.estimand,
                },
            },
            "fixture": {
                "schema_version": fixture.schema_version,
                "manifest_hash": fixture.manifest_hash,
                "workload_manifest_hash": fixture.workload.manifest_hash,
                "workload_seed_manifest_hash": fixture.workload.seed_manifest_hash,
                "workload_split_manifest_hash": fixture.workload.split_manifest_hash,
                "history_components": [
                    {
                        "history_id": history.history_id,
                        "split_component_id": history.split_component_id,
                        "lineage_component_ids": list(
                            history.lineage_component_ids
                        ),
                    }
                    for history in selected_histories
                ],
            },
            "exhaustive_event_measurements": [
                {
                    "history_id": item.history_id,
                    "event_id": item.event_id,
                    "audit_manifest_id": item.audit_manifest_id,
                    "expected_cartesian_pair_count": (
                        item.expected_cartesian_pair_count
                    ),
                    "verifier_work": {
                        "batch_call_count": item.work.batch_call_count,
                        "attempted_pair_count": item.work.attempted_pair_count,
                        "completed_pair_count": item.work.completed_pair_count,
                        "failed_pair_count": item.work.failed_pair_count,
                    },
                }
                for item in self.result.exhaustive_measurements
            ],
            "policies": [
                {
                    "policy": {
                        "policy_id": evaluation.policy.policy_id,
                        "kind": evaluation.policy.kind.value,
                        "manifest_hash": evaluation.policy.manifest_hash,
                        "approximate_budget_per_inserted_chunk": (
                            evaluation.policy.approximate_budget_per_inserted_chunk
                        ),
                        "frontier_budget_per_inserted_chunk": (
                            evaluation.policy.frontier_budget_per_inserted_chunk
                        ),
                    },
                    "event_measurements": [
                        _event_measurement(item)
                        for item in evaluation.event_measurements
                    ],
                    "metric_summaries": [
                        _summary(summary)
                        for summary in evaluation.metric_summaries
                    ],
                    "policy_minus_exhaustive_history_bootstrap": [
                        _bootstrap(result)
                        for result in evaluation.versus_exhaustive_bootstrap
                    ],
                }
                for evaluation in self.result.policy_evaluations
            ],
            "deliberate_miss_records": [
                _miss(item) for item in self.result.deliberate_misses
            ],
        }

    @property
    def manifest_hash(self) -> str:
        return stable_m4_digest(
            "m4-controlled-evaluation-report-v1",
            json.dumps(
                self._payload(),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ),
        )

    def to_canonical_json(self) -> str:
        payload = self._payload()
        payload["report_manifest_hash"] = self.manifest_hash
        return json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ) + "\n"

    def to_event_metrics_csv(self) -> str:
        """Return one integer metric row per policy/event/metric."""
        fieldnames = (
            "report_manifest_hash",
            "config_manifest_hash",
            "fixture_manifest_hash",
            "policy_id",
            "policy_kind",
            "policy_manifest_hash",
            "approximate_budget_per_inserted_chunk",
            "frontier_budget_per_inserted_chunk",
            "history_id",
            "event_id",
            "event_index",
            "event_type",
            "metric_name",
            "metric_unit",
            "numerator",
            "denominator",
            "value",
            "actual_verifier_batch_calls",
            "actual_verifier_attempted_pairs",
            "actual_verifier_completed_pairs",
            "actual_verifier_failed_pairs",
            "approximate_selected_pairs",
            "mandatory_lineage_extra_pairs",
            "frontier_extra_pairs",
            "vector_candidate_count",
            "lexical_candidate_count",
            "lineage_candidate_count",
            "frontier_candidate_count",
        )
        output = io.StringIO(newline="")
        writer = csv.DictWriter(output, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        for evaluation in self.result.policy_evaluations:
            policy = evaluation.policy
            for item in evaluation.event_measurements:
                counts = {
                    count.channel.value: count.candidate_count
                    for count in item.channel_counts
                }
                for metric in item.metric_record.metrics:
                    writer.writerow(
                        {
                            "report_manifest_hash": self.manifest_hash,
                            "config_manifest_hash": self.result.config.manifest_hash,
                            "fixture_manifest_hash": self.result.fixture.manifest_hash,
                            "policy_id": policy.policy_id,
                            "policy_kind": policy.kind.value,
                            "policy_manifest_hash": policy.manifest_hash,
                            "approximate_budget_per_inserted_chunk": (
                                policy.approximate_budget_per_inserted_chunk
                            ),
                            "frontier_budget_per_inserted_chunk": (
                                policy.frontier_budget_per_inserted_chunk
                            ),
                            "history_id": item.metric_record.history_id,
                            "event_id": item.metric_record.event_id,
                            "event_index": item.metric_record.event_index,
                            "event_type": item.metric_record.event_type,
                            "metric_name": metric.metric_name.value,
                            "metric_unit": metric.unit.value,
                            "numerator": metric.numerator,
                            "denominator": metric.denominator,
                            "value": "" if metric.value is None else metric.value,
                            "actual_verifier_batch_calls": (
                                item.verifier_work.batch_call_count
                            ),
                            "actual_verifier_attempted_pairs": (
                                item.verifier_work.attempted_pair_count
                            ),
                            "actual_verifier_completed_pairs": (
                                item.verifier_work.completed_pair_count
                            ),
                            "actual_verifier_failed_pairs": (
                                item.verifier_work.failed_pair_count
                            ),
                            "approximate_selected_pairs": (
                                item.approximate_selected_pair_count
                            ),
                            "mandatory_lineage_extra_pairs": (
                                item.mandatory_lineage_extra_pair_count
                            ),
                            "frontier_extra_pairs": item.frontier_extra_pair_count,
                            "vector_candidate_count": counts.get("vector", 0),
                            "lexical_candidate_count": counts.get("lexical", 0),
                            "lineage_candidate_count": counts.get("lineage", 0),
                            "frontier_candidate_count": counts.get("frontier", 0),
                        }
                    )
        return output.getvalue()

    def to_bootstrap_csv(self) -> str:
        """Return compact interval rows; raw replicates remain in canonical JSON."""
        fieldnames = (
            "report_manifest_hash",
            "policy_id",
            "metric_name",
            "metric_unit",
            "bootstrap_seed",
            "replicate_count",
            "total_history_component_count",
            "eligible_history_component_count",
            "observed_policy_minus_exhaustive",
            "confidence_lower",
            "confidence_upper",
        )
        output = io.StringIO(newline="")
        writer = csv.DictWriter(output, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        for evaluation in self.result.policy_evaluations:
            for result in evaluation.versus_exhaustive_bootstrap:
                writer.writerow(
                    {
                        "report_manifest_hash": self.manifest_hash,
                        "policy_id": evaluation.policy.policy_id,
                        "metric_name": result.metric_name.value,
                        "metric_unit": result.metric_name.unit.value,
                        "bootstrap_seed": self.result.config.bootstrap.seed,
                        "replicate_count": self.result.config.bootstrap.replicate_count,
                        "total_history_component_count": result.total_cluster_count,
                        "eligible_history_component_count": (
                            result.eligible_cluster_count
                        ),
                        "observed_policy_minus_exhaustive": (
                            ""
                            if result.observed_difference is None
                            else result.observed_difference
                        ),
                        "confidence_lower": (
                            ""
                            if result.confidence_lower is None
                            else result.confidence_lower
                        ),
                        "confidence_upper": (
                            ""
                            if result.confidence_upper is None
                            else result.confidence_upper
                        ),
                    }
                )
        return output.getvalue()


@dataclass(frozen=True, slots=True)
class WrittenReportBundle:
    json_path: Path
    event_metrics_csv_path: Path
    bootstrap_csv_path: Path
    report_manifest_hash: str


def write_report_bundle(
    report: ControlledEvaluationReport, output_directory: str | Path
) -> WrittenReportBundle:
    output = Path(output_directory)
    output.mkdir(parents=True, exist_ok=True)
    json_path = output / "controlled_evaluation_report.json"
    event_path = output / "controlled_event_metrics.csv"
    bootstrap_path = output / "controlled_bootstrap_intervals.csv"
    json_path.write_text(report.to_canonical_json(), encoding="utf-8")
    event_path.write_text(report.to_event_metrics_csv(), encoding="utf-8")
    bootstrap_path.write_text(report.to_bootstrap_csv(), encoding="utf-8")
    return WrittenReportBundle(
        json_path=json_path,
        event_metrics_csv_path=event_path,
        bootstrap_csv_path=bootstrap_path,
        report_manifest_hash=report.manifest_hash,
    )
