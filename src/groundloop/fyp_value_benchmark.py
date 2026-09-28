"""Fail-closed FYP value benchmark over the frozen M4 controlled study.

This module derives a presentation-facing comparison from existing controlled
evidence.  It does not change the frozen fixture or claim empirical AI quality.
"""

from __future__ import annotations

import csv
import io
import json
from dataclasses import asdict, dataclass
from enum import StrEnum
from pathlib import Path
from typing import cast

from groundloop.errors import ValidationError
from groundloop.m4.contracts import stable_m4_digest
from groundloop.m4.experiments import (
    ControlledEvaluationReport,
    ControlledEvaluationResult,
    WrittenReportBundle,
    build_frozen_controlled_fixture_v1,
    load_controlled_evaluation_config,
    run_controlled_evaluation,
    write_report_bundle,
)
from groundloop.m4.oracles import MetricName

SCHEMA_VERSION = "groundloop-fyp-value-benchmark-v1"
RESULT_SCOPE = "deterministic_fixture_value_gate_not_empirical_utility_claim"
DECISION_RULE = (
    "GO iff at least one policy uses fewer verifier-pair attempts than the "
    "exhaustive baseline, has zero failed attempts, and preserves all four "
    "frozen pooled recall metrics."
)
LIMITATIONS = (
    "The verifier is a deterministic judgment table, not a live neural model.",
    "Pair attempts are measured; embeddings, answer generation, wall-clock "
    "latency, and monetary cost are not measured.",
    "The fixture has two histories and six events and is not representative "
    "workload evidence.",
    "The frozen metrics do not measure false changes on unaffected objects.",
    "Relational exactness after observations are stored is a separate systems "
    "result; this gate evaluates semantic selection coverage.",
)

_LOWER_HEX = frozenset("0123456789abcdef")


def _require_sha256(name: str, value: str) -> None:
    if len(value) != 64 or any(character not in _LOWER_HEX for character in value):
        raise ValidationError(f"{name} must be a lowercase SHA-256 digest")


def _require_count(name: str, value: int) -> None:
    if type(value) is not int or value < 0:
        raise ValidationError(f"{name} must be a nonnegative integer")


class ValueVerdict(StrEnum):
    GO = "GO"
    NO_GO = "NO_GO"


@dataclass(frozen=True, slots=True)
class MetricValueEvidence:
    metric_name: MetricName
    numerator: int
    denominator: int

    def __post_init__(self) -> None:
        if not isinstance(self.metric_name, MetricName):
            raise ValidationError("value metric must be a frozen M4 metric")
        _require_count("metric numerator", self.numerator)
        _require_count("metric denominator", self.denominator)
        if self.numerator > self.denominator:
            raise ValidationError("metric numerator exceeds denominator")

    @property
    def has_full_recall(self) -> bool:
        return self.denominator > 0 and self.numerator == self.denominator


@dataclass(frozen=True, slots=True)
class PolicyValueEvidence:
    policy_id: str
    policy_kind: str
    policy_manifest_hash: str
    verifier_batch_call_count: int
    verifier_attempted_pair_count: int
    verifier_completed_pair_count: int
    verifier_failed_pair_count: int
    exhaustive_attempted_pair_count: int
    avoided_pair_attempt_count: int
    metrics: tuple[MetricValueEvidence, ...]
    reduces_pair_work: bool
    preserves_all_measured_effects: bool
    qualifies: bool

    def __post_init__(self) -> None:
        if not self.policy_id.strip() or not self.policy_kind.strip():
            raise ValidationError("policy identity must be non-empty")
        _require_sha256("policy_manifest_hash", self.policy_manifest_hash)
        for name, value in (
            ("verifier_batch_call_count", self.verifier_batch_call_count),
            ("verifier_attempted_pair_count", self.verifier_attempted_pair_count),
            ("verifier_completed_pair_count", self.verifier_completed_pair_count),
            ("verifier_failed_pair_count", self.verifier_failed_pair_count),
            ("exhaustive_attempted_pair_count", self.exhaustive_attempted_pair_count),
            ("avoided_pair_attempt_count", self.avoided_pair_attempt_count),
        ):
            _require_count(name, value)
        if self.exhaustive_attempted_pair_count == 0:
            raise ValidationError("value comparison requires nonempty exhaustive work")
        if self.verifier_attempted_pair_count > self.exhaustive_attempted_pair_count:
            raise ValidationError("policy pair work exceeds the exhaustive comparison")
        if self.verifier_completed_pair_count + self.verifier_failed_pair_count != (
            self.verifier_attempted_pair_count
        ):
            raise ValidationError("policy terminal work does not equal attempts")
        if self.verifier_attempted_pair_count == 0 and self.verifier_batch_call_count:
            raise ValidationError("empty policy work cannot contain a batch call")
        if self.verifier_attempted_pair_count > 0 and (
            self.verifier_batch_call_count == 0
            or self.verifier_batch_call_count > self.verifier_attempted_pair_count
        ):
            raise ValidationError("policy batch calls are inconsistent with attempts")
        if self.avoided_pair_attempt_count != (
            self.exhaustive_attempted_pair_count - self.verifier_attempted_pair_count
        ):
            raise ValidationError("avoided pair count is inconsistent")
        expected_metrics = tuple(sorted(MetricName))
        if tuple(metric.metric_name for metric in self.metrics) != expected_metrics:
            raise ValidationError("policy value metrics are incomplete or reordered")
        expected_reduction = (
            self.verifier_attempted_pair_count < self.exhaustive_attempted_pair_count
        )
        expected_preservation = all(metric.has_full_recall for metric in self.metrics)
        expected_qualification = (
            expected_reduction
            and expected_preservation
            and self.verifier_failed_pair_count == 0
        )
        if self.reduces_pair_work is not expected_reduction:
            raise ValidationError("reduces_pair_work is inconsistent")
        if self.preserves_all_measured_effects is not expected_preservation:
            raise ValidationError("effect-preservation flag is inconsistent")
        if self.qualifies is not expected_qualification:
            raise ValidationError("policy qualification is inconsistent")


@dataclass(frozen=True, slots=True)
class FypValueBenchmarkResult:
    schema_version: str
    result_scope: str
    source_report_manifest_hash: str
    config_manifest_hash: str
    fixture_manifest_hash: str
    split: str
    history_count: int
    event_count: int
    exhaustive_batch_call_count: int
    exhaustive_attempted_pair_count: int
    exhaustive_completed_pair_count: int
    exhaustive_failed_pair_count: int
    policies: tuple[PolicyValueEvidence, ...]
    qualifying_policy_ids: tuple[str, ...]
    verdict: ValueVerdict
    decision_rule: str
    limitations: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise ValidationError("unsupported FYP value benchmark schema")
        if self.result_scope != RESULT_SCOPE:
            raise ValidationError("unsupported FYP value benchmark scope")
        for name, digest_value in (
            ("source_report_manifest_hash", self.source_report_manifest_hash),
            ("config_manifest_hash", self.config_manifest_hash),
            ("fixture_manifest_hash", self.fixture_manifest_hash),
        ):
            _require_sha256(name, digest_value)
        if not self.split.strip():
            raise ValidationError("split must be non-empty")
        for name, count_value in (
            ("history_count", self.history_count),
            ("event_count", self.event_count),
            ("exhaustive_batch_call_count", self.exhaustive_batch_call_count),
            (
                "exhaustive_attempted_pair_count",
                self.exhaustive_attempted_pair_count,
            ),
            (
                "exhaustive_completed_pair_count",
                self.exhaustive_completed_pair_count,
            ),
            ("exhaustive_failed_pair_count", self.exhaustive_failed_pair_count),
        ):
            _require_count(name, count_value)
        if self.history_count == 0 or self.event_count == 0:
            raise ValidationError("value benchmark requires histories and events")
        if self.exhaustive_attempted_pair_count == 0:
            raise ValidationError("value benchmark requires exhaustive pair work")
        if (
            self.exhaustive_completed_pair_count + self.exhaustive_failed_pair_count
            != self.exhaustive_attempted_pair_count
        ):
            raise ValidationError("exhaustive terminal work does not equal attempts")
        if self.exhaustive_failed_pair_count:
            raise ValidationError("failed exhaustive work invalidates the value gate")
        if not self.policies:
            raise ValidationError("value benchmark requires policies")
        if self.policies != tuple(
            sorted(self.policies, key=lambda item: item.policy_id)
        ):
            raise ValidationError("value policies must use canonical order")
        policy_ids = tuple(policy.policy_id for policy in self.policies)
        if len(policy_ids) != len(set(policy_ids)):
            raise ValidationError("value policies contain duplicate identities")
        if any(
            policy.exhaustive_attempted_pair_count
            != self.exhaustive_attempted_pair_count
            for policy in self.policies
        ):
            raise ValidationError("policies use different exhaustive comparisons")
        expected_qualifiers = tuple(
            policy.policy_id for policy in self.policies if policy.qualifies
        )
        if self.qualifying_policy_ids != expected_qualifiers:
            raise ValidationError("qualifying policy list is inconsistent")
        expected_verdict = (
            ValueVerdict.GO if expected_qualifiers else ValueVerdict.NO_GO
        )
        if self.verdict is not expected_verdict:
            raise ValidationError("value verdict is inconsistent")
        if self.decision_rule != DECISION_RULE or self.limitations != LIMITATIONS:
            raise ValidationError("value claim boundary differs from the frozen text")

    def _payload(self) -> dict[str, object]:
        return cast(dict[str, object], asdict(self))

    @property
    def manifest_hash(self) -> str:
        return stable_m4_digest(
            SCHEMA_VERSION,
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
        payload["value_report_manifest_hash"] = self.manifest_hash
        return (
            json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            )
            + "\n"
        )

    def to_policy_csv(self) -> str:
        fieldnames = (
            "value_report_manifest_hash",
            "source_report_manifest_hash",
            "verdict",
            "policy_id",
            "policy_kind",
            "verifier_batch_calls",
            "verifier_attempted_pairs",
            "exhaustive_attempted_pairs",
            "avoided_pair_attempts",
            "reduces_pair_work",
            "preserves_all_measured_effects",
            "qualifies",
            "positive_pair_recall_numerator",
            "positive_pair_recall_denominator",
            "positive_claim_recall_numerator",
            "positive_claim_recall_denominator",
            "status_effect_recall_numerator",
            "status_effect_recall_denominator",
            "answer_effect_recall_numerator",
            "answer_effect_recall_denominator",
        )
        output = io.StringIO(newline="")
        writer = csv.DictWriter(output, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        for policy in self.policies:
            metrics = {metric.metric_name: metric for metric in policy.metrics}
            row: dict[str, object] = {
                "value_report_manifest_hash": self.manifest_hash,
                "source_report_manifest_hash": self.source_report_manifest_hash,
                "verdict": self.verdict.value,
                "policy_id": policy.policy_id,
                "policy_kind": policy.policy_kind,
                "verifier_batch_calls": policy.verifier_batch_call_count,
                "verifier_attempted_pairs": policy.verifier_attempted_pair_count,
                "exhaustive_attempted_pairs": policy.exhaustive_attempted_pair_count,
                "avoided_pair_attempts": policy.avoided_pair_attempt_count,
                "reduces_pair_work": str(policy.reduces_pair_work).lower(),
                "preserves_all_measured_effects": str(
                    policy.preserves_all_measured_effects
                ).lower(),
                "qualifies": str(policy.qualifies).lower(),
            }
            for metric_name in MetricName:
                metric = metrics[metric_name]
                row[f"{metric_name.value}_numerator"] = metric.numerator
                row[f"{metric_name.value}_denominator"] = metric.denominator
            writer.writerow(row)
        return output.getvalue()


@dataclass(frozen=True, slots=True)
class FypValueBenchmarkBundle:
    value_json_path: Path
    policy_csv_path: Path
    controlled_report_bundle: WrittenReportBundle
    value_report_manifest_hash: str


def build_fyp_value_benchmark(
    result: ControlledEvaluationResult,
) -> FypValueBenchmarkResult:
    """Derive the strict value gate from an already executed controlled run."""
    source_report = ControlledEvaluationReport(result)
    exhaustive = result.exhaustive_measurements
    exhaustive_batch_calls = sum(item.work.batch_call_count for item in exhaustive)
    exhaustive_attempts = sum(item.work.attempted_pair_count for item in exhaustive)
    exhaustive_completed = sum(item.work.completed_pair_count for item in exhaustive)
    exhaustive_failed = sum(item.work.failed_pair_count for item in exhaustive)
    policies: list[PolicyValueEvidence] = []
    for evaluation in result.policy_evaluations:
        attempts = sum(
            item.verifier_work.attempted_pair_count
            for item in evaluation.event_measurements
        )
        completed = sum(
            item.verifier_work.completed_pair_count
            for item in evaluation.event_measurements
        )
        failed = sum(
            item.verifier_work.failed_pair_count
            for item in evaluation.event_measurements
        )
        batches = sum(
            item.verifier_work.batch_call_count
            for item in evaluation.event_measurements
        )
        metrics = tuple(
            MetricValueEvidence(
                metric_name=summary.metric_name,
                numerator=summary.pooled_numerator,
                denominator=summary.pooled_denominator,
            )
            for summary in evaluation.metric_summaries
        )
        reduces_work = attempts < exhaustive_attempts
        preserves_effects = all(metric.has_full_recall for metric in metrics)
        policies.append(
            PolicyValueEvidence(
                policy_id=evaluation.policy.policy_id,
                policy_kind=evaluation.policy.kind.value,
                policy_manifest_hash=evaluation.policy.manifest_hash,
                verifier_batch_call_count=batches,
                verifier_attempted_pair_count=attempts,
                verifier_completed_pair_count=completed,
                verifier_failed_pair_count=failed,
                exhaustive_attempted_pair_count=exhaustive_attempts,
                avoided_pair_attempt_count=exhaustive_attempts - attempts,
                metrics=metrics,
                reduces_pair_work=reduces_work,
                preserves_all_measured_effects=preserves_effects,
                qualifies=reduces_work and preserves_effects and failed == 0,
            )
        )
    ordered_policies = tuple(sorted(policies, key=lambda item: item.policy_id))
    qualifiers = tuple(
        policy.policy_id for policy in ordered_policies if policy.qualifies
    )
    histories = result.fixture.workload.histories_for_split(result.config.split)
    return FypValueBenchmarkResult(
        schema_version=SCHEMA_VERSION,
        result_scope=RESULT_SCOPE,
        source_report_manifest_hash=source_report.manifest_hash,
        config_manifest_hash=result.config.manifest_hash,
        fixture_manifest_hash=result.fixture.manifest_hash,
        split=result.config.split.value,
        history_count=len(histories),
        event_count=sum(len(history.events) for history in histories),
        exhaustive_batch_call_count=exhaustive_batch_calls,
        exhaustive_attempted_pair_count=exhaustive_attempts,
        exhaustive_completed_pair_count=exhaustive_completed,
        exhaustive_failed_pair_count=exhaustive_failed,
        policies=ordered_policies,
        qualifying_policy_ids=qualifiers,
        verdict=ValueVerdict.GO if qualifiers else ValueVerdict.NO_GO,
        decision_rule=DECISION_RULE,
        limitations=LIMITATIONS,
    )


def run_fyp_value_benchmark(
    config_path: str | Path,
) -> tuple[FypValueBenchmarkResult, ControlledEvaluationReport]:
    """Execute the frozen study and derive its value verdict."""
    config = load_controlled_evaluation_config(config_path)
    fixture = build_frozen_controlled_fixture_v1()
    controlled = run_controlled_evaluation(config=config, fixture=fixture)
    report = ControlledEvaluationReport(controlled)
    value = build_fyp_value_benchmark(controlled)
    if value.source_report_manifest_hash != report.manifest_hash:
        raise ValidationError("value result is not bound to its controlled report")
    return value, report


def write_fyp_value_benchmark_bundle(
    result: FypValueBenchmarkResult,
    source_report: ControlledEvaluationReport,
    output_directory: str | Path,
) -> FypValueBenchmarkBundle:
    """Write the derived value report and its complete source evidence."""
    if result.source_report_manifest_hash != source_report.manifest_hash:
        raise ValidationError("source report does not match the value result")
    output = Path(output_directory)
    output.mkdir(parents=True, exist_ok=True)
    value_json_path = output / "value_benchmark.json"
    policy_csv_path = output / "value_benchmark_policies.csv"
    value_json_path.write_text(result.to_canonical_json(), encoding="utf-8")
    policy_csv_path.write_text(result.to_policy_csv(), encoding="utf-8")
    controlled_bundle = write_report_bundle(source_report, output / "controlled")
    return FypValueBenchmarkBundle(
        value_json_path=value_json_path,
        policy_csv_path=policy_csv_path,
        controlled_report_bundle=controlled_bundle,
        value_report_manifest_hash=result.manifest_hash,
    )


def execute_fyp_value_benchmark(
    *, config_path: str | Path, output_directory: str | Path
) -> tuple[FypValueBenchmarkResult, FypValueBenchmarkBundle]:
    result, source_report = run_fyp_value_benchmark(config_path)
    bundle = write_fyp_value_benchmark_bundle(result, source_report, output_directory)
    return result, bundle


def _fraction(numerator: int, denominator: int) -> str:
    if denominator == 0:
        return "N/A"
    return f"{numerator}/{denominator} ({100 * numerator / denominator:.1f}%)"


def format_fyp_value_benchmark_summary(
    result: FypValueBenchmarkResult,
    *,
    output_directory: str | Path,
) -> str:
    lines = [
        "GroundLoop FYP value benchmark",
        f"Verdict: {result.verdict.value}",
        f"Scope: {result.result_scope}",
        (
            "Exhaustive baseline: "
            f"{result.exhaustive_attempted_pair_count} verifier-pair attempts"
        ),
        "",
        "Policies:",
    ]
    for policy in result.policies:
        metrics = {metric.metric_name: metric for metric in policy.metrics}
        pair = metrics[MetricName.POSITIVE_PAIR_RECALL]
        answer = metrics[MetricName.ANSWER_EFFECT_RECALL]
        lines.append(
            "- "
            f"{policy.policy_id}: attempts "
            f"{policy.verifier_attempted_pair_count}/"
            f"{policy.exhaustive_attempted_pair_count}, pair recall "
            f"{_fraction(pair.numerator, pair.denominator)}, answer-effect "
            f"recall {_fraction(answer.numerator, answer.denominator)}, "
            f"qualifies={str(policy.qualifies).lower()}"
        )
    if result.verdict is ValueVerdict.NO_GO:
        lines.extend(
            (
                "",
                "Finding: this fixture does not demonstrate selective advantage. "
                "Lower-work policies miss measured effects, while the full-recall "
                "policy performs the same pair work as exhaustive verification.",
            )
        )
    else:
        lines.extend(
            (
                "",
                "Finding: at least one policy passes the bounded controlled value "
                "gate; representative real-model utility remains unproven.",
            )
        )
    lines.extend(
        (
            f"Artifacts: {Path(output_directory)}",
            f"Value report hash: {result.manifest_hash}",
            "Boundary: deterministic fixture only; no real-model accuracy, "
            "latency, cost, or representative utility claim.",
        )
    )
    return "\n".join(lines)
