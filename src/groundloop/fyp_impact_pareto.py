"""Task 5B Task-4 miss analysis and retrospective selector Pareto report."""

from __future__ import annotations

import csv
import hashlib
import io
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast

from groundloop.domain import AnswerStatus, ClaimStatus
from groundloop.errors import ValidationError
from groundloop.fyp_impact_selection import (
    POLICY_IDS,
    Partition,
    SelectionMetrics,
    evaluate_policy,
    load_impact_selection_config,
    load_revision_cases,
    rank_claims,
    run_impact_selection,
    split_revision_cases,
)
from groundloop.hosted_verifier import (
    BatchLedger,
    HostedRequestManifest,
    HostedVerifierRequest,
    HostedVerifierResult,
    build_request_manifest,
    build_request_population,
    load_hosted_verifier_config,
    write_request_population,
)
from groundloop.m4.contracts import AdmissionChannel, PairKey, stable_m4_digest
from groundloop.m4.experiments import (
    PolicyKind,
    build_frozen_controlled_fixture_v1,
    load_controlled_evaluation_config,
    run_controlled_evaluation,
)
from groundloop.m4.experiments.contracts import RankedChannelCandidate
from groundloop.m4.oracles import MetricName

CONFIG_SCHEMA_VERSION = "groundloop-fyp-impact-pareto-config-v1"
REPORT_SCHEMA_VERSION = "groundloop-fyp-impact-pareto-report-v1"
FRONTIER_SCOPE = "retrospective_after_task5a_selection"
LIMITATIONS = (
    "Task 4 uses a deterministic table verifier and a two-history, six-event "
    "fixture; its miss explanations are exact only for those frozen ranks.",
    "The complete Task 5A held-out frontier is retrospective because budget 8 "
    "was already selected and evaluated before this report.",
    "VitaminC candidate histories are constructed from two revision-sensitive "
    "claims per page and are not a natural deployed GroundLoop population.",
    "Hosted request artifacts are an offline plan, not model accuracy, latency, "
    "cost, status-effect, answer-effect, or speedup evidence.",
)
_LOWER_HEX = frozenset("0123456789abcdef")


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _require_sha256(name: str, value: str) -> None:
    if len(value) != 64 or any(character not in _LOWER_HEX for character in value):
        raise ValidationError(f"{name} must be a lowercase SHA-256 digest")


@dataclass(frozen=True, slots=True)
class ImpactParetoConfig:
    task4_config_path: str
    task5a_config_path: str
    task5a_report_manifest_hash: str
    policies: tuple[str, ...]
    budgets: tuple[int, ...]
    frontier_scope: str
    config_sha256: str

    def __post_init__(self) -> None:
        if not self.task4_config_path or not self.task5a_config_path:
            raise ValidationError("Pareto source config paths must be nonempty")
        _require_sha256(
            "task5a_report_manifest_hash", self.task5a_report_manifest_hash
        )
        _require_sha256("config_sha256", self.config_sha256)
        if self.policies != POLICY_IDS:
            raise ValidationError("Pareto policies differ from Task 5A")
        if self.budgets != (1, 2, 4, 8, 16, 32, 64, 256):
            raise ValidationError("Pareto budgets differ from the frozen sweep")
        if self.frontier_scope != FRONTIER_SCOPE:
            raise ValidationError("Pareto scope must disclose retrospective analysis")


def load_impact_pareto_config(path: str | Path) -> ImpactParetoConfig:
    raw_bytes = Path(path).read_bytes()
    try:
        value = json.loads(raw_bytes)
    except json.JSONDecodeError as error:
        raise ValidationError("impact Pareto config is invalid JSON") from error
    if not isinstance(value, dict):
        raise ValidationError("impact Pareto config must be an object")
    raw = cast(dict[str, Any], value)
    expected = {
        "schema_version",
        "task4_config_path",
        "task5a_config_path",
        "task5a_report_manifest_hash",
        "policies",
        "budgets",
        "frontier_scope",
    }
    if set(raw) != expected or raw["schema_version"] != CONFIG_SCHEMA_VERSION:
        raise ValidationError("impact Pareto config schema or fields drifted")
    if not isinstance(raw["policies"], list) or not isinstance(raw["budgets"], list):
        raise ValidationError("Pareto policies and budgets must be arrays")
    return ImpactParetoConfig(
        task4_config_path=str(raw["task4_config_path"]),
        task5a_config_path=str(raw["task5a_config_path"]),
        task5a_report_manifest_hash=str(raw["task5a_report_manifest_hash"]),
        policies=tuple(str(item) for item in raw["policies"]),
        budgets=tuple(cast(list[int], raw["budgets"])),
        frontier_scope=str(raw["frontier_scope"]),
        config_sha256=_sha256_bytes(raw_bytes),
    )


@dataclass(frozen=True, slots=True)
class Task4MissAnalysis:
    policy_id: str
    policy_kind: str
    history_id: str
    event_id: str
    claim_id: str
    chunk_version_id: str
    approximate_budget: int
    frontier_budget: int
    vector_rank: int | None
    lexical_rank: int | None
    lineage_rank: int | None
    frontier_rank: int | None
    approximate_selected_pairs: int
    lineage_extra_pairs: int
    frontier_extra_pairs: int
    attempted_pairs: int
    batch_calls: int
    pair_recall_numerator: int
    pair_recall_denominator: int
    claim_recall_numerator: int
    claim_recall_denominator: int
    status_recall_numerator: int
    status_recall_denominator: int
    answer_recall_numerator: int
    answer_recall_denominator: int
    failure_code: str
    explanation: str


def _rank(
    *,
    pair: PairKey,
    candidates: tuple[RankedChannelCandidate, ...],
    channel: AdmissionChannel,
) -> int | None:
    matches = [
        item.rank
        for item in candidates
        if item.pair == pair and item.channel is channel
    ]
    if len(matches) > 1:
        raise ValidationError("Task 4 pair has duplicate channel ranks")
    return None if not matches else matches[0]


def _failure(
    *,
    kind: PolicyKind,
    pair: PairKey,
    admitted_pairs: tuple[PairKey, ...],
    vector_rank: int | None,
    lexical_rank: int | None,
    lineage_rank: int | None,
    frontier_rank: int | None,
    budget: int,
) -> tuple[str, str]:
    if pair in admitted_pairs:
        raise ValidationError("Task 4 miss analysis received an admitted pair")
    if kind is PolicyKind.VECTOR_ONLY:
        if vector_rank is None or vector_rank <= budget:
            raise ValidationError("unsupported Task 4 vector miss shape")
        return (
            "channel_rank_outside_budget",
            f"vector rank {vector_rank} is outside budget {budget}; lexical and "
            "frontier ranks are not enabled by vector-only policy",
        )
    if kind is PolicyKind.LEXICAL_ONLY:
        if lexical_rank is None or lexical_rank <= budget:
            raise ValidationError("unsupported Task 4 lexical miss shape")
        return (
            "channel_rank_outside_budget",
            f"lexical rank {lexical_rank} is outside budget {budget}; vector and "
            "lineage ranks are not enabled by lexical-only policy",
        )
    if kind is PolicyKind.VECTOR_LEXICAL_UNION:
        if vector_rank != 2 or lexical_rank != 1 or budget != 1:
            raise ValidationError("unsupported Task 4 union miss shape")
        return (
            "vector_first_union_cap_exhaustion",
            "vector rank 1 admitted the decoy and exhausted the shared L=1 cap "
            "before lexical rank 1 could admit this oracle-positive pair",
        )
    if kind is PolicyKind.UNION_LINEAGE:
        if lineage_rank is not None or frontier_rank != 1:
            raise ValidationError("unsupported Task 4 lineage miss shape")
        return (
            "lineage_points_to_other_claim",
            "the shared L=1 cap admitted the vector decoy, lineage also points to "
            "the other claim, and this pair is recoverable only through frontier",
        )
    raise ValidationError("unsupported Task 4 missed-policy shape")


def build_task4_miss_analysis(
    *, task4_config_path: str | Path
) -> tuple[tuple[Task4MissAnalysis, ...], dict[str, object]]:
    config = load_controlled_evaluation_config(task4_config_path)
    fixture = build_frozen_controlled_fixture_v1()
    result = run_controlled_evaluation(config=config, fixture=fixture)
    event_by_id = {event.event_id: event for event in fixture.events}
    evaluations = {
        item.policy.policy_id: item for item in result.policy_evaluations
    }
    rows: list[Task4MissAnalysis] = []
    for miss in result.deliberate_misses:
        if not miss.all_missed_positive_pairs:
            continue
        evaluation = evaluations[miss.policy_id]
        measurement = next(
            item
            for item in evaluation.event_measurements
            if item.metric_record.event_id == miss.event_id
        )
        event = event_by_id[miss.event_id]
        metrics = {
            item.metric_name: item for item in measurement.metric_record.metrics
        }
        for pair in miss.all_missed_positive_pairs:
            vector_rank = _rank(
                pair=pair,
                candidates=event.candidates,
                channel=AdmissionChannel.VECTOR,
            )
            lexical_rank = _rank(
                pair=pair,
                candidates=event.candidates,
                channel=AdmissionChannel.LEXICAL,
            )
            lineage_rank = _rank(
                pair=pair,
                candidates=event.candidates,
                channel=AdmissionChannel.LINEAGE,
            )
            frontier_rank = _rank(
                pair=pair,
                candidates=event.candidates,
                channel=AdmissionChannel.FRONTIER,
            )
            failure_code, explanation = _failure(
                kind=evaluation.policy.kind,
                pair=pair,
                admitted_pairs=measurement.admitted_pairs,
                vector_rank=vector_rank,
                lexical_rank=lexical_rank,
                lineage_rank=lineage_rank,
                frontier_rank=frontier_rank,
                budget=evaluation.policy.approximate_budget_per_inserted_chunk,
            )
            pair_metric = metrics[MetricName.POSITIVE_PAIR_RECALL]
            claim_metric = metrics[MetricName.POSITIVE_CLAIM_RECALL]
            status_metric = metrics[MetricName.STATUS_EFFECT_RECALL]
            answer_metric = metrics[MetricName.ANSWER_EFFECT_RECALL]
            rows.append(
                Task4MissAnalysis(
                    policy_id=evaluation.policy.policy_id,
                    policy_kind=evaluation.policy.kind.value,
                    history_id=miss.history_id,
                    event_id=miss.event_id,
                    claim_id=pair.claim_id,
                    chunk_version_id=pair.chunk_version_id,
                    approximate_budget=(
                        evaluation.policy.approximate_budget_per_inserted_chunk
                    ),
                    frontier_budget=(
                        evaluation.policy.frontier_budget_per_inserted_chunk
                    ),
                    vector_rank=vector_rank,
                    lexical_rank=lexical_rank,
                    lineage_rank=lineage_rank,
                    frontier_rank=frontier_rank,
                    approximate_selected_pairs=(
                        measurement.approximate_selected_pair_count
                    ),
                    lineage_extra_pairs=measurement.mandatory_lineage_extra_pair_count,
                    frontier_extra_pairs=measurement.frontier_extra_pair_count,
                    attempted_pairs=measurement.verifier_work.attempted_pair_count,
                    batch_calls=measurement.verifier_work.batch_call_count,
                    pair_recall_numerator=pair_metric.numerator,
                    pair_recall_denominator=pair_metric.denominator,
                    claim_recall_numerator=claim_metric.numerator,
                    claim_recall_denominator=claim_metric.denominator,
                    status_recall_numerator=status_metric.numerator,
                    status_recall_denominator=status_metric.denominator,
                    answer_recall_numerator=answer_metric.numerator,
                    answer_recall_denominator=answer_metric.denominator,
                    failure_code=failure_code,
                    explanation=explanation,
                )
            )
    ordered = tuple(
        sorted(rows, key=lambda item: (item.policy_id, item.event_id, item.claim_id))
    )
    expected_miss_count = sum(
        len(item.all_missed_positive_pairs) for item in result.deliberate_misses
    )
    if len(ordered) != expected_miss_count or expected_miss_count != 8:
        raise ValidationError("Task 4 miss population differs from the frozen shape")
    frontier = next(
        item
        for item in result.policy_evaluations
        if item.policy.kind is PolicyKind.UNION_LINEAGE_FRONTIER
    )
    frontier_attempts = sum(
        item.verifier_work.attempted_pair_count
        for item in frontier.event_measurements
    )
    exhaustive_attempts = sum(
        item.work.attempted_pair_count for item in result.exhaustive_measurements
    )
    if frontier_attempts != exhaustive_attempts:
        raise ValidationError("Task 4 frontier no longer equals exhaustive work")
    summary = {
        "fixture_manifest_hash": fixture.manifest_hash,
        "miss_count": len(ordered),
        "frontier_attempted_pairs": frontier_attempts,
        "exhaustive_attempted_pairs": exhaustive_attempts,
        "frontier_recovered_all_positive_pairs": all(
            not item.all_missed_positive_pairs
            for item in result.deliberate_misses
            if item.policy_id == frontier.policy.policy_id
        ),
        "interpretation": (
            "frontier closes the frozen positive-pair misses only while matching "
            "the exhaustive verifier-pair count"
        ),
    }
    return ordered, summary


@dataclass(frozen=True, slots=True)
class ParetoPoint:
    policy_id: str
    budget: int
    event_count: int
    registry_claim_count: int
    exhaustive_pair_count: int
    selected_pair_count: int
    avoided_pair_count: int
    affected_claim_numerator: int
    affected_claim_denominator: int
    full_event_numerator: int
    full_event_denominator: int
    pareto_optimal: bool


def _dominates(first: SelectionMetrics, second: SelectionMetrics) -> bool:
    recall_at_least = (
        first.affected_claim_numerator * second.affected_claim_denominator
        >= second.affected_claim_numerator * first.affected_claim_denominator
    )
    work_at_most = first.selected_pair_count <= second.selected_pair_count
    recall_strict = (
        first.affected_claim_numerator * second.affected_claim_denominator
        > second.affected_claim_numerator * first.affected_claim_denominator
    )
    work_strict = first.selected_pair_count < second.selected_pair_count
    return recall_at_least and work_at_most and (recall_strict or work_strict)


def build_heldout_pareto(
    *,
    task5a_config_path: str | Path,
    source_path: str | Path,
    policies: tuple[str, ...],
    budgets: tuple[int, ...],
) -> tuple[ParetoPoint, ...]:
    config = load_impact_selection_config(task5a_config_path)
    cases = load_revision_cases(source_path, config)
    _, evaluation = split_revision_cases(cases, config)
    metrics = tuple(
        evaluate_policy(
            partition=evaluation,
            policy_id=policy_id,
            budget=budget,
            config=config,
        )[0]
        for policy_id in policies
        for budget in budgets
    )
    return tuple(
        ParetoPoint(
            policy_id=item.policy_id,
            budget=item.budget,
            event_count=item.event_count,
            registry_claim_count=item.registry_claim_count,
            exhaustive_pair_count=item.exhaustive_pair_count,
            selected_pair_count=item.selected_pair_count,
            avoided_pair_count=item.avoided_pair_count,
            affected_claim_numerator=item.affected_claim_numerator,
            affected_claim_denominator=item.affected_claim_denominator,
            full_event_numerator=item.full_event_numerator,
            full_event_denominator=item.full_event_denominator,
            pareto_optimal=not any(
                _dominates(other, item) for other in metrics if other != item
            ),
        )
        for item in metrics
    )


@dataclass(frozen=True, slots=True)
class ImpactParetoReport:
    schema_version: str
    result_scope: str
    config_sha256: str
    task5a_report_manifest_hash: str
    task4_summary: dict[str, object]
    task4_misses: tuple[Task4MissAnalysis, ...]
    frontier_scope: str
    heldout_pareto: tuple[ParetoPoint, ...]
    hosted_request_manifest_hash: str
    hosted_request_summary: dict[str, object]
    limitations: tuple[str, ...]

    @property
    def manifest_hash(self) -> str:
        return stable_m4_digest(REPORT_SCHEMA_VERSION, _canonical_json(asdict(self)))

    def to_canonical_json(self) -> str:
        payload = asdict(self)
        payload["report_manifest_hash"] = self.manifest_hash
        return _canonical_json(payload) + "\n"


@dataclass(frozen=True, slots=True)
class ImpactParetoBundle:
    report_path: Path
    misses_csv_path: Path
    pareto_csv_path: Path
    hosted_paths: tuple[Path, ...]
    report_manifest_hash: str


def _csv(values: tuple[object, ...]) -> str:
    if not values:
        raise ValidationError("CSV output requires records")
    rows = [asdict(cast(Any, item)) for item in values]
    output = io.StringIO(newline="")
    writer = csv.DictWriter(
        output, fieldnames=tuple(rows[0]), lineterminator="\n"
    )
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def run_impact_pareto(
    *,
    config_path: str | Path,
    source_path: str | Path,
    hosted_config_path: str | Path,
) -> tuple[ImpactParetoReport, HostedRequestManifest, tuple[object, ...]]:
    config_path_value = Path(config_path)
    root = config_path_value.resolve().parents[2]
    config = load_impact_pareto_config(config_path_value)
    task4_path = root / config.task4_config_path
    task5a_path = root / config.task5a_config_path
    task5a_report = run_impact_selection(
        config_path=task5a_path, source_path=source_path
    )
    if task5a_report.manifest_hash != config.task5a_report_manifest_hash:
        raise ValidationError("Task 5A report hash differs from the frozen checkpoint")
    misses, task4_summary = build_task4_miss_analysis(
        task4_config_path=task4_path
    )
    pareto = build_heldout_pareto(
        task5a_config_path=task5a_path,
        source_path=source_path,
        policies=config.policies,
        budgets=config.budgets,
    )
    task5a_config = load_impact_selection_config(task5a_path)
    cases = load_revision_cases(source_path, task5a_config)
    _, evaluation = split_revision_cases(cases, task5a_config)
    hosted_config = load_hosted_verifier_config(hosted_config_path)
    requests = build_request_population(config=hosted_config, partition=evaluation)
    hosted_manifest = build_request_manifest(
        config=hosted_config, requests=requests
    )
    report = ImpactParetoReport(
        schema_version=REPORT_SCHEMA_VERSION,
        result_scope="offline_error_pareto_and_hosted_request_freeze",
        config_sha256=config.config_sha256,
        task5a_report_manifest_hash=task5a_report.manifest_hash,
        task4_summary=task4_summary,
        task4_misses=misses,
        frontier_scope=config.frontier_scope,
        heldout_pareto=pareto,
        hosted_request_manifest_hash=hosted_manifest.manifest_hash,
        hosted_request_summary={
            "request_count": hosted_manifest.request_count,
            "old_request_count": hosted_manifest.old_request_count,
            "new_request_count": hosted_manifest.new_request_count,
            "selected_new_request_count": (
                hosted_manifest.selected_new_request_count
            ),
            "estimated_standard_cost_usd": (
                hosted_manifest.estimated_standard_cost_usd
            ),
            "estimated_batch_cost_usd": hosted_manifest.estimated_batch_cost_usd,
            "batch_part_count": len(hosted_manifest.batch_parts),
        },
        limitations=LIMITATIONS,
    )
    return report, hosted_manifest, cast(tuple[object, ...], requests)


def write_impact_pareto_bundle(
    *,
    report: ImpactParetoReport,
    hosted_manifest: HostedRequestManifest,
    requests: tuple[object, ...],
    hosted_config_path: str | Path,
    output_directory: str | Path,
) -> ImpactParetoBundle:
    from groundloop.hosted_verifier import HostedVerifierRequest

    typed_requests = cast(tuple[HostedVerifierRequest, ...], requests)
    output = Path(output_directory)
    output.mkdir(parents=True, exist_ok=True)
    report_path = output / "impact_pareto_report.json"
    misses_path = output / "task4_misses.csv"
    pareto_path = output / "heldout_pareto.csv"
    report_path.write_text(report.to_canonical_json(), encoding="utf-8")
    misses_path.write_text(_csv(report.task4_misses), encoding="utf-8")
    pareto_path.write_text(_csv(report.heldout_pareto), encoding="utf-8")
    hosted_config = load_hosted_verifier_config(hosted_config_path)
    hosted_paths = write_request_population(
        config=hosted_config,
        requests=typed_requests,
        manifest=hosted_manifest,
        output_directory=output / "hosted",
    )
    return ImpactParetoBundle(
        report_path=report_path,
        misses_csv_path=misses_path,
        pareto_csv_path=pareto_path,
        hosted_paths=hosted_paths,
        report_manifest_hash=report.manifest_hash,
    )


def execute_impact_pareto(
    *,
    config_path: str | Path,
    source_path: str | Path,
    hosted_config_path: str | Path,
    output_directory: str | Path,
) -> tuple[ImpactParetoReport, ImpactParetoBundle]:
    report, hosted_manifest, requests = run_impact_pareto(
        config_path=config_path,
        source_path=source_path,
        hosted_config_path=hosted_config_path,
    )
    bundle = write_impact_pareto_bundle(
        report=report,
        hosted_manifest=hosted_manifest,
        requests=requests,
        hosted_config_path=hosted_config_path,
        output_directory=output_directory,
    )
    return report, bundle


def format_impact_pareto_summary(
    report: ImpactParetoReport, *, output_directory: str | Path
) -> str:
    best = max(
        (
            item
            for item in report.heldout_pareto
            if item.policy_id == "old_new_rarity_coverage"
            and item.pareto_optimal
        ),
        key=lambda item: (
            item.affected_claim_numerator / item.affected_claim_denominator,
            -item.selected_pair_count,
        ),
    )
    hosted = report.hosted_request_summary
    return "\n".join(
        (
            "GroundLoop Task 5B offline impact Pareto",
            f"Task 4 oracle-positive misses explained: {len(report.task4_misses)}",
            "Task 4 frontier/exhaustive attempts: "
            f"{report.task4_summary['frontier_attempted_pairs']}/"
            f"{report.task4_summary['exhaustive_attempted_pairs']}",
            f"Retrospective points: {len(report.heldout_pareto)}",
            f"Illustrative high-recall Pareto point: {best.policy_id} budget "
            f"{best.budget}, affected claims {best.affected_claim_numerator}/"
            f"{best.affected_claim_denominator}, pairs "
            f"{best.selected_pair_count}/{best.exhaustive_pair_count}",
            f"Frozen hosted requests: {hosted['request_count']} "
            f"in {hosted['batch_part_count']} parts",
            "Estimated hosted cost: standard USD "
            f"{hosted['estimated_standard_cost_usd']}, Batch USD "
            f"{hosted['estimated_batch_cost_usd']}",
            f"Artifacts: {Path(output_directory)}",
            f"Report hash: {report.manifest_hash}",
            "Boundary: offline retrospective analysis and request freeze only; "
            "no paid inference or speedup claim.",
        )
    )


@dataclass(frozen=True, slots=True)
class GoldAnnotation:
    case_id: str
    stratum: str
    claim_sha256: str
    evidence_sha256: str
    label: str


@dataclass(frozen=True, slots=True)
class HostedAccuracy:
    evaluated_request_count: int
    correct_request_count: int
    old_request_count: int
    old_correct_count: int
    new_request_count: int
    new_correct_count: int

    def __post_init__(self) -> None:
        if self.evaluated_request_count != (
            self.old_request_count + self.new_request_count
        ):
            raise ValidationError("accuracy side counts are inconsistent")
        if self.correct_request_count != (
            self.old_correct_count + self.new_correct_count
        ):
            raise ValidationError("accuracy correct counts are inconsistent")
        if not 0 <= self.correct_request_count <= self.evaluated_request_count:
            raise ValidationError("accuracy count is invalid")


@dataclass(frozen=True, slots=True)
class HostedEffectPoint:
    policy_id: str
    budget: int
    event_count: int
    verifier_pair_count: int
    exhaustive_verifier_pair_count: int
    avoided_verifier_pair_count: int
    pair_effect_numerator: int
    pair_effect_denominator: int
    claim_effect_numerator: int
    claim_effect_denominator: int
    status_effect_numerator: int
    status_effect_denominator: int
    answer_effect_numerator: int
    answer_effect_denominator: int


def load_task5_partitions(
    *, impact_config_path: str | Path, source_path: str | Path
) -> tuple[Partition, Partition]:
    config = load_impact_selection_config(impact_config_path)
    cases = load_revision_cases(source_path, config)
    return split_revision_cases(cases, config)


def load_gold_annotations(
    *, impact_config_path: str | Path, source_path: str | Path
) -> tuple[GoldAnnotation, ...]:
    config = load_impact_selection_config(impact_config_path)
    load_revision_cases(source_path, config)
    annotations: list[GoldAnnotation] = []
    for line_number, line in enumerate(Path(source_path).read_bytes().splitlines(), 1):
        try:
            raw_value = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValidationError(
                f"gold source row {line_number} is invalid JSON"
            ) from error
        if not isinstance(raw_value, dict):
            raise ValidationError("gold source row must be an object")
        raw = cast(dict[str, Any], raw_value)
        fields = ("case_id", "stratum", "claim_sha256", "evidence_sha256", "label")
        if any(not isinstance(raw.get(field), str) for field in fields):
            raise ValidationError("gold annotation fields are invalid")
        label = cast(str, raw["label"])
        if label not in {"support", "refute", "neutral"}:
            raise ValidationError("gold annotation label is invalid")
        annotations.append(
            GoldAnnotation(
                case_id=cast(str, raw["case_id"]),
                stratum=cast(str, raw["stratum"]),
                claim_sha256=cast(str, raw["claim_sha256"]),
                evidence_sha256=cast(str, raw["evidence_sha256"]),
                label=label,
            )
        )
    keys = tuple(
        (item.claim_sha256, item.evidence_sha256) for item in annotations
    )
    if len(keys) != len(set(keys)):
        raise ValidationError("gold annotations contain duplicate pair identities")
    return tuple(annotations)


def evaluate_hosted_accuracy(
    *,
    requests: tuple[HostedVerifierRequest, ...],
    results: tuple[HostedVerifierResult, ...],
    annotations: tuple[GoldAnnotation, ...],
) -> HostedAccuracy:
    request_by_id = {item.request_id: item for item in requests}
    gold = {
        (item.claim_sha256, item.evidence_sha256): item.label
        for item in annotations
    }
    seen: set[str] = set()
    old_count = old_correct = new_count = new_correct = 0
    for result in results:
        if result.request_id in seen or result.request_id not in request_by_id:
            raise ValidationError("accuracy results contain duplicate or unknown IDs")
        seen.add(result.request_id)
        request = request_by_id[result.request_id]
        expected = gold.get(
            (request.claim_sha256, request.current_evidence_sha256)
        )
        if expected is None:
            continue
        correct = int(result.label == expected)
        if request.judged_side == "old":
            old_count += 1
            old_correct += correct
        else:
            new_count += 1
            new_correct += correct
    return HostedAccuracy(
        evaluated_request_count=old_count + new_count,
        correct_request_count=old_correct + new_correct,
        old_request_count=old_count,
        old_correct_count=old_correct,
        new_request_count=new_count,
        new_correct_count=new_correct,
    )


def _claim_status(label: str) -> ClaimStatus:
    return {
        "support": ClaimStatus.SUPPORTED,
        "refute": ClaimStatus.REFUTED,
        "neutral": ClaimStatus.UNSUPPORTED,
    }[label]


def _answer_status(statuses: tuple[ClaimStatus, ...]) -> AnswerStatus:
    if any(item is ClaimStatus.REFUTED for item in statuses):
        return AnswerStatus.CONTRADICTED
    if any(item is ClaimStatus.CONFLICTED for item in statuses):
        return AnswerStatus.CONFLICTED
    if statuses and all(item is ClaimStatus.SUPPORTED for item in statuses):
        return AnswerStatus.VALID
    if any(item is ClaimStatus.SUPPORTED for item in statuses):
        return AnswerStatus.PARTIALLY_SUPPORTED
    return AnswerStatus.UNSUPPORTED


def evaluate_hosted_effect_frontier(
    *,
    partition: Partition,
    requests: tuple[HostedVerifierRequest, ...],
    results: tuple[HostedVerifierResult, ...],
    policy_id: str,
    budgets: tuple[int, ...],
) -> tuple[HostedEffectPoint, ...]:
    if len(results) != len(requests):
        raise ValidationError("effect evaluation requires exhaustive hosted results")
    request_by_key = {
        (item.event_index, item.judged_side, item.claim_sha256): item
        for item in requests
    }
    result_by_id = {item.request_id: item for item in results}
    if len(result_by_id) != len(results):
        raise ValidationError("effect results contain duplicate request IDs")
    if set(result_by_id) != {item.request_id for item in requests}:
        raise ValidationError("effect results do not match the request population")
    answer_groups = {
        case.case_id: tuple(
            sorted(claim.claim_sha256 for claim in case.affected_claims)
        )
        for case in partition.cases
    }
    points: list[HostedEffectPoint] = []
    for budget in budgets:
        pair_num = pair_den = status_num = status_den = 0
        answer_num = answer_den = 0
        for event_index, event in enumerate(partition.cases):
            selected = {
                item.claim_sha256
                for item in rank_claims(
                    policy_id=policy_id,
                    claims=partition.registry,
                    old_evidence=event.old_evidence,
                    new_evidence=event.new_evidence,
                )[: min(budget, len(partition.registry))]
            }
            old_statuses: dict[str, ClaimStatus] = {}
            new_statuses: dict[str, ClaimStatus] = {}
            treatment_statuses: dict[str, ClaimStatus] = {}
            for claim in partition.registry:
                old_request = request_by_key[
                    (event_index, "old", claim.claim_sha256)
                ]
                new_request = request_by_key[
                    (event_index, "new", claim.claim_sha256)
                ]
                old_status = _claim_status(result_by_id[old_request.request_id].label)
                new_status = _claim_status(result_by_id[new_request.request_id].label)
                treatment_status = (
                    new_status
                    if claim.claim_sha256 in selected
                    else ClaimStatus.UNSUPPORTED
                )
                old_statuses[claim.claim_sha256] = old_status
                new_statuses[claim.claim_sha256] = new_status
                treatment_statuses[claim.claim_sha256] = treatment_status
                if old_status is not new_status:
                    pair_den += 1
                    status_den += 1
                    if claim.claim_sha256 in selected:
                        pair_num += 1
                    if treatment_status is new_status:
                        status_num += 1
            for claim_ids in answer_groups.values():
                old_answer = _answer_status(
                    tuple(old_statuses[claim_id] for claim_id in claim_ids)
                )
                new_answer = _answer_status(
                    tuple(new_statuses[claim_id] for claim_id in claim_ids)
                )
                if old_answer is new_answer:
                    continue
                answer_den += 1
                treatment_answer = _answer_status(
                    tuple(treatment_statuses[claim_id] for claim_id in claim_ids)
                )
                answer_num += int(treatment_answer is new_answer)
        verifier_pairs = len(partition.cases) * min(
            budget, len(partition.registry)
        )
        exhaustive = len(partition.cases) * len(partition.registry)
        points.append(
            HostedEffectPoint(
                policy_id=policy_id,
                budget=budget,
                event_count=len(partition.cases),
                verifier_pair_count=verifier_pairs,
                exhaustive_verifier_pair_count=exhaustive,
                avoided_verifier_pair_count=exhaustive - verifier_pairs,
                pair_effect_numerator=pair_num,
                pair_effect_denominator=pair_den,
                claim_effect_numerator=pair_num,
                claim_effect_denominator=pair_den,
                status_effect_numerator=status_num,
                status_effect_denominator=status_den,
                answer_effect_numerator=answer_num,
                answer_effect_denominator=answer_den,
            )
        )
    return tuple(points)


def batch_processing_seconds(ledger: BatchLedger, role: str) -> int:
    jobs = tuple(item for item in ledger.jobs if item.role == role)
    if not jobs or any(
        item.in_progress_at is None or item.completed_at is None for item in jobs
    ):
        raise ValidationError("batch role lacks complete timing coordinates")
    return sum(
        cast(int, item.completed_at) - cast(int, item.in_progress_at)
        for item in jobs
    )
