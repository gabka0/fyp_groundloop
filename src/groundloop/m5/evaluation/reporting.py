"""Text-free deterministic reports for the M5.5 evaluation lane."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import asdict
from pathlib import Path

from groundloop.m5.digests import (
    bool_field,
    f64_field,
    hash_field,
    int_field,
    sequence_field,
    stable_m5_digest,
    text_field,
)
from groundloop.m5.evaluation.baselines import (
    Availability,
    BaselineId,
    EvaluationRun,
)
from groundloop.m5.evaluation.config import (
    SOURCE_VERIFICATION_STATEMENT,
    ControlledEvaluationConfig,
    evaluation_config_identity_dict,
)
from groundloop.m5.evaluation.histories import ControlledHistory
from groundloop.m5.evaluation.metrics import EvaluationMetricReport
from groundloop.m5.evaluation.records import WiceAdapterResult


def _table_digest(rows: tuple[tuple[str, ...], ...], domain: str) -> str:
    return stable_m5_digest(
        domain,
        sequence_field(
            sequence_field(text_field(value) for value in row) for row in rows
        ),
    )


def _source_table_digest(adapter: WiceAdapterResult) -> str:
    primary_requirement_keys = {
        (item.split, item.subclaim_meta_id) for item in adapter.source_requirements
    }
    primary_annotations = tuple(
        item
        for item in adapter.source_annotations
        if (item.split, item.subclaim_meta_id) in primary_requirement_keys
    )
    return stable_m5_digest(
        "m5-evaluation-source-table-v1",
        sequence_field(
            sequence_field(
                (
                    text_field(item.claim_id),
                    text_field(item.split.value),
                    text_field(item.parent_meta_id),
                    text_field(item.source_label.value),
                    text_field(item.group_version_id),
                )
            )
            for item in adapter.source_claims
        ),
        sequence_field(
            sequence_field(
                (
                    text_field(item.requirement_version_id),
                    text_field(item.group_version_id),
                    text_field(item.split.value),
                    text_field(item.parent_meta_id),
                    text_field(item.subclaim_meta_id),
                    int_field(item.ordinal),
                    hash_field(item.requirement_text_hash),
                    text_field(item.source_label.value),
                )
            )
            for item in adapter.source_requirements
        ),
        sequence_field(
            sequence_field(
                (
                    text_field(item.source_annotation_id),
                    text_field(item.split.value),
                    text_field(item.parent_meta_id),
                    text_field(item.subclaim_meta_id),
                    int_field(item.evidence_set_ordinal),
                    text_field(item.evidence_unit_id),
                    hash_field(item.text_hash),
                    text_field(item.source_label.value),
                )
            )
            for item in primary_annotations
        ),
    )


def _all_valid_annotation_digest(adapter: WiceAdapterResult) -> str:
    return _table_digest(
        tuple(
            (
                item.source_annotation_id,
                item.split.value,
                item.parent_meta_id,
                item.subclaim_meta_id,
                str(item.evidence_set_ordinal),
                item.evidence_unit_id,
                item.text_hash,
                item.source_label.value,
            )
            for item in adapter.source_annotations
        ),
        "m5-evaluation-all-valid-source-annotations-v1",
    )


def _projection_table_digest(adapter: WiceAdapterResult) -> str:
    return _table_digest(
        tuple(
            (
                item.observation_id,
                item.source_annotation_id,
                item.projection_version,
                item.split.value,
                item.manifest_hash,
                item.projected_label,
                item.event_id,
                item.event_payload_hash,
            )
            for item in adapter.controlled_projections
        ),
        "m5-evaluation-controlled-projection-table-v1",
    )


def _exact_table_digest(adapter: WiceAdapterResult) -> str:
    rows: list[tuple[str, ...]] = []
    for item in adapter.exact_system_states:
        row_digest = stable_m5_digest(
            "m5-evaluation-exact-row-v1",
            text_field(item.claim_id),
            text_field(item.group_version_id),
            int_field(item.requirement_count),
            int_field(item.distinct_content_count),
            int_field(item.matching_size),
            bool_field(item.groundloop_sdr_complete),
            int_field(item.perfect_matching_count),
            sequence_field(
                text_field(value) if value is not None else text_field("<unmatched>")
                for value in item.assignment_by_ordinal
            ),
        )
        rows.append((item.claim_id, row_digest))
    return _table_digest(tuple(rows), "m5-evaluation-exact-table-v1")


def _model_table_digest(adapter: WiceAdapterResult) -> str | None:
    if not adapter.frozen_model_diagnostics:
        return None
    return stable_m5_digest(
        "m5-evaluation-frozen-model-table-v1",
        sequence_field(
            sequence_field(
                (
                    text_field(item.claim_id),
                    text_field(item.requirement_version_id),
                    text_field(item.evidence_unit_id),
                    text_field(item.model_id),
                    text_field(item.model_version),
                    text_field(item.prompt_version),
                    hash_field(item.input_hash),
                    f64_field(item.support_score),
                    f64_field(item.refute_score),
                    f64_field(item.neutral_score),
                )
            )
            for item in adapter.frozen_model_diagnostics
        ),
    )


def adapter_audit_dict(
    adapter: WiceAdapterResult,
    *,
    config: ControlledEvaluationConfig,
) -> dict[str, object]:
    """Serialize only aggregate/provenance data, never downloaded row text."""

    audit = asdict(adapter.audit)
    audit["evaluation_config"] = evaluation_config_identity_dict(config)
    audit["source_verification"] = SOURCE_VERIFICATION_STATEMENT
    primary_requirement_keys = {
        (item.split, item.subclaim_meta_id) for item in adapter.source_requirements
    }
    primary_annotation_count = sum(
        (item.split, item.subclaim_meta_id) in primary_requirement_keys
        for item in adapter.source_annotations
    )
    audit["license"] = asdict(adapter.manifest.license)
    audit["record_families"] = {
        "source_human": {
            "schema": "groundloop-m5-source-human-v1",
            "scope": "primary representable cohort",
            "primary_claim_count": len(adapter.source_claims),
            "primary_requirement_count": len(adapter.source_requirements),
            "primary_valid_annotation_count": primary_annotation_count,
            "source_semantic_state_count": len(adapter.source_semantic_states),
            "primary_table_digest": _source_table_digest(adapter),
            "all_mapped_valid_annotation_count": len(adapter.source_annotations),
            "all_mapped_valid_annotation_digest": _all_valid_annotation_digest(adapter),
            "all_annotation_scope": (
                "all mapped source rows after unit validation, including rows "
                "outside the primary cohort"
            ),
        },
        "controlled_projection": {
            "schema": "groundloop-m5-controlled-projection-v1",
            "group_count": len(adapter.controlled_groups),
            "observation_count": len(adapter.controlled_projections),
            "direct_claim_observation_count": 0,
            "table_digest": _projection_table_digest(adapter),
        },
        "exact_system_state": {
            "schema": "groundloop-m5-exact-system-state-v1",
            "state_count": len(adapter.exact_system_states),
            "table_digest": _exact_table_digest(adapter),
        },
        "frozen_model_diagnostic": {
            "schema": "groundloop-m5-frozen-model-diagnostic-v1",
            "state_count": len(adapter.frozen_model_diagnostics),
            "table_digest": _model_table_digest(adapter),
            "used_for_selection": False,
        },
    }
    return audit


def _history_manifest(histories: tuple[ControlledHistory, ...]) -> list[object]:
    rows: list[object] = []
    for history in histories:
        event_digest = stable_m5_digest(
            "m5-evaluation-one-history-events-v1",
            sequence_field(
                sequence_field(
                    (text_field(event.event_id), hash_field(event.event_hash))
                )
                for event in history.events
            ),
        )
        rows.append(
            {
                "history_id": history.history_id,
                "cluster_id": history.cluster_id,
                "cohort_kind": history.cohort_kind.value,
                "event_count": len(history.events),
                "event_digest": event_digest,
                "tags": history.tags,
                "stratum": asdict(history.stratum),
            }
        )
    return rows


def _stratified_raw_counts(
    histories: tuple[ControlledHistory, ...], run: EvaluationRun
) -> list[object]:
    history_by_id = {item.history_id: item for item in histories}
    counts: dict[tuple[str, str, BaselineId], list[int]] = defaultdict(
        lambda: [0, 0, 0, 0]
    )
    for point in run.points:
        if point.availability is Availability.UNAVAILABLE:
            continue
        assert point.y_hat is not None
        stratum = history_by_id[point.history_id].stratum
        dimensions = (
            ("requirement_count", str(stratum.requirement_count)),
            ("maximum_requirement_degree", str(stratum.maximum_requirement_degree)),
            ("content_overlap", str(stratum.has_content_overlap).lower()),
            ("initial_sdr_complete", str(stratum.initial_sdr_complete).lower()),
            (
                "alternative_assignment",
                str(stratum.has_alternative_assignment).lower(),
            ),
            ("provenance", stratum.provenance),
        )
        for dimension, value in dimensions:
            target = counts[(dimension, value, point.baseline_id)]
            target[0] += int(point.source_semantic_label and not point.y_hat)
            target[1] += int(point.source_semantic_label)
            target[2] += int(not point.source_semantic_label and point.y_hat)
            target[3] += int(not point.source_semantic_label)
    return [
        {
            "dimension": dimension,
            "value": value,
            "baseline_id": baseline.value,
            "false_invalidation_numerator": values[0],
            "false_invalidation_denominator": values[1],
            "false_retention_numerator": values[2],
            "false_retention_denominator": values[3],
        }
        for (dimension, value, baseline), values in sorted(
            counts.items(), key=lambda item: (item[0][0], item[0][1], item[0][2].value)
        )
    ]


def evaluation_report_dict(
    *,
    adapter: WiceAdapterResult,
    histories: tuple[ControlledHistory, ...],
    run: EvaluationRun,
    metrics: EvaluationMetricReport,
    config: ControlledEvaluationConfig,
) -> dict[str, object]:
    if run.event_identity_digest != metrics.event_identity_digest:
        raise ValueError("run and metric report use different event histories")
    availability = Counter(
        (point.baseline_id.value, point.availability.value) for point in run.points
    )
    config_identity = evaluation_config_identity_dict(config)
    return {
        "schema": "groundloop-m5-controlled-evaluation-report-v1",
        "evaluation_config": config_identity,
        "result_classification": config_identity["primary_gate_identity"],
        "source_verification": SOURCE_VERIFICATION_STATEMENT,
        "semantic_boundary": {
            "evidence_kind": "controlled/retrospective",
            "fresh_blinded_independent_human_cohort": False,
            "real_world_semantic_validation_claim_permitted": False,
            "m6_human_study_debt": True,
        },
        "adapter_audit": adapter_audit_dict(adapter, config=config),
        "event_protocol": {
            "event_identity_digest": run.event_identity_digest,
            "history_count": len(histories),
            "event_count": sum(len(item.events) for item in histories),
            "histories": _history_manifest(histories),
        },
        "baseline_protocol": {
            "specs": [asdict(item) for item in run.baseline_specs],
            "availability": [
                {
                    "baseline_id": baseline_id,
                    "availability": state,
                    "point_count": count,
                }
                for (baseline_id, state), count in sorted(availability.items())
            ],
            "execution_evaluator": run.evaluator,
            "timing_mode": run.timing_mode,
            "target_runtime_performance_validated": False,
            "model_calls_are_zero_by_construction": True,
        },
        "metrics": asdict(metrics),
        "stratified_raw_counts": _stratified_raw_counts(histories, run),
        "test_selection": {
            "model_selected_on_test": False,
            "policy_selected_on_test": False,
            "threshold_selected_on_test": False,
            "prompt_selected_on_test": False,
            "implementation_selected_on_test": False,
        },
    }


def write_json_report(path: Path, report: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(report, sort_keys=True, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


__all__ = [
    "adapter_audit_dict",
    "evaluation_report_dict",
    "write_json_report",
]
