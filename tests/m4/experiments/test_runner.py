"""Controlled policy runner, work-accounting and miss-detection tests."""

from __future__ import annotations

import ast
from pathlib import Path

from groundloop.m4.experiments import ControlledEvaluationResult, PolicyKind
from groundloop.m4.oracles import MetricName


def _policy(
    result: ControlledEvaluationResult, kind: PolicyKind
):
    return next(
        evaluation
        for evaluation in result.policy_evaluations
        if evaluation.policy.kind is kind
    )


def test_exhaustive_oracle_executes_exact_cartesian_work(
    controlled_result: ControlledEvaluationResult,
) -> None:
    measurements = controlled_result.exhaustive_measurements
    assert len(measurements) == 6
    assert sum(item.expected_cartesian_pair_count for item in measurements) == 8
    assert sum(item.work.attempted_pair_count for item in measurements) == 8
    assert sum(item.work.batch_call_count for item in measurements) == 4
    assert all(item.work.failed_pair_count == 0 for item in measurements)
    deletes = [item for item in measurements if item.event_id.endswith("-delete")]
    assert len(deletes) == 2
    assert all(item.expected_cartesian_pair_count == 0 for item in deletes)
    assert all(item.work.batch_call_count == 0 for item in deletes)


def test_policy_work_is_actual_unique_pairs_not_configured_cap(
    controlled_result: ControlledEvaluationResult,
) -> None:
    expected_attempts = {
        PolicyKind.VECTOR_ONLY: 4,
        PolicyKind.LEXICAL_ONLY: 4,
        PolicyKind.VECTOR_LEXICAL_UNION: 4,
        PolicyKind.UNION_LINEAGE: 4,
        PolicyKind.UNION_LINEAGE_FRONTIER: 8,
    }
    for kind, expected in expected_attempts.items():
        evaluation = _policy(controlled_result, kind)
        attempts = sum(
            item.verifier_work.attempted_pair_count
            for item in evaluation.event_measurements
        )
        assert attempts == expected
        assert all(
            item.verifier_work.attempted_pair_count == len(item.admitted_pairs)
            for item in evaluation.event_measurements
        )
        assert all(
            type(metric.numerator) is int and type(metric.denominator) is int
            for item in evaluation.event_measurements
            for metric in item.metric_record.metrics
        )

    frontier = _policy(controlled_result, PolicyKind.UNION_LINEAGE_FRONTIER)
    assert sum(
        item.frontier_extra_pair_count for item in frontier.event_measurements
    ) == 4
    assert all(
        item.approximate_selected_pair_count
        <= item.inserted_chunk_count
        * item.policy.approximate_budget_per_inserted_chunk
        for item in frontier.event_measurements
    )


def test_deliberate_positive_miss_is_policy_specific_and_inspectable(
    controlled_result: ControlledEvaluationResult,
) -> None:
    insert_records = [
        record
        for record in controlled_result.deliberate_misses
        if record.designated_pairs
    ]
    assert len(insert_records) == 10
    missed_by_kind = {
        evaluation.policy.kind: sum(
            bool(record.missed_designated_pairs)
            for record in insert_records
            if record.policy_id == evaluation.policy.policy_id
        )
        for evaluation in controlled_result.policy_evaluations
    }
    assert missed_by_kind == {
        PolicyKind.LEXICAL_ONLY: 0,
        PolicyKind.VECTOR_LEXICAL_UNION: 2,
        PolicyKind.UNION_LINEAGE_FRONTIER: 0,
        PolicyKind.UNION_LINEAGE: 2,
        PolicyKind.VECTOR_ONLY: 2,
    }
    assert all(
        record.oracle_positive_designated_pairs == record.designated_pairs
        for record in insert_records
    )


def test_recall_uses_exhaustive_denominators_and_delete_is_na(
    controlled_result: ControlledEvaluationResult,
) -> None:
    vector = _policy(controlled_result, PolicyKind.VECTOR_ONLY)
    inserts = [
        item
        for item in vector.event_measurements
        if item.metric_record.event_type == "insert"
    ]
    assert all(
        item.metric_record.metric(MetricName.POSITIVE_PAIR_RECALL).numerator == 0
        for item in inserts
    )
    assert all(
        item.metric_record.metric(MetricName.POSITIVE_PAIR_RECALL).denominator == 1
        for item in inserts
    )
    deletes = [
        item
        for item in vector.event_measurements
        if item.metric_record.event_type == "delete"
    ]
    assert all(
        item.metric_record.metric(MetricName.POSITIVE_PAIR_RECALL).value is None
        for item in deletes
    )


def test_fixed_seed_bootstrap_uses_two_independent_history_clusters(
    controlled_result: ControlledEvaluationResult,
) -> None:
    for evaluation in controlled_result.policy_evaluations:
        for bootstrap in evaluation.versus_exhaustive_bootstrap:
            assert bootstrap.config_manifest_hash == (
                controlled_result.config.bootstrap.manifest_hash
            )
            assert bootstrap.total_cluster_ids == ("test-h1", "test-h2")
            assert bootstrap.eligible_cluster_count == 2
            assert len(bootstrap.replicate_differences) == 10_000


def test_experiment_lane_has_no_sql_model_or_live_pipeline_imports() -> None:
    forbidden = {
        "groundloop.m4.models",
        "groundloop.m4.persistence",
        "groundloop.m4.pipeline",
        "psycopg",
        "sqlalchemy",
    }
    for path in Path("src/groundloop/m4/experiments").glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                imported.add(node.module)
        assert not any(
            module == blocked or module.startswith(blocked + ".")
            for module in imported
            for blocked in forbidden
        )
