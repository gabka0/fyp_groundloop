from __future__ import annotations

import csv
import io
import json
from dataclasses import replace
from pathlib import Path

import pytest

from groundloop.errors import ValidationError
from groundloop.fyp_value_benchmark import (
    FypValueBenchmarkResult,
    ValueVerdict,
    format_fyp_value_benchmark_summary,
    write_fyp_value_benchmark_bundle,
)
from groundloop.m4.experiments import ControlledEvaluationReport
from groundloop.m4.oracles import MetricName


def test_frozen_fixture_returns_honest_no_go(
    value_evidence: tuple[FypValueBenchmarkResult, ControlledEvaluationReport],
) -> None:
    result, _ = value_evidence

    assert result.verdict is ValueVerdict.NO_GO
    assert result.qualifying_policy_ids == ()
    assert result.history_count == 2
    assert result.event_count == 6
    assert result.exhaustive_batch_call_count == 4
    assert result.exhaustive_attempted_pair_count == 8

    by_kind = {policy.policy_kind: policy for policy in result.policies}
    frontier = by_kind["union_lineage_frontier"]
    assert frontier.verifier_attempted_pair_count == 8
    assert frontier.preserves_all_measured_effects is True
    assert frontier.reduces_pair_work is False
    assert frontier.qualifies is False

    for kind in (
        "vector_only",
        "lexical_only",
        "vector_lexical_union",
        "union_lineage",
    ):
        policy = by_kind[kind]
        assert policy.verifier_attempted_pair_count == 4
        assert policy.avoided_pair_attempt_count == 4
        assert policy.reduces_pair_work is True
        assert policy.preserves_all_measured_effects is False
        assert policy.qualifies is False
        metrics = {metric.metric_name: metric for metric in policy.metrics}
        assert (
            metrics[MetricName.POSITIVE_PAIR_RECALL].numerator,
            metrics[MetricName.POSITIVE_PAIR_RECALL].denominator,
        ) == (2, 4)
        assert (
            metrics[MetricName.ANSWER_EFFECT_RECALL].numerator,
            metrics[MetricName.ANSWER_EFFECT_RECALL].denominator,
        ) == (4, 6)


def test_reports_are_byte_stable_and_bound_to_source(
    tmp_path: Path,
    value_evidence: tuple[FypValueBenchmarkResult, ControlledEvaluationReport],
) -> None:
    result, source = value_evidence
    assert result.to_canonical_json() == result.to_canonical_json()
    assert result.to_policy_csv() == result.to_policy_csv()
    payload = json.loads(result.to_canonical_json())
    assert payload["value_report_manifest_hash"] == result.manifest_hash
    assert payload["source_report_manifest_hash"] == source.manifest_hash

    rows = list(csv.DictReader(io.StringIO(result.to_policy_csv())))
    assert len(rows) == 5
    assert {row["verdict"] for row in rows} == {"NO_GO"}
    assert {row["qualifies"] for row in rows} == {"false"}

    first = write_fyp_value_benchmark_bundle(result, source, tmp_path / "first")
    second = write_fyp_value_benchmark_bundle(result, source, tmp_path / "second")
    relative_paths = (
        Path("value_benchmark.json"),
        Path("value_benchmark_policies.csv"),
        Path("controlled/controlled_evaluation_report.json"),
        Path("controlled/controlled_event_metrics.csv"),
        Path("controlled/controlled_bootstrap_intervals.csv"),
    )
    for relative in relative_paths:
        assert (tmp_path / "first" / relative).read_bytes() == (
            tmp_path / "second" / relative
        ).read_bytes()
    assert first.value_report_manifest_hash == second.value_report_manifest_hash


def test_reduced_work_alone_cannot_be_marked_qualifying(
    value_evidence: tuple[FypValueBenchmarkResult, ControlledEvaluationReport],
) -> None:
    result, _ = value_evidence
    reduced = next(policy for policy in result.policies if policy.reduces_pair_work)
    assert reduced.preserves_all_measured_effects is False

    with pytest.raises(ValidationError, match="qualification is inconsistent"):
        replace(reduced, qualifies=True)


def test_writer_rejects_a_different_source_report(
    tmp_path: Path,
    value_evidence: tuple[FypValueBenchmarkResult, ControlledEvaluationReport],
) -> None:
    result, source = value_evidence
    unbound = replace(result, source_report_manifest_hash="0" * 64)

    with pytest.raises(ValidationError, match="source report does not match"):
        write_fyp_value_benchmark_bundle(unbound, source, tmp_path)


def test_summary_states_negative_result_and_claim_boundary(
    tmp_path: Path,
    value_evidence: tuple[FypValueBenchmarkResult, ControlledEvaluationReport],
) -> None:
    result, _ = value_evidence
    summary = format_fyp_value_benchmark_summary(result, output_directory=tmp_path)

    assert "Verdict: NO_GO" in summary
    assert "does not demonstrate selective advantage" in summary
    assert "no real-model accuracy" in summary
    assert str(tmp_path) in summary
