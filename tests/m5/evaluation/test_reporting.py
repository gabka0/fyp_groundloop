from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

from groundloop.m5.evaluation.baselines import BaselineId, run_baselines
from groundloop.m5.evaluation.config import ControlledEvaluationConfig
from groundloop.m5.evaluation.histories import build_wice_primary_histories
from groundloop.m5.evaluation.metrics import build_metric_report
from groundloop.m5.evaluation.records import WiceSplit
from groundloop.m5.evaluation.reporting import (
    evaluation_report_dict,
    write_json_report,
)
from groundloop.m5.evaluation.wice import load_and_adapt_wice
from m5.evaluation.helpers import Rows, parent_row, subclaim_row


def test_report_keeps_record_families_separate_and_contains_no_source_text(
    tmp_path: Path,
    wice_fixture_factory: Callable[[Rows], tuple[Path, Path]],
    controlled_config_factory: Callable[[Path], ControlledEvaluationConfig],
) -> None:
    secret_source_text = "source-text-must-not-enter-report"
    rows: Rows = {
        WiceSplit.TRAIN: (
            [parent_row("report", [secret_source_text])],
            [subclaim_row("report-0", [secret_source_text], [[0]])],
        )
    }
    root, manifest_path = wice_fixture_factory(rows)
    adapter = load_and_adapt_wice(root, manifest_path)
    config = controlled_config_factory(manifest_path)
    histories = build_wice_primary_histories(adapter)
    run = run_baselines(histories, measure_latency=True)
    metrics = build_metric_report(
        run,
        seed=config.bootstrap.seed,
        resamples=config.bootstrap.resamples,
        confidence_level=config.bootstrap.confidence_level,
    )
    report = evaluation_report_dict(
        adapter=adapter,
        histories=histories,
        run=run,
        metrics=metrics,
        config=config,
    )
    output = tmp_path / "report.json"
    write_json_report(output, report)
    rendered = output.read_text(encoding="utf-8")
    parsed = json.loads(rendered)

    assert secret_source_text not in rendered
    assert parsed["evaluation_config"]["canonical_sha256"] == (config.canonical_sha256)
    assert parsed["adapter_audit"]["evaluation_config"] == (parsed["evaluation_config"])
    assert (
        parsed["source_verification"]
        == (parsed["adapter_audit"]["source_verification"])
    )
    assert parsed["result_classification"]["classification"] == (
        "non_primary_config_bundle"
    )
    assert parsed["result_classification"]["eligible"] is False
    assert parsed["metrics"]["bootstrap_seed"] == config.bootstrap.seed
    assert parsed["metrics"]["bootstrap_resamples"] == config.bootstrap.resamples
    assert parsed["metrics"]["bootstrap_confidence_level"] == (
        config.bootstrap.confidence_level
    )
    families = parsed["adapter_audit"]["record_families"]
    assert set(families) == {
        "source_human",
        "controlled_projection",
        "exact_system_state",
        "frozen_model_diagnostic",
    }
    assert families["controlled_projection"]["direct_claim_observation_count"] == 0
    assert families["frozen_model_diagnostic"]["used_for_selection"] is False
    assert parsed["test_selection"] == {
        "implementation_selected_on_test": False,
        "model_selected_on_test": False,
        "policy_selected_on_test": False,
        "prompt_selected_on_test": False,
        "threshold_selected_on_test": False,
    }


def test_report_marks_pure_target_backend_as_not_performance_validated(
    wice_fixture_factory: Callable[[Rows], tuple[Path, Path]],
    controlled_config_factory: Callable[[Path], ControlledEvaluationConfig],
) -> None:
    rows: Rows = {
        WiceSplit.TRAIN: (
            [parent_row("backend", ["evidence"])],
            [subclaim_row("backend-0", ["evidence"], [[0]])],
        )
    }
    root, manifest_path = wice_fixture_factory(rows)
    adapter = load_and_adapt_wice(root, manifest_path)
    config = controlled_config_factory(manifest_path)
    histories = build_wice_primary_histories(adapter)
    run = run_baselines(histories, measure_latency=True)
    report = evaluation_report_dict(
        adapter=adapter,
        histories=histories,
        run=run,
        metrics=build_metric_report(run, resamples=50),
        config=config,
    )

    protocol = report["baseline_protocol"]
    assert isinstance(protocol, dict)
    assert protocol["target_runtime_performance_validated"] is False
    specs = {
        item["baseline_id"]: item
        for item in protocol["specs"]  # type: ignore[index]
    }
    target = specs[BaselineId.GROUNDLOOP_HALL_SDR.value]
    assert target["execution_backend"] == (
        "pure-affected-group-hall-recompute-scaffold-v1"
    )
    assert target["measurement_validity"] == "functional_protocol_only"
    target_points = tuple(
        point
        for point in run.points
        if point.baseline_id is BaselineId.GROUNDLOOP_HALL_SDR
    )
    assert all(point.latency_ns is None for point in target_points)
