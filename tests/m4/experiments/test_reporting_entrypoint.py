"""Canonical report serialization and module-entrypoint tests."""

from __future__ import annotations

import csv
import io
import json
from pathlib import Path

from groundloop.m4.experiments import ControlledEvaluationReport
from groundloop.m4.experiments.__main__ import main


def test_json_and_csv_are_byte_stable_complete_and_claim_free(
    controlled_report: ControlledEvaluationReport,
) -> None:
    first_json = controlled_report.to_canonical_json()
    second_json = controlled_report.to_canonical_json()
    assert first_json == second_json
    payload = json.loads(first_json)
    assert payload["report_manifest_hash"] == controlled_report.manifest_hash
    assert payload["result_scope"] == (
        "deterministic_fixture_measurement_not_empirical_claim"
    )
    assert len(payload["policies"]) == 5
    assert len(payload["exhaustive_event_measurements"]) == 6
    assert payload["config"]["bootstrap"]["seed"] == 20260719
    assert len(payload["fixture"]["history_components"]) == 2

    event_csv = controlled_report.to_event_metrics_csv()
    assert event_csv == controlled_report.to_event_metrics_csv()
    rows = list(csv.DictReader(io.StringIO(event_csv)))
    assert len(rows) == 5 * 6 * 4
    assert {row["report_manifest_hash"] for row in rows} == {
        controlled_report.manifest_hash
    }
    delete_pair_rows = [
        row
        for row in rows
        if row["event_type"] == "delete"
        and row["metric_name"] == "positive_pair_recall"
    ]
    assert len(delete_pair_rows) == 10
    assert all(row["numerator"] == "0" for row in delete_pair_rows)
    assert all(row["denominator"] == "0" for row in delete_pair_rows)
    assert all(row["value"] == "" for row in delete_pair_rows)

    bootstrap_rows = list(
        csv.DictReader(io.StringIO(controlled_report.to_bootstrap_csv()))
    )
    assert len(bootstrap_rows) == 5 * 4
    assert {row["bootstrap_seed"] for row in bootstrap_rows} == {"20260719"}
    assert {row["replicate_count"] for row in bootstrap_rows} == {"10000"}


def test_one_command_entrypoint_writes_reproducible_bundle(
    tmp_path: Path,
    controlled_report: ControlledEvaluationReport,
    capsys,
) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    assert main(["--output-dir", str(first)]) == 0
    first_stdout = json.loads(capsys.readouterr().out)
    assert first_stdout["report_manifest_hash"] == controlled_report.manifest_hash
    assert first_stdout["scope"] == (
        "deterministic_fixture_measurement_not_empirical_claim"
    )

    assert main(["--output-dir", str(second)]) == 0
    capsys.readouterr()
    names = (
        "controlled_evaluation_report.json",
        "controlled_event_metrics.csv",
        "controlled_bootstrap_intervals.csv",
    )
    for name in names:
        assert (first / name).read_bytes() == (second / name).read_bytes()
