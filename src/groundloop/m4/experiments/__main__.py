"""One-command deterministic reproduction for the frozen M4.6 fixture."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path

from groundloop.m4.experiments import (
    ControlledEvaluationReport,
    build_frozen_controlled_fixture_v1,
    load_controlled_evaluation_config,
    run_controlled_evaluation,
    write_report_bundle,
)


def _default_config_path() -> Path:
    return Path("configs/m4/evaluation/controlled_v1.json")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the deterministic, model-free GroundLoop M4.6 fixture."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=_default_config_path(),
        help="checked-in controlled evaluation config",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="directory for canonical JSON and CSV artifacts",
    )
    arguments = parser.parse_args(argv)
    config = load_controlled_evaluation_config(arguments.config)
    fixture = build_frozen_controlled_fixture_v1()
    result = run_controlled_evaluation(config=config, fixture=fixture)
    bundle = write_report_bundle(
        ControlledEvaluationReport(result), arguments.output_dir
    )
    print(
        json.dumps(
            {
                "report_manifest_hash": bundle.report_manifest_hash,
                "json": str(bundle.json_path),
                "event_metrics_csv": str(bundle.event_metrics_csv_path),
                "bootstrap_csv": str(bundle.bootstrap_csv_path),
                "scope": "deterministic_fixture_measurement_not_empirical_claim",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
