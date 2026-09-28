from __future__ import annotations

from pathlib import Path

import pytest

from groundloop.fyp_value_benchmark import (
    FypValueBenchmarkResult,
    build_fyp_value_benchmark,
)
from groundloop.m4.experiments import (
    ControlledEvaluationReport,
    build_frozen_controlled_fixture_v1,
    load_controlled_evaluation_config,
    run_controlled_evaluation,
)


@pytest.fixture(scope="session")
def value_evidence() -> tuple[FypValueBenchmarkResult, ControlledEvaluationReport]:
    config = load_controlled_evaluation_config(
        Path("configs/m4/evaluation/controlled_v1.json")
    )
    controlled = run_controlled_evaluation(
        config=config,
        fixture=build_frozen_controlled_fixture_v1(),
    )
    return build_fyp_value_benchmark(controlled), ControlledEvaluationReport(controlled)
