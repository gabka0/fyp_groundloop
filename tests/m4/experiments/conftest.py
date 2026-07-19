"""Shared deterministic M4.6 experiment fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest

from groundloop.m4.experiments import (
    ControlledEvaluationReport,
    ControlledEvaluationResult,
    build_frozen_controlled_fixture_v1,
    load_controlled_evaluation_config,
    run_controlled_evaluation,
)


@pytest.fixture(scope="session")
def controlled_result() -> ControlledEvaluationResult:
    config = load_controlled_evaluation_config(
        Path("configs/m4/evaluation/controlled_v1.json")
    )
    fixture = build_frozen_controlled_fixture_v1()
    return run_controlled_evaluation(config=config, fixture=fixture)


@pytest.fixture(scope="session")
def controlled_report(
    controlled_result: ControlledEvaluationResult,
) -> ControlledEvaluationReport:
    return ControlledEvaluationReport(controlled_result)
