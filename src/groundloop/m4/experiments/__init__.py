"""Controlled, SQL-free M4.6 policy evaluation machinery."""

from groundloop.m4.experiments.config import (
    ControlledEvaluationConfig,
    load_controlled_evaluation_config,
)
from groundloop.m4.experiments.contracts import (
    DeliberateMissRecord,
    EvaluationPolicy,
    ExhaustiveEventMeasurement,
    PolicyEvaluation,
    PolicyEventMeasurement,
    PolicyKind,
    VerifierWorkRecord,
)
from groundloop.m4.experiments.fixture import (
    FrozenControlledFixture,
    build_frozen_controlled_fixture_v1,
)
from groundloop.m4.experiments.reporting import (
    ControlledEvaluationReport,
    WrittenReportBundle,
    write_report_bundle,
)
from groundloop.m4.experiments.runner import (
    ControlledEvaluationResult,
    run_controlled_evaluation,
)

__all__ = [
    "ControlledEvaluationConfig",
    "ControlledEvaluationReport",
    "ControlledEvaluationResult",
    "DeliberateMissRecord",
    "EvaluationPolicy",
    "ExhaustiveEventMeasurement",
    "FrozenControlledFixture",
    "PolicyEvaluation",
    "PolicyEventMeasurement",
    "PolicyKind",
    "VerifierWorkRecord",
    "WrittenReportBundle",
    "build_frozen_controlled_fixture_v1",
    "load_controlled_evaluation_config",
    "run_controlled_evaluation",
    "write_report_bundle",
]
