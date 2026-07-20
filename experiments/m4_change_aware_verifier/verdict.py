"""Machine evaluation of the pre-registered M4.13 stop/go clauses."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from groundloop.errors import ValidationError

from .selection import PRIMARY_SEED, REPLICATION_SEEDS


@dataclass(frozen=True, slots=True)
class SeedGateInput:
    seed: int
    reserve_joint_correct: float
    reserve_flip_detected: float
    reserve_bidirectional_margin: float
    joint_delta_lower: float
    joint_delta_upper: float
    joint_delta_point: float
    flip_delta_lower: float
    flip_delta_upper: float
    margin_delta_lower: float
    m3_macro_f1: float
    m3_accuracy: float
    m3_macro_f1_delta_lower: float
    m3_accuracy_delta_lower: float
    calibration_valid: bool
    complete: bool


def _clause(
    identifier: str, passed: bool, observed: object, rule: str
) -> dict[str, object]:
    return {
        "id": identifier,
        "passed": passed,
        "observed": observed,
        "rule": rule,
    }


def evaluate_stop_go(
    by_seed: Mapping[int, SeedGateInput], *, provenance_complete: bool
) -> Mapping[str, object]:
    if set(by_seed) != set(REPLICATION_SEEDS):
        raise ValidationError("stop/go evaluation requires exactly three frozen seeds")
    primary = by_seed[PRIMARY_SEED]
    point_passes = {
        seed: (
            values.reserve_joint_correct >= 0.30
            and values.reserve_flip_detected >= 0.50
            and values.reserve_bidirectional_margin >= 0.75
        )
        for seed, values in by_seed.items()
    }
    clauses = (
        _clause(
            "G1",
            primary.reserve_joint_correct >= 0.30 and sum(point_passes.values()) >= 2,
            {
                "primary": primary.reserve_joint_correct,
                "seeds_passing_all_point_thresholds": sum(point_passes.values()),
            },
            "primary joint_correct >= 0.30 and at least two seeds pass all "
            "point floors",
        ),
        _clause(
            "G2",
            primary.reserve_flip_detected >= 0.50 and sum(point_passes.values()) >= 2,
            primary.reserve_flip_detected,
            "primary flip_detected >= 0.50 and at least two seeds pass all "
            "point floors",
        ),
        _clause(
            "G3",
            primary.reserve_bidirectional_margin >= 0.75
            and sum(point_passes.values()) >= 2,
            primary.reserve_bidirectional_margin,
            "primary bidirectional_margin >= 0.75 and at least two seeds pass "
            "all point floors",
        ),
        _clause(
            "G4",
            primary.joint_delta_lower > 0.0 and primary.joint_delta_point >= 0.10,
            {
                "lower": primary.joint_delta_lower,
                "upper": primary.joint_delta_upper,
                "point": primary.joint_delta_point,
            },
            "primary paired page-bootstrap joint delta lower > 0 and point >= 0.10",
        ),
        _clause(
            "G5",
            primary.flip_delta_lower > 0.0,
            {
                "lower": primary.flip_delta_lower,
                "upper": primary.flip_delta_upper,
            },
            "primary paired page-bootstrap flip delta lower > 0",
        ),
        _clause(
            "G6",
            primary.margin_delta_lower >= -0.03,
            primary.margin_delta_lower,
            "primary paired page-bootstrap margin delta lower >= -0.03",
        ),
        _clause(
            "G7",
            primary.m3_macro_f1 >= 0.4998 and primary.m3_accuracy >= 0.5873,
            {"macro_f1": primary.m3_macro_f1, "accuracy": primary.m3_accuracy},
            "primary M3 macro-F1 >= 0.4998 and accuracy >= 0.5873",
        ),
        _clause(
            "G8",
            primary.m3_macro_f1_delta_lower >= -0.05
            and primary.m3_accuracy_delta_lower >= -0.05,
            {
                "macro_f1_delta_lower": primary.m3_macro_f1_delta_lower,
                "accuracy_delta_lower": primary.m3_accuracy_delta_lower,
            },
            "primary paired M3 lower bounds for macro-F1 and accuracy >= -0.05",
        ),
        _clause(
            "G9",
            primary.calibration_valid,
            primary.calibration_valid,
            "calibration accepted or explicitly rejected in favor of T=1",
        ),
        _clause(
            "G10",
            provenance_complete and all(values.complete for values in by_seed.values()),
            {
                "provenance_complete": provenance_complete,
                "complete_seed_count": sum(
                    values.complete for values in by_seed.values()
                ),
            },
            "all provenance, determinism, raw-output and completeness gates pass",
        ),
    )
    failed = [str(clause["id"]) for clause in clauses if not clause["passed"]]
    statistical = clauses[3]["passed"] and clauses[4]["passed"]
    point_thresholds = (
        clauses[0]["passed"]
        and clauses[1]["passed"]
        and clauses[2]["passed"]
        and primary.joint_delta_point >= 0.10
    )
    nonstatistical_safety_passed = all(bool(clause["passed"]) for clause in clauses[5:])
    if not failed:
        verdict = "GO"
    elif (
        point_thresholds
        and not statistical
        and nonstatistical_safety_passed
        and (
            primary.joint_delta_lower <= 0.0 <= primary.joint_delta_upper
            or primary.flip_delta_lower <= 0.0 <= primary.flip_delta_upper
        )
        and not (primary.joint_delta_upper < 0.0 or primary.flip_delta_upper < 0.0)
    ):
        verdict = "ENGINEERING_POSITIVE_STATISTICALLY_INCONCLUSIVE"
    else:
        verdict = "NO_GO"
    return {
        "schema_version": "groundloop-m4-13-stop-go-v1",
        "verdict": verdict,
        "clauses": list(clauses),
        "failed_clauses": failed,
        "promotion_authorized": verdict == "GO",
        "terminal_tuning_authorized": False,
    }
