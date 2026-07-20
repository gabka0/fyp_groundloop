from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import pytest
from m4_13_verifier.calibrate import (
    CALIBRATION_SCHEMA,
    CalibrationFailureStage,
    DevelopmentLogit,
    _selection_allows,
    _validate_development_identities,
    _write_calibration_atomically,
    fit_group_balanced_temperature,
    group_balanced_nll,
)

from groundloop.errors import ValidationError


def _row(
    domain: str,
    row_id: str,
    group_id: str,
    label: str,
    logits: tuple[float, float, float],
) -> DevelopmentLogit:
    assert domain in {"m3", "vitaminc"}
    return DevelopmentLogit(  # type: ignore[arg-type]
        domain,
        row_id,
        group_id,
        label,
        "a" * 64,
        "b" * 64,
        logits,
        "c" * 64,
        "V3-margin-mix",
        20260720,
    )


def _nll(logits: tuple[float, float, float], base_label: int) -> float:
    maximum = max(logits)
    return (
        maximum
        + math.log(sum(math.exp(item - maximum) for item in logits))
        - logits[base_label]
    )


def test_nll_is_equal_domain_and_group_balanced_not_row_pooled() -> None:
    rows = (
        _row("m3", "m1", "large", "support", (0.0, 2.0, 0.0)),
        _row("m3", "m2", "large", "support", (0.0, 1.0, 0.0)),
        _row("m3", "m3", "small", "refute", (0.5, 0.0, 0.0)),
        _row("vitaminc", "v1", "case", "neutral", (0.0, 0.0, 1.5)),
        _row("vitaminc", "v2", "case", "support", (0.0, 1.5, 0.0)),
    )
    measured = group_balanced_nll(rows, 1.0)
    expected_m3 = 0.5 * (
        0.5 * (_nll((0.0, 2.0, 0.0), 1) + _nll((0.0, 1.0, 0.0), 1))
        + _nll((0.5, 0.0, 0.0), 0)
    )
    expected_vitamin = 0.5 * (_nll((0.0, 0.0, 1.5), 2) + _nll((0.0, 1.5, 0.0), 1))
    assert measured.m3 == pytest.approx(expected_m3)
    assert measured.vitaminc == pytest.approx(expected_vitamin)
    assert measured.combined == pytest.approx(0.5 * (expected_m3 + expected_vitamin))


def test_golden_temperature_is_deterministic_and_obeys_domain_gate() -> None:
    rows = (
        _row("m3", "m1", "g1", "support", (0.0, 1.0, 0.0)),
        _row("m3", "m2", "g2", "refute", (1.0, 0.0, 0.0)),
        _row("vitaminc", "v1", "c1", "neutral", (0.0, 0.0, 1.0)),
        _row("vitaminc", "v2", "c2", "support", (0.0, 1.0, 0.0)),
    )
    first = fit_group_balanced_temperature(rows)
    second = fit_group_balanced_temperature(rows)
    assert first == second
    assert first.accepted
    assert first.deployed_temperature == first.candidate_temperature
    assert first.nll_candidate.combined < first.nll_before.combined
    assert first.nll_candidate.m3 <= first.nll_before.m3 + 0.01
    assert first.nll_candidate.vitaminc <= first.nll_before.vitaminc + 0.01


def test_selection_must_uniquely_allow_exact_selected_checkpoint() -> None:
    selection = {
        "schema_version": "groundloop-m4-13-selection-v1",
        "sealed": True,
        "selected_variant": "V3-margin-mix",
        "primary_seed": 20260720,
        "candidate_checkpoints": [
            {
                "variant": "V3-margin-mix",
                "seed": 20260720,
                "checkpoint_tree_sha256": "c" * 64,
                "checkpoint_identity_sha256": "i" * 64,
            }
        ],
    }
    _selection_allows(
        selection,
        variant="V3-margin-mix",
        seed=20260720,
        checkpoint_tree_sha256="c" * 64,
        checkpoint_identity_sha256="i" * 64,
    )
    selection["selected_variant"] = "V2-ce-mix"
    with pytest.raises(ValidationError, match="not selected"):
        _selection_allows(
            selection,
            variant="V3-margin-mix",
            seed=20260720,
            checkpoint_tree_sha256="c" * 64,
            checkpoint_identity_sha256="i" * 64,
        )


@pytest.mark.parametrize("stage", list(CalibrationFailureStage))
def test_calibration_publish_is_failure_atomic(
    tmp_path: Path, stage: CalibrationFailureStage
) -> None:
    path = tmp_path / "calibration.json"
    with pytest.raises(RuntimeError, match="injected failure"):
        _write_calibration_atomically(
            path,
            {
                "schema_version": CALIBRATION_SCHEMA,
                "status": "complete",
                "invocation_sha256": "a" * 64,
            },
            expected_invocation_sha256="a" * 64,
            failure_stage=stage,
        )
    assert not path.exists()
    assert not list(tmp_path.glob(".calibration.json.partial-*"))


def test_existing_partial_calibration_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "calibration.json"
    path.write_text("{}\n", encoding="utf-8")
    with pytest.raises(ValidationError, match="partial"):
        _write_calibration_atomically(
            path,
            {
                "schema_version": CALIBRATION_SCHEMA,
                "status": "complete",
                "invocation_sha256": "a" * 64,
            },
            expected_invocation_sha256="a" * 64,
            failure_stage=None,
        )


def test_development_identity_check_rejects_same_count_substitution(
    tmp_path: Path,
) -> None:
    prepared = tmp_path / "prepared"
    prepared.mkdir()
    vitamin_claim = "vitamin claim"
    vitamin_evidence = "vitamin evidence"
    m3_claim = "m3 claim"
    m3_evidence = "m3 evidence"
    (prepared / "development_vitaminc.jsonl").write_text(
        json.dumps(
            {
                "split": "development",
                "unique_id": "v1",
                "case_id": "case-1",
                "label": "support",
                "claim_sha256": hashlib.sha256(vitamin_claim.encode()).hexdigest(),
                "evidence_sha256": hashlib.sha256(
                    vitamin_evidence.encode()
                ).hexdigest(),
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (prepared / "development_m3.jsonl").write_text(
        json.dumps(
            {
                "split": "development",
                "example_id": "m1",
                "claim_group_id": "group-1",
                "label": "neutral",
                "claim": m3_claim,
                "evidence": m3_evidence,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    rows = (
        DevelopmentLogit(
            "vitaminc",
            "v1",
            "case-1",
            "support",
            hashlib.sha256(vitamin_claim.encode()).hexdigest(),
            hashlib.sha256(vitamin_evidence.encode()).hexdigest(),
            (0.0, 1.0, 0.0),
            "c" * 64,
            "V3-margin-mix",
            20260720,
        ),
        DevelopmentLogit(
            "m3",
            "m1",
            "group-1",
            "neutral",
            hashlib.sha256(m3_claim.encode()).hexdigest(),
            hashlib.sha256(m3_evidence.encode()).hexdigest(),
            (0.0, 0.0, 1.0),
            "c" * 64,
            "V3-margin-mix",
            20260720,
        ),
    )
    _validate_development_identities(rows, prepared)
    substituted = (
        DevelopmentLogit(
            "vitaminc",
            "same-count-substitute",
            rows[0].group_id,
            rows[0].stored_label,
            rows[0].claim_sha256,
            rows[0].evidence_sha256,
            rows[0].logits,
            rows[0].checkpoint_tree_sha256,
            rows[0].variant,
            rows[0].seed,
        ),
        rows[1],
    )
    with pytest.raises(ValidationError, match="exactly match"):
        _validate_development_identities(substituted, prepared)
