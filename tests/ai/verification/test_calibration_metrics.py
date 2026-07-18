from __future__ import annotations

import json
from pathlib import Path

import pytest

from groundloop.ai.verification.calibration import TemperatureCalibration
from groundloop.ai.verification.constants import Label
from groundloop.ai.verification.metrics import evaluate_probabilities
from groundloop.errors import ValidationError

LOGITS = ((3.0, 1.0, 0.0), (0.0, 4.0, 1.0), (0.0, 1.0, 3.0))
LABELS = (Label.REFUTE, Label.SUPPORT, Label.NEUTRAL)


def test_temperature_fit_accepts_development_only() -> None:
    fitted = TemperatureCalibration.fit(LOGITS, LABELS, split="development")
    assert fitted.temperature > 0
    assert fitted.nll_after <= fitted.nll_before + 1e-10
    assert fitted.calibration_version.startswith("temperature-v1:")
    with pytest.raises(ValidationError, match="leakage"):
        TemperatureCalibration.fit(LOGITS, LABELS, split="test")
    with pytest.raises(ValidationError, match="leakage"):
        TemperatureCalibration.fit(LOGITS, LABELS, split="train")


def test_calibration_artifact_identity_detects_tampering(tmp_path: Path) -> None:
    path = tmp_path / "temperature.json"
    fitted = TemperatureCalibration.fit(LOGITS, LABELS, split="development")
    fitted.write_json(path)
    assert TemperatureCalibration.read_json(path) == fitted
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["temperature"] = fitted.temperature + 0.1
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValidationError, match="identity"):
        TemperatureCalibration.read_json(path)


def test_probability_validation_rejects_malformed_rows() -> None:
    with pytest.raises(ValidationError, match="normalized"):
        evaluate_probabilities(((0.8, 0.3, 0.0),), (Label.SUPPORT,))
    with pytest.raises(ValidationError, match="finite"):
        evaluate_probabilities(((float("nan"), 0.0, 1.0),), (Label.NEUTRAL,))


def test_metrics_report_absent_class_as_not_estimable() -> None:
    metrics = evaluate_probabilities(
        ((0.8, 0.1, 0.1), (0.1, 0.1, 0.8)),
        (Label.SUPPORT, Label.NEUTRAL),
        bootstrap_resamples=20,
    )
    refute = metrics.per_class[int(Label.REFUTE)]
    assert refute.support == 0
    assert refute.recall is None
    assert refute.f1 is None
    assert metrics.macro_f1 == pytest.approx(1.0)
    assert len(metrics.reliability) == 10
