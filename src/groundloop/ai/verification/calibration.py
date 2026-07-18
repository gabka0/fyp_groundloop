"""Development-only scalar temperature calibration for three-way logits."""

from __future__ import annotations

import json
import math
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from hashlib import sha256
from pathlib import Path

from groundloop.ai.verification.adapter import logits_to_score_triple
from groundloop.ai.verification.constants import Label
from groundloop.errors import ValidationError

_ALLOWED_FIT_SPLITS = frozenset({"development", "dev", "validation", "calibration"})


def _nll(
    logits: Sequence[tuple[float, float, float]],
    labels: Sequence[Label],
    temperature: float,
) -> float:
    total = 0.0
    for values, label in zip(logits, labels, strict=True):
        triple = logits_to_score_triple(values, temperature)
        probabilities = (triple.support, triple.refute, triple.neutral)
        total -= math.log(max(probabilities[int(label)], 1e-15))
    return total / len(labels)


@dataclass(frozen=True, slots=True)
class TemperatureCalibration:
    temperature: float
    fit_split: str
    example_count: int
    nll_before: float
    nll_after: float
    method: str = "scalar-temperature-golden-v1"

    @property
    def calibration_version(self) -> str:
        payload = json.dumps(
            asdict(self), sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return f"temperature-v1:{sha256(payload).hexdigest()}"

    def __post_init__(self) -> None:
        if not math.isfinite(self.temperature) or self.temperature <= 0.0:
            raise ValidationError("calibration temperature must be positive")
        if self.fit_split.casefold() not in _ALLOWED_FIT_SPLITS:
            raise ValidationError("temperature may be fit on development data only")
        if self.example_count <= 0:
            raise ValidationError("calibration requires at least one example")
        if not math.isfinite(self.nll_before) or not math.isfinite(self.nll_after):
            raise ValidationError("calibration losses must be finite")

    @classmethod
    def fit(
        cls,
        logits: Sequence[tuple[float, float, float]],
        labels: Sequence[Label],
        *,
        split: str,
        lower: float = 0.05,
        upper: float = 20.0,
        iterations: int = 96,
    ) -> TemperatureCalibration:
        if split.casefold() not in _ALLOWED_FIT_SPLITS:
            raise ValidationError("calibration leakage guard: split is not development")
        if not logits or len(logits) != len(labels):
            raise ValidationError(
                "calibration logits and labels must be nonempty and aligned"
            )
        if lower <= 0.0 or upper <= lower or iterations <= 0:
            raise ValidationError("invalid temperature-search bounds")
        # Search log-temperature so the positive domain is covered smoothly.
        left, right = math.log(lower), math.log(upper)
        ratio = (math.sqrt(5.0) - 1.0) / 2.0
        first = right - ratio * (right - left)
        second = left + ratio * (right - left)
        first_loss = _nll(logits, labels, math.exp(first))
        second_loss = _nll(logits, labels, math.exp(second))
        for _ in range(iterations):
            if first_loss <= second_loss:
                right = second
                second = first
                second_loss = first_loss
                first = right - ratio * (right - left)
                first_loss = _nll(logits, labels, math.exp(first))
            else:
                left = first
                first = second
                first_loss = second_loss
                second = left + ratio * (right - left)
                second_loss = _nll(logits, labels, math.exp(second))
        temperature = math.exp((left + right) / 2.0)
        return cls(
            temperature=temperature,
            fit_split=split,
            example_count=len(labels),
            nll_before=_nll(logits, labels, 1.0),
            nll_after=_nll(logits, labels, temperature),
        )

    def write_json(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = asdict(self)
        payload["calibration_version"] = self.calibration_version
        path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    @classmethod
    def read_json(cls, path: Path) -> TemperatureCalibration:
        if not path.is_file():
            raise FileNotFoundError(
                f"calibration artifact is absent: {path}; run calibrate.py first"
            )
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValidationError("calibration artifact must be a JSON object")
        calibration = cls(
            temperature=float(payload["temperature"]),
            fit_split=str(payload["fit_split"]),
            example_count=int(payload["example_count"]),
            nll_before=float(payload["nll_before"]),
            nll_after=float(payload["nll_after"]),
            method=str(payload["method"]),
        )
        recorded_version = payload.get("calibration_version")
        if (
            recorded_version is not None
            and str(recorded_version) != calibration.calibration_version
        ):
            raise ValidationError(
                "calibration artifact identity does not match content"
            )
        return calibration
