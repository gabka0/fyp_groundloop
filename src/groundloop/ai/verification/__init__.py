"""Three-way evidence verification at GroundLoop's empirical boundary."""

from groundloop.ai.verification.adapter import (
    AdapterDiagnostics,
    DeterministicFakeVerifier,
    PinnedMiniLMVerifier,
    VerificationCompletion,
    logits_to_score_triple,
)
from groundloop.ai.verification.calibration import TemperatureCalibration
from groundloop.ai.verification.constants import (
    BASE_MODEL_ID,
    BASE_MODEL_REVISION,
    LABELS,
    Label,
)

__all__ = [
    "AdapterDiagnostics",
    "BASE_MODEL_ID",
    "BASE_MODEL_REVISION",
    "DeterministicFakeVerifier",
    "LABELS",
    "Label",
    "PinnedMiniLMVerifier",
    "TemperatureCalibration",
    "VerificationCompletion",
    "logits_to_score_triple",
]
