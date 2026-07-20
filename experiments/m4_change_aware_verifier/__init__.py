"""Sealed evaluation utilities for the M4.13 verifier experiment."""

from .contracts import EvaluationRow, ModelSpec, RawLogitRow
from .metrics import (
    bootstrap_memberships,
    classification_metrics,
    evaluate_m3,
    evaluate_vitaminc,
    paired_bootstrap_classification,
    paired_bootstrap_vitaminc,
    summarize_training_seeds,
)
from .provenance import (
    M4_12_FROZEN_IDENTITIES,
    TERMINAL_RESERVE_SHA256,
    verify_corrected_m4_10,
    verify_m4_12_diagnostic,
    verify_terminal_reserve,
)
from .selection import SealedSelection, load_sealed_selection

__all__ = [
    "EvaluationRow",
    "M4_12_FROZEN_IDENTITIES",
    "ModelSpec",
    "RawLogitRow",
    "SealedSelection",
    "TERMINAL_RESERVE_SHA256",
    "bootstrap_memberships",
    "classification_metrics",
    "evaluate_m3",
    "evaluate_vitaminc",
    "load_sealed_selection",
    "paired_bootstrap_classification",
    "paired_bootstrap_vitaminc",
    "summarize_training_seeds",
    "verify_corrected_m4_10",
    "verify_m4_12_diagnostic",
    "verify_terminal_reserve",
]
