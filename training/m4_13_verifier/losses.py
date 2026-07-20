"""Frozen M4.13 label mapping and revision-sensitive training losses."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from groundloop.errors import ValidationError

BASE_LOGIT_ORDER = ("contradiction", "entailment", "neutral")
STORED_LABEL_ORDER = ("support", "refute", "neutral")

_VITAMINC_TO_STORED = {
    "SUPPORTS": "support",
    "REFUTES": "refute",
    "NOT ENOUGH INFO": "neutral",
}
_BASE_INDEX_BY_STORED = {"support": 1, "refute": 0, "neutral": 2}


def vitaminc_label_to_stored(label: str) -> str:
    """Map the official VitaminC label without collapsing NEI to REFUTE."""
    try:
        return _VITAMINC_TO_STORED[label.strip().upper()]
    except KeyError as error:
        raise ValidationError(f"unsupported VitaminC label: {label!r}") from error


def normalize_stored_label(label: str) -> str:
    """Validate and canonicalize a GroundLoop stored-order label."""
    normalized = label.strip().casefold()
    if normalized not in _BASE_INDEX_BY_STORED:
        raise ValidationError(f"unsupported stored verifier label: {label!r}")
    return normalized


def stored_label_to_base_index(label: str) -> int:
    """Return the MiniLM base-logit index for a GroundLoop label."""
    return _BASE_INDEX_BY_STORED[normalize_stored_label(label)]


def base_probabilities_to_stored(probabilities: Sequence[float]) -> tuple[float, ...]:
    """Reorder (contradiction, entailment, neutral) into stored score order."""
    if len(probabilities) != 3 or any(
        not math.isfinite(value) for value in probabilities
    ):
        raise ValidationError("base probabilities must contain three finite values")
    return (probabilities[1], probabilities[0], probabilities[2])


def _require_torch() -> Any:
    try:
        import torch
    except ImportError as error:  # pragma: no cover - exercised without ML extra
        raise RuntimeError(
            "M4.13 losses require the optional PyTorch dependency"
        ) from error
    return torch


def _validate_loss_inputs(logits: Any, labels: Any, class_weights: Any) -> None:
    torch = _require_torch()
    if logits.ndim != 2 or tuple(logits.shape)[1:] != (3,):
        raise ValidationError("logits must have shape [examples, 3] in base order")
    if labels.ndim != 1 or labels.shape[0] != logits.shape[0]:
        raise ValidationError("labels must be a one-dimensional aligned tensor")
    if class_weights.ndim != 1 or tuple(class_weights.shape) != (3,):
        raise ValidationError("class weights must have shape [3] in base order")
    if labels.dtype != torch.long:
        raise ValidationError("labels must use torch.long base-logit indices")
    if bool(torch.any(labels < 0)) or bool(torch.any(labels > 2)):
        raise ValidationError("labels contain an invalid base-logit index")
    if not bool(torch.all(torch.isfinite(logits))):
        raise ValidationError("logits must be finite")
    if not bool(torch.all(torch.isfinite(class_weights))) or bool(
        torch.any(class_weights <= 0)
    ):
        raise ValidationError("class weights must be finite and positive")


def endpoint_cross_entropy(logits: Any, labels: Any, class_weights: Any) -> Any:
    """Compute the exact frozen endpoint objective.

    This intentionally uses ``mean_i w[y_i] * -log p_i[y_i]``.  PyTorch's
    weighted ``cross_entropy(..., reduction='mean')`` divides by the sum of
    selected weights instead and therefore does *not* implement the plan.
    """
    torch = _require_torch()
    _validate_loss_inputs(logits, labels, class_weights)
    log_probabilities = torch.log_softmax(logits, dim=1)
    row_indices = torch.arange(labels.shape[0], device=labels.device)
    selected = log_probabilities[row_indices, labels]
    return (-(class_weights[labels] * selected)).mean()


def paired_revision_margin_loss(
    logits: Any,
    labels: Any,
    transitions: Sequence[tuple[int, int]],
    *,
    margin: float = 0.5,
) -> Any:
    """Compute the symmetric paired log-probability margin loss."""
    torch = _require_torch()
    if logits.ndim != 2 or tuple(logits.shape)[1:] != (3,):
        raise ValidationError("logits must have shape [examples, 3] in base order")
    if labels.ndim != 1 or labels.shape[0] != logits.shape[0]:
        raise ValidationError("labels must be a one-dimensional aligned tensor")
    if labels.dtype != torch.long:
        raise ValidationError("labels must use torch.long base-logit indices")
    if not math.isfinite(margin):
        raise ValidationError("paired margin must be finite")
    if not bool(torch.all(torch.isfinite(logits))):
        raise ValidationError("logits must be finite")
    log_probabilities = torch.log_softmax(logits, dim=1)
    terms: list[Any] = []
    seen: set[tuple[int, int]] = set()
    for left, right in transitions:
        if left == right or min(left, right) < 0 or max(left, right) >= logits.shape[0]:
            raise ValidationError("paired transition index is invalid")
        pair = (left, right)
        if pair in seen:
            raise ValidationError("paired transition is duplicated")
        seen.add(pair)
        left_label = int(labels[left])
        right_label = int(labels[right])
        if left_label == right_label:
            raise ValidationError(
                "paired transition endpoints must have different labels"
            )
        left_gap = (
            log_probabilities[left, left_label] - log_probabilities[right, left_label]
        )
        right_gap = (
            log_probabilities[right, right_label] - log_probabilities[left, right_label]
        )
        terms.append(
            0.5
            * (
                torch.nn.functional.softplus(
                    torch.as_tensor(margin, dtype=logits.dtype, device=logits.device)
                    - left_gap
                )
                + torch.nn.functional.softplus(
                    torch.as_tensor(margin, dtype=logits.dtype, device=logits.device)
                    - right_gap
                )
            )
        )
    if not terms:
        return logits.sum() * 0.0
    return torch.stack(terms).mean()


@dataclass(frozen=True, slots=True)
class LossBreakdown:
    """Differentiable loss components returned by the composed objective."""

    total: Any
    endpoint: Any
    paired: Any


def change_aware_loss(
    logits: Any,
    labels: Any,
    class_weights: Any,
    transitions: Sequence[tuple[int, int]],
    *,
    include_paired_margin: bool,
    margin: float = 0.5,
    paired_weight: float = 0.25,
) -> LossBreakdown:
    """Compose endpoint CE and the frozen optional revision-margin term."""
    if not math.isfinite(paired_weight) or paired_weight < 0:
        raise ValidationError("paired loss weight must be finite and non-negative")
    endpoint = endpoint_cross_entropy(logits, labels, class_weights)
    if include_paired_margin:
        paired = paired_revision_margin_loss(logits, labels, transitions, margin=margin)
    else:
        paired = logits.sum() * 0.0
    return LossBreakdown(endpoint + paired_weight * paired, endpoint, paired)
