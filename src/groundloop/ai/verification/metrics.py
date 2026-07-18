"""Dependency-light multiclass quality and calibration metrics."""

from __future__ import annotations

import math
import random
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass

from groundloop.ai.verification.constants import LABELS, Label
from groundloop.errors import ValidationError


@dataclass(frozen=True, slots=True)
class ClassMetrics:
    label: str
    support: int
    predicted: int
    true_positive: int
    precision: float | None
    recall: float | None
    f1: float | None


@dataclass(frozen=True, slots=True)
class ReliabilityRow:
    lower: float
    upper: float
    count: int
    mean_confidence: float
    accuracy: float
    absolute_gap: float


@dataclass(frozen=True, slots=True)
class ConfidenceInterval:
    lower: float
    upper: float
    level: float
    method: str
    resamples: int


@dataclass(frozen=True, slots=True)
class EvaluationMetrics:
    example_count: int
    class_counts: dict[str, int]
    per_class: tuple[ClassMetrics, ...]
    macro_f1: float
    accuracy: float
    confusion_matrix: tuple[tuple[int, ...], ...]
    multiclass_brier: float
    ece: float
    ece_bins: int
    reliability: tuple[ReliabilityRow, ...]
    confidence_intervals: dict[str, ConfidenceInterval]

    def to_json(self) -> dict[str, object]:
        return {
            "example_count": self.example_count,
            "class_counts": self.class_counts,
            "per_class": [asdict(row) for row in self.per_class],
            "macro_f1": self.macro_f1,
            "accuracy": self.accuracy,
            "confusion_matrix": [list(row) for row in self.confusion_matrix],
            "multiclass_brier": self.multiclass_brier,
            "ece": self.ece,
            "ece_bins": self.ece_bins,
            "reliability": [asdict(row) for row in self.reliability],
            "confidence_intervals": {
                name: asdict(interval)
                for name, interval in self.confidence_intervals.items()
            },
        }


def _validate(
    probabilities: Sequence[tuple[float, float, float]], labels: Sequence[Label]
) -> None:
    if not probabilities or len(probabilities) != len(labels):
        raise ValidationError(
            "metrics require nonempty aligned probabilities and labels"
        )
    for row in probabilities:
        if any(not math.isfinite(value) or value < 0.0 or value > 1.0 for value in row):
            raise ValidationError("metric probabilities must be finite in [0, 1]")
        if not math.isclose(sum(row), 1.0, rel_tol=1e-6, abs_tol=1e-6):
            raise ValidationError("metric probabilities must be normalized")


def _predictions(
    probabilities: Sequence[tuple[float, float, float]],
) -> tuple[Label, ...]:
    # Metric argmax uses stable stored order. GroundLoop's operational label is
    # still derived separately by DecisionPolicy tie rule v1.
    return tuple(
        Label(max(range(3), key=lambda index: row[index])) for row in probabilities
    )


def _point_metrics(
    probabilities: Sequence[tuple[float, float, float]], labels: Sequence[Label]
) -> tuple[float, float, float, float]:
    predicted = _predictions(probabilities)
    f1_values: list[float] = []
    for label in Label:
        true_positive = sum(
            prediction == label and expected == label
            for prediction, expected in zip(predicted, labels, strict=True)
        )
        predicted_count = sum(prediction == label for prediction in predicted)
        support = sum(expected == label for expected in labels)
        precision = true_positive / predicted_count if predicted_count else None
        recall = true_positive / support if support else None
        if support:
            precision_for_f1 = precision or 0.0
            recall_for_f1 = recall or 0.0
            f1_values.append(
                2.0
                * precision_for_f1
                * recall_for_f1
                / (precision_for_f1 + recall_for_f1)
                if precision_for_f1 + recall_for_f1
                else 0.0
            )
    macro_f1 = sum(f1_values) / len(f1_values)
    accuracy = sum(
        prediction == expected
        for prediction, expected in zip(predicted, labels, strict=True)
    ) / len(labels)
    brier = sum(
        sum(
            (probability - float(index == int(expected))) ** 2
            for index, probability in enumerate(row)
        )
        for row, expected in zip(probabilities, labels, strict=True)
    ) / len(labels)
    ece = expected_calibration_error(probabilities, labels, bins=10)[0]
    return macro_f1, accuracy, brier, ece


def expected_calibration_error(
    probabilities: Sequence[tuple[float, float, float]],
    labels: Sequence[Label],
    *,
    bins: int,
) -> tuple[float, tuple[ReliabilityRow, ...]]:
    if bins <= 0:
        raise ValidationError("ECE bins must be positive")
    predicted = _predictions(probabilities)
    rows: list[ReliabilityRow] = []
    weighted_gap = 0.0
    for index in range(bins):
        lower = index / bins
        upper = (index + 1) / bins
        members = [
            row_index
            for row_index, row in enumerate(probabilities)
            if (lower <= max(row) < upper)
            or (index == bins - 1 and math.isclose(max(row), 1.0))
        ]
        if members:
            confidence = sum(max(probabilities[item]) for item in members) / len(
                members
            )
            accuracy = sum(predicted[item] == labels[item] for item in members) / len(
                members
            )
            gap = abs(confidence - accuracy)
            weighted_gap += len(members) * gap / len(labels)
        else:
            confidence = 0.0
            accuracy = 0.0
            gap = 0.0
        rows.append(
            ReliabilityRow(lower, upper, len(members), confidence, accuracy, gap)
        )
    return weighted_gap, tuple(rows)


def _bootstrap_interval(
    probabilities: Sequence[tuple[float, float, float]],
    labels: Sequence[Label],
    metric: Callable[[Sequence[tuple[float, float, float]], Sequence[Label]], float],
    *,
    seed: int,
    resamples: int,
) -> ConfidenceInterval:
    if resamples <= 0:
        raise ValidationError("bootstrap resamples must be positive")
    generator = random.Random(seed)
    estimates: list[float] = []
    for _ in range(resamples):
        indices = [generator.randrange(len(labels)) for _ in labels]
        sampled_probabilities = [probabilities[index] for index in indices]
        sampled_labels = [labels[index] for index in indices]
        estimates.append(metric(sampled_probabilities, sampled_labels))
    estimates.sort()
    lower_index = max(0, math.floor(0.025 * resamples))
    upper_index = min(resamples - 1, math.ceil(0.975 * resamples) - 1)
    return ConfidenceInterval(
        estimates[lower_index],
        estimates[upper_index],
        0.95,
        "percentile-bootstrap-v1",
        resamples,
    )


def evaluate_probabilities(
    probabilities: Sequence[tuple[float, float, float]],
    labels: Sequence[Label],
    *,
    ece_bins: int = 10,
    bootstrap_resamples: int = 1000,
    seed: int = 20260718,
) -> EvaluationMetrics:
    _validate(probabilities, labels)
    predicted = _predictions(probabilities)
    matrix = [[0 for _ in Label] for _ in Label]
    per_class: list[ClassMetrics] = []
    for expected, prediction in zip(labels, predicted, strict=True):
        matrix[int(expected)][int(prediction)] += 1
    for label in Label:
        support = sum(expected == label for expected in labels)
        predicted_count = sum(item == label for item in predicted)
        true_positive = matrix[int(label)][int(label)]
        precision = true_positive / predicted_count if predicted_count else None
        recall = true_positive / support if support else None
        precision_for_f1 = precision or 0.0
        recall_for_f1 = recall or 0.0
        f1 = (
            2.0 * precision_for_f1 * recall_for_f1 / (precision_for_f1 + recall_for_f1)
            if support and precision_for_f1 + recall_for_f1
            else (0.0 if support else None)
        )
        per_class.append(
            ClassMetrics(
                LABELS[int(label)],
                support,
                predicted_count,
                true_positive,
                precision,
                recall,
                f1,
            )
        )
    macro_f1, accuracy, brier, _ = _point_metrics(probabilities, labels)
    ece, reliability = expected_calibration_error(probabilities, labels, bins=ece_bins)
    metric_functions: dict[
        str,
        Callable[[Sequence[tuple[float, float, float]], Sequence[Label]], float],
    ] = {
        "macro_f1": lambda p, y: _point_metrics(p, y)[0],
        "accuracy": lambda p, y: _point_metrics(p, y)[1],
        "multiclass_brier": lambda p, y: _point_metrics(p, y)[2],
        "ece": lambda p, y: expected_calibration_error(p, y, bins=ece_bins)[0],
    }
    intervals = {
        name: _bootstrap_interval(
            probabilities,
            labels,
            function,
            seed=seed + offset,
            resamples=bootstrap_resamples,
        )
        for offset, (name, function) in enumerate(metric_functions.items())
    }
    return EvaluationMetrics(
        example_count=len(labels),
        class_counts={
            LABELS[int(label)]: sum(expected == label for expected in labels)
            for label in Label
        },
        per_class=tuple(per_class),
        macro_f1=macro_f1,
        accuracy=accuracy,
        confusion_matrix=tuple(tuple(row) for row in matrix),
        multiclass_brier=brier,
        ece=ece,
        ece_bins=ece_bins,
        reliability=reliability,
        confidence_intervals=intervals,
    )
