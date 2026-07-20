"""Endpoint, revision-transition, and dependence-preserving uncertainty metrics."""

from __future__ import annotations

import math
import random
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from statistics import fmean, stdev
from typing import TypeVar, cast

from groundloop.errors import ValidationError

from .contracts import STORED_LABELS, RawLogitRow
from .scoring import operational_label

_T = TypeVar("_T")
_CLASSIFICATION_SCALARS = (
    "accuracy",
    "macro_f1",
    "nll",
    "multiclass_brier",
    "ece",
)
_TRANSITION_SCALARS = (
    "flip_detected",
    "joint_correct",
    "bidirectional_margin",
)


def _argmax(probabilities: Sequence[float]) -> int:
    return max(range(3), key=lambda index: probabilities[index])


def _label_index(label: str | None) -> int:
    if label not in STORED_LABELS:
        raise ValidationError(f"metrics require a mapped label, got {label!r}")
    return STORED_LABELS.index(label)


def _validate_rows(
    rows: Sequence[RawLogitRow], *, require_unique_ids: bool = True
) -> None:
    if not rows:
        raise ValidationError("metrics require at least one raw-logit row")
    row_ids = [row.row_id for row in rows]
    if require_unique_ids and len(set(row_ids)) != len(row_ids):
        raise ValidationError("raw-logit row IDs must be unique within a model fixture")
    for row in rows:
        _label_index(row.mapped_label)


def classification_metrics(
    rows: Sequence[RawLogitRow], *, ece_bins: int = 10
) -> Mapping[str, object]:
    """Three-way metrics with null, rather than zero, for absent true classes."""
    return _classification_metrics(
        rows,
        ece_bins=ece_bins,
        require_unique_ids=True,
        probability_surface="deployed_temperature",
    )


def classification_calibration_ablation(
    rows: Sequence[RawLogitRow], *, ece_bins: int = 10
) -> Mapping[str, object]:
    """Compare T=1, old-M3-T and deployed-T without selecting on test."""
    _validate_rows(rows)
    deployed_temperatures = {row.temperature for row in rows}
    if len(deployed_temperatures) != 1:
        raise ValidationError("calibration ablation requires one deployed temperature")
    deployed_temperature = next(iter(deployed_temperatures))
    return {
        "uncalibrated_T1": {
            "temperature": 1.0,
            **_classification_metrics(
                tuple(
                    replace(
                        row,
                        calibrated_probabilities=row.uncalibrated_probabilities,
                        temperature=1.0,
                    )
                    for row in rows
                ),
                ece_bins=ece_bins,
                require_unique_ids=True,
                probability_surface="uncalibrated_T1",
            ),
        },
        "old_m3_temperature": {
            "temperature": 1.1037657679769346,
            **_classification_metrics(
                tuple(
                    replace(
                        row,
                        calibrated_probabilities=(row.old_m3_temperature_probabilities),
                        temperature=1.1037657679769346,
                    )
                    for row in rows
                ),
                ece_bins=ece_bins,
                require_unique_ids=True,
                probability_surface="old_m3_temperature",
            ),
        },
        "deployed_temperature": {
            "temperature": deployed_temperature,
            **_classification_metrics(
                rows,
                ece_bins=ece_bins,
                require_unique_ids=True,
                probability_surface="deployed_temperature",
            ),
        },
    }


def _classification_metrics(
    rows: Sequence[RawLogitRow],
    *,
    ece_bins: int,
    require_unique_ids: bool,
    probability_surface: str = "deployed_temperature",
) -> Mapping[str, object]:
    _validate_rows(rows, require_unique_ids=require_unique_ids)
    if ece_bins <= 0:
        raise ValidationError("ECE bins must be positive")
    temperatures = {row.temperature for row in rows}
    if len(temperatures) != 1:
        raise ValidationError(
            "classification metrics require one deployed probability surface"
        )
    temperature = next(iter(temperatures))
    expected = tuple(_label_index(row.mapped_label) for row in rows)
    probabilities = tuple(row.calibrated_probabilities for row in rows)
    predicted = tuple(_argmax(row) for row in probabilities)
    matrix = [[0, 0, 0] for _ in STORED_LABELS]
    for truth, prediction in zip(expected, predicted, strict=True):
        matrix[truth][prediction] += 1

    per_class: dict[str, Mapping[str, object]] = {}
    present_f1: list[float] = []
    for index, label in enumerate(STORED_LABELS):
        support = sum(truth == index for truth in expected)
        predicted_count = sum(item == index for item in predicted)
        true_positive = matrix[index][index]
        precision = (
            true_positive / predicted_count
            if predicted_count
            else 0.0
            if support
            else None
        )
        recall = true_positive / support if support else None
        if support:
            precision_for_f1 = precision or 0.0
            recall_for_f1 = recall or 0.0
            denominator = precision_for_f1 + recall_for_f1
            f1 = (
                2.0 * precision_for_f1 * recall_for_f1 / denominator
                if denominator
                else 0.0
            )
            present_f1.append(f1)
        else:
            f1 = None
        per_class[label] = {
            "support": support,
            "predicted": predicted_count,
            "true_positive": true_positive,
            "precision": precision,
            "recall": recall,
            "f1": f1,
        }

    count = len(rows)
    accuracy = (
        sum(
            truth == prediction
            for truth, prediction in zip(expected, predicted, strict=True)
        )
        / count
    )
    nll = (
        -sum(
            math.log(max(probability[truth], 1e-15))
            for probability, truth in zip(probabilities, expected, strict=True)
        )
        / count
    )
    brier = (
        sum(
            sum(
                (value - float(class_index == truth)) ** 2
                for class_index, value in enumerate(probability)
            )
            for probability, truth in zip(probabilities, expected, strict=True)
        )
        / count
    )

    ece = 0.0
    reliability: list[Mapping[str, object]] = []
    for bin_index in range(ece_bins):
        lower = bin_index / ece_bins
        upper = (bin_index + 1) / ece_bins
        members = [
            index
            for index, probability in enumerate(probabilities)
            if lower <= max(probability) < upper
            or (bin_index == ece_bins - 1 and math.isclose(max(probability), 1.0))
        ]
        if members:
            confidence = fmean(max(probabilities[index]) for index in members)
            bin_accuracy = fmean(
                float(predicted[index] == expected[index]) for index in members
            )
            gap = abs(confidence - bin_accuracy)
            ece += len(members) * gap / count
        else:
            confidence = 0.0
            bin_accuracy = 0.0
            gap = 0.0
        reliability.append(
            {
                "lower": lower,
                "upper": upper,
                "count": len(members),
                "mean_confidence": confidence,
                "accuracy": bin_accuracy,
                "absolute_gap": gap,
            }
        )
    return {
        "probability_surface": probability_surface,
        "temperature": temperature,
        "rows": count,
        "class_counts": {
            label: sum(truth == index for truth in expected)
            for index, label in enumerate(STORED_LABELS)
        },
        "accuracy": accuracy,
        "macro_f1": fmean(present_f1),
        "macro_f1_semantics": "unweighted mean over present true-label classes",
        "zero_division_semantics": (
            "precision is 0 when a present class has no predictions; absent true "
            "classes retain null recall and F1"
        ),
        "per_class": per_class,
        "confusion_matrix": matrix,
        "confusion_matrix_order": list(STORED_LABELS),
        "nll": nll,
        "nll_probability_floor": 1e-15,
        "multiclass_brier": brier,
        "ece": ece,
        "ece_bins": ece_bins,
        "reliability": reliability,
        "raw_argmax_counts": {
            label: sum(prediction == index for prediction in predicted)
            for index, label in enumerate(STORED_LABELS)
        },
        "operational_label_counts": {
            label: sum(
                operational_label(probability) == label for probability in probabilities
            )
            for label in STORED_LABELS
        },
        "operational_policy": {
            "version": "m3-policy-v1",
            "support_threshold": 0.8,
            "refute_threshold": 0.8,
            "tie_rule_version": "v1",
        },
    }


def _ordered_unique(values: Sequence[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(values))


def bootstrap_memberships(
    units: Sequence[str], *, seed: int, resamples: int
) -> tuple[tuple[str, ...], ...]:
    """Return cluster draws; all rows for a drawn unit must be included by callers."""
    if not units or any(not unit for unit in units) or resamples <= 0:
        raise ValidationError("bootstrap units and resample count must be positive")
    universe = _ordered_unique(units)
    generator = random.Random(seed)
    return tuple(
        tuple(universe[generator.randrange(len(universe))] for _ in universe)
        for _ in range(resamples)
    )


def stratified_bootstrap_memberships(
    units: Sequence[str],
    strata: Sequence[str],
    *,
    seed: int,
    resamples: int,
) -> tuple[tuple[str, ...], ...]:
    """Resample whole clusters within fixed design strata."""
    if len(units) != len(strata) or not units:
        raise ValidationError("stratified bootstrap vectors must be row-aligned")
    unit_stratum: dict[str, str] = {}
    for unit, stratum in zip(units, strata, strict=True):
        previous = unit_stratum.setdefault(unit, stratum)
        if previous != stratum:
            raise ValidationError("a bootstrap cluster crosses design strata")
    strata_order = _ordered_unique(strata)
    by_stratum = {
        stratum: tuple(
            unit for unit in _ordered_unique(units) if unit_stratum[unit] == stratum
        )
        for stratum in strata_order
    }
    if any(not values for values in by_stratum.values()) or resamples <= 0:
        raise ValidationError("every bootstrap stratum must contain clusters")
    generator = random.Random(seed)
    return tuple(
        tuple(
            unit
            for stratum in strata_order
            for unit in (
                by_stratum[stratum][generator.randrange(len(by_stratum[stratum]))]
                for _ in by_stratum[stratum]
            )
        )
        for _ in range(resamples)
    )


def _indices_for_membership(
    units: Sequence[str], membership: Sequence[str]
) -> tuple[int, ...]:
    grouped: defaultdict[str, list[int]] = defaultdict(list)
    for index, unit in enumerate(units):
        grouped[unit].append(index)
    return tuple(index for unit in membership for index in grouped[unit])


def _percentile(values: Sequence[float], probability: float) -> float:
    if not values:
        raise ValidationError("cannot compute a percentile of no values")
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def _interval(values: Sequence[float]) -> Mapping[str, float]:
    return {
        "lower": _percentile(values, 0.025),
        "upper": _percentile(values, 0.975),
    }


def _align_models(
    baseline: Sequence[RawLogitRow], candidate: Sequence[RawLogitRow]
) -> tuple[tuple[RawLogitRow, ...], tuple[RawLogitRow, ...]]:
    _validate_rows(baseline)
    _validate_rows(candidate)
    baseline_by_id = {row.row_id: row for row in baseline}
    candidate_by_id = {row.row_id: row for row in candidate}
    if baseline_by_id.keys() != candidate_by_id.keys():
        raise ValidationError("paired models do not contain identical row IDs")
    ordered_ids = tuple(row.row_id for row in baseline)
    aligned_candidate = tuple(candidate_by_id[row_id] for row_id in ordered_ids)
    for left, right in zip(baseline, aligned_candidate, strict=True):
        identity = (
            left.fixture,
            left.split,
            left.stratum,
            left.page_id,
            left.case_id,
            left.claim_group_id,
            left.transition_id,
            left.claim_sha256,
            left.evidence_sha256,
            left.mapped_label,
            left.input_sha256,
        )
        other = (
            right.fixture,
            right.split,
            right.stratum,
            right.page_id,
            right.case_id,
            right.claim_group_id,
            right.transition_id,
            right.claim_sha256,
            right.evidence_sha256,
            right.mapped_label,
            right.input_sha256,
        )
        if identity != other:
            raise ValidationError(f"paired row identity drifted: {left.row_id}")
    return tuple(baseline), aligned_candidate


def paired_bootstrap_classification(
    baseline: Sequence[RawLogitRow],
    candidate: Sequence[RawLogitRow],
    *,
    units: Sequence[str],
    unit_name: str,
    seed: int,
    resamples: int,
    ece_bins: int = 10,
    strata: Sequence[str] | None = None,
) -> Mapping[str, object]:
    """Paired cluster bootstrap using one membership draw for both models."""
    left, right = _align_models(baseline, candidate)
    if len(units) != len(left):
        raise ValidationError("bootstrap unit vector is not row-aligned")
    memberships = (
        bootstrap_memberships(units, seed=seed, resamples=resamples)
        if strata is None
        else stratified_bootstrap_memberships(
            units, strata, seed=seed, resamples=resamples
        )
    )
    estimates: dict[str, dict[str, list[float]]] = {
        name: {"baseline": [], "candidate": [], "delta": []}
        for name in _CLASSIFICATION_SCALARS
    }
    for membership in memberships:
        indices = _indices_for_membership(units, membership)
        left_point = _classification_metrics(
            [left[index] for index in indices],
            ece_bins=ece_bins,
            require_unique_ids=False,
        )
        right_point = _classification_metrics(
            [right[index] for index in indices],
            ece_bins=ece_bins,
            require_unique_ids=False,
        )
        for name in _CLASSIFICATION_SCALARS:
            baseline_value = cast(float, left_point[name])
            candidate_value = cast(float, right_point[name])
            estimates[name]["baseline"].append(baseline_value)
            estimates[name]["candidate"].append(candidate_value)
            estimates[name]["delta"].append(candidate_value - baseline_value)
    return {
        "independent_unit": unit_name,
        "units": len(_ordered_unique(units)),
        "seed": seed,
        "resamples": resamples,
        "method": (
            "paired-cluster-percentile-bootstrap-v1"
            if strata is None
            else "paired-stratified-cluster-percentile-bootstrap-v1"
        ),
        "fixed_strata": (
            None
            if strata is None
            else {
                stratum: len(
                    {
                        unit
                        for unit, observed in zip(units, strata, strict=True)
                        if observed == stratum
                    }
                )
                for stratum in _ordered_unique(strata)
            }
        ),
        "level": 0.95,
        "intervals": {
            name: {surface: _interval(values) for surface, values in by_surface.items()}
            for name, by_surface in estimates.items()
        },
    }


def _group_by(
    rows: Sequence[_T], key: Callable[[_T], str]
) -> Mapping[str, tuple[_T, ...]]:
    grouped: defaultdict[str, list[_T]] = defaultdict(list)
    for row in rows:
        grouped[key(row)].append(row)
    return {name: tuple(values) for name, values in grouped.items()}


def _transition_records(
    rows: Sequence[RawLogitRow],
) -> tuple[Mapping[str, object], ...]:
    _validate_vitaminc_atomic_cases(rows)
    by_transition = _group_by(
        rows,
        lambda row: row.transition_id or _derived_transition_id(row),
    )
    records: list[Mapping[str, object]] = []
    for transition_id, pair in by_transition.items():
        if len(pair) != 2:
            raise ValidationError(
                f"VitaminC transition {transition_id} must contain two endpoints"
            )
        left, right = pair
        if any(
            value is None
            for value in (
                left.page_id,
                right.page_id,
                left.case_id,
                right.case_id,
                left.stratum,
                right.stratum,
            )
        ):
            raise ValidationError("VitaminC transition metadata must be complete")
        if (
            left.page_id != right.page_id
            or left.case_id != right.case_id
            or left.stratum != right.stratum
        ):
            raise ValidationError(
                f"VitaminC transition metadata differs: {transition_id}"
            )
        if any(
            (row.transition_id or _derived_transition_id(row))
            != _derived_transition_id(row)
            for row in pair
        ):
            raise ValidationError(
                f"VitaminC transition does not match case/suffix: {transition_id}"
            )
        left_label = _label_index(left.mapped_label)
        right_label = _label_index(right.mapped_label)
        if left_label == right_label:
            raise ValidationError(f"transition {transition_id} has no label change")
        left_probabilities = left.uncalibrated_probabilities
        right_probabilities = right.uncalibrated_probabilities
        left_prediction = _argmax(left_probabilities)
        right_prediction = _argmax(right_probabilities)
        records.append(
            {
                "transition_id": transition_id,
                "page_id": cast(str, left.page_id),
                "case_id": cast(str, left.case_id),
                "stratum": left.stratum,
                "flip_detected": float(left_prediction != right_prediction),
                "joint_correct": float(
                    left_prediction == left_label and right_prediction == right_label
                ),
                "bidirectional_margin": float(
                    left_probabilities[left_label] > right_probabilities[left_label]
                    and right_probabilities[right_label]
                    > left_probabilities[right_label]
                ),
            }
        )
    return tuple(records)


def _derived_transition_id(row: RawLogitRow) -> str:
    if row.case_id is None:
        raise ValidationError("VitaminC transition derivation requires case_id")
    try:
        suffix = int(row.row_id.rsplit("_", 1)[1])
    except (IndexError, ValueError) as error:
        raise ValidationError(
            "VitaminC rows need transition_id or suffix 1..4"
        ) from error
    if suffix not in {1, 2, 3, 4}:
        raise ValidationError("VitaminC row suffix must be one of 1,2,3,4")
    return f"{row.case_id}:transition-{1 if suffix <= 2 else 2}"


def _mean_records(
    records: Sequence[Mapping[str, object]], names: Sequence[str]
) -> Mapping[str, float]:
    if not records:
        raise ValidationError("record summary requires at least one row")
    return {
        name: fmean(cast(float, record[name]) for record in records) for name in names
    }


def _case_records(rows: Sequence[RawLogitRow]) -> tuple[Mapping[str, object], ...]:
    _validate_vitaminc_atomic_cases(rows)
    by_case = _group_by(
        rows,
        lambda row: row.case_id if row.case_id is not None else cast(str, row.case_id),
    )
    result: list[Mapping[str, object]] = []
    for case_id, members in by_case.items():
        if case_id is None or len(members) != 4:
            raise ValidationError("VitaminC cases must contain exactly four endpoints")
        if (
            len({row.page_id for row in members}) != 1
            or None in {row.page_id for row in members}
            or len({row.stratum for row in members}) != 1
            or None in {row.stratum for row in members}
            or {row.transition_id or _derived_transition_id(row) for row in members}
            != {f"{case_id}:transition-1", f"{case_id}:transition-2"}
        ):
            raise ValidationError("VitaminC case metadata or transition layout drifted")
        result.append(
            {
                "case_id": case_id,
                "page_id": members[0].page_id,
                "stratum": members[0].stratum,
                "case_complete": float(
                    all(
                        _argmax(row.calibrated_probabilities)
                        == _label_index(row.mapped_label)
                        for row in members
                    )
                ),
            }
        )
    return tuple(result)


def _validate_vitaminc_atomic_cases(rows: Sequence[RawLogitRow]) -> None:
    _validate_rows(rows)
    if any(row.case_id is None or row.page_id is None for row in rows):
        raise ValidationError("VitaminC rows require page and case identities")
    grouped = _group_by(rows, lambda row: cast(str, row.case_id))
    for case_id, members in grouped.items():
        if len(members) != 4:
            raise ValidationError("VitaminC cases must contain exactly four endpoints")
        try:
            by_suffix = {int(row.row_id.rsplit("_", 1)[1]): row for row in members}
        except (IndexError, ValueError) as error:
            raise ValidationError("VitaminC case row suffix is invalid") from error
        if set(by_suffix) != {1, 2, 3, 4}:
            raise ValidationError("VitaminC case suffixes must be exactly 1..4")
        ordered = tuple(by_suffix[index] for index in (1, 2, 3, 4))
        if any(
            row.row_id != f"{case_id}_{index}" for index, row in enumerate(ordered, 1)
        ):
            raise ValidationError("VitaminC row ID does not belong to its case")
        if (
            len({row.page_id for row in ordered}) != 1
            or len({row.stratum for row in ordered}) != 1
            or ordered[0].claim_sha256 != ordered[1].claim_sha256
            or ordered[2].claim_sha256 != ordered[3].claim_sha256
            or ordered[0].claim_sha256 == ordered[2].claim_sha256
            or ordered[0].evidence_sha256 != ordered[2].evidence_sha256
            or ordered[1].evidence_sha256 != ordered[3].evidence_sha256
            or ordered[0].evidence_sha256 == ordered[1].evidence_sha256
        ):
            raise ValidationError("VitaminC atomic case content semantics drifted")
        stratum = ordered[0].stratum
        if stratum is None:
            raise ValidationError("VitaminC case stratum is missing")
        expected_alternative = {
            "support_refute": "refute",
            "support_neutral": "neutral",
        }.get(stratum)
        if expected_alternative is None:
            raise ValidationError("VitaminC case has an unsupported stratum")
        expected_pair = {"support", expected_alternative}
        if {ordered[0].mapped_label, ordered[1].mapped_label} != expected_pair or {
            ordered[2].mapped_label,
            ordered[3].mapped_label,
        } != expected_pair:
            raise ValidationError("VitaminC case labels contradict its stratum")
        expected_transitions = (
            f"{case_id}:transition-1",
            f"{case_id}:transition-1",
            f"{case_id}:transition-2",
            f"{case_id}:transition-2",
        )
        if tuple(row.transition_id for row in ordered) != expected_transitions:
            raise ValidationError("VitaminC case transition identities drifted")


def _vitaminc_points(rows: Sequence[RawLogitRow]) -> Mapping[str, object]:
    transitions = _transition_records(rows)
    cases = _case_records(rows)
    temperatures = {row.temperature for row in rows}
    if len(temperatures) != 1:
        raise ValidationError("VitaminC points require one deployed temperature")
    temperature = next(iter(temperatures))
    return {
        "endpoint": classification_metrics(rows),
        "transition": {
            "transitions": len(transitions),
            "probability_surface": "uncalibrated_T1",
            "point": _mean_records(transitions, _TRANSITION_SCALARS),
            "by_stratum": {
                stratum: _mean_records(
                    [record for record in transitions if record["stratum"] == stratum],
                    _TRANSITION_SCALARS,
                )
                for stratum in ("support_refute", "support_neutral")
            },
        },
        "case": {
            "cases": len(cases),
            "probability_surface": "deployed_temperature",
            "temperature": temperature,
            "case_complete": fmean(
                cast(float, record["case_complete"]) for record in cases
            ),
            "by_stratum": {
                stratum: fmean(
                    cast(float, record["case_complete"])
                    for record in cases
                    if record["stratum"] == stratum
                )
                for stratum in ("support_refute", "support_neutral")
            },
        },
    }


def vitaminc_point_metrics(
    rows: Sequence[RawLogitRow],
) -> Mapping[str, object]:
    """Public point-metric surface used before terminal bootstrap unlock."""
    return _vitaminc_points(rows)


def _paired_transition_bootstrap(
    baseline: Sequence[RawLogitRow],
    candidate: Sequence[RawLogitRow],
    *,
    unit_kind: str,
    seed: int,
    resamples: int,
    stratified: bool = False,
) -> Mapping[str, object]:
    left, right = _align_models(baseline, candidate)
    left_records = _transition_records(left)
    right_records = _transition_records(right)
    right_by_id = {str(row["transition_id"]): row for row in right_records}
    if {str(row["transition_id"]) for row in left_records} != right_by_id.keys():
        raise ValidationError("paired transition identities differ")
    aligned_right = tuple(
        right_by_id[str(row["transition_id"])] for row in left_records
    )
    unit_field = "page_id" if unit_kind == "Wikipedia page" else "case_id"
    units = tuple(str(row[unit_field]) for row in left_records)
    strata = tuple(str(row["stratum"]) for row in left_records)
    memberships = (
        stratified_bootstrap_memberships(units, strata, seed=seed, resamples=resamples)
        if stratified
        else bootstrap_memberships(units, seed=seed, resamples=resamples)
    )
    estimates: dict[str, list[float]] = {
        f"{name}.{surface}": []
        for name in _TRANSITION_SCALARS
        for surface in ("baseline", "candidate", "delta")
    }
    for membership in memberships:
        indices = _indices_for_membership(units, membership)
        for name in _TRANSITION_SCALARS:
            baseline_value = fmean(
                cast(float, left_records[index][name]) for index in indices
            )
            candidate_value = fmean(
                cast(float, aligned_right[index][name]) for index in indices
            )
            estimates[f"{name}.baseline"].append(baseline_value)
            estimates[f"{name}.candidate"].append(candidate_value)
            estimates[f"{name}.delta"].append(candidate_value - baseline_value)
    return {
        "independent_unit": unit_kind,
        "units": len(_ordered_unique(units)),
        "seed": seed,
        "resamples": resamples,
        "method": (
            "paired-stratified-cluster-percentile-bootstrap-v1"
            if stratified
            else "paired-cluster-percentile-bootstrap-v1"
        ),
        "fixed_strata": (
            {
                stratum: len(
                    {
                        unit
                        for unit, observed in zip(units, strata, strict=True)
                        if observed == stratum
                    }
                )
                for stratum in _ordered_unique(strata)
            }
            if stratified
            else None
        ),
        "level": 0.95,
        "intervals": {
            name: {
                surface: _interval(estimates[f"{name}.{surface}"])
                for surface in ("baseline", "candidate", "delta")
            }
            for name in _TRANSITION_SCALARS
        },
    }


def _paired_case_bootstrap(
    baseline: Sequence[RawLogitRow],
    candidate: Sequence[RawLogitRow],
    *,
    unit_kind: str,
    seed: int,
    resamples: int,
    stratified: bool,
) -> Mapping[str, object]:
    left, right = _align_models(baseline, candidate)
    left_records = _case_records(left)
    right_by_id = {str(row["case_id"]): row for row in _case_records(right)}
    if {str(row["case_id"]) for row in left_records} != right_by_id.keys():
        raise ValidationError("paired case identities differ")
    unit_field = "page_id" if unit_kind == "Wikipedia page" else "case_id"
    units = tuple(str(row[unit_field]) for row in left_records)
    strata = tuple(str(row["stratum"]) for row in left_records)
    memberships = (
        stratified_bootstrap_memberships(units, strata, seed=seed, resamples=resamples)
        if stratified
        else bootstrap_memberships(units, seed=seed, resamples=resamples)
    )
    estimates: dict[str, list[float]] = {
        surface: [] for surface in ("baseline", "candidate", "delta")
    }
    for membership in memberships:
        indices = _indices_for_membership(units, membership)
        baseline_value = fmean(
            cast(float, left_records[index]["case_complete"]) for index in indices
        )
        candidate_value = fmean(
            cast(
                float,
                right_by_id[str(left_records[index]["case_id"])]["case_complete"],
            )
            for index in indices
        )
        estimates["baseline"].append(baseline_value)
        estimates["candidate"].append(candidate_value)
        estimates["delta"].append(candidate_value - baseline_value)
    return {
        "independent_unit": unit_kind,
        "units": len(_ordered_unique(units)),
        "seed": seed,
        "resamples": resamples,
        "method": (
            "paired-stratified-cluster-percentile-bootstrap-v1"
            if stratified
            else "paired-cluster-percentile-bootstrap-v1"
        ),
        "fixed_strata": (
            {
                stratum: len(
                    {
                        unit
                        for unit, observed in zip(units, strata, strict=True)
                        if observed == stratum
                    }
                )
                for stratum in _ordered_unique(strata)
            }
            if stratified
            else None
        ),
        "level": 0.95,
        "intervals": {
            "case_complete": {
                surface: _interval(values) for surface, values in estimates.items()
            }
        },
    }


def paired_bootstrap_vitaminc(
    baseline: Sequence[RawLogitRow],
    candidate: Sequence[RawLogitRow],
    *,
    seed: int,
    resamples: int,
) -> Mapping[str, object]:
    left, right = _align_models(baseline, candidate)
    pages = tuple(cast(str, row.page_id) for row in left)
    cases = tuple(cast(str, row.case_id) for row in left)
    strata = tuple(cast(str, row.stratum) for row in left)
    return {
        "endpoint_by_page": paired_bootstrap_classification(
            left,
            right,
            units=pages,
            unit_name="Wikipedia page",
            seed=seed,
            resamples=resamples,
            strata=strata,
        ),
        "endpoint_by_case": paired_bootstrap_classification(
            left,
            right,
            units=cases,
            unit_name="VitaminC case_id",
            seed=seed,
            resamples=resamples,
            strata=strata,
        ),
        "transition_by_page": _paired_transition_bootstrap(
            left,
            right,
            unit_kind="Wikipedia page",
            seed=seed,
            resamples=resamples,
            stratified=True,
        ),
        "transition_by_case": _paired_transition_bootstrap(
            left,
            right,
            unit_kind="VitaminC case_id",
            seed=seed,
            resamples=resamples,
            stratified=True,
        ),
        "case_complete_by_page": _paired_case_bootstrap(
            left,
            right,
            unit_kind="Wikipedia page",
            seed=seed,
            resamples=resamples,
            stratified=True,
        ),
        "case_complete_by_case": _paired_case_bootstrap(
            left,
            right,
            unit_kind="VitaminC case_id",
            seed=seed,
            resamples=resamples,
            stratified=True,
        ),
        "pooled_sensitivity": {
            "endpoint_by_page": paired_bootstrap_classification(
                left,
                right,
                units=pages,
                unit_name="Wikipedia page",
                seed=seed,
                resamples=resamples,
            ),
            "endpoint_by_case": paired_bootstrap_classification(
                left,
                right,
                units=cases,
                unit_name="VitaminC case_id",
                seed=seed,
                resamples=resamples,
            ),
            "transition_by_page": _paired_transition_bootstrap(
                left,
                right,
                unit_kind="Wikipedia page",
                seed=seed,
                resamples=resamples,
            ),
            "transition_by_case": _paired_transition_bootstrap(
                left,
                right,
                unit_kind="VitaminC case_id",
                seed=seed,
                resamples=resamples,
            ),
            "case_complete_by_page": _paired_case_bootstrap(
                left,
                right,
                unit_kind="Wikipedia page",
                seed=seed,
                resamples=resamples,
                stratified=False,
            ),
            "case_complete_by_case": _paired_case_bootstrap(
                left,
                right,
                unit_kind="VitaminC case_id",
                seed=seed,
                resamples=resamples,
                stratified=False,
            ),
        },
    }


def evaluate_vitaminc(
    baseline: Sequence[RawLogitRow],
    candidate: Sequence[RawLogitRow],
    *,
    seed: int,
    resamples: int,
) -> Mapping[str, object]:
    left, right = _align_models(baseline, candidate)
    return {
        "baseline": _vitaminc_points(left),
        "candidate": _vitaminc_points(right),
        "endpoint_calibration_ablation": {
            "baseline": classification_calibration_ablation(left),
            "candidate": classification_calibration_ablation(right),
        },
        "paired_bootstrap": paired_bootstrap_vitaminc(
            left, right, seed=seed, resamples=resamples
        ),
    }


def evaluate_m3(
    baseline: Sequence[RawLogitRow],
    candidate: Sequence[RawLogitRow],
    *,
    seed: int,
    resamples: int,
) -> Mapping[str, object]:
    left, right = _align_models(baseline, candidate)
    if any(row.claim_group_id is None for row in left):
        raise ValidationError("M3 rows require claim_group_id")
    groups = tuple(cast(str, row.claim_group_id) for row in left)
    return {
        "baseline": classification_metrics(left),
        "candidate": classification_metrics(right),
        "calibration_ablation": {
            "baseline": classification_calibration_ablation(left),
            "candidate": classification_calibration_ablation(right),
        },
        "paired_bootstrap": paired_bootstrap_classification(
            left,
            right,
            units=groups,
            unit_name="M3 claim_group_id",
            seed=seed,
            resamples=resamples,
        ),
        "limitation": (
            "The original M3 public test has no REFUTE examples; REFUTE recall and "
            "F1 are null and this surface cannot detect REFUTE forgetting."
        ),
    }


def summarize_training_seeds(
    values_by_seed: Mapping[int, Mapping[str, float]],
) -> Mapping[str, object]:
    """Summarize replication sensitivity without inventing a seed-level CI."""
    if not values_by_seed:
        raise ValidationError("seed summary requires at least one seed")
    names = tuple(next(iter(values_by_seed.values())))
    if any(tuple(values) != names for values in values_by_seed.values()):
        raise ValidationError("seed metric surfaces are not aligned")
    return {
        "seeds": {
            str(seed): dict(metrics) for seed, metrics in sorted(values_by_seed.items())
        },
        "arithmetic_mean": {
            name: fmean(metrics[name] for metrics in values_by_seed.values())
            for name in names
        },
        "standard_deviation": {
            name: stdev(metrics[name] for metrics in values_by_seed.values())
            for name in names
        },
        "standard_deviation_semantics": (
            "sample standard deviation over the three frozen seeds; denominator n-1"
        ),
        "confidence_interval": None,
        "confidence_interval_reason": (
            "Three training seeds describe replication sensitivity and do not "
            "justify a confidence interval over training randomness."
        ),
    }
