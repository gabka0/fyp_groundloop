from __future__ import annotations

from dataclasses import replace

import pytest

from experiments.m4_change_aware_verifier.metrics import (
    _indices_for_membership,
    bootstrap_memberships,
    classification_calibration_ablation,
    classification_metrics,
    evaluate_m3,
    evaluate_vitaminc,
    paired_bootstrap_classification,
    paired_bootstrap_vitaminc,
    stratified_bootstrap_memberships,
    summarize_training_seeds,
    vitaminc_point_metrics,
)
from experiments.m4_change_aware_verifier.scoring import (
    operational_label,
    stored_probabilities,
)
from experiments.m4_change_aware_verifier.terminal import (
    _flatten_classification_points,
    _flatten_seed_points,
)
from experiments.m4_change_aware_verifier.verdict import SeedGateInput, evaluate_stop_go
from groundloop.errors import ValidationError

from .helpers import EvaluationRow, evaluation_rows, raw_row


def test_absent_class_recall_and_f1_are_null() -> None:
    sources = (
        EvaluationRow("m3", "test", "a", "c1", "e1", "support"),
        EvaluationRow("m3", "test", "b", "c2", "e2", "neutral"),
    )
    metrics = classification_metrics(
        tuple(raw_row(row, model_key="model", correct=True) for row in sources)
    )
    refute = metrics["per_class"]["refute"]
    assert refute["support"] == 0
    assert refute["recall"] is None
    assert refute["f1"] is None


def test_bootstrap_memberships_resample_whole_clusters() -> None:
    memberships = bootstrap_memberships(
        ("page-a", "page-a", "page-b", "page-b"),
        seed=7,
        resamples=20,
    )
    assert len(memberships) == 20
    assert all(len(draw) == 2 for draw in memberships)
    assert all(set(draw) <= {"page-a", "page-b"} for draw in memberships)


def test_bootstrap_membership_vector_is_exact_and_preserves_unequal_clusters() -> None:
    units = ("a", "a", "b", "c", "c", "c")
    assert bootstrap_memberships(units, seed=7, resamples=4) == (
        ("b", "a", "b"),
        ("c", "a", "a"),
        ("c", "a", "b"),
        ("c", "a", "c"),
    )
    assert _indices_for_membership(units, ("b", "a", "b")) == (2, 0, 1, 2)
    assert _indices_for_membership(units, ("c", "a", "a")) == (
        3,
        4,
        5,
        0,
        1,
        0,
        1,
    )
    assert stratified_bootstrap_memberships(
        units,
        ("x", "x", "x", "y", "y", "y"),
        seed=7,
        resamples=2,
    ) == (("b", "a", "c"), ("a", "a", "c"))


def test_paired_bootstrap_uses_identical_draws_for_delta() -> None:
    sources = tuple(
        EvaluationRow(
            "m3",
            "test",
            f"row-{index}",
            f"claim-{index}",
            f"evidence-{index}",
            "support",
            claim_group_id=f"group-{index // 2}",
        )
        for index in range(4)
    )
    baseline = tuple(raw_row(row, model_key="base", correct=False) for row in sources)
    candidate = tuple(
        raw_row(row, model_key="candidate", correct=True) for row in sources
    )
    report = paired_bootstrap_classification(
        baseline,
        candidate,
        units=tuple(str(row.claim_group_id) for row in sources),
        unit_name="claim group",
        seed=9,
        resamples=50,
    )
    accuracy_delta = report["intervals"]["accuracy"]["delta"]
    assert accuracy_delta == {"lower": 1.0, "upper": 1.0}


def test_page_and_case_bootstraps_coincide_for_one_case_per_page() -> None:
    sources: list[EvaluationRow] = []
    for case_index in range(4):
        alternative = "refute" if case_index < 2 else "neutral"
        stratum = "support_refute" if case_index < 2 else "support_neutral"
        labels = ("support", alternative, alternative, "support")
        for suffix, label in enumerate(labels, 1):
            sources.append(
                EvaluationRow(
                    "vitaminc_terminal_reserve",
                    "terminal",
                    f"case-{case_index}_{suffix}",
                    f"claim-{case_index}-{suffix <= 2}",
                    f"evidence-{case_index}-{suffix in {1, 3}}",
                    label,
                    page_id=f"page-{case_index}",
                    case_id=f"case-{case_index}",
                    transition_id=(
                        f"case-{case_index}:transition-{1 if suffix <= 2 else 2}"
                    ),
                    stratum=stratum,
                )
            )
    baseline = tuple(raw_row(row, model_key="base", correct=False) for row in sources)
    candidate = tuple(
        raw_row(row, model_key="candidate", correct=True) for row in sources
    )
    report = paired_bootstrap_vitaminc(baseline, candidate, seed=11, resamples=100)
    assert (
        report["endpoint_by_page"]["intervals"]
        == (report["endpoint_by_case"]["intervals"])
    )
    assert (
        report["transition_by_page"]["intervals"]
        == (report["transition_by_case"]["intervals"])
    )
    assert (
        report["case_complete_by_page"]["intervals"]
        == (report["case_complete_by_case"]["intervals"])
    )
    assert report["transition_by_page"]["method"] == (
        "paired-stratified-cluster-percentile-bootstrap-v1"
    )
    assert report["transition_by_page"]["fixed_strata"] == {
        "support_refute": 2,
        "support_neutral": 2,
    }
    assert report["pooled_sensitivity"]["transition_by_page"]["method"] == (
        "paired-cluster-percentile-bootstrap-v1"
    )


def test_transition_rejects_cross_case_or_page_metadata_aliasing() -> None:
    sources = tuple(
        EvaluationRow(
            "vitaminc_terminal_reserve",
            "terminal",
            f"case-{case}_{suffix}",
            f"claim-{case}-{suffix <= 2}",
            f"evidence-{case}-{suffix in {1, 3}}",
            label,
            page_id=f"page-{case}",
            case_id=f"case-{case}",
            transition_id=f"case-{case}:transition-{1 if suffix <= 2 else 2}",
            stratum=stratum,
        )
        for case, stratum, alternative in (
            ("a", "support_refute", "refute"),
            ("b", "support_neutral", "neutral"),
        )
        for suffix, label in enumerate(
            ("support", alternative, alternative, "support"), 1
        )
    )
    baseline = tuple(raw_row(row, model_key="base", correct=False) for row in sources)
    candidate = list(
        raw_row(row, model_key="candidate", correct=True) for row in sources
    )
    candidate[1] = replace(candidate[1], page_id="page-b")
    with pytest.raises(ValidationError, match="identity drifted"):
        paired_bootstrap_vitaminc(baseline, tuple(candidate), seed=11, resamples=10)


def test_transition_rejects_suffix_alias_under_wrong_transition_id() -> None:
    sources = tuple(
        EvaluationRow(
            "vitaminc_terminal_reserve",
            "terminal",
            f"case-a_{suffix}",
            f"claim-{suffix <= 2}",
            f"evidence-{suffix in {1, 3}}",
            label,
            page_id="page-a",
            case_id="case-a",
            transition_id="case-a:transition-1",
            stratum="support_refute",
        )
        for suffix, label in enumerate(("support", "refute", "refute", "support"), 1)
    )
    rows = tuple(raw_row(row, model_key="candidate", correct=True) for row in sources)
    with pytest.raises(ValidationError, match="transition identities drifted"):
        paired_bootstrap_vitaminc(rows, rows, seed=11, resamples=10)


def test_seed_summary_has_no_fake_confidence_interval() -> None:
    summary = summarize_training_seeds(
        {1: {"joint": 0.2}, 2: {"joint": 0.3}, 3: {"joint": 0.4}}
    )
    assert summary["arithmetic_mean"]["joint"] == 0.3
    assert summary["standard_deviation"]["joint"] == pytest.approx(0.1)
    assert summary["confidence_interval"] is None


def test_base_logits_map_to_stored_probabilities_in_the_frozen_order() -> None:
    probabilities = stored_probabilities((0.1, 2.3, -1.7), temperature=1.0)
    assert probabilities[0] > probabilities[1] > probabilities[2]
    assert probabilities == pytest.approx((0.8856464018, 0.0981324185, 0.0162211797))


def test_calibration_surfaces_recompute_policy_threshold_crossing() -> None:
    source = EvaluationRow(
        "m3",
        "development",
        "threshold",
        "claim",
        "evidence",
        "support",
        claim_group_id="threshold",
    )
    logits = (0.0, 2.2, 0.0)
    t1 = stored_probabilities(logits, temperature=1.0)
    old = stored_probabilities(logits, temperature=1.1037657679769346)
    deployed_temperature = 2.0
    deployed = stored_probabilities(logits, temperature=deployed_temperature)
    row = replace(
        raw_row(source, model_key="candidate", correct=True),
        base_logits=logits,
        uncalibrated_probabilities=t1,
        old_m3_temperature_probabilities=old,
        calibrated_probabilities=deployed,
        operational_label=operational_label(deployed),
        temperature=deployed_temperature,
    )
    assert operational_label(t1) == "support"
    assert operational_label(old) == "neutral"
    report = classification_calibration_ablation((row,))
    assert {
        key: (value["probability_surface"], value["temperature"])
        for key, value in report.items()
    } == {
        "uncalibrated_T1": ("uncalibrated_T1", 1.0),
        "old_m3_temperature": (
            "old_m3_temperature",
            1.1037657679769346,
        ),
        "deployed_temperature": ("deployed_temperature", 2.0),
    }
    assert report["uncalibrated_T1"]["operational_label_counts"]["support"] == 1
    assert report["old_m3_temperature"]["operational_label_counts"]["neutral"] == 1
    assert report["deployed_temperature"]["operational_label_counts"]["neutral"] == 1


def test_seed_flattening_omits_absent_class_even_when_predicted() -> None:
    sources = (
        EvaluationRow("m3", "test", "a", "c1", "e1", "support"),
        EvaluationRow("m3", "test", "b", "c2", "e2", "neutral"),
    )
    rows = tuple(raw_row(row, model_key="candidate", correct=False) for row in sources)
    flattened = _flatten_classification_points(
        "m3.endpoint", classification_metrics(rows)
    )
    assert not any(".refute." in key for key in flattened)


def test_vitaminc_reverse_direction_transitions_are_valid() -> None:
    sources = tuple(
        EvaluationRow(
            "vitaminc_terminal_reserve",
            "terminal",
            f"case-{case}_{suffix}",
            f"claim-{case}-{suffix <= 2}",
            f"evidence-{case}-{suffix in {1, 3}}",
            label,
            page_id=f"page-{case}",
            case_id=f"case-{case}",
            transition_id=f"case-{case}:transition-{1 if suffix <= 2 else 2}",
            stratum=stratum,
        )
        for case, stratum, labels in (
            ("r", "support_refute", ("refute", "support", "support", "refute")),
            ("n", "support_neutral", ("neutral", "support", "support", "neutral")),
        )
        for suffix, label in enumerate(labels, 1)
    )
    report = vitaminc_point_metrics(
        tuple(raw_row(row, model_key="candidate", correct=True) for row in sources)
    )
    transition = report["transition"]
    assert transition["point"] == {
        "flip_detected": 1.0,
        "joint_correct": 1.0,
        "bidirectional_margin": 1.0,
    }
    assert report["case"]["probability_surface"] == "deployed_temperature"
    assert report["case"]["temperature"] == 1.0


def test_three_seed_scalar_surface_is_exact() -> None:
    vitamin, m3_rows, _git = evaluation_rows()
    base_vitamin = tuple(
        raw_row(row, model_key="base", correct=False) for row in vitamin
    )
    candidate_vitamin = tuple(
        raw_row(row, model_key="candidate", correct=True) for row in vitamin
    )
    base_m3 = tuple(raw_row(row, model_key="base", correct=False) for row in m3_rows)
    candidate_m3 = tuple(
        raw_row(row, model_key="candidate", correct=True) for row in m3_rows
    )
    vitamin_report = evaluate_vitaminc(
        base_vitamin, candidate_vitamin, seed=20260720, resamples=1
    )
    m3_report = evaluate_m3(base_m3, candidate_m3, seed=20260720, resamples=1)
    points = _flatten_seed_points(
        vitamin_candidate=vitamin_report["candidate"],
        m3_candidate=m3_report["candidate"],
        vitamin_calibration=vitamin_report["endpoint_calibration_ablation"][
            "candidate"
        ],
        m3_calibration=m3_report["calibration_ablation"]["candidate"],
    )
    scalar_names = {"accuracy", "macro_f1", "nll", "multiclass_brier", "ece"}
    expected = {
        *(f"vitaminc.endpoint.{name}" for name in scalar_names),
        *(f"m3.endpoint.{name}" for name in scalar_names),
        *(
            f"vitaminc.endpoint.{label}.{name}"
            for label in ("support", "refute", "neutral")
            for name in ("precision", "recall", "f1")
        ),
        *(
            f"m3.endpoint.{label}.{name}"
            for label in ("support", "neutral")
            for name in ("precision", "recall", "f1")
        ),
        *(
            f"vitaminc.transition.{stratum}.{name}"
            for stratum in ("overall", "support_refute", "support_neutral")
            for name in ("flip_detected", "joint_correct", "bidirectional_margin")
        ),
        *(
            f"vitaminc.case.{stratum}.case_complete"
            for stratum in ("overall", "support_refute", "support_neutral")
        ),
        *(
            f"{domain}.calibration.{surface}.{name}"
            for domain in ("vitaminc", "m3")
            for surface in (
                "uncalibrated_T1",
                "old_m3_temperature",
                "deployed_temperature",
            )
            for name in (
                "nll",
                "operational_support_proportion",
                "operational_refute_proportion",
                "operational_neutral_proportion",
            )
        ),
    }
    assert set(points) == expected
    assert not any(
        fragment in key
        for key in points
        for fragment in ("count", "confusion", "interval", "bootstrap")
    )


def test_transition_surface_is_uncalibrated_and_temperature_invariant() -> None:
    sources = tuple(
        EvaluationRow(
            "vitaminc_terminal_reserve",
            "terminal",
            f"case-{case}_{suffix}",
            f"claim-{case}-{suffix <= 2}",
            f"evidence-{case}-{suffix in {1, 3}}",
            label,
            page_id=f"page-{case}",
            case_id=f"case-{case}",
            transition_id=f"case-{case}:transition-{1 if suffix <= 2 else 2}",
            stratum=stratum,
        )
        for case, stratum, alternative in (
            ("a", "support_refute", "refute"),
            ("b", "support_neutral", "neutral"),
        )
        for suffix, label in enumerate(
            ("support", alternative, alternative, "support"), 1
        )
    )
    rows = tuple(raw_row(row, model_key="candidate", correct=True) for row in sources)
    recalibrated = tuple(
        replace(
            row,
            calibrated_probabilities=(0.01, 0.01, 0.98),
            temperature=20.0,
        )
        for row in rows
    )
    original = vitaminc_point_metrics(rows)["transition"]
    changed = vitaminc_point_metrics(recalibrated)["transition"]
    assert original == changed
    assert original["probability_surface"] == "uncalibrated_T1"


def test_inconclusive_verdict_requires_all_nonstatistical_safety_gates() -> None:
    def gate(seed: int, *, calibration_valid: bool = True) -> SeedGateInput:
        return SeedGateInput(
            seed=seed,
            reserve_joint_correct=0.40,
            reserve_flip_detected=0.60,
            reserve_bidirectional_margin=0.80,
            joint_delta_lower=-0.01,
            joint_delta_upper=0.10,
            joint_delta_point=0.12,
            flip_delta_lower=-0.01,
            flip_delta_upper=0.10,
            margin_delta_lower=0.0,
            m3_macro_f1=0.60,
            m3_accuracy=0.70,
            m3_macro_f1_delta_lower=0.0,
            m3_accuracy_delta_lower=0.0,
            calibration_valid=calibration_valid,
            complete=True,
        )

    inputs = {seed: gate(seed) for seed in (20260720, 20260721, 20260722)}
    assert evaluate_stop_go(inputs, provenance_complete=True)["verdict"] == (
        "ENGINEERING_POSITIVE_STATISTICALLY_INCONCLUSIVE"
    )
    go_inputs = {
        seed: replace(
            gate(seed),
            joint_delta_lower=0.01,
            flip_delta_lower=0.01,
        )
        for seed in (20260720, 20260721, 20260722)
    }
    assert evaluate_stop_go(go_inputs, provenance_complete=True)["verdict"] == "GO"
    below_point = dict(go_inputs)
    below_point[20260720] = replace(below_point[20260720], joint_delta_point=0.099)
    assert evaluate_stop_go(below_point, provenance_complete=True)["verdict"] == "NO_GO"
    one_seed = {
        seed: (
            values if seed == 20260720 else replace(values, reserve_joint_correct=0.0)
        )
        for seed, values in go_inputs.items()
    }
    assert evaluate_stop_go(one_seed, provenance_complete=True)["verdict"] == "NO_GO"
    inputs[20260720] = gate(20260720, calibration_valid=False)
    result = evaluate_stop_go(inputs, provenance_complete=True)
    assert result["verdict"] == "NO_GO"
    assert "G9" in result["failed_clauses"]
    negative_interval = {seed: gate(seed) for seed in (20260720, 20260721, 20260722)}
    negative_interval[20260720] = replace(
        negative_interval[20260720],
        joint_delta_upper=-0.001,
    )
    assert (
        evaluate_stop_go(negative_interval, provenance_complete=True)["verdict"]
        == "NO_GO"
    )
