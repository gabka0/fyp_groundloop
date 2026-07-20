from __future__ import annotations

import json
import math
from pathlib import Path

from groundloop.ai.verification.constants import Label
from groundloop.m4.public_ai_gate import (
    GateConfig,
    SelectedCase,
    VitaminCRow,
    classification_metrics,
    exact_normalized_overlap_counts,
    page_bootstrap_classification,
    transition_metrics,
)


def _row(
    suffix: int,
    *,
    claim: str,
    evidence: str,
    label: str,
    case_id: str = "case-1",
    page: str = "Page One",
) -> VitaminCRow:
    return VitaminCRow(
        unique_id=f"{case_id}_{suffix}",
        case_id=case_id,
        wiki_revision_id="123",
        source_label=label,
        claim=claim,
        evidence=evidence,
        page=page,
        revision_type="real",
    )


def test_three_class_metrics_and_absent_class_semantics() -> None:
    probabilities = ((0.8, 0.1, 0.1), (0.1, 0.8, 0.1), (0.1, 0.1, 0.8))
    labels = (Label.SUPPORT, Label.REFUTE, Label.NEUTRAL)
    metrics = classification_metrics(probabilities, labels, ece_bins=5)
    assert metrics["accuracy"] == 1.0
    assert metrics["macro_f1"] == 1.0
    assert metrics["confusion_matrix"] == [[1, 0, 0], [0, 1, 0], [0, 0, 1]]
    assert math.isclose(float(metrics["multiclass_brier"]), 0.06)
    assert math.isclose(float(metrics["nll"]), -math.log(0.8))
    assert math.isclose(float(metrics["ece"]), 0.2)

    no_refute = classification_metrics(
        (probabilities[0], probabilities[2]),
        (Label.SUPPORT, Label.NEUTRAL),
        ece_bins=5,
    )
    refute = list(no_refute["per_class"])[1]
    assert refute["support"] == 0
    assert refute["recall"] is None
    assert refute["f1"] is None


def test_page_cluster_bootstrap_is_deterministic_and_keeps_units() -> None:
    probabilities = (
        (0.8, 0.1, 0.1),
        (0.1, 0.8, 0.1),
        (0.1, 0.1, 0.8),
        (0.7, 0.2, 0.1),
    )
    labels = (Label.SUPPORT, Label.REFUTE, Label.NEUTRAL, Label.SUPPORT)
    pages = ("page-a", "page-a", "page-b", "page-b")
    first = page_bootstrap_classification(
        probabilities,
        labels,
        pages,
        ece_bins=5,
        seed=17,
        resamples=40,
    )
    second = page_bootstrap_classification(
        probabilities,
        labels,
        pages,
        ece_bins=5,
        seed=17,
        resamples=40,
    )
    assert first == second
    assert first["independent_unit"] == "Wikipedia page"
    assert first["units"] == 2
    assert first["resamples"] == 40


def test_transition_metrics_measure_distinct_change_properties() -> None:
    rows = (
        _row(1, claim="claim a", evidence="version a", label="SUPPORTS"),
        _row(2, claim="claim a", evidence="version b", label="REFUTES"),
        _row(3, claim="claim b", evidence="version a", label="REFUTES"),
        _row(4, claim="claim b", evidence="version b", label="SUPPORTS"),
    )
    case = SelectedCase("support_refute", "0" * 64, "case-1", "Page One", rows)
    scores = {
        rows[0].unique_id: (0.9, 0.05, 0.05),
        rows[1].unique_id: (0.05, 0.9, 0.05),
        rows[2].unique_id: (0.05, 0.9, 0.05),
        rows[3].unique_id: (0.9, 0.05, 0.05),
    }
    result = transition_metrics((case,), scores, seed=3, resamples=10)
    assert result["point"] == {
        "detected_change": 1.0,
        "both_endpoints_correct": 1.0,
        "bidirectional_margin_correct": 1.0,
    }
    assert "ordered_transition_correct" not in result["point"]


def test_exact_overlap_audit_uses_claim_evidence_and_pair_dimensions() -> None:
    rows = (
        _row(1, claim=" A   Claim ", evidence="Evidence One", label="SUPPORTS"),
    )
    overlap = exact_normalized_overlap_counts(
        rows,
        (("a claim", "different"), ("other", " evidence one ")),
    )
    assert overlap == {
        "normalized_claims": 1,
        "normalized_evidence": 1,
        "normalized_pairs": 0,
    }


def test_frozen_config_pins_primary_source_and_balanced_diagnostic_sample() -> None:
    path = Path("configs/m4/public_ai/vitaminc_revision_gate_v1.json")
    config = GateConfig.read(path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["source"]["revision"] == (
        "eb532922b88b199df68ed26afeb58dca5501b52f"
    )
    assert payload["dataset"]["archive_sha256"] == (
        "49d82dc1690cbee420d18e2c26f687a7937710bb211845d2571430dfd4dc0337"
    )
    assert payload["dataset"]["sample_cases"] == 128
    assert payload["dataset"]["sample_pages"] == 128
    assert payload["dataset"]["sample_manifest_sha256"] == (
        "214885784d13912ce603cc30eaed5904fbb588e493405767d66bbb3a490787d6"
    )
    assert config.file_sha256
