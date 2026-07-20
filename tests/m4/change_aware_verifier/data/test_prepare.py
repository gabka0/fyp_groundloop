from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from groundloop.errors import ValidationError
from training.m4_13_verifier.prepare import (
    DataConfig,
    EligibleCase,
    VitaminCRow,
    _parser,
    _require_file_hash,
    deterministic_select,
    exact_normalized_overlap,
    identity_manifest,
    normalized_page_quarantine,
    validate_atomic_case,
    validate_terminal_prerequisites,
)


def _row(
    suffix: int,
    *,
    case_id: str = "case-1",
    page: str = "Page One",
    label: str | None = None,
    claim: str | None = None,
    evidence: str | None = None,
) -> VitaminCRow:
    labels = {
        1: "SUPPORTS",
        2: "REFUTES",
        3: "REFUTES",
        4: "SUPPORTS",
    }
    claims = {1: "claim a", 2: "claim a", 3: "claim b", 4: "claim b"}
    passages = {1: "version a", 2: "version b", 3: "version a", 4: "version b"}
    return VitaminCRow(
        unique_id=f"{case_id}_{suffix}",
        case_id=case_id,
        wiki_revision_id=f"rev-{suffix}",
        source_label=labels[suffix] if label is None else label,
        claim=claims[suffix] if claim is None else claim,
        evidence=passages[suffix] if evidence is None else evidence,
        page=page,
        revision_type="real",
    )


def _case(*, case_id: str = "case-1", page: str = "Page One") -> EligibleCase:
    rows = tuple(_row(suffix, case_id=case_id, page=page) for suffix in range(1, 5))
    return validate_atomic_case(rows, stratum="support_refute")


def test_frozen_config_binds_all_cross_milestone_identities() -> None:
    config = DataConfig.read(Path("configs/m4/verifier/change_aware_v1.json"))
    assert config.dataset["normalized_page_intersections"] == {
        "train_development": 18,
        "train_test": 22,
        "development_test": 2,
        "all_three": 1,
        "unique_quarantined": 40,
        "quarantine_sha256": (
            "0d2dd3fbe64fb6759cdbac9c4bdae68529fffdaf0370d703310a28166e7b54da"
        ),
    }
    assert config.m4_12["sample_manifest_sha256"] == (
        "214885784d13912ce603cc30eaed5904fbb588e493405767d66bbb3a490787d6"
    )
    assert config.m4_12["semantic_result_sha256"] == (
        "017fd20fb810652725846e214c884114c5afdc06cb009206e4bdaf899f9ecef7"
    )
    assert config.m4_12["predictions_sha256"] == (
        "0cd315438ff94d54923b8fdefc16ac1c20d9f9dc385ae2963251c31e04c4bc0e"
    )
    assert config.dataset["terminal_reserve"]["manifest_sha256"] == (
        "3dcfcba0b809e3bcfcf2f9c036c4946f484d706faa61eec8c3fa489fbcb11dc5"
    )
    terminal = validate_terminal_prerequisites(config)
    assert terminal["structural_result_sha256"] == (
        "92d7586c78f40ae447d97f703a51caf4dbdefc76ee911cde298dc841e1a5144d"
    )


def test_file_hash_validation_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "artifact.json"
    path.write_text("{}", encoding="utf-8")
    with pytest.raises(ValidationError, match="SHA-256 mismatch"):
        _require_file_hash(path, "0" * 64, "fixture artifact")


@pytest.mark.parametrize(
    "rows,error",
    [
        (tuple(_row(suffix) for suffix in (1, 2, 3)), "four rows"),
        (
            tuple(
                _row(suffix, page="other" if suffix == 4 else "Page One")
                for suffix in range(1, 5)
            ),
            "multiple raw pages",
        ),
        (
            tuple(
                _row(suffix, claim="wrong" if suffix == 2 else None)
                for suffix in range(1, 5)
            ),
            "2x2 layout",
        ),
        (
            tuple(
                _row(suffix, label="SUPPORTS" if suffix == 2 else None)
                for suffix in range(1, 5)
            ),
            "each VitaminC transition",
        ),
    ],
)
def test_atomic_case_rejects_malformed_cases(
    rows: tuple[VitaminCRow, ...], error: str
) -> None:
    with pytest.raises(ValidationError, match=error):
        validate_atomic_case(rows, stratum="support_refute")


def test_normalized_page_quarantine_uses_union_not_pairwise_count_sum() -> None:
    split_rows = {
        "train": (_row(1, page=" Triple  Page "), _row(2, page="XXx")),
        "development": (_row(1, page="triple page"), _row(2, page="XXX")),
        "test": (_row(1, page="TRIPLE PAGE"), _row(2, page="test only")),
    }
    quarantine, pages, counts = normalized_page_quarantine(split_rows)
    assert pages["train"] == {"triple page", "xxx"}
    assert counts == {
        "train_development": 2,
        "train_test": 1,
        "development_test": 1,
        "all_three": 1,
        "unique_quarantined": 2,
    }
    assert quarantine == {"triple page", "xxx"}


def test_deterministic_selection_is_input_order_independent_and_model_free() -> None:
    first = _case(case_id="case-a", page="Page A")
    second = _case(case_id="case-b", page="Page B")
    neutral_rows = tuple(
        _row(
            suffix,
            case_id="case-c",
            page="Page C",
            label=(
                "SUPPORTS" if suffix in {1, 4} else "NOT ENOUGH INFO"
            ),
        )
        for suffix in range(1, 5)
    )
    neutral = validate_atomic_case(neutral_rows, stratum="support_neutral")
    forward = deterministic_select(
        {"support_refute": (first, second), "support_neutral": (neutral,)},
        seed=7,
        cases_per_stratum=1,
        split="train",
    )
    reverse = deterministic_select(
        {"support_refute": (second, first), "support_neutral": (neutral,)},
        seed=7,
        cases_per_stratum=1,
        split="train",
    )
    assert identity_manifest(forward) == identity_manifest(reverse)
    assert set(inspect.signature(deterministic_select).parameters) == {
        "candidates",
        "seed",
        "cases_per_stratum",
        "split",
        "excluded_pages",
    }
    assert not any(
        token in inspect.getsource(deterministic_select)
        for token in ("logits", "prediction", "probabilities", "checkpoint")
    )


def test_exact_m3_overlap_tracks_claim_evidence_and_pair_separately() -> None:
    rows = (_row(1, claim=" A claim ", evidence="Evidence one"),)
    m3 = (
        {"claim": "a   claim", "evidence": "different"},
        {"claim": "other", "evidence": " evidence ONE "},
    )
    assert exact_normalized_overlap(rows, m3) == {
        "normalized_claims": 1,
        "normalized_evidence": 1,
        "normalized_pairs": 0,
    }


def test_terminal_validation_rejects_unresolved_corrected_m4_10(
    tmp_path: Path,
) -> None:
    source = Path("configs/m4/verifier/change_aware_v1.json")
    payload = json.loads(source.read_text(encoding="utf-8"))
    payload["corrected_m4_10"] = {
        "status": "pending",
        "config_schema_version": "groundloop-m4-real-git-histories-v2",
    }
    path = tmp_path / "pending.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    config = DataConfig.read(path)
    with pytest.raises(ValidationError, match="unresolved"):
        validate_terminal_prerequisites(config)


def test_preparation_cli_has_no_test_or_candidate_output_argument() -> None:
    destinations = {action.dest for action in _parser()._actions}
    assert "test_path" not in destinations
    assert "terminal_path" not in destinations
    assert "predictions" not in destinations
    assert "logits" not in destinations
