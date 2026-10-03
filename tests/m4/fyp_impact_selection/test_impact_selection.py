from __future__ import annotations

import csv
import hashlib
import io
import json
from collections.abc import Callable
from pathlib import Path

import pytest

import groundloop.fyp_impact_selection as selection_module
from groundloop.errors import ValidationError
from groundloop.fyp_impact_selection import (
    ImpactSelectionVerdict,
    execute_impact_selection,
    load_impact_selection_config,
    load_revision_cases,
    run_impact_selection,
)


def test_synthetic_held_out_diagnostic_passes_and_is_byte_stable(
    tmp_path: Path, impact_fixture: tuple[Path, Path]
) -> None:
    config, source = impact_fixture
    report = run_impact_selection(config_path=config, source_path=source)

    assert report.verdict is ImpactSelectionVerdict.PASS
    assert report.selected_policy_id == "old_new_rarity_coverage"
    assert report.selected_budget == 2
    assert report.evaluation_result is not None
    assert report.evaluation_result.affected_claim_numerator == 4
    assert report.evaluation_result.affected_claim_denominator == 4
    assert report.evaluation_result.full_event_numerator == 2
    assert report.evaluation_result.full_event_denominator == 2
    assert report.evaluation_result.selected_pair_count == 4
    assert report.evaluation_result.exhaustive_pair_count == 8

    first, first_bundle = execute_impact_selection(
        config_path=config,
        source_path=source,
        output_directory=tmp_path / "first",
    )
    second, second_bundle = execute_impact_selection(
        config_path=config,
        source_path=source,
        output_directory=tmp_path / "second",
    )
    assert first.to_canonical_json() == second.to_canonical_json()
    assert first.to_development_csv() == second.to_development_csv()
    assert first.to_evaluation_events_csv() == second.to_evaluation_events_csv()
    assert (
        first_bundle.report_path.read_bytes()
        == second_bundle.report_path.read_bytes()
    )
    assert (
        first_bundle.development_csv_path.read_bytes()
        == second_bundle.development_csv_path.read_bytes()
    )
    assert (
        first_bundle.evaluation_events_csv_path.read_bytes()
        == second_bundle.evaluation_events_csv_path.read_bytes()
    )
    payload = json.loads(first.to_canonical_json())
    assert payload["report_manifest_hash"] == first.manifest_hash
    rows = list(csv.DictReader(io.StringIO(first.to_development_csv())))
    assert len(rows) == 6
    assert sum(row["selected"] == "true" for row in rows) == 1


def test_source_byte_mutation_is_rejected(
    impact_fixture: tuple[Path, Path], tmp_path: Path
) -> None:
    config, source = impact_fixture
    mutated = tmp_path / "mutated.jsonl"
    mutated.write_bytes(source.read_bytes() + b"\n")

    with pytest.raises(ValidationError, match="source SHA-256"):
        load_revision_cases(mutated, load_impact_selection_config(config))


def test_structural_mutation_is_rejected_after_rebinding_hash(
    impact_fixture: tuple[Path, Path],
    tmp_path: Path,
    impact_config_writer: Callable[..., Path],
) -> None:
    _, source = impact_fixture
    rows = [json.loads(line) for line in source.read_text().splitlines()]
    rows[1]["label"] = rows[0]["label"]
    mutated = tmp_path / "structural.jsonl"
    mutated.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )
    config = impact_config_writer(
        tmp_path / "structural-config.json",
        source_sha256=hashlib.sha256(mutated.read_bytes()).hexdigest(),
    )

    with pytest.raises(ValidationError, match="change gold label"):
        load_revision_cases(mutated, load_impact_selection_config(config))


def test_ranker_receives_no_case_or_gold_fields(
    impact_fixture: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    config, source = impact_fixture
    original = selection_module.rank_claims
    calls: list[frozenset[str]] = []

    def observing_ranker(**kwargs: object) -> object:
        calls.append(frozenset(kwargs))
        assert frozenset(kwargs) == {
            "policy_id",
            "claims",
            "old_evidence",
            "new_evidence",
        }
        return original(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(selection_module, "rank_claims", observing_ranker)
    run_impact_selection(config_path=config, source_path=source)

    assert calls
    assert all("label" not in fields and "case_id" not in fields for fields in calls)
