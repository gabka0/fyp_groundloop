from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from pathlib import Path

import pytest


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _case_rows(
    *, case_id: str, entity: str, stratum: str, selection_key: str
) -> list[dict[str, str]]:
    old_claim = f"{entity} status is old"
    new_claim = f"{entity} status is new"
    old_evidence = f"The record for {entity} says its status is old."
    new_evidence = f"The record for {entity} says its status is new."
    changed_label = "neutral" if stratum == "support_neutral" else "refute"
    page = f"Page {entity}"
    common = {
        "case_id": case_id,
        "normalized_page_sha256": _digest(page.casefold()),
        "page": page,
        "revision_type": "real",
        "schema_version": "groundloop-m4-13-vitaminc-row-v1",
        "selection_key": selection_key,
        "source_label": "SUPPORTS",
        "split": "development",
        "stratum": stratum,
        "wiki_revision_id": "1",
    }
    values = (
        (old_claim, old_evidence, "support"),
        (old_claim, new_evidence, changed_label),
        (new_claim, old_evidence, changed_label),
        (new_claim, new_evidence, "support"),
    )
    rows: list[dict[str, str]] = []
    for position, (claim, evidence, label) in enumerate(values, start=1):
        rows.append(
            {
                **common,
                "claim": claim,
                "claim_sha256": _digest(claim),
                "evidence": evidence,
                "evidence_sha256": _digest(evidence),
                "label": label,
                "unique_id": f"{case_id}_{position}",
            }
        )
    return rows


def write_config(path: Path, *, source_sha256: str) -> Path:
    payload = {
        "schema_version": "groundloop-fyp-impact-selection-config-v1",
        "expected_source_sha256": source_sha256,
        "expected_source_schema_version": "groundloop-m4-13-vitaminc-row-v1",
        "expected_case_count": 4,
        "strata": ["support_neutral", "support_refute"],
        "cases_per_stratum": 2,
        "development_cases_per_stratum": 1,
        "policy_ids": [
            "changed_token_overlap",
            "new_evidence_overlap",
            "old_new_rarity_coverage",
        ],
        "budgets": [1, 2],
        "minimum_affected_claim_recall": {"numerator": 1, "denominator": 1},
        "minimum_full_event_coverage": {"numerator": 1, "denominator": 1},
        "minimum_pair_work_reduction": {"numerator": 1, "denominator": 2},
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


@pytest.fixture
def impact_config_writer() -> Callable[..., Path]:
    return write_config


@pytest.fixture
def impact_fixture(tmp_path: Path) -> tuple[Path, Path]:
    cases = (
        ("case-a", "amber", "support_neutral", "0" * 64),
        ("case-b", "birch", "support_neutral", "1" * 64),
        ("case-c", "cedar", "support_refute", "2" * 64),
        ("case-d", "denim", "support_refute", "3" * 64),
    )
    rows = [
        row
        for case_id, entity, stratum, key in cases
        for row in _case_rows(
            case_id=case_id,
            entity=entity,
            stratum=stratum,
            selection_key=key,
        )
    ]
    source = tmp_path / "source.jsonl"
    source.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )
    source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    return write_config(tmp_path / "config.json", source_sha256=source_hash), source
