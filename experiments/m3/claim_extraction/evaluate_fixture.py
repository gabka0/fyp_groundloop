"""Evaluate exact annotated claim output on the software-doc fixture.

This lane-local gate validates metric definitions and structured plumbing with
deterministic annotated outputs. It is not a real-model quality result.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import TypedDict, cast


class ClaimRow(TypedDict):
    local_claim_id: str
    text: str
    required: bool
    cited_chunk_version_ids: list[str]


class EvidenceRow(TypedDict):
    chunk_version_id: str
    text: str


class FixtureCase(TypedDict):
    case_id: str
    answer_text: str
    answer_citations: list[str]
    evidence: list[EvidenceRow]
    gold_claims: list[ClaimRow]
    known_non_atomic_claim_texts: list[str]


class Fixture(TypedDict):
    schema_version: str
    cases: list[FixtureCase]


def normalize(text: str) -> str:
    return " ".join(text.split()).casefold()


def evaluate(fixture: Fixture) -> dict[str, object]:
    gold_total = 0
    covered_total = 0
    prediction_total = 0
    atomicity_violations = 0
    duplicate_claims = 0
    unsupported_additions = 0
    resolved_citations = 0
    citation_total = 0

    for case in fixture["cases"]:
        # The deterministic baseline emits the annotated claims verbatim. Real
        # Qwen output is evaluated separately after the serialized model gate.
        predictions = case["gold_claims"]
        gold_texts = {normalize(claim["text"]) for claim in case["gold_claims"]}
        predicted_texts = [normalize(claim["text"]) for claim in predictions]
        non_atomic = {
            normalize(text) for text in case["known_non_atomic_claim_texts"]
        }
        evidence_ids = {
            evidence["chunk_version_id"] for evidence in case["evidence"]
        }
        answer_citations = set(case["answer_citations"])

        gold_total += len(gold_texts)
        covered_total += len(gold_texts & set(predicted_texts))
        prediction_total += len(predictions)
        atomicity_violations += sum(text in non_atomic for text in predicted_texts)
        duplicate_claims += sum(
            count - 1 for count in Counter(predicted_texts).values() if count > 1
        )
        unsupported_additions += sum(text not in gold_texts for text in predicted_texts)
        for claim in predictions:
            for citation in claim["cited_chunk_version_ids"]:
                citation_total += 1
                if citation in answer_citations and citation in evidence_ids:
                    resolved_citations += 1

    return {
        "schema_version": "m3-claim-extraction-report-v1",
        "fixture_schema_version": fixture["schema_version"],
        "case_count": len(fixture["cases"]),
        "gold_proposition_count": gold_total,
        "predicted_claim_count": prediction_total,
        "proposition_coverage": covered_total / gold_total if gold_total else 0.0,
        "atomicity_violations": atomicity_violations,
        "duplicate_claims": duplicate_claims,
        "unsupported_additions": unsupported_additions,
        "citation_resolution_rate": (
            resolved_citations / citation_total if citation_total else 0.0
        ),
        "evaluation_mode": "annotated-deterministic-plumbing",
        "limitation": (
            "Exact normalized-text matching validates the offline metric and "
            "schema path; it is not evidence of Qwen extraction quality."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("fixture", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    fixture = cast(Fixture, json.loads(args.fixture.read_text(encoding="utf-8")))
    rendered = json.dumps(evaluate(fixture), indent=2, sort_keys=True) + "\n"
    if args.output is None:
        print(rendered, end="")
    else:
        args.output.write_text(rendered, encoding="utf-8")


if __name__ == "__main__":
    main()
