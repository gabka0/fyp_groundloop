from __future__ import annotations

from groundloop.m5.evaluation.records import WiceSplit

Rows = dict[WiceSplit, tuple[list[dict[str, object]], list[dict[str, object]]]]


def parent_row(
    identifier: str,
    evidence: list[str],
    *,
    label: str = "supported",
    claim: str = "Compound claim",
) -> dict[str, object]:
    return {
        "claim": claim,
        "evidence": evidence,
        "label": label,
        "meta": {"id": identifier},
        "supporting_sentences": [],
    }


def subclaim_row(
    identifier: str,
    evidence: list[str],
    supporting_sentences: list[object],
    *,
    label: str = "supported",
    claim: str = "Requirement",
) -> dict[str, object]:
    return {
        "claim": claim,
        "evidence": evidence,
        "label": label,
        "meta": {"id": identifier},
        "supporting_sentences": supporting_sentences,
    }
