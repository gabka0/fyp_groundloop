from __future__ import annotations

import json
from pathlib import Path

from groundloop.fyp_impact_selection import ClaimCandidate, Partition, RevisionCase
from groundloop.hosted_verifier import (
    build_request_manifest,
    build_request_population,
    load_hosted_verifier_config,
)
from groundloop.m4.contracts import sha256_text

ROOT = Path(__file__).resolve().parents[3]


def _partition() -> Partition:
    claims = tuple(
        ClaimCandidate(sha256_text(text), text)
        for text in (
            "Amber status is old.",
            "Amber status is new.",
            "Birch status is old.",
            "Birch status is new.",
        )
    )
    cases = (
        RevisionCase(
            case_id="case-amber",
            selection_key="0" * 64,
            stratum="support_refute",
            old_evidence="Amber status is old.",
            new_evidence="Amber status is new.",
            affected_claims=(claims[0], claims[1]),
        ),
        RevisionCase(
            case_id="case-birch",
            selection_key="1" * 64,
            stratum="support_refute",
            old_evidence="Birch status is old.",
            new_evidence="Birch status is new.",
            affected_claims=(claims[2], claims[3]),
        ),
    )
    return Partition(
        cases=cases,
        registry=tuple(sorted(claims, key=lambda item: item.claim_sha256)),
    )


def test_request_plan_is_deterministic_label_blind_and_under_cap() -> None:
    config = load_hosted_verifier_config(
        ROOT / "configs/fyp/hosted_verifier_openai_luna_v1.json"
    )
    first = build_request_population(config=config, partition=_partition())
    second = build_request_population(config=config, partition=_partition())

    assert first == second
    assert len(first) == 16
    assert sum(item.judged_side == "old" for item in first) == 8
    assert sum(item.judged_side == "new" for item in first) == 8
    assert len({item.request_id for item in first}) == len(first)
    forbidden = ("case_id", "page", "stratum", "source_label", "expected")
    for request in first:
        body = json.dumps(request.request_body, sort_keys=True)
        assert all(field not in body for field in forbidden)
        assert request.batch_line() == request.batch_line()

    manifest = build_request_manifest(
        config=config,
        requests=first,
        maximum_part_requests=5,
        maximum_part_estimated_input_tokens=10_000,
    )
    assert manifest.request_count == 16
    assert manifest.old_request_count == 8
    assert manifest.new_request_count == 8
    assert len(manifest.batch_parts) == 4
    assert manifest.estimated_batch_cost_usd < manifest.maximum_program_spend_usd
    assert manifest.to_canonical_json() == manifest.to_canonical_json()
