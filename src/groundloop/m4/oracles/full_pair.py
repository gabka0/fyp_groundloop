"""Independent exhaustive claim x inserted-chunk pair enumeration."""

from __future__ import annotations

from groundloop.errors import ValidationError
from groundloop.m4.contracts import (
    FullPairAuditResult,
    PairJudgment,
    PairKey,
    stable_m4_digest,
)
from groundloop.m4.oracles.identity import judgment_digest
from groundloop.m4.oracles.testing import PairJudge


def _canonical_ids(name: str, values: tuple[str, ...]) -> tuple[str, ...]:
    if any(not value.strip() for value in values):
        raise ValidationError(f"{name} must contain only non-empty IDs")
    canonical = tuple(sorted(set(values)))
    if len(canonical) != len(values):
        raise ValidationError(f"{name} must not contain duplicate IDs")
    return canonical


def _audit_manifest_id(
    event_id: str,
    claim_ids: tuple[str, ...],
    chunk_ids: tuple[str, ...],
    judgments: tuple[PairJudgment, ...],
) -> str:
    parts = ["m4-full-pair-audit-v1", event_id, *claim_ids, *chunk_ids]
    for judgment in judgments:
        parts.append(judgment_digest(judgment))
    return "full-pair-audit-" + stable_m4_digest(*parts)


def run_full_pair_audit(
    *,
    event_id: str,
    registered_claim_ids: tuple[str, ...],
    inserted_active_chunk_ids: tuple[str, ...],
    judge: PairJudge,
) -> FullPairAuditResult:
    """Judge the exact Cartesian product without consuming admitted pairs."""
    if not event_id.strip():
        raise ValidationError("event_id must be non-empty")
    claim_ids = _canonical_ids("registered_claim_ids", registered_claim_ids)
    chunk_ids = _canonical_ids(
        "inserted_active_chunk_ids", inserted_active_chunk_ids
    )
    pairs = tuple(
        PairKey(claim_id, chunk_id)
        for claim_id in claim_ids
        for chunk_id in chunk_ids
    )
    judgments = judge.judge_pairs(pairs)
    if len(judgments) != len(pairs):
        raise ValidationError("pair judge returned the wrong judgment count")
    if tuple(judgment.pair for judgment in judgments) != pairs:
        raise ValidationError("pair judge changed pair identity or order")
    return FullPairAuditResult(
        event_id=event_id,
        inserted_active_chunk_ids=chunk_ids,
        registered_claim_ids=claim_ids,
        judgments=judgments,
        manifest_id=_audit_manifest_id(event_id, claim_ids, chunk_ids, judgments),
    )
