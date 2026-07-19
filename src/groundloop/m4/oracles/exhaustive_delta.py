"""Independent exhaustive additive delta from working snapshot Bw to Bx."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from groundloop.domain import AnswerState, ClaimState
from groundloop.errors import ValidationError
from groundloop.m4.contracts import (
    AffectedSets,
    FullPairAuditResult,
    PairJudgment,
    PairKey,
)
from groundloop.m4.oracles.affected import compute_affected_sets
from groundloop.m4.oracles.grounding import recompute_grounding_states


@dataclass(frozen=True, slots=True)
class ExhaustiveAdditiveDelta:
    """Fresh Bx states and their same-baseline differences from Bw."""

    audit: FullPairAuditResult
    claim_states: tuple[ClaimState, ...]
    answer_states: tuple[AnswerState, ...]
    affected_sets: AffectedSets


def _unique_judgments(
    name: str, judgments: Iterable[PairJudgment]
) -> dict[PairKey, PairJudgment]:
    result: dict[PairKey, PairJudgment] = {}
    for judgment in judgments:
        if judgment.pair in result:
            raise ValidationError(f"{name} contains duplicate pair judgments")
        result[judgment.pair] = judgment
    return result


def compute_exhaustive_additive_delta(
    *,
    baseline_id: str,
    audit: FullPairAuditResult,
    claim_to_answer: Mapping[str, str],
    required_claim_ids: frozenset[str],
    active_chunk_text_hashes: Mapping[str, str],
    surviving_judgments: Iterable[PairJudgment],
    working_claim_states: Iterable[ClaimState],
    working_answer_states: Iterable[AnswerState],
) -> ExhaustiveAdditiveDelta:
    """Retain Bw judgments, add every audited inserted pair, and rebuild Bx.

    The caller supplies the independently withdrawn working snapshot ``Bw``.
    This function deliberately has no dependency on selective admission or
    incremental runtime code.
    """
    if tuple(sorted(claim_to_answer)) != audit.registered_claim_ids:
        raise ValidationError("audit claim registry differs from delta registry")
    missing_chunks = set(audit.inserted_active_chunk_ids) - set(
        active_chunk_text_hashes
    )
    if missing_chunks:
        raise ValidationError("audited inserted chunks must be active in Bx")

    surviving = _unique_judgments("surviving_judgments", surviving_judgments)
    audited = _unique_judgments("audit", audit.judgments)
    overlap = surviving.keys() & audited.keys()
    if overlap:
        raise ValidationError("Bw must not already contain audited inserted pairs")
    all_judgments = tuple(
        judgment
        for _, judgment in sorted((surviving | audited).items())
    )
    claim_states, answer_states = recompute_grounding_states(
        claim_to_answer=claim_to_answer,
        required_claim_ids=required_claim_ids,
        chunk_text_hashes=active_chunk_text_hashes,
        judgments=all_judgments,
    )
    affected = compute_affected_sets(
        baseline_id=baseline_id,
        before_claim_states=working_claim_states,
        after_claim_states=claim_states,
        before_answer_states=working_answer_states,
        after_answer_states=answer_states,
    )
    return ExhaustiveAdditiveDelta(
        audit=audit,
        claim_states=claim_states,
        answer_states=answer_states,
        affected_sets=affected,
    )
