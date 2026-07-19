"""Deterministic per-claim frontier repair planning."""

from __future__ import annotations

from dataclasses import dataclass

from groundloop.errors import ValidationError
from groundloop.m4.contracts import FrontierEntry, FrontierState, PairKey


@dataclass(frozen=True, slots=True)
class FrontierRefillPlan:
    claim_id: str
    candidate_policy_id: str
    target_depth: int
    current_pairs: tuple[PairKey, ...]
    already_queued_pairs: tuple[PairKey, ...]
    selected_reserve_pairs: tuple[PairKey, ...]
    inactivated_pairs: tuple[PairKey, ...]
    fresh_retrieval_required: bool
    remaining_deficit: int

    @property
    def verifier_pairs(self) -> tuple[PairKey, ...]:
        return self.selected_reserve_pairs


def plan_frontier_refill(
    *,
    claim_id: str,
    candidate_policy_id: str,
    entries: tuple[FrontierEntry, ...],
    current_verified_pairs: tuple[PairKey, ...],
    active_chunk_ids: frozenset[str],
    target_depth: int,
    retrieval_score_floor: float,
) -> FrontierRefillPlan:
    """Refill from active reserve or require fresh retrieval for every deficit."""
    if not claim_id.strip() or not candidate_policy_id.strip():
        raise ValidationError("claim and policy IDs must be non-empty")
    if target_depth <= 0:
        raise ValidationError("frontier target depth must be positive")
    seen_pairs: set[PairKey] = set()
    for entry in entries:
        if entry.claim_id != claim_id:
            raise ValidationError("frontier entry belongs to another claim")
        if entry.candidate_policy_id != candidate_policy_id:
            raise ValidationError("frontier entry belongs to another policy")
        pair = PairKey(entry.claim_id, entry.chunk_version_id)
        if pair in seen_pairs:
            raise ValidationError("frontier entries must be unique by pair")
        seen_pairs.add(pair)
    if any(pair.claim_id != claim_id for pair in current_verified_pairs):
        raise ValidationError("current verified pair belongs to another claim")
    if len(set(current_verified_pairs)) != len(current_verified_pairs):
        raise ValidationError("current verified pairs must be unique")
    current = {
        pair
        for pair in current_verified_pairs
        if pair.chunk_version_id in active_chunk_ids
    }
    queued: set[PairKey] = set()
    reserve: list[tuple[float, int, str, PairKey]] = []
    inactivated: set[PairKey] = set()
    for entry in entries:
        pair = PairKey(entry.claim_id, entry.chunk_version_id)
        if entry.chunk_version_id not in active_chunk_ids:
            inactivated.add(pair)
            current.discard(pair)
            continue
        if entry.state is FrontierState.VERIFIED_CURRENT:
            current.add(pair)
        elif entry.state is FrontierState.QUEUED:
            queued.add(pair)
        elif (
            entry.state is FrontierState.UNVERIFIED
            and entry.retrieval_score >= retrieval_score_floor
        ):
            reserve.append(
                (-entry.retrieval_score, entry.rank, entry.chunk_version_id, pair)
            )
    queued.difference_update(current)
    covered = current | queued
    deficit = max(0, target_depth - len(covered))
    selected: list[PairKey] = []
    for _score, _rank, _chunk_id, pair in sorted(reserve):
        if len(selected) >= deficit:
            break
        if pair in covered:
            continue
        selected.append(pair)
        covered.add(pair)
    remaining = max(0, target_depth - len(covered))
    return FrontierRefillPlan(
        claim_id=claim_id,
        candidate_policy_id=candidate_policy_id,
        target_depth=target_depth,
        current_pairs=tuple(sorted(current)),
        already_queued_pairs=tuple(sorted(queued)),
        selected_reserve_pairs=tuple(selected),
        inactivated_pairs=tuple(sorted(inactivated)),
        fresh_retrieval_required=remaining > 0,
        remaining_deficit=remaining,
    )
