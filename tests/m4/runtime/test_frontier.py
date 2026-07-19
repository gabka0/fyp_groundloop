from __future__ import annotations

import hashlib

from groundloop.m4.contracts import FrontierEntry, FrontierState, PairKey
from groundloop.m4.runtime.frontier import plan_frontier_refill


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _entry(
    chunk_id: str,
    state: FrontierState,
    rank: int,
    score: float,
) -> FrontierEntry:
    return FrontierEntry(
        claim_id="claim-1",
        chunk_version_id=chunk_id,
        candidate_policy_id="policy-1",
        state=state,
        rank=rank,
        retrieval_score=score,
        candidate_artifact_hash=_hash(chunk_id),
    )


def test_refill_counts_current_and_queued_then_uses_best_reserve() -> None:
    entries = (
        _entry("old", FrontierState.VERIFIED_CURRENT, 1, 0.99),
        _entry("queued", FrontierState.QUEUED, 2, 0.90),
        _entry("reserve-low", FrontierState.UNVERIFIED, 4, 0.80),
        _entry("reserve-best", FrontierState.UNVERIFIED, 3, 0.80),
    )
    current = (PairKey("claim-1", "outside-frontier"),)

    plan = plan_frontier_refill(
        claim_id="claim-1",
        candidate_policy_id="policy-1",
        entries=entries,
        current_verified_pairs=current,
        active_chunk_ids=frozenset(
            {"queued", "reserve-low", "reserve-best", "outside-frontier"}
        ),
        target_depth=3,
        retrieval_score_floor=0.5,
    )

    assert plan.inactivated_pairs == (PairKey("claim-1", "old"),)
    assert plan.already_queued_pairs == (PairKey("claim-1", "queued"),)
    assert plan.verifier_pairs == (PairKey("claim-1", "reserve-best"),)
    assert not plan.fresh_retrieval_required


def test_empty_or_below_floor_reserve_requires_fresh_retrieval() -> None:
    below_floor = (_entry("weak", FrontierState.UNVERIFIED, 1, 0.49),)
    plan = plan_frontier_refill(
        claim_id="claim-1",
        candidate_policy_id="policy-1",
        entries=below_floor,
        current_verified_pairs=(),
        active_chunk_ids=frozenset({"weak"}),
        target_depth=2,
        retrieval_score_floor=0.5,
    )

    assert plan.verifier_pairs == ()
    assert plan.fresh_retrieval_required
    assert plan.remaining_deficit == 2


def test_m3_bootstrap_has_no_invented_reserve() -> None:
    plan = plan_frontier_refill(
        claim_id="claim-1",
        candidate_policy_id="policy-1",
        entries=(),
        current_verified_pairs=(PairKey("claim-1", "current"),),
        active_chunk_ids=frozenset({"current"}),
        target_depth=2,
        retrieval_score_floor=0.0,
    )

    assert plan.current_pairs == (PairKey("claim-1", "current"),)
    assert plan.selected_reserve_pairs == ()
    assert plan.fresh_retrieval_required
    assert plan.remaining_deficit == 1


def test_failed_and_inactive_entries_are_never_promoted() -> None:
    entries = (
        _entry("failed", FrontierState.FAILED, 1, 1.0),
        _entry("inactive", FrontierState.UNVERIFIED, 2, 1.0),
        _entry("good", FrontierState.UNVERIFIED, 3, 0.9),
    )
    plan = plan_frontier_refill(
        claim_id="claim-1",
        candidate_policy_id="policy-1",
        entries=entries,
        current_verified_pairs=(),
        active_chunk_ids=frozenset({"failed", "good"}),
        target_depth=2,
        retrieval_score_floor=0.0,
    )

    assert plan.selected_reserve_pairs == (PairKey("claim-1", "good"),)
    assert plan.inactivated_pairs == (PairKey("claim-1", "inactive"),)
    assert plan.fresh_retrieval_required
    assert plan.remaining_deficit == 1
