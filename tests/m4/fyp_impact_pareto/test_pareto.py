from __future__ import annotations

from groundloop.fyp_impact_pareto import _dominates
from groundloop.fyp_impact_selection import SelectionMetrics


def _point(*, budget: int, selected: int, affected: int) -> SelectionMetrics:
    return SelectionMetrics(
        policy_id="old_new_rarity_coverage",
        budget=budget,
        event_count=2,
        registry_claim_count=4,
        exhaustive_pair_count=8,
        selected_pair_count=selected,
        avoided_pair_count=8 - selected,
        affected_claim_numerator=affected,
        affected_claim_denominator=4,
        full_event_numerator=max(0, affected - 2),
        full_event_denominator=2,
        eligible=False,
    )


def test_pareto_dominance_requires_no_more_work_and_no_less_recall() -> None:
    low = _point(budget=1, selected=2, affected=2)
    better = _point(budget=2, selected=4, affected=4)
    wasteful = _point(budget=3, selected=6, affected=4)

    assert _dominates(better, wasteful) is True
    assert _dominates(low, better) is False
    assert _dominates(better, low) is False
