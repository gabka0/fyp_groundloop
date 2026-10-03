from __future__ import annotations

from collections import Counter
from pathlib import Path

from groundloop.fyp_impact_pareto import build_task4_miss_analysis

ROOT = Path(__file__).resolve().parents[3]


def test_every_task4_positive_miss_has_a_structural_explanation() -> None:
    rows, summary = build_task4_miss_analysis(
        task4_config_path=ROOT / "configs/m4/evaluation/controlled_v1.json"
    )

    assert len(rows) == 8
    assert Counter(row.failure_code for row in rows) == {
        "channel_rank_outside_budget": 4,
        "vector_first_union_cap_exhaustion": 2,
        "lineage_points_to_other_claim": 2,
    }
    assert all(row.pair_recall_numerator == 0 for row in rows)
    assert all(row.pair_recall_denominator == 1 for row in rows)
    assert all(row.claim_recall_numerator == 0 for row in rows)
    assert all(row.status_recall_numerator == 0 for row in rows)
    assert all(row.answer_recall_numerator == 0 for row in rows)
    assert all(row.frontier_rank == 1 for row in rows if "insert" in row.event_id)
    assert summary["frontier_attempted_pairs"] == 8
    assert summary["exhaustive_attempted_pairs"] == 8
    assert summary["frontier_recovered_all_positive_pairs"] is True
