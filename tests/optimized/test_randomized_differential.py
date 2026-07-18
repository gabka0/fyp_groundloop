"""CI-sized randomized equality gate for the optimized engine."""

from algorithm_randomized_differential import (
    run_algorithm_randomized_differential,
)


def test_optimized_engine_matches_oracle_after_every_random_event() -> None:
    result = run_algorithm_randomized_differential(
        total_events=2_000,
        events_per_stream=50,
        seed=20260718,
    )
    assert result["events"] == 2_000
    assert result["streams"] == 40
