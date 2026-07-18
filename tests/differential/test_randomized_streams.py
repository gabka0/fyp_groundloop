"""Small CI-sized run of the reproducible M2 randomized stream generator."""

from run_m2_differential import run_randomized_differential


def test_randomized_streams_match_after_every_event() -> None:
    result = run_randomized_differential(
        total_events=1_000,
        events_per_stream=50,
        seed=20260718,
    )
    assert result["events"] == 1_000
    assert result["streams"] == 20
