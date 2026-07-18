"""Every branch and tie case of the frozen decision rule (tie rule v1)."""

import pytest
from helpers import make_observation

from groundloop.domain import DecisionPolicy, VerificationLabel
from groundloop.policy import decide

POLICY = DecisionPolicy(
    policy_version="k", support_threshold=0.8, refute_threshold=0.8
)


@pytest.mark.parametrize(
    ("scores", "expected"),
    [
        # clear SUPPORT: strict max, meets threshold
        ((0.9, 0.05, 0.05), VerificationLabel.SUPPORT),
        # support at exact threshold (>= semantics)
        ((0.8, 0.1, 0.1), VerificationLabel.SUPPORT),
        # support is max but below threshold -> NEUTRAL
        ((0.7, 0.1, 0.1), VerificationLabel.NEUTRAL),
        # clear REFUTE
        ((0.05, 0.9, 0.05), VerificationLabel.REFUTE),
        # refute at exact threshold
        ((0.1, 0.8, 0.1), VerificationLabel.REFUTE),
        # refute is max but below threshold; support below its threshold too
        ((0.1, 0.7, 0.1), VerificationLabel.NEUTRAL),
        # r/s tie above both thresholds resolves to REFUTE
        ((0.9, 0.9, 0.0), VerificationLabel.REFUTE),
        # r/n tie resolves to REFUTE
        ((0.0, 0.9, 0.9), VerificationLabel.REFUTE),
        # s/n tie resolves to NEUTRAL (support requires strict max)
        ((0.9, 0.0, 0.9), VerificationLabel.NEUTRAL),
        # neutral is max -> NEUTRAL
        ((0.05, 0.05, 0.9), VerificationLabel.NEUTRAL),
        # all zero -> NEUTRAL
        ((0.0, 0.0, 0.0), VerificationLabel.NEUTRAL),
    ],
)
def test_tie_rule_v1(
    scores: tuple[float, float, float], expected: VerificationLabel
) -> None:
    observation = make_observation("o", "c", "p", scores)
    assert decide(observation, POLICY) is expected


def test_support_above_threshold_still_loses_to_refute_tie() -> None:
    """Conservative surfacing: s meets its threshold but ties with r, and r
    is below its own threshold -> NEUTRAL (argmax fails its gate)."""
    policy = DecisionPolicy(
        policy_version="k2", support_threshold=0.8, refute_threshold=0.9
    )
    observation = make_observation("o", "c", "p", (0.85, 0.85, 0.0))
    assert decide(observation, policy) is VerificationLabel.NEUTRAL


def test_rule_is_monotone_in_support_score() -> None:
    for low, high in [(0.0, 0.5), (0.5, 0.79), (0.79, 0.8), (0.8, 1.0)]:
        lo = decide(make_observation("o", "c", "p", (low, 0.0, 0.0)), POLICY)
        hi = decide(make_observation("o", "c", "p", (high, 0.0, 0.0)), POLICY)
        order = {
            VerificationLabel.REFUTE: 0,
            VerificationLabel.NEUTRAL: 1,
            VerificationLabel.SUPPORT: 2,
        }
        assert order[hi] >= order[lo]
