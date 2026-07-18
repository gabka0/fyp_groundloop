"""The frozen decision rule (tie rule v1) mapping scores to labels.

This is the only place a label is ever derived. It is a pure, total,
deterministic function of (scores, policy), monotone in each score — the
property that licenses range-indexed policy deltas in M2+ (v0.2 Section 8.3).
"""

from groundloop.domain import DecisionPolicy, SemanticObservation, VerificationLabel


def decide(
    observation: SemanticObservation, policy: DecisionPolicy
) -> VerificationLabel:
    """Tie rule v1 (frozen; docs/technical_design.md Section 5).

    REFUTE  iff r >= refute_threshold and r >= s and r >= n
            (all ties resolve toward REFUTE — conservative surfacing of
            contradiction);
    else SUPPORT iff s >= support_threshold and s > r and s > n;
    else NEUTRAL.
    """
    s = observation.support_score
    r = observation.refute_score
    n = observation.neutral_score
    if r >= policy.refute_threshold and r >= s and r >= n:
        return VerificationLabel.REFUTE
    if s >= policy.support_threshold and s > r and s > n:
        return VerificationLabel.SUPPORT
    return VerificationLabel.NEUTRAL
