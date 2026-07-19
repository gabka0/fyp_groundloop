"""Content identities for immutable oracle judgments."""

from __future__ import annotations

from groundloop.m4.contracts import PairJudgment, stable_m4_digest


def judgment_digest(judgment: PairJudgment) -> str:
    """Bind every persisted judgment field, including exact model scores."""
    scores = tuple(
        "none" if score is None else float(score).hex()
        for score in (
            judgment.support_score,
            judgment.refute_score,
            judgment.neutral_score,
        )
    )
    return stable_m4_digest(
        "m4-pair-judgment-v1",
        judgment.pair.claim_id,
        judgment.pair.chunk_version_id,
        judgment.source_kind.value,
        judgment.source_artifact_id,
        judgment.decision_policy_or_guideline_id,
        judgment.derived_label.value,
        judgment.input_hash,
        judgment.split_id,
        *scores,
    )
