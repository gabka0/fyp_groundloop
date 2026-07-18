from dataclasses import FrozenInstanceError

import pytest

from groundloop.domain import (
    ChunkVersion,
    ClaimStatus,
    DecisionPolicy,
    ModelStamp,
    SemanticObservation,
    SubjectKind,
    normalized_text_hash,
)
from groundloop.errors import ValidationError

STAMP = ModelStamp(model_id="m", model_version="v", prompt_version="p")


def _observation(**overrides: float) -> SemanticObservation:
    scores = {"support_score": 0.9, "refute_score": 0.05, "neutral_score": 0.05}
    scores.update(overrides)
    return SemanticObservation(
        observation_id="o1",
        subject_kind=SubjectKind.CLAIM,
        subject_id="c1",
        chunk_version_id="p1",
        task_type="verify",
        producer=STAMP,
        input_hash="ih",
        **scores,
    )


def test_claim_status_values_are_stable() -> None:
    assert ClaimStatus.SUPPORTED.value == "supported"
    assert ClaimStatus.CONFLICTED.value == "conflicted"


def test_observation_is_immutable_and_scores_only() -> None:
    observation = _observation()
    assert observation.producer.model_version == "v"
    assert not hasattr(observation, "label")
    with pytest.raises(FrozenInstanceError):
        observation.subject_id = "changed"  # type: ignore[misc]


def test_observation_key_is_the_currency_key() -> None:
    observation = _observation()
    assert observation.key == (SubjectKind.CLAIM, "c1", "p1", "verify")


@pytest.mark.parametrize("bad", [-0.1, 1.1, float("nan"), float("inf")])
def test_observation_rejects_invalid_scores(bad: float) -> None:
    with pytest.raises(ValidationError):
        _observation(support_score=bad)


def test_policy_rejects_invalid_thresholds_and_unknown_tie_rule() -> None:
    with pytest.raises(ValidationError):
        DecisionPolicy(policy_version="k", support_threshold=1.5, refute_threshold=0.8)
    with pytest.raises(ValidationError):
        DecisionPolicy(
            policy_version="k",
            support_threshold=0.8,
            refute_threshold=0.8,
            tie_rule_version="v2",
        )


def test_normalization_v1_collapses_whitespace() -> None:
    assert normalized_text_hash("  a   b\nc ") == normalized_text_hash("a b c")
    assert normalized_text_hash("a b c") != normalized_text_hash("a b C")


def test_chunk_version_computes_text_hash() -> None:
    chunk = ChunkVersion(
        chunk_version_id="p1",
        document_version_id="dv1",
        chunk_index=0,
        text="Nimbus  supports Python 3.10 and later.",
    )
    assert chunk.text_hash == normalized_text_hash(
        "Nimbus supports Python 3.10 and later."
    )


def test_chunk_version_rejects_forged_text_hash() -> None:
    with pytest.raises(ValidationError, match="does not match normalization v1"):
        ChunkVersion(
            chunk_version_id="p1",
            document_version_id="dv1",
            chunk_index=0,
            text="actual content",
            text_hash=normalized_text_hash("different content"),
        )


def test_chunk_version_rejects_negative_index() -> None:
    with pytest.raises(ValidationError, match="chunk_index must be nonnegative"):
        ChunkVersion(
            chunk_version_id="p1",
            document_version_id="dv1",
            chunk_index=-1,
            text="content",
        )
