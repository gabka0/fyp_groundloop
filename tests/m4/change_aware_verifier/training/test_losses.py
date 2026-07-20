from __future__ import annotations

import math

import pytest
from m4_13_verifier.losses import (
    BASE_LOGIT_ORDER,
    STORED_LABEL_ORDER,
    base_probabilities_to_stored,
    change_aware_loss,
    endpoint_cross_entropy,
    paired_revision_margin_loss,
    stored_label_to_base_index,
    vitaminc_label_to_stored,
)

from groundloop.errors import ValidationError

torch = pytest.importorskip("torch")


def _log_softmax(values: tuple[float, float, float]) -> tuple[float, ...]:
    maximum = max(values)
    denominator = sum(math.exp(value - maximum) for value in values)
    log_denominator = maximum + math.log(denominator)
    return tuple(value - log_denominator for value in values)


def test_label_and_logit_order_is_exact_and_nei_is_neutral() -> None:
    assert BASE_LOGIT_ORDER == ("contradiction", "entailment", "neutral")
    assert STORED_LABEL_ORDER == ("support", "refute", "neutral")
    assert vitaminc_label_to_stored("SUPPORTS") == "support"
    assert vitaminc_label_to_stored("REFUTES") == "refute"
    assert vitaminc_label_to_stored("NOT ENOUGH INFO") == "neutral"
    assert stored_label_to_base_index("support") == 1
    assert stored_label_to_base_index("refute") == 0
    assert stored_label_to_base_index("neutral") == 2
    assert base_probabilities_to_stored((0.2, 0.7, 0.1)) == (0.7, 0.2, 0.1)
    with pytest.raises(ValidationError, match="unsupported VitaminC"):
        vitaminc_label_to_stored("NOT_SUPPORTED")


def test_endpoint_and_paired_losses_match_hand_computation() -> None:
    raw = ((2.0, 0.0, -1.0), (-0.5, 1.5, 0.25))
    logits = torch.tensor(raw, dtype=torch.float64, requires_grad=True)
    # REFUTE -> base index 0, SUPPORT -> base index 1.
    labels = torch.tensor((0, 1), dtype=torch.long)
    weights = torch.tensor((1.5, 0.75, 2.0), dtype=torch.float64)
    left_q = _log_softmax(raw[0])
    right_q = _log_softmax(raw[1])

    expected_ce = (-1.5 * left_q[0] - 0.75 * right_q[1]) / 2.0
    expected_pair = 0.5 * (
        math.log1p(math.exp(0.5 - (left_q[0] - right_q[0])))
        + math.log1p(math.exp(0.5 - (right_q[1] - left_q[1])))
    )
    endpoint = endpoint_cross_entropy(logits, labels, weights)
    paired = paired_revision_margin_loss(logits, labels, ((0, 1),), margin=0.5)
    composed = change_aware_loss(
        logits,
        labels,
        weights,
        ((0, 1),),
        include_paired_margin=True,
        margin=0.5,
        paired_weight=0.25,
    )

    assert float(endpoint.detach()) == pytest.approx(expected_ce)
    assert float(paired.detach()) == pytest.approx(expected_pair)
    assert float(composed.total.detach()) == pytest.approx(
        expected_ce + 0.25 * expected_pair
    )


def test_paired_loss_has_finite_gradients_and_rewards_correct_version_gaps() -> None:
    labels = torch.tensor((0, 1), dtype=torch.long)
    weak = torch.tensor(
        ((0.1, 0.0, -0.1), (0.0, 0.1, -0.1)),
        dtype=torch.float64,
        requires_grad=True,
    )
    strong = torch.tensor(
        ((3.0, -2.0, -0.1), (-2.0, 3.0, -0.1)),
        dtype=torch.float64,
    )
    weak_loss = paired_revision_margin_loss(weak, labels, ((0, 1),))
    strong_loss = paired_revision_margin_loss(strong, labels, ((0, 1),))
    weak_loss.backward()

    assert float(strong_loss) < float(weak_loss.detach())
    assert weak.grad is not None
    assert bool(torch.all(torch.isfinite(weak.grad)))
    assert float(torch.linalg.vector_norm(weak.grad)) > 0.0

    shifted = strong + torch.tensor(((100.0,), (-40.0,)), dtype=torch.float64)
    shifted_loss = paired_revision_margin_loss(shifted, labels, ((0, 1),))
    assert float(shifted_loss) == pytest.approx(float(strong_loss), abs=1e-12)


def test_paired_loss_rejects_same_label_or_duplicate_transitions() -> None:
    logits = torch.zeros((2, 3), dtype=torch.float64)
    same = torch.tensor((1, 1), dtype=torch.long)
    with pytest.raises(ValidationError, match="different labels"):
        paired_revision_margin_loss(logits, same, ((0, 1),))
    labels = torch.tensor((0, 1), dtype=torch.long)
    with pytest.raises(ValidationError, match="duplicated"):
        paired_revision_margin_loss(logits, labels, ((0, 1), (0, 1)))
