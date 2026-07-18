"""Adversarial differential tests for the M2 signed-delta engine."""

from copy import deepcopy

import pytest
from helpers import (
    REFUTE_SCORES,
    SUPPORT_SCORES,
    insert_document,
    make_observation,
    make_repo,
    register_answer,
)

from groundloop.differential import DifferentialRunner
from groundloop.domain import AnswerStatus, ClaimStatus, DecisionPolicy
from groundloop.events import (
    DeleteDocumentVersionEvent,
    ObserveEvent,
    PolicyChangeEvent,
)


def _runner_with_two_witnesses(*, same_text: bool = False) -> DifferentialRunner:
    repo = make_repo()
    register_answer(repo)
    text_b = "same evidence" if same_text else "independent evidence"
    insert_document(repo, "ev-insert-a", "doc-a", "dv-a", (("p-a", "same evidence"),))
    insert_document(repo, "ev-insert-b", "doc-b", "dv-b", (("p-b", text_b),))
    runner = DifferentialRunner.from_repository(repo)
    runner.apply(
        ObserveEvent(
            event_id="ev-observe-a",
            observation=make_observation("o-a", "c1", "p-a", SUPPORT_SCORES),
        )
    )
    runner.apply(
        ObserveEvent(
            event_id="ev-observe-b",
            observation=make_observation("o-b", "c1", "p-b", SUPPORT_SCORES),
        )
    )
    return runner


def test_alternative_witness_withdrawal_uses_zero_crossings() -> None:
    runner = _runner_with_two_witnesses()

    deltas = runner.apply(
        DeleteDocumentVersionEvent(event_id="ev-delete-a", document_version_id="dv-a")
    )
    state = runner.engine.claim_states["c1"]
    assert deltas == ()
    assert state.support_count == 1
    assert state.best_support_score == SUPPORT_SCORES[0]
    assert state.status is ClaimStatus.SUPPORTED
    assert runner.engine.answer_states["a1"].status is AnswerStatus.VALID
    assert runner.engine.last_stats.claim_keys_touched == 1
    assert runner.engine.last_stats.answer_keys_touched == 0
    assert runner.engine.last_stats.distinct_hash_crossings == 1

    runner.apply(
        DeleteDocumentVersionEvent(event_id="ev-delete-b", document_version_id="dv-b")
    )
    final_claim_status: ClaimStatus = runner.engine.claim_states["c1"].status
    final_answer_status: AnswerStatus = runner.engine.answer_states["a1"].status
    assert final_claim_status is ClaimStatus.UNSUPPORTED
    assert final_answer_status is AnswerStatus.UNSUPPORTED
    assert runner.engine.last_stats.answer_keys_touched == 1


def test_duplicate_content_refcount_does_not_fake_a_second_witness() -> None:
    runner = _runner_with_two_witnesses(same_text=True)
    assert runner.engine.claim_states["c1"].support_count == 1

    runner.apply(
        DeleteDocumentVersionEvent(event_id="ev-delete-a", document_version_id="dv-a")
    )
    state = runner.engine.claim_states["c1"]
    assert state.support_count == 1
    assert state.supporting_observation_ids == ("o-b",)
    assert runner.engine.last_stats.distinct_hash_crossings == 0


def test_supersession_updates_best_score_and_repairs_certificate() -> None:
    repo = make_repo()
    register_answer(repo)
    insert_document(repo, "ev-insert", "doc", "dv", (("p", "evidence"),))
    runner = DifferentialRunner.from_repository(repo)

    first = ObserveEvent(
        event_id="ev-observe-1",
        observation=make_observation("o1", "c1", "p", (0.91, 0.02, 0.07)),
    )
    runner.apply(first)
    assert runner.engine.claim_states["c1"].best_support_score == 0.91
    assert runner.engine.certificates["c1"].support_observation_id == "o1"

    runner.apply(
        ObserveEvent(
            event_id="ev-observe-2",
            observation=make_observation("o2", "c1", "p", REFUTE_SCORES),
        )
    )
    state = runner.engine.claim_states["c1"]
    assert state.best_support_score is None
    assert state.best_refute_score == REFUTE_SCORES[1]
    assert state.status is ClaimStatus.REFUTED
    certificate = runner.engine.certificates["c1"]
    assert certificate.support_observation_id is None
    assert certificate.refute_observation_id == "o2"

    # Exact replay must return its old deltas without applying the signed
    # contribution twice.
    snapshot = deepcopy(runner.engine)
    runner.apply(first)
    assert runner.engine == snapshot


def test_policy_range_index_touches_only_threshold_crossers() -> None:
    repo = make_repo(support_threshold=0.8)
    register_answer(repo, claims=(("c1", True), ("c2", True), ("c3", True)))
    insert_document(
        repo,
        "ev-insert",
        "doc",
        "dv",
        (("p1", "one"), ("p2", "two"), ("p3", "three")),
    )
    runner = DifferentialRunner.from_repository(repo)
    for index, support_score in enumerate((0.7, 0.9, 0.2), start=1):
        runner.apply(
            ObserveEvent(
                event_id=f"ev-observe-{index}",
                observation=make_observation(
                    f"o{index}",
                    f"c{index}",
                    f"p{index}",
                    (support_score, 0.05, 0.05),
                ),
            )
        )

    runner.apply(
        PolicyChangeEvent(
            event_id="ev-policy-lower",
            policy=DecisionPolicy(
                policy_version="k-lower",
                support_threshold=0.6,
                refute_threshold=0.8,
            ),
        )
    )
    assert runner.engine.last_stats.policy_index_candidates == 1
    assert runner.engine.last_stats.decision_flips == 1
    assert runner.engine.last_stats.claim_keys_touched == 1
    assert runner.engine.claim_states["c1"].status is ClaimStatus.SUPPORTED


@pytest.mark.parametrize("failure_stage", ["reference_applied", "incremental_applied"])
def test_differential_publication_is_atomic(failure_stage: str) -> None:
    repo = make_repo()
    register_answer(repo)
    insert_document(repo, "ev-insert", "doc", "dv", (("p", "evidence"),))
    runner = DifferentialRunner.from_repository(repo)
    repository_before = deepcopy(runner.repository)
    engine_before = deepcopy(runner.engine)

    def fail(stage: str) -> None:
        if stage == failure_stage:
            raise RuntimeError("injected differential failure")

    with pytest.raises(RuntimeError, match="injected differential failure"):
        runner.apply(
            ObserveEvent(
                event_id="ev-observe",
                observation=make_observation("o", "c1", "p", SUPPORT_SCORES),
            ),
            failure_injector=fail,
        )

    assert runner.repository == repository_before
    assert runner.engine == engine_before
