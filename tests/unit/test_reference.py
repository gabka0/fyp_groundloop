"""Table-driven truth-table tests for the reference recomputation."""

from helpers import (
    NEUTRAL_SCORES,
    REFUTE_SCORES,
    SUPPORT_SCORES,
    insert_document,
    make_repo,
    observe,
    register_answer,
)

from groundloop.domain import AnswerStatus, ClaimStatus
from groundloop.reference import compute_all_states


def test_claim_truth_table_all_rows() -> None:
    repo = make_repo()
    register_answer(
        repo,
        claims=(
            ("c-sup", True),
            ("c-ref", True),
            ("c-con", True),
            ("c-none", True),
        ),
    )
    insert_document(
        repo,
        "ev-1",
        "doc",
        "dv1",
        (("p1", "alpha"), ("p2", "beta"), ("p3", "gamma"), ("p4", "delta")),
    )
    observe(repo, "ev-2", "o1", "c-sup", "p1", SUPPORT_SCORES)
    observe(repo, "ev-3", "o2", "c-ref", "p2", REFUTE_SCORES)
    observe(repo, "ev-4", "o3", "c-con", "p3", SUPPORT_SCORES)
    observe(repo, "ev-5", "o4", "c-con", "p4", REFUTE_SCORES)

    claim_states, _ = compute_all_states(repo)
    assert claim_states["c-sup"].status is ClaimStatus.SUPPORTED
    assert claim_states["c-sup"].support_count == 1
    assert claim_states["c-sup"].refute_count == 0
    assert claim_states["c-sup"].supporting_observation_ids == ("o1",)
    assert claim_states["c-ref"].status is ClaimStatus.REFUTED
    assert claim_states["c-ref"].refuting_observation_ids == ("o2",)
    assert claim_states["c-con"].status is ClaimStatus.CONFLICTED
    assert claim_states["c-con"].support_count == 1
    assert claim_states["c-con"].refute_count == 1
    assert claim_states["c-none"].status is ClaimStatus.UNSUPPORTED
    assert claim_states["c-none"].supporting_observation_ids == ()


def test_answer_precedence_contradicted_beats_conflicted() -> None:
    repo = make_repo()
    register_answer(repo, claims=(("c1", True), ("c2", True)))
    insert_document(repo, "ev-1", "doc", "dv1", (("p1", "a"), ("p2", "b"), ("p3", "c")))
    observe(repo, "ev-2", "o1", "c1", "p1", REFUTE_SCORES)  # c1 REFUTED
    observe(repo, "ev-3", "o2", "c2", "p2", SUPPORT_SCORES)
    observe(repo, "ev-4", "o3", "c2", "p3", REFUTE_SCORES)  # c2 CONFLICTED

    _, answer_states = compute_all_states(repo)
    state = answer_states["a1"]
    assert state.status is AnswerStatus.CONTRADICTED
    assert state.required_claim_count == 2
    assert state.refuted_count == 1
    assert state.conflicted_count == 1


def test_answer_precedence_branches() -> None:
    # all supported -> VALID
    repo = make_repo()
    register_answer(repo, claims=(("c1", True), ("c2", True)))
    insert_document(repo, "ev-1", "doc", "dv1", (("p1", "a"), ("p2", "b")))
    observe(repo, "ev-2", "o1", "c1", "p1", SUPPORT_SCORES)
    observe(repo, "ev-3", "o2", "c2", "p2", SUPPORT_SCORES)
    _, answers = compute_all_states(repo)
    assert answers["a1"].status is AnswerStatus.VALID

    # one supported, one unsupported -> PARTIALLY_SUPPORTED
    repo = make_repo()
    register_answer(repo, claims=(("c1", True), ("c2", True)))
    insert_document(repo, "ev-1", "doc", "dv1", (("p1", "a"),))
    observe(repo, "ev-2", "o1", "c1", "p1", SUPPORT_SCORES)
    _, answers = compute_all_states(repo)
    assert answers["a1"].status is AnswerStatus.PARTIALLY_SUPPORTED

    # conflicted without refuted -> CONFLICTED
    repo = make_repo()
    register_answer(repo, claims=(("c1", True), ("c2", True)))
    insert_document(repo, "ev-1", "doc", "dv1", (("p1", "a"), ("p2", "b")))
    observe(repo, "ev-2", "o1", "c1", "p1", SUPPORT_SCORES)
    observe(repo, "ev-3", "o2", "c1", "p2", REFUTE_SCORES)
    _, answers = compute_all_states(repo)
    assert answers["a1"].status is AnswerStatus.CONFLICTED

    # nothing supported -> UNSUPPORTED
    repo = make_repo()
    register_answer(repo, claims=(("c1", True),))
    _, answers = compute_all_states(repo)
    assert answers["a1"].status is AnswerStatus.UNSUPPORTED


def test_optional_claims_never_affect_answer_status() -> None:
    repo = make_repo()
    register_answer(repo, claims=(("c-req", True), ("c-opt", False)))
    insert_document(repo, "ev-1", "doc", "dv1", (("p1", "a"), ("p2", "b")))
    observe(repo, "ev-2", "o1", "c-req", "p1", SUPPORT_SCORES)
    observe(repo, "ev-3", "o2", "c-opt", "p2", REFUTE_SCORES)

    claim_states, answer_states = compute_all_states(repo)
    assert claim_states["c-opt"].status is ClaimStatus.REFUTED
    state = answer_states["a1"]
    assert state.status is AnswerStatus.VALID
    assert state.required_claim_count == 1


def test_distinct_content_counting_deduplicates_identical_text() -> None:
    """Two active chunks with identical normalized text are one witness."""
    repo = make_repo()
    register_answer(repo)
    insert_document(repo, "ev-1", "doc-a", "dv-a", (("p1", "same  text here"),))
    insert_document(repo, "ev-2", "doc-b", "dv-b", (("p2", "same text here"),))
    observe(repo, "ev-3", "o1", "c1", "p1", SUPPORT_SCORES)
    observe(repo, "ev-4", "o2", "c1", "p2", SUPPORT_SCORES)

    claim_states, _ = compute_all_states(repo)
    state = claim_states["c1"]
    assert state.support_count == 1  # distinct text_hash, not record count
    assert state.supporting_observation_ids == ("o1", "o2")
    assert state.status is ClaimStatus.SUPPORTED


def test_neutral_contributes_to_neither_count_but_is_auditable() -> None:
    repo = make_repo()
    register_answer(repo)
    insert_document(repo, "ev-1", "doc", "dv1", (("p1", "a"),))
    observe(repo, "ev-2", "o1", "c1", "p1", NEUTRAL_SCORES)

    claim_states, answer_states = compute_all_states(repo)
    state = claim_states["c1"]
    assert state.support_count == 0
    assert state.refute_count == 0
    assert state.status is ClaimStatus.UNSUPPORTED
    assert answer_states["a1"].status is AnswerStatus.UNSUPPORTED
    assert repo.observation("o1").neutral_score == NEUTRAL_SCORES[2]
    assert repo.observations_for_chunk("p1") == ("o1",)
