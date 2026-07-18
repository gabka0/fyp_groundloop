"""M1 vertical-slice acceptance scenarios (v0.2 Section 18, M1)."""

import pytest
from helpers import (
    REFUTE_SCORES,
    SUPPORT_SCORES,
    insert_document,
    make_repo,
    observe,
    register_answer,
)

from groundloop.domain import (
    AnswerStatus,
    ClaimStatus,
    DecisionPolicy,
    StatusDelta,
)
from groundloop.errors import EventConflictError
from groundloop.events import (
    ChunkInput,
    DeleteDocumentVersionEvent,
    PolicyChangeEvent,
    ReplaceDocumentVersionEvent,
    apply_event,
)
from groundloop.reference import compute_all_states


def test_nimbus_replacement_scenario() -> None:
    """v1 supports the claim; the replacement version refutes it."""
    repo = make_repo()
    register_answer(repo)
    insert_document(
        repo,
        "ev-insert",
        "nimbus-docs",
        "dv1",
        (("p1", "Nimbus supports Python 3.10 and later."),),
    )

    deltas = observe(repo, "ev-observe-1", "o1", "c1", "p1", SUPPORT_SCORES)
    assert deltas == (
        StatusDelta(
            event_id="ev-observe-1",
            object_type="claim",
            object_id="c1",
            old_status="unsupported",
            new_status="supported",
            reason="event=ev-observe-1 op=observe observation=o1",
        ),
        StatusDelta(
            event_id="ev-observe-1",
            object_type="answer",
            object_id="a1",
            old_status="unsupported",
            new_status="valid",
            reason="event=ev-observe-1 op=observe observation=o1",
        ),
    )

    replace = ReplaceDocumentVersionEvent(
        event_id="ev-replace",
        document_id="nimbus-docs",
        old_document_version_id="dv1",
        new_document_version_id="dv2",
        content_hash="hash-dv2",
        chunks=(
            ChunkInput(
                chunk_version_id="p2",
                chunk_index=0,
                text="Nimbus requires Python 3.12 or later.",
            ),
        ),
    )
    replace_deltas = apply_event(repo, replace)
    # The old observation stops contributing; no new observation yet.
    assert [(d.object_id, d.old_status, d.new_status) for d in replace_deltas] == [
        ("c1", "supported", "unsupported"),
        ("a1", "valid", "unsupported"),
    ]

    refute_deltas = observe(repo, "ev-observe-2", "o2", "c1", "p2", REFUTE_SCORES)
    assert [(d.object_id, d.new_status) for d in refute_deltas] == [
        ("c1", "refuted"),
        ("a1", "contradicted"),
    ]

    claim_states, answer_states = compute_all_states(repo)
    state = claim_states["c1"]
    assert state.status is ClaimStatus.REFUTED
    assert state.support_count == 0
    assert state.refute_count == 1
    assert state.refuting_observation_ids == ("o2",)
    assert answer_states["a1"].status is AnswerStatus.CONTRADICTED

    # History remains auditable after replacement.
    assert repo.chunk_version("p1").text == "Nimbus supports Python 3.10 and later."
    assert not repo.is_chunk_active("p1")
    assert repo.is_chunk_active("p2")
    assert repo.observations_for_chunk("p1") == ("o1",)
    assert repo.document_versions_of("nimbus-docs") == ("dv1", "dv2")

    # Replay is idempotent: same recorded result, no growth anywhere.
    deltas_before = len(repo.status_deltas)
    assert apply_event(repo, replace) == replace_deltas
    assert len(repo.status_deltas) == deltas_before
    assert repo.document_versions_of("nimbus-docs") == ("dv1", "dv2")

    # Same event identifier with a different payload is a conflict.
    conflicting = ReplaceDocumentVersionEvent(
        event_id="ev-replace",
        document_id="nimbus-docs",
        old_document_version_id="dv1",
        new_document_version_id="dv3",
        content_hash="hash-dv3",
        chunks=(),
    )
    with pytest.raises(EventConflictError):
        apply_event(repo, conflicting)


def test_alternative_witness_prevents_false_invalidation() -> None:
    repo = make_repo()
    register_answer(repo)
    insert_document(repo, "ev-1", "doc-a", "dv-a", (("p1", "alpha evidence"),))
    insert_document(repo, "ev-2", "doc-b", "dv-b", (("p2", "beta evidence"),))
    observe(repo, "ev-3", "o1", "c1", "p1", SUPPORT_SCORES)
    observe(repo, "ev-4", "o2", "c1", "p2", SUPPORT_SCORES)

    claim_states, _ = compute_all_states(repo)
    assert claim_states["c1"].support_count == 2

    # Deleting one of two witnesses: count 2 -> 1, no status change, no deltas.
    deltas = apply_event(
        repo, DeleteDocumentVersionEvent(event_id="ev-5", document_version_id="dv-b")
    )
    assert deltas == ()
    claim_states, answer_states = compute_all_states(repo)
    assert claim_states["c1"].support_count == 1
    assert claim_states["c1"].status is ClaimStatus.SUPPORTED
    assert answer_states["a1"].status is AnswerStatus.VALID

    # Deleting the final witness crosses the zero boundary.
    deltas = apply_event(
        repo, DeleteDocumentVersionEvent(event_id="ev-6", document_version_id="dv-a")
    )
    assert [(d.object_id, d.new_status) for d in deltas] == [
        ("c1", "unsupported"),
        ("a1", "unsupported"),
    ]


def test_policy_change_flips_label_with_zero_new_observations() -> None:
    repo = make_repo(support_threshold=0.8)
    register_answer(repo)
    insert_document(repo, "ev-1", "doc", "dv1", (("p1", "evidence"),))
    # Score below the k1 support threshold: NEUTRAL -> claim UNSUPPORTED.
    observe(repo, "ev-2", "o1", "c1", "p1", (0.7, 0.05, 0.05))
    claim_states, _ = compute_all_states(repo)
    assert claim_states["c1"].status is ClaimStatus.UNSUPPORTED

    observation_count = len(repo.observations_for_chunk("p1"))
    deltas = apply_event(
        repo,
        PolicyChangeEvent(
            event_id="ev-policy-2",
            policy=DecisionPolicy(
                policy_version="k2",
                support_threshold=0.6,
                refute_threshold=0.8,
            ),
        ),
    )
    assert [(d.object_type, d.object_id, d.new_status) for d in deltas] == [
        ("claim", "c1", "supported"),
        ("answer", "a1", "valid"),
    ]
    assert deltas[0].reason == "event=ev-policy-2 op=policy_change policy=k2"
    # The zero-neural-call property: no observation was created or mutated.
    assert len(repo.observations_for_chunk("p1")) == observation_count
    assert repo.current_policy().policy_version == "k2"


def test_supersession_prevents_double_counting_and_allows_retry_flip() -> None:
    repo = make_repo()
    register_answer(repo)
    insert_document(repo, "ev-1", "doc", "dv1", (("p1", "evidence"),))
    observe(repo, "ev-2", "o1", "c1", "p1", SUPPORT_SCORES)

    # A retry for the same (claim, chunk, task) key supersedes; it must not
    # produce a second witness.
    observe(repo, "ev-3", "o2", "c1", "p1", SUPPORT_SCORES)
    claim_states, _ = compute_all_states(repo)
    state = claim_states["c1"]
    assert state.support_count == 1
    assert state.supporting_observation_ids == ("o2",)
    assert repo.is_observation_superseded("o1")

    # A retry that disagrees replaces the verdict instead of creating an
    # artificial conflict (review counterexample CE-5/CE-9).
    deltas = observe(repo, "ev-4", "o3", "c1", "p1", REFUTE_SCORES)
    assert [(d.object_id, d.new_status) for d in deltas] == [
        ("c1", "refuted"),
        ("a1", "contradicted"),
    ]
    claim_states, _ = compute_all_states(repo)
    state = claim_states["c1"]
    assert state.support_count == 0
    assert state.refute_count == 1
    # Not CONFLICTED: the superseded SUPPORT no longer counts.
    assert state.status is ClaimStatus.REFUTED


def test_multi_claim_answer_and_conflict_precedence() -> None:
    repo = make_repo()
    register_answer(repo, claims=(("c1", True), ("c2", True), ("c3", False)))
    insert_document(
        repo, "ev-1", "doc", "dv1", (("p1", "a"), ("p2", "b"), ("p3", "c"))
    )
    observe(repo, "ev-2", "o1", "c1", "p1", SUPPORT_SCORES)
    observe(repo, "ev-3", "o2", "c1", "p2", REFUTE_SCORES)  # c1 CONFLICTED
    observe(repo, "ev-4", "o3", "c2", "p3", SUPPORT_SCORES)  # c2 SUPPORTED

    claim_states, answer_states = compute_all_states(repo)
    assert claim_states["c1"].status is ClaimStatus.CONFLICTED
    assert claim_states["c2"].status is ClaimStatus.SUPPORTED
    assert answer_states["a1"].status is AnswerStatus.CONFLICTED
    assert answer_states["a1"].required_claim_count == 2
