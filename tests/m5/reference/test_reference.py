from __future__ import annotations

import itertools
from dataclasses import replace

from groundloop.domain import (
    AnswerStatus,
    ClaimStatus,
    DecisionPolicy,
    SemanticObservation,
    SubjectKind,
)
from groundloop.events import (
    DeleteDocumentVersionEvent,
    ObserveEvent,
    PolicyChangeEvent,
    apply_event,
)
from groundloop.m5.domain import (
    ClaimSupportKind,
    GroupCertificateRow,
    GroupMatchingCertificateArtifact,
    SnapshotPoint,
)
from groundloop.m5.events import (
    ObserveRequirementEvent,
    RegisterGroupEvent,
    RetireGroupEvent,
    apply_m5_event,
)
from groundloop.m5.reference import (
    build_reference_claim_certificate,
    build_reference_group_certificate,
    compute_reference_states,
    derive_active_requirement_witnesses,
    maximum_matching_with_unmatched_branch,
    validate_claim_certificate,
    validate_group_certificate,
)
from groundloop.m5.repository import M5Repository

from .conftest import (
    REFUTE,
    STAMP,
    SUPPORT,
    add_document,
    make_base,
    make_group,
    make_requirement_observation,
    sha,
)


def _independent_matching_size(neighbors: tuple[tuple[str, ...], ...]) -> int:
    hashes = tuple(sorted({value for row in neighbors for value in row}))
    maximum = 0
    for size in range(1, min(len(neighbors), len(hashes)) + 1):
        for left in itertools.combinations(range(len(neighbors)), size):
            for right in itertools.permutations(hashes, size):
                if all(
                    value in neighbors[ordinal]
                    for ordinal, value in zip(left, right, strict=True)
                ):
                    maximum = size
                    break
            if maximum == size:
                break
    return maximum


def _observe_edge(
    repo: M5Repository,
    *,
    event_id: str,
    requirement_id: str,
    chunk_id: str,
    observation_id: str | None = None,
    scores: tuple[float, float, float] = SUPPORT,
) -> None:
    apply_m5_event(
        repo,
        ObserveRequirementEvent(
            event_id=event_id,
            observation=make_requirement_observation(
                observation_id=observation_id or f"o-{event_id}",
                requirement_id=requirement_id,
                chunk_id=chunk_id,
                scores=scores,
            ),
        ),
    )


def _observe_claim(
    repo: M5Repository,
    *,
    event_id: str,
    observation_id: str,
    chunk_id: str,
    scores: tuple[float, float, float],
) -> None:
    apply_event(
        repo.base,
        ObserveEvent(
            event_id=event_id,
            observation=SemanticObservation(
                observation_id=observation_id,
                subject_kind=SubjectKind.CLAIM,
                subject_id="c1",
                chunk_version_id=chunk_id,
                task_type="verify",
                support_score=scores[0],
                refute_score=scores[1],
                neutral_score=scores[2],
                producer=STAMP,
                input_hash=f"input-{observation_id}",
            ),
        ),
    )


def test_unmatched_branch_finds_maximum_partial_matching() -> None:
    matching = maximum_matching_with_unmatched_branch(((), ("a",)))
    assert matching.size == 1
    assert matching.assignment_by_ordinal == (None, "a")
    empty = maximum_matching_with_unmatched_branch(((), (), ()))
    assert empty.size == 0
    assert empty.assignment_by_ordinal == (None, None, None)


def test_hall_counterexample_is_incomplete_despite_union_size_four() -> None:
    neighbors = (
        ("a", "b"),
        ("a", "b"),
        ("a", "b"),
        ("c", "d"),
    )
    matching = maximum_matching_with_unmatched_branch(neighbors)
    assert len({value for row in neighbors for value in row}) == 4
    assert matching.size == 3


def test_reference_matches_independent_enumerator_for_every_graph_through_four() -> (
    None
):
    graph_count = 0
    for requirement_count in range(1, 5):
        for hash_count in range(5):
            hashes = tuple(chr(ord("a") + index) for index in range(hash_count))
            edge_slots = requirement_count * hash_count
            for bits in range(1 << edge_slots):
                neighbors = tuple(
                    tuple(
                        hashes[hash_index]
                        for hash_index in range(hash_count)
                        if bits & (1 << (ordinal * hash_count + hash_index))
                    )
                    for ordinal in range(requirement_count)
                )
                expected = _independent_matching_size(neighbors)
                actual = maximum_matching_with_unmatched_branch(neighbors)
                assert actual.size == expected
                selected = tuple(
                    value for value in actual.assignment_by_ordinal if value is not None
                )
                assert len(selected) == len(set(selected)) == actual.size
                assert all(
                    value is None or value in neighbors[ordinal]
                    for ordinal, value in enumerate(actual.assignment_by_ordinal)
                )
                graph_count += 1
    assert graph_count == 74_958


def test_sparse_and_dense_eight_requirement_smoke() -> None:
    sparse = tuple((f"h{ordinal}",) for ordinal in range(8))
    dense = tuple(tuple(f"h{index}" for index in range(8)) for _ in range(8))
    assert maximum_matching_with_unmatched_branch(sparse).size == 8
    assert maximum_matching_with_unmatched_branch(dense).size == 8


def test_oracle_derives_hall_state_from_current_observations_only() -> None:
    base = make_base()
    add_document(
        base,
        event_id="document",
        document_id="doc",
        version_id="dv",
        chunks=(("a", "A"), ("b", "B"), ("c", "C"), ("d", "D")),
    )
    repo = M5Repository(base)
    group = make_group(
        texts=("r0", "r1", "r2", "r3"),
        requirement_ids=("r0", "r1", "r2", "r3"),
    )
    apply_m5_event(repo, RegisterGroupEvent(event_id="register", group=group))
    for requirement_id, chunk_id in (
        ("r0", "a"),
        ("r0", "b"),
        ("r1", "a"),
        ("r1", "b"),
        ("r2", "a"),
        ("r2", "b"),
        ("r3", "c"),
        ("r3", "d"),
    ):
        _observe_edge(
            repo,
            event_id=f"{requirement_id}-{chunk_id}",
            requirement_id=requirement_id,
            chunk_id=chunk_id,
        )

    states = compute_reference_states(repo)
    assert all(state.satisfied for state in states.requirements.values())
    assert states.groups["g1"].satisfied_count == 4
    assert states.groups["g1"].matching_size == 3
    assert not states.groups["g1"].complete
    assert states.claims["c1"].status is ClaimStatus.UNSUPPORTED
    assert len(derive_active_requirement_witnesses(repo)) == 8


def test_single_edge_loss_changes_completeness_without_requirement_zero_crossing() -> (
    None
):
    base = make_base()
    add_document(
        base,
        event_id="document",
        document_id="doc",
        version_id="dv",
        chunks=(("a", "A"), ("b", "B"), ("c", "C"), ("d", "D")),
    )
    repo = M5Repository(base)
    group = make_group(
        texts=("r0", "r1", "r2", "r3"),
        requirement_ids=("r0", "r1", "r2", "r3"),
    )
    apply_m5_event(repo, RegisterGroupEvent(event_id="register", group=group))
    edges = (
        ("r0", "a"),
        ("r0", "b"),
        ("r1", "a"),
        ("r1", "b"),
        ("r2", "a"),
        ("r2", "b"),
        ("r2", "c"),
        ("r3", "c"),
        ("r3", "d"),
    )
    for requirement_id, chunk_id in edges:
        _observe_edge(
            repo,
            event_id=f"{requirement_id}-{chunk_id}",
            requirement_id=requirement_id,
            chunk_id=chunk_id,
            observation_id=f"old-{requirement_id}-{chunk_id}",
        )
    before = compute_reference_states(repo)
    assert before.groups["g1"].complete
    assert before.claims["c1"].status is ClaimStatus.SUPPORTED

    _observe_edge(
        repo,
        event_id="remove-r2-c",
        requirement_id="r2",
        chunk_id="c",
        observation_id="new-r2-c",
        scores=REFUTE,
    )
    after = compute_reference_states(repo)
    assert all(state.satisfied for state in after.requirements.values())
    assert after.requirements["r2"].witness_count == 2
    assert after.groups["g1"].matching_size == 3
    assert not after.groups["g1"].complete
    assert after.claims["c1"].status is ClaimStatus.UNSUPPORTED


def test_group_only_support_keeps_direct_fields_empty_and_refute_is_direct_only(
    m5_repo: M5Repository,
) -> None:
    apply_m5_event(
        m5_repo,
        RegisterGroupEvent(event_id="register", group=make_group(texts=("one",))),
    )
    _observe_edge(
        m5_repo,
        event_id="group-support",
        requirement_id="g1-r0",
        chunk_id="h1",
    )
    group_only = compute_reference_states(m5_repo)
    state = group_only.claims["c1"]
    assert state.support_count == 0
    assert state.best_support_score is None
    assert state.supporting_observation_ids == ()
    assert state.complete_group_count == 1
    assert state.complete_group_ids == ("g1",)
    assert state.status is ClaimStatus.SUPPORTED
    assert group_only.answers["a1"].status is AnswerStatus.VALID

    _observe_claim(
        m5_repo,
        event_id="direct-refute",
        observation_id="claim-refute",
        chunk_id="h2",
        scores=REFUTE,
    )
    conflicted = compute_reference_states(m5_repo)
    state = conflicted.claims["c1"]
    assert state.refute_count == 1
    assert state.status is ClaimStatus.CONFLICTED
    assert conflicted.answers["a1"].status is AnswerStatus.CONFLICTED


def test_direct_support_and_alternative_groups_preserve_support(
    m5_repo: M5Repository,
) -> None:
    first = make_group(group_id="g1", family_id="f1", texts=("one",))
    second = make_group(group_id="g2", family_id="f2", texts=("two",))
    apply_m5_event(m5_repo, RegisterGroupEvent(event_id="register-1", group=first))
    apply_m5_event(m5_repo, RegisterGroupEvent(event_id="register-2", group=second))
    _observe_edge(m5_repo, event_id="edge-1", requirement_id="g1-r0", chunk_id="h1")
    _observe_edge(m5_repo, event_id="edge-2", requirement_id="g2-r0", chunk_id="h2")
    _observe_claim(
        m5_repo,
        event_id="direct-support",
        observation_id="claim-support",
        chunk_id="h3",
        scores=SUPPORT,
    )
    state = compute_reference_states(m5_repo).claims["c1"]
    assert state.support_count == 1
    assert state.complete_group_ids == ("g1", "g2")

    retirement = apply_m5_event(
        m5_repo, RetireGroupEvent(event_id="retire-1", group_version_id="g1")
    )
    assert retirement.deltas == ()
    after = compute_reference_states(m5_repo).claims["c1"]
    assert after.status is ClaimStatus.SUPPORTED
    assert after.complete_group_ids == ("g2",)


def test_two_distinct_perfect_matching_certificates_both_validate() -> None:
    base = make_base()
    add_document(
        base,
        event_id="document",
        document_id="doc",
        version_id="dv",
        chunks=(("a", "A"), ("b", "B")),
    )
    repo = M5Repository(base)
    apply_m5_event(repo, RegisterGroupEvent(event_id="register", group=make_group()))
    observations: dict[tuple[str, str], str] = {}
    for requirement_id in ("g1-r0", "g1-r1"):
        for chunk_id in ("a", "b"):
            observation_id = f"o-{requirement_id}-{chunk_id}"
            observations[(requirement_id, chunk_id)] = observation_id
            _observe_edge(
                repo,
                event_id=f"e-{requirement_id}-{chunk_id}",
                requirement_id=requirement_id,
                chunk_id=chunk_id,
                observation_id=observation_id,
            )

    hash_a, hash_b = sha("A"), sha("B")
    certificate_one = GroupMatchingCertificateArtifact(
        decision_policy_version="policy-v1",
        group_version_id="g1",
        rows=(
            GroupCertificateRow(0, "g1-r0", hash_a, observations[("g1-r0", "a")]),
            GroupCertificateRow(1, "g1-r1", hash_b, observations[("g1-r1", "b")]),
        ),
    )
    certificate_two = GroupMatchingCertificateArtifact(
        decision_policy_version="policy-v1",
        group_version_id="g1",
        rows=(
            GroupCertificateRow(0, "g1-r0", hash_b, observations[("g1-r0", "b")]),
            GroupCertificateRow(1, "g1-r1", hash_a, observations[("g1-r1", "a")]),
        ),
    )
    assert validate_group_certificate(repo, certificate_one)
    assert validate_group_certificate(repo, certificate_two)
    built = build_reference_group_certificate(repo, "g1")
    assert built in (certificate_one, certificate_two)


def test_certificate_validation_is_historical_policy_bound_and_never_raises(
    m5_repo: M5Repository,
) -> None:
    apply_m5_event(
        m5_repo,
        RegisterGroupEvent(event_id="register", group=make_group(texts=("one",))),
    )
    _observe_edge(
        m5_repo,
        event_id="edge",
        requirement_id="g1-r0",
        chunk_id="h1",
        observation_id="selected",
    )
    bound_point = m5_repo.current_point
    certificate = build_reference_group_certificate(m5_repo, "g1")
    assert certificate is not None
    assert validate_group_certificate(m5_repo, certificate, bound_point)

    _observe_edge(
        m5_repo,
        event_id="supersede",
        requirement_id="g1-r0",
        chunk_id="h1",
        observation_id="replacement",
        scores=REFUTE,
    )
    assert validate_group_certificate(m5_repo, certificate, bound_point)
    assert not validate_group_certificate(m5_repo, certificate)
    assert not validate_group_certificate(
        m5_repo, certificate, SnapshotPoint(m5_repo.current_epoch + 1, 0)
    )

    missing = GroupMatchingCertificateArtifact(
        decision_policy_version="policy-v1",
        group_version_id="g1",
        rows=(GroupCertificateRow(0, "g1-r0", sha("alpha"), "missing-observation"),),
    )
    assert not validate_group_certificate(m5_repo, missing)


def test_policy_version_change_invalidates_old_certificate_even_without_label_flip(
    m5_repo: M5Repository,
) -> None:
    apply_m5_event(
        m5_repo,
        RegisterGroupEvent(event_id="register", group=make_group(texts=("one",))),
    )
    _observe_edge(
        m5_repo,
        event_id="edge",
        requirement_id="g1-r0",
        chunk_id="h1",
        observation_id="selected",
    )
    old = build_reference_group_certificate(m5_repo, "g1")
    assert old is not None
    apply_event(
        m5_repo.base,
        PolicyChangeEvent(
            event_id="policy-2",
            policy=DecisionPolicy(
                policy_version="policy-v2",
                support_threshold=0.85,
                refute_threshold=0.85,
            ),
        ),
    )
    assert compute_reference_states(m5_repo).groups["g1"].complete
    assert not validate_group_certificate(m5_repo, old)
    rebound = build_reference_group_certificate(m5_repo, "g1")
    assert rebound is not None
    assert rebound.rows == old.rows
    assert rebound.certificate_digest != old.certificate_digest
    assert validate_group_certificate(m5_repo, rebound)


def test_claim_certificate_prefers_direct_then_group_then_none(
    m5_repo: M5Repository,
) -> None:
    apply_m5_event(
        m5_repo,
        RegisterGroupEvent(event_id="register", group=make_group(texts=("one",))),
    )
    none = build_reference_claim_certificate(m5_repo, "c1", {})
    assert none.support_kind is ClaimSupportKind.NONE
    assert validate_claim_certificate(m5_repo, none, {})

    _observe_edge(
        m5_repo,
        event_id="group-edge",
        requirement_id="g1-r0",
        chunk_id="h1",
    )
    group_certificate = build_reference_group_certificate(m5_repo, "g1")
    assert group_certificate is not None
    certificates = {"g1": group_certificate}
    group = build_reference_claim_certificate(m5_repo, "c1", certificates)
    assert group.support_kind is ClaimSupportKind.GROUP
    assert validate_claim_certificate(m5_repo, group, certificates)

    apply_m5_event(
        m5_repo,
        RegisterGroupEvent(
            event_id="register-second",
            group=make_group(group_id="g2", family_id="f2", texts=("two",)),
        ),
    )
    _observe_edge(
        m5_repo,
        event_id="second-group-edge",
        requirement_id="g2-r0",
        chunk_id="h2",
    )
    second_certificate = build_reference_group_certificate(m5_repo, "g2")
    assert second_certificate is not None
    cross_bound_map = {"g1": second_certificate}
    cross_bound_claim = build_reference_claim_certificate(
        m5_repo, "c1", cross_bound_map
    )
    assert cross_bound_claim.group_version_id == "g1"
    assert not validate_claim_certificate(m5_repo, cross_bound_claim, cross_bound_map)

    _observe_claim(
        m5_repo,
        event_id="direct",
        observation_id="direct-support",
        chunk_id="h2",
        scores=SUPPORT,
    )
    direct = build_reference_claim_certificate(m5_repo, "c1", certificates)
    assert direct.support_kind is ClaimSupportKind.DIRECT
    assert direct.direct_support_observation_id == "direct-support"
    assert validate_claim_certificate(m5_repo, direct, certificates)
    assert not validate_claim_certificate(
        m5_repo,
        replace(
            direct,
            decision_policy_version="stale-policy",
            certificate_digest="",
        ),
        certificates,
    )


def test_inactive_chunk_invalidates_current_certificate(m5_repo: M5Repository) -> None:
    apply_m5_event(
        m5_repo,
        RegisterGroupEvent(event_id="register", group=make_group(texts=("one",))),
    )
    _observe_edge(
        m5_repo,
        event_id="edge",
        requirement_id="g1-r0",
        chunk_id="h1",
    )
    certificate = build_reference_group_certificate(m5_repo, "g1")
    assert certificate is not None
    apply_event(
        m5_repo.base,
        DeleteDocumentVersionEvent(event_id="delete", document_version_id="dv1"),
    )
    assert not validate_group_certificate(m5_repo, certificate)
    assert not compute_reference_states(m5_repo).groups["g1"].complete
