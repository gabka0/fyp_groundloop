from __future__ import annotations

from dataclasses import replace

import pytest

from groundloop.domain import ClaimStatus
from groundloop.errors import ValidationError
from groundloop.m5.claim_certificates import (
    ClaimCertificateTransitionKind,
    WorkingClaimCertificateBinding,
    build_claim_certificate,
    effective_claim_binding_at,
    transition_claim_certificate,
    transition_claim_certificate_for_selected_support,
    validate_claim_binding_history,
    validate_claim_certificate,
)
from groundloop.m5.domain import (
    ClaimSupportKind,
    CombinedClaimState,
    GroupCertificateRow,
    GroupMatchingCertificateArtifact,
    SnapshotPoint,
)

HASH_A = "a" * 64
HASH_B = "b" * 64


def _group_certificate(
    group_id: str = "group-a", policy: str = "policy-a"
) -> GroupMatchingCertificateArtifact:
    return GroupMatchingCertificateArtifact(
        decision_policy_version=policy,
        group_version_id=group_id,
        rows=(GroupCertificateRow(0, f"{group_id}-requirement", HASH_A, "group-obs"),),
    )


def _state(
    *,
    direct_support: tuple[str, ...] = (),
    direct_refute: tuple[str, ...] = (),
    groups: tuple[str, ...] = (),
) -> CombinedClaimState:
    supported = bool(direct_support or groups)
    refuted = bool(direct_refute)
    if supported and refuted:
        status = ClaimStatus.CONFLICTED
    elif supported:
        status = ClaimStatus.SUPPORTED
    elif refuted:
        status = ClaimStatus.REFUTED
    else:
        status = ClaimStatus.UNSUPPORTED
    return CombinedClaimState(
        claim_id="claim-a",
        support_count=len(direct_support),
        refute_count=len(direct_refute),
        best_support_score=0.9 if direct_support else None,
        best_refute_score=0.9 if direct_refute else None,
        supporting_observation_ids=direct_support,
        refuting_observation_ids=direct_refute,
        complete_group_count=len(groups),
        complete_group_ids=groups,
        status=status,
    )


def test_exact_support_precedence_and_canonical_selection() -> None:
    groups = {
        "group-a": _group_certificate("group-a"),
        "group-b": _group_certificate("group-b"),
    }
    group_only = build_claim_certificate(
        _state(groups=("group-a", "group-b")),
        decision_policy_version="policy-a",
        group_certificates=groups,
    )
    assert group_only.support_kind is ClaimSupportKind.GROUP
    assert group_only.group_version_id == "group-a"
    assert group_only.group_certificate_digest == (groups["group-a"].certificate_digest)

    direct = build_claim_certificate(
        _state(
            direct_support=("direct-a", "direct-b"),
            direct_refute=("refute-a", "refute-b"),
            groups=("group-a", "group-b"),
        ),
        decision_policy_version="policy-a",
        group_certificates=groups,
    )
    assert direct.support_kind is ClaimSupportKind.DIRECT
    assert direct.direct_support_observation_id == "direct-a"
    assert direct.group_version_id is None
    assert direct.direct_refute_observation_id == "refute-a"


def test_group_certificate_policy_and_identity_are_checked() -> None:
    state = _state(groups=("group-a",))
    with pytest.raises(ValidationError, match="another policy"):
        build_claim_certificate(
            state,
            decision_policy_version="policy-b",
            group_certificates={"group-a": _group_certificate()},
        )


def test_claim_state_truth_table_and_direct_shapes_are_checked() -> None:
    with pytest.raises(ValidationError, match="support count"):
        build_claim_certificate(
            replace(
                _state(direct_support=("support",)),
                support_count=0,
            ),
            decision_policy_version="policy-a",
            group_certificates={},
        )
    with pytest.raises(ValidationError, match="status"):
        build_claim_certificate(
            replace(_state(), status=ClaimStatus.SUPPORTED),
            decision_policy_version="policy-a",
            group_certificates={},
        )


def test_every_complete_group_requires_a_policy_bound_certificate() -> None:
    groups = {
        "group-a": _group_certificate("group-a"),
        "group-b": _group_certificate("group-b", policy="policy-b"),
    }
    with pytest.raises(ValidationError, match="another policy"):
        build_claim_certificate(
            _state(
                direct_support=("direct",),
                groups=("group-a", "group-b"),
            ),
            decision_policy_version="policy-a",
            group_certificates=groups,
        )
    with pytest.raises(ValidationError, match="another group"):
        build_claim_certificate(
            _state(groups=("group-a",)),
            decision_policy_version="policy-a",
            group_certificates={"group-a": _group_certificate("group-b")},
        )


def test_same_epoch_retain_replace_and_policy_rebind_are_exact() -> None:
    initial = transition_claim_certificate(
        _state(),
        point=SnapshotPoint(2, 0),
        decision_policy_version="policy-a",
        group_certificates={},
        prior_binding=None,
        prior_artifact=None,
    )
    assert initial.kind is ClaimCertificateTransitionKind.BUILD

    retained = transition_claim_certificate(
        _state(),
        point=SnapshotPoint(2, 1),
        decision_policy_version="policy-a",
        group_certificates={},
        prior_binding=initial.binding,
        prior_artifact=initial.artifact,
    )
    assert retained.kind is ClaimCertificateTransitionKind.RETAIN
    assert not retained.binding_changed

    changed = transition_claim_certificate(
        _state(direct_support=("support",)),
        point=SnapshotPoint(2, 2),
        decision_policy_version="policy-a",
        group_certificates={},
        prior_binding=initial.binding,
        prior_artifact=initial.artifact,
    )
    assert changed.kind is ClaimCertificateTransitionKind.REPLACE
    assert changed.closed_prior_binding is not None
    assert changed.closed_prior_binding.valid_to_revision == 2
    assert changed.binding.valid_from_revision == 2

    rebound = transition_claim_certificate(
        _state(direct_support=("support",)),
        point=SnapshotPoint(2, 3),
        decision_policy_version="policy-b",
        group_certificates={},
        prior_binding=changed.binding,
        prior_artifact=changed.artifact,
    )
    assert rebound.kind is ClaimCertificateTransitionKind.REBIND
    assert rebound.artifact.decision_policy_version == "policy-b"


def test_cross_epoch_transition_never_closes_prior_epoch() -> None:
    initial = transition_claim_certificate(
        _state(direct_support=("support",)),
        point=SnapshotPoint(2, 4),
        decision_policy_version="policy-a",
        group_certificates={},
        prior_binding=None,
        prior_artifact=None,
    )
    carry = transition_claim_certificate(
        _state(direct_support=("support",)),
        point=SnapshotPoint(3, 0),
        decision_policy_version="policy-a",
        group_certificates={},
        prior_binding=initial.binding,
        prior_artifact=initial.artifact,
    )
    assert carry.kind is ClaimCertificateTransitionKind.EPOCH_RETAIN
    assert carry.closed_prior_binding is None
    assert initial.binding.open
    assert carry.binding.epoch_id == 3

    rebound = transition_claim_certificate(
        _state(direct_support=("support",)),
        point=SnapshotPoint(4, 0),
        decision_policy_version="policy-b",
        group_certificates={},
        prior_binding=carry.binding,
        prior_artifact=carry.artifact,
    )
    assert rebound.kind is ClaimCertificateTransitionKind.EPOCH_REBIND
    assert rebound.closed_prior_binding is None


def test_selected_support_transition_matches_exhaustive_transition_sequence() -> None:
    steps: tuple[
        tuple[
            CombinedClaimState,
            SnapshotPoint,
            str,
            dict[str, GroupMatchingCertificateArtifact],
        ],
        ...,
    ] = (
        (_state(), SnapshotPoint(2, 0), "policy-a", {}),
        (
            _state(direct_refute=("refute",)),
            SnapshotPoint(2, 1),
            "policy-a",
            {},
        ),
        (
            _state(direct_support=("support",), direct_refute=("refute",)),
            SnapshotPoint(2, 2),
            "policy-a",
            {},
        ),
        (
            _state(direct_support=("support",), direct_refute=("refute",)),
            SnapshotPoint(2, 3),
            "policy-b",
            {},
        ),
        (
            _state(direct_support=("support",), direct_refute=("refute",)),
            SnapshotPoint(3, 0),
            "policy-b",
            {},
        ),
        (
            _state(direct_refute=("refute",), groups=("group-a", "group-b")),
            SnapshotPoint(4, 0),
            "policy-b",
            {
                "group-a": _group_certificate("group-a", "policy-b"),
                "group-b": _group_certificate("group-b", "policy-b"),
            },
        ),
        (
            _state(direct_refute=("refute",), groups=("group-a", "group-b")),
            SnapshotPoint(5, 0),
            "policy-c",
            {
                "group-a": _group_certificate("group-a", "policy-c"),
                "group-b": _group_certificate("group-b", "policy-c"),
            },
        ),
    )
    expected_kinds = (
        ClaimCertificateTransitionKind.BUILD,
        ClaimCertificateTransitionKind.REPLACE,
        ClaimCertificateTransitionKind.REPLACE,
        ClaimCertificateTransitionKind.REBIND,
        ClaimCertificateTransitionKind.EPOCH_RETAIN,
        ClaimCertificateTransitionKind.EPOCH_REPLACE,
        ClaimCertificateTransitionKind.EPOCH_REBIND,
    )
    prior_binding = None
    prior_artifact = None
    actual_kinds: list[ClaimCertificateTransitionKind] = []

    for state, point, policy, certificates in steps:
        exhaustive = transition_claim_certificate(
            state,
            point=point,
            decision_policy_version=policy,
            group_certificates=certificates,
            prior_binding=prior_binding,
            prior_artifact=prior_artifact,
        )
        selected = (
            certificates[state.complete_group_ids[0]]
            if not state.supporting_observation_ids and state.complete_group_ids
            else None
        )
        maintained = transition_claim_certificate_for_selected_support(
            state,
            point=point,
            decision_policy_version=policy,
            selected_group_certificate=selected,
            prior_binding=prior_binding,
            prior_artifact=prior_artifact,
        )
        assert maintained == exhaustive
        actual_kinds.append(maintained.kind)
        prior_binding = exhaustive.binding
        prior_artifact = exhaustive.artifact

    assert tuple(actual_kinds) == expected_kinds


def test_selected_support_transition_rejects_missing_wrong_and_spurious_artifacts() -> (
    None
):
    group_state = _state(groups=("group-a",))
    with pytest.raises(ValidationError, match="has no certificate"):
        transition_claim_certificate_for_selected_support(
            group_state,
            point=SnapshotPoint(2, 0),
            decision_policy_version="policy-a",
            selected_group_certificate=None,
            prior_binding=None,
            prior_artifact=None,
        )
    with pytest.raises(ValidationError, match="another group"):
        transition_claim_certificate_for_selected_support(
            group_state,
            point=SnapshotPoint(2, 0),
            decision_policy_version="policy-a",
            selected_group_certificate=_group_certificate("group-b"),
            prior_binding=None,
            prior_artifact=None,
        )
    with pytest.raises(ValidationError, match="another policy"):
        transition_claim_certificate_for_selected_support(
            group_state,
            point=SnapshotPoint(2, 0),
            decision_policy_version="policy-a",
            selected_group_certificate=_group_certificate("group-a", "policy-b"),
            prior_binding=None,
            prior_artifact=None,
        )

    for state, expected in (
        (_state(), "NONE"),
        (_state(direct_support=("support",)), "DIRECT"),
    ):
        with pytest.raises(ValidationError, match=expected):
            transition_claim_certificate_for_selected_support(
                state,
                point=SnapshotPoint(2, 0),
                decision_policy_version="policy-a",
                selected_group_certificate=_group_certificate(),
                prior_binding=None,
                prior_artifact=None,
            )


def test_selected_support_hot_path_does_not_iterate_alternative_group_ids() -> None:
    class FirstOnlyTuple(tuple[str, ...]):
        def __iter__(self):  # type: ignore[no-untyped-def]
            raise AssertionError("hot path enumerated alternative group IDs")

    state = replace(
        _state(groups=("group-a", "group-b")),
        complete_group_ids=FirstOnlyTuple(("group-a", "group-b")),
    )

    transition = transition_claim_certificate_for_selected_support(
        state,
        point=SnapshotPoint(2, 0),
        decision_policy_version="policy-a",
        selected_group_certificate=_group_certificate("group-a"),
        prior_binding=None,
        prior_artifact=None,
    )

    assert transition.artifact.support_kind is ClaimSupportKind.GROUP
    assert transition.artifact.group_version_id == "group-a"


def test_same_point_change_and_malformed_prior_pair_are_rejected() -> None:
    initial = transition_claim_certificate(
        _state(),
        point=SnapshotPoint(2, 0),
        decision_policy_version="policy-a",
        group_certificates={},
        prior_binding=None,
        prior_artifact=None,
    )
    with pytest.raises(ValidationError, match="same snapshot point"):
        transition_claim_certificate(
            _state(direct_support=("support",)),
            point=SnapshotPoint(2, 0),
            decision_policy_version="policy-a",
            group_certificates={},
            prior_binding=initial.binding,
            prior_artifact=initial.artifact,
        )
    with pytest.raises(ValidationError, match="supplied together"):
        transition_claim_certificate(
            _state(),
            point=SnapshotPoint(3, 0),
            decision_policy_version="policy-a",
            group_certificates={},
            prior_binding=initial.binding,
            prior_artifact=None,
        )


def test_effective_as_of_binding_uses_epoch_local_then_prior_fallback() -> None:
    first = transition_claim_certificate(
        _state(),
        point=SnapshotPoint(2, 0),
        decision_policy_version="policy-a",
        group_certificates={},
        prior_binding=None,
        prior_artifact=None,
    )
    second = transition_claim_certificate(
        _state(direct_support=("support",)),
        point=SnapshotPoint(2, 2),
        decision_policy_version="policy-a",
        group_certificates={},
        prior_binding=first.binding,
        prior_artifact=first.artifact,
    )
    assert second.closed_prior_binding is not None
    history = (second.closed_prior_binding, second.binding)
    assert (
        effective_claim_binding_at(
            history, claim_id="claim-a", point=SnapshotPoint(2, 1)
        )
        == second.closed_prior_binding
    )
    assert (
        effective_claim_binding_at(
            history, claim_id="claim-a", point=SnapshotPoint(2, 2)
        )
        == second.binding
    )
    assert (
        effective_claim_binding_at(
            history, claim_id="claim-a", point=SnapshotPoint(3, 0)
        )
        == second.binding
    )
    assert (
        validate_claim_binding_history(
            history,
            {
                first.artifact.certificate_digest: first.artifact,
                second.artifact.certificate_digest: second.artifact,
            },
        )
        == ()
    )


def test_history_audit_detects_gaps_missing_artifacts() -> None:
    first = transition_claim_certificate(
        _state(),
        point=SnapshotPoint(2, 0),
        decision_policy_version="policy-a",
        group_certificates={},
        prior_binding=None,
        prior_artifact=None,
    )
    malformed = (
        replace(first.binding, valid_to_revision=1),
        WorkingClaimCertificateBinding(2, "claim-a", 2, None, HASH_B),
    )
    issues = validate_claim_binding_history(malformed, {})
    assert "binding_artifact_missing" in issues
    assert "binding_gap_or_overlap:claim-a:2" in issues


def test_validator_rejects_state_drift() -> None:
    group = _group_certificate()
    state = _state(groups=("group-a",))
    artifact = build_claim_certificate(
        state,
        decision_policy_version="policy-a",
        group_certificates={"group-a": group},
    )
    assert validate_claim_certificate(
        state,
        artifact,
        decision_policy_version="policy-a",
        group_certificates={"group-a": group},
    ).valid
    result = validate_claim_certificate(
        _state(),
        artifact,
        decision_policy_version="policy-a",
        group_certificates={},
    )
    assert not result.valid
    assert result.issues == ("artifact_does_not_match_maintained_state",)
