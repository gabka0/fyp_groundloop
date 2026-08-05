"""Stateful v2 claim-certificate selection and revision bindings.

This module consumes maintained direct/group state.  It deliberately does not
import either full-recomputation oracle or the Hall-mask implementation.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from enum import StrEnum

from groundloop.domain import ClaimStatus
from groundloop.errors import GroundLoopError, ValidationError
from groundloop.m5.domain import (
    ClaimCertificateArtifact,
    ClaimSupportKind,
    CombinedClaimState,
    GroupMatchingCertificateArtifact,
    SnapshotPoint,
)


def _require_identifier(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{name} must be a nonempty identifier")


def _require_nonnegative_integer(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValidationError(f"{name} must be a nonnegative integer")


@dataclass(frozen=True, slots=True)
class WorkingClaimCertificateBinding:
    """One half-open claim-certificate interval inside one semantic epoch."""

    epoch_id: int
    claim_id: str
    valid_from_revision: int
    valid_to_revision: int | None
    certificate_digest: str

    def __post_init__(self) -> None:
        _require_nonnegative_integer("epoch_id", self.epoch_id)
        _require_identifier("claim_id", self.claim_id)
        _require_nonnegative_integer("valid_from_revision", self.valid_from_revision)
        if self.valid_to_revision is not None:
            _require_nonnegative_integer("valid_to_revision", self.valid_to_revision)
            if self.valid_to_revision <= self.valid_from_revision:
                raise ValidationError(
                    "claim binding valid_to_revision must exceed valid_from_revision"
                )
        if (
            not isinstance(self.certificate_digest, str)
            or len(self.certificate_digest) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.certificate_digest
            )
        ):
            raise ValidationError(
                "claim binding certificate_digest must be lowercase SHA-256"
            )

    @property
    def open(self) -> bool:
        return self.valid_to_revision is None

    def covers(self, point: SnapshotPoint) -> bool:
        if point.epoch_id != self.epoch_id:
            return False
        if point.revision < self.valid_from_revision:
            return False
        return self.valid_to_revision is None or point.revision < self.valid_to_revision


class ClaimCertificateTransitionKind(StrEnum):
    BUILD = "build"
    RETAIN = "retain"
    REPLACE = "replace"
    REBIND = "rebind"
    EPOCH_RETAIN = "epoch_retain"
    EPOCH_REPLACE = "epoch_replace"
    EPOCH_REBIND = "epoch_rebind"


@dataclass(frozen=True, slots=True)
class ClaimCertificateTransition:
    kind: ClaimCertificateTransitionKind
    artifact: ClaimCertificateArtifact
    binding: WorkingClaimCertificateBinding
    closed_prior_binding: WorkingClaimCertificateBinding | None
    artifact_changed: bool
    binding_changed: bool


@dataclass(frozen=True, slots=True)
class ClaimCertificateValidation:
    valid: bool
    issues: tuple[str, ...]


def _validate_claim_state_scalars(state: CombinedClaimState) -> None:
    """Validate O(1) count, presence, and truth-table invariants."""

    _require_identifier("claim_id", state.claim_id)
    _require_nonnegative_integer("support_count", state.support_count)
    _require_nonnegative_integer("refute_count", state.refute_count)
    _require_nonnegative_integer("complete_group_count", state.complete_group_count)
    if state.complete_group_count != len(state.complete_group_ids):
        raise ValidationError("complete_group_count disagrees with group IDs")

    if bool(state.support_count) != bool(state.supporting_observation_ids):
        raise ValidationError("direct support count and observations disagree")
    if bool(state.refute_count) != bool(state.refuting_observation_ids):
        raise ValidationError("direct refute count and observations disagree")
    if (state.best_support_score is None) == bool(state.support_count):
        raise ValidationError("best support score presence disagrees with count")
    if (state.best_refute_score is None) == bool(state.refute_count):
        raise ValidationError("best refute score presence disagrees with count")
    supported = bool(state.support_count or state.complete_group_count)
    refuted = bool(state.refute_count)
    expected_status = (
        ClaimStatus.CONFLICTED
        if supported and refuted
        else ClaimStatus.SUPPORTED
        if supported
        else ClaimStatus.REFUTED
        if refuted
        else ClaimStatus.UNSUPPORTED
    )
    if state.status is not expected_status:
        raise ValidationError("combined claim status disagrees with its counts")


def _validate_claim_state_sequences(state: CombinedClaimState) -> None:
    """Exhaustively validate canonical tuples for audit/reference callers."""

    if state.complete_group_ids != tuple(sorted(set(state.complete_group_ids))):
        raise ValidationError("complete group IDs must be sorted and unique")
    if state.supporting_observation_ids != tuple(
        sorted(set(state.supporting_observation_ids))
    ):
        raise ValidationError("direct support observation IDs must be sorted")
    if state.refuting_observation_ids != tuple(
        sorted(set(state.refuting_observation_ids))
    ):
        raise ValidationError("direct refute observation IDs must be sorted")


def _validate_selected_group_certificate(
    certificate: GroupMatchingCertificateArtifact,
    *,
    group_version_id: str,
    decision_policy_version: str,
) -> None:
    if certificate.group_version_id != group_version_id:
        raise ValidationError("group certificate is bound to another group")
    if certificate.decision_policy_version != decision_policy_version:
        raise ValidationError("group certificate is bound to another policy")


def _claim_artifact_from_selected_support(
    state: CombinedClaimState,
    *,
    decision_policy_version: str,
    selected_group_certificate: GroupMatchingCertificateArtifact | None,
) -> ClaimCertificateArtifact:
    direct_support_id: str | None = None
    group_version_id: str | None = None
    group_certificate_digest: str | None = None
    if state.supporting_observation_ids:
        support_kind = ClaimSupportKind.DIRECT
        direct_support_id = state.supporting_observation_ids[0]
        if selected_group_certificate is not None:
            raise ValidationError(
                "DIRECT claim support cannot supply a selected group certificate"
            )
    elif state.complete_group_ids:
        support_kind = ClaimSupportKind.GROUP
        group_version_id = state.complete_group_ids[0]
        if selected_group_certificate is None:
            raise ValidationError(
                f"selected complete group {group_version_id} has no certificate"
            )
        _validate_selected_group_certificate(
            selected_group_certificate,
            group_version_id=group_version_id,
            decision_policy_version=decision_policy_version,
        )
        group_certificate_digest = selected_group_certificate.certificate_digest
    else:
        support_kind = ClaimSupportKind.NONE
        if selected_group_certificate is not None:
            raise ValidationError(
                "NONE claim support cannot supply a selected group certificate"
            )

    return ClaimCertificateArtifact(
        claim_id=state.claim_id,
        decision_policy_version=decision_policy_version,
        support_kind=support_kind,
        direct_support_observation_id=direct_support_id,
        group_version_id=group_version_id,
        group_certificate_digest=group_certificate_digest,
        direct_refute_observation_id=(
            state.refuting_observation_ids[0]
            if state.refuting_observation_ids
            else None
        ),
    )


def build_claim_certificate(
    state: CombinedClaimState,
    *,
    decision_policy_version: str,
    group_certificates: Mapping[str, GroupMatchingCertificateArtifact],
) -> ClaimCertificateArtifact:
    """Select and exhaustively validate the exact M5-D12 certificate."""

    _require_identifier("decision_policy_version", decision_policy_version)
    _validate_claim_state_scalars(state)
    _validate_claim_state_sequences(state)

    for complete_group_id in state.complete_group_ids:
        certificate = group_certificates.get(complete_group_id)
        if certificate is None:
            raise ValidationError(
                f"complete group {complete_group_id} has no certificate"
            )
        _validate_selected_group_certificate(
            certificate,
            group_version_id=complete_group_id,
            decision_policy_version=decision_policy_version,
        )

    selected_group_certificate = (
        group_certificates[state.complete_group_ids[0]]
        if not state.supporting_observation_ids and state.complete_group_ids
        else None
    )
    return _claim_artifact_from_selected_support(
        state,
        decision_policy_version=decision_policy_version,
        selected_group_certificate=selected_group_certificate,
    )


def validate_claim_certificate(
    state: CombinedClaimState,
    artifact: ClaimCertificateArtifact,
    *,
    decision_policy_version: str,
    group_certificates: Mapping[str, GroupMatchingCertificateArtifact],
) -> ClaimCertificateValidation:
    issues: list[str] = []
    try:
        expected = build_claim_certificate(
            state,
            decision_policy_version=decision_policy_version,
            group_certificates=group_certificates,
        )
        if artifact != expected:
            issues.append("artifact_does_not_match_maintained_state")
    except (GroundLoopError, KeyError, ValueError) as error:
        issues.append(f"invalid_certificate_inputs:{type(error).__name__}")
    return ClaimCertificateValidation(not issues, tuple(issues))


def _transition_to_desired_claim_certificate(
    state: CombinedClaimState,
    *,
    point: SnapshotPoint,
    desired: ClaimCertificateArtifact,
    prior_binding: WorkingClaimCertificateBinding | None,
    prior_artifact: ClaimCertificateArtifact | None,
) -> ClaimCertificateTransition:
    if (prior_binding is None) != (prior_artifact is None):
        raise ValidationError(
            "prior claim binding and artifact must be supplied together"
        )
    if prior_binding is None:
        binding = WorkingClaimCertificateBinding(
            epoch_id=point.epoch_id,
            claim_id=state.claim_id,
            valid_from_revision=point.revision,
            valid_to_revision=None,
            certificate_digest=desired.certificate_digest,
        )
        return ClaimCertificateTransition(
            ClaimCertificateTransitionKind.BUILD,
            desired,
            binding,
            None,
            True,
            True,
        )

    assert prior_artifact is not None
    if not prior_binding.open:
        raise ValidationError("prior claim binding must be open")
    if prior_binding.claim_id != state.claim_id:
        raise ValidationError("prior claim binding belongs to another claim")
    if prior_artifact.claim_id != state.claim_id:
        raise ValidationError("prior claim artifact belongs to another claim")
    if prior_binding.certificate_digest != prior_artifact.certificate_digest:
        raise ValidationError("prior claim binding and artifact disagree")
    if point.epoch_id < prior_binding.epoch_id:
        raise ValidationError("claim certificate transition moves backward in epoch")

    artifact_changed = desired.certificate_digest != prior_artifact.certificate_digest
    policy_changed = (
        desired.decision_policy_version != prior_artifact.decision_policy_version
    )
    if point.epoch_id == prior_binding.epoch_id:
        if point.revision < prior_binding.valid_from_revision:
            raise ValidationError(
                "claim certificate transition moves backward in revision"
            )
        if not artifact_changed:
            return ClaimCertificateTransition(
                ClaimCertificateTransitionKind.RETAIN,
                desired,
                prior_binding,
                None,
                False,
                False,
            )
        if point.revision == prior_binding.valid_from_revision:
            raise ValidationError(
                "a claim certificate cannot change twice at the same snapshot point"
            )
        closed = replace(prior_binding, valid_to_revision=point.revision)
        binding = WorkingClaimCertificateBinding(
            epoch_id=point.epoch_id,
            claim_id=state.claim_id,
            valid_from_revision=point.revision,
            valid_to_revision=None,
            certificate_digest=desired.certificate_digest,
        )
        return ClaimCertificateTransition(
            (
                ClaimCertificateTransitionKind.REBIND
                if policy_changed
                else ClaimCertificateTransitionKind.REPLACE
            ),
            desired,
            binding,
            closed,
            True,
            True,
        )

    binding = WorkingClaimCertificateBinding(
        epoch_id=point.epoch_id,
        claim_id=state.claim_id,
        valid_from_revision=point.revision,
        valid_to_revision=None,
        certificate_digest=desired.certificate_digest,
    )
    if not artifact_changed:
        kind = ClaimCertificateTransitionKind.EPOCH_RETAIN
    elif policy_changed:
        kind = ClaimCertificateTransitionKind.EPOCH_REBIND
    else:
        kind = ClaimCertificateTransitionKind.EPOCH_REPLACE
    return ClaimCertificateTransition(
        kind,
        desired,
        binding,
        None,
        artifact_changed,
        True,
    )


def transition_claim_certificate(
    state: CombinedClaimState,
    *,
    point: SnapshotPoint,
    decision_policy_version: str,
    group_certificates: Mapping[str, GroupMatchingCertificateArtifact],
    prior_binding: WorkingClaimCertificateBinding | None,
    prior_artifact: ClaimCertificateArtifact | None,
) -> ClaimCertificateTransition:
    """Apply a transition after exhaustive complete-group validation.

    A new epoch never closes a prior epoch's final binding. Within one epoch,
    replacement closes the prior interval exactly at ``point.revision``.
    Identical state inside the same epoch retains the existing binding and
    creates no history row.
    """

    desired = build_claim_certificate(
        state,
        decision_policy_version=decision_policy_version,
        group_certificates=group_certificates,
    )
    return _transition_to_desired_claim_certificate(
        state,
        point=point,
        desired=desired,
        prior_binding=prior_binding,
        prior_artifact=prior_artifact,
    )


def transition_claim_certificate_for_selected_support(
    state: CombinedClaimState,
    *,
    point: SnapshotPoint,
    decision_policy_version: str,
    selected_group_certificate: GroupMatchingCertificateArtifact | None,
    prior_binding: WorkingClaimCertificateBinding | None,
    prior_artifact: ClaimCertificateArtifact | None,
) -> ClaimCertificateTransition:
    """Apply the maintained hot path using at most one selected group artifact.

    This path trusts the overlay's already-maintained canonical identifier
    tuples. It validates O(1) scalar/truth-table invariants and the selected
    group artifact only; exhaustive audit callers must use
    :func:`transition_claim_certificate` or :func:`validate_claim_certificate`.
    """

    _require_identifier("decision_policy_version", decision_policy_version)
    _validate_claim_state_scalars(state)
    desired = _claim_artifact_from_selected_support(
        state,
        decision_policy_version=decision_policy_version,
        selected_group_certificate=selected_group_certificate,
    )
    return _transition_to_desired_claim_certificate(
        state,
        point=point,
        desired=desired,
        prior_binding=prior_binding,
        prior_artifact=prior_artifact,
    )


def effective_claim_binding_at(
    bindings: Iterable[WorkingClaimCertificateBinding],
    *,
    claim_id: str,
    point: SnapshotPoint,
) -> WorkingClaimCertificateBinding | None:
    """Return the epoch-local binding or the final prior-epoch fallback."""

    relevant = tuple(
        sorted(
            (
                binding
                for binding in bindings
                if binding.claim_id == claim_id and binding.epoch_id <= point.epoch_id
            ),
            key=lambda binding: (
                binding.epoch_id,
                binding.valid_from_revision,
            ),
        )
    )
    current = tuple(
        binding
        for binding in relevant
        if binding.epoch_id == point.epoch_id and binding.covers(point)
    )
    if len(current) > 1:
        raise ValidationError("claim certificate bindings overlap")
    if current:
        return current[0]

    prior_epochs = tuple(
        sorted(
            {
                binding.epoch_id
                for binding in relevant
                if binding.epoch_id < point.epoch_id
            }
        )
    )
    for epoch_id in reversed(prior_epochs):
        rows = tuple(binding for binding in relevant if binding.epoch_id == epoch_id)
        final = max(rows, key=lambda binding: binding.valid_from_revision)
        if final.open:
            return final
        raise ValidationError(
            "a prior claim-certificate epoch ends without an open binding"
        )
    return None


def validate_claim_binding_history(
    bindings: Iterable[WorkingClaimCertificateBinding],
    artifacts: Mapping[str, ClaimCertificateArtifact],
) -> tuple[str, ...]:
    """Audit close-once intervals and immutable artifact references."""

    issues: list[str] = []
    rows = tuple(bindings)
    groups: dict[tuple[str, int], list[WorkingClaimCertificateBinding]] = {}
    for binding in rows:
        groups.setdefault((binding.claim_id, binding.epoch_id), []).append(binding)
        artifact = artifacts.get(binding.certificate_digest)
        if artifact is None:
            issues.append("binding_artifact_missing")
        elif artifact.claim_id != binding.claim_id:
            issues.append("binding_artifact_claim_mismatch")

    for key, unsorted in groups.items():
        ordered = sorted(unsorted, key=lambda row: row.valid_from_revision)
        if len({row.valid_from_revision for row in ordered}) != len(ordered):
            issues.append(f"duplicate_binding_start:{key[0]}:{key[1]}")
        open_rows = [row for row in ordered if row.open]
        if len(open_rows) > 1:
            issues.append(f"multiple_open_bindings:{key[0]}:{key[1]}")
        for before, after in zip(ordered, ordered[1:], strict=False):
            if before.valid_to_revision != after.valid_from_revision:
                issues.append(f"binding_gap_or_overlap:{key[0]}:{key[1]}")
        if ordered and not ordered[-1].open:
            issues.append(f"terminal_binding_closed:{key[0]}:{key[1]}")
    return tuple(dict.fromkeys(issues))


__all__ = [
    "ClaimCertificateTransition",
    "ClaimCertificateTransitionKind",
    "ClaimCertificateValidation",
    "WorkingClaimCertificateBinding",
    "build_claim_certificate",
    "effective_claim_binding_at",
    "transition_claim_certificate",
    "transition_claim_certificate_for_selected_support",
    "validate_claim_binding_history",
    "validate_claim_certificate",
]
