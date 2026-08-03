"""Independent full recomputation for GroundLoop M5.

This module deliberately uses unmatched-branch backtracking for bipartite
matching.  It must never import the optimized Hall-mask kernel.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

from groundloop.domain import AnswerStatus, ClaimStatus, SubjectKind, VerificationLabel
from groundloop.errors import GroundLoopError
from groundloop.m5.digests import normalized_text_hash_v1
from groundloop.m5.domain import (
    ClaimCertificateArtifact,
    ClaimSupportKind,
    CombinedAnswerState,
    CombinedClaimState,
    GroupCertificateRow,
    GroupMatchingCertificateArtifact,
    GroupState,
    RequirementState,
    RequirementWitness,
    SnapshotPoint,
)
from groundloop.m5.repository import M5Repository
from groundloop.policy import decide
from groundloop.reference import compute_all_states

CANONICAL_REQUIREMENT_TASK = "verify_requirement_v1"


@dataclass(frozen=True, slots=True)
class M5ReferenceStates:
    requirements: dict[str, RequirementState]
    groups: dict[str, GroupState]
    claims: dict[str, CombinedClaimState]
    answers: dict[str, CombinedAnswerState]


@dataclass(frozen=True, slots=True)
class ReferenceMatching:
    size: int
    assignment_by_ordinal: tuple[str | None, ...]


def _active_requirement_edges(
    repo: M5Repository,
    point: SnapshotPoint,
) -> tuple[
    dict[str, dict[str, tuple[str, ...]]],
    dict[str, RequirementState],
]:
    policy = repo.policy_at(point.epoch_id)
    observations_by_edge: dict[str, dict[str, list[str]]] = {}
    for record in repo.current_requirement_observations(point):
        observation = record.observation
        if not record.eligible_for_currency:
            continue
        if observation.task_type != CANONICAL_REQUIREMENT_TASK:
            continue
        if not repo.is_requirement_active(observation.subject_id, point.epoch_id):
            continue
        if not repo.is_chunk_active_at(observation.chunk_version_id, point.epoch_id):
            continue
        if decide(observation, policy) is not VerificationLabel.SUPPORT:
            continue
        # M1's historical hash uses Python ``\s`` semantics.  M5 deliberately
        # derives its witness identity with the frozen 29-code-point predicate
        # so Python and PostgreSQL cannot drift across Unicode versions.
        text_hash = normalized_text_hash_v1(
            repo.base.chunk_version(observation.chunk_version_id).text
        )
        observations_by_edge.setdefault(observation.subject_id, {}).setdefault(
            text_hash, []
        ).append(observation.observation_id)

    immutable_edges: dict[str, dict[str, tuple[str, ...]]] = {}
    states: dict[str, RequirementState] = {}
    for requirement_id in repo.all_requirement_ids():
        if not repo.is_requirement_active(requirement_id, point.epoch_id):
            continue
        edges = observations_by_edge.get(requirement_id, {})
        immutable_edges[requirement_id] = {
            text_hash: tuple(sorted(observation_ids))
            for text_hash, observation_ids in sorted(edges.items())
        }
        witness_hashes = tuple(sorted(edges))
        supporting_ids = tuple(
            sorted(
                observation_id
                for observation_ids in edges.values()
                for observation_id in observation_ids
            )
        )
        states[requirement_id] = RequirementState(
            requirement_version_id=requirement_id,
            witness_hashes=witness_hashes,
            supporting_observation_ids=supporting_ids,
            witness_count=len(witness_hashes),
            satisfied=bool(witness_hashes),
        )
    return immutable_edges, states


def derive_active_requirement_witnesses(
    repo: M5Repository,
    point: SnapshotPoint | None = None,
) -> tuple[RequirementWitness, ...]:
    """Expose derived witness edges without making them writable state."""

    snapshot = repo.current_point if point is None else point
    repo.require_snapshot_point(snapshot)
    edges, _ = _active_requirement_edges(repo, snapshot)
    witnesses: list[RequirementWitness] = []
    for requirement_id in sorted(edges):
        requirement = repo.requirement(requirement_id)
        for text_hash, observation_ids in sorted(edges[requirement_id].items()):
            witnesses.append(
                RequirementWitness(
                    requirement_version_id=requirement_id,
                    requirement_ordinal=requirement.ordinal,
                    text_hash=text_hash,
                    active_observation_ids=observation_ids,
                )
            )
    return tuple(witnesses)


def maximum_matching_with_unmatched_branch(
    neighbors_by_ordinal: tuple[tuple[str, ...], ...],
) -> ReferenceMatching:
    """Return exact maximum cardinality and one deterministic assignment."""

    best_size = -1
    best_assignment: tuple[str | None, ...] | None = None

    def assignment_key(assignment: tuple[str | None, ...]) -> tuple[str, ...]:
        return tuple("\uffff" if value is None else value for value in assignment)

    def visit(
        ordinal: int,
        used_hashes: frozenset[str],
        assignment: tuple[str | None, ...],
        matched: int,
    ) -> None:
        nonlocal best_size, best_assignment
        if ordinal == len(neighbors_by_ordinal):
            if matched > best_size or (
                matched == best_size
                and (
                    best_assignment is None
                    or assignment_key(assignment) < assignment_key(best_assignment)
                )
            ):
                best_size = matched
                best_assignment = assignment
            return

        # This explicit branch is essential for maximum *partial* matching.
        visit(ordinal + 1, used_hashes, (*assignment, None), matched)
        for text_hash in sorted(neighbors_by_ordinal[ordinal]):
            if text_hash in used_hashes:
                continue
            visit(
                ordinal + 1,
                used_hashes | {text_hash},
                (*assignment, text_hash),
                matched + 1,
            )

    visit(0, frozenset(), (), 0)
    assert best_assignment is not None
    return ReferenceMatching(size=best_size, assignment_by_ordinal=best_assignment)


def compute_reference_states(
    repo: M5Repository,
    point: SnapshotPoint | None = None,
) -> M5ReferenceStates:
    snapshot = repo.current_point if point is None else point
    repo.require_snapshot_point(snapshot)
    if snapshot != repo.current_point:
        raise ValueError(
            "combined direct M1 state is available only at the current point; "
            "historical M5 certificate validation uses dedicated validators"
        )
    edges, requirement_states = _active_requirement_edges(repo, snapshot)
    group_states: dict[str, GroupState] = {}
    for group_id in repo.active_group_ids(snapshot.epoch_id):
        requirements = tuple(
            repo.requirement(requirement_id)
            for requirement_id in repo.requirement_ids_for_group(group_id)
        )
        neighbors = tuple(
            tuple(sorted(edges.get(requirement.requirement_version_id, {})))
            for requirement in requirements
        )
        matching = maximum_matching_with_unmatched_branch(neighbors)
        satisfied_count = sum(bool(values) for values in neighbors)
        group_states[group_id] = GroupState(
            group_version_id=group_id,
            requirement_count=len(requirements),
            satisfied_count=satisfied_count,
            matching_size=matching.size,
            complete=bool(requirements) and matching.size == len(requirements),
        )

    direct_claims, _ = compute_all_states(repo.base)
    combined_claims: dict[str, CombinedClaimState] = {}
    for claim_id in repo.base.all_claim_ids():
        direct = direct_claims[claim_id]
        complete_group_ids = tuple(
            sorted(
                group_id
                for group_id in repo.group_ids_for_claim(claim_id)
                if group_states.get(group_id) is not None
                and group_states[group_id].complete
            )
        )
        supported = direct.support_count > 0 or bool(complete_group_ids)
        refuted = direct.refute_count > 0
        combined_claims[claim_id] = CombinedClaimState(
            claim_id=claim_id,
            support_count=direct.support_count,
            refute_count=direct.refute_count,
            best_support_score=direct.best_support_score,
            best_refute_score=direct.best_refute_score,
            supporting_observation_ids=direct.supporting_observation_ids,
            refuting_observation_ids=direct.refuting_observation_ids,
            complete_group_count=len(complete_group_ids),
            complete_group_ids=complete_group_ids,
            status=_claim_status(supported=supported, refuted=refuted),
        )

    combined_answers = {
        answer_id: _compute_answer_state(repo, answer_id, combined_claims)
        for answer_id in repo.base.all_answer_ids()
    }
    return M5ReferenceStates(
        requirements=requirement_states,
        groups=group_states,
        claims=combined_claims,
        answers=combined_answers,
    )


def _claim_status(*, supported: bool, refuted: bool) -> ClaimStatus:
    if supported and refuted:
        return ClaimStatus.CONFLICTED
    if supported:
        return ClaimStatus.SUPPORTED
    if refuted:
        return ClaimStatus.REFUTED
    return ClaimStatus.UNSUPPORTED


def _compute_answer_state(
    repo: M5Repository,
    answer_id: str,
    claim_states: dict[str, CombinedClaimState],
) -> CombinedAnswerState:
    required_states = tuple(
        claim_states[claim_id]
        for claim_id in repo.base.claim_ids_of_answer(answer_id)
        if repo.base.claim(claim_id).required
    )
    counts = Counter(state.status for state in required_states)
    if counts[ClaimStatus.REFUTED] > 0:
        status = AnswerStatus.CONTRADICTED
    elif counts[ClaimStatus.CONFLICTED] > 0:
        status = AnswerStatus.CONFLICTED
    elif required_states and counts[ClaimStatus.SUPPORTED] == len(required_states):
        status = AnswerStatus.VALID
    elif counts[ClaimStatus.SUPPORTED] > 0:
        status = AnswerStatus.PARTIALLY_SUPPORTED
    else:
        status = AnswerStatus.UNSUPPORTED
    return CombinedAnswerState(
        answer_version_id=answer_id,
        required_claim_count=len(required_states),
        supported_count=counts[ClaimStatus.SUPPORTED],
        unsupported_count=counts[ClaimStatus.UNSUPPORTED],
        refuted_count=counts[ClaimStatus.REFUTED],
        conflicted_count=counts[ClaimStatus.CONFLICTED],
        status=status,
    )


def build_reference_group_certificate(
    repo: M5Repository,
    group_version_id: str,
    point: SnapshotPoint | None = None,
) -> GroupMatchingCertificateArtifact | None:
    """Build a fixture certificate without defining incremental identity.

    Production matching may retain another valid stateful certificate.  This
    helper exists only to create validator fixtures from base semantics.
    """

    snapshot = repo.current_point if point is None else point
    repo.require_snapshot_point(snapshot)
    if not repo.is_group_active(group_version_id, snapshot.epoch_id):
        return None
    edges, _ = _active_requirement_edges(repo, snapshot)
    requirement_ids = repo.requirement_ids_for_group(group_version_id)
    neighbors = tuple(tuple(sorted(edges.get(key, {}))) for key in requirement_ids)
    matching = maximum_matching_with_unmatched_branch(neighbors)
    if matching.size != len(requirement_ids):
        return None
    rows = tuple(
        GroupCertificateRow(
            requirement_ordinal=ordinal,
            requirement_version_id=requirement_id,
            text_hash=text_hash,
            selected_observation_id=edges[requirement_id][text_hash][0],
        )
        for ordinal, (requirement_id, text_hash) in enumerate(
            zip(requirement_ids, matching.assignment_by_ordinal, strict=True)
        )
        if text_hash is not None
    )
    return GroupMatchingCertificateArtifact(
        decision_policy_version=repo.policy_at(snapshot.epoch_id).policy_version,
        group_version_id=group_version_id,
        rows=rows,
    )


def validate_group_certificate(
    repo: M5Repository,
    certificate: GroupMatchingCertificateArtifact,
    point: SnapshotPoint | None = None,
) -> bool:
    snapshot = repo.current_point if point is None else point
    try:
        repo.require_snapshot_point(snapshot)
        policy = repo.policy_at(snapshot.epoch_id)
        if policy.policy_version != certificate.decision_policy_version:
            return False
        if not repo.is_group_active(certificate.group_version_id, snapshot.epoch_id):
            return False
        requirement_ids = repo.requirement_ids_for_group(certificate.group_version_id)
        if len(requirement_ids) != certificate.requirement_count:
            return False
        for row, expected_requirement_id in zip(
            certificate.rows, requirement_ids, strict=True
        ):
            if row.requirement_version_id != expected_requirement_id:
                return False
            record = repo.requirement_observation(row.selected_observation_id)
            observation = record.observation
            if not record.eligible_for_currency:
                return False
            if observation.subject_kind is not SubjectKind.REQUIREMENT:
                return False
            if observation.subject_id != row.requirement_version_id:
                return False
            if observation.task_type != CANONICAL_REQUIREMENT_TASK:
                return False
            if (
                repo.currency_observation_id_at(observation.key, snapshot)
                != observation.observation_id
            ):
                return False
            if not repo.is_chunk_active_at(
                observation.chunk_version_id, snapshot.epoch_id
            ):
                return False
            if decide(observation, policy) is not VerificationLabel.SUPPORT:
                return False
            if (
                normalized_text_hash_v1(
                    repo.base.chunk_version(observation.chunk_version_id).text
                )
                != row.text_hash
            ):
                return False
    except (GroundLoopError, KeyError, ValueError):
        return False
    return True


def build_reference_claim_certificate(
    repo: M5Repository,
    claim_id: str,
    group_certificates: dict[str, GroupMatchingCertificateArtifact],
) -> ClaimCertificateArtifact:
    states = compute_reference_states(repo)
    state = states.claims[claim_id]
    if state.supporting_observation_ids:
        support_kind = ClaimSupportKind.DIRECT
        direct_support = min(state.supporting_observation_ids)
        group_id = None
        group_digest = None
    elif state.complete_group_ids:
        support_kind = ClaimSupportKind.GROUP
        direct_support = None
        group_id = min(state.complete_group_ids)
        group_digest = group_certificates[group_id].certificate_digest
    else:
        support_kind = ClaimSupportKind.NONE
        direct_support = None
        group_id = None
        group_digest = None
    return ClaimCertificateArtifact(
        claim_id=claim_id,
        decision_policy_version=repo.base.current_policy().policy_version,
        support_kind=support_kind,
        direct_support_observation_id=direct_support,
        group_version_id=group_id,
        group_certificate_digest=group_digest,
        direct_refute_observation_id=(
            min(state.refuting_observation_ids)
            if state.refuting_observation_ids
            else None
        ),
    )


def validate_claim_certificate(
    repo: M5Repository,
    certificate: ClaimCertificateArtifact,
    group_certificates: dict[str, GroupMatchingCertificateArtifact],
) -> bool:
    try:
        expected = build_reference_claim_certificate(
            repo, certificate.claim_id, group_certificates
        )
    except (GroundLoopError, KeyError, ValueError):
        return False
    if expected != certificate:
        return False
    if certificate.support_kind is ClaimSupportKind.GROUP:
        assert certificate.group_version_id is not None
        assert certificate.group_certificate_digest is not None
        group_certificate = group_certificates.get(certificate.group_version_id)
        return (
            group_certificate is not None
            and group_certificate.group_version_id == certificate.group_version_id
            and group_certificate.certificate_digest
            == certificate.group_certificate_digest
            and validate_group_certificate(repo, group_certificate)
        )
    return True


__all__ = [
    "CANONICAL_REQUIREMENT_TASK",
    "M5ReferenceStates",
    "ReferenceMatching",
    "build_reference_claim_certificate",
    "build_reference_group_certificate",
    "compute_reference_states",
    "derive_active_requirement_witnesses",
    "maximum_matching_with_unmatched_branch",
    "validate_claim_certificate",
    "validate_group_certificate",
]
