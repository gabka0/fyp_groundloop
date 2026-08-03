"""Typed PostgreSQL adapter for the GroundLoop M5 semantic core.

This module persists frozen M5.1 records and reads the independent SQL oracle.
It does not implement the Hall-mask kernel or the M5.4 neural runtime.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from psycopg import Connection

from groundloop.domain import AnswerStatus, ClaimStatus, SubjectKind
from groundloop.errors import ValidationError
from groundloop.m5.domain import (
    ClaimCertificateArtifact,
    ClaimSupportKind,
    CombinedAnswerState,
    CombinedClaimState,
    EvidenceGroupFamily,
    EvidenceGroupValidity,
    EvidenceGroupVersion,
    GroupCertificateRow,
    GroupMatchingCertificateArtifact,
    GroupState,
    RequirementState,
    SnapshotPoint,
)
from groundloop.m5.repository import M5RepositorySnapshot


@dataclass(frozen=True, slots=True)
class M5OracleStates:
    requirements: dict[str, RequirementState]
    groups: dict[str, GroupState]
    claims: dict[str, CombinedClaimState]
    answers: dict[str, CombinedAnswerState]


@dataclass(frozen=True, slots=True)
class M5MismatchCounts:
    requirements: int
    groups: int
    claims: int
    answers: int
    certificates: int
    assignments: int
    assignment_cap_exceeded: int


@dataclass(frozen=True, slots=True)
class M5AssignmentAudit:
    group_version_id: str
    requirement_count: int
    hash_count: int
    edge_count: int
    preflight_state_bound: int
    visited_state_count: int | None
    assignment_matching_size: int | None
    hall_matching_size: int
    audit_status: str


@dataclass(frozen=True, slots=True)
class M5BootstrapProjection:
    """Digest-neutral base-head inputs for coordinator-owned activation.

    Migration 014 owns the persistence surfaces, but the accepted M5.4 runtime
    contract owns activation request/receipt DTOs and changed-state digests.
    This projection deliberately defines neither public activation types nor a
    competing bootstrap hash recipe.
    """

    epoch_id: int
    revision: int
    decision_policy_version: str
    states: M5OracleStates
    group_certificates: dict[str, GroupMatchingCertificateArtifact]
    claim_certificates: dict[str, ClaimCertificateArtifact]


class M5BootstrapConflictError(RuntimeError):
    """The base head cannot produce a valid M5 bootstrap projection."""


def read_m5_oracle_states(connection: Connection[Any]) -> M5OracleStates:
    requirements: dict[str, RequirementState] = {}
    for row in connection.execute(
        """
        SELECT requirement_version_id, witness_hashes,
               supporting_observation_ids, witness_count, satisfied
        FROM groundloop_m5_requirement_state_oracle
        ORDER BY requirement_version_id
        """
    ).fetchall():
        requirement_state = RequirementState(
            requirement_version_id=str(row[0]),
            witness_hashes=tuple(row[1]),
            supporting_observation_ids=tuple(row[2]),
            witness_count=int(row[3]),
            satisfied=bool(row[4]),
        )
        requirements[requirement_state.requirement_version_id] = requirement_state

    groups: dict[str, GroupState] = {}
    for row in connection.execute(
        """
        SELECT group_version_id, requirement_count, satisfied_count,
               matching_size, complete
        FROM groundloop_m5_group_state_oracle
        ORDER BY group_version_id
        """
    ).fetchall():
        group_state = GroupState(
            group_version_id=str(row[0]),
            requirement_count=int(row[1]),
            satisfied_count=int(row[2]),
            matching_size=int(row[3]),
            complete=bool(row[4]),
        )
        groups[group_state.group_version_id] = group_state

    claims: dict[str, CombinedClaimState] = {}
    for row in connection.execute(
        """
        SELECT claim_id, support_count, refute_count, best_support_score,
               best_refute_score, supporting_observation_ids,
               refuting_observation_ids, complete_group_count,
               complete_group_ids, status
        FROM groundloop_m5_claim_state_oracle
        ORDER BY claim_id
        """
    ).fetchall():
        claim_state = CombinedClaimState(
            claim_id=str(row[0]),
            support_count=int(row[1]),
            refute_count=int(row[2]),
            best_support_score=(None if row[3] is None else float(row[3])),
            best_refute_score=(None if row[4] is None else float(row[4])),
            supporting_observation_ids=tuple(row[5]),
            refuting_observation_ids=tuple(row[6]),
            complete_group_count=int(row[7]),
            complete_group_ids=tuple(row[8]),
            status=ClaimStatus(str(row[9])),
        )
        claims[claim_state.claim_id] = claim_state

    answers: dict[str, CombinedAnswerState] = {}
    for row in connection.execute(
        """
        SELECT answer_version_id, required_claim_count, supported_count,
               unsupported_count, refuted_count, conflicted_count, status
        FROM groundloop_m5_answer_state_oracle
        ORDER BY answer_version_id
        """
    ).fetchall():
        answer_state = CombinedAnswerState(
            answer_version_id=str(row[0]),
            required_claim_count=int(row[1]),
            supported_count=int(row[2]),
            unsupported_count=int(row[3]),
            refuted_count=int(row[4]),
            conflicted_count=int(row[5]),
            status=AnswerStatus(str(row[6])),
        )
        answers[answer_state.answer_version_id] = answer_state
    return M5OracleStates(requirements, groups, claims, answers)


def read_m5_mismatch_counts(connection: Connection[Any]) -> M5MismatchCounts:
    row = connection.execute(
        """
        SELECT requirement_mismatches, group_mismatches, claim_mismatches,
               answer_mismatches, certificate_mismatches,
               assignment_mismatches, assignment_cap_exceeded
        FROM groundloop_m5_oracle_mismatch_counts
        """
    ).fetchone()
    assert row is not None
    return M5MismatchCounts(*(int(value) for value in row))


def read_m5_assignment_audits(
    connection: Connection[Any],
) -> tuple[M5AssignmentAudit, ...]:
    rows = connection.execute(
        """
        SELECT group_version_id, requirement_count, hash_count, edge_count,
               preflight_state_bound, visited_state_count,
               assignment_matching_size, hall_matching_size, audit_status
        FROM groundloop_m5_assignment_audit_oracle
        ORDER BY group_version_id
        """
    ).fetchall()
    return tuple(
        M5AssignmentAudit(
            group_version_id=str(row[0]),
            requirement_count=int(row[1]),
            hash_count=int(row[2]),
            edge_count=int(row[3]),
            preflight_state_bound=int(row[4]),
            visited_state_count=None if row[5] is None else int(row[5]),
            assignment_matching_size=None if row[6] is None else int(row[6]),
            hall_matching_size=int(row[7]),
            audit_status=str(row[8]),
        )
        for row in rows
    )


def _insert_group_family(
    connection: Connection[Any],
    family: EvidenceGroupFamily,
    *,
    lifecycle_state: str,
) -> None:
    connection.execute(
        """
        INSERT INTO groundloop_m5_group_family (
            group_family_id, claim_id, creator_epoch_id, lifecycle_state
        ) VALUES (%s, %s, %s, %s)
        """,
        (
            family.group_family_id,
            family.claim_id,
            family.created_epoch,
            lifecycle_state,
        ),
    )


def _insert_group_version(
    connection: Connection[Any],
    group: EvidenceGroupVersion,
    *,
    creator_epoch_id: int,
    lifecycle_state: str,
) -> None:
    connection.execute(
        """
        INSERT INTO groundloop_m5_group_version (
            group_version_id, group_family_id, creator_epoch_id,
            lifecycle_state, group_type, construction_kind,
            construction_source_id, constructor_model_id,
            constructor_model_version, constructor_prompt_version,
            supersedes_group_version_id, semantic_structure_hash,
            record_payload_hash
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            group.group_version_id,
            group.group_family_id,
            creator_epoch_id,
            lifecycle_state,
            group.group_type.value,
            group.construction_kind.value,
            group.construction_source_id,
            group.constructor_model_id,
            group.constructor_model_version,
            group.constructor_prompt_version,
            group.supersedes_group_version_id,
            group.semantic_structure_hash,
            group.record_payload_hash,
        ),
    )
    for requirement in group.requirements:
        connection.execute(
            """
            INSERT INTO groundloop_m5_requirement_version (
                requirement_version_id, group_version_id, creator_epoch_id,
                lifecycle_state, ordinal, requirement_text,
                requirement_text_hash, constructor_model_id,
                constructor_model_version, constructor_prompt_version,
                supersedes_requirement_version_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                requirement.requirement_version_id,
                requirement.group_version_id,
                creator_epoch_id,
                lifecycle_state,
                requirement.ordinal,
                requirement.requirement_text,
                requirement.requirement_text_hash,
                requirement.constructor_model_id,
                requirement.constructor_model_version,
                requirement.constructor_prompt_version,
                requirement.supersedes_requirement_version_id,
            ),
        )


def persist_published_group(
    connection: Connection[Any],
    *,
    family: EvidenceGroupFamily | None,
    group: EvidenceGroupVersion,
    validity: EvidenceGroupValidity,
) -> None:
    """Persist one already-published immutable group version.

    ``family`` is supplied for an initial version and omitted for a successor
    whose family row already exists. Requirements (and their registry rows) are
    inserted before any caller can insert typed observations.
    """

    if family is not None:
        _insert_group_family(connection, family, lifecycle_state="PUBLISHED")
    _insert_group_version(
        connection,
        group,
        creator_epoch_id=validity.valid_from_epoch,
        lifecycle_state="PUBLISHED",
    )
    connection.execute(
        """
        INSERT INTO groundloop_m5_group_validity (
            group_version_id, group_family_id, claim_id,
            semantic_structure_hash, supersedes_group_version_id,
            valid_from_epoch, valid_to_epoch
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            group.group_version_id,
            group.group_family_id,
            group.owner_claim_id,
            group.semantic_structure_hash,
            group.supersedes_group_version_id,
            validity.valid_from_epoch,
            validity.valid_to_epoch,
        ),
    )


def _current_requirement_holders(
    snapshot: M5RepositorySnapshot,
) -> tuple[tuple[tuple[SubjectKind, str, str, str], str, SnapshotPoint], ...]:
    if not snapshot.max_revision_by_epoch:
        return ()
    current_epoch, current_revision = snapshot.max_revision_by_epoch[-1]
    point = SnapshotPoint(current_epoch, current_revision)
    holders: list[tuple[tuple[SubjectKind, str, str, str], str, SnapshotPoint]] = []
    for interval in snapshot.currency_history:
        if interval.observation_id is not None and interval.contains(point):
            holders.append((interval.key, interval.observation_id, interval.valid_from))
    return tuple(
        sorted(holders, key=lambda item: tuple(str(value) for value in item[0]))
    )


def load_m5_repository_snapshot(
    connection: Connection[Any],
    snapshot: M5RepositorySnapshot,
) -> None:
    """Load M5.1 semantic base rows after the legacy base snapshot.

    The loader intentionally orders family/group/requirement subjects before
    requirement observations. M5.1's compact total-order currency history is
    not misrepresented as revision history: strict current holders are loaded
    into shared current/published currency, while exact epoch-local history is
    loaded through ``replace_m5_working_currency`` during live histories.
    """

    validity_by_group = {
        validity.group_version_id: validity for validity in snapshot.group_validity
    }
    family_by_id = {family.group_family_id: family for family in snapshot.families}
    inserted_families: set[str] = set()
    for group in sorted(
        snapshot.groups,
        key=lambda value: (
            validity_by_group[value.group_version_id].valid_from_epoch,
            value.group_family_id,
            value.group_version_id,
        ),
    ):
        family = None
        if group.group_family_id not in inserted_families:
            family = family_by_id[group.group_family_id]
            inserted_families.add(group.group_family_id)
        persist_published_group(
            connection,
            family=family,
            group=group,
            validity=validity_by_group[group.group_version_id],
        )
    for retirement in snapshot.retirements:
        connection.execute(
            """
            INSERT INTO groundloop_m5_group_family_retirement (
                group_family_id, retired_epoch_id, event_id
            ) VALUES (%s, %s, %s)
            """,
            (
                retirement.group_family_id,
                retirement.retired_epoch_id,
                retirement.event_id,
            ),
        )

    for record in snapshot.observations:
        observation = record.observation
        connection.execute(
            """
            INSERT INTO groundloop_semantic_observation (
                observation_id, subject_kind, subject_id, chunk_version_id,
                task_type, support_score, refute_score, neutral_score,
                model_id, model_version, prompt_version, input_hash,
                produced_epoch, raw_output_hash, eligible_for_currency
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, NULL, %s
            )
            """,
            (
                observation.observation_id,
                observation.subject_kind.value,
                observation.subject_id,
                observation.chunk_version_id,
                observation.task_type,
                observation.support_score,
                observation.refute_score,
                observation.neutral_score,
                observation.producer.model_id,
                observation.producer.model_version,
                observation.producer.prompt_version,
                observation.input_hash,
                record.produced_at.epoch_id,
                record.eligible_for_currency,
            ),
        )

    for key, observation_id, valid_from in _current_requirement_holders(snapshot):
        connection.execute(
            """
            INSERT INTO groundloop_observation_currency (
                subject_kind, subject_id, chunk_version_id, task_type,
                observation_id, installed_revision
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                key[0].value,
                key[1],
                key[2],
                key[3],
                observation_id,
                valid_from.revision,
            ),
        )
        connection.execute(
            """
            INSERT INTO groundloop_published_observation_currency (
                subject_kind, subject_id, chunk_version_id, task_type,
                observation_id, valid_from_epoch, valid_to_epoch
            ) VALUES (%s, %s, %s, %s, %s, %s, NULL)
            """,
            (
                key[0].value,
                key[1],
                key[2],
                key[3],
                observation_id,
                valid_from.epoch_id,
            ),
        )
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.execute("SET CONSTRAINTS ALL DEFERRED")


def replace_m5_working_currency(
    connection: Connection[Any],
    *,
    epoch_id: int,
    subject_kind: SubjectKind,
    subject_id: str,
    chunk_version_id: str,
    task_type: str,
    observation_id: str | None,
    revision: int,
) -> None:
    """Close the prior interval and install one holder/tombstone atomically."""

    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
        raise ValidationError("currency revision must be a nonnegative integer")
    effective_prior = connection.execute(
        """
        SELECT observation_id
        FROM groundloop_m5_currency_at(%s, %s)
        WHERE subject_kind = %s
          AND subject_id = %s
          AND chunk_version_id = %s
          AND task_type = %s
        """,
        (
            epoch_id,
            revision,
            subject_kind.value,
            subject_id,
            chunk_version_id,
            task_type,
        ),
    ).fetchone()
    if (
        effective_prior is None
        and observation_id is None
        or effective_prior is not None
        and effective_prior[0] == observation_id
    ):
        raise ValidationError("currency replacement must change the effective holder")
    prior = connection.execute(
        """
        SELECT valid_from_revision, observation_id
        FROM groundloop_m5_working_currency_history
        WHERE epoch_id = %s
          AND subject_kind = %s
          AND subject_id = %s
          AND chunk_version_id = %s
          AND task_type = %s
          AND valid_to_revision IS NULL
        FOR UPDATE
        """,
        (
            epoch_id,
            subject_kind.value,
            subject_id,
            chunk_version_id,
            task_type,
        ),
    ).fetchone()
    if prior is not None:
        if revision <= int(prior[0]):
            raise ValidationError("currency revisions must advance")
        connection.execute(
            """
            UPDATE groundloop_m5_working_currency_history
            SET valid_to_revision = %s
            WHERE epoch_id = %s
              AND subject_kind = %s
              AND subject_id = %s
              AND chunk_version_id = %s
              AND task_type = %s
              AND valid_to_revision IS NULL
            """,
            (
                revision,
                epoch_id,
                subject_kind.value,
                subject_id,
                chunk_version_id,
                task_type,
            ),
        )
    connection.execute(
        """
        INSERT INTO groundloop_m5_working_currency_history (
            epoch_id, subject_kind, subject_id, chunk_version_id, task_type,
            observation_id, valid_from_revision, valid_to_revision
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, NULL)
        """,
        (
            epoch_id,
            subject_kind.value,
            subject_id,
            chunk_version_id,
            task_type,
            observation_id,
            revision,
        ),
    )


def read_m5_currency_at(
    connection: Connection[Any],
    *,
    epoch_id: int,
    revision: int,
) -> dict[tuple[SubjectKind, str, str, str], str | None]:
    if isinstance(epoch_id, bool) or not isinstance(epoch_id, int) or epoch_id < 0:
        raise ValidationError("currency epoch must be a nonnegative integer")
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
        raise ValidationError("currency revision must be a nonnegative integer")
    rows = connection.execute(
        """
        SELECT subject_kind, subject_id, chunk_version_id, task_type,
               observation_id
        FROM groundloop_m5_currency_at(%s, %s)
        ORDER BY subject_kind, subject_id, chunk_version_id, task_type
        """,
        (epoch_id, revision),
    ).fetchall()
    return {
        (SubjectKind(str(row[0])), str(row[1]), str(row[2]), str(row[3])): (
            None if row[4] is None else str(row[4])
        )
        for row in rows
    }


def persist_group_certificate(
    connection: Connection[Any],
    certificate: GroupMatchingCertificateArtifact,
) -> None:
    connection.execute(
        """
        INSERT INTO groundloop_m5_group_certificate_artifact (
            certificate_digest, decision_policy_version,
            certificate_version, group_version_id, requirement_count
        ) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (certificate_digest) DO NOTHING
        """,
        (
            certificate.certificate_digest,
            certificate.decision_policy_version,
            certificate.certificate_version,
            certificate.group_version_id,
            certificate.requirement_count,
        ),
    )
    for row in certificate.rows:
        connection.execute(
            """
            INSERT INTO groundloop_m5_group_certificate_artifact_row (
                certificate_digest, requirement_ordinal,
                requirement_version_id, text_hash,
                selected_observation_id
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (certificate_digest, requirement_ordinal) DO NOTHING
            """,
            (
                certificate.certificate_digest,
                row.requirement_ordinal,
                row.requirement_version_id,
                row.text_hash,
                row.selected_observation_id,
            ),
        )


def persist_claim_certificate(
    connection: Connection[Any],
    certificate: ClaimCertificateArtifact,
) -> None:
    connection.execute(
        """
        INSERT INTO groundloop_m5_claim_certificate_artifact (
            certificate_digest, certificate_version, claim_id,
            decision_policy_version, support_kind,
            direct_support_observation_id, group_version_id,
            group_certificate_digest, direct_refute_observation_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (certificate_digest) DO NOTHING
        """,
        (
            certificate.certificate_digest,
            certificate.certificate_version,
            certificate.claim_id,
            certificate.decision_policy_version,
            certificate.support_kind.value,
            certificate.direct_support_observation_id,
            certificate.group_version_id,
            certificate.group_certificate_digest,
            certificate.direct_refute_observation_id,
        ),
    )


def _build_bootstrap_certificates(
    connection: Connection[Any],
    states: M5OracleStates,
    decision_policy_version: str,
) -> tuple[
    dict[str, GroupMatchingCertificateArtifact],
    dict[str, ClaimCertificateArtifact],
]:
    # This constructor is used only to produce persisted witness artifacts for
    # activation. The independent SQL oracle remains the checked-in SQL views
    # and never invokes or imports the Python reference oracle.
    from groundloop.m5.reference import maximum_matching_with_unmatched_branch

    group_certificates: dict[str, GroupMatchingCertificateArtifact] = {}
    for bootstrap_group_id, group_state in sorted(states.groups.items()):
        if not group_state.complete:
            continue
        edge_rows = connection.execute(
            """
            SELECT requirement_ordinal, requirement_version_id, text_hash,
                   active_observation_ids
            FROM groundloop_m5_active_requirement_edge_oracle
            WHERE group_version_id = %s
            ORDER BY requirement_ordinal, text_hash
            """,
            (bootstrap_group_id,),
        ).fetchall()
        requirement_ids = tuple(
            str(row[0])
            for row in connection.execute(
                """
                SELECT requirement_version_id
                FROM groundloop_m5_requirement_version
                WHERE group_version_id = %s
                  AND lifecycle_state = 'PUBLISHED'
                ORDER BY ordinal
                """,
                (bootstrap_group_id,),
            ).fetchall()
        )
        neighbors: list[list[str]] = [[] for _ in requirement_ids]
        observations_by_edge: dict[tuple[int, str], tuple[str, ...]] = {}
        for row in edge_rows:
            ordinal = int(row[0])
            text_hash = str(row[2])
            neighbors[ordinal].append(text_hash)
            observations_by_edge[(ordinal, text_hash)] = tuple(row[3])
        matching = maximum_matching_with_unmatched_branch(
            tuple(tuple(values) for values in neighbors)
        )
        if matching.size != len(requirement_ids):
            raise M5BootstrapConflictError(
                "SQL complete group "
                f"{bootstrap_group_id} has no bootstrap covering matching"
            )
        rows = tuple(
            GroupCertificateRow(
                requirement_ordinal=ordinal,
                requirement_version_id=requirement_ids[ordinal],
                text_hash=text_hash,
                selected_observation_id=observations_by_edge[(ordinal, text_hash)][0],
            )
            for ordinal, text_hash_or_none in enumerate(matching.assignment_by_ordinal)
            for text_hash in (text_hash_or_none,)
            if text_hash is not None
        )
        group_certificates[bootstrap_group_id] = GroupMatchingCertificateArtifact(
            decision_policy_version=decision_policy_version,
            group_version_id=bootstrap_group_id,
            rows=rows,
        )

    claim_certificates: dict[str, ClaimCertificateArtifact] = {}
    for claim_id, state in sorted(states.claims.items()):
        direct_support: str | None
        support_group_id: str | None
        group_digest: str | None
        if state.supporting_observation_ids:
            support_kind = ClaimSupportKind.DIRECT
            direct_support = state.supporting_observation_ids[0]
            support_group_id = None
            group_digest = None
        elif state.complete_group_ids:
            support_kind = ClaimSupportKind.GROUP
            direct_support = None
            support_group_id = state.complete_group_ids[0]
            group_digest = group_certificates[support_group_id].certificate_digest
        else:
            support_kind = ClaimSupportKind.NONE
            direct_support = None
            support_group_id = None
            group_digest = None
        claim_certificates[claim_id] = ClaimCertificateArtifact(
            claim_id=claim_id,
            decision_policy_version=decision_policy_version,
            support_kind=support_kind,
            direct_support_observation_id=direct_support,
            group_version_id=support_group_id,
            group_certificate_digest=group_digest,
            direct_refute_observation_id=(
                state.refuting_observation_ids[0]
                if state.refuting_observation_ids
                else None
            ),
        )
    return group_certificates, claim_certificates


def write_m5_materialized_states(
    connection: Connection[Any],
    *,
    states: M5OracleStates,
    decision_policy_version: str,
    epoch_id: int,
    revision: int,
    group_certificates: dict[str, GroupMatchingCertificateArtifact],
    claim_certificates: dict[str, ClaimCertificateArtifact],
    publish: bool,
) -> None:
    for group_certificate in group_certificates.values():
        persist_group_certificate(connection, group_certificate)
    for claim_certificate in claim_certificates.values():
        persist_claim_certificate(connection, claim_certificate)

    for requirement_id, requirement_state in sorted(states.requirements.items()):
        connection.execute(
            """
            INSERT INTO groundloop_m5_requirement_state_materialized (
                requirement_version_id, witness_hashes,
                supporting_observation_ids, witness_count, satisfied,
                decision_policy_version, updated_epoch, updated_revision
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                requirement_id,
                list(requirement_state.witness_hashes),
                list(requirement_state.supporting_observation_ids),
                requirement_state.witness_count,
                requirement_state.satisfied,
                decision_policy_version,
                epoch_id,
                revision,
            ),
        )
        if publish:
            connection.execute(
                """
                INSERT INTO groundloop_m5_published_requirement_state (
                    requirement_version_id, valid_from_epoch, valid_to_epoch,
                    sealed_revision, witness_hashes,
                    supporting_observation_ids, witness_count, satisfied,
                    decision_policy_version
                ) VALUES (%s, %s, NULL, %s, %s, %s, %s, %s, %s)
                """,
                (
                    requirement_id,
                    epoch_id,
                    revision,
                    list(requirement_state.witness_hashes),
                    list(requirement_state.supporting_observation_ids),
                    requirement_state.witness_count,
                    requirement_state.satisfied,
                    decision_policy_version,
                ),
            )

    for group_id, group_state in sorted(states.groups.items()):
        state_group_certificate = group_certificates.get(group_id)
        digest = (
            None
            if state_group_certificate is None
            else state_group_certificate.certificate_digest
        )
        connection.execute(
            """
            INSERT INTO groundloop_m5_group_state_materialized (
                group_version_id, requirement_count, satisfied_count,
                matching_size, complete, decision_policy_version,
                certificate_digest, updated_epoch, updated_revision
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                group_id,
                group_state.requirement_count,
                group_state.satisfied_count,
                group_state.matching_size,
                group_state.complete,
                decision_policy_version,
                digest,
                epoch_id,
                revision,
            ),
        )
        if publish:
            connection.execute(
                """
                INSERT INTO groundloop_m5_published_group_state (
                    group_version_id, valid_from_epoch, valid_to_epoch,
                    sealed_revision, requirement_count, satisfied_count,
                    matching_size, complete, decision_policy_version,
                    certificate_digest
                ) VALUES (%s, %s, NULL, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    group_id,
                    epoch_id,
                    revision,
                    group_state.requirement_count,
                    group_state.satisfied_count,
                    group_state.matching_size,
                    group_state.complete,
                    decision_policy_version,
                    digest,
                ),
            )
            if state_group_certificate is not None:
                connection.execute(
                    """
                    INSERT INTO groundloop_m5_published_group_certificate_binding (
                        group_version_id, valid_from_epoch, valid_to_epoch,
                        sealed_revision, certificate_digest
                    ) VALUES (%s, %s, NULL, %s, %s)
                    """,
                    (group_id, epoch_id, revision, digest),
                )

    for claim_id, claim_state in sorted(states.claims.items()):
        certificate = claim_certificates[claim_id]
        claim_values = (
            claim_id,
            claim_state.support_count,
            claim_state.refute_count,
            claim_state.best_support_score,
            claim_state.best_refute_score,
            list(claim_state.supporting_observation_ids),
            list(claim_state.refuting_observation_ids),
            claim_state.complete_group_count,
            list(claim_state.complete_group_ids),
            claim_state.status.value,
            decision_policy_version,
            certificate.certificate_digest,
        )
        connection.execute(
            """
            INSERT INTO groundloop_m5_claim_state_materialized (
                claim_id, support_count, refute_count, best_support_score,
                best_refute_score, supporting_observation_ids,
                refuting_observation_ids, complete_group_count,
                complete_group_ids, status, decision_policy_version,
                certificate_digest, updated_epoch, updated_revision
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s
            )
            """,
            (*claim_values, epoch_id, revision),
        )
        if publish:
            connection.execute(
                """
                INSERT INTO groundloop_m5_published_claim_state (
                    claim_id, valid_from_epoch, valid_to_epoch,
                    sealed_revision, support_count, refute_count,
                    best_support_score, best_refute_score,
                    supporting_observation_ids, refuting_observation_ids,
                    complete_group_count, complete_group_ids, status,
                    decision_policy_version, certificate_digest
                ) VALUES (
                    %s, %s, NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s
                )
                """,
                (
                    claim_id,
                    epoch_id,
                    revision,
                    claim_state.support_count,
                    claim_state.refute_count,
                    claim_state.best_support_score,
                    claim_state.best_refute_score,
                    list(claim_state.supporting_observation_ids),
                    list(claim_state.refuting_observation_ids),
                    claim_state.complete_group_count,
                    list(claim_state.complete_group_ids),
                    claim_state.status.value,
                    decision_policy_version,
                    certificate.certificate_digest,
                ),
            )
            connection.execute(
                """
                INSERT INTO groundloop_m5_published_claim_certificate_binding (
                    claim_id, valid_from_epoch, valid_to_epoch,
                    sealed_revision, certificate_digest
                ) VALUES (%s, %s, NULL, %s, %s)
                """,
                (
                    claim_id,
                    epoch_id,
                    revision,
                    certificate.certificate_digest,
                ),
            )

    for answer_id, answer_state in sorted(states.answers.items()):
        answer_values = (
            answer_id,
            answer_state.required_claim_count,
            answer_state.supported_count,
            answer_state.unsupported_count,
            answer_state.refuted_count,
            answer_state.conflicted_count,
            answer_state.status.value,
        )
        connection.execute(
            """
            INSERT INTO groundloop_m5_answer_state_materialized (
                answer_version_id, required_claim_count, supported_count,
                unsupported_count, refuted_count, conflicted_count, status,
                updated_epoch, updated_revision
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (*answer_values, epoch_id, revision),
        )
        if publish:
            connection.execute(
                """
                INSERT INTO groundloop_m5_published_answer_state (
                    answer_version_id, valid_from_epoch, valid_to_epoch,
                    sealed_revision, required_claim_count, supported_count,
                    unsupported_count, refuted_count, conflicted_count, status
                ) VALUES (%s, %s, NULL, %s, %s, %s, %s, %s, %s, %s)
                """,
                (answer_id, epoch_id, revision, *answer_values[1:]),
            )


def build_m5_bootstrap_projection(
    connection: Connection[Any],
) -> M5BootstrapProjection:
    """Build digest-neutral activation inputs from the current sealed head.

    The coordinator-owned runtime layer turns this projection into exact
    ``M5ChangedStateReference`` values and the frozen
    ``m5-changed-state-set-v2`` digest before it calls the persistence writers.
    """

    snapshot = connection.execute(
        "SELECT epoch_id, policy_version FROM groundloop_m5_oracle_snapshot"
    ).fetchone()
    if snapshot is None:
        raise M5BootstrapConflictError("M5 bootstrap has no sealed head/policy")
    epoch_id = int(snapshot[0])
    decision_policy_version = str(snapshot[1])
    epoch = connection.execute(
        "SELECT revision FROM groundloop_epoch WHERE epoch_id = %s",
        (epoch_id,),
    ).fetchone()
    if epoch is None:
        raise M5BootstrapConflictError("M5 bootstrap head epoch is missing")
    states = read_m5_oracle_states(connection)
    group_certificates, claim_certificates = _build_bootstrap_certificates(
        connection,
        states,
        decision_policy_version,
    )
    return M5BootstrapProjection(
        epoch_id=epoch_id,
        revision=int(epoch[0]),
        decision_policy_version=decision_policy_version,
        states=states,
        group_certificates=group_certificates,
        claim_certificates=claim_certificates,
    )


__all__ = [
    "M5AssignmentAudit",
    "M5BootstrapConflictError",
    "M5BootstrapProjection",
    "M5MismatchCounts",
    "M5OracleStates",
    "build_m5_bootstrap_projection",
    "load_m5_repository_snapshot",
    "persist_claim_certificate",
    "persist_group_certificate",
    "persist_published_group",
    "read_m5_assignment_audits",
    "read_m5_currency_at",
    "read_m5_mismatch_counts",
    "read_m5_oracle_states",
    "replace_m5_working_currency",
    "write_m5_materialized_states",
]
