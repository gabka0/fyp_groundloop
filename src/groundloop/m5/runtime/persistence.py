"""Failure-atomic PostgreSQL persistence for the typed M5 runtime.

Migration 015 owns runtime coordination rows while migration 014 owns group
structure and semantic state.  This module is the only M5 runtime layer that
opens database transactions spanning both surfaces.
"""

from __future__ import annotations

import hashlib
import secrets
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from psycopg import Connection, Cursor, sql

from groundloop.domain import StatusDelta, SubjectKind
from groundloop.errors import EventConflictError, InvalidEventError, ValidationError
from groundloop.events import (
    DeleteDocumentVersionEvent,
    InsertDocumentEvent,
    ObserveEvent,
    PolicyChangeEvent,
    ReplaceDocumentVersionEvent,
)
from groundloop.m4.application import OpenEventReceipt, PublicationReceipt
from groundloop.m4.contracts import VectorIndexKind
from groundloop.m5.domain import EvidenceGroupVersion
from groundloop.m5.events import (
    ObserveRequirementEvent,
    RegisterGroupEvent,
    ReplaceGroupEvent,
    RetireGroupEvent,
)
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    M5AcquisitionDisposition,
    M5ActivationReceipt,
    M5ActivationRequest,
    M5AttemptCompletionReceipt,
    M5AttemptOutput,
    M5CancellationPlan,
    M5CancellationReceipt,
    M5CandidatePolicyManifest,
    M5ChangedStateReference,
    M5DiscoveryDirection,
    M5DiscoveryScopeContract,
    M5EventRunResult,
    M5ExecutionEvidenceDisposition,
    M5JobAttempt,
    M5JobCompletion,
    M5JobKind,
    M5JobLease,
    M5JobState,
    M5LeaseTerminalProjection,
    M5LogicalJobSpec,
    M5ReplayedOutcome,
    M5RequirementAttemptReturnReceipt,
    M5RequirementDiscoveryResult,
    M5RequirementPairInput,
    M5RequirementVerifierArtifact,
    M5RootBarrierReceipt,
    M5RunFailureReason,
    M5RunState,
    M5RuntimeOperationalConfig,
    M5RuntimeTiming,
    M5RuntimeTimingCoverage,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5StateReferenceKind,
    M5TerminalReason,
    M5TransitionTimingReceipt,
    M5TypedEventPlan,
    RequirementRegistrySnapshot,
    SemanticPairKey,
)
from groundloop.m5.runtime.postgres_recovery import (
    RequirementExecutionAccounting,
    StoredRequirementAttempt,
    append_transition_call_timing_checked,
    build_requirement_execution_accounting,
    finish_requirement_dispatch_accounting,
    finish_requirement_execution_accounting,
    load_requirement_dispatch,
    persist_requirement_dispatch,
    persist_requirement_execution_accounting,
    persist_structural_open_accounting,
    persist_structural_open_identity,
    persist_terminal_invocation_telemetry,
    persist_terminal_job_failure_contribution,
    read_acquisition_clock,
    read_current_event_timing,
    read_latest_requirement_attempt,
    read_requirement_attempt,
    read_requirement_execution_replay,
    require_runtime_recovery_bundle,
    requirement_fallback_required,
    root_result_is_reserved,
    start_event_accounting,
    validate_structural_open_recovery,
)
from groundloop.m5.runtime.postgres_roots import (
    cancel_m5_work,
    close_m5_requirement_roots,
    stage_m5_discovery_result,
)
from groundloop.m5.runtime.postgres_verifier import complete_m5_verifier
from groundloop.postgres.m5 import (
    M5BootstrapProjection,
    build_m5_bootstrap_projection,
    write_m5_materialized_states,
)

RuntimeFailureInjector = Callable[[str], None]

_NORMALIZER_ID = "m5-normalize-text-v1"
_NORMALIZER_PROVENANCE_HASH = (
    "d91b94f256f79c6bc7b29fafa41c7a608b90c17bd479b29a64be00fa538c49fb"
)
_WORK_COUNTER_COLUMNS = M5RuntimeWork.counter_names()
_STRUCTURAL_OPEN_EPOCH_ROWS = (
    ("groundloop_epoch", "epoch_id"),
    ("groundloop_m5_update", "epoch_id"),
    ("groundloop_m5_runtime_epoch", "epoch_id"),
    ("groundloop_m5_group_family", "creator_epoch_id"),
    ("groundloop_m5_group_version", "creator_epoch_id"),
    ("groundloop_m5_requirement_version", "creator_epoch_id"),
    ("groundloop_m5_group_deactivation", "epoch_id"),
    ("groundloop_m5_requirement_registry_snapshot", "created_epoch_id"),
    ("groundloop_m5_active_chunk_snapshot", "created_epoch_id"),
    ("groundloop_m5_discovery_scope", "epoch_id"),
    ("groundloop_m5_semantic_job", "epoch_id"),
    ("groundloop_m5_owner_pending_counter", "epoch_id"),
    ("groundloop_m5_answer_pending_counter", "epoch_id"),
    ("groundloop_m5_runtime_operational_config", "epoch_id"),
    ("groundloop_m5_requirement_root_provenance", "epoch_id"),
)


@dataclass(frozen=True, slots=True)
class _PublishedGroupOwner:
    group_family_id: str
    claim_id: str


@dataclass(frozen=True, slots=True)
class _RootDeclaration:
    scope: M5DiscoveryScopeContract
    job: M5LogicalJobSpec


@dataclass(frozen=True, slots=True)
class _RuntimeEpochHeader:
    epoch_id: int
    structural_event_id: str
    revision: int
    structural_status: str
    semantic_status: str
    evaluation_state: str
    runtime_state: str
    open_work_count: int
    open_scope_count: int
    blocking_failure_count: int


@dataclass(frozen=True, slots=True)
class _StoredM5Job:
    spec: M5LogicalJobSpec
    state: M5JobState
    admitted_pair_digest: str | None
    completion: M5JobCompletion | None
    completed_revision: int | None


@dataclass(frozen=True, slots=True)
class _StoredM5Attempt:
    attempt: M5JobAttempt
    state: str
    attempt_output_digest: str | None
    error_hash: str | None
    finished: bool


def build_m5_bootstrap_changed_state_references(
    projection: M5BootstrapProjection,
) -> tuple[M5ChangedStateReference, ...]:
    """Build the frozen six-kind activation projection from independent state."""

    references: list[M5ChangedStateReference] = []
    for requirement_id, requirement_state in sorted(
        projection.states.requirements.items()
    ):
        references.append(
            M5ChangedStateReference.build(
                kind=M5StateReferenceKind.REQUIREMENT_STATE,
                object_id=requirement_id,
                epoch_id=projection.epoch_id,
                revision=projection.revision,
                state_artifact_hash=digests.requirement_state_artifact_digest(
                    requirement_version_id=requirement_id,
                    witness_hashes=requirement_state.witness_hashes,
                    supporting_observation_ids=(
                        requirement_state.supporting_observation_ids
                    ),
                    witness_count=requirement_state.witness_count,
                    satisfied=requirement_state.satisfied,
                    decision_policy_version=projection.decision_policy_version,
                ),
            )
        )
    for group_id, group_state in sorted(projection.states.groups.items()):
        group_certificate = projection.group_certificates.get(group_id)
        certificate_digest = (
            None if group_certificate is None else group_certificate.certificate_digest
        )
        references.append(
            M5ChangedStateReference.build(
                kind=M5StateReferenceKind.GROUP_STATE,
                object_id=group_id,
                epoch_id=projection.epoch_id,
                revision=projection.revision,
                state_artifact_hash=digests.group_state_artifact_digest(
                    group_version_id=group_id,
                    requirement_count=group_state.requirement_count,
                    satisfied_count=group_state.satisfied_count,
                    matching_size=group_state.matching_size,
                    complete=group_state.complete,
                    decision_policy_version=projection.decision_policy_version,
                    certificate_digest=certificate_digest,
                ),
            )
        )
        if group_certificate is not None:
            references.append(
                M5ChangedStateReference.build(
                    kind=M5StateReferenceKind.GROUP_CERTIFICATE,
                    object_id=group_id,
                    epoch_id=projection.epoch_id,
                    revision=projection.revision,
                    state_artifact_hash=group_certificate.certificate_digest,
                )
            )
    for claim_id, claim_state in sorted(projection.states.claims.items()):
        claim_certificate = projection.claim_certificates[claim_id]
        references.append(
            M5ChangedStateReference.build(
                kind=M5StateReferenceKind.CLAIM_STATE,
                object_id=claim_id,
                epoch_id=projection.epoch_id,
                revision=projection.revision,
                state_artifact_hash=digests.claim_state_artifact_digest(
                    claim_id=claim_id,
                    support_count=claim_state.support_count,
                    refute_count=claim_state.refute_count,
                    best_support_score=claim_state.best_support_score,
                    best_refute_score=claim_state.best_refute_score,
                    supporting_observation_ids=(claim_state.supporting_observation_ids),
                    refuting_observation_ids=claim_state.refuting_observation_ids,
                    complete_group_count=claim_state.complete_group_count,
                    complete_group_ids=claim_state.complete_group_ids,
                    status=claim_state.status,
                    decision_policy_version=projection.decision_policy_version,
                    certificate_digest=claim_certificate.certificate_digest,
                ),
            )
        )
        references.append(
            M5ChangedStateReference.build(
                kind=M5StateReferenceKind.CLAIM_CERTIFICATE,
                object_id=claim_id,
                epoch_id=projection.epoch_id,
                revision=projection.revision,
                state_artifact_hash=claim_certificate.certificate_digest,
            )
        )
    for answer_id, answer_state in sorted(projection.states.answers.items()):
        references.append(
            M5ChangedStateReference.build(
                kind=M5StateReferenceKind.ANSWER_STATE,
                object_id=answer_id,
                epoch_id=projection.epoch_id,
                revision=projection.revision,
                state_artifact_hash=digests.answer_state_artifact_digest(
                    answer_version_id=answer_id,
                    required_claim_count=answer_state.required_claim_count,
                    supported_count=answer_state.supported_count,
                    unsupported_count=answer_state.unsupported_count,
                    refuted_count=answer_state.refuted_count,
                    conflicted_count=answer_state.conflicted_count,
                    status=answer_state.status,
                ),
            )
        )
    return tuple(
        sorted(
            references,
            key=lambda item: (
                item.kind.value,
                item.object_id,
                item.reference_digest,
            ),
        )
    )


def m5_bootstrap_state_hash(projection: M5BootstrapProjection) -> str:
    references = build_m5_bootstrap_changed_state_references(projection)
    return digests.changed_state_set_digest(
        reference.reference_digest for reference in references
    )


def _runtime_bundle_ledgers(cursor: Cursor[Any]) -> tuple[str, str]:
    rows = cursor.execute(
        """
        SELECT bundle_id, bundle_sha256, prerequisite_sha256
        FROM groundloop_m5_schema_bundle
        WHERE bundle_id IN (
            'm5-core-schema-bundle-v1', 'm5-runtime-schema-bundle-v2'
        )
        ORDER BY bundle_id COLLATE "C"
        """
    ).fetchall()
    ledgers = {str(row[0]): (str(row[1]).strip(), str(row[2]).strip()) for row in rows}
    core = ledgers.get("m5-core-schema-bundle-v1")
    runtime = ledgers.get("m5-runtime-schema-bundle-v2")
    if core is None or runtime is None or runtime[1] != core[0]:
        raise InvalidEventError("activation requires the exact core/runtime bundles")
    return core[0], runtime[0]


def _activation_receipt_from_row(
    request: M5ActivationRequest,
    row: tuple[Any, ...],
) -> M5ActivationReceipt:
    activation_id = str(row[0])
    payload_hash = str(row[1]).strip()
    base_epoch_id = int(row[2])
    mode = str(row[3])
    mode_revision = int(row[4])
    m4_head = int(row[5])
    m5_head = int(row[6])
    if activation_id != request.activation_id or payload_hash != request.payload_hash:
        raise EventConflictError("M5 is already activated by another request")
    receipt = M5ActivationReceipt.build(request, replayed=True)
    if (
        base_epoch_id != request.expected_base_m4_epoch_id
        or mode != "m5_active"
        or mode_revision != receipt.mode_revision
        or m4_head != base_epoch_id
        or m5_head != base_epoch_id
    ):
        raise ValidationError("durable M5 activation state is inconsistent")
    return receipt


def _inject(injector: RuntimeFailureInjector | None, point: str) -> None:
    if injector is not None:
        injector(point)


def _force_deferred_validation(cursor: Cursor[Any]) -> None:
    cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
    cursor.execute("SET CONSTRAINTS ALL DEFERRED")


def _require_sha256(name: str, value: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValidationError(f"{name} must be a lowercase SHA-256 digest")


def _read_runtime_epoch_header(
    cursor: Cursor[Any], epoch_id: int, *, for_update: bool
) -> _RuntimeEpochHeader:
    if isinstance(epoch_id, bool) or not isinstance(epoch_id, int) or epoch_id <= 0:
        raise InvalidEventError("typed epoch ID must be positive")
    lock = " FOR UPDATE" if for_update else ""
    base = cursor.execute(
        """
        SELECT event_id, revision, structural_status,
               semantic_status, evaluation_state
        FROM groundloop_epoch
        WHERE epoch_id = %s
        """
        + lock,
        (epoch_id,),
    ).fetchone()
    if base is None:
        raise InvalidEventError("typed epoch does not exist")
    runtime = cursor.execute(
        """
        SELECT structural_event_id, revision, runtime_state,
               open_work_count, open_scope_count, blocking_failure_count
        FROM groundloop_m5_runtime_epoch
        WHERE epoch_id = %s
        """
        + lock,
        (epoch_id,),
    ).fetchone()
    if runtime is None:
        raise InvalidEventError("epoch is not a typed M5 runtime epoch")
    base_revision = int(base[1])
    runtime_revision = int(runtime[1])
    if str(base[0]) != str(runtime[0]) or base_revision != runtime_revision:
        raise ValidationError("base and typed runtime epoch identities diverged")
    return _RuntimeEpochHeader(
        epoch_id=epoch_id,
        structural_event_id=str(runtime[0]),
        revision=base_revision,
        structural_status=str(base[2]),
        semantic_status=str(base[3]),
        evaluation_state=str(base[4]),
        runtime_state=str(runtime[2]),
        open_work_count=int(runtime[3]),
        open_scope_count=int(runtime[4]),
        blocking_failure_count=int(runtime[5]),
    )


def _require_pending_lifecycle_epoch(header: _RuntimeEpochHeader) -> None:
    if (
        header.structural_status,
        header.semantic_status,
        header.evaluation_state,
    ) != ("committed", "pending", "pending") or header.runtime_state not in {
        "structural_committed",
        "semantic_pending",
    }:
        raise InvalidEventError("M5 job lifecycle requires an active pending epoch")


def _stored_job_from_row(row: tuple[Any, ...], epoch_id: int) -> _StoredM5Job:
    if int(row[1]) != epoch_id:
        raise ValidationError("stored M5 job belongs to another epoch")
    job_kind = M5JobKind(str(row[3]))
    pair_values = (row[7], row[8], row[9], row[10])
    pair: SemanticPairKey | None
    semantic_pair_digest: str | None
    if all(value is None for value in pair_values):
        pair = None
        semantic_pair_digest = None
    elif any(value is None for value in pair_values):
        raise ValidationError("stored M5 job has a partial semantic pair")
    else:
        pair = SemanticPairKey(
            subject_kind=SubjectKind(str(row[7])),
            subject_id=str(row[8]),
            chunk_version_id=str(row[9]),
        )
        semantic_pair_digest = str(row[10]).strip()
    admitted_pair_digest = None if row[11] is None else str(row[11]).strip()
    if job_kind is M5JobKind.VERIFY_REQUIREMENT_PAIR:
        if admitted_pair_digest is None:
            raise ValidationError("stored verifier job lacks its admitted pair")
        _require_sha256("admitted_pair_digest", admitted_pair_digest)
    elif admitted_pair_digest is not None:
        raise ValidationError("stored root job unexpectedly names an admitted pair")
    spec = M5LogicalJobSpec(
        logical_job_id=str(row[0]).strip(),
        structural_event_id=str(row[2]),
        job_kind=job_kind,
        candidate_policy_id=str(row[4]).strip(),
        candidate_policy_manifest_hash=str(row[5]).strip(),
        parent_job_id=None if row[6] is None else str(row[6]).strip(),
        pair=pair,
        semantic_pair_digest=semantic_pair_digest,
        scope_contract_digest=str(row[12]).strip(),
        requirement_registry_snapshot_digest=str(row[13]).strip(),
        active_chunk_snapshot_digest=str(row[14]).strip(),
        role_template_hash=str(row[15]).strip(),
        execution_spec_hash=str(row[16]).strip(),
        expandable=bool(row[17]),
        payload_hash=str(row[18]).strip(),
    )
    state = M5JobState(str(row[19]))
    result_artifact_id = None if row[20] is None else str(row[20]).strip()
    result_artifact_hash = None if row[21] is None else str(row[21]).strip()
    scope_closure_digest = None if row[22] is None else str(row[22]).strip()
    child_set_hash = None if row[23] is None else str(row[23]).strip()
    archive_reason = None if row[24] is None else M5TerminalReason(str(row[24]))
    completion_digest = None if row[25] is None else str(row[25]).strip()
    completed_revision = None if row[26] is None else int(row[26])
    completion: M5JobCompletion | None = None
    if state.terminal:
        if completion_digest is None or completed_revision is None:
            raise ValidationError("terminal M5 job lacks its durable completion")
        completion = M5JobCompletion(
            logical_job_id=spec.logical_job_id,
            payload_hash=spec.payload_hash,
            execution_spec_hash=spec.execution_spec_hash,
            terminal_state=state,
            result_artifact_id=result_artifact_id,
            result_artifact_hash=result_artifact_hash,
            scope_closure_digest=scope_closure_digest,
            child_set_hash=child_set_hash,
            archive_reason=archive_reason,
            completion_digest=completion_digest,
        )
        completion.validate_job(spec)
    elif any(
        value is not None
        for value in (
            result_artifact_id,
            result_artifact_hash,
            scope_closure_digest,
            child_set_hash,
            archive_reason,
            completion_digest,
            completed_revision,
        )
    ):
        raise ValidationError("nonterminal M5 job has terminal completion fields")
    return _StoredM5Job(
        spec=spec,
        state=state,
        admitted_pair_digest=admitted_pair_digest,
        completion=completion,
        completed_revision=completed_revision,
    )


_M5_JOB_SELECT = """
    SELECT logical_job_id, epoch_id, structural_event_id, job_kind,
           candidate_policy_id, candidate_policy_manifest_hash,
           parent_job_id, subject_kind, subject_id, chunk_version_id,
           semantic_pair_digest, admitted_pair_digest,
           scope_contract_digest, requirement_registry_snapshot_digest,
           active_chunk_snapshot_digest, role_template_hash,
           execution_spec_hash, expandable, payload_hash, job_state,
           result_artifact_id, result_artifact_hash, scope_closure_digest,
           child_set_hash, archive_reason, completion_digest,
           completed_revision
    FROM groundloop_m5_semantic_job
"""


def _read_stored_job(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    logical_job_id: str,
    for_update: bool,
) -> _StoredM5Job:
    suffix = " FOR UPDATE" if for_update else ""
    row = cursor.execute(
        _M5_JOB_SELECT + " WHERE epoch_id = %s AND logical_job_id = %s" + suffix,
        (epoch_id, logical_job_id),
    ).fetchone()
    if row is None:
        raise InvalidEventError("M5 lifecycle operation names an unknown job")
    return _stored_job_from_row(tuple(row), epoch_id)


def _stored_attempt_from_row(row: tuple[Any, ...]) -> _StoredM5Attempt:
    output_digest = None if row[6] is None else str(row[6]).strip()
    error_hash = None if row[7] is None else str(row[7]).strip()
    state = str(row[5])
    finished = row[8] is not None
    if output_digest is not None:
        _require_sha256("attempt_output_digest", output_digest)
    if error_hash is not None:
        _require_sha256("attempt error_hash", error_hash)
    if (
        (
            state == "dispatched"
            and (output_digest is not None or error_hash is not None or finished)
        )
        or (
            state == "result_reserved"
            and (output_digest is None or error_hash is not None or finished)
        )
        or (
            state == "completed"
            and (output_digest is None or error_hash is not None or not finished)
        )
        or (
            state == "failed"
            and (output_digest is not None or error_hash is None or not finished)
        )
        or (
            state == "expired"
            and (output_digest is not None or error_hash is not None or not finished)
        )
        or state
        not in {"dispatched", "result_reserved", "completed", "failed", "expired"}
    ):
        raise ValidationError("stored M5 attempt has an invalid state shape")
    return _StoredM5Attempt(
        attempt=M5JobAttempt(
            attempt_id=str(row[0]).strip(),
            logical_job_id=str(row[1]).strip(),
            attempt_ordinal=int(row[2]),
            execution_spec_hash=str(row[3]).strip(),
            lease_token_hash=str(row[4]).strip(),
            lease_expires_at=row[9],
            attempt_work_digest=str(row[10]).strip(),
        ),
        state=state,
        attempt_output_digest=output_digest,
        error_hash=error_hash,
        finished=finished,
    )


def _read_stored_attempts(
    cursor: Cursor[Any], *, logical_job_id: str, for_update: bool
) -> tuple[_StoredM5Attempt, ...]:
    suffix = " FOR UPDATE" if for_update else ""
    rows = cursor.execute(
        """
        SELECT attempt_id, logical_job_id, attempt_ordinal,
               execution_spec_hash, lease_token_hash, attempt_state,
               attempt_output_digest, error_hash, finished_at,
               lease_expires_at, attempt_work_digest
        FROM groundloop_m5_job_attempt
        WHERE logical_job_id = %s
        ORDER BY attempt_ordinal
        """
        + suffix,
        (logical_job_id,),
    ).fetchall()
    attempts = tuple(_stored_attempt_from_row(tuple(row)) for row in rows)
    previous: M5JobAttempt | None = None
    for stored in attempts:
        stored.attempt.validate_previous(previous)
        previous = stored.attempt
    return attempts


def _new_lease_token_hash() -> str:
    return hashlib.sha256(secrets.token_bytes(32)).hexdigest()


def _require_executable_lease(
    lease: M5JobLease, *, expected_revision: int
) -> M5JobAttempt:
    if (
        not isinstance(lease, M5JobLease)
        or not lease.should_execute
        or lease.exact_replay
        or lease.attempt is None
    ):
        raise ValidationError("failure settlement requires an executable M5 lease")
    if lease.resulting_revision > expected_revision:
        raise EventConflictError("lease revision is newer than the expected revision")
    if lease.logical_job_id != lease.attempt.logical_job_id:
        raise EventConflictError("lease attempt belongs to another M5 job")
    return lease.attempt


def _same_requirement_attempt_identity(
    stored: M5JobAttempt, supplied: M5JobAttempt
) -> bool:
    """Compare the immutable lease tuple, excluding its settle-once work digest."""

    return (
        stored.attempt_id,
        stored.logical_job_id,
        stored.attempt_ordinal,
        stored.execution_spec_hash,
        stored.lease_token_hash,
        stored.lease_expires_at,
    ) == (
        supplied.attempt_id,
        supplied.logical_job_id,
        supplied.attempt_ordinal,
        supplied.execution_spec_hash,
        supplied.lease_token_hash,
        supplied.lease_expires_at,
    )


_M5_FAILURE_TERMINAL_REASONS = frozenset(
    {
        M5TerminalReason.RETRY_EXHAUSTED,
        M5TerminalReason.RETRIEVAL_ERROR,
        M5TerminalReason.VERIFIER_ERROR,
        M5TerminalReason.INVALID_ARTIFACT,
    }
)


def _lock_pending_revision_bindings(
    cursor: Cursor[Any], *, epoch_id: int, expected_revision: int
) -> None:
    owner_rows = cursor.execute(
        """
        SELECT owner_claim_id, updated_revision
        FROM groundloop_m5_owner_pending_counter
        WHERE epoch_id = %s
        ORDER BY owner_claim_id COLLATE "C"
        FOR UPDATE
        """,
        (epoch_id,),
    ).fetchall()
    answer_rows = cursor.execute(
        """
        SELECT answer_version_id, updated_revision
        FROM groundloop_m5_answer_pending_counter
        WHERE epoch_id = %s
        ORDER BY answer_version_id COLLATE "C"
        FOR UPDATE
        """,
        (epoch_id,),
    ).fetchall()
    if any(int(row[1]) != expected_revision for row in (*owner_rows, *answer_rows)):
        raise ValidationError("M5 PENDING revision binding diverged from its epoch")


def _advance_job_lifecycle_revision(
    cursor: Cursor[Any],
    *,
    header: _RuntimeEpochHeader,
    expected_revision: int,
    open_work_delta: int = 0,
    open_scope_delta: int = 0,
    blocking_failure_delta: int = 0,
) -> int:
    resulting_revision = expected_revision + 1
    _lock_pending_revision_bindings(
        cursor,
        epoch_id=header.epoch_id,
        expected_revision=expected_revision,
    )
    base_updated = cursor.execute(
        """
        UPDATE groundloop_epoch
        SET revision = %s
        WHERE epoch_id = %s AND revision = %s
          AND structural_status = 'committed'
          AND semantic_status = 'pending'
          AND evaluation_state = 'pending'
        """,
        (resulting_revision, header.epoch_id, expected_revision),
    ).rowcount
    if base_updated != 1:
        raise EventConflictError("stale base epoch revision")
    runtime_updated = cursor.execute(
        """
        UPDATE groundloop_m5_runtime_epoch
        SET runtime_state = 'semantic_pending', revision = %s,
            open_work_count = %s, open_scope_count = %s,
            blocking_failure_count = %s
        WHERE epoch_id = %s AND revision = %s
          AND runtime_state IN ('structural_committed', 'semantic_pending')
          AND open_work_count = %s AND open_scope_count = %s
          AND blocking_failure_count = %s
        """,
        (
            resulting_revision,
            header.open_work_count + open_work_delta,
            header.open_scope_count + open_scope_delta,
            header.blocking_failure_count + blocking_failure_delta,
            header.epoch_id,
            expected_revision,
            header.open_work_count,
            header.open_scope_count,
            header.blocking_failure_count,
        ),
    ).rowcount
    if runtime_updated != 1:
        raise EventConflictError("stale typed runtime revision")
    cursor.execute(
        """
        UPDATE groundloop_m5_owner_pending_counter
        SET updated_revision = %s
        WHERE epoch_id = %s
        """,
        (resulting_revision, header.epoch_id),
    )
    cursor.execute(
        """
        UPDATE groundloop_m5_answer_pending_counter
        SET updated_revision = %s
        WHERE epoch_id = %s
        """,
        (resulting_revision, header.epoch_id),
    )
    return resulting_revision


def _move_job_pending_to_blocking_failure(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    logical_job_id: str,
    job_kind: M5JobKind,
    expected_revision: int,
) -> None:
    """Move the exact owner/answer multiplicity for one failed job."""

    _lock_pending_revision_bindings(
        cursor, epoch_id=epoch_id, expected_revision=expected_revision
    )
    owner_rows = cursor.execute(
        """
        SELECT DISTINCT member.owner_claim_id COLLATE "C" AS owner_claim_id
        FROM groundloop_m5_semantic_job AS job
        JOIN groundloop_m5_discovery_scope AS scope
          ON scope.scope_contract_digest = job.scope_contract_digest
        JOIN groundloop_m5_requirement_registry_snapshot_member AS member
          ON member.requirement_registry_snapshot_digest =
             job.requirement_registry_snapshot_digest
         AND (
             job.job_kind = 'reverse_requirement_discovery'
             OR (job.job_kind = 'forward_requirement_retrieval'
                 AND member.requirement_version_id = scope.requirement_version_id)
             OR (job.job_kind = 'verify_requirement_pair'
                 AND member.requirement_version_id = job.subject_id)
         )
        WHERE job.epoch_id = %s AND job.logical_job_id = %s
        ORDER BY owner_claim_id
        """,
        (epoch_id, logical_job_id),
    ).fetchall()
    owner_ids = tuple(str(row[0]) for row in owner_rows)
    if not owner_ids:
        raise ValidationError("terminal M5 job has no frozen owner multiplicity")
    pending_column = {
        M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL: "forward_scope_count",
        M5JobKind.REVERSE_REQUIREMENT_DISCOVERY: "broad_reverse_scope_count",
        M5JobKind.VERIFY_REQUIREMENT_PAIR: "verifier_job_count",
    }[job_kind]
    owner_updated = cursor.execute(
        sql.SQL(
            "UPDATE groundloop_m5_owner_pending_counter "
            "SET {counter} = {counter} - 1, "
            "blocking_failure_count = blocking_failure_count + 1 "
            "WHERE epoch_id = %s AND owner_claim_id = ANY(%s) "
            "AND updated_revision = %s AND {counter} > 0"
        ).format(counter=sql.Identifier(pending_column)),
        (epoch_id, list(owner_ids), expected_revision),
    ).rowcount
    if owner_updated != len(owner_ids):
        raise ValidationError("owner PENDING failure transition lost multiplicity")
    answer_rows = cursor.execute(
        """
        SELECT answer_version_id, count(*)::bigint
        FROM groundloop_claim
        WHERE claim_id = ANY(%s) AND required
        GROUP BY answer_version_id
        ORDER BY answer_version_id COLLATE "C"
        """,
        (list(owner_ids),),
    ).fetchall()
    for answer_id, multiplicity_value in answer_rows:
        multiplicity = int(multiplicity_value)
        answer_updated = cursor.execute(
            sql.SQL(
                "UPDATE groundloop_m5_answer_pending_counter "
                "SET {counter} = {counter} - %s, "
                "blocking_failure_count = blocking_failure_count + %s "
                "WHERE epoch_id = %s AND answer_version_id = %s "
                "AND updated_revision = %s AND {counter} >= %s"
            ).format(counter=sql.Identifier(pending_column)),
            (
                multiplicity,
                multiplicity,
                epoch_id,
                str(answer_id),
                expected_revision,
                multiplicity,
            ),
        ).rowcount
        if answer_updated != 1:
            raise ValidationError("answer PENDING failure transition lost multiplicity")


def _require_fresh_version_identifiers(
    cursor: Cursor[Any], group: EvidenceGroupVersion
) -> None:
    group_collision = cursor.execute(
        """
        SELECT 1
        FROM groundloop_m5_group_version
        WHERE group_version_id = %s
        """,
        (group.group_version_id,),
    ).fetchone()
    if group_collision is not None:
        raise InvalidEventError("group version identifier is already durable")

    requirement_ids = tuple(
        requirement.requirement_version_id for requirement in group.requirements
    )
    requirement_collision = cursor.execute(
        """
        SELECT requirement_version_id
        FROM groundloop_m5_requirement_version
        WHERE requirement_version_id = ANY(%s)
        ORDER BY requirement_version_id COLLATE "C"
        LIMIT 1
        """,
        (list(requirement_ids),),
    ).fetchone()
    if requirement_collision is not None:
        raise InvalidEventError("requirement version identifier is already durable")


def _lock_active_published_group(
    cursor: Cursor[Any], group_version_id: str
) -> _PublishedGroupOwner:
    row = cursor.execute(
        """
        SELECT group_version.group_family_id, family.claim_id
        FROM groundloop_m5_group_version AS group_version
        JOIN groundloop_m5_group_family AS family
          ON family.group_family_id = group_version.group_family_id
        JOIN groundloop_m5_group_validity AS validity
          ON validity.group_version_id = group_version.group_version_id
        WHERE group_version.group_version_id = %s
          AND group_version.lifecycle_state = 'PUBLISHED'
          AND family.lifecycle_state = 'PUBLISHED'
          AND validity.valid_to_epoch IS NULL
        FOR UPDATE OF group_version, family, validity
        """,
        (group_version_id,),
    ).fetchone()
    if row is None:
        raise InvalidEventError(
            "group lifecycle event requires an active published group"
        )
    return _PublishedGroupOwner(str(row[0]), str(row[1]))


def _validate_structure_declaration(
    cursor: Cursor[Any],
    event: RegisterGroupEvent
    | ReplaceGroupEvent
    | RetireGroupEvent
    | ObserveRequirementEvent,
) -> None:
    """Validate lifecycle ownership before the epoch/event rows are inserted."""

    if isinstance(event, RegisterGroupEvent):
        family_collision = cursor.execute(
            """
            SELECT 1 FROM groundloop_m5_group_family
            WHERE group_family_id = %s
            """,
            (event.group.group_family_id,),
        ).fetchone()
        if family_collision is not None:
            raise InvalidEventError("group registration requires a new family")
        if event.group.supersedes_group_version_id is not None:
            raise InvalidEventError("initial group version cannot name a predecessor")
        _require_fresh_version_identifiers(cursor, event.group)
        return

    if isinstance(event, ReplaceGroupEvent):
        owner = _lock_active_published_group(cursor, event.old_group_version_id)
        successor = event.successor
        if successor.supersedes_group_version_id != event.old_group_version_id:
            raise InvalidEventError("group replacement must name its exact predecessor")
        if successor.group_family_id != owner.group_family_id:
            raise InvalidEventError("group replacement cannot change family")
        if successor.owner_claim_id != owner.claim_id:
            raise InvalidEventError("group replacement cannot change owner claim")
        retired = cursor.execute(
            """
            SELECT 1 FROM groundloop_m5_group_family_retirement
            WHERE group_family_id = %s
            """,
            (owner.group_family_id,),
        ).fetchone()
        if retired is not None:
            raise InvalidEventError("a retired group family cannot be replaced")
        _require_fresh_version_identifiers(cursor, successor)
        return

    if isinstance(event, RetireGroupEvent):
        _lock_active_published_group(cursor, event.group_version_id)
        return

    observation = event.observation
    if observation.subject_kind is not SubjectKind.REQUIREMENT:
        raise InvalidEventError(
            "observe-requirement event requires a requirement subject"
        )
    subject = cursor.execute(
        """
        SELECT 1
        FROM groundloop_m5_requirement_version AS requirement
        JOIN groundloop_m5_group_version AS group_version
          ON group_version.group_version_id = requirement.group_version_id
        JOIN groundloop_m5_group_validity AS validity
          ON validity.group_version_id = group_version.group_version_id
        JOIN groundloop_chunk_version AS chunk
          ON chunk.chunk_version_id = %s
        WHERE requirement.requirement_version_id = %s
          AND requirement.lifecycle_state = 'PUBLISHED'
          AND group_version.lifecycle_state = 'PUBLISHED'
          AND validity.valid_to_epoch IS NULL
          AND chunk.valid_to_epoch IS NULL
        FOR UPDATE OF requirement, group_version, validity, chunk
        """,
        (observation.chunk_version_id, observation.subject_id),
    ).fetchone()
    if subject is None:
        raise InvalidEventError(
            "observe-requirement event requires active subject, group, and chunk"
        )


def _insert_staged_group_version(
    cursor: Cursor[Any],
    *,
    group: EvidenceGroupVersion,
    epoch_id: int,
) -> None:
    cursor.execute(
        """
        INSERT INTO groundloop_m5_group_version (
            group_version_id, group_family_id, creator_epoch_id,
            lifecycle_state, group_type, construction_kind,
            construction_source_id, constructor_model_id,
            constructor_model_version, constructor_prompt_version,
            supersedes_group_version_id, semantic_structure_hash,
            record_payload_hash
        ) VALUES (
            %s, %s, %s, 'STAGED', %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            group.group_version_id,
            group.group_family_id,
            epoch_id,
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
        cursor.execute(
            """
            INSERT INTO groundloop_m5_requirement_version (
                requirement_version_id, group_version_id, creator_epoch_id,
                lifecycle_state, ordinal, requirement_text,
                requirement_text_hash, constructor_model_id,
                constructor_model_version, constructor_prompt_version,
                supersedes_requirement_version_id
            ) VALUES (
                %s, %s, %s, 'STAGED', %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                requirement.requirement_version_id,
                requirement.group_version_id,
                epoch_id,
                requirement.ordinal,
                requirement.requirement_text,
                requirement.requirement_text_hash,
                requirement.constructor_model_id,
                requirement.constructor_model_version,
                requirement.constructor_prompt_version,
                requirement.supersedes_requirement_version_id,
            ),
        )


def _stage_structure(
    cursor: Cursor[Any],
    *,
    event: RegisterGroupEvent
    | ReplaceGroupEvent
    | RetireGroupEvent
    | ObserveRequirementEvent,
    epoch_id: int,
    failure_injector: RuntimeFailureInjector | None,
) -> None:
    if isinstance(event, RegisterGroupEvent):
        cursor.execute(
            """
            INSERT INTO groundloop_m5_group_family (
                group_family_id, claim_id, creator_epoch_id, lifecycle_state
            ) VALUES (%s, %s, %s, 'STAGED')
            """,
            (event.group.group_family_id, event.group.owner_claim_id, epoch_id),
        )
        _inject(failure_injector, "typed_open_family_staged")
        _insert_staged_group_version(cursor, group=event.group, epoch_id=epoch_id)
        _inject(failure_injector, "typed_open_group_staged")
        return

    if isinstance(event, ReplaceGroupEvent):
        _insert_staged_group_version(cursor, group=event.successor, epoch_id=epoch_id)
        _inject(failure_injector, "typed_open_group_staged")
        cursor.execute(
            """
            INSERT INTO groundloop_m5_group_deactivation (
                epoch_id, group_version_id, action,
                successor_group_version_id, event_id
            ) VALUES (%s, %s, 'REPLACE', %s, %s)
            """,
            (
                epoch_id,
                event.old_group_version_id,
                event.successor.group_version_id,
                event.event_id,
            ),
        )
        _inject(failure_injector, "typed_open_deactivation_staged")
        return

    if isinstance(event, RetireGroupEvent):
        cursor.execute(
            """
            INSERT INTO groundloop_m5_group_deactivation (
                epoch_id, group_version_id, action,
                successor_group_version_id, event_id
            ) VALUES (%s, %s, 'RETIRE', NULL, %s)
            """,
            (epoch_id, event.group_version_id, event.event_id),
        )
        _inject(failure_injector, "typed_open_deactivation_staged")


def _mark_event_staged_structure_failed(cursor: Cursor[Any], epoch_id: int) -> None:
    """Retain staged audit rows while making them terminally non-publishable."""

    cursor.execute(
        """
        UPDATE groundloop_m5_requirement_version
        SET lifecycle_state = 'FAILED'
        WHERE creator_epoch_id = %s AND lifecycle_state = 'STAGED'
        """,
        (epoch_id,),
    )
    cursor.execute(
        """
        UPDATE groundloop_m5_group_version
        SET lifecycle_state = 'FAILED'
        WHERE creator_epoch_id = %s AND lifecycle_state = 'STAGED'
        """,
        (epoch_id,),
    )
    cursor.execute(
        """
        UPDATE groundloop_m5_group_family
        SET lifecycle_state = 'FAILED'
        WHERE creator_epoch_id = %s AND lifecycle_state = 'STAGED'
        """,
        (epoch_id,),
    )


def _m5_update_kind(
    event: InsertDocumentEvent
    | DeleteDocumentVersionEvent
    | ReplaceDocumentVersionEvent
    | PolicyChangeEvent
    | ObserveEvent
    | RegisterGroupEvent
    | ReplaceGroupEvent
    | RetireGroupEvent
    | ObserveRequirementEvent,
) -> str:
    if isinstance(event, InsertDocumentEvent):
        return "document_insert"
    if isinstance(event, DeleteDocumentVersionEvent):
        return "document_delete"
    if isinstance(event, ReplaceDocumentVersionEvent):
        return "document_replace"
    if isinstance(event, PolicyChangeEvent):
        return "policy_change"
    if isinstance(event, ObserveEvent):
        raise InvalidEventError(
            "legacy claim-observation events are not M5 typed structural events"
        )
    if isinstance(event, RegisterGroupEvent):
        return "register_group"
    if isinstance(event, ReplaceGroupEvent):
        return "replace_group"
    if isinstance(event, RetireGroupEvent):
        return "retire_group"
    return "observe_requirement"


def _load_candidate_manifest(
    cursor: Cursor[Any], candidate_policy_id: str
) -> M5CandidatePolicyManifest:
    row = cursor.execute(
        """
        SELECT candidate_policy_id, candidate_policy_manifest_hash,
               embedding_model_artifact_id, requirement_role_template_hash,
               chunk_role_template_hash, vector_method_version,
               vector_index_kind, vector_index_build_config_hash,
               vector_search_config_hash, lexical_method_version,
               lexical_config_hash, lexical_postgres_version,
               lexical_regconfig_identity, fusion_version,
               reverse_budget_per_inserted_chunk,
               forward_budget_per_requirement,
               verifier_execution_spec_hash, decision_policy_version,
               lineage_safety_override
        FROM groundloop_m5_candidate_policy
        WHERE candidate_policy_id = %s
        """,
        (candidate_policy_id,),
    ).fetchone()
    if row is None:
        raise InvalidEventError("typed plan names an unregistered candidate policy")
    manifest = M5CandidatePolicyManifest(
        candidate_policy_id=str(row[0]).strip(),
        embedding_model_artifact_id=str(row[2]),
        requirement_role_template_hash=str(row[3]).strip(),
        chunk_role_template_hash=str(row[4]).strip(),
        vector_method_version=str(row[5]),
        vector_index_kind=VectorIndexKind(str(row[6])),
        vector_index_build_config_hash=str(row[7]).strip(),
        vector_search_config_hash=str(row[8]).strip(),
        lexical_method_version=str(row[9]),
        lexical_config_hash=str(row[10]).strip(),
        lexical_postgres_version=str(row[11]),
        lexical_regconfig_identity=str(row[12]),
        fusion_version=str(row[13]),
        reverse_budget_per_inserted_chunk=int(row[14]),
        forward_budget_per_requirement=int(row[15]),
        verifier_execution_spec_hash=str(row[16]).strip(),
        decision_policy_version=str(row[17]),
        lineage_safety_override=bool(row[18]),
    )
    if manifest.manifest_hash != str(row[1]).strip():
        raise ValidationError("stored candidate-policy manifest identity is corrupt")
    return manifest


def _build_root_declarations(
    plan: M5TypedEventPlan, manifest: M5CandidatePolicyManifest
) -> tuple[_RootDeclaration, ...]:
    if isinstance(plan.event, RegisterGroupEvent):
        requirements = plan.event.group.requirements
    elif isinstance(plan.event, ReplaceGroupEvent):
        requirements = plan.event.successor.requirements
    else:
        requirements = ()

    declarations: list[_RootDeclaration] = []
    for requirement in requirements:
        scope = M5DiscoveryScopeContract.build(
            direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
            requirement_version_id=requirement.requirement_version_id,
            inserted_chunk_version_id=None,
            candidate_policy_id=manifest.candidate_policy_id,
            requirement_registry_snapshot_digest=(
                plan.requirement_registry_snapshot.requirement_registry_snapshot_digest
            ),
            active_chunk_snapshot_digest=(
                plan.active_chunk_snapshot.active_chunk_snapshot_digest
            ),
        )
        scope.validate_snapshots(
            plan.requirement_registry_snapshot, plan.active_chunk_snapshot
        )
        job = M5LogicalJobSpec.build(
            structural_event_id=plan.structural_event_id,
            job_kind=M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL,
            manifest=manifest,
            scope=scope,
        )
        declarations.append(_RootDeclaration(scope, job))
    ordered = tuple(
        sorted(declarations, key=lambda declaration: declaration.job.logical_job_id)
    )
    if len({declaration.job.logical_job_id for declaration in ordered}) != len(ordered):
        raise ValidationError("typed root declaration repeats a logical job")
    return ordered


def _validate_recovery_root_fallback_map(
    roots: tuple[_RootDeclaration, ...], supplied: dict[str, bool]
) -> dict[str, bool]:
    expected_ids = {
        declaration.job.logical_job_id
        for declaration in roots
        if declaration.job.job_kind is M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL
    }
    if set(supplied) != expected_ids:
        raise ValidationError(
            "recovery root fallback map must exactly cover the frozen root set"
        )
    if any(not isinstance(value, bool) for value in supplied.values()):
        raise ValidationError("recovery root fallback values must be boolean")
    return {root_id: supplied[root_id] for root_id in sorted(supplied)}


def _derive_structural_open_work(
    cursor: Cursor[Any], *, epoch_id: int
) -> M5RuntimeWork:
    """Hash the canonical database serialization of rows written by open."""

    serialized_rows: list[bytes] = []
    for table_name, epoch_column in _STRUCTURAL_OPEN_EPOCH_ROWS:
        rows = cursor.execute(
            sql.SQL(
                "SELECT to_jsonb(open_row)::text FROM {} AS open_row "
                "WHERE open_row.{} = %s "
                'ORDER BY to_jsonb(open_row)::text COLLATE "C"'
            ).format(sql.Identifier(table_name), sql.Identifier(epoch_column)),
            (epoch_id,),
        ).fetchall()
        serialized_rows.extend(str(row[0]).encode("utf-8") for row in rows)

    snapshot_members = cursor.execute(
        """
        SELECT to_jsonb(member_row)::text
        FROM groundloop_m5_requirement_registry_snapshot_member AS member_row
        JOIN groundloop_m5_requirement_registry_snapshot AS snapshot
          USING (requirement_registry_snapshot_digest)
        WHERE snapshot.created_epoch_id = %s
        UNION ALL
        SELECT to_jsonb(member_row)::text
        FROM groundloop_m5_active_chunk_snapshot_member AS member_row
        JOIN groundloop_m5_active_chunk_snapshot AS snapshot
          USING (active_chunk_snapshot_digest)
        WHERE snapshot.created_epoch_id = %s
        """,
        (epoch_id, epoch_id),
    ).fetchall()
    serialized_rows.extend(str(row[0]).encode("utf-8") for row in snapshot_members)
    if not serialized_rows:
        raise ValidationError("structural open produced no persistence rows")

    byte_count = 0
    structural_hasher = hashlib.sha256()
    for encoded_row in sorted(serialized_rows):
        frame = len(encoded_row).to_bytes(8, byteorder="big", signed=False)
        structural_hasher.update(frame)
        structural_hasher.update(encoded_row)
        byte_count += len(frame) + len(encoded_row)
    if len(structural_hasher.digest()) != hashlib.sha256().digest_size:
        raise AssertionError("SHA-256 structural-open instrumentation drift")
    return M5RuntimeWork(bytes_hashed=byte_count, bytes_serialized=byte_count)


def _requirement_snapshot_rows(
    snapshot: RequirementRegistrySnapshot,
) -> tuple[tuple[object, ...], ...]:
    return tuple(
        (
            ordinal,
            entry.requirement_version_id,
            entry.group_version_id,
            entry.group_family_id,
            entry.owner_claim_id,
            entry.normalized_requirement_text,
            entry.requirement_text_hash,
        )
        for ordinal, entry in enumerate(snapshot.entries)
    )


def _chunk_snapshot_rows(
    snapshot: ActiveChunkSnapshot,
) -> tuple[tuple[object, ...], ...]:
    return tuple(
        (ordinal, entry.chunk_version_id, entry.text_hash)
        for ordinal, entry in enumerate(snapshot.entries)
    )


def _validate_effective_snapshots(
    cursor: Cursor[Any], *, plan: M5TypedEventPlan, epoch_id: int
) -> None:
    requirement_rows = tuple(
        tuple(row)
        for row in cursor.execute(
            """
            WITH effective_requirement AS (
                SELECT requirement.requirement_version_id,
                       requirement.group_version_id,
                       group_version.group_family_id,
                       family.claim_id AS owner_claim_id,
                       requirement.requirement_text,
                       requirement.requirement_text_hash
                FROM groundloop_m5_requirement_version AS requirement
                JOIN groundloop_m5_group_version AS group_version
                  ON group_version.group_version_id = requirement.group_version_id
                JOIN groundloop_m5_group_family AS family
                  ON family.group_family_id = group_version.group_family_id
                JOIN groundloop_m5_group_validity AS validity
                  ON validity.group_version_id = group_version.group_version_id
                WHERE requirement.lifecycle_state = 'PUBLISHED'
                  AND group_version.lifecycle_state = 'PUBLISHED'
                  AND family.lifecycle_state = 'PUBLISHED'
                  AND validity.valid_to_epoch IS NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM groundloop_m5_group_deactivation AS deactivation
                      WHERE deactivation.epoch_id = %s
                        AND deactivation.group_version_id =
                            group_version.group_version_id
                  )
                UNION ALL
                SELECT requirement.requirement_version_id,
                       requirement.group_version_id,
                       group_version.group_family_id,
                       family.claim_id AS owner_claim_id,
                       requirement.requirement_text,
                       requirement.requirement_text_hash
                FROM groundloop_m5_requirement_version AS requirement
                JOIN groundloop_m5_group_version AS group_version
                  ON group_version.group_version_id = requirement.group_version_id
                JOIN groundloop_m5_group_family AS family
                  ON family.group_family_id = group_version.group_family_id
                WHERE requirement.creator_epoch_id = %s
                  AND requirement.lifecycle_state = 'STAGED'
                  AND group_version.creator_epoch_id = %s
                  AND group_version.lifecycle_state = 'STAGED'
            )
            SELECT requirement_version_id, group_version_id, group_family_id,
                   owner_claim_id, requirement_text, requirement_text_hash
            FROM effective_requirement
            ORDER BY requirement_version_id COLLATE "C"
            """,
            (epoch_id, epoch_id, epoch_id),
        ).fetchall()
    )
    planned_requirement_rows = tuple(
        (
            entry.requirement_version_id,
            entry.group_version_id,
            entry.group_family_id,
            entry.owner_claim_id,
            entry.normalized_requirement_text,
            entry.requirement_text_hash,
        )
        for entry in plan.requirement_registry_snapshot.entries
    )
    if requirement_rows != planned_requirement_rows:
        raise InvalidEventError(
            "typed requirement snapshot is not the effective structural snapshot"
        )

    chunk_rows = tuple(
        (str(row[0]), str(row[1]))
        for row in cursor.execute(
            """
            SELECT chunk_version_id,
                   encode(
                       digest(
                           convert_to(
                               groundloop_normalize_text_v1(text), 'UTF8'
                           ),
                           'sha256'
                       ),
                       'hex'
                   ) AS normalized_text_hash
            FROM groundloop_chunk_version
            WHERE valid_to_epoch IS NULL
            ORDER BY chunk_version_id COLLATE "C"
            """
        ).fetchall()
    )
    planned_chunk_rows = tuple(
        (entry.chunk_version_id, entry.text_hash)
        for entry in plan.active_chunk_snapshot.entries
    )
    if chunk_rows != planned_chunk_rows:
        raise InvalidEventError(
            "typed chunk snapshot is not the effective active-chunk snapshot"
        )


def _persist_requirement_snapshot(
    cursor: Cursor[Any], *, snapshot: RequirementRegistrySnapshot, epoch_id: int
) -> None:
    inserted = cursor.execute(
        """
        INSERT INTO groundloop_m5_requirement_registry_snapshot (
            requirement_registry_snapshot_digest, requirement_count,
            created_epoch_id
        ) VALUES (%s, %s, %s)
        ON CONFLICT (requirement_registry_snapshot_digest) DO NOTHING
        """,
        (
            snapshot.requirement_registry_snapshot_digest,
            snapshot.requirement_count,
            epoch_id,
        ),
    ).rowcount
    if inserted == 1:
        for row in _requirement_snapshot_rows(snapshot):
            cursor.execute(
                """
                INSERT INTO groundloop_m5_requirement_registry_snapshot_member (
                    requirement_registry_snapshot_digest, member_ordinal,
                    requirement_version_id, group_version_id, group_family_id,
                    owner_claim_id, normalized_requirement_text,
                    requirement_text_hash
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (snapshot.requirement_registry_snapshot_digest, *row),
            )
        return

    header = cursor.execute(
        """
        SELECT requirement_count
        FROM groundloop_m5_requirement_registry_snapshot
        WHERE requirement_registry_snapshot_digest = %s
        """,
        (snapshot.requirement_registry_snapshot_digest,),
    ).fetchone()
    members = tuple(
        tuple(row)
        for row in cursor.execute(
            """
            SELECT member_ordinal, requirement_version_id, group_version_id,
                   group_family_id, owner_claim_id,
                   normalized_requirement_text, requirement_text_hash
            FROM groundloop_m5_requirement_registry_snapshot_member
            WHERE requirement_registry_snapshot_digest = %s
            ORDER BY member_ordinal
            """,
            (snapshot.requirement_registry_snapshot_digest,),
        ).fetchall()
    )
    if header != (snapshot.requirement_count,) or members != (
        _requirement_snapshot_rows(snapshot)
    ):
        raise EventConflictError("requirement snapshot digest has conflicting rows")


def _persist_active_chunk_snapshot(
    cursor: Cursor[Any], *, snapshot: ActiveChunkSnapshot, epoch_id: int
) -> None:
    inserted = cursor.execute(
        """
        INSERT INTO groundloop_m5_active_chunk_snapshot (
            active_chunk_snapshot_digest, chunk_count, created_epoch_id,
            normalizer_id, normalizer_provenance_hash
        ) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (active_chunk_snapshot_digest) DO NOTHING
        """,
        (
            snapshot.active_chunk_snapshot_digest,
            snapshot.chunk_count,
            epoch_id,
            _NORMALIZER_ID,
            _NORMALIZER_PROVENANCE_HASH,
        ),
    ).rowcount
    if inserted == 1:
        for row in _chunk_snapshot_rows(snapshot):
            cursor.execute(
                """
                INSERT INTO groundloop_m5_active_chunk_snapshot_member (
                    active_chunk_snapshot_digest, member_ordinal,
                    chunk_version_id, text_hash
                ) VALUES (%s, %s, %s, %s)
                """,
                (snapshot.active_chunk_snapshot_digest, *row),
            )
        return

    header = cursor.execute(
        """
        SELECT chunk_count, normalizer_id, normalizer_provenance_hash
        FROM groundloop_m5_active_chunk_snapshot
        WHERE active_chunk_snapshot_digest = %s
        """,
        (snapshot.active_chunk_snapshot_digest,),
    ).fetchone()
    members = tuple(
        tuple(row)
        for row in cursor.execute(
            """
            SELECT member_ordinal, chunk_version_id, text_hash
            FROM groundloop_m5_active_chunk_snapshot_member
            WHERE active_chunk_snapshot_digest = %s
            ORDER BY member_ordinal
            """,
            (snapshot.active_chunk_snapshot_digest,),
        ).fetchall()
    )
    if header != (
        snapshot.chunk_count,
        _NORMALIZER_ID,
        _NORMALIZER_PROVENANCE_HASH,
    ) or members != _chunk_snapshot_rows(snapshot):
        raise EventConflictError("active-chunk snapshot digest has conflicting rows")


def _persist_initial_pending_counters(
    cursor: Cursor[Any],
    *,
    snapshot: RequirementRegistrySnapshot,
    epoch_id: int,
    roots: tuple[_RootDeclaration, ...],
) -> None:
    owner_claim_ids = tuple(
        sorted({entry.owner_claim_id for entry in snapshot.entries})
    )
    owner_by_requirement = {
        entry.requirement_version_id: entry.owner_claim_id for entry in snapshot.entries
    }
    forward_counts: Counter[str] = Counter()
    broad_counts: Counter[str] = Counter()
    for declaration in roots:
        if declaration.scope.direction is M5DiscoveryDirection.FORWARD_REQUIREMENT:
            assert declaration.scope.requirement_version_id is not None
            forward_counts[
                owner_by_requirement[declaration.scope.requirement_version_id]
            ] += 1
        else:
            for owner_claim_id in owner_claim_ids:
                broad_counts[owner_claim_id] += 1
    for owner_claim_id in owner_claim_ids:
        cursor.execute(
            """
            INSERT INTO groundloop_m5_owner_pending_counter (
                epoch_id, owner_claim_id, broad_reverse_scope_count,
                forward_scope_count, verifier_job_count,
                blocking_failure_count, updated_revision
            ) VALUES (%s, %s, %s, %s, 0, 0, 1)
            """,
            (
                epoch_id,
                owner_claim_id,
                broad_counts[owner_claim_id],
                forward_counts[owner_claim_id],
            ),
        )

    if not owner_claim_ids:
        return
    answer_rows = cursor.execute(
        """
        SELECT claim_id, answer_version_id
        FROM groundloop_claim
        WHERE claim_id = ANY(%s) AND required
        ORDER BY answer_version_id COLLATE "C", claim_id COLLATE "C"
        """,
        (list(owner_claim_ids),),
    ).fetchall()
    answer_broad_counts: Counter[str] = Counter()
    answer_forward_counts: Counter[str] = Counter()
    for claim_id, answer_version_id in answer_rows:
        answer = str(answer_version_id)
        owner = str(claim_id)
        answer_broad_counts[answer] += broad_counts[owner]
        answer_forward_counts[answer] += forward_counts[owner]
    for answer_version_id in sorted(answer_broad_counts):
        cursor.execute(
            """
            INSERT INTO groundloop_m5_answer_pending_counter (
                epoch_id, answer_version_id, broad_reverse_scope_count,
                forward_scope_count, verifier_job_count,
                blocking_failure_count, updated_revision
            ) VALUES (%s, %s, %s, %s, 0, 0, 1)
            """,
            (
                epoch_id,
                answer_version_id,
                answer_broad_counts[answer_version_id],
                answer_forward_counts[answer_version_id],
            ),
        )


def _persist_root_declarations(
    cursor: Cursor[Any],
    *,
    structural_event_id: str,
    epoch_id: int,
    roots: tuple[_RootDeclaration, ...],
) -> None:
    for declaration in roots:
        scope = declaration.scope
        job = declaration.job
        cursor.execute(
            """
            INSERT INTO groundloop_m5_discovery_scope (
                root_job_id, epoch_id, direction,
                requirement_version_id, inserted_chunk_version_id,
                candidate_policy_id,
                requirement_registry_snapshot_digest,
                active_chunk_snapshot_digest, scope_contract_digest,
                scope_state, staged_result_artifact_hash,
                scope_closure_digest, child_set_hash, completion_digest,
                created_revision, staged_revision, closed_revision,
                closed_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                'open', NULL, NULL, NULL, NULL, 1, NULL, NULL, NULL
            )
            """,
            (
                job.logical_job_id,
                epoch_id,
                scope.direction.value,
                scope.requirement_version_id,
                scope.inserted_chunk_version_id,
                scope.candidate_policy_id,
                scope.requirement_registry_snapshot_digest,
                scope.active_chunk_snapshot_digest,
                scope.scope_contract_digest,
            ),
        )
        cursor.execute(
            """
            INSERT INTO groundloop_m5_semantic_job (
                logical_job_id, epoch_id, structural_event_id, job_kind,
                candidate_policy_id, candidate_policy_manifest_hash,
                parent_job_id, subject_kind, subject_id, chunk_version_id,
                semantic_pair_digest, admitted_pair_digest,
                scope_contract_digest,
                requirement_registry_snapshot_digest,
                active_chunk_snapshot_digest, role_template_hash,
                execution_spec_hash, expandable, payload_hash, job_state,
                result_artifact_id, result_artifact_hash,
                scope_closure_digest, child_set_hash, archive_reason,
                completion_digest, cancelled_by_event_id,
                cancelled_by_epoch_id, cancellation_reason,
                created_revision, completed_revision, completed_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                NULL, NULL, NULL, NULL, NULL, NULL,
                %s, %s, %s, %s, %s, TRUE, %s, 'declared',
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                1, NULL, NULL
            )
            """,
            (
                job.logical_job_id,
                epoch_id,
                structural_event_id,
                job.job_kind.value,
                job.candidate_policy_id,
                job.candidate_policy_manifest_hash,
                job.scope_contract_digest,
                job.requirement_registry_snapshot_digest,
                job.active_chunk_snapshot_digest,
                job.role_template_hash,
                job.execution_spec_hash,
                job.payload_hash,
            ),
        )


def _load_root_declarations_for_failure(
    cursor: Cursor[Any], *, epoch_id: int
) -> tuple[_RootDeclaration, ...]:
    rows = cursor.execute(
        """
        SELECT scope.direction, scope.requirement_version_id,
               scope.inserted_chunk_version_id, scope.candidate_policy_id,
               scope.requirement_registry_snapshot_digest,
               scope.active_chunk_snapshot_digest,
               scope.scope_contract_digest,
               job.logical_job_id, job.structural_event_id, job.job_kind,
               job.candidate_policy_id, job.candidate_policy_manifest_hash,
               job.parent_job_id, job.semantic_pair_digest,
               job.scope_contract_digest,
               job.requirement_registry_snapshot_digest,
               job.active_chunk_snapshot_digest, job.role_template_hash,
               job.execution_spec_hash, job.expandable, job.payload_hash,
               job.job_state, scope.scope_state
        FROM groundloop_m5_semantic_job AS job
        JOIN groundloop_m5_discovery_scope AS scope
          ON scope.root_job_id = job.logical_job_id
         AND scope.epoch_id = job.epoch_id
        WHERE job.epoch_id = %s AND job.parent_job_id IS NULL
        ORDER BY job.logical_job_id COLLATE "C"
        FOR UPDATE OF job, scope
        """,
        (epoch_id,),
    ).fetchall()
    declarations: list[_RootDeclaration] = []
    for row in rows:
        if str(row[21]) != "declared" or str(row[22]) != "open":
            raise InvalidEventError(
                "M5.3-07 staged failure requires undelegated root declarations"
            )
        scope = M5DiscoveryScopeContract(
            direction=M5DiscoveryDirection(str(row[0])),
            requirement_version_id=(None if row[1] is None else str(row[1])),
            inserted_chunk_version_id=(None if row[2] is None else str(row[2])),
            candidate_policy_id=str(row[3]).strip(),
            requirement_registry_snapshot_digest=str(row[4]).strip(),
            active_chunk_snapshot_digest=str(row[5]).strip(),
            scope_contract_digest=str(row[6]).strip(),
        )
        job = M5LogicalJobSpec(
            logical_job_id=str(row[7]).strip(),
            structural_event_id=str(row[8]),
            job_kind=M5JobKind(str(row[9])),
            candidate_policy_id=str(row[10]).strip(),
            candidate_policy_manifest_hash=str(row[11]).strip(),
            parent_job_id=None,
            pair=None,
            semantic_pair_digest=(None if row[13] is None else str(row[13]).strip()),
            scope_contract_digest=str(row[14]).strip(),
            requirement_registry_snapshot_digest=str(row[15]).strip(),
            active_chunk_snapshot_digest=str(row[16]).strip(),
            role_template_hash=str(row[17]).strip(),
            execution_spec_hash=str(row[18]).strip(),
            expandable=bool(row[19]),
            payload_hash=str(row[20]).strip(),
        )
        if job.parent_job_id is not None or job.pair is not None:
            raise ValidationError("failure root declaration is not a root")
        declarations.append(_RootDeclaration(scope, job))
    return tuple(declarations)


def _cancel_root_declarations(
    cursor: Cursor[Any],
    *,
    structural_event_id: str,
    epoch_id: int,
    resulting_revision: int,
    roots: tuple[_RootDeclaration, ...],
) -> None:
    for declaration in roots:
        cancelled = M5JobCompletion.build(
            job=declaration.job,
            terminal_state=M5JobState.CANCELLED,
            archive_reason=M5TerminalReason.EPOCH_FAILED,
        )
        updated_job = cursor.execute(
            """
            UPDATE groundloop_m5_semantic_job
            SET job_state = 'cancelled', archive_reason = 'epoch_failed',
                completion_digest = %s, cancelled_by_event_id = %s,
                cancelled_by_epoch_id = %s,
                cancellation_reason = 'epoch_failed',
                completed_revision = %s, completed_at = now()
            WHERE logical_job_id = %s AND epoch_id = %s
              AND job_state = 'declared'
            """,
            (
                cancelled.completion_digest,
                structural_event_id,
                epoch_id,
                resulting_revision,
                declaration.job.logical_job_id,
                epoch_id,
            ),
        ).rowcount
        if updated_job != 1:
            raise EventConflictError("root job changed before failure cancellation")
        updated_scope = cursor.execute(
            """
            UPDATE groundloop_m5_discovery_scope
            SET scope_state = 'cancelled', completion_digest = %s,
                closed_revision = %s, closed_at = now()
            WHERE root_job_id = %s AND epoch_id = %s
              AND scope_state = 'open'
            """,
            (
                cancelled.completion_digest,
                resulting_revision,
                declaration.job.logical_job_id,
                epoch_id,
            ),
        ).rowcount
        if updated_scope != 1:
            raise EventConflictError("root scope changed before failure cancellation")


def _insert_runtime_work(
    cursor: Cursor[Any],
    *,
    structural_event_id: str,
    epoch_id: int,
    work_kind: str,
    work: M5RuntimeWork,
) -> None:
    columns = (
        "work_digest",
        "structural_event_id",
        "epoch_id",
        "work_kind",
        *_WORK_COUNTER_COLUMNS,
    )
    statement = sql.SQL(
        "INSERT INTO groundloop_m5_runtime_work ({}) VALUES ({})"
    ).format(
        sql.SQL(", ").join(sql.Identifier(column) for column in columns),
        sql.SQL(", ").join(sql.Placeholder() for _ in columns),
    )
    cursor.execute(
        statement,
        (
            work.work_digest,
            structural_event_id,
            epoch_id,
            work_kind,
            *work.counter_values(),
        ),
    )


def _load_runtime_work(
    cursor: Cursor[Any], *, structural_event_id: str, work_kind: str
) -> M5RuntimeWork | None:
    selected_columns = ("work_digest", *_WORK_COUNTER_COLUMNS)
    statement = sql.SQL(
        "SELECT {} FROM groundloop_m5_runtime_work "
        "WHERE structural_event_id = %s AND work_kind = %s"
    ).format(sql.SQL(", ").join(sql.Identifier(column) for column in selected_columns))
    row = cursor.execute(statement, (structural_event_id, work_kind)).fetchone()
    if row is None:
        return None
    values = {
        name: int(value)
        for name, value in zip(_WORK_COUNTER_COLUMNS, row[1:], strict=True)
    }
    return M5RuntimeWork(**values, work_digest=str(row[0]).strip())


def _load_runtime_work_accumulator(
    cursor: Cursor[Any], *, epoch_id: int, expected_revision: int
) -> M5RuntimeWork | None:
    relation = cursor.execute(
        "SELECT to_regclass('groundloop_m5_runtime_work_accumulator')"
    ).fetchone()
    if relation is None or relation[0] is None:
        return None
    selected_columns = (*_WORK_COUNTER_COLUMNS, "work_digest")
    row = cursor.execute(
        sql.SQL(
            "SELECT {}, updated_revision, terminalized "
            "FROM groundloop_m5_runtime_work_accumulator WHERE epoch_id = %s"
        ).format(
            sql.SQL(", ").join(sql.Identifier(column) for column in selected_columns)
        ),
        (epoch_id,),
    ).fetchone()
    if row is None:
        raise ValidationError("typed M5 epoch lacks its work accumulator")
    work_end = len(_WORK_COUNTER_COLUMNS)
    if int(row[work_end + 1]) != expected_revision or bool(row[work_end + 2]):
        raise ValidationError("M5 work accumulator is not at the active revision")
    values = {
        name: int(value)
        for name, value in zip(_WORK_COUNTER_COLUMNS, row[:work_end], strict=True)
    }
    return M5RuntimeWork(
        **values,
        work_digest=str(row[work_end]).strip(),
    )


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _load_terminal_result(
    cursor: Cursor[Any], *, structural_event_id: str, payload_hash: str
) -> M5EventRunResult | None:
    row = cursor.execute(
        """
        SELECT base.epoch_id, base.payload_hash,
               runtime.structural_event_id,
               result.outcome,
               result.original_open_receipt_binding_hash,
               result.publication_id,
               result.original_publication_receipt_binding_hash,
               result.event_work_digest,
               result.combined_status_delta_set_hash,
               result.changed_state_set_hash,
               result.failure_reason,
               result.logical_result_hash,
               result.delta_count,
               result.state_reference_count,
               result.coordinator_non_db_non_neural_ns,
               result.neural_wall_ns,
               result.postgres_roundtrip_wall_ns,
               result.external_io_wall_ns,
               result.end_to_end_wall_ns,
               result.postgres_server_execution_ns,
               result.postgres_lock_wait_ns,
               result.postgres_wal_bytes,
               result.postgres_shared_block_reads
        FROM groundloop_epoch AS base
        LEFT JOIN groundloop_m5_runtime_epoch AS runtime
          ON runtime.epoch_id = base.epoch_id
        LEFT JOIN groundloop_m5_event_result AS result
          ON result.epoch_id = base.epoch_id
        WHERE base.event_id = %s
        """,
        (structural_event_id,),
    ).fetchone()
    if row is None:
        return None
    if str(row[1]).strip() != payload_hash:
        raise EventConflictError(
            "typed structural event ID was reused with another payload"
        )
    if row[2] is None:
        raise EventConflictError("event ID belongs to a non-typed declaration")
    if str(row[2]) != structural_event_id:
        raise ValidationError("typed runtime event binding is corrupt")
    if row[3] is None:
        return None

    epoch_id = int(row[0])
    outcome = M5ReplayedOutcome(str(row[3]))
    failure_reason = None if row[10] is None else M5RunFailureReason(str(row[10]))
    event_work = _load_runtime_work(
        cursor,
        structural_event_id=structural_event_id,
        work_kind="event",
    )
    original_call_work = _load_runtime_work(
        cursor,
        structural_event_id=structural_event_id,
        work_kind="call",
    )
    if event_work is None or original_call_work is None:
        raise ValidationError("terminal M5 result lacks its durable work rows")

    delta_rows = cursor.execute(
        """
        SELECT object_type, object_id, old_status, new_status, reason
        FROM groundloop_m5_event_result_delta
        WHERE structural_event_id = %s
        ORDER BY delta_ordinal
        """,
        (structural_event_id,),
    ).fetchall()
    combined_deltas = tuple(
        StatusDelta(
            event_id=structural_event_id,
            object_type=str(delta[0]),
            object_id=str(delta[1]),
            old_status=str(delta[2]),
            new_status=str(delta[3]),
            reason=str(delta[4]),
        )
        for delta in delta_rows
    )
    reference_rows = cursor.execute(
        """
        SELECT kind, object_id, epoch_id, revision,
               state_artifact_hash, reference_digest
        FROM groundloop_m5_event_result_state_reference
        WHERE structural_event_id = %s
        ORDER BY reference_ordinal
        """,
        (structural_event_id,),
    ).fetchall()
    changed_references = tuple(
        M5ChangedStateReference(
            kind=M5StateReferenceKind(str(reference[0])),
            object_id=str(reference[1]),
            epoch_id=int(reference[2]),
            revision=int(reference[3]),
            state_artifact_hash=str(reference[4]).strip(),
            reference_digest=str(reference[5]).strip(),
        )
        for reference in reference_rows
    )
    if len(combined_deltas) != int(row[12]) or len(changed_references) != int(row[13]):
        raise ValidationError("terminal M5 result child cardinality is corrupt")
    if event_work.work_digest != str(row[7]).strip():
        raise ValidationError("terminal M5 result binds different event work")
    if digests.combined_status_delta_set_digest(combined_deltas) != str(row[8]).strip():
        raise ValidationError("terminal M5 result delta-set digest is corrupt")
    if (
        digests.changed_state_set_digest(
            reference.reference_digest for reference in changed_references
        )
        != str(row[9]).strip()
    ):
        raise ValidationError("terminal M5 result state-set digest is corrupt")

    expected_open_binding = digests.open_event_receipt_binding_digest(
        epoch_id=epoch_id,
        replayed=False,
        already_sealed=False,
        publication_id=None,
        already_failed=False,
        failure_reason=None,
    )
    if expected_open_binding != str(row[4]).strip():
        raise ValidationError("terminal M5 result open-receipt binding is corrupt")

    publication_receipt: PublicationReceipt | None = None
    if outcome is M5ReplayedOutcome.SEALED:
        if row[5] is None or row[6] is None or failure_reason is not None:
            raise ValidationError("sealed M5 result has an invalid durable shape")
        publication_id = str(row[5])
        expected_publication_binding = digests.publication_receipt_binding_digest(
            epoch_id=epoch_id,
            publication_id=publication_id,
            replayed=False,
        )
        if expected_publication_binding != str(row[6]).strip():
            raise ValidationError(
                "terminal M5 result publication-receipt binding is corrupt"
            )
        open_receipt = OpenEventReceipt(
            epoch_id=epoch_id,
            replayed=True,
            already_sealed=True,
            publication_id=publication_id,
        )
        publication_receipt = PublicationReceipt(
            epoch_id=epoch_id,
            publication_id=publication_id,
            replayed=True,
        )
    else:
        if row[5] is not None or row[6] is not None or failure_reason is None:
            raise ValidationError("failed M5 result has an invalid durable shape")
        open_receipt = OpenEventReceipt(
            epoch_id=epoch_id,
            replayed=True,
            already_sealed=False,
            already_failed=True,
            failure_reason=failure_reason.value,
        )

    event_timing = M5RuntimeTiming(
        coordinator_non_db_non_neural_ns=int(row[14]),
        neural_wall_ns=int(row[15]),
        postgres_roundtrip_wall_ns=int(row[16]),
        external_io_wall_ns=int(row[17]),
        end_to_end_wall_ns=int(row[18]),
        postgres_server_execution_ns=_optional_int(row[19]),
        postgres_lock_wait_ns=_optional_int(row[20]),
        postgres_wal_bytes=_optional_int(row[21]),
        postgres_shared_block_reads=_optional_int(row[22]),
    )
    event_timing_coverage: M5RuntimeTimingCoverage | None = None
    call_timing_coverage: M5RuntimeTimingCoverage | None = None
    coverage_relation = cursor.execute(
        "SELECT to_regclass('groundloop_m5_event_timing_coverage')"
    ).fetchone()
    if coverage_relation is not None and coverage_relation[0] is not None:
        coverage_row = cursor.execute(
            """
            SELECT required_expected_count, required_observed_count,
                   required_missing_count,
                   postgres_server_execution_expected_count,
                   postgres_server_execution_observed_count,
                   postgres_server_execution_missing_count,
                   postgres_lock_wait_expected_count,
                   postgres_lock_wait_observed_count,
                   postgres_lock_wait_missing_count,
                   postgres_wal_bytes_expected_count,
                   postgres_wal_bytes_observed_count,
                   postgres_wal_bytes_missing_count,
                   postgres_shared_block_reads_expected_count,
                   postgres_shared_block_reads_observed_count,
                   postgres_shared_block_reads_missing_count,
                   terminal_client_roundtrip_included
            FROM groundloop_m5_event_timing_coverage
            WHERE structural_event_id = %s AND epoch_id = %s
            """,
            (structural_event_id, epoch_id),
        ).fetchone()
        if coverage_row is None:
            raise ValidationError(
                "terminal M5 result lacks frozen event timing coverage"
            )
        coverage_values = tuple(coverage_row)
        event_timing_coverage = M5RuntimeTimingCoverage(
            required_expected_count=int(coverage_values[0]),
            required_observed_count=int(coverage_values[1]),
            required_missing_count=int(coverage_values[2]),
            postgres_server_execution_expected_count=int(coverage_values[3]),
            postgres_server_execution_observed_count=int(coverage_values[4]),
            postgres_server_execution_missing_count=int(coverage_values[5]),
            postgres_lock_wait_expected_count=int(coverage_values[6]),
            postgres_lock_wait_observed_count=int(coverage_values[7]),
            postgres_lock_wait_missing_count=int(coverage_values[8]),
            postgres_wal_bytes_expected_count=int(coverage_values[9]),
            postgres_wal_bytes_observed_count=int(coverage_values[10]),
            postgres_wal_bytes_missing_count=int(coverage_values[11]),
            postgres_shared_block_reads_expected_count=int(coverage_values[12]),
            postgres_shared_block_reads_observed_count=int(coverage_values[13]),
            postgres_shared_block_reads_missing_count=int(coverage_values[14]),
            terminal_client_roundtrip_included=bool(coverage_values[15]),
        )
        event_timing_coverage.validate_aggregate(event_timing)
        call_timing_coverage = M5RuntimeTimingCoverage.single_point(
            None,
            terminal_client_roundtrip_included=False,
        )
    result = M5EventRunResult.build(
        event_id=structural_event_id,
        payload_hash=payload_hash,
        epoch_id=epoch_id,
        state=M5RunState.REPLAYED,
        replayed_outcome=outcome,
        open_receipt=open_receipt,
        publication_receipt=publication_receipt,
        event_work=event_work,
        call_work=M5RuntimeWork(),
        event_timing=event_timing,
        call_timing=M5RuntimeTiming(),
        combined_deltas=combined_deltas,
        changed_state_references=changed_references,
        failure_reason=failure_reason,
        event_timing_coverage=event_timing_coverage,
        call_timing_coverage=call_timing_coverage,
    )
    if result.logical_result_hash != str(row[11]).strip():
        raise ValidationError("terminal M5 logical-result hash is corrupt")
    return result


class PostgresM5RuntimeStore:
    """Checked typed-runtime transactions over the migration-014/015 boundary."""

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def prepare_activation_request(self, activation_id: str) -> M5ActivationRequest:
        """Read a stable activation candidate; ``activate`` revalidates it."""

        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            activation = cursor.execute(
                "SELECT 1 FROM groundloop_m5_activation WHERE singleton"
            ).fetchone()
            if activation is not None:
                raise EventConflictError("M5 is already activated")
            mode_row = cursor.execute(
                """
                SELECT mode, mode_revision
                FROM groundloop_runtime_mode
                WHERE singleton
                """
            ).fetchone()
            m4_head_row = cursor.execute(
                """
                SELECT epoch_id
                FROM groundloop_m4_publication_head
                WHERE singleton
                """
            ).fetchone()
            m5_head_row = cursor.execute(
                """
                SELECT epoch_id
                FROM groundloop_m5_publication_head
                WHERE singleton
                """
            ).fetchone()
            if (
                mode_row is None
                or str(mode_row[0]) != "v1_only"
                or m4_head_row is None
                or m5_head_row is not None
            ):
                raise InvalidEventError("database is not activation-ready")
            core_bundle_hash, _ = _runtime_bundle_ledgers(cursor)
            projection = build_m5_bootstrap_projection(self._connection)
            if projection.epoch_id != int(m4_head_row[0]):
                raise InvalidEventError(
                    "M5 bootstrap projection does not equal the M4 head"
                )
            return M5ActivationRequest.build(
                activation_id=activation_id,
                expected_mode_revision=int(mode_row[1]),
                expected_base_m4_epoch_id=projection.epoch_id,
                core_schema_bundle_sha256=core_bundle_hash,
                bootstrap_state_hash=m5_bootstrap_state_hash(projection),
            )

    def _read_existing_activation_read_only(
        self, request: M5ActivationRequest
    ) -> M5ActivationReceipt | None:
        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            row = cursor.execute(
                """
                SELECT activation.activation_id, activation.payload_hash,
                       activation.base_m4_epoch_id, mode.mode,
                       mode.mode_revision, m4_head.epoch_id, m5_head.epoch_id
                FROM groundloop_m5_activation AS activation
                CROSS JOIN groundloop_runtime_mode AS mode
                CROSS JOIN groundloop_m4_publication_head AS m4_head
                CROSS JOIN groundloop_m5_publication_head AS m5_head
                WHERE activation.singleton AND mode.singleton
                  AND m4_head.singleton AND m5_head.singleton
                """
            ).fetchone()
            if row is None:
                return None
            return _activation_receipt_from_row(request, tuple(row))

    def activate(
        self,
        request: M5ActivationRequest,
        *,
        failure_injector: RuntimeFailureInjector | None = None,
    ) -> M5ActivationReceipt:
        """Atomically bootstrap M5 at the sealed M4 head and flip the route."""

        existing = self._read_existing_activation_read_only(request)
        if existing is not None:
            return existing

        with self._connection.transaction(), self._connection.cursor() as cursor:
            mode_row = cursor.execute(
                """
                SELECT mode, mode_revision
                FROM groundloop_runtime_mode
                WHERE singleton
                FOR UPDATE
                """
            ).fetchone()
            m4_head_row = cursor.execute(
                """
                SELECT epoch_id
                FROM groundloop_m4_publication_head
                WHERE singleton
                FOR UPDATE
                """
            ).fetchone()
            m5_head_row = cursor.execute(
                """
                SELECT epoch_id
                FROM groundloop_m5_publication_head
                WHERE singleton
                FOR UPDATE
                """
            ).fetchone()
            activation_row = cursor.execute(
                """
                SELECT activation_id, payload_hash, base_m4_epoch_id
                FROM groundloop_m5_activation
                WHERE singleton
                FOR UPDATE
                """
            ).fetchone()
            if mode_row is None or m4_head_row is None:
                raise InvalidEventError("activation singleton/head is missing")
            if activation_row is not None:
                if m5_head_row is None:
                    raise ValidationError("durable M5 activation lacks its head")
                return _activation_receipt_from_row(
                    request,
                    (
                        *tuple(activation_row),
                        mode_row[0],
                        mode_row[1],
                        m4_head_row[0],
                        m5_head_row[0],
                    ),
                )
            if (
                str(mode_row[0]) != "v1_only"
                or int(mode_row[1]) != request.expected_mode_revision
                or m5_head_row is not None
            ):
                raise EventConflictError("runtime mode is not activation-ready")

            base_epoch_id = int(m4_head_row[0])
            if base_epoch_id != request.expected_base_m4_epoch_id:
                raise EventConflictError("M4 head changed before activation")
            base_epoch = cursor.execute(
                """
                SELECT revision, structural_status, semantic_status,
                       evaluation_state
                FROM groundloop_epoch
                WHERE epoch_id = %s
                FOR UPDATE
                """,
                (base_epoch_id,),
            ).fetchone()
            if base_epoch is None or tuple(base_epoch[1:]) != (
                "committed",
                "sealed",
                "complete",
            ):
                raise InvalidEventError("activation base is not a sealed M4 epoch")
            live_epoch = cursor.execute(
                """
                SELECT epoch_id
                FROM groundloop_epoch
                WHERE structural_status = 'committed'
                  AND semantic_status IN ('pending', 'complete')
                ORDER BY epoch_id
                FOR UPDATE
                LIMIT 1
                """
            ).fetchone()
            if live_epoch is not None:
                raise EventConflictError("activation rejects a live mutation epoch")

            core_bundle_hash, _ = _runtime_bundle_ledgers(cursor)
            if request.core_schema_bundle_sha256 != core_bundle_hash:
                raise InvalidEventError(
                    "activation request does not bind the installed core bundle"
                )
            projection = build_m5_bootstrap_projection(self._connection)
            if projection.epoch_id != base_epoch_id or projection.revision != int(
                base_epoch[0]
            ):
                raise InvalidEventError("activation bootstrap coordinate drift")
            actual_bootstrap_hash = m5_bootstrap_state_hash(projection)
            if request.bootstrap_state_hash != actual_bootstrap_hash:
                raise InvalidEventError("activation bootstrap state hash mismatch")
            _inject(failure_injector, "activation_before_bootstrap")

            write_m5_materialized_states(
                self._connection,
                states=projection.states,
                decision_policy_version=projection.decision_policy_version,
                epoch_id=projection.epoch_id,
                revision=projection.revision,
                group_certificates=projection.group_certificates,
                claim_certificates=projection.claim_certificates,
                publish=True,
            )
            _inject(failure_injector, "activation_after_bootstrap")
            cursor.execute(
                """
                INSERT INTO groundloop_m5_publication_head (
                    singleton, epoch_id, sealed_revision, updated_at
                ) VALUES (true, %s, %s, now())
                """,
                (projection.epoch_id, projection.revision),
            )
            _inject(failure_injector, "activation_after_m5_head")
            cursor.execute(
                """
                INSERT INTO groundloop_m5_activation (
                    singleton, activation_id, payload_hash,
                    base_m4_epoch_id, activated_at
                ) VALUES (true, %s, %s, %s, now())
                """,
                (
                    request.activation_id,
                    request.payload_hash,
                    request.expected_base_m4_epoch_id,
                ),
            )
            _inject(failure_injector, "activation_after_record")
            updated = cursor.execute(
                """
                UPDATE groundloop_runtime_mode
                SET mode = 'm5_active', mode_revision = mode_revision + 1,
                    updated_at = now()
                WHERE singleton AND mode = 'v1_only' AND mode_revision = %s
                """,
                (request.expected_mode_revision,),
            ).rowcount
            if updated != 1:
                raise EventConflictError("activation mode CAS failed")
            _inject(failure_injector, "activation_after_mode")
            _force_deferred_validation(cursor)
            _inject(failure_injector, "activation_after_constraints")
            return M5ActivationReceipt.build(request)

    def register_candidate_policy(self, manifest: M5CandidatePolicyManifest) -> None:
        expected = (
            manifest.candidate_policy_id,
            manifest.manifest_hash,
            manifest.embedding_model_artifact_id,
            manifest.requirement_role_template_hash,
            manifest.chunk_role_template_hash,
            manifest.vector_method_version,
            manifest.vector_index_kind.value,
            manifest.vector_index_build_config_hash,
            manifest.vector_search_config_hash,
            manifest.lexical_method_version,
            manifest.lexical_config_hash,
            manifest.lexical_postgres_version,
            manifest.lexical_regconfig_identity,
            manifest.fusion_version,
            manifest.reverse_budget_per_inserted_chunk,
            manifest.forward_budget_per_requirement,
            manifest.verifier_execution_spec_hash,
            manifest.decision_policy_version,
            manifest.lineage_safety_override,
        )
        with self._connection.transaction(), self._connection.cursor() as cursor:
            inserted = cursor.execute(
                """
                INSERT INTO groundloop_m5_candidate_policy (
                    candidate_policy_id, candidate_policy_manifest_hash,
                    embedding_model_artifact_id, requirement_role_template_hash,
                    chunk_role_template_hash, vector_method_version,
                    vector_index_kind, vector_index_build_config_hash,
                    vector_search_config_hash, lexical_method_version,
                    lexical_config_hash, lexical_postgres_version,
                    lexical_regconfig_identity, fusion_version,
                    reverse_budget_per_inserted_chunk,
                    forward_budget_per_requirement,
                    verifier_execution_spec_hash, decision_policy_version,
                    lineage_safety_override
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (candidate_policy_id) DO NOTHING
                """,
                expected,
            ).rowcount
            if inserted == 1:
                return
            actual = cursor.execute(
                """
                SELECT candidate_policy_id, candidate_policy_manifest_hash,
                       embedding_model_artifact_id,
                       requirement_role_template_hash,
                       chunk_role_template_hash, vector_method_version,
                       vector_index_kind, vector_index_build_config_hash,
                       vector_search_config_hash, lexical_method_version,
                       lexical_config_hash, lexical_postgres_version,
                       lexical_regconfig_identity, fusion_version,
                       reverse_budget_per_inserted_chunk,
                       forward_budget_per_requirement,
                       verifier_execution_spec_hash, decision_policy_version,
                       lineage_safety_override
                FROM groundloop_m5_candidate_policy
                WHERE candidate_policy_id = %s
                """,
                (manifest.candidate_policy_id,),
            ).fetchone()
            if actual is None or tuple(actual) != expected:
                raise EventConflictError(
                    "candidate policy identifier has conflicting immutable content"
                )

    def read_typed_event_result(
        self, structural_event_id: str, payload_hash: str
    ) -> M5EventRunResult | None:
        """Read one immutable terminal result without locks or writes."""

        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            return _load_terminal_result(
                cursor,
                structural_event_id=structural_event_id,
                payload_hash=payload_hash,
            )

    def current_revision(self, epoch_id: int) -> int:
        """Read the committed revision of one typed runtime epoch."""

        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            return _read_runtime_epoch_header(
                cursor, epoch_id, for_update=False
            ).revision

    def verifier_jobs(self, epoch_id: int) -> tuple[M5LogicalJobSpec, ...]:
        """Hydrate canonical committed verifier-job identities read-only."""

        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            header = _read_runtime_epoch_header(cursor, epoch_id, for_update=False)
            rows = cursor.execute(
                _M5_JOB_SELECT
                + """
                  WHERE epoch_id = %s AND job_kind = %s
                  ORDER BY logical_job_id COLLATE "C"
                  """,
                (epoch_id, M5JobKind.VERIFY_REQUIREMENT_PAIR.value),
            ).fetchall()
            jobs = tuple(
                _stored_job_from_row(tuple(row), epoch_id).spec for row in rows
            )
            if any(
                job.structural_event_id != header.structural_event_id for job in jobs
            ):
                raise ValidationError(
                    "stored verifier job belongs to another structural event"
                )
            return jobs

    def current_event_work(self, epoch_id: int) -> M5RuntimeWork:
        """Read the terminal event row or current nonterminal point image."""

        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            header = _read_runtime_epoch_header(cursor, epoch_id, for_update=False)
            work = _load_runtime_work(
                cursor,
                structural_event_id=header.structural_event_id,
                work_kind="event",
            )
            if work is not None:
                return work
            if header.runtime_state in {"sealed", "failed"}:
                raise ValidationError("terminal M5 epoch lacks persisted event work")
            accumulator = _load_runtime_work_accumulator(
                cursor,
                epoch_id=epoch_id,
                expected_revision=header.revision,
            )
            return M5RuntimeWork() if accumulator is None else accumulator

    def append_transition_call_timing(
        self,
        epoch_id: int,
        contribution_kind: M5RuntimeWorkContributionKind,
        source_id: str,
        contribution_key_digest: str,
        anchor_revision: int,
        observed_timing: M5RuntimeTiming | None,
    ) -> M5TransitionTimingReceipt:
        """Append or exactly replay one frozen nonterminal transition point."""

        with self._connection.transaction(), self._connection.cursor() as cursor:
            require_runtime_recovery_bundle(cursor)
            header = _read_runtime_epoch_header(cursor, epoch_id, for_update=True)
            receipt = append_transition_call_timing_checked(
                cursor,
                epoch_id=epoch_id,
                contribution_kind=contribution_kind,
                source_id=source_id,
                contribution_key_digest=contribution_key_digest,
                anchor_revision=anchor_revision,
                current_revision=header.revision,
                observed_timing=observed_timing,
            )
            _force_deferred_validation(cursor)
            return receipt

    def current_event_timing(
        self, epoch_id: int
    ) -> tuple[M5RuntimeTiming, M5RuntimeTimingCoverage]:
        """Read frozen terminal timing or project the current pending point missing."""

        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            header = _read_runtime_epoch_header(cursor, epoch_id, for_update=False)
            return read_current_event_timing(
                cursor,
                epoch_id=epoch_id,
                structural_event_id=header.structural_event_id,
                current_revision=header.revision,
                terminal=header.runtime_state in {"sealed", "failed"},
            )

    def append_terminal_invocation_telemetry(
        self,
        invocation_id: str,
        event_id: str,
        epoch_id: int,
        terminal_logical_result_hash: str,
        call_timing: M5RuntimeTiming | None,
        call_timing_coverage: M5RuntimeTimingCoverage,
    ) -> None:
        """Append or exactly replay telemetry for one committed terminal call."""

        with self._connection.transaction(), self._connection.cursor() as cursor:
            require_runtime_recovery_bundle(cursor)
            persist_terminal_invocation_telemetry(
                cursor,
                invocation_id=invocation_id,
                structural_event_id=event_id,
                epoch_id=epoch_id,
                terminal_logical_result_hash=terminal_logical_result_hash,
                call_timing=call_timing,
                call_timing_coverage=call_timing_coverage,
            )
            _force_deferred_validation(cursor)

    def acquire_m5_job(
        self,
        epoch_id: int,
        expected_revision: int,
        job: M5LogicalJobSpec,
    ) -> M5JobLease:
        """Return the total checked D24 acquisition outcome for one M5 job."""

        if (
            isinstance(expected_revision, bool)
            or not isinstance(expected_revision, int)
            or expected_revision < 1
        ):
            raise InvalidEventError("expected runtime revision must be positive")
        if not isinstance(job, M5LogicalJobSpec):
            raise ValidationError("job must be an M5LogicalJobSpec")

        with self._connection.transaction(), self._connection.cursor() as cursor:
            require_runtime_recovery_bundle(cursor)
            header = _read_runtime_epoch_header(cursor, epoch_id, for_update=True)
            stored_job = _read_stored_job(
                cursor,
                epoch_id=epoch_id,
                logical_job_id=job.logical_job_id,
                for_update=True,
            )
            if stored_job.spec != job:
                raise EventConflictError(
                    "job acquisition identity differs from its durable declaration"
                )
            if job.structural_event_id != header.structural_event_id:
                raise EventConflictError("job belongs to another structural event")
            latest = read_latest_requirement_attempt(
                cursor, logical_job_id=job.logical_job_id
            )
            # D24 samples the database clock only after base -> runtime -> job ->
            # latest-attempt locks have been acquired.  Even no-write outcomes
            # use this one total decision point.
            acquisition_clock = read_acquisition_clock(cursor, epoch_id=epoch_id)

            if expected_revision > header.revision:
                raise EventConflictError("expected revision is newer than the epoch")
            if latest is not None and (
                latest.attempt.logical_job_id != job.logical_job_id
                or latest.attempt.execution_spec_hash != job.execution_spec_hash
            ):
                raise ValidationError("latest M5 attempt disagrees with its job")

            if stored_job.state.terminal:
                assert stored_job.completion is not None
                terminal_projection = M5LeaseTerminalProjection.build(
                    logical_job_id=job.logical_job_id,
                    terminal_state=stored_job.state,
                    terminal_reason=stored_job.completion.archive_reason,
                    completion_digest=stored_job.completion.completion_digest,
                )
                dispatch = (
                    None
                    if latest is None
                    else load_requirement_dispatch(
                        cursor, epoch_id=epoch_id, attempt=latest.attempt
                    )
                )
                return M5JobLease(
                    logical_job_id=job.logical_job_id,
                    attempt=None if latest is None else latest.attempt,
                    resulting_revision=header.revision,
                    should_execute=False,
                    exact_replay=True,
                    lease_expires_at=(
                        None if latest is None else latest.attempt.lease_expires_at
                    ),
                    dispatch_record_digest=(
                        None if dispatch is None else dispatch.record_digest
                    ),
                    disposition=M5AcquisitionDisposition.TERMINAL,
                    terminal_projection=terminal_projection,
                )
            if stored_job.state is M5JobState.RUNNING:
                if latest is None:
                    raise ValidationError(
                        "running M5 job lacks its live durable attempt"
                    )
                if latest.state == "result_reserved":
                    raise ValidationError(
                        "committed result_reserved M5 attempt is invalid"
                    )
                dispatch = load_requirement_dispatch(
                    cursor, epoch_id=epoch_id, attempt=latest.attempt
                )
                if latest.state == "completed":
                    if not root_result_is_reserved(
                        cursor,
                        epoch_id=epoch_id,
                        logical_job_id=job.logical_job_id,
                        attempt=latest,
                    ):
                        raise ValidationError(
                            "running completed M5 attempt lacks reserved-result closure"
                        )
                    return M5JobLease(
                        logical_job_id=job.logical_job_id,
                        attempt=latest.attempt,
                        resulting_revision=header.revision,
                        should_execute=False,
                        exact_replay=True,
                        lease_expires_at=latest.attempt.lease_expires_at,
                        dispatch_record_digest=dispatch.record_digest,
                        disposition=M5AcquisitionDisposition.RESULT_RESERVED,
                    )
                if latest.state != "dispatched":
                    raise ValidationError(
                        "running M5 job has an invalid latest attempt state"
                    )
                assert latest.attempt.lease_expires_at is not None
                if acquisition_clock.decision_time < latest.attempt.lease_expires_at:
                    return M5JobLease(
                        logical_job_id=job.logical_job_id,
                        attempt=latest.attempt,
                        resulting_revision=header.revision,
                        should_execute=False,
                        exact_replay=True,
                        lease_expires_at=latest.attempt.lease_expires_at,
                        dispatch_record_digest=dispatch.record_digest,
                        disposition=M5AcquisitionDisposition.LIVE_LEASE,
                    )
                acquisition_disposition = M5AcquisitionDisposition.DISPATCH_TAKEOVER
                takeover_attempt_id = latest.attempt.attempt_id
                next_ordinal = latest.attempt.attempt_ordinal + 1
            else:
                acquisition_disposition = M5AcquisitionDisposition.DISPATCH_NEW
                takeover_attempt_id = None
                next_ordinal = (
                    1 if latest is None else latest.attempt.attempt_ordinal + 1
                )

            _require_pending_lifecycle_epoch(header)
            if header.revision != expected_revision:
                raise EventConflictError("stale typed runtime revision")
            if stored_job.state is M5JobState.DECLARED:
                if latest is not None:
                    raise ValidationError("declared M5 job already has an attempt")
            elif stored_job.state is M5JobState.RETRYABLE_FAILED:
                if (
                    latest is None
                    or latest.state != "failed"
                    or latest.error_hash is None
                ):
                    raise ValidationError(
                        "retryable-failed M5 job lacks its failed attempt"
                    )
            elif stored_job.state is M5JobState.RUNNING:
                if acquisition_disposition is not (
                    M5AcquisitionDisposition.DISPATCH_TAKEOVER
                ):
                    raise ValidationError("running M5 job is not takeover eligible")
            else:
                raise InvalidEventError("M5 job is not acquirable")

            lease_token_hash = _new_lease_token_hash()
            if latest is not None:
                while lease_token_hash == latest.attempt.lease_token_hash:
                    lease_token_hash = _new_lease_token_hash()
            attempt = M5JobAttempt.build(
                logical_job_id=job.logical_job_id,
                attempt_ordinal=next_ordinal,
                execution_spec_hash=job.execution_spec_hash,
                lease_token_hash=lease_token_hash,
                lease_expires_at=acquisition_clock.lease_expires_at,
                attempt_work_digest=M5RuntimeWork().work_digest,
            )
            fallback_required = requirement_fallback_required(
                cursor,
                epoch_id=epoch_id,
                job_kind=job.job_kind,
                logical_job_id=job.logical_job_id,
            )
            dispatch, anchor = persist_requirement_dispatch(
                cursor,
                epoch_id=epoch_id,
                expected_revision=expected_revision,
                attempt=attempt,
                job_kind=job.job_kind,
                fallback_required=fallback_required,
                takeover_attempt_id=takeover_attempt_id,
                decision_time=acquisition_clock.decision_time,
            )
            if acquisition_disposition is M5AcquisitionDisposition.DISPATCH_NEW:
                updated = cursor.execute(
                    """
                    UPDATE groundloop_m5_semantic_job
                    SET job_state = 'running'
                    WHERE epoch_id = %s AND logical_job_id = %s
                      AND job_state = %s
                    """,
                    (
                        epoch_id,
                        job.logical_job_id,
                        stored_job.state.value,
                    ),
                ).rowcount
                if updated != 1:
                    raise EventConflictError("M5 job changed before acquisition")
            resulting_revision = _advance_job_lifecycle_revision(
                cursor, header=header, expected_revision=expected_revision
            )
            finish_requirement_dispatch_accounting(
                cursor,
                epoch_id=epoch_id,
                expected_revision=expected_revision,
                anchor=anchor,
            )
            _force_deferred_validation(cursor)
            return M5JobLease(
                logical_job_id=job.logical_job_id,
                attempt=attempt,
                resulting_revision=resulting_revision,
                should_execute=True,
                exact_replay=False,
                lease_expires_at=attempt.lease_expires_at,
                dispatch_record_digest=dispatch.record_digest,
                disposition=acquisition_disposition,
            )

    def mark_m5_retryable_failure(
        self,
        epoch_id: int,
        expected_revision: int,
        lease: M5JobLease,
        error_hash: str,
        attempt_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
    ) -> M5AttemptCompletionReceipt:
        """Persist an external-call failure without inventing result identity."""

        return self._mark_m5_failure(
            epoch_id=epoch_id,
            expected_revision=expected_revision,
            lease=lease,
            terminal_reason=None,
            error_hash=error_hash,
            attempt_work=attempt_work,
            attempt_timing=attempt_timing,
        )

    def mark_m5_terminal_failure(
        self,
        epoch_id: int,
        expected_revision: int,
        lease: M5JobLease,
        terminal_reason: M5TerminalReason,
        error_hash: str,
        attempt_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
    ) -> M5AttemptCompletionReceipt:
        """Settle one attempt and move its current job to blocking failure."""

        return self._mark_m5_failure(
            epoch_id=epoch_id,
            expected_revision=expected_revision,
            lease=lease,
            terminal_reason=terminal_reason,
            error_hash=error_hash,
            attempt_work=attempt_work,
            attempt_timing=attempt_timing,
        )

    def _mark_m5_failure(
        self,
        *,
        epoch_id: int,
        expected_revision: int,
        lease: M5JobLease,
        terminal_reason: M5TerminalReason | None,
        error_hash: str,
        attempt_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
    ) -> M5AttemptCompletionReceipt:
        if (
            isinstance(expected_revision, bool)
            or not isinstance(expected_revision, int)
            or expected_revision < 1
        ):
            raise InvalidEventError("expected runtime revision must be positive")
        _require_sha256("error_hash", error_hash)
        if terminal_reason is not None and (
            not isinstance(terminal_reason, M5TerminalReason)
            or terminal_reason not in _M5_FAILURE_TERMINAL_REASONS
        ):
            raise ValidationError("terminal failure requires a failure terminal reason")
        attempt = _require_executable_lease(lease, expected_revision=expected_revision)
        disposition = (
            M5ExecutionEvidenceDisposition.RETRYABLE_FAILURE
            if terminal_reason is None
            else M5ExecutionEvidenceDisposition.TERMINAL_FAILURE
        )

        with self._connection.transaction(), self._connection.cursor() as cursor:
            require_runtime_recovery_bundle(cursor)
            header = _read_runtime_epoch_header(cursor, epoch_id, for_update=True)
            stored_job = _read_stored_job(
                cursor,
                epoch_id=epoch_id,
                logical_job_id=lease.logical_job_id,
                for_update=True,
            )
            if (
                stored_job.spec.structural_event_id != header.structural_event_id
                or attempt.execution_spec_hash != stored_job.spec.execution_spec_hash
            ):
                raise EventConflictError("lease execution identity is inconsistent")
            dispatch = load_requirement_dispatch(
                cursor, epoch_id=epoch_id, attempt=attempt
            )
            if (
                dispatch.record_digest != lease.dispatch_record_digest
                or dispatch.dispatched_revision != lease.resulting_revision
            ):
                raise EventConflictError("lease dispatch identity is not durable")
            accounting = build_requirement_execution_accounting(
                epoch_id=epoch_id,
                expected_revision=expected_revision,
                attempt=attempt,
                dispatch=dispatch,
                disposition=disposition,
                result_or_error_hash=error_hash,
                attempt_work=attempt_work,
                attempt_timing=attempt_timing,
            )
            durable_attempt = read_requirement_attempt(
                cursor,
                logical_job_id=lease.logical_job_id,
                attempt_id=attempt.attempt_id,
            )
            if durable_attempt is None or not _same_requirement_attempt_identity(
                durable_attempt.attempt, attempt
            ):
                raise EventConflictError("lease attempt identity is not durable")
            replay_revision = read_requirement_execution_replay(
                cursor,
                expected_evidence=accounting.evidence,
                expected_observation=accounting.observation,
            )
            if replay_revision is not None:
                self._validate_m5_failure_replay(
                    cursor=cursor,
                    header=header,
                    job=stored_job,
                    attempt=durable_attempt,
                    accounting=accounting,
                    terminal_reason=terminal_reason,
                    applied_revision=replay_revision,
                )
                return M5AttemptCompletionReceipt(
                    lease.logical_job_id,
                    attempt.attempt_id,
                    header.revision,
                    True,
                )
            return self._persist_new_m5_failure(
                cursor=cursor,
                header=header,
                job=stored_job,
                attempt=attempt,
                expected_revision=expected_revision,
                terminal_reason=terminal_reason,
                error_hash=error_hash,
                accounting=accounting,
            )

    @staticmethod
    def _validate_m5_failure_replay(
        *,
        cursor: Cursor[Any],
        header: _RuntimeEpochHeader,
        job: _StoredM5Job,
        attempt: StoredRequirementAttempt,
        accounting: RequirementExecutionAccounting,
        terminal_reason: M5TerminalReason | None,
        applied_revision: int,
    ) -> None:
        evidence = accounting.evidence
        if (
            applied_revision > header.revision
            or attempt.state != "failed"
            or attempt.error_hash != evidence.result_or_error_hash
            or attempt.attempt.attempt_work_digest != evidence.attempt_work.work_digest
        ):
            raise ValidationError("M5 failure evidence lacks its settled attempt")
        terminal_columns = ", ".join(_WORK_COUNTER_COLUMNS)
        terminal_rows = cursor.execute(
            f"""
            SELECT {terminal_columns}, work_digest, source_identity_hash,
                   contribution_key_digest
            FROM groundloop_m5_runtime_work_contribution
            WHERE epoch_id = %s
              AND contribution_kind = 'terminal_job_failure'
              AND source_id = %s AND applied_revision = %s
            """,
            (header.epoch_id, job.spec.logical_job_id, applied_revision),
        ).fetchall()
        if terminal_reason is None:
            if terminal_rows:
                raise ValidationError(
                    "retryable evidence owns a terminal contribution revision"
                )
            if (
                applied_revision == header.revision
                and job.state is not M5JobState.RETRYABLE_FAILED
            ):
                raise ValidationError("retryable evidence lacks its current job state")
            return
        if (
            job.state is M5JobState.TERMINAL_FAILED
            and job.completion is not None
            and job.completion.archive_reason is not terminal_reason
        ):
            raise EventConflictError(
                "terminal failure replay changed its terminal reason"
            )
        expected_terminal_source = (
            digests.terminal_job_failure_contribution_source_digest(
                logical_job_id=job.spec.logical_job_id,
                terminal_reason=terminal_reason,
                error_hash=evidence.result_or_error_hash,
            )
        )
        expected_terminal_key = digests.runtime_work_contribution_key_digest(
            epoch_id=header.epoch_id,
            contribution_kind="terminal_job_failure",
            source_id=job.spec.logical_job_id,
        )
        terminal_expected = (
            *M5RuntimeWork().counter_values(),
            M5RuntimeWork().work_digest,
            expected_terminal_source,
            expected_terminal_key,
        )
        if len(terminal_rows) != 1 or tuple(terminal_rows[0]) != terminal_expected:
            raise ValidationError(
                "terminal evidence lacks its exact zero failure contribution"
            )
        if (
            job.state is not M5JobState.TERMINAL_FAILED
            or job.completion is None
            or job.completion.archive_reason is not terminal_reason
            or job.completed_revision != applied_revision
        ):
            raise ValidationError("terminal evidence lacks its exact job completion")
        if job.spec.parent_job_id is None:
            scope = cursor.execute(
                """
                SELECT scope_state, completion_digest, closed_revision
                FROM groundloop_m5_discovery_scope
                WHERE epoch_id = %s AND root_job_id = %s
                """,
                (header.epoch_id, job.spec.logical_job_id),
            ).fetchone()
            if scope != (
                "terminal_failed",
                job.completion.completion_digest,
                applied_revision,
            ):
                raise ValidationError("terminal root lacks its exact failed scope")

    def _persist_new_m5_failure(
        self,
        *,
        cursor: Cursor[Any],
        header: _RuntimeEpochHeader,
        job: _StoredM5Job,
        attempt: M5JobAttempt,
        expected_revision: int,
        terminal_reason: M5TerminalReason | None,
        error_hash: str,
        accounting: RequirementExecutionAccounting,
    ) -> M5AttemptCompletionReceipt:
        _require_pending_lifecycle_epoch(header)
        if header.revision != expected_revision:
            raise EventConflictError("stale typed runtime revision")
        latest = read_latest_requirement_attempt(
            cursor, logical_job_id=job.spec.logical_job_id
        )
        if latest is None or not _same_requirement_attempt_identity(
            latest.attempt, attempt
        ):
            raise EventConflictError("failure lease is no longer the latest attempt")
        if latest.state != "dispatched":
            raise EventConflictError("only a dispatched current attempt may fail")
        if job.state is not M5JobState.RUNNING:
            raise EventConflictError("failure settlement requires a running job")
        settled_row = cursor.execute("SELECT clock_timestamp()").fetchone()
        if settled_row is None or not isinstance(settled_row[0], datetime):
            raise ValidationError("PostgreSQL did not return a settlement timestamp")
        settled_at = settled_row[0]
        resulting_revision = expected_revision + 1
        cursor.execute(
            "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
            (header.epoch_id, expected_revision),
        )
        start = start_event_accounting(
            cursor,
            epoch_id=header.epoch_id,
            expected_revision=expected_revision,
        )
        persist_requirement_execution_accounting(cursor, accounting=accounting)
        updated_attempt = cursor.execute(
            """
            UPDATE groundloop_m5_job_attempt
            SET attempt_state = 'failed', error_hash = %s,
                finished_at = %s, attempt_work_digest = %s
            WHERE attempt_id = %s AND logical_job_id = %s
              AND attempt_state = 'dispatched'
              AND lease_token_hash = %s AND lease_expires_at = %s
              AND execution_spec_hash = %s
            """,
            (
                error_hash,
                settled_at,
                accounting.evidence.attempt_work.work_digest,
                attempt.attempt_id,
                attempt.logical_job_id,
                attempt.lease_token_hash,
                attempt.lease_expires_at,
                attempt.execution_spec_hash,
            ),
        ).rowcount
        if updated_attempt != 1:
            raise EventConflictError("M5 attempt changed before failure settlement")
        open_scope_delta = 0
        if terminal_reason is None:
            updated_job = cursor.execute(
                """
                UPDATE groundloop_m5_semantic_job
                SET job_state = 'retryable_failed'
                WHERE epoch_id = %s AND logical_job_id = %s
                  AND job_state = 'running'
                """,
                (header.epoch_id, job.spec.logical_job_id),
            ).rowcount
            if updated_job != 1:
                raise EventConflictError("M5 job changed before retryable failure")
        else:
            open_scope_delta = self._apply_terminal_m5_failure(
                cursor=cursor,
                header=header,
                job=job,
                terminal_reason=terminal_reason,
                error_hash=error_hash,
                resulting_revision=resulting_revision,
                settled_at=settled_at,
            )
        resulting_revision = _advance_job_lifecycle_revision(
            cursor,
            header=header,
            expected_revision=expected_revision,
            open_work_delta=-int(terminal_reason is not None),
            open_scope_delta=open_scope_delta,
            blocking_failure_delta=int(terminal_reason is not None),
        )
        finish_requirement_execution_accounting(
            cursor,
            epoch_id=header.epoch_id,
            expected_revision=expected_revision,
            start=start,
            accounting=accounting,
        )
        _force_deferred_validation(cursor)
        return M5AttemptCompletionReceipt(
            job.spec.logical_job_id,
            attempt.attempt_id,
            resulting_revision,
            False,
        )

    @staticmethod
    def _apply_terminal_m5_failure(
        *,
        cursor: Cursor[Any],
        header: _RuntimeEpochHeader,
        job: _StoredM5Job,
        terminal_reason: M5TerminalReason,
        error_hash: str,
        resulting_revision: int,
        settled_at: datetime,
    ) -> int:
        completion = M5JobCompletion.build(
            job=job.spec,
            terminal_state=M5JobState.TERMINAL_FAILED,
            archive_reason=terminal_reason,
        )
        updated_job = cursor.execute(
            """
            UPDATE groundloop_m5_semantic_job
            SET job_state = 'terminal_failed', archive_reason = %s,
                completion_digest = %s, completed_revision = %s,
                completed_at = %s
            WHERE epoch_id = %s AND logical_job_id = %s
              AND job_state = 'running'
            """,
            (
                terminal_reason.value,
                completion.completion_digest,
                resulting_revision,
                settled_at,
                header.epoch_id,
                job.spec.logical_job_id,
            ),
        ).rowcount
        if updated_job != 1:
            raise EventConflictError("M5 job changed before terminal failure")
        root_scope_delta = 0
        if job.spec.parent_job_id is None:
            updated_scope = cursor.execute(
                """
                UPDATE groundloop_m5_discovery_scope
                SET scope_state = 'terminal_failed', completion_digest = %s,
                    closed_revision = %s, closed_at = %s
                WHERE epoch_id = %s AND root_job_id = %s
                  AND scope_state IN ('open', 'result_staged')
                """,
                (
                    completion.completion_digest,
                    resulting_revision,
                    settled_at,
                    header.epoch_id,
                    job.spec.logical_job_id,
                ),
            ).rowcount
            if updated_scope != 1:
                raise EventConflictError("M5 root scope changed before failure")
            root_scope_delta = -1
        _move_job_pending_to_blocking_failure(
            cursor,
            epoch_id=header.epoch_id,
            logical_job_id=job.spec.logical_job_id,
            job_kind=job.spec.job_kind,
            expected_revision=header.revision,
        )
        persist_terminal_job_failure_contribution(
            cursor,
            epoch_id=header.epoch_id,
            resulting_revision=resulting_revision,
            logical_job_id=job.spec.logical_job_id,
            terminal_reason=terminal_reason,
            error_hash=error_hash,
        )
        return root_scope_delta

    def stage_m5_discovery_result_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        lease: M5JobLease,
        job: M5LogicalJobSpec,
        result: M5RequirementDiscoveryResult,
        attempt_output: M5AttemptOutput,
        execution_disposition: M5ExecutionEvidenceDisposition,
        attempt_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
        *,
        eligible_snapshot_exhausted: bool,
        failure_injector: RuntimeFailureInjector | None = None,
    ) -> M5RequirementAttemptReturnReceipt:
        """Stage one root result inside exactly one owned transaction."""

        with self._connection.transaction(), self._connection.cursor() as cursor:
            return stage_m5_discovery_result(
                cursor,
                epoch_id,
                expected_revision,
                lease,
                job,
                result,
                attempt_output,
                execution_disposition,
                attempt_work,
                attempt_timing,
                eligible_snapshot_exhausted=eligible_snapshot_exhausted,
                failure_injector=failure_injector,
            )

    def close_m5_requirement_roots_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        requirement_root_set_hash: str,
        *,
        failure_injector: RuntimeFailureInjector | None = None,
    ) -> M5RootBarrierReceipt:
        """Close the complete frozen root set in one owned transaction."""

        with self._connection.transaction(), self._connection.cursor() as cursor:
            return close_m5_requirement_roots(
                cursor,
                epoch_id,
                expected_revision,
                requirement_root_set_hash,
                failure_injector=failure_injector,
            )

    def complete_m5_verifier_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        lease: M5JobLease,
        job: M5LogicalJobSpec,
        pair_input: M5RequirementPairInput,
        verifier_artifact: M5RequirementVerifierArtifact,
        attempt_output: M5AttemptOutput,
        execution_disposition: M5ExecutionEvidenceDisposition,
        attempt_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
        *,
        failure_injector: RuntimeFailureInjector | None = None,
    ) -> M5RequirementAttemptReturnReceipt:
        """Complete or audit one verifier attempt in one owned transaction."""

        with self._connection.transaction(), self._connection.cursor() as cursor:
            return complete_m5_verifier(
                cursor,
                epoch_id,
                expected_revision,
                lease,
                job,
                pair_input,
                verifier_artifact,
                attempt_output,
                execution_disposition,
                attempt_work,
                attempt_timing,
                failure_injector=failure_injector,
            )

    def cancel_m5_work_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        cancellation_plan: M5CancellationPlan,
        *,
        failure_injector: RuntimeFailureInjector | None = None,
    ) -> M5CancellationReceipt:
        """Cancel one exact requirement-job batch in an owned transaction."""

        with self._connection.transaction(), self._connection.cursor() as cursor:
            return cancel_m5_work(
                cursor,
                epoch_id,
                expected_revision,
                cancellation_plan,
                failure_injector=failure_injector,
            )

    def open_typed_event_atomically(
        self,
        plan: M5TypedEventPlan,
        *,
        recovery_operational_config: M5RuntimeOperationalConfig | None = None,
        recovery_root_fallback_required: dict[str, bool] | None = None,
        failure_injector: RuntimeFailureInjector | None = None,
    ) -> OpenEventReceipt:
        """Open the structural-group production failure/replay slice."""

        if not isinstance(
            plan.event, (RegisterGroupEvent, ReplaceGroupEvent, RetireGroupEvent)
        ):
            raise InvalidEventError(
                "the M5.3-07 production slice admits group lifecycle events only"
            )
        if plan.direct_plan is not None:
            raise InvalidEventError("group lifecycle events cannot carry a direct plan")

        recovery_values = (
            recovery_operational_config,
            recovery_root_fallback_required,
        )
        if any(value is None for value in recovery_values) and not all(
            value is None for value in recovery_values
        ):
            raise ValidationError(
                "recovery open requires config and root provenance together"
            )
        recovery_open = recovery_operational_config is not None
        if not recovery_open:
            with self._connection.transaction(), self._connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION READ ONLY")
                require_runtime_recovery_bundle(cursor)
            raise ValidationError("typed open requires config and root provenance")

        assert recovery_operational_config is not None
        assert recovery_root_fallback_required is not None
        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            require_runtime_recovery_bundle(cursor)
            if not isinstance(recovery_operational_config, M5RuntimeOperationalConfig):
                raise ValidationError(
                    "recovery config must be an M5RuntimeOperationalConfig"
                )
            manifest = _load_candidate_manifest(cursor, plan.candidate_policy_id)
            roots = _build_root_declarations(plan, manifest)
            fallback_map = _validate_recovery_root_fallback_map(
                roots, recovery_root_fallback_required
            )
            existing = self._read_existing_open(cursor, plan, for_update=False)
            if existing is not None:
                validate_structural_open_recovery(
                    cursor,
                    epoch_id=existing.epoch_id,
                    structural_event_id=plan.structural_event_id,
                    payload_hash=plan.payload_hash,
                    config=recovery_operational_config,
                    root_fallback_required=fallback_map,
                )
                return existing

        with self._connection.transaction(), self._connection.cursor() as cursor:
            require_runtime_recovery_bundle(cursor)
            mode_row = cursor.execute(
                """
                SELECT mode
                FROM groundloop_runtime_mode
                WHERE singleton
                FOR UPDATE
                """
            ).fetchone()
            if mode_row is None or str(mode_row[0]) != "m5_active":
                raise InvalidEventError("typed M5 mutation requires activated mode")

            m4_head_row = cursor.execute(
                """
                SELECT epoch_id
                FROM groundloop_m4_publication_head
                WHERE singleton
                FOR UPDATE
                """
            ).fetchone()
            m5_head_row = cursor.execute(
                """
                SELECT epoch_id
                FROM groundloop_m5_publication_head
                WHERE singleton
                FOR UPDATE
                """
            ).fetchone()
            if m4_head_row is None or m5_head_row is None:
                raise InvalidEventError("typed publication heads are not initialized")
            m4_head = int(m4_head_row[0])
            m5_head = int(m5_head_row[0])
            if (
                m4_head != m5_head
                or m5_head != plan.expected_previous_published_epoch_id
            ):
                raise InvalidEventError(
                    "typed event predecessor does not equal both publication heads"
                )

            activation = cursor.execute(
                """
                SELECT activation_id
                FROM groundloop_m5_activation
                WHERE singleton
                FOR UPDATE
                """
            ).fetchone()
            if activation is None:
                raise InvalidEventError("typed M5 runtime lacks an activation record")

            existing = self._read_existing_open(cursor, plan, for_update=True)
            if existing is not None:
                if recovery_open:
                    assert recovery_operational_config is not None
                    assert recovery_root_fallback_required is not None
                    existing_manifest = _load_candidate_manifest(
                        cursor, plan.candidate_policy_id
                    )
                    existing_roots = _build_root_declarations(plan, existing_manifest)
                    existing_fallback = _validate_recovery_root_fallback_map(
                        existing_roots, recovery_root_fallback_required
                    )
                    validate_structural_open_recovery(
                        cursor,
                        epoch_id=existing.epoch_id,
                        structural_event_id=plan.structural_event_id,
                        payload_hash=plan.payload_hash,
                        config=recovery_operational_config,
                        root_fallback_required=existing_fallback,
                    )
                return existing

            live_epoch = cursor.execute(
                """
                SELECT epoch_id
                FROM groundloop_epoch
                WHERE structural_status = 'committed'
                  AND semantic_status IN ('pending', 'complete')
                ORDER BY epoch_id
                FOR UPDATE
                LIMIT 1
                """
            ).fetchone()
            if live_epoch is not None:
                raise InvalidEventError("a structural epoch is already active")

            manifest = _load_candidate_manifest(cursor, plan.candidate_policy_id)
            if manifest.manifest_hash != plan.candidate_policy_manifest_hash:
                raise InvalidEventError(
                    "typed plan does not bind a registered candidate policy"
                )
            decision_policy_version = manifest.decision_policy_version
            roots = _build_root_declarations(plan, manifest)
            recovery_fallback_map: dict[str, bool] | None = None
            if recovery_open:
                assert recovery_root_fallback_required is not None
                recovery_fallback_map = _validate_recovery_root_fallback_map(
                    roots, recovery_root_fallback_required
                )
            root_set_hash = digests.requirement_root_set_digest(
                declaration.job.logical_job_id for declaration in roots
            )

            epoch_row = cursor.execute(
                """
                INSERT INTO groundloop_epoch (
                    event_id, payload_hash, revision, structural_status,
                    semantic_status, evaluation_state, publication_mode,
                    sealed_at
                ) VALUES (
                    %s, %s, 1, 'committed', 'pending', 'pending',
                    'provisional', NULL
                )
                RETURNING epoch_id
                """,
                (plan.structural_event_id, plan.payload_hash),
            ).fetchone()
            assert epoch_row is not None
            epoch_id = int(epoch_row[0])
            _inject(failure_injector, "typed_open_epoch_inserted")

            cursor.execute(
                """
                INSERT INTO groundloop_m5_update (
                    epoch_id, update_kind, previous_published_epoch_id,
                    decision_policy_version, manifest
                ) VALUES (%s, %s, %s, %s, '{}'::jsonb)
                """,
                (
                    epoch_id,
                    _m5_update_kind(plan.event),
                    plan.expected_previous_published_epoch_id,
                    decision_policy_version,
                ),
            )
            _inject(failure_injector, "typed_open_update_inserted")

            cursor.execute(
                """
                INSERT INTO groundloop_m5_runtime_epoch (
                    epoch_id, structural_event_id, candidate_policy_id,
                    candidate_policy_manifest_hash,
                    requirement_registry_snapshot_digest,
                    active_chunk_snapshot_digest,
                    expected_previous_published_epoch_id,
                    requirement_root_set_hash, runtime_state, revision,
                    open_work_count, open_scope_count,
                    blocking_failure_count, terminal_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    'structural_committed', 1, %s, %s, 0, NULL
                )
                """,
                (
                    epoch_id,
                    plan.structural_event_id,
                    plan.candidate_policy_id,
                    plan.candidate_policy_manifest_hash,
                    plan.requirement_registry_snapshot.requirement_registry_snapshot_digest,
                    plan.active_chunk_snapshot.active_chunk_snapshot_digest,
                    plan.expected_previous_published_epoch_id,
                    root_set_hash,
                    len(roots),
                    len(roots),
                ),
            )
            _inject(failure_injector, "typed_open_runtime_header_inserted")

            _validate_structure_declaration(cursor, plan.event)
            _stage_structure(
                cursor,
                event=plan.event,
                epoch_id=epoch_id,
                failure_injector=failure_injector,
            )
            _validate_effective_snapshots(cursor, plan=plan, epoch_id=epoch_id)
            _persist_requirement_snapshot(
                cursor,
                snapshot=plan.requirement_registry_snapshot,
                epoch_id=epoch_id,
            )
            _persist_active_chunk_snapshot(
                cursor,
                snapshot=plan.active_chunk_snapshot,
                epoch_id=epoch_id,
            )
            _persist_root_declarations(
                cursor,
                structural_event_id=plan.structural_event_id,
                epoch_id=epoch_id,
                roots=roots,
            )
            _inject(failure_injector, "typed_open_roots_persisted")
            _persist_initial_pending_counters(
                cursor,
                snapshot=plan.requirement_registry_snapshot,
                epoch_id=epoch_id,
                roots=roots,
            )
            if recovery_open:
                assert recovery_operational_config is not None
                assert recovery_fallback_map is not None
                persist_structural_open_identity(
                    cursor,
                    epoch_id=epoch_id,
                    config=recovery_operational_config,
                    root_fallback_required=recovery_fallback_map,
                )
                structural_work = _derive_structural_open_work(
                    cursor, epoch_id=epoch_id
                )
                persist_structural_open_accounting(
                    cursor,
                    epoch_id=epoch_id,
                    structural_event_id=plan.structural_event_id,
                    payload_hash=plan.payload_hash,
                    structural_work=structural_work,
                )
                _inject(failure_injector, "typed_open_recovery_initialized")
            _inject(failure_injector, "typed_open_snapshots_persisted")
            _inject(failure_injector, "typed_open_before_commit")
            _force_deferred_validation(cursor)
            return OpenEventReceipt(
                epoch_id=epoch_id,
                replayed=False,
                already_sealed=False,
            )

    def fail_typed_epoch_atomically(
        self,
        epoch_id: int,
        *,
        expected_revision: int = 1,
        failure_reason: M5RunFailureReason = M5RunFailureReason.INVARIANT_FAILURE,
        failure_injector: RuntimeFailureInjector | None = None,
    ) -> M5EventRunResult:
        """Terminally fail the production M5.3-07 revision-1 slice."""

        if expected_revision < 1:
            raise InvalidEventError("expected runtime revision must be positive")
        if not isinstance(failure_reason, M5RunFailureReason):
            raise ValidationError("failure_reason must be an M5RunFailureReason")

        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            identity = cursor.execute(
                """
                SELECT event_id, payload_hash
                FROM groundloop_epoch
                WHERE epoch_id = %s
                """,
                (epoch_id,),
            ).fetchone()
            if identity is None:
                raise InvalidEventError("typed epoch does not exist")
            structural_event_id = str(identity[0])
            payload_hash = str(identity[1]).strip()
            terminal = _load_terminal_result(
                cursor,
                structural_event_id=structural_event_id,
                payload_hash=payload_hash,
            )
            if terminal is not None:
                return self._validate_failed_replay(terminal, failure_reason)

        with self._connection.transaction(), self._connection.cursor() as cursor:
            mode_row = cursor.execute(
                """
                SELECT mode
                FROM groundloop_runtime_mode
                WHERE singleton
                FOR UPDATE
                """
            ).fetchone()
            if mode_row is None or str(mode_row[0]) != "m5_active":
                raise InvalidEventError("typed M5 mutation requires activated mode")

            m4_head_row = cursor.execute(
                """
                SELECT epoch_id
                FROM groundloop_m4_publication_head
                WHERE singleton
                FOR UPDATE
                """
            ).fetchone()
            m5_head_row = cursor.execute(
                """
                SELECT epoch_id
                FROM groundloop_m5_publication_head
                WHERE singleton
                FOR UPDATE
                """
            ).fetchone()
            if m4_head_row is None or m5_head_row is None:
                raise InvalidEventError("typed publication heads are not initialized")
            m4_head = int(m4_head_row[0])
            m5_head = int(m5_head_row[0])
            if m4_head != m5_head:
                raise InvalidEventError("typed publication heads have diverged")

            activation = cursor.execute(
                """
                SELECT activation_id
                FROM groundloop_m5_activation
                WHERE singleton
                FOR UPDATE
                """
            ).fetchone()
            if activation is None:
                raise InvalidEventError("typed M5 runtime lacks an activation record")

            epoch_row = cursor.execute(
                """
                SELECT base.event_id, base.payload_hash, base.revision,
                       base.structural_status, base.semantic_status,
                       base.evaluation_state, runtime.revision,
                       runtime.runtime_state,
                       runtime.expected_previous_published_epoch_id
                FROM groundloop_epoch AS base
                JOIN groundloop_m5_runtime_epoch AS runtime
                  ON runtime.epoch_id = base.epoch_id
                WHERE base.epoch_id = %s
                FOR UPDATE OF base, runtime
                """,
                (epoch_id,),
            ).fetchone()
            if epoch_row is None:
                raise InvalidEventError("epoch is not a typed M5 runtime epoch")
            structural_event_id = str(epoch_row[0])
            payload_hash = str(epoch_row[1]).strip()

            terminal = _load_terminal_result(
                cursor,
                structural_event_id=structural_event_id,
                payload_hash=payload_hash,
            )
            if terminal is not None:
                return self._validate_failed_replay(terminal, failure_reason)

            if int(epoch_row[8]) != m5_head:
                raise InvalidEventError(
                    "typed epoch predecessor no longer equals both publication heads"
                )
            if (
                int(epoch_row[2]) != expected_revision
                or int(epoch_row[6]) != expected_revision
            ):
                raise EventConflictError("stale typed runtime revision")
            if (
                str(epoch_row[3]),
                str(epoch_row[4]),
                str(epoch_row[5]),
                str(epoch_row[7]),
            ) != ("committed", "pending", "pending", "structural_committed"):
                raise InvalidEventError(
                    "M5.3-07 failure requires a revision-1 structural epoch"
                )
            if (
                cursor.execute(
                    "SELECT 1 FROM groundloop_m4_update WHERE epoch_id = %s",
                    (epoch_id,),
                ).fetchone()
                is not None
            ):
                raise InvalidEventError(
                    "M5.3-07 retire failure cannot contain direct M4 work"
                )
            roots = _load_root_declarations_for_failure(cursor, epoch_id=epoch_id)
            runtime_children = cursor.execute(
                """
                SELECT
                    (SELECT count(*) FROM groundloop_m5_semantic_job
                     WHERE epoch_id = %s),
                    (SELECT count(*) FROM groundloop_m5_discovery_scope
                     WHERE epoch_id = %s),
                    (SELECT count(*) FROM groundloop_m5_job_attempt AS attempt
                     JOIN groundloop_m5_semantic_job AS job
                       ON job.logical_job_id = attempt.logical_job_id
                     WHERE job.epoch_id = %s)
                """,
                (epoch_id, epoch_id, epoch_id),
            ).fetchone()
            expected_children = (len(roots), len(roots), 0)
            if runtime_children is None or tuple(map(int, runtime_children)) != (
                expected_children
            ):
                raise InvalidEventError(
                    "M5.3-07 staged failure contains non-root requirement work"
                )

            cursor.execute(
                "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
                (epoch_id, expected_revision),
            )
            _inject(failure_injector, "typed_fail_authorized")
            _cancel_root_declarations(
                cursor,
                structural_event_id=structural_event_id,
                epoch_id=epoch_id,
                resulting_revision=expected_revision + 1,
                roots=roots,
            )
            _inject(failure_injector, "typed_fail_jobs_cancelled")

            cursor.execute(
                """
                UPDATE groundloop_m5_owner_pending_counter
                SET broad_reverse_scope_count = 0,
                    forward_scope_count = 0,
                    verifier_job_count = 0,
                    blocking_failure_count = 0,
                    updated_revision = %s
                WHERE epoch_id = %s
                """,
                (expected_revision + 1, epoch_id),
            )
            cursor.execute(
                """
                UPDATE groundloop_m5_answer_pending_counter
                SET broad_reverse_scope_count = 0,
                    forward_scope_count = 0,
                    verifier_job_count = 0,
                    blocking_failure_count = 0,
                    updated_revision = %s
                WHERE epoch_id = %s
                """,
                (expected_revision + 1, epoch_id),
            )
            _inject(failure_injector, "typed_fail_counters_updated")

            _mark_event_staged_structure_failed(cursor, epoch_id)
            _inject(failure_injector, "typed_fail_structure_failed")

            event_work = M5RuntimeWork(requirement_cancelled_job_count=len(roots))
            _insert_runtime_work(
                cursor,
                structural_event_id=structural_event_id,
                epoch_id=epoch_id,
                work_kind="event",
                work=event_work,
            )
            _insert_runtime_work(
                cursor,
                structural_event_id=structural_event_id,
                epoch_id=epoch_id,
                work_kind="call",
                work=event_work,
            )
            _inject(failure_injector, "typed_fail_work_inserted")

            event_timing = M5RuntimeTiming()
            result = M5EventRunResult.build(
                event_id=structural_event_id,
                payload_hash=payload_hash,
                epoch_id=epoch_id,
                state=M5RunState.FAILED,
                replayed_outcome=None,
                open_receipt=OpenEventReceipt(
                    epoch_id=epoch_id,
                    replayed=False,
                    already_sealed=False,
                ),
                publication_receipt=None,
                event_work=event_work,
                call_work=event_work,
                event_timing=event_timing,
                call_timing=M5RuntimeTiming(),
                combined_deltas=(),
                changed_state_references=(),
                failure_reason=failure_reason,
            )
            assert result.logical_result_hash is not None
            cursor.execute(
                """
                INSERT INTO groundloop_m5_event_result (
                    structural_event_id, payload_hash, epoch_id, outcome,
                    original_open_receipt_binding_hash, publication_id,
                    original_publication_receipt_binding_hash,
                    event_work_kind, event_work_digest,
                    combined_status_delta_set_hash, changed_state_set_hash,
                    failure_reason, logical_result_hash, delta_count,
                    state_reference_count,
                    coordinator_non_db_non_neural_ns, neural_wall_ns,
                    postgres_roundtrip_wall_ns, external_io_wall_ns,
                    end_to_end_wall_ns, postgres_server_execution_ns,
                    postgres_lock_wait_ns, postgres_wal_bytes,
                    postgres_shared_block_reads
                ) VALUES (
                    %s, %s, %s, 'failed', %s, NULL, NULL, 'event', %s,
                    %s, %s, %s, %s, 0, 0,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    structural_event_id,
                    payload_hash,
                    epoch_id,
                    digests.open_event_receipt_binding_digest(
                        epoch_id=epoch_id,
                        replayed=False,
                        already_sealed=False,
                        publication_id=None,
                        already_failed=False,
                        failure_reason=None,
                    ),
                    event_work.work_digest,
                    digests.combined_status_delta_set_digest(()),
                    digests.changed_state_set_digest(()),
                    failure_reason.value,
                    result.logical_result_hash,
                    event_timing.coordinator_non_db_non_neural_ns,
                    event_timing.neural_wall_ns,
                    event_timing.postgres_roundtrip_wall_ns,
                    event_timing.external_io_wall_ns,
                    event_timing.end_to_end_wall_ns,
                    event_timing.postgres_server_execution_ns,
                    event_timing.postgres_lock_wait_ns,
                    event_timing.postgres_wal_bytes,
                    event_timing.postgres_shared_block_reads,
                ),
            )
            _inject(failure_injector, "typed_fail_result_inserted")

            updated_base = cursor.execute(
                """
                UPDATE groundloop_epoch
                SET revision = %s, structural_status = 'failed',
                    semantic_status = 'failed', evaluation_state = 'failed',
                    publication_mode = 'provisional', sealed_at = NULL
                WHERE epoch_id = %s AND revision = %s
                """,
                (expected_revision + 1, epoch_id, expected_revision),
            ).rowcount
            if updated_base != 1:
                raise EventConflictError("stale base epoch revision")
            _inject(failure_injector, "typed_fail_base_updated")

            updated_runtime = cursor.execute(
                """
                UPDATE groundloop_m5_runtime_epoch
                SET runtime_state = 'failed', revision = %s,
                    open_work_count = 0, open_scope_count = 0,
                    blocking_failure_count = 0, terminal_at = now()
                WHERE epoch_id = %s AND revision = %s
                  AND runtime_state = 'structural_committed'
                """,
                (expected_revision + 1, epoch_id, expected_revision),
            ).rowcount
            if updated_runtime != 1:
                raise EventConflictError("stale typed runtime revision")
            _inject(failure_injector, "typed_fail_runtime_updated")
            _inject(failure_injector, "typed_fail_before_constraints")
            _force_deferred_validation(cursor)
            _inject(failure_injector, "typed_fail_after_constraints")
            return result

    @staticmethod
    def _validate_failed_replay(
        result: M5EventRunResult, failure_reason: M5RunFailureReason
    ) -> M5EventRunResult:
        if result.replayed_outcome is M5ReplayedOutcome.SEALED:
            raise InvalidEventError("sealed typed epoch cannot fail")
        if result.failure_reason is not failure_reason:
            raise EventConflictError(
                "failed typed epoch already records another failure reason"
            )
        return result

    def _read_existing_open(
        self,
        cursor: Cursor[Any],
        plan: M5TypedEventPlan,
        *,
        for_update: bool,
    ) -> OpenEventReceipt | None:
        query = """
            SELECT epoch_id, payload_hash
            FROM groundloop_epoch
            WHERE event_id = %s
        """
        if for_update:
            query += " FOR UPDATE"
        epoch_row = cursor.execute(query, (plan.structural_event_id,)).fetchone()
        if epoch_row is None:
            return None
        epoch_id = int(epoch_row[0])
        if str(epoch_row[1]).strip() != plan.payload_hash:
            raise EventConflictError(
                "typed structural event ID was reused with another payload"
            )
        declaration = cursor.execute(
            """
            SELECT runtime.candidate_policy_id,
                   runtime.candidate_policy_manifest_hash,
                   runtime.requirement_registry_snapshot_digest,
                   runtime.active_chunk_snapshot_digest,
                   runtime.expected_previous_published_epoch_id,
                   update.update_kind, result.outcome, result.publication_id,
                   result.failure_reason
            FROM groundloop_m5_runtime_epoch AS runtime
            JOIN groundloop_m5_update AS update USING (epoch_id)
            LEFT JOIN groundloop_m5_event_result AS result
              ON result.epoch_id = runtime.epoch_id
            WHERE runtime.epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone()
        if declaration is None:
            raise EventConflictError(
                "event ID belongs to a non-typed or incomplete declaration"
            )
        expected = (
            plan.candidate_policy_id,
            plan.candidate_policy_manifest_hash,
            plan.requirement_registry_snapshot.requirement_registry_snapshot_digest,
            plan.active_chunk_snapshot.active_chunk_snapshot_digest,
            plan.expected_previous_published_epoch_id,
            _m5_update_kind(plan.event),
        )
        actual = (
            str(declaration[0]),
            str(declaration[1]).strip(),
            str(declaration[2]).strip(),
            str(declaration[3]).strip(),
            int(declaration[4]),
            str(declaration[5]),
        )
        if actual != expected:
            raise EventConflictError("typed event declaration differs on replay")

        outcome = None if declaration[6] is None else str(declaration[6])
        if outcome is None:
            return OpenEventReceipt(
                epoch_id=epoch_id,
                replayed=True,
                already_sealed=False,
            )
        if outcome == "sealed":
            publication_id = str(declaration[7])
            return OpenEventReceipt(
                epoch_id=epoch_id,
                replayed=True,
                already_sealed=True,
                publication_id=publication_id,
            )
        failure_reason = str(declaration[8])
        return OpenEventReceipt(
            epoch_id=epoch_id,
            replayed=True,
            already_sealed=False,
            already_failed=True,
            failure_reason=failure_reason,
        )
