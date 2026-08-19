"""Cursor-local PostgreSQL transitions for M5 requirement roots.

The functions in this module deliberately do not own a transaction.  The
runtime persistence adapter supplies a cursor whose transaction spans the
shared ``groundloop_epoch`` and typed M5 runtime rows.  This keeps root-result
staging and the event-wide closure barrier composable with the coordinator's
final direct/M5 readiness projection.
"""

from __future__ import annotations

import hashlib
import json
import struct
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime, time, timedelta
from enum import Enum
from typing import Any

from psycopg import Cursor
from psycopg.types.json import Jsonb

from groundloop.domain import SubjectKind
from groundloop.errors import EventConflictError, InvalidEventError, ValidationError
from groundloop.m4.contracts import VectorIndexKind
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import (
    M5AttemptArchiveReason,
    M5AttemptDisposition,
    M5AttemptOutput,
    M5AttemptResultArtifact,
    M5CancellationPlan,
    M5CancellationReceipt,
    M5CandidatePolicyManifest,
    M5DiscoveryDirection,
    M5DiscoveryScopeContract,
    M5ExecutionEvidenceDisposition,
    M5ExpiredAttemptReturn,
    M5JobAttempt,
    M5JobCompletion,
    M5JobKind,
    M5JobLease,
    M5JobState,
    M5LogicalJobSpec,
    M5RequirementAdmissionChannel,
    M5RequirementAttemptReturnReceipt,
    M5RequirementChannelHit,
    M5RequirementDiscoveryResult,
    M5RequirementFrontierHead,
    M5RequirementReturnDisposition,
    M5RequirementScopeSelection,
    M5RetrievalTermination,
    M5RootBarrierReceipt,
    M5RuntimeSubgraph,
    M5RuntimeTiming,
    M5RuntimeTimingObservation,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5ScopeState,
    M5TerminalReason,
    SemanticPairKey,
)
from groundloop.m5.runtime.frontier import (
    M5RootBarrierPlan,
    advance_requirement_frontier,
    build_forward_frontier_head,
    build_root_barrier_plan,
    classify_attempt_activity,
    deduplicate_discovery_results,
    validate_directional_hit_order,
)
from groundloop.m5.runtime.postgres_recovery import (
    EventAccountingStart,
    RequirementExecutionAccounting,
    RequirementPostterminalReplay,
    StoredRequirementAttempt,
    build_requirement_execution_accounting,
    finish_cancellation_accounting,
    finish_requirement_execution_accounting,
    finish_root_barrier_accounting,
    load_requirement_dispatch,
    lock_epoch_failure_accounting,
    persist_cancellation_contribution,
    persist_postterminal_requirement_accounting,
    persist_preterminal_late_return_contribution,
    persist_requirement_execution_accounting,
    persist_root_barrier_contribution,
    persist_root_result_stage_contribution,
    read_latest_requirement_attempt,
    read_requirement_attempt,
    read_requirement_execution_replay,
    read_requirement_postterminal_replay,
    require_runtime_recovery_bundle,
    start_event_accounting,
)

RuntimeRootFailureInjector = Callable[[str], None]


@dataclass(frozen=True, slots=True)
class _EpochHeader:
    epoch_id: int
    structural_event_id: str
    revision: int
    structural_status: str
    semantic_status: str
    evaluation_state: str
    runtime_state: str
    candidate_policy_id: str
    candidate_policy_manifest_hash: str
    requirement_registry_snapshot_digest: str
    active_chunk_snapshot_digest: str
    requirement_root_set_hash: str
    open_work_count: int
    open_scope_count: int
    blocking_failure_count: int


@dataclass(frozen=True, slots=True)
class _StoredScope:
    root_job_id: str
    contract: M5DiscoveryScopeContract
    epoch_id: int
    state: M5ScopeState
    staged_result_artifact_hash: str | None
    scope_closure_digest: str | None
    child_set_hash: str | None
    completion_digest: str | None
    created_revision: int
    staged_revision: int | None
    closed_revision: int | None


@dataclass(frozen=True, slots=True)
class _StoredJob:
    spec: M5LogicalJobSpec
    epoch_id: int
    state: M5JobState
    admitted_pair_digest: str | None
    result_artifact_id: str | None
    result_artifact_hash: str | None
    scope_closure_digest: str | None
    child_set_hash: str | None
    archive_reason: M5TerminalReason | None
    completion_digest: str | None
    created_revision: int
    completed_revision: int | None
    cancelled_by_event_id: str | None
    cancelled_by_epoch_id: int | None
    cancellation_reason: M5TerminalReason | None


@dataclass(frozen=True, slots=True)
class _StoredAttempt:
    attempt: M5JobAttempt
    state: str
    attempt_output_digest: str | None


@dataclass(frozen=True, slots=True)
class _StoredAttemptResult:
    artifact: M5AttemptResultArtifact
    output: M5AttemptOutput


@dataclass(frozen=True, slots=True)
class _LateReturnPlan:
    disposition: M5RequirementReturnDisposition
    artifact: M5AttemptResultArtifact
    expired_return: M5ExpiredAttemptReturn | None
    return_kind: str
    return_artifact_digest: str
    terminal_logical_result_hash: str | None


@dataclass(frozen=True, slots=True)
class _StoredExpiredReturn:
    record: M5ExpiredAttemptReturn
    original_lease_token_hash: str
    original_lease_expires_at: datetime
    cancellation_attribution: dict[str, Any]
    execution_evidence_digest: str


@dataclass(frozen=True, slots=True)
class _PendingDelta:
    key: str
    broad_reverse_scope_count: int
    forward_scope_count: int
    verifier_job_count: int

    @property
    def values(self) -> tuple[int, int, int]:
        return (
            self.broad_reverse_scope_count,
            self.forward_scope_count,
            self.verifier_job_count,
        )


@dataclass(frozen=True, slots=True)
class M5EpochFailureRequirementClosure:
    """Locked accounting start and exact requirement closure at failure."""

    accounting_start: EventAccountingStart
    cancellation_plan: M5CancellationPlan | None
    cancellation_work: M5RuntimeWork
    open_work_count: int
    open_scope_count: int
    blocking_failure_count: int
    _authority: _EpochFailurePlanAuthority


def _epoch_failure_transaction_identity(cursor: Cursor[Any]) -> str:
    """Return the exact PostgreSQL transaction owning one failure plan chain."""

    row = cursor.execute("SELECT pg_current_xact_id()::text").fetchone()
    if row is None or type(row[0]) is not str or not row[0]:
        raise ValidationError("epoch failure requires a live PostgreSQL transaction")
    return row[0]


class _EpochFailurePlanAuthority:
    """Identity-and-byte capability binding one plan chain to its live cursor."""

    __slots__ = (
        "applied",
        "cursor",
        "detail_snapshot",
        "details",
        "job_snapshot",
        "jobs",
        "scope_snapshot",
        "scopes",
        "transaction_identity",
    )

    def __init__(self, cursor: Cursor[Any]) -> None:
        self.cursor = cursor
        self.transaction_identity = _epoch_failure_transaction_identity(cursor)
        self.scopes: M5EpochFailureScopeLocks | None = None
        self.scope_snapshot: tuple[Any, ...] | None = None
        self.jobs: M5EpochFailureJobLocks | None = None
        self.job_snapshot: tuple[Any, ...] | None = None
        self.details: M5EpochFailureLockedPlan | None = None
        self.detail_snapshot: tuple[Any, ...] | None = None
        self.applied = False


@dataclass(frozen=True, slots=True)
class M5EpochFailureScopeLocks:
    """Exact tier-8 scope/snapshot image issued under an outer prefix lock."""

    header: _EpochHeader
    expected_revision: int
    terminal_replay: bool
    manifest: M5CandidatePolicyManifest
    scopes: tuple[_StoredScope, ...]
    _authority: _EpochFailurePlanAuthority


@dataclass(frozen=True, slots=True)
class M5EpochFailureJobLocks:
    """Complete C-byte-ordered tier-9 requirement-job image."""

    scope_locks: M5EpochFailureScopeLocks
    jobs: tuple[_StoredJob, ...]
    _authority: _EpochFailurePlanAuthority


@dataclass(frozen=True, slots=True)
class M5EpochFailureLockedPlan:
    """Complete read-only M5 detail plan consumed once by the write phase."""

    job_locks: M5EpochFailureJobLocks
    attempts: tuple[_StoredAttempt, ...]
    selected_jobs: tuple[_StoredJob, ...]
    selected_scopes: tuple[_StoredScope, ...]
    terminal_failed_count: int
    accounting_start: EventAccountingStart
    cancellation_plan: M5CancellationPlan | None
    cancellation_work: M5RuntimeWork
    settled_at: datetime | None
    completions: tuple[tuple[str, M5JobCompletion], ...]
    owner_deltas: tuple[_PendingDelta, ...]
    answer_deltas: tuple[_PendingDelta, ...]
    pending_counter_row_counts: tuple[int, int]
    terminal_replay: bool
    _authority: _EpochFailurePlanAuthority


def _authority_type_tag(value: object) -> str:
    value_type = type(value)
    return f"{value_type.__module__}.{value_type.__qualname__}"


def _authority_primitive_snapshot(value: object) -> tuple[Any, ...]:
    """Freeze one authority value into exact, recursively primitive bytes.

    Frozen dataclasses can still be changed through ``object.__setattr__``.
    Authority comparisons therefore cannot retain aliases to any issued value,
    including nested dataclasses or containers.  Capability fields are captured
    by exact type and identity without traversing their cursor-bearing cycles.
    """

    if value is None:
        return ("none",)
    value_type = type(value)
    type_tag = _authority_type_tag(value)
    if isinstance(value, Enum):
        return ("enum", type_tag, _authority_primitive_snapshot(value.value))
    if value_type is bool:
        return ("bool", value)
    if value_type is int:
        return ("int", value)
    if value_type is str:
        return ("str", value)
    if value_type is bytes:
        assert isinstance(value, bytes)
        return ("bytes", value.hex())
    if value_type is float:
        assert isinstance(value, float)
        return ("float", struct.pack(">d", value).hex())
    if value_type is datetime:
        assert isinstance(value, datetime)
        offset = value.utcoffset()
        return (
            "datetime",
            value.isoformat(timespec="microseconds"),
            value.fold,
            None if offset is None else offset.days,
            None if offset is None else offset.seconds,
            None if offset is None else offset.microseconds,
            None if value.tzinfo is None else _authority_type_tag(value.tzinfo),
        )
    if value_type is date:
        assert isinstance(value, date)
        return ("date", value.isoformat())
    if value_type is time:
        assert isinstance(value, time)
        offset = value.utcoffset()
        return (
            "time",
            value.isoformat(timespec="microseconds"),
            value.fold,
            None if offset is None else offset.days,
            None if offset is None else offset.seconds,
            None if offset is None else offset.microseconds,
            None if value.tzinfo is None else _authority_type_tag(value.tzinfo),
        )
    if value_type is timedelta:
        assert isinstance(value, timedelta)
        return ("timedelta", value.days, value.seconds, value.microseconds)
    if value_type is tuple:
        assert isinstance(value, tuple)
        return (
            "tuple",
            tuple(_authority_primitive_snapshot(item) for item in value),
        )
    if value_type is list:
        assert isinstance(value, list)
        return (
            "list",
            tuple(_authority_primitive_snapshot(item) for item in value),
        )
    if value_type is dict:
        assert isinstance(value, dict)
        pair_snapshots = (
            (
                _authority_primitive_snapshot(key),
                _authority_primitive_snapshot(item),
            )
            for key, item in value.items()
        )
        pairs = tuple(sorted(pair_snapshots, key=lambda pair: repr(pair[0])))
        return ("dict", pairs)
    if value_type in {set, frozenset}:
        assert isinstance(value, (set, frozenset))
        members = tuple(
            sorted(
                (_authority_primitive_snapshot(item) for item in value),
                key=repr,
            )
        )
        return ("set" if value_type is set else "frozenset", members)
    if is_dataclass(value) and not isinstance(value, type):
        field_values: list[tuple[str, tuple[Any, ...]]] = []
        for descriptor in fields(value):
            field_value = getattr(value, descriptor.name)
            if descriptor.name == "_authority":
                field_snapshot = (
                    "capability",
                    _authority_type_tag(field_value),
                    id(field_value),
                )
            else:
                field_snapshot = _authority_primitive_snapshot(field_value)
            field_values.append((descriptor.name, field_snapshot))
        return ("dataclass", type_tag, tuple(field_values))
    raise ValidationError(
        f"authority snapshot contains unsupported exact type {type_tag}"
    )


def _scope_lock_snapshot(locks: M5EpochFailureScopeLocks) -> tuple[Any, ...]:
    if type(locks) is not M5EpochFailureScopeLocks:
        raise ValidationError("epoch-failure scope locks have another type")
    return _authority_primitive_snapshot(locks)


def _job_lock_snapshot(locks: M5EpochFailureJobLocks) -> tuple[Any, ...]:
    if type(locks) is not M5EpochFailureJobLocks:
        raise ValidationError("epoch-failure job locks have another type")
    return _authority_primitive_snapshot(locks)


def _detail_plan_snapshot(plan: M5EpochFailureLockedPlan) -> tuple[Any, ...]:
    if type(plan) is not M5EpochFailureLockedPlan:
        raise ValidationError("epoch-failure detail plan has another type")
    return _authority_primitive_snapshot(plan)


def _inject(injector: RuntimeRootFailureInjector | None, point: str) -> None:
    if injector is not None:
        injector(point)


def _text(value: Any) -> str:
    return str(value).strip()


def _optional_text(value: Any) -> str | None:
    return None if value is None else _text(value)


def _lock_epoch(cursor: Cursor[Any], epoch_id: int) -> _EpochHeader:
    if isinstance(epoch_id, bool) or not isinstance(epoch_id, int) or epoch_id <= 0:
        raise InvalidEventError("typed epoch ID must be positive")
    base = cursor.execute(
        """
        SELECT event_id, revision, structural_status, semantic_status,
               evaluation_state
        FROM groundloop_epoch
        WHERE epoch_id = %s
        FOR UPDATE
        """,
        (epoch_id,),
    ).fetchone()
    if base is None:
        raise InvalidEventError("typed epoch does not exist")
    runtime = cursor.execute(
        """
        SELECT structural_event_id, revision, runtime_state,
               candidate_policy_id, candidate_policy_manifest_hash,
               requirement_registry_snapshot_digest,
               active_chunk_snapshot_digest, requirement_root_set_hash,
               open_work_count, open_scope_count, blocking_failure_count
        FROM groundloop_m5_runtime_epoch
        WHERE epoch_id = %s
        FOR UPDATE
        """,
        (epoch_id,),
    ).fetchone()
    if runtime is None:
        raise InvalidEventError("epoch is not a typed M5 runtime epoch")
    if _text(base[0]) != _text(runtime[0]) or int(base[1]) != int(runtime[1]):
        raise ValidationError("base and typed runtime epoch identities diverged")
    return _EpochHeader(
        epoch_id=epoch_id,
        structural_event_id=_text(runtime[0]),
        revision=int(runtime[1]),
        structural_status=_text(base[2]),
        semantic_status=_text(base[3]),
        evaluation_state=_text(base[4]),
        runtime_state=_text(runtime[2]),
        candidate_policy_id=_text(runtime[3]),
        candidate_policy_manifest_hash=_text(runtime[4]),
        requirement_registry_snapshot_digest=_text(runtime[5]),
        active_chunk_snapshot_digest=_text(runtime[6]),
        requirement_root_set_hash=_text(runtime[7]),
        open_work_count=int(runtime[8]),
        open_scope_count=int(runtime[9]),
        blocking_failure_count=int(runtime[10]),
    )


def _require_pending_epoch(header: _EpochHeader) -> None:
    if (
        header.structural_status,
        header.semantic_status,
        header.evaluation_state,
    ) != ("committed", "pending", "pending") or header.runtime_state not in {
        "structural_committed",
        "semantic_pending",
    }:
        raise InvalidEventError("root transition requires an active pending epoch")


def _authorize(cursor: Cursor[Any], header: _EpochHeader) -> None:
    cursor.execute(
        "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
        (header.epoch_id, header.revision),
    )


def _load_manifest(
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
        raise InvalidEventError("runtime epoch names an unknown candidate policy")
    manifest = M5CandidatePolicyManifest(
        candidate_policy_id=_text(row[0]),
        embedding_model_artifact_id=str(row[2]),
        requirement_role_template_hash=_text(row[3]),
        chunk_role_template_hash=_text(row[4]),
        vector_method_version=str(row[5]),
        vector_index_kind=VectorIndexKind(str(row[6])),
        vector_index_build_config_hash=_text(row[7]),
        vector_search_config_hash=_text(row[8]),
        lexical_method_version=str(row[9]),
        lexical_config_hash=_text(row[10]),
        lexical_postgres_version=str(row[11]),
        lexical_regconfig_identity=str(row[12]),
        fusion_version=str(row[13]),
        reverse_budget_per_inserted_chunk=int(row[14]),
        forward_budget_per_requirement=int(row[15]),
        verifier_execution_spec_hash=_text(row[16]),
        decision_policy_version=str(row[17]),
        lineage_safety_override=bool(row[18]),
    )
    if manifest.manifest_hash != _text(row[1]):
        raise ValidationError("stored candidate-policy manifest identity is corrupt")
    return manifest


_SCOPE_SELECT = """
    SELECT root_job_id, epoch_id, direction, requirement_version_id,
           inserted_chunk_version_id, candidate_policy_id,
           requirement_registry_snapshot_digest,
           active_chunk_snapshot_digest, scope_contract_digest, scope_state,
           staged_result_artifact_hash, scope_closure_digest, child_set_hash,
           completion_digest, created_revision, staged_revision, closed_revision
    FROM groundloop_m5_discovery_scope
"""


def _scope_from_row(row: tuple[Any, ...]) -> _StoredScope:
    return _StoredScope(
        root_job_id=_text(row[0]),
        contract=M5DiscoveryScopeContract(
            direction=M5DiscoveryDirection(str(row[2])),
            requirement_version_id=(None if row[3] is None else str(row[3])),
            inserted_chunk_version_id=(None if row[4] is None else str(row[4])),
            candidate_policy_id=_text(row[5]),
            requirement_registry_snapshot_digest=_text(row[6]),
            active_chunk_snapshot_digest=_text(row[7]),
            scope_contract_digest=_text(row[8]),
        ),
        epoch_id=int(row[1]),
        state=M5ScopeState(str(row[9])),
        staged_result_artifact_hash=_optional_text(row[10]),
        scope_closure_digest=_optional_text(row[11]),
        child_set_hash=_optional_text(row[12]),
        completion_digest=_optional_text(row[13]),
        created_revision=int(row[14]),
        staged_revision=None if row[15] is None else int(row[15]),
        closed_revision=None if row[16] is None else int(row[16]),
    )


def _read_scope(
    cursor: Cursor[Any], *, epoch_id: int, root_job_id: str, for_update: bool
) -> _StoredScope:
    row = cursor.execute(
        _SCOPE_SELECT
        + " WHERE epoch_id = %s AND root_job_id = %s"
        + (" FOR UPDATE" if for_update else ""),
        (epoch_id, root_job_id),
    ).fetchone()
    if row is None:
        raise InvalidEventError("root transition names an unknown discovery scope")
    return _scope_from_row(tuple(row))


_JOB_SELECT = """
    SELECT logical_job_id, epoch_id, structural_event_id, job_kind,
           candidate_policy_id, candidate_policy_manifest_hash,
           parent_job_id, subject_kind, subject_id, chunk_version_id,
           semantic_pair_digest, admitted_pair_digest,
           scope_contract_digest, requirement_registry_snapshot_digest,
           active_chunk_snapshot_digest, role_template_hash,
           execution_spec_hash, expandable, payload_hash, job_state,
           result_artifact_id, result_artifact_hash, scope_closure_digest,
           child_set_hash, archive_reason, completion_digest,
           created_revision, completed_revision, cancelled_by_event_id,
           cancelled_by_epoch_id, cancellation_reason
    FROM groundloop_m5_semantic_job
"""


def _job_from_row(row: tuple[Any, ...]) -> _StoredJob:
    pair_values = row[7:11]
    if all(value is None for value in pair_values):
        pair = None
        pair_digest = None
    elif any(value is None for value in pair_values):
        raise ValidationError("stored semantic job has a partial pair identity")
    else:
        pair = SemanticPairKey(SubjectKind(str(row[7])), str(row[8]), str(row[9]))
        pair_digest = _text(row[10])
    return _StoredJob(
        spec=M5LogicalJobSpec(
            logical_job_id=_text(row[0]),
            structural_event_id=str(row[2]),
            job_kind=M5JobKind(str(row[3])),
            candidate_policy_id=_text(row[4]),
            candidate_policy_manifest_hash=_text(row[5]),
            parent_job_id=_optional_text(row[6]),
            pair=pair,
            semantic_pair_digest=pair_digest,
            scope_contract_digest=_text(row[12]),
            requirement_registry_snapshot_digest=_text(row[13]),
            active_chunk_snapshot_digest=_text(row[14]),
            role_template_hash=_text(row[15]),
            execution_spec_hash=_text(row[16]),
            expandable=bool(row[17]),
            payload_hash=_text(row[18]),
        ),
        epoch_id=int(row[1]),
        state=M5JobState(str(row[19])),
        admitted_pair_digest=_optional_text(row[11]),
        result_artifact_id=_optional_text(row[20]),
        result_artifact_hash=_optional_text(row[21]),
        scope_closure_digest=_optional_text(row[22]),
        child_set_hash=_optional_text(row[23]),
        archive_reason=(None if row[24] is None else M5TerminalReason(str(row[24]))),
        completion_digest=_optional_text(row[25]),
        created_revision=int(row[26]),
        completed_revision=None if row[27] is None else int(row[27]),
        cancelled_by_event_id=_optional_text(row[28]),
        cancelled_by_epoch_id=None if row[29] is None else int(row[29]),
        cancellation_reason=(
            None if row[30] is None else M5TerminalReason(str(row[30]))
        ),
    )


def _read_job(
    cursor: Cursor[Any], *, epoch_id: int, logical_job_id: str, for_update: bool
) -> _StoredJob:
    row = cursor.execute(
        _JOB_SELECT
        + " WHERE epoch_id = %s AND logical_job_id = %s"
        + (" FOR UPDATE" if for_update else ""),
        (epoch_id, logical_job_id),
    ).fetchone()
    if row is None:
        raise InvalidEventError("root transition names an unknown semantic job")
    return _job_from_row(tuple(row))


def _lock_cancellation_scopes(
    cursor: Cursor[Any], *, epoch_id: int, logical_job_ids: tuple[str, ...]
) -> tuple[_StoredScope, ...]:
    """Lock selected root scopes in the frozen tier-8 C order."""

    rows = cursor.execute(
        _SCOPE_SELECT
        + " WHERE epoch_id = %s AND root_job_id = ANY(%s)"
        + ' ORDER BY root_job_id COLLATE "C" FOR UPDATE',
        (epoch_id, list(logical_job_ids)),
    ).fetchall()
    return tuple(_scope_from_row(tuple(row)) for row in rows)


def _lock_cancellation_jobs(
    cursor: Cursor[Any], *, epoch_id: int, logical_job_ids: tuple[str, ...]
) -> tuple[_StoredJob, ...]:
    """Lock the complete selected job set in the frozen tier-9 C order."""

    rows = cursor.execute(
        _JOB_SELECT
        + " WHERE epoch_id = %s AND logical_job_id = ANY(%s)"
        + ' ORDER BY logical_job_id COLLATE "C" FOR UPDATE',
        (epoch_id, list(logical_job_ids)),
    ).fetchall()
    jobs = tuple(_job_from_row(tuple(row)) for row in rows)
    if tuple(job.spec.logical_job_id for job in jobs) != logical_job_ids:
        raise InvalidEventError("cancellation plan names an unknown semantic job")
    return jobs


def _read_attempt(
    cursor: Cursor[Any], *, logical_job_id: str, attempt_id: str, for_update: bool
) -> _StoredAttempt:
    row = cursor.execute(
        """
        SELECT attempt_id, logical_job_id, attempt_ordinal,
               execution_spec_hash, lease_token_hash, attempt_state,
               attempt_output_digest
        FROM groundloop_m5_job_attempt
        WHERE logical_job_id = %s AND attempt_id = %s
        """
        + (" FOR UPDATE" if for_update else ""),
        (logical_job_id, attempt_id),
    ).fetchone()
    if row is None:
        raise InvalidEventError("root transition names an unknown job attempt")
    return _StoredAttempt(
        attempt=M5JobAttempt(
            attempt_id=_text(row[0]),
            logical_job_id=_text(row[1]),
            attempt_ordinal=int(row[2]),
            execution_spec_hash=_text(row[3]),
            lease_token_hash=_text(row[4]),
        ),
        state=str(row[5]),
        attempt_output_digest=_optional_text(row[6]),
    )


def _validate_header_bindings(
    header: _EpochHeader,
    scope: _StoredScope,
    job: _StoredJob,
    manifest: M5CandidatePolicyManifest,
) -> None:
    if scope.epoch_id != header.epoch_id or job.epoch_id != header.epoch_id:
        raise ValidationError("root scope/job belongs to another epoch")
    if (
        header.candidate_policy_id != manifest.candidate_policy_id
        or header.candidate_policy_manifest_hash != manifest.manifest_hash
        or scope.contract.candidate_policy_id != header.candidate_policy_id
        or job.spec.candidate_policy_id != header.candidate_policy_id
        or job.spec.candidate_policy_manifest_hash != manifest.manifest_hash
        or scope.contract.requirement_registry_snapshot_digest
        != header.requirement_registry_snapshot_digest
        or scope.contract.active_chunk_snapshot_digest
        != header.active_chunk_snapshot_digest
        or job.spec.requirement_registry_snapshot_digest
        != header.requirement_registry_snapshot_digest
        or job.spec.active_chunk_snapshot_digest != header.active_chunk_snapshot_digest
        or job.spec.structural_event_id != header.structural_event_id
    ):
        raise ValidationError("root identity is outside its frozen runtime epoch")
    if (
        job.spec.logical_job_id != scope.root_job_id
        or job.spec.scope_contract_digest != scope.contract.scope_contract_digest
    ):
        raise ValidationError("root job and discovery scope identities diverged")
    job.spec.validate_manifest_and_scope(manifest, scope.contract)


def _validate_cancellation_bindings(
    *,
    header: _EpochHeader,
    manifest: M5CandidatePolicyManifest,
    scopes: tuple[_StoredScope, ...],
    jobs: tuple[_StoredJob, ...],
) -> None:
    scope_by_root = {scope.root_job_id: scope for scope in scopes}
    expected_root_ids = {
        job.spec.logical_job_id for job in jobs if job.spec.parent_job_id is None
    }
    if set(scope_by_root) != expected_root_ids:
        raise ValidationError("cancellation root jobs and scopes are not a bijection")
    for job in jobs:
        if (
            job.epoch_id != header.epoch_id
            or job.spec.structural_event_id != header.structural_event_id
            or job.spec.candidate_policy_id != manifest.candidate_policy_id
            or job.spec.candidate_policy_manifest_hash != manifest.manifest_hash
            or job.spec.requirement_registry_snapshot_digest
            != header.requirement_registry_snapshot_digest
            or job.spec.active_chunk_snapshot_digest
            != header.active_chunk_snapshot_digest
        ):
            raise ValidationError("cancellation job is outside its frozen epoch")
        if job.spec.parent_job_id is None:
            _validate_header_bindings(
                header,
                scope_by_root[job.spec.logical_job_id],
                job,
                manifest,
            )
        elif job.spec.job_kind is not M5JobKind.VERIFY_REQUIREMENT_PAIR:
            raise ValidationError("cancellation child is not a verifier job")


def _validate_pair_membership(
    cursor: Cursor[Any], *, scope: M5DiscoveryScopeContract, pair: SemanticPairKey
) -> None:
    scope.validate_pair(pair)
    requirement = cursor.execute(
        """
        SELECT 1
        FROM groundloop_m5_requirement_registry_snapshot_member
        WHERE requirement_registry_snapshot_digest = %s
          AND requirement_version_id = %s
        """,
        (scope.requirement_registry_snapshot_digest, pair.subject_id),
    ).fetchone()
    chunk = cursor.execute(
        """
        SELECT 1
        FROM groundloop_m5_active_chunk_snapshot_member
        WHERE active_chunk_snapshot_digest = %s
          AND chunk_version_id = %s
        """,
        (scope.active_chunk_snapshot_digest, pair.chunk_version_id),
    ).fetchone()
    if requirement is None or chunk is None:
        raise ValidationError("discovery pair is outside its frozen snapshots")


def _validate_discovery_result(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    result: M5RequirementDiscoveryResult,
    scope: _StoredScope,
    job: _StoredJob,
    manifest: M5CandidatePolicyManifest,
    eligible_snapshot_exhausted: bool,
) -> None:
    if not isinstance(eligible_snapshot_exhausted, bool):
        raise ValidationError("snapshot-exhaustion evidence must be boolean")
    if (
        result.root_job_id != job.spec.logical_job_id
        or result.scope_contract_digest != scope.contract.scope_contract_digest
    ):
        raise ValidationError("discovery result belongs to another root or scope")
    validate_directional_hit_order(result.channel_hits, scope.contract.direction)
    for hit in result.channel_hits:
        if (
            hit.epoch_id != epoch_id
            or hit.candidate_policy_id != manifest.candidate_policy_id
        ):
            raise ValidationError("discovery hit has another epoch or policy")
        _validate_pair_membership(cursor, scope=scope.contract, pair=hit.pair)
    for selection in result.selections:
        _validate_pair_membership(cursor, scope=scope.contract, pair=selection.pair)
    # The durable v2 result carries the terminal exhaustion assertion.  There
    # is intentionally no second, lossy inferred-candidate-count field.
    result.validate_policy(
        direction=scope.contract.direction,
        manifest=manifest,
        eligible_snapshot_exhausted=eligible_snapshot_exhausted,
    )


def _load_discovery_result(
    cursor: Cursor[Any], *, root_job_id: str
) -> M5RequirementDiscoveryResult | None:
    header = cursor.execute(
        """
        SELECT result_artifact_id, result_artifact_hash,
               scope_contract_digest, termination, channel_hit_count,
               selection_count, approximate_selection_count,
               mandatory_lineage_only_count
        FROM groundloop_m5_requirement_discovery_result
        WHERE root_job_id = %s
        """,
        (root_job_id,),
    ).fetchone()
    if header is None:
        return None
    hit_rows = cursor.execute(
        """
        SELECT epoch_id, scope_contract_digest, subject_kind, subject_id,
               chunk_version_id, semantic_pair_digest, candidate_policy_id,
               channel, rank, score, channel_artifact_hash, hit_digest
        FROM groundloop_m5_requirement_channel_hit
        WHERE root_job_id = %s
        ORDER BY channel COLLATE "C", rank, semantic_pair_digest COLLATE "C"
        """,
        (root_job_id,),
    ).fetchall()
    hits = tuple(
        M5RequirementChannelHit(
            epoch_id=int(row[0]),
            root_job_id=root_job_id,
            scope_contract_digest=_text(row[1]),
            pair=SemanticPairKey(SubjectKind(str(row[2])), str(row[3]), str(row[4])),
            semantic_pair_digest=_text(row[5]),
            candidate_policy_id=_text(row[6]),
            channel=M5RequirementAdmissionChannel(str(row[7])),
            rank=int(row[8]),
            score=None if row[9] is None else float(row[9]),
            channel_artifact_hash=_text(row[10]),
            hit_digest=_text(row[11]),
        )
        for row in hit_rows
    )
    selection_rows = cursor.execute(
        """
        SELECT scope_contract_digest, subject_kind, subject_id,
               chunk_version_id, semantic_pair_digest, fused_rank, reasons,
               mandatory_lineage, selection_digest
        FROM groundloop_m5_requirement_scope_selection
        WHERE root_job_id = %s
        ORDER BY fused_rank, semantic_pair_digest COLLATE "C"
        """,
        (root_job_id,),
    ).fetchall()
    selections = tuple(
        M5RequirementScopeSelection(
            root_job_id=root_job_id,
            scope_contract_digest=_text(row[0]),
            pair=SemanticPairKey(SubjectKind(str(row[1])), str(row[2]), str(row[3])),
            semantic_pair_digest=_text(row[4]),
            fused_rank=int(row[5]),
            reasons=tuple(M5RequirementAdmissionChannel(value) for value in row[6]),
            mandatory_lineage=bool(row[7]),
            selection_digest=_text(row[8]),
        )
        for row in selection_rows
    )
    if len(hits) != int(header[4]) or len(selections) != int(header[5]):
        raise ValidationError("stored discovery-result cardinality is corrupt")
    return M5RequirementDiscoveryResult(
        root_job_id=root_job_id,
        scope_contract_digest=_text(header[2]),
        termination=M5RetrievalTermination(str(header[3])),
        channel_hits=hits,
        selections=selections,
        approximate_selection_count=int(header[6]),
        mandatory_lineage_only_count=int(header[7]),
        result_artifact_hash=_text(header[1]),
        result_artifact_id=_text(header[0]),
    )


def _load_attempt_result(
    cursor: Cursor[Any], *, attempt_id: str
) -> _StoredAttemptResult | None:
    row = cursor.execute(
        """
        SELECT attempt_result_artifact_id, attempt_result_artifact_hash,
               attempt_output_digest, attempt_id, logical_job_id, job_epoch_id,
               payload_hash, execution_spec_hash, result_artifact_id,
               result_artifact_hash, job_state_at_receipt, job_state_after,
               disposition, activity_snapshot_epoch_id,
               activity_snapshot_revision, epoch_active, chunk_active,
               requirement_active, group_active, archive_reason,
               cancelled_by_event_id, cancelled_by_epoch_id,
               cancellation_reason
        FROM groundloop_m5_attempt_result_artifact
        WHERE attempt_id = %s
        """,
        (attempt_id,),
    ).fetchone()
    if row is None:
        return None
    output = M5AttemptOutput(
        attempt_id=_text(row[3]),
        logical_job_id=_text(row[4]),
        job_epoch_id=int(row[5]),
        payload_hash=_text(row[6]),
        execution_spec_hash=_text(row[7]),
        result_artifact_id=_text(row[8]),
        result_artifact_hash=_text(row[9]),
        attempt_output_digest=_text(row[2]),
    )
    artifact = M5AttemptResultArtifact(
        attempt_result_artifact_id=_text(row[0]),
        attempt_result_artifact_hash=_text(row[1]),
        attempt_output_digest=_text(row[2]),
        attempt_id=_text(row[3]),
        logical_job_id=_text(row[4]),
        job_epoch_id=int(row[5]),
        job_state_at_receipt=M5JobState(str(row[10])),
        job_state_after=M5JobState(str(row[11])),
        disposition=M5AttemptDisposition(str(row[12])),
        activity_snapshot_epoch_id=int(row[13]),
        activity_snapshot_revision=int(row[14]),
        epoch_active=bool(row[15]),
        chunk_active=None if row[16] is None else bool(row[16]),
        requirement_active=None if row[17] is None else bool(row[17]),
        group_active=None if row[18] is None else bool(row[18]),
        archive_reason=(
            None if row[19] is None else M5AttemptArchiveReason(str(row[19]))
        ),
        cancelled_by_event_id=None if row[20] is None else str(row[20]),
        cancelled_by_epoch_id=None if row[21] is None else int(row[21]),
        cancellation_reason=(
            None if row[22] is None else M5TerminalReason(str(row[22]))
        ),
    )
    return _StoredAttemptResult(artifact=artifact, output=output)


def _stored_completion(job: _StoredJob) -> M5JobCompletion:
    if job.completion_digest is None:
        raise ValidationError("terminal root lacks its completion digest")
    return M5JobCompletion(
        logical_job_id=job.spec.logical_job_id,
        payload_hash=job.spec.payload_hash,
        execution_spec_hash=job.spec.execution_spec_hash,
        terminal_state=job.state,
        result_artifact_id=job.result_artifact_id,
        result_artifact_hash=job.result_artifact_hash,
        scope_closure_digest=job.scope_closure_digest,
        child_set_hash=job.child_set_hash,
        archive_reason=job.archive_reason,
        completion_digest=job.completion_digest,
    )


def _lock_activity_snapshot(
    cursor: Cursor[Any],
    *,
    header: _EpochHeader,
    scope: M5DiscoveryScopeContract,
) -> tuple[bool, bool | None, bool | None, bool | None]:
    epoch_active = (
        header.structural_status == "committed"
        and header.semantic_status == "pending"
        and header.evaluation_state == "pending"
        and header.runtime_state in {"structural_committed", "semantic_pending"}
    )
    if scope.direction is M5DiscoveryDirection.FORWARD_REQUIREMENT:
        assert scope.requirement_version_id is not None
        row = cursor.execute(
            """
            SELECT requirement.lifecycle_state,
                   group_version.lifecycle_state,
                   family.lifecycle_state,
                   EXISTS (
                       SELECT 1
                       FROM groundloop_m5_requirement_registry_snapshot_member
                       WHERE requirement_registry_snapshot_digest = %s
                         AND requirement_version_id = %s
                   )
            FROM groundloop_m5_requirement_version AS requirement
            JOIN groundloop_m5_group_version AS group_version
              ON group_version.group_version_id = requirement.group_version_id
            JOIN groundloop_m5_group_family AS family
              ON family.group_family_id = group_version.group_family_id
            WHERE requirement.requirement_version_id = %s
            FOR UPDATE OF family, group_version, requirement
            """,
            (
                scope.requirement_registry_snapshot_digest,
                scope.requirement_version_id,
                scope.requirement_version_id,
            ),
        ).fetchone()
        if row is None:
            raise ValidationError("forward root target has no durable requirement")
        in_snapshot = bool(row[3])
        requirement_active = in_snapshot and str(row[0]) in {"STAGED", "PUBLISHED"}
        group_active = (
            in_snapshot
            and str(row[1])
            in {
                "STAGED",
                "PUBLISHED",
            }
            and str(row[2]) in {"STAGED", "PUBLISHED"}
        )
        return epoch_active, None, requirement_active, group_active

    assert scope.inserted_chunk_version_id is not None
    row = cursor.execute(
        """
        SELECT chunk.valid_to_epoch,
               EXISTS (
                   SELECT 1
                   FROM groundloop_m5_active_chunk_snapshot_member
                   WHERE active_chunk_snapshot_digest = %s
                     AND chunk_version_id = %s
               )
        FROM groundloop_chunk_version AS chunk
        WHERE chunk.chunk_version_id = %s
        FOR UPDATE OF chunk
        """,
        (
            scope.active_chunk_snapshot_digest,
            scope.inserted_chunk_version_id,
            scope.inserted_chunk_version_id,
        ),
    ).fetchone()
    if row is None:
        raise ValidationError("reverse root target has no durable chunk")
    chunk_active = bool(row[1]) and row[0] is None
    return epoch_active, chunk_active, None, None


def _lock_snapshot_headers(cursor: Cursor[Any], *, header: _EpochHeader) -> None:
    requirement = cursor.execute(
        """
        SELECT requirement_count
        FROM groundloop_m5_requirement_registry_snapshot
        WHERE requirement_registry_snapshot_digest = %s
        FOR SHARE
        """,
        (header.requirement_registry_snapshot_digest,),
    ).fetchone()
    chunk = cursor.execute(
        """
        SELECT chunk_count
        FROM groundloop_m5_active_chunk_snapshot
        WHERE active_chunk_snapshot_digest = %s
        FOR SHARE
        """,
        (header.active_chunk_snapshot_digest,),
    ).fetchone()
    if requirement is None or chunk is None:
        raise ValidationError("runtime epoch lacks its frozen snapshot headers")


def _validate_executable_lease(
    *, lease: M5JobLease, job: M5LogicalJobSpec, expected_revision: int
) -> M5JobAttempt:
    if (
        not isinstance(lease, M5JobLease)
        or not lease.should_execute
        or lease.exact_replay
        or lease.attempt is None
    ):
        raise ValidationError("root staging requires an executable M5 lease")
    if lease.logical_job_id != job.logical_job_id:
        raise EventConflictError("lease and root job identities disagree")
    if lease.resulting_revision > expected_revision:
        raise EventConflictError("lease revision is newer than expected revision")
    return lease.attempt


def _same_attempt_identity(stored: M5JobAttempt, supplied: M5JobAttempt) -> bool:
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


def _current_terminal_logical_result_hash(
    cursor: Cursor[Any], *, header: _EpochHeader
) -> str | None:
    row = cursor.execute(
        """
        SELECT logical_result_hash
        FROM groundloop_m5_event_result
        WHERE epoch_id = %s
        """,
        (header.epoch_id,),
    ).fetchone()
    if header.runtime_state in {"sealed", "failed"}:
        if row is None:
            raise ValidationError("terminal M5 epoch lacks its immutable result")
        return _text(row[0])
    if row is not None:
        raise ValidationError("nonterminal M5 epoch has an immutable result")
    return None


def _build_late_return_plan(
    *,
    header: _EpochHeader,
    job: _StoredJob,
    attempt: StoredRequirementAttempt,
    output: M5AttemptOutput,
    activity: tuple[bool, bool | None, bool | None, bool | None],
    terminal_logical_result_hash: str | None,
) -> _LateReturnPlan | None:
    postterminal = terminal_logical_result_hash is not None
    cancellation = job.state is M5JobState.CANCELLED
    cancellation_values = (
        job.cancelled_by_event_id if cancellation else None,
        job.cancelled_by_epoch_id if cancellation else None,
        job.cancellation_reason if cancellation else None,
    )
    if cancellation and any(value is None for value in cancellation_values):
        raise ValidationError("cancelled late return lacks full attribution")

    if attempt.state == "expired":
        if job.state is not M5JobState.RUNNING and not postterminal:
            raise EventConflictError(
                "nonrunning successor requires terminal event before expired audit"
            )
        receipt_state = job.state if postterminal else M5JobState.RUNNING
        if postterminal and not receipt_state.terminal:
            raise EventConflictError("postterminal expired return lacks terminal job")
        artifact = M5AttemptResultArtifact.build(
            attempt_output=output,
            job_state_at_receipt=receipt_state,
            job_state_after=receipt_state,
            disposition=M5AttemptDisposition.TERMINAL_AUDIT_ONLY,
            activity_snapshot_epoch_id=header.epoch_id,
            activity_snapshot_revision=header.revision,
            epoch_active=activity[0],
            chunk_active=activity[1],
            requirement_active=activity[2],
            group_active=activity[3],
            archive_reason=M5AttemptArchiveReason.ATTEMPT_EXPIRED,
            cancelled_by_event_id=cancellation_values[0],
            cancelled_by_epoch_id=cancellation_values[1],
            cancellation_reason=cancellation_values[2],
        )
        expired = M5ExpiredAttemptReturn.build(
            subgraph=M5RuntimeSubgraph.REQUIREMENT,
            epoch_id=header.epoch_id,
            attempt_id=attempt.attempt.attempt_id,
            logical_job_id=job.spec.logical_job_id,
            worker_output_digest=output.attempt_output_digest,
            worker_artifact_hash=output.result_artifact_hash,
            activity_snapshot_epoch_id=header.epoch_id,
            activity_snapshot_revision=header.revision,
            received_after_terminal=postterminal,
        )
        return _LateReturnPlan(
            disposition=(
                M5RequirementReturnDisposition.EXPIRED_POSTTERMINAL
                if postterminal
                else M5RequirementReturnDisposition.EXPIRED_PRETERMINAL
            ),
            artifact=artifact,
            expired_return=expired,
            return_kind="expired_return",
            return_artifact_digest=expired.expired_return_digest,
            terminal_logical_result_hash=terminal_logical_result_hash,
        )

    if attempt.state != "dispatched" or job.state is not M5JobState.CANCELLED:
        return None
    if not postterminal and job.cancellation_reason is M5TerminalReason.EPOCH_FAILED:
        raise EventConflictError(
            "epoch-failure cancellation must terminalize before return audit"
        )
    archive_reason = classify_attempt_activity(
        epoch_active=activity[0],
        chunk_active=activity[1],
        requirement_active=activity[2],
        group_active=activity[3],
        job_already_terminal=True,
    )
    if archive_reason is None:
        raise ValidationError("terminal audit lacks its activity archive reason")
    artifact = M5AttemptResultArtifact.build(
        attempt_output=output,
        job_state_at_receipt=M5JobState.CANCELLED,
        job_state_after=M5JobState.CANCELLED,
        disposition=M5AttemptDisposition.TERMINAL_AUDIT_ONLY,
        activity_snapshot_epoch_id=header.epoch_id,
        activity_snapshot_revision=header.revision,
        epoch_active=activity[0],
        chunk_active=activity[1],
        requirement_active=activity[2],
        group_active=activity[3],
        archive_reason=archive_reason,
        cancelled_by_event_id=cancellation_values[0],
        cancelled_by_epoch_id=cancellation_values[1],
        cancellation_reason=cancellation_values[2],
    )
    return _LateReturnPlan(
        disposition=(
            M5RequirementReturnDisposition.TERMINAL_AUDIT_POSTTERMINAL
            if postterminal
            else M5RequirementReturnDisposition.TERMINAL_AUDIT_PRETERMINAL
        ),
        artifact=artifact,
        expired_return=None,
        return_kind="terminal_audit_only",
        return_artifact_digest=artifact.attempt_result_artifact_hash,
        terminal_logical_result_hash=terminal_logical_result_hash,
    )


def _insert_late_attempt_artifact(
    cursor: Cursor[Any], *, plan: _LateReturnPlan, output: M5AttemptOutput
) -> None:
    artifact = plan.artifact
    cursor.execute(
        """
        INSERT INTO groundloop_m5_attempt_result_artifact (
            attempt_result_artifact_id, attempt_result_artifact_hash,
            attempt_output_digest, attempt_id, logical_job_id, job_epoch_id,
            payload_hash, execution_spec_hash, result_artifact_id,
            result_artifact_hash, job_state_at_receipt, job_state_after,
            disposition, activity_snapshot_epoch_id,
            activity_snapshot_revision, epoch_active, chunk_active,
            requirement_active, group_active, archive_reason,
            cancelled_by_event_id, cancelled_by_epoch_id, cancellation_reason
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            artifact.attempt_result_artifact_id,
            artifact.attempt_result_artifact_hash,
            artifact.attempt_output_digest,
            artifact.attempt_id,
            artifact.logical_job_id,
            artifact.job_epoch_id,
            output.payload_hash,
            output.execution_spec_hash,
            output.result_artifact_id,
            output.result_artifact_hash,
            artifact.job_state_at_receipt.value,
            artifact.job_state_after.value,
            artifact.disposition.value,
            artifact.activity_snapshot_epoch_id,
            artifact.activity_snapshot_revision,
            artifact.epoch_active,
            artifact.chunk_active,
            artifact.requirement_active,
            artifact.group_active,
            None if artifact.archive_reason is None else artifact.archive_reason.value,
            artifact.cancelled_by_event_id,
            artifact.cancelled_by_epoch_id,
            (
                None
                if artifact.cancellation_reason is None
                else artifact.cancellation_reason.value
            ),
        ),
    )


def _insert_expired_return_sidecar(
    cursor: Cursor[Any],
    *,
    plan: _LateReturnPlan,
    attempt: M5JobAttempt,
    execution_evidence_digest: str,
) -> None:
    expired = plan.expired_return
    if expired is None or attempt.lease_expires_at is None:
        raise ValidationError("expired return lacks its original lease binding")
    artifact = plan.artifact
    cursor.execute(
        """
        INSERT INTO groundloop_m5_expired_attempt_return (
            epoch_id, subgraph, attempt_id, logical_job_id,
            original_lease_token_hash, original_lease_expires_at,
            worker_output_digest, worker_artifact_hash,
            activity_snapshot_epoch_id, activity_snapshot_revision,
            cancellation_attribution, execution_evidence_digest,
            received_after_terminal, expired_return_digest
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            expired.epoch_id,
            expired.subgraph.value,
            expired.attempt_id,
            expired.logical_job_id,
            attempt.lease_token_hash,
            attempt.lease_expires_at,
            expired.worker_output_digest,
            expired.worker_artifact_hash,
            expired.activity_snapshot_epoch_id,
            expired.activity_snapshot_revision,
            Jsonb(
                {
                    "cancelled_by_event_id": artifact.cancelled_by_event_id,
                    "cancelled_by_epoch_id": artifact.cancelled_by_epoch_id,
                    "cancellation_reason": (
                        None
                        if artifact.cancellation_reason is None
                        else artifact.cancellation_reason.value
                    ),
                }
            ),
            execution_evidence_digest,
            expired.received_after_terminal,
            expired.expired_return_digest,
        ),
    )


def _derive_root_result_stage_work(
    cursor: Cursor[Any], *, epoch_id: int, logical_job_id: str, attempt_id: str
) -> M5RuntimeWork:
    """Measure the canonical final rows physically owned by root staging."""

    rows = cursor.execute(
        """
        SELECT serialized FROM (
            SELECT to_jsonb(item)::text AS serialized
            FROM groundloop_m5_job_attempt AS item
            WHERE item.attempt_id = %s
            UNION ALL
            SELECT to_jsonb(item)::text
            FROM groundloop_m5_attempt_result_artifact AS item
            WHERE item.attempt_id = %s
            UNION ALL
            SELECT to_jsonb(item)::text
            FROM groundloop_m5_requirement_channel_hit AS item
            WHERE item.root_job_id = %s
            UNION ALL
            SELECT to_jsonb(item)::text
            FROM groundloop_m5_requirement_scope_selection AS item
            WHERE item.root_job_id = %s
            UNION ALL
            SELECT to_jsonb(item)::text
            FROM groundloop_m5_requirement_discovery_result AS item
            WHERE item.root_job_id = %s
            UNION ALL
            SELECT to_jsonb(item)::text
            FROM groundloop_m5_discovery_scope AS item
            WHERE item.epoch_id = %s AND item.root_job_id = %s
        ) AS root_rows
        ORDER BY serialized COLLATE "C"
        """,
        (
            attempt_id,
            attempt_id,
            logical_job_id,
            logical_job_id,
            logical_job_id,
            epoch_id,
            logical_job_id,
        ),
    ).fetchall()
    if len(rows) < 4:
        raise ValidationError("root result staging lacks its persistence rows")
    byte_count = 0
    stage_hasher = hashlib.sha256()
    for row in rows:
        encoded = str(row[0]).encode("utf-8")
        frame = len(encoded).to_bytes(8, byteorder="big", signed=False)
        stage_hasher.update(frame)
        stage_hasher.update(encoded)
        byte_count += len(frame) + len(encoded)
    if len(stage_hasher.digest()) != hashlib.sha256().digest_size:
        raise AssertionError("SHA-256 root-result instrumentation drift")
    return M5RuntimeWork(bytes_hashed=byte_count, bytes_serialized=byte_count)


def _derive_late_return_work(
    cursor: Cursor[Any], *, attempt_id: str, expired: bool
) -> M5RuntimeWork:
    rows = cursor.execute(
        """
        SELECT serialized FROM (
            SELECT to_jsonb(item)::text AS serialized
            FROM groundloop_m5_attempt_result_artifact AS item
            WHERE item.attempt_id = %s
            UNION ALL
            SELECT to_jsonb(item)::text
            FROM groundloop_m5_expired_attempt_return AS item
            WHERE item.subgraph = 'requirement' AND item.attempt_id = %s
        ) AS late_rows
        ORDER BY serialized COLLATE "C"
        """,
        (attempt_id, attempt_id),
    ).fetchall()
    if len(rows) != 1 + int(expired):
        raise ValidationError("late return lacks its exact immutable rows")
    byte_count = 0
    late_hasher = hashlib.sha256()
    for row in rows:
        encoded = str(row[0]).encode("utf-8")
        frame = len(encoded).to_bytes(8, byteorder="big", signed=False)
        late_hasher.update(frame)
        late_hasher.update(encoded)
        byte_count += len(frame) + len(encoded)
    if len(late_hasher.digest()) != hashlib.sha256().digest_size:
        raise AssertionError("SHA-256 late-return instrumentation drift")
    return M5RuntimeWork(
        requirement_late_attempt_artifact_count=1,
        bytes_hashed=byte_count,
        bytes_serialized=byte_count,
    )


def _derive_root_barrier_work(
    *,
    plan: M5RootBarrierPlan,
    child_jobs: tuple[M5LogicalJobSpec, ...],
) -> M5RuntimeWork:
    """Measure the barrier's immutable canonical persistence projection."""

    payloads: list[tuple[Any, ...]] = [
        (
            "plan",
            plan.structural_event_id,
            plan.requirement_root_set_hash,
            plan.barrier_completion_hash,
        )
    ]
    for pair in plan.admitted_pairs:
        payloads.append(
            (
                "admitted_pair",
                pair.epoch_id,
                pair.pair.subject_kind.value,
                pair.pair.subject_id,
                pair.pair.chunk_version_id,
                pair.semantic_pair_digest,
                pair.candidate_policy_id,
                pair.owner_root_job_id,
                tuple(reason.value for reason in pair.reasons),
                pair.mandatory_lineage,
                pair.admitted_pair_digest,
            )
        )
        payloads.extend(
            (
                "admitted_source",
                pair.admitted_pair_digest,
                source.root_job_id,
                source.scope_contract_digest,
                source.selection_digest,
            )
            for source in pair.sources
        )
    payloads.extend(
        (
            "root_closure",
            closure.root_job_id,
            closure.scope_contract_digest,
            closure.semantic_pair_digests,
            closure.scope_closure_digest,
            closure.child_job_ids,
            closure.child_set_hash,
        )
        for closure in plan.root_closures
    )
    for child in child_jobs:
        assert child.pair is not None
        payloads.append(
            (
                "child_spec",
                child.logical_job_id,
                child.structural_event_id,
                child.job_kind.value,
                child.candidate_policy_id,
                child.candidate_policy_manifest_hash,
                child.parent_job_id,
                child.pair.subject_kind.value,
                child.pair.subject_id,
                child.pair.chunk_version_id,
                child.semantic_pair_digest,
                child.scope_contract_digest,
                child.requirement_registry_snapshot_digest,
                child.active_chunk_snapshot_digest,
                child.role_template_hash,
                child.execution_spec_hash,
                child.expandable,
                child.payload_hash,
            )
        )
    rows = sorted(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        for payload in payloads
    )
    byte_count = 0
    barrier_hasher = hashlib.sha256()
    for encoded in rows:
        frame = len(encoded).to_bytes(8, byteorder="big", signed=False)
        barrier_hasher.update(frame)
        barrier_hasher.update(encoded)
        byte_count += len(frame) + len(encoded)
    if len(barrier_hasher.digest()) != hashlib.sha256().digest_size:
        raise AssertionError("SHA-256 root-barrier instrumentation drift")
    return M5RuntimeWork(
        requirement_admitted_pair_count=len(plan.admitted_pairs),
        bytes_hashed=byte_count,
        bytes_serialized=byte_count,
    )


def _load_expired_return(
    cursor: Cursor[Any], *, epoch_id: int, attempt_id: str
) -> _StoredExpiredReturn | None:
    row = cursor.execute(
        """
        SELECT logical_job_id, original_lease_token_hash,
               original_lease_expires_at, worker_output_digest,
               worker_artifact_hash, activity_snapshot_epoch_id,
               activity_snapshot_revision, cancellation_attribution,
               execution_evidence_digest, received_after_terminal,
               expired_return_digest
        FROM groundloop_m5_expired_attempt_return
        WHERE epoch_id = %s AND subgraph = 'requirement' AND attempt_id = %s
        """,
        (epoch_id, attempt_id),
    ).fetchone()
    if row is None:
        return None
    if not isinstance(row[2], datetime) or not isinstance(row[7], dict):
        raise ValidationError("stored expired-return sidecar has invalid wire types")
    return _StoredExpiredReturn(
        record=M5ExpiredAttemptReturn(
            subgraph=M5RuntimeSubgraph.REQUIREMENT,
            epoch_id=epoch_id,
            attempt_id=attempt_id,
            logical_job_id=_text(row[0]),
            worker_output_digest=_text(row[3]),
            worker_artifact_hash=_text(row[4]),
            activity_snapshot_epoch_id=int(row[5]),
            activity_snapshot_revision=int(row[6]),
            received_after_terminal=bool(row[9]),
            expired_return_digest=_text(row[10]),
        ),
        original_lease_token_hash=_text(row[1]),
        original_lease_expires_at=row[2],
        cancellation_attribution=dict(row[7]),
        execution_evidence_digest=_text(row[8]),
    )


def _validate_expired_replay_sidecar(
    *,
    stored: _StoredExpiredReturn,
    artifact: M5AttemptResultArtifact,
    attempt: M5JobAttempt,
    output: M5AttemptOutput,
    accounting: RequirementExecutionAccounting,
    postterminal: bool,
) -> None:
    expected_cancellation = {
        "cancelled_by_event_id": artifact.cancelled_by_event_id,
        "cancelled_by_epoch_id": artifact.cancelled_by_epoch_id,
        "cancellation_reason": (
            None
            if artifact.cancellation_reason is None
            else artifact.cancellation_reason.value
        ),
    }
    if (
        stored.record.logical_job_id != output.logical_job_id
        or stored.record.worker_output_digest != output.attempt_output_digest
        or stored.record.worker_artifact_hash != output.result_artifact_hash
        or stored.record.activity_snapshot_epoch_id
        != artifact.activity_snapshot_epoch_id
        or stored.record.activity_snapshot_revision
        != artifact.activity_snapshot_revision
        or stored.record.received_after_terminal is not postterminal
        or stored.original_lease_token_hash != attempt.lease_token_hash
        or stored.original_lease_expires_at != attempt.lease_expires_at
        or stored.cancellation_attribution != expected_cancellation
        or stored.execution_evidence_digest != accounting.evidence.evidence_digest
    ):
        raise EventConflictError("expired-return replay changed immutable sidecar")


def _validate_late_replay(
    cursor: Cursor[Any],
    *,
    header: _EpochHeader,
    lease: M5JobLease,
    supplied_job: M5LogicalJobSpec,
    supplied_output: M5AttemptOutput,
    job: _StoredJob,
    attempt: StoredRequirementAttempt,
    accounting: RequirementExecutionAccounting,
    event_replay_revision: int | None,
    postterminal_replay: RequirementPostterminalReplay | None,
) -> M5RequirementAttemptReturnReceipt | None:
    stored = _load_attempt_result(cursor, attempt_id=attempt.attempt.attempt_id)
    if stored is None or (
        stored.artifact.disposition is not M5AttemptDisposition.TERMINAL_AUDIT_ONLY
    ):
        if postterminal_replay is not None:
            raise ValidationError("postterminal evidence lacks its audit artifact")
        return None
    if event_replay_revision is None and postterminal_replay is None:
        raise ValidationError("late-return artifact lacks execution evidence")
    if (
        supplied_job != job.spec
        or lease.attempt is None
        or not _same_attempt_identity(attempt.attempt, lease.attempt)
        or stored.output != supplied_output
        or stored.artifact.logical_job_id != job.spec.logical_job_id
    ):
        raise EventConflictError("late-return replay changed immutable output")
    stored.artifact.validate_job_shape(job.spec.job_kind)
    expired = stored.artifact.archive_reason is M5AttemptArchiveReason.ATTEMPT_EXPIRED
    expired_row = _load_expired_return(
        cursor, epoch_id=header.epoch_id, attempt_id=attempt.attempt.attempt_id
    )
    if expired != (expired_row is not None):
        raise ValidationError("late-return artifact and expired sidecar diverged")
    if expired_row is not None:
        _validate_expired_replay_sidecar(
            stored=expired_row,
            artifact=stored.artifact,
            attempt=attempt.attempt,
            output=supplied_output,
            accounting=accounting,
            postterminal=postterminal_replay is not None,
        )
        return_digest = expired_row.record.expired_return_digest
    else:
        return_digest = stored.artifact.attempt_result_artifact_hash

    work_names = M5RuntimeWork.counter_names()
    contribution = cursor.execute(
        f"""
        SELECT {", ".join(work_names)}, work_digest, source_identity_hash,
               contribution_key_digest, applied_revision
        FROM groundloop_m5_runtime_work_contribution
        WHERE epoch_id = %s AND contribution_kind = 'preterminal_late_return'
          AND source_id = %s
        """,
        (header.epoch_id, attempt.attempt.attempt_id),
    ).fetchone()
    if event_replay_revision is not None:
        if postterminal_replay is not None or contribution is None:
            raise ValidationError("late return has conflicting accounting branches")
        values = tuple(contribution)
        work_end = len(work_names)
        stored_work = M5RuntimeWork(
            **dict(zip(work_names, map(int, values[:work_end]), strict=True)),
            work_digest=_text(values[work_end]),
        )
        expected_work = _derive_late_return_work(
            cursor, attempt_id=attempt.attempt.attempt_id, expired=expired
        )
        expected_key = digests.runtime_work_contribution_key_digest(
            epoch_id=header.epoch_id,
            contribution_kind=M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN,
            source_id=attempt.attempt.attempt_id,
        )
        if (
            stored_work != expected_work
            or _text(values[work_end + 1]) != return_digest
            or _text(values[work_end + 2]) != expected_key
            or int(values[work_end + 3]) != event_replay_revision
        ):
            raise EventConflictError("late-return replay changed immutable accounting")
        disposition = (
            M5RequirementReturnDisposition.EXPIRED_PRETERMINAL
            if expired
            else M5RequirementReturnDisposition.TERMINAL_AUDIT_PRETERMINAL
        )
    else:
        if contribution is not None or postterminal_replay is None:
            raise ValidationError("postterminal return has event accounting")
        event_rows = cursor.execute(
            """
            SELECT
              (SELECT count(*) FROM groundloop_m5_runtime_work_contribution
               WHERE epoch_id = %s AND source_id = %s
                 AND contribution_kind IN (
                     'm5_attempt_execution', 'preterminal_late_return')),
              (SELECT count(*) FROM groundloop_m5_runtime_timing_contribution
               WHERE epoch_id = %s AND subgraph = 'requirement'
                 AND attempt_id = %s)
            """,
            (
                header.epoch_id,
                attempt.attempt.attempt_id,
                header.epoch_id,
                attempt.attempt.attempt_id,
            ),
        ).fetchone()
        terminal_hash = _current_terminal_logical_result_hash(cursor, header=header)
        if (
            event_rows != (0, 0)
            or postterminal_replay.return_kind
            != ("expired_return" if expired else "terminal_audit_only")
            or postterminal_replay.return_artifact_digest != return_digest
            or postterminal_replay.terminal_logical_result_hash != terminal_hash
        ):
            raise EventConflictError("postterminal replay changed immutable closure")
        disposition = (
            M5RequirementReturnDisposition.EXPIRED_POSTTERMINAL
            if expired
            else M5RequirementReturnDisposition.TERMINAL_AUDIT_POSTTERMINAL
        )
    return M5RequirementAttemptReturnReceipt(
        disposition=disposition,
        logical_job_id=job.spec.logical_job_id,
        attempt_id=attempt.attempt.attempt_id,
        resulting_revision=header.revision,
        exact_replay=True,
        execution_evidence_digest=accounting.evidence.evidence_digest,
        return_artifact_digest=return_digest,
        current_terminal_logical_result_hash=(
            _current_terminal_logical_result_hash(cursor, header=header)
        ),
        transition_anchor=None,
    )


def _persist_late_return(
    cursor: Cursor[Any],
    *,
    header: _EpochHeader,
    plan: _LateReturnPlan,
    attempt: StoredRequirementAttempt,
    output: M5AttemptOutput,
    accounting: RequirementExecutionAccounting,
    failure_injector: RuntimeRootFailureInjector | None,
) -> M5RequirementAttemptReturnReceipt:
    terminal_logical_result_hash = plan.terminal_logical_result_hash
    if terminal_logical_result_hash is not None:
        _insert_late_attempt_artifact(cursor, plan=plan, output=output)
        _inject(failure_injector, "late_return_artifact_inserted")
        if plan.expired_return is not None:
            _insert_expired_return_sidecar(
                cursor,
                plan=plan,
                attempt=attempt.attempt,
                execution_evidence_digest=accounting.evidence.evidence_digest,
            )
            _inject(failure_injector, "late_return_expired_sidecar_inserted")
        persist_postterminal_requirement_accounting(
            cursor,
            accounting=accounting,
            return_kind=plan.return_kind,
            return_artifact_digest=plan.return_artifact_digest,
            terminal_logical_result_hash=terminal_logical_result_hash,
            failure_injector=failure_injector,
        )
        transition_anchor = None
    else:
        if accounting.anchor.anchor_revision != header.revision:
            raise ValidationError("preterminal late-return anchor revision drifted")
        _authorize(cursor, header)
        accounting_start = start_event_accounting(
            cursor, epoch_id=header.epoch_id, expected_revision=header.revision
        )
        _insert_late_attempt_artifact(cursor, plan=plan, output=output)
        _inject(failure_injector, "late_return_artifact_inserted")
        if plan.expired_return is not None:
            _insert_expired_return_sidecar(
                cursor,
                plan=plan,
                attempt=attempt.attempt,
                execution_evidence_digest=accounting.evidence.evidence_digest,
            )
            _inject(failure_injector, "late_return_expired_sidecar_inserted")
        late_work = _derive_late_return_work(
            cursor,
            attempt_id=attempt.attempt.attempt_id,
            expired=plan.expired_return is not None,
        )
        persist_requirement_execution_accounting(cursor, accounting=accounting)
        persist_preterminal_late_return_contribution(
            cursor,
            accounting=accounting,
            return_artifact_digest=plan.return_artifact_digest,
            late_work=late_work,
        )
        finish_requirement_execution_accounting(
            cursor,
            epoch_id=header.epoch_id,
            expected_revision=header.revision,
            start=accounting_start,
            accounting=accounting,
            additional_work=late_work,
            same_revision=True,
        )
        transition_anchor = accounting.anchor
    _inject(failure_injector, "late_return_accounting_inserted")
    _force_deferred_validation(cursor)
    _inject(failure_injector, "late_return_constraints_validated")
    receipt = M5RequirementAttemptReturnReceipt(
        disposition=plan.disposition,
        logical_job_id=attempt.attempt.logical_job_id,
        attempt_id=attempt.attempt.attempt_id,
        resulting_revision=header.revision,
        exact_replay=False,
        execution_evidence_digest=accounting.evidence.evidence_digest,
        return_artifact_digest=plan.return_artifact_digest,
        current_terminal_logical_result_hash=plan.terminal_logical_result_hash,
        transition_anchor=transition_anchor,
    )
    if transition_anchor is not None:
        receipt.validate_anchor_context(
            epoch_id=header.epoch_id,
            expected_kind=M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN,
        )
    return receipt


def _validate_stage_replay(
    cursor: Cursor[Any],
    *,
    header: _EpochHeader,
    lease: M5JobLease,
    supplied_job: M5LogicalJobSpec,
    supplied_result: M5RequirementDiscoveryResult,
    supplied_output: M5AttemptOutput,
    scope: _StoredScope,
    job: _StoredJob,
    attempt: StoredRequirementAttempt,
    manifest: M5CandidatePolicyManifest,
    eligible_snapshot_exhausted: bool,
    accounting: RequirementExecutionAccounting,
    replay_revision: int | None,
) -> M5RequirementAttemptReturnReceipt | None:
    stored_artifact = _load_attempt_result(
        cursor, attempt_id=attempt.attempt.attempt_id
    )
    work_names = M5RuntimeWork.counter_names()
    contribution = cursor.execute(
        f"""
        SELECT {", ".join(work_names)}, work_digest, source_identity_hash,
               contribution_key_digest, applied_revision
        FROM groundloop_m5_runtime_work_contribution
        WHERE epoch_id = %s AND contribution_kind = 'root_result_stage'
          AND source_id = %s
        """,
        (header.epoch_id, attempt.attempt.attempt_id),
    ).fetchone()
    if replay_revision is None and stored_artifact is None and contribution is None:
        return None
    stored_result = _load_discovery_result(cursor, root_job_id=job.spec.logical_job_id)
    if (
        replay_revision is None
        or stored_artifact is None
        or stored_result is None
        or contribution is None
    ):
        raise EventConflictError("root staging is only partially durable")
    if (
        supplied_job != job.spec
        or lease.attempt is None
        or not _same_attempt_identity(attempt.attempt, lease.attempt)
        or supplied_output != stored_artifact.output
        or supplied_result != stored_result
        or stored_artifact.artifact.disposition
        is not M5AttemptDisposition.ROOT_RESULT_STAGED
        or stored_artifact.artifact.logical_job_id != job.spec.logical_job_id
        or attempt.state != "completed"
        or attempt.attempt_output_digest != supplied_output.attempt_output_digest
        or attempt.attempt.attempt_work_digest
        != accounting.evidence.attempt_work.work_digest
        or scope.staged_result_artifact_hash != supplied_result.result_artifact_hash
        or scope.state
        not in {
            M5ScopeState.RESULT_STAGED,
            M5ScopeState.CLOSED_ACTIVE,
            M5ScopeState.CLOSED_INACTIVE,
            M5ScopeState.TERMINAL_FAILED,
            M5ScopeState.CANCELLED,
        }
        or job.state
        not in {
            M5JobState.RUNNING,
            M5JobState.COMPLETED_ACTIVE,
            M5JobState.COMPLETED_INACTIVE,
            M5JobState.TERMINAL_FAILED,
            M5JobState.CANCELLED,
        }
    ):
        raise EventConflictError("attempt ID already records another root result")
    _validate_header_bindings(header, scope, job, manifest)
    _validate_discovery_result(
        cursor,
        epoch_id=header.epoch_id,
        result=stored_result,
        scope=scope,
        job=job,
        manifest=manifest,
        eligible_snapshot_exhausted=eligible_snapshot_exhausted,
    )
    stored_artifact.artifact.validate_job_shape(job.spec.job_kind)
    work_end = len(work_names)
    stored_work = M5RuntimeWork(
        **dict(zip(work_names, map(int, contribution[:work_end]), strict=True)),
        work_digest=_text(contribution[work_end]),
    )
    expected_work = M5RuntimeWork(
        requirement_channel_hit_count=len(stored_result.channel_hits),
        requirement_pre_dedup_selection_count=len(stored_result.selections),
        bytes_hashed=stored_work.bytes_hashed,
        bytes_serialized=stored_work.bytes_serialized,
    )
    expected_key = digests.runtime_work_contribution_key_digest(
        epoch_id=header.epoch_id,
        contribution_kind=M5RuntimeWorkContributionKind.ROOT_RESULT_STAGE,
        source_id=attempt.attempt.attempt_id,
    )
    if (
        stored_work != expected_work
        or _text(contribution[work_end + 1]) != supplied_output.attempt_output_digest
        or _text(contribution[work_end + 2]) != expected_key
        or int(contribution[work_end + 3]) != replay_revision
        or replay_revision != scope.staged_revision
    ):
        raise EventConflictError("root staging replay changed immutable accounting")
    return M5RequirementAttemptReturnReceipt(
        disposition=M5RequirementReturnDisposition.APPLIED,
        logical_job_id=job.spec.logical_job_id,
        attempt_id=attempt.attempt.attempt_id,
        resulting_revision=header.revision,
        exact_replay=True,
        execution_evidence_digest=accounting.evidence.evidence_digest,
        return_artifact_digest=(stored_artifact.artifact.attempt_result_artifact_hash),
        current_terminal_logical_result_hash=(
            _current_terminal_logical_result_hash(cursor, header=header)
        ),
        transition_anchor=None,
    )


def _lock_and_validate_pending_counter_revisions(
    cursor: Cursor[Any], *, epoch_id: int, expected_revision: int
) -> tuple[int, int]:
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
        raise ValidationError("PENDING counter revision diverged from runtime epoch")
    return len(owner_rows), len(answer_rows)


def _cancellation_pending_projection(
    cursor: Cursor[Any],
    *,
    header: _EpochHeader,
    scopes: tuple[_StoredScope, ...],
    jobs: tuple[_StoredJob, ...],
) -> tuple[tuple[_PendingDelta, ...], tuple[_PendingDelta, ...]]:
    """Freeze selected owner/answer deltas before the counter-lock tier."""

    member_rows = cursor.execute(
        """
        SELECT requirement_version_id, owner_claim_id
        FROM groundloop_m5_requirement_registry_snapshot_member
        WHERE requirement_registry_snapshot_digest = %s
        ORDER BY requirement_version_id COLLATE "C", owner_claim_id COLLATE "C"
        """,
        (header.requirement_registry_snapshot_digest,),
    ).fetchall()
    owners_by_requirement: dict[str, set[str]] = {}
    all_owner_ids: set[str] = set()
    for requirement_version_id_value, owner_claim_id_value in member_rows:
        requirement_version_id = str(requirement_version_id_value)
        owner_claim_id = str(owner_claim_id_value)
        owners_by_requirement.setdefault(requirement_version_id, set()).add(
            owner_claim_id
        )
        all_owner_ids.add(owner_claim_id)
    scope_by_root = {scope.root_job_id: scope for scope in scopes}
    owner_deltas: dict[str, Counter[str]] = {}
    counter_by_kind = {
        M5JobKind.REVERSE_REQUIREMENT_DISCOVERY: "broad_reverse_scope_count",
        M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL: "forward_scope_count",
        M5JobKind.VERIFY_REQUIREMENT_PAIR: "verifier_job_count",
    }
    for job in jobs:
        if job.spec.job_kind is M5JobKind.REVERSE_REQUIREMENT_DISCOVERY:
            selected_owner_ids = all_owner_ids
        elif job.spec.job_kind is M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL:
            scope = scope_by_root[job.spec.logical_job_id]
            scope_requirement_version_id = scope.contract.requirement_version_id
            assert scope_requirement_version_id is not None
            selected_owner_ids = owners_by_requirement.get(
                scope_requirement_version_id, set()
            )
        else:
            assert job.spec.pair is not None
            selected_owner_ids = owners_by_requirement.get(
                job.spec.pair.subject_id, set()
            )
        if not selected_owner_ids:
            raise ValidationError("cancellation job lacks frozen owner multiplicity")
        for owner_claim_id in selected_owner_ids:
            owner_delta = owner_deltas.setdefault(owner_claim_id, Counter())
            owner_delta[counter_by_kind[job.spec.job_kind]] += 1

    answer_rows = cursor.execute(
        """
        SELECT claim_id, answer_version_id
        FROM groundloop_claim
        WHERE claim_id = ANY(%s) AND required
        ORDER BY answer_version_id COLLATE "C", claim_id COLLATE "C"
        """,
        (list(owner_deltas),),
    ).fetchall()
    answer_deltas: dict[str, Counter[str]] = {}
    for owner_claim_id_value, answer_version_id_value in answer_rows:
        owner_claim_id = str(owner_claim_id_value)
        answer_version_id = str(answer_version_id_value)
        answer_delta = answer_deltas.setdefault(answer_version_id, Counter())
        answer_delta.update(owner_deltas[owner_claim_id])

    def freeze(deltas: dict[str, Counter[str]]) -> tuple[_PendingDelta, ...]:
        return tuple(
            _PendingDelta(
                key,
                counters["broad_reverse_scope_count"],
                counters["forward_scope_count"],
                counters["verifier_job_count"],
            )
            for key, counters in sorted(
                deltas.items(), key=lambda item: item[0].encode("utf-8")
            )
        )

    return freeze(owner_deltas), freeze(answer_deltas)


def _apply_cancellation_pending_deltas(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    expected_revision: int,
    resulting_revision: int,
    owner_deltas: tuple[_PendingDelta, ...],
    answer_deltas: tuple[_PendingDelta, ...],
    locked_row_counts: tuple[int, int] | None = None,
) -> None:
    """CAS the selected owner/answer units, then advance every cutoff."""

    if locked_row_counts is None:
        owner_row_count, answer_row_count = (
            _lock_and_validate_pending_counter_revisions(
                cursor, epoch_id=epoch_id, expected_revision=expected_revision
            )
        )
    else:
        if (
            type(locked_row_counts) is not tuple
            or len(locked_row_counts) != 2
            or any(type(value) is not int or value < 0 for value in locked_row_counts)
        ):
            raise ValidationError("locked PENDING counter row counts are invalid")
        owner_row_count, answer_row_count = locked_row_counts
    for delta in owner_deltas:
        values = delta.values
        updated = cursor.execute(
            """
            UPDATE groundloop_m5_owner_pending_counter
            SET broad_reverse_scope_count = broad_reverse_scope_count - %s,
                forward_scope_count = forward_scope_count - %s,
                verifier_job_count = verifier_job_count - %s
            WHERE epoch_id = %s AND owner_claim_id = %s
              AND updated_revision = %s
              AND broad_reverse_scope_count >= %s
              AND forward_scope_count >= %s
              AND verifier_job_count >= %s
            """,
            (*values, epoch_id, delta.key, expected_revision, *values),
        ).rowcount
        if updated != 1:
            raise ValidationError("owner PENDING cancellation lost multiplicity")

    for delta in answer_deltas:
        values = delta.values
        updated = cursor.execute(
            """
            UPDATE groundloop_m5_answer_pending_counter
            SET broad_reverse_scope_count = broad_reverse_scope_count - %s,
                forward_scope_count = forward_scope_count - %s,
                verifier_job_count = verifier_job_count - %s
            WHERE epoch_id = %s AND answer_version_id = %s
              AND updated_revision = %s
              AND broad_reverse_scope_count >= %s
              AND forward_scope_count >= %s
              AND verifier_job_count >= %s
            """,
            (*values, epoch_id, delta.key, expected_revision, *values),
        ).rowcount
        if updated != 1:
            raise ValidationError("answer PENDING cancellation lost multiplicity")

    owner_advanced = cursor.execute(
        """
        UPDATE groundloop_m5_owner_pending_counter
        SET updated_revision = %s
        WHERE epoch_id = %s AND updated_revision = %s
        """,
        (resulting_revision, epoch_id, expected_revision),
    ).rowcount
    answer_advanced = cursor.execute(
        """
        UPDATE groundloop_m5_answer_pending_counter
        SET updated_revision = %s
        WHERE epoch_id = %s AND updated_revision = %s
        """,
        (resulting_revision, epoch_id, expected_revision),
    ).rowcount
    if owner_advanced != owner_row_count or answer_advanced != answer_row_count:
        raise ValidationError("PENDING cancellation cutoff lost rows")


def _advance_revision(
    cursor: Cursor[Any],
    *,
    header: _EpochHeader,
    resulting_revision: int,
    pending_revision_already_updated: bool = False,
) -> None:
    if not pending_revision_already_updated:
        _lock_and_validate_pending_counter_revisions(
            cursor, epoch_id=header.epoch_id, expected_revision=header.revision
        )
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
    counts = cursor.execute(
        """
        SELECT count(*) FILTER (
                   WHERE job_state IN ('declared', 'running', 'retryable_failed')
               ),
               count(*) FILTER (WHERE job_state = 'terminal_failed'),
               (SELECT count(*)
                FROM groundloop_m5_discovery_scope
                WHERE epoch_id = %s
                  AND scope_state IN ('open', 'result_staged'))
        FROM groundloop_m5_semantic_job
        WHERE epoch_id = %s
        """,
        (header.epoch_id, header.epoch_id),
    ).fetchone()
    assert counts is not None
    updated_base = cursor.execute(
        """
        UPDATE groundloop_epoch
        SET revision = %s
        WHERE epoch_id = %s AND revision = %s
          AND structural_status = 'committed'
          AND semantic_status = 'pending'
          AND evaluation_state = 'pending'
        """,
        (resulting_revision, header.epoch_id, header.revision),
    ).rowcount
    if updated_base != 1:
        raise EventConflictError("stale base epoch revision")
    updated_runtime = cursor.execute(
        """
        UPDATE groundloop_m5_runtime_epoch
        SET runtime_state = 'semantic_pending', revision = %s,
            open_work_count = %s, blocking_failure_count = %s,
            open_scope_count = %s
        WHERE epoch_id = %s AND revision = %s
          AND runtime_state IN ('structural_committed', 'semantic_pending')
        """,
        (
            resulting_revision,
            int(counts[0]),
            int(counts[1]),
            int(counts[2]),
            header.epoch_id,
            header.revision,
        ),
    ).rowcount
    if updated_runtime != 1:
        raise EventConflictError("stale typed runtime revision")


def _advance_cancellation_revision(
    cursor: Cursor[Any],
    *,
    header: _EpochHeader,
    resulting_revision: int,
    cancelled_job_count: int,
    cancelled_root_count: int,
) -> None:
    """Advance the header by the exact selected cancellation units."""

    if (
        header.open_work_count < cancelled_job_count
        or header.open_scope_count < cancelled_root_count
    ):
        raise ValidationError("cancellation exceeds the durable open-count image")
    updated_base = cursor.execute(
        """
        UPDATE groundloop_epoch
        SET revision = %s
        WHERE epoch_id = %s AND revision = %s
          AND structural_status = 'committed'
          AND semantic_status = 'pending'
          AND evaluation_state = 'pending'
        """,
        (resulting_revision, header.epoch_id, header.revision),
    ).rowcount
    if updated_base != 1:
        raise EventConflictError("stale base epoch revision")
    updated_runtime = cursor.execute(
        """
        UPDATE groundloop_m5_runtime_epoch
        SET runtime_state = 'semantic_pending', revision = %s,
            open_work_count = %s, open_scope_count = %s
        WHERE epoch_id = %s AND revision = %s
          AND runtime_state IN ('structural_committed', 'semantic_pending')
          AND open_work_count = %s AND open_scope_count = %s
          AND blocking_failure_count = %s
        """,
        (
            resulting_revision,
            header.open_work_count - cancelled_job_count,
            header.open_scope_count - cancelled_root_count,
            header.epoch_id,
            header.revision,
            header.open_work_count,
            header.open_scope_count,
            header.blocking_failure_count,
        ),
    ).rowcount
    if updated_runtime != 1:
        raise EventConflictError("stale typed runtime revision")


def _force_deferred_validation(cursor: Cursor[Any]) -> None:
    cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
    cursor.execute("SET CONSTRAINTS ALL DEFERRED")


def stage_m5_discovery_result(
    cursor: Cursor[Any],
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
    failure_injector: RuntimeRootFailureInjector | None = None,
) -> M5RequirementAttemptReturnReceipt:
    """Stage one successful root result under the caller's transaction."""

    if (
        isinstance(expected_revision, bool)
        or not isinstance(expected_revision, int)
        or expected_revision < 1
    ):
        raise InvalidEventError("expected runtime revision must be positive")
    if execution_disposition not in {
        M5ExecutionEvidenceDisposition.RETURNED,
        M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
    }:
        raise ValidationError(
            "successful root return requires returned or reused_artifact"
        )
    require_runtime_recovery_bundle(cursor)
    leased_attempt = _validate_executable_lease(
        lease=lease, job=job, expected_revision=expected_revision
    )
    if (
        job.job_kind
        not in {
            M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL,
            M5JobKind.REVERSE_REQUIREMENT_DISCOVERY,
        }
        or not job.expandable
    ):
        raise ValidationError("discovery staging accepts only expandable root jobs")
    if (
        attempt_output.attempt_id != leased_attempt.attempt_id
        or attempt_output.logical_job_id != job.logical_job_id
        or attempt_output.job_epoch_id != epoch_id
        or attempt_output.payload_hash != job.payload_hash
        or attempt_output.execution_spec_hash != job.execution_spec_hash
        or attempt_output.result_artifact_id != result.result_artifact_id
        or attempt_output.result_artifact_hash != result.result_artifact_hash
    ):
        raise ValidationError("attempt output does not bind the supplied root result")

    header = _lock_epoch(cursor, epoch_id)
    if expected_revision > header.revision:
        raise EventConflictError("expected revision is newer than durable runtime")
    manifest = _load_manifest(cursor, header.candidate_policy_id)
    # Read the immutable target first so tier-7 activity rows can be locked
    # before the tier-8 scope row.
    scope_hint = _read_scope(
        cursor, epoch_id=epoch_id, root_job_id=job.logical_job_id, for_update=False
    )
    activity = _lock_activity_snapshot(cursor, header=header, scope=scope_hint.contract)
    _lock_snapshot_headers(cursor, header=header)
    scope = _read_scope(
        cursor, epoch_id=epoch_id, root_job_id=job.logical_job_id, for_update=True
    )
    stored_job = _read_job(
        cursor, epoch_id=epoch_id, logical_job_id=job.logical_job_id, for_update=True
    )
    attempt = read_requirement_attempt(
        cursor,
        logical_job_id=job.logical_job_id,
        attempt_id=leased_attempt.attempt_id,
    )
    if attempt is None or not _same_attempt_identity(attempt.attempt, leased_attempt):
        raise EventConflictError("root return lease identity is not durable")
    _validate_header_bindings(header, scope, stored_job, manifest)
    _validate_discovery_result(
        cursor,
        epoch_id=epoch_id,
        result=result,
        scope=scope,
        job=stored_job,
        manifest=manifest,
        eligible_snapshot_exhausted=eligible_snapshot_exhausted,
    )
    dispatch = load_requirement_dispatch(
        cursor, epoch_id=epoch_id, attempt=attempt.attempt
    )
    if (
        dispatch.record_digest != lease.dispatch_record_digest
        or dispatch.dispatched_revision != lease.resulting_revision
    ):
        raise EventConflictError("root return lease dispatch identity is not durable")
    terminal_logical_result_hash = _current_terminal_logical_result_hash(
        cursor, header=header
    )
    late_accounting = build_requirement_execution_accounting(
        epoch_id=epoch_id,
        expected_revision=expected_revision,
        attempt=leased_attempt,
        dispatch=dispatch,
        disposition=execution_disposition,
        result_or_error_hash=attempt_output.attempt_output_digest,
        attempt_work=attempt_work,
        attempt_timing=attempt_timing,
        anchor_kind=M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN,
        anchor_revision=header.revision,
    )
    replay_revision = read_requirement_execution_replay(
        cursor,
        expected_evidence=late_accounting.evidence,
        expected_observation=late_accounting.observation,
    )
    postterminal_replay = (
        None
        if replay_revision is not None
        else read_requirement_postterminal_replay(
            cursor,
            expected_evidence=late_accounting.evidence,
            expected_observation=late_accounting.observation,
        )
    )
    late_replay = _validate_late_replay(
        cursor,
        header=header,
        lease=lease,
        supplied_job=job,
        supplied_output=attempt_output,
        job=stored_job,
        attempt=attempt,
        accounting=late_accounting,
        event_replay_revision=replay_revision,
        postterminal_replay=postterminal_replay,
    )
    if late_replay is not None:
        return late_replay
    accounting = build_requirement_execution_accounting(
        epoch_id=epoch_id,
        expected_revision=expected_revision,
        attempt=leased_attempt,
        dispatch=dispatch,
        disposition=execution_disposition,
        result_or_error_hash=attempt_output.attempt_output_digest,
        attempt_work=attempt_work,
        attempt_timing=attempt_timing,
        anchor_kind=M5RuntimeWorkContributionKind.ROOT_RESULT_STAGE,
    )
    replay = _validate_stage_replay(
        cursor,
        header=header,
        lease=lease,
        supplied_job=job,
        supplied_result=result,
        supplied_output=attempt_output,
        scope=scope,
        job=stored_job,
        attempt=attempt,
        manifest=manifest,
        eligible_snapshot_exhausted=eligible_snapshot_exhausted,
        accounting=accounting,
        replay_revision=replay_revision,
    )
    if replay is not None:
        return replay
    if job != stored_job.spec:
        raise EventConflictError("supplied root job differs from durable identity")
    latest = read_latest_requirement_attempt(cursor, logical_job_id=job.logical_job_id)
    if latest is None:
        raise ValidationError("root job lost its durable attempt history")
    late_plan = _build_late_return_plan(
        header=header,
        job=stored_job,
        attempt=attempt,
        output=attempt_output,
        activity=activity,
        terminal_logical_result_hash=terminal_logical_result_hash,
    )
    if late_plan is not None:
        postterminal = late_plan.terminal_logical_result_hash is not None
        if attempt.state == "expired":
            successor = cursor.execute(
                """
                SELECT 1
                FROM groundloop_m5_job_attempt
                WHERE logical_job_id = %s AND attempt_ordinal = %s
                """,
                (job.logical_job_id, attempt.attempt.attempt_ordinal + 1),
            ).fetchone()
            if (
                successor is None
                or latest.attempt.attempt_ordinal <= attempt.attempt.attempt_ordinal
            ):
                raise ValidationError("expired root return lacks a dense successor")
        elif not _same_attempt_identity(latest.attempt, leased_attempt):
            raise EventConflictError(
                "still-current terminal audit has a later durable attempt"
            )
        if postterminal:
            if not stored_job.state.terminal or scope.state in {
                M5ScopeState.OPEN,
                M5ScopeState.RESULT_STAGED,
            }:
                raise ValidationError("postterminal root return lacks terminal closure")
        else:
            if header.revision != expected_revision:
                raise EventConflictError("stale preterminal late-return revision")
            _require_pending_epoch(header)
            if attempt.state == "expired":
                if stored_job.state is not M5JobState.RUNNING or scope.state not in {
                    M5ScopeState.OPEN,
                    M5ScopeState.RESULT_STAGED,
                }:
                    raise EventConflictError(
                        "preterminal expired return lacks its running successor"
                    )
            elif (
                stored_job.state is not M5JobState.CANCELLED
                or scope.state is not M5ScopeState.CANCELLED
            ):
                raise EventConflictError(
                    "preterminal terminal audit lacks cancelled closure"
                )
        return _persist_late_return(
            cursor,
            header=header,
            plan=late_plan,
            attempt=attempt,
            output=attempt_output,
            accounting=late_accounting,
            failure_injector=failure_injector,
        )

    if header.revision != expected_revision:
        raise EventConflictError("stale typed runtime revision")
    _require_pending_epoch(header)
    if not _same_attempt_identity(latest.attempt, leased_attempt):
        raise EventConflictError("root return lease is no longer the latest attempt")
    if attempt.state != "dispatched" or attempt.attempt_output_digest is not None:
        raise EventConflictError("root attempt is not an unreserved dispatch")
    if stored_job.state is not M5JobState.RUNNING:
        raise EventConflictError("root job is not running")
    if scope.state is not M5ScopeState.OPEN:
        raise EventConflictError("root discovery scope is not open")

    archive_reason = classify_attempt_activity(
        epoch_active=activity[0],
        chunk_active=activity[1],
        requirement_active=activity[2],
        group_active=activity[3],
        job_already_terminal=False,
    )
    if archive_reason is M5AttemptArchiveReason.JOB_ALREADY_TERMINAL:
        raise ValidationError("running root cannot classify as already terminal")
    artifact = M5AttemptResultArtifact.build(
        attempt_output=attempt_output,
        job_state_at_receipt=M5JobState.RUNNING,
        job_state_after=M5JobState.RUNNING,
        disposition=M5AttemptDisposition.ROOT_RESULT_STAGED,
        activity_snapshot_epoch_id=epoch_id,
        activity_snapshot_revision=header.revision,
        epoch_active=activity[0],
        chunk_active=activity[1],
        requirement_active=activity[2],
        group_active=activity[3],
        archive_reason=archive_reason,
    )
    artifact.validate_job_shape(job.job_kind)
    resulting_revision = header.revision + 1
    _authorize(cursor, header)
    accounting_start = start_event_accounting(
        cursor, epoch_id=epoch_id, expected_revision=expected_revision
    )
    persist_requirement_execution_accounting(cursor, accounting=accounting)
    _inject(failure_injector, "root_stage_authorized")

    reserved = cursor.execute(
        """
        UPDATE groundloop_m5_job_attempt
        SET attempt_state = 'result_reserved', attempt_output_digest = %s
        WHERE attempt_id = %s AND logical_job_id = %s
          AND attempt_state = 'dispatched' AND attempt_output_digest IS NULL
        """,
        (
            attempt_output.attempt_output_digest,
            attempt_output.attempt_id,
            job.logical_job_id,
        ),
    ).rowcount
    if reserved != 1:
        raise EventConflictError("root attempt output reservation lost its race")
    _inject(failure_injector, "root_stage_output_reserved")

    cursor.execute(
        """
        INSERT INTO groundloop_m5_attempt_result_artifact (
            attempt_result_artifact_id, attempt_result_artifact_hash,
            attempt_output_digest, attempt_id, logical_job_id, job_epoch_id,
            payload_hash, execution_spec_hash, result_artifact_id,
            result_artifact_hash, job_state_at_receipt, job_state_after,
            disposition, activity_snapshot_epoch_id,
            activity_snapshot_revision, epoch_active, chunk_active,
            requirement_active, group_active, archive_reason,
            cancelled_by_event_id, cancelled_by_epoch_id, cancellation_reason
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, NULL, NULL
        )
        """,
        (
            artifact.attempt_result_artifact_id,
            artifact.attempt_result_artifact_hash,
            artifact.attempt_output_digest,
            artifact.attempt_id,
            artifact.logical_job_id,
            artifact.job_epoch_id,
            attempt_output.payload_hash,
            attempt_output.execution_spec_hash,
            attempt_output.result_artifact_id,
            attempt_output.result_artifact_hash,
            artifact.job_state_at_receipt.value,
            artifact.job_state_after.value,
            artifact.disposition.value,
            artifact.activity_snapshot_epoch_id,
            artifact.activity_snapshot_revision,
            artifact.epoch_active,
            artifact.chunk_active,
            artifact.requirement_active,
            artifact.group_active,
            None if artifact.archive_reason is None else artifact.archive_reason.value,
        ),
    )
    _inject(failure_injector, "root_stage_attempt_artifact_inserted")

    for hit in result.channel_hits:
        cursor.execute(
            """
            INSERT INTO groundloop_m5_requirement_channel_hit (
                hit_digest, epoch_id, root_job_id, scope_contract_digest,
                subject_kind, subject_id, chunk_version_id,
                semantic_pair_digest, candidate_policy_id, channel, rank,
                score, channel_artifact_hash
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                hit.hit_digest,
                hit.epoch_id,
                hit.root_job_id,
                hit.scope_contract_digest,
                hit.pair.subject_kind.value,
                hit.pair.subject_id,
                hit.pair.chunk_version_id,
                hit.semantic_pair_digest,
                hit.candidate_policy_id,
                hit.channel.value,
                hit.rank,
                hit.score,
                hit.channel_artifact_hash,
            ),
        )
    _inject(failure_injector, "root_stage_hits_inserted")
    for selection in result.selections:
        cursor.execute(
            """
            INSERT INTO groundloop_m5_requirement_scope_selection (
                selection_digest, root_job_id, scope_contract_digest,
                subject_kind, subject_id, chunk_version_id,
                semantic_pair_digest, fused_rank, reasons, mandatory_lineage
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                selection.selection_digest,
                selection.root_job_id,
                selection.scope_contract_digest,
                selection.pair.subject_kind.value,
                selection.pair.subject_id,
                selection.pair.chunk_version_id,
                selection.semantic_pair_digest,
                selection.fused_rank,
                [reason.value for reason in selection.reasons],
                selection.mandatory_lineage,
            ),
        )
    _inject(failure_injector, "root_stage_selections_inserted")
    cursor.execute(
        """
        INSERT INTO groundloop_m5_requirement_discovery_result (
            result_artifact_id, result_artifact_hash, root_job_id,
            scope_contract_digest, termination, channel_hit_count,
            selection_count, approximate_selection_count,
            mandatory_lineage_only_count, staged_epoch_id, staged_revision
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            result.result_artifact_id,
            result.result_artifact_hash,
            result.root_job_id,
            result.scope_contract_digest,
            result.termination.value,
            len(result.channel_hits),
            len(result.selections),
            result.approximate_selection_count,
            result.mandatory_lineage_only_count,
            epoch_id,
            resulting_revision,
        ),
    )
    _inject(failure_injector, "root_stage_result_inserted")
    transitioned_scope = cursor.execute(
        """
        UPDATE groundloop_m5_discovery_scope
        SET scope_state = 'result_staged', staged_result_artifact_hash = %s,
            staged_revision = %s
        WHERE epoch_id = %s AND root_job_id = %s AND scope_state = 'open'
        """,
        (result.result_artifact_hash, resulting_revision, epoch_id, job.logical_job_id),
    ).rowcount
    if transitioned_scope != 1:
        raise EventConflictError("root scope staging lost its race")
    _inject(failure_injector, "root_stage_scope_transitioned")
    completed_attempt = cursor.execute(
        """
        UPDATE groundloop_m5_job_attempt
        SET attempt_state = 'completed', finished_at = clock_timestamp(),
            attempt_work_digest = %s
        WHERE attempt_id = %s AND logical_job_id = %s
          AND attempt_state = 'result_reserved'
          AND attempt_output_digest = %s
          AND lease_token_hash = %s AND lease_expires_at = %s
          AND execution_spec_hash = %s
        """,
        (
            accounting.evidence.attempt_work.work_digest,
            attempt_output.attempt_id,
            job.logical_job_id,
            attempt_output.attempt_output_digest,
            leased_attempt.lease_token_hash,
            leased_attempt.lease_expires_at,
            leased_attempt.execution_spec_hash,
        ),
    ).rowcount
    if completed_attempt != 1:
        raise EventConflictError("root attempt completion lost its reservation")
    _inject(failure_injector, "root_stage_attempt_completed")
    measured_work = _derive_root_result_stage_work(
        cursor,
        epoch_id=epoch_id,
        logical_job_id=job.logical_job_id,
        attempt_id=attempt_output.attempt_id,
    )
    stage_work = M5RuntimeWork(
        requirement_channel_hit_count=len(result.channel_hits),
        requirement_pre_dedup_selection_count=len(result.selections),
        bytes_hashed=measured_work.bytes_hashed,
        bytes_serialized=measured_work.bytes_serialized,
    )
    persist_root_result_stage_contribution(
        cursor,
        accounting=accounting,
        attempt_output_digest=attempt_output.attempt_output_digest,
        stage_work=stage_work,
    )
    _advance_revision(cursor, header=header, resulting_revision=resulting_revision)
    finish_requirement_execution_accounting(
        cursor,
        epoch_id=epoch_id,
        expected_revision=expected_revision,
        start=accounting_start,
        accounting=accounting,
        additional_work=stage_work,
    )
    _inject(failure_injector, "root_stage_revision_advanced")
    _force_deferred_validation(cursor)
    _inject(failure_injector, "root_stage_constraints_validated")
    receipt = M5RequirementAttemptReturnReceipt(
        disposition=M5RequirementReturnDisposition.APPLIED,
        logical_job_id=job.logical_job_id,
        attempt_id=attempt_output.attempt_id,
        resulting_revision=resulting_revision,
        exact_replay=False,
        execution_evidence_digest=accounting.evidence.evidence_digest,
        return_artifact_digest=artifact.attempt_result_artifact_hash,
        current_terminal_logical_result_hash=None,
        transition_anchor=accounting.anchor,
    )
    receipt.validate_anchor_context(
        epoch_id=epoch_id,
        expected_kind=M5RuntimeWorkContributionKind.ROOT_RESULT_STAGE,
    )
    return receipt


def _read_all_root_scopes(
    cursor: Cursor[Any], *, epoch_id: int
) -> tuple[_StoredScope, ...]:
    rows = cursor.execute(
        _SCOPE_SELECT
        + ' WHERE epoch_id = %s ORDER BY root_job_id COLLATE "C" FOR UPDATE',
        (epoch_id,),
    ).fetchall()
    return tuple(_scope_from_row(tuple(row)) for row in rows)


def _read_all_root_jobs(
    cursor: Cursor[Any], *, epoch_id: int
) -> tuple[_StoredJob, ...]:
    rows = cursor.execute(
        _JOB_SELECT
        + " WHERE epoch_id = %s AND parent_job_id IS NULL"
        + ' ORDER BY logical_job_id COLLATE "C" FOR UPDATE',
        (epoch_id,),
    ).fetchall()
    return tuple(_job_from_row(tuple(row)) for row in rows)


def _root_attempt_result(
    cursor: Cursor[Any],
    *,
    root_job_id: str,
    result: M5RequirementDiscoveryResult,
) -> _StoredAttemptResult:
    rows = cursor.execute(
        """
        SELECT attempt_id
        FROM groundloop_m5_attempt_result_artifact
        WHERE logical_job_id = %s
          AND disposition = 'root_result_staged'
          AND result_artifact_id = %s
          AND result_artifact_hash = %s
        ORDER BY attempt_id COLLATE "C"
        """,
        (root_job_id, result.result_artifact_id, result.result_artifact_hash),
    ).fetchall()
    if len(rows) != 1:
        raise ValidationError("staged root result lacks one attempt-result artifact")
    stored = _load_attempt_result(cursor, attempt_id=_text(rows[0][0]))
    assert stored is not None
    if (
        stored.artifact.logical_job_id != root_job_id
        or stored.artifact.disposition is not M5AttemptDisposition.ROOT_RESULT_STAGED
        or stored.output.result_artifact_id != result.result_artifact_id
        or stored.output.result_artifact_hash != result.result_artifact_hash
    ):
        raise ValidationError("root result and attempt artifact identities diverged")
    return stored


def _terminal_reason(reason: M5AttemptArchiveReason) -> M5TerminalReason:
    mapping = {
        M5AttemptArchiveReason.EPOCH_FAILED: M5TerminalReason.EPOCH_FAILED,
        M5AttemptArchiveReason.SUBJECT_INACTIVE: M5TerminalReason.SUBJECT_INACTIVE,
        M5AttemptArchiveReason.CHUNK_INACTIVE: M5TerminalReason.CHUNK_INACTIVE,
    }
    try:
        return mapping[reason]
    except KeyError as error:
        raise ValidationError(
            "staged root has an inapplicable terminal archive reason"
        ) from error


def _build_barrier(
    *,
    header: _EpochHeader,
    manifest: M5CandidatePolicyManifest,
    scopes: tuple[_StoredScope, ...],
    jobs: tuple[_StoredJob, ...],
    results: tuple[M5RequirementDiscoveryResult, ...],
    artifacts: tuple[_StoredAttemptResult, ...],
) -> tuple[
    M5RootBarrierPlan,
    tuple[M5LogicalJobSpec, ...],
    dict[str, M5JobCompletion],
]:
    scope_by_root = {scope.root_job_id: scope for scope in scopes}
    artifact_by_root = {
        artifact.artifact.logical_job_id: artifact for artifact in artifacts
    }
    inactive_roots = tuple(
        sorted(
            root_id
            for root_id, artifact in artifact_by_root.items()
            if artifact.artifact.archive_reason is not None
        )
    )
    deduplication = deduplicate_discovery_results(
        epoch_id=header.epoch_id,
        candidate_policy_id=header.candidate_policy_id,
        results=results,
        inactive_root_job_ids=inactive_roots,
    )
    child_jobs = tuple(
        sorted(
            (
                M5LogicalJobSpec.build(
                    structural_event_id=header.structural_event_id,
                    job_kind=M5JobKind.VERIFY_REQUIREMENT_PAIR,
                    manifest=manifest,
                    scope=scope_by_root[pair.owner_root_job_id].contract,
                    parent_job_id=pair.owner_root_job_id,
                    pair=pair.pair,
                )
                for pair in deduplication.admitted_pairs
            ),
            key=lambda child: child.logical_job_id,
        )
    )
    plan = build_root_barrier_plan(
        structural_event_id=header.structural_event_id,
        results=results,
        deduplication=deduplication,
        child_jobs=child_jobs,
    )
    closure_by_root = {closure.root_job_id: closure for closure in plan.root_closures}
    result_by_root = {result.root_job_id: result for result in results}
    completion_by_root: dict[str, M5JobCompletion] = {}
    for job in jobs:
        root_id = job.spec.logical_job_id
        result = result_by_root[root_id]
        closure = closure_by_root[root_id]
        archive_reason = artifact_by_root[root_id].artifact.archive_reason
        if archive_reason is None:
            terminal_state = M5JobState.COMPLETED_ACTIVE
            terminal_reason = None
        else:
            terminal_state = M5JobState.COMPLETED_INACTIVE
            terminal_reason = _terminal_reason(archive_reason)
        completion_by_root[root_id] = M5JobCompletion.build(
            job=job.spec,
            terminal_state=terminal_state,
            result_artifact_id=result.result_artifact_id,
            result_artifact_hash=result.result_artifact_hash,
            scope_closure_digest=closure.scope_closure_digest,
            child_set_hash=closure.child_set_hash,
            archive_reason=terminal_reason,
        )
    return plan, child_jobs, completion_by_root


def _load_frontier_head(
    cursor: Cursor[Any],
    *,
    requirement_version_id: str,
    candidate_policy_id: str,
    for_update: bool,
) -> M5RequirementFrontierHead | None:
    row = cursor.execute(
        """
        SELECT latest_root_job_id, latest_scope_contract_digest,
               latest_active_chunk_snapshot_digest,
               latest_discovery_result_artifact_hash,
               latest_scope_closure_digest, latest_completion_digest,
               completed_epoch_id, completed_revision
        FROM groundloop_m5_requirement_frontier_head
        WHERE requirement_version_id = %s AND candidate_policy_id = %s
        """
        + (" FOR UPDATE" if for_update else ""),
        (requirement_version_id, candidate_policy_id),
    ).fetchone()
    if row is None:
        return None
    return M5RequirementFrontierHead(
        requirement_version_id=requirement_version_id,
        candidate_policy_id=candidate_policy_id,
        latest_root_job_id=_text(row[0]),
        latest_scope_contract_digest=_text(row[1]),
        latest_active_chunk_snapshot_digest=_text(row[2]),
        latest_discovery_result_artifact_hash=_text(row[3]),
        latest_scope_closure_digest=_text(row[4]),
        latest_completion_digest=_text(row[5]),
        completed_epoch_id=int(row[6]),
        completed_revision=int(row[7]),
    )


def _expected_frontier_heads(
    *,
    epoch_id: int,
    completed_revision: int,
    scopes: tuple[_StoredScope, ...],
    jobs: tuple[_StoredJob, ...],
    results: tuple[M5RequirementDiscoveryResult, ...],
    completions: dict[str, M5JobCompletion],
) -> tuple[M5RequirementFrontierHead, ...]:
    job_by_root = {job.spec.logical_job_id: job for job in jobs}
    result_by_root = {result.root_job_id: result for result in results}
    heads: list[M5RequirementFrontierHead] = []
    for scope in scopes:
        completion = completions[scope.root_job_id]
        if (
            scope.contract.direction is M5DiscoveryDirection.FORWARD_REQUIREMENT
            and completion.terminal_state is M5JobState.COMPLETED_ACTIVE
        ):
            heads.append(
                build_forward_frontier_head(
                    job=job_by_root[scope.root_job_id].spec,
                    scope=scope.contract,
                    discovery_result=result_by_root[scope.root_job_id],
                    completion=completion,
                    completed_epoch_id=epoch_id,
                    completed_revision=completed_revision,
                )
            )
    ordered = tuple(
        sorted(
            heads,
            key=lambda head: (
                head.requirement_version_id.encode("utf-8"),
                head.candidate_policy_id.encode("utf-8"),
            ),
        )
    )
    keys = tuple(
        (head.requirement_version_id, head.candidate_policy_id) for head in ordered
    )
    if len(set(keys)) != len(keys):
        raise ValidationError("barrier proposes two forward heads for one key")
    return ordered


def _durable_pair_rows(
    cursor: Cursor[Any], *, epoch_id: int
) -> tuple[tuple[Any, ...], ...]:
    return tuple(
        (
            _text(row[0]),
            str(row[1]),
            str(row[2]),
            str(row[3]),
            _text(row[4]),
            _text(row[5]),
            _text(row[6]),
            list(row[7]),
            bool(row[8]),
        )
        for row in cursor.execute(
            """
            SELECT admitted_pair_digest, subject_kind, subject_id,
                   chunk_version_id, semantic_pair_digest,
                   candidate_policy_id, owner_root_job_id, reasons,
                   mandatory_lineage
            FROM groundloop_m5_requirement_admitted_pair
            WHERE epoch_id = %s
            ORDER BY semantic_pair_digest COLLATE "C"
            """,
            (epoch_id,),
        ).fetchall()
    )


def _validate_root_barrier_timing_replay(
    cursor: Cursor[Any],
    *,
    header: _EpochHeader,
    contribution_key_digest: str,
    completed_revision: int,
) -> None:
    accumulator = cursor.execute(
        """
        SELECT pending_contribution_kind, pending_source_id,
               pending_contribution_key_digest, pending_anchor_revision,
               updated_revision
        FROM groundloop_m5_runtime_timing_accumulator
        WHERE epoch_id = %s
        """,
        (header.epoch_id,),
    ).fetchone()
    if accumulator is None or int(accumulator[4]) != header.revision:
        raise ValidationError("root-barrier timing accumulator lost its cutoff")
    pending_exact = tuple(accumulator[:4]) == (
        M5RuntimeWorkContributionKind.ROOT_BARRIER.value,
        header.structural_event_id,
        contribution_key_digest,
        completed_revision,
    )
    timing_row = cursor.execute(
        """
        SELECT contribution_key_digest, anchor_revision,
               required_interval_observed,
               coordinator_non_db_non_neural_ns, neural_wall_ns,
               postgres_roundtrip_wall_ns, external_io_wall_ns,
               end_to_end_wall_ns, postgres_server_execution_ns,
               postgres_lock_wait_ns, postgres_wal_bytes,
               postgres_shared_block_reads, observation_digest,
               transition_timing_digest
        FROM groundloop_m5_transition_call_timing
        WHERE epoch_id = %s AND contribution_kind = 'root_barrier'
          AND source_id = %s AND anchor_revision = %s
        """,
        (header.epoch_id, header.structural_event_id, completed_revision),
    ).fetchone()
    if timing_row is None:
        if not pending_exact or header.revision != completed_revision:
            raise ValidationError("root-barrier timing anchor is not durable")
        return
    if pending_exact:
        raise ValidationError("root-barrier timing point is both pending and recorded")
    values = tuple(timing_row)
    observed = bool(values[2])
    raw_timing = values[3:12]
    timing = M5RuntimeTiming(*raw_timing) if observed else None
    if not observed and any(value is not None for value in raw_timing):
        raise ValidationError("missing root-barrier timing point has values")
    observation = M5RuntimeTimingObservation(observed, timing, _text(values[12]))
    expected_timing_digest = digests.transition_call_timing_digest(
        epoch_id=header.epoch_id,
        contribution_kind=M5RuntimeWorkContributionKind.ROOT_BARRIER,
        source_id=header.structural_event_id,
        contribution_key_digest=contribution_key_digest,
        anchor_revision=completed_revision,
        observation_digest=observation.observation_digest,
    )
    if (
        _text(values[0]) != contribution_key_digest
        or int(values[1]) != completed_revision
        or _text(values[13]) != expected_timing_digest
    ):
        raise EventConflictError("root-barrier replay changed its timing point")


def _validate_cancellation_timing_replay(
    cursor: Cursor[Any],
    *,
    header: _EpochHeader,
    plan_digest: str,
    contribution_key_digest: str,
    applied_revision: int,
) -> None:
    accumulator = cursor.execute(
        """
        SELECT pending_contribution_kind, pending_source_id,
               pending_contribution_key_digest, pending_anchor_revision,
               updated_revision
        FROM groundloop_m5_runtime_timing_accumulator
        WHERE epoch_id = %s
        """,
        (header.epoch_id,),
    ).fetchone()
    if accumulator is None or int(accumulator[4]) != header.revision:
        raise ValidationError("cancellation timing accumulator lost its cutoff")
    pending_exact = tuple(accumulator[:4]) == (
        M5RuntimeWorkContributionKind.CANCELLATION.value,
        plan_digest,
        contribution_key_digest,
        applied_revision,
    )
    timing_row = cursor.execute(
        """
        SELECT contribution_key_digest, anchor_revision,
               required_interval_observed,
               coordinator_non_db_non_neural_ns, neural_wall_ns,
               postgres_roundtrip_wall_ns, external_io_wall_ns,
               end_to_end_wall_ns, postgres_server_execution_ns,
               postgres_lock_wait_ns, postgres_wal_bytes,
               postgres_shared_block_reads, observation_digest,
               transition_timing_digest
        FROM groundloop_m5_transition_call_timing
        WHERE epoch_id = %s AND contribution_kind = 'cancellation'
          AND source_id = %s AND anchor_revision = %s
        """,
        (header.epoch_id, plan_digest, applied_revision),
    ).fetchone()
    if timing_row is None:
        if not pending_exact or header.revision != applied_revision:
            raise ValidationError("cancellation timing anchor is not durable")
        return
    if pending_exact:
        raise ValidationError("cancellation timing point is pending and recorded")
    values = tuple(timing_row)
    observed = bool(values[2])
    raw_timing = values[3:12]
    timing = M5RuntimeTiming(*raw_timing) if observed else None
    if not observed and any(value is not None for value in raw_timing):
        raise ValidationError("missing cancellation timing point has values")
    observation = M5RuntimeTimingObservation(observed, timing, _text(values[12]))
    expected_digest = digests.transition_call_timing_digest(
        epoch_id=header.epoch_id,
        contribution_kind=M5RuntimeWorkContributionKind.CANCELLATION,
        source_id=plan_digest,
        contribution_key_digest=contribution_key_digest,
        anchor_revision=applied_revision,
        observation_digest=observation.observation_digest,
    )
    if (
        _text(values[0]) != contribution_key_digest
        or int(values[1]) != applied_revision
        or _text(values[13]) != expected_digest
    ):
        raise EventConflictError("cancellation replay changed its timing point")


def _validate_cancellation_replay(
    cursor: Cursor[Any],
    *,
    header: _EpochHeader,
    plan: M5CancellationPlan,
    scopes: tuple[_StoredScope, ...],
    jobs: tuple[_StoredJob, ...],
) -> M5CancellationReceipt | None:
    work_names = M5RuntimeWork.counter_names()
    contribution = cursor.execute(
        f"""
        SELECT {", ".join(work_names)}, work_digest, source_identity_hash,
               contribution_key_digest, applied_revision
        FROM groundloop_m5_runtime_work_contribution
        WHERE epoch_id = %s AND contribution_kind = 'cancellation'
          AND source_id = %s
        """,
        (header.epoch_id, plan.plan_digest),
    ).fetchone()
    if contribution is None:
        return None
    work_end = len(work_names)
    stored_work = M5RuntimeWork(
        **dict(zip(work_names, map(int, contribution[:work_end]), strict=True)),
        work_digest=_text(contribution[work_end]),
    )
    expected_work = M5RuntimeWork(
        requirement_cancelled_job_count=len(plan.cancelled_job_ids)
    )
    expected_key = digests.runtime_work_contribution_key_digest(
        epoch_id=header.epoch_id,
        contribution_kind=M5RuntimeWorkContributionKind.CANCELLATION,
        source_id=plan.plan_digest,
    )
    applied_revision = int(contribution[work_end + 3])
    durable_digest_row = cursor.execute(
        """
        SELECT groundloop_m5_recovery_cancellation_plan_digest(%s, %s, %s, %s)
        """,
        (
            header.epoch_id,
            header.structural_event_id,
            plan.reason.value,
            applied_revision,
        ),
    ).fetchone()
    durable_digest = (
        None
        if durable_digest_row is None or durable_digest_row[0] is None
        else _text(durable_digest_row[0])
    )
    if (
        stored_work != expected_work
        or _text(contribution[work_end + 1]) != plan.plan_digest
        or _text(contribution[work_end + 2]) != expected_key
        or applied_revision > header.revision
        or durable_digest != plan.plan_digest
    ):
        raise EventConflictError("cancellation replay changed immutable accounting")

    scope_by_root = {scope.root_job_id: scope for scope in scopes}
    for job in jobs:
        expected_completion = M5JobCompletion.build(
            job=job.spec,
            terminal_state=M5JobState.CANCELLED,
            archive_reason=plan.reason,
        )
        if (
            _stored_completion(job) != expected_completion
            or job.completed_revision != applied_revision
            or job.cancelled_by_event_id != header.structural_event_id
            or job.cancelled_by_epoch_id != header.epoch_id
            or job.cancellation_reason is not plan.reason
        ):
            raise EventConflictError("cancelled job differs from its immutable plan")
        if job.spec.parent_job_id is None:
            scope = scope_by_root[job.spec.logical_job_id]
            if (
                scope.state is not M5ScopeState.CANCELLED
                or scope.completion_digest != expected_completion.completion_digest
                or scope.closed_revision != applied_revision
            ):
                raise EventConflictError(
                    "cancelled root scope differs from its immutable plan"
                )

    work_cutoff = cursor.execute(
        """
        SELECT updated_revision, terminalized
        FROM groundloop_m5_runtime_work_accumulator
        WHERE epoch_id = %s
        """,
        (header.epoch_id,),
    ).fetchone()
    if work_cutoff is None or (
        int(work_cutoff[0]) != header.revision
        or bool(work_cutoff[1]) != (header.runtime_state in {"sealed", "failed"})
    ):
        raise ValidationError("cancellation work accumulator lost its cutoff")
    _validate_cancellation_timing_replay(
        cursor,
        header=header,
        plan_digest=plan.plan_digest,
        contribution_key_digest=expected_key,
        applied_revision=applied_revision,
    )
    _lock_and_validate_pending_counter_revisions(
        cursor, epoch_id=header.epoch_id, expected_revision=header.revision
    )
    return M5CancellationReceipt(plan.cancelled_job_ids, header.revision, True)


def _validate_barrier_replay(
    cursor: Cursor[Any],
    *,
    header: _EpochHeader,
    plan: M5RootBarrierPlan,
    child_jobs: tuple[M5LogicalJobSpec, ...],
    scopes: tuple[_StoredScope, ...],
    jobs: tuple[_StoredJob, ...],
    results: tuple[M5RequirementDiscoveryResult, ...],
    completions: dict[str, M5JobCompletion],
) -> M5RootBarrierReceipt:
    expected_pairs = tuple(
        (
            pair.admitted_pair_digest,
            pair.pair.subject_kind.value,
            pair.pair.subject_id,
            pair.pair.chunk_version_id,
            pair.semantic_pair_digest,
            pair.candidate_policy_id,
            pair.owner_root_job_id,
            [reason.value for reason in pair.reasons],
            pair.mandatory_lineage,
        )
        for pair in plan.admitted_pairs
    )
    if _durable_pair_rows(cursor, epoch_id=header.epoch_id) != expected_pairs:
        raise EventConflictError("durable admitted-pair set differs on replay")
    expected_sources = tuple(
        sorted(
            (
                (
                    pair.admitted_pair_digest,
                    source.root_job_id,
                    source.scope_contract_digest,
                    source.selection_digest,
                )
                for pair in plan.admitted_pairs
                for source in pair.sources
            ),
            key=lambda row: (row[0], row[1]),
        )
    )
    source_rows = tuple(
        tuple(row)
        for row in cursor.execute(
            """
            SELECT source.admitted_pair_digest, source.root_job_id,
                   source.scope_contract_digest, source.selection_digest
            FROM groundloop_m5_requirement_admitted_pair_source AS source
            JOIN groundloop_m5_requirement_admitted_pair AS admitted
              ON admitted.admitted_pair_digest = source.admitted_pair_digest
            WHERE admitted.epoch_id = %s
            ORDER BY source.admitted_pair_digest COLLATE "C",
                     source.root_job_id COLLATE "C"
            """,
            (header.epoch_id,),
        ).fetchall()
    )
    if source_rows != expected_sources:
        raise EventConflictError("durable admitted sources differ on replay")

    expected_child_by_id = {child.logical_job_id: child for child in child_jobs}
    child_rows = cursor.execute(
        _JOB_SELECT
        + " WHERE epoch_id = %s AND parent_job_id IS NOT NULL"
        + ' ORDER BY logical_job_id COLLATE "C"',
        (header.epoch_id,),
    ).fetchall()
    stored_children = tuple(_job_from_row(tuple(row)) for row in child_rows)
    if set(expected_child_by_id) != {
        child.spec.logical_job_id for child in stored_children
    }:
        raise EventConflictError("durable verifier child set differs on replay")
    admitted_by_pair = {pair.semantic_pair_digest: pair for pair in plan.admitted_pairs}
    for child in stored_children:
        expected_child = expected_child_by_id[child.spec.logical_job_id]
        assert expected_child.semantic_pair_digest is not None
        if (
            child.spec != expected_child
            or child.admitted_pair_digest
            != admitted_by_pair[
                expected_child.semantic_pair_digest
            ].admitted_pair_digest
        ):
            raise EventConflictError("durable verifier identity differs on replay")
    expected_dependencies = tuple(
        sorted(
            (
                (header.epoch_id, child.parent_job_id, child.logical_job_id)
                for child in child_jobs
            ),
            key=lambda row: (row[1], row[2]),
        )
    )
    dependency_rows = tuple(
        tuple(row)
        for row in cursor.execute(
            """
            SELECT epoch_id, parent_job_id, child_job_id
            FROM groundloop_m5_job_dependency
            WHERE epoch_id = %s
            ORDER BY parent_job_id COLLATE "C", child_job_id COLLATE "C"
            """,
            (header.epoch_id,),
        ).fetchall()
    )
    if dependency_rows != expected_dependencies:
        raise EventConflictError("durable root dependencies differ on replay")

    scope_by_root = {scope.root_job_id: scope for scope in scopes}
    for job in jobs:
        expected_completion = completions[job.spec.logical_job_id]
        if _stored_completion(job) != expected_completion:
            raise EventConflictError("durable root completion differs on replay")
        scope = scope_by_root[job.spec.logical_job_id]
        expected_scope_state = (
            M5ScopeState.CLOSED_ACTIVE
            if expected_completion.terminal_state is M5JobState.COMPLETED_ACTIVE
            else M5ScopeState.CLOSED_INACTIVE
        )
        if (
            scope.state is not expected_scope_state
            or scope.scope_closure_digest != expected_completion.scope_closure_digest
            or scope.child_set_hash != expected_completion.child_set_hash
            or scope.completion_digest != expected_completion.completion_digest
            or scope.closed_revision != job.completed_revision
        ):
            raise EventConflictError("durable root scope closure differs on replay")

    completed_revisions = {
        job.completed_revision for job in jobs if job.completed_revision is not None
    }
    if len(completed_revisions) != 1:
        raise ValidationError("closed root set does not share one barrier revision")
    completed_revision = next(iter(completed_revisions))
    work_names = M5RuntimeWork.counter_names()
    contribution = cursor.execute(
        f"""
        SELECT {", ".join(work_names)}, work_digest, source_identity_hash,
               contribution_key_digest, applied_revision
        FROM groundloop_m5_runtime_work_contribution
        WHERE epoch_id = %s AND contribution_kind = 'root_barrier'
          AND source_id = %s
        """,
        (header.epoch_id, header.structural_event_id),
    ).fetchone()
    if contribution is None:
        raise ValidationError("closed root barrier lacks its work contribution")
    work_end = len(work_names)
    stored_work = M5RuntimeWork(
        **dict(zip(work_names, map(int, contribution[:work_end]), strict=True)),
        work_digest=_text(contribution[work_end]),
    )
    expected_work = _derive_root_barrier_work(plan=plan, child_jobs=child_jobs)
    expected_key = digests.runtime_work_contribution_key_digest(
        epoch_id=header.epoch_id,
        contribution_kind=M5RuntimeWorkContributionKind.ROOT_BARRIER,
        source_id=header.structural_event_id,
    )
    if (
        stored_work != expected_work
        or _text(contribution[work_end + 1]) != plan.barrier_completion_hash
        or _text(contribution[work_end + 2]) != expected_key
        or int(contribution[work_end + 3]) != completed_revision
    ):
        raise EventConflictError("root-barrier replay changed immutable accounting")
    work_cutoff = cursor.execute(
        """
        SELECT updated_revision, terminalized
        FROM groundloop_m5_runtime_work_accumulator
        WHERE epoch_id = %s
        """,
        (header.epoch_id,),
    ).fetchone()
    if work_cutoff is None or (
        int(work_cutoff[0]) != header.revision
        or bool(work_cutoff[1]) != (header.runtime_state in {"sealed", "failed"})
    ):
        raise ValidationError("root-barrier work accumulator lost its cutoff")
    _validate_root_barrier_timing_replay(
        cursor,
        header=header,
        contribution_key_digest=expected_key,
        completed_revision=completed_revision,
    )
    expected_heads = _expected_frontier_heads(
        epoch_id=header.epoch_id,
        completed_revision=completed_revision,
        scopes=scopes,
        jobs=jobs,
        results=results,
        completions=completions,
    )
    for expected_head in expected_heads:
        current = _load_frontier_head(
            cursor,
            requirement_version_id=expected_head.requirement_version_id,
            candidate_policy_id=expected_head.candidate_policy_id,
            for_update=False,
        )
        if current is None:
            raise EventConflictError("durable forward frontier is missing on replay")
        try:
            selected = advance_requirement_frontier(expected_head, current)
        except ValidationError as exc:
            raise EventConflictError(
                "durable forward frontier differs on replay"
            ) from exc
        if selected != current:
            raise EventConflictError("durable forward frontier regressed on replay")
    return M5RootBarrierReceipt(
        requirement_root_set_hash=plan.requirement_root_set_hash,
        barrier_completion_hash=plan.barrier_completion_hash,
        resulting_revision=header.revision,
        exact_replay=True,
    )


def _recompute_pending_counters(
    cursor: Cursor[Any], *, epoch_id: int, resulting_revision: int
) -> None:
    cursor.execute(
        """
        WITH owner_universe AS (
            SELECT DISTINCT runtime.epoch_id, member.owner_claim_id
            FROM groundloop_m5_runtime_epoch AS runtime
            JOIN groundloop_m5_requirement_registry_snapshot_member AS member
              ON member.requirement_registry_snapshot_digest =
                 runtime.requirement_registry_snapshot_digest
            WHERE runtime.epoch_id = %s
        ),
        job_owner AS (
            SELECT job.epoch_id, owner.owner_claim_id,
                   CASE WHEN job.job_state IN (
                       'declared', 'running', 'retryable_failed'
                   ) AND job.job_kind = 'reverse_requirement_discovery'
                       THEN 1 ELSE 0 END AS broad_count,
                   CASE WHEN job.job_state IN (
                       'declared', 'running', 'retryable_failed'
                   ) AND job.job_kind = 'forward_requirement_retrieval'
                       THEN 1 ELSE 0 END AS forward_count,
                   CASE WHEN job.job_state IN (
                       'declared', 'running', 'retryable_failed'
                   ) AND job.job_kind = 'verify_requirement_pair'
                       THEN 1 ELSE 0 END AS verifier_count,
                   CASE WHEN job.job_state = 'terminal_failed'
                       THEN 1 ELSE 0 END AS failure_count
            FROM groundloop_m5_semantic_job AS job
            JOIN groundloop_m5_discovery_scope AS scope
              ON scope.scope_contract_digest = job.scope_contract_digest
            JOIN LATERAL (
                SELECT DISTINCT member.owner_claim_id
                FROM groundloop_m5_requirement_registry_snapshot_member AS member
                WHERE member.requirement_registry_snapshot_digest =
                      job.requirement_registry_snapshot_digest
                  AND (
                      job.job_kind = 'reverse_requirement_discovery'
                      OR (job.job_kind = 'forward_requirement_retrieval'
                          AND member.requirement_version_id =
                              scope.requirement_version_id)
                      OR (job.job_kind = 'verify_requirement_pair'
                          AND member.requirement_version_id = job.subject_id)
                  )
            ) AS owner ON true
            WHERE job.epoch_id = %s
              AND job.job_state IN (
                  'declared', 'running', 'retryable_failed', 'terminal_failed'
              )
        ),
        expected AS (
            SELECT owner_universe.epoch_id, owner_universe.owner_claim_id,
                   COALESCE(sum(job_owner.broad_count), 0)::bigint AS broad_count,
                   COALESCE(sum(job_owner.forward_count), 0)::bigint AS forward_count,
                   COALESCE(sum(job_owner.verifier_count), 0)::bigint AS verifier_count,
                   COALESCE(sum(job_owner.failure_count), 0)::bigint AS failure_count
            FROM owner_universe
            LEFT JOIN job_owner
              ON job_owner.epoch_id = owner_universe.epoch_id
             AND job_owner.owner_claim_id = owner_universe.owner_claim_id
            GROUP BY owner_universe.epoch_id, owner_universe.owner_claim_id
        )
        UPDATE groundloop_m5_owner_pending_counter AS counter
        SET broad_reverse_scope_count = expected.broad_count,
            forward_scope_count = expected.forward_count,
            verifier_job_count = expected.verifier_count,
            blocking_failure_count = expected.failure_count,
            updated_revision = %s
        FROM expected
        WHERE counter.epoch_id = expected.epoch_id
          AND counter.owner_claim_id = expected.owner_claim_id
        """,
        (epoch_id, epoch_id, resulting_revision),
    )
    cursor.execute(
        """
        WITH expected AS (
            SELECT owner.epoch_id, claim.answer_version_id,
                   sum(owner.broad_reverse_scope_count)::bigint AS broad_count,
                   sum(owner.forward_scope_count)::bigint AS forward_count,
                   sum(owner.verifier_job_count)::bigint AS verifier_count,
                   sum(owner.blocking_failure_count)::bigint AS failure_count
            FROM groundloop_m5_owner_pending_counter AS owner
            JOIN groundloop_claim AS claim
              ON claim.claim_id = owner.owner_claim_id AND claim.required
            WHERE owner.epoch_id = %s
            GROUP BY owner.epoch_id, claim.answer_version_id
        )
        UPDATE groundloop_m5_answer_pending_counter AS counter
        SET broad_reverse_scope_count = expected.broad_count,
            forward_scope_count = expected.forward_count,
            verifier_job_count = expected.verifier_count,
            blocking_failure_count = expected.failure_count,
            updated_revision = %s
        FROM expected
        WHERE counter.epoch_id = expected.epoch_id
          AND counter.answer_version_id = expected.answer_version_id
        """,
        (epoch_id, resulting_revision),
    )


def close_m5_requirement_roots(
    cursor: Cursor[Any],
    epoch_id: int,
    expected_revision: int,
    requirement_root_set_hash: str,
    *,
    failure_injector: RuntimeRootFailureInjector | None = None,
) -> M5RootBarrierReceipt:
    """Close the complete frozen root set and declare verifier children."""

    if (
        isinstance(expected_revision, bool)
        or not isinstance(expected_revision, int)
        or expected_revision < 1
    ):
        raise InvalidEventError("expected runtime revision must be positive")
    require_runtime_recovery_bundle(cursor)
    header = _lock_epoch(cursor, epoch_id)
    if expected_revision > header.revision:
        raise EventConflictError("expected revision is newer than durable runtime")
    if requirement_root_set_hash != header.requirement_root_set_hash:
        raise EventConflictError("requirement root-set hash differs from runtime")
    manifest = _load_manifest(cursor, header.candidate_policy_id)
    _lock_snapshot_headers(cursor, header=header)
    scopes = _read_all_root_scopes(cursor, epoch_id=epoch_id)
    jobs = _read_all_root_jobs(cursor, epoch_id=epoch_id)
    if not jobs:
        raise InvalidEventError("an empty root set has no closure barrier")
    root_ids = tuple(job.spec.logical_job_id for job in jobs)
    if tuple(scope.root_job_id for scope in scopes) != root_ids:
        raise ValidationError("frozen root scopes and root jobs are not a bijection")
    for scope, job in zip(scopes, jobs, strict=True):
        _validate_header_bindings(header, scope, job, manifest)

    results: list[M5RequirementDiscoveryResult] = []
    artifacts: list[_StoredAttemptResult] = []
    for scope, job in zip(scopes, jobs, strict=True):
        result = _load_discovery_result(cursor, root_job_id=job.spec.logical_job_id)
        if result is None:
            if scope.state in {
                M5ScopeState.OPEN,
                M5ScopeState.TERMINAL_FAILED,
                M5ScopeState.CANCELLED,
            } or job.state in {
                M5JobState.DECLARED,
                M5JobState.RUNNING,
                M5JobState.RETRYABLE_FAILED,
                M5JobState.TERMINAL_FAILED,
                M5JobState.CANCELLED,
            }:
                raise InvalidEventError(
                    "root barrier is incomplete or blocked by a non-staged root"
                )
            raise ValidationError("closed root lacks its staged discovery result")
        _validate_discovery_result(
            cursor,
            epoch_id=epoch_id,
            result=result,
            scope=scope,
            job=job,
            manifest=manifest,
            eligible_snapshot_exhausted=True,
        )
        artifact = _root_attempt_result(
            cursor, root_job_id=job.spec.logical_job_id, result=result
        )
        artifact.artifact.validate_job_shape(job.spec.job_kind)
        results.append(result)
        artifacts.append(artifact)
    ordered_results = tuple(results)
    ordered_artifacts = tuple(artifacts)
    plan, child_jobs, completions = _build_barrier(
        header=header,
        manifest=manifest,
        scopes=scopes,
        jobs=jobs,
        results=ordered_results,
        artifacts=ordered_artifacts,
    )
    if plan.requirement_root_set_hash != requirement_root_set_hash:
        raise EventConflictError("frozen root-set digest does not match durable roots")

    closed_states = {M5ScopeState.CLOSED_ACTIVE, M5ScopeState.CLOSED_INACTIVE}
    if all(scope.state in closed_states for scope in scopes):
        return _validate_barrier_replay(
            cursor,
            header=header,
            plan=plan,
            child_jobs=child_jobs,
            scopes=scopes,
            jobs=jobs,
            results=ordered_results,
            completions=completions,
        )
    if any(scope.state in closed_states for scope in scopes):
        raise EventConflictError("root barrier is only partially installed")
    if header.revision != expected_revision:
        raise EventConflictError("stale typed runtime revision")
    _require_pending_epoch(header)
    if any(scope.state is not M5ScopeState.RESULT_STAGED for scope in scopes):
        raise InvalidEventError("root barrier requires every scope result staged")
    if any(job.state is not M5JobState.RUNNING for job in jobs):
        raise InvalidEventError("root barrier requires every root job running")

    resulting_revision = header.revision + 1
    accounting_start = start_event_accounting(
        cursor, epoch_id=epoch_id, expected_revision=header.revision
    )
    _authorize(cursor, header)
    _inject(failure_injector, "root_barrier_authorized")
    for pair in plan.admitted_pairs:
        cursor.execute(
            """
            INSERT INTO groundloop_m5_requirement_admitted_pair (
                admitted_pair_digest, epoch_id, subject_kind, subject_id,
                chunk_version_id, semantic_pair_digest, candidate_policy_id,
                owner_root_job_id, reasons, mandatory_lineage
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                pair.admitted_pair_digest,
                pair.epoch_id,
                pair.pair.subject_kind.value,
                pair.pair.subject_id,
                pair.pair.chunk_version_id,
                pair.semantic_pair_digest,
                pair.candidate_policy_id,
                pair.owner_root_job_id,
                [reason.value for reason in pair.reasons],
                pair.mandatory_lineage,
            ),
        )
        for source in pair.sources:
            cursor.execute(
                """
                INSERT INTO groundloop_m5_requirement_admitted_pair_source (
                    admitted_pair_digest, root_job_id,
                    scope_contract_digest, selection_digest
                ) VALUES (%s, %s, %s, %s)
                """,
                (
                    pair.admitted_pair_digest,
                    source.root_job_id,
                    source.scope_contract_digest,
                    source.selection_digest,
                ),
            )
    _inject(failure_injector, "root_barrier_admitted_pairs_inserted")

    admitted_by_pair = {pair.semantic_pair_digest: pair for pair in plan.admitted_pairs}
    for child in child_jobs:
        assert child.pair is not None
        assert child.semantic_pair_digest is not None
        admitted = admitted_by_pair[child.semantic_pair_digest]
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
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, FALSE, %s, 'declared',
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                %s, NULL, NULL
            )
            """,
            (
                child.logical_job_id,
                epoch_id,
                child.structural_event_id,
                child.job_kind.value,
                child.candidate_policy_id,
                child.candidate_policy_manifest_hash,
                child.parent_job_id,
                child.pair.subject_kind.value,
                child.pair.subject_id,
                child.pair.chunk_version_id,
                child.semantic_pair_digest,
                admitted.admitted_pair_digest,
                child.scope_contract_digest,
                child.requirement_registry_snapshot_digest,
                child.active_chunk_snapshot_digest,
                child.role_template_hash,
                child.execution_spec_hash,
                child.payload_hash,
                resulting_revision,
            ),
        )
        cursor.execute(
            """
            INSERT INTO groundloop_m5_job_dependency (
                epoch_id, parent_job_id, child_job_id
            ) VALUES (%s, %s, %s)
            """,
            (epoch_id, child.parent_job_id, child.logical_job_id),
        )
    _inject(failure_injector, "root_barrier_children_inserted")

    for job in jobs:
        completion = completions[job.spec.logical_job_id]
        transitioned = cursor.execute(
            """
            UPDATE groundloop_m5_semantic_job
            SET job_state = %s, result_artifact_id = %s,
                result_artifact_hash = %s, scope_closure_digest = %s,
                child_set_hash = %s, archive_reason = %s,
                completion_digest = %s, completed_revision = %s,
                completed_at = now()
            WHERE epoch_id = %s AND logical_job_id = %s
              AND job_state = 'running'
            """,
            (
                completion.terminal_state.value,
                completion.result_artifact_id,
                completion.result_artifact_hash,
                completion.scope_closure_digest,
                completion.child_set_hash,
                (
                    None
                    if completion.archive_reason is None
                    else completion.archive_reason.value
                ),
                completion.completion_digest,
                resulting_revision,
                epoch_id,
                job.spec.logical_job_id,
            ),
        ).rowcount
        if transitioned != 1:
            raise EventConflictError("root completion lost its barrier race")
    _inject(failure_injector, "root_barrier_jobs_completed")

    for scope in scopes:
        completion = completions[scope.root_job_id]
        scope_state = (
            M5ScopeState.CLOSED_ACTIVE
            if completion.terminal_state is M5JobState.COMPLETED_ACTIVE
            else M5ScopeState.CLOSED_INACTIVE
        )
        transitioned = cursor.execute(
            """
            UPDATE groundloop_m5_discovery_scope
            SET scope_state = %s, scope_closure_digest = %s,
                child_set_hash = %s, completion_digest = %s,
                closed_revision = %s, closed_at = now()
            WHERE epoch_id = %s AND root_job_id = %s
              AND scope_state = 'result_staged'
            """,
            (
                scope_state.value,
                completion.scope_closure_digest,
                completion.child_set_hash,
                completion.completion_digest,
                resulting_revision,
                epoch_id,
                scope.root_job_id,
            ),
        ).rowcount
        if transitioned != 1:
            raise EventConflictError("root scope closure lost its barrier race")
    _inject(failure_injector, "root_barrier_scopes_closed")

    heads = _expected_frontier_heads(
        epoch_id=epoch_id,
        completed_revision=resulting_revision,
        scopes=scopes,
        jobs=jobs,
        results=ordered_results,
        completions=completions,
    )
    for candidate in heads:
        current = _load_frontier_head(
            cursor,
            requirement_version_id=candidate.requirement_version_id,
            candidate_policy_id=candidate.candidate_policy_id,
            for_update=True,
        )
        selected = advance_requirement_frontier(current, candidate)
        if current == selected:
            continue
        if current is None:
            cursor.execute(
                """
                INSERT INTO groundloop_m5_requirement_frontier_head (
                    requirement_version_id, candidate_policy_id,
                    latest_root_job_id, latest_scope_contract_digest,
                    latest_active_chunk_snapshot_digest,
                    latest_discovery_result_artifact_hash,
                    latest_scope_closure_digest, latest_completion_digest,
                    completed_epoch_id, completed_revision
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    selected.requirement_version_id,
                    selected.candidate_policy_id,
                    selected.latest_root_job_id,
                    selected.latest_scope_contract_digest,
                    selected.latest_active_chunk_snapshot_digest,
                    selected.latest_discovery_result_artifact_hash,
                    selected.latest_scope_closure_digest,
                    selected.latest_completion_digest,
                    selected.completed_epoch_id,
                    selected.completed_revision,
                ),
            )
        else:
            cursor.execute(
                """
                UPDATE groundloop_m5_requirement_frontier_head
                SET latest_root_job_id = %s,
                    latest_scope_contract_digest = %s,
                    latest_active_chunk_snapshot_digest = %s,
                    latest_discovery_result_artifact_hash = %s,
                    latest_scope_closure_digest = %s,
                    latest_completion_digest = %s,
                    completed_epoch_id = %s, completed_revision = %s,
                    updated_at = now()
                WHERE requirement_version_id = %s AND candidate_policy_id = %s
                """,
                (
                    selected.latest_root_job_id,
                    selected.latest_scope_contract_digest,
                    selected.latest_active_chunk_snapshot_digest,
                    selected.latest_discovery_result_artifact_hash,
                    selected.latest_scope_closure_digest,
                    selected.latest_completion_digest,
                    selected.completed_epoch_id,
                    selected.completed_revision,
                    selected.requirement_version_id,
                    selected.candidate_policy_id,
                ),
            )
    _inject(failure_injector, "root_barrier_frontiers_updated")

    _lock_and_validate_pending_counter_revisions(
        cursor, epoch_id=epoch_id, expected_revision=header.revision
    )
    _recompute_pending_counters(
        cursor, epoch_id=epoch_id, resulting_revision=resulting_revision
    )
    _inject(failure_injector, "root_barrier_pending_recomputed")
    _advance_revision(
        cursor,
        header=header,
        resulting_revision=resulting_revision,
        pending_revision_already_updated=True,
    )
    _inject(failure_injector, "root_barrier_revision_advanced")
    barrier_work = _derive_root_barrier_work(plan=plan, child_jobs=child_jobs)
    anchor = persist_root_barrier_contribution(
        cursor,
        epoch_id=epoch_id,
        structural_event_id=header.structural_event_id,
        barrier_completion_hash=plan.barrier_completion_hash,
        resulting_revision=resulting_revision,
        barrier_work=barrier_work,
    )
    _inject(failure_injector, "root_barrier_contribution_inserted")
    finish_root_barrier_accounting(
        cursor,
        epoch_id=epoch_id,
        expected_revision=header.revision,
        start=accounting_start,
        anchor=anchor,
        barrier_work=barrier_work,
    )
    _inject(failure_injector, "root_barrier_accounting_finished")
    _force_deferred_validation(cursor)
    _inject(failure_injector, "root_barrier_constraints_validated")
    return M5RootBarrierReceipt(
        requirement_root_set_hash=requirement_root_set_hash,
        barrier_completion_hash=plan.barrier_completion_hash,
        resulting_revision=resulting_revision,
        exact_replay=False,
    )


def _persist_new_cancellation(
    cursor: Cursor[Any],
    *,
    header: _EpochHeader,
    plan: M5CancellationPlan,
    scopes: tuple[_StoredScope, ...],
    jobs: tuple[_StoredJob, ...],
    owner_deltas: tuple[_PendingDelta, ...],
    answer_deltas: tuple[_PendingDelta, ...],
    failure_injector: RuntimeRootFailureInjector | None,
) -> M5CancellationReceipt:
    resulting_revision = header.revision + 1
    accounting_start = start_event_accounting(
        cursor, epoch_id=header.epoch_id, expected_revision=header.revision
    )
    _authorize(cursor, header)
    _inject(failure_injector, "cancellation_authorized")
    settled_row = cursor.execute("SELECT clock_timestamp()").fetchone()
    if settled_row is None or not isinstance(settled_row[0], datetime):
        raise ValidationError("PostgreSQL did not return a cancellation timestamp")
    settled_at = settled_row[0]
    completions = {
        job.spec.logical_job_id: M5JobCompletion.build(
            job=job.spec,
            terminal_state=M5JobState.CANCELLED,
            archive_reason=plan.reason,
        )
        for job in jobs
    }
    for job in jobs:
        completion = completions[job.spec.logical_job_id]
        updated = cursor.execute(
            """
            UPDATE groundloop_m5_semantic_job
            SET job_state = 'cancelled', archive_reason = %s,
                result_artifact_id = NULL, result_artifact_hash = NULL,
                scope_closure_digest = NULL, child_set_hash = NULL,
                completion_digest = %s, cancelled_by_event_id = %s,
                cancelled_by_epoch_id = %s, cancellation_reason = %s,
                completed_revision = %s, completed_at = %s
            WHERE epoch_id = %s AND logical_job_id = %s AND job_state = %s
            """,
            (
                plan.reason.value,
                completion.completion_digest,
                plan.structural_event_id,
                plan.epoch_id,
                plan.reason.value,
                resulting_revision,
                settled_at,
                header.epoch_id,
                job.spec.logical_job_id,
                job.state.value,
            ),
        ).rowcount
        if updated != 1:
            raise EventConflictError("semantic job lost its cancellation race")
    _inject(failure_injector, "cancellation_jobs_closed")

    for scope in scopes:
        completion = completions[scope.root_job_id]
        updated = cursor.execute(
            """
            UPDATE groundloop_m5_discovery_scope
            SET scope_state = 'cancelled', completion_digest = %s,
                closed_revision = %s, closed_at = %s
            WHERE epoch_id = %s AND root_job_id = %s AND scope_state = %s
            """,
            (
                completion.completion_digest,
                resulting_revision,
                settled_at,
                header.epoch_id,
                scope.root_job_id,
                scope.state.value,
            ),
        ).rowcount
        if updated != 1:
            raise EventConflictError("root scope lost its cancellation race")
    _inject(failure_injector, "cancellation_scopes_closed")

    _apply_cancellation_pending_deltas(
        cursor,
        epoch_id=header.epoch_id,
        expected_revision=header.revision,
        resulting_revision=resulting_revision,
        owner_deltas=owner_deltas,
        answer_deltas=answer_deltas,
    )
    _inject(failure_injector, "cancellation_pending_recomputed")
    _advance_cancellation_revision(
        cursor,
        header=header,
        resulting_revision=resulting_revision,
        cancelled_job_count=len(jobs),
        cancelled_root_count=len(scopes),
    )
    _inject(failure_injector, "cancellation_revision_advanced")

    cancellation_work = M5RuntimeWork(
        requirement_cancelled_job_count=len(plan.cancelled_job_ids)
    )
    anchor = persist_cancellation_contribution(
        cursor,
        epoch_id=header.epoch_id,
        plan_digest=plan.plan_digest,
        resulting_revision=resulting_revision,
        cancellation_work=cancellation_work,
    )
    _inject(failure_injector, "cancellation_contribution_inserted")
    finish_cancellation_accounting(
        cursor,
        epoch_id=header.epoch_id,
        expected_revision=header.revision,
        start=accounting_start,
        anchor=anchor,
        cancellation_work=cancellation_work,
    )
    _inject(failure_injector, "cancellation_accounting_finished")
    _force_deferred_validation(cursor)
    _inject(failure_injector, "cancellation_constraints_validated")
    return M5CancellationReceipt(plan.cancelled_job_ids, resulting_revision, False)


def _classify_epoch_failure_header(
    header: _EpochHeader, expected_revision: int
) -> bool:
    if expected_revision > header.revision:
        raise EventConflictError("expected revision is newer than durable runtime")
    pending_shape = (
        header.structural_status,
        header.semantic_status,
        header.evaluation_state,
    ) == ("committed", "pending", "pending") and header.runtime_state in {
        "structural_committed",
        "semantic_pending",
    }
    complete_shape = (
        header.structural_status,
        header.semantic_status,
        header.evaluation_state,
        header.runtime_state,
    ) == ("committed", "complete", "complete", "semantic_complete")
    if pending_shape or complete_shape:
        if expected_revision != header.revision:
            raise EventConflictError("stale typed runtime revision")
        return False
    failed_shape = (
        header.structural_status,
        header.semantic_status,
        header.evaluation_state,
        header.runtime_state,
    ) == ("failed", "failed", "failed", "failed")
    if failed_shape:
        return True
    raise InvalidEventError("epoch failure requires an active or failed typed epoch")


def lock_m5_requirement_scopes_for_epoch_failure(
    cursor: Cursor[Any],
    header: _EpochHeader,
    expected_revision: int,
) -> M5EpochFailureScopeLocks:
    """Lock tier-8 snapshot/scope rows after the outer prefix is held."""

    if type(header) is not _EpochHeader:
        raise ValidationError("epoch-failure header has another type")
    terminal_replay = _classify_epoch_failure_header(header, expected_revision)
    manifest = _load_manifest(cursor, header.candidate_policy_id)
    _lock_snapshot_headers(cursor, header=header)
    scopes = _read_all_root_scopes(cursor, epoch_id=header.epoch_id)
    authority = _EpochFailurePlanAuthority(cursor)
    locks = M5EpochFailureScopeLocks(
        header,
        expected_revision,
        terminal_replay,
        manifest,
        scopes,
        authority,
    )
    authority.scopes = locks
    authority.scope_snapshot = _scope_lock_snapshot(locks)
    return locks


def _require_scope_lock_authority(
    cursor: Cursor[Any], locks: M5EpochFailureScopeLocks
) -> _EpochFailurePlanAuthority:
    if (
        type(locks) is not M5EpochFailureScopeLocks
        or type(locks._authority) is not _EpochFailurePlanAuthority
        or locks._authority.cursor is not cursor
        or locks._authority.transaction_identity
        != _epoch_failure_transaction_identity(cursor)
        or locks._authority.scopes is not locks
        or locks._authority.scope_snapshot != _scope_lock_snapshot(locks)
        or locks._authority.applied
    ):
        raise ValidationError("epoch-failure scope locks are stale or copied")
    return locks._authority


def lock_m5_requirement_jobs_for_epoch_failure(
    cursor: Cursor[Any],
    scope_locks: M5EpochFailureScopeLocks,
    *,
    failure_injector: RuntimeRootFailureInjector | None = None,
) -> M5EpochFailureJobLocks:
    """Lock the complete tier-9 M5 job set after the M4 job snapshot."""

    authority = _require_scope_lock_authority(cursor, scope_locks)
    if authority.jobs is not None:
        raise ValidationError("epoch-failure M5 jobs were already locked")
    header = scope_locks.header
    rows = cursor.execute(
        _JOB_SELECT
        + ' WHERE epoch_id = %s ORDER BY logical_job_id COLLATE "C" FOR UPDATE',
        (header.epoch_id,),
    ).fetchall()
    jobs = tuple(_job_from_row(tuple(row)) for row in rows)
    _validate_cancellation_bindings(
        header=header,
        manifest=scope_locks.manifest,
        scopes=scope_locks.scopes,
        jobs=jobs,
    )
    locks = M5EpochFailureJobLocks(scope_locks, jobs, authority)
    authority.jobs = locks
    authority.job_snapshot = _job_lock_snapshot(locks)
    _inject(failure_injector, "typed_fail_jobs_locked")
    return locks


def _require_job_lock_authority(
    cursor: Cursor[Any], locks: M5EpochFailureJobLocks
) -> _EpochFailurePlanAuthority:
    if (
        type(locks) is not M5EpochFailureJobLocks
        or type(locks._authority) is not _EpochFailurePlanAuthority
        or locks._authority.cursor is not cursor
        or locks._authority.transaction_identity
        != _epoch_failure_transaction_identity(cursor)
        or locks._authority.jobs is not locks
        or locks.scope_locks._authority is not locks._authority
        or locks._authority.scopes is not locks.scope_locks
        or locks._authority.scope_snapshot != _scope_lock_snapshot(locks.scope_locks)
        or locks._authority.job_snapshot != _job_lock_snapshot(locks)
        or locks._authority.applied
    ):
        raise ValidationError("epoch-failure M5 job locks are stale or copied")
    return locks._authority


def lock_m5_requirement_details_for_epoch_failure(
    cursor: Cursor[Any],
    job_locks: M5EpochFailureJobLocks,
    *,
    failure_injector: RuntimeRootFailureInjector | None = None,
) -> M5EpochFailureLockedPlan:
    """Lock all M5 tier-10+ details and derive a no-write failure plan."""

    authority = _require_job_lock_authority(cursor, job_locks)
    if authority.details is not None:
        raise ValidationError("epoch-failure M5 details were already locked")
    scope_locks = job_locks.scope_locks
    header = scope_locks.header
    expected_revision = header.revision
    attempt_rows = cursor.execute(
        """
        SELECT attempt.attempt_id, attempt.logical_job_id,
               attempt.attempt_ordinal, attempt.execution_spec_hash,
               attempt.lease_token_hash, attempt.attempt_state,
               attempt.attempt_output_digest
        FROM groundloop_m5_job_attempt AS attempt
        JOIN groundloop_m5_semantic_job AS job
          ON job.logical_job_id = attempt.logical_job_id
        WHERE job.epoch_id = %s
        ORDER BY job.logical_job_id COLLATE "C", attempt.attempt_ordinal,
                 attempt.attempt_id COLLATE "C"
        FOR UPDATE OF attempt
        """,
        (header.epoch_id,),
    ).fetchall()
    attempts = tuple(
        _StoredAttempt(
            attempt=M5JobAttempt(
                attempt_id=_text(row[0]),
                logical_job_id=_text(row[1]),
                attempt_ordinal=int(row[2]),
                execution_spec_hash=_text(row[3]),
                lease_token_hash=_text(row[4]),
            ),
            state=str(row[5]),
            attempt_output_digest=_optional_text(row[6]),
        )
        for row in attempt_rows
    )
    _inject(failure_injector, "typed_fail_attempts_locked")

    terminal_replay = scope_locks.terminal_replay
    open_job_states = {
        M5JobState.DECLARED,
        M5JobState.RUNNING,
        M5JobState.RETRYABLE_FAILED,
    }
    open_scope_states = {M5ScopeState.OPEN, M5ScopeState.RESULT_STAGED}
    if terminal_replay:
        if any(job.state in open_job_states for job in job_locks.jobs) or any(
            scope.state in open_scope_states for scope in scope_locks.scopes
        ):
            raise ValidationError("failed epoch retains open requirement work")
        selected_jobs = tuple(
            job
            for job in job_locks.jobs
            if job.state is M5JobState.CANCELLED
            and job.archive_reason is M5TerminalReason.EPOCH_FAILED
            and job.cancelled_by_event_id == header.structural_event_id
            and job.cancelled_by_epoch_id == header.epoch_id
            and job.cancellation_reason is M5TerminalReason.EPOCH_FAILED
            and job.completed_revision == header.revision
        )
    else:
        selected_jobs = tuple(
            job for job in job_locks.jobs if job.state in open_job_states
        )
    selected_root_ids = {
        job.spec.logical_job_id
        for job in selected_jobs
        if job.spec.parent_job_id is None
    }
    if terminal_replay:
        selected_scopes = tuple(
            scope
            for scope in scope_locks.scopes
            if scope.root_job_id in selected_root_ids
        )
        if any(
            scope.state is not M5ScopeState.CANCELLED
            or scope.closed_revision != header.revision
            for scope in selected_scopes
        ):
            raise ValidationError(
                "failed epoch cancellation scopes differ from their job image"
            )
    else:
        selected_scopes = tuple(
            scope for scope in scope_locks.scopes if scope.state in open_scope_states
        )
    if {scope.root_job_id for scope in selected_scopes} != selected_root_ids:
        raise ValidationError("open requirement root jobs and scopes diverged")
    terminal_failed_count = sum(
        job.state is M5JobState.TERMINAL_FAILED for job in job_locks.jobs
    )
    if (
        header.open_work_count != (0 if terminal_replay else len(selected_jobs))
        or header.open_scope_count != (0 if terminal_replay else len(selected_scopes))
        or header.blocking_failure_count != terminal_failed_count
    ):
        raise ValidationError("runtime requirement counts are not exact")

    accounting_start = lock_epoch_failure_accounting(
        cursor,
        epoch_id=header.epoch_id,
        expected_revision=expected_revision,
        terminal_replay=terminal_replay,
    )
    _inject(failure_injector, "typed_fail_accounting_started")
    if not terminal_replay:
        _authorize(cursor, header)
        _inject(failure_injector, "typed_fail_authorized")
    cancellation_plan = (
        None
        if not selected_jobs
        else M5CancellationPlan.build(
            structural_event_id=header.structural_event_id,
            epoch_id=header.epoch_id,
            cancelled_job_ids=tuple(job.spec.logical_job_id for job in selected_jobs),
            reason=M5TerminalReason.EPOCH_FAILED,
        )
    )
    completions = tuple(
        (
            job.spec.logical_job_id,
            M5JobCompletion.build(
                job=job.spec,
                terminal_state=M5JobState.CANCELLED,
                archive_reason=M5TerminalReason.EPOCH_FAILED,
            ),
        )
        for job in selected_jobs
    )
    if terminal_replay:
        for job, (_job_id, completion) in zip(selected_jobs, completions, strict=True):
            if job.completion_digest != completion.completion_digest:
                raise ValidationError(
                    "failed epoch cancellation completion is not canonical"
                )
        completion_by_id = dict(completions)
        if any(
            scope.completion_digest
            != completion_by_id[scope.root_job_id].completion_digest
            for scope in selected_scopes
        ):
            raise ValidationError(
                "failed epoch scope and cancellation completion diverged"
            )
        owner_deltas: tuple[_PendingDelta, ...] = ()
        answer_deltas: tuple[_PendingDelta, ...] = ()
        settled_at = None
    else:
        owner_deltas, answer_deltas = _cancellation_pending_projection(
            cursor,
            header=header,
            scopes=scope_locks.scopes,
            jobs=selected_jobs,
        )
        settled_row = cursor.execute("SELECT clock_timestamp()").fetchone()
        if settled_row is None or type(settled_row[0]) is not datetime:
            raise ValidationError("PostgreSQL did not return a failure timestamp")
        settled_at = settled_row[0]
    pending_counts = _lock_and_validate_pending_counter_revisions(
        cursor,
        epoch_id=header.epoch_id,
        expected_revision=expected_revision,
    )
    cancellation_work = M5RuntimeWork(
        requirement_cancelled_job_count=len(selected_jobs)
    )
    if terminal_replay and cancellation_plan is not None:
        work_names = M5RuntimeWork.counter_names()
        contribution = cursor.execute(
            f"""
            SELECT {", ".join(work_names)}, work_digest,
                   source_identity_hash, contribution_key_digest,
                   applied_revision
            FROM groundloop_m5_runtime_work_contribution
            WHERE epoch_id = %s AND contribution_kind = 'cancellation'
              AND source_id = %s
            FOR UPDATE
            """,
            (header.epoch_id, cancellation_plan.plan_digest),
        ).fetchone()
        work_end = len(work_names)
        expected_key = digests.runtime_work_contribution_key_digest(
            epoch_id=header.epoch_id,
            contribution_kind=M5RuntimeWorkContributionKind.CANCELLATION,
            source_id=cancellation_plan.plan_digest,
        )
        if contribution is None:
            raise ValidationError("failed epoch lacks its cancellation contribution")
        stored_work = M5RuntimeWork(
            **dict(
                zip(
                    work_names,
                    map(int, contribution[:work_end]),
                    strict=True,
                )
            ),
            work_digest=_text(contribution[work_end]),
        )
        if (
            stored_work != cancellation_work
            or _text(contribution[work_end + 1]) != cancellation_plan.plan_digest
            or _text(contribution[work_end + 2]) != expected_key
            or int(contribution[work_end + 3]) != header.revision
        ):
            raise ValidationError("failed epoch cancellation contribution changed")
    plan = M5EpochFailureLockedPlan(
        job_locks=job_locks,
        attempts=attempts,
        selected_jobs=selected_jobs,
        selected_scopes=selected_scopes,
        terminal_failed_count=terminal_failed_count,
        accounting_start=accounting_start,
        cancellation_plan=cancellation_plan,
        cancellation_work=cancellation_work,
        settled_at=settled_at,
        completions=completions,
        owner_deltas=owner_deltas,
        answer_deltas=answer_deltas,
        pending_counter_row_counts=pending_counts,
        terminal_replay=terminal_replay,
        _authority=authority,
    )
    authority.details = plan
    authority.detail_snapshot = _detail_plan_snapshot(plan)
    return plan


def apply_m5_requirement_epoch_failure(
    cursor: Cursor[Any],
    plan: M5EpochFailureLockedPlan,
    *,
    failure_injector: RuntimeRootFailureInjector | None = None,
) -> M5EpochFailureRequirementClosure:
    """Consume one authentic detail plan without acquiring another lock."""

    if (
        type(plan) is not M5EpochFailureLockedPlan
        or type(plan._authority) is not _EpochFailurePlanAuthority
        or plan._authority.cursor is not cursor
        or plan._authority.transaction_identity
        != _epoch_failure_transaction_identity(cursor)
        or plan._authority.details is not plan
        or plan._authority.jobs is not plan.job_locks
        or plan._authority.scopes is not plan.job_locks.scope_locks
        or plan._authority.scope_snapshot
        != _scope_lock_snapshot(plan.job_locks.scope_locks)
        or plan._authority.job_snapshot != _job_lock_snapshot(plan.job_locks)
        or plan._authority.detail_snapshot != _detail_plan_snapshot(plan)
        or plan._authority.applied
    ):
        raise ValidationError("epoch-failure M5 detail plan is stale or copied")
    if plan.terminal_replay:
        raise EventConflictError("terminal replay M5 plan is read-only")
    if plan.settled_at is None:
        raise ValidationError("active epoch-failure plan lacks its settlement time")
    authority = plan._authority
    header = plan.job_locks.scope_locks.header
    expected_revision = header.revision
    derived_selected_jobs = tuple(
        job
        for job in plan.job_locks.jobs
        if job.state
        in {
            M5JobState.DECLARED,
            M5JobState.RUNNING,
            M5JobState.RETRYABLE_FAILED,
        }
    )
    derived_selected_scopes = tuple(
        scope
        for scope in plan.job_locks.scope_locks.scopes
        if scope.state in {M5ScopeState.OPEN, M5ScopeState.RESULT_STAGED}
    )
    if (
        plan.selected_jobs != derived_selected_jobs
        or plan.selected_scopes != derived_selected_scopes
        or tuple(job_id for job_id, _completion in plan.completions)
        != tuple(job.spec.logical_job_id for job in plan.selected_jobs)
        or plan.cancellation_work.requirement_cancelled_job_count
        != len(plan.selected_jobs)
    ):
        raise ValidationError("epoch-failure M5 detail plan was mutated")
    completions = dict(plan.completions)
    authority.applied = True

    for job in plan.selected_jobs:
        completion = completions[job.spec.logical_job_id]
        updated = cursor.execute(
            """
            UPDATE groundloop_m5_semantic_job
            SET job_state = 'cancelled', archive_reason = 'epoch_failed',
                result_artifact_id = NULL, result_artifact_hash = NULL,
                scope_closure_digest = NULL, child_set_hash = NULL,
                completion_digest = %s, cancelled_by_event_id = %s,
                cancelled_by_epoch_id = %s,
                cancellation_reason = 'epoch_failed',
                completed_revision = %s, completed_at = %s
            WHERE epoch_id = %s AND logical_job_id = %s AND job_state = %s
            """,
            (
                completion.completion_digest,
                header.structural_event_id,
                header.epoch_id,
                expected_revision + 1,
                plan.settled_at,
                header.epoch_id,
                job.spec.logical_job_id,
                job.state.value,
            ),
        ).rowcount
        if updated != 1:
            raise EventConflictError("semantic job lost its epoch-failure race")
    _inject(failure_injector, "typed_fail_jobs_cancelled")

    for scope in plan.selected_scopes:
        completion = completions[scope.root_job_id]
        updated = cursor.execute(
            """
            UPDATE groundloop_m5_discovery_scope
            SET scope_state = 'cancelled', completion_digest = %s,
                closed_revision = %s, closed_at = %s
            WHERE epoch_id = %s AND root_job_id = %s AND scope_state = %s
            """,
            (
                completion.completion_digest,
                expected_revision + 1,
                plan.settled_at,
                header.epoch_id,
                scope.root_job_id,
                scope.state.value,
            ),
        ).rowcount
        if updated != 1:
            raise EventConflictError("root scope lost its epoch-failure race")
    _inject(failure_injector, "typed_fail_scopes_closed")

    _apply_cancellation_pending_deltas(
        cursor,
        epoch_id=header.epoch_id,
        expected_revision=expected_revision,
        resulting_revision=expected_revision + 1,
        owner_deltas=plan.owner_deltas,
        answer_deltas=plan.answer_deltas,
        locked_row_counts=plan.pending_counter_row_counts,
    )
    _inject(failure_injector, "typed_fail_counters_updated")
    counts = cursor.execute(
        """
        SELECT count(*) FILTER (
                   WHERE job_state IN ('declared', 'running', 'retryable_failed')
               ),
               count(*) FILTER (WHERE job_state = 'terminal_failed'),
               (SELECT count(*)
                FROM groundloop_m5_discovery_scope
                WHERE epoch_id = %s
                  AND scope_state IN ('open', 'result_staged'))
        FROM groundloop_m5_semantic_job
        WHERE epoch_id = %s
        """,
        (header.epoch_id, header.epoch_id),
    ).fetchone()
    if counts is None:
        raise ValidationError("failed to read terminal requirement counts")
    exact_counts = tuple(map(int, counts))
    if exact_counts != (0, plan.terminal_failed_count, 0):
        raise ValidationError("epoch failure did not close exact requirement counts")
    return M5EpochFailureRequirementClosure(
        accounting_start=plan.accounting_start,
        cancellation_plan=plan.cancellation_plan,
        cancellation_work=plan.cancellation_work,
        open_work_count=exact_counts[0],
        blocking_failure_count=exact_counts[1],
        open_scope_count=exact_counts[2],
        _authority=authority,
    )


def terminalize_m5_requirement_work_for_epoch_failure(
    cursor: Cursor[Any],
    epoch_id: int,
    expected_revision: int,
    *,
    failure_injector: RuntimeRootFailureInjector | None = None,
) -> M5EpochFailureRequirementClosure:
    """Compatibility wrapper over the C7 read-plan/write-apply phases."""

    if (
        isinstance(expected_revision, bool)
        or not isinstance(expected_revision, int)
        or expected_revision < 1
    ):
        raise InvalidEventError("expected runtime revision must be positive")
    header = _lock_epoch(cursor, epoch_id)
    scope_locks = lock_m5_requirement_scopes_for_epoch_failure(
        cursor, header, expected_revision
    )
    job_locks = lock_m5_requirement_jobs_for_epoch_failure(
        cursor, scope_locks, failure_injector=failure_injector
    )
    details = lock_m5_requirement_details_for_epoch_failure(
        cursor, job_locks, failure_injector=failure_injector
    )
    return apply_m5_requirement_epoch_failure(
        cursor, details, failure_injector=failure_injector
    )


def cancel_m5_work(
    cursor: Cursor[Any],
    epoch_id: int,
    expected_revision: int,
    cancellation_plan: M5CancellationPlan,
    *,
    failure_injector: RuntimeRootFailureInjector | None = None,
) -> M5CancellationReceipt:
    """Cancel one exact sorted batch of open requirement jobs and root scopes."""

    if (
        isinstance(expected_revision, bool)
        or not isinstance(expected_revision, int)
        or expected_revision < 1
    ):
        raise InvalidEventError("expected runtime revision must be positive")
    if not isinstance(cancellation_plan, M5CancellationPlan):
        raise ValidationError("cancellation_plan must be an M5CancellationPlan")
    require_runtime_recovery_bundle(cursor)
    header = _lock_epoch(cursor, epoch_id)
    if expected_revision > header.revision:
        raise EventConflictError("expected revision is newer than durable runtime")
    if (
        cancellation_plan.epoch_id != header.epoch_id
        or cancellation_plan.structural_event_id != header.structural_event_id
    ):
        raise EventConflictError("cancellation plan belongs to another typed epoch")
    manifest = _load_manifest(cursor, header.candidate_policy_id)
    scopes = _lock_cancellation_scopes(
        cursor,
        epoch_id=header.epoch_id,
        logical_job_ids=cancellation_plan.cancelled_job_ids,
    )
    jobs = _lock_cancellation_jobs(
        cursor,
        epoch_id=header.epoch_id,
        logical_job_ids=cancellation_plan.cancelled_job_ids,
    )
    _validate_cancellation_bindings(
        header=header,
        manifest=manifest,
        scopes=scopes,
        jobs=jobs,
    )
    replay = _validate_cancellation_replay(
        cursor,
        header=header,
        plan=cancellation_plan,
        scopes=scopes,
        jobs=jobs,
    )
    if replay is not None:
        return replay

    _require_pending_epoch(header)
    if header.revision != expected_revision:
        raise EventConflictError("stale typed runtime revision")
    open_job_states = {
        M5JobState.DECLARED,
        M5JobState.RUNNING,
        M5JobState.RETRYABLE_FAILED,
    }
    if any(job.state not in open_job_states for job in jobs):
        raise EventConflictError("cancellation plan does not name only open jobs")
    open_scope_states = {M5ScopeState.OPEN, M5ScopeState.RESULT_STAGED}
    if any(scope.state not in open_scope_states for scope in scopes):
        raise EventConflictError("cancellation plan does not name only open scopes")
    owner_deltas, answer_deltas = _cancellation_pending_projection(
        cursor,
        header=header,
        scopes=scopes,
        jobs=jobs,
    )
    return _persist_new_cancellation(
        cursor,
        header=header,
        plan=cancellation_plan,
        scopes=scopes,
        jobs=jobs,
        owner_deltas=owner_deltas,
        answer_deltas=answer_deltas,
        failure_injector=failure_injector,
    )


__all__ = [
    "RuntimeRootFailureInjector",
    "cancel_m5_work",
    "close_m5_requirement_roots",
    "stage_m5_discovery_result",
]
