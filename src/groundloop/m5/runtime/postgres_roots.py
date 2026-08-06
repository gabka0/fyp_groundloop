"""Cursor-local PostgreSQL transitions for M5 requirement roots.

The functions in this module deliberately do not own a transaction.  The
runtime persistence adapter supplies a cursor whose transaction spans the
shared ``groundloop_epoch`` and typed M5 runtime rows.  This keeps root-result
staging and the event-wide closure barrier composable with the coordinator's
final direct/M5 readiness projection.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from psycopg import Cursor

from groundloop.domain import SubjectKind
from groundloop.errors import EventConflictError, InvalidEventError, ValidationError
from groundloop.m4.contracts import VectorIndexKind
from groundloop.m5.runtime.contracts import (
    M5AttemptArchiveReason,
    M5AttemptCompletionReceipt,
    M5AttemptDisposition,
    M5AttemptOutput,
    M5AttemptResultArtifact,
    M5CandidatePolicyManifest,
    M5DiscoveryDirection,
    M5DiscoveryScopeContract,
    M5JobAttempt,
    M5JobCompletion,
    M5JobKind,
    M5JobLease,
    M5JobState,
    M5LogicalJobSpec,
    M5RequirementAdmissionChannel,
    M5RequirementChannelHit,
    M5RequirementDiscoveryResult,
    M5RequirementFrontierHead,
    M5RequirementScopeSelection,
    M5RetrievalTermination,
    M5RootBarrierReceipt,
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


@dataclass(frozen=True, slots=True)
class _StoredAttempt:
    attempt: M5JobAttempt
    state: str
    attempt_output_digest: str | None


@dataclass(frozen=True, slots=True)
class _StoredAttemptResult:
    artifact: M5AttemptResultArtifact
    output: M5AttemptOutput


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
               active_chunk_snapshot_digest, requirement_root_set_hash
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
           created_revision, completed_revision
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
        pair = SemanticPairKey(
            SubjectKind(str(row[7])), str(row[8]), str(row[9])
        )
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
        archive_reason=(
            None if row[24] is None else M5TerminalReason(str(row[24]))
        ),
        completion_digest=_optional_text(row[25]),
        created_revision=int(row[26]),
        completed_revision=None if row[27] is None else int(row[27]),
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
        or job.spec.active_chunk_snapshot_digest
        != header.active_chunk_snapshot_digest
        or job.spec.structural_event_id != header.structural_event_id
    ):
        raise ValidationError("root identity is outside its frozen runtime epoch")
    if (
        job.spec.logical_job_id != scope.root_job_id
        or job.spec.scope_contract_digest != scope.contract.scope_contract_digest
    ):
        raise ValidationError("root job and discovery scope identities diverged")
    job.spec.validate_manifest_and_scope(manifest, scope.contract)


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
) -> None:
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
        eligible_snapshot_exhausted=(
            result.termination is M5RetrievalTermination.SNAPSHOT_EXHAUSTED
        ),
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
            pair=SemanticPairKey(
                SubjectKind(str(row[2])), str(row[3]), str(row[4])
            ),
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
            pair=SemanticPairKey(
                SubjectKind(str(row[1])), str(row[2]), str(row[3])
            ),
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
        and header.runtime_state
        in {"structural_committed", "semantic_pending"}
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
        group_active = in_snapshot and str(row[1]) in {
            "STAGED",
            "PUBLISHED",
        } and str(row[2]) in {"STAGED", "PUBLISHED"}
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


def _lock_snapshot_headers(
    cursor: Cursor[Any], *, header: _EpochHeader
) -> None:
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
    attempt: _StoredAttempt,
    manifest: M5CandidatePolicyManifest,
) -> M5AttemptCompletionReceipt | None:
    stored_artifact = _load_attempt_result(
        cursor, attempt_id=attempt.attempt.attempt_id
    )
    stored_result = _load_discovery_result(
        cursor, root_job_id=job.spec.logical_job_id
    )
    if stored_artifact is None and stored_result is None:
        return None
    if stored_artifact is None or stored_result is None:
        raise EventConflictError("root staging is only partially durable")
    if (
        supplied_job != job.spec
        or lease.attempt != attempt.attempt
        or supplied_output != stored_artifact.output
        or supplied_result != stored_result
        or stored_artifact.artifact.disposition
        is not M5AttemptDisposition.ROOT_RESULT_STAGED
        or stored_artifact.artifact.logical_job_id != job.spec.logical_job_id
        or attempt.state != "completed"
        or attempt.attempt_output_digest != supplied_output.attempt_output_digest
        or scope.staged_result_artifact_hash != supplied_result.result_artifact_hash
        or scope.state
        not in {
            M5ScopeState.RESULT_STAGED,
            M5ScopeState.CLOSED_ACTIVE,
            M5ScopeState.CLOSED_INACTIVE,
        }
        or job.state
        not in {
            M5JobState.RUNNING,
            M5JobState.COMPLETED_ACTIVE,
            M5JobState.COMPLETED_INACTIVE,
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
    )
    stored_artifact.artifact.validate_job_shape(job.spec.job_kind)
    return M5AttemptCompletionReceipt(
        logical_job_id=job.spec.logical_job_id,
        attempt_id=attempt.attempt.attempt_id,
        resulting_revision=header.revision,
        exact_replay=True,
    )


def _lock_and_validate_pending_counter_revisions(
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
        raise ValidationError("PENDING counter revision diverged from runtime epoch")


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
    *,
    failure_injector: RuntimeRootFailureInjector | None = None,
) -> M5AttemptCompletionReceipt:
    """Stage one successful root result under the caller's transaction."""

    if (
        isinstance(expected_revision, bool)
        or not isinstance(expected_revision, int)
        or expected_revision < 1
    ):
        raise InvalidEventError("expected runtime revision must be positive")
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
    manifest = _load_manifest(cursor, header.candidate_policy_id)
    # Read the immutable target first so tier-7 activity rows can be locked
    # before the tier-8 scope row.
    scope_hint = _read_scope(
        cursor, epoch_id=epoch_id, root_job_id=job.logical_job_id, for_update=False
    )
    activity = _lock_activity_snapshot(
        cursor, header=header, scope=scope_hint.contract
    )
    _lock_snapshot_headers(cursor, header=header)
    scope = _read_scope(
        cursor, epoch_id=epoch_id, root_job_id=job.logical_job_id, for_update=True
    )
    stored_job = _read_job(
        cursor, epoch_id=epoch_id, logical_job_id=job.logical_job_id, for_update=True
    )
    attempt = _read_attempt(
        cursor,
        logical_job_id=job.logical_job_id,
        attempt_id=leased_attempt.attempt_id,
        for_update=True,
    )
    _validate_header_bindings(header, scope, stored_job, manifest)
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
    )
    if replay is not None:
        return replay
    if header.revision != expected_revision:
        raise EventConflictError("stale typed runtime revision")
    _require_pending_epoch(header)
    if job != stored_job.spec:
        raise EventConflictError("supplied root job differs from durable identity")
    if lease.attempt != attempt.attempt:
        raise EventConflictError("supplied lease differs from durable attempt")
    if attempt.state != "dispatched" or attempt.attempt_output_digest is not None:
        raise EventConflictError("root attempt is not an unreserved dispatch")
    if stored_job.state is not M5JobState.RUNNING:
        raise EventConflictError("root job is not running")
    if scope.state is not M5ScopeState.OPEN:
        raise EventConflictError("root discovery scope is not open")
    _validate_discovery_result(
        cursor,
        epoch_id=epoch_id,
        result=result,
        scope=scope,
        job=stored_job,
        manifest=manifest,
    )

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
        SET attempt_state = 'completed', finished_at = now()
        WHERE attempt_id = %s AND logical_job_id = %s
          AND attempt_state = 'result_reserved'
          AND attempt_output_digest = %s
        """,
        (
            attempt_output.attempt_id,
            job.logical_job_id,
            attempt_output.attempt_output_digest,
        ),
    ).rowcount
    if completed_attempt != 1:
        raise EventConflictError("root attempt completion lost its reservation")
    _inject(failure_injector, "root_stage_attempt_completed")
    _advance_revision(
        cursor, header=header, resulting_revision=resulting_revision
    )
    _inject(failure_injector, "root_stage_revision_advanced")
    _force_deferred_validation(cursor)
    _inject(failure_injector, "root_stage_constraints_validated")
    return M5AttemptCompletionReceipt(
        logical_job_id=job.logical_job_id,
        attempt_id=attempt_output.attempt_id,
        resulting_revision=resulting_revision,
        exact_replay=False,
    )


def _read_all_root_scopes(
    cursor: Cursor[Any], *, epoch_id: int
) -> tuple[_StoredScope, ...]:
    rows = cursor.execute(
        _SCOPE_SELECT
        + " WHERE epoch_id = %s ORDER BY root_job_id COLLATE \"C\" FOR UPDATE",
        (epoch_id,),
    ).fetchall()
    return tuple(_scope_from_row(tuple(row)) for row in rows)


def _read_all_root_jobs(
    cursor: Cursor[Any], *, epoch_id: int
) -> tuple[_StoredJob, ...]:
    rows = cursor.execute(
        _JOB_SELECT
        + " WHERE epoch_id = %s AND parent_job_id IS NULL"
        + " ORDER BY logical_job_id COLLATE \"C\" FOR UPDATE",
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
        or stored.artifact.disposition
        is not M5AttemptDisposition.ROOT_RESULT_STAGED
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
    closure_by_root = {
        closure.root_job_id: closure for closure in plan.root_closures
    }
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
        + " ORDER BY logical_job_id COLLATE \"C\"",
        (header.epoch_id,),
    ).fetchall()
    stored_children = tuple(_job_from_row(tuple(row)) for row in child_rows)
    if set(expected_child_by_id) != {
        child.spec.logical_job_id for child in stored_children
    }:
        raise EventConflictError("durable verifier child set differs on replay")
    admitted_by_pair = {
        pair.semantic_pair_digest: pair for pair in plan.admitted_pairs
    }
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
            or scope.scope_closure_digest
            != expected_completion.scope_closure_digest
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
        if current != expected_head:
            raise EventConflictError("durable forward frontier differs on replay")
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
    header = _lock_epoch(cursor, epoch_id)
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

    admitted_by_pair = {
        pair.semantic_pair_digest: pair for pair in plan.admitted_pairs
    }
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
    _force_deferred_validation(cursor)
    _inject(failure_injector, "root_barrier_constraints_validated")
    return M5RootBarrierReceipt(
        requirement_root_set_hash=requirement_root_set_hash,
        barrier_completion_hash=plan.barrier_completion_hash,
        resulting_revision=resulting_revision,
        exact_replay=False,
    )


__all__ = [
    "RuntimeRootFailureInjector",
    "close_m5_requirement_roots",
    "stage_m5_discovery_result",
]
