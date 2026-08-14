"""Cursor-local checked persistence for M5 requirement-verifier returns.

This module owns the R1-P verifier settlement boundary.  It deliberately does
not own a transaction and it deliberately does not implement the active
maintained-matching transition reserved for M5-D25.  Exact durable replay and
late audit branches remain available; a first all-active completion is
rejected before checked-write authorization or any database mutation.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from typing import Any

from psycopg import Cursor, sql

from groundloop.domain import DecisionPolicy, SubjectKind
from groundloop.errors import EventConflictError, InvalidEventError, ValidationError
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshotEntry,
    M5AttemptArchiveReason,
    M5AttemptDisposition,
    M5AttemptOutput,
    M5AttemptResultArtifact,
    M5ExecutionEvidenceDisposition,
    M5JobCompletion,
    M5JobKind,
    M5JobLease,
    M5JobState,
    M5LogicalJobSpec,
    M5RequirementAttemptReturnReceipt,
    M5RequirementPairInput,
    M5RequirementReturnDisposition,
    M5RequirementVerifierArtifact,
    M5RuntimeTiming,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5ScopeState,
    M5TerminalReason,
    RequirementRegistrySnapshotEntry,
)
from groundloop.m5.runtime.postgres_recovery import (
    RequirementExecutionAccounting,
    build_requirement_execution_accounting,
    finish_requirement_execution_accounting,
    load_requirement_dispatch,
    persist_requirement_execution_accounting,
    read_latest_requirement_attempt,
    read_requirement_attempt,
    read_requirement_execution_replay,
    read_requirement_postterminal_replay,
    require_runtime_recovery_bundle,
    start_event_accounting,
)
from groundloop.m5.runtime.postgres_roots import (
    _JOB_SELECT,
    _advance_revision,
    _authorize,
    _build_late_return_plan,
    _current_terminal_logical_result_hash,
    _force_deferred_validation,
    _job_from_row,
    _load_attempt_result,
    _load_manifest,
    _lock_and_validate_pending_counter_revisions,
    _lock_epoch,
    _lock_snapshot_headers,
    _persist_late_return,
    _read_scope,
    _require_pending_epoch,
    _same_attempt_identity,
    _stored_completion,
    _validate_executable_lease,
    _validate_header_bindings,
    _validate_late_replay,
)

RuntimeVerifierFailureInjector = Callable[[str], None]


def _inject(
    injector: RuntimeVerifierFailureInjector | None,
    point: str,
) -> None:
    if injector is not None:
        injector(point)


def _text(value: Any) -> str:
    return str(value).strip()


def _optional_text(value: Any) -> str | None:
    return None if value is None else _text(value)


def _validate_success_disposition(
    disposition: M5ExecutionEvidenceDisposition,
) -> None:
    if disposition not in {
        M5ExecutionEvidenceDisposition.RETURNED,
        M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
    }:
        raise ValidationError(
            "successful verifier return requires returned or reused_artifact"
        )


def _lock_verifier_activity(
    cursor: Cursor[Any],
    *,
    header: Any,
    pair_input: M5RequirementPairInput,
) -> tuple[bool, bool, bool, bool]:
    """Lock and classify every applicable verifier activity predicate.

    The statements follow frozen tier 7: chunk first, then family, group, and
    requirement.  Snapshot membership is included in the classification but
    the immutable snapshot members need no row lock.
    """

    chunk_row = cursor.execute(
        """
        SELECT chunk.valid_to_epoch,
               EXISTS (
                   SELECT 1
                   FROM groundloop_m5_active_chunk_snapshot_member
                   WHERE active_chunk_snapshot_digest = %s
                     AND chunk_version_id = chunk.chunk_version_id
               )
        FROM groundloop_chunk_version AS chunk
        WHERE chunk.chunk_version_id = %s
        FOR UPDATE OF chunk
        """,
        (
            header.active_chunk_snapshot_digest,
            pair_input.pair.chunk_version_id,
        ),
    ).fetchone()
    if chunk_row is None:
        raise ValidationError("verifier pair names an unknown chunk version")

    family_row = cursor.execute(
        """
        SELECT family.lifecycle_state
        FROM groundloop_m5_group_family AS family
        WHERE family.group_family_id = %s
        FOR UPDATE
        """,
        (pair_input.group_family_id,),
    ).fetchone()
    if family_row is None:
        raise ValidationError("verifier pair names an unknown group family")

    group_row = cursor.execute(
        """
        SELECT group_version.lifecycle_state, group_version.group_family_id
        FROM groundloop_m5_group_version AS group_version
        WHERE group_version.group_version_id = %s
        FOR UPDATE
        """,
        (pair_input.group_version_id,),
    ).fetchone()
    if group_row is None or str(group_row[1]) != pair_input.group_family_id:
        raise ValidationError("verifier pair group binding is corrupt")

    requirement_row = cursor.execute(
        """
        SELECT requirement.lifecycle_state,
               requirement.group_version_id,
               EXISTS (
                   SELECT 1
                   FROM groundloop_m5_requirement_registry_snapshot_member
                   WHERE requirement_registry_snapshot_digest = %s
                     AND requirement_version_id =
                         requirement.requirement_version_id
               )
        FROM groundloop_m5_requirement_version AS requirement
        WHERE requirement.requirement_version_id = %s
        FOR UPDATE
        """,
        (
            header.requirement_registry_snapshot_digest,
            pair_input.pair.subject_id,
        ),
    ).fetchone()
    if (
        requirement_row is None
        or str(requirement_row[1]) != pair_input.group_version_id
    ):
        raise ValidationError("verifier pair names an unknown requirement")

    epoch_active = (
        header.structural_status == "committed"
        and header.semantic_status == "pending"
        and header.evaluation_state == "pending"
        and header.runtime_state in {"structural_committed", "semantic_pending"}
    )
    chunk_active = bool(chunk_row[1]) and chunk_row[0] is None
    in_requirement_snapshot = bool(requirement_row[2])
    requirement_active = in_requirement_snapshot and str(requirement_row[0]) in {
        "STAGED",
        "PUBLISHED",
    }
    group_active = (
        in_requirement_snapshot
        and str(group_row[0]) in {"STAGED", "PUBLISHED"}
        and str(family_row[0]) in {"STAGED", "PUBLISHED"}
    )
    return epoch_active, chunk_active, requirement_active, group_active


def _lock_verifier_jobs(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    parent_job_id: str,
    verifier_job_id: str,
) -> tuple[Any, Any]:
    """Lock the root and child together in tier-9 C order."""

    expected_ids = {parent_job_id, verifier_job_id}
    rows = cursor.execute(
        _JOB_SELECT
        + " WHERE epoch_id = %s AND logical_job_id = ANY(%s)"
        + ' ORDER BY logical_job_id COLLATE "C" FOR UPDATE',
        (epoch_id, list(expected_ids)),
    ).fetchall()
    jobs = tuple(_job_from_row(tuple(row)) for row in rows)
    if {item.spec.logical_job_id for item in jobs} != expected_ids:
        raise InvalidEventError("verifier completion lacks its root/child job pair")
    by_id = {item.spec.logical_job_id: item for item in jobs}
    return by_id[parent_job_id], by_id[verifier_job_id]


def _load_decision_policy(
    cursor: Cursor[Any], *, policy_version: str
) -> DecisionPolicy:
    row = cursor.execute(
        """
        SELECT policy_version, support_threshold, refute_threshold,
               tie_rule_version
        FROM groundloop_decision_policy
        WHERE policy_version = %s
        """,
        (policy_version,),
    ).fetchone()
    if row is None:
        raise ValidationError("verifier artifact names an unknown decision policy")
    return DecisionPolicy(str(row[0]), float(row[1]), float(row[2]), str(row[3]))


def _validate_pair_input_core(
    cursor: Cursor[Any],
    *,
    header: Any,
    pair_input: M5RequirementPairInput,
) -> None:
    requirement_row = cursor.execute(
        """
        SELECT requirement_version_id, group_version_id, group_family_id,
               owner_claim_id, normalized_requirement_text,
               requirement_text_hash
        FROM groundloop_m5_requirement_registry_snapshot_member
        WHERE requirement_registry_snapshot_digest = %s
          AND requirement_version_id = %s
        """,
        (
            header.requirement_registry_snapshot_digest,
            pair_input.pair.subject_id,
        ),
    ).fetchone()
    chunk_snapshot_row = cursor.execute(
        """
        SELECT chunk_version_id, text_hash
        FROM groundloop_m5_active_chunk_snapshot_member
        WHERE active_chunk_snapshot_digest = %s AND chunk_version_id = %s
        """,
        (header.active_chunk_snapshot_digest, pair_input.pair.chunk_version_id),
    ).fetchone()
    if requirement_row is None or chunk_snapshot_row is None:
        raise ValidationError("verifier pair is outside its frozen snapshots")
    requirement_entry = RequirementRegistrySnapshotEntry(
        requirement_version_id=str(requirement_row[0]),
        group_version_id=str(requirement_row[1]),
        group_family_id=str(requirement_row[2]),
        owner_claim_id=str(requirement_row[3]),
        normalized_requirement_text=str(requirement_row[4]),
        requirement_text_hash=_text(requirement_row[5]),
    )
    chunk_entry = ActiveChunkSnapshotEntry(
        chunk_version_id=str(chunk_snapshot_row[0]),
        text_hash=_text(chunk_snapshot_row[1]),
    )
    pair_input.validate_bound_rows(
        requirement_entry=requirement_entry,
        chunk_entry=chunk_entry,
    )

    core_row = cursor.execute(
        """
        SELECT requirement.ordinal, requirement.requirement_text,
               requirement.requirement_text_hash,
               group_version.group_version_id,
               group_version.group_family_id, family.claim_id,
               chunk.document_version_id, chunk.chunk_index, chunk.text,
               chunk.text_hash, provenance.chunker_artifact_id
        FROM groundloop_m5_requirement_version AS requirement
        JOIN groundloop_m5_group_version AS group_version
          ON group_version.group_version_id = requirement.group_version_id
        JOIN groundloop_m5_group_family AS family
          ON family.group_family_id = group_version.group_family_id
        JOIN groundloop_chunk_version AS chunk
          ON chunk.chunk_version_id = %s
        JOIN groundloop_chunk_provenance AS provenance
          ON provenance.chunk_version_id = chunk.chunk_version_id
        WHERE requirement.requirement_version_id = %s
        """,
        (pair_input.pair.chunk_version_id, pair_input.pair.subject_id),
    ).fetchone()
    expected_core = (
        pair_input.requirement_ordinal,
        pair_input.normalized_requirement_text,
        pair_input.requirement_text_hash,
        pair_input.group_version_id,
        pair_input.group_family_id,
        pair_input.owner_claim_id,
        pair_input.document_version_id,
        pair_input.chunk_index,
        pair_input.chunk_text,
        pair_input.stored_chunk_text_hash,
        pair_input.chunker_artifact_id,
    )
    if (
        core_row is None
        or tuple(
            _text(value) if index in {2, 9} else value
            for index, value in enumerate(core_row)
        )
        != expected_core
    ):
        raise EventConflictError("pair input differs from immutable core rows")


def _validate_verifier_bindings(
    cursor: Cursor[Any],
    *,
    header: Any,
    scope: Any,
    parent_job: Any,
    stored_job: Any,
    supplied_job: M5LogicalJobSpec,
    pair_input: M5RequirementPairInput,
    verifier_artifact: M5RequirementVerifierArtifact,
    attempt_output: M5AttemptOutput,
    manifest: Any,
) -> None:
    if supplied_job != stored_job.spec:
        raise EventConflictError("supplied verifier job differs from durable identity")
    if (
        supplied_job.job_kind is not M5JobKind.VERIFY_REQUIREMENT_PAIR
        or supplied_job.expandable
        or supplied_job.parent_job_id is None
        or supplied_job.pair is None
        or supplied_job.scope_contract_digest is None
    ):
        raise ValidationError("verifier completion requires one pair child job")
    _validate_header_bindings(header, scope, parent_job, manifest)
    supplied_job.validate_manifest_and_scope(manifest, scope.contract)
    if (
        parent_job.spec.logical_job_id != supplied_job.parent_job_id
        or parent_job.state
        not in {M5JobState.COMPLETED_ACTIVE, M5JobState.COMPLETED_INACTIVE}
        or scope.state not in {M5ScopeState.CLOSED_ACTIVE, M5ScopeState.CLOSED_INACTIVE}
    ):
        raise ValidationError("verifier job lacks its closed root scope")

    dependency = cursor.execute(
        """
        SELECT 1
        FROM groundloop_m5_job_dependency
        WHERE epoch_id = %s AND parent_job_id = %s AND child_job_id = %s
        """,
        (header.epoch_id, supplied_job.parent_job_id, supplied_job.logical_job_id),
    ).fetchone()
    admitted = cursor.execute(
        """
        SELECT admitted_pair_digest, subject_kind, subject_id,
               chunk_version_id, semantic_pair_digest, candidate_policy_id
        FROM groundloop_m5_requirement_admitted_pair
        WHERE epoch_id = %s AND admitted_pair_digest = %s
        """,
        (header.epoch_id, stored_job.admitted_pair_digest),
    ).fetchone()
    expected_admitted = (
        stored_job.admitted_pair_digest,
        SubjectKind.REQUIREMENT.value,
        supplied_job.pair.subject_id,
        supplied_job.pair.chunk_version_id,
        supplied_job.semantic_pair_digest,
        supplied_job.candidate_policy_id,
    )
    if (
        dependency is None
        or admitted is None
        or tuple(map(str, admitted)) != tuple(map(str, expected_admitted))
    ):
        raise ValidationError("verifier job lacks its exact admitted-pair closure")

    _validate_pair_input_core(cursor, header=header, pair_input=pair_input)
    policy = _load_decision_policy(
        cursor, policy_version=manifest.decision_policy_version
    )
    verifier_artifact.validate_decision_policy(policy)
    if (
        pair_input.pair != supplied_job.pair
        or pair_input.scope_contract_digest != supplied_job.scope_contract_digest
        or pair_input.candidate_policy_id != supplied_job.candidate_policy_id
        or verifier_artifact.pair != supplied_job.pair
        or verifier_artifact.pair_input_hash != pair_input.pair_input_hash
        or verifier_artifact.execution_spec_hash != supplied_job.execution_spec_hash
        or verifier_artifact.decision_policy_version != manifest.decision_policy_version
        or attempt_output.logical_job_id != supplied_job.logical_job_id
        or attempt_output.job_epoch_id != header.epoch_id
        or attempt_output.payload_hash != supplied_job.payload_hash
        or attempt_output.execution_spec_hash != supplied_job.execution_spec_hash
        or attempt_output.result_artifact_id != verifier_artifact.artifact_id
        or attempt_output.result_artifact_hash != verifier_artifact.artifact_hash
    ):
        raise ValidationError("verifier return does not bind its job and artifact")


def _pair_input_values(pair_input: M5RequirementPairInput) -> tuple[Any, ...]:
    return (
        pair_input.pair_input_hash,
        pair_input.pair.subject_kind.value,
        pair_input.pair.subject_id,
        pair_input.pair.chunk_version_id,
        pair_input.semantic_pair_digest,
        pair_input.scope_contract_digest,
        pair_input.candidate_policy_id,
        pair_input.owner_claim_id,
        pair_input.group_version_id,
        pair_input.group_family_id,
        pair_input.requirement_ordinal,
        pair_input.normalized_requirement_text,
        pair_input.requirement_text_hash,
        pair_input.document_version_id,
        pair_input.chunk_index,
        pair_input.chunk_text,
        pair_input.stored_chunk_text_hash,
        pair_input.m5_chunk_text_hash,
        pair_input.chunker_artifact_id,
        pair_input.normalizer_id,
        pair_input.normalizer_provenance_hash,
    )


def _artifact_values(
    artifact: M5RequirementVerifierArtifact,
) -> tuple[Any, ...]:
    return (
        artifact.artifact_id,
        artifact.artifact_hash,
        artifact.pair.subject_kind.value,
        artifact.pair.subject_id,
        artifact.pair.chunk_version_id,
        artifact.semantic_pair_digest,
        artifact.pair_input_hash,
        artifact.execution_spec_hash,
        artifact.model_artifact_id,
        artifact.model_id,
        artifact.model_revision,
        artifact.prompt_artifact_id,
        artifact.prompt_version,
        artifact.calibration_version,
        artifact.calibration_artifact_hash,
        artifact.temperature,
        artifact.decision_policy_version,
        artifact.decision_policy_hash,
        artifact.support_score,
        artifact.refute_score,
        artifact.neutral_score,
        *artifact.raw_logits,
        artifact.raw_output_hash,
        artifact.operational_label.value,
    )


def _observation_values(
    *,
    epoch_id: int,
    artifact: M5RequirementVerifierArtifact,
    eligible_for_currency: bool,
) -> tuple[Any, ...]:
    observation = artifact.to_semantic_observation()
    return (
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
        epoch_id,
        artifact.raw_output_hash,
        eligible_for_currency,
    )


def _execution_values(
    *,
    epoch_id: int,
    job: M5LogicalJobSpec,
    attempt_id: str,
    pair_input: M5RequirementPairInput,
    artifact: M5RequirementVerifierArtifact,
    eligible_for_currency: bool,
) -> tuple[Any, ...]:
    return (
        artifact.to_semantic_observation().observation_id,
        artifact.artifact_id,
        artifact.artifact_hash,
        job.logical_job_id,
        attempt_id,
        pair_input.pair_input_hash,
        artifact.decision_policy_version,
        artifact.decision_policy_hash,
        eligible_for_currency,
        epoch_id,
    )


def _validate_existing_row(
    row: Any,
    expected: tuple[Any, ...],
    *,
    label: str,
) -> bool:
    if row is None:
        return False
    values = tuple(row)
    normalized = tuple(
        _text(value) if isinstance(expected_value, str) else value
        for value, expected_value in zip(values, expected, strict=True)
    )
    if normalized != expected:
        raise EventConflictError(f"existing {label} differs from verifier return")
    return True


def _persist_verifier_closure(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    job: M5LogicalJobSpec,
    attempt_id: str,
    pair_input: M5RequirementPairInput,
    artifact: M5RequirementVerifierArtifact,
    eligible_for_currency: bool,
    failure_injector: RuntimeVerifierFailureInjector | None = None,
) -> None:
    pair_values = _pair_input_values(pair_input)
    pair_row = cursor.execute(
        """
        SELECT pair_input_hash, subject_kind, subject_id, chunk_version_id,
               semantic_pair_digest, scope_contract_digest,
               candidate_policy_id, owner_claim_id, group_version_id,
               group_family_id, requirement_ordinal,
               normalized_requirement_text, requirement_text_hash,
               document_version_id, chunk_index, chunk_text,
               stored_chunk_text_hash, m5_chunk_text_hash,
               chunker_artifact_id, normalizer_id,
               normalizer_provenance_hash
        FROM groundloop_m5_requirement_pair_input
        WHERE pair_input_hash = %s
        """,
        (pair_input.pair_input_hash,),
    ).fetchone()
    if not _validate_existing_row(pair_row, pair_values, label="pair input"):
        cursor.execute(
            """
            INSERT INTO groundloop_m5_requirement_pair_input (
                pair_input_hash, subject_kind, subject_id, chunk_version_id,
                semantic_pair_digest, scope_contract_digest,
                candidate_policy_id, owner_claim_id, group_version_id,
                group_family_id, requirement_ordinal,
                normalized_requirement_text, requirement_text_hash,
                document_version_id, chunk_index, chunk_text,
                stored_chunk_text_hash, m5_chunk_text_hash,
                chunker_artifact_id, normalizer_id,
                normalizer_provenance_hash
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            pair_values,
        )
        _inject(failure_injector, "verifier_pair_input_inserted")

    artifact_values = _artifact_values(artifact)
    artifact_row = cursor.execute(
        """
        SELECT artifact_id, artifact_hash, subject_kind, subject_id,
               chunk_version_id, semantic_pair_digest, pair_input_hash,
               execution_spec_hash, model_artifact_id, model_id,
               model_revision, prompt_artifact_id, prompt_version,
               calibration_version, calibration_artifact_hash, temperature,
               decision_policy_version, decision_policy_hash,
               support_score, refute_score, neutral_score,
               raw_logit_contradiction, raw_logit_entailment,
               raw_logit_neutral, raw_output_hash, operational_label
        FROM groundloop_m5_requirement_verifier_artifact
        WHERE artifact_id = %s
        """,
        (artifact.artifact_id,),
    ).fetchone()
    if not _validate_existing_row(
        artifact_row, artifact_values, label="verifier artifact"
    ):
        cursor.execute(
            """
            INSERT INTO groundloop_m5_requirement_verifier_artifact (
                artifact_id, artifact_hash, subject_kind, subject_id,
                chunk_version_id, semantic_pair_digest, pair_input_hash,
                execution_spec_hash, model_artifact_id, model_id,
                model_revision, prompt_artifact_id, prompt_version,
                calibration_version, calibration_artifact_hash, temperature,
                decision_policy_version, decision_policy_hash,
                support_score, refute_score, neutral_score,
                raw_logit_contradiction, raw_logit_entailment,
                raw_logit_neutral, raw_output_hash, operational_label
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            artifact_values,
        )
        _inject(failure_injector, "verifier_artifact_inserted")

    observation_values = _observation_values(
        epoch_id=epoch_id,
        artifact=artifact,
        eligible_for_currency=eligible_for_currency,
    )
    observation_id = str(observation_values[0])
    observation_row = cursor.execute(
        """
        SELECT observation_id, subject_kind, subject_id, chunk_version_id,
               task_type, support_score, refute_score, neutral_score,
               model_id, model_version, prompt_version, input_hash,
               produced_epoch, raw_output_hash, eligible_for_currency
        FROM groundloop_semantic_observation
        WHERE observation_id = %s
        """,
        (observation_id,),
    ).fetchone()
    if not _validate_existing_row(
        observation_row, observation_values, label="semantic observation"
    ):
        cursor.execute(
            """
            INSERT INTO groundloop_semantic_observation (
                observation_id, subject_kind, subject_id, chunk_version_id,
                task_type, support_score, refute_score, neutral_score,
                model_id, model_version, prompt_version, input_hash,
                produced_epoch, raw_output_hash, eligible_for_currency
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s
            )
            """,
            observation_values,
        )
        _inject(failure_injector, "verifier_observation_inserted")

    execution_values = _execution_values(
        epoch_id=epoch_id,
        job=job,
        attempt_id=attempt_id,
        pair_input=pair_input,
        artifact=artifact,
        eligible_for_currency=eligible_for_currency,
    )
    execution_row = cursor.execute(
        """
        SELECT observation_id, artifact_id, artifact_hash, logical_job_id,
               attempt_id, pair_input_hash, decision_policy_version,
               decision_policy_hash, eligible_for_currency, produced_epoch_id
        FROM groundloop_m5_requirement_verifier_execution
        WHERE artifact_id = %s OR (logical_job_id = %s AND attempt_id = %s)
        ORDER BY artifact_id COLLATE "C"
        """,
        (artifact.artifact_id, job.logical_job_id, attempt_id),
    ).fetchall()
    if not execution_row:
        cursor.execute(
            """
            INSERT INTO groundloop_m5_requirement_verifier_execution (
                observation_id, artifact_id, artifact_hash, logical_job_id,
                attempt_id, pair_input_hash, decision_policy_version,
                decision_policy_hash, eligible_for_currency, produced_epoch_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            execution_values,
        )
        _inject(failure_injector, "verifier_execution_inserted")
    elif len(execution_row) != 1:
        raise ValidationError("verifier execution unique identities diverged")
    else:
        _validate_existing_row(
            execution_row[0], execution_values, label="verifier execution"
        )


def _validate_verifier_closure_replay(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    job: M5LogicalJobSpec,
    attempt_id: str,
    pair_input: M5RequirementPairInput,
    artifact: M5RequirementVerifierArtifact,
    eligible_for_currency: bool,
) -> bool:
    """Validate the complete attempt-linked closure without writing."""

    execution = cursor.execute(
        """
        SELECT observation_id, artifact_id, artifact_hash, logical_job_id,
               attempt_id, pair_input_hash, decision_policy_version,
               decision_policy_hash, eligible_for_currency, produced_epoch_id
        FROM groundloop_m5_requirement_verifier_execution
        WHERE logical_job_id = %s AND attempt_id = %s
        """,
        (job.logical_job_id, attempt_id),
    ).fetchone()
    if execution is None:
        return False
    _validate_existing_row(
        execution,
        _execution_values(
            epoch_id=epoch_id,
            job=job,
            attempt_id=attempt_id,
            pair_input=pair_input,
            artifact=artifact,
            eligible_for_currency=eligible_for_currency,
        ),
        label="verifier execution",
    )
    pair_row = cursor.execute(
        """
        SELECT pair_input_hash, subject_kind, subject_id, chunk_version_id,
               semantic_pair_digest, scope_contract_digest,
               candidate_policy_id, owner_claim_id, group_version_id,
               group_family_id, requirement_ordinal,
               normalized_requirement_text, requirement_text_hash,
               document_version_id, chunk_index, chunk_text,
               stored_chunk_text_hash, m5_chunk_text_hash,
               chunker_artifact_id, normalizer_id,
               normalizer_provenance_hash
        FROM groundloop_m5_requirement_pair_input
        WHERE pair_input_hash = %s
        """,
        (pair_input.pair_input_hash,),
    ).fetchone()
    artifact_row = cursor.execute(
        """
        SELECT artifact_id, artifact_hash, subject_kind, subject_id,
               chunk_version_id, semantic_pair_digest, pair_input_hash,
               execution_spec_hash, model_artifact_id, model_id,
               model_revision, prompt_artifact_id, prompt_version,
               calibration_version, calibration_artifact_hash, temperature,
               decision_policy_version, decision_policy_hash,
               support_score, refute_score, neutral_score,
               raw_logit_contradiction, raw_logit_entailment,
               raw_logit_neutral, raw_output_hash, operational_label
        FROM groundloop_m5_requirement_verifier_artifact
        WHERE artifact_id = %s
        """,
        (artifact.artifact_id,),
    ).fetchone()
    observation_row = cursor.execute(
        """
        SELECT observation_id, subject_kind, subject_id, chunk_version_id,
               task_type, support_score, refute_score, neutral_score,
               model_id, model_version, prompt_version, input_hash,
               produced_epoch, raw_output_hash, eligible_for_currency
        FROM groundloop_semantic_observation
        WHERE observation_id = %s
        """,
        (artifact.to_semantic_observation().observation_id,),
    ).fetchone()
    if pair_row is None or artifact_row is None or observation_row is None:
        raise ValidationError("verifier execution closure is only partially durable")
    _validate_existing_row(pair_row, _pair_input_values(pair_input), label="pair input")
    _validate_existing_row(
        artifact_row, _artifact_values(artifact), label="verifier artifact"
    )
    _validate_existing_row(
        observation_row,
        _observation_values(
            epoch_id=epoch_id,
            artifact=artifact,
            eligible_for_currency=eligible_for_currency,
        ),
        label="semantic observation",
    )
    return True


def _derive_verifier_completion_work(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    logical_job_id: str,
    attempt_id: str,
    active: bool,
) -> M5RuntimeWork:
    rows = cursor.execute(
        """
        SELECT serialized FROM (
            SELECT to_jsonb(item)::text AS serialized
            FROM groundloop_m5_requirement_pair_input AS item
            JOIN groundloop_m5_requirement_verifier_execution AS execution
              ON execution.pair_input_hash = item.pair_input_hash
            WHERE execution.logical_job_id = %s AND execution.attempt_id = %s
            UNION ALL
            SELECT to_jsonb(item)::text
            FROM groundloop_m5_requirement_verifier_artifact AS item
            JOIN groundloop_m5_requirement_verifier_execution AS execution
              ON execution.artifact_id = item.artifact_id
            WHERE execution.logical_job_id = %s AND execution.attempt_id = %s
            UNION ALL
            SELECT to_jsonb(item)::text
            FROM groundloop_semantic_observation AS item
            JOIN groundloop_m5_requirement_verifier_execution AS execution
              ON execution.observation_id = item.observation_id
            WHERE execution.logical_job_id = %s AND execution.attempt_id = %s
            UNION ALL
            SELECT to_jsonb(item)::text
            FROM groundloop_m5_requirement_verifier_execution AS item
            WHERE item.logical_job_id = %s AND item.attempt_id = %s
            UNION ALL
            SELECT to_jsonb(item)::text
            FROM groundloop_m5_attempt_result_artifact AS item
            WHERE item.logical_job_id = %s AND item.attempt_id = %s
            UNION ALL
            SELECT to_jsonb(item)::text
            FROM groundloop_m5_job_attempt AS item
            WHERE item.logical_job_id = %s AND item.attempt_id = %s
            UNION ALL
            SELECT to_jsonb(item)::text
            FROM groundloop_m5_semantic_job AS item
            WHERE item.epoch_id = %s AND item.logical_job_id = %s
        ) AS verifier_rows
        ORDER BY serialized COLLATE "C"
        """,
        (
            logical_job_id,
            attempt_id,
            logical_job_id,
            attempt_id,
            logical_job_id,
            attempt_id,
            logical_job_id,
            attempt_id,
            logical_job_id,
            attempt_id,
            logical_job_id,
            attempt_id,
            epoch_id,
            logical_job_id,
        ),
    ).fetchall()
    if len(rows) != 7:
        raise ValidationError("verifier completion lacks its exact persistence rows")
    byte_count = 0
    hasher = hashlib.sha256()
    for row in rows:
        encoded = str(row[0]).encode("utf-8")
        frame = len(encoded).to_bytes(8, byteorder="big", signed=False)
        hasher.update(frame)
        hasher.update(encoded)
        byte_count += len(frame) + len(encoded)
    if len(hasher.digest()) != hashlib.sha256().digest_size:
        raise AssertionError("SHA-256 verifier instrumentation drift")
    return M5RuntimeWork(
        requirement_observation_artifact_count=1,
        requirement_effective_observation_count=int(active),
        requirement_inactive_completion_count=int(not active),
        bytes_hashed=byte_count,
        bytes_serialized=byte_count,
    )


def _insert_verifier_completion_contribution(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    attempt_id: str,
    artifact_hash: str,
    resulting_revision: int,
    work: M5RuntimeWork,
) -> None:
    kind = M5RuntimeWorkContributionKind.VERIFIER_COMPLETION
    key = digests.runtime_work_contribution_key_digest(
        epoch_id=epoch_id,
        contribution_kind=kind,
        source_id=attempt_id,
    )
    columns = (
        "epoch_id",
        *M5RuntimeWork.counter_names(),
        "work_digest",
        "contribution_kind",
        "source_id",
        "source_identity_hash",
        "contribution_key_digest",
        "applied_revision",
    )
    cursor.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_runtime_work_contribution ({}) VALUES ({})"
        ).format(
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        ),
        (
            epoch_id,
            *work.counter_values(),
            work.work_digest,
            kind.value,
            attempt_id,
            artifact_hash,
            key,
            resulting_revision,
        ),
    )


def _load_verifier_completion_contribution(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    attempt_id: str,
) -> tuple[M5RuntimeWork, str, str, int] | None:
    names = M5RuntimeWork.counter_names()
    row = cursor.execute(
        sql.SQL(
            "SELECT {}, work_digest, source_identity_hash, "
            "contribution_key_digest, applied_revision "
            "FROM groundloop_m5_runtime_work_contribution "
            "WHERE epoch_id = %s AND contribution_kind = "
            "'verifier_completion' AND source_id = %s"
        ).format(sql.SQL(", ").join(map(sql.Identifier, names))),
        (epoch_id, attempt_id),
    ).fetchone()
    if row is None:
        return None
    values = tuple(row)
    end = len(names)
    work = M5RuntimeWork(
        **dict(zip(names, map(int, values[:end]), strict=True)),
        work_digest=_text(values[end]),
    )
    return (
        work,
        _text(values[end + 1]),
        _text(values[end + 2]),
        int(values[end + 3]),
    )


def _settle_verifier_pending(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    owner_claim_id: str,
    expected_revision: int,
    resulting_revision: int,
) -> None:
    owner = cursor.execute(
        """
        SELECT required, answer_version_id
        FROM groundloop_claim
        WHERE claim_id = %s
        """,
        (owner_claim_id,),
    ).fetchone()
    if owner is None:
        raise ValidationError("verifier owner claim is not durable")
    owner_count, answer_count = _lock_and_validate_pending_counter_revisions(
        cursor, epoch_id=epoch_id, expected_revision=expected_revision
    )
    updated_owner = cursor.execute(
        """
        UPDATE groundloop_m5_owner_pending_counter
        SET verifier_job_count = verifier_job_count - 1
        WHERE epoch_id = %s AND owner_claim_id = %s
          AND updated_revision = %s AND verifier_job_count >= 1
        """,
        (epoch_id, owner_claim_id, expected_revision),
    ).rowcount
    if updated_owner != 1:
        raise ValidationError("verifier owner PENDING unit is absent")
    if bool(owner[0]):
        updated_answer = cursor.execute(
            """
            UPDATE groundloop_m5_answer_pending_counter
            SET verifier_job_count = verifier_job_count - 1
            WHERE epoch_id = %s AND answer_version_id = %s
              AND updated_revision = %s AND verifier_job_count >= 1
            """,
            (epoch_id, str(owner[1]), expected_revision),
        ).rowcount
        if updated_answer != 1:
            raise ValidationError("verifier answer PENDING unit is absent")

    owners_advanced = cursor.execute(
        """
        UPDATE groundloop_m5_owner_pending_counter
        SET updated_revision = %s
        WHERE epoch_id = %s AND updated_revision = %s
        """,
        (resulting_revision, epoch_id, expected_revision),
    ).rowcount
    answers_advanced = cursor.execute(
        """
        UPDATE groundloop_m5_answer_pending_counter
        SET updated_revision = %s
        WHERE epoch_id = %s AND updated_revision = %s
        """,
        (resulting_revision, epoch_id, expected_revision),
    ).rowcount
    if owners_advanced != owner_count or answers_advanced != answer_count:
        raise ValidationError("verifier PENDING cutoff lost rows")


def _validate_applied_replay(
    cursor: Cursor[Any],
    *,
    header: Any,
    lease: M5JobLease,
    supplied_job: M5LogicalJobSpec,
    pair_input: M5RequirementPairInput,
    verifier_artifact: M5RequirementVerifierArtifact,
    attempt_output: M5AttemptOutput,
    stored_job: Any,
    attempt: Any,
    accounting: RequirementExecutionAccounting,
    replay_revision: int | None,
) -> M5RequirementAttemptReturnReceipt | None:
    stored_result = _load_attempt_result(cursor, attempt_id=attempt.attempt.attempt_id)
    contribution = _load_verifier_completion_contribution(
        cursor,
        epoch_id=header.epoch_id,
        attempt_id=attempt.attempt.attempt_id,
    )
    verifier_dispositions = {
        M5AttemptDisposition.VERIFIER_COMPLETED_ACTIVE,
        M5AttemptDisposition.VERIFIER_COMPLETED_INACTIVE,
    }
    has_result = (
        stored_result is not None
        and stored_result.artifact.disposition in verifier_dispositions
    )
    if replay_revision is None and not has_result and contribution is None:
        return None
    if (
        replay_revision is None
        or not has_result
        or stored_result is None
        or contribution is None
    ):
        raise EventConflictError("verifier completion is only partially durable")
    active = (
        stored_result.artifact.disposition
        is M5AttemptDisposition.VERIFIER_COMPLETED_ACTIVE
    )
    if not _validate_verifier_closure_replay(
        cursor,
        epoch_id=header.epoch_id,
        job=supplied_job,
        attempt_id=attempt.attempt.attempt_id,
        pair_input=pair_input,
        artifact=verifier_artifact,
        eligible_for_currency=active,
    ):
        raise ValidationError("verifier completion lacks its immutable closure")
    stored_result.artifact.validate_job_shape(M5JobKind.VERIFY_REQUIREMENT_PAIR)
    completion = _stored_completion(stored_job)
    archive_reason: M5TerminalReason | None = None
    if not active:
        stored_archive_reason = stored_result.artifact.archive_reason
        if stored_archive_reason not in {
            M5AttemptArchiveReason.SUBJECT_INACTIVE,
            M5AttemptArchiveReason.CHUNK_INACTIVE,
        }:
            raise ValidationError(
                "inactive verifier completion lacks an inactivity reason"
            )
        assert stored_archive_reason is not None
        archive_reason = M5TerminalReason(stored_archive_reason.value)
    expected_completion = M5JobCompletion.build(
        job=supplied_job,
        terminal_state=(
            M5JobState.COMPLETED_ACTIVE if active else M5JobState.COMPLETED_INACTIVE
        ),
        result_artifact_id=verifier_artifact.artifact_id,
        result_artifact_hash=verifier_artifact.artifact_hash,
        archive_reason=archive_reason,
    )
    work, source_identity, contribution_key, applied_revision = contribution
    expected_key = digests.runtime_work_contribution_key_digest(
        epoch_id=header.epoch_id,
        contribution_kind=M5RuntimeWorkContributionKind.VERIFIER_COMPLETION,
        source_id=attempt.attempt.attempt_id,
    )
    expected_counts = (
        1,
        int(active),
        int(not active),
    )
    actual_counts = (
        work.requirement_observation_artifact_count,
        work.requirement_effective_observation_count,
        work.requirement_inactive_completion_count,
    )
    if (
        supplied_job != stored_job.spec
        or lease.attempt is None
        or not _same_attempt_identity(attempt.attempt, lease.attempt)
        or stored_result.output != attempt_output
        or stored_result.artifact.logical_job_id != supplied_job.logical_job_id
        or attempt.state != "completed"
        or attempt.attempt_output_digest != attempt_output.attempt_output_digest
        or attempt.attempt.attempt_work_digest
        != accounting.evidence.attempt_work.work_digest
        or stored_job.state
        is not (
            M5JobState.COMPLETED_ACTIVE if active else M5JobState.COMPLETED_INACTIVE
        )
        or completion != expected_completion
        or source_identity != stored_result.artifact.attempt_result_artifact_hash
        or contribution_key != expected_key
        or applied_revision != replay_revision
        or stored_job.completed_revision != replay_revision
        or actual_counts != expected_counts
    ):
        raise EventConflictError("verifier replay changed immutable completion")
    return M5RequirementAttemptReturnReceipt(
        disposition=M5RequirementReturnDisposition.APPLIED,
        logical_job_id=supplied_job.logical_job_id,
        attempt_id=attempt.attempt.attempt_id,
        resulting_revision=header.revision,
        exact_replay=True,
        execution_evidence_digest=accounting.evidence.evidence_digest,
        return_artifact_digest=(stored_result.artifact.attempt_result_artifact_hash),
        current_terminal_logical_result_hash=(
            _current_terminal_logical_result_hash(cursor, header=header)
        ),
        transition_anchor=None,
    )


def _insert_attempt_result(
    cursor: Cursor[Any],
    *,
    artifact: M5AttemptResultArtifact,
    output: M5AttemptOutput,
) -> None:
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
        ),
    )


def complete_m5_verifier(
    cursor: Cursor[Any],
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
    failure_injector: RuntimeVerifierFailureInjector | None = None,
) -> M5RequirementAttemptReturnReceipt:
    """Settle one successful requirement-verifier return atomically.

    The caller owns the transaction.  R1 supports exact replay, every D24 late
    branch, and first inactive completion.  First all-active completion is a
    zero-write D25 blocker because no persisted matching implementation is yet
    authorized on this branch.
    """

    if (
        isinstance(expected_revision, bool)
        or not isinstance(expected_revision, int)
        or expected_revision < 1
    ):
        raise InvalidEventError("expected runtime revision must be positive")
    _validate_success_disposition(execution_disposition)
    require_runtime_recovery_bundle(cursor)
    leased_attempt = _validate_executable_lease(
        lease=lease, job=job, expected_revision=expected_revision
    )
    if (
        job.job_kind is not M5JobKind.VERIFY_REQUIREMENT_PAIR
        or job.parent_job_id is None
        or job.pair is None
        or job.expandable
    ):
        raise ValidationError("verifier completion accepts only pair child jobs")
    if attempt_output.attempt_id != leased_attempt.attempt_id:
        raise ValidationError("attempt output belongs to another verifier lease")

    header = _lock_epoch(cursor, epoch_id)
    if expected_revision > header.revision:
        raise EventConflictError("expected revision is newer than durable runtime")
    manifest = _load_manifest(cursor, header.candidate_policy_id)
    activity = _lock_verifier_activity(cursor, header=header, pair_input=pair_input)
    _lock_snapshot_headers(cursor, header=header)
    scope = _read_scope(
        cursor,
        epoch_id=epoch_id,
        root_job_id=job.parent_job_id,
        for_update=True,
    )
    parent_job, stored_job = _lock_verifier_jobs(
        cursor,
        epoch_id=epoch_id,
        parent_job_id=job.parent_job_id,
        verifier_job_id=job.logical_job_id,
    )
    _validate_verifier_bindings(
        cursor,
        header=header,
        scope=scope,
        parent_job=parent_job,
        stored_job=stored_job,
        supplied_job=job,
        pair_input=pair_input,
        verifier_artifact=verifier_artifact,
        attempt_output=attempt_output,
        manifest=manifest,
    )
    attempt = read_requirement_attempt(
        cursor,
        logical_job_id=job.logical_job_id,
        attempt_id=leased_attempt.attempt_id,
    )
    if attempt is None or not _same_attempt_identity(attempt.attempt, leased_attempt):
        raise EventConflictError("verifier return lease identity is not durable")
    dispatch = load_requirement_dispatch(
        cursor, epoch_id=epoch_id, attempt=attempt.attempt
    )
    if (
        dispatch.record_digest != lease.dispatch_record_digest
        or dispatch.dispatched_revision != lease.resulting_revision
    ):
        raise EventConflictError("verifier lease dispatch identity is not durable")

    terminal_logical_result_hash = _current_terminal_logical_result_hash(
        cursor, header=header
    )
    standard_accounting = build_requirement_execution_accounting(
        epoch_id=epoch_id,
        expected_revision=expected_revision,
        attempt=leased_attempt,
        dispatch=dispatch,
        disposition=execution_disposition,
        result_or_error_hash=attempt_output.attempt_output_digest,
        attempt_work=attempt_work,
        attempt_timing=attempt_timing,
        anchor_kind=M5RuntimeWorkContributionKind.VERIFIER_COMPLETION,
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
        expected_evidence=standard_accounting.evidence,
        expected_observation=standard_accounting.observation,
    )
    postterminal_replay = (
        None
        if replay_revision is not None
        else read_requirement_postterminal_replay(
            cursor,
            expected_evidence=standard_accounting.evidence,
            expected_observation=standard_accounting.observation,
        )
    )

    # Immutable replay lookup precedes latest-attempt enforcement.  A late
    # return does not install semantic verifier rows; its output digest binds
    # the supplied, self-validating artifact bytes.
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
    applied_replay = _validate_applied_replay(
        cursor,
        header=header,
        lease=lease,
        supplied_job=job,
        pair_input=pair_input,
        verifier_artifact=verifier_artifact,
        attempt_output=attempt_output,
        stored_job=stored_job,
        attempt=attempt,
        accounting=standard_accounting,
        replay_revision=replay_revision,
    )
    if applied_replay is not None:
        return applied_replay

    latest = read_latest_requirement_attempt(cursor, logical_job_id=job.logical_job_id)
    if latest is None:
        raise ValidationError("verifier job lost its durable attempt history")
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
                raise ValidationError("expired verifier return lacks dense successor")
        elif not _same_attempt_identity(latest.attempt, leased_attempt):
            raise EventConflictError(
                "still-current verifier audit has a later durable attempt"
            )
        if postterminal:
            if not stored_job.state.terminal:
                raise ValidationError("postterminal verifier return lacks terminal job")
        else:
            if header.revision != expected_revision:
                raise EventConflictError("stale preterminal verifier return revision")
            _require_pending_epoch(header)
            if attempt.state == "expired":
                if stored_job.state is not M5JobState.RUNNING:
                    raise EventConflictError(
                        "preterminal expired verifier lacks running successor job"
                    )
            elif stored_job.state is not M5JobState.CANCELLED:
                raise EventConflictError(
                    "preterminal verifier audit lacks cancelled closure"
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
        raise EventConflictError("verifier lease is no longer the latest attempt")
    if attempt.state != "dispatched" or attempt.attempt_output_digest is not None:
        raise EventConflictError("verifier attempt is not an unreserved dispatch")
    if stored_job.state is not M5JobState.RUNNING:
        raise EventConflictError("verifier job is not running")

    active = all(activity)
    if active:
        # This must remain before authorization, accounting inserts, output
        # reservation, or any semantic row write.  Replays were handled above.
        raise ValidationError(
            "active verifier completion requires M5-D25 persisted matching"
        )

    archive_reason = (
        M5AttemptArchiveReason.EPOCH_FAILED
        if not activity[0]
        else (
            M5AttemptArchiveReason.SUBJECT_INACTIVE
            if not activity[2] or not activity[3]
            else M5AttemptArchiveReason.CHUNK_INACTIVE
        )
    )
    terminal_reason = M5TerminalReason(archive_reason.value)
    attempt_result = M5AttemptResultArtifact.build(
        attempt_output=attempt_output,
        job_state_at_receipt=M5JobState.RUNNING,
        job_state_after=M5JobState.COMPLETED_INACTIVE,
        disposition=M5AttemptDisposition.VERIFIER_COMPLETED_INACTIVE,
        activity_snapshot_epoch_id=epoch_id,
        activity_snapshot_revision=header.revision,
        epoch_active=activity[0],
        chunk_active=activity[1],
        requirement_active=activity[2],
        group_active=activity[3],
        archive_reason=archive_reason,
    )
    attempt_result.validate_job_shape(job.job_kind)
    completion = M5JobCompletion.build(
        job=job,
        terminal_state=M5JobState.COMPLETED_INACTIVE,
        result_artifact_id=verifier_artifact.artifact_id,
        result_artifact_hash=verifier_artifact.artifact_hash,
        archive_reason=terminal_reason,
    )
    resulting_revision = header.revision + 1

    _authorize(cursor, header)
    accounting_start = start_event_accounting(
        cursor, epoch_id=epoch_id, expected_revision=expected_revision
    )
    persist_requirement_execution_accounting(cursor, accounting=standard_accounting)
    _inject(failure_injector, "verifier_accounting_inserted")

    reserved = cursor.execute(
        """
        UPDATE groundloop_m5_job_attempt
        SET attempt_state = 'result_reserved', attempt_output_digest = %s
        WHERE attempt_id = %s AND logical_job_id = %s
          AND attempt_state = 'dispatched' AND attempt_output_digest IS NULL
          AND lease_token_hash = %s AND lease_expires_at = %s
          AND execution_spec_hash = %s
        """,
        (
            attempt_output.attempt_output_digest,
            attempt_output.attempt_id,
            job.logical_job_id,
            leased_attempt.lease_token_hash,
            leased_attempt.lease_expires_at,
            leased_attempt.execution_spec_hash,
        ),
    ).rowcount
    if reserved != 1:
        raise EventConflictError("verifier output reservation lost its race")
    _inject(failure_injector, "verifier_output_reserved")

    _persist_verifier_closure(
        cursor,
        epoch_id=epoch_id,
        job=job,
        attempt_id=attempt_output.attempt_id,
        pair_input=pair_input,
        artifact=verifier_artifact,
        eligible_for_currency=False,
        failure_injector=failure_injector,
    )
    _insert_attempt_result(cursor, artifact=attempt_result, output=attempt_output)
    _inject(failure_injector, "verifier_attempt_artifact_inserted")

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
            standard_accounting.evidence.attempt_work.work_digest,
            attempt_output.attempt_id,
            job.logical_job_id,
            attempt_output.attempt_output_digest,
            leased_attempt.lease_token_hash,
            leased_attempt.lease_expires_at,
            leased_attempt.execution_spec_hash,
        ),
    ).rowcount
    if completed_attempt != 1:
        raise EventConflictError("verifier attempt completion lost its reservation")
    _inject(failure_injector, "verifier_attempt_completed")

    completed_job = cursor.execute(
        """
        UPDATE groundloop_m5_semantic_job
        SET job_state = 'completed_inactive', result_artifact_id = %s,
            result_artifact_hash = %s, archive_reason = %s,
            completion_digest = %s, completed_revision = %s,
            completed_at = clock_timestamp()
        WHERE epoch_id = %s AND logical_job_id = %s AND job_state = 'running'
          AND result_artifact_id IS NULL AND result_artifact_hash IS NULL
          AND completion_digest IS NULL AND completed_revision IS NULL
        """,
        (
            verifier_artifact.artifact_id,
            verifier_artifact.artifact_hash,
            terminal_reason.value,
            completion.completion_digest,
            resulting_revision,
            epoch_id,
            job.logical_job_id,
        ),
    ).rowcount
    if completed_job != 1:
        raise EventConflictError("verifier job completion lost its race")
    _inject(failure_injector, "verifier_job_completed")

    _settle_verifier_pending(
        cursor,
        epoch_id=epoch_id,
        owner_claim_id=pair_input.owner_claim_id,
        expected_revision=expected_revision,
        resulting_revision=resulting_revision,
    )
    _inject(failure_injector, "verifier_pending_updated")

    verifier_work = _derive_verifier_completion_work(
        cursor,
        epoch_id=epoch_id,
        logical_job_id=job.logical_job_id,
        attempt_id=attempt_output.attempt_id,
        active=False,
    )
    _insert_verifier_completion_contribution(
        cursor,
        epoch_id=epoch_id,
        attempt_id=attempt_output.attempt_id,
        artifact_hash=attempt_result.attempt_result_artifact_hash,
        resulting_revision=resulting_revision,
        work=verifier_work,
    )
    _inject(failure_injector, "verifier_contribution_inserted")

    _advance_revision(
        cursor,
        header=header,
        resulting_revision=resulting_revision,
        pending_revision_already_updated=True,
    )
    _inject(failure_injector, "verifier_epoch_revision_advanced")
    finish_requirement_execution_accounting(
        cursor,
        epoch_id=epoch_id,
        expected_revision=expected_revision,
        start=accounting_start,
        accounting=standard_accounting,
        additional_work=verifier_work,
        failure_injector=failure_injector,
    )
    _inject(failure_injector, "verifier_revision_advanced")
    _force_deferred_validation(cursor)
    _inject(failure_injector, "verifier_constraints_validated")
    receipt = M5RequirementAttemptReturnReceipt(
        disposition=M5RequirementReturnDisposition.APPLIED,
        logical_job_id=job.logical_job_id,
        attempt_id=attempt_output.attempt_id,
        resulting_revision=resulting_revision,
        exact_replay=False,
        execution_evidence_digest=standard_accounting.evidence.evidence_digest,
        return_artifact_digest=attempt_result.attempt_result_artifact_hash,
        current_terminal_logical_result_hash=None,
        transition_anchor=standard_accounting.anchor,
    )
    receipt.validate_anchor_context(
        epoch_id=epoch_id,
        expected_kind=M5RuntimeWorkContributionKind.VERIFIER_COMPLETION,
    )
    return receipt


__all__ = ["RuntimeVerifierFailureInjector", "complete_m5_verifier"]
