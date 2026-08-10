"""Checked PostgreSQL recovery primitives for the typed-direct M4 subgraph.

The public M4-v1 ports remain unchanged.  This M5-owned layer is the only
place that combines the frozen M4 attempt identities with migration-016
dispatch, point work and point timing accounting.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, Literal

from psycopg import Cursor, sql
from psycopg.types.json import Jsonb

from groundloop.errors import EventConflictError, InvalidEventError, ValidationError
from groundloop.incremental import IncrementalMaintenanceEngine
from groundloop.m4.application import JobLease, ObservationCompletionReceipt
from groundloop.m4.contracts import (
    JobAttempt,
    JobState,
    LogicalJobSpec,
    stable_m4_digest,
)
from groundloop.m4.persistence import PointJobRecord
from groundloop.m4.pipeline import PostgresM4ApplicationPorts
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import (
    M5AcquisitionDisposition,
    M5AttemptExecutionEvidence,
    M5DirectCursorContributionReceipt,
    M5DirectLateCursorContributionReceipt,
    M5DirectLateReturnDisposition,
    M5DispatchRecord,
    M5ExecutionEvidenceDisposition,
    M5ExpiredAttemptReturn,
    M5RuntimeSubgraph,
    M5RuntimeTiming,
    M5RuntimeTimingObservation,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5TransitionTimingAnchor,
    M5TypedDirectJobLease,
    M5TypedDirectLateReturnEnvelope,
    M5TypedDirectReturnKind,
    M5TypedDirectTerminalProjection,
)
from groundloop.repository import InMemoryRepository

_RECOVERY_BUNDLE_ID = "m5-runtime-recovery-schema-bundle-v1"
_RECOVERY_BUNDLE_SHA256 = (
    "28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565"
)
_RECOVERY_MIGRATION_SHA256 = (
    "a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7"
)
_RECOVERY_ORACLE_SHA256 = (
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)
_RECOVERY_PREREQUISITE_SHA256 = (
    "b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd"
)
_WORK_COUNTER_COLUMNS = M5RuntimeWork.counter_names()
_SUCCESS_DISPOSITIONS = frozenset(
    {
        M5ExecutionEvidenceDisposition.RETURNED,
        M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
    }
)
_TERMINAL_RUNTIME_STATES = frozenset({"sealed", "failed"})


@dataclass(frozen=True, slots=True)
class _LockedDirectHeader:
    base_revision: int
    semantic_status: str
    runtime_revision: int
    runtime_state: str


@dataclass(frozen=True, slots=True)
class _DirectAttemptBinding:
    attempt: JobAttempt
    state: str
    lease_expires_at: datetime


@dataclass(frozen=True, slots=True)
class _StoredDirectReturn:
    branch: Literal["normal", "late"]
    evidence: M5AttemptExecutionEvidence
    disposition: M5DirectLateReturnDisposition | None
    return_artifact_digest: str
    expired_return_digest: str | None
    terminal_logical_result_hash: str | None


@dataclass(frozen=True, slots=True)
class _DirectReturnSettlement:
    normal: M5DirectCursorContributionReceipt | None
    late: M5DirectLateCursorContributionReceipt | None
    resulting_revision: int
    exact_replay: bool
    current_terminal_logical_result_hash: str | None

    def __post_init__(self) -> None:
        if (self.normal is None) == (self.late is None):
            raise ValidationError(
                "direct settlement requires one normal or late branch"
            )


@dataclass(frozen=True, slots=True)
class _DirectVerifierSettlement:
    settlement: _DirectReturnSettlement
    repository: InMemoryRepository | None
    engine: IncrementalMaintenanceEngine | None

    def __post_init__(self) -> None:
        if (self.repository is None) != (self.engine is None):
            raise ValidationError(
                "direct verifier cache candidates must be jointly present or absent"
            )


def _strip(value: object) -> str:
    return str(value).strip()


def _require_direct_lease_binding(
    lease: M5TypedDirectJobLease,
) -> tuple[str, str, datetime]:
    if lease.attempt_id is None or lease.lease_token_hash is None:
        raise ValidationError("direct settlement requires an attempt-bound lease")
    if lease.lease_expires_at is None or lease.dispatch_record_digest is None:
        raise ValidationError("direct settlement requires its D24 dispatch binding")
    return lease.attempt_id, lease.lease_token_hash, lease.lease_expires_at


def _public_m4_lease(lease: M5TypedDirectJobLease) -> JobLease:
    attempt_id, lease_token_hash, _deadline = _require_direct_lease_binding(lease)
    return JobLease(
        job_id=lease.job_id,
        should_execute=True,
        already_completed=False,
        attempt_id=attempt_id,
        lease_token_hash=lease_token_hash,
        expected_revision=lease.resulting_revision,
    )


def _load_named_attempt(
    cursor: Cursor[Any], *, epoch_id: int, job_id: str, attempt_id: str
) -> _DirectAttemptBinding:
    row = cursor.execute(
        """
        SELECT attempt.attempt_id, attempt.job_id,
               btrim(attempt.execution_spec_hash), attempt.attempt_ordinal,
               btrim(attempt.lease_token_hash), attempt.attempt_state,
               attempt.lease_expires_at
        FROM groundloop_semantic_job_attempt AS attempt
        JOIN groundloop_semantic_job AS job ON job.job_id = attempt.job_id
        WHERE job.epoch_id = %s AND job.job_id = %s
          AND attempt.attempt_id = %s
        """,
        (epoch_id, job_id, attempt_id),
    ).fetchone()
    if row is None or not isinstance(row[6], datetime):
        raise EventConflictError("direct settlement names an unknown attempt")
    return _DirectAttemptBinding(
        attempt=JobAttempt(
            attempt_id=str(row[0]),
            job_id=str(row[1]),
            execution_spec_hash=_strip(row[2]),
            attempt_ordinal=int(row[3]),
            lease_token_hash=_strip(row[4]),
        ),
        state=str(row[5]),
        lease_expires_at=row[6],
    )


def _validate_lease_against_attempt(
    lease: M5TypedDirectJobLease,
    binding: _DirectAttemptBinding,
) -> None:
    attempt_id, token, deadline = _require_direct_lease_binding(lease)
    if (
        attempt_id != binding.attempt.attempt_id
        or lease.job_id != binding.attempt.job_id
        or token != binding.attempt.lease_token_hash
        or deadline != binding.lease_expires_at
    ):
        raise EventConflictError("direct settlement lease bytes differ from dispatch")


def _validate_discovery_return_input(
    *,
    epoch_id: int,
    lease: M5TypedDirectJobLease,
    envelope: M5TypedDirectLateReturnEnvelope,
    binding: _DirectAttemptBinding,
    dispatch: M5DispatchRecord,
    children: tuple[LogicalJobSpec, ...],
    execution_disposition: M5ExecutionEvidenceDisposition,
) -> None:
    if envelope.return_kind is not M5TypedDirectReturnKind.DISCOVERY:
        raise ValidationError("direct expansion requires a discovery envelope")
    if (
        not isinstance(execution_disposition, M5ExecutionEvidenceDisposition)
        or execution_disposition not in _SUCCESS_DISPOSITIONS
    ):
        raise ValidationError(
            "successful direct return disposition must be returned or reused_artifact"
        )
    attempt_id, _token, _deadline = _require_direct_lease_binding(lease)
    if (
        envelope.epoch_id != epoch_id
        or envelope.job_id != lease.job_id
        or envelope.attempt_id != attempt_id
        or envelope.attempt != binding.attempt
    ):
        raise EventConflictError("direct discovery envelope belongs to another lease")
    if (
        dispatch.logical_job_id != lease.job_id
        or dispatch.attempt_id != attempt_id
        or dispatch.attempt_ordinal != binding.attempt.attempt_ordinal
        or dispatch.job_kind != envelope.job.kind.value
        or dispatch.lease_expires_at != binding.lease_expires_at
        or dispatch.record_digest != lease.dispatch_record_digest
        or dispatch.dispatched_revision != lease.resulting_revision
    ):
        raise EventConflictError("direct discovery dispatch closure differs")
    closure = envelope.completion.child_closure
    if envelope.discovery is None or envelope.scope is None or closure is None:
        raise ValidationError("direct discovery envelope is incomplete")
    if tuple(child.job_id for child in children) != closure.child_job_ids:
        raise EventConflictError("direct discovery children differ from its envelope")


def _validate_verifier_return_input(
    *,
    epoch_id: int,
    lease: M5TypedDirectJobLease,
    envelope: M5TypedDirectLateReturnEnvelope,
    binding: _DirectAttemptBinding,
    dispatch: M5DispatchRecord,
    execution_disposition: M5ExecutionEvidenceDisposition,
) -> None:
    if envelope.return_kind is not M5TypedDirectReturnKind.VERIFIER:
        raise ValidationError("direct verifier settlement requires a verifier envelope")
    if (
        not isinstance(execution_disposition, M5ExecutionEvidenceDisposition)
        or execution_disposition not in _SUCCESS_DISPOSITIONS
    ):
        raise ValidationError(
            "successful direct return disposition must be returned or reused_artifact"
        )
    attempt_id, _token, _deadline = _require_direct_lease_binding(lease)
    if (
        envelope.epoch_id != epoch_id
        or envelope.job_id != lease.job_id
        or envelope.attempt_id != attempt_id
        or envelope.attempt != binding.attempt
    ):
        raise EventConflictError("direct verifier envelope belongs to another lease")
    if (
        dispatch.logical_job_id != lease.job_id
        or dispatch.attempt_id != attempt_id
        or dispatch.attempt_ordinal != binding.attempt.attempt_ordinal
        or dispatch.job_kind != envelope.job.kind.value
        or dispatch.lease_expires_at != binding.lease_expires_at
        or dispatch.record_digest != lease.dispatch_record_digest
        or dispatch.dispatched_revision != lease.resulting_revision
    ):
        raise EventConflictError("direct verifier dispatch closure differs")
    if (
        envelope.observation is None
        or envelope.requested_make_effective is None
        or envelope.completion.child_closure is not None
    ):
        raise ValidationError("direct verifier envelope is incomplete")


def _assert_discovery_scope_binding(
    cursor: Cursor[Any],
    *,
    envelope: M5TypedDirectLateReturnEnvelope,
    resulting_revision: int,
    replay: bool,
) -> None:
    scope = envelope.scope
    assert scope is not None and envelope.persisted_scope_kind is not None
    row = cursor.execute(
        """
        SELECT registry_snapshot_id, scope_kind, explicit_claim_ids,
               closed_revision
        FROM groundloop_discovery_scope
        WHERE root_job_id = %s AND epoch_id = %s
        """,
        (envelope.job_id, envelope.epoch_id),
    ).fetchone()
    if row is None:
        raise ValidationError("direct discovery lacks its persisted M4 scope")
    members = tuple(
        str(member[0])
        for member in cursor.execute(
            """
            SELECT claim_id FROM groundloop_m4_claim_registry_member
            WHERE claim_registry_snapshot_id = %s
            ORDER BY member_ordinal
            """,
            (str(row[0]),),
        ).fetchall()
    )
    explicit = None if row[2] is None else tuple(str(value) for value in row[2])
    expected_closed = resulting_revision if replay else None
    if (
        scope.root_job_id != envelope.job_id
        or scope.registry_snapshot_id != str(row[0])
        or scope.registered_claim_ids != members
        or envelope.persisted_scope_kind.value != str(row[1])
        or envelope.explicit_claim_ids != explicit
        or scope.closed
        or envelope.closed_revision is not None
        or row[3] != expected_closed
    ):
        raise EventConflictError("direct discovery scope closure differs")


def _assert_discovery_artifact_replay(
    cursor: Cursor[Any],
    *,
    envelope: M5TypedDirectLateReturnEnvelope,
) -> None:
    discovery = envelope.discovery
    assert discovery is not None
    bindings = _envelope_json_bindings(envelope)
    expected = bindings[3]
    row = cursor.execute(
        """
        SELECT result_artifact_id, btrim(result_artifact_hash),
               fallback_satisfied, channel_hit_count, admitted_pair_count,
               btrim(channel_set_hash), btrim(admitted_pair_set_hash)
        FROM groundloop_m4_discovery_result
        WHERE root_job_id = %s AND epoch_id = %s
        """,
        (envelope.job_id, envelope.epoch_id),
    ).fetchone()
    expected_row = (
        expected["result_artifact_id"],
        expected["result_artifact_hash"],
        expected["fallback_satisfied"],
        expected["channel_hit_count"],
        expected["admitted_pair_count"],
        expected["channel_set_hash"],
        expected["admitted_pair_set_hash"],
    )
    if row is None or tuple(row) != expected_row:
        raise EventConflictError("normal direct discovery artifact replay differs")
    rows = cursor.execute(
        """
        SELECT epoch_id, claim_id, chunk_version_id, candidate_policy_id,
               channel, rank, score, btrim(channel_artifact_hash)
        FROM groundloop_impact_channel_hit
        WHERE epoch_id = %s AND candidate_policy_id = %s
          AND ((%s = 'impact_discovery' AND chunk_version_id = %s)
               OR (%s = 'frontier_retrieve' AND claim_id = %s))
        ORDER BY channel, rank, claim_id, chunk_version_id,
                 candidate_policy_id, channel_artifact_hash
        """,
        (
            envelope.epoch_id,
            envelope.job.candidate_policy_id,
            envelope.job.kind.value,
            envelope.job.target_chunk_version_id,
            envelope.job.kind.value,
            envelope.job.target_claim_id,
        ),
    ).fetchall()
    actual_hits = [
        {
            "epoch_id": int(item[0]),
            "claim_id": str(item[1]),
            "chunk_version_id": str(item[2]),
            "candidate_policy_id": str(item[3]),
            "channel": str(item[4]),
            "rank": int(item[5]),
            "score": None if item[6] is None else _f64_hex(float(item[6])),
            "channel_artifact_hash": _strip(item[7]),
        }
        for item in rows
    ]
    admitted_rows = cursor.execute(
        """
        SELECT epoch_id, claim_id, chunk_version_id, candidate_policy_id,
               fused_rank, reasons, mandatory_lineage
        FROM groundloop_admitted_pair
        WHERE epoch_id = %s AND candidate_policy_id = %s
          AND ((%s = 'impact_discovery' AND chunk_version_id = %s)
               OR (%s = 'frontier_retrieve' AND claim_id = %s))
        ORDER BY fused_rank, claim_id, chunk_version_id, candidate_policy_id
        """,
        (
            envelope.epoch_id,
            envelope.job.candidate_policy_id,
            envelope.job.kind.value,
            envelope.job.target_chunk_version_id,
            envelope.job.kind.value,
            envelope.job.target_claim_id,
        ),
    ).fetchall()
    actual_admitted = [
        {
            "epoch_id": int(item[0]),
            "claim_id": str(item[1]),
            "chunk_version_id": str(item[2]),
            "candidate_policy_id": str(item[3]),
            "fused_rank": int(item[4]),
            "reasons": [str(reason) for reason in item[5]],
            "mandatory_lineage": bool(item[6]),
        }
        for item in admitted_rows
    ]
    if (
        actual_hits != expected["channel_hits"]
        or actual_admitted != expected["admitted_pairs"]
    ):
        raise EventConflictError("normal direct discovery row replay differs")


def _assert_verifier_observation_replay(
    cursor: Cursor[Any], *, envelope: M5TypedDirectLateReturnEnvelope
) -> None:
    observation = envelope.observation
    assert observation is not None
    row = cursor.execute(
        """
        SELECT subject_kind::text, subject_id, chunk_version_id, task_type,
               encode(float8send(support_score), 'hex'),
               encode(float8send(refute_score), 'hex'),
               encode(float8send(neutral_score), 'hex'),
               model_id, model_version, prompt_version, input_hash,
               produced_epoch, btrim(raw_output_hash), eligible_for_currency
        FROM groundloop_semantic_observation
        WHERE observation_id = %s
        """,
        (observation.observation_id,),
    ).fetchone()
    expected = (
        observation.subject_kind.value,
        observation.subject_id,
        observation.chunk_version_id,
        observation.task_type,
        _f64_hex(observation.support_score),
        _f64_hex(observation.refute_score),
        _f64_hex(observation.neutral_score),
        observation.producer.model_id,
        observation.producer.model_version,
        observation.producer.prompt_version,
        observation.input_hash,
        envelope.observation_produced_epoch,
        envelope.observation_raw_output_hash,
        envelope.observation_eligible_for_currency,
    )
    if row is None or tuple(row) != expected:
        raise EventConflictError("normal direct verifier observation replay differs")


def _assert_verifier_execution_replay(
    cursor: Cursor[Any], *, envelope: M5TypedDirectLateReturnEnvelope
) -> None:
    observation = envelope.observation
    assert observation is not None
    execution = envelope.verification_execution
    if execution is None:
        row = cursor.execute(
            "SELECT 1 FROM groundloop_m4_verification_execution "
            "WHERE observation_id = %s",
            (observation.observation_id,),
        ).fetchone()
        if row is not None:
            raise EventConflictError(
                "normal direct verifier unexpectedly persisted execution provenance"
            )
        return
    row = cursor.execute(
        """
        SELECT job_id, btrim(admitted_pair_id), model_artifact_id,
               prompt_artifact_id, btrim(execution_spec_hash),
               btrim(pair_input_hash), calibration_version,
               btrim(calibration_artifact_sha256),
               encode(float8send(temperature), 'hex'),
               ARRAY(
                   SELECT encode(float8send(value), 'hex')
                   FROM unnest(raw_logits) WITH ORDINALITY AS logit(value, ordinal)
                   ORDER BY ordinal
               ),
               btrim(raw_output_hash), reused_from_observation_id
        FROM groundloop_m4_verification_execution
        WHERE observation_id = %s
        """,
        (execution.observation_id,),
    ).fetchone()
    expected = (
        execution.job_id,
        execution.admitted_pair_id,
        execution.model_artifact_id,
        execution.prompt_artifact_id,
        execution.execution_spec_hash,
        execution.pair_input_hash,
        execution.calibration_version,
        execution.calibration_artifact_sha256,
        _f64_hex(execution.temperature),
        [_f64_hex(value) for value in execution.raw_logits],
        execution.raw_output_hash,
        execution.reused_from_observation_id,
    )
    if row is None or tuple(row) != expected:
        raise EventConflictError(
            "normal direct verifier execution-provenance replay differs"
        )


def _assert_verifier_artifact_replay(
    cursor: Cursor[Any],
    *,
    envelope: M5TypedDirectLateReturnEnvelope,
) -> None:
    """Validate immutable artifact bytes, never later mutable working state."""

    _assert_verifier_observation_replay(cursor, envelope=envelope)
    _assert_verifier_execution_replay(cursor, envelope=envelope)


def _advance_runtime_header(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    expected_revision: int,
    resulting_revision: int,
) -> None:
    base = cursor.execute(
        "SELECT semantic_status FROM groundloop_epoch WHERE epoch_id = %s",
        (epoch_id,),
    ).fetchone()
    if base is None:
        raise InvalidEventError(f"unknown typed-direct epoch_id: {epoch_id}")
    semantic_status = str(base[0])
    runtime_state = (
        "semantic_complete" if semantic_status == "complete" else "semantic_pending"
    )
    changed = cursor.execute(
        """
        UPDATE groundloop_m5_runtime_epoch
        SET runtime_state = %s, revision = %s
        WHERE epoch_id = %s AND revision = %s
          AND runtime_state NOT IN ('sealed', 'failed')
        """,
        (runtime_state, resulting_revision, epoch_id, expected_revision),
    ).rowcount
    if changed != 1:
        raise EventConflictError("typed runtime changed during direct settlement")


def _insert_terminal_projection(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    job_id: str,
    terminal_state: JobState,
    terminal_reason: str | None,
    completion_digest: str | None,
    completed_revision: int,
) -> M5TypedDirectTerminalProjection:
    projection = M5TypedDirectTerminalProjection.build(
        job_id=job_id,
        terminal_state=terminal_state,
        terminal_reason=terminal_reason,
        m4_completion_digest=completion_digest,
        completed_revision=completed_revision,
    )
    cursor.execute(
        """
        INSERT INTO groundloop_m5_direct_terminal_projection (
            epoch_id, job_id, terminal_state, terminal_reason,
            m4_completion_digest, completed_revision, terminal_identity_hash
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            epoch_id,
            job_id,
            terminal_state.value,
            terminal_reason,
            completion_digest,
            completed_revision,
            projection.terminal_identity_hash,
        ),
    )
    return projection


def _f64_hex(value: float) -> str:
    return struct.pack(">d", float(value)).hex()


def _envelope_json_bindings(
    envelope: M5TypedDirectLateReturnEnvelope,
) -> tuple[dict[str, object], ...]:
    job = envelope.job
    pair = job.pair
    job_binding: dict[str, object] = {
        "job_id": job.job_id,
        "event_id": job.event_id,
        "job_kind": job.kind.value,
        "candidate_policy_id": job.candidate_policy_id,
        "payload_hash": job.payload_hash,
        "execution_spec_hash": job.execution_spec_hash,
        "parent_job_id": job.parent_job_id,
        "pair_claim_id": None if pair is None else pair.claim_id,
        "pair_chunk_version_id": None if pair is None else pair.chunk_version_id,
        "target_claim_id": job.target_claim_id,
        "target_chunk_version_id": job.target_chunk_version_id,
        "expandable": job.expandable,
    }
    attempt = envelope.attempt
    attempt_binding: dict[str, object] = {
        "attempt_id": attempt.attempt_id,
        "job_id": attempt.job_id,
        "execution_spec_hash": attempt.execution_spec_hash,
        "attempt_ordinal": attempt.attempt_ordinal,
        "lease_token_hash": attempt.lease_token_hash,
    }
    completion = envelope.completion
    closure = completion.child_closure
    completion_binding: dict[str, object] = {
        "job_id": completion.job_id,
        "payload_hash": completion.payload_hash,
        "execution_spec_hash": completion.execution_spec_hash,
        "result_artifact_id": completion.result_artifact_id,
        "result_artifact_hash": completion.result_artifact_hash,
        "terminal_state": completion.terminal_state.value,
        "completion_digest": completion.completion_digest,
        "child_parent_job_id": None if closure is None else closure.parent_job_id,
        "child_completion_digest": (
            None if closure is None else closure.completion_digest
        ),
        "child_set_hash": None if closure is None else closure.child_set_hash,
        "child_job_ids": [] if closure is None else list(closure.child_job_ids),
    }
    if envelope.discovery is not None:
        discovery = envelope.discovery
        hits = tuple(
            sorted(
                discovery.channel_hits,
                key=lambda item: (
                    item.channel.value,
                    item.rank,
                    item.pair.claim_id,
                    item.pair.chunk_version_id,
                    item.candidate_policy_id,
                    item.channel_artifact_hash,
                ),
            )
        )
        admitted_pairs = tuple(
            sorted(
                discovery.admitted_pairs,
                key=lambda item: (
                    item.fused_rank,
                    item.pair.claim_id,
                    item.pair.chunk_version_id,
                    item.candidate_policy_id,
                ),
            )
        )
        channel_identities = sorted(
            stable_m4_digest(
                "m4-discovery-channel-v1",
                str(item.epoch_id),
                item.pair.claim_id,
                item.pair.chunk_version_id,
                item.candidate_policy_id,
                item.channel.value,
                str(item.rank),
                "" if item.score is None else format(item.score, ".17g"),
                item.channel_artifact_hash,
            )
            for item in hits
        )
        admitted_identities = sorted(
            stable_m4_digest(
                "m4-admitted-pair-v1",
                str(item.epoch_id),
                item.pair.claim_id,
                item.pair.chunk_version_id,
                item.candidate_policy_id,
            )
            for item in admitted_pairs
        )
        discovery_binding: dict[str, object] = {
            "root_job_id": discovery.root_job_id,
            "result_artifact_id": discovery.result_artifact_id,
            "result_artifact_hash": discovery.result_artifact_hash,
            "fallback_satisfied": discovery.fallback_satisfied,
            "channel_hit_count": len(hits),
            "admitted_pair_count": len(admitted_pairs),
            "channel_set_hash": stable_m4_digest(
                "m4-discovery-channel-set-v1", *channel_identities
            ),
            "admitted_pair_set_hash": stable_m4_digest(
                "m4-discovery-admitted-set-v1", *admitted_identities
            ),
            "channel_hits": [
                {
                    "epoch_id": item.epoch_id,
                    "claim_id": item.pair.claim_id,
                    "chunk_version_id": item.pair.chunk_version_id,
                    "candidate_policy_id": item.candidate_policy_id,
                    "channel": item.channel.value,
                    "rank": item.rank,
                    "score": None if item.score is None else _f64_hex(item.score),
                    "channel_artifact_hash": item.channel_artifact_hash,
                }
                for item in hits
            ],
            "admitted_pairs": [
                {
                    "epoch_id": item.epoch_id,
                    "claim_id": item.pair.claim_id,
                    "chunk_version_id": item.pair.chunk_version_id,
                    "candidate_policy_id": item.candidate_policy_id,
                    "fused_rank": item.fused_rank,
                    "reasons": [reason.value for reason in item.reasons],
                    "mandatory_lineage": item.mandatory_lineage,
                }
                for item in admitted_pairs
            ],
        }
        scope = envelope.scope
        assert scope is not None and envelope.persisted_scope_kind is not None
        scope_binding: dict[str, object] = {
            "root_job_id": scope.root_job_id,
            "epoch_id": envelope.epoch_id,
            "registry_snapshot_id": scope.registry_snapshot_id,
            "registered_claim_ids": list(scope.registered_claim_ids),
            "closed": scope.closed,
            "persisted_scope_kind": envelope.persisted_scope_kind.value,
            "explicit_claim_ids": (
                None
                if envelope.explicit_claim_ids is None
                else list(envelope.explicit_claim_ids)
            ),
            "closed_revision": envelope.closed_revision,
        }
        return (
            job_binding,
            attempt_binding,
            completion_binding,
            discovery_binding,
            scope_binding,
        )

    observation = envelope.observation
    assert observation is not None
    execution = envelope.verification_execution
    execution_binding: dict[str, object] | None = None
    if execution is not None:
        execution_binding = {
            "observation_id": execution.observation_id,
            "job_id": execution.job_id,
            "admitted_pair_id": execution.admitted_pair_id,
            "model_artifact_id": execution.model_artifact_id,
            "prompt_artifact_id": execution.prompt_artifact_id,
            "execution_spec_hash": execution.execution_spec_hash,
            "pair_input_hash": execution.pair_input_hash,
            "calibration_version": execution.calibration_version,
            "calibration_artifact_sha256": (execution.calibration_artifact_sha256),
            "temperature": _f64_hex(execution.temperature),
            "raw_logits": [_f64_hex(value) for value in execution.raw_logits],
            "raw_output_hash": execution.raw_output_hash,
            "reused_from_observation_id": execution.reused_from_observation_id,
        }
    verifier_binding: dict[str, object] = {
        "result_artifact_id": envelope.result_artifact_id,
        "result_artifact_hash": envelope.result_artifact_hash,
        "verification_execution": execution_binding,
        "observation": {
            "observation_id": observation.observation_id,
            "subject_kind": observation.subject_kind.value,
            "subject_id": observation.subject_id,
            "chunk_version_id": observation.chunk_version_id,
            "task_type": observation.task_type,
            "support_score": _f64_hex(observation.support_score),
            "refute_score": _f64_hex(observation.refute_score),
            "neutral_score": _f64_hex(observation.neutral_score),
            "model_id": observation.producer.model_id,
            "model_version": observation.producer.model_version,
            "prompt_version": observation.producer.prompt_version,
            "input_hash": observation.input_hash,
            "produced_epoch": envelope.observation_produced_epoch,
            "raw_output_hash": envelope.observation_raw_output_hash,
            "eligible_for_currency": envelope.observation_eligible_for_currency,
            "requested_make_effective": envelope.requested_make_effective,
        },
    }
    return job_binding, attempt_binding, completion_binding, verifier_binding


def _insert_late_envelope(
    cursor: Cursor[Any], envelope: M5TypedDirectLateReturnEnvelope
) -> None:
    bindings = _envelope_json_bindings(envelope)
    discovery_binding: dict[str, object] | None = None
    scope_binding: dict[str, object] | None = None
    verifier_binding: dict[str, object] | None = None
    if envelope.discovery is not None:
        discovery_binding = bindings[3]
        scope_binding = bindings[4]
    else:
        verifier_binding = bindings[3]
    cursor.execute(
        """
        INSERT INTO groundloop_m5_typed_direct_late_return_envelope (
            epoch_id, return_kind, job_id, attempt_id,
            result_artifact_id, result_artifact_hash,
            verification_execution_present, observation_eligible_for_currency,
            requested_make_effective, job_binding, attempt_binding,
            completion_binding, discovery_binding, scope_binding,
            verifier_binding, job_binding_digest, attempt_binding_digest,
            completion_binding_digest, discovery_binding_digest,
            scope_binding_digest, verifier_binding_digest, envelope_digest
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            envelope.epoch_id,
            envelope.return_kind.value,
            envelope.job_id,
            envelope.attempt_id,
            envelope.result_artifact_id,
            envelope.result_artifact_hash,
            envelope.verification_execution_present,
            envelope.observation_eligible_for_currency,
            envelope.requested_make_effective,
            Jsonb(bindings[0]),
            Jsonb(bindings[1]),
            Jsonb(bindings[2]),
            None if discovery_binding is None else Jsonb(discovery_binding),
            None if scope_binding is None else Jsonb(scope_binding),
            None if verifier_binding is None else Jsonb(verifier_binding),
            envelope.job_binding_digest,
            envelope.attempt_binding_digest,
            envelope.completion_binding_digest,
            envelope.discovery_binding_digest,
            envelope.scope_binding_digest,
            envelope.verifier_binding_digest,
            envelope.envelope_digest,
        ),
    )


def _assert_late_envelope_replay(
    cursor: Cursor[Any], envelope: M5TypedDirectLateReturnEnvelope
) -> None:
    row = cursor.execute(
        """
        SELECT return_kind, job_id, attempt_id, result_artifact_id,
               btrim(result_artifact_hash), verification_execution_present,
               observation_eligible_for_currency, requested_make_effective,
               job_binding, attempt_binding, completion_binding,
               discovery_binding, scope_binding, verifier_binding,
               btrim(job_binding_digest), btrim(attempt_binding_digest),
               btrim(completion_binding_digest), btrim(discovery_binding_digest),
               btrim(scope_binding_digest), btrim(verifier_binding_digest),
               btrim(envelope_digest)
        FROM groundloop_m5_typed_direct_late_return_envelope
        WHERE epoch_id = %s AND attempt_id = %s
        """,
        (envelope.epoch_id, envelope.attempt_id),
    ).fetchone()
    if row is None:
        raise ValidationError("direct late evidence lacks its byte-total envelope")
    bindings = _envelope_json_bindings(envelope)
    expected_discovery = bindings[3] if envelope.discovery is not None else None
    expected_scope = bindings[4] if envelope.discovery is not None else None
    expected_verifier = bindings[3] if envelope.discovery is None else None
    expected = (
        envelope.return_kind.value,
        envelope.job_id,
        envelope.attempt_id,
        envelope.result_artifact_id,
        envelope.result_artifact_hash,
        envelope.verification_execution_present,
        envelope.observation_eligible_for_currency,
        envelope.requested_make_effective,
        bindings[0],
        bindings[1],
        bindings[2],
        expected_discovery,
        expected_scope,
        expected_verifier,
        envelope.job_binding_digest,
        envelope.attempt_binding_digest,
        envelope.completion_binding_digest,
        envelope.discovery_binding_digest,
        envelope.scope_binding_digest,
        envelope.verifier_binding_digest,
        envelope.envelope_digest,
    )
    stripped_indexes = {4, 14, 15, 16, 17, 18, 19, 20}
    actual = tuple(
        None
        if value is None
        else (_strip(value) if index in stripped_indexes else value)
        for index, value in enumerate(row)
    )
    if actual != expected:
        raise EventConflictError("typed-direct late envelope replay differs")


def _current_terminal_result_hash(cursor: Cursor[Any], *, epoch_id: int) -> str | None:
    row = cursor.execute(
        "SELECT logical_result_hash FROM groundloop_m5_event_result "
        "WHERE epoch_id = %s",
        (epoch_id,),
    ).fetchone()
    return None if row is None else _strip(row[0])


def _terminal_hash_for_locked_cutoff(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    locked: _LockedDirectHeader,
) -> str | None:
    result_hash = _current_terminal_result_hash(cursor, epoch_id=epoch_id)
    terminal = locked.runtime_state in _TERMINAL_RUNTIME_STATES
    if terminal != (result_hash is not None):
        raise ValidationError(
            "typed-direct terminal state and immutable event result diverged"
        )
    return result_hash


def _insert_expired_return(
    cursor: Cursor[Any],
    *,
    envelope: M5TypedDirectLateReturnEnvelope,
    binding: _DirectAttemptBinding,
    evidence: M5AttemptExecutionEvidence,
    activity_revision: int,
    received_after_terminal: bool,
) -> M5ExpiredAttemptReturn:
    expired = M5ExpiredAttemptReturn.build(
        subgraph=M5RuntimeSubgraph.DIRECT,
        epoch_id=envelope.epoch_id,
        attempt_id=envelope.attempt_id,
        logical_job_id=envelope.job_id,
        worker_output_digest=envelope.envelope_digest,
        worker_artifact_hash=envelope.result_artifact_hash,
        activity_snapshot_epoch_id=envelope.epoch_id,
        activity_snapshot_revision=activity_revision,
        received_after_terminal=received_after_terminal,
    )
    cursor.execute(
        """
        INSERT INTO groundloop_m5_expired_attempt_return (
            epoch_id, subgraph, attempt_id, logical_job_id,
            original_lease_token_hash, original_lease_expires_at,
            worker_output_digest, worker_artifact_hash,
            activity_snapshot_epoch_id, activity_snapshot_revision,
            cancellation_attribution, archive_reason,
            execution_evidence_digest, received_after_terminal,
            expired_return_digest
        ) VALUES (
            %s, 'direct', %s, %s, %s, %s, %s, %s, %s, %s,
            jsonb_build_object(
                'cancelled_by_event_id', NULL,
                'cancelled_by_epoch_id', NULL,
                'cancellation_reason', NULL
            ), 'attempt_expired', %s, %s, %s
        )
        """,
        (
            envelope.epoch_id,
            envelope.attempt_id,
            envelope.job_id,
            binding.attempt.lease_token_hash,
            binding.lease_expires_at,
            envelope.envelope_digest,
            envelope.result_artifact_hash,
            envelope.epoch_id,
            activity_revision,
            evidence.evidence_digest,
            received_after_terminal,
            expired.expired_return_digest,
        ),
    )
    return expired


def _insert_postterminal_timing(
    cursor: Cursor[Any],
    *,
    evidence: M5AttemptExecutionEvidence,
    observation: M5RuntimeTimingObservation,
) -> None:
    evidence.validate_timing(observation)
    values = _timing_row_values(observation)
    cursor.execute(
        """
        INSERT INTO groundloop_m5_post_terminal_attempt_timing (
            epoch_id, subgraph, attempt_id, required_interval_observed,
            coordinator_non_db_non_neural_ns, neural_wall_ns,
            postgres_roundtrip_wall_ns, external_io_wall_ns,
            end_to_end_wall_ns, postgres_server_execution_ns,
            postgres_lock_wait_ns, postgres_wal_bytes,
            postgres_shared_block_reads, observation_digest,
            attempt_timing_digest
        ) VALUES (
            %s, 'direct', %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
        """,
        (
            evidence.epoch_id,
            evidence.attempt_id,
            observation.required_interval_observed,
            *values,
            observation.observation_digest,
            evidence.attempt_timing_digest,
        ),
    )


def _insert_postterminal_audit(
    cursor: Cursor[Any],
    *,
    evidence: M5AttemptExecutionEvidence,
    return_kind: Literal["expired_return", "terminal_audit_only"],
    return_artifact_digest: str,
    terminal_logical_result_hash: str,
) -> None:
    cursor.execute(
        """
        INSERT INTO groundloop_m5_post_terminal_attempt_audit (
            epoch_id, subgraph, attempt_id, return_kind,
            return_artifact_digest, execution_evidence_digest,
            work_digest, timing_digest, terminal_logical_result_hash
        ) VALUES (%s, 'direct', %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            evidence.epoch_id,
            evidence.attempt_id,
            return_kind,
            return_artifact_digest,
            evidence.evidence_digest,
            evidence.attempt_work.work_digest,
            evidence.attempt_timing_digest,
            terminal_logical_result_hash,
        ),
    )


def _assert_postterminal_timing_replay(
    cursor: Cursor[Any],
    *,
    evidence: M5AttemptExecutionEvidence,
    observation: M5RuntimeTimingObservation,
) -> None:
    row = cursor.execute(
        """
        SELECT required_interval_observed,
               coordinator_non_db_non_neural_ns, neural_wall_ns,
               postgres_roundtrip_wall_ns, external_io_wall_ns,
               end_to_end_wall_ns, postgres_server_execution_ns,
               postgres_lock_wait_ns, postgres_wal_bytes,
               postgres_shared_block_reads, btrim(observation_digest),
               btrim(attempt_timing_digest)
        FROM groundloop_m5_post_terminal_attempt_timing
        WHERE epoch_id = %s AND subgraph = 'direct' AND attempt_id = %s
        """,
        (evidence.epoch_id, evidence.attempt_id),
    ).fetchone()
    expected = (
        observation.required_interval_observed,
        *_timing_row_values(observation),
        observation.observation_digest,
        evidence.attempt_timing_digest,
    )
    if row is None or tuple(row) != expected:
        raise EventConflictError("postterminal direct timing replay differs")


def _assert_expired_return_replay(
    cursor: Cursor[Any],
    *,
    envelope: M5TypedDirectLateReturnEnvelope,
    binding: _DirectAttemptBinding,
    evidence: M5AttemptExecutionEvidence,
    expected_digest: str,
) -> None:
    row = cursor.execute(
        """
        SELECT btrim(original_lease_token_hash), original_lease_expires_at,
               btrim(worker_output_digest), btrim(worker_artifact_hash),
               activity_snapshot_epoch_id, activity_snapshot_revision,
               cancellation_attribution, archive_reason,
               btrim(execution_evidence_digest), received_after_terminal,
               btrim(expired_return_digest)
        FROM groundloop_m5_expired_attempt_return
        WHERE epoch_id = %s AND subgraph = 'direct' AND attempt_id = %s
        """,
        (envelope.epoch_id, envelope.attempt_id),
    ).fetchone()
    if row is None:
        raise ValidationError("expired direct replay lacks its audit row")
    expected_return = M5ExpiredAttemptReturn.build(
        subgraph=M5RuntimeSubgraph.DIRECT,
        epoch_id=envelope.epoch_id,
        attempt_id=envelope.attempt_id,
        logical_job_id=envelope.job_id,
        worker_output_digest=envelope.envelope_digest,
        worker_artifact_hash=envelope.result_artifact_hash,
        activity_snapshot_epoch_id=int(row[4]),
        activity_snapshot_revision=int(row[5]),
        received_after_terminal=bool(row[9]),
    )
    expected = (
        binding.attempt.lease_token_hash,
        binding.lease_expires_at,
        envelope.envelope_digest,
        envelope.result_artifact_hash,
        envelope.epoch_id,
        int(row[5]),
        {
            "cancelled_by_event_id": None,
            "cancelled_by_epoch_id": None,
            "cancellation_reason": None,
        },
        "attempt_expired",
        evidence.evidence_digest,
        bool(row[9]),
        expected_return.expired_return_digest,
    )
    if (
        tuple(row) != expected
        or expected_return.expired_return_digest != expected_digest
    ):
        raise EventConflictError("expired direct return replay differs")


def _assert_postterminal_audit_replay(
    cursor: Cursor[Any],
    *,
    stored: _StoredDirectReturn,
    envelope: M5TypedDirectLateReturnEnvelope,
    evidence: M5AttemptExecutionEvidence,
    current_terminal_hash: str,
) -> None:
    row = cursor.execute(
        """
        SELECT return_kind, btrim(return_artifact_digest),
               btrim(execution_evidence_digest), btrim(work_digest),
               btrim(timing_digest), btrim(terminal_logical_result_hash)
        FROM groundloop_m5_post_terminal_attempt_audit
        WHERE epoch_id = %s AND subgraph = 'direct' AND attempt_id = %s
        """,
        (envelope.epoch_id, envelope.attempt_id),
    ).fetchone()
    return_kind = (
        "expired_return"
        if stored.expired_return_digest is not None
        else "terminal_audit_only"
    )
    return_digest = stored.expired_return_digest or envelope.envelope_digest
    expected = (
        return_kind,
        return_digest,
        evidence.evidence_digest,
        evidence.attempt_work.work_digest,
        evidence.attempt_timing_digest,
        current_terminal_hash,
    )
    if row is None or tuple(row) != expected:
        raise EventConflictError("postterminal direct audit replay differs")


def _load_stored_direct_return(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    attempt_id: str,
) -> _StoredDirectReturn | None:
    evidence = _load_execution_evidence(
        cursor, epoch_id=epoch_id, attempt_id=attempt_id
    )
    if evidence is None:
        return None
    if evidence.disposition not in {
        M5ExecutionEvidenceDisposition.RETURNED,
        M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
    }:
        raise EventConflictError("direct successful return follows failure evidence")
    envelope = cursor.execute(
        """
        SELECT btrim(envelope_digest)
        FROM groundloop_m5_typed_direct_late_return_envelope
        WHERE epoch_id = %s AND attempt_id = %s
        """,
        (epoch_id, attempt_id),
    ).fetchone()
    if envelope is None:
        return _StoredDirectReturn(
            branch="normal",
            evidence=evidence,
            disposition=None,
            return_artifact_digest=evidence.result_or_error_hash,
            expired_return_digest=None,
            terminal_logical_result_hash=None,
        )
    expired = cursor.execute(
        """
        SELECT btrim(expired_return_digest), received_after_terminal
        FROM groundloop_m5_expired_attempt_return
        WHERE epoch_id = %s AND subgraph = 'direct' AND attempt_id = %s
        """,
        (epoch_id, attempt_id),
    ).fetchone()
    audit = cursor.execute(
        """
        SELECT return_kind, btrim(return_artifact_digest),
               btrim(terminal_logical_result_hash)
        FROM groundloop_m5_post_terminal_attempt_audit
        WHERE epoch_id = %s AND subgraph = 'direct' AND attempt_id = %s
        """,
        (epoch_id, attempt_id),
    ).fetchone()
    if expired is not None:
        postterminal = bool(expired[1])
        disposition = (
            M5DirectLateReturnDisposition.EXPIRED_POSTTERMINAL
            if postterminal
            else M5DirectLateReturnDisposition.EXPIRED_PRETERMINAL
        )
        if postterminal and (
            audit is None
            or str(audit[0]) != "expired_return"
            or _strip(audit[1]) != _strip(expired[0])
        ):
            raise ValidationError("expired postterminal direct audit is incomplete")
        if not postterminal and audit is not None:
            raise ValidationError("preterminal expired return has postterminal audit")
        return _StoredDirectReturn(
            branch="late",
            evidence=evidence,
            disposition=disposition,
            return_artifact_digest=_strip(envelope[0]),
            expired_return_digest=_strip(expired[0]),
            terminal_logical_result_hash=(None if audit is None else _strip(audit[2])),
        )
    if audit is not None:
        if str(audit[0]) != "terminal_audit_only" or _strip(audit[1]) != _strip(
            envelope[0]
        ):
            raise ValidationError("postterminal direct audit envelope differs")
        return _StoredDirectReturn(
            branch="late",
            evidence=evidence,
            disposition=(M5DirectLateReturnDisposition.TERMINAL_AUDIT_POSTTERMINAL),
            return_artifact_digest=_strip(envelope[0]),
            expired_return_digest=None,
            terminal_logical_result_hash=_strip(audit[2]),
        )
    contribution = cursor.execute(
        """
        SELECT btrim(source_identity_hash)
        FROM groundloop_m5_runtime_work_contribution
        WHERE epoch_id = %s AND contribution_kind = 'preterminal_late_return'
          AND source_id = %s
        """,
        (epoch_id, attempt_id),
    ).fetchone()
    if contribution is None or _strip(contribution[0]) != _strip(envelope[0]):
        raise ValidationError("preterminal direct audit lacks its late contribution")
    return _StoredDirectReturn(
        branch="late",
        evidence=evidence,
        disposition=M5DirectLateReturnDisposition.TERMINAL_AUDIT_PRETERMINAL,
        return_artifact_digest=_strip(envelope[0]),
        expired_return_digest=None,
        terminal_logical_result_hash=None,
    )


def _direct_transition_identity(
    cursor: Cursor[Any], *, epoch_id: int, transition_id: str, revision: int
) -> str:
    row = cursor.execute(
        """
        SELECT btrim(payload_hash)
        FROM groundloop_m4_evaluation_counter_transition
        WHERE epoch_id = %s AND transition_id = %s AND to_revision = %s
        """,
        (epoch_id, transition_id, revision),
    ).fetchone()
    if row is None:
        raise ValidationError("normal direct return lacks its exact M4 transition")
    return _strip(row[0])


def _direct_transition_work(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    resulting_revision: int,
    observation_completion: ObservationCompletionReceipt | None,
) -> M5RuntimeWork:
    if observation_completion is None:
        return M5RuntimeWork()
    claim_count = 0
    answer_count = 0
    if observation_completion.made_effective:
        claim_row = cursor.execute(
            """
            SELECT count(*)
            FROM groundloop_m4_working_claim_state
            WHERE epoch_id = %s AND updated_revision = %s
            """,
            (epoch_id, resulting_revision),
        ).fetchone()
        answer_row = cursor.execute(
            """
            SELECT count(*)
            FROM groundloop_m4_working_answer_state
            WHERE epoch_id = %s AND updated_revision = %s
            """,
            (epoch_id, resulting_revision),
        ).fetchone()
        assert claim_row is not None and answer_row is not None
        claim_count = int(claim_row[0])
        answer_count = int(answer_row[0])
    return M5RuntimeWork(
        direct_observation_artifact_count=int(observation_completion.artifact_stored),
        direct_effective_observation_count=int(observation_completion.made_effective),
        direct_inactive_completion_count=int(not observation_completion.made_effective),
        claim_state_write_count=claim_count,
        answer_state_write_count=answer_count,
    )


def _load_direct_transition_work(
    cursor: Cursor[Any], *, epoch_id: int, transition_id: str
) -> M5RuntimeWork:
    row = cursor.execute(
        sql.SQL(
            "SELECT {}, btrim(work_digest) "
            "FROM groundloop_m5_runtime_work_contribution "
            "WHERE epoch_id = %s AND contribution_kind = 'direct_transition' "
            "AND source_id = %s"
        ).format(
            sql.SQL(", ").join(sql.Identifier(name) for name in _WORK_COUNTER_COLUMNS)
        ),
        (epoch_id, transition_id),
    ).fetchone()
    if row is None:
        raise ValidationError("normal direct return lacks its transition work")
    values = tuple(int(value) for value in row[: len(_WORK_COUNTER_COLUMNS)])
    work = replace(
        M5RuntimeWork(),
        **dict(zip(_WORK_COUNTER_COLUMNS, values, strict=True)),
        work_digest="",
    )
    if _strip(row[-1]) != work.work_digest:
        raise EventConflictError("normal direct transition work digest differs")
    return work


def _assert_direct_transition_replay(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    transition_id: str,
    source_identity_hash: str,
    work: M5RuntimeWork,
) -> str:
    row = cursor.execute(
        sql.SQL(
            "SELECT {}, btrim(work_digest), btrim(source_identity_hash), "
            "btrim(contribution_key_digest) "
            "FROM groundloop_m5_runtime_work_contribution "
            "WHERE epoch_id = %s AND contribution_kind = 'direct_transition' "
            "AND source_id = %s"
        ).format(
            sql.SQL(", ").join(sql.Identifier(name) for name in _WORK_COUNTER_COLUMNS)
        ),
        (epoch_id, transition_id),
    ).fetchone()
    expected_key = digests.runtime_work_contribution_key_digest(
        epoch_id=epoch_id,
        contribution_kind=M5RuntimeWorkContributionKind.DIRECT_TRANSITION,
        source_id=transition_id,
    )
    offset = len(_WORK_COUNTER_COLUMNS)
    if row is None or (
        tuple(int(value) for value in row[:offset]),
        str(row[offset]),
        str(row[offset + 1]),
        str(row[offset + 2]),
    ) != (
        work.counter_values(),
        work.work_digest,
        source_identity_hash,
        expected_key,
    ):
        raise EventConflictError("normal direct-transition replay differs")
    return expected_key


def _assert_normal_discovery_replay(
    cursor: Cursor[Any],
    *,
    ports: PostgresM4ApplicationPorts,
    envelope: M5TypedDirectLateReturnEnvelope,
    binding: _DirectAttemptBinding,
    children: tuple[LogicalJobSpec, ...],
) -> tuple[str, str, str]:
    point_job = ports.runtime_store.read_job_point(
        envelope.epoch_id, envelope.job_id, cursor=cursor
    )
    completion = envelope.completion
    closure = completion.child_closure
    if closure is None:
        raise ValidationError("normal direct discovery lacks its child closure")
    if (
        point_job.spec != envelope.job
        or point_job.state is not completion.terminal_state
        or not point_job.child_closed
        or point_job.child_set_hash != closure.child_set_hash
        or point_job.completion_digest != completion.completion_digest
        or point_job.result_artifact_id != completion.result_artifact_id
        or point_job.result_artifact_hash != completion.result_artifact_hash
        or binding.state != "completed"
    ):
        raise EventConflictError("normal direct completion replay differs")
    stored_children = ports.runtime_store.read_children_point(
        envelope.epoch_id, envelope.job_id, cursor=cursor
    )
    if stored_children != children:
        raise EventConflictError("normal direct child-spec replay differs")
    projection = _load_terminal_projection(cursor, envelope.epoch_id, envelope.job_id)
    terminal_reason = (
        None
        if completion.terminal_state is JobState.COMPLETED_ACTIVE
        else "inactive_at_completion"
    )
    expected_projection = M5TypedDirectTerminalProjection.build(
        job_id=envelope.job_id,
        terminal_state=completion.terminal_state,
        terminal_reason=terminal_reason,
        m4_completion_digest=completion.completion_digest,
        completed_revision=projection.completed_revision,
    )
    if projection != expected_projection:
        raise EventConflictError("normal direct terminal projection replay differs")
    _assert_discovery_scope_binding(
        cursor,
        envelope=envelope,
        resulting_revision=projection.completed_revision,
        replay=True,
    )
    _assert_discovery_artifact_replay(cursor, envelope=envelope)
    source_id = completion.completion_digest
    source_identity = _direct_transition_identity(
        cursor,
        epoch_id=envelope.epoch_id,
        transition_id=source_id,
        revision=projection.completed_revision,
    )
    contribution_key = _assert_direct_transition_replay(
        cursor,
        epoch_id=envelope.epoch_id,
        transition_id=source_id,
        source_identity_hash=source_identity,
        work=M5RuntimeWork(),
    )
    return source_id, source_identity, contribution_key


def _assert_normal_verifier_replay(
    cursor: Cursor[Any],
    *,
    ports: PostgresM4ApplicationPorts,
    envelope: M5TypedDirectLateReturnEnvelope,
    binding: _DirectAttemptBinding,
) -> tuple[str, str, str, ObservationCompletionReceipt]:
    point_job = ports.runtime_store.read_job_point(
        envelope.epoch_id, envelope.job_id, cursor=cursor
    )
    completion = envelope.completion
    if (
        point_job.spec != envelope.job
        or point_job.state is not completion.terminal_state
        or point_job.child_closed
        or point_job.child_set_hash is not None
        or point_job.completion_digest != completion.completion_digest
        or point_job.result_artifact_id != completion.result_artifact_id
        or point_job.result_artifact_hash != completion.result_artifact_hash
        or binding.state != "completed"
        or ports.runtime_store.read_children_point(
            envelope.epoch_id, envelope.job_id, cursor=cursor
        )
    ):
        raise EventConflictError("normal direct verifier completion replay differs")
    projection = _load_terminal_projection(cursor, envelope.epoch_id, envelope.job_id)
    terminal_reason = (
        None
        if completion.terminal_state is JobState.COMPLETED_ACTIVE
        else "inactive_at_completion"
    )
    expected_projection = M5TypedDirectTerminalProjection.build(
        job_id=envelope.job_id,
        terminal_state=completion.terminal_state,
        terminal_reason=terminal_reason,
        m4_completion_digest=completion.completion_digest,
        completed_revision=projection.completed_revision,
    )
    if projection != expected_projection:
        raise EventConflictError("normal direct verifier projection replay differs")
    _assert_verifier_artifact_replay(cursor, envelope=envelope)
    source_id = completion.completion_digest
    source_identity = _direct_transition_identity(
        cursor,
        epoch_id=envelope.epoch_id,
        transition_id=source_id,
        revision=projection.completed_revision,
    )
    stored_work = _load_direct_transition_work(
        cursor, epoch_id=envelope.epoch_id, transition_id=source_id
    )
    artifact_count = stored_work.direct_observation_artifact_count
    if artifact_count not in {0, 1}:
        raise EventConflictError("normal direct verifier receipt count is invalid")
    assert envelope.requested_make_effective is not None
    observation_receipt = ObservationCompletionReceipt(
        artifact_stored=bool(artifact_count),
        made_effective=envelope.requested_make_effective,
    )
    expected_work = M5RuntimeWork(
        direct_observation_artifact_count=int(observation_receipt.artifact_stored),
        direct_effective_observation_count=int(observation_receipt.made_effective),
        direct_inactive_completion_count=int(not observation_receipt.made_effective),
        claim_state_write_count=stored_work.claim_state_write_count,
        answer_state_write_count=stored_work.answer_state_write_count,
    )
    if stored_work != expected_work or (
        not observation_receipt.made_effective
        and (
            stored_work.claim_state_write_count != 0
            or stored_work.answer_state_write_count != 0
        )
    ):
        raise EventConflictError("normal direct verifier transition work differs")
    contribution_key = _assert_direct_transition_replay(
        cursor,
        epoch_id=envelope.epoch_id,
        transition_id=source_id,
        source_identity_hash=source_identity,
        work=expected_work,
    )
    return source_id, source_identity, contribution_key, observation_receipt


def _assert_preterminal_late_replay(
    cursor: Cursor[Any],
    *,
    envelope: M5TypedDirectLateReturnEnvelope,
    stored: _StoredDirectReturn,
    observation: M5RuntimeTimingObservation,
    expected_evidence: M5AttemptExecutionEvidence,
) -> tuple[str, str]:
    attempt_key = _assert_attempt_accounting_replay(
        cursor, evidence=expected_evidence, observation=observation
    )
    assert stored.disposition is not None
    source_identity = (
        stored.expired_return_digest
        if stored.expired_return_digest is not None
        else envelope.envelope_digest
    )
    late_work = M5RuntimeWork(requirement_late_attempt_artifact_count=1)
    row = cursor.execute(
        sql.SQL(
            "SELECT {}, btrim(work_digest), btrim(source_identity_hash), "
            "btrim(contribution_key_digest) "
            "FROM groundloop_m5_runtime_work_contribution "
            "WHERE epoch_id = %s AND contribution_kind = "
            "'preterminal_late_return' AND source_id = %s"
        ).format(
            sql.SQL(", ").join(sql.Identifier(name) for name in _WORK_COUNTER_COLUMNS)
        ),
        (envelope.epoch_id, envelope.attempt_id),
    ).fetchone()
    expected_key = digests.runtime_work_contribution_key_digest(
        epoch_id=envelope.epoch_id,
        contribution_kind=M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN,
        source_id=envelope.attempt_id,
    )
    offset = len(_WORK_COUNTER_COLUMNS)
    if row is None or (
        tuple(int(value) for value in row[:offset]),
        str(row[offset]),
        str(row[offset + 1]),
        str(row[offset + 2]),
    ) != (
        late_work.counter_values(),
        late_work.work_digest,
        source_identity,
        expected_key,
    ):
        raise EventConflictError("preterminal direct late replay differs")
    return attempt_key, expected_key


def _replay_direct_late_return(
    cursor: Cursor[Any],
    *,
    locked: _LockedDirectHeader,
    stored: _StoredDirectReturn,
    envelope: M5TypedDirectLateReturnEnvelope,
    binding: _DirectAttemptBinding,
    evidence: M5AttemptExecutionEvidence,
    observation: M5RuntimeTimingObservation,
    current_hash: str | None,
) -> _DirectReturnSettlement:
    assert stored.disposition is not None
    _assert_late_envelope_replay(cursor, envelope)
    if stored.expired_return_digest is not None:
        _assert_expired_return_replay(
            cursor,
            envelope=envelope,
            binding=binding,
            evidence=evidence,
            expected_digest=stored.expired_return_digest,
        )
    preterminal = stored.disposition in {
        M5DirectLateReturnDisposition.EXPIRED_PRETERMINAL,
        M5DirectLateReturnDisposition.TERMINAL_AUDIT_PRETERMINAL,
    }
    if preterminal:
        attempt_key, late_key = _assert_preterminal_late_replay(
            cursor,
            envelope=envelope,
            stored=stored,
            observation=observation,
            expected_evidence=evidence,
        )
        postterminal_hash = None
    else:
        if current_hash is None:
            raise ValidationError("postterminal direct replay lost its event result")
        _assert_postterminal_timing_replay(
            cursor, evidence=evidence, observation=observation
        )
        _assert_postterminal_audit_replay(
            cursor,
            stored=stored,
            envelope=envelope,
            evidence=evidence,
            current_terminal_hash=current_hash,
        )
        attempt_key = None
        late_key = None
        postterminal_hash = current_hash
    late = M5DirectLateCursorContributionReceipt(
        disposition=stored.disposition,
        epoch_id=envelope.epoch_id,
        job_id=envelope.job_id,
        attempt_id=envelope.attempt_id,
        envelope_digest=envelope.envelope_digest,
        execution_evidence_digest=evidence.evidence_digest,
        expired_return_digest=stored.expired_return_digest,
        attempt_execution_contribution_key_digest=attempt_key,
        preterminal_late_contribution_key_digest=late_key,
        postterminal_logical_result_hash=postterminal_hash,
    )
    return _DirectReturnSettlement(
        normal=None,
        late=late,
        resulting_revision=locked.runtime_revision,
        exact_replay=True,
        current_terminal_logical_result_hash=current_hash,
    )


def _replay_direct_discovery_return(
    cursor: Cursor[Any],
    *,
    ports: PostgresM4ApplicationPorts,
    locked: _LockedDirectHeader,
    stored: _StoredDirectReturn,
    envelope: M5TypedDirectLateReturnEnvelope,
    binding: _DirectAttemptBinding,
    children: tuple[LogicalJobSpec, ...],
    evidence: M5AttemptExecutionEvidence,
    observation: M5RuntimeTimingObservation,
) -> _DirectReturnSettlement:
    current_hash = _terminal_hash_for_locked_cutoff(
        cursor, epoch_id=envelope.epoch_id, locked=locked
    )
    if stored.evidence != evidence:
        raise EventConflictError("direct successful-return replay differs")
    if stored.branch == "normal":
        if stored.return_artifact_digest != envelope.result_artifact_hash:
            raise EventConflictError("normal direct return artifact replay differs")
        normal_attempt_key = _assert_attempt_accounting_replay(
            cursor, evidence=evidence, observation=observation
        )
        source_id, source_identity, transition_key = _assert_normal_discovery_replay(
            cursor,
            ports=ports,
            envelope=envelope,
            binding=binding,
            children=children,
        )
        normal = M5DirectCursorContributionReceipt(
            epoch_id=envelope.epoch_id,
            job_id=envelope.job_id,
            attempt_id=envelope.attempt_id,
            execution_evidence_digest=evidence.evidence_digest,
            attempt_execution_contribution_key_digest=normal_attempt_key,
            direct_transition_source_id=source_id,
            direct_transition_source_identity_hash=source_identity,
            direct_transition_contribution_key_digest=transition_key,
            observation_completion=None,
        )
        return _DirectReturnSettlement(
            normal=normal,
            late=None,
            resulting_revision=locked.runtime_revision,
            exact_replay=True,
            current_terminal_logical_result_hash=current_hash,
        )

    return _replay_direct_late_return(
        cursor,
        locked=locked,
        stored=stored,
        envelope=envelope,
        binding=binding,
        evidence=evidence,
        observation=observation,
        current_hash=current_hash,
    )


def _replay_direct_verifier_return(
    cursor: Cursor[Any],
    *,
    ports: PostgresM4ApplicationPorts,
    locked: _LockedDirectHeader,
    stored: _StoredDirectReturn,
    envelope: M5TypedDirectLateReturnEnvelope,
    binding: _DirectAttemptBinding,
    evidence: M5AttemptExecutionEvidence,
    observation: M5RuntimeTimingObservation,
) -> _DirectReturnSettlement:
    current_hash = _terminal_hash_for_locked_cutoff(
        cursor, epoch_id=envelope.epoch_id, locked=locked
    )
    if stored.evidence != evidence:
        raise EventConflictError("direct successful-return replay differs")
    if stored.branch != "normal":
        return _replay_direct_late_return(
            cursor,
            locked=locked,
            stored=stored,
            envelope=envelope,
            binding=binding,
            evidence=evidence,
            observation=observation,
            current_hash=current_hash,
        )
    if stored.return_artifact_digest != envelope.result_artifact_hash:
        raise EventConflictError("normal direct return artifact replay differs")
    attempt_key = _assert_attempt_accounting_replay(
        cursor, evidence=evidence, observation=observation
    )
    source_id, source_identity, transition_key, observation_receipt = (
        _assert_normal_verifier_replay(
            cursor,
            ports=ports,
            envelope=envelope,
            binding=binding,
        )
    )
    normal = M5DirectCursorContributionReceipt(
        epoch_id=envelope.epoch_id,
        job_id=envelope.job_id,
        attempt_id=envelope.attempt_id,
        execution_evidence_digest=evidence.evidence_digest,
        attempt_execution_contribution_key_digest=attempt_key,
        direct_transition_source_id=source_id,
        direct_transition_source_identity_hash=source_identity,
        direct_transition_contribution_key_digest=transition_key,
        observation_completion=observation_receipt,
    )
    return _DirectReturnSettlement(
        normal=normal,
        late=None,
        resulting_revision=locked.runtime_revision,
        exact_replay=True,
        current_terminal_logical_result_hash=current_hash,
    )


def _assert_literal_recovery_bundle(cursor: Cursor[Any]) -> None:
    row = cursor.execute(
        """
        SELECT bundle_sha256, migration_sha256, oracle_sha256,
               prerequisite_sha256
        FROM groundloop_m5_schema_bundle
        WHERE bundle_id = %s
        """,
        (_RECOVERY_BUNDLE_ID,),
    ).fetchone()
    expected = (
        _RECOVERY_BUNDLE_SHA256,
        _RECOVERY_MIGRATION_SHA256,
        _RECOVERY_ORACLE_SHA256,
        _RECOVERY_PREREQUISITE_SHA256,
    )
    if row is None or tuple(_strip(value) for value in row) != expected:
        raise ValidationError(
            "accepted literal migration-016 recovery ledger tuple is required"
        )


def _lock_direct_header(cursor: Cursor[Any], epoch_id: int) -> _LockedDirectHeader:
    base = cursor.execute(
        """
        SELECT revision, semantic_status
        FROM groundloop_epoch
        WHERE epoch_id = %s
        FOR UPDATE
        """,
        (epoch_id,),
    ).fetchone()
    if base is None:
        raise InvalidEventError(f"unknown typed-direct epoch_id: {epoch_id}")
    runtime = cursor.execute(
        """
        SELECT revision, runtime_state
        FROM groundloop_m5_runtime_epoch
        WHERE epoch_id = %s
        FOR UPDATE
        """,
        (epoch_id,),
    ).fetchone()
    if runtime is None:
        raise InvalidEventError("typed-direct epoch lacks its M5 runtime header")
    locked = _LockedDirectHeader(
        base_revision=int(base[0]),
        semantic_status=str(base[1]),
        runtime_revision=int(runtime[0]),
        runtime_state=str(runtime[1]),
    )
    if locked.base_revision != locked.runtime_revision:
        raise ValidationError("typed-direct base/runtime revisions diverged")
    return locked


def _sample_database_deadline(
    cursor: Cursor[Any], epoch_id: int
) -> tuple[datetime, datetime]:
    config = cursor.execute(
        """
        SELECT lease_duration_ms
        FROM groundloop_m5_runtime_operational_config
        WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    if config is None:
        raise ValidationError("typed-direct epoch lacks its D24 operational config")
    row = cursor.execute(
        """
        WITH decision AS MATERIALIZED (
            SELECT clock_timestamp() AS decision_time
        )
        SELECT decision_time,
               decision_time + %s * interval '1 millisecond'
        FROM decision
        """,
        (int(config[0]),),
    ).fetchone()
    assert row is not None
    if not isinstance(row[0], datetime) or not isinstance(row[1], datetime):
        raise ValidationError("PostgreSQL returned invalid lease timestamps")
    if row[1] <= row[0]:
        raise ValidationError("PostgreSQL computed a nonpositive direct lease")
    return row[0], row[1]


def _dispatch_record_for_attempt(
    cursor: Cursor[Any], epoch_id: int, attempt_id: str
) -> tuple[str, datetime] | None:
    row = cursor.execute(
        """
        SELECT record_digest, lease_expires_at
        FROM groundloop_m5_dispatch_record
        WHERE epoch_id = %s AND subgraph = 'direct' AND attempt_id = %s
        """,
        (epoch_id, attempt_id),
    ).fetchone()
    if row is None:
        return None
    if not isinstance(row[1], datetime):
        raise ValidationError("stored direct dispatch deadline is not a timestamp")
    return _strip(row[0]), row[1]


def _load_dispatch_record(
    cursor: Cursor[Any], epoch_id: int, attempt_id: str
) -> M5DispatchRecord:
    counter_projection = sql.SQL(", ").join(
        sql.Identifier(f"maximum_{name}") for name in _WORK_COUNTER_COLUMNS
    )
    row = cursor.execute(
        sql.SQL(
            "SELECT {}, maximum_work_digest, logical_job_id, attempt_ordinal, "
            "job_kind, fallback_required, dispatched_revision, "
            "lease_expires_at, record_digest "
            "FROM groundloop_m5_dispatch_record "
            "WHERE epoch_id = %s AND subgraph = 'direct' AND attempt_id = %s"
        ).format(counter_projection),
        (epoch_id, attempt_id),
    ).fetchone()
    if row is None:
        raise ValidationError("typed-direct attempt lacks its dispatch record")
    offset = len(_WORK_COUNTER_COLUMNS)
    if not isinstance(row[offset + 6], datetime):
        raise ValidationError("stored typed-direct dispatch deadline is invalid")
    dispatch = M5DispatchRecord.build(
        epoch_id=epoch_id,
        subgraph=M5RuntimeSubgraph.DIRECT,
        attempt_id=attempt_id,
        logical_job_id=str(row[offset + 1]),
        attempt_ordinal=int(row[offset + 2]),
        job_kind=str(row[offset + 3]),
        fallback_required=bool(row[offset + 4]),
        dispatched_revision=int(row[offset + 5]),
        lease_expires_at=row[offset + 6],
    )
    stored_values = tuple(int(value) for value in row[:offset])
    if (
        stored_values != dispatch.maximum_ambiguous_call_work.counter_values()
        or _strip(row[offset]) != dispatch.maximum_ambiguous_call_work.work_digest
        or _strip(row[offset + 7]) != dispatch.record_digest
    ):
        raise ValidationError("typed-direct dispatch bytes differ from frozen identity")
    return dispatch


def _insert_dispatch_record(cursor: Cursor[Any], dispatch: M5DispatchRecord) -> None:
    columns = (
        "epoch_id",
        *tuple(f"maximum_{name}" for name in _WORK_COUNTER_COLUMNS),
        "maximum_work_digest",
        "subgraph",
        "attempt_id",
        "logical_job_id",
        "attempt_ordinal",
        "job_kind",
        "fallback_required",
        "dispatched_revision",
        "lease_expires_at",
        "record_digest",
    )
    values: tuple[object, ...] = (
        dispatch.epoch_id,
        *dispatch.maximum_ambiguous_call_work.counter_values(),
        dispatch.maximum_ambiguous_call_work.work_digest,
        dispatch.subgraph.value,
        dispatch.attempt_id,
        dispatch.logical_job_id,
        dispatch.attempt_ordinal,
        dispatch.job_kind,
        dispatch.fallback_required,
        dispatch.dispatched_revision,
        dispatch.lease_expires_at,
        dispatch.record_digest,
    )
    cursor.execute(
        sql.SQL("INSERT INTO groundloop_m5_dispatch_record ({}) VALUES ({})").format(
            sql.SQL(", ").join(sql.Identifier(name) for name in columns),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        ),
        values,
    )


def _insert_work_contribution(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    contribution_kind: M5RuntimeWorkContributionKind,
    source_id: str,
    source_identity_hash: str,
    work: M5RuntimeWork,
    applied_revision: int,
) -> str:
    key_digest = digests.runtime_work_contribution_key_digest(
        epoch_id=epoch_id,
        contribution_kind=contribution_kind,
        source_id=source_id,
    )
    columns = (
        "epoch_id",
        *_WORK_COUNTER_COLUMNS,
        "work_digest",
        "contribution_kind",
        "source_id",
        "source_identity_hash",
        "contribution_key_digest",
        "applied_revision",
    )
    values: tuple[object, ...] = (
        epoch_id,
        *work.counter_values(),
        work.work_digest,
        contribution_kind.value,
        source_id,
        source_identity_hash,
        key_digest,
        applied_revision,
    )
    cursor.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_runtime_work_contribution ({}) VALUES ({})"
        ).format(
            sql.SQL(", ").join(sql.Identifier(name) for name in columns),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        ),
        values,
    )
    return key_digest


def _execution_timing_observation(
    *, epoch_id: int, attempt_id: str, timing: M5RuntimeTiming | None
) -> tuple[M5RuntimeTimingObservation, str]:
    observation = M5RuntimeTimingObservation.build(timing)
    timing_digest = digests.attempt_runtime_timing_digest(
        epoch_id=epoch_id,
        subgraph=M5RuntimeSubgraph.DIRECT,
        attempt_id=attempt_id,
        observation_digest=observation.observation_digest,
    )
    return observation, timing_digest


def _insert_execution_evidence(
    cursor: Cursor[Any],
    *,
    evidence: M5AttemptExecutionEvidence,
    dispatch: M5DispatchRecord,
) -> None:
    evidence.validate_dispatch(dispatch)
    columns = (
        "epoch_id",
        *tuple(f"attempt_{name}" for name in _WORK_COUNTER_COLUMNS),
        "attempt_work_digest",
        "subgraph",
        "attempt_id",
        "disposition",
        "result_or_error_hash",
        "attempt_timing_digest",
        "evidence_digest",
    )
    values: tuple[object, ...] = (
        evidence.epoch_id,
        *evidence.attempt_work.counter_values(),
        evidence.attempt_work.work_digest,
        evidence.subgraph.value,
        evidence.attempt_id,
        evidence.disposition.value,
        evidence.result_or_error_hash,
        evidence.attempt_timing_digest,
        evidence.evidence_digest,
    )
    cursor.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_attempt_execution_evidence ({}) VALUES ({})"
        ).format(
            sql.SQL(", ").join(sql.Identifier(name) for name in columns),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        ),
        values,
    )


def _load_execution_evidence(
    cursor: Cursor[Any], *, epoch_id: int, attempt_id: str
) -> M5AttemptExecutionEvidence | None:
    counter_projection = sql.SQL(", ").join(
        sql.Identifier(f"attempt_{name}") for name in _WORK_COUNTER_COLUMNS
    )
    row = cursor.execute(
        sql.SQL(
            "SELECT {}, attempt_work_digest, disposition, result_or_error_hash, "
            "attempt_timing_digest, evidence_digest "
            "FROM groundloop_m5_attempt_execution_evidence "
            "WHERE epoch_id = %s AND subgraph = 'direct' AND attempt_id = %s"
        ).format(counter_projection),
        (epoch_id, attempt_id),
    ).fetchone()
    if row is None:
        return None
    offset = len(_WORK_COUNTER_COLUMNS)
    work = M5RuntimeWork(
        **dict(
            zip(
                _WORK_COUNTER_COLUMNS,
                (int(value) for value in row[:offset]),
                strict=True,
            )
        ),
        work_digest=_strip(row[offset]),
    )
    return M5AttemptExecutionEvidence(
        epoch_id=epoch_id,
        subgraph=M5RuntimeSubgraph.DIRECT,
        attempt_id=attempt_id,
        disposition=M5ExecutionEvidenceDisposition(str(row[offset + 1])),
        result_or_error_hash=_strip(row[offset + 2]),
        attempt_work=work,
        attempt_timing_digest=_strip(row[offset + 3]),
        evidence_digest=_strip(row[offset + 4]),
    )


def _insert_attempt_timing(
    cursor: Cursor[Any],
    *,
    evidence: M5AttemptExecutionEvidence,
    observation: M5RuntimeTimingObservation,
) -> None:
    evidence.validate_timing(observation)
    timing = observation.timing
    cursor.execute(
        """
        INSERT INTO groundloop_m5_runtime_timing_contribution (
            epoch_id, subgraph, attempt_id, execution_evidence_digest,
            required_interval_observed,
            coordinator_non_db_non_neural_ns, neural_wall_ns,
            postgres_roundtrip_wall_ns, external_io_wall_ns,
            end_to_end_wall_ns, postgres_server_execution_ns,
            postgres_lock_wait_ns, postgres_wal_bytes,
            postgres_shared_block_reads, observation_digest,
            attempt_timing_digest
        ) VALUES (
            %s, 'direct', %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            evidence.epoch_id,
            evidence.attempt_id,
            evidence.evidence_digest,
            observation.required_interval_observed,
            None if timing is None else timing.coordinator_non_db_non_neural_ns,
            None if timing is None else timing.neural_wall_ns,
            None if timing is None else timing.postgres_roundtrip_wall_ns,
            None if timing is None else timing.external_io_wall_ns,
            None if timing is None else timing.end_to_end_wall_ns,
            None if timing is None else timing.postgres_server_execution_ns,
            None if timing is None else timing.postgres_lock_wait_ns,
            None if timing is None else timing.postgres_wal_bytes,
            None if timing is None else timing.postgres_shared_block_reads,
            observation.observation_digest,
            evidence.attempt_timing_digest,
        ),
    )


def _assert_attempt_accounting_replay(
    cursor: Cursor[Any],
    *,
    evidence: M5AttemptExecutionEvidence,
    observation: M5RuntimeTimingObservation,
) -> str:
    """Validate the immutable evidence, timing and attempt-contribution bytes."""

    stored = _load_execution_evidence(
        cursor, epoch_id=evidence.epoch_id, attempt_id=evidence.attempt_id
    )
    if stored != evidence:
        raise EventConflictError("direct execution evidence replay differs")
    row = cursor.execute(
        sql.SQL(
            "SELECT {}, work_digest, source_identity_hash, "
            "contribution_key_digest "
            "FROM groundloop_m5_runtime_work_contribution "
            "WHERE epoch_id = %s AND contribution_kind = "
            "'direct_attempt_execution' AND source_id = %s"
        ).format(
            sql.SQL(", ").join(sql.Identifier(name) for name in _WORK_COUNTER_COLUMNS)
        ),
        (evidence.epoch_id, evidence.attempt_id),
    ).fetchone()
    if row is None:
        raise ValidationError("direct evidence lacks its attempt contribution")
    offset = len(_WORK_COUNTER_COLUMNS)
    expected_key = digests.runtime_work_contribution_key_digest(
        epoch_id=evidence.epoch_id,
        contribution_kind=M5RuntimeWorkContributionKind.DIRECT_ATTEMPT_EXECUTION,
        source_id=evidence.attempt_id,
    )
    if (
        tuple(int(value) for value in row[:offset])
        != evidence.attempt_work.counter_values()
        or _strip(row[offset]) != evidence.attempt_work.work_digest
        or _strip(row[offset + 1]) != evidence.evidence_digest
        or _strip(row[offset + 2]) != expected_key
    ):
        raise EventConflictError("direct attempt contribution replay differs")
    timing = cursor.execute(
        """
        SELECT required_interval_observed,
               coordinator_non_db_non_neural_ns, neural_wall_ns,
               postgres_roundtrip_wall_ns, external_io_wall_ns,
               end_to_end_wall_ns, postgres_server_execution_ns,
               postgres_lock_wait_ns, postgres_wal_bytes,
               postgres_shared_block_reads, observation_digest,
               attempt_timing_digest, execution_evidence_digest
        FROM groundloop_m5_runtime_timing_contribution
        WHERE epoch_id = %s AND subgraph = 'direct' AND attempt_id = %s
        """,
        (evidence.epoch_id, evidence.attempt_id),
    ).fetchone()
    expected_values = _timing_row_values(observation)
    if timing is None or (
        bool(timing[0]),
        *timing[1:10],
        _strip(timing[10]),
        _strip(timing[11]),
        _strip(timing[12]),
    ) != (
        observation.required_interval_observed,
        *expected_values,
        observation.observation_digest,
        evidence.attempt_timing_digest,
        evidence.evidence_digest,
    ):
        raise EventConflictError("direct attempt timing replay differs")
    return expected_key


def _timing_row_values(
    observation: M5RuntimeTimingObservation,
) -> tuple[int | None, ...]:
    timing = observation.timing
    if timing is None:
        return (None,) * 9
    return (
        timing.coordinator_non_db_non_neural_ns,
        timing.neural_wall_ns,
        timing.postgres_roundtrip_wall_ns,
        timing.external_io_wall_ns,
        timing.end_to_end_wall_ns,
        timing.postgres_server_execution_ns,
        timing.postgres_lock_wait_ns,
        timing.postgres_wal_bytes,
        timing.postgres_shared_block_reads,
    )


def _resolve_pending_anchor(
    cursor: Cursor[Any], *, epoch_id: int, expected_updated_revision: int
) -> tuple[object, ...]:
    row = cursor.execute(
        """
        SELECT pending_contribution_kind, pending_source_id,
               pending_contribution_key_digest, pending_anchor_revision,
               updated_revision, terminalized
        FROM groundloop_m5_runtime_timing_accumulator
        WHERE epoch_id = %s
        FOR UPDATE
        """,
        (epoch_id,),
    ).fetchone()
    if row is None:
        raise ValidationError("typed-direct epoch lacks its timing accumulator")
    if bool(row[5]):
        raise InvalidEventError("terminalized timing accumulator cannot be advanced")
    if int(row[4]) != expected_updated_revision:
        raise ValidationError(
            "typed-direct timing accumulator revision differs from locked cutoff"
        )
    if row[3] is not None:
        assert row[0] is not None and row[1] is not None and row[2] is not None
        _insert_missing_transition_timing(
            cursor,
            epoch_id=epoch_id,
            contribution_kind=str(row[0]),
            source_id=str(row[1]),
            contribution_key_digest=_strip(row[2]),
            anchor_revision=int(row[3]),
        )
    return tuple(row)


def _advance_timing_accumulator_for_attempt(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    observation: M5RuntimeTimingObservation,
    prior_revision: int,
    resulting_revision: int,
) -> None:
    prior = _resolve_pending_anchor(
        cursor,
        epoch_id=epoch_id,
        expected_updated_revision=prior_revision,
    )
    pending_missing = int(prior[3] is not None)
    timing = observation.timing
    required_observed = int(timing is not None)
    required_missing = 1 - required_observed

    def optional(value: int | None) -> tuple[int, int]:
        observed = int(timing is not None and value is not None)
        return observed, 1 - observed

    server_observed, server_missing = optional(
        None if timing is None else timing.postgres_server_execution_ns
    )
    lock_observed, lock_missing = optional(
        None if timing is None else timing.postgres_lock_wait_ns
    )
    wal_observed, wal_missing = optional(
        None if timing is None else timing.postgres_wal_bytes
    )
    blocks_observed, blocks_missing = optional(
        None if timing is None else timing.postgres_shared_block_reads
    )
    values = _timing_row_values(observation)
    changed = cursor.execute(
        """
        UPDATE groundloop_m5_runtime_timing_accumulator
        SET coordinator_non_db_non_neural_ns =
                coordinator_non_db_non_neural_ns + %s,
            neural_wall_ns = neural_wall_ns + %s,
            postgres_roundtrip_wall_ns = postgres_roundtrip_wall_ns + %s,
            external_io_wall_ns = external_io_wall_ns + %s,
            end_to_end_wall_ns = end_to_end_wall_ns + %s,
            postgres_server_execution_ns = postgres_server_execution_ns + %s,
            postgres_lock_wait_ns = postgres_lock_wait_ns + %s,
            postgres_wal_bytes = postgres_wal_bytes + %s,
            postgres_shared_block_reads = postgres_shared_block_reads + %s,
            required_expected_count = required_expected_count + 1,
            required_observed_count = required_observed_count + %s,
            required_missing_count = required_missing_count + %s + %s,
            postgres_server_execution_expected_count =
                postgres_server_execution_expected_count + 1,
            postgres_server_execution_observed_count =
                postgres_server_execution_observed_count + %s,
            postgres_server_execution_missing_count =
                postgres_server_execution_missing_count + %s + %s,
            postgres_lock_wait_expected_count =
                postgres_lock_wait_expected_count + 1,
            postgres_lock_wait_observed_count =
                postgres_lock_wait_observed_count + %s,
            postgres_lock_wait_missing_count =
                postgres_lock_wait_missing_count + %s + %s,
            postgres_wal_bytes_expected_count =
                postgres_wal_bytes_expected_count + 1,
            postgres_wal_bytes_observed_count =
                postgres_wal_bytes_observed_count + %s,
            postgres_wal_bytes_missing_count =
                postgres_wal_bytes_missing_count + %s + %s,
            postgres_shared_block_reads_expected_count =
                postgres_shared_block_reads_expected_count + 1,
            postgres_shared_block_reads_observed_count =
                postgres_shared_block_reads_observed_count + %s,
            postgres_shared_block_reads_missing_count =
                postgres_shared_block_reads_missing_count + %s + %s,
            pending_contribution_kind = NULL,
            pending_source_id = NULL,
            pending_contribution_key_digest = NULL,
            pending_anchor_revision = NULL,
            updated_revision = %s,
            updated_at = clock_timestamp()
        WHERE epoch_id = %s AND updated_revision = %s AND NOT terminalized
        """,
        (
            *(0 if value is None else value for value in values),
            required_observed,
            required_missing,
            pending_missing,
            server_observed,
            server_missing,
            pending_missing,
            lock_observed,
            lock_missing,
            pending_missing,
            wal_observed,
            wal_missing,
            pending_missing,
            blocks_observed,
            blocks_missing,
            pending_missing,
            resulting_revision,
            epoch_id,
            prior_revision,
        ),
    ).rowcount
    if changed != 1:
        raise EventConflictError(
            "typed-direct timing accumulator changed during attempt settlement"
        )


def _advance_work_accumulator(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    work_items: tuple[M5RuntimeWork, ...],
    prior_revision: int,
    resulting_revision: int,
) -> None:
    row = cursor.execute(
        sql.SQL(
            "SELECT {}, updated_revision, terminalized "
            "FROM groundloop_m5_runtime_work_accumulator "
            "WHERE epoch_id = %s FOR UPDATE"
        ).format(
            sql.SQL(", ").join(sql.Identifier(name) for name in _WORK_COUNTER_COLUMNS)
        ),
        (epoch_id,),
    ).fetchone()
    if row is None:
        raise ValidationError("typed-direct epoch lacks its work accumulator")
    if bool(row[-1]):
        raise InvalidEventError("terminalized work accumulator cannot be advanced")
    if int(row[-2]) != prior_revision:
        raise ValidationError(
            "typed-direct work accumulator revision differs from locked cutoff"
        )
    values = tuple(
        int(row[index]) + sum(item.counter_values()[index] for item in work_items)
        for index in range(len(_WORK_COUNTER_COLUMNS))
    )
    updated = M5RuntimeWork(
        **dict(zip(_WORK_COUNTER_COLUMNS, values, strict=True)),
        work_digest=digests.runtime_work_digest(values),
    )
    assignments = tuple(
        sql.SQL("{} = %s").format(sql.Identifier(name))
        for name in _WORK_COUNTER_COLUMNS
    )
    changed = cursor.execute(
        sql.SQL(
            "UPDATE groundloop_m5_runtime_work_accumulator SET {}, "
            "work_digest = %s, updated_revision = %s, updated_at = clock_timestamp() "
            "WHERE epoch_id = %s AND updated_revision = %s AND NOT terminalized"
        ).format(sql.SQL(", ").join(assignments)),
        (
            *updated.counter_values(),
            updated.work_digest,
            resulting_revision,
            epoch_id,
            prior_revision,
        ),
    ).rowcount
    if changed != 1:
        raise EventConflictError(
            "typed-direct work accumulator changed during point settlement"
        )


def _insert_missing_transition_timing(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    contribution_kind: str,
    source_id: str,
    contribution_key_digest: str,
    anchor_revision: int,
) -> None:
    observation = M5RuntimeTimingObservation.build(None)
    timing_digest = digests.transition_call_timing_digest(
        epoch_id=epoch_id,
        contribution_kind=contribution_kind,
        source_id=source_id,
        contribution_key_digest=contribution_key_digest,
        anchor_revision=anchor_revision,
        observation_digest=observation.observation_digest,
    )
    cursor.execute(
        """
        INSERT INTO groundloop_m5_transition_call_timing (
            epoch_id, contribution_kind, source_id,
            contribution_key_digest, anchor_revision,
            required_interval_observed,
            coordinator_non_db_non_neural_ns, neural_wall_ns,
            postgres_roundtrip_wall_ns, external_io_wall_ns,
            end_to_end_wall_ns, postgres_server_execution_ns,
            postgres_lock_wait_ns, postgres_wal_bytes,
            postgres_shared_block_reads, observation_digest,
            transition_timing_digest
        ) VALUES (
            %s, %s, %s, %s, %s, false,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, %s, %s
        )
        """,
        (
            epoch_id,
            contribution_kind,
            source_id,
            contribution_key_digest,
            anchor_revision,
            observation.observation_digest,
            timing_digest,
        ),
    )


def _install_pending_anchor(
    cursor: Cursor[Any], anchor: M5TransitionTimingAnchor
) -> None:
    expected_updated_revision = (
        anchor.anchor_revision - 1
        if anchor.contribution_kind is M5RuntimeWorkContributionKind.DIRECT_ACQUISITION
        else anchor.anchor_revision
    )
    row = cursor.execute(
        """
        SELECT pending_contribution_kind, pending_source_id,
               pending_contribution_key_digest, pending_anchor_revision,
               updated_revision, terminalized
        FROM groundloop_m5_runtime_timing_accumulator
        WHERE epoch_id = %s
        FOR UPDATE
        """,
        (anchor.epoch_id,),
    ).fetchone()
    if row is None:
        raise ValidationError("typed-direct epoch lacks its timing accumulator")
    if bool(row[5]):
        raise InvalidEventError("terminalized timing accumulator cannot be advanced")
    if int(row[4]) != expected_updated_revision:
        raise ValidationError(
            "typed-direct anchor input revision differs from timing accumulator"
        )
    had_pending = row[3] is not None
    if had_pending:
        assert row[0] is not None and row[1] is not None and row[2] is not None
        _insert_missing_transition_timing(
            cursor,
            epoch_id=anchor.epoch_id,
            contribution_kind=str(row[0]),
            source_id=str(row[1]),
            contribution_key_digest=_strip(row[2]),
            anchor_revision=int(row[3]),
        )
    missing_delta = int(had_pending)
    changed = cursor.execute(
        """
        UPDATE groundloop_m5_runtime_timing_accumulator
        SET required_expected_count = required_expected_count + 1,
            required_missing_count = required_missing_count + %s,
            postgres_server_execution_expected_count =
                postgres_server_execution_expected_count + 1,
            postgres_server_execution_missing_count =
                postgres_server_execution_missing_count + %s,
            postgres_lock_wait_expected_count =
                postgres_lock_wait_expected_count + 1,
            postgres_lock_wait_missing_count =
                postgres_lock_wait_missing_count + %s,
            postgres_wal_bytes_expected_count =
                postgres_wal_bytes_expected_count + 1,
            postgres_wal_bytes_missing_count =
                postgres_wal_bytes_missing_count + %s,
            postgres_shared_block_reads_expected_count =
                postgres_shared_block_reads_expected_count + 1,
            postgres_shared_block_reads_missing_count =
                postgres_shared_block_reads_missing_count + %s,
            pending_contribution_kind = %s,
            pending_source_id = %s,
            pending_contribution_key_digest = %s,
            pending_anchor_revision = %s,
            updated_revision = %s,
            updated_at = clock_timestamp()
        WHERE epoch_id = %s AND updated_revision = %s AND NOT terminalized
        """,
        (
            missing_delta,
            missing_delta,
            missing_delta,
            missing_delta,
            missing_delta,
            anchor.contribution_kind.value,
            anchor.source_id,
            anchor.contribution_key_digest,
            anchor.anchor_revision,
            anchor.anchor_revision,
            anchor.epoch_id,
            expected_updated_revision,
        ),
    ).rowcount
    if changed != 1:
        raise EventConflictError(
            "typed-direct timing accumulator changed during anchor selection"
        )


def _load_terminal_projection(
    cursor: Cursor[Any], epoch_id: int, job_id: str
) -> M5TypedDirectTerminalProjection:
    row = cursor.execute(
        """
        SELECT terminal_state, terminal_reason, m4_completion_digest,
               completed_revision, terminal_identity_hash
        FROM groundloop_m5_direct_terminal_projection
        WHERE epoch_id = %s AND job_id = %s
        """,
        (epoch_id, job_id),
    ).fetchone()
    if row is None:
        raise ValidationError("terminal direct job lacks its exact D24 projection")
    return M5TypedDirectTerminalProjection(
        terminal_state=JobState(str(row[0])),
        terminal_reason=None if row[1] is None else str(row[1]),
        m4_completion_digest=None if row[2] is None else _strip(row[2]),
        completed_revision=int(row[3]),
        terminal_identity_hash=_strip(row[4]),
    )


def _classify_first_late_return(
    cursor: Cursor[Any],
    *,
    locked: _LockedDirectHeader,
    point_job: PointJobRecord,
    envelope: M5TypedDirectLateReturnEnvelope,
    binding: _DirectAttemptBinding,
) -> M5DirectLateReturnDisposition:
    postterminal = locked.runtime_state in _TERMINAL_RUNTIME_STATES
    if binding.state == "expired":
        successor = cursor.execute(
            """
            SELECT attempt_id
            FROM groundloop_semantic_job_attempt
            WHERE job_id = %s AND attempt_ordinal = %s
            """,
            (envelope.job_id, binding.attempt.attempt_ordinal + 1),
        ).fetchone()
        if successor is None:
            raise ValidationError("expired direct return lacks its dense successor")
        _load_dispatch_record(cursor, envelope.epoch_id, str(successor[0]))
        return (
            M5DirectLateReturnDisposition.EXPIRED_POSTTERMINAL
            if postterminal
            else M5DirectLateReturnDisposition.EXPIRED_PRETERMINAL
        )
    latest = point_job.latest_attempt
    if (
        binding.state != "leased"
        or point_job.state is not JobState.CANCELLED
        or latest is None
        or latest.attempt.attempt_id != envelope.attempt_id
        or latest.state != "leased"
    ):
        raise EventConflictError("direct return is neither current nor auditable late")
    projection = _load_terminal_projection(cursor, envelope.epoch_id, envelope.job_id)
    job_row = cursor.execute(
        """
        SELECT job_state, completed_revision, completion_digest
        FROM groundloop_semantic_job
        WHERE epoch_id = %s AND job_id = %s
        """,
        (envelope.epoch_id, envelope.job_id),
    ).fetchone()
    if (
        job_row is None
        or str(job_row[0]) != JobState.CANCELLED.value
        or job_row[1] is None
        or int(job_row[1]) > locked.runtime_revision
        or projection.terminal_state is not JobState.CANCELLED
        or projection.terminal_reason is None
        or (
            projection.terminal_reason == "epoch_failed"
            and locked.runtime_state != "failed"
        )
        or projection.completed_revision != int(job_row[1])
        or projection.m4_completion_digest
        != (None if job_row[2] is None else _strip(job_row[2]))
    ):
        raise ValidationError("direct terminal-audit projection is inexact")
    return (
        M5DirectLateReturnDisposition.TERMINAL_AUDIT_POSTTERMINAL
        if postterminal
        else M5DirectLateReturnDisposition.TERMINAL_AUDIT_PRETERMINAL
    )


class PostgresM5DirectRecoveryStore:
    """Migration-016 checked operations for one measured M4 application port."""

    def __init__(self, ports: PostgresM4ApplicationPorts) -> None:
        self._ports = ports

    def acquire_direct_job(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        expected_revision: int,
        job: LogicalJobSpec,
        lease_token_hash: str,
    ) -> M5TypedDirectJobLease:
        """Return the total D24 direct acquisition under the caller transaction."""

        _assert_literal_recovery_bundle(cursor)
        locked = _lock_direct_header(cursor, epoch_id)
        if expected_revision > locked.runtime_revision:
            raise EventConflictError("direct acquisition names a future revision")
        point_job = self._ports.runtime_store.read_job_point(
            epoch_id, job.job_id, for_update=True, cursor=cursor
        )
        if point_job.spec != job:
            raise EventConflictError("requested direct job differs from persisted job")
        decision_time, lease_expires_at = _sample_database_deadline(cursor, epoch_id)
        latest = point_job.latest_attempt

        if point_job.state.terminal:
            projection = _load_terminal_projection(cursor, epoch_id, job.job_id)
            dispatch: tuple[str, datetime] | None = None
            if latest is not None:
                dispatch = _dispatch_record_for_attempt(
                    cursor, epoch_id, latest.attempt.attempt_id
                )
                if dispatch is None:
                    raise ValidationError(
                        "terminal direct attempt lacks its exact dispatch record"
                    )
            return M5TypedDirectJobLease(
                job_id=job.job_id,
                attempt_id=None if latest is None else latest.attempt.attempt_id,
                lease_token_hash=(
                    None if latest is None else latest.attempt.lease_token_hash
                ),
                lease_expires_at=None if latest is None else latest.lease_expires_at,
                dispatch_record_digest=None if dispatch is None else dispatch[0],
                resulting_revision=locked.runtime_revision,
                disposition=M5AcquisitionDisposition.TERMINAL,
                should_execute=False,
                exact_replay=True,
                already_completed=point_job.state
                in {JobState.COMPLETED_ACTIVE, JobState.COMPLETED_INACTIVE},
                terminal_projection=projection,
            )

        if point_job.state is JobState.RUNNING:
            if latest is None or latest.state != "leased":
                raise ValidationError(
                    "running direct job lacks a leased latest attempt"
                )
            dispatch = _dispatch_record_for_attempt(
                cursor, epoch_id, latest.attempt.attempt_id
            )
            if dispatch is None or dispatch[1] != latest.lease_expires_at:
                raise ValidationError("active direct attempt lacks exact dispatch")
            if decision_time < latest.lease_expires_at:
                if lease_token_hash != latest.attempt.lease_token_hash:
                    raise EventConflictError(
                        "live direct lease token differs from active attempt"
                    )
                return M5TypedDirectJobLease(
                    job_id=job.job_id,
                    attempt_id=latest.attempt.attempt_id,
                    lease_token_hash=latest.attempt.lease_token_hash,
                    lease_expires_at=latest.lease_expires_at,
                    dispatch_record_digest=dispatch[0],
                    resulting_revision=locked.runtime_revision,
                    disposition=M5AcquisitionDisposition.LIVE_LEASE,
                    should_execute=False,
                    exact_replay=True,
                    already_completed=False,
                    terminal_projection=None,
                )
            ordinal = latest.attempt.attempt_ordinal + 1
            disposition = M5AcquisitionDisposition.DISPATCH_TAKEOVER
        elif point_job.state in {JobState.DECLARED, JobState.RETRYABLE_FAILED}:
            ordinal = 1 if latest is None else latest.attempt.attempt_ordinal + 1
            disposition = M5AcquisitionDisposition.DISPATCH_NEW
        else:
            raise InvalidEventError("direct job is not executable")

        if expected_revision != locked.runtime_revision:
            raise EventConflictError("stale revision cannot dispatch direct work")
        expected_token = stable_m4_digest("m4-lease-token-v1", job.job_id, str(ordinal))
        if lease_token_hash != expected_token:
            raise EventConflictError("direct acquisition token is not deterministic")
        attempt = JobAttempt(
            attempt_id=stable_m4_digest("m4-job-attempt-v1", job.job_id, str(ordinal)),
            job_id=job.job_id,
            execution_spec_hash=job.execution_spec_hash,
            attempt_ordinal=ordinal,
            lease_token_hash=expected_token,
        )
        cursor.execute(
            "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
            (epoch_id, expected_revision),
        )
        if disposition is M5AcquisitionDisposition.DISPATCH_NEW:
            public_lease = self._ports._acquire_direct_job_local(
                cursor,
                epoch_id,
                expected_revision,
                job,
                expected_token,
                lease_expires_at=lease_expires_at,
            )
        else:
            assert latest is not None
            public_lease = self._ports._takeover_direct_job_local(
                cursor,
                epoch_id,
                expected_revision,
                job,
                latest.attempt.attempt_id,
                attempt,
                decision_time=decision_time,
                lease_expires_at=lease_expires_at,
            )
        resulting_revision = expected_revision + 1
        if public_lease.expected_revision != resulting_revision:
            raise ValidationError(
                "M4 direct acquisition advanced an unexpected revision"
            )
        base = cursor.execute(
            "SELECT semantic_status FROM groundloop_epoch WHERE epoch_id = %s",
            (epoch_id,),
        ).fetchone()
        assert base is not None
        runtime_state = (
            "semantic_complete" if str(base[0]) == "complete" else "semantic_pending"
        )
        changed = cursor.execute(
            """
            UPDATE groundloop_m5_runtime_epoch
            SET runtime_state = %s, revision = %s
            WHERE epoch_id = %s AND revision = %s
            """,
            (runtime_state, resulting_revision, epoch_id, expected_revision),
        ).rowcount
        if changed != 1:
            raise EventConflictError("typed runtime changed before direct dispatch")
        dispatch_record = M5DispatchRecord.build(
            epoch_id=epoch_id,
            subgraph=M5RuntimeSubgraph.DIRECT,
            attempt_id=attempt.attempt_id,
            logical_job_id=job.job_id,
            attempt_ordinal=ordinal,
            job_kind=job.kind.value,
            fallback_required=False,
            dispatched_revision=resulting_revision,
            lease_expires_at=lease_expires_at,
        )
        _insert_dispatch_record(cursor, dispatch_record)
        zero_work = M5RuntimeWork()
        contribution_key = _insert_work_contribution(
            cursor,
            epoch_id=epoch_id,
            contribution_kind=M5RuntimeWorkContributionKind.DIRECT_ACQUISITION,
            source_id=dispatch_record.record_digest,
            source_identity_hash=dispatch_record.record_digest,
            work=zero_work,
            applied_revision=resulting_revision,
        )
        _advance_work_accumulator(
            cursor,
            epoch_id=epoch_id,
            work_items=(zero_work,),
            prior_revision=expected_revision,
            resulting_revision=resulting_revision,
        )
        anchor = M5TransitionTimingAnchor(
            epoch_id=epoch_id,
            contribution_kind=M5RuntimeWorkContributionKind.DIRECT_ACQUISITION,
            source_id=dispatch_record.record_digest,
            contribution_key_digest=contribution_key,
            anchor_revision=resulting_revision,
            terminal_transition=False,
        )
        _install_pending_anchor(cursor, anchor)
        return M5TypedDirectJobLease(
            job_id=job.job_id,
            attempt_id=attempt.attempt_id,
            lease_token_hash=attempt.lease_token_hash,
            lease_expires_at=lease_expires_at,
            dispatch_record_digest=dispatch_record.record_digest,
            resulting_revision=resulting_revision,
            disposition=disposition,
            should_execute=True,
            exact_replay=False,
            already_completed=False,
            terminal_projection=None,
        )

    def settle_direct_expansion_cursor(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        expected_revision: int,
        lease: M5TypedDirectJobLease,
        envelope: M5TypedDirectLateReturnEnvelope,
        children: tuple[LogicalJobSpec, ...],
        execution_disposition: M5ExecutionEvidenceDisposition,
        attempt_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
    ) -> _DirectReturnSettlement:
        """Choose normal versus late discovery under one locked cutoff."""

        if not isinstance(lease, M5TypedDirectJobLease):
            raise ValidationError("direct expansion requires a typed lease")
        if not isinstance(envelope, M5TypedDirectLateReturnEnvelope):
            raise ValidationError("direct expansion requires a byte-total envelope")
        if not isinstance(children, tuple) or any(
            not isinstance(child, LogicalJobSpec) for child in children
        ):
            raise ValidationError("direct expansion children must be a tuple of jobs")
        if not isinstance(attempt_work, M5RuntimeWork):
            raise ValidationError("direct expansion requires typed attempt work")
        if attempt_timing is not None and not isinstance(
            attempt_timing, M5RuntimeTiming
        ):
            raise ValidationError("direct expansion timing must be typed or absent")
        _assert_literal_recovery_bundle(cursor)
        locked = _lock_direct_header(cursor, epoch_id)
        if expected_revision > locked.runtime_revision:
            raise EventConflictError("direct expansion names a future revision")
        attempt_id, token, _deadline = _require_direct_lease_binding(lease)
        point_job = self._ports.runtime_store.read_job_point(
            epoch_id, lease.job_id, for_update=True, cursor=cursor
        )
        if point_job.spec != envelope.job:
            raise EventConflictError("direct expansion job differs from its envelope")
        binding = _load_named_attempt(
            cursor, epoch_id=epoch_id, job_id=lease.job_id, attempt_id=attempt_id
        )
        _validate_lease_against_attempt(lease, binding)
        dispatch = _load_dispatch_record(cursor, epoch_id, attempt_id)
        _validate_discovery_return_input(
            epoch_id=epoch_id,
            lease=lease,
            envelope=envelope,
            binding=binding,
            dispatch=dispatch,
            children=children,
            execution_disposition=execution_disposition,
        )
        observation, timing_digest = _execution_timing_observation(
            epoch_id=epoch_id, attempt_id=attempt_id, timing=attempt_timing
        )
        stored = _load_stored_direct_return(
            cursor, epoch_id=epoch_id, attempt_id=attempt_id
        )
        result_hash = (
            envelope.result_artifact_hash
            if stored is None or stored.branch == "normal"
            else envelope.envelope_digest
        )
        evidence = M5AttemptExecutionEvidence.build(
            epoch_id=epoch_id,
            subgraph=M5RuntimeSubgraph.DIRECT,
            attempt_id=attempt_id,
            disposition=execution_disposition,
            result_or_error_hash=result_hash,
            attempt_work=attempt_work,
            attempt_timing_digest=timing_digest,
        )
        if stored is not None:
            return _replay_direct_discovery_return(
                cursor,
                ports=self._ports,
                locked=locked,
                stored=stored,
                envelope=envelope,
                binding=binding,
                children=children,
                evidence=evidence,
                observation=observation,
            )
        latest = point_job.latest_attempt
        current = (
            locked.runtime_state not in _TERMINAL_RUNTIME_STATES
            and point_job.state is JobState.RUNNING
            and binding.state == "leased"
            and latest is not None
            and latest.state == "leased"
            and latest.attempt.attempt_id == attempt_id
            and latest.attempt.lease_token_hash == token
        )
        if current:
            return self._stage_normal_direct_expansion(
                cursor,
                locked=locked,
                expected_revision=expected_revision,
                lease=lease,
                envelope=envelope,
                children=children,
                binding=binding,
                dispatch=dispatch,
                evidence=evidence,
                observation=observation,
            )
        late_evidence = M5AttemptExecutionEvidence.build(
            epoch_id=epoch_id,
            subgraph=M5RuntimeSubgraph.DIRECT,
            attempt_id=attempt_id,
            disposition=execution_disposition,
            result_or_error_hash=envelope.envelope_digest,
            attempt_work=attempt_work,
            attempt_timing_digest=timing_digest,
        )
        return self._stage_late_direct_return(
            cursor,
            locked=locked,
            point_job=point_job,
            envelope=envelope,
            binding=binding,
            dispatch=dispatch,
            evidence=late_evidence,
            observation=observation,
        )

    def settle_direct_verifier_cursor(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        expected_revision: int,
        lease: M5TypedDirectJobLease,
        envelope: M5TypedDirectLateReturnEnvelope,
        execution_disposition: M5ExecutionEvidenceDisposition,
        attempt_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
    ) -> _DirectVerifierSettlement:
        """Choose normal versus late verifier settlement under one lock."""

        if not isinstance(lease, M5TypedDirectJobLease):
            raise ValidationError("direct verifier settlement requires a typed lease")
        if not isinstance(envelope, M5TypedDirectLateReturnEnvelope):
            raise ValidationError("direct verifier settlement requires an envelope")
        if not isinstance(attempt_work, M5RuntimeWork):
            raise ValidationError("direct verifier settlement requires typed work")
        if attempt_timing is not None and not isinstance(
            attempt_timing, M5RuntimeTiming
        ):
            raise ValidationError("direct verifier timing must be typed or absent")
        _assert_literal_recovery_bundle(cursor)
        locked = _lock_direct_header(cursor, epoch_id)
        if expected_revision > locked.runtime_revision:
            raise EventConflictError("direct verifier return names a future revision")
        attempt_id, token, _deadline = _require_direct_lease_binding(lease)
        point_job = self._ports.runtime_store.read_job_point(
            epoch_id, lease.job_id, for_update=True, cursor=cursor
        )
        if point_job.spec != envelope.job:
            raise EventConflictError("direct verifier job differs from its envelope")
        binding = _load_named_attempt(
            cursor, epoch_id=epoch_id, job_id=lease.job_id, attempt_id=attempt_id
        )
        _validate_lease_against_attempt(lease, binding)
        dispatch = _load_dispatch_record(cursor, epoch_id, attempt_id)
        _validate_verifier_return_input(
            epoch_id=epoch_id,
            lease=lease,
            envelope=envelope,
            binding=binding,
            dispatch=dispatch,
            execution_disposition=execution_disposition,
        )
        observation, timing_digest = _execution_timing_observation(
            epoch_id=epoch_id, attempt_id=attempt_id, timing=attempt_timing
        )
        stored = _load_stored_direct_return(
            cursor, epoch_id=epoch_id, attempt_id=attempt_id
        )
        result_hash = (
            envelope.result_artifact_hash
            if stored is None or stored.branch == "normal"
            else envelope.envelope_digest
        )
        evidence = M5AttemptExecutionEvidence.build(
            epoch_id=epoch_id,
            subgraph=M5RuntimeSubgraph.DIRECT,
            attempt_id=attempt_id,
            disposition=execution_disposition,
            result_or_error_hash=result_hash,
            attempt_work=attempt_work,
            attempt_timing_digest=timing_digest,
        )
        if stored is not None:
            return _DirectVerifierSettlement(
                settlement=_replay_direct_verifier_return(
                    cursor,
                    ports=self._ports,
                    locked=locked,
                    stored=stored,
                    envelope=envelope,
                    binding=binding,
                    evidence=evidence,
                    observation=observation,
                ),
                repository=None,
                engine=None,
            )
        latest = point_job.latest_attempt
        current = (
            locked.runtime_state not in _TERMINAL_RUNTIME_STATES
            and point_job.state is JobState.RUNNING
            and binding.state == "leased"
            and latest is not None
            and latest.state == "leased"
            and latest.attempt.attempt_id == attempt_id
            and latest.attempt.lease_token_hash == token
        )
        if current:
            return self._stage_normal_direct_verifier(
                cursor,
                locked=locked,
                expected_revision=expected_revision,
                lease=lease,
                envelope=envelope,
                binding=binding,
                dispatch=dispatch,
                evidence=evidence,
                observation=observation,
            )
        late_evidence = M5AttemptExecutionEvidence.build(
            epoch_id=epoch_id,
            subgraph=M5RuntimeSubgraph.DIRECT,
            attempt_id=attempt_id,
            disposition=execution_disposition,
            result_or_error_hash=envelope.envelope_digest,
            attempt_work=attempt_work,
            attempt_timing_digest=timing_digest,
        )
        return _DirectVerifierSettlement(
            settlement=self._stage_late_direct_return(
                cursor,
                locked=locked,
                point_job=point_job,
                envelope=envelope,
                binding=binding,
                dispatch=dispatch,
                evidence=late_evidence,
                observation=observation,
            ),
            repository=None,
            engine=None,
        )

    def _stage_normal_direct_expansion(
        self,
        cursor: Cursor[Any],
        *,
        locked: _LockedDirectHeader,
        expected_revision: int,
        lease: M5TypedDirectJobLease,
        envelope: M5TypedDirectLateReturnEnvelope,
        children: tuple[LogicalJobSpec, ...],
        binding: _DirectAttemptBinding,
        dispatch: M5DispatchRecord,
        evidence: M5AttemptExecutionEvidence,
        observation: M5RuntimeTimingObservation,
    ) -> _DirectReturnSettlement:
        if expected_revision != locked.runtime_revision:
            raise EventConflictError("stale revision cannot apply direct expansion")
        resulting_revision = expected_revision + 1
        _assert_discovery_scope_binding(
            cursor,
            envelope=envelope,
            resulting_revision=resulting_revision,
            replay=False,
        )
        cursor.execute(
            "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
            (envelope.epoch_id, expected_revision),
        )
        assert envelope.discovery is not None
        self._ports._stage_direct_expansion_local(
            cursor,
            envelope.epoch_id,
            expected_revision,
            _public_m4_lease(lease),
            envelope.discovery,
            envelope.completion,
            children,
        )
        _advance_runtime_header(
            cursor,
            epoch_id=envelope.epoch_id,
            expected_revision=expected_revision,
            resulting_revision=resulting_revision,
        )
        terminal_reason = (
            None
            if envelope.completion.terminal_state is JobState.COMPLETED_ACTIVE
            else "inactive_at_completion"
        )
        _insert_terminal_projection(
            cursor,
            epoch_id=envelope.epoch_id,
            job_id=envelope.job_id,
            terminal_state=envelope.completion.terminal_state,
            terminal_reason=terminal_reason,
            completion_digest=envelope.completion.completion_digest,
            completed_revision=resulting_revision,
        )
        _assert_discovery_scope_binding(
            cursor,
            envelope=envelope,
            resulting_revision=resulting_revision,
            replay=True,
        )
        _assert_discovery_artifact_replay(cursor, envelope=envelope)
        stored_children = self._ports.runtime_store.read_children_point(
            envelope.epoch_id, envelope.job_id, cursor=cursor
        )
        if stored_children != children or binding.state != "leased":
            raise EventConflictError("normal direct expansion closure changed")
        _insert_execution_evidence(cursor, evidence=evidence, dispatch=dispatch)
        _insert_attempt_timing(cursor, evidence=evidence, observation=observation)
        attempt_key = _insert_work_contribution(
            cursor,
            epoch_id=envelope.epoch_id,
            contribution_kind=M5RuntimeWorkContributionKind.DIRECT_ATTEMPT_EXECUTION,
            source_id=envelope.attempt_id,
            source_identity_hash=evidence.evidence_digest,
            work=evidence.attempt_work,
            applied_revision=resulting_revision,
        )
        transition_id = envelope.completion.completion_digest
        transition_identity = _direct_transition_identity(
            cursor,
            epoch_id=envelope.epoch_id,
            transition_id=transition_id,
            revision=resulting_revision,
        )
        transition_work = _direct_transition_work(
            cursor,
            epoch_id=envelope.epoch_id,
            resulting_revision=resulting_revision,
            observation_completion=None,
        )
        transition_key = _insert_work_contribution(
            cursor,
            epoch_id=envelope.epoch_id,
            contribution_kind=M5RuntimeWorkContributionKind.DIRECT_TRANSITION,
            source_id=transition_id,
            source_identity_hash=transition_identity,
            work=transition_work,
            applied_revision=resulting_revision,
        )
        _advance_work_accumulator(
            cursor,
            epoch_id=envelope.epoch_id,
            work_items=(evidence.attempt_work, transition_work),
            prior_revision=expected_revision,
            resulting_revision=resulting_revision,
        )
        _advance_timing_accumulator_for_attempt(
            cursor,
            epoch_id=envelope.epoch_id,
            observation=observation,
            prior_revision=expected_revision,
            resulting_revision=resulting_revision,
        )
        normal = M5DirectCursorContributionReceipt(
            epoch_id=envelope.epoch_id,
            job_id=envelope.job_id,
            attempt_id=envelope.attempt_id,
            execution_evidence_digest=evidence.evidence_digest,
            attempt_execution_contribution_key_digest=attempt_key,
            direct_transition_source_id=transition_id,
            direct_transition_source_identity_hash=transition_identity,
            direct_transition_contribution_key_digest=transition_key,
            observation_completion=None,
        )
        return _DirectReturnSettlement(
            normal=normal,
            late=None,
            resulting_revision=resulting_revision,
            exact_replay=False,
            current_terminal_logical_result_hash=None,
        )

    def _stage_normal_direct_verifier(
        self,
        cursor: Cursor[Any],
        *,
        locked: _LockedDirectHeader,
        expected_revision: int,
        lease: M5TypedDirectJobLease,
        envelope: M5TypedDirectLateReturnEnvelope,
        binding: _DirectAttemptBinding,
        dispatch: M5DispatchRecord,
        evidence: M5AttemptExecutionEvidence,
        observation: M5RuntimeTimingObservation,
    ) -> _DirectVerifierSettlement:
        if expected_revision != locked.runtime_revision:
            raise EventConflictError("stale revision cannot apply direct verifier")
        if binding.state != "leased":
            raise EventConflictError("normal direct verifier attempt is not leased")
        semantic_observation = envelope.observation
        make_effective = envelope.requested_make_effective
        assert semantic_observation is not None and make_effective is not None
        resulting_revision = expected_revision + 1
        cursor.execute(
            "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
            (envelope.epoch_id, expected_revision),
        )
        observation_receipt, repository, engine = (
            self._ports._stage_direct_verifier_completion_local(
                cursor,
                envelope.epoch_id,
                expected_revision,
                _public_m4_lease(lease),
                envelope.job,
                envelope.completion,
                semantic_observation,
                make_effective=make_effective,
            )
        )
        _advance_runtime_header(
            cursor,
            epoch_id=envelope.epoch_id,
            expected_revision=expected_revision,
            resulting_revision=resulting_revision,
        )
        terminal_reason = (
            None
            if envelope.completion.terminal_state is JobState.COMPLETED_ACTIVE
            else "inactive_at_completion"
        )
        _insert_terminal_projection(
            cursor,
            epoch_id=envelope.epoch_id,
            job_id=envelope.job_id,
            terminal_state=envelope.completion.terminal_state,
            terminal_reason=terminal_reason,
            completion_digest=envelope.completion.completion_digest,
            completed_revision=resulting_revision,
        )
        _assert_verifier_artifact_replay(cursor, envelope=envelope)
        _insert_execution_evidence(cursor, evidence=evidence, dispatch=dispatch)
        _insert_attempt_timing(cursor, evidence=evidence, observation=observation)
        attempt_key = _insert_work_contribution(
            cursor,
            epoch_id=envelope.epoch_id,
            contribution_kind=M5RuntimeWorkContributionKind.DIRECT_ATTEMPT_EXECUTION,
            source_id=envelope.attempt_id,
            source_identity_hash=evidence.evidence_digest,
            work=evidence.attempt_work,
            applied_revision=resulting_revision,
        )
        transition_id = envelope.completion.completion_digest
        transition_identity = _direct_transition_identity(
            cursor,
            epoch_id=envelope.epoch_id,
            transition_id=transition_id,
            revision=resulting_revision,
        )
        transition_work = _direct_transition_work(
            cursor,
            epoch_id=envelope.epoch_id,
            resulting_revision=resulting_revision,
            observation_completion=observation_receipt,
        )
        transition_key = _insert_work_contribution(
            cursor,
            epoch_id=envelope.epoch_id,
            contribution_kind=M5RuntimeWorkContributionKind.DIRECT_TRANSITION,
            source_id=transition_id,
            source_identity_hash=transition_identity,
            work=transition_work,
            applied_revision=resulting_revision,
        )
        _advance_work_accumulator(
            cursor,
            epoch_id=envelope.epoch_id,
            work_items=(evidence.attempt_work, transition_work),
            prior_revision=expected_revision,
            resulting_revision=resulting_revision,
        )
        _advance_timing_accumulator_for_attempt(
            cursor,
            epoch_id=envelope.epoch_id,
            observation=observation,
            prior_revision=expected_revision,
            resulting_revision=resulting_revision,
        )
        completed_binding = _load_named_attempt(
            cursor,
            epoch_id=envelope.epoch_id,
            job_id=envelope.job_id,
            attempt_id=envelope.attempt_id,
        )
        replay_values = _assert_normal_verifier_replay(
            cursor,
            ports=self._ports,
            envelope=envelope,
            binding=completed_binding,
        )
        if replay_values != (
            transition_id,
            transition_identity,
            transition_key,
            observation_receipt,
        ):
            raise EventConflictError("normal direct verifier receipt closure differs")
        normal = M5DirectCursorContributionReceipt(
            epoch_id=envelope.epoch_id,
            job_id=envelope.job_id,
            attempt_id=envelope.attempt_id,
            execution_evidence_digest=evidence.evidence_digest,
            attempt_execution_contribution_key_digest=attempt_key,
            direct_transition_source_id=transition_id,
            direct_transition_source_identity_hash=transition_identity,
            direct_transition_contribution_key_digest=transition_key,
            observation_completion=observation_receipt,
        )
        return _DirectVerifierSettlement(
            settlement=_DirectReturnSettlement(
                normal=normal,
                late=None,
                resulting_revision=resulting_revision,
                exact_replay=False,
                current_terminal_logical_result_hash=None,
            ),
            repository=repository,
            engine=engine,
        )

    def _stage_late_direct_return(
        self,
        cursor: Cursor[Any],
        *,
        locked: _LockedDirectHeader,
        point_job: PointJobRecord,
        envelope: M5TypedDirectLateReturnEnvelope,
        binding: _DirectAttemptBinding,
        dispatch: M5DispatchRecord,
        evidence: M5AttemptExecutionEvidence,
        observation: M5RuntimeTimingObservation,
    ) -> _DirectReturnSettlement:
        disposition = _classify_first_late_return(
            cursor,
            locked=locked,
            point_job=point_job,
            envelope=envelope,
            binding=binding,
        )
        current_hash = _terminal_hash_for_locked_cutoff(
            cursor, epoch_id=envelope.epoch_id, locked=locked
        )
        postterminal = disposition in {
            M5DirectLateReturnDisposition.EXPIRED_POSTTERMINAL,
            M5DirectLateReturnDisposition.TERMINAL_AUDIT_POSTTERMINAL,
        }
        if not postterminal:
            cursor.execute(
                "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
                (envelope.epoch_id, locked.runtime_revision),
            )
        _insert_execution_evidence(cursor, evidence=evidence, dispatch=dispatch)
        _insert_late_envelope(cursor, envelope)
        expired_return: M5ExpiredAttemptReturn | None = None
        if disposition in {
            M5DirectLateReturnDisposition.EXPIRED_PRETERMINAL,
            M5DirectLateReturnDisposition.EXPIRED_POSTTERMINAL,
        }:
            expired_return = _insert_expired_return(
                cursor,
                envelope=envelope,
                binding=binding,
                evidence=evidence,
                activity_revision=locked.runtime_revision,
                received_after_terminal=postterminal,
            )
        if postterminal:
            if current_hash is None:
                raise ValidationError("postterminal direct return lacks event result")
            _insert_postterminal_timing(
                cursor, evidence=evidence, observation=observation
            )
            return_digest = (
                envelope.envelope_digest
                if expired_return is None
                else expired_return.expired_return_digest
            )
            _insert_postterminal_audit(
                cursor,
                evidence=evidence,
                return_kind=(
                    "terminal_audit_only"
                    if expired_return is None
                    else "expired_return"
                ),
                return_artifact_digest=return_digest,
                terminal_logical_result_hash=current_hash,
            )
            attempt_key = None
            late_key = None
            postterminal_hash = current_hash
        else:
            _insert_attempt_timing(cursor, evidence=evidence, observation=observation)
            attempt_key = _insert_work_contribution(
                cursor,
                epoch_id=envelope.epoch_id,
                contribution_kind=(
                    M5RuntimeWorkContributionKind.DIRECT_ATTEMPT_EXECUTION
                ),
                source_id=envelope.attempt_id,
                source_identity_hash=evidence.evidence_digest,
                work=evidence.attempt_work,
                applied_revision=locked.runtime_revision,
            )
            late_work = M5RuntimeWork(requirement_late_attempt_artifact_count=1)
            late_identity = (
                envelope.envelope_digest
                if expired_return is None
                else expired_return.expired_return_digest
            )
            late_key = _insert_work_contribution(
                cursor,
                epoch_id=envelope.epoch_id,
                contribution_kind=(
                    M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN
                ),
                source_id=envelope.attempt_id,
                source_identity_hash=late_identity,
                work=late_work,
                applied_revision=locked.runtime_revision,
            )
            _advance_work_accumulator(
                cursor,
                epoch_id=envelope.epoch_id,
                work_items=(evidence.attempt_work, late_work),
                prior_revision=locked.runtime_revision,
                resulting_revision=locked.runtime_revision,
            )
            _advance_timing_accumulator_for_attempt(
                cursor,
                epoch_id=envelope.epoch_id,
                observation=observation,
                prior_revision=locked.runtime_revision,
                resulting_revision=locked.runtime_revision,
            )
            postterminal_hash = None
        late = M5DirectLateCursorContributionReceipt(
            disposition=disposition,
            epoch_id=envelope.epoch_id,
            job_id=envelope.job_id,
            attempt_id=envelope.attempt_id,
            envelope_digest=envelope.envelope_digest,
            execution_evidence_digest=evidence.evidence_digest,
            expired_return_digest=(
                None if expired_return is None else expired_return.expired_return_digest
            ),
            attempt_execution_contribution_key_digest=attempt_key,
            preterminal_late_contribution_key_digest=late_key,
            postterminal_logical_result_hash=postterminal_hash,
        )
        return _DirectReturnSettlement(
            normal=None,
            late=late,
            resulting_revision=locked.runtime_revision,
            exact_replay=False,
            current_terminal_logical_result_hash=current_hash,
        )

    def mark_direct_retryable_failure(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        expected_revision: int,
        lease: M5TypedDirectJobLease,
        error_hash: str,
        attempt_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
    ) -> M5DirectCursorContributionReceipt:
        return self._settle_direct_failure(
            cursor,
            epoch_id=epoch_id,
            expected_revision=expected_revision,
            lease=lease,
            terminal_reason=None,
            error_hash=error_hash,
            attempt_work=attempt_work,
            attempt_timing=attempt_timing,
        )

    def mark_direct_terminal_failure(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        expected_revision: int,
        lease: M5TypedDirectJobLease,
        terminal_reason: str,
        error_hash: str,
        attempt_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
    ) -> M5DirectCursorContributionReceipt:
        if not terminal_reason.strip():
            raise ValidationError("direct terminal reason must be non-empty")
        return self._settle_direct_failure(
            cursor,
            epoch_id=epoch_id,
            expected_revision=expected_revision,
            lease=lease,
            terminal_reason=terminal_reason,
            error_hash=error_hash,
            attempt_work=attempt_work,
            attempt_timing=attempt_timing,
        )

    def _settle_direct_failure(
        self,
        cursor: Cursor[Any],
        *,
        epoch_id: int,
        expected_revision: int,
        lease: M5TypedDirectJobLease,
        terminal_reason: str | None,
        error_hash: str,
        attempt_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
    ) -> M5DirectCursorContributionReceipt:
        _assert_literal_recovery_bundle(cursor)
        locked = _lock_direct_header(cursor, epoch_id)
        if expected_revision > locked.runtime_revision:
            raise EventConflictError("direct failure names a future revision")
        attempt_id, token, _deadline = _require_direct_lease_binding(lease)
        point_job = self._ports.runtime_store.read_job_point(
            epoch_id, lease.job_id, for_update=True, cursor=cursor
        )
        binding = _load_named_attempt(
            cursor, epoch_id=epoch_id, job_id=lease.job_id, attempt_id=attempt_id
        )
        _validate_lease_against_attempt(lease, binding)
        dispatch = _load_dispatch_record(cursor, epoch_id, attempt_id)
        if dispatch.record_digest != lease.dispatch_record_digest:
            raise EventConflictError("direct failure dispatch digest differs")
        observation, timing_digest = _execution_timing_observation(
            epoch_id=epoch_id, attempt_id=attempt_id, timing=attempt_timing
        )
        disposition = (
            M5ExecutionEvidenceDisposition.TERMINAL_FAILURE
            if terminal_reason is not None
            else M5ExecutionEvidenceDisposition.RETRYABLE_FAILURE
        )
        evidence = M5AttemptExecutionEvidence.build(
            epoch_id=epoch_id,
            subgraph=M5RuntimeSubgraph.DIRECT,
            attempt_id=attempt_id,
            disposition=disposition,
            result_or_error_hash=error_hash,
            attempt_work=attempt_work,
            attempt_timing_digest=timing_digest,
        )
        existing = _load_execution_evidence(
            cursor, epoch_id=epoch_id, attempt_id=attempt_id
        )
        if existing is not None:
            contribution_key = _assert_attempt_accounting_replay(
                cursor, evidence=evidence, observation=observation
            )
            if terminal_reason is not None:
                projection = _load_terminal_projection(cursor, epoch_id, lease.job_id)
                if (
                    projection.terminal_state is not JobState.TERMINAL_FAILED
                    or projection.terminal_reason != terminal_reason
                ):
                    raise EventConflictError("direct terminal-failure replay differs")
            return M5DirectCursorContributionReceipt(
                epoch_id=epoch_id,
                job_id=lease.job_id,
                attempt_id=attempt_id,
                execution_evidence_digest=evidence.evidence_digest,
                attempt_execution_contribution_key_digest=contribution_key,
                direct_transition_source_id=None,
                direct_transition_source_identity_hash=None,
                direct_transition_contribution_key_digest=None,
                observation_completion=None,
            )

        if expected_revision != locked.runtime_revision:
            raise EventConflictError("stale revision cannot settle direct failure")
        latest = point_job.latest_attempt
        if (
            point_job.state is not JobState.RUNNING
            or latest is None
            or latest.attempt.attempt_id != attempt_id
            or latest.attempt.lease_token_hash != token
            or latest.state != "leased"
        ):
            raise EventConflictError("direct failure lost its current leased attempt")
        cursor.execute(
            "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
            (epoch_id, expected_revision),
        )
        public_lease = _public_m4_lease(lease)
        if terminal_reason is None:
            self._ports._mark_direct_retryable_failure_local(
                cursor, epoch_id, expected_revision, public_lease
            )
        else:
            self._ports._mark_direct_terminal_failure_local(
                cursor, epoch_id, expected_revision, public_lease
            )
        resulting_revision = expected_revision + 1
        _advance_runtime_header(
            cursor,
            epoch_id=epoch_id,
            expected_revision=expected_revision,
            resulting_revision=resulting_revision,
        )
        if terminal_reason is not None:
            _insert_terminal_projection(
                cursor,
                epoch_id=epoch_id,
                job_id=lease.job_id,
                terminal_state=JobState.TERMINAL_FAILED,
                terminal_reason=terminal_reason,
                completion_digest=None,
                completed_revision=resulting_revision,
            )
        _insert_execution_evidence(cursor, evidence=evidence, dispatch=dispatch)
        _insert_attempt_timing(cursor, evidence=evidence, observation=observation)
        contribution_key = _insert_work_contribution(
            cursor,
            epoch_id=epoch_id,
            contribution_kind=(M5RuntimeWorkContributionKind.DIRECT_ATTEMPT_EXECUTION),
            source_id=attempt_id,
            source_identity_hash=evidence.evidence_digest,
            work=attempt_work,
            applied_revision=resulting_revision,
        )
        _advance_work_accumulator(
            cursor,
            epoch_id=epoch_id,
            work_items=(attempt_work,),
            prior_revision=expected_revision,
            resulting_revision=resulting_revision,
        )
        _advance_timing_accumulator_for_attempt(
            cursor,
            epoch_id=epoch_id,
            observation=observation,
            prior_revision=expected_revision,
            resulting_revision=resulting_revision,
        )
        return M5DirectCursorContributionReceipt(
            epoch_id=epoch_id,
            job_id=lease.job_id,
            attempt_id=attempt_id,
            execution_evidence_digest=evidence.evidence_digest,
            attempt_execution_contribution_key_digest=contribution_key,
            direct_transition_source_id=None,
            direct_transition_source_identity_hash=None,
            direct_transition_contribution_key_digest=None,
            observation_completion=None,
        )

    def install_outer_transition_anchor(
        self, cursor: Cursor[Any], anchor: M5TransitionTimingAnchor
    ) -> None:
        """Install the one anchor selected by the transaction's outer owner."""

        _assert_literal_recovery_bundle(cursor)
        _install_pending_anchor(cursor, anchor)


__all__ = ["PostgresM5DirectRecoveryStore"]
