"""D30 falsifiers 6 and 9--11: complete held-owner authority."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from groundloop.domain import ModelStamp, SemanticObservation, SubjectKind
from groundloop.errors import EventConflictError
from groundloop.m4.application import OpenEventReceipt, PublicationReceipt
from groundloop.m4.contracts import (
    JobAttempt,
    JobCompletion,
    JobKind,
    JobState,
    LogicalJobSpec,
    PairKey,
    stable_m4_digest,
)
from groundloop.m5.runtime import postgres_withdrawal
from groundloop.m5.runtime.contracts import (
    M5AttemptExecutionEvidence,
    M5DispatchRecord,
    M5EventRunResult,
    M5ExecutionEvidenceDisposition,
    M5ExpiredAttemptReturn,
    M5ReplayedOutcome,
    M5RunState,
    M5RuntimeSubgraph,
    M5RuntimeTiming,
    M5RuntimeTimingCoverage,
    M5RuntimeTimingObservation,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5TypedDirectLateReturnEnvelope,
    M5TypedDirectTerminalProjection,
    M5TypedDirectVerificationExecution,
)
from groundloop.m5.runtime.postgres_direct_recovery import _envelope_json_bindings
from tests.m5.postgres_runtime.d30_store.test_dynamic_claim_provenance import (
    _cell,
    _valid_dynamic_authority,
)


def _validate(owner: object, *, policy_id: str = "policy-a") -> object:
    return postgres_withdrawal._validate_d30_owner_topology_from_held_rows(
        owner,  # type: ignore[arg-type]
        candidate_policy_id=policy_id,
        verifier_execution_spec_hash="2" * 64,
        activation_base_epoch_id=10,
        predecessor_epoch_id=10,
    )


def _valid_legacy_owner() -> object:
    _claim, owner = _valid_dynamic_authority()
    return owner


class _OwnerHeaderCursor:
    def __init__(self, rows: tuple[object, object, object]) -> None:
        self._rows = iter(rows)
        self._row: object = None
        self.statements: list[str] = []

    def execute(self, statement: str, _parameters: object) -> _OwnerHeaderCursor:
        self.statements.append(statement)
        self._row = next(self._rows)
        return self

    def fetchone(self) -> object:
        return self._row


def _work_row(work: M5RuntimeWork) -> tuple[object, ...]:
    return (*work.counter_values(), work.work_digest)


def _contribution_row(
    *,
    epoch_id: int,
    kind: M5RuntimeWorkContributionKind,
    source_id: str,
    source_identity_hash: str,
    revision: int,
    work: M5RuntimeWork | None = None,
) -> tuple[object, ...]:
    selected = M5RuntimeWork() if work is None else work
    return (
        epoch_id,
        *_work_row(selected),
        kind.value,
        source_id,
        source_identity_hash,
        postgres_withdrawal.digests.runtime_work_contribution_key_digest(
            epoch_id=epoch_id,
            contribution_kind=kind,
            source_id=source_id,
        ),
        revision,
    )


def _contribution_with_work(
    row: tuple[object, ...], work: M5RuntimeWork
) -> tuple[object, ...]:
    offset = len(M5RuntimeWork.counter_names())
    return _contribution_row(
        epoch_id=int(row[0]),
        kind=M5RuntimeWorkContributionKind(str(row[offset + 2])),
        source_id=str(row[offset + 3]),
        source_identity_hash=str(row[offset + 4]),
        revision=int(row[offset + 6]),
        work=work,
    )


def _transition_timing_row(
    *,
    epoch_id: int,
    kind: M5RuntimeWorkContributionKind,
    source_id: str,
    revision: int,
) -> tuple[object, ...]:
    observation = M5RuntimeTimingObservation.build(None)
    key = postgres_withdrawal.digests.runtime_work_contribution_key_digest(
        epoch_id=epoch_id,
        contribution_kind=kind,
        source_id=source_id,
    )
    return (
        epoch_id,
        kind.value,
        source_id,
        key,
        revision,
        False,
        *(None for _ in range(9)),
        observation.observation_digest,
        postgres_withdrawal.digests.transition_call_timing_digest(
            epoch_id=epoch_id,
            contribution_kind=kind,
            source_id=source_id,
            contribution_key_digest=key,
            anchor_revision=revision,
            observation_digest=observation.observation_digest,
        ),
    )


def _completed_d24_attempt(
    owner: object,
    job: tuple[object, ...],
    attempt: tuple[object, ...],
) -> object:
    epoch_id = int(owner.epoch_id)  # type: ignore[attr-defined]
    completed_revision = int(job[17])
    dispatched_revision = completed_revision - 1
    dispatch = M5DispatchRecord.build(
        epoch_id=epoch_id,
        subgraph=M5RuntimeSubgraph.DIRECT,
        attempt_id=str(attempt[0]),
        logical_job_id=str(job[0]),
        attempt_ordinal=int(attempt[3]),
        job_kind=str(job[3]),
        fallback_required=False,
        dispatched_revision=dispatched_revision,
        lease_expires_at=attempt[6],  # type: ignore[arg-type]
    )
    timing = M5RuntimeTimingObservation.build(None)
    attempt_timing_digest = postgres_withdrawal.digests.attempt_runtime_timing_digest(
        epoch_id=epoch_id,
        subgraph=M5RuntimeSubgraph.DIRECT,
        attempt_id=str(attempt[0]),
        observation_digest=timing.observation_digest,
    )
    evidence = M5AttemptExecutionEvidence.build(
        epoch_id=epoch_id,
        subgraph=M5RuntimeSubgraph.DIRECT,
        attempt_id=str(attempt[0]),
        disposition=M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
        result_or_error_hash=str(job[15]),
        attempt_work=M5RuntimeWork(),
        attempt_timing_digest=attempt_timing_digest,
    )
    timing_row = (
        epoch_id,
        M5RuntimeSubgraph.DIRECT.value,
        str(attempt[0]),
        evidence.evidence_digest,
        False,
        *(None for _ in range(9)),
        timing.observation_digest,
        attempt_timing_digest,
    )
    transition_payload_hash = str(job[5])
    return postgres_withdrawal._D30D24AttemptLocator(
        attempt_id=str(attempt[0]),
        job_id=str(job[0]),
        attempt_state="completed",
        dispatch_row=(
            epoch_id,
            *_work_row(dispatch.maximum_ambiguous_call_work),
            dispatch.subgraph.value,
            dispatch.attempt_id,
            dispatch.logical_job_id,
            dispatch.attempt_ordinal,
            dispatch.job_kind,
            dispatch.fallback_required,
            dispatch.dispatched_revision,
            dispatch.lease_expires_at,
            dispatch.record_digest,
        ),
        evidence_row=(
            epoch_id,
            *_work_row(evidence.attempt_work),
            evidence.subgraph.value,
            evidence.attempt_id,
            evidence.disposition.value,
            evidence.result_or_error_hash,
            evidence.attempt_timing_digest,
            evidence.evidence_digest,
        ),
        timing_row=timing_row,
        acquisition_contribution_row=_contribution_row(
            epoch_id=epoch_id,
            kind=M5RuntimeWorkContributionKind.DIRECT_ACQUISITION,
            source_id=dispatch.record_digest,
            source_identity_hash=dispatch.record_digest,
            revision=dispatched_revision,
        ),
        attempt_contribution_row=_contribution_row(
            epoch_id=epoch_id,
            kind=M5RuntimeWorkContributionKind.DIRECT_ATTEMPT_EXECUTION,
            source_id=str(attempt[0]),
            source_identity_hash=evidence.evidence_digest,
            revision=completed_revision,
        ),
        transition_contribution_row=_contribution_row(
            epoch_id=epoch_id,
            kind=M5RuntimeWorkContributionKind.DIRECT_TRANSITION,
            source_id=str(job[13]),
            source_identity_hash=transition_payload_hash,
            revision=completed_revision,
        ),
        transition_timing_rows=(
            _transition_timing_row(
                epoch_id=epoch_id,
                kind=M5RuntimeWorkContributionKind.DIRECT_ACQUISITION,
                source_id=dispatch.record_digest,
                revision=dispatched_revision,
            ),
            _transition_timing_row(
                epoch_id=epoch_id,
                kind=M5RuntimeWorkContributionKind.DIRECT_TRANSITION,
                source_id=str(job[13]),
                revision=completed_revision,
            ),
        ),
        m4_transition_row=(
            epoch_id,
            str(job[13]),
            transition_payload_hash,
            "delta",
            completed_revision - 1,
            completed_revision,
            0,
        ),
        late_envelope_row=None,
        expired_return_row=None,
        postterminal_timing_row=None,
        postterminal_audit_row=None,
        preterminal_late_contribution_row=None,
    )


def _expired_preterminal_d24_attempt(
    owner: object,
    job: tuple[object, ...],
    attempt: tuple[object, ...],
    *,
    verification_execution_present: bool = False,
    observation_produced_epoch: int | None = None,
    observation_eligible_for_currency: bool = True,
) -> object:
    epoch_id = int(owner.epoch_id)  # type: ignore[attr-defined]
    activity_revision = int(job[17]) - 2
    dispatch_revision = activity_revision - 1
    job_kind = JobKind(str(job[3]))
    assert job_kind is JobKind.VERIFY_PAIR
    pair = PairKey(str(job[7]), str(job[8]))
    job_spec = LogicalJobSpec(
        job_id=str(job[0]),
        event_id=str(owner.epoch_row[1]),  # type: ignore[attr-defined]
        kind=job_kind,
        candidate_policy_id=str(job[4]),
        payload_hash=str(job[5]),
        execution_spec_hash=str(job[6]),
        parent_job_id=str(job[2]),
        pair=pair,
        expandable=False,
    )
    job_attempt = JobAttempt(
        attempt_id=str(attempt[0]),
        job_id=str(job[0]),
        execution_spec_hash=str(job[6]),
        attempt_ordinal=int(attempt[3]),
        lease_token_hash=str(attempt[4]),
    )
    completion = JobCompletion(
        job_id=str(job[0]),
        payload_hash=str(job[5]),
        execution_spec_hash=str(job[6]),
        result_artifact_id=str(job[14]),
        result_artifact_hash=str(job[15]),
        terminal_state=JobState(str(job[10])),
        completion_digest=str(job[13]),
    )
    observation = SemanticObservation(
        observation_id="expired-preterminal-observation",
        subject_kind=SubjectKind.CLAIM,
        subject_id=pair.claim_id,
        chunk_version_id=pair.chunk_version_id,
        task_type="arbitrary-expired-task",
        support_score=0.8,
        refute_score=0.1,
        neutral_score=0.1,
        producer=ModelStamp("model", "revision", "prompt"),
        input_hash="6" * 64,
    )
    raw_output_hash = "7" * 64 if verification_execution_present else str(job[15])
    verification_execution = (
        M5TypedDirectVerificationExecution(
            observation_id=observation.observation_id,
            job_id=job_spec.job_id,
            admitted_pair_id=stable_m4_digest(
                "m4-admitted-pair-v1",
                str(epoch_id),
                pair.claim_id,
                pair.chunk_version_id,
                job_spec.candidate_policy_id,
            ),
            model_artifact_id="expired-model-artifact",
            prompt_artifact_id="expired-prompt-artifact",
            execution_spec_hash=job_spec.execution_spec_hash,
            pair_input_hash="8" * 64,
            calibration_version="expired-calibration-v1",
            calibration_artifact_sha256="9" * 64,
            temperature=1.0,
            raw_logits=(1.0, 0.0, -1.0),
            raw_output_hash=raw_output_hash,
            reused_from_observation_id=None,
        )
        if verification_execution_present
        else None
    )
    envelope = M5TypedDirectLateReturnEnvelope.build_verifier(
        epoch_id=epoch_id,
        job=job_spec,
        attempt=job_attempt,
        completion=completion,
        verification_execution=verification_execution,
        observation=observation,
        observation_produced_epoch=epoch_id,
        observation_raw_output_hash=raw_output_hash,
        observation_eligible_for_currency=True,
        requested_make_effective=True,
    )
    job_binding, attempt_binding, completion_binding, verifier_binding = (
        _envelope_json_bindings(envelope)
    )
    effective_produced_epoch = (
        epoch_id if observation_produced_epoch is None else observation_produced_epoch
    )
    effective_eligible = observation_eligible_for_currency
    verifier_binding = dict(verifier_binding)
    observation_binding = dict(verifier_binding["observation"])
    observation_binding["produced_epoch"] = effective_produced_epoch
    observation_binding["eligible_for_currency"] = effective_eligible
    verifier_binding["observation"] = observation_binding
    verifier_binding_digest = (
        postgres_withdrawal.digests.typed_direct_late_verifier_binding_digest(
            result_artifact_id=completion.result_artifact_id,
            result_artifact_hash=completion.result_artifact_hash,
            verification_execution_present=verification_execution is not None,
            verification_execution=(
                None
                if verification_execution is None
                else verification_execution.digest_values
            ),
            observation_id=observation.observation_id,
            observation_subject_kind=observation.subject_kind,
            observation_subject_id=observation.subject_id,
            observation_chunk_version_id=observation.chunk_version_id,
            observation_task_type=observation.task_type,
            observation_support_score=observation.support_score,
            observation_refute_score=observation.refute_score,
            observation_neutral_score=observation.neutral_score,
            observation_model_id=observation.producer.model_id,
            observation_model_version=observation.producer.model_version,
            observation_prompt_version=observation.producer.prompt_version,
            observation_input_hash=observation.input_hash,
            observation_produced_epoch=effective_produced_epoch,
            observation_raw_output_hash=raw_output_hash,
            observation_eligible_for_currency=effective_eligible,
            requested_make_effective=True,
        )
    )
    envelope_digest = (
        postgres_withdrawal.digests.typed_direct_late_return_envelope_digest(
            epoch_id=epoch_id,
            return_kind=envelope.return_kind,
            job_binding_digest=envelope.job_binding_digest,
            attempt_binding_digest=envelope.attempt_binding_digest,
            completion_binding_digest=envelope.completion_binding_digest,
            discovery_binding_digest=None,
            scope_binding_digest=None,
            verifier_binding_digest=verifier_binding_digest,
        )
    )
    late_envelope_row = (
        epoch_id,
        envelope.return_kind.value,
        envelope.job_id,
        envelope.attempt_id,
        envelope.result_artifact_id,
        envelope.result_artifact_hash,
        envelope.verification_execution_present,
        effective_eligible,
        envelope.requested_make_effective,
        job_binding,
        attempt_binding,
        completion_binding,
        None,
        None,
        verifier_binding,
        envelope.job_binding_digest,
        envelope.attempt_binding_digest,
        envelope.completion_binding_digest,
        None,
        None,
        verifier_binding_digest,
        envelope_digest,
    )
    expired = M5ExpiredAttemptReturn.build(
        subgraph=M5RuntimeSubgraph.DIRECT,
        epoch_id=epoch_id,
        attempt_id=str(attempt[0]),
        logical_job_id=str(job[0]),
        worker_output_digest=envelope_digest,
        worker_artifact_hash=str(job[15]),
        activity_snapshot_epoch_id=epoch_id,
        activity_snapshot_revision=activity_revision,
        received_after_terminal=False,
    )
    timing = M5RuntimeTimingObservation.build(None)
    attempt_timing_digest = postgres_withdrawal.digests.attempt_runtime_timing_digest(
        epoch_id=epoch_id,
        subgraph=M5RuntimeSubgraph.DIRECT,
        attempt_id=str(attempt[0]),
        observation_digest=timing.observation_digest,
    )
    evidence = M5AttemptExecutionEvidence.build(
        epoch_id=epoch_id,
        subgraph=M5RuntimeSubgraph.DIRECT,
        attempt_id=str(attempt[0]),
        disposition=M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
        result_or_error_hash=envelope_digest,
        attempt_work=M5RuntimeWork(),
        attempt_timing_digest=attempt_timing_digest,
    )
    dispatch = M5DispatchRecord.build(
        epoch_id=epoch_id,
        subgraph=M5RuntimeSubgraph.DIRECT,
        attempt_id=str(attempt[0]),
        logical_job_id=str(job[0]),
        attempt_ordinal=int(attempt[3]),
        job_kind=str(job[3]),
        fallback_required=False,
        dispatched_revision=dispatch_revision,
        lease_expires_at=attempt[6],  # type: ignore[arg-type]
    )
    return postgres_withdrawal._D30D24AttemptLocator(
        attempt_id=str(attempt[0]),
        job_id=str(job[0]),
        attempt_state="expired",
        dispatch_row=(
            epoch_id,
            *_work_row(dispatch.maximum_ambiguous_call_work),
            dispatch.subgraph.value,
            dispatch.attempt_id,
            dispatch.logical_job_id,
            dispatch.attempt_ordinal,
            dispatch.job_kind,
            dispatch.fallback_required,
            dispatch.dispatched_revision,
            dispatch.lease_expires_at,
            dispatch.record_digest,
        ),
        evidence_row=(
            epoch_id,
            *_work_row(evidence.attempt_work),
            evidence.subgraph.value,
            evidence.attempt_id,
            evidence.disposition.value,
            evidence.result_or_error_hash,
            evidence.attempt_timing_digest,
            evidence.evidence_digest,
        ),
        timing_row=(
            epoch_id,
            M5RuntimeSubgraph.DIRECT.value,
            str(attempt[0]),
            evidence.evidence_digest,
            False,
            *(None for _ in range(9)),
            timing.observation_digest,
            attempt_timing_digest,
        ),
        acquisition_contribution_row=_contribution_row(
            epoch_id=epoch_id,
            kind=M5RuntimeWorkContributionKind.DIRECT_ACQUISITION,
            source_id=dispatch.record_digest,
            source_identity_hash=dispatch.record_digest,
            revision=dispatch_revision,
        ),
        attempt_contribution_row=_contribution_row(
            epoch_id=epoch_id,
            kind=M5RuntimeWorkContributionKind.DIRECT_ATTEMPT_EXECUTION,
            source_id=str(attempt[0]),
            source_identity_hash=evidence.evidence_digest,
            revision=activity_revision,
        ),
        transition_contribution_row=None,
        transition_timing_rows=(
            _transition_timing_row(
                epoch_id=epoch_id,
                kind=M5RuntimeWorkContributionKind.DIRECT_ACQUISITION,
                source_id=dispatch.record_digest,
                revision=dispatch_revision,
            ),
            _transition_timing_row(
                epoch_id=epoch_id,
                kind=M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN,
                source_id=str(attempt[0]),
                revision=activity_revision,
            ),
        ),
        m4_transition_row=None,
        late_envelope_row=late_envelope_row,
        expired_return_row=(
            epoch_id,
            expired.subgraph.value,
            expired.attempt_id,
            expired.logical_job_id,
            str(attempt[4]),
            attempt[6],
            expired.worker_output_digest,
            expired.worker_artifact_hash,
            expired.activity_snapshot_epoch_id,
            expired.activity_snapshot_revision,
            {
                "cancelled_by_event_id": None,
                "cancelled_by_epoch_id": None,
                "cancellation_reason": None,
            },
            "attempt_expired",
            evidence.evidence_digest,
            expired.received_after_terminal,
            expired.expired_return_digest,
        ),
        postterminal_timing_row=None,
        postterminal_audit_row=None,
        preterminal_late_contribution_row=_contribution_row(
            epoch_id=epoch_id,
            kind=M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN,
            source_id=str(attempt[0]),
            source_identity_hash=expired.expired_return_digest,
            revision=activity_revision,
            work=M5RuntimeWork(requirement_late_attempt_artifact_count=1),
        ),
    )


def _terminal_rows(
    result: M5EventRunResult,
    terminal_call_work: M5RuntimeWork,
) -> object:
    publication = result.publication_receipt
    coverage = result.event_timing_coverage
    assert publication is not None
    assert coverage is not None
    assert result.replayed_outcome is M5ReplayedOutcome.SEALED
    assert result.logical_result_hash is not None
    timing = result.event_timing
    event_id = result.event_id
    event_result_row = (
        event_id,
        result.payload_hash,
        result.epoch_id,
        result.replayed_outcome.value,
        postgres_withdrawal.digests.open_event_receipt_binding_digest(
            epoch_id=result.epoch_id,
            replayed=False,
            already_sealed=False,
            publication_id=None,
            already_failed=False,
            failure_reason=None,
        ),
        publication.publication_id,
        postgres_withdrawal.digests.publication_receipt_binding_digest(
            epoch_id=result.epoch_id,
            publication_id=publication.publication_id,
            replayed=False,
        ),
        "event",
        result.event_work.work_digest,
        postgres_withdrawal.digests.combined_status_delta_set_digest(
            result.combined_deltas
        ),
        postgres_withdrawal.digests.changed_state_set_digest(
            reference.reference_digest for reference in result.changed_state_references
        ),
        None,
        result.logical_result_hash,
        len(result.combined_deltas),
        len(result.changed_state_references),
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
    work_rows = tuple(
        sorted(
            (
                (
                    result.epoch_id,
                    work.work_digest,
                    *work.counter_values(),
                    event_id,
                    kind,
                )
                for kind, work in (
                    ("event", result.event_work),
                    ("call", terminal_call_work),
                )
            ),
            key=lambda row: str(row[-1]).encode(),
        )
    )
    timing_coverage_row = (
        event_id,
        result.epoch_id,
        coverage.required_expected_count,
        coverage.required_observed_count,
        coverage.required_missing_count,
        coverage.postgres_server_execution_expected_count,
        coverage.postgres_server_execution_observed_count,
        coverage.postgres_server_execution_missing_count,
        coverage.postgres_lock_wait_expected_count,
        coverage.postgres_lock_wait_observed_count,
        coverage.postgres_lock_wait_missing_count,
        coverage.postgres_wal_bytes_expected_count,
        coverage.postgres_wal_bytes_observed_count,
        coverage.postgres_wal_bytes_missing_count,
        coverage.postgres_shared_block_reads_expected_count,
        coverage.postgres_shared_block_reads_observed_count,
        coverage.postgres_shared_block_reads_missing_count,
        coverage.terminal_client_roundtrip_included,
    )
    return postgres_withdrawal._D30D24TerminalLocator(
        event_result_row=event_result_row,
        work_rows=work_rows,
        timing_coverage_row=timing_coverage_row,
        delta_rows=(),
        reference_rows=(),
    )


def _valid_d24(
    owner: object,
    *,
    terminal_call_work: M5RuntimeWork | None = None,
    expired_verification_execution_present: bool = False,
    expired_observation_produced_epoch: int | None = None,
    expired_observation_eligible_for_currency: bool = True,
) -> object:
    epoch_id = int(owner.epoch_id)  # type: ignore[attr-defined]
    revision = int(owner.epoch_row[3])  # type: ignore[attr-defined]
    attempt_rows = tuple(
        (
            _completed_d24_attempt(owner, job, attempt)
            if str(attempt[5]) == "completed"
            else _expired_preterminal_d24_attempt(
                owner,
                job,
                attempt,
                verification_execution_present=(expired_verification_execution_present),
                observation_produced_epoch=expired_observation_produced_epoch,
                observation_eligible_for_currency=(
                    expired_observation_eligible_for_currency
                ),
            )
        )
        for job in owner.jobs  # type: ignore[attr-defined]
        for attempt in dict(owner.attempts)[str(job[0])]  # type: ignore[attr-defined]
    )
    zero_work = M5RuntimeWork()
    call_work = zero_work if terminal_call_work is None else terminal_call_work
    event_work = M5RuntimeWork(
        requirement_late_attempt_artifact_count=sum(
            attempt.preterminal_late_contribution_row is not None
            for attempt in attempt_rows
        )
    )
    zero_timing = M5RuntimeTiming()
    coverage = M5RuntimeTimingCoverage.single_point(
        None,
        terminal_client_roundtrip_included=False,
    )
    publication_id = stable_m4_digest("m4-publication-v1", str(epoch_id))
    result = M5EventRunResult.build(
        event_id=str(owner.epoch_row[1]),  # type: ignore[attr-defined]
        payload_hash=str(owner.epoch_row[2]),  # type: ignore[attr-defined]
        epoch_id=epoch_id,
        state=M5RunState.REPLAYED,
        replayed_outcome=M5ReplayedOutcome.SEALED,
        open_receipt=OpenEventReceipt(
            epoch_id,
            True,
            True,
            publication_id,
        ),
        publication_receipt=PublicationReceipt(
            epoch_id,
            publication_id,
            True,
        ),
        event_work=event_work,
        call_work=zero_work,
        event_timing=zero_timing,
        call_timing=zero_timing,
        combined_deltas=(),
        changed_state_references=(),
        failure_reason=None,
        event_timing_coverage=coverage,
        call_timing_coverage=coverage,
    )
    seal_identity = postgres_withdrawal.digests.seal_contribution_source_digest(
        structural_event_id=str(owner.epoch_row[1]),  # type: ignore[attr-defined]
        combined_status_delta_set_hash=(
            postgres_withdrawal.digests.combined_status_delta_set_digest(())
        ),
        changed_state_set_hash=postgres_withdrawal.digests.changed_state_set_digest(()),
        publication_id=publication_id,
    )
    timing_coverage_values = (
        coverage.required_expected_count,
        coverage.required_observed_count,
        coverage.required_missing_count,
        coverage.postgres_server_execution_expected_count,
        coverage.postgres_server_execution_observed_count,
        coverage.postgres_server_execution_missing_count,
        coverage.postgres_lock_wait_expected_count,
        coverage.postgres_lock_wait_observed_count,
        coverage.postgres_lock_wait_missing_count,
        coverage.postgres_wal_bytes_expected_count,
        coverage.postgres_wal_bytes_observed_count,
        coverage.postgres_wal_bytes_missing_count,
        coverage.postgres_shared_block_reads_expected_count,
        coverage.postgres_shared_block_reads_observed_count,
        coverage.postgres_shared_block_reads_missing_count,
    )
    return postgres_withdrawal._D30D24OwnerLocator(
        attempts=attempt_rows,
        work_accumulator_row=(
            epoch_id,
            *_work_row(event_work),
            revision,
            True,
        ),
        timing_accumulator_row=(
            epoch_id,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            *timing_coverage_values,
            None,
            None,
            None,
            None,
            revision,
            True,
        ),
        seal_contribution_row=_contribution_row(
            epoch_id=epoch_id,
            kind=M5RuntimeWorkContributionKind.SEAL,
            source_id=str(owner.epoch_row[1]),  # type: ignore[attr-defined]
            source_identity_hash=seal_identity,
            revision=revision,
        ),
        terminal_rows=_terminal_rows(result, call_work),
        terminal_call_work=call_work,
        terminal_result=result,
    )


def _valid_typed_owner() -> object:
    owner = _valid_legacy_owner()
    expires_at = datetime(2030, 1, 1, tzinfo=UTC)
    typed_attempts = tuple(
        (
            job_id,
            tuple(_cell(attempt, 6, expires_at) for attempt in attempts),
        )
        for job_id, attempts in owner.attempts  # type: ignore[attr-defined]
    )
    projections: list[tuple[str, tuple[object, ...]]] = []
    for job in owner.jobs:  # type: ignore[attr-defined]
        projection = M5TypedDirectTerminalProjection.build(
            job_id=str(job[0]),
            terminal_state=postgres_withdrawal.M4JobState(str(job[10])),
            terminal_reason=None,
            m4_completion_digest=str(job[13]),
            completed_revision=int(job[17]),
        )
        projections.append(
            (
                str(job[0]),
                (
                    owner.epoch_id,  # type: ignore[attr-defined]
                    str(job[0]),
                    projection.terminal_state.value,
                    projection.terminal_reason,
                    projection.m4_completion_digest,
                    projection.completed_revision,
                    projection.terminal_identity_hash,
                ),
            )
        )
    typed = replace(
        owner,
        runtime_row=(
            owner.epoch_id,  # type: ignore[attr-defined]
            str(owner.epoch_row[1]),  # type: ignore[attr-defined]
            "policy-a",
            "a" * 64,
            "b" * 64,
            "c" * 64,
            None,
            "d" * 64,
            "sealed",
            int(owner.epoch_row[3]),  # type: ignore[attr-defined]
            0,
            0,
            0,
            object(),
        ),
        attempts=typed_attempts,
        projections=tuple(sorted(projections, key=lambda item: item[0].encode())),
    )
    return replace(typed, d24=_valid_d24(typed))


def _valid_typed_owner_with_expired_preterminal(
    *,
    verification_execution_present: bool = False,
    observation_produced_epoch: int | None = None,
    observation_eligible_for_currency: bool = True,
) -> object:
    owner = _valid_typed_owner()
    jobs = {str(row[0]): row for row in owner.jobs}
    child_id = next(str(row[0]) for row in owner.jobs if row[2] is not None)
    child = jobs[child_id]
    attempts_by_job = dict(owner.attempts)
    previous = attempts_by_job[child_id][0]
    expired_attempt = (
        stable_m4_digest("m4-job-attempt-v1", child_id, "1"),
        child_id,
        str(child[6]),
        1,
        stable_m4_digest("m4-lease-token-v1", child_id, "1"),
        "expired",
        previous[6],
        previous[7],
        previous[8],
    )
    completed_attempt = (
        stable_m4_digest("m4-job-attempt-v1", child_id, "2"),
        child_id,
        str(child[6]),
        2,
        stable_m4_digest("m4-lease-token-v1", child_id, "2"),
        "completed",
        previous[6],
        previous[7],
        previous[8],
    )
    attempts_by_job[child_id] = (expired_attempt, completed_attempt)
    changed = replace(
        owner,
        attempts=tuple(
            sorted(attempts_by_job.items(), key=lambda item: item[0].encode())
        ),
        d24=None,
    )
    return replace(
        changed,
        d24=_valid_d24(
            changed,
            expired_verification_execution_present=(verification_execution_present),
            expired_observation_produced_epoch=observation_produced_epoch,
            expired_observation_eligible_for_currency=(
                observation_eligible_for_currency
            ),
        ),
    )


def _valid_typed_owner_with_expired_postterminal() -> object:
    owner = _valid_typed_owner_with_expired_preterminal()
    assert owner.d24 is not None
    position, located = next(
        (index, row)
        for index, row in enumerate(owner.d24.attempts)
        if row.attempt_state == "expired"
    )
    assert located.late_envelope_row is not None
    assert located.expired_return_row is not None
    assert located.evidence_row is not None
    raw_attempt = next(
        attempt
        for job_id, attempts in owner.attempts
        if job_id == located.job_id
        for attempt in attempts
        if str(attempt[0]) == located.attempt_id
    )
    epoch_id = owner.epoch_id
    terminal_revision = int(owner.epoch_row[3])
    expired = M5ExpiredAttemptReturn.build(
        subgraph=M5RuntimeSubgraph.DIRECT,
        epoch_id=epoch_id,
        attempt_id=located.attempt_id,
        logical_job_id=located.job_id,
        worker_output_digest=str(located.late_envelope_row[21]),
        worker_artifact_hash=str(located.late_envelope_row[5]),
        activity_snapshot_epoch_id=epoch_id,
        activity_snapshot_revision=terminal_revision,
        received_after_terminal=True,
    )
    evidence_offset = len(M5RuntimeWork.counter_names())
    evidence_work_digest = str(located.evidence_row[evidence_offset + 1])
    attempt_timing_digest = str(located.evidence_row[evidence_offset + 6])
    evidence_digest = str(located.evidence_row[evidence_offset + 7])
    timing = M5RuntimeTimingObservation.build(None)
    assert attempt_timing_digest == (
        postgres_withdrawal.digests.attempt_runtime_timing_digest(
            epoch_id=epoch_id,
            subgraph=M5RuntimeSubgraph.DIRECT,
            attempt_id=located.attempt_id,
            observation_digest=timing.observation_digest,
        )
    )
    expired_row = (
        epoch_id,
        M5RuntimeSubgraph.DIRECT.value,
        located.attempt_id,
        located.job_id,
        str(raw_attempt[4]),
        raw_attempt[6],
        expired.worker_output_digest,
        expired.worker_artifact_hash,
        expired.activity_snapshot_epoch_id,
        expired.activity_snapshot_revision,
        {
            "cancelled_by_event_id": None,
            "cancelled_by_epoch_id": None,
            "cancellation_reason": None,
        },
        "attempt_expired",
        evidence_digest,
        True,
        expired.expired_return_digest,
    )
    postterminal_timing_row = (
        epoch_id,
        M5RuntimeSubgraph.DIRECT.value,
        located.attempt_id,
        False,
        *(None for _ in range(9)),
        timing.observation_digest,
        attempt_timing_digest,
    )
    zero_work = M5RuntimeWork()
    previous_result = owner.d24.terminal_result
    result = M5EventRunResult.build(
        event_id=previous_result.event_id,
        payload_hash=previous_result.payload_hash,
        epoch_id=previous_result.epoch_id,
        state=previous_result.state,
        replayed_outcome=previous_result.replayed_outcome,
        open_receipt=previous_result.open_receipt,
        publication_receipt=previous_result.publication_receipt,
        event_work=zero_work,
        call_work=previous_result.call_work,
        event_timing=previous_result.event_timing,
        call_timing=previous_result.call_timing,
        combined_deltas=previous_result.combined_deltas,
        changed_state_references=previous_result.changed_state_references,
        failure_reason=previous_result.failure_reason,
        event_timing_coverage=previous_result.event_timing_coverage,
        call_timing_coverage=previous_result.call_timing_coverage,
    )
    assert result.logical_result_hash is not None
    postterminal_audit_row = (
        epoch_id,
        M5RuntimeSubgraph.DIRECT.value,
        located.attempt_id,
        "expired_return",
        expired.expired_return_digest,
        evidence_digest,
        evidence_work_digest,
        attempt_timing_digest,
        result.logical_result_hash,
    )
    postterminal = replace(
        located,
        timing_row=None,
        attempt_contribution_row=None,
        transition_timing_rows=located.transition_timing_rows[:1],
        expired_return_row=expired_row,
        postterminal_timing_row=postterminal_timing_row,
        postterminal_audit_row=postterminal_audit_row,
        preterminal_late_contribution_row=None,
    )
    attempts = list(owner.d24.attempts)
    attempts[position] = postterminal
    return replace(
        owner,
        d24=replace(
            owner.d24,
            attempts=tuple(attempts),
            work_accumulator_row=(
                epoch_id,
                *_work_row(zero_work),
                terminal_revision,
                True,
            ),
            terminal_rows=_terminal_rows(result, owner.d24.terminal_call_work),
            terminal_result=result,
        ),
    )


def test_owner_reader_uses_one_complete_epoch_job_and_dependency_range(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_d30_owner_locator")
    assert "FROM groundloop_semantic_job\n" in source
    assert source.count("WHERE epoch_id = %s") >= 2
    assert 'ORDER BY job_id COLLATE "C"' in source
    assert "FROM groundloop_semantic_job_dependency" in source
    assert 'parent_job_id COLLATE "C"' in source
    assert 'child_job_id COLLATE "C"' in source


def test_valid_legacy_and_typed_owner_topologies_pass() -> None:
    legacy = _validate(_valid_legacy_owner())
    typed = _validate(_valid_typed_owner())
    assert set(legacy) == set(typed)  # type: ignore[arg-type]
    assert len(legacy) == 2  # type: ignore[arg-type]


def test_typed_owner_without_d24_closure_fails_closed() -> None:
    with pytest.raises(EventConflictError, match="D24"):
        _validate(replace(_valid_typed_owner(), d24=None))


def test_typed_d24_attempt_set_is_exact() -> None:
    owner = _valid_typed_owner()
    assert owner.d24 is not None
    with pytest.raises(EventConflictError):
        _validate(
            replace(owner, d24=replace(owner.d24, attempts=owner.d24.attempts[1:]))
        )


def test_expired_preterminal_d24_closure_passes() -> None:
    _validate(_valid_typed_owner_with_expired_preterminal())


def test_expired_verifier_execution_allows_distinct_raw_and_artifact_hashes() -> None:
    owner = _valid_typed_owner_with_expired_preterminal(
        verification_execution_present=True
    )
    assert owner.d24 is not None
    located = next(row for row in owner.d24.attempts if row.attempt_state == "expired")
    assert located.late_envelope_row is not None
    verifier = located.late_envelope_row[14]
    assert isinstance(verifier, dict)
    observation = verifier["observation"]
    assert isinstance(observation, dict)
    assert observation["raw_output_hash"] != located.late_envelope_row[5]
    _validate(owner)


@pytest.mark.parametrize(
    "options",
    (
        {"observation_produced_epoch": 9},
        {"observation_eligible_for_currency": False},
    ),
)
def test_expired_verifier_rejects_self_consistent_ineligible_or_foreign_observation(
    options: dict[str, object],
) -> None:
    owner = _valid_typed_owner_with_expired_preterminal(**options)
    with pytest.raises(EventConflictError):
        _validate(owner)


def test_typed_terminal_call_work_may_be_nonzero() -> None:
    owner = _valid_typed_owner()
    call_work = M5RuntimeWork(direct_verifier_call_count=3)
    changed = replace(
        owner,
        d24=_valid_d24(owner, terminal_call_work=call_work),
    )
    assert changed.d24 is not None
    assert changed.d24.terminal_call_work == call_work
    _validate(changed)


def test_optional_timing_raw_sum_is_hidden_when_coverage_is_incomplete() -> None:
    owner = _valid_typed_owner()
    assert owner.d24 is not None
    timing_row = _cell(owner.d24.timing_accumulator_row, 6, 123)
    _validate(
        replace(
            owner,
            d24=replace(owner.d24, timing_accumulator_row=timing_row),
        )
    )


def test_expired_postterminal_d24_closure_passes() -> None:
    owner = _valid_typed_owner_with_expired_postterminal()
    assert owner.d24 is not None
    located = next(row for row in owner.d24.attempts if row.attempt_state == "expired")
    assert located.timing_row is None
    assert located.attempt_contribution_row is None
    assert located.preterminal_late_contribution_row is None
    assert located.postterminal_timing_row is not None
    assert located.postterminal_audit_row is not None
    assert len(located.transition_timing_rows) == 1
    _validate(owner)


@pytest.mark.parametrize(
    ("index", "bad"),
    (
        (0, 11),
        (1, "other-subgraph"),
        (2, "other-attempt"),
        (3, "other-return-kind"),
        (4, "0" * 64),
        (5, "1" * 64),
        (6, "2" * 64),
        (7, "3" * 64),
        (8, "4" * 64),
    ),
)
def test_expired_postterminal_audit_requires_every_exact_binding(
    index: int,
    bad: object,
) -> None:
    owner = _valid_typed_owner_with_expired_postterminal()
    assert owner.d24 is not None
    position, located = next(
        (position, row)
        for position, row in enumerate(owner.d24.attempts)
        if row.attempt_state == "expired"
    )
    assert located.postterminal_audit_row is not None
    attempts = list(owner.d24.attempts)
    attempts[position] = replace(
        located,
        postterminal_audit_row=_cell(located.postterminal_audit_row, index, bad),
    )
    with pytest.raises(EventConflictError):
        _validate(replace(owner, d24=replace(owner.d24, attempts=tuple(attempts))))


@pytest.mark.parametrize("index", (13, 14))
def test_expired_postterminal_timing_digests_are_exact(index: int) -> None:
    owner = _valid_typed_owner_with_expired_postterminal()
    assert owner.d24 is not None
    position, located = next(
        (position, row)
        for position, row in enumerate(owner.d24.attempts)
        if row.attempt_state == "expired"
    )
    assert located.postterminal_timing_row is not None
    attempts = list(owner.d24.attempts)
    attempts[position] = replace(
        located,
        postterminal_timing_row=_cell(located.postterminal_timing_row, index, "5" * 64),
    )
    with pytest.raises(EventConflictError):
        _validate(replace(owner, d24=replace(owner.d24, attempts=tuple(attempts))))


def test_expired_postterminal_activity_revision_is_terminal_revision() -> None:
    owner = _valid_typed_owner_with_expired_postterminal()
    assert owner.d24 is not None
    position, located = next(
        (position, row)
        for position, row in enumerate(owner.d24.attempts)
        if row.attempt_state == "expired"
    )
    row = located.expired_return_row
    assert row is not None
    changed_revision = int(owner.epoch_row[3]) - 1
    changed_expired = M5ExpiredAttemptReturn.build(
        subgraph=M5RuntimeSubgraph.DIRECT,
        epoch_id=owner.epoch_id,
        attempt_id=located.attempt_id,
        logical_job_id=located.job_id,
        worker_output_digest=str(row[6]),
        worker_artifact_hash=str(row[7]),
        activity_snapshot_epoch_id=owner.epoch_id,
        activity_snapshot_revision=changed_revision,
        received_after_terminal=True,
    )
    changed_row = _cell(
        _cell(row, 9, changed_revision),
        14,
        changed_expired.expired_return_digest,
    )
    assert located.postterminal_audit_row is not None
    changed_audit = _cell(
        located.postterminal_audit_row,
        4,
        changed_expired.expired_return_digest,
    )
    attempts = list(owner.d24.attempts)
    attempts[position] = replace(
        located,
        expired_return_row=changed_row,
        postterminal_audit_row=changed_audit,
    )
    with pytest.raises(EventConflictError):
        _validate(replace(owner, d24=replace(owner.d24, attempts=tuple(attempts))))


def test_expired_attempt_without_return_has_only_acquisition_closure() -> None:
    owner = _valid_typed_owner_with_expired_preterminal()
    assert owner.d24 is not None
    position, attempt = next(
        (index, row)
        for index, row in enumerate(owner.d24.attempts)
        if row.attempt_state == "expired"
    )
    unresolved = replace(
        attempt,
        evidence_row=None,
        timing_row=None,
        attempt_contribution_row=None,
        transition_timing_rows=attempt.transition_timing_rows[:1],
        late_envelope_row=None,
        expired_return_row=None,
        postterminal_timing_row=None,
        postterminal_audit_row=None,
        preterminal_late_contribution_row=None,
    )
    attempts = list(owner.d24.attempts)
    attempts[position] = unresolved
    _validate(replace(owner, d24=replace(owner.d24, attempts=tuple(attempts))))


@pytest.mark.parametrize(
    "field",
    (
        "late_envelope_row",
        "expired_return_row",
        "timing_row",
        "attempt_contribution_row",
        "preterminal_late_contribution_row",
    ),
)
def test_expired_preterminal_requires_every_exact_sidecar(field: str) -> None:
    owner = _valid_typed_owner_with_expired_preterminal()
    assert owner.d24 is not None
    position, attempt = next(
        (index, row)
        for index, row in enumerate(owner.d24.attempts)
        if row.attempt_state == "expired"
    )
    attempts = list(owner.d24.attempts)
    attempts[position] = replace(attempt, **{field: None})
    with pytest.raises(EventConflictError):
        _validate(replace(owner, d24=replace(owner.d24, attempts=tuple(attempts))))


def test_expired_preterminal_timing_cannot_use_direct_attempt_anchor() -> None:
    owner = _valid_typed_owner_with_expired_preterminal()
    assert owner.d24 is not None
    position, attempt = next(
        (index, row)
        for index, row in enumerate(owner.d24.attempts)
        if row.attempt_state == "expired"
    )
    assert attempt.expired_return_row is not None
    changed_timing = _transition_timing_row(
        epoch_id=owner.epoch_id,
        kind=M5RuntimeWorkContributionKind.DIRECT_ATTEMPT_EXECUTION,
        source_id=attempt.attempt_id,
        revision=int(attempt.expired_return_row[9]),
    )
    attempts = list(owner.d24.attempts)
    attempts[position] = replace(
        attempt,
        transition_timing_rows=(attempt.transition_timing_rows[0], changed_timing),
    )
    with pytest.raises(EventConflictError):
        _validate(replace(owner, d24=replace(owner.d24, attempts=tuple(attempts))))


def test_expired_preterminal_timing_anchor_is_exact_activity_revision() -> None:
    owner = _valid_typed_owner_with_expired_preterminal()
    assert owner.d24 is not None
    position, attempt = next(
        (index, row)
        for index, row in enumerate(owner.d24.attempts)
        if row.attempt_state == "expired"
    )
    assert attempt.expired_return_row is not None
    changed_timing = _transition_timing_row(
        epoch_id=owner.epoch_id,
        kind=M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN,
        source_id=attempt.attempt_id,
        revision=int(attempt.expired_return_row[9]) + 1,
    )
    attempts = list(owner.d24.attempts)
    attempts[position] = replace(
        attempt,
        transition_timing_rows=(attempt.transition_timing_rows[0], changed_timing),
    )
    with pytest.raises(EventConflictError):
        _validate(replace(owner, d24=replace(owner.d24, attempts=tuple(attempts))))


@pytest.mark.parametrize(
    "field",
    (
        "evidence_row",
        "timing_row",
        "attempt_contribution_row",
        "transition_contribution_row",
        "m4_transition_row",
    ),
)
def test_completed_typed_attempt_requires_its_exact_d24_closure(field: str) -> None:
    owner = _valid_typed_owner()
    assert owner.d24 is not None
    attempt = owner.d24.attempts[0]
    changed = replace(attempt, **{field: None})
    with pytest.raises(EventConflictError):
        _validate(
            replace(
                owner,
                d24=replace(
                    owner.d24,
                    attempts=(changed, *owner.d24.attempts[1:]),
                ),
            )
        )


def test_completed_typed_evidence_result_hash_cross_binding_is_exact() -> None:
    owner = _valid_typed_owner()
    assert owner.d24 is not None
    attempt = owner.d24.attempts[0]
    row = attempt.evidence_row
    assert row is not None and attempt.timing_row is not None
    offset = len(M5RuntimeWork.counter_names())
    changed_evidence = M5AttemptExecutionEvidence.build(
        epoch_id=int(row[0]),
        subgraph=M5RuntimeSubgraph(str(row[offset + 2])),
        attempt_id=str(row[offset + 3]),
        disposition=M5ExecutionEvidenceDisposition(str(row[offset + 4])),
        result_or_error_hash="e" * 64,
        attempt_work=M5RuntimeWork(),
        attempt_timing_digest=str(row[offset + 6]),
    )
    changed = replace(
        attempt,
        evidence_row=(
            int(row[0]),
            *_work_row(changed_evidence.attempt_work),
            changed_evidence.subgraph.value,
            changed_evidence.attempt_id,
            changed_evidence.disposition.value,
            changed_evidence.result_or_error_hash,
            changed_evidence.attempt_timing_digest,
            changed_evidence.evidence_digest,
        ),
        timing_row=_cell(attempt.timing_row, 3, changed_evidence.evidence_digest),
        attempt_contribution_row=_contribution_row(
            epoch_id=owner.epoch_id,
            kind=M5RuntimeWorkContributionKind.DIRECT_ATTEMPT_EXECUTION,
            source_id=attempt.attempt_id,
            source_identity_hash=changed_evidence.evidence_digest,
            revision=int(
                next(job for job in owner.jobs if str(job[0]) == attempt.job_id)[17]
            ),
        ),
    )
    with pytest.raises(EventConflictError):
        _validate(
            replace(
                owner,
                d24=replace(
                    owner.d24,
                    attempts=(changed, *owner.d24.attempts[1:]),
                ),
            )
        )


@pytest.mark.parametrize(
    "field",
    (
        "late_envelope_row",
        "expired_return_row",
        "postterminal_timing_row",
        "postterminal_audit_row",
        "preterminal_late_contribution_row",
    ),
)
def test_completed_typed_attempt_rejects_every_late_return_sidecar(field: str) -> None:
    owner = _valid_typed_owner()
    assert owner.d24 is not None
    attempt = owner.d24.attempts[0]
    changed = replace(attempt, **{field: ("unexpected",)})
    with pytest.raises(EventConflictError):
        _validate(
            replace(
                owner,
                d24=replace(
                    owner.d24,
                    attempts=(changed, *owner.d24.attempts[1:]),
                ),
            )
        )


@pytest.mark.parametrize(
    ("field", "index", "bad"),
    (
        ("work_accumulator_row", -2, 99),
        ("timing_accumulator_row", -2, 99),
        ("seal_contribution_row", -1, 99),
    ),
)
def test_typed_terminal_accounting_corruptions_fail_closed(
    field: str,
    index: int,
    bad: object,
) -> None:
    owner = _valid_typed_owner()
    assert owner.d24 is not None
    row = getattr(owner.d24, field)
    with pytest.raises(EventConflictError):
        _validate(
            replace(
                owner,
                d24=replace(owner.d24, **{field: _cell(row, index, bad)}),
            )
        )


def test_seal_accepts_nonzero_owned_counter_without_historical_resumming() -> None:
    owner = _valid_typed_owner()
    assert owner.d24 is not None
    seal = _contribution_with_work(
        owner.d24.seal_contribution_row,
        M5RuntimeWork(bytes_hashed=1),
    )
    _validate(replace(owner, d24=replace(owner.d24, seal_contribution_row=seal)))


@pytest.mark.parametrize(
    "work",
    (
        M5RuntimeWork(direct_verifier_call_count=1),
        M5RuntimeWork(public_delta_count=1),
    ),
)
def test_self_consistently_rehashed_invalid_seal_work_fails_closed(
    work: M5RuntimeWork,
) -> None:
    owner = _valid_typed_owner()
    assert owner.d24 is not None
    seal = _contribution_with_work(owner.d24.seal_contribution_row, work)
    with pytest.raises(EventConflictError):
        _validate(replace(owner, d24=replace(owner.d24, seal_contribution_row=seal)))


def test_direct_transition_accepts_owned_counter_and_rejects_foreign_counter() -> None:
    owner = _valid_typed_owner()
    assert owner.d24 is not None
    attempt = next(
        item
        for item in owner.d24.attempts
        if item.transition_contribution_row is not None
    )
    assert attempt.transition_contribution_row is not None
    allowed = replace(
        attempt,
        transition_contribution_row=_contribution_with_work(
            attempt.transition_contribution_row,
            M5RuntimeWork(direct_observation_artifact_count=1),
        ),
    )
    attempts = tuple(
        allowed if item.attempt_id == attempt.attempt_id else item
        for item in owner.d24.attempts
    )
    _validate(replace(owner, d24=replace(owner.d24, attempts=attempts)))

    forbidden = replace(
        attempt,
        transition_contribution_row=_contribution_with_work(
            attempt.transition_contribution_row,
            M5RuntimeWork(requirement_forward_retrieval_call_count=1),
        ),
    )
    attempts = tuple(
        forbidden if item.attempt_id == attempt.attempt_id else item
        for item in owner.d24.attempts
    )
    with pytest.raises(EventConflictError):
        _validate(replace(owner, d24=replace(owner.d24, attempts=attempts)))


def test_empty_task_is_legacy_only() -> None:
    claim, legacy_owner = _valid_dynamic_authority(task_type="")
    _validate(legacy_owner)
    postgres_withdrawal._validate_d30_dynamic_claim(
        claim,
        owner=legacy_owner,
        candidate_policy_id="policy-a",
    )
    with pytest.raises(EventConflictError):
        postgres_withdrawal._validate_d30_dynamic_claim(
            claim,
            owner=_valid_typed_owner(),
            candidate_policy_id="policy-a",
        )


@pytest.mark.parametrize(
    "mutate",
    (
        lambda owner: replace(owner, dependencies=()),
        lambda owner: replace(
            owner,
            dependencies=(*owner.dependencies, owner.dependencies[0]),
        ),
        lambda owner: replace(owner, jobs=(*owner.jobs, owner.jobs[-1])),
        lambda owner: replace(
            owner,
            jobs=tuple(
                _cell(job, 10, "running") if job[2] is not None else job
                for job in owner.jobs
            ),
        ),
        lambda owner: replace(
            owner,
            jobs=tuple(
                _cell(job, 2, "missing-parent") if job[2] is not None else job
                for job in owner.jobs
            ),
        ),
        lambda owner: replace(
            owner,
            scopes=tuple(
                (job_id, None) if scope is not None else (job_id, scope)
                for job_id, scope in owner.scopes
            ),
        ),
        lambda owner: replace(owner, attempts=owner.attempts[:-1]),
        lambda owner: replace(owner, discovery_results=owner.discovery_results[:-1]),
        lambda owner: replace(owner, projections=owner.projections[:-1]),
    ),
)
def test_missing_extra_or_malformed_owner_topology_fails_closed(mutate: object) -> None:
    owner = mutate(_valid_legacy_owner())  # type: ignore[operator]
    with pytest.raises(EventConflictError):
        _validate(owner)


@pytest.mark.parametrize(
    ("parent_kind", "parent_target_claim_id", "parent_target_chunk_version_id"),
    (
        (JobKind.IMPACT_DISCOVERY, "claim-a", "chunk-other"),
        (JobKind.FRONTIER_RETRIEVE, "claim-other", "chunk-a"),
    ),
)
def test_verifier_pair_must_match_its_contextual_root_target(
    parent_kind: JobKind,
    parent_target_claim_id: str,
    parent_target_chunk_version_id: str,
) -> None:
    _claim, owner = _valid_dynamic_authority(
        parent_kind=parent_kind,
        parent_target_claim_id=parent_target_claim_id,
        parent_target_chunk_version_id=parent_target_chunk_version_id,
    )

    with pytest.raises(EventConflictError, match="target disagrees"):
        _validate(owner)


@pytest.mark.parametrize(
    ("index", "bad"),
    (
        (0, "wrong-child-id"),
        (4, "policy-b"),
        (5, "a" * 64),
        (6, "b" * 64),
        (13, "c" * 64),
        (14, "other-child-result"),
        (15, "d" * 64),
        (17, 8),
    ),
)
def test_child_identity_policy_completion_and_result_corruptions_fail_closed(
    index: int,
    bad: object,
) -> None:
    owner = _valid_legacy_owner()
    jobs = tuple(
        _cell(job, index, bad) if job[2] is not None else job for job in owner.jobs
    )
    changed_owner = replace(owner, jobs=jobs)
    if index == 17:
        _validate(changed_owner)
        claim, _original_owner = _valid_dynamic_authority()
        with pytest.raises(EventConflictError):
            postgres_withdrawal._validate_d30_dynamic_claim(
                claim,
                owner=changed_owner,
                candidate_policy_id="policy-a",
            )
        return
    with pytest.raises(EventConflictError):
        _validate(changed_owner)


def test_typed_projection_field_corruption_fails_closed() -> None:
    owner = _valid_typed_owner()
    job_id, projection = owner.projections[0]
    assert projection is not None
    for changed in (
        _cell(projection, 1, "other-job"),
        _cell(projection, 4, "e" * 64),
        _cell(projection, 5, int(projection[5]) + 1),
        _cell(projection, 6, "f" * 64),
    ):
        projections = ((job_id, changed), *owner.projections[1:])
        with pytest.raises(EventConflictError):
            _validate(replace(owner, projections=projections))


def test_cross_policy_failed_future_and_mixed_branch_owners_fail_closed() -> None:
    with pytest.raises(EventConflictError):
        _validate(_valid_legacy_owner(), policy_id="policy-b")

    owner = _valid_legacy_owner()
    future = replace(owner, epoch_id=11)
    with pytest.raises(EventConflictError):
        _validate(future)

    typed = _valid_typed_owner()
    mixed = replace(
        typed,
        projections=tuple((job_id, None) for job_id, _ in typed.projections),
    )
    with pytest.raises(EventConflictError):
        _validate(mixed)

    legacy_with_projection = replace(
        owner,
        projections=_valid_typed_owner().projections,
    )
    with pytest.raises(EventConflictError):
        _validate(legacy_with_projection)


def test_parent_result_allows_arbitrary_id_but_rejects_cross_binding() -> None:
    owner = _valid_legacy_owner()
    parent_id, result = next(
        (job_id, row) for job_id, row in owner.discovery_results if row is not None
    )
    assert result is not None and str(result[2]) == "opaque-parent-result"
    _validate(owner)
    for changed in (
        _cell(result, 2, "another-opaque-id"),
        _cell(result, 3, "e" * 64),
    ):
        rows = tuple(
            (job_id, changed if job_id == parent_id else row)
            for job_id, row in owner.discovery_results
        )
        with pytest.raises(EventConflictError):
            _validate(replace(owner, discovery_results=rows))


def test_typed_owner_path_names_complete_d24_evidence(
    function_source: Callable[[str], str],
) -> None:
    source = "\n".join(
        function_source(name)
        for name in (
            "_lock_d29_direct_attempts",
            "_lock_d30_typed_owner_evidence",
            "_validate_d30_d24_owner_from_held_rows",
            "_lock_d29_tier_10_authority",
            "_validate_d30_owner_topology_from_held_rows",
            "_lock_d30_dynamic_owner_topology",
        )
    ).lower()
    for authority in (
        "dispatch",
        "execution_evidence",
        "work_contribution",
        "timing",
        "terminal",
    ):
        assert authority in source


def test_expired_preterminal_locator_uses_only_late_return_timing_anchor(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_d30_d24_owner_locator")
    branch = source.split('str(attempt[5]) == "expired"', 1)[1].split(
        "attempt_locators.append", 1
    )[0]
    assert "PRETERMINAL_LATE_RETURN" in branch
    assert "expired_return[9]" in branch
    assert "DIRECT_ATTEMPT_EXECUTION" not in branch


def test_locked_orchestration_finalizes_owner_before_tier_11a(
    function_source: Callable[[str], str],
) -> None:
    prepare = function_source("_prepare_locked_document_open")
    continuation = function_source("_continue_locked_document_open")
    assert "_gather_d29_locator_authority" in prepare
    assert "_gather_d29_locator_authority" not in continuation
    topology = continuation.index("_lock_d30_dynamic_owner_topology")
    tier_11a = continuation.index("_lock_d29_observation_authority")
    dml_boundary = continuation.index("plan_requirement_withdrawal")
    assert topology < tier_11a < dml_boundary


def test_owner_headers_are_nonlocking_reread_before_held_row_validation(
    function_source: Callable[[str], str],
) -> None:
    owner = _valid_legacy_owner()
    cursor = _OwnerHeaderCursor(
        (owner.epoch_row, owner.update_row, owner.runtime_row)  # type: ignore[attr-defined]
    )
    postgres_withdrawal._reread_d30_owner_headers(  # type: ignore[arg-type]
        cursor,
        owner,  # type: ignore[arg-type]
    )
    source = "\n".join(cursor.statements)
    assert source.index("groundloop_epoch") < source.index("groundloop_m4_update")
    assert source.index("groundloop_m4_update") < source.index(
        "groundloop_m5_runtime_epoch"
    )
    assert "FOR UPDATE" not in source

    changed = _OwnerHeaderCursor((owner.epoch_row, owner.update_row, (10,)))  # type: ignore[attr-defined]
    with pytest.raises(EventConflictError, match="owner header changed"):
        postgres_withdrawal._reread_d30_owner_headers(  # type: ignore[arg-type]
            changed,
            owner,  # type: ignore[arg-type]
        )

    orchestration_source = function_source("_lock_d30_dynamic_owner_topology")
    reread = orchestration_source.index("_reread_d30_owner_headers")
    validate = orchestration_source.index("_validate_d30_owner_topology_from_held_rows")
    assert reread < validate


def test_tier_10_locks_attempt_outputs_then_typed_evidence_then_projections(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_lock_d29_tier_10_authority")
    m5_attempts = source.index("_lock_d29_m5_attempt_and_output_rows")
    typed_evidence = source.index("_lock_d30_typed_owner_evidence")
    projections = source.index("groundloop_m5_direct_terminal_projection")
    assert m5_attempts < typed_evidence < projections
