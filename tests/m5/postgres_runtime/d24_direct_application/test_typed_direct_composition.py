"""Live composition evidence for the production typed-direct facade."""

from __future__ import annotations

import time
from copy import copy
from dataclasses import dataclass, field, replace
from datetime import timedelta
from typing import Any, cast

import pytest
from psycopg.pq import TransactionStatus

from groundloop.errors import EventConflictError, ValidationError
from groundloop.events import InsertDocumentEvent
from groundloop.m4.application import DiscoveryResult, OpenEventReceipt
from groundloop.m4.contracts import (
    JobAttempt,
    JobKind,
    JobState,
    LogicalJobSpec,
    stable_m4_digest,
)
from groundloop.m5.runtime import (
    digests,
    postgres_direct_application,
    postgres_direct_recovery,
)
from groundloop.m5.runtime.application import (
    M5ExternalWorkFailure,
    M5RequirementRootDeclaration,
)
from groundloop.m5.runtime.contracts import (
    M5AcquisitionDisposition,
    M5CheckedDirectTerminalFailureReceipt,
    M5DirectAttemptReturnReceipt,
    M5DirectCursorContributionReceipt,
    M5DirectLateReturnReceipt,
    M5DirectNormalReturnReceipt,
    M5DiscoveryDirection,
    M5DiscoveryScopeContract,
    M5EventRunResult,
    M5ExecutionEvidenceDisposition,
    M5JobKind,
    M5LogicalJobSpec,
    M5ReplayedOutcome,
    M5RunFailureReason,
    M5RunState,
    M5RuntimeSubgraph,
    M5RuntimeTiming,
    M5RuntimeTimingObservation,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5TransitionTimingAnchor,
    M5TypedDirectAcquisitionReceipt,
    M5TypedDirectJobLease,
    M5TypedDirectTerminalProjection,
    M5TypedEventPlan,
)
from groundloop.m5.runtime.postgres_direct_application import (
    M5PostgresDirectDiscoveryExecution,
    M5PostgresDirectExternalWorkFailure,
    M5PostgresDirectVerifierExecution,
)
from groundloop.m5.runtime.postgres_direct_recovery import (
    PostgresM5DirectRecoveryStore,
)
from tests.m5.postgres.helpers import insert_observation, install_current_currency
from tests.m5.postgres_runtime.d24_direct_application.conftest import (
    ATTEMPT_TIMING,
    ControlledDirectDiscovery,
    ControlledDirectVerifier,
    ControlledMeasurements,
    DirectApplicationD24Database,
    assemble_typed_application,
    database_snapshot,
    open_event,
    open_inputs,
    sha,
)


class _StringSubclass(str):
    pass


class _TupleSubclass(tuple[object, ...]):
    pass


class _DictSubclass(dict[str, bool]):
    pass


class _StaticDiscovery:
    def __init__(self, result: M5PostgresDirectDiscoveryExecution) -> None:
        self.result = result
        self.calls = 0

    def discover_direct(self, *_: object) -> M5PostgresDirectDiscoveryExecution:
        self.calls += 1
        return self.result


def _attempt_for_job(job: LogicalJobSpec, ordinal: int) -> JobAttempt:
    return JobAttempt(
        stable_m4_digest("m4-job-attempt-v1", job.job_id, str(ordinal)),
        job.job_id,
        job.execution_spec_hash,
        ordinal,
        stable_m4_digest("m4-lease-token-v1", job.job_id, str(ordinal)),
    )


def _direct_evidence_digest(
    *,
    epoch_id: int,
    attempt_id: str,
    disposition: M5ExecutionEvidenceDisposition,
    result_or_error_hash: str,
    attempt_work: M5RuntimeWork,
    attempt_timing: M5RuntimeTiming | None,
) -> str:
    observation = M5RuntimeTimingObservation.build(attempt_timing)
    timing_digest = digests.attempt_runtime_timing_digest(
        epoch_id=epoch_id,
        subgraph=M5RuntimeSubgraph.DIRECT,
        attempt_id=attempt_id,
        observation_digest=observation.observation_digest,
    )
    return digests.attempt_execution_evidence_digest(
        epoch_id=epoch_id,
        subgraph=M5RuntimeSubgraph.DIRECT,
        attempt_id=attempt_id,
        disposition=disposition,
        result_or_error_hash=result_or_error_hash,
        attempt_work_digest=attempt_work.work_digest,
        attempt_timing_digest=timing_digest,
    )


def _forged_terminal_loser(
    acquisition: M5TypedDirectAcquisitionReceipt,
    *,
    mode: str,
    alternate_job: LogicalJobSpec | None = None,
) -> M5TypedDirectAcquisitionReceipt:
    assert acquisition.attempt is not None
    assert acquisition.lease.lease_expires_at is not None
    job = acquisition.job
    attempt = acquisition.attempt
    epoch_id = acquisition.epoch_id
    deadline = acquisition.lease.lease_expires_at
    dispatch_digest = acquisition.lease.dispatch_record_digest
    terminal_state = JobState.CANCELLED
    terminal_reason = "epoch_failed"
    resulting_revision = acquisition.lease.resulting_revision + 1
    if mode == "epoch":
        epoch_id += 1
    elif mode == "job":
        assert alternate_job is not None and alternate_job != job
        job = alternate_job
        attempt = _attempt_for_job(job, acquisition.attempt.attempt_ordinal)
    elif mode == "attempt":
        attempt = _attempt_for_job(job, acquisition.attempt.attempt_ordinal + 1)
    elif mode == "deadline":
        deadline += timedelta(microseconds=1)
    elif mode == "dispatch":
        dispatch_digest = sha("forged-loser-dispatch")
    elif mode == "terminal_failed":
        terminal_state = JobState.TERMINAL_FAILED
        terminal_reason = "forged-terminal-failure"
    elif mode == "revision_regressed":
        resulting_revision = acquisition.lease.resulting_revision
    elif mode == "revision_skipped":
        resulting_revision = acquisition.lease.resulting_revision + 2
    else:
        raise AssertionError(f"unknown forged loser mode: {mode}")
    projection = M5TypedDirectTerminalProjection.build(
        job_id=job.job_id,
        terminal_state=terminal_state,
        terminal_reason=terminal_reason,
        m4_completion_digest=None,
        completed_revision=resulting_revision,
    )
    lease = M5TypedDirectJobLease(
        job_id=job.job_id,
        attempt_id=attempt.attempt_id,
        lease_token_hash=attempt.lease_token_hash,
        lease_expires_at=deadline,
        dispatch_record_digest=dispatch_digest,
        resulting_revision=resulting_revision,
        disposition=M5AcquisitionDisposition.TERMINAL,
        should_execute=False,
        exact_replay=True,
        already_completed=False,
        terminal_projection=projection,
    )
    return M5TypedDirectAcquisitionReceipt(epoch_id, job, lease, attempt)


def _forged_checked_failure(
    acquisition: M5TypedDirectAcquisitionReceipt,
    *,
    event: M5TypedEventPlan,
    open_receipt: OpenEventReceipt,
    error_hash: str,
    attempt_work: M5RuntimeWork,
    attempt_timing: M5RuntimeTiming | None,
    call_work: M5RuntimeWork,
    mode: str,
) -> M5CheckedDirectTerminalFailureReceipt:
    assert acquisition.lease.attempt_id is not None
    epoch_id = acquisition.epoch_id
    job_id = acquisition.job.job_id
    attempt_id = acquisition.lease.attempt_id
    reason = M5RunFailureReason.INVALID_ARTIFACT
    event_id = event.structural_event_id
    payload_hash = event.payload_hash
    selected_open = open_receipt
    resulting_revision = acquisition.lease.resulting_revision + 1
    replayed = mode in {"replay_regressed", "replay_skipped"}
    if mode == "epoch":
        epoch_id += 1
        selected_open = OpenEventReceipt(epoch_id, False, False)
    elif mode == "job":
        job_id = sha("forged-checked-job")
    elif mode == "attempt":
        attempt_id = sha("forged-checked-attempt")
    elif mode == "reason":
        reason = M5RunFailureReason.INVARIANT_FAILURE
    elif mode == "event":
        event_id = "forged-checked-event"
    elif mode == "payload":
        payload_hash = sha("forged-checked-payload")
    elif mode == "held_object":
        selected_open = replace(open_receipt)
        assert selected_open == open_receipt and selected_open is not open_receipt
    elif mode == "evidence":
        pass
    elif mode in {"first_regressed", "replay_regressed"}:
        resulting_revision = acquisition.lease.resulting_revision
    elif mode in {"first_skipped", "replay_skipped"}:
        resulting_revision = acquisition.lease.resulting_revision + 2
    else:
        raise AssertionError(f"unknown forged checked mode: {mode}")
    if replayed:
        selected_open = OpenEventReceipt(
            epoch_id,
            True,
            False,
            None,
            True,
            reason.value,
        )
    evidence_digest = _direct_evidence_digest(
        epoch_id=epoch_id,
        attempt_id=attempt_id,
        disposition=M5ExecutionEvidenceDisposition.TERMINAL_FAILURE,
        result_or_error_hash=error_hash,
        attempt_work=attempt_work,
        attempt_timing=attempt_timing,
    )
    if mode == "evidence":
        evidence_digest = sha("forged-checked-evidence")
    terminal_result = M5EventRunResult.build(
        event_id=event_id,
        payload_hash=payload_hash,
        epoch_id=epoch_id,
        state=M5RunState.REPLAYED if replayed else M5RunState.FAILED,
        replayed_outcome=M5ReplayedOutcome.FAILED if replayed else None,
        open_receipt=selected_open,
        publication_receipt=None,
        event_work=call_work,
        call_work=M5RuntimeWork() if replayed else call_work,
        event_timing=M5RuntimeTiming(),
        call_timing=M5RuntimeTiming(),
        combined_deltas=(),
        changed_state_references=(),
        failure_reason=reason,
    )
    direct_failure = M5DirectCursorContributionReceipt(
        epoch_id=epoch_id,
        job_id=job_id,
        attempt_id=attempt_id,
        execution_evidence_digest=evidence_digest,
        attempt_execution_contribution_key_digest=(
            digests.runtime_work_contribution_key_digest(
                epoch_id=epoch_id,
                contribution_kind=(
                    M5RuntimeWorkContributionKind.DIRECT_ATTEMPT_EXECUTION
                ),
                source_id=attempt_id,
            )
        ),
        direct_transition_source_id=None,
        direct_transition_source_identity_hash=None,
        direct_transition_contribution_key_digest=None,
        observation_completion=None,
    )
    return M5CheckedDirectTerminalFailureReceipt(
        direct_failure,
        reason,
        resulting_revision,
        terminal_result,
    )


def _wrong_epoch_direct_return(
    receipt: M5DirectAttemptReturnReceipt,
) -> M5DirectAttemptReturnReceipt:
    if receipt.normal is not None:
        normal_branch = receipt.normal
        wrong_epoch = normal_branch.epoch_id + 1000
        anchor = M5TransitionTimingAnchor.build(
            epoch_id=wrong_epoch,
            contribution_kind=M5RuntimeWorkContributionKind.DIRECT_TRANSITION,
            source_id=normal_branch.direct_transition_source_id,
            anchor_revision=normal_branch.resulting_revision,
            terminal_transition=False,
        )
        normal = M5DirectNormalReturnReceipt(
            epoch_id=wrong_epoch,
            job_id=normal_branch.job_id,
            attempt_id=normal_branch.attempt_id,
            resulting_revision=normal_branch.resulting_revision,
            exact_replay=normal_branch.exact_replay,
            execution_evidence_digest=normal_branch.execution_evidence_digest,
            return_artifact_digest=normal_branch.return_artifact_digest,
            direct_transition_source_id=normal_branch.direct_transition_source_id,
            direct_transition_source_identity_hash=(
                normal_branch.direct_transition_source_identity_hash
            ),
            direct_transition_contribution_key_digest=(anchor.contribution_key_digest),
            observation_completion=normal_branch.observation_completion,
            current_terminal_logical_result_hash=(
                normal_branch.current_terminal_logical_result_hash
            ),
            transition_anchor=anchor,
        )
        return M5DirectAttemptReturnReceipt(receipt.return_kind, normal, None)
    late_branch = receipt.late
    assert late_branch is not None
    wrong_epoch = late_branch.epoch_id + 1000
    anchor = M5TransitionTimingAnchor.build(
        epoch_id=wrong_epoch,
        contribution_kind=M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN,
        source_id=late_branch.attempt_id,
        anchor_revision=late_branch.resulting_revision,
        terminal_transition=False,
    )
    late = M5DirectLateReturnReceipt(
        disposition=late_branch.disposition,
        epoch_id=wrong_epoch,
        job_id=late_branch.job_id,
        attempt_id=late_branch.attempt_id,
        resulting_revision=late_branch.resulting_revision,
        exact_replay=late_branch.exact_replay,
        envelope_digest=late_branch.envelope_digest,
        execution_evidence_digest=late_branch.execution_evidence_digest,
        expired_return_digest=late_branch.expired_return_digest,
        current_terminal_logical_result_hash=(
            late_branch.current_terminal_logical_result_hash
        ),
        transition_anchor=anchor,
    )
    return M5DirectAttemptReturnReceipt(receipt.return_kind, None, late)


def _changed_revision_direct_return(
    receipt: M5DirectAttemptReturnReceipt,
    *,
    resulting_revision: int,
) -> M5DirectAttemptReturnReceipt:
    if receipt.normal is not None:
        normal_branch = receipt.normal
        anchor = M5TransitionTimingAnchor.build(
            epoch_id=normal_branch.epoch_id,
            contribution_kind=M5RuntimeWorkContributionKind.DIRECT_TRANSITION,
            source_id=normal_branch.direct_transition_source_id,
            anchor_revision=resulting_revision,
            terminal_transition=False,
        )
        normal = replace(
            normal_branch,
            resulting_revision=resulting_revision,
            transition_anchor=anchor,
        )
        return M5DirectAttemptReturnReceipt(receipt.return_kind, normal, None)
    late_branch = receipt.late
    assert late_branch is not None
    anchor = M5TransitionTimingAnchor.build(
        epoch_id=late_branch.epoch_id,
        contribution_kind=M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN,
        source_id=late_branch.attempt_id,
        anchor_revision=resulting_revision,
        terminal_transition=False,
    )
    late = replace(
        late_branch,
        resulting_revision=resulting_revision,
        transition_anchor=anchor,
    )
    return M5DirectAttemptReturnReceipt(receipt.return_kind, None, late)


def _changed_direct_return_provenance(
    receipt: M5DirectAttemptReturnReceipt,
    *,
    mutation: str,
) -> M5DirectAttemptReturnReceipt:
    if receipt.normal is not None:
        normal = receipt.normal
        if mutation == "artifact":
            normal = replace(
                normal,
                return_artifact_digest=sha("forged-normal-return-artifact"),
            )
        elif mutation == "source":
            source_id = sha("forged-normal-transition-source")
            anchor = M5TransitionTimingAnchor.build(
                epoch_id=normal.epoch_id,
                contribution_kind=M5RuntimeWorkContributionKind.DIRECT_TRANSITION,
                source_id=source_id,
                anchor_revision=normal.resulting_revision,
                terminal_transition=False,
            )
            normal = replace(
                normal,
                direct_transition_source_id=source_id,
                direct_transition_contribution_key_digest=(
                    anchor.contribution_key_digest
                ),
                transition_anchor=anchor,
            )
        elif mutation == "evidence":
            normal = replace(
                normal,
                execution_evidence_digest=sha("forged-normal-execution-evidence"),
            )
        else:
            raise AssertionError(f"unknown normal provenance mutation: {mutation}")
        return M5DirectAttemptReturnReceipt(receipt.return_kind, normal, None)
    late = receipt.late
    assert late is not None
    if mutation == "envelope":
        late = replace(late, envelope_digest=sha("forged-late-envelope"))
    elif mutation == "evidence":
        late = replace(
            late,
            execution_evidence_digest=sha("forged-late-execution-evidence"),
        )
    else:
        raise AssertionError(f"unknown late provenance mutation: {mutation}")
    return M5DirectAttemptReturnReceipt(receipt.return_kind, None, late)


def _take_over_expired_attempt(
    database: DirectApplicationD24Database,
    epoch_id: int,
    acquisition: M5TypedDirectAcquisitionReceipt,
) -> M5TypedDirectAcquisitionReceipt:
    try:
        with database.reconnect() as competitor:
            revision = competitor.facade.current_revision(epoch_id)
            successor = competitor.adapter.acquire_direct_job_atomically(
                epoch_id,
                revision,
                acquisition.job,
            )
    finally:
        database.discovery.connection = database.connection
        database.verifier.connection = database.connection
    assert successor.lease.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert successor.lease.should_execute
    assert successor.attempt is not None
    return successor


@dataclass(slots=True)
class _SlowDiscovery(ControlledDirectDiscovery):
    takeover_call: int = 1
    successors: list[M5TypedDirectAcquisitionReceipt] = field(default_factory=list)

    def discover_direct(
        self,
        epoch_id: int,
        acquisition: M5TypedDirectAcquisitionReceipt,
        event: M5TypedEventPlan,
    ) -> M5PostgresDirectDiscoveryExecution:
        execution = ControlledDirectDiscovery.discover_direct(
            self, epoch_id, acquisition, event
        )
        if len(self.calls) != self.takeover_call:
            return execution
        time.sleep(0.35)
        self.successors.append(
            _take_over_expired_attempt(self.database, epoch_id, acquisition)
        )
        return execution


@dataclass(slots=True)
class _SlowVerifier(ControlledDirectVerifier):
    takeover_call: int = 1
    successors: list[M5TypedDirectAcquisitionReceipt] = field(default_factory=list)

    def verify_direct(
        self,
        epoch_id: int,
        acquisition: M5TypedDirectAcquisitionReceipt,
        event: M5TypedEventPlan,
    ) -> M5PostgresDirectVerifierExecution:
        execution = ControlledDirectVerifier.verify_direct(
            self, epoch_id, acquisition, event
        )
        if len(self.calls) != self.takeover_call:
            return execution
        time.sleep(0.35)
        self.successors.append(
            _take_over_expired_attempt(self.database, epoch_id, acquisition)
        )
        return execution


@dataclass(slots=True)
class _OrderedMeasurements(ControlledMeasurements):
    events: list[str] = field(default_factory=list)

    def transition_call_timing(
        self, anchor: M5TransitionTimingAnchor
    ) -> M5RuntimeTiming | None:
        self.events.append(f"measure:{anchor.contribution_kind.value}")
        return ControlledMeasurements.transition_call_timing(self, anchor)


@dataclass(slots=True)
class _OrderedDiscovery(ControlledDirectDiscovery):
    events: list[str] = field(default_factory=list)

    def discover_direct(
        self,
        epoch_id: int,
        acquisition: M5TypedDirectAcquisitionReceipt,
        event: M5TypedEventPlan,
    ) -> M5PostgresDirectDiscoveryExecution:
        self.events.append("provider:discovery")
        return ControlledDirectDiscovery.discover_direct(
            self, epoch_id, acquisition, event
        )


@dataclass(slots=True)
class _OrderedVerifier(ControlledDirectVerifier):
    events: list[str] = field(default_factory=list)

    def verify_direct(
        self,
        epoch_id: int,
        acquisition: M5TypedDirectAcquisitionReceipt,
        event: M5TypedEventPlan,
    ) -> M5PostgresDirectVerifierExecution:
        self.events.append("provider:verifier")
        return ControlledDirectVerifier.verify_direct(
            self, epoch_id, acquisition, event
        )


@dataclass(slots=True)
class _MutatingProviderInputs(ControlledDirectDiscovery):
    mutation: str = "acquisition"
    snapshots: list[tuple[tuple[str, tuple[tuple[str, str], ...]], ...]] = field(
        default_factory=list
    )

    def discover_direct(
        self,
        epoch_id: int,
        acquisition: M5TypedDirectAcquisitionReceipt,
        event: M5TypedEventPlan,
    ) -> M5PostgresDirectDiscoveryExecution:
        execution = ControlledDirectDiscovery.discover_direct(
            self, epoch_id, acquisition, event
        )
        self.snapshots.append(database_snapshot(self.database.connection))
        if self.mutation == "acquisition":
            object.__setattr__(
                acquisition.lease,
                "dispatch_record_digest",
                sha("mutated-provider-dispatch"),
            )
        elif self.mutation == "event":
            entry = event.active_chunk_snapshot.entries[0]
            object.__setattr__(entry, "text_hash", sha("mutated-provider-chunk"))
        else:
            raise AssertionError(f"unknown provider input mutation: {self.mutation}")
        return execution


@dataclass(slots=True)
class _RetainedInputFailureDiscovery(ControlledDirectDiscovery):
    retained: M5TypedDirectAcquisitionReceipt | None = None
    snapshot_before_failure: (
        tuple[tuple[str, tuple[tuple[str, str], ...]], ...] | None
    ) = None

    def discover_direct(
        self,
        epoch_id: int,
        acquisition: M5TypedDirectAcquisitionReceipt,
        event: M5TypedEventPlan,
    ) -> M5PostgresDirectDiscoveryExecution:
        execution = ControlledDirectDiscovery.discover_direct(
            self, epoch_id, acquisition, event
        )
        if self.retained is None:
            self.retained = acquisition
            return execution
        retained = self.retained
        object.__setattr__(acquisition, "job", retained.job)
        object.__setattr__(acquisition, "lease", retained.lease)
        object.__setattr__(acquisition, "attempt", retained.attempt)
        self.snapshot_before_failure = database_snapshot(self.database.connection)
        raise M5PostgresDirectExternalWorkFailure(
            M5RunFailureReason.RETRIEVAL_UNAVAILABLE,
            retryable=True,
            direct_terminal_reason=None,
            call_work=self.call_work,
            attempt_timing=self.attempt_timing,
            error_hash=sha("retained-input-provider-failure"),
        )


@dataclass(slots=True)
class _MutatingPriorOutputDiscovery(ControlledDirectDiscovery):
    retained: M5PostgresDirectDiscoveryExecution | None = None

    def discover_direct(
        self,
        epoch_id: int,
        acquisition: M5TypedDirectAcquisitionReceipt,
        event: M5TypedEventPlan,
    ) -> M5PostgresDirectDiscoveryExecution:
        execution = ControlledDirectDiscovery.discover_direct(
            self, epoch_id, acquisition, event
        )
        if self.retained is None:
            self.retained = execution
        else:
            object.__setattr__(
                self.retained.result,
                "result_artifact_hash",
                sha("later-provider-mutated-prior-output"),
            )
        return execution


@dataclass(slots=True)
class _MutatingDirectMeasurements(ControlledMeasurements):
    def transition_call_timing(
        self, anchor: M5TransitionTimingAnchor
    ) -> M5RuntimeTiming | None:
        observed = ControlledMeasurements.transition_call_timing(self, anchor)
        object.__setattr__(anchor, "source_id", "mutated-direct-timing-source")
        return observed


def _rows(
    database: DirectApplicationD24Database,
    query: str,
    parameters: tuple[object, ...],
) -> tuple[tuple[object, ...], ...]:
    with database.connection.transaction():
        return tuple(
            tuple(row)
            for row in database.connection.execute(query, parameters).fetchall()
        )


@pytest.mark.parametrize("include_pair", (False, True))
def test_production_direct_open_success_and_reconnect_replay_are_exact(
    d24_direct_application_db: DirectApplicationD24Database,
    include_pair: bool,
) -> None:
    database = d24_direct_application_db
    database.discovery.include_pair = include_pair
    plan = database.insert_plan(tag=f"success-{include_pair}")
    inputs = open_inputs(database, plan)
    opened = open_event(database, plan)
    assert opened == OpenEventReceipt(opened.epoch_id, False, False)
    assert database.facade.current_revision(opened.epoch_id) == 1
    assert len(inputs.direct_roots) == 1
    assert len(inputs.direct_scopes) == 1
    assert inputs.direct_scopes[0].registered_claim_ids == ()
    root = inputs.direct_roots[0]
    assert root.kind is JobKind.IMPACT_DISCOVERY
    assert root.payload_hash == stable_m4_digest(
        "m4-application-job-payload-v1",
        plan.payload_hash,
        root.kind.value,
        "",
        "",
        root.target_chunk_version_id or "",
    )

    result = database.facade.run_pending_direct(
        opened.epoch_id,
        1,
        plan,
        opened,
    )
    assert result.blocked_reason is None
    assert result.terminal_failure_reason is None
    assert result.selected_terminal_acquisition_receipt is None
    assert result.selected_checked_combined_failure_receipt is None
    assert result.resulting_revision == (5 if include_pair else 3)
    assert result.call_work.direct_discovery_call_count == 1
    assert result.call_work.direct_verifier_call_count == int(include_pair)
    assert result.call_work.embedding_model_call_count == 1
    assert result.call_work.verifier_model_call_count == int(include_pair)
    assert result.call_work.verifier_input_token_count == (5 if include_pair else 0)
    assert result.call_work.verifier_output_token_count == (1 if include_pair else 0)
    assert len(database.discovery.calls) == 1
    assert len(database.verifier.calls) == int(include_pair)
    assert database.discovery.transaction_statuses == [TransactionStatus.IDLE]
    assert database.verifier.transaction_statuses == (
        [TransactionStatus.IDLE] if include_pair else []
    )
    measurements = cast(ControlledMeasurements, database.bound.measurements)
    expected_timing_kinds = (
        M5RuntimeWorkContributionKind.DIRECT_ACQUISITION,
        M5RuntimeWorkContributionKind.DIRECT_TRANSITION,
    ) * (1 + int(include_pair))
    assert (
        tuple(anchor.contribution_kind for anchor in measurements.transition_calls)
        == expected_timing_kinds
    )
    assert _rows(
        database,
        """
        SELECT contribution_kind, required_interval_observed,
               coordinator_non_db_non_neural_ns,
               postgres_roundtrip_wall_ns, end_to_end_wall_ns
        FROM groundloop_m5_transition_call_timing
        WHERE epoch_id = %s AND contribution_kind LIKE 'direct_%%'
        ORDER BY anchor_revision, contribution_kind
        """,
        (opened.epoch_id,),
    ) == tuple((kind.value, True, 11, 13, 17) for kind in expected_timing_kinds)
    expected_direct_calls = len(expected_timing_kinds)
    expected_attempt_points = 1 + int(include_pair)
    assert _rows(
        database,
        """
        SELECT required_expected_count, required_observed_count,
               required_missing_count, pending_contribution_kind,
               pending_source_id, pending_anchor_revision, updated_revision
        FROM groundloop_m5_runtime_timing_accumulator
        WHERE epoch_id = %s
        """,
        (opened.epoch_id,),
    ) == (
        (
            expected_direct_calls + expected_attempt_points + 1,
            expected_direct_calls + expected_attempt_points,
            1,
            None,
            None,
            None,
            result.resulting_revision,
        ),
    )
    terminal_rows = _rows(
        database,
        """
        SELECT job_kind, job_state, count(*)
        FROM groundloop_semantic_job
        WHERE epoch_id = %s
        GROUP BY job_kind, job_state
        ORDER BY job_kind, job_state
        """,
        (opened.epoch_id,),
    )
    assert all(type(row[2]) is int for row in terminal_rows)
    assert sum(cast(int, row[2]) for row in terminal_rows) == (2 if include_pair else 1)
    assert all(row[1] == JobState.COMPLETED_ACTIVE.value for row in terminal_rows)
    assert plan.direct_plan is not None
    registry_rows = _rows(
        database,
        """
        SELECT claim_id
        FROM groundloop_m4_claim_registry_member
        WHERE claim_registry_snapshot_id = %s
        ORDER BY member_ordinal
        """,
        (plan.direct_plan.claim_registry_snapshot_id,),
    )
    assert registry_rows == tuple(
        (claim_id,) for claim_id in database.base.base.claim_ids
    )
    evidence_rows = _rows(
        database,
        """
        SELECT dispatch.job_kind,
               evidence.attempt_direct_discovery_call_count,
               evidence.attempt_direct_verifier_call_count,
               evidence.attempt_embedding_model_call_count,
               evidence.attempt_verifier_model_call_count
        FROM groundloop_m5_attempt_execution_evidence AS evidence
        JOIN groundloop_m5_dispatch_record AS dispatch
          ON dispatch.epoch_id = evidence.epoch_id
         AND dispatch.subgraph = evidence.subgraph
         AND dispatch.attempt_id = evidence.attempt_id
        WHERE evidence.epoch_id = %s AND evidence.subgraph = 'direct'
        ORDER BY dispatch.job_kind
        """,
        (opened.epoch_id,),
    )
    assert evidence_rows == ((JobKind.IMPACT_DISCOVERY.value, 1, 0, 1, 0),) + (
        ((JobKind.VERIFY_PAIR.value, 0, 1, 0, 1),) if include_pair else ()
    )
    assert _rows(
        database,
        """
        SELECT count(*)
        FROM groundloop_m5_typed_direct_late_return_envelope
        WHERE epoch_id = %s
        """,
        (opened.epoch_id,),
    ) == ((0,),)

    before = database_snapshot(database.connection)
    reconnect_discovery = ControlledDirectDiscovery(database, include_pair=include_pair)
    reconnect_verifier = ControlledDirectVerifier(database)
    with database.reconnect() as rebound:
        rebound = database.bind(
            rebound.connection,
            discovery=reconnect_discovery,
            verifier=reconnect_verifier,
        )
        replayed = open_event(database, plan, facade=rebound.facade)
        assert replayed == replace(opened, replayed=True)
        replay_result = rebound.facade.run_pending_direct(
            opened.epoch_id,
            result.resulting_revision,
            plan,
            replayed,
        )
        replay_measurements = cast(ControlledMeasurements, rebound.measurements)
        assert replay_measurements.transition_calls == []
    assert replay_result.resulting_revision == result.resulting_revision
    assert replay_result.call_work.is_zero
    assert reconnect_discovery.calls == []
    assert reconnect_verifier.calls == []
    assert database_snapshot(database.connection) == before


@pytest.mark.parametrize("include_pair", (False, True), ids=("discovery", "verifier"))
def test_direct_measurement_precedes_provider_and_next_mutator(
    d24_direct_application_db: DirectApplicationD24Database,
    include_pair: bool,
) -> None:
    database = d24_direct_application_db
    events: list[str] = []
    measurements = _OrderedMeasurements(events=events)
    discovery = _OrderedDiscovery(
        database,
        include_pair=include_pair,
        events=events,
    )
    verifier = _OrderedVerifier(database, events=events)
    bound = database.bind(
        database.connection,
        discovery=discovery,
        verifier=verifier,
        measurements=measurements,
    )
    plan = database.insert_plan(tag=f"timing-order-{include_pair}")
    opened = open_event(database, plan, facade=bound.facade)

    result = bound.facade.run_pending_direct(opened.epoch_id, 1, plan, opened)

    expected = [
        "measure:direct_acquisition",
        "provider:discovery",
        "measure:direct_transition",
    ]
    if include_pair:
        expected.extend(
            (
                "measure:direct_acquisition",
                "provider:verifier",
                "measure:direct_transition",
            )
        )
    assert events == expected
    assert result.resulting_revision == (5 if include_pair else 3)
    assert all(
        status is TransactionStatus.IDLE
        for status in (*discovery.transaction_statuses, *verifier.transaction_statuses)
    )


def test_direct_measurement_anchor_mutation_rejects_before_append_or_provider(
    d24_direct_application_db: DirectApplicationD24Database,
) -> None:
    database = d24_direct_application_db
    measurements = _MutatingDirectMeasurements()
    discovery = ControlledDirectDiscovery(database)
    bound = database.bind(
        database.connection,
        discovery=discovery,
        measurements=measurements,
    )
    plan = database.insert_plan(tag="mutated-direct-timing-anchor")
    opened = open_event(database, plan, facade=bound.facade)

    with pytest.raises(ValidationError):
        bound.facade.run_pending_direct(opened.epoch_id, 1, plan, opened)

    assert len(measurements.transition_calls) == 1
    assert discovery.calls == []
    assert _rows(
        database,
        """
        SELECT count(*)
        FROM groundloop_m5_transition_call_timing
        WHERE epoch_id = %s AND contribution_kind = 'direct_acquisition'
        """,
        (opened.epoch_id,),
    ) == ((0,),)
    assert _rows(
        database,
        """
        SELECT revision, pending_contribution_kind
        FROM groundloop_m5_runtime_epoch AS runtime
        JOIN groundloop_m5_runtime_timing_accumulator AS timing USING (epoch_id)
        WHERE runtime.epoch_id = %s
        """,
        (opened.epoch_id,),
    ) == ((2, M5RuntimeWorkContributionKind.DIRECT_ACQUISITION.value),)


@pytest.mark.parametrize("mutation", ("acquisition", "event"))
def test_provider_input_mutation_rejects_before_settlement(
    d24_direct_application_db: DirectApplicationD24Database,
    mutation: str,
) -> None:
    database = d24_direct_application_db
    discovery = _MutatingProviderInputs(database, mutation=mutation)
    measurements = ControlledMeasurements()
    bound = database.bind(
        database.connection,
        discovery=discovery,
        measurements=measurements,
    )
    plan = database.insert_plan(tag=f"provider-input-{mutation}")
    opened = open_event(database, plan, facade=bound.facade)

    with pytest.raises(ValidationError):
        bound.facade.run_pending_direct(opened.epoch_id, 1, plan, opened)

    assert len(discovery.snapshots) == 1
    assert database_snapshot(database.connection) == discovery.snapshots[0]
    assert tuple(
        anchor.contribution_kind for anchor in measurements.transition_calls
    ) == (M5RuntimeWorkContributionKind.DIRECT_ACQUISITION,)
    assert _rows(
        database,
        """
        SELECT count(*) FROM groundloop_m5_attempt_execution_evidence
        WHERE epoch_id = %s AND subgraph = 'direct'
        """,
        (opened.epoch_id,),
    ) == ((0,),)


def test_two_root_failure_cannot_rebind_callback_acquisition(
    d24_direct_application_db: DirectApplicationD24Database,
) -> None:
    database = d24_direct_application_db
    discovery = _RetainedInputFailureDiscovery(database)
    measurements = ControlledMeasurements()
    bound = database.bind(
        database.connection,
        discovery=discovery,
        measurements=measurements,
    )
    plan = database.insert_plan(tag="provider-retained-input", chunk_count=2)
    opened = open_event(database, plan, facade=bound.facade)

    with pytest.raises(ValidationError, match="mutated its callback inputs"):
        bound.facade.run_pending_direct(opened.epoch_id, 1, plan, opened)

    assert discovery.snapshot_before_failure is not None
    assert database_snapshot(database.connection) == discovery.snapshot_before_failure
    assert tuple(
        anchor.contribution_kind for anchor in measurements.transition_calls
    ) == (
        M5RuntimeWorkContributionKind.DIRECT_ACQUISITION,
        M5RuntimeWorkContributionKind.DIRECT_ACQUISITION,
    )
    assert _rows(
        database,
        """
        SELECT count(*) FROM groundloop_m5_attempt_execution_evidence
        WHERE epoch_id = %s AND subgraph = 'direct'
        """,
        (opened.epoch_id,),
    ) == ((0,),)


def test_later_provider_cannot_mutate_trusted_prior_output(
    d24_direct_application_db: DirectApplicationD24Database,
) -> None:
    database = d24_direct_application_db
    discovery = _MutatingPriorOutputDiscovery(database)
    bound = database.bind(database.connection, discovery=discovery)
    plan = database.insert_plan(tag="provider-prior-output", chunk_count=2)
    opened = open_event(database, plan, facade=bound.facade)
    roots = open_inputs(database, plan).direct_roots

    result = bound.facade.run_pending_direct(opened.epoch_id, 1, plan, opened)

    assert result.resulting_revision == 5
    assert result.blocked_reason is None
    assert discovery.retained is not None
    assert discovery.retained.result.result_artifact_hash == sha(
        "later-provider-mutated-prior-output"
    )
    assert _rows(
        database,
        """
        SELECT job_id, btrim(result_artifact_hash)
        FROM groundloop_semantic_job
        WHERE epoch_id = %s
        ORDER BY job_id COLLATE "C"
        """,
        (opened.epoch_id,),
    ) == tuple(
        (
            root.job_id,
            sha(f"direct-discovery:{opened.epoch_id}:{root.job_id}"),
        )
        for root in sorted(roots, key=lambda item: item.job_id)
    )


@pytest.mark.parametrize(
    ("return_branch", "mutation"),
    (
        ("normal", "epoch"),
        ("late", "epoch"),
        ("normal", "stale_revision"),
        ("late", "stale_revision"),
        ("normal", "skipped_revision"),
        ("normal", "artifact"),
        ("normal", "source"),
        ("normal", "evidence"),
        ("late", "envelope"),
        ("late", "evidence"),
    ),
)
def test_direct_return_provenance_rejects_before_transition_measurement(
    d24_direct_application_db: DirectApplicationD24Database,
    monkeypatch: pytest.MonkeyPatch,
    return_branch: str,
    mutation: str,
) -> None:
    database = d24_direct_application_db
    discovery: ControlledDirectDiscovery = (
        ControlledDirectDiscovery(database)
        if return_branch == "normal"
        else _SlowDiscovery(database)
    )
    measurements = ControlledMeasurements()
    bound = database.bind(
        database.connection,
        discovery=discovery,
        measurements=measurements,
    )
    plan = database.insert_plan(tag=f"return-{return_branch}-{mutation}")
    opened = open_event(database, plan, facade=bound.facade)
    original_settle = bound.adapter.settle_direct_expansion_atomically
    calls_before_return: list[int] = []

    def forged_settle(*args: Any, **kwargs: Any) -> M5DirectAttemptReturnReceipt:
        expected_revision = cast(int, args[1])
        receipt = original_settle(*args, **kwargs)
        calls_before_return.append(len(measurements.transition_calls))
        if mutation == "epoch":
            return _wrong_epoch_direct_return(receipt)
        if mutation == "stale_revision":
            return _changed_revision_direct_return(
                receipt,
                resulting_revision=expected_revision - 1,
            )
        if mutation == "skipped_revision":
            return _changed_revision_direct_return(
                receipt,
                resulting_revision=expected_revision + 2,
            )
        if mutation in {"artifact", "source", "evidence", "envelope"}:
            return _changed_direct_return_provenance(receipt, mutation=mutation)
        raise AssertionError(f"unknown return mutation: {mutation}")

    monkeypatch.setattr(
        bound.adapter,
        "settle_direct_expansion_atomically",
        forged_settle,
    )

    with pytest.raises(ValidationError):
        bound.facade.run_pending_direct(opened.epoch_id, 1, plan, opened)

    assert calls_before_return == [1]
    assert len(measurements.transition_calls) == 1
    rejected_kind = (
        M5RuntimeWorkContributionKind.DIRECT_TRANSITION
        if return_branch == "normal"
        else M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN
    )
    assert _rows(
        database,
        """
        SELECT count(*) FROM groundloop_m5_transition_call_timing
        WHERE epoch_id = %s AND contribution_kind = %s
        """,
        (opened.epoch_id, rejected_kind.value),
    ) == ((0,),)


def test_verifier_effective_receipt_is_bound_before_transition_measurement(
    d24_direct_application_db: DirectApplicationD24Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = d24_direct_application_db
    database.discovery.include_pair = True
    measurements = ControlledMeasurements()
    bound = database.bind(database.connection, measurements=measurements)
    plan = database.insert_plan(tag="verifier-effective-provenance")
    opened = open_event(database, plan, facade=bound.facade)
    original_settle = bound.adapter.settle_direct_verifier_atomically

    def forged_settle(*args: Any, **kwargs: Any) -> M5DirectAttemptReturnReceipt:
        receipt = original_settle(*args, **kwargs)
        normal = receipt.normal
        assert normal is not None and normal.observation_completion is not None
        forged_observation = replace(
            normal.observation_completion,
            made_effective=not normal.observation_completion.made_effective,
        )
        return M5DirectAttemptReturnReceipt(
            receipt.return_kind,
            replace(normal, observation_completion=forged_observation),
            None,
        )

    monkeypatch.setattr(
        bound.adapter,
        "settle_direct_verifier_atomically",
        forged_settle,
    )

    with pytest.raises(ValidationError, match="changed its effective request"):
        bound.facade.run_pending_direct(opened.epoch_id, 1, plan, opened)

    assert tuple(
        anchor.contribution_kind for anchor in measurements.transition_calls
    ) == (
        M5RuntimeWorkContributionKind.DIRECT_ACQUISITION,
        M5RuntimeWorkContributionKind.DIRECT_TRANSITION,
        M5RuntimeWorkContributionKind.DIRECT_ACQUISITION,
    )
    assert _rows(
        database,
        """
        SELECT count(*) FROM groundloop_m5_transition_call_timing
        WHERE epoch_id = %s AND contribution_kind = 'direct_transition'
        """,
        (opened.epoch_id,),
    ) == ((1,),)


@pytest.mark.parametrize("revision_delta", (-1, 2), ids=("regressed", "skipped"))
def test_direct_dispatch_revision_rejects_before_measurement_or_provider(
    d24_direct_application_db: DirectApplicationD24Database,
    monkeypatch: pytest.MonkeyPatch,
    revision_delta: int,
) -> None:
    database = d24_direct_application_db
    measurements = ControlledMeasurements()
    discovery = ControlledDirectDiscovery(database)
    bound = database.bind(
        database.connection,
        discovery=discovery,
        measurements=measurements,
    )
    plan = database.insert_plan(tag=f"dispatch-revision-{revision_delta}")
    opened = open_event(database, plan, facade=bound.facade)
    original_acquire = bound.adapter.acquire_direct_job_atomically

    def forged_acquire(
        epoch_id: int,
        expected_revision: int,
        job: LogicalJobSpec,
    ) -> M5TypedDirectAcquisitionReceipt:
        receipt = original_acquire(epoch_id, expected_revision, job)
        changed_lease = replace(
            receipt.lease,
            resulting_revision=expected_revision + revision_delta,
        )
        return replace(receipt, lease=changed_lease)

    monkeypatch.setattr(
        bound.adapter,
        "acquire_direct_job_atomically",
        forged_acquire,
    )

    with pytest.raises(ValidationError):
        bound.facade.run_pending_direct(opened.epoch_id, 1, plan, opened)

    assert measurements.transition_calls == []
    assert discovery.calls == []
    assert _rows(
        database,
        """
        SELECT count(*) FROM groundloop_m5_transition_call_timing
        WHERE epoch_id = %s AND contribution_kind = 'direct_acquisition'
        """,
        (opened.epoch_id,),
    ) == ((0,),)


@pytest.mark.parametrize(
    "mode",
    (
        "event_id_subclass",
        "chunk_index_bool",
        "direct_tuple_subclass",
        "direct_job_payload_subclass",
        "direct_scope_bool",
        "withdrawal_counter_bool",
        "requirement_roots_list",
        "root_hash_subclass",
    ),
)
def test_facade_recursive_open_boundary_rejects_without_writes(
    d24_direct_application_db: DirectApplicationD24Database,
    mode: str,
) -> None:
    database = d24_direct_application_db
    plan = database.insert_plan(tag=f"malformed-{mode}")
    inputs = open_inputs(database, plan)
    direct_payload = inputs.direct_payload
    direct_withdrawal = inputs.direct_withdrawal
    direct_roots = inputs.direct_roots
    direct_scopes = inputs.direct_scopes
    requirement_roots: object = inputs.requirement_roots
    root_hash: object = inputs.requirement_root_set_hash
    if mode == "event_id_subclass":
        event = copy(plan.event)
        object.__setattr__(event, "event_id", _StringSubclass(event.event_id))
        plan = replace(plan, event=event)
    elif mode == "chunk_index_bool":
        event = copy(plan.event)
        assert isinstance(event, InsertDocumentEvent)
        chunk = copy(event.chunks[0])
        object.__setattr__(chunk, "chunk_index", False)
        object.__setattr__(event, "chunks", (chunk,))
        unsafe_plan = copy(plan)
        object.__setattr__(unsafe_plan, "event", event)
        plan = unsafe_plan
    elif mode == "direct_tuple_subclass":
        direct = copy(plan.direct_plan)
        assert direct is not None
        object.__setattr__(
            direct,
            "inserted_chunk_version_ids",
            _TupleSubclass(direct.inserted_chunk_version_ids),
        )
        plan = replace(plan, direct_plan=direct)
    elif mode == "direct_job_payload_subclass":
        root = copy(direct_roots[0])
        object.__setattr__(root, "payload_hash", _StringSubclass(root.payload_hash))
        direct_roots = (root,)
    elif mode == "direct_scope_bool":
        scope = copy(direct_scopes[0])
        object.__setattr__(scope, "closed", 0)
        direct_scopes = (scope,)
    elif mode == "withdrawal_counter_bool":
        withdrawal_plan = copy(direct_withdrawal.plan)
        object.__setattr__(withdrawal_plan, "chunk_lookups", False)
        direct_withdrawal = replace(direct_withdrawal, plan=withdrawal_plan)
    elif mode == "requirement_roots_list":
        requirement_roots = list(inputs.requirement_roots)
    elif mode == "root_hash_subclass":
        root_hash = _StringSubclass(inputs.requirement_root_set_hash)
    else:
        raise AssertionError(f"unknown malformed mode: {mode}")

    before = database_snapshot(database.connection)
    with pytest.raises(ValidationError):
        database.facade.open_typed_event_atomically(
            plan,
            direct_payload,
            direct_withdrawal,
            inputs.requirement_withdrawal,
            direct_roots,
            direct_scopes,
            cast(tuple[M5RequirementRootDeclaration, ...], requirement_roots),
            cast(str, root_hash),
        )
    assert database.discovery.calls == []
    assert database.verifier.calls == []
    assert database_snapshot(database.connection) == before


@pytest.mark.parametrize("mode", ("admitted_list", "fallback_int", "artifact_subclass"))
def test_provider_recursive_result_rejection_stops_before_settlement(
    d24_direct_application_db: DirectApplicationD24Database,
    mode: str,
) -> None:
    database = d24_direct_application_db
    plan = database.insert_plan(tag=f"provider-{mode}")
    opened = open_event(database, plan)
    root = open_inputs(database, plan).direct_roots[0]
    valid_result = DiscoveryResult(
        root.job_id,
        f"provider-result:{mode}",
        sha(f"provider-result:{mode}"),
        (),
    )
    mutated_result = copy(valid_result)
    if mode == "admitted_list":
        object.__setattr__(mutated_result, "admitted_pairs", [])
    elif mode == "fallback_int":
        object.__setattr__(mutated_result, "fallback_satisfied", 1)
    elif mode == "artifact_subclass":
        object.__setattr__(
            mutated_result,
            "result_artifact_id",
            _StringSubclass(valid_result.result_artifact_id),
        )
    execution = object.__new__(M5PostgresDirectDiscoveryExecution)
    object.__setattr__(execution, "result", mutated_result)
    object.__setattr__(
        execution,
        "execution_disposition",
        M5ExecutionEvidenceDisposition.RETURNED,
    )
    object.__setattr__(execution, "call_work", M5RuntimeWork())
    object.__setattr__(execution, "attempt_timing", None)
    provider = _StaticDiscovery(execution)
    bound = database.bind(database.connection, discovery=provider)  # type: ignore[arg-type]

    with pytest.raises(ValidationError):
        bound.facade.run_pending_direct(opened.epoch_id, 1, plan, opened)
    assert provider.calls == 1
    assert _rows(
        database,
        """
        SELECT revision FROM groundloop_epoch WHERE epoch_id = %s
        """,
        (opened.epoch_id,),
    ) == ((2,),)
    assert _rows(
        database,
        """
        SELECT count(*) FROM groundloop_m5_attempt_execution_evidence
        WHERE epoch_id = %s AND subgraph = 'direct'
        """,
        (opened.epoch_id,),
    ) == ((0,),)


def test_nonretryable_provider_failure_persists_both_independent_reasons(
    d24_direct_application_db: DirectApplicationD24Database,
) -> None:
    database = d24_direct_application_db
    direct_reason = "opaque-provider-wire/revision-7"
    database.discovery.failure_reason = M5RunFailureReason.INVALID_ARTIFACT
    database.discovery.retryable = False
    database.discovery.direct_terminal_reason = direct_reason
    plan = database.insert_plan(tag="terminal-failure")
    opened = open_event(database, plan)
    result = database.facade.run_pending_direct(
        opened.epoch_id,
        1,
        plan,
        opened,
    )
    checked = result.selected_checked_combined_failure_receipt
    assert checked is not None
    assert checked.requested_failure_reason is M5RunFailureReason.INVALID_ARTIFACT
    assert checked.terminal_result.state is M5RunState.FAILED
    assert checked.terminal_result.failure_reason is M5RunFailureReason.INVALID_ARTIFACT
    assert result.resulting_revision == 3
    assert database.discovery.transaction_statuses == [TransactionStatus.IDLE]
    assert _rows(
        database,
        """
        SELECT job.job_state, projection.terminal_reason
        FROM groundloop_semantic_job AS job
        JOIN groundloop_m5_direct_terminal_projection AS projection
          USING (epoch_id, job_id)
        WHERE job.epoch_id = %s
        """,
        (opened.epoch_id,),
    ) == ((JobState.TERMINAL_FAILED.value, direct_reason),)
    assert _rows(
        database,
        """
        SELECT transition_kind, override_rows_written
        FROM groundloop_m4_evaluation_counter_transition
        WHERE epoch_id = %s AND to_revision = %s
        """,
        (opened.epoch_id, result.resulting_revision),
    ) == (("fail", 0),)
    assert _rows(
        database,
        """
        SELECT failure_reason FROM groundloop_m5_event_result
        WHERE epoch_id = %s
        """,
        (opened.epoch_id,),
    ) == ((M5RunFailureReason.INVALID_ARTIFACT.value,),)

    measurements = cast(ControlledMeasurements, database.bound.measurements)
    before_measurements = tuple(measurements.transition_calls)
    before = database_snapshot(database.connection)
    replay = database.facade.run_pending_direct(
        opened.epoch_id,
        result.resulting_revision,
        plan,
        opened,
    )
    assert replay.selected_terminal_acquisition_receipt is not None
    assert replay.resulting_revision == result.resulting_revision
    assert replay.call_work.is_zero
    assert tuple(measurements.transition_calls) == before_measurements
    assert database_snapshot(database.connection) == before


def test_checked_replay_rejects_changed_m4_failure_reason_on_reconnect(
    d24_direct_application_db: DirectApplicationD24Database,
) -> None:
    database = d24_direct_application_db
    plan = database.insert_plan(tag="m4-replay-reason")
    opened = open_event(database, plan)
    root = open_inputs(database, plan).direct_roots[0]
    acquisition = database.bound.adapter.acquire_direct_job_atomically(
        opened.epoch_id,
        1,
        root,
    )
    direct_reason = "opaque-provider-m4-replay-reason"
    error_hash = sha("m4-replay-reason-error")
    fail_combined = (
        database.bound.adapter.fail_typed_epoch_after_direct_terminal_failure_atomically
    )
    first = fail_combined(
        opened.epoch_id,
        2,
        root,
        acquisition.lease,
        direct_reason,
        error_hash,
        database.discovery.call_work,
        ATTEMPT_TIMING,
        M5RunFailureReason.INVALID_ARTIFACT,
        opened,
        database.discovery.call_work,
    )
    assert type(first) is M5CheckedDirectTerminalFailureReceipt

    with database.connection.transaction():
        changed = database.connection.execute(
            """
            UPDATE groundloop_m4_update
            SET manifest = jsonb_set(
                manifest,
                '{_groundloop_m4_runtime_v1,failure_reason}',
                to_jsonb(%s::text),
                false
            )
            WHERE epoch_id = %s
            """,
            (M5RunFailureReason.VERIFIER_ERROR.value, opened.epoch_id),
        ).rowcount
        assert changed == 1
        database.connection.execute("SET CONSTRAINTS ALL IMMEDIATE")

    before = database_snapshot(database.connection)
    with database.reconnect() as rebound:
        with pytest.raises(
            EventConflictError,
            match="M4 projection records another reason",
        ):
            adapter = rebound.adapter
            rebound_fail_combined = (
                adapter.fail_typed_epoch_after_direct_terminal_failure_atomically
            )
            rebound_fail_combined(
                opened.epoch_id,
                2,
                root,
                acquisition.lease,
                direct_reason,
                error_hash,
                database.discovery.call_work,
                ATTEMPT_TIMING,
                M5RunFailureReason.INVALID_ARTIFACT,
                opened,
                database.discovery.call_work,
            )
        assert database_snapshot(rebound.connection) == before


def test_terminal_replay_rejects_a_reopened_direct_job_on_reconnect(
    d24_direct_application_db: DirectApplicationD24Database,
) -> None:
    database = d24_direct_application_db
    plan = database.insert_plan(tag="reopened-terminal-job", chunk_count=2)
    opened = open_event(database, plan)
    direct_roots = open_inputs(database, plan).direct_roots
    assert len(direct_roots) == 2
    call_work = M5RuntimeWork(bytes_hashed=11)
    first = database.bound.adapter.fail_typed_epoch_with_open_receipt_atomically(
        opened.epoch_id,
        1,
        M5RunFailureReason.INVALID_ARTIFACT,
        opened,
        call_work,
    )
    assert first.state is M5RunState.FAILED
    reopened = direct_roots[0]

    with database.connection.transaction():
        database.connection.execute(
            """
            ALTER TABLE groundloop_semantic_job
            DISABLE TRIGGER groundloop_semantic_job_transition
            """
        )
        database.connection.execute(
            """
            ALTER TABLE groundloop_semantic_job
            DISABLE TRIGGER groundloop_semantic_job_open_count
            """
        )
        changed = database.connection.execute(
            """
            UPDATE groundloop_semantic_job
            SET job_state = 'declared', child_closed = false,
                child_set_hash = NULL, completion_digest = NULL,
                result_artifact_id = NULL, result_artifact_hash = NULL,
                completed_revision = NULL, completed_at = NULL
            WHERE epoch_id = %s AND job_id = %s AND job_state = 'cancelled'
            """,
            (opened.epoch_id, reopened.job_id),
        ).rowcount
        assert changed == 1
        database.connection.execute(
            """
            ALTER TABLE groundloop_semantic_job
            ENABLE TRIGGER groundloop_semantic_job_open_count
            """
        )
        database.connection.execute(
            """
            ALTER TABLE groundloop_semantic_job
            ENABLE TRIGGER groundloop_semantic_job_transition
            """
        )
        database.connection.execute(
            """
            ALTER TABLE groundloop_m5_direct_terminal_projection
            DISABLE TRIGGER groundloop_m5_direct_terminal_projection_immutable
            """
        )
        deleted = database.connection.execute(
            """
            DELETE FROM groundloop_m5_direct_terminal_projection
            WHERE epoch_id = %s AND job_id = %s
            """,
            (opened.epoch_id, reopened.job_id),
        ).rowcount
        assert deleted == 1
        database.connection.execute(
            """
            ALTER TABLE groundloop_m5_direct_terminal_projection
            ENABLE TRIGGER groundloop_m5_direct_terminal_projection_immutable
            """
        )
        database.connection.execute("SET CONSTRAINTS ALL IMMEDIATE")

    before = database_snapshot(database.connection)
    with database.reconnect() as rebound:
        with pytest.raises(
            EventConflictError,
            match="terminal plan still has open direct jobs",
        ):
            rebound.adapter.fail_typed_epoch_with_open_receipt_atomically(
                opened.epoch_id,
                1,
                M5RunFailureReason.INVALID_ARTIFACT,
                opened,
                call_work,
            )
        assert database_snapshot(rebound.connection) == before


def test_top_level_application_finishes_one_active_failure_envelope_and_telemetry(
    d24_direct_application_db: DirectApplicationD24Database,
) -> None:
    database = d24_direct_application_db
    database.discovery.failure_reason = M5RunFailureReason.INVALID_ARTIFACT
    database.discovery.direct_terminal_reason = "model-wire:invalid-output:v3"
    plan = database.insert_plan(tag="top-level-terminal")
    assembled = assemble_typed_application(database)

    result = assembled.application.run_event(plan)
    assert result.state is M5RunState.FAILED
    assert result.failure_reason is M5RunFailureReason.INVALID_ARTIFACT
    assert result.call_work == database.discovery.call_work
    assert result.event_work.direct_discovery_call_count == 1
    assert result.logical_result_hash is not None
    assert assembled.requirement_discovery.calls == []
    assert assembled.requirement_verifier.calls == []
    assert len(assembled.measurements.terminal_calls) == 1
    assert tuple(
        anchor.contribution_kind for anchor in assembled.measurements.transition_calls
    ) == (
        M5RuntimeWorkContributionKind.STRUCTURAL_OPEN,
        M5RuntimeWorkContributionKind.DIRECT_ACQUISITION,
    )
    assert database.discovery.transaction_statuses == [TransactionStatus.IDLE]
    durable_identity = (
        result.logical_result_hash,
        result.event_work,
        result.event_timing,
        result.event_timing_coverage,
    )
    assert _rows(
        database,
        """
        SELECT structural_event_id, epoch_id, terminal_logical_result_hash
        FROM groundloop_m5_postcommit_invocation_telemetry
        WHERE structural_event_id = %s
        ORDER BY invocation_id COLLATE "C"
        """,
        (plan.structural_event_id,),
    ) == ((plan.structural_event_id, result.epoch_id, result.logical_result_hash),)

    replay = assembled.application.run_event(plan)
    assert replay.state is M5RunState.REPLAYED
    assert replay.replayed_outcome is not None
    assert (
        replay.logical_result_hash,
        replay.event_work,
        replay.event_timing,
        replay.event_timing_coverage,
    ) == durable_identity
    assert replay.call_work.is_zero
    assert len(assembled.measurements.terminal_calls) == 2
    assert (
        len(
            _rows(
                database,
                """
            SELECT invocation_id
            FROM groundloop_m5_postcommit_invocation_telemetry
            WHERE structural_event_id = %s
            """,
                (plan.structural_event_id,),
            )
        )
        == 2
    )


def test_retryable_provider_failure_is_blocked_and_nonterminal(
    d24_direct_application_db: DirectApplicationD24Database,
) -> None:
    database = d24_direct_application_db
    database.discovery.failure_reason = M5RunFailureReason.RETRIEVAL_UNAVAILABLE
    database.discovery.retryable = True
    database.discovery.direct_terminal_reason = None
    plan = database.insert_plan(tag="retryable")
    opened = open_event(database, plan)
    result = database.facade.run_pending_direct(
        opened.epoch_id,
        1,
        plan,
        opened,
    )
    assert result.blocked_reason is M5RunFailureReason.RETRIEVAL_UNAVAILABLE
    assert result.resulting_revision == 3
    assert result.selected_checked_combined_failure_receipt is None
    assert database.discovery.transaction_statuses == [TransactionStatus.IDLE]
    measurements = cast(ControlledMeasurements, database.bound.measurements)
    assert tuple(
        anchor.contribution_kind for anchor in measurements.transition_calls
    ) == (
        M5RuntimeWorkContributionKind.DIRECT_ACQUISITION,
        M5RuntimeWorkContributionKind.DIRECT_ATTEMPT_EXECUTION,
    )
    assert _rows(
        database,
        """
        SELECT runtime_state, revision
        FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s
        """,
        (opened.epoch_id,),
    ) == (("semantic_pending", 3),)
    assert _rows(
        database,
        """
        SELECT job_state FROM groundloop_semantic_job WHERE epoch_id = %s
        """,
        (opened.epoch_id,),
    ) == ((JobState.RETRYABLE_FAILED.value,),)
    assert _rows(
        database,
        """
        SELECT contribution_kind, required_interval_observed
        FROM groundloop_m5_transition_call_timing
        WHERE epoch_id = %s AND contribution_kind LIKE 'direct_%%'
        ORDER BY anchor_revision, contribution_kind
        """,
        (opened.epoch_id,),
    ) == (
        (M5RuntimeWorkContributionKind.DIRECT_ACQUISITION.value, True),
        (M5RuntimeWorkContributionKind.DIRECT_ATTEMPT_EXECUTION.value, True),
    )
    assert _rows(
        database,
        """
        SELECT required_expected_count, required_observed_count,
               required_missing_count, pending_contribution_kind,
               updated_revision
        FROM groundloop_m5_runtime_timing_accumulator
        WHERE epoch_id = %s
        """,
        (opened.epoch_id,),
    ) == ((4, 3, 1, None, result.resulting_revision),)


@pytest.mark.parametrize(
    "mode",
    (
        "first_regressed",
        "first_skipped",
        "replay_regressed",
        "first_without_anchor",
        "replay_with_anchor",
        "evidence",
    ),
)
def test_retryable_outcome_revision_and_anchor_reject_before_measurement(
    d24_direct_application_db: DirectApplicationD24Database,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    database = d24_direct_application_db
    discovery = ControlledDirectDiscovery(
        database,
        failure_reason=M5RunFailureReason.RETRIEVAL_UNAVAILABLE,
        retryable=True,
    )
    measurements = ControlledMeasurements()
    bound = database.bind(
        database.connection,
        discovery=discovery,
        measurements=measurements,
    )
    plan = database.insert_plan(tag=f"retryable-outcome-{mode}")
    opened = open_event(database, plan, facade=bound.facade)
    original_failure = bound.adapter.mark_direct_retryable_failure_atomically

    def forged_failure(*args: Any, **kwargs: Any) -> Any:
        input_revision = cast(int, args[1])
        first = original_failure(*args, **kwargs)
        if mode in {"replay_regressed", "replay_with_anchor"}:
            outcome = original_failure(*args, **kwargs)
            if mode == "replay_regressed":
                object.__setattr__(outcome, "resulting_revision", input_revision - 1)
            else:
                object.__setattr__(
                    outcome, "transition_anchor", first.transition_anchor
                )
            return outcome
        if mode == "first_without_anchor":
            object.__setattr__(first, "transition_anchor", None)
            return first
        if mode == "evidence":
            object.__setattr__(
                first.receipt,
                "execution_evidence_digest",
                sha("forged-retryable-execution-evidence"),
            )
            return first
        changed_revision = (
            input_revision - 1 if mode == "first_regressed" else input_revision + 2
        )
        anchor = first.transition_anchor
        assert anchor is not None
        changed_anchor = replace(anchor, anchor_revision=changed_revision)
        object.__setattr__(first, "resulting_revision", changed_revision)
        object.__setattr__(first, "transition_anchor", changed_anchor)
        return first

    wrapper_calls = 0

    def unexpected_wrapper(*_: Any, **__: Any) -> None:
        nonlocal wrapper_calls
        wrapper_calls += 1
        raise AssertionError("invalid retryable outcome reached wrapper")

    monkeypatch.setattr(
        bound.adapter,
        "mark_direct_retryable_failure_atomically",
        forged_failure,
    )
    monkeypatch.setattr(
        postgres_direct_application,
        "M5DirectExecutionReceipt",
        unexpected_wrapper,
    )

    with pytest.raises(ValidationError):
        bound.facade.run_pending_direct(opened.epoch_id, 1, plan, opened)

    assert wrapper_calls == 0
    assert tuple(
        anchor.contribution_kind for anchor in measurements.transition_calls
    ) == (M5RuntimeWorkContributionKind.DIRECT_ACQUISITION,)
    assert _rows(
        database,
        """
        SELECT count(*) FROM groundloop_m5_transition_call_timing
        WHERE epoch_id = %s AND contribution_kind = 'direct_attempt_execution'
        """,
        (opened.epoch_id,),
    ) == ((0,),)


def test_live_lease_and_expired_preterminal_verifier_are_wip(
    d24_direct_application_db: DirectApplicationD24Database,
) -> None:
    database = d24_direct_application_db
    plan = database.insert_plan(tag="wip-branches")
    opened = open_event(database, plan)
    root = open_inputs(database, plan).direct_roots[0]
    acquisition = database.bound.adapter.acquire_direct_job_atomically(
        opened.epoch_id,
        1,
        root,
    )
    assert acquisition.attempt is not None
    assert acquisition.lease.resulting_revision == 2
    before_live = database_snapshot(database.connection)
    measurements = cast(ControlledMeasurements, database.bound.measurements)
    before_measurements = tuple(measurements.transition_calls)
    live = database.facade.run_pending_direct(
        opened.epoch_id,
        2,
        plan,
        opened,
    )
    assert live.blocked_reason is M5RunFailureReason.WORK_IN_PROGRESS
    assert live.resulting_revision == 2
    assert database.discovery.calls == []
    assert tuple(measurements.transition_calls) == before_measurements
    assert database_snapshot(database.connection) == before_live


def test_live_second_root_preserves_prior_work_and_adds_no_settlement(
    d24_direct_application_db: DirectApplicationD24Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = d24_direct_application_db
    plan = database.insert_plan(tag="live-after-work", chunk_count=2)
    opened = open_event(database, plan)
    roots = open_inputs(database, plan).direct_roots
    assert len(roots) == 2
    first, second = roots
    live = database.bound.adapter.acquire_direct_job_atomically(
        opened.epoch_id,
        1,
        second,
    )
    assert live.lease.should_execute
    assert live.lease.resulting_revision == 2
    assert live.lease.attempt_id is not None

    original_acquire = database.bound.adapter.acquire_direct_job_atomically
    live_snapshots: list[
        tuple[
            tuple[tuple[str, tuple[tuple[str, str], ...]], ...],
            tuple[tuple[str, tuple[tuple[str, str], ...]], ...],
        ]
    ] = []

    def recording_acquire(
        epoch_id: int,
        expected_revision: int,
        job: LogicalJobSpec,
    ) -> M5TypedDirectAcquisitionReceipt:
        if job.job_id != second.job_id:
            return original_acquire(epoch_id, expected_revision, job)
        before = database_snapshot(database.connection)
        receipt = original_acquire(epoch_id, expected_revision, job)
        after = database_snapshot(database.connection)
        live_snapshots.append((before, after))
        return receipt

    monkeypatch.setattr(
        database.bound.adapter,
        "acquire_direct_job_atomically",
        recording_acquire,
    )
    result = database.facade.run_pending_direct(
        opened.epoch_id,
        2,
        plan,
        opened,
    )
    assert result.blocked_reason is M5RunFailureReason.WORK_IN_PROGRESS
    assert result.resulting_revision == 3
    assert result.call_work == database.discovery.call_work
    assert database.discovery.calls == [first.job_id]
    assert live_snapshots and live_snapshots[0][0] == live_snapshots[0][1]
    assert _rows(
        database,
        """
        SELECT count(*)
        FROM groundloop_m5_attempt_execution_evidence
        WHERE epoch_id = %s AND subgraph = 'direct' AND attempt_id = %s
        """,
        (opened.epoch_id, live.lease.attempt_id),
    ) == ((0,),)
    assert _rows(
        database,
        """
        SELECT count(*)
        FROM groundloop_m5_runtime_timing_contribution
        WHERE epoch_id = %s AND subgraph = 'direct' AND attempt_id = %s
        """,
        (opened.epoch_id, live.lease.attempt_id),
    ) == ((0,),)


def test_expired_preterminal_verifier_return_is_wip_without_observation(
    d24_direct_application_db: DirectApplicationD24Database,
) -> None:
    database = d24_direct_application_db
    database.discovery.include_pair = True
    slow = _SlowVerifier(database)
    bound = database.bind(database.connection, verifier=slow)
    plan = database.insert_plan(tag="expired-verifier")
    opened = open_event(database, plan, facade=bound.facade)
    result = bound.facade.run_pending_direct(
        opened.epoch_id,
        1,
        plan,
        opened,
    )
    assert result.blocked_reason is M5RunFailureReason.WORK_IN_PROGRESS
    assert result.resulting_revision == 5
    assert len(slow.successors) == 1
    assert slow.transaction_statuses == [TransactionStatus.IDLE]
    assert _rows(
        database,
        """
        SELECT count(*) FROM groundloop_m5_expired_attempt_return
        WHERE epoch_id = %s AND subgraph = 'direct'
        """,
        (opened.epoch_id,),
    ) == ((1,),)
    assert _rows(
        database,
        """
            SELECT count(*) FROM groundloop_semantic_observation
            WHERE observation_id LIKE %s
            """,
        ("direct-observation:%",),
    ) == ((0,),)


def test_expired_preterminal_discovery_return_is_wip_without_m4_completion(
    d24_direct_application_db: DirectApplicationD24Database,
) -> None:
    database = d24_direct_application_db
    slow = _SlowDiscovery(database)
    bound = database.bind(database.connection, discovery=slow)
    plan = database.insert_plan(tag="expired-discovery")
    opened = open_event(database, plan, facade=bound.facade)
    result = bound.facade.run_pending_direct(
        opened.epoch_id,
        1,
        plan,
        opened,
    )
    assert result.blocked_reason is M5RunFailureReason.WORK_IN_PROGRESS
    assert result.resulting_revision == 3
    assert len(slow.successors) == 1
    assert slow.transaction_statuses == [TransactionStatus.IDLE]
    assert _rows(
        database,
        """
        SELECT job_state, completed_revision
        FROM groundloop_semantic_job
        WHERE epoch_id = %s
        """,
        (opened.epoch_id,),
    ) == ((JobState.RUNNING.value, None),)
    assert _rows(
        database,
        """
        SELECT count(*) FROM groundloop_m5_expired_attempt_return
        WHERE epoch_id = %s AND subgraph = 'direct'
        """,
        (opened.epoch_id,),
    ) == ((1,),)


def test_top_level_application_projects_expired_direct_return_as_blocked(
    d24_direct_application_db: DirectApplicationD24Database,
) -> None:
    database = d24_direct_application_db
    slow = _SlowDiscovery(database)
    bound = database.bind(database.connection, discovery=slow)
    assembled = assemble_typed_application(database, facade=bound.facade)
    plan = database.insert_plan(tag="top-level-expired-discovery")
    result = assembled.application.run_event(plan)
    assert result.state is M5RunState.BLOCKED
    assert result.failure_reason is M5RunFailureReason.WORK_IN_PROGRESS
    assert result.logical_result_hash is None
    assert result.call_work == slow.call_work
    assert slow.transaction_statuses == [TransactionStatus.IDLE]
    assert assembled.measurements.terminal_calls == []
    assert tuple(
        anchor.contribution_kind for anchor in assembled.measurements.transition_calls
    ) == (
        M5RuntimeWorkContributionKind.STRUCTURAL_OPEN,
        M5RuntimeWorkContributionKind.DIRECT_ACQUISITION,
        M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN,
    )


@pytest.mark.parametrize("expired_provider", ("discovery", "verifier"))
def test_top_level_expired_preterminal_preserves_prior_direct_work_only(
    d24_direct_application_db: DirectApplicationD24Database,
    expired_provider: str,
) -> None:
    database = d24_direct_application_db
    discovery: ControlledDirectDiscovery
    verifier: ControlledDirectVerifier
    successor_receipts: list[M5TypedDirectAcquisitionReceipt]
    if expired_provider == "discovery":
        discovery = _SlowDiscovery(database, takeover_call=2)
        verifier = ControlledDirectVerifier(database)
        expected_work = M5RuntimeWork(
            direct_discovery_call_count=2,
            embedding_model_call_count=2,
        )
        expected_direct_attempts = 2
        successor_receipts = discovery.successors
    else:
        discovery = ControlledDirectDiscovery(database, include_pair=True)
        verifier = _SlowVerifier(database)
        expected_work = M5RuntimeWork(
            direct_discovery_call_count=2,
            direct_verifier_call_count=1,
            embedding_model_call_count=2,
            verifier_model_call_count=1,
            verifier_input_token_count=5,
            verifier_output_token_count=1,
        )
        expected_direct_attempts = 3
        successor_receipts = verifier.successors
    bound = database.bind(
        database.connection,
        discovery=discovery,
        verifier=verifier,
    )
    assembled = assemble_typed_application(database, facade=bound.facade)
    plan = database.insert_plan(
        tag=f"top-level-expired-{expired_provider}-after-work",
        chunk_count=2,
    )

    result = assembled.application.run_event(plan)

    assert result.state is M5RunState.BLOCKED
    assert result.failure_reason is M5RunFailureReason.WORK_IN_PROGRESS
    assert result.logical_result_hash is None
    assert result.call_work == expected_work
    assert result.event_work.direct_discovery_call_count == 2
    assert result.event_work.direct_verifier_call_count == int(
        expired_provider == "verifier"
    )
    assert result.event_work.requirement_late_attempt_artifact_count == 1
    assert all(
        getattr(result.event_work, name) == 0
        for name in M5RuntimeWork.counter_names()
        if name.startswith("requirement_")
        and name != "requirement_late_attempt_artifact_count"
    )
    assert all(
        status is TransactionStatus.IDLE
        for status in (*discovery.transaction_statuses, *verifier.transaction_statuses)
    )
    assert len(discovery.calls) == 2
    assert len(verifier.calls) == int(expired_provider == "verifier")
    assert len(successor_receipts) == 1
    successor = successor_receipts[0]
    assert successor.attempt is not None
    assert assembled.requirement_discovery.calls == []
    assert assembled.requirement_verifier.calls == []
    assert assembled.measurements.terminal_calls == []
    expected_transition_kinds = [
        M5RuntimeWorkContributionKind.STRUCTURAL_OPEN,
        M5RuntimeWorkContributionKind.DIRECT_ACQUISITION,
        M5RuntimeWorkContributionKind.DIRECT_ACQUISITION,
        M5RuntimeWorkContributionKind.DIRECT_TRANSITION,
    ]
    if expired_provider == "verifier":
        expected_transition_kinds.extend(
            (
                M5RuntimeWorkContributionKind.DIRECT_TRANSITION,
                M5RuntimeWorkContributionKind.DIRECT_ACQUISITION,
                M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN,
            )
        )
    else:
        expected_transition_kinds.append(
            M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN
        )
    assert [
        anchor.contribution_kind for anchor in assembled.measurements.transition_calls
    ] == expected_transition_kinds
    assert _rows(
        database,
        """
        SELECT count(*)
        FROM groundloop_m5_job_attempt AS attempt
        JOIN groundloop_m5_semantic_job AS job
          ON job.logical_job_id = attempt.logical_job_id
        WHERE job.epoch_id = %s
        """,
        (result.epoch_id,),
    ) == ((0,),)
    assert _rows(
        database,
        """
        SELECT
            count(*) FILTER (WHERE subgraph = 'direct'),
            count(*) FILTER (WHERE subgraph = 'requirement')
        FROM groundloop_m5_attempt_execution_evidence
        WHERE epoch_id = %s
        """,
        (result.epoch_id,),
    ) == ((expected_direct_attempts, 0),)
    assert _rows(
        database,
        """
        SELECT
            count(*) FILTER (WHERE subgraph = 'direct'),
            count(*) FILTER (WHERE subgraph = 'requirement')
        FROM groundloop_m5_runtime_timing_contribution
        WHERE epoch_id = %s
        """,
        (result.epoch_id,),
    ) == ((expected_direct_attempts, 0),)
    assert _rows(
        database,
        """
        SELECT count(*) FROM groundloop_m5_attempt_execution_evidence
        WHERE epoch_id = %s AND subgraph = 'direct' AND attempt_id = %s
        """,
        (result.epoch_id, successor.attempt.attempt_id),
    ) == ((0,),)
    assert _rows(
        database,
        """
        SELECT count(*) FROM groundloop_m5_runtime_timing_contribution
        WHERE epoch_id = %s AND subgraph = 'direct' AND attempt_id = %s
        """,
        (result.epoch_id, successor.attempt.attempt_id),
    ) == ((0,),)
    assert _rows(
        database,
        """
        SELECT count(*) FROM groundloop_m5_event_result
        WHERE structural_event_id = %s
        """,
        (plan.structural_event_id,),
    ) == ((0,),)
    assert _rows(
        database,
        """
        SELECT count(*) FROM groundloop_m5_postcommit_invocation_telemetry
        WHERE structural_event_id = %s
        """,
        (plan.structural_event_id,),
    ) == ((0,),)


def test_unsatisfied_frontier_is_retryable_blocked(
    d24_direct_application_db: DirectApplicationD24Database,
) -> None:
    database = d24_direct_application_db
    database.discovery.fallback_satisfied = False
    observation_id = "d24-direct-unsatisfied-frontier-observation"
    with database.connection.transaction():
        insert_observation(
            database.connection,
            observation_id=observation_id,
            subject_kind="claim",
            subject_id=database.base.base.claim_ids[0],
            chunk_id=database.base.base.chunk_ids[0],
            produced_epoch=database.base.base.epoch_id,
            task_type="verify_claim_v1",
            scores=(0.1, 0.1, 0.8),
        )
        install_current_currency(
            database.connection,
            observation_id=observation_id,
            subject_kind="claim",
            subject_id=database.base.base.claim_ids[0],
            chunk_id=database.base.base.chunk_ids[0],
            task_type="verify_claim_v1",
            epoch_id=database.base.base.epoch_id,
            publish=True,
        )
    plan = database.delete_plan(tag="unsatisfied-frontier")
    inputs = open_inputs(database, plan)
    assert inputs.direct_roots
    assert all(root.kind is JobKind.FRONTIER_RETRIEVE for root in inputs.direct_roots)
    opened = open_event(database, plan)
    result = database.facade.run_pending_direct(
        opened.epoch_id,
        1,
        plan,
        opened,
    )
    assert result.blocked_reason is M5RunFailureReason.RETRIEVAL_UNAVAILABLE
    assert result.selected_checked_combined_failure_receipt is None
    expected_result_hash = sha(
        f"direct-discovery:{opened.epoch_id}:{inputs.direct_roots[0].job_id}"
    )
    assert _rows(
        database,
        """
        SELECT btrim(result_or_error_hash)
        FROM groundloop_m5_attempt_execution_evidence
        WHERE epoch_id = %s AND subgraph = 'direct'
        ORDER BY attempt_id COLLATE "C"
        """,
        (opened.epoch_id,),
    ) == ((expected_result_hash,),)


@pytest.mark.parametrize(
    "mode",
    (
        "tuple_subclass",
        "outer_subclass",
        "nested_scope_primitive",
        "nested_job_primitive",
        "wrong_reverse_coverage",
        "wrong_hash",
        "fallback_dict_subclass",
        "fallback_missing",
        "fallback_extra",
        "fallback_wrong_key",
        "fallback_nonbool",
    ),
)
def test_raw_direct_root_and_fallback_boundary_is_pre_callback_zero_write(
    d24_direct_application_db: DirectApplicationD24Database,
    mode: str,
) -> None:
    database = d24_direct_application_db
    plan = database.insert_plan(tag=f"raw-root-{mode}")
    canonical = open_inputs(database, plan)
    requirement_id = database.base.published_group.requirements[
        0
    ].requirement_version_id
    forward_scope = M5DiscoveryScopeContract.build(
        direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
        requirement_version_id=requirement_id,
        inserted_chunk_version_id=None,
        candidate_policy_id=plan.candidate_policy_id,
        requirement_registry_snapshot_digest=(
            plan.requirement_registry_snapshot.requirement_registry_snapshot_digest
        ),
        active_chunk_snapshot_digest=(
            plan.active_chunk_snapshot.active_chunk_snapshot_digest
        ),
    )
    forward = M5RequirementRootDeclaration(
        forward_scope,
        M5LogicalJobSpec.build(
            structural_event_id=plan.structural_event_id,
            job_kind=M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL,
            manifest=database.base.manifest,
            scope=forward_scope,
        ),
    )
    roots: object = tuple(
        sorted(
            (*canonical.requirement_roots, forward),
            key=lambda item: item.job.logical_job_id,
        )
    )
    fallback: object = {forward.job.logical_job_id: False}
    root_hash: object = digests.requirement_root_set_digest(
        item.job.logical_job_id
        for item in cast(tuple[M5RequirementRootDeclaration, ...], roots)
    )
    if mode == "tuple_subclass":
        roots = _TupleSubclass(cast(tuple[object, ...], roots))
    elif mode == "outer_subclass":

        class _DeclarationSubclass(M5RequirementRootDeclaration):
            pass

        concrete = cast(tuple[M5RequirementRootDeclaration, ...], roots)
        roots = (
            _DeclarationSubclass(concrete[0].scope, concrete[0].job),
            *concrete[1:],
        )
    elif mode == "nested_scope_primitive":
        concrete = cast(tuple[M5RequirementRootDeclaration, ...], roots)
        mutated_scope = copy(concrete[0].scope)
        object.__setattr__(
            mutated_scope,
            "candidate_policy_id",
            _StringSubclass(mutated_scope.candidate_policy_id),
        )
        roots = (replace(concrete[0], scope=mutated_scope), *concrete[1:])
    elif mode == "nested_job_primitive":
        concrete = cast(tuple[M5RequirementRootDeclaration, ...], roots)
        mutated_job = copy(concrete[0].job)
        object.__setattr__(
            mutated_job,
            "payload_hash",
            _StringSubclass(mutated_job.payload_hash),
        )
        roots = (replace(concrete[0], job=mutated_job), *concrete[1:])
    elif mode == "wrong_reverse_coverage":
        roots = (forward,)
        root_hash = digests.requirement_root_set_digest((forward.job.logical_job_id,))
    elif mode == "wrong_hash":
        root_hash = sha("wrong-direct-root-hash")
    elif mode == "fallback_dict_subclass":
        fallback = _DictSubclass(cast(dict[str, bool], fallback))
    elif mode == "fallback_missing":
        fallback = {}
    elif mode == "fallback_extra":
        fallback = {**cast(dict[str, bool], fallback), sha("extra-root"): False}
    elif mode == "fallback_wrong_key":
        fallback = {sha("wrong-forward-root"): False}
    elif mode == "fallback_nonbool":
        fallback = {forward.job.logical_job_id: 0}
    else:
        raise AssertionError(f"unknown root mode: {mode}")

    callback_calls = 0

    def unexpected_stage(_: Any) -> tuple[OpenEventReceipt, M5RuntimeWork]:
        nonlocal callback_calls
        callback_calls += 1
        raise AssertionError("malformed root boundary reached direct callback")

    before = database_snapshot(database.connection)
    with pytest.raises(ValidationError):
        database.bound.store.open_typed_direct_event_atomically(
            plan,
            requirement_roots=cast(tuple[M5RequirementRootDeclaration, ...], roots),
            requirement_root_set_hash=cast(str, root_hash),
            direct_stage=unexpected_stage,
            recovery_operational_config=database.base.operational_config,
            recovery_root_fallback_required=cast(dict[str, bool], fallback),
        )
    assert callback_calls == 0
    assert database_snapshot(database.connection) == before


def test_held_receipt_unsafe_primitive_is_rejected_before_acquisition(
    d24_direct_application_db: DirectApplicationD24Database,
) -> None:
    database = d24_direct_application_db
    plan = database.insert_plan(tag="unsafe-receipt")
    opened = open_event(database, plan)
    malformed = copy(opened)
    object.__setattr__(malformed, "replayed", 0)
    before = database_snapshot(database.connection)
    with pytest.raises(ValidationError, match="held nonterminal receipt"):
        database.facade.run_pending_direct(
            opened.epoch_id,
            1,
            plan,
            malformed,
        )
    assert database.discovery.calls == []
    assert database_snapshot(database.connection) == before


def test_provider_work_and_timing_unsafe_primitives_are_rejected(
    d24_direct_application_db: DirectApplicationD24Database,
) -> None:
    database = d24_direct_application_db
    plan = database.insert_plan(tag="unsafe-provider-accounting")
    opened = open_event(database, plan)
    work = copy(M5RuntimeWork(direct_discovery_call_count=1))
    object.__setattr__(work, "direct_discovery_call_count", True)
    timing = copy(ATTEMPT_TIMING)
    object.__setattr__(timing, "neural_wall_ns", False)
    valid = DiscoveryResult(
        open_inputs(database, plan).direct_roots[0].job_id,
        "unsafe-provider-accounting",
        sha("unsafe-provider-accounting"),
        (),
    )
    execution = object.__new__(M5PostgresDirectDiscoveryExecution)
    object.__setattr__(execution, "result", valid)
    object.__setattr__(
        execution,
        "execution_disposition",
        M5ExecutionEvidenceDisposition.RETURNED,
    )
    object.__setattr__(execution, "call_work", work)
    object.__setattr__(execution, "attempt_timing", timing)
    provider = _StaticDiscovery(execution)
    bound = database.bind(database.connection, discovery=provider)  # type: ignore[arg-type]
    with pytest.raises(ValidationError, match="nonexact counter"):
        bound.facade.run_pending_direct(opened.epoch_id, 1, plan, opened)
    assert _rows(
        database,
        """
        SELECT count(*) FROM groundloop_m5_attempt_execution_evidence
        WHERE epoch_id = %s
        """,
        (opened.epoch_id,),
    ) == ((0,),)


def test_generic_external_failure_from_direct_provider_has_no_authority(
    d24_direct_application_db: DirectApplicationD24Database,
) -> None:
    database = d24_direct_application_db
    snapshots_at_provider: list[
        tuple[tuple[str, tuple[tuple[str, str], ...]], ...]
    ] = []

    class _GenericFailureDiscovery(ControlledDirectDiscovery):
        def discover_direct(self, *args: Any) -> M5PostgresDirectDiscoveryExecution:
            connection = self.database.connection
            status = connection.info.transaction_status
            self.transaction_statuses.append(status)
            if status is not TransactionStatus.IDLE:
                raise AssertionError(
                    "generic direct failure ran inside a database transaction"
                )
            acquisition = cast(M5TypedDirectAcquisitionReceipt, args[1])
            self.calls.append(acquisition.job.job_id)
            snapshots_at_provider.append(database_snapshot(connection))
            raise M5ExternalWorkFailure(
                M5RunFailureReason.INVALID_ARTIFACT,
                retryable=False,
                call_work=self.call_work,
                attempt_timing=self.attempt_timing,
                error_hash=sha("generic-direct-provider-failure"),
            )

    provider = _GenericFailureDiscovery(database)
    bound = database.bind(database.connection, discovery=provider)
    plan = database.insert_plan(tag="generic-direct-failure")
    opened = open_event(database, plan, facade=bound.facade)
    with pytest.raises(
        ValidationError, match="direct provider raised a generic external failure"
    ):
        bound.facade.run_pending_direct(opened.epoch_id, 1, plan, opened)
    assert provider.transaction_statuses == [TransactionStatus.IDLE]
    assert len(snapshots_at_provider) == 1
    assert database_snapshot(database.connection) == snapshots_at_provider[0]
    assert _rows(
        database,
        """
        SELECT revision FROM groundloop_epoch WHERE epoch_id = %s
        """,
        (opened.epoch_id,),
    ) == ((2,),)
    assert _rows(
        database,
        """
        SELECT job_state FROM groundloop_semantic_job WHERE epoch_id = %s
        """,
        (opened.epoch_id,),
    ) == ((JobState.RUNNING.value,),)
    assert _rows(
        database,
        """
        SELECT count(*) FROM groundloop_m5_attempt_execution_evidence
        WHERE epoch_id = %s
        """,
        (opened.epoch_id,),
    ) == ((0,),)
    assert _rows(
        database,
        """
        SELECT count(*) FROM groundloop_m5_event_result WHERE epoch_id = %s
        """,
        (opened.epoch_id,),
    ) == ((0,),)
    assert _rows(
        database,
        """
        SELECT count(*) FROM groundloop_m5_postcommit_invocation_telemetry
        WHERE epoch_id = %s
        """,
        (opened.epoch_id,),
    ) == ((0,),)


@pytest.mark.parametrize(
    ("mode", "message"),
    (
        ("epoch", "changed its epoch"),
        ("job", "changed its target job"),
        ("attempt", "changed its acquired attempt"),
        ("deadline", "changed its lease provenance"),
        ("dispatch", "changed its lease provenance"),
        ("terminal_failed", "lacks the epoch-failed cancellation"),
        ("revision_regressed", "changed its terminal revision"),
        ("revision_skipped", "changed its terminal revision"),
    ),
)
def test_terminal_loser_receipt_is_bound_before_wrapper_and_telemetry(
    d24_direct_application_db: DirectApplicationD24Database,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    message: str,
) -> None:
    database = d24_direct_application_db
    database.discovery.failure_reason = M5RunFailureReason.INVALID_ARTIFACT
    database.discovery.retryable = False
    database.discovery.direct_terminal_reason = "forged-loser-provider-reason"
    plan = database.insert_plan(
        tag=f"forged-loser-{mode}",
        chunk_count=2 if mode == "job" else 1,
    )
    opened = open_event(database, plan)
    roots = open_inputs(database, plan).direct_roots
    adapter = database.bound.adapter
    acquisitions: list[M5TypedDirectAcquisitionReceipt] = []
    snapshots_at_combined: list[
        tuple[tuple[str, tuple[tuple[str, str], ...]], ...]
    ] = []
    original_acquire = adapter.acquire_direct_job_atomically

    def recording_acquire(
        epoch_id: int,
        expected_revision: int,
        job: LogicalJobSpec,
    ) -> M5TypedDirectAcquisitionReceipt:
        receipt = original_acquire(epoch_id, expected_revision, job)
        acquisitions.append(receipt)
        return receipt

    def forged_combined(*_: Any) -> M5TypedDirectAcquisitionReceipt:
        assert len(acquisitions) == 1
        snapshots_at_combined.append(database_snapshot(database.connection))
        alternate_job = next(
            (job for job in roots if job != acquisitions[0].job),
            None,
        )
        return _forged_terminal_loser(
            acquisitions[0],
            mode=mode,
            alternate_job=alternate_job,
        )

    wrapper_calls = 0

    def unexpected_wrapper(*_: Any, **__: Any) -> None:
        nonlocal wrapper_calls
        wrapper_calls += 1
        raise AssertionError("invalid terminal loser reached the public wrapper")

    monkeypatch.setattr(adapter, "acquire_direct_job_atomically", recording_acquire)
    monkeypatch.setattr(
        adapter,
        "fail_typed_epoch_after_direct_terminal_failure_atomically",
        forged_combined,
    )
    monkeypatch.setattr(
        postgres_direct_application,
        "M5DirectExecutionReceipt",
        unexpected_wrapper,
    )

    with pytest.raises(ValidationError, match=message):
        database.facade.run_pending_direct(opened.epoch_id, 1, plan, opened)
    assert wrapper_calls == 0
    assert database.discovery.transaction_statuses == [TransactionStatus.IDLE]
    assert len(snapshots_at_combined) == 1
    assert database_snapshot(database.connection) == snapshots_at_combined[0]
    assert _rows(
        database,
        """
        SELECT count(*) FROM groundloop_m5_postcommit_invocation_telemetry
        WHERE epoch_id = %s
        """,
        (opened.epoch_id,),
    ) == ((0,),)


@pytest.mark.parametrize(
    ("mode", "message"),
    (
        ("epoch", "changed its acquired attempt"),
        ("job", "changed its acquired attempt"),
        ("attempt", "changed its acquired attempt"),
        ("reason", "changed its requested reason"),
        ("event", "changed its event provenance"),
        ("payload", "changed its event provenance"),
        ("held_object", "changed its held open receipt"),
        ("evidence", "changed its execution evidence"),
        ("first_regressed", "changed its terminal revision"),
        ("first_skipped", "changed its terminal revision"),
        ("replay_regressed", "changed its terminal revision"),
        ("replay_skipped", "changed its terminal revision"),
    ),
)
def test_checked_failure_receipt_is_bound_before_wrapper_and_telemetry(
    d24_direct_application_db: DirectApplicationD24Database,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    message: str,
) -> None:
    database = d24_direct_application_db
    database.discovery.failure_reason = M5RunFailureReason.INVALID_ARTIFACT
    database.discovery.retryable = False
    database.discovery.direct_terminal_reason = "forged-checked-provider-reason"
    plan = database.insert_plan(tag=f"forged-checked-{mode}")
    opened = open_event(database, plan)
    adapter = database.bound.adapter
    acquisitions: list[M5TypedDirectAcquisitionReceipt] = []
    snapshots_at_combined: list[
        tuple[tuple[str, tuple[tuple[str, str], ...]], ...]
    ] = []
    original_acquire = adapter.acquire_direct_job_atomically

    def recording_acquire(
        epoch_id: int,
        expected_revision: int,
        job: LogicalJobSpec,
    ) -> M5TypedDirectAcquisitionReceipt:
        receipt = original_acquire(epoch_id, expected_revision, job)
        acquisitions.append(receipt)
        return receipt

    def forged_combined(*args: Any) -> M5CheckedDirectTerminalFailureReceipt:
        assert len(acquisitions) == 1
        snapshots_at_combined.append(database_snapshot(database.connection))
        return _forged_checked_failure(
            acquisitions[0],
            event=plan,
            open_receipt=opened,
            error_hash=cast(str, args[5]),
            attempt_work=cast(M5RuntimeWork, args[6]),
            attempt_timing=cast(M5RuntimeTiming | None, args[7]),
            call_work=cast(M5RuntimeWork, args[-1]),
            mode=mode,
        )

    wrapper_calls = 0

    def unexpected_wrapper(*_: Any, **__: Any) -> None:
        nonlocal wrapper_calls
        wrapper_calls += 1
        raise AssertionError("invalid checked failure reached the public wrapper")

    monkeypatch.setattr(adapter, "acquire_direct_job_atomically", recording_acquire)
    monkeypatch.setattr(
        adapter,
        "fail_typed_epoch_after_direct_terminal_failure_atomically",
        forged_combined,
    )
    monkeypatch.setattr(
        postgres_direct_application,
        "M5DirectExecutionReceipt",
        unexpected_wrapper,
    )

    with pytest.raises(ValidationError, match=message):
        database.facade.run_pending_direct(opened.epoch_id, 1, plan, opened)
    assert wrapper_calls == 0
    assert database.discovery.transaction_statuses == [TransactionStatus.IDLE]
    assert len(snapshots_at_combined) == 1
    assert database_snapshot(database.connection) == snapshots_at_combined[0]
    assert _rows(
        database,
        """
        SELECT count(*) FROM groundloop_m5_postcommit_invocation_telemetry
        WHERE epoch_id = %s
        """,
        (opened.epoch_id,),
    ) == ((0,),)


@pytest.mark.parametrize("branch", ("generic", "checked_non_target_replay"))
def test_new_epoch_failed_projection_rejects_an_arbitrary_cancel_reason(
    d24_direct_application_db: DirectApplicationD24Database,
    branch: str,
) -> None:
    database = d24_direct_application_db
    plan = database.insert_plan(tag=f"projection-reason-{branch}", chunk_count=2)
    opened = open_event(database, plan)
    if branch == "generic":
        generic_terminal = (
            database.bound.adapter.fail_typed_epoch_with_open_receipt_atomically(
                opened.epoch_id,
                1,
                M5RunFailureReason.INVALID_ARTIFACT,
                opened,
                M5RuntimeWork(bytes_hashed=7),
            )
        )
        assert generic_terminal.state is M5RunState.FAILED
        resulting_revision = database.facade.current_revision(opened.epoch_id)
    elif branch == "checked_non_target_replay":
        target = open_inputs(database, plan).direct_roots[0]
        acquisition = database.bound.adapter.acquire_direct_job_atomically(
            opened.epoch_id,
            1,
            target,
        )
        adapter = database.bound.adapter
        fail_combined = (
            adapter.fail_typed_epoch_after_direct_terminal_failure_atomically
        )
        checked_terminal = fail_combined(
            opened.epoch_id,
            2,
            target,
            acquisition.lease,
            "arbitrary-target-provider-reason",
            sha("projection-checked-error"),
            database.discovery.call_work,
            ATTEMPT_TIMING,
            M5RunFailureReason.INVALID_ARTIFACT,
            opened,
            database.discovery.call_work,
        )
        assert isinstance(checked_terminal, M5CheckedDirectTerminalFailureReceipt)
        replay = fail_combined(
            opened.epoch_id,
            2,
            target,
            acquisition.lease,
            "arbitrary-target-provider-reason",
            sha("projection-checked-error"),
            database.discovery.call_work,
            ATTEMPT_TIMING,
            M5RunFailureReason.INVALID_ARTIFACT,
            opened,
            database.discovery.call_work,
        )
        assert isinstance(replay, M5CheckedDirectTerminalFailureReceipt)
        assert replay.direct_failure == checked_terminal.direct_failure
        assert replay.requested_failure_reason is M5RunFailureReason.INVALID_ARTIFACT
        assert replay.resulting_revision == checked_terminal.resulting_revision
        assert replay.terminal_result.state is M5RunState.REPLAYED
        assert replay.terminal_result.replayed_outcome is M5ReplayedOutcome.FAILED
        resulting_revision = checked_terminal.resulting_revision
    else:
        raise AssertionError(f"unknown projection branch: {branch}")

    before = database_snapshot(database.connection)
    ports = database.bound.adapter._ports
    with database.connection.transaction():
        with database.connection.cursor() as cursor:
            locks = ports._lock_typed_direct_epoch_failure_jobs_local(
                cursor,
                opened.epoch_id,
                resulting_revision - 1,
            )
            m4_plan = ports._lock_typed_direct_epoch_failure_details_local(
                cursor,
                locks,
                None,
            )
            projections = postgres_direct_recovery._lock_all_terminal_projections(
                cursor,
                opened.epoch_id,
            )
            newly_cancelled = tuple(
                image
                for image in m4_plan.job_locks.jobs
                if image.state is JobState.CANCELLED
                and image.completed_revision == resulting_revision
            )
            assert newly_cancelled
            corrupted = newly_cancelled[0]
            projections[corrupted.job.job_id] = M5TypedDirectTerminalProjection.build(
                job_id=corrupted.job.job_id,
                terminal_state=JobState.CANCELLED,
                terminal_reason="not-epoch-failed",
                m4_completion_digest=None,
                completed_revision=resulting_revision,
            )
            with pytest.raises(
                ValidationError,
                match="newly cancelled direct projection must be epoch_failed",
            ):
                PostgresM5DirectRecoveryStore._validate_complete_terminal_projection_image(
                    m4_plan,
                    projections,
                )
            ports._discard_typed_direct_epoch_failure_plan_local(
                cursor,
                m4_plan,
                M5RunFailureReason.INVALID_ARTIFACT.value,
            )
    assert database_snapshot(database.connection) == before


@pytest.mark.parametrize(
    "layer",
    ("job_locks", "derived_plan", "direct_plan"),
)
def test_failure_authority_rejects_same_object_proper_subset_before_commit(
    d24_direct_application_db: DirectApplicationD24Database,
    layer: str,
) -> None:
    database = d24_direct_application_db
    plan = database.insert_plan(tag=f"authority-subset-{layer}", chunk_count=2)
    opened = open_event(database, plan)
    ports = database.bound.adapter._ports
    recovery = database.bound.adapter._recovery
    before = database_snapshot(database.connection)

    with pytest.raises(EventConflictError, match="mutated"):
        with database.connection.transaction():
            with database.connection.cursor() as cursor:
                locks = ports._lock_typed_direct_epoch_failure_jobs_local(
                    cursor,
                    opened.epoch_id,
                    1,
                )
                assert len(locks.jobs) == 2
                retained_images = locks.jobs[:1]
                assert retained_images[0].latest_attempt is None
                if layer == "job_locks":
                    object.__setattr__(locks, "jobs", retained_images)
                    ports._lock_typed_direct_epoch_failure_details_local(
                        cursor,
                        locks,
                        None,
                    )
                    raise AssertionError("mutated job locks reached details")

                m4_plan = ports._lock_typed_direct_epoch_failure_details_local(
                    cursor,
                    locks,
                    None,
                )
                retained_jobs = tuple(image.job for image in retained_images)
                if layer == "derived_plan":
                    object.__setattr__(m4_plan.job_locks, "jobs", retained_images)
                    object.__setattr__(m4_plan, "cancelled_jobs", retained_jobs)
                    ports._stage_typed_direct_epoch_failure_local(
                        cursor,
                        m4_plan,
                        M5RunFailureReason.INVALID_ARTIFACT.value,
                        None,
                    )
                    raise AssertionError("mutated M4 plan reached its write stage")

                direct_plan = recovery.lock_direct_epoch_failure_details(
                    cursor,
                    epoch_id=opened.epoch_id,
                    expected_revision=1,
                    m4_plan=m4_plan,
                    job=None,
                    lease=None,
                    direct_terminal_reason=None,
                    error_hash=None,
                    attempt_work=None,
                    attempt_timing=None,
                )
                m4_stage = ports._stage_typed_direct_epoch_failure_local(
                    cursor,
                    m4_plan,
                    M5RunFailureReason.INVALID_ARTIFACT.value,
                    None,
                )
                object.__setattr__(m4_plan.job_locks, "jobs", retained_images)
                object.__setattr__(m4_plan, "cancelled_jobs", retained_jobs)
                object.__setattr__(m4_stage, "cancelled_jobs", retained_jobs)
                recovery.apply_direct_epoch_failure(
                    cursor,
                    plan=direct_plan,
                    m4_stage=m4_stage,
                )
                raise AssertionError("mutated direct plan reached sidecar writes")
    assert database_snapshot(database.connection) == before


@pytest.mark.parametrize(
    "mode",
    (
        "job_reordered",
        "job_nested_mutation",
        "job_copied",
        "job_reused",
        "job_cross_cursor",
        "plan_copied",
        "plan_reused",
        "plan_cross_cursor",
        "direct_copied",
        "direct_reused",
        "direct_stale",
        "direct_cross_cursor",
    ),
)
def test_failure_authority_is_exact_single_use_and_cursor_local(
    d24_direct_application_db: DirectApplicationD24Database,
    mode: str,
) -> None:
    database = d24_direct_application_db
    plan = database.insert_plan(tag=f"authority-{mode}", chunk_count=2)
    opened = open_event(database, plan)
    ports = database.bound.adapter._ports
    recovery = database.bound.adapter._recovery
    before = database_snapshot(database.connection)

    try:
        with pytest.raises((EventConflictError, ValidationError)):
            with database.connection.transaction():
                with database.connection.cursor() as cursor:
                    locks = ports._lock_typed_direct_epoch_failure_jobs_local(
                        cursor,
                        opened.epoch_id,
                        1,
                    )
                    assert len(locks.jobs) == 2
                    if mode == "job_reordered":
                        object.__setattr__(locks, "jobs", tuple(reversed(locks.jobs)))
                        ports._lock_typed_direct_epoch_failure_details_local(
                            cursor,
                            locks,
                            None,
                        )
                    elif mode == "job_nested_mutation":
                        object.__setattr__(
                            locks.jobs[0].job,
                            "payload_hash",
                            sha("mutated-authority-job-payload"),
                        )
                        ports._lock_typed_direct_epoch_failure_details_local(
                            cursor,
                            locks,
                            None,
                        )
                    elif mode == "job_copied":
                        ports._lock_typed_direct_epoch_failure_details_local(
                            cursor,
                            replace(locks),
                            None,
                        )
                    elif mode == "job_cross_cursor":
                        with database.connection.cursor() as other:
                            ports._lock_typed_direct_epoch_failure_details_local(
                                other,
                                locks,
                                None,
                            )
                    else:
                        m4_plan = ports._lock_typed_direct_epoch_failure_details_local(
                            cursor,
                            locks,
                            None,
                        )
                        if mode == "job_reused":
                            ports._lock_typed_direct_epoch_failure_details_local(
                                cursor,
                                locks,
                                None,
                            )
                        elif mode == "plan_copied":
                            ports._stage_typed_direct_epoch_failure_local(
                                cursor,
                                replace(m4_plan),
                                M5RunFailureReason.INVALID_ARTIFACT.value,
                                None,
                            )
                        elif mode == "plan_reused":
                            ports._stage_typed_direct_epoch_failure_local(
                                cursor,
                                m4_plan,
                                M5RunFailureReason.INVALID_ARTIFACT.value,
                                None,
                            )
                            ports._stage_typed_direct_epoch_failure_local(
                                cursor,
                                m4_plan,
                                M5RunFailureReason.INVALID_ARTIFACT.value,
                                None,
                            )
                        elif mode == "plan_cross_cursor":
                            with database.connection.cursor() as other:
                                ports._stage_typed_direct_epoch_failure_local(
                                    other,
                                    m4_plan,
                                    M5RunFailureReason.INVALID_ARTIFACT.value,
                                    None,
                                )
                        else:
                            direct_plan = recovery.lock_direct_epoch_failure_details(
                                cursor,
                                epoch_id=opened.epoch_id,
                                expected_revision=1,
                                m4_plan=m4_plan,
                                job=None,
                                lease=None,
                                direct_terminal_reason=None,
                                error_hash=None,
                                attempt_work=None,
                                attempt_timing=None,
                            )
                            m4_stage = ports._stage_typed_direct_epoch_failure_local(
                                cursor,
                                m4_plan,
                                M5RunFailureReason.INVALID_ARTIFACT.value,
                                None,
                            )
                            if mode == "direct_copied":
                                recovery.apply_direct_epoch_failure(
                                    cursor,
                                    plan=replace(direct_plan),
                                    m4_stage=m4_stage,
                                )
                            elif mode == "direct_reused":
                                recovery.apply_direct_epoch_failure(
                                    cursor,
                                    plan=direct_plan,
                                    m4_stage=m4_stage,
                                )
                                recovery.apply_direct_epoch_failure(
                                    cursor,
                                    plan=direct_plan,
                                    m4_stage=m4_stage,
                                )
                            elif mode == "direct_stale":
                                recovery.discard_epoch_failure_authority()
                                recovery.apply_direct_epoch_failure(
                                    cursor,
                                    plan=direct_plan,
                                    m4_stage=m4_stage,
                                )
                            elif mode == "direct_cross_cursor":
                                with database.connection.cursor() as other:
                                    recovery.apply_direct_epoch_failure(
                                        other,
                                        plan=direct_plan,
                                        m4_stage=m4_stage,
                                    )
                            else:
                                raise AssertionError(
                                    f"unknown failure-authority mode: {mode}"
                                )
                    raise AssertionError(f"failure authority mode was accepted: {mode}")
    finally:
        ports._typed_failure_job_lock_authority.clear()
        ports._typed_failure_plan_authority.clear()
        recovery.discard_epoch_failure_authority()
    assert database_snapshot(database.connection) == before
