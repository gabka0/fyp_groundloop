"""Live reconnect and terminal-cutoff races for the R2c pre-seal bridge.

Every durable transition in this module goes through the concrete PostgreSQL
store.  Test-local ports may stop a process or choose which real transaction
commits first, but they never manufacture a lease, receipt, result, or row.
"""

from __future__ import annotations

from copy import copy
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pytest
from psycopg import Connection, sql

from groundloop.errors import EventConflictError, InvalidEventError, ValidationError
from groundloop.m5.runtime.application import (
    M5DiscoveryExecution,
    M5TerminalInvocationTelemetry,
)
from groundloop.m5.runtime.contracts import (
    M5AcquisitionDisposition,
    M5AttemptOutput,
    M5CandidatePolicyManifest,
    M5EventRunResult,
    M5ExecutionEvidenceDisposition,
    M5JobLease,
    M5LeaseTerminalProjection,
    M5LogicalJobSpec,
    M5ReplayedOutcome,
    M5RequirementAttemptReturnReceipt,
    M5RequirementDiscoveryResult,
    M5RequirementPairInput,
    M5RequirementVerifierArtifact,
    M5RunFailureReason,
    M5RunState,
    M5RuntimeTiming,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5TransitionTimingAnchor,
    M5TypedEventPlan,
)
from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
from groundloop.m5.runtime.postgres_application import (
    PostgresM5GroupRequirementPreSealPorts,
)
from tests.m5.postgres_runtime.d24_application.conftest import (
    ApplicationD24Database,
    ControlledDiscovery,
    ControlledMeasurements,
    ControlledVerifier,
    assemble_application,
    database_snapshot,
    relation_rows,
    sha,
)


class SyntheticProcessLoss(RuntimeError):
    """A test-local process stop after the preceding real commit."""


DISCOVERY_WORK = M5RuntimeWork(
    requirement_forward_retrieval_call_count=1,
    embedding_model_call_count=1,
    embedding_input_token_count=7,
)
VERIFIER_WORK = M5RuntimeWork(
    requirement_verifier_call_count=1,
    verifier_model_call_count=1,
    verifier_input_token_count=11,
    verifier_output_token_count=3,
)


@dataclass(slots=True)
class CrashOnTransitionMeasurements:
    """Lose the process after one selected transition has committed."""

    target_kind: M5RuntimeWorkContributionKind
    delegate: ControlledMeasurements = field(default_factory=ControlledMeasurements)
    crashed: bool = False

    def transition_call_timing(
        self, anchor: M5TransitionTimingAnchor
    ) -> M5RuntimeTiming | None:
        if not self.crashed and anchor.contribution_kind is self.target_kind:
            self.crashed = True
            raise SyntheticProcessLoss(
                f"lost before {anchor.contribution_kind.value} timing append"
            )
        return self.delegate.transition_call_timing(anchor)

    def terminal_invocation(
        self, event: M5TypedEventPlan, result: M5EventRunResult
    ) -> M5TerminalInvocationTelemetry:
        return self.delegate.terminal_invocation(event, result)


@dataclass(slots=True)
class CrashInDiscovery:
    """Record the concrete lease, then model a worker/process loss."""

    calls: list[str] = field(default_factory=list)
    leases: list[M5JobLease] = field(default_factory=list)

    def discover_requirement_scope(
        self,
        epoch_id: int,
        lease: M5JobLease,
        job: M5LogicalJobSpec,
        manifest: M5CandidatePolicyManifest,
        event: M5TypedEventPlan,
    ) -> M5DiscoveryExecution:
        del epoch_id, manifest, event
        self.calls.append(job.logical_job_id)
        self.leases.append(lease)
        raise SyntheticProcessLoss("lost after acquisition and before worker return")


class CompetingFailurePorts(PostgresM5GroupRequirementPreSealPorts):
    """Base for race ports that commit one real competing epoch failure."""

    def __init__(
        self,
        database: ApplicationD24Database,
        store: PostgresM5RuntimeStore,
        reason: M5RunFailureReason = M5RunFailureReason.RETRIEVAL_ERROR,
    ) -> None:
        super().__init__(store, database.manifest, database.operational_config)
        self.database = database
        self.reason = reason
        self.canonical: M5EventRunResult | None = None
        self.winner: M5EventRunResult | None = None
        self.validation_checkpoint: (
            tuple[tuple[str, tuple[tuple[str, str], ...]], ...] | None
        ) = None

    def commit_competing_failure(self, epoch_id: int) -> None:
        if self.canonical is not None:
            raise AssertionError("competing terminal transaction ran more than once")
        with self.database.reconnect() as competitor:
            revision = competitor.store.current_revision(epoch_id)
            winner = competitor.store.fail_typed_epoch_atomically(
                epoch_id,
                revision,
                self.reason,
                M5RuntimeWork(),
            )
            assert winner.state is M5RunState.FAILED
            canonical = competitor.store.read_typed_event_result(
                winner.event_id, winner.payload_hash
            )
            assert canonical is not None
            assert canonical.state is M5RunState.REPLAYED
            assert canonical.call_work.is_zero
            self.winner = winner
            self.canonical = canonical

    def capture_validation_checkpoint(self) -> None:
        with self.database.reconnect() as checkpoint:
            self.validation_checkpoint = database_snapshot(checkpoint.connection)


class DiscoveryCutoffPorts(CompetingFailurePorts):
    """Commit failure immediately before the real discovery-return transaction."""

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
    ) -> M5RequirementAttemptReturnReceipt:
        self.commit_competing_failure(epoch_id)
        return super().stage_m5_discovery_result_atomically(
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
        )


class VerifierCutoffPorts(CompetingFailurePorts):
    """Commit failure immediately before the real verifier-return transaction."""

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
    ) -> M5RequirementAttemptReturnReceipt:
        self.commit_competing_failure(epoch_id)
        return super().complete_m5_verifier_atomically(
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
        )


class LaterAcquisitionCutoffPorts(CompetingFailurePorts):
    """Fail the epoch before the second real requirement acquisition."""

    def __init__(
        self,
        database: ApplicationD24Database,
        store: PostgresM5RuntimeStore,
    ) -> None:
        super().__init__(database, store)
        self.acquisition_count = 0

    def acquire_m5_job(
        self,
        epoch_id: int,
        expected_revision: int,
        job: M5LogicalJobSpec,
    ) -> M5JobLease:
        self.acquisition_count += 1
        if self.acquisition_count == 2:
            self.commit_competing_failure(epoch_id)
        return super().acquire_m5_job(epoch_id, expected_revision, job)


class FailureMutatorCutoffPorts(CompetingFailurePorts):
    """Let another same-reason failure win before the checked mutator call."""

    def fail_typed_epoch_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        failure_reason: M5RunFailureReason,
        call_work: M5RuntimeWork,
    ) -> M5EventRunResult:
        if failure_reason is not self.reason:
            raise AssertionError("checked failure used another reason")
        self.commit_competing_failure(epoch_id)
        return super().fail_typed_epoch_atomically(
            epoch_id,
            expected_revision,
            failure_reason,
            call_work,
        )


class DifferentReasonFailureMutatorPorts(CompetingFailurePorts):
    """Commit another failure reason before the checked application mutator."""

    def __init__(
        self,
        database: ApplicationD24Database,
        store: PostgresM5RuntimeStore,
    ) -> None:
        super().__init__(database, store, M5RunFailureReason.VERIFIER_ERROR)

    def fail_typed_epoch_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        failure_reason: M5RunFailureReason,
        call_work: M5RuntimeWork,
    ) -> M5EventRunResult:
        self.commit_competing_failure(epoch_id)
        return PostgresM5GroupRequirementPreSealPorts.fail_typed_epoch_atomically(
            self,
            epoch_id,
            expected_revision,
            failure_reason,
            call_work,
        )


class CorruptDiscoveryCutoffPorts(DiscoveryCutoffPorts):
    """Return one malformed post-commit receipt/read for validation falsifiers."""

    def __init__(
        self,
        database: ApplicationD24Database,
        store: PostgresM5RuntimeStore,
        mode: str,
    ) -> None:
        super().__init__(database, store)
        self.mode = mode
        self.corrupted = False

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
    ) -> M5RequirementAttemptReturnReceipt:
        receipt = super().stage_m5_discovery_result_atomically(
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
        )
        if self.mode != "wrong_hash":
            return receipt
        self.capture_validation_checkpoint()
        corrupted = copy(receipt)
        object.__setattr__(
            corrupted,
            "current_terminal_logical_result_hash",
            sha("wrong-terminal-logical-result"),
        )
        self.corrupted = True
        return corrupted

    def read_typed_event_result(
        self, event_id: str, payload_hash: str
    ) -> M5EventRunResult | None:
        result = super().read_typed_event_result(event_id, payload_hash)
        if (
            result is None
            or self.canonical is None
            or self.mode == "wrong_hash"
            or self.corrupted
        ):
            return result
        self.capture_validation_checkpoint()
        corrupted = copy(result)
        if self.mode == "wrong_event":
            object.__setattr__(corrupted, "event_id", "another-event")
        elif self.mode == "wrong_payload":
            object.__setattr__(corrupted, "payload_hash", sha("another-payload"))
        elif self.mode == "wrong_epoch":
            object.__setattr__(corrupted, "epoch_id", result.epoch_id + 1)
        elif self.mode == "wrong_outcome":
            object.__setattr__(
                corrupted,
                "replayed_outcome",
                M5ReplayedOutcome.SEALED,
            )
        elif self.mode == "malformed_receipt":
            receipt = copy(result.open_receipt)
            object.__setattr__(receipt, "already_failed", False)
            object.__setattr__(receipt, "failure_reason", None)
            object.__setattr__(corrupted, "open_receipt", receipt)
        elif self.mode == "changed_frozen_field":
            object.__setattr__(
                corrupted,
                "event_work",
                M5RuntimeWork(bytes_hashed=result.event_work.bytes_hashed + 1),
            )
        else:
            raise AssertionError(f"unknown canonical corruption mode: {self.mode}")
        self.corrupted = True
        return corrupted


class CorruptLaterAcquisitionPorts(LaterAcquisitionCutoffPorts):
    """Return one malformed concrete terminal lease after failure wins."""

    def acquire_m5_job(
        self,
        epoch_id: int,
        expected_revision: int,
        job: M5LogicalJobSpec,
    ) -> M5JobLease:
        lease = super().acquire_m5_job(epoch_id, expected_revision, job)
        if self.acquisition_count != 2:
            return lease
        assert lease.disposition is M5AcquisitionDisposition.TERMINAL
        assert isinstance(lease.terminal_projection, M5LeaseTerminalProjection)
        self.capture_validation_checkpoint()
        projection = copy(lease.terminal_projection)
        object.__setattr__(
            projection,
            "terminal_identity_hash",
            sha("wrong-terminal-job"),
        )
        corrupted = copy(lease)
        object.__setattr__(corrupted, "terminal_projection", projection)
        return corrupted


class ResultFirstThenFailurePorts(CompetingFailurePorts):
    """Commit the real discovery result before the competing failure."""

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
    ) -> M5RequirementAttemptReturnReceipt:
        receipt = super().stage_m5_discovery_result_atomically(
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
        )
        assert receipt.current_terminal_logical_result_hash is None
        self.commit_competing_failure(epoch_id)
        return receipt


def _epoch_id(database: ApplicationD24Database, event_id: str) -> int:
    row = database.connection.execute(
        """
        SELECT epoch_id
        FROM groundloop_m5_runtime_epoch
        WHERE structural_event_id = %s
        """,
        (event_id,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _relation_count(
    connection: Connection[Any], relation: str, *, epoch_id: int
) -> int:
    row = connection.execute(
        sql.SQL("SELECT count(*) FROM {} WHERE epoch_id = %s").format(
            sql.Identifier(relation)
        ),
        (epoch_id,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _contribution_count(
    connection: Connection[Any],
    *,
    epoch_id: int,
    kind: M5RuntimeWorkContributionKind,
) -> int:
    row = connection.execute(
        """
        SELECT count(*)
        FROM groundloop_m5_runtime_work_contribution
        WHERE epoch_id = %s AND contribution_kind = %s
        """,
        (epoch_id, kind.value),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _transition_rows(
    connection: Connection[Any],
    *,
    epoch_id: int,
    kind: M5RuntimeWorkContributionKind,
) -> tuple[tuple[object, ...], ...]:
    return tuple(
        tuple(row)
        for row in connection.execute(
            """
            SELECT source_id, anchor_revision, required_interval_observed,
                   observation_digest, transition_timing_digest
            FROM groundloop_m5_transition_call_timing
            WHERE epoch_id = %s AND contribution_kind = %s
            ORDER BY anchor_revision, source_id COLLATE "C"
            """,
            (epoch_id, kind.value),
        ).fetchall()
    )


def _all_transition_rows(
    connection: Connection[Any],
    *,
    epoch_id: int,
) -> tuple[tuple[object, ...], ...]:
    """Snapshot every immutable transition-timing row for one event."""

    return tuple(
        tuple(row)
        for row in connection.execute(
            """
            SELECT contribution_kind, source_id, contribution_key_digest,
                   anchor_revision, required_interval_observed,
                   coordinator_non_db_non_neural_ns, neural_wall_ns,
                   postgres_roundtrip_wall_ns, external_io_wall_ns,
                   end_to_end_wall_ns, postgres_server_execution_ns,
                   postgres_lock_wait_ns, postgres_wal_bytes,
                   postgres_shared_block_reads, observation_digest,
                   transition_timing_digest
            FROM groundloop_m5_transition_call_timing
            WHERE epoch_id = %s
            ORDER BY anchor_revision, contribution_kind COLLATE "C",
                     source_id COLLATE "C"
            """,
            (epoch_id,),
        ).fetchall()
    )


def _execution_identity_snapshot(
    connection: Connection[Any],
    *,
    epoch_id: int,
) -> tuple[tuple[str, tuple[tuple[object, ...], ...]], ...]:
    """Read immutable job, attempt, dispatch, and evidence identities."""

    semantic_jobs = tuple(
        tuple(row)
        for row in connection.execute(
            """
            SELECT logical_job_id, epoch_id, structural_event_id, job_kind,
                   candidate_policy_id, candidate_policy_manifest_hash,
                   parent_job_id, subject_kind, subject_id, chunk_version_id,
                   semantic_pair_digest, admitted_pair_digest,
                   scope_contract_digest,
                   requirement_registry_snapshot_digest,
                   active_chunk_snapshot_digest, role_template_hash,
                   execution_spec_hash, expandable, payload_hash
            FROM groundloop_m5_semantic_job
            WHERE epoch_id = %s
            ORDER BY logical_job_id COLLATE "C"
            """,
            (epoch_id,),
        ).fetchall()
    )
    attempts = tuple(
        tuple(row)
        for row in connection.execute(
            """
            SELECT attempt.attempt_id, attempt.logical_job_id,
                   attempt.attempt_ordinal, attempt.execution_spec_hash,
                   attempt.lease_token_hash, attempt.dispatched_at
            FROM groundloop_m5_job_attempt AS attempt
            JOIN groundloop_m5_semantic_job AS job
              ON job.logical_job_id = attempt.logical_job_id
            WHERE job.epoch_id = %s
            ORDER BY attempt.logical_job_id COLLATE "C",
                     attempt.attempt_ordinal, attempt.attempt_id COLLATE "C"
            """,
            (epoch_id,),
        ).fetchall()
    )
    dispatches = tuple(
        tuple(row)
        for row in connection.execute(
            """
            SELECT epoch_id, subgraph, attempt_id, logical_job_id,
                   attempt_ordinal, job_kind, fallback_required,
                   dispatched_revision, lease_expires_at,
                   maximum_work_digest, record_digest
            FROM groundloop_m5_dispatch_record
            WHERE epoch_id = %s
            ORDER BY subgraph COLLATE "C", logical_job_id COLLATE "C",
                     attempt_ordinal, attempt_id COLLATE "C"
            """,
            (epoch_id,),
        ).fetchall()
    )
    evidence = tuple(
        tuple(row)
        for row in connection.execute(
            """
            SELECT epoch_id, subgraph, attempt_id, disposition,
                   result_or_error_hash, attempt_work_digest,
                   attempt_timing_digest, evidence_digest
            FROM groundloop_m5_attempt_execution_evidence
            WHERE epoch_id = %s
            ORDER BY subgraph COLLATE "C", attempt_id COLLATE "C"
            """,
            (epoch_id,),
        ).fetchall()
    )
    return (
        ("semantic_job", semantic_jobs),
        ("job_attempt", attempts),
        ("dispatch_record", dispatches),
        ("attempt_execution_evidence", evidence),
    )


def _wait_until_database_deadline(
    connection: Connection[Any], deadline: datetime
) -> None:
    """Wait and prove deadline passage using PostgreSQL's own clock only."""

    connection.execute(
        """
        SELECT pg_sleep(
            greatest(
                extract(epoch FROM (%s::timestamptz - clock_timestamp())),
                0
            ) + 0.02
        )
        """,
        (deadline,),
    )
    row = connection.execute(
        "SELECT clock_timestamp() >= %s::timestamptz",
        (deadline,),
    ).fetchone()
    assert row == (True,)
    connection.commit()


def _provider_free_replay(
    database: ApplicationD24Database,
    plan: M5TypedEventPlan,
    measurements: ControlledMeasurements | None = None,
) -> tuple[M5EventRunResult, ControlledMeasurements]:
    discovery = ControlledDiscovery(database)
    verifier = ControlledVerifier(database)
    active_measurements = measurements or ControlledMeasurements()
    with database.reconnect() as bound:
        application = assemble_application(
            database,
            discovery,
            verifier,
            active_measurements,
            bound=bound,
        )
        result = application.run_event(plan)
    assert discovery.calls == []
    assert verifier.calls == []
    return result, active_measurements


def _prepare_resumed_nonterminal(
    database: ApplicationD24Database,
    plan: M5TypedEventPlan,
) -> None:
    """Commit structural open, then lose its caller before any job dispatch."""

    measurements = CrashOnTransitionMeasurements(
        M5RuntimeWorkContributionKind.STRUCTURAL_OPEN
    )
    application = assemble_application(
        database,
        ControlledDiscovery(database),
        ControlledVerifier(database),
        measurements,
    )
    with pytest.raises(SyntheticProcessLoss, match="structural_open timing"):
        application.run_event(plan)


def _telemetry_count(database: ApplicationD24Database, epoch_id: int) -> int:
    return _relation_count(
        database.connection,
        "groundloop_m5_postcommit_invocation_telemetry",
        epoch_id=epoch_id,
    )


def _assert_active_failed_projection_and_reconnect(
    database: ApplicationD24Database,
    plan: M5TypedEventPlan,
    ports: CompetingFailurePorts,
    result: M5EventRunResult,
    measurements: ControlledMeasurements,
    *,
    resumed: bool,
    expected_call_work: M5RuntimeWork,
) -> None:
    canonical = ports.canonical
    assert canonical is not None
    assert canonical.state is M5RunState.REPLAYED
    assert canonical.replayed_outcome is M5ReplayedOutcome.FAILED
    assert canonical.failure_reason is M5RunFailureReason.RETRIEVAL_ERROR
    assert canonical.open_receipt.replayed
    assert canonical.open_receipt.already_failed
    assert canonical.call_work.is_zero

    assert result.state is M5RunState.REPLAYED
    assert result.replayed_outcome is M5ReplayedOutcome.FAILED
    assert result.failure_reason is M5RunFailureReason.RETRIEVAL_ERROR
    assert result.open_receipt.replayed is resumed
    assert not result.open_receipt.already_sealed
    assert not result.open_receipt.already_failed
    assert result.open_receipt.publication_id is None
    assert result.open_receipt.failure_reason is None
    assert result.call_work == expected_call_work
    for name in (
        "event_id",
        "payload_hash",
        "epoch_id",
        "replayed_outcome",
        "publication_receipt",
        "event_work",
        "event_timing",
        "event_timing_coverage",
        "combined_deltas",
        "changed_state_references",
        "failure_reason",
        "logical_result_hash",
    ):
        assert getattr(result, name) == getattr(canonical, name)
    assert len(measurements.terminal_calls) == 1
    assert _telemetry_count(database, result.epoch_id) == 1

    dispatches = _relation_count(
        database.connection,
        "groundloop_m5_dispatch_record",
        epoch_id=result.epoch_id,
    )
    transition_calls_before = tuple(measurements.transition_calls)
    with database.reconnect() as before_reconnect:
        transition_rows_before = _all_transition_rows(
            before_reconnect.connection,
            epoch_id=result.epoch_id,
        )
        execution_identities_before = _execution_identity_snapshot(
            before_reconnect.connection,
            epoch_id=result.epoch_id,
        )
    replay, replay_measurements = _provider_free_replay(
        database,
        plan,
        measurements,
    )
    assert replay_measurements is measurements
    assert len(measurements.terminal_calls) == 2
    assert replay.state is M5RunState.REPLAYED
    assert replay.replayed_outcome is M5ReplayedOutcome.FAILED
    assert replay.call_work.is_zero
    assert replay.open_receipt.replayed
    assert replay.open_receipt.already_failed
    assert replay.logical_result_hash == result.logical_result_hash
    assert replay.event_work == result.event_work
    assert replay.event_timing == result.event_timing
    assert replay.event_timing_coverage == result.event_timing_coverage
    assert tuple(measurements.transition_calls) == transition_calls_before
    assert _telemetry_count(database, result.epoch_id) == 2
    assert (
        _relation_count(
            database.connection,
            "groundloop_m5_dispatch_record",
            epoch_id=result.epoch_id,
        )
        == dispatches
    )
    with database.reconnect() as after_reconnect:
        assert (
            _all_transition_rows(
                after_reconnect.connection,
                epoch_id=result.epoch_id,
            )
            == transition_rows_before
        )
        assert (
            _execution_identity_snapshot(
                after_reconnect.connection,
                epoch_id=result.epoch_id,
            )
            == execution_identities_before
        )


def _assert_validation_added_no_write(
    database: ApplicationD24Database,
    ports: CompetingFailurePorts,
) -> None:
    assert ports.validation_checkpoint is not None
    assert ports.canonical is not None
    with database.reconnect() as after:
        assert database_snapshot(after.connection) == ports.validation_checkpoint
    assert _telemetry_count(database, ports.canonical.epoch_id) == 0


def test_structural_open_process_loss_is_missing_and_replay_never_duplicates(
    d24_application_db: ApplicationD24Database,
) -> None:
    """The next real mutator resolves the lost open interval as missing."""

    plan = d24_application_db.register_plan(tag="open-loss", requirement_count=1)
    crash_measurements = CrashOnTransitionMeasurements(
        M5RuntimeWorkContributionKind.STRUCTURAL_OPEN
    )
    first = assemble_application(
        d24_application_db,
        ControlledDiscovery(d24_application_db),
        ControlledVerifier(d24_application_db),
        crash_measurements,
    )

    with pytest.raises(SyntheticProcessLoss, match="structural_open timing"):
        first.run_event(plan)

    epoch_id = _epoch_id(d24_application_db, plan.structural_event_id)
    assert (
        _contribution_count(
            d24_application_db.connection,
            epoch_id=epoch_id,
            kind=M5RuntimeWorkContributionKind.STRUCTURAL_OPEN,
        )
        == 1
    )
    assert (
        _transition_rows(
            d24_application_db.connection,
            epoch_id=epoch_id,
            kind=M5RuntimeWorkContributionKind.STRUCTURAL_OPEN,
        )
        == ()
    )

    crashed_worker = CrashInDiscovery()
    with d24_application_db.reconnect() as bound:
        resumed = assemble_application(
            d24_application_db,
            crashed_worker,
            ControlledVerifier(d24_application_db),
            ControlledMeasurements(),
            bound=bound,
        )
        with pytest.raises(SyntheticProcessLoss, match="before worker return"):
            resumed.run_event(plan)

    assert len(crashed_worker.calls) == 1
    structural_timing = _transition_rows(
        d24_application_db.connection,
        epoch_id=epoch_id,
        kind=M5RuntimeWorkContributionKind.STRUCTURAL_OPEN,
    )
    assert len(structural_timing) == 1
    assert structural_timing[0][2] is False
    assert (
        _contribution_count(
            d24_application_db.connection,
            epoch_id=epoch_id,
            kind=M5RuntimeWorkContributionKind.STRUCTURAL_OPEN,
        )
        == 1
    )
    assert (
        _contribution_count(
            d24_application_db.connection,
            epoch_id=epoch_id,
            kind=M5RuntimeWorkContributionKind.M5_ACQUISITION,
        )
        == 1
    )

    waiting_provider = ControlledDiscovery(d24_application_db)
    waiting_measurements = ControlledMeasurements()
    transition_rows_before_live_lease = _all_transition_rows(
        d24_application_db.connection,
        epoch_id=epoch_id,
    )
    with d24_application_db.reconnect() as bound:
        waiting = assemble_application(
            d24_application_db,
            waiting_provider,
            ControlledVerifier(d24_application_db),
            waiting_measurements,
            bound=bound,
        ).run_event(plan)
    assert waiting.state is M5RunState.BLOCKED
    assert waiting.failure_reason is M5RunFailureReason.WORK_IN_PROGRESS
    assert waiting.open_receipt.replayed
    assert waiting_provider.calls == []
    assert waiting_measurements.transition_calls == []
    assert (
        _all_transition_rows(d24_application_db.connection, epoch_id=epoch_id)
        == transition_rows_before_live_lease
    )
    assert (
        _contribution_count(
            d24_application_db.connection,
            epoch_id=epoch_id,
            kind=M5RuntimeWorkContributionKind.STRUCTURAL_OPEN,
        )
        == 1
    )
    assert (
        len(
            _transition_rows(
                d24_application_db.connection,
                epoch_id=epoch_id,
                kind=M5RuntimeWorkContributionKind.STRUCTURAL_OPEN,
            )
        )
        == 1
    )


def test_database_clock_takeover_creates_one_dense_successor_and_one_new_anchor(
    d24_application_db: ApplicationD24Database,
) -> None:
    plan = d24_application_db.register_plan(tag="takeover", requirement_count=1)
    crashed_worker = CrashInDiscovery()
    application = assemble_application(
        d24_application_db,
        crashed_worker,
        ControlledVerifier(d24_application_db),
        ControlledMeasurements(),
    )
    with pytest.raises(SyntheticProcessLoss, match="before worker return"):
        application.run_event(plan)

    assert len(crashed_worker.leases) == 1
    original = crashed_worker.leases[0]
    assert original.attempt is not None
    assert original.lease_expires_at is not None
    epoch_id = _epoch_id(d24_application_db, plan.structural_event_id)
    before_dispatches = _relation_count(
        d24_application_db.connection,
        "groundloop_m5_dispatch_record",
        epoch_id=epoch_id,
    )
    before_anchors = len(
        _transition_rows(
            d24_application_db.connection,
            epoch_id=epoch_id,
            kind=M5RuntimeWorkContributionKind.M5_ACQUISITION,
        )
    )
    _wait_until_database_deadline(
        d24_application_db.connection, original.lease_expires_at
    )

    retryable = ControlledDiscovery(
        d24_application_db,
        failure_reason=M5RunFailureReason.RETRIEVAL_UNAVAILABLE,
        retryable=True,
        call_work=DISCOVERY_WORK,
    )
    with d24_application_db.reconnect() as bound:
        result = assemble_application(
            d24_application_db,
            retryable,
            ControlledVerifier(d24_application_db),
            ControlledMeasurements(),
            bound=bound,
        ).run_event(plan)

    assert result.state is M5RunState.BLOCKED
    assert result.failure_reason is M5RunFailureReason.RETRIEVAL_UNAVAILABLE
    assert result.call_work == DISCOVERY_WORK
    assert len(retryable.calls) == 1
    attempts = relation_rows(
        d24_application_db.connection,
        "groundloop_m5_job_attempt",
        ("attempt_ordinal", "attempt_state"),
        order_by=("attempt_ordinal",),
    )
    assert attempts == ((1, "expired"), (2, "failed"))
    assert (
        _relation_count(
            d24_application_db.connection,
            "groundloop_m5_dispatch_record",
            epoch_id=epoch_id,
        )
        == before_dispatches + 1
    )
    assert (
        len(
            _transition_rows(
                d24_application_db.connection,
                epoch_id=epoch_id,
                kind=M5RuntimeWorkContributionKind.M5_ACQUISITION,
            )
        )
        == before_anchors + 1
    )


def test_retryable_failure_reconnect_reacquires_without_reusing_failed_attempt(
    d24_application_db: ApplicationD24Database,
) -> None:
    plan = d24_application_db.register_plan(tag="retry", requirement_count=1)
    first_provider = ControlledDiscovery(
        d24_application_db,
        failure_reason=M5RunFailureReason.RETRIEVAL_UNAVAILABLE,
        retryable=True,
        call_work=DISCOVERY_WORK,
    )
    first = assemble_application(
        d24_application_db,
        first_provider,
        ControlledVerifier(d24_application_db),
        ControlledMeasurements(),
    ).run_event(plan)
    assert first.state is M5RunState.BLOCKED
    assert first.failure_reason is M5RunFailureReason.RETRIEVAL_UNAVAILABLE
    assert first.call_work == DISCOVERY_WORK

    second_provider = ControlledDiscovery(
        d24_application_db,
        failure_reason=M5RunFailureReason.RETRIEVAL_UNAVAILABLE,
        retryable=True,
        call_work=M5RuntimeWork(),
    )
    with d24_application_db.reconnect() as bound:
        second = assemble_application(
            d24_application_db,
            second_provider,
            ControlledVerifier(d24_application_db),
            ControlledMeasurements(),
            bound=bound,
        ).run_event(plan)

    assert second.state is M5RunState.BLOCKED
    assert second.failure_reason is M5RunFailureReason.RETRIEVAL_UNAVAILABLE
    assert second.open_receipt.replayed
    assert second.call_work.is_zero
    assert len(first_provider.calls) == len(second_provider.calls) == 1
    attempts = relation_rows(
        d24_application_db.connection,
        "groundloop_m5_job_attempt",
        ("attempt_ordinal", "attempt_state"),
        order_by=("attempt_ordinal",),
    )
    assert attempts == ((1, "failed"), (2, "failed"))


def test_terminal_failure_reconnect_is_canonical_and_provider_free(
    d24_application_db: ApplicationD24Database,
) -> None:
    plan = d24_application_db.register_plan(tag="terminal", requirement_count=1)
    provider = ControlledDiscovery(
        d24_application_db,
        failure_reason=M5RunFailureReason.RETRIEVAL_ERROR,
        retryable=False,
        call_work=DISCOVERY_WORK,
    )
    first_measurements = ControlledMeasurements()
    failed = assemble_application(
        d24_application_db,
        provider,
        ControlledVerifier(d24_application_db),
        first_measurements,
    ).run_event(plan)

    assert failed.state is M5RunState.FAILED
    assert failed.failure_reason is M5RunFailureReason.RETRIEVAL_ERROR
    assert failed.call_work == DISCOVERY_WORK
    assert len(provider.calls) == 1
    assert len(first_measurements.terminal_calls) == 1
    epoch_id = failed.epoch_id
    dispatches = _relation_count(
        d24_application_db.connection,
        "groundloop_m5_dispatch_record",
        epoch_id=epoch_id,
    )

    replay, replay_measurements = _provider_free_replay(
        d24_application_db,
        plan,
        first_measurements,
    )
    assert replay.state is M5RunState.REPLAYED
    assert replay.replayed_outcome is M5ReplayedOutcome.FAILED
    assert replay.failure_reason is M5RunFailureReason.RETRIEVAL_ERROR
    assert replay.call_work.is_zero
    assert replay.open_receipt.replayed
    assert replay.open_receipt.already_failed
    assert (
        replay.open_receipt.failure_reason == M5RunFailureReason.RETRIEVAL_ERROR.value
    )
    assert replay.logical_result_hash == failed.logical_result_hash
    assert replay.event_work == failed.event_work
    assert replay.event_timing == failed.event_timing
    assert replay.event_timing_coverage == failed.event_timing_coverage
    assert replay_measurements is first_measurements
    assert len(replay_measurements.terminal_calls) == 2
    assert _telemetry_count(d24_application_db, epoch_id) == 2
    assert (
        _relation_count(
            d24_application_db.connection,
            "groundloop_m5_dispatch_record",
            epoch_id=epoch_id,
        )
        == dispatches
    )


@pytest.mark.parametrize("resumed", (False, True), ids=("fresh", "resumed"))
@pytest.mark.parametrize("nonzero", (False, True), ids=("zero", "nonzero"))
def test_c5_concrete_discovery_cutoff_preserves_active_invocation_work(
    d24_application_db: ApplicationD24Database,
    resumed: bool,
    nonzero: bool,
) -> None:
    plan = d24_application_db.register_plan(
        tag=f"c5-discovery-{int(resumed)}-{int(nonzero)}",
        requirement_count=1,
    )
    if resumed:
        _prepare_resumed_nonterminal(d24_application_db, plan)
    call_work = DISCOVERY_WORK if nonzero else M5RuntimeWork()
    discovery = ControlledDiscovery(d24_application_db, call_work=call_work)
    measurements = ControlledMeasurements()

    with d24_application_db.reconnect() as bound:
        ports = DiscoveryCutoffPorts(d24_application_db, bound.store)
        result = assemble_application(
            d24_application_db,
            discovery,
            ControlledVerifier(d24_application_db),
            measurements,
            runtime_override=ports,
            bound=bound,
        ).run_event(plan)

    assert len(discovery.calls) == 1
    assert (
        _relation_count(
            d24_application_db.connection,
            "groundloop_m5_post_terminal_attempt_audit",
            epoch_id=result.epoch_id,
        )
        == 1
    )
    _assert_active_failed_projection_and_reconnect(
        d24_application_db,
        plan,
        ports,
        result,
        measurements,
        resumed=resumed,
        expected_call_work=call_work,
    )


@pytest.mark.parametrize("resumed", (False, True), ids=("fresh", "resumed"))
@pytest.mark.parametrize("nonzero", (False, True), ids=("zero", "nonzero"))
def test_c5_concrete_verifier_cutoff_uses_postterminal_audit_only(
    d24_application_db: ApplicationD24Database,
    resumed: bool,
    nonzero: bool,
) -> None:
    plan = d24_application_db.register_plan(
        tag=f"c5-verifier-{int(resumed)}-{int(nonzero)}",
        requirement_count=1,
    )
    if resumed:
        _prepare_resumed_nonterminal(d24_application_db, plan)
    verifier_work = VERIFIER_WORK if nonzero else M5RuntimeWork()
    discovery = ControlledDiscovery(d24_application_db, include_pair=True)
    verifier = ControlledVerifier(d24_application_db, call_work=verifier_work)
    measurements = ControlledMeasurements()

    with d24_application_db.reconnect() as bound:
        ports = VerifierCutoffPorts(d24_application_db, bound.store)
        result = assemble_application(
            d24_application_db,
            discovery,
            verifier,
            measurements,
            runtime_override=ports,
            bound=bound,
        ).run_event(plan)

    assert len(discovery.calls) == len(verifier.calls) == 1
    assert (
        _relation_count(
            d24_application_db.connection,
            "groundloop_m5_post_terminal_attempt_audit",
            epoch_id=result.epoch_id,
        )
        == 1
    )
    _assert_active_failed_projection_and_reconnect(
        d24_application_db,
        plan,
        ports,
        result,
        measurements,
        resumed=resumed,
        expected_call_work=verifier_work,
    )


@pytest.mark.parametrize("resumed", (False, True), ids=("fresh", "resumed"))
@pytest.mark.parametrize("nonzero", (False, True), ids=("zero", "nonzero"))
def test_c6_later_acquisition_observes_concrete_epoch_failed_projection(
    d24_application_db: ApplicationD24Database,
    resumed: bool,
    nonzero: bool,
) -> None:
    plan = d24_application_db.register_plan(
        tag=f"c6-acquire-{int(resumed)}-{int(nonzero)}",
        requirement_count=2,
    )
    if resumed:
        _prepare_resumed_nonterminal(d24_application_db, plan)
    call_work = DISCOVERY_WORK if nonzero else M5RuntimeWork()
    discovery = ControlledDiscovery(d24_application_db, call_work=call_work)
    measurements = ControlledMeasurements()

    with d24_application_db.reconnect() as bound:
        ports = LaterAcquisitionCutoffPorts(d24_application_db, bound.store)
        result = assemble_application(
            d24_application_db,
            discovery,
            ControlledVerifier(d24_application_db),
            measurements,
            runtime_override=ports,
            bound=bound,
        ).run_event(plan)

    assert ports.acquisition_count == 2
    assert len(discovery.calls) == 1
    _assert_active_failed_projection_and_reconnect(
        d24_application_db,
        plan,
        ports,
        result,
        measurements,
        resumed=resumed,
        expected_call_work=call_work,
    )


@pytest.mark.parametrize("resumed", (False, True), ids=("fresh", "resumed"))
@pytest.mark.parametrize("nonzero", (False, True), ids=("zero", "nonzero"))
def test_c6_same_reason_failure_mutator_replay_preserves_active_work(
    d24_application_db: ApplicationD24Database,
    resumed: bool,
    nonzero: bool,
) -> None:
    plan = d24_application_db.register_plan(
        tag=f"c6-fail-{int(resumed)}-{int(nonzero)}",
        requirement_count=1,
    )
    if resumed:
        _prepare_resumed_nonterminal(d24_application_db, plan)
    call_work = DISCOVERY_WORK if nonzero else M5RuntimeWork()
    discovery = ControlledDiscovery(
        d24_application_db,
        failure_reason=M5RunFailureReason.RETRIEVAL_ERROR,
        retryable=False,
        call_work=call_work,
    )
    measurements = ControlledMeasurements()

    with d24_application_db.reconnect() as bound:
        ports = FailureMutatorCutoffPorts(d24_application_db, bound.store)
        result = assemble_application(
            d24_application_db,
            discovery,
            ControlledVerifier(d24_application_db),
            measurements,
            runtime_override=ports,
            bound=bound,
        ).run_event(plan)

    assert len(discovery.calls) == 1
    _assert_active_failed_projection_and_reconnect(
        d24_application_db,
        plan,
        ports,
        result,
        measurements,
        resumed=resumed,
        expected_call_work=call_work,
    )


def test_c6_failure_mutator_rejects_different_durable_reason_without_telemetry(
    d24_application_db: ApplicationD24Database,
) -> None:
    plan = d24_application_db.register_plan(tag="c6-wrong-reason")
    discovery = ControlledDiscovery(
        d24_application_db,
        failure_reason=M5RunFailureReason.RETRIEVAL_ERROR,
        retryable=False,
        call_work=DISCOVERY_WORK,
    )
    measurements = ControlledMeasurements()
    with d24_application_db.reconnect() as bound:
        ports = DifferentReasonFailureMutatorPorts(d24_application_db, bound.store)
        with pytest.raises(EventConflictError, match="another failure reason"):
            assemble_application(
                d24_application_db,
                discovery,
                ControlledVerifier(d24_application_db),
                measurements,
                runtime_override=ports,
                bound=bound,
            ).run_event(plan)

    assert len(discovery.calls) == 1
    assert measurements.terminal_calls == []
    assert ports.canonical is not None
    assert ports.canonical.failure_reason is M5RunFailureReason.VERIFIER_ERROR
    assert _telemetry_count(d24_application_db, ports.canonical.epoch_id) == 0


@pytest.mark.parametrize(
    "mode",
    (
        "wrong_hash",
        "wrong_event",
        "wrong_payload",
        "wrong_epoch",
        "wrong_outcome",
        "malformed_receipt",
        "changed_frozen_field",
    ),
)
def test_c5_concrete_cutoff_rejects_malformed_receipt_or_canonical_result(
    d24_application_db: ApplicationD24Database,
    mode: str,
) -> None:
    plan = d24_application_db.register_plan(tag=f"c5-invalid-{mode}")
    discovery = ControlledDiscovery(
        d24_application_db,
        call_work=DISCOVERY_WORK,
    )
    measurements = ControlledMeasurements()
    with d24_application_db.reconnect() as bound:
        ports = CorruptDiscoveryCutoffPorts(
            d24_application_db,
            bound.store,
            mode,
        )
        with pytest.raises(ValidationError):
            assemble_application(
                d24_application_db,
                discovery,
                ControlledVerifier(d24_application_db),
                measurements,
                runtime_override=ports,
                bound=bound,
            ).run_event(plan)

    assert ports.corrupted
    assert len(discovery.calls) == 1
    assert measurements.terminal_calls == []
    _assert_validation_added_no_write(d24_application_db, ports)


def test_c6_concrete_cutoff_rejects_malformed_terminal_lease_projection(
    d24_application_db: ApplicationD24Database,
) -> None:
    plan = d24_application_db.register_plan(
        tag="c6-invalid-lease",
        requirement_count=2,
    )
    discovery = ControlledDiscovery(
        d24_application_db,
        call_work=DISCOVERY_WORK,
    )
    measurements = ControlledMeasurements()
    with d24_application_db.reconnect() as bound:
        ports = CorruptLaterAcquisitionPorts(d24_application_db, bound.store)
        with pytest.raises(ValidationError):
            assemble_application(
                d24_application_db,
                discovery,
                ControlledVerifier(d24_application_db),
                measurements,
                runtime_override=ports,
                bound=bound,
            ).run_event(plan)

    assert ports.acquisition_count == 2
    assert len(discovery.calls) == 1
    assert measurements.terminal_calls == []
    _assert_validation_added_no_write(d24_application_db, ports)


def test_discovery_result_first_order_is_applied_before_ordinary_terminal_reconnect(
    d24_application_db: ApplicationD24Database,
) -> None:
    """The opposite C5 order has no terminal hash and gains no projection."""

    plan = d24_application_db.register_plan(tag="c5-result-first")
    discovery = ControlledDiscovery(
        d24_application_db,
        call_work=DISCOVERY_WORK,
    )
    measurements = ControlledMeasurements(
        transition_timing=None,
        terminal_timing=None,
    )
    with d24_application_db.reconnect() as bound:
        ports = ResultFirstThenFailurePorts(d24_application_db, bound.store)
        with pytest.raises((EventConflictError, InvalidEventError, ValidationError)):
            assemble_application(
                d24_application_db,
                discovery,
                ControlledVerifier(d24_application_db),
                measurements,
                runtime_override=ports,
                bound=bound,
            ).run_event(plan)

    assert len(discovery.calls) == 1
    assert ports.canonical is not None
    assert ports.canonical.event_work.requirement_forward_retrieval_call_count == 1
    assert (
        _relation_count(
            d24_application_db.connection,
            "groundloop_m5_post_terminal_attempt_audit",
            epoch_id=ports.canonical.epoch_id,
        )
        == 0
    )
    assert _telemetry_count(d24_application_db, ports.canonical.epoch_id) == 0

    replay, replay_measurements = _provider_free_replay(
        d24_application_db,
        plan,
        measurements,
    )
    assert replay.state is M5RunState.REPLAYED
    assert replay.call_work.is_zero
    assert replay.logical_result_hash == ports.canonical.logical_result_hash
    assert len(replay_measurements.terminal_calls) == 1


def test_verifier_result_first_order_stops_at_exact_d25_boundary(
    d24_application_db: ApplicationD24Database,
) -> None:
    """An active verifier return cannot commit first before M5-D25."""

    plan = d24_application_db.register_plan(tag="c5-verifier-first")
    discovery = ControlledDiscovery(d24_application_db, include_pair=True)
    verifier = ControlledVerifier(d24_application_db, call_work=VERIFIER_WORK)
    measurements = ControlledMeasurements()
    with pytest.raises(
        ValidationError,
        match="active verifier completion requires M5-D25 persisted matching",
    ):
        assemble_application(
            d24_application_db,
            discovery,
            verifier,
            measurements,
        ).run_event(plan)

    assert len(discovery.calls) == len(verifier.calls) == 1
    epoch_id = _epoch_id(d24_application_db, plan.structural_event_id)
    assert (
        _relation_count(
            d24_application_db.connection,
            "groundloop_m5_postcommit_invocation_telemetry",
            epoch_id=epoch_id,
        )
        == 0
    )
    verifier_rows = d24_application_db.connection.execute(
        """
        SELECT count(*)
        FROM groundloop_m5_attempt_execution_evidence AS evidence
        JOIN groundloop_m5_job_attempt AS attempt
          ON attempt.attempt_id = evidence.attempt_id
        JOIN groundloop_m5_semantic_job AS job
          ON job.logical_job_id = attempt.logical_job_id
        WHERE evidence.epoch_id = %s
          AND job.job_kind = 'verify_requirement_pair'
        """,
        (epoch_id,),
    ).fetchone()
    assert verifier_rows == (0,)
