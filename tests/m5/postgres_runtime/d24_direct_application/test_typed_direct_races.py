"""Live serialization falsifiers for the production typed-direct facade.

Every successful transition in this module uses the concrete PostgreSQL
facade.  Test-local wrappers only control which real transaction reaches the
shared lock set first or inject a failure after the read-only lock phases.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from threading import Event
from typing import Any, cast

import pytest
from psycopg import Connection
from psycopg.pq import TransactionStatus

import groundloop.m5.runtime.direct_m4 as direct_m4_module
from groundloop.errors import EventConflictError, InvalidEventError, ValidationError
from groundloop.events import ChunkInput, InsertDocumentEvent
from groundloop.m4.application import OpenEventReceipt
from groundloop.m4.contracts import (
    ChildClosure,
    JobCompletion,
    JobState,
    LogicalJobSpec,
    stable_m4_digest,
)
from groundloop.m5.events import legacy_event_payload_digest
from groundloop.m5.runtime import digests as runtime_digests
from groundloop.m5.runtime.application import M5DirectExecutionReceipt
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    ActiveChunkSnapshotEntry,
    M5AcquisitionDisposition,
    M5CheckedDirectTerminalFailureReceipt,
    M5EventRunResult,
    M5ExecutionEvidenceDisposition,
    M5ReplayedOutcome,
    M5RunFailureReason,
    M5RunState,
    M5RuntimeTiming,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5TypedDirectAcquisitionReceipt,
    M5TypedDirectJobLease,
    M5TypedDirectReturnKind,
    M5TypedDirectTerminalProjection,
    M5TypedEventPlan,
)
from groundloop.m5.runtime.postgres_direct_application import (
    M5PostgresDirectDiscoveryExecution,
    M5PostgresDirectExternalWorkFailure,
    M5PostgresDirectVerifierExecution,
)
from tests.m5.postgres_runtime.d24_direct.conftest import wait_until_expired
from tests.m5.postgres_runtime.d24_direct_application.conftest import (
    ATTEMPT_TIMING,
    BoundDirectApplication,
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

_PROVIDER_REASON = M5RunFailureReason.INVALID_ARTIFACT
_GENERIC_REASON = M5RunFailureReason.RETRIEVAL_ERROR
_DIRECT_REASON = "opaque-direct-provider-terminal/revision-9"
_ATTEMPT_WORK = M5RuntimeWork(
    direct_discovery_call_count=1,
    embedding_model_call_count=1,
    embedding_input_token_count=13,
)
_CALL_WORK = M5RuntimeWork(
    direct_discovery_call_count=1,
    embedding_model_call_count=1,
    embedding_input_token_count=13,
    bytes_hashed=17,
)
_CHECKED_ERRORS = (EventConflictError, InvalidEventError, ValidationError)


@dataclass(frozen=True, slots=True)
class _PreparedFailure:
    plan: M5TypedEventPlan
    opened: OpenEventReceipt
    acquisition: M5TypedDirectAcquisitionReceipt

    @property
    def epoch_id(self) -> int:
        return self.opened.epoch_id

    @property
    def expected_revision(self) -> int:
        return self.acquisition.lease.resulting_revision


def _prepare_failure(
    database: DirectApplicationD24Database,
    *,
    tag: str,
    plan: M5TypedEventPlan | None = None,
) -> _PreparedFailure:
    selected_plan = database.insert_plan(tag=tag) if plan is None else plan
    opened = open_event(database, selected_plan)
    roots = open_inputs(database, selected_plan).direct_roots
    assert roots
    acquisition = database.bound.adapter.acquire_direct_job_atomically(
        opened.epoch_id,
        1,
        roots[0],
    )
    assert acquisition.lease.disposition is M5AcquisitionDisposition.DISPATCH_NEW
    assert acquisition.lease.should_execute
    return _PreparedFailure(selected_plan, opened, acquisition)


def _combined_failure(
    bound: BoundDirectApplication,
    prepared: _PreparedFailure,
    *,
    expected_revision: int | None = None,
    job: LogicalJobSpec | None = None,
    lease: M5TypedDirectJobLease | None = None,
    direct_reason: str = _DIRECT_REASON,
    error_hash: str | None = None,
    attempt_work: M5RuntimeWork = _ATTEMPT_WORK,
    attempt_timing: M5RuntimeTiming | None = ATTEMPT_TIMING,
    requested_reason: M5RunFailureReason = _PROVIDER_REASON,
    call_work: M5RuntimeWork = _CALL_WORK,
) -> M5CheckedDirectTerminalFailureReceipt | M5TypedDirectAcquisitionReceipt:
    acquisition = prepared.acquisition
    return bound.adapter.fail_typed_epoch_after_direct_terminal_failure_atomically(
        prepared.epoch_id,
        prepared.expected_revision if expected_revision is None else expected_revision,
        acquisition.job if job is None else job,
        acquisition.lease if lease is None else lease,
        direct_reason,
        sha("typed-direct-race-provider-error") if error_hash is None else error_hash,
        attempt_work,
        attempt_timing,
        requested_reason,
        prepared.opened,
        call_work,
    )


def _generic_failure(
    bound: BoundDirectApplication,
    prepared: _PreparedFailure,
    *,
    expected_revision: int | None = None,
    reason: M5RunFailureReason = _GENERIC_REASON,
    call_work: M5RuntimeWork | None = None,
) -> M5EventRunResult:
    return bound.adapter.fail_typed_epoch_with_open_receipt_atomically(
        prepared.epoch_id,
        (
            prepared.expected_revision
            if expected_revision is None
            else expected_revision
        ),
        reason,
        prepared.opened,
        M5RuntimeWork() if call_work is None else call_work,
    )


def _serialize_calls(
    database: DirectApplicationD24Database,
    first: Callable[[BoundDirectApplication], object],
    second: Callable[[BoundDirectApplication], object],
) -> tuple[object, object]:
    """Hold the winning outer transaction until the competing call blocks."""

    with database.two_connections() as (left, right):
        started = Event()

        def blocked_second() -> object:
            started.set()
            return second(right)

        with ThreadPoolExecutor(max_workers=1) as pool:
            with left.connection.transaction():
                first_result = first(left)
                future = pool.submit(blocked_second)
                assert started.wait(timeout=5)
                assert not future.done()
            second_result = future.result(timeout=15)
    return first_result, second_result


def _job_rows(
    database: DirectApplicationD24Database,
    epoch_id: int,
) -> tuple[tuple[object, ...], ...]:
    with database.connection.transaction():
        return tuple(
            tuple(row)
            for row in database.connection.execute(
                """
                SELECT job.job_id, job.job_state, job.completed_revision,
                       projection.terminal_state,
                       projection.terminal_reason,
                       projection.completed_revision
                FROM groundloop_semantic_job AS job
                LEFT JOIN groundloop_m5_direct_terminal_projection AS projection
                  ON projection.epoch_id = job.epoch_id
                 AND projection.job_id = job.job_id
                WHERE job.epoch_id = %s
                ORDER BY job.job_id COLLATE "C"
                """,
                (epoch_id,),
            ).fetchall()
        )


def _job_family_snapshot(
    database: DirectApplicationD24Database,
    *,
    epoch_id: int,
    job_id: str,
) -> tuple[tuple[str, tuple[tuple[str, str], ...]], ...]:
    """Pin row bytes and xmins for one already-terminal direct job family."""

    with database.connection.transaction():
        image: list[tuple[str, tuple[tuple[str, str], ...]]] = []
        for table, predicate, parameters in (
            (
                "groundloop_semantic_job",
                "epoch_id = %s AND job_id = %s",
                (epoch_id, job_id),
            ),
            (
                "groundloop_semantic_job_attempt",
                "job_id = %s",
                (job_id,),
            ),
            (
                "groundloop_m5_dispatch_record",
                "epoch_id = %s AND logical_job_id = %s",
                (epoch_id, job_id),
            ),
            (
                "groundloop_m5_attempt_execution_evidence",
                "epoch_id = %s AND attempt_id IN "
                "(SELECT attempt_id FROM groundloop_semantic_job_attempt "
                " WHERE job_id = %s)",
                (epoch_id, job_id),
            ),
            (
                "groundloop_m5_runtime_timing_contribution",
                "epoch_id = %s AND attempt_id IN "
                "(SELECT attempt_id FROM groundloop_semantic_job_attempt "
                " WHERE job_id = %s)",
                (epoch_id, job_id),
            ),
            (
                "groundloop_m5_runtime_work_contribution",
                "epoch_id = %s AND (source_id IN "
                " (SELECT attempt_id FROM groundloop_semantic_job_attempt "
                "  WHERE job_id = %s) OR source_id IN "
                " (SELECT record_digest FROM groundloop_m5_dispatch_record "
                "  WHERE epoch_id = %s AND logical_job_id = %s) OR source_id IN "
                " (SELECT btrim(completion_digest) FROM groundloop_semantic_job "
                "  WHERE epoch_id = %s AND job_id = %s "
                "    AND completion_digest IS NOT NULL))",
                (epoch_id, job_id, epoch_id, job_id, epoch_id, job_id),
            ),
            (
                "groundloop_m5_direct_terminal_projection",
                "epoch_id = %s AND job_id = %s",
                (epoch_id, job_id),
            ),
        ):
            selected = database.connection.execute(
                f"SELECT xmin::text, row_to_json(stored)::text "
                f"FROM {table} AS stored WHERE {predicate} "
                'ORDER BY row_to_json(stored)::text COLLATE "C", xmin::text',
                parameters,
            ).fetchall()
            image.append(
                (
                    table,
                    tuple((str(row[0]), str(row[1])) for row in selected),
                )
            )
    return tuple(image)


def _multi_chunk_plan(
    database: DirectApplicationD24Database,
    *,
    tag: str,
    chunk_count: int = 3,
) -> M5TypedEventPlan:
    if chunk_count < 2:
        raise ValueError("multi-chunk race plan requires at least two chunks")
    original = database.insert_plan(tag=tag)
    event = original.event
    assert isinstance(event, InsertDocumentEvent)
    first = event.chunks[0]
    chunks = tuple(
        ChunkInput(
            first.chunk_version_id
            if ordinal == 0
            else f"{first.chunk_version_id}-{ordinal}",
            ordinal,
            f"{first.text} race-{ordinal}",
        )
        for ordinal in range(chunk_count)
    )
    concrete = replace(event, chunks=chunks)
    payload_hash = legacy_event_payload_digest(concrete)
    direct = original.direct_plan
    assert direct is not None
    checked_direct = replace(
        direct,
        update=replace(direct.update, payload_hash=payload_hash),
        inserted_chunk_version_ids=tuple(chunk.chunk_version_id for chunk in chunks),
    )
    active = ActiveChunkSnapshot.build(
        (
            *database.base.active_chunk_snapshot.entries,
            *(
                ActiveChunkSnapshotEntry.build(
                    chunk_version_id=chunk.chunk_version_id,
                    chunk_text=chunk.text,
                )
                for chunk in chunks
            ),
        )
    )
    return replace(
        original,
        event=concrete,
        payload_hash=payload_hash,
        direct_plan=checked_direct,
        active_chunk_snapshot=active,
    )


def _evaluation_failure_payload_hash(expected_revision: int) -> str:
    encoded = json.dumps(
        {
            "kind": "fail",
            "expected_revision": expected_revision,
            "scope_delta": 0,
            "claim_job_deltas": [],
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _assert_combined_accounting(
    database: DirectApplicationD24Database,
    prepared: _PreparedFailure,
    receipt: M5CheckedDirectTerminalFailureReceipt,
) -> None:
    epoch_id = prepared.epoch_id
    attempt = prepared.acquisition.attempt
    assert attempt is not None
    final_revision = prepared.expected_revision + 1
    transition_id = stable_m4_digest(
        "m4-evaluation-failure-v1",
        str(epoch_id),
        _PROVIDER_REASON.value,
    )
    forbidden_transition = stable_m4_digest(
        "m4-evaluation-terminal-failure-v1",
        str(epoch_id),
        prepared.acquisition.job.job_id,
        attempt.attempt_id,
    )
    with database.connection.transaction():
        assert tuple(
            database.connection.execute(
                """
                SELECT transition_id, btrim(payload_hash), transition_kind,
                       from_revision, to_revision, override_rows_written
                FROM groundloop_m4_evaluation_counter_transition
                WHERE epoch_id = %s AND to_revision = %s
                ORDER BY transition_id COLLATE "C"
                """,
                (epoch_id, final_revision),
            ).fetchall()
        ) == (
            (
                transition_id,
                _evaluation_failure_payload_hash(prepared.expected_revision),
                "fail",
                prepared.expected_revision,
                final_revision,
                0,
            ),
        )
        assert database.connection.execute(
            """
            SELECT count(*)
            FROM groundloop_m4_evaluation_counter_transition
            WHERE epoch_id = %s AND transition_id = %s
            """,
            (epoch_id, forbidden_transition),
        ).fetchone() == (0,)
        assert database.connection.execute(
            """
            SELECT applied_revision, btrim(source_identity_hash),
                   btrim(contribution_key_digest)
            FROM groundloop_m5_runtime_work_contribution
            WHERE epoch_id = %s
              AND contribution_kind = 'direct_attempt_execution'
              AND source_id = %s
            """,
            (epoch_id, attempt.attempt_id),
        ).fetchone() == (
            final_revision,
            receipt.direct_failure.execution_evidence_digest,
            receipt.direct_failure.attempt_execution_contribution_key_digest,
        )
        failure_source = runtime_digests.epoch_failure_contribution_source_digest(
            structural_event_id=prepared.plan.structural_event_id,
            failure_reason=_PROVIDER_REASON,
        )
        failure_key = runtime_digests.runtime_work_contribution_key_digest(
            epoch_id=epoch_id,
            contribution_kind=M5RuntimeWorkContributionKind.EPOCH_FAILURE,
            source_id=prepared.plan.structural_event_id,
        )
        assert tuple(
            database.connection.execute(
                """
                SELECT contribution_kind, source_id,
                       btrim(source_identity_hash),
                       btrim(contribution_key_digest), applied_revision,
                       btrim(work_digest)
                FROM groundloop_m5_runtime_work_contribution
                WHERE epoch_id = %s
                  AND contribution_kind IN ('epoch_failure', 'seal')
                ORDER BY contribution_kind COLLATE "C", source_id COLLATE "C"
                """,
                (epoch_id,),
            ).fetchall()
        ) == (
            (
                M5RuntimeWorkContributionKind.EPOCH_FAILURE.value,
                prepared.plan.structural_event_id,
                failure_source,
                failure_key,
                final_revision,
                M5RuntimeWork().work_digest,
            ),
        )
        assert database.connection.execute(
            """
            SELECT required_interval_observed,
                   coordinator_non_db_non_neural_ns, neural_wall_ns,
                   postgres_roundtrip_wall_ns, external_io_wall_ns,
                   end_to_end_wall_ns
            FROM groundloop_m5_runtime_timing_contribution
            WHERE epoch_id = %s AND subgraph = 'direct' AND attempt_id = %s
            """,
            (epoch_id, attempt.attempt_id),
        ).fetchone() == (
            True,
            ATTEMPT_TIMING.coordinator_non_db_non_neural_ns,
            ATTEMPT_TIMING.neural_wall_ns,
            ATTEMPT_TIMING.postgres_roundtrip_wall_ns,
            ATTEMPT_TIMING.external_io_wall_ns,
            ATTEMPT_TIMING.end_to_end_wall_ns,
        )
        transition_timing = tuple(
            database.connection.execute(
                """
                SELECT contribution_kind, anchor_revision,
                       required_interval_observed
                FROM groundloop_m5_transition_call_timing
                WHERE epoch_id = %s
                ORDER BY anchor_revision, contribution_kind COLLATE "C"
                """,
                (epoch_id,),
            ).fetchall()
        )
        assert (
            "direct_acquisition",
            prepared.expected_revision,
            False,
        ) in transition_timing
        # Terminal anchors are represented by the canonical-zero work
        # contribution and the accumulator's immediate missing point.  The
        # transition-call table intentionally excludes terminal revisions.
        assert not any(row[0] in {"epoch_failure", "seal"} for row in transition_timing)
        timing = database.connection.execute(
            """
            SELECT required_expected_count, required_observed_count,
                   required_missing_count, pending_contribution_kind,
                   pending_source_id, pending_contribution_key_digest,
                   pending_anchor_revision, updated_revision, terminalized
            FROM groundloop_m5_runtime_timing_accumulator
            WHERE epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone()
        assert timing is not None
        assert timing[:3] == (4, 1, 3)
        assert timing[3:7] == (None, None, None, None)
        assert timing[7:] == (final_revision, True)


def _corrupt_cancelled_projection_reason(
    database: DirectApplicationD24Database,
    *,
    epoch_id: int,
    job_id: str,
    final_revision: int,
) -> None:
    corrupted = M5TypedDirectTerminalProjection.build(
        job_id=job_id,
        terminal_state=JobState.CANCELLED,
        terminal_reason="corrupted-epoch-failure-wire",
        m4_completion_digest=None,
        completed_revision=final_revision,
    )
    with database.connection.transaction():
        database.connection.execute(
            """
            ALTER TABLE groundloop_m5_direct_terminal_projection
            DISABLE TRIGGER groundloop_m5_direct_terminal_projection_immutable
            """
        )
        database.connection.execute(
            """
            UPDATE groundloop_m5_direct_terminal_projection
            SET terminal_reason = %s, terminal_identity_hash = %s
            WHERE epoch_id = %s AND job_id = %s
            """,
            (
                corrupted.terminal_reason,
                corrupted.terminal_identity_hash,
                epoch_id,
                job_id,
            ),
        )
        database.connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        database.connection.execute(
            """
            ALTER TABLE groundloop_m5_direct_terminal_projection
            ENABLE TRIGGER groundloop_m5_direct_terminal_projection_immutable
            """
        )


@dataclass(slots=True)
class _ScriptedDiscovery:
    database: DirectApplicationD24Database
    failure_call: int
    retryable: bool
    calls: list[str] = field(default_factory=list)
    connection: Connection[Any] | None = None

    def discover_direct(
        self,
        epoch_id: int,
        acquisition: M5TypedDirectAcquisitionReceipt,
        event: M5TypedEventPlan,
    ) -> M5PostgresDirectDiscoveryExecution:
        self.calls.append(acquisition.job.job_id)
        if len(self.calls) == self.failure_call:
            raise M5PostgresDirectExternalWorkFailure(
                (
                    M5RunFailureReason.RETRIEVAL_UNAVAILABLE
                    if self.retryable
                    else _PROVIDER_REASON
                ),
                retryable=self.retryable,
                direct_terminal_reason=None if self.retryable else _DIRECT_REASON,
                call_work=_ATTEMPT_WORK,
                attempt_timing=ATTEMPT_TIMING,
                error_hash=sha(
                    f"scripted-direct-error:{epoch_id}:{acquisition.job.job_id}"
                ),
            )
        delegate = ControlledDirectDiscovery(
            self.database,
            connection=self.connection,
        )
        return delegate.discover_direct(epoch_id, acquisition, event)


def _complete_one_direct_root(
    database: DirectApplicationD24Database,
    bound: BoundDirectApplication,
    plan: M5TypedEventPlan,
    opened: OpenEventReceipt,
    root: LogicalJobSpec,
    *,
    expected_revision: int,
) -> int:
    """Commit one real root before constructing a later failure closure."""

    acquisition = bound.adapter.acquire_direct_job_atomically(
        opened.epoch_id,
        expected_revision,
        root,
    )
    assert acquisition.lease.disposition is M5AcquisitionDisposition.DISPATCH_NEW
    assert acquisition.lease.should_execute
    provider = ControlledDirectDiscovery(
        database,
        connection=bound.connection,
    )
    execution = provider.discover_direct(opened.epoch_id, acquisition, plan)
    assert not execution.result.admitted_pairs
    closure = ChildClosure.build(
        parent_job_id=root.job_id,
        result_artifact_hash=execution.result.result_artifact_hash,
        child_job_ids=(),
    )
    completion = JobCompletion.build(
        job_id=root.job_id,
        payload_hash=root.payload_hash,
        execution_spec_hash=root.execution_spec_hash,
        result_artifact_id=execution.result.result_artifact_id,
        result_artifact_hash=execution.result.result_artifact_hash,
        terminal_state=JobState.COMPLETED_ACTIVE,
        child_closure=closure,
    )
    envelope = bound.facade._discovery_envelope(
        opened.epoch_id,
        plan,
        acquisition,
        execution.result,
        completion,
    )
    settled = bound.adapter.settle_direct_expansion_atomically(
        opened.epoch_id,
        acquisition.lease.resulting_revision,
        acquisition.lease,
        envelope,
        (),
        M5ExecutionEvidenceDisposition.RETURNED,
        execution.call_work,
        execution.attempt_timing,
    )
    assert settled.normal is not None
    assert settled.late is None
    assert settled.normal.current_terminal_logical_result_hash is None
    return settled.normal.resulting_revision


@dataclass(slots=True)
class _DirectCutoffContext:
    database: DirectApplicationD24Database
    events: list[str] = field(default_factory=list)
    open_receipt: OpenEventReceipt | None = None
    winner: M5EventRunResult | None = None
    canonical: M5EventRunResult | None = None
    direct_result: M5DirectExecutionReceipt | None = None

    def commit_generic_failure(self, epoch_id: int) -> None:
        held = self.open_receipt
        if held is None:
            raise AssertionError("provider cutoff lacks the held open receipt")
        self.events.append("terminal_origin")
        with self.database.reconnect() as competitor:
            revision = competitor.facade.current_revision(epoch_id)
            winner = competitor.adapter.fail_typed_epoch_with_open_receipt_atomically(
                epoch_id,
                revision,
                _GENERIC_REASON,
                held,
                M5RuntimeWork(),
            )
            assert winner.state is M5RunState.FAILED
            assert winner.open_receipt is held
            canonical = competitor.store.read_typed_event_result(
                winner.event_id,
                winner.payload_hash,
            )
            assert canonical is not None
            assert canonical.state is M5RunState.REPLAYED
            assert canonical.call_work.is_zero
        self.winner = winner
        self.canonical = canonical


class _CutoffDiscovery(ControlledDirectDiscovery):
    def __init__(
        self,
        database: DirectApplicationD24Database,
        context: _DirectCutoffContext,
    ) -> None:
        super().__init__(database)
        self._cutoff_context = context

    def discover_direct(
        self,
        epoch_id: int,
        acquisition: M5TypedDirectAcquisitionReceipt,
        event: M5TypedEventPlan,
    ) -> M5PostgresDirectDiscoveryExecution:
        execution = super().discover_direct(epoch_id, acquisition, event)
        self._cutoff_context.events.append("provider_success")
        self._cutoff_context.commit_generic_failure(epoch_id)
        return execution


class _CutoffVerifier(ControlledDirectVerifier):
    def __init__(
        self,
        database: DirectApplicationD24Database,
        context: _DirectCutoffContext,
    ) -> None:
        super().__init__(database)
        self._cutoff_context = context

    def verify_direct(
        self,
        epoch_id: int,
        acquisition: M5TypedDirectAcquisitionReceipt,
        event: M5TypedEventPlan,
    ) -> M5PostgresDirectVerifierExecution:
        execution = super().verify_direct(epoch_id, acquisition, event)
        self._cutoff_context.events.append("provider_success")
        self._cutoff_context.commit_generic_failure(epoch_id)
        return execution


class _LosingFailureDiscovery(ControlledDirectDiscovery):
    def __init__(
        self,
        database: DirectApplicationD24Database,
        context: _DirectCutoffContext,
    ) -> None:
        super().__init__(database)
        self._cutoff_context = context

    def discover_direct(
        self,
        epoch_id: int,
        acquisition: M5TypedDirectAcquisitionReceipt,
        event: M5TypedEventPlan,
    ) -> M5PostgresDirectDiscoveryExecution:
        super().discover_direct(epoch_id, acquisition, event)
        self._cutoff_context.events.append("provider_failure")
        self._cutoff_context.commit_generic_failure(epoch_id)
        raise M5PostgresDirectExternalWorkFailure(
            _PROVIDER_REASON,
            retryable=False,
            direct_terminal_reason=_DIRECT_REASON,
            call_work=self.call_work,
            attempt_timing=self.attempt_timing,
            error_hash=sha(f"losing-discovery:{epoch_id}:{acquisition.job.job_id}"),
        )


class _LosingFailureVerifier(ControlledDirectVerifier):
    def __init__(
        self,
        database: DirectApplicationD24Database,
        context: _DirectCutoffContext,
    ) -> None:
        super().__init__(database)
        self._cutoff_context = context

    def verify_direct(
        self,
        epoch_id: int,
        acquisition: M5TypedDirectAcquisitionReceipt,
        event: M5TypedEventPlan,
    ) -> M5PostgresDirectVerifierExecution:
        super().verify_direct(epoch_id, acquisition, event)
        self._cutoff_context.events.append("provider_failure")
        self._cutoff_context.commit_generic_failure(epoch_id)
        raise M5PostgresDirectExternalWorkFailure(
            _PROVIDER_REASON,
            retryable=False,
            direct_terminal_reason=_DIRECT_REASON,
            call_work=self.call_work,
            attempt_timing=self.attempt_timing,
            error_hash=sha(f"losing-verifier:{epoch_id}:{acquisition.job.job_id}"),
        )


@dataclass(slots=True)
class _RecordingMeasurements:
    delegate: Any
    events: list[str]

    def transition_call_timing(self, anchor: Any) -> Any:
        return self.delegate.transition_call_timing(anchor)

    def terminal_invocation(self, event: Any, result: Any) -> Any:
        self.events.append("measurement")
        return self.delegate.terminal_invocation(event, result)


@pytest.mark.parametrize("first_committer", ("provider", "generic"))
@pytest.mark.parametrize(
    "call_work",
    (M5RuntimeWork(), _CALL_WORK),
    ids=("zero-work", "nonzero-work"),
)
def test_provider_failure_and_generic_failure_serialize_in_both_orders(
    d24_direct_application_db: DirectApplicationD24Database,
    first_committer: str,
    call_work: M5RuntimeWork,
) -> None:
    database = d24_direct_application_db
    prepared = _prepare_failure(database, tag=f"provider-generic-{first_committer}")

    if first_committer == "provider":
        first, second = _serialize_calls(
            database,
            lambda bound: _combined_failure(bound, prepared, call_work=call_work),
            lambda bound: _generic_failure(
                bound,
                prepared,
                reason=_PROVIDER_REASON,
                call_work=call_work,
            ),
        )
        assert type(first) is M5CheckedDirectTerminalFailureReceipt
        assert first.terminal_result.state is M5RunState.FAILED
        assert type(second) is M5EventRunResult
        replay = second
        assert replay.state is M5RunState.REPLAYED
        assert replay.replayed_outcome is M5ReplayedOutcome.FAILED
        assert first.terminal_result.call_work == call_work
        assert replay.call_work.is_zero
        expected_state = JobState.TERMINAL_FAILED
    else:
        first, second = _serialize_calls(
            database,
            lambda bound: _generic_failure(
                bound,
                prepared,
                call_work=call_work,
            ),
            lambda bound: _combined_failure(
                bound,
                prepared,
                call_work=call_work,
            ),
        )
        assert type(first) is M5EventRunResult
        assert first.state is M5RunState.FAILED
        assert first.call_work == call_work
        assert type(second) is M5TypedDirectAcquisitionReceipt
        loser = second
        assert loser.lease.disposition is M5AcquisitionDisposition.TERMINAL
        assert loser.lease.terminal_projection is not None
        assert loser.lease.terminal_projection.terminal_state is JobState.CANCELLED
        assert loser.lease.terminal_projection.terminal_reason == "epoch_failed"
        expected_state = JobState.CANCELLED

    rows = _job_rows(database, prepared.epoch_id)
    assert len(rows) == 1
    assert rows[0][1] == expected_state.value
    assert rows[0][3] == expected_state.value


@pytest.mark.parametrize(
    "call_work",
    (M5RuntimeWork(), _CALL_WORK),
    ids=("zero-work", "nonzero-work"),
)
def test_same_attempt_provider_failures_serialize_as_first_and_checked_replay(
    d24_direct_application_db: DirectApplicationD24Database,
    call_work: M5RuntimeWork,
) -> None:
    database = d24_direct_application_db
    prepared = _prepare_failure(database, tag="combined-race")
    first, second = _serialize_calls(
        database,
        lambda bound: _combined_failure(bound, prepared, call_work=call_work),
        lambda bound: _combined_failure(bound, prepared, call_work=call_work),
    )
    assert type(first) is M5CheckedDirectTerminalFailureReceipt
    assert type(second) is M5CheckedDirectTerminalFailureReceipt
    winner = first
    replay = second
    assert winner.terminal_result.state is M5RunState.FAILED
    assert winner.terminal_result.call_work == call_work
    assert replay.terminal_result.state is M5RunState.REPLAYED
    assert replay.terminal_result.replayed_outcome is M5ReplayedOutcome.FAILED
    assert replay.terminal_result.call_work.is_zero
    assert winner.direct_failure == replay.direct_failure
    assert winner.resulting_revision == replay.resulting_revision
    assert (
        winner.terminal_result.logical_result_hash
        == replay.terminal_result.logical_result_hash
    )
    _assert_combined_accounting(database, prepared, winner)


def test_checked_terminal_failure_replays_exactly_on_reconnect(
    d24_direct_application_db: DirectApplicationD24Database,
) -> None:
    database = d24_direct_application_db
    prepared = _prepare_failure(database, tag="checked-reconnect")
    first = _combined_failure(database.bound, prepared)
    assert type(first) is M5CheckedDirectTerminalFailureReceipt
    before = database_snapshot(database.connection)

    with database.reconnect() as rebound:
        replay = _combined_failure(rebound, prepared)

    assert type(replay) is M5CheckedDirectTerminalFailureReceipt
    checked_first = first
    checked_replay = replay
    assert checked_replay.terminal_result.state is M5RunState.REPLAYED
    assert checked_replay.terminal_result.replayed_outcome is M5ReplayedOutcome.FAILED
    assert checked_replay.direct_failure == checked_first.direct_failure
    assert (
        checked_replay.requested_failure_reason
        is checked_first.requested_failure_reason
    )
    assert checked_replay.resulting_revision == checked_first.resulting_revision
    assert (
        checked_replay.terminal_result.logical_result_hash
        == checked_first.terminal_result.logical_result_hash
    )
    assert checked_replay.terminal_result.call_work.is_zero
    assert database_snapshot(database.connection) == before


@pytest.mark.parametrize("route", ("generic", "combined"))
@pytest.mark.parametrize("resumed", (False, True), ids=("fresh", "resumed"))
@pytest.mark.parametrize(
    "call_work",
    (M5RuntimeWork(), _CALL_WORK),
    ids=("zero-work", "nonzero-work"),
)
def test_first_failure_returns_exact_fresh_or_resumed_held_receipt(
    d24_direct_application_db: DirectApplicationD24Database,
    route: str,
    resumed: bool,
    call_work: M5RuntimeWork,
) -> None:
    database = d24_direct_application_db
    prepared = _prepare_failure(
        database,
        tag=f"held-{route}-{'resumed' if resumed else 'fresh'}-"
        f"{'work' if not call_work.is_zero else 'zero'}",
    )
    if resumed:
        resumed_open = open_event(database, prepared.plan)
        assert resumed_open.replayed is True
        assert resumed_open is not prepared.opened
        prepared = replace(prepared, opened=resumed_open)
    else:
        assert prepared.opened.replayed is False

    if route == "generic":
        result = _generic_failure(
            database.bound,
            prepared,
            call_work=call_work,
        )
        assert result.state is M5RunState.FAILED
        assert result.open_receipt is prepared.opened
        assert result.call_work == call_work
        assert result.epoch_id == prepared.epoch_id
        assert result.failure_reason is _GENERIC_REASON
    elif route == "combined":
        combined = _combined_failure(
            database.bound,
            prepared,
            call_work=call_work,
        )
        assert type(combined) is M5CheckedDirectTerminalFailureReceipt
        assert combined.terminal_result.state is M5RunState.FAILED
        assert combined.terminal_result.open_receipt is prepared.opened
        assert combined.terminal_result.call_work == call_work
        assert combined.requested_failure_reason is _PROVIDER_REASON
    else:
        raise AssertionError(f"unknown first failure route: {route}")


@pytest.mark.parametrize(
    ("route", "expected_state", "expected_direct_reason"),
    (
        ("terminal_failed", JobState.TERMINAL_FAILED, _DIRECT_REASON),
        ("terminal_failed_epoch_reason", JobState.TERMINAL_FAILED, "epoch_failed"),
        ("cancelled_epoch_failed", JobState.CANCELLED, "epoch_failed"),
    ),
)
def test_live_terminal_acquisition_projects_only_canonical_failed_authority(
    d24_direct_application_db: DirectApplicationD24Database,
    route: str,
    expected_state: JobState,
    expected_direct_reason: str,
) -> None:
    database = d24_direct_application_db
    prepared = _prepare_failure(database, tag=f"terminal-acquisition-{route}")
    if expected_state is JobState.CANCELLED:
        failed = _generic_failure(database.bound, prepared)
        assert failed.state is M5RunState.FAILED
        final_revision = prepared.expected_revision + 1
    else:
        combined = _combined_failure(
            database.bound,
            prepared,
            direct_reason=expected_direct_reason,
        )
        assert type(combined) is M5CheckedDirectTerminalFailureReceipt
        assert combined.terminal_result.state is M5RunState.FAILED
        final_revision = combined.resulting_revision
    before = database_snapshot(database.connection)

    selected = database.facade.run_pending_direct(
        prepared.epoch_id,
        final_revision,
        prepared.plan,
        prepared.opened,
    )
    acquisition = selected.selected_terminal_acquisition_receipt
    assert acquisition is not None
    assert acquisition.job == prepared.acquisition.job
    assert acquisition.lease.disposition is M5AcquisitionDisposition.TERMINAL
    projection = acquisition.lease.terminal_projection
    assert projection is not None
    assert projection.terminal_state is expected_state
    assert projection.terminal_reason == expected_direct_reason
    assert selected.resulting_revision == final_revision
    assert selected.call_work.is_zero
    assert database_snapshot(database.connection) == before

    canonical = database.bound.store.read_typed_event_result(
        prepared.plan.structural_event_id,
        prepared.plan.payload_hash,
    )
    assert canonical is not None
    assert canonical.state is M5RunState.REPLAYED
    assert canonical.replayed_outcome is M5ReplayedOutcome.FAILED
    assert canonical.failure_reason is (
        _GENERIC_REASON if expected_state is JobState.CANCELLED else _PROVIDER_REASON
    )
    assert canonical.logical_result_hash is not None
    assert canonical.call_work.is_zero


@pytest.mark.parametrize("provider_kind", ("discovery", "verifier"))
def test_top_level_successful_direct_return_loses_generic_terminal_cutoff_once(
    d24_direct_application_db: DirectApplicationD24Database,
    monkeypatch: pytest.MonkeyPatch,
    provider_kind: str,
) -> None:
    database = d24_direct_application_db
    context = _DirectCutoffContext(database)
    discovery: ControlledDirectDiscovery
    verifier: ControlledDirectVerifier
    if provider_kind == "discovery":
        discovery = _CutoffDiscovery(database, context)
        verifier = ControlledDirectVerifier(database)
        expected_kind = M5TypedDirectReturnKind.DISCOVERY
    elif provider_kind == "verifier":
        discovery = ControlledDirectDiscovery(database, include_pair=True)
        verifier = _CutoffVerifier(database, context)
        expected_kind = M5TypedDirectReturnKind.VERIFIER
    else:
        raise AssertionError(f"unknown direct cutoff provider: {provider_kind}")
    baseline_measurements = cast(ControlledMeasurements, database.bound.measurements)
    recording_measurements = _RecordingMeasurements(
        baseline_measurements,
        context.events,
    )
    bound = database.bind(
        database.connection,
        discovery=discovery,
        verifier=verifier,
        measurements=recording_measurements,
    )
    assembled = assemble_typed_application(
        database,
        facade=bound.facade,
        measurements=recording_measurements,
    )
    plan = database.insert_plan(tag=f"top-level-cutoff-{provider_kind}")

    original_direct = bound.facade.run_pending_direct

    def capture_direct(
        epoch_id: int,
        expected_revision: int,
        event: M5TypedEventPlan,
        open_receipt: OpenEventReceipt,
    ) -> M5DirectExecutionReceipt:
        context.open_receipt = open_receipt
        receipt = original_direct(
            epoch_id,
            expected_revision,
            event,
            open_receipt,
        )
        context.direct_result = receipt
        context.events.append("direct_selected")
        return receipt

    original_validation = assembled.application._validate_selected_direct_outer_receipt

    def capture_validation(receipt: Any) -> Any:
        context.events.append("validation")
        return original_validation(receipt)

    original_telemetry = bound.facade.append_terminal_invocation_telemetry

    def capture_telemetry(*args: Any, **kwargs: Any) -> Any:
        context.events.append("telemetry")
        return original_telemetry(*args, **kwargs)

    monkeypatch.setattr(bound.facade, "run_pending_direct", capture_direct)
    monkeypatch.setattr(
        type(assembled.application),
        "_validate_selected_direct_outer_receipt",
        staticmethod(capture_validation),
    )
    monkeypatch.setattr(
        bound.facade,
        "append_terminal_invocation_telemetry",
        capture_telemetry,
    )

    result = assembled.application.run_event(plan)
    assert result.state is M5RunState.REPLAYED
    assert result.replayed_outcome is M5ReplayedOutcome.FAILED
    assert result.failure_reason is _GENERIC_REASON
    assert result.open_receipt is context.open_receipt
    assert result.logical_result_hash is not None
    assert context.canonical is not None
    assert result.logical_result_hash == context.canonical.logical_result_hash
    assert context.canonical.call_work.is_zero
    for field_name in (
        "event_work",
        "event_timing",
        "event_timing_coverage",
        "combined_deltas",
        "changed_state_references",
        "logical_result_hash",
    ):
        assert getattr(result, field_name) == getattr(
            context.canonical,
            field_name,
        )
    durable_after = database.bound.store.read_typed_event_result(
        plan.structural_event_id,
        plan.payload_hash,
    )
    assert durable_after == context.canonical
    direct_result = context.direct_result
    assert direct_result is not None
    selected = direct_result.selected_successful_outer_receipt
    assert selected is not None
    assert selected.return_kind is expected_kind
    assert selected.normal is None
    assert selected.late is not None
    assert direct_result.selected_successful_outer_return_kind is expected_kind
    assert direct_result.selected_successful_outer_job_id is not None
    assert direct_result.selected_terminal_acquisition_receipt is None
    assert direct_result.selected_checked_combined_failure_receipt is None
    assert direct_result.blocked_reason is None
    assert direct_result.terminal_failure_reason is None
    assert result.call_work == direct_result.call_work
    assert not result.call_work.is_zero
    assert context.events[:3] == [
        "provider_success",
        "terminal_origin",
        "direct_selected",
    ]
    validation_events = context.events[3:-2]
    assert validation_events
    assert all(event == "validation" for event in validation_events)
    assert context.events[-2:] == ["measurement", "telemetry"]
    assert context.events.count("measurement") == 1
    assert context.events.count("telemetry") == 1
    assert len(baseline_measurements.terminal_calls) == 1
    assert all(
        status is TransactionStatus.IDLE
        for status in (*discovery.transaction_statuses, *verifier.transaction_statuses)
    )
    assert len(discovery.transaction_statuses) == 1
    assert len(verifier.transaction_statuses) == (
        0 if provider_kind == "discovery" else 1
    )
    with database.connection.transaction():
        assert database.connection.execute(
            """
            SELECT count(*)
            FROM groundloop_m5_postcommit_invocation_telemetry
            WHERE structural_event_id = %s
              AND terminal_logical_result_hash = %s
            """,
            (plan.structural_event_id, result.logical_result_hash),
        ).fetchone() == (1,)


@pytest.mark.parametrize("provider_kind", ("discovery", "verifier"))
@pytest.mark.parametrize("resumed", (False, True), ids=("fresh", "resumed"))
def test_top_level_nonretryable_provider_failure_loser_is_invocation_only(
    d24_direct_application_db: DirectApplicationD24Database,
    monkeypatch: pytest.MonkeyPatch,
    provider_kind: str,
    resumed: bool,
) -> None:
    database = d24_direct_application_db
    context = _DirectCutoffContext(database)
    discovery: ControlledDirectDiscovery
    verifier: ControlledDirectVerifier
    if provider_kind == "discovery":
        discovery = _LosingFailureDiscovery(database, context)
        verifier = ControlledDirectVerifier(database)
    elif provider_kind == "verifier":
        discovery = ControlledDirectDiscovery(database, include_pair=True)
        verifier = _LosingFailureVerifier(database, context)
    else:
        raise AssertionError(f"unknown losing provider: {provider_kind}")
    baseline_measurements = cast(ControlledMeasurements, database.bound.measurements)
    recording_measurements = _RecordingMeasurements(
        baseline_measurements,
        context.events,
    )
    bound = database.bind(
        database.connection,
        discovery=discovery,
        verifier=verifier,
        measurements=recording_measurements,
    )
    plan = database.insert_plan(
        tag=f"top-losing-{provider_kind}-{'resumed' if resumed else 'fresh'}"
    )
    if resumed:
        first_open = open_event(database, plan, facade=bound.facade)
        assert first_open.replayed is False
    assembled = assemble_typed_application(
        database,
        facade=bound.facade,
        measurements=recording_measurements,
    )
    original_direct = bound.facade.run_pending_direct

    def capture_direct(
        epoch_id: int,
        expected_revision: int,
        event: M5TypedEventPlan,
        open_receipt: OpenEventReceipt,
    ) -> M5DirectExecutionReceipt:
        context.open_receipt = open_receipt
        receipt = original_direct(
            epoch_id,
            expected_revision,
            event,
            open_receipt,
        )
        context.direct_result = receipt
        context.events.append("direct_selected")
        return receipt

    original_validation = type(
        assembled.application
    )._validate_active_terminal_projection

    def capture_validation(_cls: Any, *args: Any, **kwargs: Any) -> Any:
        context.events.append("validation")
        return original_validation(*args, **kwargs)

    original_telemetry = bound.facade.append_terminal_invocation_telemetry

    def capture_telemetry(*args: Any, **kwargs: Any) -> Any:
        context.events.append("telemetry")
        return original_telemetry(*args, **kwargs)

    monkeypatch.setattr(bound.facade, "run_pending_direct", capture_direct)
    monkeypatch.setattr(
        type(assembled.application),
        "_validate_active_terminal_projection",
        classmethod(capture_validation),
    )
    monkeypatch.setattr(
        bound.facade,
        "append_terminal_invocation_telemetry",
        capture_telemetry,
    )

    result = assembled.application.run_event(plan)
    assert result.state is M5RunState.REPLAYED
    assert result.replayed_outcome is M5ReplayedOutcome.FAILED
    assert result.failure_reason is _GENERIC_REASON
    assert result.open_receipt is context.open_receipt
    assert result.open_receipt.replayed is resumed
    assert not result.call_work.is_zero
    direct_result = context.direct_result
    assert direct_result is not None
    assert result.call_work == direct_result.call_work
    selected = direct_result.selected_terminal_acquisition_receipt
    assert selected is not None
    assert selected.lease.disposition is M5AcquisitionDisposition.TERMINAL
    projection = selected.lease.terminal_projection
    assert projection is not None
    assert projection.terminal_state is JobState.CANCELLED
    assert projection.terminal_reason == "epoch_failed"
    assert direct_result.selected_checked_combined_failure_receipt is None
    assert direct_result.selected_successful_outer_receipt is None
    assert context.events == [
        "provider_failure",
        "terminal_origin",
        "direct_selected",
        "validation",
        "measurement",
        "telemetry",
    ]

    canonical = context.canonical
    assert canonical is not None
    assert canonical.call_work.is_zero
    for field_name in (
        "event_work",
        "event_timing",
        "event_timing_coverage",
        "combined_deltas",
        "changed_state_references",
        "logical_result_hash",
    ):
        assert getattr(result, field_name) == getattr(canonical, field_name)
    if provider_kind == "discovery":
        assert result.call_work.direct_discovery_call_count == 1
        assert canonical.event_work.direct_discovery_call_count == 0
        assert discovery.calls and verifier.calls == []
    else:
        assert result.call_work.direct_verifier_call_count == 1
        assert canonical.event_work.direct_verifier_call_count == 0
        assert discovery.calls and verifier.calls
    durable_after = database.bound.store.read_typed_event_result(
        plan.structural_event_id,
        plan.payload_hash,
    )
    assert durable_after == canonical
    assert len(baseline_measurements.terminal_calls) == 1
    with database.connection.transaction():
        assert database.connection.execute(
            """
            SELECT count(*)
            FROM groundloop_m5_postcommit_invocation_telemetry
            WHERE structural_event_id = %s
              AND terminal_logical_result_hash = %s
            """,
            (plan.structural_event_id, result.logical_result_hash),
        ).fetchone() == (1,)


@pytest.mark.parametrize(
    ("route", "expected_state", "direct_reason"),
    (
        ("generic", JobState.CANCELLED, "epoch_failed"),
        ("combined", JobState.TERMINAL_FAILED, _DIRECT_REASON),
        ("combined_overlap", JobState.TERMINAL_FAILED, "epoch_failed"),
    ),
)
@pytest.mark.parametrize("resumed", (False, True), ids=("fresh", "resumed"))
@pytest.mark.parametrize(
    "prior_work",
    (False, True),
    ids=("zero-prior-work", "nonzero-prior-work"),
)
def test_top_level_terminal_acquisition_preserves_active_held_work_projection(
    d24_direct_application_db: DirectApplicationD24Database,
    monkeypatch: pytest.MonkeyPatch,
    route: str,
    expected_state: JobState,
    direct_reason: str,
    resumed: bool,
    prior_work: bool,
) -> None:
    database = d24_direct_application_db
    discovery = ControlledDirectDiscovery(database)
    verifier = ControlledDirectVerifier(database)
    bound = database.bind(
        database.connection,
        discovery=discovery,
        verifier=verifier,
    )
    plan = database.insert_plan(
        tag=(
            f"top-terminal-{route}-{'resumed' if resumed else 'fresh'}-"
            f"{'work' if prior_work else 'zero'}"
        ),
        chunk_count=2 if prior_work else 1,
    )
    if resumed:
        first_open = open_event(database, plan, facade=bound.facade)
        assert first_open.replayed is False
    assembled = assemble_typed_application(database, facade=bound.facade)
    context = _DirectCutoffContext(database)
    cutoff_ordinal = 2 if prior_work else 1
    acquire_count = 0
    original_acquire = bound.adapter.acquire_direct_job_atomically
    original_direct = bound.facade.run_pending_direct

    def capture_direct(
        epoch_id: int,
        expected_revision: int,
        event: M5TypedEventPlan,
        open_receipt: OpenEventReceipt,
    ) -> M5DirectExecutionReceipt:
        context.open_receipt = open_receipt
        receipt = original_direct(
            epoch_id,
            expected_revision,
            event,
            open_receipt,
        )
        context.direct_result = receipt
        return receipt

    def cutoff_acquire(
        epoch_id: int,
        expected_revision: int,
        job: LogicalJobSpec,
    ) -> M5TypedDirectAcquisitionReceipt:
        nonlocal acquire_count
        acquire_count += 1
        if acquire_count == cutoff_ordinal:
            held = context.open_receipt
            if held is None:
                raise AssertionError("terminal acquisition race lost its held open")
            with database.reconnect() as competitor:
                current = competitor.facade.current_revision(epoch_id)
                if route == "generic":
                    fail_generic = (
                        competitor.adapter.fail_typed_epoch_with_open_receipt_atomically
                    )
                    winner = fail_generic(
                        epoch_id,
                        current,
                        _GENERIC_REASON,
                        held,
                        M5RuntimeWork(),
                    )
                    assert winner.state is M5RunState.FAILED
                elif route in {"combined", "combined_overlap"}:
                    competing_acquisition = (
                        competitor.adapter.acquire_direct_job_atomically(
                            epoch_id,
                            current,
                            job,
                        )
                    )
                    combined = _combined_failure(
                        competitor,
                        _PreparedFailure(plan, held, competing_acquisition),
                        direct_reason=direct_reason,
                        call_work=M5RuntimeWork(),
                    )
                    assert type(combined) is M5CheckedDirectTerminalFailureReceipt
                    winner = combined.terminal_result
                    assert winner.state is M5RunState.FAILED
                else:
                    raise AssertionError(f"unknown terminal acquisition route: {route}")
                assert winner.open_receipt is held
                canonical = competitor.store.read_typed_event_result(
                    plan.structural_event_id,
                    plan.payload_hash,
                )
                assert canonical is not None
                assert canonical.state is M5RunState.REPLAYED
                assert canonical.call_work.is_zero
            context.winner = winner
            context.canonical = canonical
        return original_acquire(epoch_id, expected_revision, job)

    monkeypatch.setattr(bound.facade, "run_pending_direct", capture_direct)
    monkeypatch.setattr(
        bound.adapter,
        "acquire_direct_job_atomically",
        cutoff_acquire,
    )

    result = assembled.application.run_event(plan)
    assert result.state is M5RunState.REPLAYED
    assert result.replayed_outcome is M5ReplayedOutcome.FAILED
    assert result.failure_reason is (
        _GENERIC_REASON if route == "generic" else _PROVIDER_REASON
    )
    assert result.open_receipt is context.open_receipt
    assert result.open_receipt.replayed is resumed
    expected_call_work = discovery.call_work if prior_work else M5RuntimeWork()
    assert result.call_work == expected_call_work
    assert acquire_count == cutoff_ordinal
    assert len(discovery.calls) == int(prior_work)
    assert verifier.calls == []
    assert assembled.requirement_discovery.calls == []
    assert assembled.requirement_verifier.calls == []

    direct_result = context.direct_result
    assert direct_result is not None
    assert direct_result.call_work == expected_call_work
    selected = direct_result.selected_terminal_acquisition_receipt
    assert selected is not None
    assert selected.lease.disposition is M5AcquisitionDisposition.TERMINAL
    projection = selected.lease.terminal_projection
    assert projection is not None
    assert projection.terminal_state is expected_state
    assert projection.terminal_reason == direct_reason
    assert direct_result.selected_successful_outer_receipt is None
    assert direct_result.selected_checked_combined_failure_receipt is None

    canonical = context.canonical
    assert canonical is not None
    for field_name in (
        "event_work",
        "event_timing",
        "event_timing_coverage",
        "combined_deltas",
        "changed_state_references",
        "logical_result_hash",
    ):
        assert getattr(result, field_name) == getattr(canonical, field_name)
    assert canonical.call_work.is_zero
    durable_after = database.bound.store.read_typed_event_result(
        plan.structural_event_id,
        plan.payload_hash,
    )
    assert durable_after == canonical
    assert len(assembled.measurements.terminal_calls) == 1
    with database.connection.transaction():
        assert database.connection.execute(
            """
            SELECT count(*)
            FROM groundloop_m5_postcommit_invocation_telemetry
            WHERE structural_event_id = %s
              AND terminal_logical_result_hash = %s
            """,
            (plan.structural_event_id, result.logical_result_hash),
        ).fetchone() == (1,)


def test_cancelled_epoch_failed_loser_is_zero_write_on_reconnect(
    d24_direct_application_db: DirectApplicationD24Database,
) -> None:
    database = d24_direct_application_db
    prepared = _prepare_failure(database, tag="cancelled-loser")
    winner = _generic_failure(database.bound, prepared)
    assert winner.state is M5RunState.FAILED
    before = database_snapshot(database.connection)

    with database.reconnect() as rebound:
        loser = _combined_failure(
            rebound,
            prepared,
            requested_reason=_PROVIDER_REASON,
            direct_reason="a-different-provider-reason",
        )

    assert type(loser) is M5TypedDirectAcquisitionReceipt
    acquisition = loser
    assert acquisition.job == prepared.acquisition.job
    assert acquisition.attempt == prepared.acquisition.attempt
    assert acquisition.lease.disposition is M5AcquisitionDisposition.TERMINAL
    assert acquisition.lease.resulting_revision == prepared.expected_revision + 1
    assert acquisition.lease.terminal_projection is not None
    assert acquisition.lease.terminal_projection.terminal_state is JobState.CANCELLED
    assert acquisition.lease.terminal_projection.terminal_reason == "epoch_failed"
    assert database_snapshot(database.connection) == before
    attempt = prepared.acquisition.attempt
    assert attempt is not None
    with database.connection.transaction():
        assert database.connection.execute(
            """
            SELECT attempt_state,
                   (SELECT count(*)
                    FROM groundloop_m5_attempt_execution_evidence AS evidence
                    WHERE evidence.epoch_id = %s
                      AND evidence.attempt_id = attempt.attempt_id)
            FROM groundloop_semantic_job_attempt AS attempt
            WHERE attempt.attempt_id = %s
            """,
            (prepared.epoch_id, attempt.attempt_id),
        ).fetchone() == ("leased", 0)


@pytest.mark.parametrize(
    "mode",
    (
        "stale_revision",
        "direct_reason",
        "result_reason",
        "job",
        "attempt",
        "token",
        "dispatch",
        "error_hash",
        "attempt_work",
        "attempt_timing",
    ),
)
def test_checked_replay_conflicts_are_whole_schema_zero_write(
    d24_direct_application_db: DirectApplicationD24Database,
    mode: str,
) -> None:
    database = d24_direct_application_db
    conflict_plan = (
        _multi_chunk_plan(database, tag=f"checked-conflict-{mode}", chunk_count=2)
        if mode == "job"
        else None
    )
    prepared = _prepare_failure(
        database,
        tag=f"checked-conflict-{mode}",
        plan=conflict_plan,
    )
    first = _combined_failure(database.bound, prepared)
    assert type(first) is M5CheckedDirectTerminalFailureReceipt

    expected_revision = prepared.expected_revision
    job = prepared.acquisition.job
    lease = prepared.acquisition.lease
    direct_reason = _DIRECT_REASON
    requested_reason = _PROVIDER_REASON
    error_hash = sha("typed-direct-race-provider-error")
    attempt_work = _ATTEMPT_WORK
    attempt_timing = ATTEMPT_TIMING
    if mode == "stale_revision":
        expected_revision -= 1
    elif mode == "direct_reason":
        direct_reason = "changed-direct-terminal-reason"
    elif mode == "result_reason":
        requested_reason = M5RunFailureReason.VERIFIER_ERROR
    elif mode == "job":
        job = next(
            root
            for root in open_inputs(database, prepared.plan).direct_roots
            if root.job_id != prepared.acquisition.job.job_id
        )
    elif mode == "attempt":
        lease = replace(lease, attempt_id=sha("another-direct-attempt"))
    elif mode == "token":
        lease = replace(lease, lease_token_hash=sha("another-direct-token"))
    elif mode == "dispatch":
        lease = replace(lease, dispatch_record_digest=sha("another-dispatch"))
    elif mode == "error_hash":
        error_hash = sha("changed-provider-error")
    elif mode == "attempt_work":
        attempt_work = M5RuntimeWork(
            direct_discovery_call_count=(_ATTEMPT_WORK.direct_discovery_call_count + 1),
            embedding_model_call_count=_ATTEMPT_WORK.embedding_model_call_count,
            embedding_input_token_count=_ATTEMPT_WORK.embedding_input_token_count,
        )
    elif mode == "attempt_timing":
        assert ATTEMPT_TIMING is not None
        attempt_timing = replace(
            ATTEMPT_TIMING,
            neural_wall_ns=ATTEMPT_TIMING.neural_wall_ns + 1,
        )
    else:
        raise AssertionError(f"unknown checked replay conflict: {mode}")

    before = database_snapshot(database.connection)
    with pytest.raises(_CHECKED_ERRORS):
        _combined_failure(
            database.bound,
            prepared,
            expected_revision=expected_revision,
            job=job,
            lease=lease,
            direct_reason=direct_reason,
            error_hash=error_hash,
            attempt_work=attempt_work,
            attempt_timing=attempt_timing,
            requested_reason=requested_reason,
        )
    assert database_snapshot(database.connection) == before


def test_elapsed_current_lease_can_commit_checked_first_failure(
    d24_direct_application_db: DirectApplicationD24Database,
) -> None:
    database = d24_direct_application_db
    prepared = _prepare_failure(database, tag="elapsed-current-lease")
    deadline = prepared.acquisition.lease.lease_expires_at
    assert deadline is not None
    wait_until_expired(database.connection, deadline)

    before = database_snapshot(database.connection)
    checked = _combined_failure(database.bound, prepared)
    assert type(checked) is M5CheckedDirectTerminalFailureReceipt
    assert checked.direct_failure.epoch_id == prepared.epoch_id
    assert checked.direct_failure.job_id == prepared.acquisition.job.job_id
    assert checked.direct_failure.attempt_id == prepared.acquisition.lease.attempt_id
    assert checked.requested_failure_reason is _PROVIDER_REASON
    assert checked.terminal_result.state is M5RunState.FAILED
    assert checked.resulting_revision == prepared.expected_revision + 1
    assert database_snapshot(database.connection) != before

    terminal_before = database_snapshot(database.connection)
    terminal = database.bound.adapter.acquire_direct_job_atomically(
        prepared.epoch_id,
        checked.resulting_revision,
        prepared.acquisition.job,
    )
    assert terminal.lease.disposition is M5AcquisitionDisposition.TERMINAL
    assert terminal.lease.should_execute is False
    assert terminal.attempt == prepared.acquisition.attempt
    assert terminal.lease.terminal_projection is not None
    assert terminal.lease.terminal_projection.terminal_state is JobState.TERMINAL_FAILED
    assert terminal.lease.terminal_projection.terminal_reason == _DIRECT_REASON
    assert database_snapshot(database.connection) == terminal_before


@pytest.mark.parametrize(
    "mode",
    ("replaced_nonlatest", "nonleased_evidence_present"),
)
def test_preterminal_attempt_conflicts_are_whole_schema_zero_write(
    d24_direct_application_db: DirectApplicationD24Database,
    mode: str,
) -> None:
    database = d24_direct_application_db
    prepared = _prepare_failure(database, tag=f"attempt-conflict-{mode}")
    deadline = prepared.acquisition.lease.lease_expires_at
    assert deadline is not None
    if mode == "replaced_nonlatest":
        wait_until_expired(database.connection, deadline)
    if mode == "replaced_nonlatest":
        replacement = database.bound.adapter.acquire_direct_job_atomically(
            prepared.epoch_id,
            prepared.expected_revision,
            prepared.acquisition.job,
        )
        assert (
            replacement.lease.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
        )
    elif mode == "nonleased_evidence_present":
        retry_outcome = database.bound.adapter.mark_direct_retryable_failure_atomically(
            prepared.epoch_id,
            prepared.expected_revision,
            prepared.acquisition.lease,
            sha("preterminal-evidence-present"),
            _ATTEMPT_WORK,
            ATTEMPT_TIMING,
        )
        failed = _generic_failure(
            database.bound,
            prepared,
            expected_revision=retry_outcome.resulting_revision,
        )
        assert failed.state is M5RunState.FAILED

    before = database_snapshot(database.connection)
    with pytest.raises(_CHECKED_ERRORS):
        _combined_failure(database.bound, prepared)
    assert database_snapshot(database.connection) == before


@pytest.mark.parametrize("replay_route", ("generic", "checked"))
def test_corrupted_new_cancelled_projection_rejects_replay_without_writes(
    d24_direct_application_db: DirectApplicationD24Database,
    replay_route: str,
) -> None:
    database = d24_direct_application_db
    plan = _multi_chunk_plan(
        database,
        tag=f"corrupt-cancelled-{replay_route}",
        chunk_count=2,
    )
    prepared = _prepare_failure(
        database,
        tag=f"corrupt-cancelled-{replay_route}",
        plan=plan,
    )
    if replay_route == "generic":
        terminal = _generic_failure(database.bound, prepared)
        assert terminal.state is M5RunState.FAILED
        final_revision = prepared.expected_revision + 1
    elif replay_route == "checked":
        checked = _combined_failure(database.bound, prepared)
        assert type(checked) is M5CheckedDirectTerminalFailureReceipt
        final_revision = checked.resulting_revision
    else:
        raise AssertionError(f"unknown projection replay route: {replay_route}")
    cancelled = tuple(
        row
        for row in _job_rows(database, prepared.epoch_id)
        if row[1] == JobState.CANCELLED.value and row[5] == final_revision
    )
    assert cancelled
    _corrupt_cancelled_projection_reason(
        database,
        epoch_id=prepared.epoch_id,
        job_id=cast(str, cancelled[0][0]),
        final_revision=final_revision,
    )
    before = database_snapshot(database.connection)

    with pytest.raises(_CHECKED_ERRORS):
        if replay_route == "generic":
            _generic_failure(database.bound, prepared)
        else:
            _combined_failure(database.bound, prepared)
    assert database_snapshot(database.connection) == before


@pytest.mark.parametrize(
    "mode",
    (
        "scope_subset",
        "scope_nested",
        "job_subset",
        "job_nested",
        "detail_subset",
        "detail_nested",
        "prelock_held_receipt",
        "prelock_work",
    ),
)
def test_same_identity_shared_failure_authority_mutation_rolls_back_zero_write(
    d24_direct_application_db: DirectApplicationD24Database,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    database = d24_direct_application_db
    plan = _multi_chunk_plan(database, tag=f"shared-authority-{mode}", chunk_count=2)
    prepared = _prepare_failure(
        database,
        tag=f"shared-authority-{mode}",
        plan=plan,
    )
    with database.connection.transaction():
        zero_attempt_jobs = tuple(
            cast(str, row[0])
            for row in database.connection.execute(
                """
                SELECT job.logical_job_id
                FROM groundloop_m5_semantic_job AS job
                LEFT JOIN groundloop_m5_job_attempt AS attempt
                  ON attempt.logical_job_id = job.logical_job_id
                WHERE job.epoch_id = %s AND job.job_state = 'declared'
                GROUP BY job.logical_job_id
                HAVING count(attempt.attempt_id) = 0
                ORDER BY job.logical_job_id COLLATE "C"
                """,
                (prepared.epoch_id,),
            ).fetchall()
        )
    assert len(zero_attempt_jobs) >= 2

    if mode.startswith("scope_"):
        original: Any = vars(direct_m4_module)[
            "lock_m5_requirement_jobs_for_epoch_failure"
        ]

        def mutate_scope(cursor: Any, scope_locks: Any, **kwargs: Any) -> Any:
            original_id = id(scope_locks)
            assert len(scope_locks.scopes) >= 2
            if mode == "scope_subset":
                object.__setattr__(
                    scope_locks,
                    "scopes",
                    scope_locks.scopes[:-1],
                )
            else:
                nested = scope_locks.scopes[0]
                nested_id = id(nested)
                object.__setattr__(nested, "closed_revision", 999_999)
                assert id(nested) == nested_id
            assert id(scope_locks) == original_id
            return original(cursor, scope_locks, **kwargs)

        monkeypatch.setattr(
            direct_m4_module,
            "lock_m5_requirement_jobs_for_epoch_failure",
            mutate_scope,
        )
    elif mode.startswith("job_"):
        original = vars(direct_m4_module)[
            "lock_m5_requirement_details_for_epoch_failure"
        ]

        def mutate_jobs(cursor: Any, job_locks: Any, **kwargs: Any) -> Any:
            original_id = id(job_locks)
            if mode == "job_subset":
                omitted = zero_attempt_jobs[-1]
                assert any(job.spec.logical_job_id == omitted for job in job_locks.jobs)
                object.__setattr__(
                    job_locks,
                    "jobs",
                    tuple(
                        job
                        for job in job_locks.jobs
                        if job.spec.logical_job_id != omitted
                    ),
                )
            else:
                nested = job_locks.jobs[0]
                nested_id = id(nested)
                object.__setattr__(nested, "completed_revision", 999_999)
                assert id(nested) == nested_id
            assert id(job_locks) == original_id
            return original(cursor, job_locks, **kwargs)

        monkeypatch.setattr(
            direct_m4_module,
            "lock_m5_requirement_details_for_epoch_failure",
            mutate_jobs,
        )
    elif mode.startswith("detail_"):
        original = vars(direct_m4_module)["apply_m5_requirement_epoch_failure"]

        def mutate_details(cursor: Any, detail_plan: Any, **kwargs: Any) -> Any:
            original_id = id(detail_plan)
            assert detail_plan.selected_jobs
            if mode == "detail_subset":
                object.__setattr__(
                    detail_plan,
                    "selected_jobs",
                    detail_plan.selected_jobs[:-1],
                )
            else:
                assert detail_plan.completions
                nested = detail_plan.completions[0][1]
                nested_id = id(nested)
                object.__setattr__(
                    nested,
                    "completion_digest",
                    sha("mutated-shared-detail-completion"),
                )
                assert id(nested) == nested_id
            assert id(detail_plan) == original_id
            return original(cursor, detail_plan, **kwargs)

        monkeypatch.setattr(
            direct_m4_module,
            "apply_m5_requirement_epoch_failure",
            mutate_details,
        )
    else:
        original = vars(direct_m4_module)["_lock_typed_epoch_failure_preconditions"]

        def mutate_prelock(*args: Any, **kwargs: Any) -> Any:
            prelock = original(*args, **kwargs)
            original_id = id(prelock)
            if mode == "prelock_held_receipt":
                held = prelock.open_receipt
                held_id = id(held)
                object.__setattr__(held, "replayed", not held.replayed)
                assert id(held) == held_id
            elif mode == "prelock_work":
                object.__setattr__(
                    prelock,
                    "call_work",
                    replace(
                        prelock.call_work,
                        bytes_hashed=prelock.call_work.bytes_hashed + 1,
                    ),
                )
            else:
                raise AssertionError(f"unknown prelock mutation: {mode}")
            assert id(prelock) == original_id
            return prelock

        monkeypatch.setattr(
            direct_m4_module,
            "_lock_typed_epoch_failure_preconditions",
            mutate_prelock,
        )

    before = database_snapshot(database.connection)
    with pytest.raises(_CHECKED_ERRORS):
        _combined_failure(
            database.bound,
            prepared,
            call_work=replace(_CALL_WORK),
        )
    assert database_snapshot(database.connection) == before


@pytest.mark.parametrize(
    "capability",
    (
        "prelock",
        "scope_locks",
        "job_locks",
        "detail_plan",
        "applied_closure",
    ),
)
def test_shared_failure_authority_rejects_same_cursor_new_transaction_zero_write(
    d24_direct_application_db: DirectApplicationD24Database,
    capability: str,
) -> None:
    """A Python cursor identity cannot carry row-lock authority across xacts."""

    database = d24_direct_application_db
    prepared = _prepare_failure(database, tag=f"xact-authority-{capability}")
    lock_preconditions: Any = vars(direct_m4_module)[
        "_lock_typed_epoch_failure_preconditions"
    ]
    require_prelock: Any = vars(direct_m4_module)[
        "_require_typed_epoch_failure_prelock"
    ]
    lock_jobs: Any = vars(direct_m4_module)[
        "lock_m5_requirement_jobs_for_epoch_failure"
    ]
    lock_details: Any = vars(direct_m4_module)[
        "lock_m5_requirement_details_for_epoch_failure"
    ]
    apply_details: Any = vars(direct_m4_module)["apply_m5_requirement_epoch_failure"]
    finalize: Any = vars(direct_m4_module)["_finalize_typed_epoch_failure_locked"]

    class _RollbackProbe(Exception):
        pass

    prelock: Any = None
    scope_locks: Any = None
    job_locks: Any = None
    detail_plan: Any = None
    closure: Any = None
    first_xact: tuple[Any, ...] | None = None
    with database.connection.cursor() as cursor:
        cursor_identity = id(cursor)
        try:
            with database.connection.transaction():
                prelock = lock_preconditions(
                    cursor,
                    epoch_id=prepared.epoch_id,
                    expected_revision=prepared.expected_revision,
                    failure_reason=_GENERIC_REASON,
                    open_receipt=prepared.opened,
                    call_work=M5RuntimeWork(),
                )
                scope_locks = prelock.root_scope_locks
                if capability in {"job_locks", "detail_plan", "applied_closure"}:
                    job_locks = lock_jobs(cursor, scope_locks)
                if capability in {"detail_plan", "applied_closure"}:
                    detail_plan = lock_details(cursor, job_locks)
                first_xact = cursor.execute(
                    "SELECT pg_current_xact_id()::text"
                ).fetchone()
                assert first_xact is not None
                if capability == "applied_closure":
                    closure = apply_details(cursor, detail_plan)
                    raise _RollbackProbe
        except _RollbackProbe:
            pass

        before = database_snapshot(database.connection)
        assert first_xact is not None
        with pytest.raises(_CHECKED_ERRORS):
            with database.connection.transaction():
                assert id(cursor) == cursor_identity
                second_xact = cursor.execute(
                    "SELECT pg_current_xact_id()::text"
                ).fetchone()
                assert second_xact is not None
                assert second_xact != first_xact
                if capability == "prelock":
                    require_prelock(cursor, prelock)
                elif capability == "scope_locks":
                    lock_jobs(cursor, scope_locks)
                elif capability == "job_locks":
                    lock_details(cursor, job_locks)
                elif capability == "detail_plan":
                    apply_details(cursor, detail_plan)
                elif capability == "applied_closure":
                    finalize(
                        cursor,
                        prelock=prelock,
                        closure=closure,
                        terminal_work=closure.cancellation_work,
                        attempt_observation=None,
                        m4_resulting_revision=None,
                    )
                else:
                    raise AssertionError(
                        f"unknown shared xact capability: {capability}"
                    )
        assert database_snapshot(database.connection) == before


def test_target_absent_failure_closes_all_open_jobs_and_preserves_prior_terminal(
    d24_direct_application_db: DirectApplicationD24Database,
) -> None:
    database = d24_direct_application_db
    plan = _multi_chunk_plan(database, tag="generic-closure")
    opened = open_event(database, plan)
    roots = open_inputs(database, plan).direct_roots
    preserved_job = roots[0].job_id
    revision = _complete_one_direct_root(
        database,
        database.bound,
        plan,
        opened,
        roots[0],
        expected_revision=1,
    )
    preserved = _job_family_snapshot(
        database,
        epoch_id=opened.epoch_id,
        job_id=preserved_job,
    )
    provider = _ScriptedDiscovery(database, failure_call=1, retryable=True)
    bound = database.bind(database.connection, discovery=provider)  # type: ignore[arg-type]
    blocked = bound.facade.run_pending_direct(
        opened.epoch_id,
        revision,
        plan,
        opened,
    )
    assert blocked.blocked_reason is M5RunFailureReason.RETRIEVAL_UNAVAILABLE
    assert len(provider.calls) == 1
    assert provider.calls[0] != preserved_job

    failed = bound.adapter.fail_typed_epoch_with_open_receipt_atomically(
        opened.epoch_id,
        blocked.resulting_revision,
        _GENERIC_REASON,
        opened,
        blocked.call_work,
    )
    assert failed.state is M5RunState.FAILED
    assert (
        _job_family_snapshot(database, epoch_id=opened.epoch_id, job_id=preserved_job)
        == preserved
    )
    rows = _job_rows(database, opened.epoch_id)
    assert len(rows) == 3
    assert {row[0] for row in rows} == {row[0] for row in rows if row[3] is not None}
    state_by_id = {cast(str, row[0]): cast(str, row[1]) for row in rows}
    assert state_by_id[preserved_job] == JobState.COMPLETED_ACTIVE.value
    assert set(state_by_id.values()) == {
        JobState.COMPLETED_ACTIVE.value,
        JobState.CANCELLED.value,
    }
    cancelled = tuple(row for row in rows if row[1] == JobState.CANCELLED.value)
    assert len(cancelled) == 2
    assert all(row[3] == JobState.CANCELLED.value for row in cancelled)
    assert all(row[4] == "epoch_failed" for row in cancelled)
    assert all(row[5] == blocked.resulting_revision + 1 for row in cancelled)
    root_by_id = {
        root.job_id: root for root in open_inputs(database, plan).direct_roots
    }
    before_acquisition = database_snapshot(database.connection)
    with database.reconnect() as rebound:
        terminal = tuple(
            rebound.adapter.acquire_direct_job_atomically(
                opened.epoch_id,
                blocked.resulting_revision + 1,
                root_by_id[cast(str, row[0])],
            )
            for row in cancelled
        )
    assert database_snapshot(database.connection) == before_acquisition
    assert {receipt.attempt is None for receipt in terminal} == {False, True}
    assert all(
        receipt.lease.disposition is M5AcquisitionDisposition.TERMINAL
        and receipt.lease.terminal_projection is not None
        and receipt.lease.terminal_projection.terminal_state is JobState.CANCELLED
        and receipt.lease.terminal_projection.terminal_reason == "epoch_failed"
        for receipt in terminal
    )


def test_target_present_failure_closes_other_open_jobs_and_preserves_prior_terminal(
    d24_direct_application_db: DirectApplicationD24Database,
) -> None:
    database = d24_direct_application_db
    plan = _multi_chunk_plan(database, tag="combined-closure")
    opened = open_event(database, plan)
    roots = open_inputs(database, plan).direct_roots
    preserved_job = roots[0].job_id
    revision = _complete_one_direct_root(
        database,
        database.bound,
        plan,
        opened,
        roots[0],
        expected_revision=1,
    )
    preserved = _job_family_snapshot(
        database,
        epoch_id=opened.epoch_id,
        job_id=preserved_job,
    )
    provider = _ScriptedDiscovery(database, failure_call=1, retryable=False)
    bound = database.bind(database.connection, discovery=provider)  # type: ignore[arg-type]
    result = bound.facade.run_pending_direct(
        opened.epoch_id,
        revision,
        plan,
        opened,
    )
    checked = result.selected_checked_combined_failure_receipt
    assert checked is not None
    assert checked.terminal_result.state is M5RunState.FAILED
    assert len(provider.calls) == 1
    failed_job = provider.calls[0]
    assert failed_job != preserved_job
    rows = _job_rows(database, opened.epoch_id)
    assert len(rows) == 3
    assert {row[0] for row in rows} == {row[0] for row in rows if row[3] is not None}
    row_by_id = {cast(str, row[0]): row for row in rows}
    assert row_by_id[preserved_job][1] == JobState.COMPLETED_ACTIVE.value
    assert row_by_id[failed_job][1] == JobState.TERMINAL_FAILED.value
    assert row_by_id[failed_job][3] == JobState.TERMINAL_FAILED.value
    assert row_by_id[failed_job][4] == _DIRECT_REASON
    assert row_by_id[failed_job][5] == checked.resulting_revision
    assert (
        _job_family_snapshot(database, epoch_id=opened.epoch_id, job_id=preserved_job)
        == preserved
    )
    cancelled = tuple(
        row
        for job_id, row in row_by_id.items()
        if job_id not in {preserved_job, failed_job}
    )
    assert len(cancelled) == 1
    assert cancelled[0][1] == JobState.CANCELLED.value
    assert cancelled[0][3] == JobState.CANCELLED.value
    assert cancelled[0][4] == "epoch_failed"
    assert cancelled[0][5] == checked.resulting_revision
    cancelled_job_id = cast(str, cancelled[0][0])
    cancelled_root = next(
        root
        for root in open_inputs(database, plan).direct_roots
        if root.job_id == cancelled_job_id
    )
    before_reconnect = database_snapshot(database.connection)
    with database.reconnect() as rebound:
        terminal = rebound.adapter.acquire_direct_job_atomically(
            opened.epoch_id,
            checked.resulting_revision,
            cancelled_root,
        )
    assert database_snapshot(database.connection) == before_reconnect
    assert terminal.epoch_id == opened.epoch_id
    assert terminal.job == cancelled_root
    assert terminal.attempt is None
    assert terminal.lease.disposition is M5AcquisitionDisposition.TERMINAL
    assert terminal.lease.exact_replay is True
    assert terminal.lease.should_execute is False
    assert terminal.lease.terminal_projection is not None
    assert terminal.lease.terminal_projection.terminal_state is JobState.CANCELLED
    assert terminal.lease.terminal_projection.terminal_reason == "epoch_failed"


def test_failure_lock_order_is_complete_before_first_write(
    d24_direct_application_db: DirectApplicationD24Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = d24_direct_application_db
    prepared = _prepare_failure(database, tag="lock-order")
    adapter = database.bound.adapter
    before = database_snapshot(database.connection)
    order: list[str] = []

    original_m4_jobs = adapter._ports._lock_typed_direct_epoch_failure_jobs_local
    original_m5_jobs: Any = vars(direct_m4_module)[
        "lock_m5_requirement_jobs_for_epoch_failure"
    ]
    original_m4_details = adapter._ports._lock_typed_direct_epoch_failure_details_local
    original_m5_details: Any = vars(direct_m4_module)[
        "lock_m5_requirement_details_for_epoch_failure"
    ]
    original_direct_details = adapter._recovery.lock_direct_epoch_failure_details
    original_stage = adapter._ports._stage_typed_direct_epoch_failure_local
    original_direct_apply = adapter._recovery.apply_direct_epoch_failure
    original_m5_apply: Any = vars(direct_m4_module)[
        "apply_m5_requirement_epoch_failure"
    ]
    original_finalize: Any = vars(direct_m4_module)[
        "_finalize_typed_epoch_failure_locked"
    ]

    def assert_prewrite_locks() -> None:
        assert order[:5] == [
            "m4_jobs",
            "m5_jobs",
            "m4_details",
            "m5_details",
            "direct_details",
        ]
        assert all(order.count(phase) == 1 for phase in order[:5])

    def m4_jobs(*args: Any, **kwargs: Any) -> Any:
        assert "first_write" not in order
        order.append("m4_jobs")
        return original_m4_jobs(*args, **kwargs)

    def m5_jobs(*args: Any, **kwargs: Any) -> Any:
        assert "first_write" not in order
        order.append("m5_jobs")
        return original_m5_jobs(*args, **kwargs)

    def m4_details(*args: Any, **kwargs: Any) -> Any:
        assert "first_write" not in order
        order.append("m4_details")
        return original_m4_details(*args, **kwargs)

    def m5_details(*args: Any, **kwargs: Any) -> Any:
        assert "first_write" not in order
        order.append("m5_details")
        return original_m5_details(*args, **kwargs)

    def direct_details(*args: Any, **kwargs: Any) -> Any:
        assert "first_write" not in order
        order.append("direct_details")
        return original_direct_details(*args, **kwargs)

    def stage(*args: Any, **kwargs: Any) -> Any:
        assert_prewrite_locks()
        assert database_snapshot(database.connection) == before
        order.append("first_write")
        return original_stage(*args, **kwargs)

    def direct_apply(*args: Any, **kwargs: Any) -> Any:
        assert_prewrite_locks()
        assert order[-1] == "first_write"
        order.append("direct_apply")
        return original_direct_apply(*args, **kwargs)

    def m5_apply(*args: Any, **kwargs: Any) -> Any:
        assert_prewrite_locks()
        assert order[-1] == "direct_apply"
        order.append("m5_apply")
        return original_m5_apply(*args, **kwargs)

    def finalize(*args: Any, **kwargs: Any) -> Any:
        assert_prewrite_locks()
        assert order[-1] == "m5_apply"
        order.append("finalize")
        return original_finalize(*args, **kwargs)

    monkeypatch.setattr(
        adapter._ports, "_lock_typed_direct_epoch_failure_jobs_local", m4_jobs
    )
    monkeypatch.setattr(
        direct_m4_module, "lock_m5_requirement_jobs_for_epoch_failure", m5_jobs
    )
    monkeypatch.setattr(
        adapter._ports,
        "_lock_typed_direct_epoch_failure_details_local",
        m4_details,
    )
    monkeypatch.setattr(
        direct_m4_module,
        "lock_m5_requirement_details_for_epoch_failure",
        m5_details,
    )
    monkeypatch.setattr(
        adapter._recovery, "lock_direct_epoch_failure_details", direct_details
    )
    monkeypatch.setattr(
        adapter._ports, "_stage_typed_direct_epoch_failure_local", stage
    )
    monkeypatch.setattr(
        adapter._recovery,
        "apply_direct_epoch_failure",
        direct_apply,
    )
    monkeypatch.setattr(
        direct_m4_module,
        "apply_m5_requirement_epoch_failure",
        m5_apply,
    )
    monkeypatch.setattr(
        direct_m4_module,
        "_finalize_typed_epoch_failure_locked",
        finalize,
    )

    receipt = _combined_failure(database.bound, prepared)
    assert type(receipt) is M5CheckedDirectTerminalFailureReceipt
    assert order == [
        "m4_jobs",
        "m5_jobs",
        "m4_details",
        "m5_details",
        "direct_details",
        "first_write",
        "direct_apply",
        "m5_apply",
        "finalize",
    ]


def test_failure_injection_rolls_back_whole_schema_then_reconnect_succeeds(
    d24_direct_application_db: DirectApplicationD24Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = d24_direct_application_db
    prepared = _prepare_failure(database, tag="rollback")
    before = database_snapshot(database.connection)
    original: Any = vars(direct_m4_module)["_finalize_typed_epoch_failure_locked"]

    def injected_failure(*_: Any, **__: Any) -> Any:
        raise RuntimeError("race-injected failure after direct/M4 apply")

    with monkeypatch.context() as context:
        context.setattr(
            direct_m4_module,
            "_finalize_typed_epoch_failure_locked",
            injected_failure,
        )
        with pytest.raises(RuntimeError, match="race-injected"):
            _combined_failure(database.bound, prepared)
    assert vars(direct_m4_module)["_finalize_typed_epoch_failure_locked"] is original
    assert database_snapshot(database.connection) == before

    with database.reconnect() as rebound:
        receipt = _combined_failure(rebound, prepared)
    assert type(receipt) is M5CheckedDirectTerminalFailureReceipt
    checked = receipt
    assert checked.terminal_result.state is M5RunState.FAILED
    assert checked.resulting_revision == prepared.expected_revision + 1
