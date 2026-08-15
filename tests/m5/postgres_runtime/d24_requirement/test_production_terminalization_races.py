"""Two-connection races for D24 R2a failure terminalization."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event
from typing import Any

import pytest

from groundloop.errors import EventConflictError
from groundloop.m5.runtime.contracts import (
    M5AcquisitionDisposition,
    M5ExecutionEvidenceDisposition,
    M5ReplayedOutcome,
    M5RequirementReturnDisposition,
    M5RunFailureReason,
    M5RunState,
    M5RuntimeWork,
)
from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
from tests.m5.postgres_runtime.d24_requirement.test_recovery import (
    _LATE_SEMANTIC_TABLES,
    _attempt_timing,
    _complete_verifier,
    _forward_attempt_work,
    _full_snapshot,
    _late_requirement_row_counts,
    _prepare_verifier_attempt,
    _root_result_and_output,
    _snapshot_tables,
    _verifier_semantic_closure_counts,
    _wait_until_expired,
)


def _fail(
    connection: Any,
    database: Any,
    *,
    expected_revision: int,
    call_work: M5RuntimeWork | None = None,
) -> Any:
    return PostgresM5RuntimeStore(connection).fail_typed_epoch_atomically(
        database.epoch_id,
        expected_revision,
        M5RunFailureReason.RETRIEVAL_UNAVAILABLE,
        call_work or M5RuntimeWork(),
    )


def test_concurrent_failure_callers_commit_one_terminal_result(
    d24_requirement_db: Any,
) -> None:
    """Two equivalent callers serialize to one write and one exact replay."""

    call_work = M5RuntimeWork(bytes_hashed=17, bytes_serialized=19)
    with d24_requirement_db.two_connections() as (left, right):
        start = Barrier(2)

        def fail(connection: Any) -> Any:
            start.wait()
            return _fail(
                connection,
                d24_requirement_db,
                expected_revision=1,
                call_work=call_work,
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = tuple(
                future.result(timeout=15)
                for future in (pool.submit(fail, left), pool.submit(fail, right))
            )

    assert {result.state for result in results} == {
        M5RunState.FAILED,
        M5RunState.REPLAYED,
    }
    assert {result.replayed_outcome for result in results} == {
        None,
        M5ReplayedOutcome.FAILED,
    }
    first = next(result for result in results if result.replayed_outcome is None)
    replay = next(result for result in results if result.replayed_outcome is not None)
    assert first.call_work == call_work
    assert replay.call_work.is_zero
    assert first.logical_result_hash == replay.logical_result_hash
    assert (
        PostgresM5RuntimeStore(d24_requirement_db.connection).current_revision(
            d24_requirement_db.epoch_id
        )
        == 2
    )


@pytest.mark.parametrize("first_committer", ("failure", "acquisition"))
def test_failure_and_root_acquisition_serialize_in_both_orders(
    d24_requirement_db: Any,
    first_committer: str,
) -> None:
    """A failure either wins first or observes the acquisition revision."""

    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    with d24_requirement_db.two_connections() as (left, right):
        with ThreadPoolExecutor(max_workers=1) as pool:
            if first_committer == "failure":
                acquire_started = Event()

                def blocked_acquire() -> Any:
                    acquire_started.set()
                    return PostgresM5RuntimeStore(right).acquire_m5_job(
                        epoch_id, 1, job
                    )

                with left.transaction():
                    failed = _fail(left, d24_requirement_db, expected_revision=1)
                    acquire_future = pool.submit(blocked_acquire)
                    assert acquire_started.wait(timeout=5)
                    assert not acquire_future.done()
                terminal_lease = acquire_future.result(timeout=10)
                assert failed.state is M5RunState.FAILED
                assert terminal_lease.disposition is M5AcquisitionDisposition.TERMINAL
                assert terminal_lease.resulting_revision == 2
            else:
                failure_started = Event()

                def blocked_failure() -> Any:
                    failure_started.set()
                    return _fail(right, d24_requirement_db, expected_revision=1)

                with left.transaction():
                    lease = PostgresM5RuntimeStore(left).acquire_m5_job(
                        epoch_id, 1, job
                    )
                    assert lease.resulting_revision == 2
                    failure_future = pool.submit(blocked_failure)
                    assert failure_started.wait(timeout=5)
                    assert not failure_future.done()
                with pytest.raises(
                    EventConflictError, match="stale typed runtime revision"
                ):
                    failure_future.result(timeout=10)
                assert (
                    PostgresM5RuntimeStore(
                        d24_requirement_db.connection
                    ).current_revision(epoch_id)
                    == 2
                )
                failed = _fail(
                    d24_requirement_db.connection,
                    d24_requirement_db,
                    expected_revision=2,
                )
                assert failed.state is M5RunState.FAILED
                terminal_lease = PostgresM5RuntimeStore(
                    d24_requirement_db.connection
                ).acquire_m5_job(epoch_id, 2, job)
                assert terminal_lease.disposition is M5AcquisitionDisposition.TERMINAL
                assert terminal_lease.resulting_revision == 3


@pytest.mark.parametrize("first_committer", ("expired_output", "failure"))
def test_failure_and_expired_root_output_serialize_in_both_orders(
    d24_requirement_db: Any,
    first_committer: str,
) -> None:
    """Root late output is event-accounted before failure or isolated after it."""

    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    original = store.acquire_m5_job(epoch_id, 1, job)
    assert original.attempt is not None and original.lease_expires_at is not None
    _wait_until_expired(d24_requirement_db.connection, original.lease_expires_at)
    successor = store.acquire_m5_job(epoch_id, 2, job)
    assert successor.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert successor.attempt is not None and successor.resulting_revision == 3
    result, output = _root_result_and_output(
        d24_requirement_db, job=job, attempt=original.attempt
    )
    semantic_before = _snapshot_tables(
        d24_requirement_db.connection, _LATE_SEMANTIC_TABLES
    )

    def stage(connection: Any) -> Any:
        return PostgresM5RuntimeStore(connection).stage_m5_discovery_result_atomically(
            epoch_id,
            3,
            original,
            job,
            result,
            output,
            M5ExecutionEvidenceDisposition.RETURNED,
            _forward_attempt_work(),
            _attempt_timing(),
            eligible_snapshot_exhausted=True,
        )

    with d24_requirement_db.two_connections() as (left, right):
        with ThreadPoolExecutor(max_workers=1) as pool:
            if first_committer == "expired_output":
                failure_started = Event()

                def blocked_failure() -> Any:
                    failure_started.set()
                    return _fail(right, d24_requirement_db, expected_revision=3)

                with left.transaction():
                    receipt = stage(left)
                    assert (
                        receipt.disposition
                        is M5RequirementReturnDisposition.EXPIRED_PRETERMINAL
                    )
                    failure_future = pool.submit(blocked_failure)
                    assert failure_started.wait(timeout=5)
                    assert not failure_future.done()
                failed = failure_future.result(timeout=10)
            else:
                output_started = Event()

                def blocked_output() -> Any:
                    output_started.set()
                    return stage(right)

                with left.transaction():
                    failed = _fail(left, d24_requirement_db, expected_revision=3)
                    output_future = pool.submit(blocked_output)
                    assert output_started.wait(timeout=5)
                    assert not output_future.done()
                receipt = output_future.result(timeout=10)
                assert (
                    receipt.disposition
                    is M5RequirementReturnDisposition.EXPIRED_POSTTERMINAL
                )

    assert failed.state is M5RunState.FAILED
    assert store.current_revision(epoch_id) == 4
    expected_late_counts = (
        (1, 1, 1, 0, 0) if first_committer == "expired_output" else (1, 1, 1, 1, 1)
    )
    assert (
        _late_requirement_row_counts(
            d24_requirement_db.connection,
            epoch_id=epoch_id,
            attempt_id=original.attempt.attempt_id,
        )
        == expected_late_counts
    )
    assert (
        _snapshot_tables(d24_requirement_db.connection, _LATE_SEMANTIC_TABLES)
        == semantic_before
    )
    attempt_states = d24_requirement_db.connection.execute(
        """
        SELECT attempt_state
        FROM groundloop_m5_job_attempt
        WHERE logical_job_id = %s
        ORDER BY attempt_ordinal
        """,
        (job.logical_job_id,),
    ).fetchall()
    assert attempt_states == [("expired",), ("dispatched",)]

    frozen = _full_snapshot(d24_requirement_db.connection)
    replay = _fail(
        d24_requirement_db.connection,
        d24_requirement_db,
        expected_revision=3,
        call_work=M5RuntimeWork(bytes_hashed=999),
    )
    assert replay.replayed_outcome is M5ReplayedOutcome.FAILED
    assert _full_snapshot(d24_requirement_db.connection) == frozen


@pytest.mark.parametrize("first_committer", ("expired_output", "failure"))
def test_failure_and_expired_verifier_output_serialize_in_both_orders(
    d24_requirement_db: Any,
    first_committer: str,
) -> None:
    """Verifier late output follows the same pre/postterminal lock order."""

    store, fixture = _prepare_verifier_attempt(d24_requirement_db)
    epoch_id = d24_requirement_db.epoch_id
    assert fixture.lease.attempt is not None
    assert fixture.lease.lease_expires_at is not None
    _wait_until_expired(d24_requirement_db.connection, fixture.lease.lease_expires_at)
    successor = store.acquire_m5_job(epoch_id, 5, fixture.job)
    assert successor.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert successor.attempt is not None and successor.resulting_revision == 6
    semantic_before = _snapshot_tables(
        d24_requirement_db.connection, _LATE_SEMANTIC_TABLES
    )

    def complete(connection: Any) -> Any:
        return _complete_verifier(
            PostgresM5RuntimeStore(connection),
            d24_requirement_db,
            fixture,
            expected_revision=6,
            disposition=M5ExecutionEvidenceDisposition.RETURNED,
            timing=_attempt_timing(),
        )

    with d24_requirement_db.two_connections() as (left, right):
        with ThreadPoolExecutor(max_workers=1) as pool:
            if first_committer == "expired_output":
                failure_started = Event()

                def blocked_failure() -> Any:
                    failure_started.set()
                    return _fail(right, d24_requirement_db, expected_revision=6)

                with left.transaction():
                    receipt = complete(left)
                    assert (
                        receipt.disposition
                        is M5RequirementReturnDisposition.EXPIRED_PRETERMINAL
                    )
                    failure_future = pool.submit(blocked_failure)
                    assert failure_started.wait(timeout=5)
                    assert not failure_future.done()
                failed = failure_future.result(timeout=10)
            else:
                output_started = Event()

                def blocked_output() -> Any:
                    output_started.set()
                    return complete(right)

                with left.transaction():
                    failed = _fail(left, d24_requirement_db, expected_revision=6)
                    output_future = pool.submit(blocked_output)
                    assert output_started.wait(timeout=5)
                    assert not output_future.done()
                receipt = output_future.result(timeout=10)
                assert (
                    receipt.disposition
                    is M5RequirementReturnDisposition.EXPIRED_POSTTERMINAL
                )

    assert failed.state is M5RunState.FAILED
    assert store.current_revision(epoch_id) == 7
    expected_late_counts = (
        (1, 1, 1, 0, 0) if first_committer == "expired_output" else (1, 1, 1, 1, 1)
    )
    assert (
        _late_requirement_row_counts(
            d24_requirement_db.connection,
            epoch_id=epoch_id,
            attempt_id=fixture.lease.attempt.attempt_id,
        )
        == expected_late_counts
    )
    assert _verifier_semantic_closure_counts(d24_requirement_db, fixture) == (
        0,
        0,
        0,
        0,
    )
    assert (
        _snapshot_tables(d24_requirement_db.connection, _LATE_SEMANTIC_TABLES)
        == semantic_before
    )
    attempt_states = d24_requirement_db.connection.execute(
        """
        SELECT attempt_state
        FROM groundloop_m5_job_attempt
        WHERE logical_job_id = %s
        ORDER BY attempt_ordinal
        """,
        (fixture.job.logical_job_id,),
    ).fetchall()
    assert attempt_states == [("expired",), ("dispatched",)]

    frozen = _full_snapshot(d24_requirement_db.connection)
    replay = _fail(
        d24_requirement_db.connection,
        d24_requirement_db,
        expected_revision=6,
        call_work=M5RuntimeWork(bytes_serialized=999),
    )
    assert replay.replayed_outcome is M5ReplayedOutcome.FAILED
    assert _full_snapshot(d24_requirement_db.connection) == frozen
