"""Serialized race falsifiers for M5-D24 typed-direct persistence."""

from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from typing import Any

from psycopg import Connection, sql

from groundloop.errors import EventConflictError
from groundloop.m4.contracts import JobState
from groundloop.m5.runtime.contracts import (
    M5AcquisitionDisposition,
    M5DirectLateReturnDisposition,
    M5ExecutionEvidenceDisposition,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5TransitionTimingAnchor,
    M5TypedDirectJobLease,
)
from groundloop.m5.runtime.direct_m4 import PostgresM5DirectM4Adapter
from tests.m5.postgres_runtime.d24_direct.conftest import (
    DirectD24Database,
    OpenedDirectEpoch,
    deterministic_token,
    discovery_envelope,
    open_direct_epoch,
    wait_until_expired,
)

_RACE_TABLES = (
    "groundloop_epoch",
    "groundloop_m5_runtime_epoch",
    "groundloop_semantic_job",
    "groundloop_semantic_job_attempt",
    "groundloop_semantic_job_dependency",
    "groundloop_discovery_scope",
    "groundloop_m5_dispatch_record",
    "groundloop_m5_attempt_execution_evidence",
    "groundloop_m5_runtime_work_contribution",
    "groundloop_m5_runtime_work_accumulator",
    "groundloop_m5_runtime_timing_contribution",
    "groundloop_m5_transition_call_timing",
    "groundloop_m5_runtime_timing_accumulator",
    "groundloop_m5_expired_attempt_return",
    "groundloop_m5_typed_direct_late_return_envelope",
    "groundloop_m5_direct_terminal_projection",
    "groundloop_m4_discovery_result",
    "groundloop_impact_channel_hit",
    "groundloop_admitted_pair",
)


def _snapshot(
    connection: Connection[Any],
) -> tuple[tuple[str, tuple[tuple[str, str], ...]], ...]:
    captured = []
    for table_name in _RACE_TABLES:
        rows = connection.execute(
            sql.SQL(
                "SELECT xmin::text, to_jsonb(snapshot_row)::text "
                "FROM {} AS snapshot_row "
                'ORDER BY to_jsonb(snapshot_row)::text COLLATE "C", xmin::text'
            ).format(sql.Identifier(table_name))
        ).fetchall()
        captured.append((table_name, tuple((str(row[0]), str(row[1])) for row in rows)))
    return tuple(captured)


def _acquire(
    opened: OpenedDirectEpoch,
    *,
    expected_revision: int,
    ordinal: int,
) -> M5TypedDirectJobLease:
    connection = opened.database.connection
    with connection.transaction(), connection.cursor() as cursor:
        lease = opened.adapter.acquire_direct_job(
            cursor,
            opened.epoch_id,
            expected_revision,
            opened.root,
            deterministic_token(opened.root, ordinal),
        )
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
    return lease


def _failure_hash(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def test_two_takeover_acquirers_create_one_dense_successor(
    d24_direct_db: DirectD24Database,
) -> None:
    opened = open_direct_epoch(d24_direct_db, lease_duration_ms=500)
    first = _acquire(opened, expected_revision=1, ordinal=1)
    assert first.lease_expires_at is not None
    wait_until_expired(d24_direct_db.connection, first.lease_expires_at)

    with d24_direct_db.two_connections() as (left, right):
        left_adapter = d24_direct_db.adapter_for(left)
        right_adapter = d24_direct_db.adapter_for(right)
        barrier = Barrier(2)

        def acquire_after_barrier(
            connection: Connection[Any], adapter: PostgresM5DirectM4Adapter
        ) -> M5TypedDirectJobLease:
            barrier.wait(timeout=10)
            with connection.transaction(), connection.cursor() as cursor:
                lease = adapter.acquire_direct_job(
                    cursor,
                    opened.epoch_id,
                    2,
                    opened.root,
                    deterministic_token(opened.root, 2),
                )
                cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
            return lease

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = (
                pool.submit(acquire_after_barrier, left, left_adapter),
                pool.submit(acquire_after_barrier, right, right_adapter),
            )
            leases = tuple(future.result(timeout=15) for future in futures)

    takeover = next(
        lease
        for lease in leases
        if lease.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    )
    live = next(
        lease
        for lease in leases
        if lease.disposition is M5AcquisitionDisposition.LIVE_LEASE
    )
    assert takeover.should_execute and not takeover.exact_replay
    assert not live.should_execute and live.exact_replay
    assert takeover.attempt_id == live.attempt_id
    assert takeover.lease_token_hash == live.lease_token_hash
    assert takeover.resulting_revision == live.resulting_revision == 3
    assert tuple(
        d24_direct_db.connection.execute(
            """
            SELECT attempt_ordinal, attempt_state
            FROM groundloop_semantic_job_attempt
            WHERE job_id = %s ORDER BY attempt_ordinal
            """,
            (opened.root.job_id,),
        ).fetchall()
    ) == ((1, "expired"), (2, "leased"))
    assert d24_direct_db.connection.execute(
        """
        SELECT base.revision, runtime.revision,
               count(DISTINCT dispatch.record_digest),
               count(DISTINCT contribution.contribution_key_digest) FILTER (
                   WHERE contribution.contribution_kind = 'direct_acquisition'
               )
        FROM groundloop_epoch AS base
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        JOIN groundloop_m5_dispatch_record AS dispatch
          ON dispatch.epoch_id = base.epoch_id AND dispatch.subgraph = 'direct'
        JOIN groundloop_m5_runtime_work_contribution AS contribution
          ON contribution.epoch_id = base.epoch_id
        WHERE base.epoch_id = %s
        GROUP BY base.revision, runtime.revision
        """,
        (opened.epoch_id,),
    ).fetchone() == (3, 3, 2, 2)


def test_output_before_takeover_leaves_terminal_projection_and_no_successor(
    d24_direct_db: DirectD24Database,
) -> None:
    opened = open_direct_epoch(d24_direct_db, lease_duration_ms=500)
    lease = _acquire(opened, expected_revision=1, ordinal=1)
    envelope, children = discovery_envelope(
        opened,
        lease,
        attempt_ordinal=1,
    )
    completed = opened.adapter.settle_direct_expansion_atomically(
        opened.epoch_id,
        2,
        lease,
        envelope,
        children,
        M5ExecutionEvidenceDisposition.RETURNED,
        M5RuntimeWork(),
        None,
    )
    assert completed.normal is not None and completed.normal.resulting_revision == 3
    assert lease.lease_expires_at is not None
    wait_until_expired(d24_direct_db.connection, lease.lease_expires_at)

    before = _snapshot(d24_direct_db.connection)
    with (
        d24_direct_db.connection.transaction(),
        d24_direct_db.connection.cursor() as cursor,
    ):
        terminal = opened.adapter.acquire_direct_job(
            cursor,
            opened.epoch_id,
            2,
            opened.root,
            deterministic_token(opened.root, 2),
        )
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
    assert terminal.disposition is M5AcquisitionDisposition.TERMINAL
    assert terminal.exact_replay and not terminal.should_execute
    assert terminal.already_completed
    assert terminal.resulting_revision == 3
    assert terminal.terminal_projection is not None
    assert terminal.terminal_projection.terminal_state is JobState.COMPLETED_ACTIVE
    assert _snapshot(d24_direct_db.connection) == before
    assert d24_direct_db.connection.execute(
        "SELECT count(*) FROM groundloop_semantic_job_attempt WHERE job_id = %s",
        (opened.root.job_id,),
    ).fetchone() == (1,)


def test_takeover_before_output_archives_old_return_without_touching_successor(
    d24_direct_db: DirectD24Database,
) -> None:
    opened = open_direct_epoch(d24_direct_db, lease_duration_ms=250)
    first = _acquire(opened, expected_revision=1, ordinal=1)
    assert first.lease_expires_at is not None
    wait_until_expired(d24_direct_db.connection, first.lease_expires_at)
    successor = _acquire(opened, expected_revision=2, ordinal=2)
    assert successor.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    envelope, children = discovery_envelope(
        opened,
        first,
        attempt_ordinal=1,
    )
    stale = opened.adapter.settle_direct_expansion_atomically(
        opened.epoch_id,
        2,
        first,
        envelope,
        children,
        M5ExecutionEvidenceDisposition.RETURNED,
        M5RuntimeWork(),
        None,
    )
    assert stale.normal is None and stale.late is not None
    assert stale.late.disposition is M5DirectLateReturnDisposition.EXPIRED_PRETERMINAL
    assert stale.late.resulting_revision == 3
    assert d24_direct_db.connection.execute(
        """
        SELECT base.revision, runtime.revision, job.job_state,
               scope.closed_revision,
               (SELECT count(*) FROM groundloop_semantic_job_dependency
                WHERE epoch_id = base.epoch_id)
        FROM groundloop_epoch AS base
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        JOIN groundloop_semantic_job AS job USING (epoch_id)
        JOIN groundloop_discovery_scope AS scope USING (epoch_id)
        WHERE base.epoch_id = %s AND job.job_id = %s
        """,
        (opened.epoch_id, opened.root.job_id),
    ).fetchone() == (3, 3, "running", None, 0)
    assert tuple(
        d24_direct_db.connection.execute(
            """
            SELECT attempt_ordinal, attempt_state
            FROM groundloop_semantic_job_attempt
            WHERE job_id = %s ORDER BY attempt_ordinal
            """,
            (opened.root.job_id,),
        ).fetchall()
    ) == ((1, "expired"), (2, "leased"))

    before_live = _snapshot(d24_direct_db.connection)
    live = _acquire(opened, expected_revision=2, ordinal=2)
    assert live.disposition is M5AcquisitionDisposition.LIVE_LEASE
    assert live.attempt_id == successor.attempt_id
    assert _snapshot(d24_direct_db.connection) == before_live


def test_retryable_failure_before_takeover_is_followed_by_one_new_dispatch(
    d24_direct_db: DirectD24Database,
) -> None:
    opened = open_direct_epoch(d24_direct_db, lease_duration_ms=500)
    first = _acquire(opened, expected_revision=1, ordinal=1)
    work = M5RuntimeWork(direct_discovery_call_count=1)
    with (
        d24_direct_db.connection.transaction(),
        d24_direct_db.connection.cursor() as cursor,
    ):
        failure = opened.adapter.mark_direct_retryable_failure(
            cursor,
            opened.epoch_id,
            2,
            first,
            _failure_hash("d24-direct-retryable-first"),
            work,
            None,
        )
        opened.adapter.install_outer_transition_anchor(
            cursor,
            M5TransitionTimingAnchor.build(
                epoch_id=opened.epoch_id,
                contribution_kind=(
                    M5RuntimeWorkContributionKind.DIRECT_ATTEMPT_EXECUTION
                ),
                source_id=failure.attempt_id,
                anchor_revision=3,
                terminal_transition=False,
            ),
        )
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")

    assert d24_direct_db.connection.execute(
        "SELECT job_state FROM groundloop_semantic_job WHERE job_id = %s",
        (opened.root.job_id,),
    ).fetchone() == ("retryable_failed",)
    assert d24_direct_db.connection.execute(
        "SELECT attempt_state FROM groundloop_semantic_job_attempt "
        "WHERE job_id = %s AND attempt_ordinal = 1",
        (opened.root.job_id,),
    ).fetchone() == ("failed",)
    replacement = _acquire(opened, expected_revision=3, ordinal=2)
    assert replacement.disposition is M5AcquisitionDisposition.DISPATCH_NEW
    assert replacement.resulting_revision == 4 and replacement.should_execute
    assert tuple(
        d24_direct_db.connection.execute(
            """
            SELECT attempt_ordinal, attempt_state
            FROM groundloop_semantic_job_attempt
            WHERE job_id = %s ORDER BY attempt_ordinal
            """,
            (opened.root.job_id,),
        ).fetchall()
    ) == ((1, "failed"), (2, "leased"))


def test_takeover_before_old_failures_conflicts_without_writes(
    d24_direct_db: DirectD24Database,
) -> None:
    opened = open_direct_epoch(d24_direct_db, lease_duration_ms=250)
    first = _acquire(opened, expected_revision=1, ordinal=1)
    assert first.lease_expires_at is not None
    wait_until_expired(d24_direct_db.connection, first.lease_expires_at)
    successor = _acquire(opened, expected_revision=2, ordinal=2)
    assert successor.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    work = M5RuntimeWork(direct_discovery_call_count=1)

    for terminal in (False, True):
        before = _snapshot(d24_direct_db.connection)
        try:
            with (
                d24_direct_db.connection.transaction(),
                d24_direct_db.connection.cursor() as cursor,
            ):
                if terminal:
                    opened.adapter.mark_direct_terminal_failure(
                        cursor,
                        opened.epoch_id,
                        2,
                        first,
                        "retrieval_error",
                        _failure_hash("d24-direct-old-terminal"),
                        work,
                        None,
                    )
                else:
                    opened.adapter.mark_direct_retryable_failure(
                        cursor,
                        opened.epoch_id,
                        2,
                        first,
                        _failure_hash("d24-direct-old-retryable"),
                        work,
                        None,
                    )
        except EventConflictError:
            pass
        else:
            raise AssertionError("old direct failure unexpectedly beat its takeover")
        assert _snapshot(d24_direct_db.connection) == before

    live = _acquire(opened, expected_revision=2, ordinal=2)
    assert live.disposition is M5AcquisitionDisposition.LIVE_LEASE
    assert live.attempt_id == successor.attempt_id
