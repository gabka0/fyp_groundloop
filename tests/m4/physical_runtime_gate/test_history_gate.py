from __future__ import annotations

import copy
from dataclasses import dataclass, field, replace
from typing import Any

import pytest
from history_gate_harness import (
    OPTIONAL_CLAIM_ID,
    PUBLICATION_TABLES,
    REQUIRED_ANSWER_ID,
    REQUIRED_CLAIM_ID,
    FailingVerifier,
    GuardedEvent,
    HistoryAdmission,
    HistoryVerifier,
    all_groundloop_table_names,
    application,
    assert_point_bounded_sql,
    audit_after_kernel,
    compact_event,
    conflicting_event,
    document,
    forbid_global_path,
    prepare_measured_ports,
    run_guarded_event,
    successful_kernel_guard,
    table_projection,
)
from physical_gate_harness import (
    CountingConnection,
    ExecutionAccounting,
    isolated_connection,
)

from groundloop.errors import EventConflictError
from groundloop.m4.application import EventRunState
from groundloop.m4.contracts import (
    ChildClosure,
    DiscoveryScope,
    JobCompletion,
    JobState,
    UpdateKind,
)
from groundloop.m4.pipeline import StructuralPayload

SMALL_SCALE = (8, 2)
LARGE_SCALE = (256, 256)


def _status(
    connection: CountingConnection, claim_id: str = REQUIRED_CLAIM_ID
) -> tuple[str, str]:
    claim = connection.execute(
        "SELECT status FROM groundloop_claim_state_materialized WHERE claim_id = %s",
        (claim_id,),
    ).fetchone()
    answer = connection.execute(
        "SELECT status FROM groundloop_answer_state_materialized "
        "WHERE answer_version_id = %s",
        (REQUIRED_ANSWER_ID,),
    ).fetchone()
    assert claim is not None and answer is not None
    return str(claim[0]), str(answer[0])


def _event_job_projection(
    connection: CountingConnection, epoch_id: int
) -> tuple[tuple[object, ...], ...]:
    return tuple(
        connection.execute(
            """
            SELECT job_kind, claim_id, chunk_version_id, job_state,
                   parent_job_id IS NOT NULL, child_closed
            FROM groundloop_semantic_job
            WHERE epoch_id = %s
            ORDER BY job_kind, claim_id NULLS FIRST, chunk_version_id NULLS FIRST
            """,
            (epoch_id,),
        ).fetchall()
    )


def _payloads() -> dict[str, StructuralPayload]:
    zero = document("zero", "No registered claim matches this chunk.")
    support = document("support", "The required claim is supported.")
    neutral = document(
        "neutral", "The required claim is merely mentioned.", document_id="history-swap"
    )
    mixed = document("mixed", "One required and one optional claim are considered.")
    refute = document(
        "refute", "The required claim is refuted.", document_id="history-swap"
    )
    retry = document("retry", "Retry produces an empty discovery closure.")
    late = document("late", "A verifier result arrives after epoch failure.")
    rollback = document("rollback", "A completion transaction is rolled back.")
    return {
        "history-zero": StructuralPayload(inserted=zero),
        "history-support": StructuralPayload(inserted=support),
        "history-delete-support": StructuralPayload(
            deactivated_document_version_id=support.version.document_version_id
        ),
        "history-neutral": StructuralPayload(inserted=neutral),
        "history-mixed": StructuralPayload(inserted=mixed),
        "history-replace-refute": StructuralPayload(
            inserted=refute,
            deactivated_document_version_id=neutral.version.document_version_id,
        ),
        "history-retry": StructuralPayload(inserted=retry),
        "history-late": StructuralPayload(inserted=late),
        "history-rollback": StructuralPayload(inserted=rollback),
    }


def _labels() -> dict[tuple[str, str], str]:
    return {
        (REQUIRED_CLAIM_ID, "history-chunk-support"): "support",
        (REQUIRED_CLAIM_ID, "history-chunk-neutral"): "neutral",
        (REQUIRED_CLAIM_ID, "history-chunk-mixed"): "neutral",
        (OPTIONAL_CLAIM_ID, "history-chunk-mixed"): "refute",
        (REQUIRED_CLAIM_ID, "history-chunk-refute"): "refute",
        (REQUIRED_CLAIM_ID, "history-chunk-late"): "neutral",
        (REQUIRED_CLAIM_ID, "history-chunk-rollback"): "neutral",
    }


@dataclass(frozen=True, slots=True)
class SuccessfulHistoryResult:
    sql_by_event: dict[str, tuple[str, ...]]
    accounting_by_event: dict[str, ExecutionAccounting]
    jobs_by_event: dict[str, tuple[tuple[object, ...], ...]]
    retry_sql: tuple[str, ...]
    retry_attempts: tuple[tuple[int, str], ...]


def _run_successful_history(
    database_url: str,
    monkeypatch: pytest.MonkeyPatch,
    *,
    registry_size: int,
    unrelated_job_count: int,
) -> SuccessfulHistoryResult:
    with isolated_connection(
        database_url, label=f"history_{registry_size}_{unrelated_job_count}"
    ) as connection:
        payloads = _payloads()
        base, ports = prepare_measured_ports(
            connection,
            registry_size=registry_size,
            unrelated_job_count=unrelated_job_count,
            payloads=payloads,
        )
        admission = HistoryAdmission(
            {
                "history-chunk-support": (REQUIRED_CLAIM_ID,),
                "history-chunk-neutral": (REQUIRED_CLAIM_ID,),
                "history-chunk-mixed": (
                    REQUIRED_CLAIM_ID,
                    OPTIONAL_CLAIM_ID,
                ),
                "history-chunk-refute": (REQUIRED_CLAIM_ID,),
            }
        )
        verifier = HistoryVerifier(_labels())
        app = application(connection, ports, admission, verifier)
        sql_by_event: dict[str, tuple[str, ...]] = {}
        accounting_by_event: dict[str, ExecutionAccounting] = {}
        jobs_by_event: dict[str, tuple[tuple[object, ...], ...]] = {}

        def run(event: Any) -> GuardedEvent:
            guarded = run_guarded_event(connection, monkeypatch, ports, app, event)
            assert guarded.result.state is EventRunState.SEALED
            sql_by_event[event.update.event_id] = guarded.sql
            accounting_by_event[event.update.event_id] = guarded.accounting
            jobs_by_event[event.update.event_id] = _event_job_projection(
                connection, guarded.result.epoch_id
            )
            audit_after_kernel(connection, ports, guarded.result.epoch_id)
            return guarded

        zero = compact_event(
            "history-zero",
            UpdateKind.INSERT,
            base,
            inserted=("history-chunk-zero",),
        )
        zero_result = run(zero)
        assert zero_result.result.verifier_call_count == 0
        assert _event_job_projection(connection, zero_result.result.epoch_id) == (
            (
                "impact_discovery",
                None,
                "history-chunk-zero",
                "completed_active",
                False,
                True,
            ),
        )

        support = compact_event(
            "history-support",
            UpdateKind.INSERT,
            zero_result.result.epoch_id,
            inserted=("history-chunk-support",),
        )
        support_result = run(support)
        assert _status(connection) == ("supported", "valid")

        deletion = compact_event(
            "history-delete-support",
            UpdateKind.DELETE,
            support_result.result.epoch_id,
            deactivated=("history-chunk-support",),
        )
        deletion_result = run(deletion)
        assert deletion_result.result.discovery_call_count == 1
        assert deletion_result.result.verifier_call_count == 0
        assert _status(connection) == ("unsupported", "unsupported")
        assert _event_job_projection(connection, deletion_result.result.epoch_id) == (
            (
                "frontier_retrieve",
                REQUIRED_CLAIM_ID,
                None,
                "completed_active",
                False,
                True,
            ),
        )
        assert connection.execute(
            """
            SELECT result.fallback_satisfied, result.admitted_pair_count,
                   scope.root_job_id IS NULL
            FROM groundloop_m4_discovery_result AS result
            LEFT JOIN groundloop_discovery_scope AS scope
              ON scope.root_job_id = result.root_job_id
            WHERE result.epoch_id = %s
            """,
            (deletion_result.result.epoch_id,),
        ).fetchone() == (True, 0, True)

        neutral = compact_event(
            "history-neutral",
            UpdateKind.INSERT,
            deletion_result.result.epoch_id,
            inserted=("history-chunk-neutral",),
        )
        neutral_result = run(neutral)
        assert _status(connection) == ("unsupported", "unsupported")

        mixed = compact_event(
            "history-mixed",
            UpdateKind.INSERT,
            neutral_result.result.epoch_id,
            inserted=("history-chunk-mixed",),
        )
        mixed_result = run(mixed)
        assert mixed_result.result.verifier_call_count == 2
        assert _status(connection, OPTIONAL_CLAIM_ID) == (
            "refuted",
            "unsupported",
        )
        assert connection.execute(
            """
            SELECT claim_id, required, answer_version_id
            FROM groundloop_claim WHERE claim_id IN (%s, %s)
            ORDER BY claim_id
            """,
            (REQUIRED_CLAIM_ID, OPTIONAL_CLAIM_ID),
        ).fetchall() == [
            (REQUIRED_CLAIM_ID, True, REQUIRED_ANSWER_ID),
            (OPTIONAL_CLAIM_ID, False, REQUIRED_ANSWER_ID),
        ]
        assert connection.execute(
            """
            SELECT job.claim_id, transition.override_rows_written
            FROM groundloop_semantic_job AS job
            JOIN groundloop_m4_evaluation_counter_transition AS transition
              ON transition.epoch_id = job.epoch_id
             AND transition.transition_id = job.completion_digest
            WHERE job.epoch_id = %s AND job.job_kind = 'verify_pair'
            ORDER BY job.claim_id
            """,
            (mixed_result.result.epoch_id,),
        ).fetchall() == [
            (REQUIRED_CLAIM_ID, 2),
            (OPTIONAL_CLAIM_ID, 1),
        ]

        replacement = compact_event(
            "history-replace-refute",
            UpdateKind.REPLACE,
            mixed_result.result.epoch_id,
            inserted=("history-chunk-refute",),
            deactivated=("history-chunk-neutral",),
        )
        replacement_result = run(replacement)
        assert _status(connection) == ("refuted", "contradicted")

        # Exercise the composed retryable-failure transition: runtime and
        # evaluation state move atomically, exact failure replay is a no-op,
        # and reacquisition must not double-count the frontier pending delta.
        retry = compact_event(
            "history-retry",
            UpdateKind.INSERT,
            replacement_result.result.epoch_id,
            inserted=("history-chunk-retry",),
        )
        withdrawal = ports.plan_exact_withdrawal(retry)
        roots = app._root_jobs(retry, withdrawal.fallback_claim_ids)
        assert len(roots) == 1
        scopes = (
            DiscoveryScope(roots[0].job_id, retry.claim_registry_snapshot_id, ()),
        )
        connection.reset_statement_trace()
        with successful_kernel_guard(monkeypatch, ports):
            opened = ports.open_event(retry, withdrawal, roots, scopes)
            first = ports.acquire_job(opened.epoch_id, roots[0])
            assert first.attempt_id is not None and first.expected_revision is not None
            ports.mark_retryable_failure(opened.epoch_id, first)
            ports.mark_retryable_failure(opened.epoch_id, first)
            second = ports.acquire_job(opened.epoch_id, roots[0])
            discovered = admission.discover(opened.epoch_id, roots[0])
            closure = ChildClosure.build(
                parent_job_id=roots[0].job_id,
                result_artifact_hash=discovered.result_artifact_hash,
                child_job_ids=(),
            )
            completion = JobCompletion.build(
                job_id=roots[0].job_id,
                payload_hash=roots[0].payload_hash,
                execution_spec_hash=roots[0].execution_spec_hash,
                result_artifact_id=discovered.result_artifact_id,
                result_artifact_hash=discovered.result_artifact_hash,
                terminal_state=JobState.COMPLETED_ACTIVE,
                child_closure=closure,
            )
            ports.complete_expansion(
                opened.epoch_id, second, discovered, completion, ()
            )
            snapshot = ports.sealing_snapshot(opened.epoch_id)
            assert snapshot.ready
            ports.check_grounding(opened.epoch_id)
            ports.check_coordination(opened.epoch_id)
            ports.check_evaluation(opened.epoch_id)
            ports.request_seal(opened.epoch_id, snapshot.revision, retry.update)
            replay = app.run_event(retry)
            assert replay.state is EventRunState.REPLAYED
            assert replay.discovery_call_count == replay.verifier_call_count == 0
        retry_sql = tuple(connection.statement_fingerprints)
        assert_point_bounded_sql(retry_sql)
        retry_attempts = tuple(
            (int(row[0]), str(row[1]))
            for row in connection.execute(
                """
                SELECT attempt_ordinal, attempt_state
                FROM groundloop_semantic_job_attempt
                WHERE job_id = %s ORDER BY attempt_ordinal
                """,
                (roots[0].job_id,),
            ).fetchall()
        )
        assert retry_attempts == ((1, "failed"), (2, "completed"))
        audit_after_kernel(connection, ports, opened.epoch_id)

        before_conflict = table_projection(
            connection, all_groundloop_table_names(connection)
        )
        with (
            successful_kernel_guard(monkeypatch, ports),
            pytest.raises(EventConflictError),
        ):
            app.run_event(conflicting_event(retry))
        assert (
            table_projection(connection, all_groundloop_table_names(connection))
            == before_conflict
        )

        return SuccessfulHistoryResult(
            sql_by_event,
            accounting_by_event,
            jobs_by_event,
            retry_sql,
            retry_attempts,
        )


def test_successful_measured_histories_are_independent_of_unrelated_scale(
    physical_gate_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    small = _run_successful_history(
        physical_gate_database_url,
        monkeypatch,
        registry_size=SMALL_SCALE[0],
        unrelated_job_count=SMALL_SCALE[1],
    )
    large = _run_successful_history(
        physical_gate_database_url,
        monkeypatch,
        registry_size=LARGE_SCALE[0],
        unrelated_job_count=LARGE_SCALE[1],
    )

    assert small.sql_by_event == large.sql_by_event
    assert small.accounting_by_event == large.accounting_by_event
    assert small.jobs_by_event == large.jobs_by_event
    assert small.retry_sql == large.retry_sql
    assert (
        small.retry_attempts
        == large.retry_attempts
        == (
            (1, "failed"),
            (2, "completed"),
        )
    )


def test_failed_epoch_and_late_inactive_completion_preserve_full_publication(
    physical_gate_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with isolated_connection(
        physical_gate_database_url, label="history_late"
    ) as connection:
        payloads = _payloads()
        base, ports = prepare_measured_ports(
            connection,
            registry_size=SMALL_SCALE[0],
            unrelated_job_count=SMALL_SCALE[1],
            payloads=payloads,
        )
        event = compact_event(
            "history-late",
            UpdateKind.INSERT,
            base,
            inserted=("history-chunk-late",),
        )
        admission = HistoryAdmission({"history-chunk-late": (REQUIRED_CLAIM_ID,)})
        failing = FailingVerifier()
        app = application(connection, ports, admission, failing)
        publication_before = table_projection(connection, PUBLICATION_TABLES)

        # Failure cleanup deliberately rehydrates the last published snapshot;
        # the formal successful-event bound excludes this recovery work.
        hydration_calls = 0
        original_loader = __import__(
            "groundloop.m4.pipeline", fromlist=["_load_published_repository"]
        )._load_published_repository

        def count_hydration(*args: object, **kwargs: object) -> Any:
            nonlocal hydration_calls
            hydration_calls += 1
            return original_loader(*args, **kwargs)

        import groundloop.m4.pipeline as pipeline

        connection.reset_statement_trace()
        with monkeypatch.context() as guard:
            guard.setattr(pipeline, "_load_published_repository", count_hydration)
            guard.setattr(copy, "deepcopy", forbid_global_path)
            guard.setattr(pipeline, "deepcopy", forbid_global_path)
            guard.setattr(pipeline, "compute_all_states", forbid_global_path)
            guard.setattr(ports.runtime_store, "read_epoch", forbid_global_path)
            guard.setattr(ports.runtime_store, "read_book", forbid_global_path)
            guard.setattr(ports, "_assert_grounding_equality", forbid_global_path)
            failed = app.run_event(event)
        assert failed.state is EventRunState.FAILED
        assert hydration_calls == 1
        assert table_projection(connection, PUBLICATION_TABLES) == publication_before

        root = app._root_jobs(event, ())[0]
        child = ports.runtime_store.read_children_point(failed.epoch_id, root.job_id)[0]
        lease = ports.acquire_job(failed.epoch_id, child)
        verified = HistoryVerifier(_labels()).verify(failed.epoch_id, child)
        completion = JobCompletion.build(
            job_id=child.job_id,
            payload_hash=child.payload_hash,
            execution_spec_hash=child.execution_spec_hash,
            result_artifact_id=verified.result_artifact_id,
            result_artifact_hash=verified.result_artifact_hash,
            terminal_state=JobState.COMPLETED_INACTIVE,
        )
        connection.reset_statement_trace()
        with successful_kernel_guard(monkeypatch, ports):
            receipt = ports.complete_verifier_atomically(
                failed.epoch_id,
                lease,
                child,
                completion,
                verified.observation,
                make_effective=False,
            )
        assert receipt.artifact_stored and not receipt.made_effective
        assert_point_bounded_sql(tuple(connection.statement_fingerprints))
        assert table_projection(connection, PUBLICATION_TABLES) == publication_before
        failed_header = ports.runtime_store.read_epoch_header_point(failed.epoch_id)
        assert failed_header.state.value == "failed"
        assert connection.execute(
            "SELECT job_state FROM groundloop_semantic_job WHERE job_id = %s",
            (child.job_id,),
        ).fetchone() == ("completed_inactive",)

        before_replay = table_projection(
            connection, all_groundloop_table_names(connection)
        )
        replay = app.run_event(event)
        assert replay.state is EventRunState.FAILED
        assert failing.calls == 1
        assert (
            table_projection(connection, all_groundloop_table_names(connection))
            == before_replay
        )


def test_conflicting_sealed_replay_changes_no_table(
    physical_gate_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with isolated_connection(
        physical_gate_database_url, label="history_conflict"
    ) as connection:
        payloads = _payloads()
        base, ports = prepare_measured_ports(
            connection,
            registry_size=SMALL_SCALE[0],
            unrelated_job_count=SMALL_SCALE[1],
            payloads=payloads,
        )
        event = compact_event(
            "history-zero",
            UpdateKind.INSERT,
            base,
            inserted=("history-chunk-zero",),
        )
        admission = HistoryAdmission({})
        verifier = HistoryVerifier({})
        app = application(connection, ports, admission, verifier)
        sealed = run_guarded_event(connection, monkeypatch, ports, app, event)
        assert sealed.result.state is EventRunState.SEALED

        connection.reset_statement_trace()
        with successful_kernel_guard(monkeypatch, ports):
            replay = app.run_event(event)
        assert replay.state is EventRunState.REPLAYED
        assert replay.discovery_call_count == replay.verifier_call_count == 0
        assert_point_bounded_sql(tuple(connection.statement_fingerprints))

        before = table_projection(connection, all_groundloop_table_names(connection))
        with (
            successful_kernel_guard(monkeypatch, ports),
            pytest.raises(EventConflictError),
        ):
            app.run_event(conflicting_event(event))
        assert (
            table_projection(connection, all_groundloop_table_names(connection))
            == before
        )


def test_verifier_transaction_rollback_restores_every_groundloop_table(
    physical_gate_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before_completion: dict[str, tuple[str, ...]] | None = None
    before_accounting: ExecutionAccounting | None = None

    def inject(point: str) -> None:
        if point == "verifier_state_written":
            raise RuntimeError("injected verifier rollback")

    with isolated_connection(
        physical_gate_database_url, label="history_rollback"
    ) as connection:
        payloads = _payloads()
        base, ports = prepare_measured_ports(
            connection,
            registry_size=SMALL_SCALE[0],
            unrelated_job_count=SMALL_SCALE[1],
            payloads=payloads,
            failure_injector=inject,
        )
        event = compact_event(
            "history-rollback",
            UpdateKind.INSERT,
            base,
            inserted=("history-chunk-rollback",),
        )
        admission = HistoryAdmission({"history-chunk-rollback": (REQUIRED_CLAIM_ID,)})

        @dataclass(slots=True)
        class SnapshotVerifier:
            delegate: HistoryVerifier = field(
                default_factory=lambda: HistoryVerifier(_labels())
            )

            def verify(self, epoch_id: int, job: Any) -> Any:
                nonlocal before_accounting, before_completion
                result = self.delegate.verify(epoch_id, job)
                before_accounting = ExecutionAccounting.read(connection, epoch_id)
                names = tuple(
                    name
                    for name in all_groundloop_table_names(connection)
                    if name != "groundloop_m4_execution_accounting"
                )
                before_completion = table_projection(connection, names)
                return result

        app = application(connection, ports, admission, SnapshotVerifier())
        import groundloop.m4.pipeline as pipeline

        with monkeypatch.context() as guard:
            guard.setattr(copy, "deepcopy", forbid_global_path)
            guard.setattr(pipeline, "deepcopy", forbid_global_path)
            guard.setattr(pipeline, "compute_all_states", forbid_global_path)
            guard.setattr(ports.runtime_store, "read_epoch", forbid_global_path)
            guard.setattr(ports.runtime_store, "read_book", forbid_global_path)
            guard.setattr(ports, "_assert_grounding_equality", forbid_global_path)
            with pytest.raises(RuntimeError, match="injected verifier rollback"):
                app.run_event(event)
        assert before_accounting is not None and before_completion is not None
        names = tuple(
            name
            for name in all_groundloop_table_names(connection)
            if name != "groundloop_m4_execution_accounting"
        )
        assert table_projection(connection, names) == before_completion
        epoch_id = connection.execute(
            "SELECT epoch_id FROM groundloop_epoch WHERE event_id = %s",
            (event.update.event_id,),
        ).fetchone()
        assert epoch_id is not None
        after_accounting = ExecutionAccounting.read(connection, int(epoch_id[0]))
        assert after_accounting == replace(
            before_accounting,
            active_chunk_rows_examined=(
                before_accounting.active_chunk_rows_examined + 1
            ),
        )
        assert connection.execute(
            "SELECT count(*) FROM groundloop_semantic_observation "
            "WHERE produced_epoch = %s",
            (epoch_id[0],),
        ).fetchone() == (0,)
